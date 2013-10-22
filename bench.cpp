#include <cstdio>
#include <atomic>
#include <thread>
#include <memory>
#include <cinttypes>
#include <mutex>
#include <chrono>
#include <functional>
#include <stdexcept>
#include <cassert>

#include <immintrin.h> /* for _mm_pause() */

#include "thread_util.hpp"
#include "time.hpp"

#include "spinlock.hpp"
#include "bench_util.hpp"
#include "util.hpp"

/**
 * Recent Intel CPU's cacheline size is 64byte.
 * You should use this with alignas(64).
 */
struct CacheLine
{
    uint64_t i[8]; // 64 byte.
    CacheLine() { i[0] = 0; }
};

/**
 * Counter without any synchronization.
 */
class NoneWorker : public bench::Worker
{
private:
    uint64_t &counter_;
public:
    NoneWorker(uint64_t &counter, const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : bench::Worker(isReady, isEnd), counter_(counter) {
    }
private:
    void run() override {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            counter_++;
        }
    }
};

/**
 * Shared counter using an atomic integer.
 */
class AtomicWorker : public bench::Worker
{
private:
    std::atomic<uint64_t> &counter_;
public:
    AtomicWorker(std::atomic<uint64_t> &counter,
                 const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : bench::Worker(isReady, isEnd), counter_(counter) {
    }
private:
    void run() override {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            counter_.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

/**
 * Using spinlock w/wi HLE, TTAS.
 */
template <bool useHLE, bool useTTAS, int delayUs, bool isCountLater>
class SpinWorkerT : public bench::Worker
{
private:
    char &mutex_;
    uint64_t &counter_;
public:
    SpinWorkerT(char &mutex, uint64_t &counter,
                const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : bench::Worker(isReady, isEnd), mutex_(mutex), counter_(counter) {
    }
private:
    void run() override {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            cybozu::SpinlockT<useHLE, useTTAS> lk(mutex_);
            if (!isCountLater) counter_++;
            if (0 < delayUs) bench::delayUsec(delayUs);
            if (isCountLater) counter_++;
        }
    }
};

/**
 * This will access multiple cache lines.
 * Using spinlock.
 */
template <bool useHLE, bool useTTAS>
class SpinAccessSizeWorkerT : public bench::Worker
{
private:
    char &mutex_;
    uint64_t &counter_; /* number of executed critical sections. not shared. */
    const size_t nAccess_;
    const size_t nLines_;
    std::vector<CacheLine> counters_;
public:
    SpinAccessSizeWorkerT(
        char &mutex, uint64_t &counter,
        size_t nAccess, size_t nLines,
        const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : bench::Worker(isReady, isEnd), mutex_(mutex), counter_(counter)
        , nAccess_(nAccess), nLines_(nLines), counters_(nLines - 1) {
        assert(1 < nLines);
    }
private:
    /**
     * TODO: fair choise of access area.
     */
    void run() override {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            cybozu::SpinlockT<useHLE, useTTAS> lk(mutex_);
            for (size_t i = 0; i < nAccess_; i++) {
                size_t idx = i % (nLines_ - 1);
                counters_[idx].i[0]++;
            }
            counter_++;
         }
     }
 };

/**
 * Using mutex.
 */
class MutexWorker : public bench::Worker
{
private:
    std::mutex &mutex_;
    uint64_t &counter_; /* shared counter */
public:
    MutexWorker(std::mutex &mutex, uint64_t &counter,
                const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : bench::Worker(isReady, isEnd), mutex_(mutex), counter_(counter) {
    }
private:
    void run() override {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            std::lock_guard<std::mutex> lk(mutex_);
            counter_++;
        }
    }
};

/**
 * Run counter benchmark with NoneWorker.
 * Collision 0%.
 */
void testNone(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    std::vector<CacheLine> counterV(nThreads);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<NoneWorker>(counterV[i].i[0], isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    uint64_t counter = 0;
    for (auto c : counterV) counter += c.i[0];
    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("None:       %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
    ::fflush(::stdout);
}

/**
 * Run counter benchmark with atomic value.
 * Collision 100%.
 */
void testAtomic(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) std::atomic<uint64_t> counter(0);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<AtomicWorker>(counter, isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    double throughput = counter.load() / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter.load();
    ::printf("Atomic:     %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter.load(), ts.elapsedInUs(), nThreads, throughput, latency);
    ::fflush(::stdout);
}

/**
 * Run counter benchmark with SpinWorker.
 * Collision 0%.
 */
template <bool useHLE, bool useTTAS>
void testSpinlockSh(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) char mutex = 0;
    std::vector<CacheLine> counterV(nThreads);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinWorkerT<useHLE, useTTAS, 0, false> >(
                      mutex, counterV[i].i[0], isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    uint64_t counter = 0;
    for (auto c : counterV) counter += c.i[0];
    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("SpinSh_%d_%d: %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , useHLE, useTTAS, counter, ts.elapsedInUs(), nThreads, throughput, latency);
    ::fflush(::stdout);
}

/**
 * Run counter benchmark using SpinlockHle.
 * Collision 100%.
 */
template <bool useHLE, bool useTTAS>
void testSpinlockEx(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) char mutex = 0;
    alignas(64) uint64_t counter = 0;
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinWorkerT<useHLE, useTTAS, 0, false> >(
                      mutex, counter, isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("SpinEx_%d_%d: %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , useHLE, useTTAS, counter, ts.elapsedInUs(), nThreads, throughput, latency);
    ::fflush(::stdout);
}

/**
 * Run counter benchmark using mutex.
 * Collision 100%.
 */
void testMutexlock(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) std::mutex mutex;
    alignas(64) uint64_t counter = 0;
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<MutexWorker>(mutex, counter, isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("Mutexlock:  %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
    ::fflush(::stdout);
}

int main(UNUSED int argc, UNUSED char *argv[])
{
    //size_t nThreads = 4;
    //if (1 < argc) nThreads = ::atoi(argv[1]);
#if 1
    size_t execMs = 10000;
    size_t nTrials = 20;
#else
    size_t execMs = 3000;
    size_t nTrials = 2;
#endif
    for (size_t nThreads = 1; nThreads <= 12; nThreads++) {
        for (size_t i = 0; i < nTrials; i++) {
            testNone(nThreads, execMs);
            testAtomic(nThreads, execMs);
            testMutexlock(nThreads, execMs);

            testSpinlockEx<0,0>(nThreads, execMs);
            testSpinlockEx<0,1>(nThreads, execMs);
            testSpinlockEx<1,0>(nThreads, execMs);
            testSpinlockEx<1,1>(nThreads, execMs);
            testSpinlockSh<0,0>(nThreads, execMs);
            testSpinlockSh<0,1>(nThreads, execMs);
            testSpinlockSh<1,0>(nThreads, execMs);
            testSpinlockSh<1,1>(nThreads, execMs);
        }
    }
    return 0;
}
