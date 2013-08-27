#include <cstdio>
#include <atomic>
#include <thread>
#include <memory>
#include <cinttypes>
#include <mutex>
#include <chrono>
#include <functional>
#include <stdexcept>

#include <immintrin.h> /* for _mm_pause() */

#include "thread_util.hpp"
#include "time.hpp"

#include "spinlock.hpp"
#include "bench_util.hpp"

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
 * Using spinlock.
 */
class SpinWorker : public bench::Worker
{
private:
    std::atomic_flag &mutex_;
    uint64_t &counter_;
public:
    SpinWorker(std::atomic_flag &mutex, uint64_t &counter,
               const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : bench::Worker(isReady, isEnd), mutex_(mutex), counter_(counter) {
    }
private:
    void run() override {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            cybozu::Spinlock lk(mutex_);
            counter_++;
        }
    }
};

/**
 * Using spinlock with HLE.
 */
template <int useHLE>
class SpinHleWorker : public bench::Worker
{
private:
    char &mutex_;
    uint64_t &counter_;
public:
    SpinHleWorker(char &mutex, uint64_t &counter,
                  const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : bench::Worker(isReady, isEnd), mutex_(mutex), counter_(counter) {
    }
private:
    void run() override {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            cybozu::SpinlockHle<useHLE> lk(mutex_);
            counter_++;
            //delayUsec(1);
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
    uint64_t &counter_;
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
 * Recent Intel CPU's cacheline size is 64byte.
 * You should use this with alignas(64).
 */
struct CacheLine
{
    uint64_t i[8]; // 64 byte.
    CacheLine() { i[0] = 0; }
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
    ::printf("None:        %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

/**
 * Run counter benchmark with SpinWorker.
 * Collision 0%.
 */
void testSpinlock0pct(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) std::atomic_flag mutex = ATOMIC_FLAG_INIT;
    std::vector<CacheLine> counterV(nThreads);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinWorker>(mutex, counterV[i].i[0], isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    uint64_t counter = 0;
    for (auto c : counterV) counter += c.i[0];
    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("Spinlock0:   %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

/**
 * Run counter benchmark with SpinHleWorker.
 * Collision 0%.
 */
template <int useHLE>
void testSpinlockHle0pct(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) char mutex = 0;
    std::vector<CacheLine> counterV(nThreads);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinHleWorker<useHLE> >(
                      mutex, counterV[i].i[0], isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    uint64_t counter = 0;
    for (auto c : counterV) counter += c.i[0];
    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("SpinHle0(%d): %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , useHLE, counter, ts.elapsedInUs(), nThreads, throughput, latency);
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
    ::printf("Atomic:      %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter.load(), ts.elapsedInUs(), nThreads, throughput, latency);
}

/**
 * Run counter benchmark using Spinlock.
 * Collision 100%.
 */
void testSpinlock100pct(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) std::atomic_flag mutex = ATOMIC_FLAG_INIT;
    alignas(64) uint64_t counter = 0;
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinWorker>(mutex, counter, isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("Spinlock:    %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

/**
 * Run counter benchmark using SpinlockHle.
 * Collision 100%.
 */
template <int useHLE>
void testSpinlockHle100pct(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) char mutex = 0;
    alignas(64) uint64_t counter = 0;
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinHleWorker<useHLE> >(
                      mutex, std::ref(counter), isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("SpinHle100(%d):  %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , useHLE, counter, ts.elapsedInUs(), nThreads, throughput, latency);
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
    ::printf("Mutexlock:   %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

int main(__attribute__((unused)) int argc, __attribute__((unused)) char *argv[])
{
    //size_t nThreads = 4;
    //if (1 < argc) nThreads = ::atoi(argv[1]);
    size_t execMs = 10000;
    size_t nTrials = 10;
    for (size_t nThreads = 1; nThreads <= 8; nThreads++) {
        for (size_t i = 0; i < nTrials; i++) {
            testNone(nThreads, execMs);
            testAtomic(nThreads, execMs);
            testMutexlock(nThreads, execMs);

            testSpinlock100pct(nThreads, execMs);
            testSpinlockHle100pct<0>(nThreads, execMs);
            testSpinlockHle100pct<1>(nThreads, execMs);

            testSpinlock0pct(nThreads, execMs);
            testSpinlockHle0pct<0>(nThreads, execMs);
            testSpinlockHle0pct<1>(nThreads, execMs);
        }
    }
    return 0;
}
