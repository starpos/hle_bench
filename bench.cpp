#include <cstdio>
#include <atomic>
#include <thread>
#include <memory>
#include <cinttypes>
#include <mutex>
#include <immintrin.h> /* for _mm_pause() */

#include "thread_util.hpp"
#include "time.hpp"

static inline void pause(void)
{
    asm volatile("pause" ::: "memory");
}

class Spinlock
{
private:
    std::atomic_flag &lock_;
public:
    explicit Spinlock(std::atomic_flag &lock) : lock_(lock) {
        while (lock_.test_and_set(std::memory_order_acquire)) {
            pause();
        }
    }
    ~Spinlock() noexcept {
        lock_.clear(std::memory_order_release);
    }
};

template <int useHLE>
class SpinlockHle
{
private:
    char &lock_;
public:
    explicit SpinlockHle(char &lock) : lock_(lock) {
        /* Acquire lock with lock elision */
        int flags = __ATOMIC_ACQUIRE | (useHLE ? __ATOMIC_HLE_ACQUIRE : 0);
        while (__atomic_exchange_n(&lock_, 1, flags))
            _mm_pause(); /* Abort failed transaction */
    }
    ~SpinlockHle() noexcept {
        /* Free lock with lock elision */
        int flags = __ATOMIC_RELEASE | (useHLE ? __ATOMIC_HLE_RELEASE : 0);
        __atomic_clear(&lock_, flags);
    }
};

class Worker : public cybozu::thread::Runnable
{
protected:
    const std::atomic<bool> &isReady_;
    const std::atomic<bool> &isEnd_;
public:
    Worker(const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : isReady_(isReady), isEnd_(isEnd) {
    }
    virtual ~Worker() noexcept = default;
protected:
    void waitForReady() {
        while (!isReady_.load(std::memory_order_relaxed)) {
            pause();
        }
    }
};

class NoneWorker : public Worker
{
private:
    uint64_t &counter_;
public:
    NoneWorker(uint64_t &counter, const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : Worker(isReady, isEnd), counter_(counter) {
    }
    void operator()() noexcept override {
        try {
            waitForReady();
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
private:
    void run() {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            counter_++;
        }
    }
};

class AtomicWorker : public Worker
{
private:
    std::atomic<uint64_t> &counter_;
public:
    AtomicWorker(std::atomic<uint64_t> &counter,
                 const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : Worker(isReady, isEnd), counter_(counter) {
    }
    void operator()() noexcept override {
        try {
            waitForReady();
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
private:
    void run() {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            counter_.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

class SpinWorker : public Worker
{
private:
    std::atomic_flag &mutex_;
    uint64_t &counter_;
public:
    SpinWorker(std::atomic_flag &mutex, uint64_t &counter,
               const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : Worker(isReady, isEnd), mutex_(mutex), counter_(counter) {
    }
    void operator()() noexcept override {
        try {
            waitForReady();
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
private:
    void run() {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            Spinlock lk(mutex_);
            counter_++;
        }
    }
};

template <int useHLE>
class SpinHleWorker : public Worker
{
private:
    char &mutex_;
    uint64_t &counter_;
public:
    SpinHleWorker(char &mutex, uint64_t &counter,
                  const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : Worker(isReady, isEnd), mutex_(mutex), counter_(counter) {
    }
    void operator()() noexcept override {
        try {
            waitForReady();
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
private:
    void run() {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            SpinlockHle<useHLE> lk(mutex_);
            counter_++;
        }
    }
};

class MutexWorker : public Worker
{
private:
    std::mutex &mutex_;
    uint64_t &counter_;
public:
    MutexWorker(std::mutex &mutex, uint64_t &counter,
                const std::atomic<bool> &isReady, const std::atomic<bool> &isEnd)
        : Worker(isReady, isEnd), mutex_(mutex), counter_(counter) {
    }
    void operator()() noexcept override {
        try {
            waitForReady();
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
private:
    void run() {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            std::lock_guard<std::mutex> lk(mutex_);
            counter_++;
        }
    }
};

void runBench(cybozu::thread::ThreadRunnerSet &thSet,
              std::atomic<bool> &isReady, std::atomic<bool> &isEnd,
              cybozu::time::TimeStack<> &ts, size_t execMs)
{
    thSet.start();
    ts.pushNow();
    isReady.store(true, std::memory_order_relaxed);
    std::this_thread::sleep_for(std::chrono::milliseconds(execMs));
    isEnd.store(true, std::memory_order_relaxed);
    ts.pushNow();
    thSet.join();
}

void testNone(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    std::vector<uint64_t> counterV(nThreads, 0);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<NoneWorker>(counterV[i], isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    runBench(thSet, isReady, isEnd, ts, execMs);

    uint64_t counter = 0;
    for (uint64_t c : counterV) counter += c;
    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("None:        %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

void testSpinlockN(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) std::atomic_flag mutex = ATOMIC_FLAG_INIT;
    std::vector<uint64_t> counterV(nThreads, 0);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinWorker>(mutex, counterV[i], isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    runBench(thSet, isReady, isEnd, ts, execMs);

    uint64_t counter = 0;
    for (uint64_t c : counterV) counter += c;
    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("SpinlockN:   %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

template <int useHLE>
void testSpinlockHleN(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) char mutex = 0;
    std::vector<uint64_t> counterV(nThreads, 0);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinHleWorker<useHLE> >(mutex, counterV[i], isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    runBench(thSet, isReady, isEnd, ts, execMs);

    uint64_t counter = 0;
    for (uint64_t c : counterV) counter += c;
    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("SpinHleN(%d): %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , useHLE, counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

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
    runBench(thSet, isReady, isEnd, ts, execMs);

    double throughput = counter.load() / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter.load();
    ::printf("Atomic:      %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter.load(), ts.elapsedInUs(), nThreads, throughput, latency);
}

void testSpinlock(size_t nThreads, size_t execMs)
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
    runBench(thSet, isReady, isEnd, ts, execMs);

    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("Spinlock:    %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

template <int useHLE>
void testSpinlockHle(size_t nThreads, size_t execMs)
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
    runBench(thSet, isReady, isEnd, ts, execMs);

    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("SpinHle(%d):  %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , useHLE, counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

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
    runBench(thSet, isReady, isEnd, ts, execMs);

    double throughput = counter / (double)ts.elapsedInUs();
    double latency = ts.elapsedInNs() / (double)counter;
    ::printf("Mutexlock:   %12" PRIu64 " counts  %lu us  %zu threads  %f counts/us  %f ns/count\n"
             , counter, ts.elapsedInUs(), nThreads, throughput, latency);
}

int main(int argc, char *argv[])
{
    //size_t nThreads = 4;
    //if (1 < argc) nThreads = ::atoi(argv[1]);
    size_t execMs = 1000;
    size_t nTrials = 1;
    for (size_t nThreads = 1; nThreads <= 4; nThreads++) {
        for (size_t i = 0; i < nTrials; i++) {
            testNone(nThreads, execMs);
#if 1
            testAtomic(nThreads, execMs);
            testSpinlock(nThreads, execMs);
            testSpinlockHle<0>(nThreads, execMs);
            testSpinlockHle<1>(nThreads, execMs);
            testMutexlock(nThreads, execMs);
#endif
            testSpinlockN(nThreads, execMs);
            testSpinlockHleN<0>(nThreads, execMs);
            testSpinlockHleN<1>(nThreads, execMs);
        }
    }
    return 0;
}
