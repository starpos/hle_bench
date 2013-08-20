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

class SpinlockHle
{
private:
    char &lock_;
public:
    explicit SpinlockHle(char &lock) : lock_(lock) {
        /* Acquire lock with lock elision */
#if 1
        while (__atomic_exchange_n(&lock_, 1, __ATOMIC_ACQUIRE|__ATOMIC_HLE_ACQUIRE))
#else
        while (__atomic_exchange_n(&lock_, 1, __ATOMIC_ACQUIRE))
#endif
            _mm_pause(); /* Abort failed transaction */
    }
    ~SpinlockHle() noexcept {
        /* Free lock with lock elision */
#if 1
        __atomic_clear(&lock_, __ATOMIC_RELEASE|__ATOMIC_HLE_RELEASE);
#else
        __atomic_clear(&lock_, __ATOMIC_RELEASE);
#endif
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
            SpinlockHle lk(mutex_);
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
    thSet.start();
    ts.pushNow();
    isReady.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(execMs));
    isEnd.store(true);
    ts.pushNow();
    thSet.join();
    ::printf("Spinlock:  %" PRIu64 " %lu us\n", counter, ts.elapsedInUs());
}

void testSpinlockHle(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) char mutex = 0;
    alignas(64) uint64_t counter = 0;
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinHleWorker>(
                      mutex, std::ref(counter), isReady, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    thSet.start();
    ts.pushNow();
    isReady.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(execMs));
    isEnd.store(true);
    ts.pushNow();
    thSet.join();
    ::printf("SpinlockHle:  %" PRIu64 " %lu us\n", counter, ts.elapsedInUs());
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
    thSet.start();
    ts.pushNow();
    isReady.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(execMs));
    isEnd.store(true);
    ts.pushNow();
    thSet.join();
    ::printf("Mutexlock: %" PRIu64 " %lu us\n", counter, ts.elapsedInUs());
}

int main(int argc, char *argv[])
{
    size_t nThreads = 4;
    if (1 < argc) nThreads = ::atoi(argv[1]);
    size_t execMs = 1000;
    size_t nTrials = 5;
    for (size_t i = 0; i < nTrials; i++) {
        testSpinlock(nThreads, execMs);
    }
    for (size_t i = 0; i < nTrials; i++) {
        testSpinlockHle(nThreads, execMs);
    }
    for (size_t i = 0; i < nTrials; i++) {
        testMutexlock(nThreads, execMs);
    }
    return 0;
}
