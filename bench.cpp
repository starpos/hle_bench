#include <cstdio>
#include <atomic>
#include <thread>
#include <memory>
#include <cinttypes>
#include <mutex>

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

class SpinWorker : public cybozu::thread::Runnable
{
private:
    std::atomic_flag &mutex_;
    uint64_t &counter_;
    const uint64_t n_;
    
public:
    SpinWorker(std::atomic_flag &mutex, uint64_t &counter, uint64_t n)
        : mutex_(mutex), counter_(counter), n_(n) {
    }
    void run() {
        for (uint64_t i = 0; i < n_; i++) {
            Spinlock lk(mutex_);
            counter_++;
        }
    }
    void operator()() noexcept override {
        try {
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
};

class MutexWorker : public cybozu::thread::Runnable
{
private:
    std::mutex &mutex_;
    uint64_t &counter_;
    const uint64_t n_;
public:
    MutexWorker(std::mutex &mutex, uint64_t &counter, uint64_t n)
        : mutex_(mutex), counter_(counter), n_(n) {
    }
    void run() {
        for (uint64_t i = 0; i < n_; i++) {
            std::lock_guard<std::mutex> lk(mutex_);
            counter_++;
        }
    }
    void operator()() noexcept override {
        try {
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
};

void testSpinlock(size_t nThreads, uint64_t n)
{
    cybozu::thread::ThreadRunnerSet thSet;
    std::atomic_flag mutex = ATOMIC_FLAG_INIT;
    uint64_t counter = 0;
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinWorker>(mutex, counter, n));
    }
    cybozu::time::TimeStack<> ts;
    ts.pushNow();
    thSet.start();
    thSet.join();
    ts.pushNow();
    ::printf("Spinlock:  %" PRIu64 " %lu us\n", counter, ts.elapsedInUs());
}

void testMutexlock(size_t nThreads, uint64_t n)
{
    cybozu::thread::ThreadRunnerSet thSet;
    std::mutex mutex;
    uint64_t counter = 0;
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<MutexWorker>(mutex, counter, n));
    }
    cybozu::time::TimeStack<> ts;
    ts.pushNow();
    thSet.start();
    thSet.join();
    ts.pushNow();
    ::printf("Mutexlock: %" PRIu64 " %lu us\n", counter, ts.elapsedInUs());
}

int main()
{
    size_t nThreads = 6;
    size_t nCounts = 500000;
    size_t nTrials = 5;
    for (size_t i = 0; i < nTrials; i++) {
        testSpinlock(nThreads, nCounts);
    }
    for (size_t i = 0; i < nTrials; i++) {
        testMutexlock(nThreads, nCounts);
    }
    return 0;
}
