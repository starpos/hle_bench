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
    const std::atomic<bool> &isBgn_;
    const std::atomic<bool> &isEnd_;
public:
    SpinWorker(std::atomic_flag &mutex, uint64_t &counter, const std::atomic<bool> &isBgn, const std::atomic<bool> &isEnd)
        : mutex_(mutex), counter_(counter), isBgn_(isBgn), isEnd_(isEnd) {
    }
    void run() {
        while (!isEnd_.load()) {
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
    std::atomic<bool> &isBgn_;
    std::atomic<bool> &isEnd_;
public:
    MutexWorker(std::mutex &mutex, uint64_t &counter, std::atomic<bool> &isBgn, std::atomic<bool> &isEnd)
        : mutex_(mutex), counter_(counter), isBgn_(isBgn), isEnd_(isEnd) {
    }
    void run() {
        while (!isBgn_.load()) {
        }
        while (!isEnd_.load()) {
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

void testSpinlock(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) std::atomic_flag mutex = ATOMIC_FLAG_INIT;
    alignas(64) uint64_t counter = 0;
    alignas(64) std::atomic<bool> isBgn(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<SpinWorker>(mutex, std::ref(counter), isBgn, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    thSet.start();
    ts.pushNow();
    isBgn.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(execMs));
    isEnd.store(true);
    ts.pushNow();
    thSet.join();
    ::printf("Spinlock:  %" PRIu64 " %lu us\n", counter, ts.elapsedInUs());
}

void testMutexlock(size_t nThreads, size_t execMs)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) std::mutex mutex;
    alignas(64) uint64_t counter = 0;
    alignas(64) std::atomic<bool> isBgn(false);
    alignas(64) std::atomic<bool> isEnd(false);
    for (size_t i = 0; i < nThreads; i++) {
        thSet.add(std::make_shared<MutexWorker>(mutex, std::ref(counter), isBgn, isEnd));
    }
    cybozu::time::TimeStack<> ts;
    thSet.start();
    ts.pushNow();
    isBgn.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(execMs));
    isEnd.store(true);
    ts.pushNow();
    thSet.join();
    ::printf("Mutexlock: %" PRIu64 " %lu us\n", counter, ts.elapsedInUs());
}

int main(int argc, char *argv[])
{
    size_t nThreads = 1;
    if (1 < argc) nThreads = ::atoi(argv[1]);
    size_t execMs = 1000;
    size_t nTrials = 5;
    for (size_t i = 0; i < nTrials; i++) {
        testSpinlock(nThreads, execMs);
    }
    for (size_t i = 0; i < nTrials; i++) {
        testMutexlock(nThreads, execMs);
    }
    return 0;
}
