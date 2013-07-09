#include <cstdio>
#include <atomic>
#include <thread>
#include <memory>
#include <cinttypes>

#include "thread_util.hpp"

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
            //pause();
        }
    }
    ~Spinlock() noexcept {
        lock_.clear(std::memory_order_release);
    }
};

class Worker : public cybozu::thread::Runnable
{
private:
    std::atomic_flag &mutex_;
    uint64_t &counter_;
    uint64_t n_;
    
public:
    Worker(std::atomic_flag &mutex, uint64_t &counter, uint64_t n)
        : mutex_(mutex), counter_(counter), n_(n) {
    }
    void run() {
        for (uint64_t i = 0; i < n_; i++) {
            //Spinlock lk(mutex_);
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

int main()
{
    cybozu::thread::ThreadRunnerSet thSet;
    std::atomic_flag mutex = ATOMIC_FLAG_INIT;
    uint64_t counter = 0;
    for (size_t i = 0; i < 10; i++) {
        thSet.add(std::make_shared<Worker>(mutex, counter, 100000));
    }
    thSet.start();
    thSet.join();
    ::printf("%" PRIu64 "\n", counter);
    return 0;
}
