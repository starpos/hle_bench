#pragma once
/**
 * @file
 * @description benchmark utilities.
 * @author HOSHINO Takashi
 * @license GPLv2 or later, new BSD.
 *
 * (C) 2013 HOSHINO Takashi
 */
#include <cstdio>
#include <chrono>
#include <thread>
#include <atomic>
#include <stdexcept>

#include <immintrin.h> /* for _mm_pause() */

#include "thread_util.hpp"
#include "time.hpp"

namespace bench {

static inline void delayUsec(uint64_t usec)
{
    if (1000 < usec) {
        throw std::runtime_error("Over 1000 usec busy sleep is not allowed.");
    }
    auto ts0 = std::chrono::high_resolution_clock::now();
    uint64_t diff = 0;
    while (diff < usec) {
        auto ts1 = std::chrono::high_resolution_clock::now();
        diff = std::chrono::duration_cast<
            std::chrono::microseconds>(ts1 - ts0).count();
    }
    return;
}

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
    void operator()() noexcept {
        try {
            waitForReady();
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
    virtual void run() = 0;
protected:
    void waitForReady() {
        while (!isReady_.load(std::memory_order_relaxed)) {
            _mm_pause();
        }
    }
};

/**
 * Run a benchmark.
 *
 * @thSet thread runner set that contains multiple workers.
 * @isReady shared by all workers.
 * @isEnd shared by all workers.
 * @ts to measure exact execution time.
 * @execMs execution time [ms].
 */
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

} //namespace bench
