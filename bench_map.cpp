#include <cstdio>
#include <map>
#include <atomic>
#include <vector>
#include <memory>
#include <cinttypes>
#include "thread_util.hpp"
#include "random.hpp"
#include "time.hpp"
#include "spinlock.hpp"
#include "bench_util.hpp"
#include "btree.hpp"

using MapT = std::map<uint32_t, uint32_t>;
using BtreeMapT = cybozu::BtreeMap<uint32_t, uint32_t>;

/**
 * uint64_t integer that owns a 64bytes cache line.
 * Use with alignas(64).
 */
struct CacheLine
{
    uint64_t value;
    uint64_t hidden_data[7]; /* not used. */
    CacheLine() : value(0) {}
};

template <int useHLE>
class SpinStdMapWorker : public bench::Worker
{
private:
    char &mutex_;
    MapT &map_;
    uint64_t &counter_;
    cybozu::util::XorShift128 rand_;
    uint16_t readPct_; /* [0, 10000]. */
public:
    SpinStdMapWorker(char &mutex, MapT &map, uint64_t &counter,
                     uint32_t seed, uint16_t readPct,
                     const std::atomic<bool> &isReady,
                     const std::atomic<bool> &isEnd)
        : bench::Worker(isReady, isEnd)
        , mutex_(mutex), map_(map), counter_(counter)
        , rand_(seed), readPct_(readPct) {
    }
private:
    void run() override {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            cybozu::SpinlockHle<useHLE> lk(mutex_);
            runCriticalSection();
            counter_++;
        }
    }
    void runCriticalSection() {
        bool isDeleted = false;
        if (!map_.empty()) {
            while (true) {
                /* Search a key. */
                auto it = map_.lower_bound(rand_());
                if (it == map_.end()) continue;
                if (readPct_ <= rand_() % 10000) {
                    /* Delete a value. */
                    map_.erase(it);
                    isDeleted = true;
                }
                break;
            }
        }
        /* Insert */
        if (isDeleted) {
            map_.insert(std::make_pair(rand_(), 0));
        }
    }
};

template <int useHLE>
class SpinBtreeMapWorker : public bench::Worker
{
private:
    char &mutex_;
    BtreeMapT &map_;
    uint64_t &counter_;
    cybozu::util::XorShift128 rand_;
    uint16_t readPct_; /* [0, 10000]. */
public:
    SpinBtreeMapWorker(char &mutex, BtreeMapT &map, uint64_t &counter,
                       uint32_t seed, uint16_t readPct,
                       const std::atomic<bool> &isReady,
                       const std::atomic<bool> &isEnd)
        : bench::Worker(isReady, isEnd)
        , mutex_(mutex), map_(map), counter_(counter)
        , rand_(seed), readPct_(readPct) {
    }
private:
    void run() override {
        while (!isEnd_.load(std::memory_order_relaxed)) {
            cybozu::SpinlockHle<useHLE> lk(mutex_);
            runCriticalSection();
            counter_++;
        }
    }
    void runCriticalSection() {
        bool isDeleted = false;
        if (!map_.empty()) {
            while (true) {
                /* Search a key. */
                auto it = map_.lowerBound(rand_());
                if (it.isEnd()) continue;
                if (readPct_ <= rand_() % 10000) {
                    /* Delete a value. */
                    it.erase();
                    isDeleted = true;
                }
                break;
            }
        }
        /* Insert */
        if (isDeleted) {
            map_.insert(rand_(), 0);
        }
    }
};

template <int useHLE>
void testSpinStdMapWorker(
    size_t nThreads, size_t execMs, uint32_t nInitItems, uint16_t readPct)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) char mutex = 0;
    std::vector<CacheLine> counterV(nThreads);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    cybozu::util::Random<uint32_t> rand;
    MapT map;
    for (size_t i = 0; i < nInitItems; i++) {
        map.insert(std::make_pair(rand(), 0));
    }
    for (size_t i = 0; i < nThreads; i++) {
        uint32_t seed = rand();
        auto worker = std::make_shared<SpinStdMapWorker<useHLE> >(
            mutex, map, counterV[i].value, seed, readPct, isReady, isEnd);
        thSet.add(worker);
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    uint64_t counter = 0;
    for (const CacheLine &c : counterV) {
        counter += c.value;
    }

    ::printf("SpinStdMap_%d_%" PRIu32 "_%05u    %12" PRIu64 " counts  %lu us  %zu threads\n"
             , useHLE, nInitItems, readPct
             , counter, ts.elapsedInUs(), nThreads);
    ::fflush(::stdout);
}

template <int useHLE>
void testSpinBtreeMapWorker(
    size_t nThreads, size_t execMs, uint32_t nInitItems, uint16_t readPct)
{
    cybozu::thread::ThreadRunnerSet thSet;
    alignas(64) char mutex = 0;
    std::vector<CacheLine> counterV(nThreads);
    alignas(64) std::atomic<bool> isReady(false);
    alignas(64) std::atomic<bool> isEnd(false);
    cybozu::util::Random<uint32_t> rand;
    BtreeMapT map;
    for (size_t i = 0; i < nInitItems; i++) {
        map.insert(rand(), 0);
    }
    for (size_t i = 0; i < nThreads; i++) {
        uint32_t seed = rand();
        auto worker = std::make_shared<SpinBtreeMapWorker<useHLE> >(
            mutex, map, counterV[i].value, seed, readPct, isReady, isEnd);
        thSet.add(worker);
    }
    cybozu::time::TimeStack<> ts;
    bench::runBench(thSet, isReady, isEnd, ts, execMs);

    uint64_t counter = 0;
    for (const CacheLine &c : counterV) {
        counter += c.value;
    }

    ::printf("SpinBtreeMap_%d_%" PRIu32 "_%05u  %12" PRIu64 " counts  %lu us  %zu threads\n"
             , useHLE, nInitItems, readPct
             , counter, ts.elapsedInUs(), nThreads);
    ::fflush(::stdout);
}

int main()
{
    size_t execMs = 10000;
    uint32_t nInitItems = 10000;
    size_t nTrials = 10;
    for (size_t nThreads = 1; nThreads <= 12; nThreads++) {
        //for (uint16_t readPct : {0, 9000, 10000}) {
        for (uint16_t readPct : {0, 9000, 9900, 10000}) {
            for (size_t i = 0; i < nTrials; i++) {
                testSpinStdMapWorker<0>(nThreads, execMs, nInitItems, readPct);
                testSpinStdMapWorker<1>(nThreads, execMs, nInitItems, readPct);
                testSpinBtreeMapWorker<0>(nThreads, execMs, nInitItems, readPct);
                testSpinBtreeMapWorker<1>(nThreads, execMs, nInitItems, readPct);
            }
        }
    }
}
