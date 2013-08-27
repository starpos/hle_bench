#pragma once
/**
 * @file
 * @description spinlock for Intel TSX HLE.
 * @author HOSHINO Takashi
 *
 * HLE spinlock implementation refers to GCC manual.
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <atomic>
#include <immintrin.h> /* for _mm_pause() */

namespace cybozu {

static inline void pause(void)
{
    asm volatile("pause" ::: "memory");
}

/**
 * A reference implementation of spinlock.
 */
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

/**
 * Spinlock with HLE support
 * using GCC extension (gcc-4.8 or more is required).
 */
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

} //namespace cybozu
