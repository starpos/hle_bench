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

#if 0
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
#endif

/**
 * Spinlock with HLE support.
 * You can use test_and_test_and_set with useTTAS flag.
 * using GCC extension (gcc-4.8 or more is required).
 */
template <bool useHLE, bool useTTAS>
class SpinlockT
{
private:
    char &lock_;
public:
    explicit SpinlockT(char &lock) : lock_(lock) {
        int flags = __ATOMIC_ACQUIRE | (useHLE ? __ATOMIC_HLE_ACQUIRE : 0);
        if (useTTAS) {
            while (lock_ || __atomic_exchange_n(&lock_, 1, flags))
                _mm_pause();
        } else {
            while (__atomic_exchange_n(&lock_, 1, flags))
                _mm_pause();
        }
    }
    ~SpinlockT() noexcept {
        int flags = __ATOMIC_RELEASE | (useHLE ? __ATOMIC_HLE_RELEASE : 0);
        __atomic_clear(&lock_, flags);
    }
};

using Spinlock = SpinlockT<false, false>;
using SpinlockHle = SpinlockT<true, false>;
using Ttaslock = SpinlockT<false, true>;
using TtaslockHle = SpinlockT<true, true>;

} //namespace cybozu
