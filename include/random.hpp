/**
 * @file
 * @brief Mmaped file wrapper.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <random>
#include <limits>
#include <cassert>

#ifndef RANDOM_HPP
#define RANDOM_HPP

namespace cybozu {
namespace util {

template <typename IntType>
class Random
{
private:
    std::random_device rd_;
    std::mt19937 gen_;
    std::uniform_int_distribution<IntType> dist_;

public:
    Random(IntType minValue = std::numeric_limits<IntType>::min(),
           IntType maxValue = std::numeric_limits<IntType>::max())
        : rd_()
        , gen_(rd_())
        , dist_(minValue, maxValue) {
    }

    IntType operator()() {
        return dist_(gen_);
    }
};

class XorShift128
{
private:
    uint32_t x_, y_, z_, w_;

public:
    explicit XorShift128(uint32_t seed)
        : x_(123456789)
        , y_(362436069)
        , z_(521288629)
        , w_(88675123) {
        x_ ^= seed;
        y_ ^= (seed << 8)  | (seed >> (32 - 8));
        z_ ^= (seed << 16) | (seed >> (32 - 16));
        w_ ^= (seed << 24) | (seed >> (32 - 24));
    }

    uint32_t operator()() {
        return get();
    }

    uint32_t get() {
        const uint32_t t = x_ ^ (x_ << 11);
        x_ = y_;
        y_ = z_;
        z_ = w_;
        w_ = (w_ ^ (w_ >> 19)) ^ (t ^ (t >> 8));
        return  w_;
    }

    uint32_t get(uint32_t max) {
        return get() % max;
    }

    uint32_t get(uint32_t min, uint32_t max) {
        assert(min < max);
        return get() % (max - min) + min;
    }
};

}} //namespace cybozu::util

#endif /* RANDOM_HPP */
