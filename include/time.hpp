/**
 * @file
 * @brief clock wrappers.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef CYBOZU_TIME_HPP
#define CYBOZU_TIME_HPP

#include <chrono>
#include <deque>

namespace cybozu {
namespace time {

template <typename Clock = std::chrono::high_resolution_clock>
class TimeStack
{
private:
    std::deque<std::chrono::time_point<Clock> > q_;
public:
    void pushNow() {
        q_.push_front(Clock::now());
    }

    void pushTime(std::chrono::time_point<Clock> tp) {
        q_.push_front(Clock::now());
    }

    template <typename Duration>
    unsigned long elapsed() const {
        if (q_.size() < 2) {
            return std::chrono::duration_cast<Duration>(
                std::chrono::duration<long>::zero()).count();
        } else {
            return std::chrono::duration_cast<Duration>(q_[0] - q_[1]).count();
        }
    }

    unsigned long elapsedInSec() const {
        return elapsed<std::chrono::seconds>();
    }

    unsigned long elapsedInMs() const {
        return elapsed<std::chrono::milliseconds>();
    }

    unsigned long elapsedInUs() const {
        return elapsed<std::chrono::microseconds>();
    }

    unsigned long elapsedInNs() const {
        return elapsed<std::chrono::nanoseconds>();
    }

    void clear() {
        q_.clear();
    }
};

}} // namespace cybozu::time

#endif /* CYBOZU_TIME_HPP */
