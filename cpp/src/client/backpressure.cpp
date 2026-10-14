#include "rocketmq/client/backpressure.h"

#include <algorithm>
#include <chrono>

namespace rocketmq {

FairSemaphore::FairSemaphore(int64_t permits) : total_(permits), free_(permits) {}

bool FairSemaphore::tryAcquire(int64_t permits, int32_t timeoutMillis) {
    auto waiter = std::make_shared<Waiter>(permits);
    const int32_t bounded = std::max(timeoutMillis, 0);
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(bounded);
    std::unique_lock<std::mutex> lk(m_);
    queue_.push_back(waiter);
    for (;;) {
        if (!queue_.empty() && queue_.front() == waiter && free_ >= permits) {
            queue_.pop_front();
            free_ -= permits;
            // A new queue head means someone behind went from "not my turn" to "my turn": the notify from release
            // may only wake the head, and whoever queued up later is never called again. Without this nudge they sleep
            // until their own timeout (in async send that burns the full sendMsgTimeout before a TooMuchRequest callback); observed in practice.
            cv_.notify_all();
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            // Only this thread removes its own waiter (on acquiring the permit, and on timeout), so the lookup always succeeds.
            for (auto it = queue_.begin(); it != queue_.end(); ++it) {
                if (*it == waiter) {
                    queue_.erase(it);
                    break;
                }
            }
            cv_.notify_all();  // the blocker is gone, same reasoning as above
            return false;
        }
        cv_.wait_until(lk, deadline);
    }
}

void FairSemaphore::release(int64_t permits) {
    if (permits <= 0) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(m_);
        free_ += permits;
    }
    // Notifying one by one would make a woken waiter keep waiting because it is "not the head", wasting a round;
    // waiters are blocked by the head anyway, so waking them all to recheck is faster.
    cv_.notify_all();
}

int64_t FairSemaphore::availablePermits() const {
    std::lock_guard<std::mutex> lk(m_);
    return free_;
}

int64_t FairSemaphore::totalPermits() const {
    std::lock_guard<std::mutex> lk(m_);
    return total_;
}

int32_t FairSemaphore::waitingCount() const {
    std::lock_guard<std::mutex> lk(m_);
    return static_cast<int32_t>(queue_.size());
}

void FairSemaphore::setTotalPermits(int64_t total) {
    {
        std::lock_guard<std::mutex> lk(m_);
        // The whole delta is charged to free permits => the in-flight count (total - free) stays unchanged
        free_ += total - total_;
        total_ = total;
    }
    cv_.notify_all();
}

}  // namespace rocketmq
