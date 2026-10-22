// Fairness only guarantees "the queue head is the only place a grant can come from", not that the granted order equals
// arrival order: a timed-out waiter removes itself, and at that moment the order is set by the arrival order of the rest — as Semaphore(true).
#ifndef ROCKETMQ_CLIENT_BACKPRESSURE_H
#define ROCKETMQ_CLIENT_BACKPRESSURE_H

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>

namespace rocketmq {

constexpr int64_t kMinAsyncSendNum = 10;
constexpr int64_t kMinAsyncSendSize = 1024 * 1024;

class FairSemaphore {
 public:
    explicit FairSemaphore(int64_t permits);

    FairSemaphore(const FairSemaphore&) = delete;
    FairSemaphore& operator=(const FairSemaphore&) = delete;

    // Acquire permits shares, waiting at most timeoutMillis milliseconds. On failure **return false unchanged**, leaving
    // no half permit. Permits are granted only at the queue head (fairness), so permits > total capacity waits until timeout.
    // permits <= 0 means "consumes no quota": it passes at once on reaching the head (same semantics as Semaphore).
    bool tryAcquire(int64_t permits, int32_t timeoutMillis);

    void release(int64_t permits);

    // Currently free permits. May be negative (see setTotalPermits), hence the signed return type rather than a non-negative remaining count.
    int64_t availablePermits() const;
    int64_t totalPermits() const;

    int32_t waitingCount() const;

    // Change the total to total in place: the in-flight share stays untouched, so free permits = total - in-flight share,
    // which can go negative after a shrink. Waiters are woken afterwards (waking is required when growing, harmless when shrinking).
    void setTotalPermits(int64_t total);

 private:
    struct Waiter {
        int64_t permits;
        explicit Waiter(int64_t p) : permits(p) {}
    };

    // Abandon the ticket; the caller must hold m_.
    void leaveQueue(const std::shared_ptr<Waiter>& waiter);

    // Grants the offline test that has to hold m_ across a wake-up (to keep a woken waiter from resuming). It cannot
    // call release() for that -- std::mutex is not reentrant -- so this seam exposes the lock and the release body.
    friend struct FairSemaphoreTestAccess;

    mutable std::mutex m_;
    std::condition_variable cv_;
    int64_t total_;
    int64_t free_;
    std::deque<std::shared_ptr<Waiter>> queue_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_BACKPRESSURE_H
