// 公平性只保证「队列头部才可能拿到」，不保证拿到顺序严格等于到达顺序：超时的等待者会
// 从队列里摘掉自己，摘除瞬间顺序由剩余等待者的到达顺序决定 —— 与 Semaphore(true) 一致。
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

    // 拿 permits 份，最多等 timeoutMillis 毫秒。拿不到就**原样返回 false**，不会留下半份许可。
    // 只排在队头时才发放（公平），所以 permits 大于总容量时会一直等到超时。
    // permits <= 0 视为「不占额度」，只要排到队头就立即通过（Semaphore 同语义）。
    bool tryAcquire(int64_t permits, int32_t timeoutMillis);

    void release(int64_t permits);

    // 当前空闲许可。可能为负（见 setTotalPermits），所以签名是有符号数而不是「剩余非负」。
    int64_t availablePermits() const;
    int64_t totalPermits() const;

    int32_t waitingCount() const;

    // 原地把总量改成 total：在途份数不动，所以空闲许可 = total - 在途份数，
    // 调小之后可以为负。改完叫醒等待者（扩容要叫醒，缩容不需要但无害）。
    void setTotalPermits(int64_t total);

 private:
    struct Waiter {
        int64_t permits;
        explicit Waiter(int64_t p) : permits(p) {}
    };

    mutable std::mutex m_;
    std::condition_variable cv_;
    int64_t total_;
    int64_t free_;
    std::deque<std::shared_ptr<Waiter>> queue_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_BACKPRESSURE_H
