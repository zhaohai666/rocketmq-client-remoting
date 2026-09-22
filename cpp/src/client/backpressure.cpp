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
            // 队首换人 = 后面某个人的「轮不到我」变成了「该我了」：release 那次通知可能只
            // 够队首用，之后才排上队的人不会再被谁喊一声。少这一嗓子他就睡到自己的超时
            // （异步发送里是白等满 sendMsgTimeout 再回调 TooMuchRequest），实测真会撞上。
            cv_.notify_all();
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            // 只有本线程会摘自己的等待者（拿到许可时自己摘，超时时自己摘），所以一定找得到。
            for (auto it = queue_.begin(); it != queue_.end(); ++it) {
                if (*it == waiter) {
                    queue_.erase(it);
                    break;
                }
            }
            cv_.notify_all();  // 挡路的人走了，理由同上
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
    // 一次 notify 只叫醒一个，被叫醒的人可能因为「不是队头」而继续等 —— 那就白等一轮。
    // 等待者本来就被队头挡住，全部叫醒让它们各自复核更快，Java 的 AQS 也是逐个放行的。
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
        // 差额全记在空闲许可上 ⇒ 在途份数 (total - free) 保持不变
        free_ += total - total_;
        total_ = total;
    }
    cv_.notify_all();
}

}  // namespace rocketmq
