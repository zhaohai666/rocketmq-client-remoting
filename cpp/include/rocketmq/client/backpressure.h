// 异步发送背压：Java DefaultMQProducerImpl 的那两个公平计数信号量。
//
// Java 在把任务投给 AsyncSenderExecutor **之前**、也就是在**调用方线程**上过一道闸
// （DefaultMQProducerImpl.executeAsyncMessageSend:635-682）：按「在途条数」和「在途字节数」
// 两个维度各拿一份许可，拿不到就回调 RemotingTooMuchRequestException。为什么要有这道闸：
// 异步发送本身不阻塞调用方，一个不节制的生产者可以把任意多的消息压进发送池 —— 池子有界
// （队满 reject），但**在途请求**没有上界，慢 broker 会把整条链（含消息 body）留在内存里。
//
// 与 Java 的两处实现差别（语义等价）：
//   1. Java 用 java.util.concurrent.Semaphore(permits, true)。它的公平模式靠 AQS 的
//      等待队列保证 FIFO，但**改容量只能换新对象**（DefaultMQProducer:1383-1391 就是
//      new Semaphore(num - acquired)），换对象的瞬间已在等待的线程与新对象无关。
//      这里用条件变量自己实现，容量可以**原地**改：在途份数（total - free）保持不变，
//      等待队列也不清空，扩容时把卡住的人叫醒。
//   2. Java 的 setBackPressureForAsyncSendNum 外面套了一层 ReadWriteCASLock（自旋、写优先，
//      而且**跨** tryAcquire 持有）。这里不需要：resize 只是一个在锁内的加减，读者走
//      availablePermits 也是同一把锁。
//
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

// Java DefaultMQProducer:1386 / :1402 的两个地板值：配置不大于地板值时按地板值建信号量
// （Java 的分支写成 `if (cfg > 10) ... else ...`，**恰好等于**地板值也走 else）。
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

    // 归还 permits 份（<= 0 时什么都不做，对应 Semaphore.release 不接受负数的情形）。
    // 不需要事先 tryAcquire 过：可以归还到超过 total，Java 同样允许。
    void release(int64_t permits);

    // 当前空闲许可。可能为负（见 setTotalPermits），所以签名是有符号数而不是「剩余非负」。
    int64_t availablePermits() const;
    int64_t totalPermits() const;

    // 正在等许可的线程数（Java Semaphore#getQueueLength）。观测/测试用 —— 公平性只有靠
    // 「谁先排上队」才说得清，没有这个口径就只能靠 sleep 猜顺序。
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
