// 打开 `autoBatch` 之后，`send(Message)` 不再一条一条直发，而是先按
// `AggregateKey(topic, mq, waitStoreMsgOK, tag)` 归并进 `MessageAccumulation`，攒够
// `holdMs` / `holdSize`（或被守卫线程唤醒）再合成**一个** `MessageBatch` 发出去，最后把
// broker 回的**批量** `SendResult` 拆回每条消息各自的 `SendResult` —— 调用方看到的东西
// 与直发一致。
//
//
// ## 与其它三端的差异
#ifndef ROCKETMQ_CLIENT_PRODUCE_ACCUMULATOR_H
#define ROCKETMQ_CLIENT_PRODUCE_ACCUMULATOR_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"

namespace rocketmq {

constexpr int64_t PRODUCE_ACCUMULATOR_DEFAULT_TOTAL_HOLD_SIZE = 32 * 1024 * 1024;
constexpr int64_t PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE = 32 * 1024;
constexpr int32_t PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS = 10;

class AccumulatorSender {
public:
    virtual ~AccumulatorSender() = default;

    virtual SendResult sendDirectBlocking(const MessageBatch& batch,
                                          const MessageQueue* mq) = 0;

    virtual void sendDirectAsync(const MessageBatch& batch, const MessageQueue* mq,
                                 std::shared_ptr<SendCallback> callback) = 0;
};

// ---------------------------------------------------------------- 归并键

struct AggregateKey {
    std::string topic;
    bool hasMq = false;
    MessageQueue mq;
    bool waitStoreMsgOk = true;
    bool hasTag = false;
    std::string tag;

    static AggregateKey ofMessage(const Message& msg);
    static AggregateKey ofMessageWithMq(const Message& msg, const MessageQueue& mq);

    bool operator==(const AggregateKey& o) const;
    bool operator!=(const AggregateKey& o) const { return !(*this == o); }
    bool operator<(const AggregateKey& o) const;

    std::string toString() const;
};

// ---------------------------------------------------------------- 批次

class ProduceAccumulator;  // 前向声明：批次只持它的裸指针（生命周期由累加器的表保证）

// 两类调用方：同步 `add()` 会在批次达到阈值前**阻塞**自己（等到本批真的发出去，自己的那条
// 消息才有 SendResult 可返回）；异步 `add(msg, callback)` 立刻返回，结果走回调。
//
// 非拷贝：`std::shared_ptr` 持有，`owner_` 是裸指针（累加器的表持着它，生命周期更长）。
class MessageAccumulation {
public:
    MessageAccumulation(AggregateKey key, ProduceAccumulator* owner);
    ~MessageAccumulation();

    MessageAccumulation(const MessageAccumulation&) = delete;
    MessageAccumulation& operator=(const MessageAccumulation&) = delete;

    const AggregateKey& aggregateKey() const { return key_; }

    int64_t add(const Message& msg);

    bool add(const Message& msg, std::shared_ptr<SendCallback> callback);

    bool readyToSend() const;
    bool closed() const { return closed_.load(); }
    void wakeup();
    int64_t count() const;
    int64_t messagesSize() const;
    std::set<std::string> keys() const;
    std::vector<Message> messages() const;
    std::vector<SendResult> sendResults() const;

    void forceSyncSend();
    void forceAsyncSend();

private:
    void markClosed();
    MessageBatch buildBatch() const;
    void splitSendResults(const SendResult& sendResult);
    void sendSync();
    void sendAsyncNow();
    void notifyCallbacksError(const std::exception_ptr& error);
    void releaseHold();
    void finish(const SendResult* result, const std::exception_ptr& error);

    AggregateKey key_;
    ProduceAccumulator* owner_;
    std::vector<Message> messages_;
    std::vector<std::shared_ptr<SendCallback>> callbacks_;
    std::set<std::string> keys_;
    std::vector<SendResult> sendResults_;
    mutable std::mutex stateMutex_;
    int64_t count_ = 0;
    int64_t messagesSize_ = 0;
    std::atomic<bool> closed_{false};
    int64_t createTime_ = 0;
    mutable std::mutex waitMutex_;
    std::condition_variable waitCv_;

    friend class ProduceAccumulator;
};

// ---------------------------------------------------------------- 累加器

class ProduceAccumulator {
public:
    ProduceAccumulator(std::string instanceName, AccumulatorSender* sender);
    ~ProduceAccumulator();

    ProduceAccumulator(const ProduceAccumulator&) = delete;
    ProduceAccumulator& operator=(const ProduceAccumulator&) = delete;

    const std::string& instanceName() const { return instanceName_; }

    void start();
    void shutdown();

    int32_t getBatchMaxDelayMs() const { return holdMs_.load(); }
    void setBatchMaxDelayMs(int32_t holdMs);
    int32_t getBatchMaxBytes() const { return static_cast<int32_t>(holdSize_.load()); }
    void setBatchMaxBytes(int32_t holdSize);
    int32_t getTotalBatchMaxBytes() const { return static_cast<int32_t>(holdSize_.load()); }
    void setTotalBatchMaxBytes(int32_t totalHoldSize);
    int64_t totalHoldSize() const { return totalHoldSize_.load(); }
    int64_t currentlyHoldSize() const;

    bool tryAddMessage(const Message& message);
    void releaseHold(int64_t size);

    SendResult send(const Message& msg);
    SendResult sendWithMq(const Message& msg, const MessageQueue& mq);
    void sendAsync(const Message& msg, std::shared_ptr<SendCallback> callback);
    void sendAsyncWithMq(const Message& msg, const MessageQueue& mq,
                         std::shared_ptr<SendCallback> callback);

    // ---------------- sender 生命周期 ----------------
    // 生产者析构 / shutdown 时调用：之后守卫线程不再碰那个 sender。
    void detachSender();
    // 重新绑定（`getOrCreateProduceAccumulator` 在 sender 已 detach 时用）。
    void attachSender(AccumulatorSender* sender);
    bool hasSender() const;

    // ---------------- 取证 / 测试接缝 ----------------
    size_t syncBatchCount() const;
    size_t asyncBatchCount() const;
    std::vector<std::shared_ptr<MessageAccumulation>> syncBatchesSnapshot() const;
    std::vector<std::shared_ptr<MessageAccumulation>> asyncBatchesSnapshot() const;
    // 往同步表里放一个（尚无人 add 的）批次 —— 验证守卫线程对 `messagesSize == 0` 的清理口径。
    std::shared_ptr<MessageAccumulation> putEmptySyncBatch(const AggregateKey& key);
    // 手工跑一轮守卫**主体**（守卫线程每个周期里 `sleep(max(1, holdMs/2))` 之前的那段）。
    void runGuardOnce(bool sync);

private:
    std::shared_ptr<MessageAccumulation> getOrCreateSyncBatch(const AggregateKey& key);
    std::shared_ptr<MessageAccumulation> getOrCreateAsyncBatch(const AggregateKey& key);
    void removeSyncBatch(const AggregateKey& key,
                         const std::shared_ptr<MessageAccumulation>& batch);
    void removeAsyncBatch(const AggregateKey& key,
                          const std::shared_ptr<MessageAccumulation>& batch);
    void guardOnce(bool sync);
    void guardLoop(bool sync, const std::string& name);
    int64_t holdMs() const { return holdMs_.load(); }
    int64_t holdSize() const { return holdSize_.load(); }

    std::string instanceName_;
    // `mutable`：`hasSender()` 是 const，要拿它。
    mutable std::mutex senderMutex_;
    AccumulatorSender* sender_ = nullptr;
    std::atomic<int32_t> holdMs_{PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS};
    std::atomic<int64_t> holdSize_{PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE};
    std::atomic<int64_t> totalHoldSize_{PRODUCE_ACCUMULATOR_DEFAULT_TOTAL_HOLD_SIZE};
    mutable std::mutex holdMutex_;
    int64_t currentlyHoldSize_ = 0;
    mutable std::mutex syncTableMutex_;
    std::map<AggregateKey, std::shared_ptr<MessageAccumulation>> syncBatches_;
    mutable std::mutex asyncTableMutex_;
    std::map<AggregateKey, std::shared_ptr<MessageAccumulation>> asyncBatches_;
    std::atomic<bool> stopped_{false};
    std::mutex guardMutex_;
    std::thread syncGuard_;
    std::thread asyncGuard_;

    friend class MessageAccumulation;
};

std::shared_ptr<ProduceAccumulator> getOrCreateProduceAccumulator(const std::string& clientId,
                                                                 AccumulatorSender* sender);

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_PRODUCE_ACCUMULATOR_H
