// With `autoBatch` turned on, `send(Message)` no longer dispatches one message at a time: it first
// merges into `MessageAccumulation` per `AggregateKey(topic, mq, waitStoreMsgOK, tag)` and, once
// `holdMs` / `holdSize` are reached (or the guard thread wakes it), composes **one** `MessageBatch` and
// sends it out; the **batch** `SendResult` from the broker is then split back into a `SendResult` per
// message, so what the caller sees is identical to a direct send.
//
//
// ## Semantics of the batching path
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

// ---------------------------------------------------------------- Aggregate key

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

// ---------------------------------------------------------------- Batch

class ProduceAccumulator;  // Forward declaration: a batch only holds a raw pointer (its lifetime is guaranteed by the accumulator's table)

// Two kinds of callers: synchronous `add()` **blocks** itself until the batch reaches its threshold
// (only a really sent batch yields the SendResult its message returns); asynchronous `add(msg, callback)` returns at once and reports through the callback.
//
// Not copyable: held by a `std::shared_ptr`, `owner_` is a raw pointer (the accumulator's table holds it and outlives it).
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

// ---------------------------------------------------------------- Accumulator

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

    // ---------------- sender lifecycle ----------------
    // Called on producer destruction / shutdown: guard threads never touch that sender afterwards.
    void detachSender();
    // Rebind (used by `getOrCreateProduceAccumulator` when the sender had been detached).
    void attachSender(AccumulatorSender* sender);
    bool hasSender() const;

    // ---------------- Forensics / test seams ----------------
    size_t syncBatchCount() const;
    size_t asyncBatchCount() const;
    std::vector<std::shared_ptr<MessageAccumulation>> syncBatchesSnapshot() const;
    std::vector<std::shared_ptr<MessageAccumulation>> asyncBatchesSnapshot() const;
    // Put a batch nobody has added to into the sync table; checks the guard thread's cleanup rule for `messagesSize == 0`.
    std::shared_ptr<MessageAccumulation> putEmptySyncBatch(const AggregateKey& key);
    // Run one round of the guard **body** by hand (the part of each guard cycle before `sleep(max(1, holdMs/2))`).
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
    // `mutable`: `hasSender()` is const and has to read it.
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
