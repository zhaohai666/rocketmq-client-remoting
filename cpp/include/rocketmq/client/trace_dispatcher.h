// Responsibility: hooks drop TraceContext into an in-memory queue (`append`); a background thread flushes when
// either "batchNum contexts accumulated" or "more than 5s since the last send" triggers, then sends the encoded text
// to the trace topic (default RMQ_SYS_TRACE_TOPIC) via **a separate internal producer**.
//
//
// ⚠ Recursion guard: the internal producer's own enableTrace must be false, and SendMessageTraceHook skips
//   messages whose topic starts with the trace topic prefix — both safeguards are required, otherwise traces self-replicate infinitely.
#ifndef ROCKETMQ_CLIENT_TRACE_DISPATCHER_H
#define ROCKETMQ_CLIENT_TRACE_DISPATCHER_H

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/trace.h"

namespace rocketmq {

class DefaultMQProducer;        // Forward declaration only: avoids an include cycle with producer.h
class DefaultMQPushConsumer;     // same as above

class TraceDispatcher {
public:
    virtual ~TraceDispatcher() = default;
    virtual bool append(const std::shared_ptr<TraceContext>& ctx) = 0;
    virtual std::string getTraceTopicName() const = 0;
    // The host client's clientId (the EndTransaction trace's clientHost uses it); defaults to empty string.
    virtual std::string clientId() const { return std::string(); }
};

enum class TraceDispatcherType {
    PRODUCE = 0,
    CONSUME = 1,
};

// Async trace dispatcher: fully in-memory, a single background flush thread, an independent internal producer that does not trace itself.
class AsyncTraceDispatcher : public TraceDispatcher {
public:
    static constexpr int32_t QUEUE_CAPACITY = 2048;
    static constexpr int32_t MAX_BATCH_NUM = 20;
    static constexpr int32_t FLUSH_TRACE_INTERVAL = 5000;
    static constexpr int32_t WAIT_FOR_SHUTDOWN = 5000;
    static constexpr int32_t MAX_MSG_SIZE = 128000;

    // batchNum defaults to 10, max 20; traceTopicName defaults to RMQ_SYS_TRACE_TOPIC.
    AsyncTraceDispatcher(const std::string& group, TraceDispatcherType type,
                         int32_t batchNum = 10, const std::string& traceTopicName = std::string(),
                         std::shared_ptr<void> rpcHook = nullptr);
    virtual ~AsyncTraceDispatcher();

    AsyncTraceDispatcher(const AsyncTraceDispatcher&) = delete;
    AsyncTraceDispatcher& operator=(const AsyncTraceDispatcher&) = delete;

    // ---------------- Lifecycle ----------------
    void start(const std::string& nameSrvAddr,
               AccessChannel accessChannel = AccessChannel::LOCAL);
    void shutdown();

    bool append(const std::shared_ptr<TraceContext>& ctx);
    void flush();

    // ---------------- Accessors ----------------
    std::string getTraceTopicName() const { return traceTopicName_; }
    void setHostProducer(DefaultMQProducer* host) { hostProducer_ = host; }
    void setHostConsumer(DefaultMQPushConsumer* host) { hostConsumer_ = host; }
    void setHostClientId(const std::string& id) { clientId_ = id; }
    std::string clientId() const { return clientId_; }
    void setNamespaceV2(const std::string& ns) { namespaceV2_ = ns; }
    const std::string& namespaceV2() const { return namespaceV2_; }
    int64_t discardCount() const { return discardCount_.load(); }
    int32_t batchNum() const { return batchNum_; }
    bool isStarted() const { return started_.load(); }

protected:
    // Virtual-method seam: lets unit tests capture sends with a fake producer (no real network).
    // Default implementation sends to the traceTopic with the internal producer (body is the encoded trace text, keys are the lookup keys).
    virtual void sendTraceMessage(const std::string& traceTopic, const std::string& body,
                                  const std::string& keys);

    void sendTraceData(const std::vector<std::shared_ptr<TraceContext>>& contextList);
    void flushData(const std::vector<TraceTransferBean>& transBeanList,
                   const std::string& topic, const std::string& traceTopic);

private:
    std::string genGroupNameForTrace() const;
    void asyncRun();
    void flushTraceContext(bool forceFlush);
    void asyncSendTraceMessage(const std::vector<std::shared_ptr<TraceContext>>& contextList);

    int32_t traceInstanceId_ = 0;
    std::string group_;
    TraceDispatcherType type_;
    int32_t batchNum_;
    int32_t maxMsgSize_ = MAX_MSG_SIZE;
    std::string traceTopicName_;
    std::string namespaceV2_;
    std::shared_ptr<void> rpcHook_;
    std::unique_ptr<DefaultMQProducer> traceProducer_;

    std::deque<std::shared_ptr<TraceContext>> traceContextQueue_;
    mutable std::mutex queueMutex_;
    std::atomic<int64_t> discardCount_{0};
    std::atomic<bool> stopped_{false};
    std::atomic<bool> started_{false};

    DefaultMQProducer* hostProducer_ = nullptr;
    DefaultMQPushConsumer* hostConsumer_ = nullptr;
    std::string clientId_;

    AccessChannel accessChannel_ = AccessChannel::LOCAL;

    std::thread worker_;
    std::condition_variable cv_;
    std::mutex cvMutex_;
    std::atomic<int64_t> lastFlushTime_{0};

    // Process-level monotonically increasing counter (suffix for the internal producer's group name)
    static std::atomic<int32_t> instanceCounter_;
    static std::atomic<int32_t> groupCounter_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_TRACE_DISPATCHER_H
