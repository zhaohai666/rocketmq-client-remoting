// 异步轨迹分发器（对应 org.apache.rocketmq.client.trace.AsyncTraceDispatcher）。
//
// 职责：钩子把 TraceContext 丢进内存队列（``append``），后台线程按
// 「攒够 batchNum 条 或 距上次发送超过 5s」两个条件触发刷写，再把编码后的文本
// 用**独立的内部生产者**发到轨迹 topic（默认 RMQ_SYS_TRACE_TOPIC）。
//
// 与 Java 的对应关系：
//   * 内存队列           ← ArrayBlockingQueue<TraceContext>(2048)
//   * _flushTraceContext ← flushTraceContext（含 force_flush 语义）
//   * _sendTraceData     ← sendTraceData（按 topic@traceTopic 分组）
//   * _flushData         ← flushData（按 maxMessageSize 128K 切块）
//   * _sendTraceDataByMq ← sendTraceDataByMQ（用内部生产者发送）
//   * 内部生产者组名      ← _INNER_TRACE_PRODUCER-<group>-<PRODUCE|CONSUME>-<N>
//
// ⚠ 防递归：内部生产者自身的 enableTrace 必须为 false，且 SendMessageTraceHook 会跳过
//   topic 以轨迹 topic 开头的消息 —— 两道保险都要有，否则轨迹会自我复制到无限。
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

class DefaultMQProducer;        // 仅前向声明：避免与 producer.h 形成包含环
class DefaultMQPushConsumer;     // 同上

// 轨迹分发器抽象接口（对应 Java org.apache.rocketmq.client.trace.TraceDispatcher）。
// 钩子只依赖这个接口，便于单测用假实现（FakeDispatcher）替换真实分发器。
class TraceDispatcher {
public:
    virtual ~TraceDispatcher() = default;
    virtual bool append(const std::shared_ptr<TraceContext>& ctx) = 0;
    virtual std::string getTraceTopicName() const = 0;
    // 宿主客户端的 clientId（EndTransaction 轨迹的 clientHost 用它），默认空串。
    virtual std::string clientId() const { return std::string(); }
};

// 对应 org.apache.rocketmq.client.trace.TraceDispatcher.Type
enum class TraceDispatcherType {
    PRODUCE = 0,
    CONSUME = 1,
};

// 异步轨迹分发器。C++ 移植版：全内存、单后台刷写线程、独立的内部生产者不追踪自身。
class AsyncTraceDispatcher : public TraceDispatcher {
public:
    static constexpr int32_t QUEUE_CAPACITY = 2048;
    static constexpr int32_t MAX_BATCH_NUM = 20;
    static constexpr int32_t FLUSH_TRACE_INTERVAL = 5000;
    static constexpr int32_t WAIT_FOR_SHUTDOWN = 5000;
    static constexpr int32_t MAX_MSG_SIZE = 128000;

    // batchNum 默认 10、最大 20；traceTopicName 缺省 RMQ_SYS_TRACE_TOPIC。
    AsyncTraceDispatcher(const std::string& group, TraceDispatcherType type,
                         int32_t batchNum = 10, const std::string& traceTopicName = std::string(),
                         std::shared_ptr<void> rpcHook = nullptr);
    virtual ~AsyncTraceDispatcher();

    AsyncTraceDispatcher(const AsyncTraceDispatcher&) = delete;
    AsyncTraceDispatcher& operator=(const AsyncTraceDispatcher&) = delete;

    // ---------------- 生命周期 ----------------
    void start(const std::string& nameSrvAddr,
               AccessChannel accessChannel = AccessChannel::LOCAL);
    void shutdown();

    // ---------------- 入队 / 刷写 ----------------
    // 把一个 TraceContext 入队。队列满时计数并丢弃（与 Java 一致，不阻塞业务）。
    bool append(const std::shared_ptr<TraceContext>& ctx);
    // 强制刷空队列（Java flush()）。
    void flush();

    // ---------------- 访问 ----------------
    std::string getTraceTopicName() const { return traceTopicName_; }
    void setHostProducer(DefaultMQProducer* host) { hostProducer_ = host; }
    void setHostConsumer(DefaultMQPushConsumer* host) { hostConsumer_ = host; }
    void setHostClientId(const std::string& id) { clientId_ = id; }
    std::string clientId() const { return clientId_; }
    int64_t discardCount() const { return discardCount_.load(); }
    int32_t batchNum() const { return batchNum_; }
    bool isStarted() const { return started_.load(); }

protected:
    // 虚方法 seam：便于单测用假生产者捕获发送（不打真实网络）。
    // 默认实现用内部生产者发到 traceTopic（body 即编码后的轨迹文本，keys 为反查键）。
    virtual void sendTraceMessage(const std::string& traceTopic, const std::string& body,
                                  const std::string& keys);

    // 按 (业务 topic, 轨迹 topic) 分组后逐组发送（对应 Java sendTraceData）。
    void sendTraceData(const std::vector<std::shared_ptr<TraceContext>>& contextList);
    // 把一批 TraceTransferBean 按 maxMsgSize 切块发送（对应 Java flushData）。
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

    // 进程级递增计数器（用于内部生产者组名后缀）
    static std::atomic<int32_t> instanceCounter_;
    static std::atomic<int32_t> groupCounter_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_TRACE_DISPATCHER_H
