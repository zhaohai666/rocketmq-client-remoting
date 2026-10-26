// Difference from the push consumer: **the caller pulls and manages the offsets itself**.
// There is no background pull thread and no offset store driving the consumer; only:
//   fetchSubscribeMessageQueues / fetchMessageQueuesInBalance / pull / pullBlockIfNotFound /
//   fetchConsumeOffset / updateConsumeOffset / searchOffset / maxOffset / minOffset /
//   earliestMsgStoreTime / sendMessageBack / createTopic.
// fetchMessageQueuesInBalance recomputes the topic's share, so it is also the point where a registered
// queue listener learns that the share moved.
#ifndef ROCKETMQ_CLIENT_PULL_CONSUMER_H
#define ROCKETMQ_CLIENT_PULL_CONSUMER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/allocate_strategy.h"
#include "rocketmq/client/consume_executor.h"
#include "rocketmq/client/message_queue_listener.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/rpchook.h"

namespace rocketmq {

class DefaultMQPullConsumer {
public:
    explicit DefaultMQPullConsumer(
        const std::string& consumerGroup = MixAll::DEFAULT_CONSUMER_GROUP);
    ~DefaultMQPullConsumer();

    DefaultMQPullConsumer(const DefaultMQPullConsumer&) = delete;
    DefaultMQPullConsumer& operator=(const DefaultMQPullConsumer&) = delete;

    // ---------------- configuration ----------------
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }

    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    void setUnitMode(bool unitMode) { unitMode_ = unitMode; }
    bool isUnitMode() const { return unitMode_; }

    // When true, every request carries the extension field `ReqT=0` and clientId gets an extra `@STREAM` suffix segment.
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }
    void setPollNameServerIntervalMillis(int32_t millis) { pollNameServerIntervalMillis_ = millis; }
    int32_t pollNameServerIntervalMillis() const { return pollNameServerIntervalMillis_; }
    // Broker VIP channel: when true the rewritten broker request family is sent to port - 2 of the
    // route address. Must be set before start(); forwarded to the MQClientInstance that owns the RPCs.
    void setVipChannelEnabled(bool enable) { vipChannelEnabled_ = enable; }
    bool isVipChannelEnabled() const { return vipChannelEnabled_; }
    // decodeReadBody (default true): false = pull / POP bodies are walked but not materialized.
    // Must be set before start(); forwarded to the MQClientInstance that owns the RPCs.
    void setDecodeReadBody(bool readBody) { decodeReadBody_ = readBody; }
    bool isDecodeReadBody() const { return decodeReadBody_; }
    // mqClientApiTimeout (default 3000ms): default timeout of heartbeat / route / unregister /
    // consumer-list RPCs. Forwarded to the instance in start().
    void setMqClientApiTimeout(int32_t millis) { mqClientApiTimeout_ = millis; }
    int32_t mqClientApiTimeout() const { return mqClientApiTimeout_; }
    // Concurrent heartbeat (default off) and its pool size (default: available processors).
    void setEnableConcurrentHeartbeat(bool enable) { enableConcurrentHeartbeat_ = enable; }
    bool isEnableConcurrentHeartbeat() const { return enableConcurrentHeartbeat_; }
    void setConcurrentHeartbeatThreadPoolSize(int32_t size) {
        concurrentHeartbeatThreadPoolSize_ = size;
    }
    int32_t concurrentHeartbeatThreadPoolSize() const {
        return concurrentHeartbeatThreadPoolSize_;
    }
    void setHeartbeatEnabled(bool b) { heartbeatEnabled_ = b; }
    bool isHeartbeatEnabled() const { return heartbeatEnabled_; }
    void setHeartbeatBrokerIntervalMillis(int32_t millis) {
        heartbeatBrokerIntervalMillis_ = millis;
    }
    int32_t heartbeatBrokerIntervalMillis() const { return heartbeatBrokerIntervalMillis_; }
    void setMessageModel(const std::string& model) { messageModel_ = model; }
    const std::string& messageModel() const { return messageModel_; }
    // Queue listeners are told only when fetchMessageQueuesInBalance hands out a view whose queue set
    // moved for that topic: a re-ordering is not a change, a same-count swap to another broker is.
    // The topic-keyed listener wins for its own topic; the single listener is the sink for the rest.
    void setMessageQueueListener(std::shared_ptr<MessageQueueListener> listener) {
        std::lock_guard<std::mutex> lk(assignmentNotifyMutex_);
        messageQueueListener_ = std::move(listener);
    }
    void setAllocateMessageQueueStrategy(std::shared_ptr<AllocateMessageQueueStrategy> strategy) {
        allocateStrategy_ = std::move(strategy);
    }
    const std::shared_ptr<AllocateMessageQueueStrategy>& allocateMessageQueueStrategy() const {
        return allocateStrategy_;
    }
    void registerMessageQueueListener(const std::string& topic,
                                      std::shared_ptr<MessageQueueListener> listener);

    void setNamespace(const std::string& ns) { namespace_ = ns; }
    void setNamespaceV2(const std::string& ns) { namespaceV2_ = ns; }
    const std::string& namespaceV2() const { return namespaceV2_; }

    // Test-only: hand the consumer a topic's balanced view the way fetchMessageQueuesInBalance does
    // after it recomputes one, and report whether the registered listeners were told.
    bool publishAssignmentForTest(const std::string& topic, const std::vector<MessageQueue>& all,
                                  const std::vector<MessageQueue>& divided) {
        return notifyAssignmentChanged(topic, all, divided);
    }
    static bool assignmentChangedForTest(const std::vector<MessageQueue>& before,
                                        const std::vector<MessageQueue>& after) {
        return assignmentChanged(before, after);
    }

    void setRPCHook(std::shared_ptr<RPCHook> hook) { rpcHook_ = std::move(hook); }
    void setCredentials(const std::string& accessKey, const std::string& secretKey,
                        const std::string& securityToken = std::string()) {
        rpcHook_ = std::make_shared<AclClientRPCHook>(
            SessionCredentials(accessKey, secretKey, securityToken));
    }
    const std::shared_ptr<RPCHook>& rpcHook() const { return rpcHook_; }

    const std::string& consumerGroup() const { return consumerGroup_; }
    const std::string& clientId() const { return clientId_; }
    const std::string& namespaceOf() const { return namespace_; }
    const std::set<std::string>& registerTopics() const { return registerTopics_; }
    bool isStarted() const { return started_; }
    // Number of successful heartbeat rounds (for real-environment verification).
    int32_t heartbeatCount() const { return heartbeatCount_.load(); }

    // ---------------- lifecycle ----------------
    void start();
    void shutdown();

    int32_t sendHeartbeatToAllBroker();

    std::vector<MessageQueue> fetchSubscribeMessageQueues(const std::string& topic);

    std::vector<MessageQueue> fetchMessageQueuesInBalance(const std::string& topic);

    PullResult pull(const MessageQueue& mq, const std::string& subExpression = "*",
                    int64_t offset = 0, int32_t maxNums = 32, int32_t timeoutMillis = -1);
    PullResult pullBlockIfNotFound(const MessageQueue& mq, const std::string& subExpression,
                                   int64_t offset, int32_t maxNums);
    // Same pulls driven by a subscription selector: the request carries the subscription's own
    // expression type and its raw expression. A tag selector reproduces the string form exactly (the
    // expression is parsed into tagsSet / codeSet and subVersion stays 0); a selector such as SQL92
    // sends the expression verbatim with the version the subscription was built with, and tagsSet /
    // codeSet stay empty so no client-side re-filter runs on it.
    PullResult pull(const MessageQueue& mq, const MessageSelector& selector, int64_t offset = 0,
                    int32_t maxNums = 32, int32_t timeoutMillis = -1);
    PullResult pullBlockIfNotFound(const MessageQueue& mq, const MessageSelector& selector,
                                   int64_t offset, int32_t maxNums);

    // Async pull (DefaultMQPullConsumer.pull(mq, subExpression, offset, maxNums, PullCallback, timeout)):
    // returns at once; a worker thread runs the very same synchronous pull kernel and the callback pool
    // delivers onSuccess/onException **exactly once**. Same two-pool shape as the producer's async send
    // (PullAsyncExecutor for the request work + PullCallbackExecutor for the user callback).
    // Not started / null callback throw synchronously; after shutdown() the in-flight requests are drained,
    // and a late pullAsync throws instead of dropping the callback silently.
    void pullAsync(const MessageQueue& mq, const std::string& subExpression, int64_t offset,
                   int32_t maxNums, std::shared_ptr<PullCallback> callback,
                   int32_t timeoutMillis = -1);
    // Async pull of a selector subscription: the worker runs the selector-shaped pull kernel above,
    // so the delivery contract (exactly once) and the wire shape are identical to the synchronous call.
    void pullAsync(const MessageQueue& mq, const MessageSelector& selector, int64_t offset,
                   int32_t maxNums, std::shared_ptr<PullCallback> callback,
                   int32_t timeoutMillis = -1);

    void setAsyncPullQueueCapacity(int32_t n) { asyncPullQueueCapacity_ = n; }
    int32_t getAsyncPullQueueCapacity() const { return asyncPullQueueCapacity_; }

    int64_t recalculatePullFromWhichNode(const MessageQueue& mq) const;

    void updatePullFromWhichNode(const MessageQueue& mq, const PullResult& result);

    // ---------------- offset management ----------------
    // false means the consumer group has no offset on this queue yet (the broker answers QUERY_NOT_FOUND).
    bool fetchConsumeOffset(const MessageQueue& mq, int64_t& outOffset);
    void updateConsumeOffset(const MessageQueue& mq, int64_t offset);
    void persistConsumeOffset(const MessageQueue& mq, int64_t offset) {
        updateConsumeOffset(mq, offset);
    }
    int64_t searchOffset(const MessageQueue& mq, int64_t timestamp);
    int64_t maxOffset(const MessageQueue& mq);
    int64_t minOffset(const MessageQueue& mq);
    int64_t earliestMsgStoreTime(const MessageQueue& mq);

    void sendMessageBack(const MessageExt& msg, int32_t delayLevel);

    void createTopic(const std::string& key, const std::string& newTopic, int32_t queueNum = 4);

private:
    // The heartbeat trio: refresh the route (only with addresses is there anyone to send to), assemble ConsumerData, run the resident loop.
    void refreshRouteForHeartbeat();
    HeartbeatData buildHeartbeat() const;
    void startHeartbeatLoop();
    void heartbeatLoop();

    // Tell the registered listeners about a topic's balanced view, but only when its own queue set
    // moved since the view last handed out. Returns whether they were told.
    bool notifyAssignmentChanged(const std::string& topic, const std::vector<MessageQueue>& all,
                                 const std::vector<MessageQueue>& divided);
    static bool assignmentChanged(const std::vector<MessageQueue>& before,
                                  const std::vector<MessageQueue>& after);

    // ---------------- pullAsync plumbing (producer-shaped) ----------------
    void createAsyncExecutors();
    // Runs fn on the callback pool; falls back to inline once the pool is gone (shutdown race),
    // so the exactly-once delivery never depends on the pool still being alive.
    void executeOnCallbackThread(std::function<void()> fn);

    std::string consumerGroup_;
    std::string namespace_;
    std::string namespaceV2_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string unitName_;
    bool unitMode_ = false;
    bool enableStreamRequestType_ = true;
    int32_t pollNameServerIntervalMillis_ = 30000;
    bool vipChannelEnabled_ = false;
    bool decodeReadBody_ = true;
    int32_t mqClientApiTimeout_ = MQClientInstance::kMqClientApiTimeoutMillis;
    bool enableConcurrentHeartbeat_ = false;
    int32_t concurrentHeartbeatThreadPoolSize_ =
        MQClientInstance::defaultConcurrentHeartbeatThreadPoolSize();
    std::string messageModel_ = MessageModel::CLUSTERING;

    int32_t brokerSuspendMaxTimeMillis_ = 20000;
    int32_t consumerPullTimeoutMillis_ = 10000;
    int32_t consumerTimeoutMillisWhenSuspend_ = 30000;

    std::vector<std::string> nameServerAddrs_;
    std::shared_ptr<RPCHook> rpcHook_;
    std::set<std::string> registerTopics_;
    // The queue listeners and the last balanced view per topic are read while a caller recomputes an
    // assignment, so they share one guard.
    mutable std::mutex assignmentNotifyMutex_;
    std::map<std::string, std::shared_ptr<MessageQueueListener>> messageQueueListeners_;
    std::shared_ptr<MessageQueueListener> messageQueueListener_;
    std::map<std::string, std::vector<MessageQueue>> lastBalancedQueues_;
    std::shared_ptr<AllocateMessageQueueStrategy> allocateStrategy_ =
        std::make_shared<AllocateMessageQueueAveragely>();

    std::unique_ptr<MQClientInstance> mqClient_;
    // pullAsync worker threads and synchronous callers share the table, so every read/write is guarded
    // (recalculatePullFromWhichNode / updatePullFromWhichNode / fetchMessageQueuesInBalance).
    mutable std::mutex pullFromWhichNodeMutex_;
    std::map<MessageQueue, int64_t> pullFromWhichNode_;
    bool started_ = false;

    // ---------------- pullAsync pools ----------------
    // shared_ptr + swap-on-shutdown, the producer's convention: an in-flight submission keeps its own
    // reference while shutdown() drains the queue, so nothing dangles and nothing is dropped silently.
    std::mutex asyncLock_;
    int32_t asyncPullQueueCapacity_ = 50000;
    std::shared_ptr<ConsumeExecutor> asyncPullExecutor_;
    std::shared_ptr<ConsumeExecutor> callbackExecutor_;

    bool heartbeatEnabled_ = true;
    int32_t heartbeatBrokerIntervalMillis_ = 30000;
    std::atomic<int32_t> heartbeatCount_{0};
    std::atomic<bool> heartbeatStop_{false};
    std::thread heartbeatThread_;
    std::mutex heartbeatLock_;
    std::condition_variable heartbeatCv_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_PULL_CONSUMER_H
