#ifndef ROCKETMQ_CLIENT_LITE_PULL_CONSUMER_H
#define ROCKETMQ_CLIENT_LITE_PULL_CONSUMER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/allocate_strategy.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/rpchook.h"

namespace rocketmq {

class LiteMessageQueueListener {
public:
    virtual ~LiteMessageQueueListener() = default;
    // mqAll = every queue of the current subscription; mqDivided = queues newly assigned to this instance.
    virtual void messageQueueChanged(const std::vector<MessageQueue>& mqAll,
                                    const std::vector<MessageQueue>& mqDivided) = 0;
};

class TopicMessageQueueChangeListener {
public:
    virtual ~TopicMessageQueueChangeListener() = default;
    virtual void onChanged(const std::string& topic,
                           const std::vector<MessageQueue>& messageQueues) = 0;
};

class DefaultLitePullConsumer {
public:
    explicit DefaultLitePullConsumer(
        const std::string& consumerGroup = MixAll::DEFAULT_CONSUMER_GROUP);
    ~DefaultLitePullConsumer();

    DefaultLitePullConsumer(const DefaultLitePullConsumer&) = delete;
    DefaultLitePullConsumer& operator=(const DefaultLitePullConsumer&) = delete;

    // ---------------- Configuration ----------------
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }

    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    void setUnitMode(bool unitMode) { unitMode_ = unitMode; }
    bool isUnitMode() const { return unitMode_; }

    // When true every request carries the extension field `ReqT=0`, and the clientId gains a trailing `@STREAM` segment.
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }
    void setPollNameServerIntervalMillis(int32_t millis) { pollNameServerIntervalMillis_ = millis; }
    int32_t pollNameServerIntervalMillis() const { return pollNameServerIntervalMillis_; }
    // Broker VIP channel: when true the rewritten broker request family is sent to port - 2 of the
    // route address. Must be set before start(); forwarded to the MQClientInstance that owns the RPCs.
    void setVipChannelEnabled(bool enable) { vipChannelEnabled_ = enable; }
    bool isVipChannelEnabled() const { return vipChannelEnabled_; }
    void setMessageModel(const std::string& model) { messageModel_ = model; }
    void setNamespace(const std::string& ns) { namespace_ = ns; }
    void setNamespaceV2(const std::string& ns) { namespaceV2_ = ns; }
    const std::string& namespaceV2() const { return namespaceV2_; }
    void setRPCHook(std::shared_ptr<RPCHook> hook) { rpcHook_ = std::move(hook); }
    void setCredentials(const std::string& accessKey, const std::string& secretKey,
                        const std::string& securityToken = std::string()) {
        rpcHook_ = std::make_shared<AclClientRPCHook>(
            SessionCredentials(accessKey, secretKey, securityToken));
    }
    const std::shared_ptr<RPCHook>& rpcHook() const { return rpcHook_; }

    void setConsumeFromWhere(const std::string& where) { consumeFromWhere_ = where; }
    void setConsumeTimestamp(const std::string& ts) { consumeTimestamp_ = ts; }
    void setPullBatchSize(int32_t n) { pullBatchSize_ = std::max(1, n); }
    void setPollTimeoutMillis(int32_t ms) { pollTimeoutMillis_ = std::max(0, ms); }
    void setAutoCommit(bool b) { autoCommit_ = b; }
    void setAutoCommitIntervalMillis(int32_t ms) { autoCommitIntervalMillis_ = std::max(0, ms); }
    void setConsumerTimeoutMillisWhenSuspend(int32_t ms) {
        consumerTimeoutMillisWhenSuspend_ = ms;
    }
    void setBrokerSuspendMaxTimeMillis(int32_t ms) { brokerSuspendMaxTimeMillis_ = ms; }
    void setPullIntervalMillis(int32_t ms) { pullIntervalMillis_ = std::max(0, ms); }
    void setMessageQueueListener(std::shared_ptr<LiteMessageQueueListener> listener) {
        messageQueueListener_ = std::move(listener);
    }
    void setTopicMetadataCheckIntervalMillis(int32_t millis) {
        topicMetadataCheckIntervalMillis_ = std::max(1000, millis);
    }
    int32_t topicMetadataCheckIntervalMillis() const {
        return topicMetadataCheckIntervalMillis_;
    }
    void setAllocateMessageQueueStrategy(std::shared_ptr<AllocateMessageQueueStrategy> strategy);
    std::shared_ptr<AllocateMessageQueueStrategy> allocateMessageQueueStrategy() const;

    const std::string& consumerGroup() const { return consumerGroup_; }
    const std::string& clientId() const { return clientId_; }
    const std::string& consumeTimestamp() const { return consumeTimestamp_; }
    const std::string& namespaceOf() const { return namespace_; }
    bool isStarted() const { return started_; }
    bool isRunning() const { return running_; }

    // ---------------- Subscription / assignment ----------------
    // Subscribe mode: registers a topic subscription (tag expressions supported), rebalance assigns queues automatically.
    void subscribe(const std::string& topic, const std::string& subExpression = "*");
    void subscribe(const std::string& topic, const std::string& subExpression,
                   std::shared_ptr<LiteMessageQueueListener> messageQueueListener);
    void registerTopicMessageQueueChangeListener(
        const std::string& topic, std::shared_ptr<TopicMessageQueueChangeListener> listener);
    // One **pass** of the background comparison: walk the registered queue-change listeners, re-query
    // each topic's queue set, compare it for set equality with the last snapshot, and update the
    // snapshot and call back only on a real change. Public so tests need not wait for the background first check.
    void fetchTopicMessageQueuesAndCompare();
    void setSubExpressionForAssign(const std::string& topic, const std::string& subExpression);
    // Assign mode: queues are given explicitly, no rebalance involved.
    void assign(const std::vector<MessageQueue>& messageQueues);

    // ---------------- Lifecycle ----------------
    void start();
    void shutdown();

    // ---------------- Pull / poll ----------------
    // Batch-read from the local buffer, blocking at most timeoutMillis ms; returns what was taken (may be empty).
    std::vector<MessageExt> poll(int32_t timeoutMillis = -1);

    void seek(const MessageQueue& mq, int64_t offset);
    void seekToBegin(const MessageQueue& mq);
    void seekToEnd(const MessageQueue& mq);
    int64_t committed(const MessageQueue& mq);
    void commit();
    void commit(const std::map<MessageQueue, int64_t>& offsets, bool persist = true);
    void commit(const std::set<MessageQueue>& messageQueues, bool persist = true);
    int64_t offsetForTimestamp(const MessageQueue& mq, int64_t timestamp);

    int64_t pullCursorOf(const MessageQueue& mq);
    int64_t consumeCursorOf(const MessageQueue& mq);

    // ---------------- Queue query / control ----------------
    // All consumable queues of the topic (taken from the broker route, namespace already applied).
    std::vector<MessageQueue> fetchMessageQueues(const std::string& topic);
    // The queues currently assigned to this instance.
    std::vector<MessageQueue> assignment();
    // Pause / resume pulling from specific queues.
    void pause(const std::vector<MessageQueue>& messageQueues);
    void resume(const std::vector<MessageQueue>& messageQueues);

private:
    std::string withNamespace(const std::string& topic) const {
        return namespace_.empty() ? topic : NamespaceUtil::wrapNamespace(namespace_, topic);
    }

    // Collect every queue of the subscription (for the listener callback)
    std::vector<MessageQueue> mqAllOfSubscription() const;
    // std::set<MessageQueue> -> vector (for the listener callback)
    std::vector<MessageQueue> newSetAsVector(const std::set<MessageQueue>& s) const;

    void rebalance();
    void metadataLoop();
    bool pullOne(const MessageQueue& mq);
    std::string subscriptionFor(const std::string& topic) const;
    bool filterTags(const std::string& topic, std::vector<MessageExt>& msgs, const std::string& sub);
    void enqueue(const std::vector<MessageExt>& msgs);
    void maybeAutoCommit();
    // The half shared by commit()'s three entry points: write the in-memory offset table + persistAll on demand.
    void commitOffsets(const std::map<MessageQueue, int64_t>& targets,
                       const std::set<MessageQueue>& scope, bool persist);
    void persistOffset(const MessageQueue& mq);
    void persistOffsetTable(const std::set<MessageQueue>& mqs);
    // Advance the consumed cursor after poll() delivers, mirroring assignedMessageQueue.updateConsumeOffset.
    void advanceConsumeOffset(const std::vector<MessageExt>& msgs);
    int64_t resolveInitialOffset(const MessageQueue& mq);

    // Heartbeat (registers the tag subscriptions with the broker)
    HeartbeatData buildHeartbeat() const;
    int32_t sendHeartbeatToAllBroker();
    void heartbeatLoop();
    void pullServiceLoop();

    std::string consumerGroup_;
    std::shared_ptr<AllocateMessageQueueStrategy> allocateStrategy_;
    std::string namespace_;
    std::string namespaceV2_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string unitName_;
    bool unitMode_ = false;
    bool enableStreamRequestType_ = true;
    int32_t pollNameServerIntervalMillis_ = 30000;
    bool vipChannelEnabled_ = false;
    std::string messageModel_ = MessageModel::CLUSTERING;
    std::string consumeFromWhere_ = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
    std::string consumeTimestamp_;

    int32_t brokerSuspendMaxTimeMillis_ = 20000;
    int32_t consumerTimeoutMillisWhenSuspend_ = 30000;
    int32_t pollTimeoutMillis_ = 5000;
    int32_t pullBatchSize_ = 32;
    bool autoCommit_ = true;
    int32_t autoCommitIntervalMillis_ = 5000;
    int32_t pullIntervalMillis_ = 50;

    std::vector<std::string> nameServerAddrs_;
    std::shared_ptr<RPCHook> rpcHook_;

    // Subscription table in subscribe mode (topic -> sub_expression, namespace already applied)
    std::map<std::string, std::string> subscription_;
    // SubscriptionData per subscription (with tagsSet), used by the heartbeat sent to the broker to register tag filtering
    std::map<std::string, SubscriptionData> subscriptionData_;
    // Assign mode: topic -> tag filter expression (passed through to pull)
    std::map<std::string, std::string> assignSubExpr_;
    // Assign and subscribe modes are mutually exclusive; the pull thread reads this every round while the
    // setter runs on the caller thread, so it is atomic rather than a plain bool under stateMutex_ (many read sites, not worth locking).
    std::atomic<bool> assignMode_{false};
    // All mutable assignment and offset state (assigned_ / the three cursor tables / the auto-commit
    // deadline / the paused set) is guarded by this single stateMutex_: the pull thread advances the
    // pull cursor, rebalance rewrites the assignment and poll/commit caller threads read at the same
    // time, so unlocked access is a data race (undefined behavior). Rules: **no network I/O** under it, never take bufferMutex_.
    mutable std::mutex stateMutex_;
    std::set<MessageQueue> assigned_;

    std::shared_ptr<LiteMessageQueueListener> messageQueueListener_;

    // Per-subscription listeners (the three-argument subscribe overload) and queue-change listeners, all
    // keyed by topic (namespace already applied); their registration and iteration are under topicListenerMutex_.
    mutable std::mutex topicListenerMutex_;
    std::map<std::string, std::shared_ptr<LiteMessageQueueListener>> topicListeners_;
    std::map<std::string, std::shared_ptr<TopicMessageQueueChangeListener>> topicChangeListeners_;
    std::map<std::string, std::vector<MessageQueue>> messageQueuesForTopic_;
    int32_t topicMetadataCheckIntervalMillis_ = 30000;
    std::thread metadataThread_;

    std::unique_ptr<MQClientInstance> mqClient_;
    std::atomic<bool> started_{false};
    std::atomic<bool> running_{false};

    std::mutex bufferMutex_;
    std::condition_variable bufferCv_;
    std::deque<MessageExt> localBuffer_;

    std::map<MessageQueue, int64_t> nextOffset_;
    std::map<MessageQueue, int64_t> consumeOffset_;
    std::map<MessageQueue, int64_t> offsetTable_;
    std::map<MessageQueue, int64_t> seekOffset_;
    std::set<MessageQueue> paused_;
    int64_t nextAutoCommitDeadline_ = -1;

    std::chrono::steady_clock::time_point lastRebalanceTs_;
    std::thread pullThread_;
    std::thread heartbeatThread_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_LITE_PULL_CONSUMER_H
