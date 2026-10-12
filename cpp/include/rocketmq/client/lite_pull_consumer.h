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
    // mqAll=当前订阅的全部队列；mqDivided=本次新分配给本实例的队列。
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

    // ---------------- 配置 ----------------
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }

    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    void setUnitMode(bool unitMode) { unitMode_ = unitMode; }
    bool isUnitMode() const { return unitMode_; }

    // true 时每个请求带扩展字段 `ReqT=0`，且 clientId 末尾多一段 `@STREAM`。
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }
    void setPollNameServerIntervalMillis(int32_t millis) { pollNameServerIntervalMillis_ = millis; }
    int32_t pollNameServerIntervalMillis() const { return pollNameServerIntervalMillis_; }
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

    // ---------------- 订阅 / 分配 ----------------
    // subscribe 模式：登记 topic 订阅（支持 tag 表达式），自动 rebalance 分配队列。
    void subscribe(const std::string& topic, const std::string& subExpression = "*");
    void subscribe(const std::string& topic, const std::string& subExpression,
                   std::shared_ptr<LiteMessageQueueListener> messageQueueListener);
    void registerTopicMessageQueueChangeListener(
        const std::string& topic, std::shared_ptr<TopicMessageQueueChangeListener> listener);
    // 后台比对的**一趟**：遍历已注册的队列变更监听器，逐 topic 现查队列集合，与上次快照
    // 按集合相等比较，只有真的变了才更新快照并回调。公开是为了让单测能自己驱动这一趟，
    // 不必等后台线程的首查延迟。
    void fetchTopicMessageQueuesAndCompare();
    void setSubExpressionForAssign(const std::string& topic, const std::string& subExpression);
    // assign 模式：显式指定队列，不走 rebalance。
    void assign(const std::vector<MessageQueue>& messageQueues);

    // ---------------- 生命周期 ----------------
    void start();
    void shutdown();

    // ---------------- 拉取 / poll ----------------
    // 从本地缓冲批量取消息，阻塞至多 timeoutMillis 毫秒。返回取到的消息（可能为空）。
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

    // ---------------- 队列查询 / 控制 ----------------
    // 该 topic 的全部可消费队列（按 broker 路由取，已套命名空间）。
    std::vector<MessageQueue> fetchMessageQueues(const std::string& topic);
    // 当前分配给本实例的队列。
    std::vector<MessageQueue> assignment();
    // 暂停 / 恢复某些队列的拉取。
    void pause(const std::vector<MessageQueue>& messageQueues);
    void resume(const std::vector<MessageQueue>& messageQueues);

private:
    std::string withNamespace(const std::string& topic) const {
        return namespace_.empty() ? topic : NamespaceUtil::wrapNamespace(namespace_, topic);
    }

    // 收集订阅的全部队列（listener 回调用）
    std::vector<MessageQueue> mqAllOfSubscription() const;
    // std::set<MessageQueue> -> vector（listener 回调用）
    std::vector<MessageQueue> newSetAsVector(const std::set<MessageQueue>& s) const;

    void rebalance();
    void metadataLoop();
    bool pullOne(const MessageQueue& mq);
    std::string subscriptionFor(const std::string& topic) const;
    bool filterTags(const std::string& topic, std::vector<MessageExt>& msgs, const std::string& sub);
    void enqueue(const std::vector<MessageExt>& msgs);
    void maybeAutoCommit();
    // commit() 的三个入口共用的那半段：写内存位点表 + 按需 persistAll。
    void commitOffsets(const std::map<MessageQueue, int64_t>& targets,
                       const std::set<MessageQueue>& scope, bool persist);
    void persistOffset(const MessageQueue& mq);
    void persistOffsetTable(const std::set<MessageQueue>& mqs);
    // poll() 交付之后推进「已消费游标」，对位 assignedMessageQueue.updateConsumeOffset。
    void advanceConsumeOffset(const std::vector<MessageExt>& msgs);
    int64_t resolveInitialOffset(const MessageQueue& mq);

    // 心跳（把 tag 订阅注册给 broker）
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

    // subscribe 模式的订阅表（topic -> sub_expression，已套命名空间）
    std::map<std::string, std::string> subscription_;
    // 订阅对应的 SubscriptionData（带 tagsSet），用于发给 broker 的心跳做 tag 过滤注册
    std::map<std::string, SubscriptionData> subscriptionData_;
    // assign 模式：topic -> tag 过滤表达式（透传给 pull）
    std::map<std::string, std::string> assignSubExpr_;
    // assign 模式与 subscribe 模式互斥；拉取线程每轮都读它，赋值方是调用方线程，
    // 所以是原子量而不是 stateMutex_ 下的普通 bool（读点很多，不值得为它加锁）。
    std::atomic<bool> assignMode_{false};
    // 分配与位点的全部可变状态（assigned_ / 三张游标表 / 自动提交截止时刻 / 暂停集合）
    // 由这一把 stateMutex_ 保护：拉取线程在推进拉取游标、rebalance 在改分配，
    // 试用 poll/commit 的调用方线程同时在读，无锁就是数据竞争（C++ 里是 UB）。
    // 两条铁律：拿着这把锁**不做网络 I/O**，也**不去拿 bufferMutex_**。
    mutable std::mutex stateMutex_;
    std::set<MessageQueue> assigned_;

    std::shared_ptr<LiteMessageQueueListener> messageQueueListener_;

    // per-subscription 监听器（subscribe 的三参重载）与队列变化监听器，都按
    // topic（已套命名空间）登记；各自的注册/遍历由 topicListenerMutex_ 保护。
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
