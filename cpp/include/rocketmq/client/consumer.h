// 推模式消费者（对齐 org.apache.rocketmq.client.consumer.DefaultMQPushConsumer）。
//
// 架构（对齐 Java PushConsumer 的三层模型）：
//   1. 拉取层：每个队列一个拉取线程（对应 Java PullMessageService 的并发长轮询——
//      broker 为每个队列挂起长轮询请求、消息到达立即返回），拉到的消息进
//      pending_ 缓冲（对应 ProcessQueue），拉取游标推进到 nextBeginOffset；
//   2. 分发层：单分发线程从缓冲按 consumeMessageBatchMaxSize 取批次交给
//      MessageListener；RECONSUME_LATER/异常批次逐条回投 %RETRY%topic
//      （延迟梯度 3+reconsumeTimes，超 maxReconsumeTimes 由 broker 转 %DLQ%）；
//   3. 位点层：_consume_offsets 记录"已消费位点"，每 5s 用 UPDATE_CONSUMER_OFFSET
//      提交 broker（Java persistAllConsumerOffset），启动先 QUERY_CONSUMER_OFFSET。
//
// 关键工程点（真机验证得出，勿删注释）：
//   - 拉取必须按队列并行：单线程顺序长轮询下，空闲队列的 suspend 会阻塞
//     其余队列投递（曾导致"第一批消息能收到、后续全迟到"）。
//   - broker 会把客户端下发的 suspend 时间钳制到自身 brokerSuspendMaxTimeMillis，
//     故空闲队列仍会周期性客户端超时 —— 这是**良性**的，按 debug 处理不记 ERROR。
#ifndef ROCKETMQ_CLIENT_CONSUMER_H
#define ROCKETMQ_CLIENT_CONSUMER_H

#include <algorithm>
#include <atomic>
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

#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

namespace rocketmq {

// 选择器（对应 Java MessageSelector / Python MessageSelector）
struct MessageSelector {
    std::string type = ExpressionType::TAG;
    std::string expression = "*";

    static MessageSelector byTag(const std::string& tag) {
        return MessageSelector{ExpressionType::TAG, tag};
    }
    static MessageSelector bySql(const std::string& sql) {
        return MessageSelector{ExpressionType::SQL92, sql};
    }
};

class DefaultMQPushConsumer {
public:
    explicit DefaultMQPushConsumer(
        const std::string& consumerGroup = MixAll::DEFAULT_CONSUMER_GROUP);
    ~DefaultMQPushConsumer();

    DefaultMQPushConsumer(const DefaultMQPushConsumer&) = delete;
    DefaultMQPushConsumer& operator=(const DefaultMQPushConsumer&) = delete;

    // ---------------- 配置 ----------------
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }
    void setMessageModel(const std::string& model) { messageModel_ = model; }
    void setConsumeFromWhere(const std::string& where) { consumeFromWhere_ = where; }
    void setConsumeThreadNums(int32_t n);
    void setMessageListener(std::shared_ptr<MessageListener> listener);
    void setPullBatchSize(int32_t n) { pullBatchSize_ = n; }
    void setPullBatchSizeInBytes(int32_t n) { pullBatchSizeInBytes_ = n; }
    void setConsumeMessageBatchMaxSize(int32_t n) { consumeMessageBatchMaxSize_ = std::max(1, n); }
    void setPullTimeoutMillis(int32_t t) { pullTimeoutMillis_ = t; }
    void setPullSuspendTimeoutMillis(int32_t t) { pullSuspendTimeoutMillis_ = t; }
    void setSuspendCurrentQueueTimeMillis(int32_t t) { suspendCurrentQueueTimeMillis_ = t; }
    void setMaxReconsumeTimes(int32_t n) { maxReconsumeTimes_ = n; }
    void setPullIntervalMillis(int32_t t) { pullIntervalMillis_ = t; }
    // 每队列"已拉未消费"阈值，超过则暂停该队列拉取（Java pullThresholdForQueue，默认 1000）
    void setPullThresholdForQueue(int32_t n) { pullThresholdForQueue_ = n; }
    // 是否在消费循环里周期性发 HEART_BEAT（默认开启；失败仅告警不影响消费）
    void setHeartbeatEnabled(bool b) { heartbeatEnabled_ = b; }
    void setHeartbeatIntervalMillis(int32_t t) { heartbeatIntervalMillis_ = t; }

    const std::string& consumerGroup() const { return consumerGroup_; }
    const std::string& clientId() const { return clientId_; }
    const std::string& messageModel() const { return messageModel_; }
    bool isStarted() const { return started_.load(); }
    int32_t pullTimeoutMillis() const { return pullTimeoutMillis_; }
    int32_t pullSuspendTimeoutMillis() const { return pullSuspendTimeoutMillis_; }
    // 已成功消费的消息总数（用于测试/监控）
    int64_t consumedCount() const { return consumedCount_.load(); }
    // broker 心跳成功次数（用于验证心跳能力）
    int64_t heartbeatCount() const { return heartbeatCount_.load(); }
    // 流控触发次数（用于验证流控能力）
    int64_t flowControlTriggered() const { return flowControlTriggered_.load(); }
    // 当前分给本实例的队列 key 列表（真机验证"同组两实例不重不漏"用）。
    // key 格式与 offsetKey 一致：topic + brokerName + queueId。
    std::vector<std::string> assignedQueueKeys() const;
    // 查消费组在某 topic 上的全部 clientId（对应 Java findConsumerIdList），用于验证多实例注册。
    std::vector<std::string> consumerIdListOfGroup(const std::string& topic) const;

    // ---------------- 订阅 ----------------
    void subscribe(const std::string& topic, const std::string& subExpression = "*");
    void subscribe(const std::string& topic, const MessageSelector& selector);
    void unsubscribe(const std::string& topic);
    std::vector<std::string> subscribedTopics() const;

    // ---------------- 生命周期 ----------------
    void start();
    void shutdown();

    MQClientInstance& client();

    // ---------------- 管理 ----------------
    std::vector<MessageQueue> fetchSubscribeMessageQueues(const std::string& topic);
    // 消息重投（对应 Java sendMessageBack）：返回 false 表示被 broker 拒收
    bool sendMessageBack(const MessageExt& msg, int32_t delayLevel,
                         const std::string& brokerName = std::string());
    // 向所有已知 broker 发一次心跳（对应 Java sendHeartbeatToAllBrokerWithLock）
    int32_t sendHeartbeatToAllBroker();

private:
    // 拉取：每个队列一个线程（对齐 Java PullMessageService 的并发长轮询语义：
    // broker 为每个队列挂起长轮询、消息到达立即返回；若单线程顺序轮询，
    // 一个空队列的 suspend 会阻塞其余队列的投递）。
    void rebalancePullThreads();
    void rebalanceLoop();
    void queuePullLoop(const MessageQueue& mq);
    // 分发：单线程从各队列缓冲取批次交给监听器
    void dispatchLoop();
    // 消费一个批次，处理回投/挂起；返回消费位点是否前进
    bool consumeBatch(const std::string& key, const MessageQueue& mq,
                      const std::vector<MessageExt>& batch);
    // 失败批次逐条回投（Java processConsumeResult → sendMessageBack）
    bool sendBackBatch(const std::vector<MessageExt>& batch,
                       const ConsumeConcurrentlyContext& ctx);
    void advanceConsumeOffset(const std::string& key, const std::vector<MessageExt>& batch);
    // 位点持久化：每 5s 把"已消费位点"提交 broker（Java persistAllConsumerOffset）
    void offsetPersistLoop();
    void persistOffsetsOnce();
    // 广播模式本地位点文件（Java LocalFileOffsetStore）
    std::string localOffsetPath() const;
    void saveLocalOffsets();
    std::map<std::string, int64_t> loadLocalOffsets() const;
    // 顺序消费 broker 队列锁（Java ConsumeMessageOrderlyService.lockMQ，每 20s）
    void lockLoop();
    bool isOrderly() const;
    void maybeSendHeartbeat();

    std::vector<MessageQueue> assignedQueues();
    int64_t resolveInitialOffset(const MessageQueue& mq, const SubscriptionData& sub);
    static std::string offsetKey(const MessageQueue& mq);

    // ---- 真实 rebalance（对齐 Java RebalanceImpl.rebalanceByTopic）----
    // 计算本实例应持有的队列集并写入 assignedQueues_，再同步拉取线程；
    // 新分配的队列**立刻**解析初始位点写入 offsetTable_。
    void doRebalance();
    // 当前分配里「topic 的全部队列」（对应 Java RebalanceImpl.topicSubscribeInfoTable）。
    std::vector<MessageQueue> allQueuesOfTopic(const std::string& topic);
    // AllocateMessageQueueAveragely（对齐 Java 同名字段逐条实现）。
    static std::vector<MessageQueue> allocateMessageQueueAveragely(
        const std::string& consumerGroup, const std::string& currentCid,
        const std::vector<MessageQueue>& mqAll, const std::vector<std::string>& cidAll);
    // 本拉取线程是否仍持有该队列（rebalance 撤走或换了拉取线程后即失效）。
    bool ownsQueue(const std::string& key) const;
    // 队列被撤走时的收尾（对应 Java removeUnnecessaryMessageQueue）：持久化已消费位点、
    // 丢弃在途缓冲、顺序消费集群模式解锁。revoked 为 (队列, 已消费位点) 列表。
    void onQueuesRevoked(const std::vector<std::pair<MessageQueue, int64_t>>& revoked);
    // broker 通知消费组实例变化 → 立即重算（对齐 Java rebalanceImmediately）。
    void onConsumerIdsChanged(const RemotingCommand& cmd);
    // 分发前把重投消息的 topic 还原成业务原始 topic（对应 Java resetRetryAndNamespace）。
    void resetRetryTopicAndNamespace(std::vector<MessageExt>& msgs);

    std::string consumerGroup_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string messageModel_ = MessageModel::CLUSTERING;
    std::string consumeFromWhere_ = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;

    int32_t consumeThreadNums_ = 1;
    int32_t pullBatchSize_ = 32;
    int32_t pullBatchSizeInBytes_ = 256 * 1024;
    int32_t consumeMessageBatchMaxSize_ = 1;
    int32_t pullTimeoutMillis_ = 30000;
    int32_t pullSuspendTimeoutMillis_ = 15000;
    int32_t suspendCurrentQueueTimeMillis_ = 1000;
    int32_t maxReconsumeTimes_ = -1;
    int32_t pullIntervalMillis_ = 0;
    int32_t pullThresholdForQueue_ = 1000;
    bool heartbeatEnabled_ = true;
    int32_t heartbeatIntervalMillis_ = 30000;

    std::vector<std::string> nameServerAddrs_;
    mutable std::mutex lock_;
    std::map<std::string, SubscriptionData> subscriptionData_;
    std::shared_ptr<MessageListener> messageListener_;
    // 拉取游标（nextBeginOffset）
    std::map<std::string, int64_t> offsetTable_;
    // 已消费位点（周期持久化的对象；Java ProcessQueue.removeMessage 后的 commitOffset）
    std::map<std::string, int64_t> consumeOffsetTable_;
    std::map<std::string, MessageQueue> mqMap_;
    // 已拉未消费缓冲（Java ProcessQueue 的简化版）
    std::map<std::string, std::deque<MessageExt>> pending_;
    // 顺序消费：broker LOCK_BATCH_MQ 确认锁定成功的队列 key 集
    std::set<std::string> lockOk_;

    std::unique_ptr<MQClientInstance> mqClient_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stop_{false};
    std::map<std::string, std::thread> pullThreads_;
    // 被撤销队列对应的旧拉取线程（已脱离 pullThreads_，等待其自然退出后回收）
    std::vector<std::thread> retiredThreads_;
    std::thread dispatchThread_;
    std::thread persistThread_;
    std::thread lockThread_;
    std::thread rebalanceThread_;
    // 真实 rebalance 计算出的本实例队列集（对应 Java ProcessQueueTable 的键集）。
    // 取代旧实现里「订阅 topic 的全部队列」，避免同组多实例重复消费。
    std::vector<MessageQueue> assignedQueues_;
    std::condition_variable cv_;
    std::mutex waitMutex_;
    // 即时重算信号（broker 发 NOTIFY_CONSUMER_IDS_CHANGED 时置位）
    std::atomic<bool> rebalanceNow_{false};
    std::mutex rebalanceMutex_;
    std::condition_variable rebalanceCv_;
    int64_t startMillis_ = 0;
    std::atomic<int64_t> consumedCount_{0};
    std::atomic<int64_t> heartbeatCount_{0};
    std::atomic<int64_t> lastHeartbeatMs_{0};
    std::atomic<int64_t> flowControlTriggered_{0};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_CONSUMER_H
