// 推模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPushConsumer
// 与 Python client/consumer.py 的 DefaultMQPushConsumer）。
//
// 实现方式与 Python 参考实现一致：**单线程拉取循环 + 本地消费**
//   1. start() 启动一个消费线程；
//   2. 每轮对「已分配队列」逐个调用 PULL_MESSAGE（带订阅信息的长轮询）；
//   3. 按 PullStatus 推进 offset：FOUND -> offset + 成功消费条数；
//      NO_NEW_MSG / OFFSET_ILLEGAL -> nextBeginOffset；
//   4. 把消息交给 MessageListener（并发/顺序两种）。
//
// 关键工程点（真机验证得出，勿删注释）：
//   - 单线程顺序长轮询下，排在满载队列前面的**空闲队列**会用 suspend 长轮询
//     阻塞整轮，把满载队列饿死（顺序消息尤其明显，因为同 key 全落一个队列）。
//     因此 pull_suspend_timeout_millis 与 pull_timeout_millis 必须可配且设短。
//   - broker 会把客户端下发的 suspend 时间钳制到自身 brokerSuspendMaxTimeMillis，
//     故空闲队列仍会周期性客户端超时 —— 这是**良性**的，按 debug 处理不记 ERROR。
#ifndef ROCKETMQ_CLIENT_CONSUMER_H
#define ROCKETMQ_CLIENT_CONSUMER_H

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
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
    void consumeLoop();
    void pullAndConsumeOnce();
    std::vector<MessageQueue> assignedQueues();
    int64_t resolveInitialOffset(const MessageQueue& mq, const SubscriptionData& sub);
    // 返回可推进 offset 的消息条数
    int32_t dispatchMessages(const MessageQueue& mq, const std::vector<MessageExt>& msgs);
    void maybeSendHeartbeat();
    static std::string offsetKey(const MessageQueue& mq);

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
    bool heartbeatEnabled_ = true;
    int32_t heartbeatIntervalMillis_ = 30000;

    std::vector<std::string> nameServerAddrs_;
    mutable std::mutex lock_;
    std::map<std::string, SubscriptionData> subscriptionData_;
    std::shared_ptr<MessageListener> messageListener_;
    std::map<std::string, int64_t> offsetTable_;

    std::unique_ptr<MQClientInstance> mqClient_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stop_{false};
    std::vector<std::thread> consumeThreads_;
    std::condition_variable cv_;
    std::mutex waitMutex_;
    std::atomic<int64_t> consumedCount_{0};
    std::atomic<int64_t> heartbeatCount_{0};
    std::atomic<int64_t> lastHeartbeatMs_{0};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_CONSUMER_H
