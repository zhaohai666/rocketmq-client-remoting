// 拉模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPullConsumer
// 与 Python client/consumer.py 的 DefaultMQPullConsumer）。
//
// 与 push 消费者的区别：**由调用方自己拉、自己管位点**。
// 没有后台拉取线程、没有 rebalance、没有消费监听器；只有：
//   fetchSubscribeMessageQueues / pull / pullBlockIfNotFound /
//   fetchConsumeOffset / updateConsumeOffset / searchOffset / maxOffset / minOffset /
//   earliestMsgStoreTime / sendMessageBack / createTopic。
//
// 这正是 pull 模式的语义（Java 亦如此）：把队列分配与位点推进交给使用方，
// 便于做批量离线消费、按时间回溯、精确控制提交时机等 push 模式做不到的事。
//
// ⚠ 与 Java 的一处有意差异：Java DefaultMQPullConsumer 内嵌 MQPullConsumerImpl，
// 起了一个定时 rebalance 并在队列变更时回调 MessageQueueListener；本实现（同 Python
// 参考实现）**不做 rebalance**——队列由 fetchSubscribeMessageQueues 显式取，监听器
// 只作为 API 形状保留。需要自动分配队列请用 push 消费者。
#ifndef ROCKETMQ_CLIENT_PULL_CONSUMER_H
#define ROCKETMQ_CLIENT_PULL_CONSUMER_H

#include <cstdint>
#include <memory>
#include <set>
#include <string>
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

// 队列变更监听器（对应 Java MessageQueueListener / Python MessageQueueListener）。
class MessageQueueListener {
public:
    virtual ~MessageQueueListener() = default;
    virtual void messageQueueChanged(const std::string& topic,
                                     const std::vector<MessageQueue>& mqAll,
                                     const std::vector<MessageQueue>& mqDivided) = 0;
};

class DefaultMQPullConsumer {
public:
    explicit DefaultMQPullConsumer(
        const std::string& consumerGroup = MixAll::DEFAULT_CONSUMER_GROUP);
    ~DefaultMQPullConsumer();

    DefaultMQPullConsumer(const DefaultMQPullConsumer&) = delete;
    DefaultMQPullConsumer& operator=(const DefaultMQPullConsumer&) = delete;

    // ---------------- 配置 ----------------
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // 对应 Java `ClientConfig` 的三个同名开关。⚠ 必须在 start() 之前设置：
    // unitName / @STREAM 决定 clientId 的形状，stream 决定请求钩子链（`ReqT` 要进 ACL 签名）。
    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    // Java 在消息过滤上下文（`DefaultMQPushConsumerImpl:640`）、心跳里的
    // `ConsumerData.unitMode`（`MQClientInstance:1039`）和回投请求头（`:927/948`）三处读它。
    void setUnitMode(bool unitMode) { unitMode_ = unitMode; }
    bool isUnitMode() const { return unitMode_; }

    // true 时每个请求带扩展字段 `ReqT=0`，且 clientId 末尾多一段 `@STREAM`。
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }
    void setMessageModel(const std::string& model) { messageModel_ = model; }
    void setMessageQueueListener(std::shared_ptr<MessageQueueListener> listener) {
        messageQueueListener_ = std::move(listener);
    }
    // 队列分配策略，对应 Java DefaultMQPullConsumer.allocateMessageQueueStrategy
    // （字段初值 new AllocateMessageQueueAveragely():89，getter/setter:196-202）。
    // setter 不校验，置 nullptr 由 start() 按 Java checkConfig(:803) 拒绝。
    // 本端口拉模式不做 rebalance（见文件头），所以它只是配置面 + 启动校验。
    void setAllocateMessageQueueStrategy(std::shared_ptr<AllocateMessageQueueStrategy> strategy) {
        allocateStrategy_ = std::move(strategy);
    }
    // 对应 Java DefaultMQPullConsumer.getAllocateMessageQueueStrategy(:196)。
    const std::shared_ptr<AllocateMessageQueueStrategy>& allocateMessageQueueStrategy() const {
        return allocateStrategy_;
    }
    // 对应 Java registerMessageQueueListener(topic, listener)：登记 topic + 该 topic 的监听器。
    void registerMessageQueueListener(const std::string& topic,
                                      std::shared_ptr<MessageQueueListener> listener);

    // 命名空间（对应 Java DefaultMQPullConsumer.setNamespace）：非空时把 topic / group
    // 套上 "ns%" 前缀再与 broker 交互。
    void setNamespace(const std::string& ns) { namespace_ = ns; }

    // ---------------- ACL 鉴权（对应 Java DefaultMQPullConsumer(rpcHook)）----------------
    // 必须在 start() 之前调用：钩子在 start() 里绑定到 MQClientInstance。
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

    // ---------------- 生命周期 ----------------
    void start();
    void shutdown();

    // ---------------- 队列 ----------------
    // 该 topic 的全部可消费队列（按 broker 路由取，Java fetchSubscribeMessageQueues）。
    std::vector<MessageQueue> fetchSubscribeMessageQueues(const std::string& topic);

    // ---------------- 拉取 ----------------
    // 一次短轮询拉取（Java pull）。timeoutMillis <= 0 表示用默认 consumerPullTimeoutMillis。
    PullResult pull(const MessageQueue& mq, const std::string& subExpression = "*",
                    int64_t offset = 0, int32_t maxNums = 32, int32_t timeoutMillis = -1);
    // 长轮询拉取（Java pullBlockIfNotFound）：broker 端挂起到有消息或超时。
    PullResult pullBlockIfNotFound(const MessageQueue& mq, const std::string& subExpression,
                                   int64_t offset, int32_t maxNums);

    // ---------------- 位点管理 ----------------
    // 返回 false 表示该消费组在该队列上尚无位点（broker 回 QUERY_NOT_FOUND）。
    bool fetchConsumeOffset(const MessageQueue& mq, int64_t& outOffset);
    void updateConsumeOffset(const MessageQueue& mq, int64_t offset);
    void persistConsumeOffset(const MessageQueue& mq, int64_t offset) {
        updateConsumeOffset(mq, offset);
    }
    int64_t searchOffset(const MessageQueue& mq, int64_t timestamp);
    int64_t maxOffset(const MessageQueue& mq);
    int64_t minOffset(const MessageQueue& mq);
    int64_t earliestMsgStoreTime(const MessageQueue& mq);

    // ---------------- 回投 / 建 topic ----------------
    // 消息回投（Java sendMessageBack）：delayLevel=0 时 broker 会改写成 3+reconsumeTimes。
    void sendMessageBack(const MessageExt& msg, int32_t delayLevel);

    void createTopic(const std::string& key, const std::string& newTopic, int32_t queueNum = 4);

private:
    std::string consumerGroup_;
    std::string namespace_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string unitName_;
    bool unitMode_ = false;
    // Java 的 pull / lite 消费者在**每个构造函数**里置 true（DefaultMQPullConsumer:113/126、
    // DefaultLitePullConsumer:213/228），生产者与推送消费者保持 false。
    bool enableStreamRequestType_ = true;
    std::string messageModel_ = MessageModel::CLUSTERING;

    int32_t brokerSuspendMaxTimeMillis_ = 20000;
    int32_t consumerPullTimeoutMillis_ = 10000;
    int32_t consumerTimeoutMillisWhenSuspend_ = 30000;

    std::vector<std::string> nameServerAddrs_;
    std::shared_ptr<RPCHook> rpcHook_;
    std::set<std::string> registerTopics_;
    std::map<std::string, std::shared_ptr<MessageQueueListener>> messageQueueListeners_;
    std::shared_ptr<MessageQueueListener> messageQueueListener_;
    // 对应 Java DefaultMQPullConsumer.allocateMessageQueueStrategy 的默认值
    std::shared_ptr<AllocateMessageQueueStrategy> allocateStrategy_ =
        std::make_shared<AllocateMessageQueueAveragely>();

    std::unique_ptr<MQClientInstance> mqClient_;
    bool started_ = false;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_PULL_CONSUMER_H
