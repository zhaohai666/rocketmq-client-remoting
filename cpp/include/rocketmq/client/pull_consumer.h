// Difference from the push consumer: **the caller pulls and manages the offsets itself**.
// There is no background pull thread, no rebalance and no message listener; only:
//   fetchSubscribeMessageQueues / fetchMessageQueuesInBalance / pull / pullBlockIfNotFound /
//   fetchConsumeOffset / updateConsumeOffset / searchOffset / maxOffset / minOffset /
//   earliestMsgStoreTime / sendMessageBack / createTopic.
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
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/rpchook.h"

namespace rocketmq {

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
    void setHeartbeatEnabled(bool b) { heartbeatEnabled_ = b; }
    bool isHeartbeatEnabled() const { return heartbeatEnabled_; }
    void setHeartbeatBrokerIntervalMillis(int32_t millis) {
        heartbeatBrokerIntervalMillis_ = millis;
    }
    int32_t heartbeatBrokerIntervalMillis() const { return heartbeatBrokerIntervalMillis_; }
    void setMessageModel(const std::string& model) { messageModel_ = model; }
    void setMessageQueueListener(std::shared_ptr<MessageQueueListener> listener) {
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

    std::string consumerGroup_;
    std::string namespace_;
    std::string namespaceV2_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string unitName_;
    bool unitMode_ = false;
    bool enableStreamRequestType_ = true;
    int32_t pollNameServerIntervalMillis_ = 30000;
    std::string messageModel_ = MessageModel::CLUSTERING;

    int32_t brokerSuspendMaxTimeMillis_ = 20000;
    int32_t consumerPullTimeoutMillis_ = 10000;
    int32_t consumerTimeoutMillisWhenSuspend_ = 30000;

    std::vector<std::string> nameServerAddrs_;
    std::shared_ptr<RPCHook> rpcHook_;
    std::set<std::string> registerTopics_;
    std::map<std::string, std::shared_ptr<MessageQueueListener>> messageQueueListeners_;
    std::shared_ptr<MessageQueueListener> messageQueueListener_;
    std::shared_ptr<AllocateMessageQueueStrategy> allocateStrategy_ =
        std::make_shared<AllocateMessageQueueAveragely>();

    std::unique_ptr<MQClientInstance> mqClient_;
    std::map<MessageQueue, int64_t> pullFromWhichNode_;
    bool started_ = false;

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
