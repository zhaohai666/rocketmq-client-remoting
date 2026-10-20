// Responsibilities: name server address management, topic route fetching and caching, broker
// address resolution, message sending (SEND_MESSAGE_V2), pulling (PULL_MESSAGE), consumer offset
// query/update, heartbeat, querying messages by key, and topic creation.
#ifndef ROCKETMQ_CLIENT_MQ_CLIENT_H
#define ROCKETMQ_CLIENT_MQ_CLIENT_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/consumer_stats.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/top_addressing.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/remoting_client.h"

namespace rocketmq {

std::string changeInstanceNameToPID(const std::string& instanceName);

std::string buildMqClientId(const std::string& clientIp, const std::string& instanceName,
                            const std::string& unitName = std::string(),
                            bool enableStreamRequestType = false);

std::string buildClientId(const std::string& instanceName,
                          const std::string& unitName = std::string(),
                          bool enableStreamRequestType = false);

class TopicPublishInfo {
public:
    bool orderTopic = false;
    std::vector<MessageQueue> msgQueueList;
    TopicRouteData topicRouteData;

    TopicPublishInfo() = default;
    TopicPublishInfo(const TopicPublishInfo&) = delete;
    TopicPublishInfo& operator=(const TopicPublishInfo&) = delete;

    bool ok() const { return !msgQueueList.empty(); }

    MessageQueue selectOneMessageQueue();
    MessageQueue selectOneMessageQueue(const std::string& lastBrokerName);

    // Filtered round-robin: the cursor advances as usual, and a full round in which nothing
    // matches returns nullopt so the caller falls back to a degraded pick.
    // A queue is selected only when every filter accepts it.
    std::optional<MessageQueue> selectOneMessageQueue(
        const std::function<bool(const MessageQueue&)>& filter,
        const std::function<bool(const MessageQueue&)>& brokerFilter);
    // Reset the round-robin cursor (resetIndex, used for fault avoidance)
    void resetIndex() { index_.store(0); }

private:
    std::atomic<uint64_t> index_{0};
};

class MQClientInstance {
public:
    static bool tlsEnabledFromEnv();

    MQClientInstance(const std::string& clientId,
                     const std::vector<std::string>& nameServerAddrs,
                     int32_t connectTimeoutMillis = 3000,
                     int32_t invokeTimeoutMillis = 15000,
                     bool tlsEnable = tlsEnabledFromEnv(),
                     const std::string& unitName = std::string(),
                     int32_t pollNameServerIntervalMillis = 30000);
    ~MQClientInstance();

    MQClientInstance(const MQClientInstance&) = delete;
    MQClientInstance& operator=(const MQClientInstance&) = delete;

    // ---------------- Lifecycle ----------------
    void start();
    void shutdown();

    const std::string& clientId() const { return clientId_; }
    std::vector<std::string> nameServerAddrs() const;
    void updateNameServerAddressList(const std::vector<std::string>& addrs);
    RemotingClient& remotingClient() { return *remotingClient_; }
    // Route refresh interval held by this instance (offline / live-broker tests assert the facade forwards it).
    int32_t pollNameServerIntervalMillis() const { return pollNameServerIntervalMillis_; }

    // Broker VIP channel: when true every broker-facing request in the rewritten family (message
    // sends, offset query/update, queue locks, CHECK_CLIENT_CONFIG, GET_CONSUMER_LIST_BY_GROUP,
    // CONSUMER_SEND_MSG_BACK, QUERY_MESSAGE, create/delete topic) is sent to port - 2 of the route
    // address (10911 -> 10909). Off (default) leaves the address unchanged. Name-server requests and
    // the pull/pop/ack/heartbeat/unregister family are never affected by this switch.
    void setVipChannelEnabled(bool enable) { vipChannelEnabled_ = enable; }
    bool isVipChannelEnabled() const { return vipChannelEnabled_; }

    DefaultTopAddressing& topAddressing() { return topAddressing_; }
    void fetchNameServerAddr();

    ConsumerStatsManager& consumerStats() { return consumerStats_; }

    void registerRebalanceWakeup(const std::string& group, std::function<void()> wakeup);
    void unregisterRebalanceWakeup(const std::string& group);
    void rebalanceImmediately();
    // How many broker code-40 notifications have been received. Reverse requests can only be sent
    // by the broker and tests cannot inject one, so the counter is the only live assertion point.
    size_t consumerIdsChangedCount() const { return consumerIdsChangedCount_.load(); }
    void processNotifyConsumerIdsChanged(const RemotingCommand& cmd, const std::string& addr);

    bool registerRPCHook(std::shared_ptr<RPCHook> hook) {
        return remotingClient_->registerRPCHook(std::move(hook));
    }

    // ---- TLS strict verification: a non-empty caCert means strict chain + hostname checks ----
    // Must be called before start(); it also enables TLS automatically (equivalent to
    // setTlsEnable(true)) and forwards the options to the RemotingClient, live on the first connection.
    void setTlsOptions(const TlsOptions& options) {
        tlsOptions_ = options;
        tlsEnable_ = true;
        remotingClient_->setTlsOptions(options);
    }

    void setSocksProxy(const SocksProxyConfig& proxy) {
        remotingClient_->setSocksProxy(proxy);
    }

    bool updateTopicRouteInfoFromNameServer(const std::string& topic,
                                            int32_t timeoutMillis = 5000,
                                            bool isDefault = false);
    // Get the publish info (**the cached instance is shared**, the round-robin cursor advances inside it);
    // a cache miss triggers one route refresh, and if it still fails an MQClientException is thrown.
    // isDefault is passed through to updateTopicRouteInfoFromNameServer (only the producer send path sets true).
    std::shared_ptr<TopicPublishInfo> getTopicPublishInfo(const std::string& topic,
                                                         bool isDefault = false);
    std::vector<MessageQueue> getTopicSubscribeInfo(const std::string& topic);
    void registerTopicInUse(const std::string& topic);
    std::shared_ptr<TopicRouteData> getTopicRouteData(const std::string& topic);
    // Read-only cache-hit probe (**no network pull triggered**); getTopicRouteData pulls once on a
    // miss, so live checks of "when the periodic refresh brings a new topic" use it to avoid masking the period.
    bool isTopicRouteCached(const std::string& topic) const;

    static std::string findBrokerAddrInRoute(const TopicRouteData& route,
                                             const std::string& brokerName);

    static std::pair<std::string, bool> findBrokerAddressInSubscribe(
        const std::map<int64_t, std::string>& brokerAddrs, int64_t brokerId,
        bool onlyThisBroker = false);

    // ---------------- Message sending ----------------
    // sysFlag is computed by the caller (Producer): the compression flag and compression-type bits are
    // sent from here, and msg.body must already hold the compressed bytes (see DefaultMQProducer::prepareForSend).
    SendResult sendMessage(const std::string& producerGroup, const Message& msg,
                           const MessageQueue& mq, int32_t timeoutMillis = 3000,
                           int32_t sysFlag = 0, bool unitMode = false,
                           const std::optional<std::string>& createTopicKey = std::nullopt,
                           const std::optional<int32_t>& defaultTopicQueueNums = std::nullopt);
    void sendMessageOneway(const std::string& producerGroup, const Message& msg,
                           const MessageQueue& mq, int32_t timeoutMillis = 3000,
                           int32_t sysFlag = 0, bool unitMode = false,
                           const std::optional<std::string>& createTopicKey = std::nullopt,
                           const std::optional<int32_t>& defaultTopicQueueNums = std::nullopt);

    RemotingCommand buildSendRequest(const std::string& producerGroup, const Message& msg,
                                     const MessageQueue& mq, int32_t sysFlag, bool unitMode,
                                     const std::optional<std::string>& createTopicKey = std::nullopt,
                                     const std::optional<int32_t>& defaultTopicQueueNums = std::nullopt);
    // Parse the response into a SendResult; throws MQBrokerException when the broker returns a non-success code.
    static SendResult parseSendResponse(const RemotingCommand& response, const Message& msg,
                                        const MessageQueue& mq);
    void sendMessageAsync(const std::string& addr, RemotingCommand& request, const Message& msg,
                          const MessageQueue& mq, int32_t timeoutMillis,
                          std::function<void(const SendResult&, const InvokeError&)> onComplete);

    std::string recallMessage(const std::string& addr, const RecallMessageRequestHeader& header,
                              int32_t timeoutMillis = 3000);

    PullResult pullMessage(const std::string& consumerGroup, const MessageQueue& mq,
                           int64_t queueOffset, int32_t maxMsgNums, int32_t sysFlag,
                           int64_t commitOffset, const std::string& subscription,
                           int64_t subVersion, const std::string& expressionType,
                           int32_t timeoutMillis = 30000, int32_t maxMsgBytes = -1,
                           int32_t suspendTimeoutMillis = 15000,
                           const std::string& addr = std::string(),
                           int32_t requestSource = 0,
                           std::optional<int64_t> brokerId = std::nullopt);

    bool queryConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                             int64_t& outOffset, int32_t timeoutMillis = 5000,
                             const std::string& addr = std::string(),
                             bool setZeroIfNotFound = false);
    void updateConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                              int64_t commitOffset, int32_t timeoutMillis = 5000,
                              const std::string& addr = std::string());
    std::vector<MessageQueue> lockBatchMq(const std::string& consumerGroup,
                                          const std::string& clientId,
                                          const std::vector<MessageQueue>& mqs,
                                          int32_t timeoutMillis = 5000);
    void unlockBatchMq(const std::string& consumerGroup, const std::string& clientId,
                       const std::vector<MessageQueue>& mqs, int32_t timeoutMillis = 5000);
    int64_t getMaxOffset(const MessageQueue& mq, int32_t timeoutMillis = 5000,
                         const std::string& addr = std::string());
    int64_t getMinOffset(const MessageQueue& mq, int32_t timeoutMillis = 5000,
                         const std::string& addr = std::string());
    int64_t searchOffsetByTimestamp(const MessageQueue& mq, int64_t timestamp,
                                    int32_t timeoutMillis = 5000,
                                    const std::string& addr = std::string());
    int64_t searchOffsetByBoundary(const MessageQueue& mq, int64_t timestamp,
                                   const std::optional<BoundaryType>& boundaryType,
                                   int32_t timeoutMillis = 5000,
                                   const std::string& addr = std::string());

    // ---------------- POP mode (5.x lightweight consumption) ----------------
    //
    // Semantics differ from pull: **no offset commit**, acknowledgement goes through ackMessage after
    // consumption; un-acked messages are revived by the broker after invisibleTime and redelivered to
    // %RETRY%<group>_<topic> (V1), popping back on the next POP (at-least-once). queueId = -1 pops all queues of the topic.
    //
    // Every returned message is already stamped with POP_CK (the 8-segment CK string rebuilt client-side) and 1ST_POP_TIME.
    PopResult popMessage(const std::string& consumerGroup, const std::string& topic,
                         int32_t queueId, int32_t maxMsgNums, int64_t invisibleTime,
                         int64_t pollTime, int32_t initMode,
                         const std::string& expression = std::string(),
                         const std::string& expressionType = std::string(),
                         bool order = false, int32_t timeoutMillis = 10000,
                         const std::string& brokerName = std::string(),
                         const std::string& addr = std::string());

    // Ack one POP message. extraInfo uses the POP_CK property on the message; offset must be the
    // **consumeQueue offset** (the 8th segment of the CK string), not the commitlog offset; passing
    // the wrong one makes the broker answer NO_MESSAGE(208). Returns the broker code; SUCCESS(0) is success.
    int32_t ackMessage(const std::string& consumerGroup, const std::string& topic,
                       int32_t queueId, const std::string& extraInfo, int64_t offset,
                       int32_t timeoutMillis = 3000,
                       const std::string& brokerName = std::string(),
                       const std::string& addr = std::string());

    // Ack a batch of POP messages in one BATCH_ACK_MESSAGE request. extraInfoList are the POP_CK
    // strings of the messages to acknowledge (the same 8-segment form ackMessage takes one of).
    // Grouping rule: the merge key is "<retry>@<queueId>@<ckQueueOffset>@<popTime>". The first extra
    // info seen supplies the body's broker name. The first extra info of a merge key creates one
    // batch entry, pre-filled with consumerGroup/topic/retry/startOffset(=ckQueueOffset)/queueId/
    // reviveQueueId/popTime/invisibleTime and an empty offset bit set; a repeated merge key only
    // sets the bit at index (queueOffset - ckQueueOffset). The request carries no custom header; the
    // JSON body is {"brokerName":..,"acks":[..]} with the compact per-entry keys. addr is the broker
    // address; when empty it is resolved from the broker name of the first extra info the same way
    // ackMessage does. Returns the broker code; SUCCESS(0) is success.
    int32_t batchAckMessage(const std::string& consumerGroup, const std::string& topic,
                            const std::vector<std::string>& extraInfoList,
                            int32_t timeoutMillis = 3000,
                            const std::string& addr = std::string());

    // Extend a POP message's invisibility. The response returns the **new** popTime/invisibleTime/
    // reviveQid, from which the client rebuilds the 8-segment extraInfo (result field) for the later ACK.
    ChangeInvisibleTimeResult changeInvisibleTime(const std::string& consumerGroup,
                                                  const std::string& topic, int32_t queueId,
                                                  const std::string& extraInfo, int64_t offset,
                                                  int64_t invisibleTime,
                                                  int32_t timeoutMillis = 3000,
                                                  const std::string& brokerName = std::string(),
                                                  const std::string& addr = std::string());

    // Exposed as a standalone static function for **unit-testability**: this is the logic in the whole
    // POP implementation that is easiest to get wrong and hardest to pin down against a live broker
    // (POP straight to a normal topic leaves POP_CK unwritten, so the client must rebuild it from startOffsetInfo/msgOffsetInfo, otherwise ACK has nothing to work with).
    //
    // Note: the caller must invoke this **before rewriting the message topic**, because retryFlag is
    // derived from the message's original topic (the broker may have rewritten the topic).
    static void stampPopCk(std::vector<MessageExt>& msgs, const std::string& brokerName,
                           const PopMessageResponseHeader& respHeader);

    // ---------------- Heartbeat / unregister ----------------
    void sendHeartbeat(const std::string& addr, const HeartbeatData& heartbeatData,
                       int32_t timeoutMillis = 5000);
    void unregisterClient(const std::string& addr, const std::string& clientId,
                          const std::string& producerGroup, const std::string& consumerGroup,
                          int32_t timeoutMillis = kMqClientApiTimeoutMillis);
    void unregisterClientAllBrokers(const std::string& clientId,
                                    const std::string& producerGroup,
                                    const std::string& consumerGroup,
                                    int32_t timeoutMillis = kMqClientApiTimeoutMillis);

    static constexpr int32_t kMqClientApiTimeoutMillis = 3000;

    std::string findBrokerAddrByTopic(const std::string& topic);

    void checkClientConfig(const std::string& brokerAddr, const std::string& consumerGroup,
                           const std::string& clientId, const SubscriptionData& subscriptionData,
                           int32_t timeoutMillis = kMqClientApiTimeoutMillis);

    void checkSubscriptionsInBroker(const std::string& group,
                                    const std::vector<SubscriptionData>& subs);

    // ---------------- Generic synchronous invoke (reused by the admin side) ----------------
    // Send any requestCode + extFields + body. When languageOverride >= 0 it overrides the request's
    // language field: a few RPCs change behavior based on it (INVOKE_BROKER_TO_RESET_OFFSET returns a
    // parseable offsetTable response body only for CPP/PYTHON).
    //
    // invokeSyncRaw does not validate the response code, leaving that to the caller (for many admin
    // APIs "not found" is a normal branch, e.g. QUERY_NOT_FOUND); invokeSync throws MQBrokerException.
    RemotingCommand invokeSyncRaw(const std::string& addr, int32_t code,
                                  const PropertyMap& extFields = PropertyMap(),
                                  const Bytes& body = Bytes(), bool hasBody = false,
                                  int32_t timeoutMillis = 3000,
                                  int32_t languageOverride = -1);
    RemotingCommand invokeSync(const std::string& addr, int32_t code,
                               const PropertyMap& extFields = PropertyMap(),
                               const Bytes& body = Bytes(), bool hasBody = false,
                               int32_t timeoutMillis = 3000,
                               int32_t languageOverride = -1);
    // Turn a response code other than SUCCESS into an MQBrokerException
    static void checkResponseCode(const RemotingCommand& response);

    // ---------------- Administrative operations ----------------
    void createTopicInBroker(const std::string& brokerAddr, const std::string& defaultTopic,
                             const std::string& topic, int32_t readQueueNums = 4,
                             int32_t writeQueueNums = 4, int32_t perm = 6,
                             int32_t topicSysFlag = 0,
                             const std::string& topicFilterType = TopicFilterType::SINGLE_TAG,
                             const std::string& attributes = std::string(),
                             bool force = false, int32_t timeoutMillis = 5000,
                             int32_t retryTimes = 5);
    void createTopicInRoute(const std::string& topic, int32_t readQueueNums = 4,
                            int32_t writeQueueNums = 4, int32_t perm = 6,
                            int32_t timeoutMillis = 5000);
    void deleteTopicInBroker(const std::string& brokerAddr, const std::string& topic,
                             int32_t timeoutMillis = 5000);
    void deleteTopicInNamesrv(const std::string& topic, int32_t timeoutMillis = 5000);

    // ---------------- Cluster / topic / consumer lists ----------------
    ClusterInfo getBrokerClusterInfo(int32_t timeoutMillis = 10000);
    TopicList getAllTopicListFromNameServer(int32_t timeoutMillis = 10000);
    GetConsumerListByGroupResponseBody getConsumerListByGroup(
        const std::string& consumerGroup, const std::string& addr,
        int32_t timeoutMillis = 5000);
    std::vector<std::string> getConsumerIdListByGroup(const std::string& topic,
                                                     const std::string& consumerGroup,
                                                     int32_t timeoutMillis = 5000);

    // ---------------- Query messages by key / uniqKey ----------------
    // indexType is one of MessageConst::INDEX_*_TYPE; when uniqKey is true, MixAll::UNIQUE_MSG_QUERY_FLAG
    // is sent as well and a hit is re-checked against msgId == key.
    bool queryMessage(const std::string& topic, const std::string& key, int32_t maxNum,
                      int64_t beginTimestamp, int64_t endTimestamp, Bytes& outBody,
                      int32_t timeoutMillis = 15000, const std::string& addr = std::string(),
                      const std::string& indexType = std::string(), bool uniqKey = false);
    std::vector<MessageExt> queryMessageAllBrokers(const std::string& topic,
                                                   const std::string& key, int32_t maxNum,
                                                   int64_t beginTimestamp, int64_t endTimestamp,
                                                   const std::string& indexType = std::string(),
                                                   bool uniqKey = false,
                                                   int32_t timeoutMillis = 15000);

    // ---------------- Utilities ----------------
    std::string brokerAddrOf(const std::string& brokerName);
    // Resolve the broker address of mq (public version; throws MQClientException when not found)
    std::string brokerAddrForMq(const MessageQueue& mq);

    std::string findBrokerAddressInPublish(const std::string& brokerName);

    std::string getBrokerNameFromMessageQueue(const MessageQueue& mq);

    // Targeted sends do not fetch publish info inside sendDefaultImpl, so this is their only route source;
    // during a master/slave switch this is also the dividing line between failing locally at once and
    // hitting a slave only to eat a pointless SYSTEM_BUSY(2) round: slaves do not accept the write
    // requests SEND_MESSAGE / CONSUMER_SEND_MSG_BACK / CHANGE_INVISIBLE_TIME.
    std::string publishAddrFor(const std::string& brokerName, const std::string& topic);
    // Address resolution for the three POP calls (popMessage / ackMessage / changeInvisibleTime).
    //
    //
    // brokerName is passed by reference: when the caller omits it, the brokerName of the first broker
    // picked from the route fills it in place (POP_CK rebuilt from an empty brokerName computes retryFlag wrongly); a non-empty addrIn is returned unchanged.
    std::string resolvePopAddr(const std::string& topic, std::string& brokerName,
                               const std::string& addrIn);
    // Address resolution for POP ack / changeInvisibleTime.
    std::string popAckAddrFor(const std::string& topic, const std::string& brokerName,
                              int32_t queueId, const std::string& addrIn);
    std::vector<std::string> getRouteOfAllBrokers();
    // List every broker address in the known routes (used for liveness probing)
    std::vector<std::string> knownBrokerAddrs();
    std::vector<std::string> getAllBrokerAddrs();

private:
    // Resolve the broker address of mq; throws MQClientException when not found
    std::string brokerAddr(const MessageQueue& mq);
    std::string publishAddrInAdmin(const MessageQueue& mq);
    std::string consumerOffsetAddr(const MessageQueue& mq);
    // Target address of a broker request in the rewritten family: port - 2 when the VIP switch is on,
    // otherwise the address unchanged. Name-server addresses must never be passed here. The value is
    // computed per request at the invoke boundary; a shifted address is never cached.
    std::string vipAddr(const std::string& addr) const;
    RemotingCommand invokeSyncOnAddr(const std::string& addr, RemotingCommand& request,
                                     int32_t timeoutMillis);
    void routeRefreshLoop();

    // For seeding the endpoint table in offline unit tests (offline has no name server; production code writes this table only in the route-commit block).
    friend struct MQClientInstanceTestAccess;

    std::string clientId_;
    std::vector<std::string> nameServerAddrs_;
    std::unique_ptr<RemotingClient> remotingClient_;
    int32_t pollNameServerIntervalMillis_ = 30000;

    mutable std::recursive_mutex routeLock_;
    std::map<std::string, TopicRouteData> topicRouteTable_;
    // Failure reason of the latest "every name server unreachable" (written by updateTopicRouteInfoFromNameServer
    // and attached when getTopicPublishInfo reports "Can not find Message Queue": a root cause such as
    // a TLS handshake CA verification failure must not be black-boxed as a missing route). Read-heavy: own small lock.
    std::mutex lastRouteErrorMutex_;
    std::string lastRouteError_;
    std::map<std::string, std::map<int64_t, std::string>> brokerAddrTable_;
    std::map<std::string, std::map<MessageQueue, std::string>> topicEndPointsTable_;
    std::map<std::string, std::shared_ptr<TopicPublishInfo>> topicPublishInfoTable_;
    // Topics in use (subscribed by consumers + sent by producers), routes refreshed by the periodic task
    std::set<std::string> topicsInUse_;
    bool tlsEnable_ = false;
    bool vipChannelEnabled_ = false;
    TlsOptions tlsOptions_;
    bool started_ = false;
    std::atomic<bool> routeRefreshStop_{false};
    std::thread routeRefreshThread_;
    std::atomic<bool> namesrvRefreshStop_{false};
    std::thread namesrvRefreshThread_;
    void namesrvRefreshLoop();
    DefaultTopAddressing topAddressing_;
    ConsumerStatsManager consumerStats_;
    // group -> consumer "wakeup" callback (removed when the consumer shuts down, to avoid a dangling this)
    std::mutex rebalanceWakeupLock_;
    std::map<std::string, std::function<void()>> rebalanceWakeups_;
    std::atomic<size_t> consumerIdsChangedCount_{0};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_MQ_CLIENT_H
