// Coverage: topic add/delete/query and configuration, broker cluster info plus runtime info/config,
// name server KV configuration, subscription-group management, consumer/producer connections,
// consume statistics, offset management (incl. real offset reset on a live broker), message queries (key / uniqKey / msgId / ConsumeQueue).
#ifndef ROCKETMQ_CLIENT_ADMIN_H
#define ROCKETMQ_CLIENT_ADMIN_H

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/remoting/protocol/admin_body.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/protocol/subscription.h"

namespace rocketmq {

enum class TrackType {
    CONSUMED,
    CONSUMED_BUT_FILTERED,
    PULL,
    NOT_CONSUME_YET,
    NOT_ONLINE,
    CONSUME_BROADCASTING,
    UNKNOWN,
};

const char* trackTypeName(TrackType type);

struct MessageTrack {
    std::string consumerGroup;
    TrackType trackType = TrackType::UNKNOWN;
    std::string exceptionDesc;
};

struct GroupForbidden {
    std::string topic;
    std::string group;
    bool readable = false;

    // Returns false on a parse failure (never throws)
    static bool decode(const Bytes& data, GroupForbidden& out);
};

class DefaultMQAdminExt {
public:
    // Matches DefaultMQAdminExt.DEFAULT_TIMEOUT = 5000 * 3
    static constexpr int32_t DEFAULT_TIMEOUT = 5000 * 3;

    explicit DefaultMQAdminExt(const std::string& instanceName = "ADMIN");
    DefaultMQAdminExt(const DefaultMQAdminExt&) = delete;
    DefaultMQAdminExt& operator=(const DefaultMQAdminExt&) = delete;
    ~DefaultMQAdminExt();

    // ---------------- Configuration and lifecycle ----------------
    void setNamesrvAddr(const std::string& addr);  // Semicolon-separated
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }

    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    // When true every request carries the extension field `ReqT=0`, and the clientId gains a trailing `@STREAM` segment.
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }
    void setPollNameServerIntervalMillis(int32_t millis) { pollNameServerIntervalMillis_ = millis; }
    int32_t pollNameServerIntervalMillis() const { return pollNameServerIntervalMillis_; }
    void setVipChannelEnabled(bool enable) { vipChannelEnabled_ = enable; }
    bool isVipChannelEnabled() const { return vipChannelEnabled_; }
    std::string getNamesrvAddr() const;
    std::vector<std::string> getNameServerAddressList() const { return nameServerAddrs_; }
    void setTimeoutMillis(int32_t millis) { timeoutMillis_ = millis; }
    int32_t getTimeoutMillis() const { return timeoutMillis_; }
    void addKvNamespaceToDeleteList(const std::string& ns) {
        kvNamespaceToDeleteList_.push_back(ns);
    }

    void setRPCHook(std::shared_ptr<RPCHook> hook) { rpcHook_ = std::move(hook); }
    void setCredentials(const std::string& accessKey, const std::string& secretKey,
                        const std::string& securityToken = std::string()) {
        rpcHook_ = std::make_shared<AclClientRPCHook>(
            SessionCredentials(accessKey, secretKey, securityToken));
    }
    void setNamespaceV2(const std::string& ns) { namespaceV2_ = ns; }
    const std::string& namespaceV2() const { return namespaceV2_; }

    void start();
    void shutdown();
    bool isStarted() const { return started_; }
    MQClientInstance& getMQClientInstance();
    MQClientInstance& client() { return getMQClientInstance(); }

    // ---------------- Topic management ----------------
    void createTopic(const std::string& key, const std::string& newTopic,
                     int32_t queueNum = 4, int32_t topicSysFlag = 0);
    void createAndUpdateTopicConfig(const std::string& addr, const TopicConfig& config);
    void createTopicInBroker(const std::string& brokerAddr, const std::string& topic,
                             int32_t readQueueNums = 4, int32_t writeQueueNums = 4,
                             int32_t perm = 6);
    void deleteTopicInBroker(const std::string& brokerAddr, const std::string& topic);
    void deleteTopicInNameServer(const std::vector<std::string>& addrs,
                                 const std::string& topic);
    void deleteTopicInNamesrv(const std::string& topic);
    void deleteTopic(const std::string& topic, const std::string& clusterName = std::string());
    TopicList fetchAllTopicList();
    std::set<std::string> fetchTopicsByCluster(const std::string& clusterName);
    std::set<std::string> getClusterList(const std::string& topic);
    std::vector<TopicRouteData> fetchAllTopicRoute();
    TopicRouteData examineTopicRoute(const std::string& topic);
    TopicConfig examineTopicConfig(const std::string& addr, const std::string& topic);
    TopicConfigSerializeWrapper getAllTopicConfig(const std::string& brokerAddr,
                                                 int32_t timeoutMillis = -1);
    TopicConfigSerializeWrapper getUserTopicConfig(const std::string& brokerAddr,
                                                   bool specialTopic = false,
                                                   int32_t timeoutMillis = -1);
    TopicList getSystemTopicListFromBroker(const std::string& brokerAddr,
                                          int32_t timeoutMillis = -1);
    TopicStatsTable examineTopicStats(const std::string& topic);
    TopicStatsTable examineTopicStatsByBroker(const std::string& brokerAddr,
                                             const std::string& topic);

    void createAndUpdateTopicConfigList(const std::string& brokerAddr,
                                        const std::vector<TopicConfig>& configs);
    // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST(225): same shape, but the body key is
    // `groupConfigList` (note the difference from the bare object used by single-group 200).
    void createAndUpdateSubscriptionGroupConfigList(
        const std::string& brokerAddr, const std::vector<SubscriptionGroupConfig>& configs);
    // UPDATE_AND_CREATE_STATIC_TOPIC(513): the header reuses CreateTopicRequestHeader fields
    // (bools become lowercase "true"/"false"), the body is the TopicQueueMappingDetail JSON document.
    void createStaticTopic(const std::string& brokerAddr, const std::string& defaultTopic,
                           const TopicConfig& config, const JsonValue& mappingDetail,
                           bool force = false);
    // UPDATE_AND_GET_GROUP_FORBIDDEN(353): readable is sent only when not nullopt;
    // returns the GroupForbidden decoded from the broker.
    GroupForbidden updateAndGetGroupReadForbidden(
        const std::string& brokerAddr, const std::string& group, const std::string& topic,
        const std::optional<bool>& readable = std::nullopt);
    bool resumeCheckHalfMessage(const std::string& brokerAddr, const std::string& topic,
                                const std::string& msgId = std::string());
    // **Not a standalone request**: a read-modify-write on the name server KV namespace=ORDER_TOPIC_CONFIG.
    // With isCluster the value goes to putKVConfig verbatim; otherwise the stored value is treated as a
    // ";"-separated list of "key:value" entries, the entry with a matching key is replaced or appended, then the whole list is written back.
    void createOrUpdateOrderConf(const std::string& key, const std::string& value,
                                 bool isCluster = false);

    void cleanExpiredConsumerQueue(const std::string& brokerAddr, int32_t timeHours);
    // Call 306 per address and return the list of **failed addresses** (no fail-fast)
    std::vector<std::string> cleanExpiredConsumerQueueByAddr(
        const std::vector<std::string>& addrs, int32_t timeHours);
    // DELETE_EXPIRED_COMMITLOG(329): header `time`
    void deleteExpiredCommitLog(const std::string& brokerAddr, int32_t timeHours);
    // Multi-address form of 329
    std::vector<std::string> deleteExpiredCommitLogByAddr(
        const std::vector<std::string>& addrs, int32_t timeHours);
    // CLEAN_UNUSED_TOPIC(316): a **single request**, the broker cleans up by itself. Never iterate the
    // client-side topic table and delete one by one (broker-created topics would be rejected).
    void cleanUnusedTopicByAddr(const std::string& brokerAddr);
    // QUERY_CONSUME_TIME_SPAN(303): fan out to every master along the topic route and aggregate the
    // consumeTimeSpanSet JSON arrays from the response bodies (the aggregated array is returned).
    JsonValue queryConsumeTimeSpan(const std::string& topic, const std::string& group);
    std::set<std::string> getTopicClusterList(const std::string& topic);
    // SET_MESSAGE_REQUEST_MODE(401): popShareQueueNum is carried only when > 0.
    void setMessageRequestMode(const std::string& brokerAddr, const std::string& topic,
                               const std::string& consumerGroup, const std::string& mode,
                               int32_t popShareQueueNum = 0);

    void updateNameServerConfig(const PropertyMap& properties, int32_t timeoutMillis = -1);
    // Query one by one (all name servers by default); the body is parsed as properties text and
    // the result is an {address: properties map}.
    std::map<std::string, PropertyMap> getNameServerConfig(
        const std::vector<std::string>& namesrvAddrs = std::vector<std::string>(),
        int32_t timeoutMillis = -1);

    // ---------------- Cluster / broker ----------------
    ClusterInfo fetchBrokerClusterInfo();
    ClusterInfo examineBrokerClusterInfo() { return fetchBrokerClusterInfo(); }
    KVTable fetchBrokerRuntimeStats(const std::string& brokerAddr, int32_t timeoutMillis = -1);
    KVTable getBrokerRuntimeInfo(const std::string& brokerAddr, int32_t timeoutMillis = -1) {
        return fetchBrokerRuntimeStats(brokerAddr, timeoutMillis);
    }
    // The GET_BROKER_CONFIG response body is **properties text**, parsed here into k=v
    PropertyMap getBrokerConfig(const std::string& brokerAddr, int32_t timeoutMillis = -1);
    void updateBrokerConfig(const std::string& brokerAddr, const PropertyMap& properties,
                            int32_t timeoutMillis = -1);
    int32_t wipeWritePermOfBroker(const std::string& namesrvAddr, const std::string& brokerName);
    int32_t addWritePermOfBroker(const std::string& namesrvAddr, const std::string& brokerName);
    bool cleanUnusedTopic(const std::string& clusterName = std::string(),
                          const std::string& topic = std::string());
    JsonValue viewBrokerStatsData(const std::string& brokerAddr, const std::string& statsName,
                                  const std::string& statsKey);

    // ---------------- Name server KV configuration ----------------
    void createAndUpdateKvConfig(const std::string& ns, const std::string& key,
                                 const std::string& value);
    void putKvConfig(const std::string& ns, const std::string& key, const std::string& value) {
        createAndUpdateKvConfig(ns, key, value);
    }
    // Returns false when the key does not exist
    bool getKvConfig(const std::string& ns, const std::string& key, std::string& outValue);
    void deleteKvConfig(const std::string& ns, const std::string& key);
    KVTable getKvListByNamespace(const std::string& ns);

    // ---------------- Subscription group management ----------------
    void createAndUpdateSubscriptionGroupConfig(const std::string& addr,
                                                const SubscriptionGroupConfig& config);
    // Returns false when the subscription group does not exist
    bool examineSubscriptionGroupConfig(const std::string& addr, const std::string& group,
                                        SubscriptionGroupConfig& out);
    bool getSubscriptionGroupConfig(const std::string& addr, const std::string& group,
                                    SubscriptionGroupConfig& out);
    SubscriptionGroupWrapper getAllSubscriptionGroup(const std::string& brokerAddr,
                                                     int32_t timeoutMillis = -1);
    SubscriptionGroupWrapper getUserSubscriptionGroup(const std::string& brokerAddr,
                                                      int32_t timeoutMillis = -1);
    void deleteSubscriptionGroup(const std::string& addr, const std::string& groupName,
                                 bool removeOffset = false);

    // ---------------- Consumer / producer connections ----------------
    ConsumerConnection examineConsumerConnectionInfo(
        const std::string& consumerGroup, const std::string& brokerAddr = std::string());
    ConsumerConnection examineConsumerConnection(
        const std::string& consumerGroup, const std::string& brokerAddr = std::string()) {
        return examineConsumerConnectionInfo(consumerGroup, brokerAddr);
    }
    ProducerConnection examineProducerConnectionInfo(
        const std::string& producerGroup, const std::string& brokerAddr = std::string());
    ConsumerRunningInfo examineConsumerRunningInfo(
        const std::string& consumerGroup, const std::string& clientId, bool jstack = false,
        const std::string& brokerAddr = std::string());
    GetConsumerListByGroupResponseBody getConsumerListByGroup(
        const std::string& consumerGroup, const std::string& brokerAddr = std::string());

    // ---------------- Consume statistics ----------------
    ConsumeStats examineConsumeStats(const std::string& brokerAddr,
                                     const std::string& consumerGroup,
                                     const std::string& topic = std::string(),
                                     const std::vector<std::string>& topicList = {});
    ConsumeStatsList fetchConsumeStatsInBroker(const std::string& brokerAddr,
                                               bool isOrder = false,
                                               int32_t timeoutMillis = -1);
    std::set<std::string> queryTopicConsumeByWho(const std::string& brokerAddr,
                                                 const std::string& topic);
    ConsumeStats examineConsumeStatsGroup(const std::string& consumerGroup,
                                          const std::string& topic = std::string());
    bool consumed(const MessageExt& msg, const std::string& consumerGroup);
    std::vector<MessageTrack> messageTrackDetail(const MessageExt& msg);
    TopicList queryTopicsByConsumerToBroker(const std::string& brokerAddr,
                                            const std::string& group);
    TopicList queryTopicsByConsumer(const std::string& group);
    JsonValue querySubscription(const std::string& brokerAddr, const std::string& group,
                                const std::string& topic);
    JsonValue getConsumeStatus(const std::string& brokerAddr, const std::string& topic,
                               const std::string& group,
                               const std::string& clientAddr = std::string());
    void cloneGroupOffset(const std::string& brokerAddr, const std::string& srcGroup,
                          const std::string& destGroup, const std::string& topic,
                          bool offline = false);

    // ---------------- Offset management ----------------
    int64_t maxOffset(const MessageQueue& mq);
    int64_t minOffset(const MessageQueue& mq);
    int64_t searchOffset(const MessageQueue& mq, int64_t timestamp);
    int64_t searchLowerBoundaryOffset(const MessageQueue& mq, int64_t timestamp);
    int64_t searchUpperBoundaryOffset(const MessageQueue& mq, int64_t timestamp);
    int64_t earliestMsgStoreTime(const MessageQueue& mq);
    // Returns false when the consumer group has no offset on this queue yet
    bool examineConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                               int64_t& outOffset);
    void updateConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                              int64_t offset);
    void updateConsumerOffsetToBroker(const std::string& brokerAddr,
                                      const std::string& consumerGroup, const MessageQueue& mq,
                                      int64_t offset);
    std::map<MessageQueue, int64_t> resetOffsetByTimestamp(
        const std::string& topic, const std::string& group, int64_t timestamp,
        bool isForce = true, const std::string& clusterName = std::string(), bool isCpp = false);
    void resetOffsetNew(const std::string& consumerGroup, const std::string& topic,
                        int64_t timestamp);
    std::map<MessageQueue, int64_t> resetOffsetByTimestampOld(
        const std::string& consumerGroup, const std::string& topic, int64_t timestamp,
        bool force = true);
    std::map<MessageQueue, int64_t> resetOffsetByQueueId(
        const std::string& brokerAddr, const std::string& consumerGroup,
        const std::string& topic, int32_t queueId, int64_t resetOffset);

    static PropertyMap buildResetOffsetExtFields(const std::string& topic,
                                                 const std::string& group, int64_t timestamp,
                                                 bool isForce, int32_t queueId, int64_t offset);

    // ---------------- Message query ----------------
    std::vector<MessageExt> queryMessage(const std::string& topic, const std::string& key,
                                         int32_t maxNum, int64_t begin, int64_t end);
    // Returns false on a miss (note: with the default file index, uniqKey queries need the RocksDB index)
    bool queryMessageByUniqKey(const std::string& topic, const std::string& uniqKey,
                               MessageExt& out);
    std::vector<MessageExt> queryMessageByKey(const std::string& topic, const std::string& key,
                                             int32_t maxNum = 32);
    MessageExt viewMessage(const std::string& topic, const std::string& msgId);
    QueryConsumeQueueResponseBody queryConsumeQueue(const std::string& brokerAddr,
                                                   const std::string& topic, int32_t queueId,
                                                   int64_t index, int32_t count = 32,
                                                   const std::string& consumerGroup = std::string());

    // ---------------- Direct-consume probe (CONSUME_MESSAGE_DIRECTLY(309)) ----------------
    // 309 is an admin -> broker request: after the broker receives it, it **pushes it on** to the online
    // consumer for that clientId (see the client handler in DefaultMQPushConsumer) and brings back the
    // verdict of that consumer really consuming the message once locally (the ConsumeMessageDirectlyResult body).
    ConsumeMessageDirectlyResult consumeMessageDirectly(const std::string& consumerGroup,
                                                        const std::string& clientId,
                                                        const std::string& topic,
                                                        const std::string& msgId);
    ConsumeMessageDirectlyResult consumeMessageDirectly(const std::string& brokerAddr,
                                                        const std::string& consumerGroup,
                                                        const std::string& clientId,
                                                        const std::string& topic,
                                                        const std::string& msgId,
                                                        int32_t timeoutMillis = -1);
    static PropertyMap buildConsumeMessageDirectlyExtFields(
        const std::string& topic, const std::string& consumerGroup, const std::string& clientId,
        const std::string& msgId);

private:
    // ---------------- Low-level invoke helpers ----------------
    MQClientInstance& requireClient();
    // Target address of broker requests: port - 2 when the VIP switch is on. Name server requests do not go through here.
    std::string vipAddr(const std::string& addr) const {
        return MixAll::brokerVipChannel(vipChannelEnabled_, addr);
    }
    std::map<MessageQueue, int64_t> invokeBrokerToResetOffset(
        const std::string& brokerAddr, const std::string& topic, const std::string& group,
        int64_t timestamp, bool isForce, bool isCpp, int32_t queueId, int64_t offset);
    static std::string firstBrokerAddr(MQClientInstance& client);
    std::string findFirstBrokerAddr(MQClientInstance& client);
    std::string brokerAddrForMq(MQClientInstance& client, const MessageQueue& mq);
    std::vector<std::string> brokerAddrsOfCluster(MQClientInstance& client,
                                                  const std::string& clusterName);

    std::string instanceName_;
    std::string clientId_;
    std::string unitName_;
    std::string namespaceV2_;
    bool enableStreamRequestType_ = false;
    bool vipChannelEnabled_ = false;
    int32_t pollNameServerIntervalMillis_ = 30000;
    std::vector<std::string> nameServerAddrs_;
    std::vector<std::string> kvNamespaceToDeleteList_;
    int32_t timeoutMillis_ = DEFAULT_TIMEOUT;

    std::unique_ptr<MQClientInstance> mqClient_;
    // ACL hook, bound to the MQClientInstance transport at start()
    std::shared_ptr<RPCHook> rpcHook_;
    bool started_ = false;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_ADMIN_H
