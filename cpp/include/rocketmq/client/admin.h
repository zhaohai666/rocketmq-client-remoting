// Coverage: topic add/delete/query and configuration, broker cluster info plus runtime info/config,
// name server KV configuration, subscription-group management, consumer/producer connections,
// consume statistics, offset management (incl. real offset reset on a live broker), message queries (key / uniqKey / msgId / ConsumeQueue),
// AUTH user management and ACL management.
#ifndef ROCKETMQ_CLIENT_ADMIN_H
#define ROCKETMQ_CLIENT_ADMIN_H

#include <cstdint>
#include <functional>
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

// Code values carried by the AdminToolResult envelope of the concurrent fan-out forms.
struct AdminToolResultCode {
    static constexpr int32_t SUCCESS = 200;
    static constexpr int32_t REMOTING_ERROR = -1001;
    static constexpr int32_t MQ_BROKER_ERROR = -1002;
    static constexpr int32_t MQ_CLIENT_ERROR = -1003;
    static constexpr int32_t INTERRUPT_ERROR = -1004;
    static constexpr int32_t TOPIC_ROUTE_INFO_NOT_EXIST = -2001;
    static constexpr int32_t CONSUMER_NOT_ONLINE = -2002;
    static constexpr int32_t BROADCAST_CONSUMPTION = -2003;
};

// Envelope of the concurrent fan-out forms: success=true always pairs with code=SUCCESS,
// and a failure keeps `data` when the partial per-broker outcome is still useful.
template <typename T>
struct AdminToolResult {
    bool success = false;
    int32_t code = 0;
    std::string errorMsg;
    T data{};
};

// Response header of CONTROLLER_GET_METADATA_INFO(1005)
struct ControllerMetaData {
    std::string group;
    std::string controllerLeaderId;
    std::string controllerLeaderAddress;
    bool isLeader = false;
    std::string peers;
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
    void setVipChannelEnabled(bool enable) {
        vipChannelEnabled_ = enable;
        // The shared instance runs the offset / consumer-list family that the admin delegates
        // to it, so flipping the switch has to reach that instance too -- otherwise the admin's
        // own RPCs move while the delegated ones keep the listening port.
        if (mqClient_) {
            mqClient_->setVipChannelEnabled(enable);
        }
    }
    bool isVipChannelEnabled() const { return vipChannelEnabled_; }
    std::string getNamesrvAddr() const;
    std::vector<std::string> getNameServerAddressList() const { return nameServerAddrs_; }
    void setTimeoutMillis(int32_t millis) { timeoutMillis_ = millis; }
    int32_t getTimeoutMillis() const { return timeoutMillis_; }
    // maxPageSizeInGetMetadata (default 2000): page size carried as maxTopicNum / maxGroupNum by
    // the paged getAllTopicConfig / getAllSubscriptionGroup requests. Forwarded live to the shared instance.
    void setMaxPageSizeInGetMetadata(int32_t size) {
        maxPageSizeInGetMetadata_ = size;
        if (mqClient_) {
            mqClient_->setMaxPageSizeInGetMetadata(size);
        }
    }
    int32_t getMaxPageSizeInGetMetadata() const { return maxPageSizeInGetMetadata_; }
    int32_t maxPageSizeInGetMetadata() const { return maxPageSizeInGetMetadata_; }
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
    // SET_MESSAGE_REQUEST_MODE(401): no custom header at all -- the request is one JSON body
    // {topic, consumerGroup, mode, popShareQueueNum}, and `popShareQueueNum` is carried even when it is 0.
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

    // ---------------- AUTH user management (3001-3005) ----------------
    // Addressing is verbatim: the request goes to exactly the brokerAddr passed in (no VIP-channel
    // rewrite, no master resolution). Any response code other than SUCCESS throws MQBrokerException
    // carrying that code and remark. The convenience forms leave userStatus unset, so the body omits
    // that key entirely.
    void createUser(const std::string& brokerAddr, const UserInfo& user);
    void createUser(const std::string& brokerAddr, const std::string& username,
                    const std::string& password, const std::string& userType);
    void updateUser(const std::string& brokerAddr, const UserInfo& user);
    void updateUser(const std::string& brokerAddr, const std::string& username,
                    const std::string& password, const std::string& userType);
    void deleteUser(const std::string& brokerAddr, const std::string& username);
    // An empty response body leaves the result value untouched (all fields unset)
    UserInfo getUser(const std::string& brokerAddr, const std::string& username);
    // The filter header is sent only when present; an empty body decodes to an empty list
    std::vector<UserInfo> listUser(const std::string& brokerAddr,
                                   const std::optional<std::string>& filter = std::nullopt);

    // ---------------- AUTH ACL management (3006-3010) ----------------
    // The convenience forms build exactly one policy with no policyType and one entry per resource,
    // each entry repeating the same actions / sourceIps / decision.
    void createAcl(const std::string& brokerAddr, const AclInfo& acl);
    void createAcl(const std::string& brokerAddr, const std::string& subject,
                   const std::vector<std::string>& resources,
                   const std::vector<std::string>& actions,
                   const std::vector<std::string>& sourceIps, const std::string& decision);
    void updateAcl(const std::string& brokerAddr, const AclInfo& acl);
    void updateAcl(const std::string& brokerAddr, const std::string& subject,
                   const std::vector<std::string>& resources,
                   const std::vector<std::string>& actions,
                   const std::vector<std::string>& sourceIps, const std::string& decision);
    // Only the subject and resource headers are carried, nothing else
    void deleteAcl(const std::string& brokerAddr, const std::string& subject,
                   const std::string& resource);
    AclInfo getAcl(const std::string& brokerAddr, const std::string& subject);
    std::vector<AclInfo> listAcl(const std::string& brokerAddr,
                                 const std::optional<std::string>& subjectFilter = std::nullopt,
                                 const std::optional<std::string>& resourceFilter = std::nullopt);

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

    // ---------------- Container management (ADD_BROKER(902) / REMOVE_BROKER(903)) ----------------
    // Both go to the container address verbatim (no VIP rewrite), with the admin default timeout,
    // and any response code other than SUCCESS throws MQBrokerException. configPath is omitted from
    // the header when empty (nullable); the removeBroker fields are always sent.
    void addBroker(const std::string& containerAddr, const std::string& configPath);
    void removeBroker(const std::string& containerAddr, const std::string& brokerClusterName,
                      const std::string& brokerName, int64_t brokerId);

    // ---------------- Controller administration (1005/1006/1009/1010/1011) ----------------
    // 1005 goes to the controller address verbatim with a fixed 3000ms timeout and an empty header;
    // the response header becomes ControllerMetaData. Any other code throws MQBrokerException.
    ControllerMetaData getControllerMetaData(const std::string& controllerAddr);
    // Two-phase: 1005 to controllerAddr first, then 1006 to the controller leader with the broker
    // names as a raw JSON array body (both phases 3000ms, no VIP). Response decodes to BrokerReplicasInfo.
    BrokerReplicasInfo getInSyncStateData(const std::string& controllerAddr,
                                          const std::vector<std::string>& brokers);
    // 1010 per controller server (the name-server list when none given; still empty -> empty map).
    // The body is properties text; a non-SUCCESS code throws MQClientException.
    std::map<std::string, PropertyMap> getControllerConfig(
        const std::vector<std::string>& controllerServers = std::vector<std::string>(),
        int32_t timeoutMillis = -1);
    // 1009 with the properties as body text. No request is sent when the text is empty or the
    // server list is empty; otherwise every server gets the request and the last non-SUCCESS reply
    // throws MQClientException after the loop.
    void updateControllerConfig(
        const PropertyMap& properties,
        const std::vector<std::string>& controllers = std::vector<std::string>(),
        int32_t timeoutMillis = -1);
    // Two-phase: 1005 to controllerAddr first, then 1011 to the controller leader (3000ms, no VIP).
    // clusterName and brokerControllerIdsToClean are omitted from the header when empty;
    // isCleanLivingBroker and invokeTime are always sent.
    void cleanBrokerData(const std::string& controllerAddr, const std::string& clusterName,
                         const std::string& brokerName,
                         const std::string& brokerControllerIdsToClean = std::string(),
                         bool isCleanLivingBroker = false);

    // ---------------- Cold-data flow control (2001/2002/2003) ----------------
    // 2001 carries the properties as body text (no request at all when the text is empty);
    // 2002 carries the consumer group as a plain string body (skipped when empty). Both VIP-rewritten.
    // 2003 carries nothing and is NOT VIP-rewritten; an empty response body comes back as "".
    void updateColdDataFlowCtrGroupConfig(const std::string& brokerAddr, const PropertyMap& properties);
    void removeColdDataFlowCtrGroupConfig(const std::string& brokerAddr, const std::string& consumerGroup);
    std::string getColdDataFlowCtrInfo(const std::string& brokerAddr);

    // ---------------- CommitLog read-ahead (SET_COMMITLOG_READ_MODE(2004)) ----------------
    // The mode travels in a `READ_AHEAD_MODE` ext field, no body, no VIP rewrite; the verdict is
    // the response remark ("" when the broker sent none).
    std::string setCommitLogReadAheadMode(const std::string& brokerAddr, const std::string& mode);

    // ---------------- Pop rollback record export (POP_ROLLBACK(200056)) ----------------
    // No header, no body, no VIP rewrite; the caller's timeout is used verbatim (never the admin default).
    void exportPopRecords(const std::string& brokerAddr, int32_t timeoutMillis);

    // ---------------- RocksDB maintenance (354 / 355) ----------------
    // Both go to the broker address verbatim (no VIP rewrite), admin default timeout, and both
    // throw MQClientException (not MQBrokerException) on a non-SUCCESS code.
    CheckRocksdbCqWriteResult checkRocksdbCqWriteProgress(const std::string& brokerAddr,
                                                          const std::string& topic,
                                                          int64_t checkStoreTime);
    // configType entries are joined with a ';' written after every entry ("topics;" / "topics;subscriptionGroups;")
    void exportRocksDBConfigToJson(const std::string& brokerAddr,
                                   const std::vector<std::string>& configTypes);

    // ---------------- Lite topic administration (200074-200079) ----------------
    // All six go through one helper: VIP-rewritten, admin default timeout, body decoded only when
    // the broker sent one (else a null JsonValue), non-SUCCESS throws MQBrokerException.
    JsonValue getBrokerLiteInfo(const std::string& brokerAddr);
    JsonValue getParentTopicInfo(const std::string& brokerAddr, const std::string& topic);
    JsonValue getLiteTopicInfo(const std::string& brokerAddr, const std::string& parentTopic,
                               const std::string& liteTopic);
    // maxCount is a header primitive and is always sent (default 1000 when the caller leaves it unset)
    JsonValue getLiteClientInfo(const std::string& brokerAddr, const std::string& parentTopic,
                                const std::string& group, const std::string& clientId,
                                int32_t maxCount = 1000);
    // topK is a header primitive and is always sent, even as 0; liteTopic omitted when empty
    JsonValue getLiteGroupInfo(const std::string& brokerAddr, const std::string& group,
                               const std::string& liteTopic, int32_t topK);
    void triggerLiteDispatch(const std::string& brokerAddr, const std::string& group,
                             const std::string& clientId);

    // ---------------- Concurrent fan-out forms ----------------
    // All seven submit one task per target and wait for the batch with a bounded wait
    // (timeoutMillis, default the admin timeout). Task failures are logged, never propagated;
    // the result reflects whatever finished inside the window.
    // Route-less topics yield an empty success for the stats/time-span forms and a
    // TOPIC_ROUTE_INFO_NOT_EXIST failure for the others, mirroring each route probe.
    AdminToolResult<TopicStatsTable> examineTopicStatsConcurrent(const std::string& topic,
                                                                 int32_t timeoutMillis = -1);
    AdminToolResult<ConsumeStats> examineConsumeStatsConcurrent(const std::string& consumerGroup,
                                                                const std::string& topic,
                                                                int32_t timeoutMillis = -1);
    AdminToolResult<BrokerOperatorResult> deleteTopicInBrokerConcurrent(
        const std::vector<std::string>& addrs, const std::string& topic, int32_t timeoutMillis = -1);
    AdminToolResult<BrokerOperatorResult> resetOffsetNewConcurrent(const std::string& group,
                                                                   const std::string& topic,
                                                                   int64_t timestamp,
                                                                   int32_t timeoutMillis = -1);
    AdminToolResult<TopicList> queryTopicsByConsumerConcurrent(const std::string& group,
                                                               int32_t timeoutMillis = -1);
    AdminToolResult<JsonValue> queryConsumeTimeSpanConcurrent(const std::string& topic,
                                                              const std::string& group,
                                                              int32_t timeoutMillis = -1);
    // Not wrapped in the AdminToolResult envelope: fans over the consumer groups of the topic.
    std::vector<MessageTrack> messageTrackDetailConcurrent(const MessageExt& msg,
                                                           int32_t timeoutMillis = -1);

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

    // ---------------- Long-tail helpers ----------------
    // Route probe used by the fan-out forms: TOPIC_NOT_EXIST yields false (no route), any other
    // failure is rethrown untouched so the envelope classifies it.
    bool examineTopicRouteOrNull(const std::string& topic, TopicRouteData& out);
    // Shared funnel of the lite family: VIP rewrite, admin timeout, decode only when a body came
    // back, MQBrokerException otherwise.
    JsonValue invokeBrokerMethod(const std::string& addr, int32_t requestCode,
                                 const PropertyMap& extFields, int32_t timeoutMillis);
    // 29-family reset leaf for the concurrent form: SUCCESS without a body is a failure
    // (code 0 + remark), matching the fall-through of the raw switch.
    std::map<MessageQueue, int64_t> invokeBrokerToResetOffsetConcurrent(
        const std::string& brokerAddr, const std::string& topic, const std::string& group,
        int64_t timestamp, int32_t timeoutMillis);
    // Per-broker consume-stats walk used as the fallback when the group never came online.
    void resetOffsetOldOnBroker(const std::string& brokerAddr, const QueueData& queueData,
                                const std::string& group, const std::string& topic,
                                int64_t timestamp, bool force);
    // One queue of that walk: timestamp -1 means max offset; the update runs when forced or when
    // the reset target is not ahead of the consumer.
    void resetOffsetOldOnQueue(const std::string& brokerAddr, const std::string& group,
                               const MessageQueue& mq, const OffsetWrapper& wrapper,
                               int64_t timestamp, bool force);
    // Per-group verdict shared by messageTrackDetail and messageTrackDetailConcurrent;
    // mapBroadcast selects the broadcast verdict the single form applies (the concurrent form leaves it).
    MessageTrack trackMessageGroup(const MessageExt& msg, const std::string& group,
                                   bool mapBroadcast);
    // Bounded fan-out: one thread per entry, a bounded wait on the batch, then every thread is
    // joined before the caller reads shared containers.
    void runBoundedFanOut(size_t count, int32_t timeoutMillis,
                          const std::function<void(size_t)>& task);
    // Envelope wrapper: exceptions become failure codes (remoting -> REMOTING_ERROR,
    // client TOPIC_NOT_EXIST -> TOPIC_ROUTE_INFO_NOT_EXIST, other client -> MQ_CLIENT_ERROR,
    // broker and anything else -> MQ_BROKER_ERROR).
    template <typename T>
    AdminToolResult<T> adminToolExecute(const std::function<AdminToolResult<T>()>& handler) const;

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
    int32_t maxPageSizeInGetMetadata_ = 2000;

    std::unique_ptr<MQClientInstance> mqClient_;
    // ACL hook, bound to the MQClientInstance transport at start()
    std::shared_ptr<RPCHook> rpcHook_;
    bool started_ = false;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_ADMIN_H
