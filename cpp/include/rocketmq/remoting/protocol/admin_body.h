// ⚠ Key pitfall: the Map keys of these classes are **MessageQueue**, and broker-side fastjson inlines the key as a JSON
// object, producing **invalid JSON**, for example:
//   {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:{...}}}
// So the parser in json.cpp must tolerate "an object as key" (keeping the raw JSON text as the key name),
// and parseMessageQueueKey() then restores it into a MessageQueue.
// Field names and structures were all confirmed by measurements with the broker-side serialization probe.
#ifndef ROCKETMQ_REMOTING_PROTOCOL_ADMIN_BODY_H
#define ROCKETMQ_REMOTING_PROTOCOL_ADMIN_BODY_H

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

// ---------------------------------------------------------------- MessageQueue as map key

// Serialize a MessageQueue into a fastjson2-style inlined object key (keys in alphabetical order, as fastjson2 does)
std::string messageQueueKey(const MessageQueue& mq);

// Restore a fastjson2-written inlined MessageQueue object key back into a MessageQueue.
// Returning false means this key is not an inlined object (an ordinary string key such as "0" or "G1").
bool parseMessageQueueKey(const std::string& key, MessageQueue& out);

// Generic: parse a map keyed by MessageQueue, with raw JsonValue values
std::map<MessageQueue, JsonValue> decodeMessageQueueMap(const JsonValue& raw);

struct TopicOffset {
    int64_t minOffset = 0;
    int64_t maxOffset = 0;
    int64_t lastUpdateTimestamp = 0;

    JsonValue toJson() const;
    static TopicOffset fromJson(const JsonValue& v);
};

struct TopicStatsTable {
    std::map<MessageQueue, TopicOffset> offsetTable;
    double topicPutTps = 0.0;

    // Sum of maxOffset over all queues (the convergence check used most often on the admin side)
    int64_t totalMaxOffset() const;

    JsonValue toJson() const;
    static TopicStatsTable fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, TopicStatsTable& out);
};

struct OffsetWrapper {
    int64_t brokerOffset = 0;
    int64_t consumerOffset = 0;
    int64_t lastTimestamp = 0;
    int64_t pullOffset = 0;

    // Lag in protocol terms: brokerOffset - consumerOffset
    int64_t lag() const { return brokerOffset - consumerOffset; }

    JsonValue toJson() const;
    static OffsetWrapper fromJson(const JsonValue& v);
};

struct ConsumeStats {
    std::map<MessageQueue, OffsetWrapper> offsetTable;
    double consumeTps = 0.0;

    int64_t totalLag() const;

    JsonValue toJson() const;
    static ConsumeStats fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ConsumeStats& out);
};

struct TopicConfigSerializeWrapper {
    std::map<std::string, TopicConfig> topicConfigTable;
    JsonValue dataVersion;  // Pass-through

    JsonValue toJson() const;
    static TopicConfigSerializeWrapper fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, TopicConfigSerializeWrapper& out);
};

struct ConsumeQueueData {
    int64_t physicOffset = 0;
    int64_t physicSize = 0;
    int64_t tagsCode = 0;
    bool eval = false;
    std::string extendDataJson;
    bool hasExtendDataJson = false;
    std::string bitMap;
    bool hasBitMap = false;
    std::string msg;
    bool hasMsg = false;

    JsonValue toJson() const;  // null fields are not serialized (broker-side convention)
    static ConsumeQueueData fromJson(const JsonValue& v);
};

struct QueryConsumeQueueResponseBody {
    JsonValue subscriptionData;  // Pass-through (null means absent)
    std::string filterData;
    bool hasFilterData = false;
    std::vector<ConsumeQueueData> queueData;
    bool hasQueueData = false;
    int64_t maxQueueIndex = 0;
    int64_t minQueueIndex = 0;

    JsonValue toJson() const;
    static QueryConsumeQueueResponseBody fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, QueryConsumeQueueResponseBody& out);
};

// ---------------------------------------------------------------- AUTH user / ACL management
// The enum-valued fields are carried as fixed wire strings; the spelling and casing below
// are byte-exact parts of the protocol.
struct UserType {
    static constexpr const char* SUPER = "Super";
    static constexpr const char* NORMAL = "Normal";
};

struct UserStatus {
    static constexpr const char* ENABLE = "enable";
    static constexpr const char* DISABLE = "disable";
};

struct AclPolicyType {
    static constexpr const char* CUSTOM = "Custom";
    static constexpr const char* DEFAULT = "Default";
};

struct AclDecision {
    static constexpr const char* ALLOW = "Allow";
    static constexpr const char* DENY = "Deny";
};

// Unset fields are omitted from the JSON (never emitted as null); the has* flags carry that.
struct UserInfo {
    std::string username;
    std::string password;
    bool hasPassword = false;
    std::string userType;
    bool hasUserType = false;
    std::string userStatus;
    bool hasUserStatus = false;

    JsonValue toJson() const;
    static UserInfo fromJson(const JsonValue& v);
    // Response of the list operation: a JSON array of user objects; a non-array decodes to empty
    static std::vector<UserInfo> listFromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, UserInfo& out);
};

struct AclEntry {
    std::string resource;
    std::vector<std::string> actions;
    bool hasActions = false;
    std::vector<std::string> sourceIps;
    bool hasSourceIps = false;
    std::string decision;
    bool hasDecision = false;

    JsonValue toJson() const;
    static AclEntry fromJson(const JsonValue& v);
};

struct AclPolicy {
    std::string policyType;
    bool hasPolicyType = false;
    std::vector<AclEntry> entries;

    JsonValue toJson() const;
    static AclPolicy fromJson(const JsonValue& v);
};

struct AclInfo {
    std::string subject;
    std::vector<AclPolicy> policies;

    JsonValue toJson() const;
    static AclInfo fromJson(const JsonValue& v);
    // Response of the list operation: a JSON array of acl objects; a non-array decodes to empty
    static std::vector<AclInfo> listFromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, AclInfo& out);
};

// ---------------------------------------------------------------- Controller replicas / sync state
// Unset fields are omitted from the JSON (never emitted as null); the has* flags carry that.
struct ReplicaIdentity {
    std::string brokerName;
    int64_t brokerId = 0;
    std::string brokerAddress;
    bool hasBrokerAddress = false;
    bool alive = false;
    bool hasAlive = false;

    JsonValue toJson() const;
    static ReplicaIdentity fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ReplicaIdentity& out);
};

struct ReplicasInfo {
    int64_t masterBrokerId = 0;
    bool hasMasterBrokerId = false;
    std::string masterAddress;
    bool hasMasterAddress = false;
    int32_t masterEpoch = 0;
    int32_t syncStateSetEpoch = 0;
    std::vector<ReplicaIdentity> inSyncReplicas;
    std::vector<ReplicaIdentity> notInSyncReplicas;

    JsonValue toJson() const;
    static ReplicasInfo fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ReplicasInfo& out);
};

// Response body of CONTROLLER_GET_SYNC_STATE_DATA(1006): {replicasInfoTable: {brokerName: ReplicasInfo}}
struct BrokerReplicasInfo {
    std::map<std::string, ReplicasInfo> replicasInfoTable;

    JsonValue toJson() const;
    static BrokerReplicasInfo fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, BrokerReplicasInfo& out);
};

// Response body of CHECK_ROCKSDB_CQ_WRITE_PROGRESS(354)
struct CheckRocksdbCqWriteStatus {
    static constexpr int32_t CHECK_OK = 0;
    static constexpr int32_t CHECK_NOT_OK = 1;
    static constexpr int32_t CHECK_IN_PROGRESS = 2;
    static constexpr int32_t CHECK_ERROR = 3;
};

struct CheckRocksdbCqWriteResult {
    std::string checkResult;
    int32_t checkStatus = 0;

    JsonValue toJson() const;
    static CheckRocksdbCqWriteResult fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, CheckRocksdbCqWriteResult& out);
};

// Aggregate result of the fan-out delete/reset operations
struct BrokerOperatorResult {
    std::vector<std::string> successList;
    std::vector<std::string> failureList;

    JsonValue toJson() const;
    static BrokerOperatorResult fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, BrokerOperatorResult& out);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_ADMIN_BODY_H
