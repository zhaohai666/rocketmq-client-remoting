// Composite fields whose content the admin side does not need to interpret (subscriptionTable / mqTable / statsList, ...)
// are always passed through as JsonValue, so over-modeling cannot drop fields.
#ifndef ROCKETMQ_REMOTING_PROTOCOL_BODY_H
#define ROCKETMQ_REMOTING_PROTOCOL_BODY_H

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/common/types.h"
#include "rocketmq/remoting/protocol/admin_body.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/route.h"

namespace rocketmq {

struct KVTable {
    PropertyMap table;

    JsonValue toJson() const;
    static KVTable fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, KVTable& out);
};

struct TopicList {
    std::vector<std::string> topicList;
    std::string brokerAddr;
    bool hasBrokerAddr = false;

    JsonValue toJson() const;
    static TopicList fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, TopicList& out);

    bool contains(const std::string& topic) const;
};

struct GetConsumerListByGroupResponseBody {
    std::vector<std::string> consumerIdList;

    JsonValue toJson() const;
    static GetConsumerListByGroupResponseBody fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, GetConsumerListByGroupResponseBody& out);
};

// Used only by CHECK_CLIENT_CONFIG(46): the broker's ClientManageProcessor#checkClientConfig
// logs clientId/group, and what actually gets validated is only the expressionType and
// subString of subscriptionData. The protocol defines a namespace field but the sender leaves it unset, so
// the field is kept here without a value as well (an unset field takes no part in the JSON, matching the broker's convention of not serializing null fields).
struct CheckClientRequestBody {
    std::string clientId;
    std::string group;
    SubscriptionData subscriptionData;
    std::string clientNamespace;  // Protocol field name namespace (namespace is a keyword in C++)
    bool hasNamespace = false;

    JsonValue toJson() const;
    static CheckClientRequestBody fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, CheckClientRequestBody& out);
};

struct ClusterInfo {
    std::map<std::string, BrokerData> brokerAddrTable;
    std::map<std::string, std::vector<std::string>> clusterAddrTable;

    // Collect every broker address (deduplicated, ordered by brokerName then brokerId)
    std::vector<std::string> getBrokerAddrs() const;
    // List all broker addresses under a cluster name (returns everything when clusterName is empty)
    std::vector<std::string> getBrokerAddrsOfCluster(const std::string& clusterName) const;

    JsonValue toJson() const;
    static ClusterInfo fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ClusterInfo& out);
};

// A single connection element of ConsumerConnection.connectionSet
struct Connection {
    std::string clientId;
    std::string clientAddr;  // Protocol field name clientAddr
    std::string language;
    int32_t version = 0;

    JsonValue toJson() const;
    static Connection fromJson(const JsonValue& v);
};

struct ConsumerConnection {
    std::vector<Connection> connectionSet;
    JsonValue subscriptionTable;  // Pass-through (Map<String, SubscriptionData>)
    std::string consumeType;
    std::string messageModel;
    std::string consumeFromWhere;

    JsonValue toJson() const;
    static ConsumerConnection fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ConsumerConnection& out);
};

struct ProducerConnection {
    std::vector<Connection> connectionSet;

    JsonValue toJson() const;
    static ProducerConnection fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ProducerConnection& out);
};

struct ConsumeStatus {
    double pullRT = 0.0;
    double pullTPS = 0.0;
    double consumeRT = 0.0;
    double consumeOKTPS = 0.0;
    double consumeFailedTPS = 0.0;
    int64_t consumeFailedMsgs = 0;

    JsonValue toJson() const;
    static ConsumeStatus fromJson(const JsonValue& v);
};

struct ConsumerRunningInfo {
    // Fixed keys of ConsumerRunningInfo.properties (key names fixed by the protocol)
    static constexpr const char* PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR";
    static constexpr const char* PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE";
    static constexpr const char* PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY";  // The protocol key has no underscore
    static constexpr const char* PROP_CONSUME_TYPE = "PROP_CONSUME_TYPE";
    static constexpr const char* PROP_CLIENT_VERSION = "PROP_CLIENT_VERSION";
    static constexpr const char* PROP_CONSUMER_START_TIMESTAMP = "PROP_CONSUMER_START_TIMESTAMP";

    PropertyMap properties;
    JsonValue subscriptionSet;  // Pass-through (List<SubscriptionData>)
    JsonValue mqTable;          // Pass-through (Map<MessageQueue, ProcessQueueInfo>)
    JsonValue mqPopTable;       // Pass-through (Map<MessageQueue, ProcessQueueInfo>, POP mode)
    JsonValue statusTable;      // Pass-through (Map<String, ConsumeStatus>)
    JsonValue userConsumerInfo; // Pass-through (Map<String, String>)
    std::string jstack;
    bool hasJstack = false;

    JsonValue toJson() const;
    static ConsumerRunningInfo fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ConsumerRunningInfo& out);
};

// ⚠ The JSON key is the protocol field name consumeStatsList, not statsList: a wrong key parses a real
// broker response into an empty list, which looks like "this broker has no backlog".
struct ConsumeStatsList {
    JsonValue statsList;  // Pass-through
    std::string brokerAddr;
    bool hasBrokerAddr = false;
    // int64_t (not long long): on LP64 int64_t==long is a different type from long long, so
    // `tryGetInt(key, int64_t&)` cannot bind a long long& lvalue (the CI gcc errors out directly).
    int64_t totalDiff = 0;
    int64_t totalInflightDiff = 0;

    JsonValue toJson() const;
    static ConsumeStatsList fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ConsumeStatsList& out);
};

// ⚠ The protocol field offsetTable is Map<MessageQueue, Long>, **not** a nested
// topic->queueId->offset map; a wrong shape makes a real broker response unparseable.
// The broker's fastjson inlines the MessageQueue key as a JSON object, so the key helpers of admin_body parse it.
struct ResetOffsetBody {
    std::map<MessageQueue, int64_t> offsetTable;

    JsonValue toJson() const;
    static ResetOffsetBody fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ResetOffsetBody& out);
};

struct MessageQueueForC {
    std::string topic;
    std::string brokerName;
    int32_t queueId = 0;
    int64_t offset = 0;
};

struct ResetOffsetBodyForC {
    std::vector<MessageQueueForC> offsetTable;

    JsonValue toJson() const;
    static ResetOffsetBodyForC fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ResetOffsetBodyForC& out);
};

struct GetConsumerStatusBody {
    std::map<MessageQueue, int64_t> messageQueueTable;

    JsonValue toJson() const;
    Bytes encode() const;
};

struct ConsumeMessageDirectlyResult {
    bool order = false;
    bool autoCommit = true;
    std::string consumeResult;
    std::string remark;
    int64_t spentTimeMills = 0;

    JsonValue toJson() const;
    static ConsumeMessageDirectlyResult fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ConsumeMessageDirectlyResult& out);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_BODY_H
