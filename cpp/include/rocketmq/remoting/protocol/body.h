// 复合且管理端不需要解释内容的字段（subscriptionTable / mqTable / statsList 等）
// 一律用 JsonValue 透传，避免过度建模导致字段丢失。
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

// 只被 CHECK_CLIENT_CONFIG(46) 用到：broker 侧 ClientManageProcessor#checkClientConfig
// 拿 clientId/group 记日志，真正被校验的只有 subscriptionData 的 expressionType 与
// subString。协议里定义了 namespace 字段但发送端不填，这里同样保留字段而不写值
// （未置位时不参与 JSON，与 broker 侧不序列化 null 字段的约定一致）。
struct CheckClientRequestBody {
    std::string clientId;
    std::string group;
    SubscriptionData subscriptionData;
    std::string clientNamespace;  // 协议字段名 namespace（C++ 里 namespace 是关键字）
    bool hasNamespace = false;

    JsonValue toJson() const;
    static CheckClientRequestBody fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, CheckClientRequestBody& out);
};

struct ClusterInfo {
    std::map<std::string, BrokerData> brokerAddrTable;
    std::map<std::string, std::vector<std::string>> clusterAddrTable;

    // 收集所有 broker 地址（去重，按 brokerName、brokerId 顺序）
    std::vector<std::string> getBrokerAddrs() const;
    // 按集群名列出其下所有 broker 地址（clusterName 为空则返回全部）
    std::vector<std::string> getBrokerAddrsOfCluster(const std::string& clusterName) const;

    JsonValue toJson() const;
    static ClusterInfo fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ClusterInfo& out);
};

// ConsumerConnection.connectionSet 的单个连接元素
struct Connection {
    std::string clientId;
    std::string clientAddr;  // 协议字段名 clientAddr
    std::string language;
    int32_t version = 0;

    JsonValue toJson() const;
    static Connection fromJson(const JsonValue& v);
};

struct ConsumerConnection {
    std::vector<Connection> connectionSet;
    JsonValue subscriptionTable;  // 透传（Map<String, SubscriptionData>）
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
    // ConsumerRunningInfo.properties 的固定键（键名由协议约定）
    static constexpr const char* PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR";
    static constexpr const char* PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE";
    static constexpr const char* PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY";  // 协议键名无下划线
    static constexpr const char* PROP_CONSUME_TYPE = "PROP_CONSUME_TYPE";
    static constexpr const char* PROP_CLIENT_VERSION = "PROP_CLIENT_VERSION";
    static constexpr const char* PROP_CONSUMER_START_TIMESTAMP = "PROP_CONSUMER_START_TIMESTAMP";

    PropertyMap properties;
    JsonValue subscriptionSet;  // 透传（List<SubscriptionData>）
    JsonValue mqTable;          // 透传（Map<MessageQueue, ProcessQueueInfo>）
    JsonValue mqPopTable;       // 透传（Map<MessageQueue, ProcessQueueInfo>，POP 模式）
    JsonValue statusTable;      // 透传（Map<String, ConsumeStatus>）
    JsonValue userConsumerInfo; // 透传（Map<String, String>）
    std::string jstack;
    bool hasJstack = false;

    JsonValue toJson() const;
    static ConsumerRunningInfo fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ConsumerRunningInfo& out);
};

// ⚠ JSON 键是协议字段名 consumeStatsList，不是 statsList：写错时真机响应会
// 解析成空列表，看着像「这个 broker 没有积压」。
struct ConsumeStatsList {
    JsonValue statsList;  // 透传
    std::string brokerAddr;
    bool hasBrokerAddr = false;
    // int64_t（不是 long long）：在 LP64 上 int64_t==long 与 long long 是不同类型，
    // `tryGetInt(key, int64_t&)` 无法绑定 long long& 左值（CI 的 gcc 直接报错）。
    int64_t totalDiff = 0;
    int64_t totalInflightDiff = 0;

    JsonValue toJson() const;
    static ConsumeStatsList fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ConsumeStatsList& out);
};

// ⚠ 协议字段 offsetTable 是 Map<MessageQueue, Long>，**不是** topic->queueId->offset
// 的嵌套 map；写错时真实 broker 响应解析不出来。
// broker 侧 fastjson 会把 MessageQueue 键内联成 JSON 对象，故用 admin_body 的键工具解析。
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
