// 公共 body（对应 org.apache.rocketmq.remoting.protocol.body.* 常用部分）：
//   KVTable / TopicList / GetConsumerListByGroupResponseBody / CheckClientRequestBody /
//   ClusterInfo / Connection / ConsumerConnection /
//   ProducerConnection / ConsumerRunningInfo / ConsumeStatsList / ResetOffsetBody
//
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

// 对应 org.apache.rocketmq.remoting.protocol.body.KVTable
struct KVTable {
    PropertyMap table;

    JsonValue toJson() const;
    static KVTable fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, KVTable& out);
};

// 对应 org.apache.rocketmq.remoting.protocol.body.TopicList
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

// 对应 org.apache.rocketmq.remoting.protocol.body.GetConsumerListByGroupResponseBody
struct GetConsumerListByGroupResponseBody {
    std::vector<std::string> consumerIdList;

    JsonValue toJson() const;
    static GetConsumerListByGroupResponseBody fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, GetConsumerListByGroupResponseBody& out);
};

// 对应 org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody
//
// 只被 CHECK_CLIENT_CONFIG(46) 用到：broker 拿 clientId/group 记日志，真正被校验的只有
// subscriptionData 的 expressionType 与 subString（Java
// ClientManageProcessor#checkClientConfig）。namespace 字段 Java 5.5.1 里存在但发送端
// 不填，这里同样保留字段而不写值（未置位时不参与 JSON，对齐 fastjson 不序列化 null）。
struct CheckClientRequestBody {
    std::string clientId;
    std::string group;
    SubscriptionData subscriptionData;
    std::string clientNamespace;  // Java 字段名 namespace（C++ 里 namespace 是关键字）
    bool hasNamespace = false;

    JsonValue toJson() const;
    static CheckClientRequestBody fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, CheckClientRequestBody& out);
};

// 对应 org.apache.rocketmq.remoting.protocol.body.ClusterInfo
// Java 的 brokerAddrTable 是 Map<String, BrokerData>，这里直接复用 route.h 的 BrokerData。
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

// 对应 org.apache.rocketmq.client.common.Connection
struct Connection {
    std::string clientId;
    std::string clientAddr;  // Java 字段名 clientAddr
    std::string language;
    int32_t version = 0;

    JsonValue toJson() const;
    static Connection fromJson(const JsonValue& v);
};

// 对应 org.apache.rocketmq.remoting.protocol.body.ConsumerConnection
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

// 对应 org.apache.rocketmq.remoting.protocol.body.ProducerConnection
struct ProducerConnection {
    std::vector<Connection> connectionSet;

    JsonValue toJson() const;
    static ProducerConnection fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ProducerConnection& out);
};

// 对应 org.apache.rocketmq.remoting.protocol.body.ConsumeStatus
// （ConsumerRunningInfo.statusTable 的值，字段全部来自 ConsumerStatsManager 的快照）
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

// 对应 org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo
struct ConsumerRunningInfo {
    // Java ConsumerRunningInfo 里 properties 的固定键（常量名照抄 Java）
    static constexpr const char* PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR";
    static constexpr const char* PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE";
    static constexpr const char* PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY";  // Java 常量名无下划线
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

// 对应 org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList
// （GET_BROKER_CONSUME_STATS 的响应：consumeStatsList 为
//   <groupName, List<ConsumeStats>> 的列表）
//
// ⚠ JSON 键是 Java 字段名 consumeStatsList，不是 statsList：写错时真机响应会
// 解析成空列表，看着像「这个 broker 没有积压」。
struct ConsumeStatsList {
    JsonValue statsList;  // 透传
    std::string brokerAddr;
    bool hasBrokerAddr = false;
    long long totalDiff = 0;
    long long totalInflightDiff = 0;

    JsonValue toJson() const;
    static ConsumeStatsList fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ConsumeStatsList& out);
};

// 对应 org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody
//
// ⚠ Java 字段是 Map<MessageQueue, Long> offsetTable，**不是** topic->queueId->offset
// 的嵌套 map（早期 Python 实现写错了，真实 broker 响应解析不出来）。
// fastjson2 会把 MessageQueue 键内联成 JSON 对象，故用 admin_body 的键工具解析。
struct ResetOffsetBody {
    std::map<MessageQueue, int64_t> offsetTable;

    JsonValue toJson() const;
    static ResetOffsetBody fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ResetOffsetBody& out);
};

// 对应 org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody
// （GET_CONSUMER_STATUS_FROM_CLIENT(221) 的应答体）。messageQueueTable 的键是
// MessageQueue（fastjson2 内联对象键）；consumerTable 是 Java 保留的废弃字段
// （clientId -> 位点表），本客户端不填，序列化时按 Java 形状带空对象。
struct GetConsumerStatusBody {
    std::map<MessageQueue, int64_t> messageQueueTable;

    JsonValue toJson() const;
    Bytes encode() const;
};

// 对应 org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult
// （CONSUME_MESSAGE_DIRECTLY(309) 的应答体）。字段全是标量——唯一不需要处理
// MessageQueue 内联键的 body。consumeResult 取 CMResult 常量：
// CR_SUCCESS / CR_LATER / CR_ROLLBACK / CR_COMMIT / CR_THROW_EXCEPTION / CR_RETURN_NULL。
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
