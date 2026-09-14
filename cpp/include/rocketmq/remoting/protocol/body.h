// 公共 body（对应 org.apache.rocketmq.remoting.protocol.body.* 常用部分）：
//   KVTable / TopicList / ClusterInfo / Connection / ConsumerConnection /
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

// 对应 org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo
struct ConsumerRunningInfo {
    PropertyMap properties;
    JsonValue subscriptionSet;  // 透传（List<SubscriptionData>）
    JsonValue mqTable;          // 透传（Map<MessageQueue, ProcessQueueInfo>）
    std::string jstack;
    bool hasJstack = false;

    JsonValue toJson() const;
    static ConsumerRunningInfo fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, ConsumerRunningInfo& out);
};

// 对应 org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList
// （GET_BROKER_CONSUME_STATS 的响应：statsList 为 <topic, List<ConsumeStats>> 的列表）
struct ConsumeStatsList {
    JsonValue statsList;  // 透传
    std::string brokerAddr;
    bool hasBrokerAddr = false;

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

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_BODY_H
