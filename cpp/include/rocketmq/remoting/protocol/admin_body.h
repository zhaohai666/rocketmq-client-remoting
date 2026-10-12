// ⚠ 关键坑：这几个类的 Map 键是 **MessageQueue**，broker 侧 fastjson 会把键直接内联成 JSON 对象，
// 产出**非法 JSON**，例如：
//   {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:{...}}}
// 因此 json.cpp 的解析器必须容忍"对象作为键"（保留原始 JSON 文本作键名），
// 再由 parseMessageQueueKey() 还原成 MessageQueue。
// 字段名与结构均由 broker 侧序列化探针实测确认。
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

// ---------------------------------------------------------------- MessageQueue 作为 map 键

// 把 MessageQueue 序列化成 fastjson2 风格的内联对象键（键按字母序，与 fastjson2 一致）
std::string messageQueueKey(const MessageQueue& mq);

// 把 fastjson2 写出的 MessageQueue 内联对象键还原成 MessageQueue。
// 返回 false 表示这个键不是内联对象（例如 "0"、"G1" 这类普通字符串键）。
bool parseMessageQueueKey(const std::string& key, MessageQueue& out);

// 通用：解析以 MessageQueue 为键的 map，值为原始 JsonValue
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

    // 各队列 maxOffset 之和（管理端最常用的收敛判断）
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

    // lag 协议语义：brokerOffset - consumerOffset
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
    JsonValue dataVersion;  // 透传

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

    JsonValue toJson() const;  // null 字段不序列化（broker 侧约定）
    static ConsumeQueueData fromJson(const JsonValue& v);
};

struct QueryConsumeQueueResponseBody {
    JsonValue subscriptionData;  // 透传（null 表示不出现）
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

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_ADMIN_BODY_H
