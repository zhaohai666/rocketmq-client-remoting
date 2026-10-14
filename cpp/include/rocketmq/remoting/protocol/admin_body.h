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

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_ADMIN_BODY_H
