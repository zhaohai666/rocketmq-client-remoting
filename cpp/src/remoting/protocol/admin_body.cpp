// 管理端响应体实现（对应 Python remoting/protocol/admin_body.py）。
#include "rocketmq/remoting/protocol/admin_body.h"

#include <string>

#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

namespace {

bool getBool(const JsonValue& v, const char* key, bool def) {
    const JsonValue* p = v.find(key);
    if (p == nullptr) return def;
    if (p->isBool()) return p->boolValue();
    if (p->isNumber()) return p->intValue() != 0;
    return def;
}

int64_t getInt(const JsonValue& v, const char* key, int64_t def) {
    const JsonValue* p = v.find(key);
    return (p != nullptr && p->isNumber()) ? p->intValue() : def;
}

std::string getStr(const JsonValue& v, const char* key, const std::string& def) {
    std::string s;
    return v.tryGetString(key, s) ? s : def;
}

double getDouble(const JsonValue& v, const char* key, double def) {
    const JsonValue* p = v.find(key);
    return (p != nullptr && p->isNumber()) ? p->doubleValue() : def;
}

JsonValue rawSub(const JsonValue& v, const char* key) {
    const JsonValue* p = v.find(key);
    return p != nullptr ? *p : JsonValue::makeNull();
}

// 把 <MessageQueue, T> 编码成 fastjson2 风格的 offsetTable 对象
template <typename T>
JsonValue encodeMqKeyedMap(const std::map<MessageQueue, T>& m) {
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : m) {
        table.set(messageQueueKey(kv.first), kv.second.toJson());
    }
    return table;
}

}  // namespace

// ---------------------------------------------------------------- MessageQueue 作为 map 键
std::string messageQueueKey(const MessageQueue& mq) {
    // 键按字母序（fastjson2 行为）：brokerName, queueId, topic
    JsonValue v = JsonValue::makeObject();
    v.set("brokerName", JsonValue::makeString(mq.brokerName));
    v.set("queueId", JsonValue::makeInt(mq.queueId));
    v.set("topic", JsonValue::makeString(mq.topic));
    return v.dump();
}

bool parseMessageQueueKey(const std::string& key, MessageQueue& out) {
    // 先看是不是内联对象键（以 '{' 起头）；否则是普通字符串键，直接不接受
    size_t b = 0;
    while (b < key.size() && (key[b] == ' ' || key[b] == '\t')) ++b;
    if (b >= key.size() || key[b] != '{') return false;

    JsonValue v;
    if (!jsonParse(key, v, nullptr) || !v.isObject()) return false;
    out.topic = getStr(v, "topic", "");
    out.brokerName = getStr(v, "brokerName", "");
    out.queueId = static_cast<int32_t>(getInt(v, "queueId", 0));
    return true;
}

std::map<MessageQueue, JsonValue> decodeMessageQueueMap(const JsonValue& raw) {
    std::map<MessageQueue, JsonValue> result;
    if (!raw.isObject()) return result;
    for (const auto& kv : raw.objectItems()) {
        MessageQueue mq;
        if (parseMessageQueueKey(kv.first, mq)) {
            result[mq] = kv.second;
        }
    }
    return result;
}

// ---------------------------------------------------------------- TopicOffset
JsonValue TopicOffset::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("minOffset", JsonValue::makeInt(minOffset));
    v.set("maxOffset", JsonValue::makeInt(maxOffset));
    v.set("lastUpdateTimestamp", JsonValue::makeInt(lastUpdateTimestamp));
    return v;
}

TopicOffset TopicOffset::fromJson(const JsonValue& v) {
    TopicOffset o;
    o.minOffset = getInt(v, "minOffset", 0);
    o.maxOffset = getInt(v, "maxOffset", 0);
    o.lastUpdateTimestamp = getInt(v, "lastUpdateTimestamp", 0);
    return o;
}

// ---------------------------------------------------------------- TopicStatsTable
int64_t TopicStatsTable::totalMaxOffset() const {
    int64_t sum = 0;
    for (const auto& kv : offsetTable) sum += kv.second.maxOffset;
    return sum;
}

JsonValue TopicStatsTable::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("offsetTable", encodeMqKeyedMap(offsetTable));
    v.set("topicPutTps", JsonValue::makeDouble(topicPutTps));
    return v;
}

TopicStatsTable TopicStatsTable::fromJson(const JsonValue& v) {
    TopicStatsTable t;
    for (const auto& kv : decodeMessageQueueMap(rawSub(v, "offsetTable"))) {
        t.offsetTable[kv.first] = TopicOffset::fromJson(kv.second);
    }
    t.topicPutTps = getDouble(v, "topicPutTps", 0.0);
    return t;
}

Bytes TopicStatsTable::encode() const { return RemotingSerializable::encode(toJson()); }

bool TopicStatsTable::decode(const Bytes& data, TopicStatsTable& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- OffsetWrapper
JsonValue OffsetWrapper::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("brokerOffset", JsonValue::makeInt(brokerOffset));
    v.set("consumerOffset", JsonValue::makeInt(consumerOffset));
    v.set("lastTimestamp", JsonValue::makeInt(lastTimestamp));
    v.set("pullOffset", JsonValue::makeInt(pullOffset));
    return v;
}

OffsetWrapper OffsetWrapper::fromJson(const JsonValue& v) {
    OffsetWrapper o;
    o.brokerOffset = getInt(v, "brokerOffset", 0);
    o.consumerOffset = getInt(v, "consumerOffset", 0);
    o.lastTimestamp = getInt(v, "lastTimestamp", 0);
    o.pullOffset = getInt(v, "pullOffset", 0);
    return o;
}

// ---------------------------------------------------------------- ConsumeStats
int64_t ConsumeStats::totalLag() const {
    int64_t sum = 0;
    for (const auto& kv : offsetTable) sum += kv.second.lag();
    return sum;
}

JsonValue ConsumeStats::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("offsetTable", encodeMqKeyedMap(offsetTable));
    v.set("consumeTps", JsonValue::makeDouble(consumeTps));
    return v;
}

ConsumeStats ConsumeStats::fromJson(const JsonValue& v) {
    ConsumeStats c;
    for (const auto& kv : decodeMessageQueueMap(rawSub(v, "offsetTable"))) {
        c.offsetTable[kv.first] = OffsetWrapper::fromJson(kv.second);
    }
    c.consumeTps = getDouble(v, "consumeTps", 0.0);
    return c;
}

Bytes ConsumeStats::encode() const { return RemotingSerializable::encode(toJson()); }

bool ConsumeStats::decode(const Bytes& data, ConsumeStats& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- TopicConfigSerializeWrapper
JsonValue TopicConfigSerializeWrapper::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("dataVersion", dataVersion.isNull() ? JsonValue::makeObject() : dataVersion);
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : topicConfigTable) {
        table.set(kv.first, kv.second.toJson());
    }
    v.set("topicConfigTable", table);
    return v;
}

TopicConfigSerializeWrapper TopicConfigSerializeWrapper::fromJson(const JsonValue& v) {
    TopicConfigSerializeWrapper w;
    const JsonValue* table = v.find("topicConfigTable");
    if (table != nullptr && table->isObject()) {
        for (const auto& kv : table->objectItems()) {
            w.topicConfigTable[kv.first] = TopicConfig::fromJson(kv.second);
        }
    }
    w.dataVersion = rawSub(v, "dataVersion");
    return w;
}

Bytes TopicConfigSerializeWrapper::encode() const {
    return RemotingSerializable::encode(toJson());
}

bool TopicConfigSerializeWrapper::decode(const Bytes& data, TopicConfigSerializeWrapper& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ConsumeQueueData
JsonValue ConsumeQueueData::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("physicOffset", JsonValue::makeInt(physicOffset));
    v.set("physicSize", JsonValue::makeInt(physicSize));
    v.set("tagsCode", JsonValue::makeInt(tagsCode));
    v.set("eval", JsonValue::makeBool(eval));
    if (hasBitMap) v.set("bitMap", JsonValue::makeString(bitMap));
    else v.set("bitMap", JsonValue::makeNull());
    // 同 Java：null 字段不序列化（这里用 has* 显式表达 null）
    if (hasExtendDataJson) v.set("extendDataJson", JsonValue::makeString(extendDataJson));
    if (hasMsg) v.set("msg", JsonValue::makeString(msg));
    return v;
}

ConsumeQueueData ConsumeQueueData::fromJson(const JsonValue& v) {
    ConsumeQueueData d;
    d.physicOffset = getInt(v, "physicOffset", 0);
    d.physicSize = getInt(v, "physicSize", 0);
    d.tagsCode = getInt(v, "tagsCode", 0);
    d.eval = getBool(v, "eval", false);
    std::string tmp;
    if (v.tryGetString("extendDataJson", tmp)) {
        d.extendDataJson = tmp;
        d.hasExtendDataJson = true;
    }
    if (v.tryGetString("bitMap", tmp)) {
        d.bitMap = tmp;
        d.hasBitMap = true;
    }
    if (v.tryGetString("msg", tmp)) {
        d.msg = tmp;
        d.hasMsg = true;
    }
    return d;
}

// ---------------------------------------------------------------- QueryConsumeQueueResponseBody
JsonValue QueryConsumeQueueResponseBody::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("maxQueueIndex", JsonValue::makeInt(maxQueueIndex));
    v.set("minQueueIndex", JsonValue::makeInt(minQueueIndex));
    if (!subscriptionData.isNull()) v.set("subscriptionData", subscriptionData);
    if (hasFilterData) v.set("filterData", JsonValue::makeString(filterData));
    if (hasQueueData) {
        JsonValue arr = JsonValue::makeArray();
        for (const auto& q : queueData) arr.pushArray(q.toJson());
        v.set("queueData", arr);
    }
    return v;
}

QueryConsumeQueueResponseBody QueryConsumeQueueResponseBody::fromJson(const JsonValue& v) {
    QueryConsumeQueueResponseBody b;
    b.subscriptionData = rawSub(v, "subscriptionData");
    std::string tmp;
    if (v.tryGetString("filterData", tmp)) {
        b.filterData = tmp;
        b.hasFilterData = true;
    }
    const JsonValue* raw = v.find("queueData");
    if (raw != nullptr && raw->isArray()) {
        b.hasQueueData = true;
        for (size_t i = 0; i < raw->size(); ++i) {
            b.queueData.push_back(ConsumeQueueData::fromJson(raw->at(i)));
        }
    }
    b.maxQueueIndex = getInt(v, "maxQueueIndex", 0);
    b.minQueueIndex = getInt(v, "minQueueIndex", 0);
    return b;
}

Bytes QueryConsumeQueueResponseBody::encode() const {
    return RemotingSerializable::encode(toJson());
}

bool QueryConsumeQueueResponseBody::decode(const Bytes& data,
                                          QueryConsumeQueueResponseBody& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

}  // namespace rocketmq
