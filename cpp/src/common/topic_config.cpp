// TopicConfig 实现（对应 Python common/topic_config.py）。
#include "rocketmq/common/topic_config.h"

#include <string>

#include "rocketmq/common/sysflag.h"
#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

std::string TopicConfig::permString() const { return PermName::permToString(perm); }

JsonValue TopicConfig::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("topicName", JsonValue::makeString(topicName));
    v.set("readQueueNums", JsonValue::makeInt(readQueueNums));
    v.set("writeQueueNums", JsonValue::makeInt(writeQueueNums));
    v.set("perm", JsonValue::makeInt(perm));
    v.set("topicFilterType", JsonValue::makeString(topicFilterType));
    v.set("topicSysFlag", JsonValue::makeInt(topicSysFlag));
    v.set("order", JsonValue::makeBool(order));
    // Java：attributes 没有 serialize=false，因此**始终序列化**（即使是空 map）
    JsonValue attrs = JsonValue::makeObject();
    for (const auto& kv : attributes) {
        attrs.set(kv.first, JsonValue::makeString(kv.second));
    }
    v.set("attributes", attrs);
    return v;
}

TopicConfig TopicConfig::fromJson(const JsonValue& v) {
    TopicConfig c;
    c.topicName = v.get("topicName").stringValue();
    const JsonValue& rq = v.get("readQueueNums");
    c.readQueueNums = rq.isNumber() ? static_cast<int32_t>(rq.intValue()) : DEFAULT_READ_QUEUE_NUMS;
    const JsonValue& wq = v.get("writeQueueNums");
    c.writeQueueNums = wq.isNumber() ? static_cast<int32_t>(wq.intValue()) : DEFAULT_WRITE_QUEUE_NUMS;
    const JsonValue& pm = v.get("perm");
    c.perm = pm.isNumber() ? static_cast<int32_t>(pm.intValue()) : DEFAULT_PERM;
    std::string ft;
    c.topicFilterType = v.tryGetString("topicFilterType", ft) ? ft : TopicFilterType::SINGLE_TAG;
    const JsonValue& sf = v.get("topicSysFlag");
    c.topicSysFlag = sf.isNumber() ? static_cast<int32_t>(sf.intValue()) : 0;
    const JsonValue* ord = v.find("order");
    c.order = ord != nullptr && ord->isBool() ? ord->boolValue() : false;
    const JsonValue* attrs = v.find("attributes");
    if (attrs != nullptr && attrs->isObject()) {
        for (const auto& kv : attrs->objectItems()) {
            if (kv.second.isString()) c.attributes[kv.first] = kv.second.stringValue();
        }
    }
    return c;
}

Bytes TopicConfig::encode() const { return RemotingSerializable::encode(toJson()); }

bool TopicConfig::decode(const Bytes& data, TopicConfig& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

std::string TopicConfig::toString() const {
    return "TopicConfig [topicName=" + topicName
         + ", readQueueNums=" + std::to_string(readQueueNums)
         + ", writeQueueNums=" + std::to_string(writeQueueNums)
         + ", perm=" + permString()
         + ", topicFilterType=" + topicFilterType
         + ", topicSysFlag=" + std::to_string(topicSysFlag)
         + ", order=" + (order ? "true" : "false") + "]";
}

}  // namespace rocketmq
