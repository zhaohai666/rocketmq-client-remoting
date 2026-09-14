// SubscriptionData / FilterAPI 的实现。
#include "rocketmq/common/subscription_data.h"

#include <cstdint>
#include <string>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/util_all.h"

namespace rocketmq {

const char* const ExpressionType::TAG = "TAG";
const char* const ExpressionType::SQL92 = "SQL92";
const char* const ExpressionType::CLASS_FILTER = "CLASS_FILTER";

namespace {

// Java Set.hashCode() = 各元素 hashCode 之和（32 位有符号回绕）
int32_t setHashCode(const std::set<std::string>& s) {
    int64_t h = 0;
    for (const std::string& e : s) {
        h += javaStringHash(e);
        h &= 0xFFFFFFFFL;
    }
    return static_cast<int32_t>(h);
}

int32_t setHashCode(const std::set<int64_t>& s) {
    int64_t h = 0;
    for (int64_t e : s) {
        h += e;
        h &= 0xFFFFFFFFL;
    }
    return static_cast<int32_t>(h);
}

// Java set 序列化为 JSON 数组
JsonValue setToJson(const std::set<std::string>& s) {
    JsonValue arr = JsonValue::makeArray();
    for (const std::string& e : s) {
        arr.pushArray(JsonValue::makeString(e));
    }
    return arr;
}

JsonValue setToJson(const std::set<int64_t>& s) {
    JsonValue arr = JsonValue::makeArray();
    for (int64_t e : s) {
        arr.pushArray(JsonValue::makeInt(e));
    }
    return arr;
}

}  // namespace

SubscriptionData::SubscriptionData() : subVersion(UtilAll::currentTimeMillis()) {}

SubscriptionData::SubscriptionData(const std::string& topic, const std::string& subString)
    : topic(topic), subString(subString), subVersion(UtilAll::currentTimeMillis()) {}

JsonValue SubscriptionData::toJson() const {
    // 键按字母序，与 fastjson2 输出一致
    JsonValue o = JsonValue::makeObject();
    o.set("classFilterMode", JsonValue::makeBool(classFilterMode));
    o.set("codeSet", setToJson(codeSet));
    o.set("expressionType", JsonValue::makeString(expressionType));
    o.set("subString", JsonValue::makeString(subString));
    o.set("subVersion", JsonValue::makeInt(subVersion));
    o.set("tagsSet", setToJson(tagsSet));
    o.set("topic", JsonValue::makeString(topic));
    return o;
}

SubscriptionData SubscriptionData::fromJson(const JsonValue& v) {
    SubscriptionData sd;
    if (!v.isObject()) {
        return sd;
    }
    bool b = false;
    if (v.tryGetBool("classFilterMode", b)) {
        sd.classFilterMode = b;
    }
    std::string s;
    if (v.tryGetString("expressionType", s)) {
        sd.expressionType = s;
    }
    if (v.tryGetString("subString", s)) {
        sd.subString = s;
    }
    if (v.tryGetString("topic", s)) {
        sd.topic = s;
    }
    int64_t n = 0;
    if (v.tryGetInt("subVersion", n)) {
        sd.subVersion = n;
    }
    const JsonValue* tags = v.find("tagsSet");
    if (tags != nullptr && tags->isArray()) {
        for (size_t i = 0; i < tags->size(); ++i) {
            sd.tagsSet.insert(tags->at(i).stringValue());
        }
    }
    const JsonValue* codes = v.find("codeSet");
    if (codes != nullptr && codes->isArray()) {
        for (size_t i = 0; i < codes->size(); ++i) {
            sd.codeSet.insert(codes->at(i).intValue());
        }
    }
    return sd;
}

bool SubscriptionData::operator==(const SubscriptionData& o) const {
    return classFilterMode == o.classFilterMode && codeSet == o.codeSet
        && subString == o.subString && subVersion == o.subVersion && tagsSet == o.tagsSet
        && topic == o.topic && expressionType == o.expressionType;
}

int SubscriptionData::compareTo(const SubscriptionData& o) const {
    const std::string a = topic + "@" + subString;
    const std::string b = o.topic + "@" + o.subString;
    if (a == b) {
        return 0;
    }
    return a < b ? -1 : 1;
}

int SubscriptionData::hashCode() const {
    int64_t result = 1;
    result = 31 * result + (classFilterMode ? 1231 : 1237);
    result = 31 * result + setHashCode(codeSet);
    result = 31 * result + javaStringHash(subString);
    result = 31 * result + setHashCode(tagsSet);
    result = 31 * result + javaStringHash(topic);
    result = 31 * result + javaStringHash(expressionType);
    result &= 0xFFFFFFFFL;
    return static_cast<int32_t>(result);
}

std::string SubscriptionData::toString() const {
    return "SubscriptionData [classFilterMode=" + std::string(classFilterMode ? "true" : "false")
         + ", topic=" + topic + ", subString=" + subString + ", tagsSet=" + setToJson(tagsSet).dump()
         + ", codeSet=" + setToJson(codeSet).dump() + ", subVersion=" + std::to_string(subVersion)
         + ", expressionType=" + expressionType + "]";
}

SubscriptionData FilterAPI::buildSubscriptionData(const std::string& topic,
                                                  const std::string& subString) {
    SubscriptionData sub(topic, subString);
    // Java: null / "*" / 全空白 -> tagsSet = {"*"}
    if (UtilAll::isBlank(subString) || subString == "*") {
        sub.tagsSet.insert("*");
    } else {
        // 按 "||" 切分，trim 后丢弃空片段
        size_t pos = 0;
        while (pos <= subString.size()) {
            size_t next = subString.find("||", pos);
            std::string tag = (next == std::string::npos)
                                  ? subString.substr(pos)
                                  : subString.substr(pos, next - pos);
            // trim
            size_t b = tag.find_first_not_of(" \t\r\n");
            size_t e = tag.find_last_not_of(" \t\r\n");
            tag = (b == std::string::npos) ? std::string() : tag.substr(b, e - b + 1);
            if (!tag.empty()) {
                sub.tagsSet.insert(tag);
            }
            if (next == std::string::npos) {
                break;
            }
            pos = next + 2;
        }
    }
    return sub;
}

}  // namespace rocketmq
