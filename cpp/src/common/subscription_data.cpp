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
    // Java `FilterAPI.buildSubscriptionData` 逐句对齐（探针 /tmp/subprobe/SubProbe.java 与
    // BlankProbe.java 实测过四种输入）：
    //
    //   StringUtils.isEmpty(subString) || subString.equals("*")  → setSubString("*") 后**直接 return**
    //       ⇒ tagsSet 与 codeSet 都保持空
    //   否则按 "||" 做 Java String.split（**丢弃末尾空串**），然后 trim、丢空片段，
    //       每个 tag 同时进 tagsSet 与 codeSet（codeSet 存 tag.hashCode()）。
    //       Java-split 结果数组长度为 0 时（即 subString 形如 "||"、"||||"）抛 Exception。
    //
    // ⚠ 两处曾有的偏差（都会污染心跳、并让客户端二次 tag 过滤失效）：
    //   ① 给 "*" 塞 tagsSet={"*"} —— Java 里 tagsSet 非空才是"客户端二次 tag 过滤"的开关
    //      （PullAPIWrapper.processPullResult 的 `!tagsSet.isEmpty()`），塞了 "*" 会让订阅
    //      全量时把所有正常 tag 的消息客户端自己过滤掉；
    //   ② 从不填 codeSet —— 它是 broker 侧按 tag 哈希过滤的依据
    //      （ExpressionMessageFilter.isMatchedByConsumeQueue 走 codeSet.contains）。
    // 另注：判空必须用"空串"而不是"全空白"—— Java StringUtils.isEmpty 只认 null/""，
    // 纯空白（如 "   "）会走进 split 分支，结果 tagsSet 空但 subString 原样保留。
    if (subString.empty() || subString == "*") {
        sub.subString = "*";
        return sub;
    }
    // Java String.split("\\|\\|")：先全切，再丢弃**末尾**空串
    std::vector<std::string> rawTags;
    size_t pos = 0;
    while (true) {
        size_t next = subString.find("||", pos);
        if (next == std::string::npos) {
            rawTags.push_back(subString.substr(pos));
            break;
        }
        rawTags.push_back(subString.substr(pos, next - pos));
        pos = next + 2;
    }
    while (!rawTags.empty() && rawTags.back().empty()) {
        rawTags.pop_back();
    }
    if (rawTags.empty()) {
        // Java: throw new Exception("subString split error")
        throw std::invalid_argument("subString split error");
    }
    for (std::string& tag : rawTags) {
        size_t b = tag.find_first_not_of(" \t\r\n");
        size_t e = tag.find_last_not_of(" \t\r\n");
        tag = (b == std::string::npos) ? std::string() : tag.substr(b, e - b + 1);
        if (!tag.empty()) {
            sub.tagsSet.insert(tag);
            sub.codeSet.insert(static_cast<int64_t>(javaStringHash(tag)));
        }
    }
    return sub;
}

}  // namespace rocketmq
