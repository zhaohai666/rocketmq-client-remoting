// 订阅数据（对应 org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData
// 与 org.apache.rocketmq.common.filter.ExpressionType / FilterAPI）。
//
// 用途：Consumer 注册到 broker 的心跳里携带 SubscriptionData，broker 依据
// tagsSet / codeSet 做 TAG 过滤；subString 为 "||" 分隔的 tag 表达式。
//
// JSON 字段（与 fastjson2 序列化结果一致，键按字母序）：
//   classFilterMode | codeSet | expressionType | subString | subVersion | tagsSet | topic
// 注意：Java 的 filterClassSource 标注了 @JSONField(serialize = false)，**不参与**序列化。
#ifndef ROCKETMQ_COMMON_SUBSCRIPTION_DATA_H
#define ROCKETMQ_COMMON_SUBSCRIPTION_DATA_H

#include <cstdint>
#include <set>
#include <string>

#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

// org.apache.rocketmq.common.filter.ExpressionType
struct ExpressionType {
    static const char* const TAG;           // "TAG"
    static const char* const SQL92;         // "SQL92"
    static const char* const CLASS_FILTER;  // "CLASS_FILTER"
};

class SubscriptionData {
public:
    bool classFilterMode = false;
    std::string topic;
    std::string subString;
    std::set<std::string> tagsSet;
    std::set<int64_t> codeSet;
    int64_t subVersion = 0;  // 默认取当前毫秒时间戳（见构造函数）
    std::string expressionType = ExpressionType::TAG;
    // 仅本地使用，不参与 JSON（对齐 Java @JSONField(serialize = false)）
    std::string filterClassSource;

    SubscriptionData();
    SubscriptionData(const std::string& topic, const std::string& subString);

    // ---- Java 风格访问器 ----
    const std::string& getTopic() const { return topic; }
    void setTopic(const std::string& t) { topic = t; }

    const std::string& getSubString() const { return subString; }
    void setSubString(const std::string& s) { subString = s; }

    const std::set<std::string>& getTagsSet() const { return tagsSet; }
    void setTagsSet(const std::set<std::string>& s) { tagsSet = s; }

    const std::set<int64_t>& getCodeSet() const { return codeSet; }
    void setCodeSet(const std::set<int64_t>& s) { codeSet = s; }

    int64_t getSubVersion() const { return subVersion; }
    void setSubVersion(int64_t v) { subVersion = v; }

    const std::string& getExpressionType() const { return expressionType; }
    void setExpressionType(const std::string& t) { expressionType = t; }

    const std::string& getFilterClassSource() const { return filterClassSource; }
    void setFilterClassSource(const std::string& s) { filterClassSource = s; }

    bool isClassFilterMode() const { return classFilterMode; }
    void setClassFilterMode(bool m) { classFilterMode = m; }

    // ---- JSON ----
    JsonValue toJson() const;
    static SubscriptionData fromJson(const JsonValue& v);

    // Java equals 语义：比较 classFilterMode/codeSet/subString/subVersion/tagsSet/
    // topic/expressionType（**含** subVersion；不含 filterClassSource）。
    bool operator==(const SubscriptionData& o) const;
    bool operator!=(const SubscriptionData& o) const { return !(*this == o); }

    // Java compareTo：按 "topic@subString" 字符串序
    int compareTo(const SubscriptionData& o) const;

    int hashCode() const;

    std::string toString() const;
};

// org.apache.rocketmq.common.filter.FilterAPI
struct FilterAPI {
    // subString 为空 / "*" / 全空白 => tagsSet = {"*"}；否则按 "||" 切分并 trim，
    // 空片段丢弃（与 Java FilterAPI.buildSubscriptionData 一致）。
    static SubscriptionData buildSubscriptionData(const std::string& topic,
                                                  const std::string& subString);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_SUBSCRIPTION_DATA_H
