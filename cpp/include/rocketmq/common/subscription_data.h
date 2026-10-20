// Purpose: the SubscriptionData carried in a consumer's heartbeat registration to the broker; the broker
// performs TAG filtering from tagsSet / codeSet; subString is the "||" separated tag expression.
#ifndef ROCKETMQ_COMMON_SUBSCRIPTION_DATA_H
#define ROCKETMQ_COMMON_SUBSCRIPTION_DATA_H

#include <cstdint>
#include <set>
#include <string>

#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

struct ExpressionType {
    static const char* const TAG;           // "TAG"
    static const char* const SQL92;         // "SQL92"
    static const char* const CLASS_FILTER;  // "CLASS_FILTER"
};

// Subscription selector (tag / SQL92 expression)
struct MessageSelector {
    std::string type = ExpressionType::TAG;
    std::string expression = "*";

    static MessageSelector byTag(const std::string& tag) {
        return MessageSelector{ExpressionType::TAG, tag};
    }
    static MessageSelector bySql(const std::string& sql) {
        return MessageSelector{ExpressionType::SQL92, sql};
    }
};

class SubscriptionData {
public:
    bool classFilterMode = false;
    std::string topic;
    std::string subString;
    std::set<std::string> tagsSet;
    std::set<int64_t> codeSet;
    int64_t subVersion = 0;  // Defaults to the current millisecond timestamp (see the constructors)
    std::string expressionType = ExpressionType::TAG;
    std::string filterClassSource;

    SubscriptionData();
    SubscriptionData(const std::string& topic, const std::string& subString);

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

    bool operator==(const SubscriptionData& o) const;
    bool operator!=(const SubscriptionData& o) const { return !(*this == o); }

    int compareTo(const SubscriptionData& o) const;

    int hashCode() const;

    std::string toString() const;
};

struct FilterAPI {
    static SubscriptionData buildSubscriptionData(const std::string& topic,
                                                  const std::string& subString);
    // Selector-aware build, the same shape for every subscription kind a caller can ask for:
    //   - TAG: exactly the tag builder above (the expression is parsed into tagsSet / codeSet);
    //   - anything else (SQL92 and friends): the expression goes on the wire **verbatim**, so
    //     tagsSet / codeSet stay empty -- a selector expression is not a tag list and parsing it as one
    //     would make the client-side re-filter drop every message the broker did not filter.
    // The subscription keeps the version it was built with; the caller decides which version reaches
    // the wire (see the pull paths).
    static SubscriptionData buildSubscriptionData(const std::string& topic,
                                                  const std::string& expression,
                                                  const std::string& expressionType);
    // ---- What a PULL_MESSAGE request carries for a subscription ----
    // The expression: a tag subscription sends the tag builder's normalised form (an empty expression
    // is the "*" wildcard), a selector subscription sends its expression **verbatim**.
    static std::string pullExpression(const SubscriptionData& sub);
    // The version: see the rule above -- 0 for TAG, the subscription's own version for a selector.
    static int64_t pullSubVersionOf(const SubscriptionData& sub);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_SUBSCRIPTION_DATA_H
