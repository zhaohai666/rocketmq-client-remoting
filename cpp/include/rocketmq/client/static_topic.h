#ifndef ROCKETMQ_CLIENT_STATIC_TOPIC_H
#define ROCKETMQ_CLIENT_STATIC_TOPIC_H

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/route.h"

namespace rocketmq {
namespace static_topic {

// Reserved constants for static topics on the broker side: the global mapping scope, the mock broker-name prefix
// for logical queues, and the sentinel name meaning "this logical queue has no real mapping on this broker".
inline const char* kMetadataScopeGlobal = "__global__";
inline const char* kLogicalQueueMockBrokerPrefix = "__syslo__";
inline const char* kLogicalQueueMockNotExist = "__syslo__none__";

inline bool isLogicalQueueMockBrokerName(const std::string& brokerName) {
    return brokerName.rfind(kLogicalQueueMockBrokerPrefix, 0) == 0;
}

// getMockBrokerName: for "__global__" (the scope itself) strip the first two characters; other scopes get the prefix as-is.
inline std::string getMockBrokerName(const std::string& scope) {
    if (scope == kMetadataScopeGlobal) {
        return std::string(kLogicalQueueMockBrokerPrefix) + scope.substr(2);
    }
    return std::string(kLogicalQueueMockBrokerPrefix) + scope;
}

struct TopicQueueMappingInfo {
    std::string scope = kMetadataScopeGlobal;
    int32_t totalQueues = 0;
    std::string bname;
    int64_t epoch = 0;
    // globalId -> physical queueId. The physical id takes no part in endpoint derivation and is kept only for troubleshooting.
    std::map<int32_t, int32_t> currIdMap;
};

bool parseTopicQueueMappingInfo(const JsonValue& raw, TopicQueueMappingInfo& out);

// topicRouteData2EndpointsForStaticTopic: build the logical queue set
// MessageQueue(topic, mockBrokerName(scope), globalId) -> the real broker name carrying it
// (a logical id with no carrier inside the total range points at "__syslo__none__").
// An empty mapping (non static topic) returns false and leaves out untouched.
bool topicRouteData2EndpointsForStaticTopic(
    const std::string& topic, const TopicRouteData& route,
    std::map<MessageQueue, std::string>& out);

}  // namespace static_topic
}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_STATIC_TOPIC_H
