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

// broker 侧静态主题的保留常量：全局 mapping 作用域、逻辑队列的 mock broker 名前缀、
// 以及「该逻辑队列在本 broker 上没有真实映射」的哨兵名。
inline const char* kMetadataScopeGlobal = "__global__";
inline const char* kLogicalQueueMockBrokerPrefix = "__syslo__";
inline const char* kLogicalQueueMockNotExist = "__syslo__none__";

inline bool isLogicalQueueMockBrokerName(const std::string& brokerName) {
    return brokerName.rfind(kLogicalQueueMockBrokerPrefix, 0) == 0;
}

// getMockBrokerName："__global__"（scope 本身）剥掉头两个字符，其余 scope 原样加前缀。
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
    // globalId -> 物理 queueId。物理 id 不参与端点推导，只保留给排查用。
    std::map<int32_t, int32_t> currIdMap;
};

bool parseTopicQueueMappingInfo(const JsonValue& raw, TopicQueueMappingInfo& out);

// ClientMetadata.topicRouteData2EndpointsForStaticTopic：构建逻辑队列集
// MessageQueue(topic, mockBrokerName(scope), globalId) -> 承载它的真实 broker 名
// （total 范围内没有承载者的逻辑 id 指向 "__syslo__none__"）。
// 空映射（非静态主题）返回 false 且不动 out。
bool topicRouteData2EndpointsForStaticTopic(
    const std::string& topic, const TopicRouteData& route,
    std::map<MessageQueue, std::string>& out);

}  // namespace static_topic
}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_STATIC_TOPIC_H
