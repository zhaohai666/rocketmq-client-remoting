// 静态主题（单元化主题）消费者侧路由支持，移植自 Java 客户端：
//   - org.apache.rocketmq.remoting.rpc.ClientMetadata
//     .topicRouteData2EndpointsForStaticTopic
//   - org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils
//     .getMockBrokerName
//   - org.apache.rocketmq.client.impl.factory.MQClientInstance
//     .getBrokerNameFromMessageQueue（见 MQClientInstance 同名方法）
//
// wire 事实（不得"简化"）：
//   - 路由解码侧把 topicQueueMappingByBroker 保持为不透明 JsonValue；
//     类型化视图在这里解析，不动协议层透传。
//   - TopicQueueMappingInfo.scope 缺省为 MixAll.METADATA_SCOPE_GLOBAL
//     （"__global__"）；global scope 的 mock broker 名会剥掉开头两个下划线
//     （"__syslo__global__"）。
//   - Java 的 maxTotalNumOfEpoch 读了但从不赋值（恒 -1），所以
//     epoch >= maxTotalNumOfEpoch 守卫恒真，maxTotalNums 就是该 scope 的
//     totalQueues 最大值 —— 这个 quirk 照抄。
//   - 同 epoch 的后到条目覆盖先到条目（Java `oldEpoch <= epoch`）。
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

// MixAll.METADATA_SCOPE_GLOBAL / LOGICAL_QUEUE_MOCK_BROKER_PREFIX /
// LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST。
inline const char* kMetadataScopeGlobal = "__global__";
inline const char* kLogicalQueueMockBrokerPrefix = "__syslo__";
inline const char* kLogicalQueueMockNotExist = "__syslo__none__";

// Java MixAll::isLogicalQueueMockBrokerName（前缀判断）。
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

// topicQueueMappingByBroker[brokerName] 单条目的类型化视图
// （Java TopicQueueMappingInfo）。
struct TopicQueueMappingInfo {
    std::string scope = kMetadataScopeGlobal;
    int32_t totalQueues = 0;
    std::string bname;
    int64_t epoch = 0;
    // globalId -> 物理 queueId。物理 id 不参与端点推导，只保留给排查用。
    std::map<int32_t, int32_t> currIdMap;
};

// 从路由解码侧保留下来的原始 json 对象里解析类型化视图。
// 畸形条目返回 false —— Java 遇到会 NPE；能抢救的部分映射好过整个崩溃。
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
