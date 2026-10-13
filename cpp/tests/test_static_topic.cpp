// 静态主题路由单测 —— 不需要集群。
//
// 覆盖：mock broker 名推导、类型化映射解析的缺省值、端点推导
// （未承载队列 -> "__syslo__none__" 哨兵、scope 不合并、epoch 围栏）、
// 以及 MQClientInstance::getBrokerNameFromMessageQueue 的重写/透传。
#include <cstdio>
#include <string>

#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/static_topic.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

// friend 访问器（见 mq_client.h）：离线单测播种端点表。
// 必须定义在 rocketmq 命名空间作用域 —— friend 声明绑定的是外层命名空间的名字，
// 匿名命名空间里的是另一个类型。
namespace rocketmq {
struct MQClientInstanceTestAccess {
    static void seedEndPoints(MQClientInstance& inst, const std::string& topic,
                              std::map<MessageQueue, std::string> endpoints) {
        inst.topicEndPointsTable_[topic] = std::move(endpoints);
    }
};
}  // namespace rocketmq

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s%s%s\n", name.c_str(), detail.empty() ? "" : ": ",
                    detail.c_str());
    }
}

JsonValue rawMapping(const std::string& scope, int totalQueues, const std::string& bname,
                     int64_t epoch, const JsonValue& currIdMap) {
    JsonValue v = JsonValue::makeObject();
    v.set("scope", JsonValue::makeString(scope));
    v.set("totalQueues", JsonValue::makeInt(totalQueues));
    v.set("bname", JsonValue::makeString(bname));
    v.set("epoch", JsonValue::makeInt(epoch));
    v.set("dirty", JsonValue::makeBool(false));
    v.set("currIdMap", currIdMap);
    return v;
}

JsonValue currIdMapOf(std::initializer_list<std::pair<const char*, int>> pairs) {
    JsonValue v = JsonValue::makeObject();
    for (const auto& p : pairs) {
        v.set(p.first, JsonValue::makeInt(p.second));
    }
    return v;
}

TopicRouteData mappingRoute(
    std::initializer_list<std::pair<const char*, JsonValue>> entries) {
    TopicRouteData route;
    JsonValue mapping = JsonValue::makeObject();
    for (const auto& e : entries) {
        mapping.set(e.first, e.second);
    }
    route.setTopicQueueMappingByBroker(mapping);
    return route;
}

void testMockBrokerName() {
    expect(static_topic::getMockBrokerName("__global__") == "__syslo__global__",
           "mock name strips global underscores");
    expect(static_topic::getMockBrokerName("zone-a") == "__syslo__zone-a",
           "mock name prefixes other scopes verbatim");
    expect(static_topic::isLogicalQueueMockBrokerName("__syslo__global__"),
           "mock prefix detected");
    expect(!static_topic::isLogicalQueueMockBrokerName("broker-a"),
           "real broker name is not mock");
}

void testParseDefaults() {
    JsonValue raw = JsonValue::makeObject();
    raw.set("totalQueues", JsonValue::makeInt(2));
    raw.set("bname", JsonValue::makeString("b1"));
    raw.set("epoch", JsonValue::makeInt(3));
    static_topic::TopicQueueMappingInfo info;
    expect(static_topic::parseTopicQueueMappingInfo(raw, info), "parse ok");
    expect(info.scope == "__global__", "scope defaults to __global__", info.scope);
    expect(info.totalQueues == 2 && info.bname == "b1" && info.epoch == 3, "fields kept");
    expect(!static_topic::parseTopicQueueMappingInfo(JsonValue::makeString("x"), info),
           "non-object raw rejected");
}

void testEndpointsSingleScope() {
    TopicRouteData route = mappingRoute({
        {"broker-a", rawMapping("__global__", 4, "broker-a", 0,
                                currIdMapOf({{"0", 0}, {"1", 1}, {"2", 2}}))},
    });
    std::map<MessageQueue, std::string> endpoints;
    expect(static_topic::topicRouteData2EndpointsForStaticTopic("StaticTopic", route,
                                                                endpoints),
           "static route yields endpoints");
    expect(endpoints.size() == 4, "hosted + unhosted = 4",
           std::to_string(endpoints.size()));
    for (int32_t id = 0; id < 3; ++id) {
        MessageQueue mq("StaticTopic", "__syslo__global__", id);
        expect(endpoints[mq] == "broker-a", "global id hosted", std::to_string(id));
    }
    MessageQueue unhosted("StaticTopic", "__syslo__global__", 3);
    expect(endpoints[unhosted] == "__syslo__none__", "unhosted -> NOT_EXIST sentinel");
    // 空映射（非静态主题）返回 false。
    std::map<MessageQueue, std::string> none;
    expect(!static_topic::topicRouteData2EndpointsForStaticTopic("T", TopicRouteData(),
                                                                 none),
           "empty mapping yields no endpoints");
}

void testScopesAndEpochFence() {
    TopicRouteData route = mappingRoute({
        {"broker-old", rawMapping("__global__", 1, "broker-old", 3, currIdMapOf({{"0", 0}}))},
        {"broker-new", rawMapping("__global__", 1, "broker-new", 7, currIdMapOf({{"0", 0}}))},
        {"broker-b", rawMapping("zone-x", 1, "broker-b", 9, currIdMapOf({{"0", 0}}))},
    });
    std::map<MessageQueue, std::string> endpoints;
    expect(static_topic::topicRouteData2EndpointsForStaticTopic("T", route, endpoints),
           "route yields endpoints");
    expect(endpoints[MessageQueue("T", "__syslo__global__", 0)] == "broker-new",
           "higher epoch fences");
    expect(endpoints[MessageQueue("T", "__syslo__zone-x", 0)] == "broker-b",
           "zone scope kept apart");
    expect(endpoints.size() == 2, "scopes do not merge",
           std::to_string(endpoints.size()));
}

void testInstanceRewrite() {
    MQClientInstance inst("static-test@DEFAULT", {"127.0.0.1:9876"});
    // 直接种端点表（离线没有 namesrv；落库逻辑由纯函数用例覆盖）。
    std::map<MessageQueue, std::string> endpoints = {
        {MessageQueue("StaticTopic", "__syslo__global__", 0), "broker-a"},
        {MessageQueue("StaticTopic", "__syslo__global__", 1), "broker-b"},
    };
    MQClientInstanceTestAccess::seedEndPoints(inst, "StaticTopic", std::move(endpoints));
    expect(inst.getBrokerNameFromMessageQueue(
               MessageQueue("StaticTopic", "__syslo__global__", 0)) == "broker-a",
           "global 0 rewritten");
    expect(inst.getBrokerNameFromMessageQueue(
               MessageQueue("StaticTopic", "__syslo__global__", 1)) == "broker-b",
           "global 1 rewritten");
    // 表里没有的队列 / 非静态主题原样透传自己的 broker 名。
    expect(inst.getBrokerNameFromMessageQueue(
               MessageQueue("StaticTopic", "__syslo__global__", 99)) == "__syslo__global__",
           "unknown queue passes mock name through");
    expect(inst.getBrokerNameFromMessageQueue(MessageQueue("Plain", "broker-a", 0)) ==
               "broker-a",
           "non-static topic passes through");
}

}  // namespace

int main() {
    testMockBrokerName();
    testParseDefaults();
    testEndpointsSingleScope();
    testScopesAndEpochFence();
    testInstanceRewrite();
    std::printf("static_topic: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
