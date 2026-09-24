// 订阅后置 + 立即心跳（#73）离线单测 —— 不需要集群。
//
// 对齐基准（Java 5.5.1 `DefaultMQPushConsumerImpl`）：
//   * `subscribe(topic, subExpression):1265-1275`（以及 class-filter / MessageSelector 两个
//     重载）都是 put 进 `subscriptionInner` 之后 `if (mQClientFactory != null)
//     mQClientFactory.sendHeartbeatToAllBrokerWithLock();` —— **没有**「started 之后禁止
//     订阅」这道闸门，心跳是同步立即发的。
//   * `unsubscribe(topic):1317-1319` 只 remove，**不**发心跳。
//
// 为什么必须锁死：真机上「新订阅没推给 broker」是静默的 —— 只表现为
// QUERY_TOPIC_CONSUME_BY_WHO(300) 查不到本组、新 topic 分不到队列，客户端一声不响。
//
// 离线能测到哪一步：这里让消费者对着一个**连不上**的 name server 启动（路由拉不到 ⇒
// knownBrokerAddrs 为空 ⇒ 心跳一台都发不出去），因此只能证明
//   ① start() 之后 subscribe 不再抛「already started」；
//   ② 新订阅立刻进活订阅表（`subscribedTopics()`，也就是心跳与 rebalance 读的那张表）；
//   ③ unsubscribe 照旧只删表项。
// 报文层面「broker 真收到了带新订阅的心跳」由 examples/live_subscribe.cpp 在真机验证
// （broker 侧 topicGroupTable 的 300 号查询是唯一可信观测）。
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

namespace {

int gChecks = 0;
int gFails = 0;

void check(bool ok, const std::string& name, const std::string& detail = std::string()) {
    ++gChecks;
    if (!ok) {
        ++gFails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

// 一个永远连不上的地址：路由拉取当场 ECONNREFUSED（catch 住只记 debug 日志），
// knownBrokerAddrs() 因此为空 —— 消费者能正常 start()，但一个包都发不出去。
const char* kDeadNamesrv = "127.0.0.1:1";

class NoopListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                            ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

std::unique_ptr<DefaultMQPushConsumer> startedConsumer(const std::string& group) {
    auto c = std::make_unique<DefaultMQPushConsumer>(group);
    c->setNamesrvAddr(kDeadNamesrv);
    c->subscribe("BaseTopic", "*");
    c->setMessageListener(std::make_shared<NoopListener>());
    c->start();
    return c;
}

bool hasTopic(const std::vector<std::string>& topics, const std::string& topic) {
    return std::find(topics.begin(), topics.end(), topic) != topics.end();
}

// ① 后置 subscribe：Java 允许，且立即进活订阅表（旧实现在这里抛 already started）。
void testSubscribeAfterStartIsAllowed() {
    auto c = startedConsumer("GID_SubAfterStartUnit");
    check(c->isStarted(), "consumer is started");

    bool threw = false;
    std::string err;
    try {
        c->subscribe("LateTopic", "TagA||TagB");
    } catch (const std::exception& e) {
        threw = true;
        err = e.what();
    }
    check(!threw, "subscribe after start does not throw", threw ? err : "");
    check(hasTopic(c->subscribedTopics(), "LateTopic"),
          "late subscription lands in the live subscription table");

    // MessageSelector 重载同样允许（Java :1289-1303）。
    MessageSelector sql;
    sql.type = ExpressionType::SQL92;
    sql.expression = "a > 1";
    threw = false;
    try {
        c->subscribe("LateSqlTopic", sql);
    } catch (const std::exception& e) {
        threw = true;
        err = e.what();
    }
    check(!threw, "selector subscribe after start does not throw", threw ? err : "");
    check(hasTopic(c->subscribedTopics(), "LateSqlTopic"),
          "late selector subscription lands in the live subscription table");

    // 零 broker：心跳一台都发不出去，计数器不动（这条同时是「上面没偷偷发包」的对照）。
    check(c->heartbeatCount() == 0,
          "no broker known → heartbeat count stays 0",
          std::to_string(c->heartbeatCount()));

    c->shutdown();
    check(!c->isStarted(), "consumer stopped");
}

// ② unsubscribe 不发心跳（Java :1317-1319）：计数器不动，表项消失。
void testUnsubscribeOnlyDropsTheEntry() {
    auto c = startedConsumer("GID_SubUnsubUnit");
    c->subscribe("LateTopic", "*");
    const int64_t before = c->heartbeatCount();

    c->unsubscribe("LateTopic");

    check(c->heartbeatCount() == before, "unsubscribe does not push a heartbeat");
    check(!hasTopic(c->subscribedTopics(), "LateTopic"), "unsubscribed topic is gone");
    check(hasTopic(c->subscribedTopics(), "BaseTopic"), "other subscriptions untouched");
    c->shutdown();
}

// ③ 启动前订阅照旧（回归底线）：老用法不受影响。
void testSubscribeBeforeStartStillWorks() {
    DefaultMQPushConsumer c("GID_SubBeforeStartUnit");
    c.subscribe("T_A", "*");
    check(hasTopic(c.subscribedTopics(), "T_A"), "pre-start subscribe recorded");
    check(c.heartbeatCount() == 0, "pre-start subscribe sends nothing");
}

}  // namespace

int main() {
    testSubscribeBeforeStartStillWorks();
    testSubscribeAfterStartIsAllowed();
    testUnsubscribeOnlyDropsTheEntry();

    std::printf("%s: %d checks, %d failed\n", gFails == 0 ? "PASS" : "FAIL", gChecks, gFails);
    return gFails == 0 ? 0 : 1;
}
