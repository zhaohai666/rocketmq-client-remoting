// Subscribe-after-start plus immediate heartbeat (#73) offline unit test -- no cluster needed.
//
//
// Why it must be pinned down: "the new subscription was never pushed to the broker" is silent on a
// real cluster -- no error at all, only QUERY_TOPIC_CONSUME_BY_WHO(300) missing the group and no queue.
//
// How far offline testing reaches: the consumer starts against a name server it **cannot reach** (no
// route => knownBrokerAddrs empty => not one heartbeat can go out), so only these are provable:
//   (1) subscribe() after start() no longer throws "already started";
//   (2) the new subscription enters the live table subscribedTopics(), which heartbeat/rebalance read;
//   (3) unsubscribe still only deletes the table entry.
// That the broker really received a heartbeat carrying the new subscription is verified on a live
// cluster by examples/live_subscribe.cpp (the 300 query on the broker-side topicGroupTable is the only proof).
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

// An address that can never be reached: route fetching gets ECONNREFUSED at once (caught, logged at
// debug only), so knownBrokerAddrs() stays empty -- start() works normally but no packet goes out.
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

    // Zero brokers: no heartbeat can be sent so the counter stays put (control for "nothing sent silently").
    check(c->heartbeatCount() == 0,
          "no broker known → heartbeat count stays 0",
          std::to_string(c->heartbeatCount()));

    c->shutdown();
    check(!c->isStarted(), "consumer stopped");
}

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

// Subscribing before start() behaves exactly as before (regression floor): the old usage is untouched.
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
