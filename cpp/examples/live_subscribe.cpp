// 后置订阅真机验证（C++ 对齐 Java `subscribe` 之后的「立即推一轮心跳」，与
// python/verify_subscribe_live.py、rust/examples/live_subscribe.rs、
// dotnet/examples/RocketMQ.Examples/LiveSubscribe.cs 一一对应）。
//
// Java `DefaultMQPushConsumerImpl.subscribe:1265-1275` 只做两件事：
// `subscriptionInner.put(...)` + `if (this.mQClientFactory != null)
// this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();` —— 允许 `start()` 之后订阅，
// 而且**同步**推一轮心跳。观测点是 broker 的 topic→group 表
// （`ConsumerManager#registerConsumer` 维护，`QUERY_TOPIC_CONSUME_BY_WHO(300)` 读取）：
// 订阅路径不推心跳的话，表里要等下一个 30s 心跳周期才出现本组。
//
//   S0 正对照：`start()` 之后基础 topic B 已登记本组（心跳链路与 300 查询本身是通的）。
//   S1 负对照：本轮**还没**订阅的 L，300 查不到本组。
//   S2 后置订阅立即生效：`subscribe(L)` 之后不睡直接查 300(L) → 本组已在表里，
//      且耗时远小于心跳周期（默认 30s）⇒ 只可能来自订阅路径那一轮同步心跳。
//   S3 后置订阅真会被消费：L 进分配集 → 发一条消息 → listener 收到。
//   S4 活订阅表：`unsubscribe(L)` 后本组订阅集立刻少掉 L。
//      （Java:1317-1319 只删表项、**不**推心跳，且 broker 的 topicGroupTable 只在整组
//      无订阅时才清 —— 所以这里不拿 broker 的表当断言。）
//
// 前置：NameServer + Broker 已起（本仓库 /tmp/rmq_rust_live/broker.conf）。
// 用法：./rmq_live_subscribe 127.0.0.1:9876
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 100) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

// Java ClientConfig#heartbeatBrokerInterval 默认 30s：登记必须远快于它。
const int64_t kHeartbeatPeriodMs = 30000;

std::string join(const std::set<std::string>& v) {
    std::string s = "[";
    for (const std::string& x : v) {
        if (s.size() > 1) s += ", ";
        s += x;
    }
    return s + "]";
}

struct BodySink {
    std::mutex mtx;
    std::vector<std::string> bodies;

    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies;
    }
};

class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(BodySink& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.bodies.push_back(std::string(m.body.begin(), m.body.end()));
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    BodySink& sink_;
};

std::string jsonVec(const std::vector<std::string>& v) {
    std::string s = "[";
    for (const std::string& x : v) {
        if (s.size() > 1) s += ", ";
        s += "\"" + x + "\"";
    }
    return s + "]";
}

bool contains(const std::set<std::string>& v, const std::string& x) { return v.count(x) > 0; }

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_subscribe <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const std::string stamp = std::to_string(nowMs() % 1000000);
    const std::string tBase = "SubBaseTopic_" + stamp;
    const std::string tLate = "SubLateTopic_" + stamp;
    const std::string group = "G_sub_after_start_" + stamp;
    const std::string pGroup = "G_sub_producer_" + stamp;

    DefaultMQAdminExt admin;
    admin.setNamesrvAddr(nsAddr);
    admin.start();
    std::string brokerAddr;
    try {
        const ClusterInfo cluster = admin.fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = cluster.getBrokerAddrs();
        if (addrs.empty()) {
            check("集群探活", false, "nameServer 无 broker 注册");
            admin.shutdown();
            return 1;
        }
        brokerAddr = addrs[0];
    } catch (const std::exception& e) {
        check("集群探活", false, std::string("fetchBrokerClusterInfo: ") + e.what());
        admin.shutdown();
        return 1;
    }
    check("集群探活", true, "broker=" + brokerAddr);

    std::shared_ptr<DefaultMQPushConsumer> consumer;
    std::shared_ptr<DefaultMQProducer> producer;
    BodySink sink;

    try {
        // 先建 topic：消费者不做默认 topic 兜底（对齐 Java），topic 不存在就拿不到路由。
        admin.createTopic(tBase, tBase, 4, 0);
        admin.createTopic(tLate, tLate, 4, 0);

        consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setInstanceName("live-subscribe-" + stamp);
        consumer->setMessageListener(std::make_shared<CollectListener>(sink));
        consumer->subscribe(tBase, "*");
        consumer->start();

        // ---------------- S0 正对照 ----------------
        const int64_t t0 = nowMs();
        const bool s0 = waitUntil(
            [&] { return contains(admin.queryTopicConsumeByWho(brokerAddr, tBase), group); }, 35000);
        check("S0-基础 topic B 已登记本组（300 查得到）", s0,
              "groupList=" + join(admin.queryTopicConsumeByWho(brokerAddr, tBase)) +
                  " elapsed=" + std::to_string(nowMs() - t0) + "ms");

        // ---------------- S1 负对照 ----------------
        const std::set<std::string> gotL0 = admin.queryTopicConsumeByWho(brokerAddr, tLate);
        check("S1-负对照：未订阅的 L 查不到本组", !contains(gotL0, group),
              "groupList=" + join(gotL0));

        // ---------------- S2 后置订阅立即生效 ----------------
        const int64_t t1 = nowMs();
        consumer->subscribe(tLate, "*");
        const int64_t subscribeMs = nowMs() - t1;
        const std::set<std::string> gotL1 = admin.queryTopicConsumeByWho(brokerAddr, tLate);
        const int64_t elapsedMs = nowMs() - t1;
        const bool registered = contains(gotL1, group);
        check("S2-后置订阅后 broker 立刻登记本组（300 查得到）", registered,
              "groupList=" + join(gotL1));
        // 心跳周期默认 30s：只有订阅路径那一轮**同步**心跳才能让登记这么快出现。
        check("S2-登记耗时远小于 30s 心跳周期（只可能是订阅路径推的）",
              registered && elapsedMs < kHeartbeatPeriodMs / 6,
              "subscribe 返回耗时=" + std::to_string(subscribeMs) +
                  "ms，查询完成耗时=" + std::to_string(elapsedMs) + "ms");

        // ---------------- S3 后置订阅真会被消费 ----------------
        const int64_t t2 = nowMs();
        std::vector<std::string> lateKeys;
        const bool assigned = waitUntil(
            [&] {
                lateKeys.clear();
                for (const std::string& k : consumer->assignedQueueKeys()) {
                    if (k.compare(0, tLate.size(), tLate) == 0) lateKeys.push_back(k);
                }
                return !lateKeys.empty();
            },
            45000);
        check("S3-新 topic L 进入本实例分配集（rebalance 生效）", assigned,
              "assigned=" + jsonVec(lateKeys) +
                  " elapsed=" + std::to_string(nowMs() - t2) + "ms");

        producer = std::make_shared<DefaultMQProducer>(pGroup);
        producer->setNamesrvAddr(nsAddr);
        producer->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer->send(Message(tLate, Bytes{'l', 'a', 't', 'e', '-', 's', 'u', 'b', 's', 'c',
                                            'r', 'i', 'b', 'e', '-', 'm', 'e'}));
        const bool consumed = waitUntil(
            [&] {
                for (const std::string& b : sink.snapshot()) {
                    if (b == "late-subscribe-me") return true;
                }
                return false;
            },
            30000);
        check("S3-后置订阅的 topic 上的消息真的被消费", consumed,
              "seen=" + jsonVec(sink.snapshot()));

        // ---------------- S4 活订阅表 ----------------
        consumer->unsubscribe(tLate);
        const std::vector<std::string> live = consumer->subscribedTopics();
        bool hasBase = false, hasLate = false;
        for (const std::string& t : live) {
            if (t == tBase) hasBase = true;
            if (t == tLate) hasLate = true;
        }
        check("S4-unsubscribe 后本组订阅集立刻少掉 L（只删表项，不发心跳）",
              !hasLate && hasBase, "live=" + jsonVec(live));
    } catch (const std::exception& e) {
        check("场景执行", false, e.what());
    }

    if (consumer != nullptr) {
        try {
            consumer->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (consumer shutdown 失败: %s)\n", e.what());
        }
    }
    if (producer != nullptr) {
        try {
            producer->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (producer shutdown 失败: %s)\n", e.what());
        }
    }
    for (const std::string& t : {tBase, tLate}) {
        try {
            admin.deleteTopic(t);
            std::printf("    (deleteTopic(%s) OK)\n", t.c_str());
        } catch (const std::exception& e) {
            std::printf("    (deleteTopic(%s) 失败: %s)\n", t.c_str(), e.what());
        }
    }
    try {
        admin.deleteSubscriptionGroup(brokerAddr, group, true);
    } catch (const std::exception& e) {
        std::printf("    (deleteSubscriptionGroup(%s) 失败: %s)\n", group.c_str(), e.what());
    }
    try {
        admin.shutdown();
    } catch (const std::exception& e) {
        std::printf("    (admin shutdown 失败: %s)\n", e.what());
    }

    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
