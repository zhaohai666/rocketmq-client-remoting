// Prerequisite: NameServer + broker already up (this repo's /tmp/rmq_rust_live/broker.conf).
// Usage: ./rmq_live_subscribe 127.0.0.1:9876
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
            check("cluster probe", false, "no broker registered on the name server");
            admin.shutdown();
            return 1;
        }
        brokerAddr = addrs[0];
    } catch (const std::exception& e) {
        check("cluster probe", false, std::string("fetchBrokerClusterInfo: ") + e.what());
        admin.shutdown();
        return 1;
    }
    check("cluster probe", true, "broker=" + brokerAddr);

    std::shared_ptr<DefaultMQPushConsumer> consumer;
    std::shared_ptr<DefaultMQProducer> producer;
    BodySink sink;

    try {
        admin.createTopic(tBase, tBase, 4, 0);
        admin.createTopic(tLate, tLate, 4, 0);

        consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setInstanceName("live-subscribe-" + stamp);
        consumer->setMessageListener(std::make_shared<CollectListener>(sink));
        consumer->subscribe(tBase, "*");
        consumer->start();

        // ---------------- S0 positive control ----------------
        const int64_t t0 = nowMs();
        const bool s0 = waitUntil(
            [&] { return contains(admin.queryTopicConsumeByWho(brokerAddr, tBase), group); }, 35000);
        check("S0-base topic B already lists this group (queryable through 300)", s0,
              "groupList=" + join(admin.queryTopicConsumeByWho(brokerAddr, tBase)) +
                  " elapsed=" + std::to_string(nowMs() - t0) + "ms");

        // ---------------- S1 negative control ----------------
        const std::set<std::string> gotL0 = admin.queryTopicConsumeByWho(brokerAddr, tLate);
        check("S1-negative control: the never-subscribed L does not list this group", !contains(gotL0, group),
              "groupList=" + join(gotL0));

        // ---------------- S2 post-start subscribe takes effect at once ----------------
        const int64_t t1 = nowMs();
        consumer->subscribe(tLate, "*");
        const int64_t subscribeMs = nowMs() - t1;
        const std::set<std::string> gotL1 = admin.queryTopicConsumeByWho(brokerAddr, tLate);
        const int64_t elapsedMs = nowMs() - t1;
        const bool registered = contains(gotL1, group);
        check("S2-the broker lists this group right after the post-start subscribe (queryable through 300)", registered,
              "groupList=" + join(gotL1));
        // The heartbeat period defaults to 30s: only the **synchronous** heartbeat on the subscribe path registers this fast.
        check("S2-registration took far less than the 30s heartbeat period (only the subscribe path can push it)",
              registered && elapsedMs < kHeartbeatPeriodMs / 6,
              "subscribe returned after=" + std::to_string(subscribeMs) +
                  "ms, query finished after=" + std::to_string(elapsedMs) + "ms");

        // ---------------- S3 the post-start subscription really gets consumed ----------------
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
        check("S3-new topic L entered this instance's assigned set (rebalance took effect)", assigned,
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
        check("S3-messages on the post-start subscribed topic are really consumed", consumed,
              "seen=" + jsonVec(sink.snapshot()));

        // ---------------- S4 live subscription table ----------------
        consumer->unsubscribe(tLate);
        const std::vector<std::string> live = consumer->subscribedTopics();
        bool hasBase = false, hasLate = false;
        for (const std::string& t : live) {
            if (t == tBase) hasBase = true;
            if (t == tLate) hasLate = true;
        }
        check("S4-after unsubscribe this group's subscription set drops L at once (table entry only, no heartbeat)",
              !hasLate && hasBase, "live=" + jsonVec(live));
    } catch (const std::exception& e) {
        check("scenario execution", false, e.what());
    }

    if (consumer != nullptr) {
        try {
            consumer->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (consumer shutdown failed: %s)\n", e.what());
        }
    }
    if (producer != nullptr) {
        try {
            producer->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (producer shutdown failed: %s)\n", e.what());
        }
    }
    for (const std::string& t : {tBase, tLate}) {
        try {
            admin.deleteTopic(t);
            std::printf("    (deleteTopic(%s) OK)\n", t.c_str());
        } catch (const std::exception& e) {
            std::printf("    (deleteTopic(%s) failed: %s)\n", t.c_str(), e.what());
        }
    }
    try {
        admin.deleteSubscriptionGroup(brokerAddr, group, true);
    } catch (const std::exception& e) {
        std::printf("    (deleteSubscriptionGroup(%s) failed: %s)\n", group.c_str(), e.what());
    }
    try {
        admin.shutdown();
    } catch (const std::exception& e) {
        std::printf("    (admin shutdown failed: %s)\n", e.what());
    }

    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
