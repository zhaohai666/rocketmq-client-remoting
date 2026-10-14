// Route-refresh interval / offset-persist interval live verification (scenarios I1–I3 asserted one by one).
//
//
//
//
//   I3 pass-through: after producer/consumer start, the live instance's `pollNameServerIntervalMillis()`
//      is exactly the value the caller set (not merely stored on the façade).
//
// Prerequisite: NameServer + Broker already up (broker.conf at /tmp/rmq_rust_live in this setup).
// Usage: ./rmq_live_scheduled_intervals 127.0.0.1:9876
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;
int32_t gSkip = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

void skip(const std::string& name, const std::string& detail) {
    ++gSkip;
    std::printf("  [SKIP] %s  %s\n", name.c_str(), detail.c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// Live delivery is affected by broker long polling / flow control and same-host load; a fixed sleep would turn a correct implementation into a false failure.
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 200) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

std::string secs(int64_t fromMs, int64_t toMs) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.2fs", static_cast<double>(toMs - fromMs) / 1000.0);
    return std::string(buf);
}

struct BodySink {
    std::mutex mtx;
    std::vector<std::string> bodies;

    size_t size() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies.size();
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

// Committed offset of this queue for the group on the broker (`setZeroIfNotFound=true` ⇒ returns 0 when never committed).
// The criterion is "the broker really received the offset", so only the broker can be queried.
int64_t brokerOffset(DefaultMQAdminExt& admin, const std::string& group, const MessageQueue& mq) {
    int64_t off = 0;
    if (!admin.client().queryConsumerOffset(group, mq, off, 5000, std::string(), true)) {
        return 0;
    }
    return off;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_scheduled_intervals <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const std::string stamp = std::to_string(nowMs() % 1000000);
    const std::string tPoll = "IntervalPollTopic_" + stamp;
    const std::string tPersist = "IntervalPersistTopic_" + stamp;
    const std::string gPollFast = "G_poll_fast_" + stamp;
    const std::string gPollSlow = "G_poll_slow_" + stamp;
    const std::string gFast = "G_persist_fast_" + stamp;
    const std::string gSlow = "G_persist_slow_" + stamp;
    const int32_t kFastPollMs = 1000;
    const int32_t kSlowPollMs = 30000;
    const int32_t kFastPersistMs = 1000;
    const int32_t kSlowPersistMs = 60000;
    const double kPersistInitialDelaySec = 10.0;

    DefaultMQAdminExt admin;
    admin.setNamesrvAddr(nsAddr);
    admin.start();
    ClusterInfo cluster;
    try {
        cluster = admin.fetchBrokerClusterInfo();
    } catch (const std::exception& e) {
        check("cluster liveness probe", false, std::string("fetchBrokerClusterInfo: ") + e.what());
        admin.shutdown();
        return 1;
    }
    const std::vector<std::string> brokerAddrs = cluster.getBrokerAddrs();
    if (brokerAddrs.empty()) {
        check("cluster liveness probe", false, "no broker registered on the name server");
        admin.shutdown();
        return 1;
    }
    const std::string brokerAddr = brokerAddrs[0];
    check("cluster liveness probe", true, "broker=" + brokerAddr);

    DefaultMQProducer* producer = nullptr;
    std::shared_ptr<DefaultMQProducer> fastPoll;
    std::shared_ptr<DefaultMQProducer> slowPoll;
    std::vector<std::shared_ptr<DefaultMQPushConsumer>> consumers;

    try {
        // ---------------- I1 route refresh interval ----------------
        fastPoll = std::make_shared<DefaultMQProducer>(gPollFast);
        fastPoll->setNamesrvAddr(nsAddr);
        fastPoll->setInstanceName("interval-poll-fast-" + stamp);
        fastPoll->setPollNameServerIntervalMillis(kFastPollMs);
        fastPoll->start();

        slowPoll = std::make_shared<DefaultMQProducer>(gPollSlow);
        slowPoll->setNamesrvAddr(nsAddr);
        slowPoll->setInstanceName("interval-poll-slow-" + stamp);
        slowPoll->start();

        check("I3 producer instance applies the configured refresh interval (1s group)",
              fastPoll->client().pollNameServerIntervalMillis() == kFastPollMs,
              "client.pollNameServerIntervalMillis=" +
                  std::to_string(fastPoll->client().pollNameServerIntervalMillis()));
        check("I3 producer instance defaults to 30s (control group)",
              slowPoll->client().pollNameServerIntervalMillis() == kSlowPollMs,
              "client.pollNameServerIntervalMillis=" +
                  std::to_string(slowPoll->client().pollNameServerIntervalMillis()));

        // Register a not-yet-created topic into the periodic-refresh set: neither producer sends
        // to it, so when it appears in the cache is decided only by each one's refresh interval.
        fastPoll->client().registerTopicInUse(tPoll);
        slowPoll->client().registerTopicInUse(tPoll);
        check("I1 both producers registered " + tPoll + " in the topic-in-use set (registration itself does not pull)",
              !fastPoll->client().isTopicRouteCached(tPoll) &&
                  !slowPoll->client().isTopicRouteCached(tPoll),
              "both caches empty after registration");

        std::this_thread::sleep_for(std::chrono::milliseconds(1500));

        const int64_t t0 = nowMs();
        admin.createTopic(MixAll::DEFAULT_TOPIC, tPoll, 1);
        check("I1 admin created topic OK", true, tPoll + " 1 queue");

        const bool fastOk =
            waitUntil([&] { return fastPoll->client().isTopicRouteCached(tPoll); }, 6000, 100);
        const int64_t dtFast = nowMs() - t0;
        check("I1 1s-interval group fetched the new topic route from NameServer within 6s", fastOk,
              "dt=" + secs(t0, nowMs()) + " interval=" + std::to_string(kFastPollMs) + "ms");
        check("I1 at this moment the 30s-interval group has **not** fetched yet (control: the interval decides timing)",
              !slowPoll->client().isTopicRouteCached(tPoll),
              "dt=" + secs(t0, nowMs()) + " interval=" + std::to_string(kSlowPollMs) + "ms");

        const bool slowOk =
            waitUntil([&] { return slowPoll->client().isTopicRouteCached(tPoll); }, 40000, 500);
        const int64_t dtSlow = nowMs() - t0;
        check("I1 the 30s-interval group eventually fetched too (the default is only slow, not broken)", slowOk,
              "dt=" + secs(t0, nowMs()) + " interval=" + std::to_string(kSlowPollMs) + "ms");
        check("I1 the two groups' gaps match the configured magnitude (30s group at least 20s later)", (dtSlow - dtFast) >= 20000,
              "fast=" + secs(0, dtFast) + " slow=" + secs(0, dtSlow));
        fastPoll->shutdown();
        slowPoll->shutdown();

        // ---------------- I2 offset persistence interval ----------------
        admin.createTopic(MixAll::DEFAULT_TOPIC, tPersist, 1);
        const TopicRouteData persistRoute = admin.examineTopicRoute(tPersist);
        const std::vector<MessageQueue> persistQueues = persistRoute.getAllMessageQueue(tPersist);
        if (persistQueues.empty()) {
            throw std::runtime_error("examineTopicRoute returned no " + tPersist + " queues");
        }
        const MessageQueue mq = persistQueues[0];

        producer = new DefaultMQProducer("G_persist_producer_" + stamp);
        producer->setNamesrvAddr(nsAddr);
        producer->setInstanceName("interval-persist-prod-" + stamp);
        producer->start();
        for (int i = 0; i < 3; ++i) {
            producer->send(Message(tPersist, "batch1-" + std::to_string(i)));
        }

        BodySink sinkFast;
        BodySink sinkSlow;
        auto fastC = std::make_shared<DefaultMQPushConsumer>(gFast);
        fastC->setNamesrvAddr(nsAddr);
        fastC->setInstanceName("interval-persist-fast-" + stamp);
        fastC->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        fastC->setPollNameServerIntervalMillis(2000);
        fastC->setPersistConsumerOffsetIntervalMillis(kFastPersistMs);
        fastC->setMessageListener(std::make_shared<CollectListener>(sinkFast));
        fastC->subscribe(tPersist);

        auto slowC = std::make_shared<DefaultMQPushConsumer>(gSlow);
        slowC->setNamesrvAddr(nsAddr);
        slowC->setInstanceName("interval-persist-slow-" + stamp);
        slowC->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        slowC->setPersistConsumerOffsetIntervalMillis(kSlowPersistMs);
        slowC->setMessageListener(std::make_shared<CollectListener>(sinkSlow));
        slowC->subscribe(tPersist);

        const int64_t tStart = nowMs();
        fastC->start();
        consumers.push_back(fastC);
        check("I3 consumer instance applies the configured refresh interval",
              fastC->client().pollNameServerIntervalMillis() == 2000,
              "client.pollNameServerIntervalMillis=" +
                  std::to_string(fastC->client().pollNameServerIntervalMillis()));
        slowC->start();
        consumers.push_back(slowC);

        const bool consumed =
            waitUntil([&] { return sinkFast.size() >= 3 && sinkSlow.size() >= 3; }, 30000);
        check("I2 both consumers consumed 3 messages (no commit / shutdown yet)", consumed,
              "fast=" + std::to_string(sinkFast.size()) + " slow=" +
                  std::to_string(sinkSlow.size()));

        // The first persist happens ~10s after start: if consumption finished within 10s, neither group has pushed anything yet. When consumption itself
        // takes longer than 10s this check loses discriminating power, so record SKIP instead of a hard failure (timing drift caused by live-cluster
        // load should not be blamed on the client).
        if (nowMs() - tStart < 9000) {
            const int64_t offFastNow = brokerOffset(admin, gFast, mq);
            const int64_t offSlowNow = brokerOffset(admin, gSlow, mq);
            check("I2 broker offset is not pushed immediately after consumption", offFastNow < 3 && offSlowNow < 3,
                  "fast=" + std::to_string(offFastNow) + " slow=" + std::to_string(offSlowNow) +
                      " elapsed=" + secs(tStart, nowMs()));
        } else {
            skip("I2 broker offset is not pushed immediately after consumption",
                 "consumption took " + secs(tStart, nowMs()) + " ≥ 10s, past the first-persist window");
        }

        const bool fastFirst =
            waitUntil([&] { return brokerOffset(admin, gFast, mq) == 3; }, 20000, 300);
        const int64_t dtFastFirst = nowMs() - tStart;
        const bool slowFirst =
            waitUntil([&] { return brokerOffset(admin, gSlow, mq) == 3; }, 20000, 300);
        const int64_t dtSlowFirst = nowMs() - tStart;
        check("I2 the 1s group's first persist happens after initialDelay(~10s)", fastFirst,
              "dt=" + secs(0, dtFastFirst) + " interval=" + std::to_string(kFastPersistMs) + "ms");
        const double fastFirstSec = static_cast<double>(dtFastFirst) / 1000.0;
        check("I2 first persist no earlier than initialDelay 10s",
              fastFirst && fastFirstSec >= kPersistInitialDelaySec - 0.5,
              "dt=" + secs(0, dtFastFirst));
        check("I2 the 60s group also completes its first persist at ~10s (interval not due yet, initialDelay goes first)", slowFirst,
              "dt=" + secs(0, dtSlowFirst) + " interval=" + std::to_string(kSlowPersistMs) + "ms");

        // Second batch: both groups consume it, but the broker-side offset is pushed only by each group's own periodic task.
        const int64_t t2 = nowMs();
        for (int i = 0; i < 3; ++i) {
            producer->send(Message(tPersist, "batch2-" + std::to_string(i)));
        }
        check("I2 both consumers consumed the second batch (6 messages)",
              waitUntil([&] { return sinkFast.size() >= 6 && sinkSlow.size() >= 6; }, 20000),
              "fast=" + std::to_string(sinkFast.size()) + " slow=" +
                  std::to_string(sinkSlow.size()));
        const bool fastSecond =
            waitUntil([&] { return brokerOffset(admin, gFast, mq) == 6; }, 5000, 200);
        check("I2 the 1s group pushes the second-batch offset within one interval", fastSecond,
              "dt=" + secs(t2, nowMs()) + " interval=" + std::to_string(kFastPersistMs) + "ms");
        const int64_t offSlowBatch2 = brokerOffset(admin, gSlow, mq);
        check("I2 at this moment the 60s group is still at 3 (the 60s interval is far from due, and it really consumed 6)",
              offSlowBatch2 == 3 && sinkSlow.size() >= 6,
              "broker=" + std::to_string(offSlowBatch2) + " consumed=" +
                  std::to_string(sinkSlow.size()) + " interval=" + std::to_string(kSlowPersistMs) + "ms");
        const int64_t sinceSlowFirst = nowMs() - (tStart + dtSlowFirst);
        check("I2 the 60s group's next interval has not arrived (< 60s since the first persist)",
              sinceSlowFirst < kSlowPersistMs, "elapsed=" + secs(0, sinceSlowFirst));

        slowC->shutdown();
        consumers.erase(consumers.begin() + 1);
        const bool slowSecond =
            waitUntil([&] { return brokerOffset(admin, gSlow, mq) == 6; }, 10000, 300);
        check("I2 the 60s group persists 6 at shutdown() (persistAllConsumerOffset finishes up)", slowSecond,
              "broker=" + std::to_string(brokerOffset(admin, gSlow, mq)));
    } catch (const std::exception& e) {
        check("exception thrown during verification", false, e.what());
    }

    // ---------------- cleanup ----------------
    for (auto& c : consumers) {
        try {
            c->shutdown();
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
        delete producer;
    }
    for (const std::shared_ptr<DefaultMQProducer>& p : {fastPoll, slowPoll}) {
        if (!p) continue;
        try {
            p->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (producer shutdown failed: %s)\n", e.what());
        }
    }
    for (const std::string& t : {tPoll, tPersist}) {
        try {
            admin.deleteTopic(t);
            std::printf("    (deleteTopic(%s) OK)\n", t.c_str());
        } catch (const std::exception& e) {
            std::printf("    (deleteTopic(%s) failed: %s)\n", t.c_str(), e.what());
        }
    }
    for (const std::string& g : {gPollFast, gPollSlow, gFast, gSlow}) {
        try {
            admin.deleteSubscriptionGroup(brokerAddr, g, true);
        } catch (const std::exception& e) {
            std::printf("    (deleteSubscriptionGroup(%s) failed: %s)\n", g.c_str(), e.what());
        }
    }
    try {
        admin.shutdown();
    } catch (const std::exception& e) {
        std::printf("    (admin shutdown failed: %s)\n", e.what());
    }

    std::printf("\nPASS=%d FAIL=%d SKIP=%d\n", gPass, gFail, gSkip);
    return gFail == 0 ? 0 : 1;
}
