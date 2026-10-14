#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <iterator>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/route.h"

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

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::string str2bytes(const std::string& s) { return s; }

// Create the topic first, per real usage, then start the consumer. In a real environment the topic is created
// ahead of time by an admin or by the first send; the consumer does no default topic fallback, so when the topic is absent it
// gets no route and no queues are assigned. Create it explicitly here to avoid a false failure of
// "the implementation is fine but it waits for a timeout / misses messages".
void prepareTopic(DefaultMQProducer& producer, const std::string& topic, int32_t queues = 4) {
    try {
        producer.createTopic("init", topic, queues);
    } catch (const std::exception& e) {
        std::printf("  pre-create topic %s failed (falling back to auto-creation): %s\n", topic.c_str(), e.what());
    }
    // Wait for the name server route to propagate, otherwise the consumer's first rebalance still finds nothing
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

// Poll until the condition holds: on a real machine delivery is subject to broker long-polling, flow control and same-host
// load, so a fixed sleep turns "the implementation is fine" into missed messages (measured on the same code: got=7/10, 35/40 jitter).
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return pred();
}

// On-site forensics for %DLQ%<group>: first use a one-shot probe to wait until the broker registers the dead-letter topic into the
// route, then use an **independent consumer group + FIRST_OFFSET** to read out the dead letters already present (LAST would start at the
// queue tail and skip the message already landed in the DLQ => a false failure). routeFound is returned separately: in negative cases "the
// broker never created the DLQ topic at all" is itself the correct conclusion, and must not be merged into the same empty result as "never got the route".
struct DlqSight {
    bool routeFound = false;
    std::vector<MessageExt> msgs;
};

DlqSight readDlq(const std::string& nsAddr, const std::string& dlqTopic,
                 const std::string& readerGroup, int64_t timeoutMs) {
    DlqSight sight;
    std::shared_ptr<TopicRouteData> route;
    {
        MQClientInstance probe("dlqprobe-" + std::to_string(nowMs()),
                              std::vector<std::string>{nsAddr});
        probe.start();
        const int64_t deadline = nowMs() + timeoutMs;
        while (nowMs() < deadline) {
            route = probe.getTopicRouteData(dlqTopic);
            if (route && !route->queueDatas.empty()) break;
            route.reset();
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        probe.shutdown();
    }
    if (!route || route->queueDatas.empty()) return sight;
    sight.routeFound = true;
    std::vector<MessageQueue> queues;
    for (const QueueData& q : route->queueDatas) {
        for (int32_t i = 0; i < q.readQueueNums; ++i) {
            queues.emplace_back(dlqTopic, q.brokerName, i);
        }
    }
    DefaultLitePullConsumer reader(readerGroup);
    reader.setNamesrvAddr(nsAddr);
    reader.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    reader.assign(queues);
    reader.start();
    for (const MessageQueue& mq : queues) reader.seekToBegin(mq);
    // seekToBegin must come after start() (it throws if not started); it rewinds the pull cursor to the minimum offset. If the
    // pull loop has already pulled the same message into the local buffer first, that delivered copy is not cleared by the seek (seek
    // only drops "before the offset"), so the same message appears twice with the same (queueId, queueOffset). Deduplicate by stored
    // position: a genuine two-copy DLQ write lands at different queueOffsets and is not eaten.
    std::set<std::pair<int32_t, int64_t>> seenAt;
    auto collect = [&](const std::vector<MessageExt>& batch) {
        for (const MessageExt& m : batch) {
            if (seenAt.insert({m.queueId, m.queueOffset}).second) sight.msgs.push_back(m);
        }
    };
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        std::vector<MessageExt> batch = reader.poll(1000);
        if (batch.empty()) continue;
        collect(batch);
        // After receiving, poll a few more times: asserting "the DLQ holds only this one" requires draining the
        // rest too, but the total window must be bounded, otherwise positive cases would wait out the full timeoutMs every time.
        const int64_t drainTo = nowMs() + 3000;
        while (nowMs() < drainTo) {
            collect(reader.poll(500));
        }
        break;
    }
    reader.shutdown();
    return sight;
}

// Collection box: the lock and the buffer are bound together; the main thread must take the same lock before reading.
struct BodySink {
    std::mutex mtx;
    std::vector<std::string> bodies;

    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies;
    }

    size_t size() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies.size();
    }

    bool empty() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies.empty();
    }
};

// Convenience listener: records each received body into a list, returns CONSUME_SUCCESS for all.
// The lock must travel with the buffer: earlier each listener held its own mtx_, so when the main thread read the vector
// nobody held a lock — a data race (measured S6 counts drifting to impossible values like 71/40, 35/40).
class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(BodySink& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.bodies.push_back(bodyOf(m));
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    BodySink& sink_;
};

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_redelivery <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    const std::string gPrefix = "GapCpp_" + std::to_string(nowMs() % 1000000);

    DefaultMQProducer producer(gPrefix + "_pg");
    producer.setNamesrvAddr(nsAddr);
    producer.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // ---------------- S1 redelivery ----------------
    {
        const std::string topic = gPrefix + "_Retry";
        prepareTopic(producer, topic);
        auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g1");
        std::mutex mtx;
        struct Arrival {
            std::string body;
            std::string topic;
            int32_t reconsumeTimes;
            int64_t ts;
        };
        std::vector<Arrival> seen;
        class L : public MessageListenerConcurrently {
        public:
            L(std::mutex& mtx, std::vector<Arrival>& seen) : mtx_(mtx), seen_(seen) {}
            ConsumeConcurrentlyStatus consumeMessage(
                const std::vector<MessageExt>& msgs, ConsumeConcurrentlyContext&) override {
                bool needRetry = false;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    for (const MessageExt& m : msgs) {
                        seen_.push_back({bodyOf(m), m.topic, m.getReconsumeTimes(), nowMs()});
                        if (bodyOf(m) == "retry-me" && m.getReconsumeTimes() == 0) {
                            needRetry = true;
                        }
                    }
                }
                // retry-me fails on the first delivery, succeeds after redelivery (redelivery count rides MessageExt field 13)
                return needRetry ? ConsumeConcurrentlyStatus::RECONSUME_LATER
                                 : ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            std::mutex& mtx_;
            std::vector<Arrival>& seen_;
        };
        consumer->setMessageListener(std::make_shared<L>(mtx, seen));
        consumer->setNamesrvAddr(nsAddr);
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        producer.send(Message(topic, str2bytes("retry-me")));
        producer.send(Message(topic, str2bytes("normal-1")));
        std::printf("S1: sent, waiting for redelivery (delay ladder level3≈10s)...\n");
        std::this_thread::sleep_for(std::chrono::seconds(22));
        consumer->shutdown();

        std::vector<Arrival> retryArrivals;
        int32_t normalCount = 0;
        {
            std::lock_guard<std::mutex> lk(mtx);
            for (const Arrival& a : seen) {
                if (a.body == "retry-me") retryArrivals.push_back(a);
                if (a.body == "normal-1") ++normalCount;
            }
        }
        check("S1-retry-me delivered multiple times", retryArrivals.size() >= 2,
              "arrivals=" + std::to_string(retryArrivals.size()));
        bool fromRetry = false;
        for (const Arrival& a : retryArrivals) {
            if (a.reconsumeTimes >= 1 || a.topic.rfind("%RETRY%", 0) == 0) fromRetry = true;
        }
        check("S1-redelivery came from %RETRY%/reconsumeTimes", fromRetry);
        if (retryArrivals.size() >= 2) {
            const double gapSec =
                static_cast<double>(retryArrivals.back().ts - retryArrivals.front().ts) / 1000.0;
            check("S1-redelivery has a delay ladder (>=8s)", gapSec >= 8.0,
                  "gap=" + std::to_string(gapSec).substr(0, 5) + "s");
        } else {
            check("S1-redelivery has a delay ladder (>=8s)", false, "fewer than two deliveries");
        }
        check("S1-normal message delivered only once", normalCount == 1,
              "arrivals=" + std::to_string(normalCount));
    }

    // ---------------- S2 offset persistence ----------------
    {
        const std::string topic = gPrefix + "_Offset";
        const std::string group = gPrefix + "_g2";
        prepareTopic(producer, topic);
        BodySink round1;
        {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setMessageListener(std::make_shared<CollectListener>(round1));
            consumer->setNamesrvAddr(nsAddr);
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            for (int i = 0; i < 3; ++i) {
                producer.send(Message(topic, str2bytes("persist-" + std::to_string(i))));
            }
            waitUntil([&] { return round1.size() >= 3; }, 40000);
            consumer->shutdown();  // shutdown persists the offset
        }
        const std::vector<std::string> seen1 = round1.snapshot();
        int32_t gotFirst = 0;
        for (int i = 0; i < 3; ++i) {
            if (std::find(seen1.begin(), seen1.end(),
                          "persist-" + std::to_string(i)) != seen1.end()) {
                ++gotFirst;
            }
        }
        check("S2-first round consumed 3 messages", gotFirst == 3, "got=" + std::to_string(gotFirst));

        BodySink round2;
        {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setMessageListener(std::make_shared<CollectListener>(round2));
            consumer->setNamesrvAddr(nsAddr);
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            producer.send(Message(topic, str2bytes("persist-new")));
            waitUntil([&] { return round2.size() >= 1; }, 40000);
            consumer->shutdown();
        }
        const std::vector<std::string> seen2 = round2.snapshot();
        bool newSeen = std::find(seen2.begin(), seen2.end(), "persist-new") != seen2.end();
        int32_t oldResent = 0;
        for (const std::string& b : seen2) {
            if (b.rfind("persist-", 0) == 0 && b != "persist-new") ++oldResent;
        }
        check("S2-new messages still delivered after restart", newSeen, newSeen ? "" : "not received");
        check("S2-restart does not re-consume old messages", oldResent == 0,
              "resent=" + std::to_string(oldResent));
    }

    // ---------------- S3 orderly consumption + broker lock ----------------
    {
        const std::string topic = gPrefix + "_Orderly";
        prepareTopic(producer, topic);
        auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g3");
        std::atomic<int32_t> got{0};
        class L : public MessageListenerOrderly {
        public:
            explicit L(std::atomic<int32_t>& got) : got_(got) {}
            ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                ConsumeOrderlyContext&) override {
                got_.fetch_add(static_cast<int32_t>(msgs.size()));
                return ConsumeOrderlyStatus::SUCCESS;
            }

        private:
            std::atomic<int32_t>& got_;
        };
        consumer->setMessageListener(std::make_shared<L>(got));
        consumer->setNamesrvAddr(nsAddr);
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(5));  // wait for the first LOCK_BATCH_MQ round to take effect
        for (int i = 0; i < 4; ++i) {
            producer.send(Message(topic, str2bytes("orderly-" + std::to_string(i))));
        }
        const bool orderlyAll = waitUntil([&] { return got.load() >= 4; }, 30000);
        consumer->shutdown();
        check("S3-orderly consumption receives all", orderlyAll && got.load() == 4, "got=" + std::to_string(got.load()));
        // lockOK count is verified indirectly via successful consumption + no exceptions (the internal lock set being non-empty is visible in debug logs)
        check("S3-orderly consumption path alive", got.load() > 0);
    }

    // ---------------- S4 broadcasting mode ----------------
    {
        const std::string topic = gPrefix + "_Bc";
        const std::string group = gPrefix + "_g4";
        prepareTopic(producer, topic);
        BodySink gotA;
        BodySink gotB;
        auto ca = std::make_shared<DefaultMQPushConsumer>(group);
        ca->setInstanceName("bc-a");
        ca->setMessageListener(std::make_shared<CollectListener>(gotA));
        ca->setMessageModel(MessageModel::BROADCASTING);
        ca->setNamesrvAddr(nsAddr);
        ca->subscribe(topic);
        ca->start();
        auto cb = std::make_shared<DefaultMQPushConsumer>(group);
        cb->setInstanceName("bc-b");
        cb->setMessageListener(std::make_shared<CollectListener>(gotB));
        cb->setMessageModel(MessageModel::BROADCASTING);
        cb->setNamesrvAddr(nsAddr);
        cb->subscribe(topic);
        cb->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 3; ++i) {
            producer.send(Message(topic, str2bytes("bc-" + std::to_string(i))));
        }
        // In broadcasting mode each of the two instances receives all 3 messages on its own
        const bool aAll = waitUntil([&] { return gotA.size() >= 3; }, 30000);
        const bool bAll = waitUntil([&] { return gotB.size() >= 3; }, 30000);
        ca->shutdown();
        cb->shutdown();
        check("S4-broadcast consumer A receives all", aAll && gotA.size() == 3,
              "got=" + std::to_string(gotA.size()));
        check("S4-broadcast consumer B receives all", bAll && gotB.size() == 3,
              "got=" + std::to_string(gotB.size()));
    }

    // ---------------- S5 flow control ----------------
    {
        const std::string topic = gPrefix + "_Flow";
        prepareTopic(producer, topic);
        auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g5");
        std::atomic<int32_t> got{0};
        class L : public MessageListenerConcurrently {
        public:
            explicit L(std::atomic<int32_t>& got) : got_(got) {}
            ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                     ConsumeConcurrentlyContext&) override {
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                got_.fetch_add(static_cast<int32_t>(msgs.size()));
                return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            std::atomic<int32_t>& got_;
        };
        consumer->setMessageListener(std::make_shared<L>(got));
        consumer->setPullThresholdForQueue(2);
        consumer->setNamesrvAddr(nsAddr);
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 10; ++i) {
            producer.send(Message(topic, str2bytes("flow-" + std::to_string(i))));
        }
        // Threshold 2 + 300ms sleep per message: only when all land does it prove "flow control just pauses pulling, it does
        // not drop messages". A fixed sleep on a busy machine would read a false got=7/10 (rerunning the same code turns it green).
        const bool allArrived = waitUntil([&] { return got.load() >= 10; }, 45000);
        const int64_t fc = consumer->flowControlTriggered();
        consumer->shutdown();
        check("S5-all messages arrive under slow consumption", allArrived && got.load() == 10,
              "got=" + std::to_string(got.load()));
        check("S5-flow-control trigger count > 0", fc > 0, "triggered=" + std::to_string(fc));
    }

    // ---------------- S6 cluster multi-instance rebalance (queue allocation) ----------------
    {
        const std::string topic6 = gPrefix + "_Rebalance";
        const std::string group6 = gPrefix + "_g6";
        // Create the topic first (8 queues) and wait for route propagation, otherwise the consumers start with
        // no route and allocation only stabilizes at the next 20s rebalance round (you would read an intermediate state)
        try {
            producer.createTopic("init", topic6, 8);
            std::this_thread::sleep_for(std::chrono::seconds(3));
        } catch (const std::exception& e) {
            std::printf("S6: pre-create topic failed (falling back to auto-creation): %s\n", e.what());
        }
        BodySink recA;
        BodySink recB;
        auto ca = std::make_shared<DefaultMQPushConsumer>(group6);
        ca->setInstanceName("inst-a");
        ca->setMessageListener(std::make_shared<CollectListener>(recA));
        ca->setNamesrvAddr(nsAddr);
        ca->subscribe(topic6);
        ca->start();
        auto cb = std::make_shared<DefaultMQPushConsumer>(group6);
        cb->setInstanceName("inst-b");
        cb->setMessageListener(std::make_shared<CollectListener>(recB));
        cb->setNamesrvAddr(nsAddr);
        cb->subscribe(topic6);
        cb->start();
        // Wait for allocation to stabilize: empty intersection + both sides non-empty + main topic queues fully covered (wait at most 45s)
        std::vector<std::string> keysA, keysB;
        int asgA = 0, asgB = 0;
        std::set<std::string> expectedKeys;
        {
            for (const MessageQueue& mq : ca->fetchSubscribeMessageQueues(topic6)) {
                expectedKeys.insert(mq.topic + mq.brokerName + std::to_string(mq.queueId));
            }
        }
        int64_t deadline = nowMs() + 45000;
        while (nowMs() < deadline) {
            keysA = ca->assignedQueueKeys();
            keysB = cb->assignedQueueKeys();
            asgA = static_cast<int>(keysA.size());
            asgB = static_cast<int>(keysB.size());
            std::set<std::string> ua(keysA.begin(), keysA.end());
            std::set<std::string> ub(keysB.begin(), keysB.end());
            std::set<std::string> inter;
            std::set_intersection(ua.begin(), ua.end(), ub.begin(), ub.end(),
                                  std::inserter(inter, inter.begin()));
            std::set<std::string> uni = ua;
            uni.insert(ub.begin(), ub.end());
            std::set<std::string> covered;
            std::set_intersection(uni.begin(), uni.end(), expectedKeys.begin(), expectedKeys.end(),
                                  std::inserter(covered, covered.begin()));
            if (asgA > 0 && asgB > 0 && inter.empty() && covered == expectedKeys) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        const int64_t hbA = ca->heartbeatCount();
        std::vector<std::string> cidList = ca->consumerIdListOfGroup(topic6);
        const int n6 = 40;
        for (int i = 0; i < n6; ++i) {
            producer.send(Message(topic6, str2bytes("rb-" + std::to_string(i))));
        }
        std::this_thread::sleep_for(std::chrono::seconds(15));
        // Give one round of fixed delivery time first, then wait until all 40 are received (a fixed 15s on a busy machine reads a false 35/40);
        // after all are in, wait a bit longer to give duplicate deliveries a chance to surface — only then are extra rows genuine duplicates, not "not yet arrived".
        const bool allArrived = waitUntil([&] { return recA.size() + recB.size() >= 40; }, 60000);
        std::this_thread::sleep_for(std::chrono::seconds(8));
        const int total6 = static_cast<int>(recA.size() + recB.size());
        std::vector<std::string> all6 = recA.snapshot();
        const std::vector<std::string> b6 = recB.snapshot();
        all6.insert(all6.end(), b6.begin(), b6.end());
        std::set<std::string> uniq(all6.begin(), all6.end());
        int dup6 = static_cast<int>(all6.size()) - static_cast<int>(uniq.size());
        // When a group member changes, the broker pushes 40 back over the long connection; the reverse-request case cannot be injected, so
        // this counter is the only landing point that proves "the instance-level 40 handler really ran" (it must be sampled before shutdown).
        const size_t notifiedA = ca->client().consumerIdsChangedCount();
        const size_t notifiedB = cb->client().consumerIdsChangedCount();
        ca->shutdown();
        cb->shutdown();
        check("S6-consumer registered via heartbeat",
              hbA > 0 && cidList.size() == 2,
              "heartbeats=" + std::to_string(hbA)
                  + " brokerCids=" + std::to_string(cidList.size()));
        std::set<std::string> sa(keysA.begin(), keysA.end());
        std::set<std::string> sb(keysB.begin(), keysB.end());
        std::set<std::string> inter2;
        std::set_intersection(sa.begin(), sa.end(), sb.begin(), sb.end(),
                              std::inserter(inter2, inter2.begin()));
        std::set<std::string> uni2 = sa;
        uni2.insert(sb.begin(), sb.end());
        std::set<std::string> covered2;
        std::set_intersection(uni2.begin(), uni2.end(), expectedKeys.begin(), expectedKeys.end(),
                              std::inserter(covered2, covered2.begin()));
        check("S6-queues no overlap no gap (a=" + std::to_string(asgA) + ",b=" + std::to_string(asgB)
                  + ",inter=" + std::to_string(inter2.size()) + ",covered=" + std::to_string(covered2.size())
                  + "/" + std::to_string(expectedKeys.size()) + ")",
              asgA > 0 && asgB > 0 && inter2.empty() && !expectedKeys.empty()
                  && covered2 == expectedKeys);
        check("S6-no duplicate message consumption", allArrived && dup6 == 0 && total6 == n6,
              "got=" + std::to_string(total6) + "/" + std::to_string(n6)
                  + " dup=" + std::to_string(dup6));
        check("S6-received broker NOTIFY_CONSUMER_IDS_CHANGED(40) on member change",
              notifiedA > 0 && notifiedB > 0,
              "a=" + std::to_string(notifiedA) + " b=" + std::to_string(notifiedB)
                  + " (0 means the instance-level handler never received a reverse notification)");
    }

    // ---------------- S7 graceful unpublish ----------------
    {
        // Reuse the 8-queue topic built in S6; on shutdown UNREGISTER_CLIENT should be sent so the broker removes it immediately,
        // without waiting for the heartbeat timeout (~120s). Query with a separate probe client (after the consumer shuts down, its internal client is already closed).
        const std::string topic7 = gPrefix + "_Rebalance";
        const std::string group7 = gPrefix + "_g7";
        MQClientInstance probe("probe-" + std::to_string(nowMs()),
                               std::vector<std::string>{nsAddr});
        probe.start();
        auto qc = std::make_shared<DefaultMQPushConsumer>(group7);
        qc->setInstanceName("inst-c");
        BodySink sink;
        qc->setMessageListener(std::make_shared<CollectListener>(sink));
        qc->setNamesrvAddr(nsAddr);
        qc->subscribe(topic7);
        qc->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        const std::string cid7 = qc->clientId();
        std::vector<std::string> listBefore = probe.getConsumerIdListByGroup(topic7, group7);
        qc->shutdown();
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::vector<std::string> listAfter = probe.getConsumerIdListByGroup(topic7, group7);
        probe.shutdown();
        bool beforeHas =
            std::find(listBefore.begin(), listBefore.end(), cid7) != listBefore.end();
        bool afterHas = std::find(listAfter.begin(), listAfter.end(), cid7) != listAfter.end();
        check("S7-shutdown unpublished the clientId",
              beforeHas && !afterHas,
              "before=" + std::to_string(listBefore.size())
                  + " after=" + std::to_string(listAfter.size()));
    }

    // ---------------- S8 namespace (multi-tenant isolation) ----------------
    {
        const std::string ns = "NSCpp" + std::to_string(nowMs() % 100000);
        const std::string topic = gPrefix + "_Ns";
        DefaultMQProducer nsProducer(gPrefix + "_ns_pg");
        nsProducer.setNamesrvAddr(nsAddr);
        nsProducer.setNamespace(ns);
        nsProducer.start();
        // createTopic also goes through the namespace wrapper: what is really created is "<ns>%<topic>"
        prepareTopic(nsProducer, topic);

        BodySink nsGot;
        {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g8");
            consumer->setNamespace(ns);
            consumer->setMessageListener(std::make_shared<CollectListener>(nsGot));
            consumer->setNamesrvAddr(nsAddr);
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            for (int i = 0; i < 3; ++i) {
                nsProducer.send(Message(topic, str2bytes("ns-" + std::to_string(i))));
            }
            waitUntil([&] { return nsGot.size() >= 3; }, 30000);
            consumer->shutdown();
        }
        check("S8-namespaced produce/consume receives all", nsGot.size() == 3,
              "got=" + std::to_string(nsGot.size()));

        // A consumer without a namespace subscribing to the same bare topic => receives nothing (proves the real topic is ns%topic)
        BodySink plainGot;
        try {
            auto consumer = std::make_shared<DefaultMQPushConsumer>(gPrefix + "_g8plain");
            consumer->setMessageListener(std::make_shared<CollectListener>(plainGot));
            consumer->setNamesrvAddr(nsAddr);
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(8));
            consumer->shutdown();
        } catch (const std::exception& e) {
            std::printf("  (no-ns consumer threw, consistent with the isolation expectation: %s)\n", e.what());
        }
        check("S8-no-namespace consumer receives nothing (isolation)", plainGot.empty(),
              "got=" + std::to_string(plainGot.size()));

        nsProducer.shutdown();
    }

    {
        const std::string topic = gPrefix + "_Dlq";
        const std::string group = gPrefix + "_g9";
        const int32_t kMaxReconsume = 2;
        prepareTopic(producer, topic, 1);
        std::mutex mtx;
        struct Arrival {
            int32_t reconsumeTimes;
            std::string topic;
            int64_t atMs;  // delivery delay relative to the send time, to tell "wrong count" from "arrived slow"
        };
        std::vector<Arrival> seen;
        int64_t sentAtMs = 0;
        class L : public MessageListenerConcurrently {
        public:
            L(std::mutex& mtx, std::vector<Arrival>& seen, const int64_t& sentAtMs)
                : mtx_(mtx), seen_(seen), sentAtMs_(sentAtMs) {}
            ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                     ConsumeConcurrentlyContext&) override {
                bool mine = false;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    for (const MessageExt& m : msgs) {
                        seen_.push_back({m.getReconsumeTimes(), m.topic, nowMs() - sentAtMs_});
                        if (bodyOf(m) == "dlq-me") mine = true;
                    }
                }
                return mine ? ConsumeConcurrentlyStatus::RECONSUME_LATER
                            : ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            std::mutex& mtx_;
            std::vector<Arrival>& seen_;
            const int64_t& sentAtMs_;
        };
        auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setMaxReconsumeTimes(kMaxReconsume);
        consumer->setMessageListener(std::make_shared<L>(mtx, seen, sentAtMs));
        consumer->setNamesrvAddr(nsAddr);
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        sentAtMs = nowMs();
        producer.send(Message(topic, str2bytes("dlq-me")));
        // Redelivery tier = 3 + reconsumeTimes => level3(10s) + level4(30s): theoretically closes in 40s.
        // Give the timeout budget 150s: when four live cases run on the same host the 3rd delivery was measured
        // dragging past 100s (a slow environment, not a wrong implementation); how slow is exposed directly by times=[...]@Ns.
        std::vector<Arrival> firstThree;
        {
            int64_t deadline = nowMs() + 150000;
            while (nowMs() < deadline) {
                size_t n;
                {
                    std::lock_guard<std::mutex> lk(mtx);
                    n = seen.size();
                }
                if (n >= 3) break;
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
            std::lock_guard<std::mutex> lk(mtx);
            firstThree = seen;
        }
        std::this_thread::sleep_for(std::chrono::seconds(15));  // counter-evidence: there should be no 4th time
        std::vector<Arrival> finalSeen;
        {
            std::lock_guard<std::mutex> lk(mtx);
            finalSeen = seen;
        }
        consumer->shutdown();
        std::string times;
        for (const Arrival& a : firstThree) {
            times += (times.empty() ? "" : ",") + std::to_string(a.reconsumeTimes) + "@"
                + std::to_string(a.atMs / 1000) + "s";
        }
        bool ladder = firstThree.size() >= 3 && firstThree[0].reconsumeTimes == 0
            && firstThree[1].reconsumeTimes == 1 && firstThree[2].reconsumeTimes == 2;
        check("S9-maxReconsumeTimes=2 => delivered 3 times (reconsumeTimes 0/1/2)", ladder,
              "times=[" + times + "]");
        check("S9-no delivery after exhausting (only 3 within the observation window)", finalSeen.size() == 3,
              "arrivals=" + std::to_string(finalSeen.size()));
        bool topicKept = true;
        for (const Arrival& a : firstThree) {
            if (a.topic != topic) topicKept = false;
        }
        check("S9-during redelivery the listener sees the business topic (not %RETRY%)", topicKept);

        const std::string dlqTopic = MixAll::getDlqTopic(group);
        const DlqSight sight = readDlq(nsAddr, dlqTopic, gPrefix + "_g9dlq", 40000);
        check("S9-broker auto-created and registered the %DLQ%<group> route", sight.routeFound,
              "dlq=" + dlqTopic);

        std::vector<MessageExt> dlqMsgs = sight.msgs;
        const bool one = dlqMsgs.size() == 1 && bodyOf(dlqMsgs[0]) == "dlq-me";
        check("S9-message landed in %DLQ%<group>", one,
              "n=" + std::to_string(dlqMsgs.size()));
        if (one) {
            const MessageExt& d = dlqMsgs.front();
            check("S9-DLQ reconsumeTimes = maxReconsumeTimes + 1 (+1 when the broker stores it)",
                d.getReconsumeTimes() == kMaxReconsume + 1,
                "reconsumeTimes=" + std::to_string(d.getReconsumeTimes()));
            auto retryIt = d.properties.find("RETRY_TOPIC");
            check("S9-DLQ keeps RETRY_TOPIC=business topic, topic already is %DLQ%<group>",
                retryIt != d.properties.end() && retryIt->second == topic && d.topic == dlqTopic,
                "retryTopic="
                    + (retryIt == d.properties.end() ? std::string("<missing>") : retryIt->second));
        }
    }

    {
        const std::string topic = gPrefix + "_AckIndex";
        prepareTopic(producer, topic, 1);  // 1 queue: only then is a batch of 3 messages contiguous and ordered

        struct Rec {
            std::string body;
            std::string topic;
            int32_t times;
        };
        struct Sink {
            std::mutex mtx;
            std::vector<Rec> recs;
            std::vector<size_t> batchSizes;

            std::vector<Rec> snapshot() {
                std::lock_guard<std::mutex> lk(mtx);
                return recs;
            }
            size_t batchSize(size_t i) {
                std::lock_guard<std::mutex> lk(mtx);
                return i < batchSizes.size() ? batchSizes[i] : 0;
            }
            size_t count() {
                std::lock_guard<std::mutex> lk(mtx);
                return recs.size();
            }
        };

        // ackFirst < 0 = never touch ackIndex (the control group)
        class L : public MessageListenerConcurrently {
        public:
            L(Sink& sink, int32_t ackFirst) : sink_(sink), ackFirst_(ackFirst) {}
            ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                     ConsumeConcurrentlyContext& ctx) override {
                std::lock_guard<std::mutex> lk(sink_.mtx);
                for (const MessageExt& m : msgs) {
                    sink_.recs.push_back({bodyOf(m), m.topic, m.getReconsumeTimes()});
                }
                // Only narrow ackIndex on the **first batch**: later batches must be acknowledged whole, otherwise the tail never finishes redelivering
                sink_.batchSizes.push_back(msgs.size());
                if (ackFirst_ >= 0 && sink_.batchSizes.size() == 1) {
                    ctx.ackIndex = ackFirst_;
                }
                return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            Sink& sink_;
            int32_t ackFirst_;
        };

        const std::string group = gPrefix + "_g10";
        const std::string controlGroup = gPrefix + "_g10ctrl";
        Sink partial, control;
        auto mk = [&](const std::string& g, Sink& s, int32_t ackFirst) {
            auto c = std::make_shared<DefaultMQPushConsumer>(g);
            c->setNamesrvAddr(nsAddr);
            // Default batch size is 1; without raising it to 3 there is no "partial" to speak of
            c->setConsumeMessageBatchMaxSize(3);
            c->subscribe(topic);
            c->setMessageListener(std::make_shared<L>(s, ackFirst));
            return c;
        };
        auto pc = mk(group, partial, 0);
        auto cc = mk(controlGroup, control, -1);
        // Put the 3 messages up first, then start the consumers: how batches are split depends on pull timing, and only sending first
        // guarantees "one whole batch of 3"; otherwise the first batch might have just 1~2, leaving the number of tail messages held back by
        // ackIndex=0 undetermined. A new group under LAST_OFFSET starts from the latest offset at assignment time, so consume explicitly from FIRST_OFFSET.
        pc->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        cc->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        for (int i = 0; i < 3; ++i) {
            producer.send(Message(topic, str2bytes("ack-" + std::to_string(i))));
        }
        pc->start();
        cc->start();
        waitUntil([&] { return partial.count() >= 3 && control.count() >= 3; }, 40000);

        const std::vector<std::string> tail = {"ack-1", "ack-2"};
        auto redelivered = [&](Sink& s) {
            const std::vector<Rec> seen = s.snapshot();
            size_t hit = 0;
            for (const std::string& b : tail) {
                for (const Rec& r : seen) {
                    if (r.body == b && r.times >= 1 && r.topic == topic) {
                        ++hit;
                        break;
                    }
                }
            }
            return hit;
        };
        // The tail must go through %RETRY% (delay level 3≈10s) + retry-topic route registration + the next rebalance round
        const bool tailBack = waitUntil([&] { return redelivered(partial) == tail.size(); }, 150000);
        std::this_thread::sleep_for(std::chrono::seconds(10));  // counter-evidence window: any extra duplicate delivery will surface
        const std::vector<Rec> seen = partial.snapshot();

        check("S10-first batch really got 3 messages (only then does ackIndex=0 give a 'partial')",
              partial.batchSize(0) == 3, "firstBatch=" + std::to_string(partial.batchSize(0)));
        check("S10-unacknowledged tail came back from %RETRY% (reconsumeTimes>=1 and topic is the business topic)",
              tailBack, "redelivered=" + std::to_string(redelivered(partial)) + "/2");
        size_t ackedHits = 0;
        for (const Rec& r : seen) {
            if (r.body == "ack-0") ++ackedHits;
        }
        check("S10-the acknowledged one delivered only once across the window (prefix not redelivered)", ackedHits == 1,
              "arrivals=" + std::to_string(ackedHits));
        std::set<std::string> distinct;
        for (const Rec& r : seen) distinct.insert(r.body);
        check("S10-all 3 finally consumed (nothing lost)", distinct.size() == 3,
              "distinct=" + std::to_string(distinct.size()));

        size_t ctrlRetried = 0;
        const std::vector<Rec> ctrl = control.snapshot();
        for (const Rec& r : ctrl) {
            if (r.times >= 1) ++ctrlRetried;
        }
        check("S10-control group default ackIndex(MAX_VALUE): none redelivered",
              ctrl.size() == 3 && ctrlRetried == 0,
              "deliveries=" + std::to_string(ctrl.size()) + " retried=" + std::to_string(ctrlRetried));

        // Broker-side view: both groups' business-queue offsets must commit the whole batch to 3 (a partial ack is not
        // "committing less"; the tail is already handed to the broker for redelivery, this queue owes nothing)
        DefaultMQAdminExt admin(gPrefix + "_admin10");
        admin.setNamesrvAddr(nsAddr);
        try {
            admin.start();
            const std::vector<MessageQueue> queues = pc->fetchSubscribeMessageQueues(topic);
            check("S10-business topic has a queue whose offset can be queried", !queues.empty(),
                  "queues=" + std::to_string(queues.size()));
            if (!queues.empty()) {
                const MessageQueue mq = queues.front();
                // -1 = the broker has no offset for this group yet (QUERY_NOT_FOUND)
                auto readOffset = [&](const std::string& g) -> int64_t {
                    int64_t off = -1;
                    try {
                        if (!admin.examineConsumerOffset(g, mq, off)) return -1;
                    } catch (const std::exception&) {
                        return -1;
                    }
                    return off;
                };
                auto committedIs = [&](const std::string& g, int64_t want) {
                    return readOffset(g) == want;
                };
                const bool p = waitUntil([&] { return committedIs(group, 3); }, 30000);
                check("S10-after a partial ack the business-queue offset still advances the whole batch to 3", p,
                      "committed=" + std::to_string(readOffset(group)));
                const bool c = waitUntil([&] { return committedIs(controlGroup, 3); }, 30000);
                check("S10-control-group business-queue offset likewise reaches 3", c,
                      "committed=" + std::to_string(readOffset(controlGroup)));
            }
        } catch (const std::exception& e) {
            check("S10-offset query available", false, e.what());
        }
        admin.shutdown();
        pc->shutdown();
        cc->shutdown();
    }

    {
        const std::string topic11 = gPrefix + "_Heal";
        const std::string group11 = gPrefix + "_g11";
        prepareTopic(producer, topic11, 1);   // 1 queue: the injection point is unique and so is the offset criterion

        struct Rec { std::string body; int32_t times; };
        struct Sink {
            std::mutex mtx;
            std::vector<Rec> recs;
            std::vector<Rec> snapshot() {
                std::lock_guard<std::mutex> lk(mtx);
                return recs;
            }
            size_t count() {
                std::lock_guard<std::mutex> lk(mtx);
                return recs.size();
            }
        };
        class L : public MessageListenerConcurrently {
        public:
            explicit L(Sink& s) : sink_(s) {}
            ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                     ConsumeConcurrentlyContext&) override {
                std::lock_guard<std::mutex> lk(sink_.mtx);
                for (const MessageExt& m : msgs) {
                    sink_.recs.push_back({bodyOf(m), m.getReconsumeTimes()});
                }
                return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
            }

        private:
            Sink& sink_;
        };

        Sink sink;
        auto c = std::make_shared<DefaultMQPushConsumer>(group11);
        c->setNamesrvAddr(nsAddr);
        c->subscribe(topic11);
        c->setMessageListener(std::make_shared<L>(sink));
        c->start();

        const std::vector<MessageQueue> queues = c->fetchSubscribeMessageQueues(topic11);
        check("S11-queue allocated", !queues.empty(), "queues=" + std::to_string(queues.size()));
        if (!queues.empty()) {
            const std::string key = DefaultMQPushConsumer::offsetKey(queues.front());
            auto epochMs = [] {
                return std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch()).count();
            };

            for (int i = 0; i < 3; ++i) {
                producer.send(Message(topic11, str2bytes("heal-1-" + std::to_string(i))));
            }
            const bool base = waitUntil([&] { return sink.count() >= 3; }, 40000);
            check("S11-baseline: 3 messages consumed", base, "arrivals=" + std::to_string(sink.count()));

            const int64_t injected = epochMs() - kPullMaxIdleTime - 5000;
            c->setLastPullAt(key, injected);
            std::string body;
            try {
                const Bytes encoded = c->consumerRunningInfo().encode();
                body.assign(reinterpret_cast<const char*>(encoded.data()), encoded.size());
            } catch (const std::exception& e) {
                body = std::string("threw: ") + e.what();
            }
            check("S11-running info reports lastPullTimestamp as the true value",
                  body.find("\"lastPullTimestamp\":" + std::to_string(injected)) != std::string::npos,
                  "want=" + std::to_string(injected));
            check("S11-rewound past 120s is judged stalled", c->pullStalled(key), "key=" + key);

            c->syncPullThreads();
            const bool healed = waitUntil([&] {
                const int64_t now = c->lastPullAt(key);
                return now > injected && !c->pullStalled(key);
            }, 30000);
            check("S11-stalled queue was dropped and rebuilt, the new loop re-stamped it", healed,
                  "lastPullAt=" + std::to_string(c->lastPullAt(key)));

            for (int i = 0; i < 3; ++i) {
                producer.send(Message(topic11, str2bytes("heal-2-" + std::to_string(i))));
            }
            const bool resumed = waitUntil([&] { return sink.count() >= 6; }, 40000);
            check("S11-after self-healing the same queue keeps consuming (rebuild is not idle spinning)", resumed,
                  "arrivals=" + std::to_string(sink.count()));

            std::set<std::string> distinct;
            int32_t redelivered = 0;
            for (const Rec& r : sink.snapshot()) {
                distinct.insert(r.body);
                if (r.times >= 1) ++redelivered;
            }
            check("S11-each of the 6 delivered once (offset persisted before drop, resumed from broker after rebuild)",
                  distinct.size() == 6 && redelivered == 0,
                  "distinct=" + std::to_string(distinct.size())
                      + " redelivered=" + std::to_string(redelivered));
        }
        c->shutdown();
    }

    {
        const std::string topic = gPrefix + "_OrdDlq";
        const std::string group = gPrefix + "_g12";
        const int32_t kMaxReconsume = 2;
        prepareTopic(producer, topic, 1);  // 1 queue: ord-after must line up behind the poison message

        struct OrdSink {
            std::mutex mtx;
            std::vector<std::pair<int32_t, std::string>> poison;  // (reconsumeTimes, topic)
            size_t after = 0;

            size_t poisonCount() {
                std::lock_guard<std::mutex> lk(mtx);
                return poison.size();
            }
            size_t afterCount() {
                std::lock_guard<std::mutex> lk(mtx);
                return after;
            }
            std::vector<std::pair<int32_t, std::string>> snapshot() {
                std::lock_guard<std::mutex> lk(mtx);
                return poison;
            }
        };
        class OL : public MessageListenerOrderly {
        public:
            explicit OL(OrdSink& s) : sink_(s) {}
            ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                ConsumeOrderlyContext&) override {
                bool mine = false;
                {
                    std::lock_guard<std::mutex> lk(sink_.mtx);
                    for (const MessageExt& m : msgs) {
                        const std::string b = bodyOf(m);
                        if (b == "ord-poison") {
                            sink_.poison.push_back({m.getReconsumeTimes(), m.topic});
                            mine = true;
                        } else if (b == "ord-after") {
                            ++sink_.after;
                        }
                    }
                }
                return mine ? ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT
                            : ConsumeOrderlyStatus::SUCCESS;
            }

        private:
            OrdSink& sink_;
        };

        OrdSink sink;
        auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setConsumeMessageBatchMaxSize(1);
        consumer->setSuspendCurrentQueueTimeMillis(500);
        consumer->setMaxReconsumeTimes(kMaxReconsume);
        consumer->setMessageListener(std::make_shared<OL>(sink));
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        producer.send(Message(topic, str2bytes("ord-poison")));
        producer.send(Message(topic, str2bytes("ord-after")));

        const bool threeTimes = waitUntil([&] { return sink.poisonCount() >= 3; }, 60000);
        const std::vector<std::pair<int32_t, std::string>> firstThree = sink.snapshot();
        std::string ladder;
        for (const auto& p : firstThree) {
            ladder += (ladder.empty() ? "" : ",") + std::to_string(p.first);
        }
        const bool counted = firstThree.size() >= 3 && firstThree[0].first == 0
            && firstThree[1].first == 1 && firstThree[2].first == 2;
        check("S12-poison delivered exactly 3 times (local +1 each: reconsumeTimes 0/1/2)",
              threeTimes && counted, "times=[" + ladder + "]");
        const bool handedOver = waitUntil([&] { return sink.afterCount() >= 1; }, 60000);
        check("S12-after handing to the broker the business queue advances (the next one is consumed)", handedOver,
              "after=" + std::to_string(sink.afterCount()));
        std::this_thread::sleep_for(std::chrono::seconds(15));  // counter-evidence: there should be no 4th time
        check("S12-no in-place suspend after exhausting (poison delivered only 3 times within the window)",
              sink.poisonCount() == 3, "arrivals=" + std::to_string(sink.poisonCount()));
        bool topicKept = true;
        for (const auto& p : firstThree) {
            if (p.second != topic) topicKept = false;
        }
        check("S12-during suspend the listener always sees the business topic (local redelivery does not change topic)", topicKept);
        consumer->shutdown();

        const std::string dlqTopic = MixAll::getDlqTopic(group);
        const DlqSight sight = readDlq(nsAddr, dlqTopic, gPrefix + "_g12dlq", 40000);
        check("S12-broker auto-created and registered the %DLQ%<group> route", sight.routeFound,
              "dlq=" + dlqTopic);
        const bool one = sight.msgs.size() == 1 && bodyOf(sight.msgs.front()) == "ord-poison";
        check("S12-poison landed in %DLQ%<group> (orderly redelivery goes through the rebalance lock, entering the DLQ immediately)", one,
              "n=" + std::to_string(sight.msgs.size()));
        if (one) {
            const MessageExt& d = sight.msgs.front();
            // 3 = the +1 the client writes on RECONSUME_TIME, landed as the stored value via the V2 header j (not the local 2,
            // nor the concurrent-path tier); if j is left empty the broker judges by retryMaxTimes=16 and it never enters the DLQ.
            check("S12-DLQ reconsumeTimes = maxReconsumeTimes + 1",
                  d.getReconsumeTimes() == kMaxReconsume + 1,
                  "reconsumeTimes=" + std::to_string(d.getReconsumeTimes()));
            auto retryIt = d.properties.find("RETRY_TOPIC");
            check("S12-DLQ keeps RETRY_TOPIC=business topic, topic already is %DLQ%<group>",
                  retryIt != d.properties.end() && retryIt->second == topic
                      && d.topic == dlqTopic,
                  "retryTopic="
                      + (retryIt == d.properties.end() ? std::string("<missing>") : retryIt->second));
        }
    }

    {
        const std::string topic = gPrefix + "_OrdNoCap";
        const std::string group = gPrefix + "_g12b";
        prepareTopic(producer, topic, 1);

        struct CountSink {
            std::mutex mtx;
            std::vector<int32_t> times;
            size_t count() {
                std::lock_guard<std::mutex> lk(mtx);
                return times.size();
            }
            std::vector<int32_t> snapshot() {
                std::lock_guard<std::mutex> lk(mtx);
                return times;
            }
        };
        class OL : public MessageListenerOrderly {
        public:
            explicit OL(CountSink& s) : sink_(s) {}
            ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                ConsumeOrderlyContext&) override {
                {
                    std::lock_guard<std::mutex> lk(sink_.mtx);
                    for (const MessageExt& m : msgs) {
                        sink_.times.push_back(m.getReconsumeTimes());
                    }
                }
                return ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }

        private:
            CountSink& sink_;
        };

        CountSink sink;
        auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
        consumer->setNamesrvAddr(nsAddr);
        consumer->setConsumeMessageBatchMaxSize(1);
        consumer->setSuspendCurrentQueueTimeMillis(200);
        // Explicitly set no cap (the default is already -1; writing it out gives the "default" assertion a source)
        consumer->setMaxReconsumeTimes(-1);
        consumer->setMessageListener(std::make_shared<OL>(sink));
        consumer->subscribe(topic);
        consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        producer.send(Message(topic, str2bytes("ord-forever")));

        // Just crossing the concurrent-side 16 proves the right constant is used; the window is 90s because the 17th
        // redelivery's delay tier is level20 (2h) — once it goes there it never comes back, so we can only pin it
        // from both ends: "how many local deliveries + an empty %DLQ%".
        const bool uncapped = waitUntil([&] { return sink.count() >= 18; }, 90000);
        const std::vector<int32_t> times = sink.snapshot();
        int32_t maxTimes = -1;
        for (int32_t t : times) maxTimes = std::max(maxTimes, t);
        check("S12b-with maxReconsumeTimes=-1 orderly consumption sets no cap (delivered >=18 times, 16 not in effect)",
              uncapped && maxTimes >= 17,
              "arrivals=" + std::to_string(times.size()) + " maxTimes=" + std::to_string(maxTimes));
        consumer->shutdown();

        const std::string dlqTopic = MixAll::getDlqTopic(group);
        const DlqSight sight = readDlq(nsAddr, dlqTopic, gPrefix + "_g12bdlq", 20000);
        check("S12b-below the threshold there should be no DLQ (the broker did not even create the %DLQ% topic)",
              sight.msgs.empty(),
              "routeFound=" + std::string(sight.routeFound ? "true" : "false")
                  + " n=" + std::to_string(sight.msgs.size()));
    }

    // For the two clamp endpoints the same-path interval method can only catch "no busy wait" (`>= 10ms`): the dispatch loop itself
    // has a fixed 50ms cadence (in consumer.cpp it sleeps 50ms when there is no progress), so the interval method cannot tell 10ms from
    // 1ms — the exact value is pinned by the offline matrix (tests/test_orderly_reconsume.cpp::testOrderlySuspendMillisResolvesThenClamps).
    {
        const auto medianOf = [](std::vector<double> xs) {
            std::sort(xs.begin(), xs.end());
            const size_t n = xs.size();
            if (n == 0) return 0.0;
            return (n % 2 == 1) ? xs[n / 2] : (xs[n / 2 - 1] + xs[n / 2]) / 2.0;
        };

        struct GapSink {
            std::mutex mtx;
            std::vector<int64_t> stampsMs;  // delivery time of each poison-message hit
            size_t count() {
                std::lock_guard<std::mutex> lk(mtx);
                return stampsMs.size();
            }
            std::vector<double> gapsSeconds() {
                std::lock_guard<std::mutex> lk(mtx);
                std::vector<double> gaps;
                for (size_t i = 1; i < stampsMs.size(); ++i) {
                    gaps.push_back(static_cast<double>(stampsMs[i] - stampsMs[i - 1]) / 1000.0);
                }
                return gaps;
            }
        };
        class Gaps : public MessageListenerOrderly {
        public:
            Gaps(GapSink& s, std::string poison, int32_t askedMs)
                : sink_(s), poison_(std::move(poison)), askedMs_(askedMs) {}
            ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                ConsumeOrderlyContext& ctx) override {
                bool hit = false;
                for (const MessageExt& m : msgs) {
                    if (bodyOf(m) == poison_) hit = true;
                }
                if (!hit) return ConsumeOrderlyStatus::SUCCESS;
                {
                    std::lock_guard<std::mutex> lk(sink_.mtx);
                    sink_.stampsMs.push_back(nowMs());
                }
                ctx.suspendCurrentQueueTimeMillis = askedMs_;
                return ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }

        private:
            GapSink& sink_;
            std::string poison_;
            int32_t askedMs_;
        };

        // ---- probe A: config 900ms / context 70ms => the effective value is context's 70ms ----
        {
            const std::string topic = gPrefix + "_OrdSusp";
            const std::string group = gPrefix + "_g12c";
            prepareTopic(producer, topic, 1);
            GapSink sink;
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setNamesrvAddr(nsAddr);
            consumer->setConsumeMessageBatchMaxSize(1);
            consumer->setMaxReconsumeTimes(-1);  // the orderly side sets no cap, so enough intervals can be sampled
            consumer->setSuspendCurrentQueueTimeMillis(900);
            consumer->setMessageListener(
                std::make_shared<Gaps>(sink, "ord-susp-poison", 70));
            consumer->subscribe(topic);
            consumer->start();
            // Wait for the first LOCK_BATCH_MQ round: without the queue lock the broker reroutes orderly redelivery straight to the DLQ, and the interval is gone
            std::this_thread::sleep_for(std::chrono::seconds(4));
            producer.send(Message(topic, str2bytes("ord-susp-poison")));
            // At least 9 deliveries to get 8 intervals; 9 hits take about 9*120ms
            const bool enough = waitUntil([&] { return sink.count() >= 9; }, 25000);
            const std::vector<double> gaps = sink.gapsSeconds();
            const double med = medianOf(gaps);
            check("S12c-context's 70ms takes effect (not the consumer's configured 900ms)",
                  enough && gaps.size() >= 6 && med >= 0.03 && med <= 0.4,
                  "n=" + std::to_string(sink.count()) + " median=" + std::to_string(med) + "s");
            consumer->shutdown();
        }

        // ---- probe B: config 0 / context 1ms, both illegal values must clamp to the floor, never become a busy wait ----
        {
            const std::string topic = gPrefix + "_OrdSuspFloor";
            const std::string group = gPrefix + "_g12cfloor";
            prepareTopic(producer, topic, 1);
            GapSink sink;
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setNamesrvAddr(nsAddr);
            consumer->setConsumeMessageBatchMaxSize(1);
            consumer->setMaxReconsumeTimes(-1);
            consumer->setSuspendCurrentQueueTimeMillis(0);
            consumer->setMessageListener(
                std::make_shared<Gaps>(sink, "ord-susp-floor", 1));
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(4));
            producer.send(Message(topic, str2bytes("ord-susp-floor")));
            const bool enough = waitUntil([&] { return sink.count() >= 9; }, 20000);
            const std::vector<double> gaps = sink.gapsSeconds();
            const double med = medianOf(gaps);
            check("S12c-clamp floor: no busy wait at context 1ms / config 0 (interval >= 10ms)",
                  enough && gaps.size() >= 6 && med >= 0.009,
                  "n=" + std::to_string(sink.count()) + " median=" + std::to_string(med) + "s");
            consumer->shutdown();
        }

        // ---- probe C (the other half of refuting A): context stays -1 => fall back to the consumer config, not a constant 70ms ----
        // Without this, A only proves "70 takes effect" and cannot prove "-1 falls back"; an implementation that treats -1 as a
        // fixed value (or ignores the config outright) still passes A. Here config 400ms, context untouched.
        {
            const std::string topic = gPrefix + "_OrdSuspCfg";
            const std::string group = gPrefix + "_g12ccfg";
            prepareTopic(producer, topic, 1);
            GapSink sink;
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setNamesrvAddr(nsAddr);
            consumer->setConsumeMessageBatchMaxSize(1);
            consumer->setMaxReconsumeTimes(-1);
            consumer->setSuspendCurrentQueueTimeMillis(400);
            consumer->setMessageListener(
                std::make_shared<Gaps>(sink, "ord-susp-cfg", -1));
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(4));
            producer.send(Message(topic, str2bytes("ord-susp-cfg")));
            const bool enough = waitUntil([&] { return sink.count() >= 6; }, 25000);
            const std::vector<double> gaps = sink.gapsSeconds();
            const double med = medianOf(gaps);
            check("S12c-when context stays -1 it falls back to the consumer's configured 400ms",
                  enough && gaps.size() >= 4 && med >= 0.25 && med <= 0.75,
                  "n=" + std::to_string(sink.count()) + " median=" + std::to_string(med) + "s");
            consumer->shutdown();
        }
    }

    {
        enum class Manual { COMMIT_ONCE, ROLLBACK_THEN_COMMIT, ILLEGAL_ROLLBACK };

        struct ManualSink {
            std::mutex mtx;
            std::vector<std::vector<std::string>> batches;
            std::vector<std::string> bodies;
            std::vector<int32_t> times;      // reconsumeTimes of each delivery
            std::vector<std::string> topics;
            std::vector<int64_t> at;         // delivery time of each (in milliseconds)
            int32_t headCalls = 0;

            std::vector<std::vector<std::string>> batchSnapshot() {
                std::lock_guard<std::mutex> lk(mtx);
                return batches;
            }
            std::vector<std::string> bodySnapshot() {
                std::lock_guard<std::mutex> lk(mtx);
                return bodies;
            }
            size_t count(const std::string& body) {
                std::lock_guard<std::mutex> lk(mtx);
                size_t n = 0;
                for (const std::string& b : bodies) {
                    if (b == body) ++n;
                }
                return n;
            }
            int64_t firstIndex(const std::string& body) {
                std::lock_guard<std::mutex> lk(mtx);
                for (size_t i = 0; i < bodies.size(); ++i) {
                    if (bodies[i] == body) return static_cast<int64_t>(i);
                }
                return -1;
            }
            int64_t nthIndex(const std::string& body, size_t n) {
                std::lock_guard<std::mutex> lk(mtx);
                size_t seen = 0;
                for (size_t i = 0; i < bodies.size(); ++i) {
                    if (bodies[i] == body && seen++ == n) return static_cast<int64_t>(i);
                }
                return -1;
            }
            int32_t headCount() {
                std::lock_guard<std::mutex> lk(mtx);
                return headCalls;
            }
            // On a real machine each head batch holds only one message, so the delivery time is the sample point; returns adjacent gaps (seconds).
            std::vector<double> headGaps(const std::string& head) {
                std::lock_guard<std::mutex> lk(mtx);
                std::vector<double> out;
                int64_t prev = -1;
                for (size_t i = 0; i < bodies.size(); ++i) {
                    if (bodies[i] != head) continue;
                    if (prev >= 0) out.push_back((at[i] - prev) / 1000.0);
                    prev = at[i];
                }
                return out;
            }
            std::vector<int32_t> headTimes(const std::string& head) {
                std::lock_guard<std::mutex> lk(mtx);
                std::vector<int32_t> out;
                for (size_t i = 0; i < bodies.size(); ++i) {
                    if (bodies[i] == head) out.push_back(times[i]);
                }
                return out;
            }
            std::vector<std::string> headTopics(const std::string& head) {
                std::lock_guard<std::mutex> lk(mtx);
                std::vector<std::string> out;
                for (size_t i = 0; i < bodies.size(); ++i) {
                    if (bodies[i] == head) out.push_back(topics[i]);
                }
                return out;
            }
        };

        class ManualListener : public MessageListenerOrderly {
        public:
            ManualListener(ManualSink& s, Manual mode, std::string head, int32_t rollbackCalls)
                : sink_(s), mode_(mode), head_(std::move(head)), rollbackCalls_(rollbackCalls) {}

            ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                                ConsumeOrderlyContext& ctx) override {
                const int64_t at = nowMs();
                bool hit = false;
                int32_t n = 0;
                {
                    std::lock_guard<std::mutex> lk(sink_.mtx);
                    std::vector<std::string> batch;
                    batch.reserve(msgs.size());
                    for (const MessageExt& m : msgs) {
                        const std::string b = bodyOf(m);
                        if (b == head_) hit = true;
                        batch.push_back(b);
                        sink_.bodies.push_back(b);
                        sink_.times.push_back(m.getReconsumeTimes());
                        sink_.topics.push_back(m.topic);
                        sink_.at.push_back(at);
                    }
                    sink_.batches.push_back(std::move(batch));
                    if (hit) ++sink_.headCalls;
                    n = sink_.headCalls;
                }
                switch (mode_) {
                    case Manual::COMMIT_ONCE:
                        ctx.autoCommit = false;
                        return ConsumeOrderlyStatus::COMMIT;
                    case Manual::ROLLBACK_THEN_COMMIT:
                        ctx.autoCommit = false;
                        if (hit && n <= rollbackCalls_) {
                            return ConsumeOrderlyStatus::ROLLBACK;
                        }
                        return ConsumeOrderlyStatus::COMMIT;
                    case Manual::ILLEGAL_ROLLBACK:
                    default:
                        return ConsumeOrderlyStatus::ROLLBACK;
                }
            }

        private:
            ManualSink& sink_;
            Manual mode_;
            std::string head_;
            int32_t rollbackCalls_;
        };

        // One topic per segment, each route just built, so re-fetch the queues once per segment
        // (do not prefetch before creating the topic: that stuffs the fallback route into the producer's publish cache,
        //  and even after really creating a 1-queue topic it may still pick a nonexistent queueId)
        std::vector<MessageQueue> queues13;
        DefaultMQAdminExt admin(gPrefix + "_admin13");
        admin.setNamesrvAddr(nsAddr);

        {
            const std::string topic = gPrefix + "_OrdCommit";
            const std::string group = gPrefix + "_g13a";
            prepareTopic(producer, topic, 1);   // 1 queue: the offset criterion is unique

            ManualSink sink;
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setNamesrvAddr(nsAddr);
            consumer->setConsumeMessageBatchMaxSize(3);
            consumer->setSuspendCurrentQueueTimeMillis(200);
            consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
            consumer->setMessageListener(std::make_shared<ManualListener>(
                sink, Manual::COMMIT_ONCE, std::string(), 0));
            consumer->subscribe(topic);
            // Send first then start: the first batch is necessarily the full three (only "whole-batch one-time ack" gives a deterministic observation surface)
            producer.send(Message(topic, str2bytes("ack-1")));
            producer.send(Message(topic, str2bytes("ack-2")));
            producer.send(Message(topic, str2bytes("ack-3")));
            consumer->start();
            const bool firstBatch = waitUntil([&] { return !sink.batchSnapshot().empty(); }, 30000);
            // counter-evidence window: when COMMIT is treated as "not committing", this batch gets redelivered ~30 times at the suspend cadence
            std::this_thread::sleep_for(std::chrono::seconds(6));
            consumer->shutdown();

            const std::vector<std::vector<std::string>> batches = sink.batchSnapshot();
            std::string shape;
            for (const auto& b : batches) {
                shape += (shape.empty() ? "" : " | ");
                for (const auto& s : b) shape += s + ",";
            }
            const bool oneThree =
                firstBatch && batches.size() == 1 && batches.front().size() == 3;
            check("S13a-first batch is the full three-element batch (send-then-start + batch cap 3)", oneThree,
                  "batches=" + std::to_string(batches.size()) + " shape=[" + shape + "]");
            bool exact = batches.size() == 1 && batches.front().size() == 3
                && batches.front()[0] == "ack-1" && batches.front()[1] == "ack-2"
                && batches.front()[2] == "ack-3";
            check("S13a-explicit COMMIT acknowledges the whole batch: delivered once, none of the three redelivered", exact,
                  "shape=[" + shape + "]");

            queues13 = producer.fetchPublishMessageQueues(topic);
            if (queues13.empty()) {
                check("S13a-business topic has a queue whose offset can be queried", false, "queues=0");
            } else {
                admin.start();
                int64_t committed = -1;
                const MessageQueue mq = queues13.front();
                auto readOffset = [&]() -> int64_t {
                    int64_t off = -1;
                    try {
                        if (!admin.examineConsumerOffset(group, mq, off)) return -1;
                    } catch (const std::exception&) {
                        return -1;
                    }
                    return off;
                };
                const bool ok = waitUntil([&] { committed = readOffset(); return committed == 3; }, 30000);
                check("S13a-broker offset advances to 3 in one step (whole-batch ack, not stuck at 0)", ok,
                      "committed=" + std::to_string(committed));
                admin.shutdown();
            }
        }

        {
            const std::string topic = gPrefix + "_OrdRollback";
            const std::string group = gPrefix + "_g13b";
            const std::string head = "rb-head";
            const std::string next = "rb-next";
            prepareTopic(producer, topic, 1);

            ManualSink sink;
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setNamesrvAddr(nsAddr);
            consumer->setConsumeMessageBatchMaxSize(1);
            consumer->setMaxReconsumeTimes(-1);
            consumer->setSuspendCurrentQueueTimeMillis(200);
            consumer->setMessageListener(std::make_shared<ManualListener>(
                sink, Manual::ROLLBACK_THEN_COMMIT, head, 6));
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            producer.send(Message(topic, str2bytes(head)));
            producer.send(Message(topic, str2bytes(next)));

            const auto medianOf = [](std::vector<double> xs) {
                std::sort(xs.begin(), xs.end());
                const size_t n = xs.size();
                if (n == 0) return 0.0;
                return (n % 2 == 1) ? xs[n / 2] : (xs[n / 2 - 1] + xs[n / 2]) / 2.0;
            };
            const bool arrived = waitUntil([&] {
                return sink.headCount() >= 7 && sink.firstIndex(next) >= 0;
            }, 45000);
            const std::vector<double> gaps = sink.headGaps(head);
            const double med = medianOf(gaps);
            double worst = 0;
            for (double g : gaps) worst = std::max(worst, g);
            const int64_t idx7 = sink.nthIndex(head, 6);
            const int64_t idxNext = sink.firstIndex(next);
            check("S13b-explicit rollback sends the same batch back for redelivery (head delivered 7 times = 6 rollbacks + 1 commit)",
                  arrived && sink.count(head) >= 7,
                  "deliveries=" + std::to_string(sink.count(head)));
            check("S13b-local redelivery does not go through the broker: adjacent intervals hug the 200ms suspend (%RETRY%'s fastest 10s tier)",
                  gaps.size() >= 5 && med < 1.0 && worst < 5.0,
                  "n=" + std::to_string(gaps.size()) + " median=" + std::to_string(med)
                      + "s max=" + std::to_string(worst) + "s");
            const std::vector<int32_t> htimes = sink.headTimes(head);
            const std::vector<std::string> htopics = sink.headTopics(head);
            bool timesZero = !htimes.empty();
            for (int32_t t : htimes) timesZero = timesZero && t == 0;
            bool topicKept = !htopics.empty();
            for (const std::string& t : htopics) topicKept = topicKept && t == topic;
            check("S13b-local redelivery leaves reconsumeTimes untouched (all 0) and does not change topic",
                  timesZero && topicKept,
                  "times=" + std::to_string(htimes.empty() ? -1 : htimes.front())
                      + " n=" + std::to_string(htimes.size()));
            check("S13b-during rollback later messages do not overtake (next first appears after the 7th delivery)",
                  idx7 >= 0 && idxNext > idx7,
                  "idx7=" + std::to_string(idx7) + " idxNext=" + std::to_string(idxNext));
            // counter-evidence window: after commit head should not come back
            std::this_thread::sleep_for(std::chrono::seconds(2));
            check("S13b-after explicit commit each delivered once (commit really released head)",
                  sink.count(head) == 7 && sink.count(next) == 1,
                  "head=" + std::to_string(sink.count(head))
                      + " next=" + std::to_string(sink.count(next)));
            consumer->shutdown();

            queues13 = producer.fetchPublishMessageQueues(topic);
            if (!queues13.empty()) {
                admin.start();
                const MessageQueue mq = queues13.front();
                int64_t committed = -1;
                auto readOffset = [&]() -> int64_t {
                    int64_t off = -1;
                    try {
                        if (!admin.examineConsumerOffset(group, mq, off)) return -1;
                    } catch (const std::exception&) {
                        return -1;
                    }
                    return off;
                };
                const bool ok = waitUntil([&] { committed = readOffset(); return committed == 2; }, 30000);
                check("S13b-broker offset advances to 2 (both acked)", ok,
                      "committed=" + std::to_string(committed));
                admin.shutdown();
            }
        }

        {
            const std::string topic = gPrefix + "_OrdIllegal";
            const std::string group = gPrefix + "_g13c";
            const std::string head = "il-head";
            const std::string next = "il-next";
            prepareTopic(producer, topic, 1);

            ManualSink sink;
            auto consumer = std::make_shared<DefaultMQPushConsumer>(group);
            consumer->setNamesrvAddr(nsAddr);
            consumer->setConsumeMessageBatchMaxSize(1);
            consumer->setSuspendCurrentQueueTimeMillis(200);
            consumer->setMessageListener(std::make_shared<ManualListener>(
                sink, Manual::ILLEGAL_ROLLBACK, head, 0));
            consumer->subscribe(topic);
            consumer->start();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            producer.send(Message(topic, str2bytes(head)));
            producer.send(Message(topic, str2bytes(next)));

            const bool nextSeen = waitUntil([&] { return sink.count(next) >= 1; }, 30000);
            // counter-evidence window: if it really followed the rollback, head would be redelivered ~10 times within these 2.5s
            std::this_thread::sleep_for(std::chrono::milliseconds(2500));
            check("S13c-with autoCommit=true ROLLBACK is treated as ack: head delivered only once",
                  sink.count(head) == 1, "head=" + std::to_string(sink.count(head)));
            check("S13c-illegal ROLLBACK does not block the queue: next consumed immediately",
                  nextSeen && sink.count(next) == 1,
                  "next=" + std::to_string(sink.count(next)));
            consumer->shutdown();

            queues13 = producer.fetchPublishMessageQueues(topic);
            if (!queues13.empty()) {
                admin.start();
                const MessageQueue mq = queues13.front();
                int64_t committed = -1;
                auto readOffset = [&]() -> int64_t {
                    int64_t off = -1;
                    try {
                        if (!admin.examineConsumerOffset(group, mq, off)) return -1;
                    } catch (const std::exception&) {
                        return -1;
                    }
                    return off;
                };
                const bool ok = waitUntil([&] { committed = readOffset(); return committed == 2; }, 30000);
                check("S13c-broker offset advances to 2 (illegal usage treated as a successful ack)", ok,
                      "committed=" + std::to_string(committed));
                admin.shutdown();
            }
        }
        (void)queues13;
    }

    producer.shutdown();
    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}