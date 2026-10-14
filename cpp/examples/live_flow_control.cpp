// Pull flow-control thresholds against a live cluster (per-scenario assertions S0~S5).
//
// Offline unit tests (tests/test_flow_control.cpp) lock down the **decision criterion itself**;
// this program locks down two things that offline tests can never prove on a real cluster:
//   A. the gate **really gets hit** on a real broker (one wrong unit digit or one wrong threshold
//      field, and the offline mock buffer would still "hit", but on a real cluster it never hits or always hits);
//   B. after a hit, **not a single message may be lost**: flow control only "pauses pulling", it
//      does not "drop/skip". While paused the offset must not pass messages not yet consumed; after resuming, the same queue must keep consuming to the end.
//
//
// ⚠ Big messages must be **incompressible** random bytes: the producer first tries to compress
//   bodies above the compression threshold; an all-identical-byte payload compresses down to a few
//   hundred bytes, the storeSize landed by the broker follows, and "the size gate never hits" becomes a fixture problem instead of an implementation problem.
// ⚠ Every scenario must **create the topic before starting the consumer**: the consumer has no
//   default-topic fallback, no route means no queue allocation; S1 must also create a topic with
//   a **single queue**, otherwise 8 × 400KB messages spread over 4 queues put only 800KB on each, never reaching the 1MiB queue-level gate.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::printf("  [%s] %s%s\n", ok ? "PASS" : "FAIL", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// Poll until the condition holds: live delivery is affected by long polling, flow-control
// backoff and same-host load, so a fixed sleep would report a correct implementation as losing messages when the machine is busy.
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return pred();
}

std::string num(int64_t v) { return std::to_string(v); }

// 400KB incompressible body: 6-byte prefix + random tail (the prefix only helps read logs; assertions always use message counts)
Bytes bigBody(int32_t tag) {
    static std::mt19937_64 rng(std::random_device{}());
    std::string body = "FCBIG-" + std::to_string(tag);
    body.reserve(400 * 1024);
    std::uniform_int_distribution<int> byteDist(1, 255);
    while (body.size() < 400u * 1024u) {
        body.push_back(static_cast<char>(byteDist(rng)));
    }
    return body;
}

Bytes bodyBytes(const std::string& s) { return Bytes(s); }

void prepareTopic(DefaultMQProducer& producer, const std::string& topic, int32_t queues) {
    try {
        producer.createTopic("init", topic, queues);
    } catch (const std::exception& e) {
        std::printf("  pre-creating topic %s failed (falling back to auto-creation): %s\n", topic.c_str(), e.what());
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

// Message-collecting listener: optionally sleeps slowMs per batch (creates a "pulled but not consumed" backlog)
class Sink : public MessageListenerConcurrently {
public:
    explicit Sink(int32_t slowMs) : slowMs_(slowMs) {}

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        if (slowMs_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(slowMs_));
        }
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) {
            bodies_.insert(m.body);
            ++got_;
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    int32_t got() {
        std::lock_guard<std::mutex> lk(mtx_);
        return got_;
    }

    size_t distinct() {
        std::lock_guard<std::mutex> lk(mtx_);
        return bodies_.size();
    }

private:
    int32_t slowMs_;
    std::mutex mtx_;
    int32_t got_ = 0;
    std::set<std::string> bodies_;
};

struct Case {
    std::shared_ptr<DefaultMQPushConsumer> consumer;
    std::shared_ptr<Sink> sink;
};

Case startConsumer(const std::string& nsAddr, const std::string& group,
                   const std::string& topic, int32_t slowMs) {
    Case c;
    c.sink = std::make_shared<Sink>(slowMs);
    c.consumer = std::make_shared<DefaultMQPushConsumer>(group);
    c.consumer->setMessageListener(c.sink);
    c.consumer->setNamesrvAddr(nsAddr);
    c.consumer->subscribe(topic);
    return c;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    const std::string prefix = "FcCpp_" + num(nowMs());

    DefaultMQProducer producer(prefix + "_pg");
    producer.setNamesrvAddr(nsAddr);
    try {
        producer.start();
    } catch (const std::exception& e) {
        std::printf("producer start failed: %s\n", e.what());
        return 2;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));

    constexpr int32_t kOffCount = 65535;
    constexpr int32_t kOffSizeMiB = 1024;

    // ---------------- S0 default gates + fast consumption: must not trigger ----------------
    {
        const std::string topic = prefix + "_Defaults";
        prepareTopic(producer, topic, 4);
        Case c = startConsumer(nsAddr, prefix + "_g0", topic, 0);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 12; ++i) {
            producer.send(Message(topic, bodyBytes("ok-" + std::to_string(i))));
        }
        const bool all = waitUntil([&] { return c.sink->got() >= 12; }, 30000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S0-default gates not triggered", fc == 0, "triggered=" + num(fc));
        check("S0-all arrive under default gates", all && c.sink->got() == 12, "got=" + num(c.sink->got()));
        check("S0-no duplicate, no loss", c.sink->distinct() == 12, "distinct=" + num(c.sink->distinct()));
    }

    // ---------------- S1 queue-level byte gate (single-queue topic) ----------------
    {
        const std::string topic = prefix + "_Size";
        prepareTopic(producer, topic, 1);
        Case c = startConsumer(nsAddr, prefix + "_g1", topic, 300);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdSizeForQueue(1);  // 1 MiB
        c.consumer->setConsumeConcurrentlyMaxSpan(kOffCount);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 8; ++i) {
            Message m(topic, bodyBytes("big-" + std::to_string(i)));
            m.body = bigBody(i);
            producer.send(m);
        }
        const bool hit = waitUntil([&] { return c.consumer->flowControlTriggered() > 0; }, 30000);
        const bool all = waitUntil([&] { return c.sink->got() >= 8; }, 40000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S1-queue-level byte gate hits on live broker", hit, "triggered=" + num(fc));
        check("S1-no big message lost", all && c.sink->got() == 8, "got=" + num(c.sink->got()));
        check("S1-all 8 400KB messages distinct", c.sink->distinct() == 8,
              "distinct=" + num(c.sink->distinct()));
    }

    // ---------------- S2 offset-span gate ----------------
    {
        const std::string topic = prefix + "_Span";
        prepareTopic(producer, topic, 4);
        Case c = startConsumer(nsAddr, prefix + "_g2", topic, 300);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdSizeForQueue(kOffSizeMiB);
        c.consumer->setConsumeConcurrentlyMaxSpan(2);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 14; ++i) {
            producer.send(Message(topic, bodyBytes("s-" + std::to_string(i))));
        }
        const bool hit = waitUntil([&] { return c.consumer->flowControlTriggered() > 0; }, 30000);
        const bool all = waitUntil([&] { return c.sink->got() >= 14; }, 40000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S2-span gate hits on live broker", hit, "triggered=" + num(fc));
        check("S2-all still consumed after span exceeded", all && c.sink->got() == 14, "got=" + num(c.sink->got()));
    }

    // ---------------- S3 topic-level count gate (accumulated across queues) ----------------
    {
        const std::string topic = prefix + "_Topic";
        prepareTopic(producer, topic, 4);
        Case c = startConsumer(nsAddr, prefix + "_g3", topic, 300);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdSizeForQueue(kOffSizeMiB);
        c.consumer->setConsumeConcurrentlyMaxSpan(kOffCount);
        c.consumer->setPullThresholdForTopic(4);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 16; ++i) {
            producer.send(Message(topic, bodyBytes("t-" + std::to_string(i))));
        }
        const bool hit = waitUntil([&] { return c.consumer->flowControlTriggered() > 0; }, 30000);
        const bool all = waitUntil([&] { return c.sink->got() >= 16; }, 40000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S3-topic-level count gate hits on live broker", hit, "triggered=" + num(fc));
        check("S3-all still consumed after cross-queue accumulation", all && c.sink->got() == 16,
              "got=" + num(c.sink->got()));
    }

    // ---------------- S4 a queue that hit flow control keeps consuming after recovery ----------------
    // Reuses S1's group and topic (S1 already committed the offset to the broker's tail). This
    // case locks the mistake of writing "pause 100ms" as "exit the pull loop" — that queue would stall forever, and the messages S1 already consumed show no difference.
    {
        const std::string topic = prefix + "_Size";
        Case c = startConsumer(nsAddr, prefix + "_g1", topic, 300);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdSizeForQueue(1);
        c.consumer->setConsumeConcurrentlyMaxSpan(kOffCount);
        c.consumer->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        for (int i = 0; i < 6; ++i) {
            Message m(topic, bodyBytes("r-" + std::to_string(i)));
            m.body = bigBody(100 + i);
            producer.send(m);
        }
        const bool all = waitUntil([&] { return c.sink->got() >= 6; }, 40000);
        const bool hit = waitUntil([&] { return c.consumer->flowControlTriggered() > 0; }, 10000);
        const int64_t fc = c.consumer->flowControlTriggered();
        c.consumer->shutdown();
        check("S4-queue that triggered flow control keeps consuming after recovery", all && c.sink->got() == 6,
              "got=" + num(c.sink->got()));
        check("S4-recovered batch still hits flow control (the gate does not expire after one hit)", hit,
              "triggered=" + num(fc));
        check("S4-recovered batch has no duplicates", c.sink->distinct() == 6, "distinct=" + num(c.sink->distinct()));
    }

    {
        const std::string topic = prefix + "_Config";
        prepareTopic(producer, topic, 4);
        const std::string goodGroup = prefix + "_g5";
        const std::string badGroup = prefix + "_g6";

        Case c = startConsumer(nsAddr, goodGroup, topic, 0);
        c.consumer->setConsumeThreadMin(1);
        c.consumer->setConsumeThreadMax(2);
        c.consumer->setConsumeConcurrentlyMaxSpan(kOffCount);
        c.consumer->setPullThresholdForQueue(kOffCount);
        c.consumer->setPullThresholdForTopic(-1);
        c.consumer->setPullThresholdSizeForQueue(kOffSizeMiB);
        c.consumer->setPullThresholdSizeForTopic(-1);
        c.consumer->setPullIntervalMillis(0);
        c.consumer->setConsumeMessageBatchMaxSize(1);
        c.consumer->setPullBatchSize(1024);
        c.consumer->setPopInvisibleTime(300000);
        c.consumer->setPopBatchNums(32);
        bool boundaryStarted = true;
        try {
            c.consumer->start();
        } catch (const std::exception& e) {
            boundaryStarted = false;
            check("S5-boundary-value config can start", false, e.what());
        }
        if (boundaryStarted) {
            check("S5-boundary-value config can start", true, "isStarted=" + num(c.consumer->isStarted()));
            std::this_thread::sleep_for(std::chrono::seconds(3));
            for (int i = 0; i < 10; ++i) {
                producer.send(Message(topic, bodyBytes("c-" + std::to_string(i))));
            }
            const bool all = waitUntil([&] { return c.sink->got() >= 10; }, 30000);
            check("S5-all 10 arrive under boundary-value config",
                  all && c.sink->got() == 10 && c.sink->distinct() == 10,
                  "got=" + num(c.sink->got()) + " distinct=" + num(c.sink->distinct()));
        }

        struct BadCase {
            const char* want;
            std::function<void(DefaultMQPushConsumer&)> apply;
        };
        const BadCase kBad[] = {
            {"pullThresholdSizeForQueue Out of range [1, 1024]",
             [](DefaultMQPushConsumer& x) { x.setPullThresholdSizeForQueue(0); }},
            {"pullBatchSize Out of range [1, 1024]",
             [](DefaultMQPushConsumer& x) { x.setPullBatchSize(1025); }},
            {"popInvisibleTime Out of range [5000, 300000]",
             [](DefaultMQPushConsumer& x) { x.setPopInvisibleTime(4999); }},
            {"popBatchNums Out of range [1, 32]",
             [](DefaultMQPushConsumer& x) { x.setPopBatchNums(33); }},
            {"consumeThreadMin (8) is larger than consumeThreadMax (4)",
             [](DefaultMQPushConsumer& x) {
                 x.setConsumeThreadMin(8);
                 x.setConsumeThreadMax(4);
             }},
        };
        for (const BadCase& b : kBad) {
            auto sink = std::make_shared<Sink>(0);
            DefaultMQPushConsumer bad(badGroup);
            bad.setMessageListener(sink);
            bad.setNamesrvAddr(nsAddr);
            bad.subscribe(topic);
            b.apply(bad);
            bool rejected = false;
            std::string actual;
            try {
                bad.start();
                actual = "start() unexpectedly succeeded";
                bad.shutdown();
            } catch (const MQClientException& e) {
                rejected = true;
                actual = e.what();
            } catch (const std::exception& e) {
                actual = std::string("other exception: ") + e.what();
            }
            check(std::string("S5-out-of-range config rejected: ") + b.want, rejected && actual == b.want,
                  "actual=" + actual);
            check(std::string("S5-out-of-range config leaves no half-started instance: ") + b.want, !bad.isStarted());
        }

        // Broker-side counter-evidence: the rejected group is not found, the boundary-value group is.
        // Must use the **raw** getConsumerListByGroup — getConsumerIdListByGroup swallows exceptions
        // internally and returns an empty list, so "rejected" and "not registered" look identical to the caller.
        if (boundaryStarted) {
            MQClientInstance probe("FC_CPP_PROBE_" + num(nowMs()), {nsAddr});
            probe.start();
            std::string addr;
            auto route = probe.getTopicRouteData(topic);
            if (route != nullptr && !route->brokerDatas.empty()) {
                addr = route->brokerDatas[0].selectBrokerAddr();
            }
            check("S5-got broker addr for consumer-group lookup", !addr.empty(), "addr=" + addr);
            if (!addr.empty()) {
                // A group that was never registered: the broker's GET_CONSUMER_LIST_BY_GROUP does
                // not return an empty list but throws code=1 "no consumer for this group" directly.
                // Both forms count as "group not found", but it **must never** return any clientId.
                bool absent = false;
                std::string detail;
                try {
                    auto ids = probe.getConsumerListByGroup(badGroup, addr, 5000);
                    absent = ids.consumerIdList.empty();
                    detail = "ids=" + num(ids.consumerIdList.size());
                } catch (const MQBrokerException& e) {
                    absent = true;
                    detail = "broker rejected directly: code=" + num(e.getResponseCode()) + " "
                           + e.getResponseMessage();
                } catch (const std::exception& e) {
                    absent = false;
                    detail = std::string("probe request failed: ") + e.what();
                }
                check("S5-broker does not know the rejected consumer group", absent, detail);

                std::string goodDetail;
                size_t goodN = 0;
                try {
                    goodN = probe.getConsumerListByGroup(goodGroup, addr, 5000)
                                .consumerIdList.size();
                } catch (const std::exception& e) {
                    goodDetail = std::string("probe failed: ") + e.what();
                }
                check("S5-broker recognizes the boundary-value consumer", goodN == 1,
                      "n=" + num(static_cast<int32_t>(goodN))
                          + (goodDetail.empty() ? "" : " " + goodDetail));
            }
            probe.shutdown();
        }
        if (boundaryStarted) {
            c.consumer->shutdown();
        }
    }

    producer.shutdown();
    std::printf("flow control live: %d PASS / %d FAIL\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
