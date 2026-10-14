// Offset advance after tag-subscription correction, live-cluster test (scenarios S1–S4 asserted one by one).
//
// The offline unit test (tests/test_correct_tags_offset.cpp) locks down the **criteria**; here we lock down two things offline cannot prove on a real cluster:
//   A. **the correction really reaches the broker**: the offset is finally persisted via UPDATE_CONSUMER_OFFSET; only a live
//      cluster can prove the committed offset on the broker advanced.
//   B. **it advanced while delivering zero messages**: when the subscription expression never matches, the broker-side
//      group-subscription filtering (PullMessageProcessor uses the SubscriptionData registered via heartbeat) → PULL_RETRY_IMMEDIATELY
//      → client NO_MATCHED_MSG. Without the correction this queue's offset would stay "uncommitted" forever.
//
// Scenarios:
//   S1 control group: TagA subscription consumes 5 messages normally — proves the messages really are in the queues, and
//      that the numeric criterion "committed offset == each queue's maxOffset" is where regular consumption lands (rules out a false green in S2).
//   S2 NO_MATCHED_MSG: TagB subscription (never matches). Assert listener 0 messages + each queue's committed offset
//      == that queue's maxOffset.
//   S3 NO_NEW_MSG: the same consumer auto-adds %RETRY%<group> at start (that queue is empty) → PULL_NOT_FOUND
//      → NO_NEW_MSG. Assert a broker record with value == maxOffset(0) appears.
//   S4 wrap-up: wait one more silent window, the listener still has 0 messages (the correction must never invent deliveries).
//
// Two criteria that offline tests cannot catch and that fail quietly when written wrong:
//   * the queue list must be taken from the **route** only: `%RETRY%<group>` is created by the broker and has
//     only 1 queue; querying with the main topic's 4 queues would misreport "only q0 has a record" as a failure.
//   * offset queries use `setZeroIfNotFound=false` to separate "no record" from "record is 0": the broker answers
//     QUERY_NOT_FOUND for the former (this function returns false), SUCCESS + offset=0 for the latter. Using true
//     would let S3 pass even when "the record was never created".
//
// Usage: ./rmq_live_correct_tags_offset 127.0.0.1:9876
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
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/route.h"

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

// Live delivery is affected by broker long polling / flow control and same-host load; a fixed sleep would turn a correct implementation into a false failure.
bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 250) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

const int32_t kQueues = 4;
const int32_t kMsgs = 5;
// First offset persist lands 10s in + periodic 5s (defaults), plus one period of headroom.
const int64_t kPersistWindowMs = 45000;

// Records only the arrival count and tags: this run cares about "not a single message should arrive", not message content.
struct TagSink {
    std::mutex mtx;
    std::vector<std::string> tags;

    size_t size() {
        std::lock_guard<std::mutex> lk(mtx);
        return tags.size();
    }
};

class TagListener : public MessageListenerConcurrently {
public:
    explicit TagListener(TagSink& sink) : sink_(sink) {}

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.tags.push_back(m.getTags());
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    TagSink& sink_;
};

struct Row {
    MessageQueue mq;
    int64_t maxOffset = -1;
    bool hasCommitted = false;  // false = no record found on the broker (QUERY_NOT_FOUND)
    int64_t committed = -1;
};

bool allCommittedEqMax(const std::vector<Row>& rows) {
    if (rows.empty()) return false;
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != r.maxOffset) return false;
    }
    return true;
}

std::string fmtRows(const std::vector<Row>& rows) {
    std::string s;
    for (const Row& r : rows) {
        if (!s.empty()) s += " ";
        s += "q" + std::to_string(r.mq.queueId) + ":" +
             (r.hasCommitted ? std::to_string(r.committed) : std::string("None")) + "/" +
             std::to_string(r.maxOffset);
    }
    return s;
}

int64_t sumMax(const std::vector<Row>& rows) {
    int64_t s = 0;
    for (const Row& r : rows) s += r.maxOffset;
    return s;
}

int64_t sumCommitted(const std::vector<Row>& rows) {
    int64_t s = 0;
    for (const Row& r : rows) s += r.hasCommitted ? r.committed : -1;
    return s;
}

struct Fixture {
    std::string namesrv;
    std::string stamp;
    std::string brokerAddr;
    DefaultMQAdminExt admin;
    std::shared_ptr<DefaultMQProducer> producer;

    void start(const std::string& ns) {
        namesrv = ns;
        stamp = std::to_string(nowMs() % 1000000);
        admin.setNamesrvAddr(ns);
        admin.start();
        ClusterInfo cluster = admin.fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = cluster.getBrokerAddrs();
        if (addrs.empty()) {
            throw std::runtime_error("no broker registered on the name server");
        }
        brokerAddr = addrs[0];
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_live_cto_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-cto-prod-" + stamp);
        producer->start();
    }

    std::string topic() const { return "CppLiveCto" + stamp; }

    // The real queue list of this topic according to the route.
    //
    // Do not assume "the main topic's queue count" universally: `%RETRY%<group>` is created by the
    // broker and has only 1 queue. Querying with the 4 main-topic queues would misreport
    // "only q0 has a record" as a failure.
    std::vector<MessageQueue> queues(const std::string& topicName) {
        TopicRouteData route = admin.examineTopicRoute(topicName);
        return route.getAllMessageQueue(topicName);
    }

    // [(mq, maxOffset, committed)]; hasCommitted=false means no record found on the broker.
    //
    // `setZeroIfNotFound=false`: the broker answers QUERY_NOT_FOUND when there is no record (returns false) and only
    // SUCCESS + offset when one exists — an empty queue record 0 and "no record" must be separable, otherwise S3 is a false green.
    std::vector<Row> offsets(const std::string& group, const std::string& topicName) {
        std::vector<Row> out;
        for (const MessageQueue& mq : queues(topicName)) {
            Row r;
            r.mq = mq;
            try {
                r.maxOffset = admin.client().getMaxOffset(mq, 5000, brokerAddr);
            } catch (const std::exception&) {
                r.maxOffset = -1;
            }
            int64_t off = 0;
            try {
                r.hasCommitted =
                    admin.client().queryConsumerOffset(group, mq, off, 5000, brokerAddr,
                                                       /*setZeroIfNotFound=*/false);
                r.committed = off;
            } catch (const std::exception&) {
                r.hasCommitted = false;
            }
            out.push_back(r);
        }
        return out;
    }

    void shutdown() {
        if (producer) {
            try {
                producer->shutdown();
            } catch (const std::exception&) {
            }
        }
        try {
            admin.shutdown();
        } catch (const std::exception&) {
        }
    }
};

std::shared_ptr<DefaultMQPushConsumer> startConsumer(Fixture& fx, const std::string& group,
                                                     const std::string& role,
                                                     const std::string& topic,
                                                     const std::string& expression, TagSink& sink,
                                                     const char* fromWhere) {
    auto c = std::make_shared<DefaultMQPushConsumer>(group);
    c->setNamesrvAddr(fx.namesrv);
    c->setInstanceName("cpp-live-cto-" + role + "-" + fx.stamp);
    if (fromWhere != nullptr) {
        c->setConsumeFromWhere(fromWhere);
    }
    c->setConsumeMessageBatchMaxSize(3);
    c->setMessageListener(std::make_shared<TagListener>(sink));
    c->subscribe(topic, expression);
    c->start();
    return c;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_correct_tags_offset <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    Fixture fx;
    try {
        fx.start(nsAddr);
    } catch (const std::exception& e) {
        check("cluster liveness probe", false, e.what());
        return 1;
    }
    check("cluster liveness probe", true, "broker=" + fx.brokerAddr);

    const std::string topic = fx.topic();
    const std::string gCtrl = "GID_cpp_live_cto_ctrl_" + fx.stamp;
    const std::string gTest = "GID_cpp_live_cto_test_" + fx.stamp;
    const std::string retryTopic = MixAll::getRetryTopic(gTest);

    std::shared_ptr<DefaultMQPushConsumer> ctrl;
    std::shared_ptr<DefaultMQPushConsumer> test;
    std::vector<Row> ctrlRows;
    std::vector<Row> testRows;
    std::vector<Row> retryRows;

    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic, kQueues);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic=%s queues=%d\n", topic.c_str(), kQueues);

        // ---------------- S1 control group ----------------
        TagSink ctrlSink;
        ctrl = startConsumer(fx, gCtrl, "ctrl", topic, "TagA", ctrlSink,
                             ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        for (int32_t i = 0; i < kMsgs; ++i) {
            Message m(topic, "cto-" + std::to_string(i));
            m.setTags("TagA");
            fx.producer->send(m);
        }
        std::printf("S1: sent %d TagA messages, waiting for the control group to consume...\n", kMsgs);
        const bool got = waitUntil([&] { return ctrlSink.size() >= static_cast<size_t>(kMsgs); }, 30000);
        check("S1-control group (TagA) received all " + std::to_string(kMsgs) + " messages — they really are in the queues",
              got && ctrlSink.size() == static_cast<size_t>(kMsgs),
              "arrivals=" + std::to_string(ctrlSink.size()));

        const bool ctrlOk = waitUntil(
            [&] {
                try {
                    ctrlRows = fx.offsets(gCtrl, topic);
                } catch (const std::exception& e) {
                    std::printf("  [WARN] read offsets: %s\n", e.what());
                    return false;
                }
                return allCommittedEqMax(ctrlRows);
            },
            25000, 500);
        check("S1-control group committed offsets == each queue maxOffset (numeric criterion)", ctrlOk,
              fmtRows(ctrlRows));
        check("S1-control group really advanced messages into the queues (maxOffset sum > 0)", sumMax(ctrlRows) > 0,
              "maxSum=" + std::to_string(sumMax(ctrlRows)));
        ctrl->shutdown();
        ctrl.reset();

        // ---------------- S2 NO_MATCHED_MSG: never-matching subscription, zero deliveries but the offset must advance ----------------
        TagSink testSink;
        test = startConsumer(fx, gTest, "test", topic, "TagB", testSink, nullptr);
        std::printf("S2: consumer (TagB, never matches) started, waiting for the empty-ack correction to land (first hop 10s + period 5s)...\n");
        const bool testOk = waitUntil(
            [&] {
                try {
                    testRows = fx.offsets(gTest, topic);
                } catch (const std::exception& e) {
                    std::printf("  [WARN] read offsets: %s\n", e.what());
                    return false;
                }
                return allCommittedEqMax(testRows);
            },
            kPersistWindowMs, 500);
        check("S2-zero deliveries (the listener received nothing)", testSink.size() == 0,
              "arrivals=" + std::to_string(testSink.size()));
        check("S2-every queue's committed offset == that queue's maxOffset (empty-ack correction works)", testOk,
              fmtRows(testRows));
        check("S2-sum of corrected offsets == the control group (max offsets of the same queues)",
              sumCommitted(testRows) == sumMax(ctrlRows),
              "test=" + std::to_string(sumCommitted(testRows)) + " ctrl=" +
                  std::to_string(sumMax(ctrlRows)));

        // ---------------- S3 NO_NEW_MSG: the empty %RETRY%<group> queue must also leave an offset record ----------------
        std::printf("S3: waiting for the offset record of %s (empty-queue NO_NEW_MSG)...\n", retryTopic.c_str());
        const bool retryOk = waitUntil(
            [&] {
                try {
                    retryRows = fx.offsets(gTest, retryTopic);
                } catch (const std::exception& e) {
                    std::printf("  [WARN] read retry offsets: %s\n", e.what());
                    return false;
                }
                return allCommittedEqMax(retryRows);
            },
            kPersistWindowMs, 500);
        check("S3-an offset record appears on " + retryTopic + " and equals maxOffset", retryOk,
              fmtRows(retryRows));
        bool allZero = !retryRows.empty();
        for (const Row& r : retryRows) {
            if (!r.hasCommitted || r.committed != 0) allZero = false;
        }
        check("S3-that offset really is 0 (the empty queue's nextBeginOffset)", allZero, fmtRows(retryRows));

        // ---------------- S4 wrap-up: still zero deliveries during the silent window ----------------
        std::this_thread::sleep_for(std::chrono::seconds(6));
        check("S4-after the whole run the listener still has 0 messages (the correction never invents deliveries)", testSink.size() == 0,
              "arrivals=" + std::to_string(testSink.size()));
    } catch (const std::exception& e) {
        check("exception thrown during verification", false, e.what());
    }

    // ---------------- cleanup ----------------
    for (const std::shared_ptr<DefaultMQPushConsumer>& c : {ctrl, test}) {
        if (!c) continue;
        try {
            c->shutdown();
        } catch (const std::exception& e) {
            std::printf("    (consumer shutdown failed: %s)\n", e.what());
        }
    }
    try {
        fx.admin.deleteTopic(topic);
        std::printf("    (deleteTopic(%s) OK)\n", topic.c_str());
    } catch (const std::exception& e) {
        std::printf("    (deleteTopic(%s) failed: %s)\n", topic.c_str(), e.what());
    }
    for (const std::string& g : {gCtrl, gTest}) {
        try {
            fx.admin.deleteSubscriptionGroup(fx.brokerAddr, g, true);
        } catch (const std::exception& e) {
            std::printf("    (deleteSubscriptionGroup(%s) failed: %s)\n", g.c_str(), e.what());
        }
    }
    fx.shutdown();

    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
