// Illegal-offset (OFFSET_ILLEGAL) self-healing branch live-cluster test (scenarios S1/S2 asserted one by one).
//
// This branch does four things: the offset switches to the corrected value from the broker (`setNextOffset`) → drop
// the messages already fetched but not consumed on this queue (`ProcessQueue.setDropped(true)`) → persist the corrected offset
// **immediately** (`updateAndFreezeOffset` + `persist`) → remove the queue so rebalance rebuilds it
// at the corrected value (`removeProcessQueue` + `rebalanceImmediately`). The offline unit test (tests/test_offset_illegal_recover.cpp)
// can only lock how local state is cleared and which ack is voided; only a real cluster can prove the two things below:
//
//
//   S2 "persist immediately" — the corrected offset must be pushed to the broker at once, not waiting for the periodic
//      persist. Method: exploit the existing behavior that resetOffsetByQueueId's two RPCs are non-atomic (the 1st
//      commitOffset lands with no range validation; the 2nd, code 222, is rejected by resetOffsetInner — see the 5.5.1
//      measurements recorded in admin.h): plant the committed offset on the broker as the illegal value 103, then let a
//      fresh consumer with persistConsumerOffsetIntervalMillis=60000 start pulling from 103. Within the window the only
//      path that can write 103 back to 3 (maxOffset) is the persist carried by the recovery branch itself, with zero deliveries throughout.
//
//
// Usage: ./rmq_live_offset_illegal 127.0.0.1:9876
#include <chrono>
#include <condition_variable>
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

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 200) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

const int32_t kMsgs = 3;
const int64_t kIllegalTarget = 103;

std::string fmtOffsets(const std::vector<int64_t>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i != 0) s += ",";
        s += std::to_string(v[i]);
    }
    return s + "]";
}

bool contains(const std::vector<int64_t>& v, int64_t x) {
    for (int64_t y : v) {
        if (y == x) return true;
    }
    return false;
}

// The first batch is held at the gate (keeps the "1 in-flight + the rest in buffer" window); after release it returns normally.
class GatedListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            for (const MessageExt& m : msgs) {
                arrivals_.push_back(m.queueOffset);
            }
        }
        std::unique_lock<std::mutex> lk(mtx_);
        // The cap only prevents deadlock: on the normal path release() opens it explicitly, and it must happen after the recovery
        cv_.wait_for(lk, std::chrono::seconds(90), [this] { return released_; });
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    void release() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            released_ = true;
        }
        cv_.notify_all();
    }

    std::vector<int64_t> arrivals() {
        std::lock_guard<std::mutex> lk(mtx_);
        return arrivals_;
    }

private:
    std::mutex mtx_;
    std::condition_variable cv_;
    bool released_ = false;
    std::vector<int64_t> arrivals_;
};

class RecordingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) {
            arrivals_.push_back(m.queueOffset);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    std::vector<int64_t> arrivals() {
        std::lock_guard<std::mutex> lk(mtx_);
        return arrivals_;
    }

private:
    std::mutex mtx_;
    std::vector<int64_t> arrivals_;
};

struct Row {
    MessageQueue mq;
    int64_t maxOffset = -1;
    bool hasCommitted = false;  // false = no record found on the broker (QUERY_NOT_FOUND)
    int64_t committed = -1;
};

std::string fmtRows(const std::vector<Row>& rows) {
    std::string s;
    for (const Row& r : rows) {
        if (!s.empty()) s += " ";
        s += "q" + std::to_string(r.mq.queueId) + ":" +
             (r.hasCommitted ? std::to_string(r.committed) : std::string("None")) + "/max" +
             std::to_string(r.maxOffset);
    }
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
        admin.setTimeoutMillis(10000);
        admin.start();
        ClusterInfo cluster = admin.fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = cluster.getBrokerAddrs();
        if (addrs.empty()) {
            throw std::runtime_error("no broker registered on the name server");
        }
        brokerAddr = addrs[0];
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_live_ilo_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-ilo-prod-" + stamp);
        producer->start();
    }

    std::vector<MessageQueue> queues(const std::string& topicName) {
        TopicRouteData route = admin.examineTopicRoute(topicName);
        return route.getAllMessageQueue(topicName);
    }

    // [(mq, maxOffset, committed)]; hasCommitted=false means no record on the broker.
    // setZeroIfNotFound=false: an empty record of 0 and "no record" must be distinguishable.
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

// The queue key of this topic from the consumer's view (built from the routed mq, same format as offsetKey).
std::string queueKeyOf(DefaultMQPushConsumer& c, const std::string& topic) {
    for (const MessageQueue& mq : c.fetchSubscribeMessageQueues(topic)) {
        if (mq.queueId == 0) {
            return DefaultMQPushConsumer::offsetKey(mq);
        }
    }
    return std::string();
}

std::shared_ptr<DefaultMQPushConsumer> startConsumer(
    Fixture& fx, const std::string& group, const std::string& role, const std::string& topic,
    std::shared_ptr<MessageListener> listener, int32_t batchSize, int32_t persistIntervalMs) {
    auto c = std::make_shared<DefaultMQPushConsumer>(group);
    c->setNamesrvAddr(fx.namesrv);
    c->setInstanceName("cpp-live-ilo-" + role + "-" + fx.stamp);
    c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c->setConsumeMessageBatchMaxSize(batchSize);
    if (persistIntervalMs > 0) {
        c->setPersistConsumerOffsetIntervalMillis(persistIntervalMs);
    }
    c->setMessageListener(listener);
    c->subscribe(topic, "*");
    c->start();
    return c;
}

// ------------------------------------------------ S1: drop the whole batch + rebuild at the corrected offset

void s1DropAndRebuild(Fixture& fx, const std::string& topic, const std::string& group) {
    std::printf("\n--- S1: OFFSET_ILLEGAL voids in-flight/buffered messages as a batch and rebuilds at the corrected offset ---\n");
    auto listener = std::make_shared<GatedListener>();
    std::shared_ptr<DefaultMQPushConsumer> c =
        startConsumer(fx, group, "s1", topic, listener, /*batchSize=*/1, /*persist=*/0);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    for (int32_t i = 0; i < kMsgs; ++i) {
        Message m(topic, "ilo-" + std::to_string(i));
        fx.producer->send(m);
    }
    std::printf("S1: sent %d messages (single queue), waiting for the listener to be gated on the 1st...\n", kMsgs);

    std::string key = queueKeyOf(*c, topic);
    if (key.empty()) {
        check("S1-queue key obtained", false, "no q0 in the route");
        listener->release();
        c->shutdown();
        return;
    }

    size_t pending = 0;
    const bool arranged = waitUntil(
        [&] {
            pending = c->pendingMessages(key).size();
            const size_t arrived = listener->arrivals().size();
            return pending == static_cast<size_t>(kMsgs - 1) && arrived == 1;
        },
        30000);
    check("S1-window ready: 1 in flight (listener gated) + " + std::to_string(kMsgs - 1) +
              " left in the buffer",
          arranged, "pending=" + std::to_string(pending) +
                        " arrivals=" + fmtOffsets(listener->arrivals()));

    // Server-side reset to maxOffset: RPC 2 writes the resetOffsetTable, the next pull hits
    // PullMessageProcessor:539-545 and is short-circuited into OFFSET_RESET (nextBeginOffset = corrected value)
    fx.admin.resetOffsetByQueueId(fx.brokerAddr, group, topic, 0, kMsgs);
    std::printf("S1: issued resetOffsetByQueueId(->%d), waiting for the next pull to consume the server-side reset...\n", kMsgs);

    const bool dropped = waitUntil([&] { return c->queueEpoch(key) >= 1; }, 45000);
    check("S1-after the broker deems the offset illegal this side drops the queue (ProcessQueue.setDropped: epoch +1)", dropped,
          "epoch=" + std::to_string(c->queueEpoch(key)) +
              " arrivals=" + fmtOffsets(listener->arrivals()));

    const bool atCorrected = waitUntil(
        [&] {
            const std::vector<Row> rows = fx.offsets(group, topic);
            for (const Row& r : rows) {
                if (!r.hasCommitted || r.committed != kMsgs) return false;
            }
            return !rows.empty();
        },
        20000, 500);
    check("S1-the broker-side offset rests at the corrected value " + std::to_string(kMsgs) +
              " (the two reset RPCs of this scenario already wrote it once; weak assertion)",
          atCorrected, fmtRows(fx.offsets(group, topic)));

    listener->release();
    std::this_thread::sleep_for(std::chrono::seconds(6));
    std::vector<int64_t> arr = listener->arrivals();
    check("S1-the " + std::to_string(kMsgs - 1) + " fetched messages in the buffer are voided as a batch (the 1st and 2nd are never delivered)",
          !arr.empty() && !contains(arr, 1) && !contains(arr, 2), "arrivals=" + fmtOffsets(arr));

    Message after(topic, "ilo-after");
    fx.producer->send(after);
    const bool gotNew = waitUntil(
        [&] {
            const std::vector<int64_t> a = listener->arrivals();
            return contains(a, kMsgs);
        },
        20000, 500);
    arr = listener->arrivals();
    check("S1-the rebuilt queue keeps pulling from the corrected offset (the " + std::to_string(kMsgs) +
              "th new message is delivered normally; history already taken is not redelivered)",
          gotNew && !contains(arr, 1) && !contains(arr, 2), "arrivals=" + fmtOffsets(arr));

    const bool advanced = waitUntil(
        [&] {
            const std::vector<Row> rows = fx.offsets(group, topic);
            for (const Row& r : rows) {
                if (!r.hasCommitted || r.committed != kMsgs + 1) return false;
            }
            return !rows.empty();
        },
        25000, 1000);
    check("S1-the freeze lifts with the rebuild (the new message's ack keeps advancing the broker offset to " +
              std::to_string(kMsgs + 1) + ")",
          advanced, fmtRows(fx.offsets(group, topic)));
    c->shutdown();
}

// ------------------------------------------------ S2: the recovery persists the corrected offset immediately

void s2ImmediatePersist(Fixture& fx, const std::string& topic, const std::string& group) {
    std::printf("\n--- S2: the recovery persists the corrected offset immediately (not waiting for the periodic persist) ---\n");
    auto l1 = std::make_shared<RecordingListener>();
    std::shared_ptr<DefaultMQPushConsumer> c =
        startConsumer(fx, group, "s2a", topic, l1, /*batchSize=*/1, /*persist=*/0);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    for (int32_t i = 0; i < kMsgs; ++i) {
        Message m(topic, "ilo2-" + std::to_string(i));
        fx.producer->send(m);
    }
    const bool consumed = waitUntil(
        [&] { return l1->arrivals().size() >= static_cast<size_t>(kMsgs); }, 30000, 500);
    check("S2-setup: the consumer first consumes " + std::to_string(kMsgs) + " messages normally", consumed,
          "arrivals=" + fmtOffsets(l1->arrivals()));
    // shutdown persists once synchronously, so the broker offset becomes 3; it must be stopped first,
    // otherwise its periodic / shutdown persist would overwrite the illegal value planted below.
    c->shutdown();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    bool rejected = false;
    std::string remark;
    try {
        fx.admin.resetOffsetByQueueId(fx.brokerAddr, group, topic, 0, kIllegalTarget);
    } catch (const std::exception& e) {
        rejected = true;
        remark = std::string(e.what()).substr(0, 140);
    }
    check("S2-setup: the out-of-range target is rejected by resetOffsetInner (the 2nd RPC)", rejected, remark);

    std::vector<Row> rows = fx.offsets(group, topic);
    bool allIllegal = !rows.empty();
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != kIllegalTarget) allIllegal = false;
    }
    check("S2-setup: the 1st commitOffset already persisted the illegal offset " + std::to_string(kIllegalTarget) +
              " (the two RPCs are non-atomic)",
          allIllegal, fmtRows(rows));

    auto l2 = std::make_shared<RecordingListener>();
    // Stretch the periodic persist to 60s: within the window the only path that can rewrite the broker offset is the immediate persist carried by the recovery branch
    std::shared_ptr<DefaultMQPushConsumer> c2 =
        startConsumer(fx, group, "s2b", topic, l2, /*batchSize=*/1, /*persist=*/60000);
    std::printf("S2: the new consumer pulls from the illegal offset %lld, waiting for the recovery to write the broker offset back to %d (periodic persist=60s)...\n",
                static_cast<long long>(kIllegalTarget), kMsgs);
    const int64_t t0 = nowMs();

    const bool back = waitUntil(
        [&] {
            const std::vector<Row> rs = fx.offsets(group, topic);
            for (const Row& r : rs) {
                if (!r.hasCommitted || r.committed != kMsgs) return false;
            }
            return !rs.empty();
        },
        20000, 500);
    const double elapsedS = (nowMs() - t0) / 1000.0;
    char detail[160];
    std::snprintf(detail, sizeof(detail), "elapsed=%.1fs %s", elapsedS,
                  fmtRows(fx.offsets(group, topic)).c_str());
    check("S2-the broker offset is corrected from " + std::to_string(kIllegalTarget) + " back to " +
              std::to_string(kMsgs) + " (the persist inherent to the recovery branch)",
          back, detail);

    // Wait one more quiet window: after the offset is corrected it neither redelivers 0..2 nor gets rewritten again
    std::this_thread::sleep_for(std::chrono::seconds(6));
    check("S2-zero deliveries throughout (the corrected offset lands after the history messages, nothing is dispatched)",
          l2->arrivals().empty(), "arrivals=" + fmtOffsets(l2->arrivals()));
    bool still = true;
    rows = fx.offsets(group, topic);
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != kMsgs) still = false;
    }
    check("S2-after the quiet window the offset still rests at " + std::to_string(kMsgs), still && !rows.empty(),
          fmtRows(rows));
    c2->shutdown();
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_offset_illegal <namesrv>\n");
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

    const std::string topic1 = "CppLiveIloDrop" + fx.stamp;
    const std::string topic2 = "CppLiveIloPersist" + fx.stamp;
    const std::string g1 = "GID_cpp_live_ilo_drop_" + fx.stamp;
    const std::string g2 = "GID_cpp_live_ilo_persist_" + fx.stamp;

    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic1, 1);
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic2, 1);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic1=%s topic2=%s (1 queue each, %d messages)\n", topic1.c_str(),
                    topic2.c_str(), kMsgs);

        s1DropAndRebuild(fx, topic1, g1);
        s2ImmediatePersist(fx, topic2, g2);
    } catch (const std::exception& e) {
        check("exception thrown during verification", false, e.what());
    }

    for (const std::string& t : {topic1, topic2}) {
        try {
            fx.admin.deleteTopic(t);
            std::printf("    (deleteTopic(%s) OK)\n", t.c_str());
        } catch (const std::exception& e) {
            std::printf("    (deleteTopic(%s) failed: %s)\n", t.c_str(), e.what());
        }
    }
    for (const std::string& g : {g1, g2}) {
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
