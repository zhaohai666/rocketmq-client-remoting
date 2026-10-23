// Lite pull consumer (DefaultLitePullConsumer) live-machine verification.
// Usage: rmq_live_lite_pull 127.0.0.1:9876
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/allocate_strategy.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/util_all.h"

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

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

std::string join(const std::set<std::string>& s) {
    std::string out;
    for (const std::string& v : s) {
        out += v + " ";
    }
    return out;
}

std::string join(const std::vector<std::string>& v) {
    std::string out;
    for (const std::string& s : v) out += s + " ";
    return out;
}

std::vector<MessageQueue> waitAssignment(DefaultLitePullConsumer& c, int32_t timeoutMs = 20000) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        std::vector<MessageQueue> a = c.assignment();
        if (!a.empty()) return a;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return {};
}

std::vector<MessageExt> drain(DefaultLitePullConsumer& c, size_t expect, int32_t timeoutMs = 30000) {
    std::vector<MessageExt> collected;
    const int64_t deadline = nowMs() + timeoutMs;
    while (collected.size() < expect && nowMs() < deadline) {
        std::vector<MessageExt> batch = c.poll(1000);
        for (MessageExt& m : batch) collected.push_back(std::move(m));
    }
    return collected;
}

// Drain the buffer by **time** (not waiting for a count): when the strategy only assigns part of the queues, the received count is unknown
std::vector<MessageExt> drainFor(DefaultLitePullConsumer& c, int32_t millis) {
    std::vector<MessageExt> collected;
    const int64_t deadline = nowMs() + millis;
    while (nowMs() < deadline) {
        std::vector<MessageExt> batch = c.poll(1000);
        for (MessageExt& m : batch) collected.push_back(std::move(m));
    }
    return collected;
}

// queue set -> key set ("brokerName#queueId")
std::set<std::string> queueKeys(const std::vector<MessageQueue>& qs) {
    std::set<std::string> keys;
    for (const MessageQueue& q : qs) keys.insert(q.brokerName + "#" + std::to_string(q.queueId));
    return keys;
}

std::string keySetText(const std::set<std::string>& keys) {
    std::string out;
    for (const std::string& k : keys) out += k + " ";
    return out;
}

// Wait until the two instances **finish splitting** the queues (allocation convergence needs one heartbeat + rebalance round on each side), at most timeoutMs.
std::pair<std::vector<MessageQueue>, std::vector<MessageQueue>> waitSplitAssignment(
    DefaultLitePullConsumer& a, DefaultLitePullConsumer& b, size_t total, int32_t timeoutMs = 25000) {
    const int64_t deadline = nowMs() + timeoutMs;
    std::vector<MessageQueue> va, vb;
    while (nowMs() < deadline) {
        va = a.assignment();
        vb = b.assignment();
        std::set<std::string> sa = queueKeys(va), sb = queueKeys(vb);
        std::set<std::string> both, overlap;
        std::set_union(sa.begin(), sa.end(), sb.begin(), sb.end(), std::inserter(both, both.begin()));
        std::set_intersection(sa.begin(), sa.end(), sb.begin(), sb.end(),
                              std::inserter(overlap, overlap.begin()));
        // finished = union covers all queues and the two sides have no intersection
        if (both.size() == total && overlap.empty() && !sa.empty() && !sb.empty()) return {va, vb};
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return {va, vb};
}

// Attach a strategy + subscription to the lite consumer (not started yet). S7d~S7f all reuse the same configuration surface.
void setupConsumer(DefaultLitePullConsumer& c, const std::string& namesrv,
                   const std::string& instanceName,
                   const std::shared_ptr<AllocateMessageQueueStrategy>& strategy,
                   const std::string& topic) {
    c.setNamesrvAddr(namesrv);
    c.setInstanceName(instanceName);
    c.setPollTimeoutMillis(1000);
    // Existing messages were all sent back in S2; a new group defaults to LAST and skips them -> receives nothing.
    // What we verify here is that "the strategy really drives send/recv", so consume from the head of the queue.
    c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c.setAllocateMessageQueueStrategy(strategy);
    c.subscribe(topic, "*");
}

std::string snapshotText(const std::vector<DefaultLitePullConsumer*>& asserted,
                         const std::vector<std::set<std::string>>& live,
                         const std::vector<std::set<std::string>>& expected,
                         const std::vector<std::string>& cidAll) {
    std::string out;
    for (size_t i = 0; i < asserted.size(); ++i) {
        out += "cid=" + asserted[i]->clientId() + " live=[" + keySetText(live[i]) + "] predict=[" +
               keySetText(expected[i]) + "] | ";
    }
    out += "cidAll=[" + join(cidAll) + "]";
    return out;
}

/// Run the strategy OFFLINE with **real** inputs (route mqAll + real cidMembers clientId as cidAll),
/// and **wait until the online assignment() converges to this prediction**.
///
/// Why use the prediction as the convergence condition instead of "first wait for no-dup-no-loss, then compare":
/// the hash ring could well assign all 4 queues to one instance, and the other side's assignment() is
/// empty before the first rebalance -- that initial state also satisfies "no-dup-no-loss".
bool waitUntilPredictionConverged(const std::string& group, const std::vector<MessageQueue>& mqAll,
                                 const std::vector<DefaultLitePullConsumer*>& cidMembers,
                                 const std::vector<DefaultLitePullConsumer*>& asserted,
                                 const std::vector<std::shared_ptr<AllocateMessageQueueStrategy>>& strategies,
                                 std::string& detail, int32_t timeoutMs = 45000) {
    std::vector<std::string> cidAll;
    cidAll.reserve(cidMembers.size());
    for (DefaultLitePullConsumer* c : cidMembers) cidAll.push_back(c->clientId());
    std::sort(cidAll.begin(), cidAll.end());

    std::vector<std::set<std::string>> expected;
    expected.reserve(asserted.size());
    for (size_t i = 0; i < asserted.size(); ++i) {
        expected.push_back(queueKeys(strategies[i]->allocate(
            group, asserted[i]->clientId(), mqAll, cidAll)));
    }

    const int64_t deadline = nowMs() + timeoutMs;
    for (;;) {
        std::vector<std::set<std::string>> live;
        live.reserve(asserted.size());
        for (DefaultLitePullConsumer* c : asserted) live.push_back(queueKeys(c->assignment()));
        if (live == expected) {
            detail = snapshotText(asserted, live, expected, cidAll);
            return true;
        }
        if (nowMs() >= deadline) {
            detail = snapshotText(asserted, live, expected, cidAll);
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

// NEARBY landing point: the real cluster has only one broker, record both queue and client as the same machine room,
// so NEARBY must take the "own machine room" branch, equivalent to the inner strategy.
// Also keep the call record -- proving rebalance really queried the machine room of each queue and client one by one,
// rather than the strategy object just renamed itself yet never participated in allocation.
const char kRoom[] = "room1";

class OneRoom : public MachineRoomResolver {
public:
    std::string brokerDeployIn(const MessageQueue& messageQueue) override {
        std::lock_guard<std::mutex> lk(lock_);
        brokerCalls_.push_back(messageQueue.brokerName);
        return kRoom;
    }
    std::string consumerDeployIn(const std::string& clientId) override {
        std::lock_guard<std::mutex> lk(lock_);
        consumerCalls_.push_back(clientId);
        return kRoom;
    }
    std::vector<std::string> brokerCalls() const {
        std::lock_guard<std::mutex> lk(lock_);
        return brokerCalls_;
    }
    std::vector<std::string> consumerCalls() const {
        std::lock_guard<std::mutex> lk(lock_);
        return consumerCalls_;
    }

private:
    mutable std::mutex lock_;
    std::vector<std::string> brokerCalls_, consumerCalls_;
};

}  // namespace

int main(int argc, char** argv) {
    const std::string namesrv = argc > 1 ? argv[1] : std::string("127.0.0.1:9876");
    const std::string stamp = std::to_string(nowMs());
    const std::string topic = "LiteLiveCpp_" + stamp;
    const std::string group1 = "LitePG1Cpp_" + stamp;
    const std::string group2 = "LitePG2Cpp_" + stamp;
    const std::string group3 = "LitePG3Cpp_" + stamp;
    const std::string group4 = "LitePG4Cpp_" + stamp;
    const std::string group5 = "LitePG5Cpp_" + stamp;
    const std::string group6 = "LitePG6Cpp_" + stamp;
    const std::string group7 = "LitePG7Cpp_" + stamp;
    const std::string group8 = "LitePG8Cpp_" + stamp;
    const std::string group9 = "LitePG9Cpp_" + stamp;
    const std::string group10 = "LitePG10Cpp_" + stamp;  // S7d consistent hash ring
    const std::string group11 = "LitePG11Cpp_" + stamp;  // S7e machine-room nearby
    const std::string group12 = "LitePG12Cpp_" + stamp;  // S7f mismatched machine room
    const int32_t nMsg = 12;
    const int32_t queueNum = 4;

    std::printf("======================================================================\n");
    std::printf("LitePullConsumer live (C++): namesrv=%s topic=%s group=%s\n", namesrv.c_str(),
                topic.c_str(), group1.c_str());
    std::printf("======================================================================\n");

    // ---------------- create topic ----------------
    {
        DefaultMQProducer prep("PG_PrepareLiteCpp_" + stamp);
        prep.setNamesrvAddr(namesrv);
        prep.start();
        try {
            prep.createTopic("TBW102", topic, queueNum);
        } catch (const std::exception& e) {
            std::printf("!! createTopic failed: %s\n", e.what());
        }
        prep.shutdown();
    }

    // ---------------- S1 subscribe mode: start consumer first, then send messages ----------------
    std::printf("\nS1 subscribe mode start + wait for rebalance to assign offsets\n");
    DefaultLitePullConsumer c1(group1);
    c1.setNamesrvAddr(namesrv);
    c1.setPollTimeoutMillis(1000);
    c1.subscribe(topic, "*");
    c1.start();

    std::vector<MessageQueue> assigned = waitAssignment(c1);
    check("S1 rebalance assigned " + std::to_string(queueNum) + " queues",
          static_cast<int32_t>(assigned.size()) == queueNum,
          "assigned=" + std::to_string(assigned.size()));
    if (assigned.empty()) {
        std::printf("!! no queue assigned, skipping the rest\n");
        c1.shutdown();
        std::printf("\nLitePullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
        return 1;
    }

    // ---------------- S2 produce + poll to receive all ----------------
    std::printf("\nS2 produce %d messages (alternating TagA/TagB)\n", nMsg);
    std::set<std::string> sent;
    {
        DefaultMQProducer prod("PG_LiteLiveCpp_" + stamp);
        prod.setNamesrvAddr(namesrv);
        prod.start();
        int32_t sentOk = 0;
        for (int i = 0; i < nMsg; ++i) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "lite-%02d", i);
            const std::string body(buf);
            try {
                Message msg(topic, body);
                msg.setKeys(std::string("lite-key-") + buf);
                msg.setTags(i % 2 == 0 ? "TagA" : "TagB");
                SendResult r = prod.send(msg);
                if (r.getSendStatus() == SendStatus::SEND_OK) {
                    ++sentOk;
                    sent.insert(body);
                }
            } catch (const std::exception& e) {
                std::printf("   send %d failed: %s\n", i, e.what());
            }
        }
        check("S2 produced " + std::to_string(nMsg) + " messages successfully", sentOk == nMsg,
              "sentOk=" + std::to_string(sentOk));
        prod.shutdown();
    }

    std::vector<MessageExt> got = drain(c1, static_cast<size_t>(nMsg));
    {
        std::set<std::string> gotSet;
        for (const MessageExt& m : got) gotSet.insert(bodyOf(m));
        std::set<std::string> missing;
        std::set<std::string> extra;
        std::set_difference(sent.begin(), sent.end(), gotSet.begin(), gotSet.end(),
                            std::inserter(missing, missing.begin()));
        std::set_difference(gotSet.begin(), gotSet.end(), sent.begin(), sent.end(),
                            std::inserter(extra, extra.begin()));
        check("S2 subscribe+poll received all " + std::to_string(nMsg) + " messages with matching content", gotSet == sent,
              "got=" + std::to_string(gotSet.size()) + " missing=[" + join(missing) +
                  "] extra=[" + join(extra) + "]");
    }

    // ---------------- S3 auto-commit offset ----------------
    std::printf("\nS3 auto-commit offset\n");
    {
        bool allPositive = false;
        std::string detail;
        const int64_t deadline = nowMs() + 20000;
        for (;;) {
            const std::vector<MessageExt> idle = c1.poll(500);   // an idle round still counts as one poll
            (void)idle;
            allPositive = true;
            detail.clear();
            for (const MessageQueue& mq : assigned) {
                const int64_t v = c1.committed(mq);
                detail += std::to_string(v) + " ";
                if (v <= 0) allPositive = false;
            }
            if (allPositive || nowMs() >= deadline) break;
        }
        check("S3 after continued poll past the auto-commit cycle the offset lands on disk by itself (nobody called commit)", allPositive,
              "committed=" + detail);
    }

    // ---------------- S4 assign mode: assign all queues + seek to head to re-receive all ----------------
    std::printf("\nS4 assign mode: assign all queues + seek to head to re-receive all\n");
    {
        DefaultLitePullConsumer c2(group2);
        c2.setNamesrvAddr(namesrv);
        c2.setPollTimeoutMillis(1000);
        c2.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        std::vector<MessageQueue> allQueues;
        try {
            allQueues = c1.fetchMessageQueues(topic);
        } catch (const std::exception& e) {
            check("S4 fetchMessageQueues", false, e.what());
        }
        c2.assign(allQueues);
        c2.start();
        for (const MessageQueue& mq : allQueues) {
            c2.seekToBegin(mq);
        }
        std::vector<MessageExt> got2 = drain(c2, static_cast<size_t>(nMsg));
        std::set<std::string> got2Set;
        for (const MessageExt& m : got2) got2Set.insert(bodyOf(m));
        check("S4 assign+seek+poll re-received all " + std::to_string(nMsg) + " messages", got2Set == sent,
              "got=" + std::to_string(got2Set.size()));
        c2.shutdown();
    }

    // ---------------- S5 subscription-level tag filter ----------------
    std::printf("\nS5 subscribe TagA: only receive the 6 TagA messages\n");
    {
        DefaultLitePullConsumer c3(group3);
        c3.setNamesrvAddr(namesrv);
        c3.setPollTimeoutMillis(1000);
        c3.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c3.subscribe(topic, "TagA");
        c3.start();
        waitAssignment(c3);
        std::vector<MessageExt> got3 = drain(c3, 6);
        std::set<std::string> got3Set;
        for (const MessageExt& m : got3) got3Set.insert(bodyOf(m));
        bool onlyA = got3Set.size() == 6;
        for (const std::string& b : got3Set) {
            // body looks like lite-00..lite-11: only when the last (units) digit is even is it TagA
            if (b.size() == 7 && (b[6] - '0') % 2 != 0) onlyA = false;
        }
        check("S5 only TagA received and exactly 6 messages", onlyA, "got=" + std::to_string(got3Set.size()));
        c3.shutdown();
    }

    // ---------------- S6 CONSUME_FROM_TIMESTAMP: 14-digit local wall clock ----------------
    std::printf("\nS6 CONSUME_FROM_TIMESTAMP: wall-clock start receives all + timestamp-to-offset mapping\n");
    {
        const int64_t wall = UtilAll::currentTimeMillis();

        DefaultLitePullConsumer c4(group4);
        c4.setNamesrvAddr(namesrv);
        c4.setPollTimeoutMillis(1000);
        c4.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_TIMESTAMP);
        c4.setConsumeTimestamp(UtilAll::timeMillisToHumanString3(wall - 30 * 60 * 1000));
        c4.subscribe(topic, "*");
        c4.start();
        waitAssignment(c4);
        std::vector<MessageExt> got4 = drain(c4, static_cast<size_t>(nMsg));
        std::set<std::string> got4Set;
        for (const MessageExt& m : got4) got4Set.insert(bodyOf(m));
        check("S6a start=" + c4.consumeTimestamp() + " earlier than all messages -> receive all " +
                  std::to_string(nMsg) + " messages",
              got4Set == sent, "got=" + std::to_string(got4Set.size()));
        c4.shutdown();

        DefaultLitePullConsumer c5(group5);
        c5.setNamesrvAddr(namesrv);
        c5.setPollTimeoutMillis(1000);
        c5.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_TIMESTAMP);
        c5.setConsumeTimestamp(UtilAll::timeMillisToHumanString3(wall + 10 * 60 * 1000));
        c5.subscribe(topic, "*");
        c5.start();
        std::vector<MessageQueue> q5 = waitAssignment(c5);
        int64_t sumPast = 0, sumFuture = 0;
        for (const MessageQueue& mq : q5) {
            sumPast += c5.offsetForTimestamp(mq, wall - 30 * 60 * 1000);
            sumFuture += c5.offsetForTimestamp(mq, wall + 10 * 60 * 1000);
        }
        check("S6b 30 minutes ago -> head of each queue", sumPast == 0,
              "sumOffset=" + std::to_string(sumPast));
        check("S6b 10 minutes later -> past all " + std::to_string(nMsg) + " messages",
              sumFuture == nMsg, "sumOffset=" + std::to_string(sumFuture));
        c5.shutdown();
    }

    // ---------------- S7 pluggable queue allocation strategy ----------------
    std::printf("\nS7 pluggable queue allocation strategy (maps to setAllocateMessageQueueStrategy)\n");
    {
        std::vector<MessageQueue> allQueues;
        try {
            allQueues = c1.fetchMessageQueues(topic);
        } catch (const std::exception& e) {
            check("S7 fetchMessageQueues", false, e.what());
        }
        std::sort(allQueues.begin(), allQueues.end());
        const size_t totalQueues = allQueues.size();

        DefaultLitePullConsumer probe(group6);
        probe.setNamesrvAddr(namesrv);
        check("S7a default strategy name = AVG",
              probe.allocateMessageQueueStrategy() != nullptr
                  && probe.allocateMessageQueueStrategy()->getName() == "AVG",
              "name=" + (probe.allocateMessageQueueStrategy()
                             ? probe.allocateMessageQueueStrategy()->getName()
                             : std::string("<null>")));
        probe.setAllocateMessageQueueStrategy(nullptr);
        bool nullRejected = false;
        try {
            probe.subscribe(topic, "*");
            probe.start();
        } catch (const std::exception& e) {
            nullRejected =
                std::string(e.what()).find("allocateMessageQueueStrategy is null") != std::string::npos;
            check("S7a when strategy is null start() reports the protocol-style message", nullRejected, e.what());
        }
        if (!nullRejected) probe.shutdown();

        // S7b AVG_BY_CIRCLE: two instances in the same group split interleaved, no dup no loss
        auto circle = std::make_shared<AllocateMessageQueueAveragelyByCircle>();
        DefaultLitePullConsumer ca(group7), cb(group7);
        ca.setNamesrvAddr(namesrv);
        cb.setNamesrvAddr(namesrv);
        ca.setInstanceName("s7ca");
        cb.setInstanceName("s7cb");
        ca.setAllocateMessageQueueStrategy(circle);
        cb.setAllocateMessageQueueStrategy(circle);
        check("S7b strategy name after replacement = AVG_BY_CIRCLE",
              ca.allocateMessageQueueStrategy()->getName() == "AVG_BY_CIRCLE");
        ca.subscribe(topic, "*");
        cb.subscribe(topic, "*");
        ca.start();
        cb.start();
        std::pair<std::vector<MessageQueue>, std::vector<MessageQueue>> split =
            waitSplitAssignment(ca, cb, totalQueues);
        std::set<std::string> ka = queueKeys(split.first), kb = queueKeys(split.second);
        std::set<std::string> overlap, both;
        std::set_intersection(ka.begin(), ka.end(), kb.begin(), kb.end(),
                              std::inserter(overlap, overlap.begin()));
        std::set_union(ka.begin(), ka.end(), kb.begin(), kb.end(), std::inserter(both, both.begin()));
        check("S7b two instances have disjoint assignments", overlap.empty(), "overlap=[" + keySetText(overlap) + "]");
        check("S7b union covers all " + std::to_string(totalQueues) + " queues", both == queueKeys(allQueues),
              "a=" + std::to_string(ka.size()) + " b=" + std::to_string(kb.size()));
        // Signature of ring allocation: interleaved queues by index modulo, not a contiguous run
        // (4 queues / 2 instances -> 2 each with index stride 2; AVG would give two contiguous runs).
        std::set<int32_t> posA;
        for (size_t i = 0; i < allQueues.size(); ++i) {
            if (ka.count(allQueues[i].brokerName + "#" + std::to_string(allQueues[i].queueId))) {
                posA.insert(static_cast<int32_t>(i));
            }
        }
        bool circleShape = posA.size() == 2;
        int32_t prev = -1;
        for (int32_t p : posA) {
            if (prev >= 0 && (p - prev) % 2 != 0) circleShape = false;
            prev = p;
        }
        check("S7b assignment shape is interleaved (stride 2), not AVG's contiguous run",
              totalQueues != 4 || circleShape, "posA=" + std::to_string(posA.size()));
        ca.shutdown();
        cb.shutdown();

        // S7c CONFIG: only assign the configured half of queues, polled messages can only come from those queues
        std::vector<MessageQueue> halfA(allQueues.begin(), allQueues.begin() + totalQueues / 2);
        std::vector<MessageQueue> halfB(allQueues.begin() + totalQueues / 2, allQueues.end());
        auto cfgA = std::make_shared<AllocateMessageQueueByConfig>(halfA);
        auto cfgB = std::make_shared<AllocateMessageQueueByConfig>(halfB);
        DefaultLitePullConsumer c6(group8), c7(group9);
        c6.setNamesrvAddr(namesrv);
        c7.setNamesrvAddr(namesrv);
        c6.setPollTimeoutMillis(1000);
        c7.setPollTimeoutMillis(1000);
        c6.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c7.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c6.setAllocateMessageQueueStrategy(cfgA);
        c7.setAllocateMessageQueueStrategy(cfgB);
        c6.subscribe(topic, "*");
        c7.subscribe(topic, "*");
        c6.start();
        c7.start();
        std::vector<MessageQueue> a6 = waitAssignment(c6);
        std::vector<MessageQueue> a7 = waitAssignment(c7);
        check("S7c CONFIG only gives the configured queues (ignores mqAll/cidAll)",
              queueKeys(a6) == queueKeys(halfA) && queueKeys(a7) == queueKeys(halfB),
              "a=" + keySetText(queueKeys(a6)) + " b=" + keySetText(queueKeys(a7)));
        // Two CONFIG consumers each see half: union is exactly all 12, intersection empty -> strategy really drives send/recv
        std::vector<MessageExt> g6 = drainFor(c6, 8000);
        std::vector<MessageExt> g7 = drainFor(c7, 8000);
        std::set<std::string> b6, b7, onCfgA, onCfgB;
        std::set<std::string> cfgKeysA = queueKeys(halfA), cfgKeysB = queueKeys(halfB);
        for (const MessageExt& m : g6) {
            b6.insert(bodyOf(m));
            if (cfgKeysA.count(m.getBrokerName() + "#" + std::to_string(m.getQueueId()))) {
                onCfgA.insert(bodyOf(m));
            }
        }
        for (const MessageExt& m : g7) {
            b7.insert(bodyOf(m));
            if (cfgKeysB.count(m.getBrokerName() + "#" + std::to_string(m.getQueueId()))) {
                onCfgB.insert(bodyOf(m));
            }
        }
        check("S7c CONFIG consumer only receives messages from its own configured queues", b6 == onCfgA && b7 == onCfgB,
              "a=" + std::to_string(b6.size()) + " aOnCfg=" + std::to_string(onCfgA.size()) + " b="
                  + std::to_string(b7.size()) + " bOnCfg=" + std::to_string(onCfgB.size()));
        std::set<std::string> unionBodies, interBodies;
        std::set_union(b6.begin(), b6.end(), b7.begin(), b7.end(),
                       std::inserter(unionBodies, unionBodies.begin()));
        std::set_intersection(b6.begin(), b6.end(), b7.begin(), b7.end(),
                              std::inserter(interBodies, interBodies.begin()));
        check("S7c the two halves exactly cover all " + std::to_string(nMsg) + " messages without overlap",
              unionBodies == sent && interBodies.empty(),
              "union=" + std::to_string(unionBodies.size()) + " inter=" + std::to_string(interBodies.size()));
        c6.shutdown();
        c7.shutdown();

        {
            auto ch = std::make_shared<AllocateMessageQueueConsistentHash>();
            DefaultLitePullConsumer d1(group10), d2(group10);
            setupConsumer(d1, namesrv, "s7da", ch, topic);
            setupConsumer(d2, namesrv, "s7db", ch, topic);
            check("S7d strategy name after replacement = CONSISTENT_HASH",
                  d1.allocateMessageQueueStrategy()->getName() == "CONSISTENT_HASH",
                  d1.allocateMessageQueueStrategy()->getName());
            d1.start();
            d2.start();
            std::string detail;
            const bool converged = waitUntilPredictionConverged(
                group10, allQueues, {&d1, &d2}, {&d1, &d2}, {ch, ch}, detail);
            check("S7d online assignment converges to the consistent-ring offline prediction", converged, detail);
            std::set<std::string> k1 = queueKeys(d1.assignment()), k2 = queueKeys(d2.assignment());
            std::set<std::string> hit, both;
            std::set_intersection(k1.begin(), k1.end(), k2.begin(), k2.end(),
                                  std::inserter(hit, hit.begin()));
            std::set_union(k1.begin(), k1.end(), k2.begin(), k2.end(), std::inserter(both, both.begin()));
            check("S7d two instances have disjoint assignments", hit.empty(), "overlap=[" + keySetText(hit) + "]");
            check("S7d union covers all " + std::to_string(totalQueues) + " queues",
                  both == queueKeys(allQueues),
                  "a=" + std::to_string(k1.size()) + " b=" + std::to_string(k2.size()));
            std::vector<MessageExt> g1 = drainFor(d1, 8000), g2 = drainFor(d2, 8000);
            std::set<std::string> b1, b2;
            for (const MessageExt& m : g1) b1.insert(bodyOf(m));
            for (const MessageExt& m : g2) b2.insert(bodyOf(m));
            std::set<std::string> joined;
            std::set_union(b1.begin(), b1.end(), b2.begin(), b2.end(),
                           std::inserter(joined, joined.begin()));
            check("S7d two instances together received all " + std::to_string(nMsg) + " messages (the ring really drives send/recv)",
                  joined == sent, "union=" + std::to_string(joined.size()));
            d1.shutdown();
            d2.shutdown();
        }

        // S7e MACHINE_ROOM_NEARBY: the real cluster has only one machine room => the decorator must pass the inner strategy through unchanged.
        {
            auto inner = std::make_shared<AllocateMessageQueueConsistentHash>();
            auto resolver = std::make_shared<OneRoom>();
            auto nearby = std::make_shared<AllocateMachineRoomNearby>(inner, resolver);
            DefaultLitePullConsumer e1(group11), e2(group11);
            setupConsumer(e1, namesrv, "s7ea", nearby, topic);
            setupConsumer(e2, namesrv, "s7eb", nearby, topic);
            check("S7e decorated strategy name = MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
                  e1.allocateMessageQueueStrategy()->getName() ==
                      "MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
                  e1.allocateMessageQueueStrategy()->getName());
            e1.start();
            e2.start();
            std::string detail;
            const bool converged = waitUntilPredictionConverged(
                group11, allQueues, {&e1, &e2}, {&e1, &e2}, {inner, inner}, detail);
            check("S7e NEARBY online assignment == inner ring offline prediction", converged, detail);
            std::set<std::string> k1 = queueKeys(e1.assignment()), k2 = queueKeys(e2.assignment());
            std::set<std::string> hit, both;
            std::set_intersection(k1.begin(), k1.end(), k2.begin(), k2.end(),
                                  std::inserter(hit, hit.begin()));
            std::set_union(k1.begin(), k1.end(), k2.begin(), k2.end(), std::inserter(both, both.begin()));
            check("S7e NEARBY two instances disjoint and no loss",
                  hit.empty() && both == queueKeys(allQueues),
                  "a=" + std::to_string(k1.size()) + " b=" + std::to_string(k2.size()) +
                      " overlap=" + std::to_string(hit.size()));
            // the resolver really was called by rebalance, seeing the real brokerName + two real clientIds.
            const std::vector<std::string> brokerCalls = resolver->brokerCalls();
            const std::vector<std::string> consumerCalls = resolver->consumerCalls();
            std::set<std::string> seenBrokers(brokerCalls.begin(), brokerCalls.end());
            std::set<std::string> realBrokers;
            for (const MessageQueue& mq : allQueues) realBrokers.insert(mq.brokerName);
            check("S7e resolver asked each queue's machine room (" + std::to_string(brokerCalls.size()) + " times)",
                  !brokerCalls.empty() && seenBrokers == realBrokers,
                  "brokers=[" + join(seenBrokers) + "]");
            std::set<std::string> callSet(consumerCalls.begin(), consumerCalls.end());
            check("S7e resolver was asked about two real clientIds",
                  callSet.count(e1.clientId()) == 1 && callSet.count(e2.clientId()) == 1,
                  "calls=[" + join(std::vector<std::string>(callSet.begin(), callSet.end())) + "]");
            e1.shutdown();
            e2.shutdown();
        }

        {
            auto room = std::make_shared<AllocateMessageQueueByMachineRoom>(
                std::set<std::string>{kRoom});
            check("S7f strategy name = MACHINE_ROOM and whitelist reads back",
                  room->getName() == "MACHINE_ROOM" && room->getConsumeridcs().count(kRoom) == 1,
                  "idcs=[" + join(room->getConsumeridcs()) + "]");
            auto avg = std::make_shared<AllocateMessageQueueAveragely>();
            DefaultLitePullConsumer f1(group12), f2(group12);
            setupConsumer(f1, namesrv, "s7fa", room, topic);
            setupConsumer(f2, namesrv, "s7fb", avg, topic);
            f1.start();
            f2.start();
            std::vector<MessageQueue> ctrl = waitAssignment(f2);
            check("S7f same-group control (AVG) assigns queues normally", !ctrl.empty(),
                  "ctrl=[" + keySetText(queueKeys(ctrl)) + "]");
            check("S7f machine room mismatching the real brokerName -> assign nothing (no error, no wrong consumption)",
                  f1.assignment().empty(), "assignment=[" + keySetText(queueKeys(f1.assignment())) + "]");
            std::string detail;
            const bool converged = waitUntilPredictionConverged(
                group12, allQueues, {&f1, &f2}, {&f1, &f2}, {room, avg}, detail);
            check("S7f both sides' online assignments each converge to their own strategy's offline prediction", converged, detail);
            std::vector<MessageExt> starved = drainFor(f1, 5000);
            check("S7f the starved side polls no messages and throws no error", starved.empty(),
                  "got=" + std::to_string(starved.size()));
            f1.shutdown();
            f2.shutdown();
        }
    }

    c1.shutdown();

    std::printf("\nS8 three offset tables: pull cursor / consumed cursor / commit landing point\n");
    {
        const std::string offTopic = "LiteOffCpp_" + stamp;
        const std::string offGroup = "LiteOffPGCpp_" + stamp;
        const int32_t nBig = 1200;      // > the 1024 per-poll cap => one delivery necessarily leaves an undelivered tail
        {
            DefaultMQProducer prep("PG_PrepareOffCpp_" + stamp);
            prep.setNamesrvAddr(namesrv);
            prep.start();
            try {
                prep.createTopic("TBW102", offTopic, 1);
            } catch (const std::exception& e) {
                std::printf("!! createTopic(%s) failed: %s\n", offTopic.c_str(), e.what());
            }
            std::vector<MessageQueue> qs;
            const int64_t routeDeadline = nowMs() + 20000;
            while (qs.empty() && nowMs() < routeDeadline) {
                try {
                    qs = prep.fetchPublishMessageQueues(offTopic);
                } catch (...) {
                }
                if (qs.empty()) std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            check("S8 prepare the topic (1 queue)", qs.size() == 1,
                  "queues=" + std::to_string(qs.size()));
            int32_t landed = 0;
            if (qs.size() == 1) {
                for (int32_t from = 0; from < nBig; from += 300) {
                    std::vector<Message> chunk;
                    for (int32_t i = from; i < std::min(nBig, from + 300); ++i) {
                        char buf[32];
                        std::snprintf(buf, sizeof(buf), "off-%04d", i);
                        chunk.emplace_back(offTopic, std::string(buf));
                    }
                    try {
                        SendResult r = prep.sendBatch(chunk, qs[0]);
                        if (r.getSendStatus() == SendStatus::SEND_OK) {
                            landed += static_cast<int32_t>(chunk.size());
                        }
                    } catch (const std::exception& e) {
                        std::printf("   sendBatch failed: %s\n", e.what());
                    }
                }
            }
            prep.shutdown();
            check("S8 produced " + std::to_string(nBig) + " messages successfully", landed == nBig,
                  "landed=" + std::to_string(landed));

            // Probe that reads broker offsets independently (own group, own connection, touching none of the tested instance's memory)
            DefaultMQPullConsumer probe(offGroup);
            probe.setNamesrvAddr(namesrv);
            probe.start();
            const MessageQueue q0 = qs.empty() ? MessageQueue(offTopic, "broker-a", 0) : qs[0];

            DefaultLitePullConsumer o(offGroup);
            o.setNamesrvAddr(namesrv);
            o.setInstanceName("liteoff");
            o.setPollTimeoutMillis(1000);
            o.setPullBatchSize(32);
            // Every cache gate is lifted above this scenario's backlog on purpose: S8 buffers 1200 messages in one
            // queue and the default per-queue count budget (1000) would stop the background pull at 1024 -- that is
            // the gate working (S9 measures it), not a defect. S8 is about the three offset tables.
            o.setPullThresholdForAll(nBig * 32);
            o.setPullThresholdForQueue(nBig + 100);
            o.setPullThresholdSizeForQueue(1024);
            o.setConsumeMaxSpan(nBig + 1000);
            o.setAutoCommit(false);          // commit timing is fully controlled by the scenario
            o.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
            o.assign({q0});
            o.start();

            // S8a pull only, deliver nothing: the pull cursor runs up to 1200, the consumed cursor may not move a single step
            std::vector<MessageExt> all;
            const int64_t pullDeadline = nowMs() + 30000;
            while (o.pullCursorOf(q0) < nBig && nowMs() < pullDeadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            check("S8a the background pulled " + std::to_string(nBig) + " messages into the local buffer (pullCursor=" +
                      std::to_string(o.pullCursorOf(q0)) + ")",
                  o.pullCursorOf(q0) == nBig, "pullCursor=" + std::to_string(o.pullCursorOf(q0)));
            check("S8a nothing delivered => the consumed cursor stays at -1", o.consumeCursorOf(q0) == -1,
                  "consumeCursor=" + std::to_string(o.consumeCursorOf(q0)));
            // This is the core of #68: the old commit() iterated the pull cursor, so it would publish 1200 here,
            // while the caller crashed before that => 1200 messages never consumed yet never redelivered.
            o.commit();
            int64_t brokerAfterEmptyCommit = -1;
            probe.fetchConsumeOffset(q0, brokerAfterEmptyCommit);
            check("S8a never delivered => the broker offset was not pushed forward (the old implementation committed 1200 here)",
                  brokerAfterEmptyCommit != nBig,
                  "brokerOffset=" + std::to_string(brokerAfterEmptyCommit));

            // S8b the 1024 per-poll cap < the 1200 in the buffer => the 176-message tail does not count as consumed
            std::vector<MessageExt> first = o.poll(3000);
            check("S8b one poll hands out 1024 messages (per-delivery cap)", first.size() == 1024,
                  "got=" + std::to_string(first.size()));
            check("S8b consumed cursor = the slot that was handed out", o.consumeCursorOf(q0) == 1024,
                  "consumeCursor=" + std::to_string(o.consumeCursorOf(q0)));
            check("S8b the buffer still holds " + std::to_string(nBig - 1024) + " undelivered messages",
                  o.pullCursorOf(q0) == nBig && o.consumeCursorOf(q0) < o.pullCursorOf(q0),
                  "pull=" + std::to_string(o.pullCursorOf(q0)));
            o.commit();
            int64_t brokerOffset = -1;
            probe.fetchConsumeOffset(q0, brokerOffset);
            check("S8b what gets committed to the broker is exactly 1024 (not 1200)", brokerOffset == 1024,
                  "brokerOffset=" + std::to_string(brokerOffset));
            for (MessageExt& m : first) all.push_back(std::move(m));

            // S8c commit an earlier offset: only the commit landing point changes, neither cursor may move
            const int64_t pullBefore = o.pullCursorOf(q0);
            const int64_t consumeBefore = o.consumeCursorOf(q0);
            std::map<MessageQueue, int64_t> rewind;
            rewind[q0] = 5;
            o.commit(rewind, true);
            int64_t brokerRewound = -1;
            probe.fetchConsumeOffset(q0, brokerRewound);
            check("S8c commit(map) moves the broker offset to the caller-specified 5", brokerRewound == 5,
                  "brokerOffset=" + std::to_string(brokerRewound));
            check("S8c committing an offset does not move the pull cursor", o.pullCursorOf(q0) == pullBefore,
                  "pullCursor=" + std::to_string(o.pullCursorOf(q0)));
            check("S8c committing an offset does not move the consumed cursor", o.consumeCursorOf(q0) == consumeBefore,
                  "consumeCursor=" + std::to_string(o.consumeCursorOf(q0)));
            // after the offset rewinds to 5, the 176-message tail is still delivered (the buffer does not depend on the broker offset)
            std::vector<MessageExt> tail = o.poll(3000);
            all.insert(all.end(), std::make_move_iterator(tail.begin()),
                       std::make_move_iterator(tail.end()));
            check("S8c after rewinding to 5 the " + std::to_string(nBig - 1024) + " messages left in the buffer are still delivered",
                  static_cast<int64_t>(tail.size()) == nBig - 1024,
                  "got=" + std::to_string(tail.size()));
            {
                std::set<std::string> bodies;
                for (const MessageExt& m : all) bodies.insert(bodyOf(m));
                check("S8 the whole run received all " + std::to_string(nBig) + " messages with no duplicate and no loss",
                      bodies.size() == static_cast<size_t>(nBig),
                      "distinct=" + std::to_string(bodies.size()));
            }

            // S8d not a single byte may go on the wire: committed() sees it, the broker does not
            std::map<MessageQueue, int64_t> memoryOnly;
            memoryOnly[q0] = 777;
            o.commit(memoryOnly, false);
            check("S8d persist=false: committed() reads the 777 from the in-memory table",
                  o.committed(q0) == 777, "committed=" + std::to_string(o.committed(q0)));
            int64_t brokerStill = -1;
            probe.fetchConsumeOffset(q0, brokerStill);
            check("S8d persist=false: the broker side still holds the previous round's 5", brokerStill == 5,
                  "brokerOffset=" + std::to_string(brokerStill));

            // S8e a new instance (same group) resumes consumption from the broker's slot: the in-memory table is not shared across instances
            DefaultLitePullConsumer o2(offGroup);
            o2.setNamesrvAddr(namesrv);
            o2.setInstanceName("liteoff2");
            o2.setPullBatchSize(32);
            o2.setAutoCommit(false);
            o2.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
            o2.assign({q0});
            o2.start();
            check("S8e the new instance starts at the broker's 5 (not another instance's in-memory 777)",
                  o2.pullCursorOf(q0) == 5, "pullCursor=" + std::to_string(o2.pullCursorOf(q0)));
            o2.shutdown();

            // S8f seek rewrites both cursors: a replayed span must not be skipped by the old offset
            o.seek(q0, 60);
            check("S8f seek moves the pull cursor", o.pullCursorOf(q0) == 60,
                  "pullCursor=" + std::to_string(o.pullCursorOf(q0)));
            check("S8f seek also moves the consumed cursor", o.consumeCursorOf(q0) == 60,
                  "consumeCursor=" + std::to_string(o.consumeCursorOf(q0)));
            o.commit();
            int64_t brokerSeeked = -1;
            probe.fetchConsumeOffset(q0, brokerSeeked);
            check("S8f the commit after seek lands on 60", brokerSeeked == 60,
                  "brokerOffset=" + std::to_string(brokerSeeked));

            const MessageQueue foreign = assigned[1];   // a queue of another topic, offGroup never touched it
            o.assign({q0, foreign});                   // adds one cursor only, does not affect the pairs reconciled above
            std::map<MessageQueue, int64_t> memoryOnly2;
            memoryOnly2[q0] = 300;
            o.commit(memoryOnly2, false);              // in-memory table only
            check("S8g the unpersisted in-memory value is visible first", o.committed(q0) == 300,
                  "committed=" + std::to_string(o.committed(q0)));
            std::set<MessageQueue> none;
            o.commit(none, true);
            check("S8g an empty set neither clears the table nor sends anything", o.committed(q0) == 300,
                  "committed=" + std::to_string(o.committed(q0)));
            int64_t brokerStill2 = -1;
            probe.fetchConsumeOffset(q0, brokerStill2);
            check("S8g the empty set left the broker untouched", brokerStill2 == 60,
                  "brokerOffset=" + std::to_string(brokerStill2));
            // Naming one foreign queue: it has no consumption record (the -1 guard blocks the table write),
            // but while scanning the table persistAll drops q0's unpersisted value -- that is the price of committing only part of the queues.
            std::set<MessageQueue> onlyForeign;
            onlyForeign.insert(foreign);
            o.commit(onlyForeign, true);
            check("S8g committing named queues wipes the in-memory value of the unnamed ones (remove unused mq)",
                  o.committed(q0) != 300, "committed=" + std::to_string(o.committed(q0)));
            check("S8g after the wipe the read-back is the broker's slot",
                  o.committed(q0) == 60, "committed=" + std::to_string(o.committed(q0)));
            int64_t brokerAfterPrune = -1;
            probe.fetchConsumeOffset(q0, brokerAfterPrune);
            check("S8g pruning only drops the in-memory value, it never writes 300 to the broker", brokerAfterPrune == 60,
                  "brokerOffset=" + std::to_string(brokerAfterPrune));
            // commit(Set) takes the current consumed cursor, not the slot held in memory
            std::map<MessageQueue, int64_t> rewindAgain;
            rewindAgain[q0] = 300;
            o.commit(rewindAgain, false);
            o.commit({q0}, true);
            int64_t brokerSet = -1;
            probe.fetchConsumeOffset(q0, brokerSet);
            check("S8g commit(Set) submits the consumed cursor (60), not the in-memory 300",
                  brokerSet == 60, "brokerOffset=" + std::to_string(brokerSet));
            check("S8g after persistAll the in-memory table returns to the consumed cursor (updateConsumeOffset)",
                  o.committed(q0) == 60, "committed=" + std::to_string(o.committed(q0)));

            o.shutdown();
            probe.shutdown();
        }
    }

    // S9: the per-queue cached-count gate on a real broker. Threshold 64 with 32 messages per round and a
    // 200-message backlog: the background pull must stop above the threshold and strictly below the tail, then
    // release as soon as poll() drains the buffer. The paired control is a second consumer on the **same
    // queue** whose threshold clears the backlog -- if the pull stopped for any other reason (route, switch,
    // broker refusal) it would stop for the control too.
    std::printf("\nS9 cached-count gate: the pull parks itself and resumes after draining\n");
    {
        const std::string gateTopic = "LiteGateCpp_" + stamp;
        const std::string gateGroup = "LiteGatePGCpp_" + stamp;
        const std::string ctlGroup = "LiteGateCtlCpp_" + stamp;
        const int32_t nGate = 200;
        const int32_t batch = 32;
        const int32_t queueThreshold = 64;

        std::vector<MessageQueue> qs;
        int32_t landed = 0;
        {
            DefaultMQProducer prep("PG_PrepareGateCpp_" + stamp);
            prep.setNamesrvAddr(namesrv);
            prep.start();
            try {
                prep.createTopic("TBW102", gateTopic, 1);
            } catch (const std::exception& e) {
                std::printf("!! createTopic(%s) failed: %s\n", gateTopic.c_str(), e.what());
            }
            const int64_t routeDeadline = nowMs() + 20000;
            while (qs.empty() && nowMs() < routeDeadline) {
                try {
                    qs = prep.fetchPublishMessageQueues(gateTopic);
                } catch (...) {
                }
                if (qs.empty()) std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            if (qs.size() == 1) {
                for (int32_t from = 0; from < nGate; from += 50) {
                    std::vector<Message> chunk;
                    for (int32_t i = from; i < std::min(nGate, from + 50); ++i) {
                        char buf[32];
                        std::snprintf(buf, sizeof(buf), "gate-%04d", i);
                        chunk.emplace_back(gateTopic, std::string(buf));
                    }
                    try {
                        if (prep.sendBatch(chunk, qs[0]).getSendStatus() == SendStatus::SEND_OK) {
                            landed += static_cast<int32_t>(chunk.size());
                        }
                    } catch (const std::exception& e) {
                        std::printf("   sendBatch failed: %s\n", e.what());
                    }
                }
            }
            prep.shutdown();
        }
        check("S9 prepare the topic (1 queue)", qs.size() == 1, "queues=" + std::to_string(qs.size()));
        check("S9 produced " + std::to_string(nGate) + " messages successfully", landed == nGate,
              "landed=" + std::to_string(landed));

        if (qs.size() == 1) {
            const MessageQueue q0 = qs[0];
            // Only the count gate is under test: the aggregate budget is threshold*batch so it stays clear of
            // this backlog, and 200 tiny messages cannot reach the size (100 MiB) or span (2000) gates.
            auto gateConsumer = [&](const std::string& group, const std::string& instance, int32_t threshold) {
                std::unique_ptr<DefaultLitePullConsumer> c(
                    std::make_unique<DefaultLitePullConsumer>(group));
                c->setNamesrvAddr(namesrv);
                c->setInstanceName(instance);
                c->setPollTimeoutMillis(1000);
                c->setPullBatchSize(batch);
                c->setPullThresholdForAll(nGate * batch);
                c->setPullThresholdForQueue(threshold);
                c->setAutoCommit(false);
                c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
                c->assign({q0});
                c->start();
                return c;
            };

            std::unique_ptr<DefaultLitePullConsumer> gated = gateConsumer(gateGroup, "litegate", queueThreshold);
            const int64_t reached = nowMs() + 20000;
            while (gated->pullCursorOf(q0) <= queueThreshold && nowMs() < reached) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            const int64_t parked = gated->pullCursorOf(q0);
            check("S9a the gate let this queue cache past its threshold (" + std::to_string(queueThreshold) + ")",
                  parked > queueThreshold, "pullCursor=" + std::to_string(parked));
            check("S9a the count gate recorded at least one refusal", gated->queueFlowControlTimes() > 0,
                  "queueFlowControlTimes=" + std::to_string(gated->queueFlowControlTimes()));

            const int64_t held = nowMs() + 5000;
            while (nowMs() < held) std::this_thread::sleep_for(std::chrono::milliseconds(500));
            const int64_t still = gated->pullCursorOf(q0);
            check("S9a with the buffer over budget the cursor holds its ground (never reaches "
                      + std::to_string(nGate) + ")",
                  still == parked && still < nGate, "pullCursor=" + std::to_string(still));
            check("S9a the refusal costs at most two more pull rounds",
                  still - queueThreshold <= 2 * batch,
                  "pullCursor=" + std::to_string(still) + " threshold=" + std::to_string(queueThreshold));

            {
                std::unique_ptr<DefaultLitePullConsumer> control =
                    gateConsumer(ctlGroup, "litegatectl", nGate + 100);
                const int64_t run = nowMs() + 25000;
                while (control->pullCursorOf(q0) < nGate && nowMs() < run) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(300));
                }
                check("S9-control the same queue pulls all " + std::to_string(nGate)
                          + " once the threshold clears",
                      control->pullCursorOf(q0) == nGate,
                      "pullCursor=" + std::to_string(control->pullCursorOf(q0)));
                control->shutdown();
            }

            const std::vector<MessageExt> rest = drain(*gated, static_cast<size_t>(nGate), 60000);
            std::set<std::string> bodies;
            for (const MessageExt& m : rest) bodies.insert(bodyOf(m));
            check("S9b after the buffer drains the rest arrives (" + std::to_string(nGate) + ")",
                  rest.size() == static_cast<size_t>(nGate) && bodies.size() == static_cast<size_t>(nGate),
                  "got=" + std::to_string(rest.size()) + " distinct=" + std::to_string(bodies.size()));
            check("S9b the cursor runs to the tail once the queue is consumed",
                  gated->pullCursorOf(q0) == nGate, "pullCursor=" + std::to_string(gated->pullCursorOf(q0)));
            gated->shutdown();
        }
    }

    std::printf("\nLitePullConsumer: PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
