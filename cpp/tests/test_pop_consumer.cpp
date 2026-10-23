// POP **consumer side** (DefaultMQPushConsumer::setPopMode(true)) unit tests -- no cluster needed.
//
// Why a separate file: the protocol pipeline passing (test_pop.cpp) does not mean the consume loop is right. The consumer side
// has its own set of easy-to-get-wrong semantics, and they fail **quietly** -- for example with a wrong default ackIndex,
// CONSUME_SUCCESS acks nothing and the broker revives and redelivers the messages once invisibleTime expires; if the
// observation window is shorter than invisibleTime a live run still looks like "everything passed". Anything lockable offline must be locked down first.
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/extra_info.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

const char* kGroup = "GID_PopCppUnit";
const char* kTopic = "PopCppUnitTopic";
const char* kBroker = "broker-a";

// Hand-assembled 8-segment CK: buildExtraInfo derives retryFlag from the topic, but here the retry segment must be set explicitly
std::string ckSeg(int32_t queueId = 0, int64_t offset = 0, const std::string& retry = "0",
                  int64_t popTime = 1000, int64_t invisible = 60000) {
    char buf[256];
    std::snprintf(buf, sizeof(buf), "0 %lld %lld 0 %s %s %d %lld",
                  static_cast<long long>(popTime), static_cast<long long>(invisible),
                  retry.c_str(), kBroker, queueId, static_cast<long long>(offset));
    return std::string(buf);
}

MessageExt makeMsg(const std::string& topic = kTopic, int32_t queueId = 0,
                   int64_t queueOffset = 0, const std::string& popCk = std::string()) {
    MessageExt m;
    m.topic = topic;
    m.queueId = queueId;
    m.queueOffset = queueOffset;
    m.bornTimestamp = 0;
    if (!popCk.empty()) {
        m.properties[MessageConst::PROPERTY_POP_CK] = popCk;
    }
    return m;
}

// Silence the recorder for a round that deliberately trips a gate: what is pinned here is the return
// value and the shared hit counter, and the log wording itself is captured by the flow-control suite,
// which redirects the recorder into a file under /tmp.
class QuietLog {
public:
    QuietLog() {
        previous_ = logLevel();
        setLogLevel(LOG_OFF);
    }
    ~QuietLog() { setLogLevel(previous_); }

private:
    int previous_ = 0;
};

// A pop queue that has `debt` messages popped and not acked yet -- the state queuePopLoop hands to its
// flow-control gate between two rounds.
std::shared_ptr<PopProcessQueue> popQueueWithDebt(int32_t debt) {
    auto pq = std::make_shared<PopProcessQueue>();
    pq->incFoundMsg(debt);
    return pq;
}

void testProcessQueue() {
    PopProcessQueue pq;
    expect(pq.waitAckCount() == 0, "pq.initial");
    pq.incFoundMsg(3);
    expect(pq.waitAckCount() == 3, "pq.inc");
    pq.ack();
    pq.ack();
    expect(pq.waitAckCount() == 1, "pq.ack");
    pq.decFoundMsg(-1);
    expect(pq.waitAckCount() == 0, "pq.dec");
    expect(!pq.isDropped(), "pq.notDropped");
    pq.setDropped(true);
    expect(pq.isDropped(), "pq.dropped");
}

// The POP wait-ack gate of the pop loop: it judges **this queue's own** unacked debt against the POP
// ceiling (strictly greater), and acking the batch releases the pause again. The hit counter it ticks
// is the client-wide flow-control counter the pull path uses, so a throttled consumer reports one
// number whichever consumption mode it runs; the log wording is pinned by the flow-control suite.
void testPopWaitAckGateFollowsTheQueueDebt() {
    QuietLog quiet;
    DefaultMQPushConsumer c(kGroup);
    const MessageQueue mq(kTopic, kBroker, 0);
    const MessageQueue sibling(kTopic, kBroker, 1);
    c.setPopThresholdForQueue(2);
    auto pq = std::make_shared<PopProcessQueue>();
    expect(!c.popFlowControlHitForTest(mq, pq), "popGate.nothingPoppedYet");
    pq->incFoundMsg(2);                 // two popped, none acked: exactly at the ceiling
    expect(!c.popFlowControlHitForTest(mq, pq), "popGate.atTheCeilingStillPops");
    expect(c.flowControlTriggered() == 0, "popGate.noTickWhileInsideTheCeiling",
           std::to_string(c.flowControlTriggered()));
    pq->incFoundMsg(1);                 // the next batch puts this queue over it
    expect(c.popFlowControlHitForTest(mq, pq), "popGate.oneOverTheCeilingPauses");
    expect(c.flowControlTriggered() == 1, "popGate.tripsTheSharedCounter",
           std::to_string(c.flowControlTriggered()));
    // A sibling queue holding nothing is never paused by another queue's debt.
    auto empty = std::make_shared<PopProcessQueue>();
    expect(!c.popFlowControlHitForTest(sibling, empty), "popGate.siblingQueueUnaffected");
    expect(c.flowControlTriggered() == 1, "popGate.siblingAddsNothing",
           std::to_string(c.flowControlTriggered()));
    // The consume side acks the whole batch: the same debt that paused the round is gone, so the queue
    // pops again and the counter stops moving.
    for (int i = 0; i < 3; i++) pq->ack();
    expect(pq->waitAckCount() == 0, "popGate.everythingAcked", std::to_string(pq->waitAckCount()));
    expect(!c.popFlowControlHitForTest(mq, pq), "popGate.drainedQueuePopsAgain");
    expect(c.flowControlTriggered() == 1, "popGate.drainingTicksNothing",
           std::to_string(c.flowControlTriggered()));
    // The default ceiling (96) is the shipped contract: 96 unacked still pops, 97 pauses.
    DefaultMQPushConsumer d(kGroup);
    expect(d.popThresholdForQueue() == 96, "popGate.defaultCeiling",
           std::to_string(d.popThresholdForQueue()));
    expect(!d.popFlowControlHitForTest(mq, popQueueWithDebt(96)), "popGate.defaultAt96Pops");
    expect(d.popFlowControlHitForTest(mq, popQueueWithDebt(97)), "popGate.defaultAt97Pauses");
    expect(d.flowControlTriggered() == 1, "popGate.defaultRoundTicksOnce",
           std::to_string(d.flowControlTriggered()));
}

void testDefaults() {
    DefaultMQPushConsumer c(kGroup);
    // When switched off it must take exactly the original pull path
    expect(!c.popMode(), "defaults.popModeOff");
    expect(c.popInvisibleTime() == 60000, "defaults.invisible",
           std::to_string(c.popInvisibleTime()));
    expect(c.popBatchNums() == 32, "defaults.batchNums", std::to_string(c.popBatchNums()));
    expect(c.popThresholdForQueue() == 96, "defaults.threshold",
           std::to_string(c.popThresholdForQueue()));
    // broker side returns INVALID_PARAMETER when maxMsgNums > 32
    expect(c.popBatchNums() <= 32, "defaults.batchNumsLe32");
    // the long-polling suspend time must be < the request timeout, otherwise the client times out first and spins idle every round
    expect(c.popPollTimeMillis() < c.popTimeoutMillis(), "defaults.pollLtTimeout");
    // the delay-level table is in seconds and the first level is 10s (the send delay table starts at 1s; do not mix them up)
    const std::vector<int32_t>& t = popDelayLevelTable();
    expect(t[0] == 10, "defaults.levelFirst10");
    expect(t.back() == 7200, "defaults.levelLast7200");
    for (size_t i = 1; i < t.size(); i++) {
        expect(t[i - 1] < t[i], "defaults.levelAscending", std::to_string(i));
    }
    expect(kMinPopInvisibleTime == 5000 && kMaxPopInvisibleTime == 300000,
           "defaults.invisibleBounds");
}

void testPopCkTarget() {
    DefaultMQPushConsumer c(kGroup);

    // plain topic: restored as-is
    {
        MessageExt m = makeMsg(kTopic, 3, 0, ckSeg(3, 7));
        auto t = c.popCkTarget(m);
        expect(t.has_value(), "ck.normal.hasValue");
        if (t) {
            expect(t->topic == kTopic, "ck.normal.topic", t->topic);
            expect(t->brokerName == kBroker, "ck.normal.broker", t->brokerName);
            expect(t->queueId == 3, "ck.normal.queueId", std::to_string(t->queueId));
            expect(t->offset == 7, "ck.normal.offset", std::to_string(t->offset));
        }
    }
    // retryFlag=1 -> the real topic is %RETRY%<group>_<topic> (V1 underscore)
    {
        MessageExt m = makeMsg(kTopic, 1, 2, ckSeg(1, 2, "1"));
        auto t = c.popCkTarget(m);
        expect(t.has_value(), "ck.retry1.hasValue");
        if (t) {
            expect(t->topic == std::string("%RETRY%") + kGroup + "_" + kTopic,
                   "ck.retry1.topic", t->topic);
        }
    }
    // retryFlag=2 -> V2 separates with '+'
    {
        MessageExt m = makeMsg(kTopic, 0, 0, ckSeg(0, 0, "2"));
        auto t = c.popCkTarget(m);
        expect(t.has_value(), "ck.retry2.hasValue");
        if (t) {
            expect(t->topic == std::string("%RETRY%") + kGroup + "+" + kTopic,
                   "ck.retry2.topic", t->topic);
        }
    }
    // no CK -> nullopt (give up the ack and leave it to broker revival)
    expect(!c.popCkTarget(makeMsg()).has_value(), "ck.missing");
    // too few segments (7) -> nullopt, must not throw
    {
        std::string seven = extra_info::buildExtraInfo(0, 1, 2, 3, kTopic, kBroker, 4);
        expect(!c.popCkTarget(makeMsg(kTopic, 4, 0, seven)).has_value(), "ck.short7");
    }
    // garbage string -> nullopt
    expect(!c.popCkTarget(makeMsg(kTopic, 0, 0, "only two")).has_value(), "ck.garbage");
}

void testIsPopTimeout() {
    expect(DefaultMQPushConsumer::isPopTimeout(0, 0), "timeout.bothZero");
    expect(DefaultMQPushConsumer::isPopTimeout(0, 60000), "timeout.popTimeZero");
    expect(DefaultMQPushConsumer::isPopTimeout(1000, 0), "timeout.invisibleZero");
    // inside the window is not a timeout (use the real current time, do not hardcode a timestamp)
    const int64_t now = UtilAll::currentTimeMillis();
    expect(!DefaultMQPushConsumer::isPopTimeout(now, 60000), "timeout.within");
    expect(DefaultMQPushConsumer::isPopTimeout(now - 60001, 60000), "timeout.past");
}

void testPopPullStats() {
    DefaultMQPushConsumer c(kGroup);
    ConsumerStatsManager stats;
    const std::string key = std::string(kTopic) + "@" + kGroup;

    PopResult found;
    found.status = PopStatus::FOUND;
    found.msgFoundList = {MessageExt(), MessageExt()};
    // began is taken 30ms ago: sampled within the same millisecond the rounded value is 0, and "recorded" vs "recorded a 0" become indistinguishable
    c.recordPopPullStats(stats, kTopic, found, UtilAll::currentTimeMillis() - 30);
    std::shared_ptr<StatsItem> rt = stats.topicAndGroupPullRT().find(key);
    std::shared_ptr<StatsItem> tps = stats.topicAndGroupPullTPS().find(key);
    expect(rt != nullptr && rt->times() == 1, "stats.found.rt_once");
    expect(rt != nullptr && rt->value() >= 30, "stats.found.rt_positive",
           rt ? std::to_string(rt->value()) : std::string("nullptr"));
    expect(tps != nullptr && tps->times() == 1 && tps->value() == 2, "stats.found.tps_2",
           tps ? std::to_string(tps->value()) : std::string("nullptr"));

    PopResult emptyFound;
    emptyFound.status = PopStatus::FOUND;
    c.recordPopPullStats(stats, kTopic, emptyFound, UtilAll::currentTimeMillis() - 10);
    expect(rt->times() == 2, "stats.empty_found.rt_still");
    expect(tps->times() == 1 && tps->value() == 2, "stats.empty_found.tps_untouched");

    // a long poll coming back empty (POLLING_NOT_FOUND) is the norm for POP: not a single slot may be recorded,
    // otherwise the average pull time gets diluted by the suspend duration into fake data.
    PopResult polling;
    polling.status = PopStatus::POLLING_NOT_FOUND;
    c.recordPopPullStats(stats, kTopic, polling, UtilAll::currentTimeMillis() - 5000);
    expect(rt->times() == 2, "stats.polling.rt_untouched");
    expect(tps->times() == 1, "stats.polling.tps_untouched");
}

}  // namespace

int main() {
    testProcessQueue();
    testPopWaitAckGateFollowsTheQueueDebt();
    testDefaults();
    testPopCkTarget();
    testIsPopTimeout();
    testPopPullStats();
    std::printf("pop consumer: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
