// Unit test for the pre-pull flow-control decision and for the topic-budget division that feeds its
// per-queue gates -- no cluster needed.
#include <random>
#include <cstdio>
#include <limits>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"

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

constexpr int32_t kMiB = 1024 * 1024;
const char* kTopic = "FlowControlCppUnitTopic";
const char* kOtherTopic = "FlowControlCppUnitOther";
const char* kBroker = "broker-a";

MessageQueue queueOf(const char* topic, int32_t id) {
    return MessageQueue(topic, kBroker, id);
}

MessageExt sized(const char* topic, int32_t storeSize, int64_t queueOffset) {
    MessageExt m;
    m.topic = topic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.storeSize = storeSize;
    m.body = "x";
    return m;
}

// Turn off every gate except the threshold under test: whichever hits first masks the branch.
void gatesOffExceptCounts(DefaultMQPushConsumer& c) {
    c.setPullThresholdForQueue(std::numeric_limits<int32_t>::max());
    c.setPullThresholdSizeForQueue(0);
    c.setConsumeConcurrentlyMaxSpan(0);
    c.setPullThresholdForTopic(-1);
    c.setPullThresholdSizeForTopic(-1);
}

// Preload a batch into one queue's buffer and mark that queue assigned (mqMap_ is the divisor of the
// topic-budget division, so staging a queue also makes it count as one owned queue).
void stage(DefaultMQPushConsumer& c, const MessageQueue& mq, const std::vector<MessageExt>& msgs) {
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    c.setAssignedQueue(key, mq);
    c.setPendingMessages(key, msgs);
}

// Own `count` consecutive queues of one topic: live per-queue pull state, i.e. the divisor the topic
// budget is divided over (queues of every topic add up together).
void own(DefaultMQPushConsumer& c, const char* topic, int count, int32_t firstQueueId = 0) {
    for (int i = 0; i < count; i++) {
        const MessageQueue mq = queueOf(topic, firstQueueId + i);
        c.setAssignedQueue(DefaultMQPushConsumer::offsetKey(mq), mq);
    }
}

std::vector<MessageExt> nSized(const char* topic, int32_t storeSize, size_t n) {
    std::vector<MessageExt> out;
    for (size_t i = 0; i < n; i++) {
        out.push_back(sized(topic, storeSize, static_cast<int64_t>(i)));
    }
    return out;
}

// Count the lines of one tier the recorder wrote to the log file. The level name occupies a
// left-aligned 5-character field, so "WARN" is followed by two spaces before the bracket and
// "DEBUG"/"INFO" by one; pass the whole prefix to avoid matching a level name inside a message.
std::vector<std::string> linesOf(const std::string& path, const std::string& prefix) {
    std::vector<std::string> out;
    std::FILE* fp = std::fopen(path.c_str(), "r");
    if (fp == nullptr) return out;
    char buf[4096];
    while (std::fgets(buf, sizeof(buf), fp) != nullptr) {
        const std::string line(buf);
        if (line.find(prefix) != std::string::npos) out.push_back(line);
    }
    std::fclose(fp);
    return out;
}

int countWith(const std::vector<std::string>& lines, const std::string& needle) {
    int n = 0;
    for (const std::string& line : lines) {
        if (line.find(needle) != std::string::npos) ++n;
    }
    return n;
}

// Record everything written while it is alive into a fresh file under /tmp (never $HOME, where this
// port's default log file lives), then hand the requested tiers back to the caller. The level is
// pushed down to DEBUG so even the per-round debug channel is readable.
class LogCapture {
public:
    LogCapture() {
        static std::mt19937_64 rng{std::random_device{}()};
        path_ = "/tmp/rmq_cpp_fc_capture_" + std::to_string(rng()) + ".log";
        std::remove(path_.c_str());
        previousLevel_ = logLevel();
        setLogFile(path_);
        setLogLevel(LOG_DEBUG);
    }
    ~LogCapture() {
        flushLogFile();
        setLogFile("");
        flushLogFile();
        setLogLevel(previousLevel_);
        std::remove(path_.c_str());
    }
    std::vector<std::string> lines(const std::string& prefix) {
        flushLogFile();
        return linesOf(path_, prefix);
    }

private:
    std::string path_;
    int previousLevel_ = 0;
};

void testDefaultsOnlyCountGate() {
    DefaultMQPushConsumer c("GID_FlowControlDefaults");
    const MessageQueue mq = queueOf(kTopic, 0);
    // Defaults 1000 msgs / 100MiB / span 2000, topic level off: a small buffer never trips them
    stage(c, mq, nSized(kTopic, 100, 1));
    expect(!c.flowControlHit(mq, DefaultMQPushConsumer::offsetKey(mq)), "defaults.noHit");
    expect(c.flowControlTriggered() == 0, "defaults.counterUntouched",
           std::to_string(c.flowControlTriggered()));
    // An empty buffer (nothing pulled yet) also misses, and the span must not turn out negative
    stage(c, mq, {});
    expect(!c.flowControlHit(mq, DefaultMQPushConsumer::offsetKey(mq)), "empty.noHit");
}

void testCountGate() {
    DefaultMQPushConsumer c("GID_FlowControlCount");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    gatesOffExceptCounts(c);
    c.setPullThresholdForQueue(3);
    stage(c, mq, nSized(kTopic, 100, 2));
    expect(!c.flowControlHit(mq, key), "count.below");
    // The gate is strictly greater than the threshold: a buffer that has reached 3 exactly is still
    // allowed to pull, so the threshold reads as "how many may be cached".
    stage(c, mq, nSized(kTopic, 100, 3));
    expect(!c.flowControlHit(mq, key), "count.exactlyAtThreshold");
    expect(c.flowControlTriggered() == 0, "count.noCounter",
           std::to_string(c.flowControlTriggered()));
    stage(c, mq, nSized(kTopic, 100, 4));
    expect(c.flowControlHit(mq, key), "count.over");
    expect(c.flowControlTriggered() == 1, "count.counterOnce",
           std::to_string(c.flowControlTriggered()));
    // The max(1,n) guard makes a 0 threshold behave like 1, so 1 still fits and 2 fires.
    c.setPullThresholdForQueue(0);
    stage(c, mq, nSized(kTopic, 100, 1));
    expect(!c.flowControlHit(mq, key), "count.zeroBehavesAsOneStillFits");
    stage(c, mq, nSized(kTopic, 100, 2));
    expect(c.flowControlHit(mq, key), "count.zeroBehavesAsOne");
}

void testSizeGate() {
    DefaultMQPushConsumer c("GID_FlowControlSize");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    gatesOffExceptCounts(c);
    c.setPullThresholdSizeForQueue(1);           // 1 MiB
    stage(c, mq, nSized(kTopic, 300, 3));        // 900 B
    expect(!c.flowControlHit(mq, key), "size.below");
    // The unit is MiB, not bytes, and the gate is strictly greater than the threshold: a buffer of
    // exactly 1 MiB is still inside the budget.
    stage(c, mq, {sized(kTopic, kMiB, 0)});
    expect(!c.flowControlHit(mq, key), "size.exactlyOneMiB");
    stage(c, mq, {sized(kTopic, kMiB + 1, 0)});
    expect(c.flowControlHit(mq, key), "size.oneByteOver");
    stage(c, mq, nSized(kTopic, 2 * kMiB, 4));
    expect(c.flowControlHit(mq, key), "size.over");
    // 0 disables this gate, however large the buffer becomes
    c.setPullThresholdSizeForQueue(0);
    expect(!c.flowControlHit(mq, key), "size.disabled");
}

void testSpanGate() {
    DefaultMQPushConsumer c("GID_FlowControlSpan");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    gatesOffExceptCounts(c);
    c.setConsumeConcurrentlyMaxSpan(10);
    stage(c, mq, {sized(kTopic, 1, 0), sized(kTopic, 1, 100)});
    expect(c.flowControlHit(mq, key), "span.over");
    c.setConsumeConcurrentlyMaxSpan(100);
    expect(!c.flowControlHit(mq, key), "span.exactlyAtThreshold");
    // An out-of-order buffer must still yield the real span (min/max, not head minus tail)
    c.setConsumeConcurrentlyMaxSpan(10);
    stage(c, mq, {sized(kTopic, 1, 50), sized(kTopic, 1, 5), sized(kTopic, 1, 7)});
    expect(c.flowControlHit(mq, key), "span.unorderedBuffer");
    c.setConsumeConcurrentlyMaxSpan(0);
    expect(!c.flowControlHit(mq, key), "span.disabled");
}

// The pre-pull gate never sums the sibling queues of a topic: each queue is judged against its **own**
// buffer and the divided per-queue share. Under the old aggregation this very fixture paused all four
// queues (their sum reached the topic budget), so every !hit below is a control for the removal.
void testGateNeverSumsTheSiblingQueues() {
    DefaultMQPushConsumer c("GID_FlowControlNoSiblingSum");
    const MessageQueue q0 = queueOf(kTopic, 0);
    const MessageQueue q1 = queueOf(kTopic, 1);
    const MessageQueue q2 = queueOf(kTopic, 2);
    const MessageQueue q3 = queueOf(kTopic, 3);
    const MessageQueue all[4] = {q0, q1, q2, q3};
    gatesOffExceptCounts(c);
    c.setPullThresholdForTopic(8);               // 8 messages for the whole topic
    own(c, kTopic, 4);                           // 4 owned queues => 8 / 4 = 2 per queue
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 2, "noSum.dividedShareIsTheGate",
           std::to_string(c.pullThresholdForQueue()));
    // Every queue exactly AT its divided share still pulls (the gate is strictly greater than).
    for (int i = 0; i < 4; i++) {
        stage(c, all[i], nSized(kTopic, 100, 2));
    }
    for (int i = 0; i < 4; i++) {
        expect(!c.flowControlHit(all[i], DefaultMQPushConsumer::offsetKey(all[i])),
               "noSum.everyQueueAtItsSharePulls.q" + std::to_string(i));
    }
    // One message over its OWN share pauses that one queue; the three siblings, whose topic sum is now
    // 9 against a budget of 8, are untouched.
    stage(c, q0, nSized(kTopic, 100, 3));
    expect(c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "noSum.overSharePauses");
    expect(!c.flowControlHit(q1, DefaultMQPushConsumer::offsetKey(q1)), "noSum.siblingUnaffected.q1");
    expect(!c.flowControlHit(q2, DefaultMQPushConsumer::offsetKey(q2)), "noSum.siblingUnaffected.q2");
    expect(!c.flowControlHit(q3, DefaultMQPushConsumer::offsetKey(q3)), "noSum.siblingUnaffected.q3");
    // And the recorded reason names this queue's own count only: a leftover cross-queue branch would
    // say "topicCount=" / "topicSize=".
    LogCapture log;
    expect(c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "noSum.reasonRoundHits");
    const std::vector<std::string> lines = log.lines(" DEBUG [");
    expect(countWith(lines, "count=3") == 1, "noSum.reasonIsThePerQueueCount",
           std::to_string(lines.size()));
    expect(countWith(lines, "topicCount=") == 0, "noSum.noTopicCountWording");
    expect(countWith(lines, "topicSize=") == 0, "noSum.noTopicSizeWording");
}

// One throttled round bumps the counter exactly once even when several per-queue gates match at the
// same time (the first matching branch wins and the rest are never evaluated).
void testOneHitCountsOneTickEvenWhenEveryGateMatches() {
    DefaultMQPushConsumer c("GID_FlowControlOneTick");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    c.setPullThresholdForQueue(1);
    c.setPullThresholdSizeForQueue(1);           // MiB
    c.setConsumeConcurrentlyMaxSpan(1);
    c.setPullThresholdForTopic(-1);
    c.setPullThresholdSizeForTopic(-1);
    stage(c, mq, nSized(kTopic, 2 * kMiB, 4));   // count 4, 8 MiB, span 3: all three gates match
    expect(c.flowControlHit(mq, key), "oneTick.hits");
    expect(c.flowControlTriggered() == 1, "oneTick.oneCounterTick",
           std::to_string(c.flowControlTriggered()));
}

// ---------------------------------------------------------------- Topic budgets: division, not aggregation

// The message-count budget divides over the owned queues and moves **only** the count knob; the off
// byte budget leaves its knob alone, and the INFO line names the old and the new value.
void testTopicCountBudgetDividesIntoThePerQueueCountKnob() {
    DefaultMQPushConsumer c("GID_FlowControlDivideCount");
    c.setPullThresholdForQueue(65535);           // the user's own value, overwritten on purpose
    c.setPullThresholdSizeForQueue(77);
    c.setPullThresholdForTopic(2000);
    c.setPullThresholdSizeForTopic(-1);          // byte budget off
    own(c, kTopic, 4);
    LogCapture log;
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 500, "divideCount.quotient",
           std::to_string(c.pullThresholdForQueue()));
    expect(c.pullThresholdSizeForQueue() == 77, "divideCount.otherKnobUntouched",
           std::to_string(c.pullThresholdSizeForQueue()));
    expect(c.pullThresholdForTopic() == 2000, "divideCount.budgetNotConsumed",
           std::to_string(c.pullThresholdForTopic()));
    const std::vector<std::string> infos = log.lines(" INFO  [");
    expect(countWith(infos, "The pullThresholdForQueue is changed from 65535 to 500") == 1,
           "divideCount.infoLineNamesOldAndNew", std::to_string(infos.size()));
    // Control for "only its own knob": no byte line at all, and nothing else on the INFO channel.
    expect(countWith(infos, "pullThresholdSizeForQueue") == 0, "divideCount.noSizeLine");
    expect(infos.size() == 1, "divideCount.exactlyOneLine", std::to_string(infos.size()));
}

// The byte budget divides in MiB into the size knob only; the count knob keeps the user's value.
void testTopicByteBudgetDividesIntoThePerQueueSizeKnob() {
    DefaultMQPushConsumer c("GID_FlowControlDivideSize");
    c.setPullThresholdForQueue(4242);
    c.setPullThresholdSizeForQueue(100);
    c.setPullThresholdForTopic(-1);              // count budget off
    c.setPullThresholdSizeForTopic(8);           // 8 MiB over 4 queues
    own(c, kTopic, 4);
    LogCapture log;
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdSizeForQueue() == 2, "divideSize.quotient",
           std::to_string(c.pullThresholdSizeForQueue()));
    expect(c.pullThresholdForQueue() == 4242, "divideSize.otherKnobUntouched",
           std::to_string(c.pullThresholdForQueue()));
    expect(c.pullThresholdSizeForTopic() == 8, "divideSize.budgetNotConsumed",
           std::to_string(c.pullThresholdSizeForTopic()));
    const std::vector<std::string> infos = log.lines(" INFO  [");
    expect(countWith(infos, "The pullThresholdSizeForQueue is changed from 100 to 2") == 1,
           "divideSize.infoLineNamesOldAndNew", std::to_string(infos.size()));
    expect(countWith(infos, "pullThresholdForQueue") == 0, "divideSize.noCountLine");
    expect(infos.size() == 1, "divideSize.exactlyOneLine", std::to_string(infos.size()));
}

// Both budgets off (-1) is the default shape: the user's per-queue values survive whatever the queue
// count is, and the division stays silent. The tail is the paired control proving the silence comes
// from the switches, not from a division that never runs.
void testBothBudgetsOffKeepTheUserKnobsAtAnyQueueCount() {
    DefaultMQPushConsumer c("GID_FlowControlBothOff");
    c.setPullThresholdForQueue(9);
    c.setPullThresholdSizeForQueue(5);
    c.setPullThresholdForTopic(-1);
    c.setPullThresholdSizeForTopic(-1);
    own(c, kTopic, 1);
    own(c, kOtherTopic, 7, 100);                 // a prime count: any stray quotient would differ
    LogCapture log;
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 9, "bothOff.countKnobKept",
           std::to_string(c.pullThresholdForQueue()));
    expect(c.pullThresholdSizeForQueue() == 5, "bothOff.sizeKnobKept",
           std::to_string(c.pullThresholdSizeForQueue()));
    expect(log.lines(" INFO  [").empty(), "bothOff.nothingLogged",
           std::to_string(log.lines(" INFO  [").size()));
    // Same fixture, one budget switched on: the very next call does move that knob and says so.
    c.setPullThresholdForTopic(8);
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 1, "bothOff.switchingItOnDivides",
           std::to_string(c.pullThresholdForQueue()));
    expect(c.pullThresholdSizeForQueue() == 5, "bothOff.stillOnlyThatKnob",
           std::to_string(c.pullThresholdSizeForQueue()));
    expect(countWith(log.lines(" INFO  ["), "The pullThresholdForQueue is changed from 9 to 1") == 1,
           "bothOff.controlDoesLog");
    expect(countWith(log.lines(" INFO  ["), "pullThresholdSizeForQueue") == 0,
           "bothOff.controlKeepsTheSizeQuiet");
}

// A budget smaller than the owned count floors at 1: a 0 per-queue threshold would mean "not even one
// message may be cached" and freeze every queue of the instance.
void testBudgetSmallerThanOwnedFloorsAtOne() {
    DefaultMQPushConsumer c("GID_FlowControlFloorCount");
    c.setPullThresholdForQueue(1000);
    c.setPullThresholdSizeForQueue(100);
    c.setPullThresholdForTopic(3);               // 3 messages, 4 queues
    c.setPullThresholdSizeForTopic(-1);
    own(c, kTopic, 4);
    LogCapture log;
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 1, "floorCount.isOneNotZero",
           std::to_string(c.pullThresholdForQueue()));
    expect(c.pullThresholdSizeForQueue() == 100, "floorCount.sizeKnobUntouched",
           std::to_string(c.pullThresholdSizeForQueue()));
    expect(countWith(log.lines(" INFO  ["),
                     "The pullThresholdForQueue is changed from 1000 to 1") == 1,
           "floorCount.infoLine");

    DefaultMQPushConsumer s("GID_FlowControlFloorSize");
    s.setPullThresholdForQueue(1000);
    s.setPullThresholdSizeForQueue(100);
    s.setPullThresholdForTopic(-1);
    s.setPullThresholdSizeForTopic(2);           // 2 MiB, 5 queues
    own(s, kTopic, 5);
    LogCapture slog;
    s.divideTopicThresholdsForTest();
    expect(s.pullThresholdSizeForQueue() == 1, "floorSize.isOneNotZero",
           std::to_string(s.pullThresholdSizeForQueue()));
    expect(s.pullThresholdForQueue() == 1000, "floorSize.countKnobUntouched",
           std::to_string(s.pullThresholdForQueue()));
    expect(countWith(slog.lines(" INFO  ["),
                     "The pullThresholdSizeForQueue is changed from 100 to 1") == 1,
           "floorSize.infoLine");
}

// Owning no queue at all leaves both knobs exactly as the user set them: dividing would be a division
// by zero, and flooring the meaningless quotient would collapse both gates.
void testOwningNoQueueLeavesBothKnobsUntouched() {
    DefaultMQPushConsumer c("GID_FlowControlNothingOwned");
    c.setPullThresholdForQueue(64);
    c.setPullThresholdSizeForQueue(9);
    c.setPullThresholdForTopic(1000);
    c.setPullThresholdSizeForTopic(8);
    LogCapture log;
    c.divideTopicThresholdsForTest();            // must not crash on the empty table
    expect(c.pullThresholdForQueue() == 64, "noneOwned.countKnobKept",
           std::to_string(c.pullThresholdForQueue()));
    expect(c.pullThresholdSizeForQueue() == 9, "noneOwned.sizeKnobKept",
           std::to_string(c.pullThresholdSizeForQueue()));
    expect(log.lines(" INFO  [").empty(), "noneOwned.nothingLogged");
    // Paired control: the very same consumer with one owned queue divides (1000/1, 8/1).
    own(c, kTopic, 1);
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 1000, "noneOwned.dividesOnceOwned",
           std::to_string(c.pullThresholdForQueue()));
    expect(c.pullThresholdSizeForQueue() == 8, "noneOwned.dividesSizeOnceOwned",
           std::to_string(c.pullThresholdSizeForQueue()));
}

// Repeating the division over an unchanged queue count rewrites the same number and says nothing new
// (the numerator is the budget, not the knob). Growing the set re-derives from the budget: 1000/8 =
// 125, never the previous quotient divided again (250/8 = 31 would be the bug).
void testDivisionIsIdempotentAndReDerivesFromTheBudget() {
    DefaultMQPushConsumer c("GID_FlowControlIdempotent");
    c.setPullThresholdForQueue(1000);
    c.setPullThresholdSizeForQueue(100);
    c.setPullThresholdForTopic(1000);
    c.setPullThresholdSizeForTopic(-1);
    own(c, kTopic, 4);
    {
        LogCapture log;
        c.divideTopicThresholdsForTest();
        expect(c.pullThresholdForQueue() == 250, "idempotent.firstRound",
               std::to_string(c.pullThresholdForQueue()));
        expect(countWith(log.lines(" INFO  ["),
                         "The pullThresholdForQueue is changed from 1000 to 250") == 1,
               "idempotent.firstRoundLine");
    }
    LogCapture again;
    c.divideTopicThresholdsForTest();
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 250, "idempotent.stillTheSameNumber",
           std::to_string(c.pullThresholdForQueue()));
    expect(again.lines(" INFO  [").empty(), "idempotent.noNewLine",
           std::to_string(again.lines(" INFO  [").size()));
    // Grow the owned set to 8: divide the BUDGET by the new count, not the old quotient again.
    own(c, kOtherTopic, 4, 100);
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 125, "idempotent.reDerivesFromBudget",
           std::to_string(c.pullThresholdForQueue()));
    expect(countWith(again.lines(" INFO  ["),
                     "The pullThresholdForQueue is changed from 250 to 125") == 1,
           "idempotent.growthLineNamesBothValues");
}

// The divisor counts this client's queues across EVERY topic, not one topic's queues: 2 + 2 owned
// queues divide by 4. A per-topic divisor would have written 1000/2 = 500.
void testDivisorCountsQueuesAcrossEveryTopic() {
    DefaultMQPushConsumer c("GID_FlowControlAcrossTopics");
    c.setPullThresholdForQueue(1000);
    c.setPullThresholdSizeForQueue(100);
    c.setPullThresholdForTopic(1000);
    c.setPullThresholdSizeForTopic(12);
    own(c, kTopic, 2);
    own(c, kOtherTopic, 2, 100);
    LogCapture log;
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 250, "acrossTopics.countDividesByFour",
           std::to_string(c.pullThresholdForQueue()));
    expect(c.pullThresholdSizeForQueue() == 3, "acrossTopics.sizeDividesByFour",
           std::to_string(c.pullThresholdSizeForQueue()));
    const std::vector<std::string> infos = log.lines(" INFO  [");
    expect(countWith(infos, "The pullThresholdForQueue is changed from 1000 to 250") == 1,
           "acrossTopics.countLine");
    expect(countWith(infos, "The pullThresholdSizeForQueue is changed from 100 to 3") == 1,
           "acrossTopics.sizeLine");
}

// POP mode divides over the POP pull-state table, classic mode over the classic one: the two legs seed
// the same tables (4 POP queues, 2 classic queues, deliberately disagreeing) and must give 1000/4 and
// 1000/2. Reading the wrong table, or the union of both, goes red on one of the two numbers.
void testPopModeDividesOverThePopTableAndClassicOverMqMap() {
    auto seed = [](DefaultMQPushConsumer& c) {
        c.setPullThresholdForQueue(1000);
        c.setPullThresholdSizeForQueue(100);
        c.setPullThresholdForTopic(1000);
        c.setPullThresholdSizeForTopic(-1);
        own(c, kTopic, 2);                        // classic table: 2 queues with live pull state
        for (int32_t id = 0; id < 4; id++) {      // POP table: 4, deliberately disagreeing
            c.setPopQueue(DefaultMQPushConsumer::offsetKey(queueOf(kTopic, id)));
        }
    };

    DefaultMQPushConsumer p("GID_FlowControlPopDivisor");
    p.setPopMode(true);
    seed(p);
    p.divideTopicThresholdsForTest();
    expect(p.pullThresholdForQueue() == 250, "pop.popTableIsTheDivisor",
           std::to_string(p.pullThresholdForQueue()));

    DefaultMQPushConsumer m("GID_FlowControlClassicDivisor");
    m.setPopMode(false);
    seed(m);
    m.divideTopicThresholdsForTest();
    expect(m.pullThresholdForQueue() == 500, "classic.mqMapIsTheDivisor",
           std::to_string(m.pullThresholdForQueue()));
}

// Drive 'rounds' throttled rounds with the log redirected to a fresh file, and return the WARN lines.
std::vector<std::string> captureWarns(DefaultMQPushConsumer& c, const MessageQueue& mq,
                                      const std::string& key, int rounds) {
    static std::mt19937_64 rng{std::random_device{}()};
    const std::string path = "/tmp/rmq_cpp_fc_flow_" + std::to_string(rng()) + ".log";
    std::remove(path.c_str());
    const int previousLevel = logLevel();
    setLogFile(path);
    setLogLevel(LOG_DEBUG);
    for (int i = 0; i < rounds; i++) {
        expect(c.flowControlHit(mq, key), "warnCadence.roundHits");
    }
    flushLogFile();
    std::vector<std::string> warns = linesOf(path, " WARN  [");
    const std::vector<std::string> debugs = linesOf(path, " DEBUG [");
    expect(static_cast<int>(debugs.size()) == rounds, "warnCadence.everyRoundKeepsDebug",
           std::to_string(debugs.size()) + " != " + std::to_string(rounds));
    setLogFile("");
    flushLogFile();
    setLogLevel(previousLevel);
    std::remove(path.c_str());
    return warns;
}

// The cache pause is the client's own short re-schedule; it must not collapse onto the broker's
// FLOW_CONTROL(215) number nor onto the general error backoff.
void testCachePauseIsFiftyMillis() {
    expect(kPullPauseWhenClientFlowControl == 50, "cachePause.is50ms",
           std::to_string(kPullPauseWhenClientFlowControl));
    expect(kPullPauseWhenClientFlowControl != kPullPauseWhenBrokerFlowControl,
           "cachePause.differsFromBrokerPause");
    expect(kPullPauseWhenClientFlowControl != kPullPauseWhenPullError,
           "cachePause.differsFromErrorPause");
}

// A throttled queue trips on every round, so the warn is throttled too: the 1st trip and then every
// 1000th. The modulo is pinned through the predicate, and the wiring by driving the gate 1000 and
// 1001 rounds and counting the WARN lines actually written.
void testWarnCadence() {
    expect(kFlowControlWarnEvery == 1000, "warnCadence.everyIs1000");
    for (int64_t n : {int64_t{1}, int64_t{1001}, int64_t{2001}}) {
        expect(isFlowControlWarnTrip(n), "warnCadence.tripsOn" + std::to_string(n));
    }
    // Control: the trips in between stay on the debug channel only.
    for (int64_t n : {int64_t{0}, int64_t{2}, int64_t{999}, int64_t{1000}, int64_t{2000}}) {
        expect(!isFlowControlWarnTrip(n), "warnCadence.staysQuietOn" + std::to_string(n));
    }

    DefaultMQPushConsumer c("GID_FlowControlWarn");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    gatesOffExceptCounts(c);
    c.setPullThresholdForQueue(1);
    stage(c, mq, nSized(kTopic, 100, 2));

    std::vector<std::string> warns = captureWarns(c, mq, key, 1000);
    expect(c.flowControlTriggered() == 1000, "warnCadence.counterAt1000",
           std::to_string(c.flowControlTriggered()));
    expect(warns.size() == 1, "warnCadence.oneWarnIn1000Rounds", std::to_string(warns.size()));
    expect(!warns.empty() && warns[0].find("(flowControlTimes=1)") != std::string::npos,
           "warnCadence.firstWarnCarriesCounter");

    warns = captureWarns(c, mq, key, 1);
    expect(c.flowControlTriggered() == 1001, "warnCadence.counterAt1001",
           std::to_string(c.flowControlTriggered()));
    expect(warns.size() == 1, "warnCadence.nextRoundWarns", std::to_string(warns.size()));
    expect(!warns.empty() && warns[0].find("(flowControlTimes=1001)") != std::string::npos,
           "warnCadence.secondWarnCarriesCounter");
}

}  // namespace

int main() {
    testDefaultsOnlyCountGate();
    testCountGate();
    testSizeGate();
    testSpanGate();
    testGateNeverSumsTheSiblingQueues();
    testOneHitCountsOneTickEvenWhenEveryGateMatches();
    testTopicCountBudgetDividesIntoThePerQueueCountKnob();
    testTopicByteBudgetDividesIntoThePerQueueSizeKnob();
    testBothBudgetsOffKeepTheUserKnobsAtAnyQueueCount();
    testBudgetSmallerThanOwnedFloorsAtOne();
    testOwningNoQueueLeavesBothKnobsUntouched();
    testDivisionIsIdempotentAndReDerivesFromTheBudget();
    testDivisorCountsQueuesAcrossEveryTopic();
    testPopModeDividesOverThePopTableAndClassicOverMqMap();
    testCachePauseIsFiftyMillis();
    testWarnCadence();
    std::printf("flow control: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
