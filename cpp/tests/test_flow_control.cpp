// Unit test for the pre-pull flow-control decision, for the POP-mode wait-ack gate that shares its
// hit counter and warn cadence, and for the topic-budget division that feeds the per-queue gates
// -- no cluster needed.
#include <random>
#include <cstdio>
#include <limits>
#include <memory>
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

MessageExt sized(const char* topic, size_t bodyBytes, int64_t queueOffset) {
    MessageExt m;
    m.topic = topic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    // storeSize is deliberately left at 0: the size gate reads the message BODY, so an implementation
    // that read the storage record would see 0 bytes here and let every size case through. The two
    // storeSize legs in testSizeGate pin both directions of that choice.
    m.storeSize = 0;
    m.body = std::string(bodyBytes, 'x');
    return m;
}

// Park the two sibling gates so only the threshold a case measures can trip. Neither gate can be
// switched off with 0 -- 0 is refused by checkConfig() at start-up and the gate itself compares the
// number it was given -- so the only way to quiet a gate is a threshold no fixture reaches.
void parkSiblingGates(DefaultMQPushConsumer& c) {
    c.setPullThresholdForQueue(std::numeric_limits<int32_t>::max());
    c.setPullThresholdSizeForQueue(std::numeric_limits<int32_t>::max());
    c.setConsumeConcurrentlyMaxSpan(std::numeric_limits<int64_t>::max());
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

std::vector<MessageExt> nSized(const char* topic, size_t bodyBytes, size_t n) {
    std::vector<MessageExt> out;
    for (size_t i = 0; i < n; i++) {
        out.push_back(sized(topic, bodyBytes, static_cast<int64_t>(i)));
    }
    return out;
}

// Same, starting at an explicit queueOffset: two sides of the held set need disjoint offsets.
std::vector<MessageExt> nSizedFrom(const char* topic, size_t bodyBytes, int64_t firstOffset,
                                   size_t n) {
    std::vector<MessageExt> out;
    for (size_t i = 0; i < n; i++) {
        out.push_back(sized(topic, bodyBytes, firstOffset + static_cast<int64_t>(i)));
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
    parkSiblingGates(c);
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
    // A threshold of 0 is NOT an off-switch and is NOT floored to 1 by the gate: it means "not even one
    // message may stay held", so the first cached message pauses the queue. checkConfig() rejects the
    // value at start-up; the gate itself compares the number it was given, so a max(1, n) floor here
    // would go red on the first of this pair.
    c.setPullThresholdForQueue(0);
    stage(c, mq, {});
    expect(!c.flowControlHit(mq, key), "count.zeroWithEmptyBufferStillPulls");
    stage(c, mq, nSized(kTopic, 100, 1));
    expect(c.flowControlHit(mq, key), "count.zeroTripsOnTheFirstMessage");
}

// The gate's number is everything the queue still HOLDS: the pulled-but-undispatched buffer plus the
// batches already handed to the listener and not settled yet. A batch in the listener's hands is still
// this client's cached message, so a gate that read only the buffer would keep pulling on top of it.
void testHeldSetIsTheBufferPlusTheBatchesInTheListenerHands() {
    DefaultMQPushConsumer c("GID_FlowControlHeldSet");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    parkSiblingGates(c);
    c.setPullThresholdForQueue(2);
    c.setAssignedQueue(key, mq);
    // 1 buffered + 1 in the listener's hands: the held set is 2, exactly the threshold, so it pulls.
    // The dispatch window is ordered, so the batch in the listener's hands carries the lower offset.
    c.setPendingMessages(key, nSizedFrom(kTopic, 100, 1, 1));
    c.setInflightMessages(key, nSizedFrom(kTopic, 100, 0, 1));
    expect(c.processQueueEntries(key).size() == 2, "held.unionIsWhatTheGateReads",
           std::to_string(c.processQueueEntries(key).size()));
    expect(!c.flowControlHit(mq, key), "held.atThresholdStillPulls");
    // One more buffered: 2 buffered + 1 outstanding is 3 held, over the threshold. A gate that read the
    // buffer alone would still see 2 and keep pulling.
    c.setPendingMessages(key, nSizedFrom(kTopic, 100, 1, 2));
    expect(c.flowControlHit(mq, key), "held.bufferPlusListenerTrips");
    // Paired control for "the gate reads the buffer only": nothing buffered, all three messages still
    // in the listener's hands. A buffer-only gate would see an empty queue and pull.
    c.setPendingMessages(key, {});
    c.setInflightMessages(key, nSizedFrom(kTopic, 100, 0, 3));
    expect(c.flowControlHit(mq, key), "held.listenerOutstandingAloneTrips");
    // The listener settles one batch: 2 held is exactly the threshold again, so the queue pulls.
    c.setInflightMessages(key, nSizedFrom(kTopic, 100, 0, 2));
    expect(!c.flowControlHit(mq, key), "held.settledBatchStopsCounting");
    // The two sides are one set, not two sums: the same offset staged on both sides counts once, so a
    // threshold of 1 with offset 5 on both sides must NOT read as 2 held.
    c.setPullThresholdForQueue(1);
    c.setPendingMessages(key, nSizedFrom(kTopic, 100, 5, 1));
    c.setInflightMessages(key, nSizedFrom(kTopic, 100, 5, 1));
    expect(!c.flowControlHit(mq, key), "held.overlapCountsOnce");
    expect(c.processQueueEntries(key).size() == 1, "held.overlapViewIsOneEntry",
           std::to_string(c.processQueueEntries(key).size()));
}

void testSizeGate() {
    DefaultMQPushConsumer c("GID_FlowControlSize");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    parkSiblingGates(c);
    c.setPullThresholdSizeForQueue(1);           // 1 MiB
    stage(c, mq, nSized(kTopic, 300, 3));        // 900 B
    expect(!c.flowControlHit(mq, key), "size.below");
    // The unit is MiB of body, not bytes, and the gate is strictly greater than the threshold: a
    // buffer of exactly 1 MiB is still inside the budget.
    stage(c, mq, {sized(kTopic, kMiB, 0)});
    expect(!c.flowControlHit(mq, key), "size.exactlyOneMiB");
    // The division TRUNCATES: one byte over 1 MiB is still 1 MiB after the divide, so a 1 MiB budget
    // tolerates 1.9 MiB. A ceiling-dividing implementation goes red on this leg.
    stage(c, mq, {sized(kTopic, kMiB + 1, 0)});
    expect(!c.flowControlHit(mq, key), "size.oneByteOverIsStillOneWholeMiB");
    stage(c, mq, {sized(kTopic, static_cast<size_t>(1.9 * kMiB), 0)});
    expect(!c.flowControlHit(mq, key), "size.justUnderTwoMiBIsStillOne");
    // A whole MiB above the budget is a refusal.
    stage(c, mq, {sized(kTopic, 2 * kMiB, 0)});
    expect(c.flowControlHit(mq, key), "size.twoMiBOverOneMiBBudget");
    // The budget sums the batches, it does not read only the first one: 3 x 1 MiB = 3 MiB.
    stage(c, mq, nSized(kTopic, kMiB, 3));
    expect(c.flowControlHit(mq, key), "size.sumsTheHeldBodies");
    // 0 is not an off-switch: with the count and span gates parked, a whole MiB over a 0 MiB budget
    // trips. (checkConfig() refuses 0 at start-up; the gate just compares.)
    c.setPullThresholdSizeForQueue(0);
    stage(c, mq, {sized(kTopic, kMiB, 0)});
    expect(c.flowControlHit(mq, key), "size.zeroIsNotAnOffSwitch");
    stage(c, mq, {sized(kTopic, kMiB - 1, 0)});
    expect(!c.flowControlHit(mq, key), "size.zeroBudgetStillToleratesUnderOneMiB");
}

// The byte budget is the message BODY, never storeSize: storeSize is what the broker charged for the
// whole storage record (metadata and properties included) and is a telemetry field. Both directions
// are pinned, so reading the wrong field is red whichever way a future refactor moves the fixture.
void testSizeGateMeasuresBodyBytesNeverStoreSize() {
    {
        DefaultMQPushConsumer c("GID_FlowControlBodyBigStore");
        const MessageQueue mq = queueOf(kTopic, 0);
        const std::string key = DefaultMQPushConsumer::offsetKey(mq);
        parkSiblingGates(c);
        c.setPullThresholdSizeForQueue(1);
        MessageExt decoy = sized(kTopic, /*bodyBytes=*/16, 0);
        decoy.storeSize = static_cast<int32_t>(4 * kMiB);   // huge record, tiny body
        stage(c, mq, {decoy});
        expect(!c.flowControlHit(mq, key), "storeSize.largeStoreTinyBodyDoesNotTrip");
    }
    {
        DefaultMQPushConsumer c("GID_FlowControlBodyOnly");
        const MessageQueue mq = queueOf(kTopic, 0);
        const std::string key = DefaultMQPushConsumer::offsetKey(mq);
        parkSiblingGates(c);
        c.setPullThresholdSizeForQueue(1);
        MessageExt body = sized(kTopic, 2 * kMiB, 0);       // 2 MiB body, storeSize left at 0
        body.storeSize = 0;
        stage(c, mq, {body});
        expect(c.flowControlHit(mq, key), "storeSize.bodyAloneTripsTheGate");
    }
}

void testSpanGate() {
    DefaultMQPushConsumer c("GID_FlowControlSpan");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    parkSiblingGates(c);
    c.setConsumeConcurrentlyMaxSpan(10);
    stage(c, mq, {sized(kTopic, 1, 0), sized(kTopic, 1, 100)});
    expect(c.flowControlHit(mq, key), "span.over");
    c.setConsumeConcurrentlyMaxSpan(100);
    expect(!c.flowControlHit(mq, key), "span.exactlyAtThreshold");
    // An out-of-order buffer must still yield the real span (min/max, not head minus tail)
    c.setConsumeConcurrentlyMaxSpan(10);
    stage(c, mq, {sized(kTopic, 1, 50), sized(kTopic, 1, 5), sized(kTopic, 1, 7)});
    expect(c.flowControlHit(mq, key), "span.unorderedBuffer");
    // 0 does not switch the gate off: any queue holding two different offsets has a span above 0.
    c.setConsumeConcurrentlyMaxSpan(0);
    stage(c, mq, {sized(kTopic, 1, 5), sized(kTopic, 1, 6)});
    expect(c.flowControlHit(mq, key), "span.zeroIsNotAnOffSwitch");
    // Paired control: a single held message spans 0, which is not above 0, so that queue still pulls.
    stage(c, mq, {sized(kTopic, 1, 5)});
    expect(!c.flowControlHit(mq, key), "span.oneMessageSpansNothing");
}

// The span knob is named consumeConcurrentlyMaxSpan because it bounds CONCURRENT consumption: an
// orderly consumer deliberately leaves this gate alone, since orderly queues hold a backlog on
// purpose and judging them would stall the head of the line. The same fixture, only the listener
// mode changed, is the paired control. MessageListener's only pure virtual is the mode flag, so the
// stub says exactly what the gate reads and nothing else.
class ModeFlagListener : public MessageListener {
public:
    explicit ModeFlagListener(bool orderly) : orderly_(orderly) {}
    bool orderly() const override { return orderly_; }

private:
    bool orderly_;
};

void testSpanGateNeverRunsForOrderlyConsumption() {
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    DefaultMQPushConsumer o("GID_FlowControlOrderlySpan");
    o.setMessageListener(std::make_shared<ModeFlagListener>(true));
    DefaultMQPushConsumer cn("GID_FlowControlConcurrentSpan");
    cn.setMessageListener(std::make_shared<ModeFlagListener>(false));
    for (DefaultMQPushConsumer* c : {&o, &cn}) {
        parkSiblingGates(*c);
        c->setConsumeConcurrentlyMaxSpan(10);
        c->setAssignedQueue(key, mq);
        c->setPendingMessages(key, {sized(kTopic, 1, 0), sized(kTopic, 1, 5000)});
    }
    expect(o.processQueueEntries(key).size() == 2, "orderlySpan.fixtureIsStaged",
           std::to_string(o.processQueueEntries(key).size()));
    expect(!o.flowControlHit(mq, key), "orderlySpan.spanOf5000IsNotJudged");
    expect(o.flowControlTriggered() == 0, "orderlySpan.noTickForAnOrderlyQueue",
           std::to_string(o.flowControlTriggered()));
    // Control: the identical backlog under a concurrent listener is a trip.
    expect(cn.flowControlHit(mq, key), "orderlySpan.concurrentDoesJudgeIt");
    expect(cn.flowControlTriggered() == 1, "orderlySpan.controlTicksOnce",
           std::to_string(cn.flowControlTriggered()));
    // An orderly queue is not exempt from the other two gates: the same queue over its count
    // threshold pauses even with an orderly listener.
    o.setPullThresholdForQueue(1);
    expect(o.flowControlHit(mq, key), "orderlySpan.countGateStillJudgesOrderly");
    // And its size gate too, so the exemption is the span branch alone.
    o.setPullThresholdForQueue(std::numeric_limits<int32_t>::max());
    o.setPullThresholdSizeForQueue(1);
    o.setPendingMessages(key, {sized(kTopic, 2 * kMiB, 0)});
    expect(o.flowControlHit(mq, key), "orderlySpan.sizeGateStillJudgesOrderly");
}

// pullBatchSize is the count a pull round asks for and, on the lite path, the multiplier of the
// aggregate budget: a push consumer asks for 32 per round.
void testPushPullBatchSizeDefaultsToThirtyTwo() {
    DefaultMQPushConsumer c("GID_FlowControlBatchSizeDefault");
    expect(c.pullBatchSize() == 32, "batchSize.pushDefaultIs32", std::to_string(c.pullBatchSize()));
    c.setPullBatchSize(3);
    expect(c.pullBatchSize() == 3, "batchSize.setterDoesNotClamp", std::to_string(c.pullBatchSize()));
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
    parkSiblingGates(c);
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

// ---------------------------------------------------------------- POP mode: the wait-ack gate

// A pop queue holding `debt` messages that were popped but not acked yet -- exactly what queuePopLoop
// leaves behind between two rounds (queuePopLoop does incFoundMsg for a found batch, the consume side
// does ack()).
std::shared_ptr<PopProcessQueue> popQueueWith(int32_t debt) {
    auto pq = std::make_shared<PopProcessQueue>();
    pq->incFoundMsg(debt);
    return pq;
}

// The ceiling is a ceiling, not a target: the gate is **strictly greater than** it, exactly like the
// three pull-side cache gates, so a queue that reached it exactly still pops. Every assertion below is
// a pair -- the same debt one message later trips -- so an inverted or collapsed comparison goes red.
void testPopGateBoundaryIsStrictlyOverTheCeiling() {
    DefaultMQPushConsumer c("GID_PopFlowControlBoundary");
    const MessageQueue mq = queueOf(kTopic, 0);
    c.setPopThresholdForQueue(3);
    expect(!c.popFlowControlHitForTest(mq, popQueueWith(0)), "popGate.nothingUnacked");
    expect(!c.popFlowControlHitForTest(mq, popQueueWith(2)), "popGate.below");
    expect(!c.popFlowControlHitForTest(mq, popQueueWith(3)), "popGate.exactlyAtCeiling");
    expect(c.flowControlTriggered() == 0, "popGate.threeMissesTickNothing",
           std::to_string(c.flowControlTriggered()));
    expect(c.popFlowControlHitForTest(mq, popQueueWith(4)), "popGate.oneOverTheCeiling");
    expect(c.flowControlTriggered() == 1, "popGate.overTicksOnce",
           std::to_string(c.flowControlTriggered()));
    // No pop state at all (the queue was revoked before the round) = nothing waits for an ack.
    expect(!c.popFlowControlHitForTest(mq, nullptr), "popGate.noPopStateIsQuiet");
    expect(c.flowControlTriggered() == 1, "popGate.noPopStateTicksNothing",
           std::to_string(c.flowControlTriggered()));
    // The gate reads THIS round's debt, it is not a latched flag: acking one message back to the
    // ceiling lets the next round pop again.
    auto pq = popQueueWith(4);
    expect(c.popFlowControlHitForTest(mq, pq), "popGate.stillOverTheCeiling");
    pq->ack();
    expect(!c.popFlowControlHitForTest(mq, pq), "popGate.drainedBackUnderTheCeiling");
    expect(c.flowControlTriggered() == 2, "popGate.drainingTicksNothing",
           std::to_string(c.flowControlTriggered()));
}

// The default ceiling is 96 and it is NOT one of the two divided knobs: a topic budget rewrites the
// pull-side knobs only, and the POP gate keeps judging this queue's own debt against popThresholdForQueue.
void testPopCeilingDefaultsTo96AndIsNeverADividedKnob() {
    DefaultMQPushConsumer c("GID_PopFlowControlNotDivided");
    const MessageQueue mq = queueOf(kTopic, 0);
    expect(c.popThresholdForQueue() == 96, "popDefault.ceilingIs96",
           std::to_string(c.popThresholdForQueue()));
    expect(!c.popFlowControlHitForTest(mq, popQueueWith(96)), "popDefault.at96StillPops");
    expect(c.popFlowControlHitForTest(mq, popQueueWith(97)), "popDefault.at97Trips");
    expect(c.flowControlTriggered() == 1, "popDefault.firstTripOnly",
           std::to_string(c.flowControlTriggered()));

    c.setPopMode(true);
    c.setPullThresholdForQueue(1000);
    c.setPullThresholdSizeForQueue(100);
    c.setPullThresholdForTopic(1000);
    c.setPullThresholdSizeForTopic(8);
    for (int32_t id = 0; id < 4; id++) {          // 4 owned POP queues
        c.setPopQueue(DefaultMQPushConsumer::offsetKey(queueOf(kTopic, id)));
    }
    c.divideTopicThresholdsForTest();
    expect(c.pullThresholdForQueue() == 250, "popDefault.pullCountKnobDivided",
           std::to_string(c.pullThresholdForQueue()));
    expect(c.pullThresholdSizeForQueue() == 2, "popDefault.pullSizeKnobDivided",
           std::to_string(c.pullThresholdSizeForQueue()));
    // The paired control: the two knobs above moved precisely because they are the divided ones; the
    // POP ceiling is not, so a stray division of it would read 250 here.
    expect(c.popThresholdForQueue() == 96, "popDefault.popCeilingUnchanged",
           std::to_string(c.popThresholdForQueue()));
    expect(!c.popFlowControlHitForTest(mq, popQueueWith(96)), "popDefault.gateStillReads96");
    expect(c.popFlowControlHitForTest(mq, popQueueWith(97)), "popDefault.gateStillTripsAt97");
}

// One client-wide counter: a pull trip and a POP trip tick the SAME number, so flowControlTriggered()
// stays the single "how often did this client throttle itself" reading across both consumption modes.
void testPopTripSharesThePullFlowControlCounter() {
    DefaultMQPushConsumer c("GID_PopFlowControlSharedCounter");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    parkSiblingGates(c);
    c.setPullThresholdForQueue(1);
    c.setPopThresholdForQueue(1);
    expect(c.flowControlTriggered() == 0, "sharedCounter.freshConsumer");
    stage(c, mq, nSized(kTopic, 100, 2));         // 2 over a ceiling of 1: the pull gate trips
    expect(c.flowControlHit(mq, key), "sharedCounter.pullRoundTrips");
    expect(c.flowControlTriggered() == 1, "sharedCounter.pullTickIsOne",
           std::to_string(c.flowControlTriggered()));
    expect(c.popFlowControlHitForTest(mq, popQueueWith(2)), "sharedCounter.popRoundTrips");
    // The load-bearing assertion: reverts to a POP gate with a counter of its own (or with none) go red here.
    expect(c.flowControlTriggered() == 2, "sharedCounter.bothPathsTickOneCounter",
           std::to_string(c.flowControlTriggered()));
    // Paired controls: a miss on either path adds nothing to the shared number.
    expect(!c.popFlowControlHitForTest(mq, popQueueWith(1)), "sharedCounter.popMissStaysQuiet");
    stage(c, mq, nSized(kTopic, 100, 1));
    expect(!c.flowControlHit(mq, key), "sharedCounter.pullMissStaysQuiet");
    expect(c.flowControlTriggered() == 2, "sharedCounter.missesTickNothing",
           std::to_string(c.flowControlTriggered()));
    // And the next trip on either side keeps counting in the same place.
    expect(c.popFlowControlHitForTest(mq, popQueueWith(5)), "sharedCounter.secondPopRoundTrips");
    expect(c.flowControlTriggered() == 3, "sharedCounter.popKeepsTheSameCounter",
           std::to_string(c.flowControlTriggered()));
    stage(c, mq, nSized(kTopic, 100, 3));
    expect(c.flowControlHit(mq, key), "sharedCounter.secondPullRoundTrips");
    expect(c.flowControlTriggered() == 4, "sharedCounter.pullJoinsItAgain",
           std::to_string(c.flowControlTriggered()));
}

// The POP gate never sums its sibling queues: only a queue whose **own** debt is over the ceiling
// pauses. Under an aggregating gate the five fixtures below (3 + 0 + 2 + 2 + 2 against a ceiling of 2)
// would pause every queue of the topic, so each miss is its own control.
void testPopGateIsPerQueueAndNeverSumsItsSiblings() {
    DefaultMQPushConsumer c("GID_PopFlowControlNoSiblingSum");
    const MessageQueue hot = queueOf(kTopic, 0);
    const MessageQueue idle = queueOf(kTopic, 1);
    c.setPopThresholdForQueue(2);
    auto hotPq = popQueueWith(3);
    auto idlePq = popQueueWith(0);
    expect(c.popFlowControlHitForTest(hot, hotPq), "popNoSum.ownDebtOverTheCeilingTrips");
    expect(!c.popFlowControlHitForTest(idle, idlePq), "popNoSum.siblingUnaffected");
    // Three more queues each exactly AT the ceiling: together with the hot one they hold 9 messages
    // against a ceiling of 2, yet only the queue that is over its own ceiling pauses.
    for (int32_t id = 2; id <= 4; id++) {
        expect(!c.popFlowControlHitForTest(queueOf(kTopic, id), popQueueWith(2)),
               "popNoSum.atOwnCeilingStillPops.q" + std::to_string(id));
    }
    expect(c.flowControlTriggered() == 1, "popNoSum.exactlyOneTick",
           std::to_string(c.flowControlTriggered()));
    // The same hot queue trips again next round (a pause that never re-checks would show up as a
    // single tick here).
    expect(c.popFlowControlHitForTest(hot, hotPq), "popNoSum.tripsAgainNextRound");
    expect(c.flowControlTriggered() == 2, "popNoSum.secondRoundSecondTick",
           std::to_string(c.flowControlTriggered()));
    // The recorded reason names this queue's own debt; a leftover cross-queue branch would sum them.
    LogCapture log;
    expect(c.popFlowControlHitForTest(hot, hotPq), "popNoSum.reasonRoundTrips");
    const std::vector<std::string> debugs = log.lines(" DEBUG [");
    expect(countWith(debugs, "waitAck=3") == 1, "popNoSum.reasonIsTheOwnDebt",
           std::to_string(debugs.size()));
    expect(countWith(debugs, "topicWaitAck=") == 0, "popNoSum.noTopicSumWording");
    expect(countWith(debugs, "pause pop") == 1, "popNoSum.debugOncePerTrip",
           std::to_string(debugs.size()));
}

// The WARN an operator actually reads has to explain the pause by itself: this queue's wait-ack debt,
// the POP ceiling it exceeded, the pause length and the shared trip counter. The quiet control (a debt
// exactly at the ceiling writes nothing) is what proves the text comes from the gate and not from some
// other round.
void testPopWarnTextNamesTheDebtAndTheCeiling() {
    DefaultMQPushConsumer c("GID_PopFlowControlWarnText");
    const MessageQueue mq = queueOf(kTopic, 7);
    c.setPopThresholdForQueue(5);
    {
        LogCapture log;
        expect(c.popFlowControlHitForTest(mq, popQueueWith(9)), "popWarn.trips");
        const std::vector<std::string> warns = log.lines(" WARN  [");
        expect(warns.size() == 1, "popWarn.exactlyOneWarn", std::to_string(warns.size()));
        const std::string line = warns.empty() ? std::string() : warns[0];
        expect(line.find("waitAck=9") != std::string::npos, "popWarn.namesTheWaitAckDebt", line);
        expect(line.find("popThresholdForQueue=5") != std::string::npos,
               "popWarn.namesThePopCeiling", line);
        expect(line.find("50 ms") != std::string::npos, "popWarn.namesThePauseMillis", line);
        expect(line.find("(flowControlTimes=1)") != std::string::npos,
               "popWarn.namesTheSharedCounter", line);
        expect(line.find(mq.toString()) != std::string::npos, "popWarn.namesTheQueue", line);
        // The per-round debug channel keeps the same two numbers, so a throttled queue is readable at
        // DEBUG without waiting for the 1000th trip.
        const std::vector<std::string> debugs = log.lines(" DEBUG [");
        expect(countWith(debugs, "waitAck=9") == 1, "popWarn.debugAlsoNamesTheDebt",
               std::to_string(debugs.size()));
        expect(countWith(debugs, "popThresholdForQueue=5") == 1, "popWarn.debugNamesTheCeiling",
               std::to_string(debugs.size()));
    }
    DefaultMQPushConsumer quiet("GID_PopFlowControlWarnTextQuiet");
    quiet.setPopThresholdForQueue(5);
    LogCapture log;
    expect(!quiet.popFlowControlHitForTest(mq, popQueueWith(5)), "popWarn.atCeilingIsQuiet");
    expect(log.lines(" WARN  [").empty(), "popWarn.noWarnForTheQuietRound",
           std::to_string(log.lines(" WARN  [").size()));
    expect(log.lines(" DEBUG [").empty(), "popWarn.noDebugForTheQuietRound",
           std::to_string(log.lines(" DEBUG [").size()));
    expect(quiet.flowControlTriggered() == 0, "popWarn.quietRoundTicksNothing");
}

// The POP trip joins the warn cadence of the shared counter instead of running its own. Bringing the
// counter to 999 through the pull gate is 999 pure function calls (no sleeps, no threads), so the two
// POP rounds that follow really are trip 1000 and trip 1001: the first stays on the debug channel, the
// second is the one that warns -- and it says so with the shared number.
void testPopTripJoinsTheWarnCadenceOfTheSharedCounter() {
    expect(kFlowControlWarnEvery == 1000, "popCadence.everyIs1000");
    expect(isFlowControlWarnTrip(1), "popCadence.firstTripWarns");
    expect(isFlowControlWarnTrip(1001), "popCadence.secondWarnIsTrip1001");
    // Controls: a POP trip at these positions writes the debug line only.
    for (int64_t n : {int64_t{0}, int64_t{2}, int64_t{1000}}) {
        expect(!isFlowControlWarnTrip(n), "popCadence.staysQuietOn" + std::to_string(n));
    }

    DefaultMQPushConsumer c("GID_PopFlowControlCadence");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    parkSiblingGates(c);
    c.setPullThresholdForQueue(1);
    c.setPopThresholdForQueue(1);
    stage(c, mq, nSized(kTopic, 100, 2));
    {
        LogCapture seed;
        for (int i = 0; i < 999; i++) {
            expect(c.flowControlHit(mq, key), "popCadence.pullRoundTrips");
        }
        expect(c.flowControlTriggered() == 999, "popCadence.seededAt999",
               std::to_string(c.flowControlTriggered()));
    }
    LogCapture log;
    expect(c.popFlowControlHitForTest(mq, popQueueWith(2)), "popCadence.popRoundAt1000Trips");
    expect(c.flowControlTriggered() == 1000, "popCadence.counterAt1000",
           std::to_string(c.flowControlTriggered()));
    expect(log.lines(" WARN  [").empty(), "popCadence.trip1000StaysOnDebug",
           std::to_string(log.lines(" WARN  [").size()));
    expect(countWith(log.lines(" DEBUG ["), "waitAck=2") == 1, "popCadence.debugEveryRound",
           std::to_string(log.lines(" DEBUG [").size()));
    expect(c.popFlowControlHitForTest(mq, popQueueWith(2)), "popCadence.popRoundAt1001Trips");
    const std::vector<std::string> warns = log.lines(" WARN  [");
    // The control for "the POP path never reaches the cadence": exactly one WARN, and it carries the
    // shared counter, not a POP-local one.
    expect(warns.size() == 1, "popCadence.thePopTripDoesWarn", std::to_string(warns.size()));
    expect(!warns.empty() && warns[0].find("(flowControlTimes=1001)") != std::string::npos,
           "popCadence.warnCarriesTheSharedCounter", warns.empty() ? std::string() : warns[0]);
    expect(countWith(log.lines(" DEBUG ["), "waitAck=2") == 2, "popCadence.debugBothRounds",
           std::to_string(log.lines(" DEBUG [").size()));
    // A quiet round in between changes neither the counter nor the cadence.
    expect(!c.popFlowControlHitForTest(mq, popQueueWith(1)), "popCadence.missIsQuiet");
    expect(c.flowControlTriggered() == 1001, "popCadence.missChangesNothing",
           std::to_string(c.flowControlTriggered()));
    expect(log.lines(" WARN  [").size() == 1, "popCadence.stillOneWarn",
           std::to_string(log.lines(" WARN  [").size()));
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
    parkSiblingGates(c);
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
    testHeldSetIsTheBufferPlusTheBatchesInTheListenerHands();
    testSizeGate();
    testSizeGateMeasuresBodyBytesNeverStoreSize();
    testSpanGate();
    testSpanGateNeverRunsForOrderlyConsumption();
    testPushPullBatchSizeDefaultsToThirtyTwo();
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
    testPopGateBoundaryIsStrictlyOverTheCeiling();
    testPopCeilingDefaultsTo96AndIsNeverADividedKnob();
    testPopTripSharesThePullFlowControlCounter();
    testPopGateIsPerQueueAndNeverSumsItsSiblings();
    testPopWarnTextNamesTheDebtAndTheCeiling();
    testPopTripJoinsTheWarnCadenceOfTheSharedCounter();
    testCachePauseIsFiftyMillis();
    testWarnCadence();
    std::printf("flow control: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
