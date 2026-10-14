// Why it must be locked down offline: this sweep is the only reclaim path when "the listener blocks and never returns". Without it,
// one stuck message pins that queue's offset forever with no exception, log or timeout visible anywhere -- on a live cluster it only
// shows indirectly as "the message was sent but never arrives a second time"; and the sweep's own window is minutes-long while the
// 16-entries-per-round cap is only verifiable at second granularity offline with a fake clock. All three semantic cornerstones "fail quietly":
//   - only the head (smallest offset) is looked at: judged past it -> the messages behind a stuck head are never swept;
//   - the expiry test is **strictly greater than** consumeTimeout: writing >= sends the boundary message back early (a phantom redelivery);
//   - one round takes min(size,16) and the loop bound is computed before entering the loop: a while would empty the whole queue in one round.
//
//
// Offline can lock the "send-back failed" branch: for a consumer that never started sendMessageBack always fails (the function reports through
// its return value and throws nothing), so the settlement assertion is "the message stays put, the offset must not move"; "send-back succeeded -> removed ->
// %RETRY% redelivery -> the second delivery with reconsumeTimes=1" can only be evidenced on a live cluster
// (examples/live_clean_expired_msg.cpp is that live leg).
// The same assertions are covered by the sibling offline clean-expired-message tests in this repository.
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"

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

const char* kGroup = "GID_CleanExpiredCppUnit";
const char* kTopic = "CleanExpiredCppUnitTopic";
const char* kBroker = "broker-a";
const int64_t kNow = 1800000000000LL;

MessageQueue queue0() {
    return MessageQueue(kTopic, kBroker, 0);
}

MessageExt ext(int64_t queueOffset) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.body = "x";
    return m;
}

MessageExt stamped(int64_t queueOffset, int64_t tsMs) {
    MessageExt m = ext(queueOffset);
    m.putProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP, std::to_string(tsMs));
    return m;
}

std::vector<int64_t> offsetsOf(const std::vector<MessageExt>& msgs) {
    std::vector<int64_t> out;
    for (const MessageExt& m : msgs) out.push_back(m.queueOffset);
    return out;
}

bool hasOffset(const std::vector<MessageExt>& msgs, int64_t offset) {
    for (const MessageExt& m : msgs) {
        if (m.queueOffset == offset) return true;
    }
    return false;
}

// A consumer that never started is enough: sweeping, registration and offset advance are all local state; the only network action is the send-back
// (sendMessageBack), which must throw through client() when unstarted, and by contract the function swallows it and returns false.
class Harness {
public:
    explicit Harness(int32_t consumeTimeoutMinutes = 1)
        : consumer_(kGroup), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        consumer_.setConsumeTimeout(consumeTimeoutMinutes);
    }

    DefaultMQPushConsumer& consumer() { return consumer_; }
    const std::string& key() const { return key_; }

    void seedInflight(const std::vector<MessageExt>& msgs) {
        consumer_.setInflightMessages(key_, msgs);
    }
    void seedPending(const std::vector<MessageExt>& msgs) {
        consumer_.setPendingMessages(key_, msgs);
    }
    std::vector<MessageExt> entries() const { return consumer_.processQueueEntries(key_); }
    std::vector<MessageExt> pending() const { return consumer_.pendingMessages(key_); }
    int32_t sweep(int64_t nowMs) { return consumer_.cleanExpiredQueue(key_, nowMs); }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
};

class NoopOrderlyListener : public MessageListenerOrderly {
public:
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>&,
                                        ConsumeOrderlyContext&) override {
        return ConsumeOrderlyStatus::SUCCESS;
    }
};

// ---------------------------------------------------------------- criterion and cadence

void testExpiryArithmetic() {
    expect(!DefaultMQPushConsumer::isConsumeExpired(ext(0), 1, kNow),
           "expiry.emptyStamp.notExpired");

    expect(!DefaultMQPushConsumer::isConsumeExpired(stamped(0, kNow - 60000), 1, kNow),
           "expiry.exactlyAtLimit.notExpired");
    expect(DefaultMQPushConsumer::isConsumeExpired(stamped(0, kNow - 60001), 1, kNow),
           "expiry.oneMsPast.expired");

    // A future timestamp (clock rollback / across machines) must not be counted as expired either
    expect(!DefaultMQPushConsumer::isConsumeExpired(stamped(0, kNow + 5000), 1, kNow),
           "expiry.futureStamp.notExpired");

    // The threshold follows the consume configuration: the same message is still too fresh under the default 15 minute setting
    expect(!DefaultMQPushConsumer::isConsumeExpired(stamped(0, kNow - 60001), 15, kNow),
           "expiry.default15.notExpired");

    MessageExt bad = ext(0);
    bad.putProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP, "not-a-number");
    bool threw = false;
    try {
        (void)DefaultMQPushConsumer::isConsumeExpired(bad, 1, kNow);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    expect(threw, "expiry.malformedStamp.throws");
}

void testPeriodMatchesSchedule() {
    expect(DefaultMQPushConsumer::cleanExpirePeriodMillis(15) == 900000,
           "period.default15", std::to_string(DefaultMQPushConsumer::cleanExpirePeriodMillis(15)));
    expect(DefaultMQPushConsumer::cleanExpirePeriodMillis(1) == 60000, "period.oneMinute");
    expect(DefaultMQPushConsumer::cleanExpirePeriodMillis(0) == 60000, "period.zeroClamps");
    expect(DefaultMQPushConsumer::cleanExpirePeriodMillis(-5) == 60000, "period.negativeClamps");
}

// ---------------------------------------------------------------- entry selection semantics

void testOnlyHeadIsConsideredAndExpiryIsStrict() {
    Harness h;
    h.seedInflight({ext(0), stamped(1, kNow - 120000)});
    expect(h.sweep(kNow) == 0, "sweep.unstampedHead.noAttempt");
    expect(offsetsOf(h.entries()) == std::vector<int64_t>({0, 1}), "sweep.unstampedHead.entriesKept");

    // A head stamped just now (not expired) stops the sweep the same way
    Harness fresh;
    fresh.seedInflight({stamped(0, kNow - 1000)});
    expect(fresh.sweep(kNow) == 0, "sweep.fresh.notAttempted");

    Harness order;
    order.seedInflight({stamped(5, kNow - 1), stamped(3, kNow - 120000)});
    expect(order.sweep(kNow) == 2, "sweep.headIsMinOffset.notInsertionOrder");
    expect(offsetsOf(order.entries()).size() == 2, "sweep.headIsMinOffset.entriesKept");
}

void testFailedSendBackKeepsTheEntry() {
    Harness h;
    h.seedInflight({stamped(0, kNow - 120000)});
    expect(h.sweep(kNow) == 1, "fail.oneAttempt");
    expect(h.entries().size() == 1, "fail.entryKept", std::to_string(h.entries().size()));
    expect(!h.consumer().consumeOffset(h.key()).has_value(), "fail.offsetUntouched");
    h.sweep(kNow);
    expect(h.entries().size() == 1, "fail.stillKeptOnSecondRound");
}

void testSixteenCapPerPass() {
    Harness h;
    std::vector<MessageExt> msgs;
    for (int i = 0; i < 20; i++) msgs.push_back(stamped(i, kNow - 120000));
    h.seedInflight(msgs);
    const int32_t attempts = h.sweep(kNow);
    expect(attempts == 16, "cap.sixteenAttempts", std::to_string(attempts));
    expect(h.entries().size() == 20, "cap.entriesAllKept", std::to_string(h.entries().size()));
}

void testRemoveOnlyWhenStillHead() {
    Harness h;
    h.seedPending({ext(5), ext(3)});
    const auto head = h.consumer().processQueueHead(h.key());
    expect(head.has_value() && head->queueOffset == 3, "remove.headIsMinOffset");

    h.consumer().removeExpiredEntryIfStillHead(h.key(), ext(3));
    expect(offsetsOf(h.entries()) == std::vector<int64_t>({5}), "remove.stillHeadRemoved");

    // 5 is pending now, but what is passed in is a 5 that "is no longer the head": if a 4 appears in front, 5 must not be removed
    h.seedPending({ext(4), ext(5)});
    h.consumer().removeExpiredEntryIfStillHead(h.key(), ext(5));
    expect(hasOffset(h.entries(), 5), "remove.notHeadLeftAlone");
    expect(h.entries().size() == 2, "remove.notHeadNothingRemoved");

    // An offset absent from the table: no-op (it must not remove somebody else's entry)
    h.consumer().removeExpiredEntryIfStillHead(h.key(), ext(99));
    expect(h.entries().size() == 2, "remove.absentIsNoop");
}

void testContainsMessageIsByQueueOffset() {
    Harness h;
    h.seedInflight({stamped(7, kNow - 120000)});
    h.seedPending({ext(8)});
    expect(h.consumer().processQueueContains(h.key(), ext(7)), "contains.inflightSide");
    expect(h.consumer().processQueueContains(h.key(), ext(8)), "contains.pendingSide");
    expect(!h.consumer().processQueueContains(h.key(), ext(9)), "contains.absent");
    expect(!h.consumer().processQueueContains("no-such-queue", ext(7)), "contains.missingKey");
}

void testEntriesViewIsUnionDeduped() {
    Harness h;
    h.seedPending({ext(0), ext(1)});
    h.seedInflight({ext(1), ext(2)});
    expect(offsetsOf(h.entries()).size() == 3, "union.size",
           std::to_string(h.entries().size()));
    expect(hasOffset(h.entries(), 0) && hasOffset(h.entries(), 2), "union.bothSides");
}

// ---------------------------------------------------------------- sweep scope and orderly consumption

void testSweepScopeIsHeldQueuesOnly() {
    DefaultMQPushConsumer c(kGroup);
    c.setConsumeTimeout(1);
    const std::string k0 = DefaultMQPushConsumer::offsetKey(queue0());
    const MessageQueue q1(kTopic, kBroker, 1);
    const std::string k1 = DefaultMQPushConsumer::offsetKey(q1);
    c.setInflightMessages(k0, {stamped(0, kNow - 120000)});
    c.setInflightMessages(k1, {stamped(0, kNow - 120000)});

    expect(c.cleanExpiredMsgOnce(kNow) == 0, "scope.noneHeld");
    c.setAssignedQueue(k0, queue0());
    expect(c.cleanExpiredMsgOnce(kNow) == 1, "scope.onlyHeldSwept");
    c.setAssignedQueue(k1, q1);
    expect(c.cleanExpiredMsgOnce(kNow) == 2, "scope.bothHeld");

    // The queue was revoked (the OFFSET_ILLEGAL correction goes through retireQueueLocked): registration cleared, no longer swept
    c.handleOffsetIllegal(k0, 5);
    expect(c.processQueueEntries(k0).empty(), "scope.retiredClearsInflight");
    expect(c.cleanExpiredMsgOnce(kNow) == 1, "scope.retiredNotSwept");
}

void testOrderlyConsumerIsNeverSwept() {
    DefaultMQPushConsumer c(kGroup);
    c.setConsumeTimeout(1);
    c.setMessageListener(std::make_shared<NoopOrderlyListener>());
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    c.setInflightMessages(key, {stamped(0, kNow - 3600000)});
    expect(c.cleanExpiredQueue(key, kNow) == 0, "orderly.noAttempt");
    expect(c.processQueueEntries(key).size() == 1, "orderly.entryKept");
}

// ---------------------------------------------------------------- interplay with the consumption path

class ObservingListener : public MessageListenerConcurrently {
public:
    ObservingListener(DefaultMQPushConsumer* c, std::string key, bool reclaim)
        : consumer_(c), key_(std::move(key)), reclaim_(reclaim) {}

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        containsDuring_ = consumer_->processQueueContains(key_, msgs[0]);
        entriesDuring_ = static_cast<int>(consumer_->processQueueEntries(key_).size());
        const auto head = consumer_->processQueueHead(key_);
        // The entry lying in the registry must be the **stamped** copy: batches in this port are value copies, so if the
        // stamping and registration order were swapped the sweep would never see these messages (silently broken on a live cluster)
        headStamp_ = head.has_value()
                         ? head->getProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP)
                         : std::string();
        if (reclaim_) {
            // Simulate "during the suspend the sweep sent it back successfully and removed it" (the sweep thread does this on a live cluster)
            consumer_->removeExpiredEntryIfStillHead(key_, msgs[0]);
        }
        return ConsumeConcurrentlyStatus::RECONSUME_LATER;
    }

    bool containsDuring_ = false;
    int entriesDuring_ = 0;
    std::string headStamp_;

private:
    DefaultMQPushConsumer* consumer_;
    std::string key_;
    bool reclaim_;
};

class OkListener : public MessageListenerConcurrently {
public:
    explicit OkListener(int* seen) : seen_(seen) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        ++(*seen_);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    int* seen_;
};

void testConcurrentBatchSeesItselfInTheProcessQueue() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    auto listener = std::make_shared<ObservingListener>(&c, key, /*reclaim=*/false);
    c.setMessageListener(listener);
    // In the real dispatch chain the pending_ slot exists before this batch is taken (takeBatchLocked pops and leaves an empty
    // deque behind), and a failed send-back is pushed back into that very slot -- the offline setup reproduces the same shape.
    c.setPendingMessages(key, {});

    std::vector<MessageExt> batch{ext(0)};
    const bool advanced = c.consumeBatch(key, queue0(), batch);

    expect(listener->containsDuring_, "batch.containsDuringListener");
    expect(listener->entriesDuring_ == 1, "batch.entriesDuringListener",
           std::to_string(listener->entriesDuring_));
    expect(!listener->headStamp_.empty(), "batch.registeredCopyIsStamped");

    // Not started: the send-back fails -> the message goes back to the queue head, the offset does not move, false is returned (existing ackIndex semantics)
    expect(!advanced, "batch.failedSendBackReturnsFalse");
    expect(offsetsOf(c.pendingMessages(key)) == std::vector<int64_t>({0}), "batch.requeuedToPending");
    expect(!c.consumeOffset(key).has_value(), "batch.offsetUntouched");
    // The inflight side must be deregistered (what remains is the pending copy "pushed back after the failed send-back")
    expect(hasOffset(c.processQueueEntries(key), 0) && c.processQueueEntries(key).size() == 1,
           "batch.inflightDeregistered");
}

void testSendBackSkipsAMessageTheSweepAlreadyReclaimed() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    auto listener = std::make_shared<ObservingListener>(&c, key, /*reclaim=*/true);
    c.setMessageListener(listener);

    std::vector<MessageExt> batch{ext(0)};
    const bool advanced = c.consumeBatch(key, queue0(), batch);

    expect(advanced, "skip.returnsTrueWhenNothingToSendBack");
    expect(c.pendingMessages(key).empty(), "skip.notRequeued");
    expect(c.processQueueEntries(key).empty(), "skip.entryGone",
           std::to_string(c.processQueueEntries(key).size()));
    expect(c.consumeOffset(key) == 1, "skip.offsetAdvanced");
}

void testConcurrentBatchDeregistersOnSuccessAndAcrossRounds() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    int seen = 0;
    c.setMessageListener(std::make_shared<OkListener>(&seen));

    std::vector<MessageExt> first{ext(0)};
    expect(c.consumeBatch(key, queue0(), first), "ok.firstReturnsTrue");
    expect(c.processQueueEntries(key).empty(), "ok.deregisteredAfterReturn");
    expect(c.consumeOffset(key) == 1, "ok.offsetAdvanced");
    expect(seen == 1, "ok.listenerCalled");

    // Second round: if the previous batch missed a deregistration, the in-register view would show 2 entries here (leak detection)
    std::vector<MessageExt> second{ext(1)};
    expect(c.consumeBatch(key, queue0(), second), "ok.secondReturnsTrue");
    expect(c.processQueueEntries(key).empty(), "ok.noLeakAcrossRounds");
    expect(c.consumeOffset(key) == 2, "ok.offsetAdvancedAgain");
}

void testTakeThenConsumeKeepsOneCopy() {
    // An offline miniature of the real dispatch chain: takeBatchForConsume (takes from pending and registers as in-flight) ->
    // consumeBatch (consume + deregister). After the take the message may exist only on the in-flight side; both sides holding it means duplicate delivery.
    DefaultMQPushConsumer c(kGroup);
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    int seen = 0;
    c.setMessageListener(std::make_shared<OkListener>(&seen));
    c.setPendingMessages(key, {ext(0)});

    uint64_t epoch = 0;
    std::vector<MessageExt> batch = c.takeBatchForConsume(key, &epoch);
    expect(batch.size() == 1, "pipeline.tookOne", std::to_string(batch.size()));
    expect(c.pendingMessages(key).empty(), "pipeline.pendingEmptied");
    // There is a transient window between the take and "registering the in-flight message": the message hangs on inflightCount_ (a counter),
    // not yet in the union view. The sweep cannot reach it (no stamp for this round) and the correctTagsOffset gate reads the
    // counter -- neither side misjudges, so the shape of the window is pinned here exactly as it is.
    expect(c.processQueueEntries(key).empty(), "pipeline.takeWindowNotInUnionView");

    expect(c.consumeBatch(key, queue0(), batch, epoch), "pipeline.consumeReturnsTrue");
    expect(c.processQueueEntries(key).empty(), "pipeline.drained");
    expect(c.consumeOffset(key) == 1, "pipeline.offsetAdvanced");
    expect(seen == 1, "pipeline.listenerCalledOnce");
}

}  // namespace

int main() {
    testExpiryArithmetic();
    testPeriodMatchesSchedule();
    testOnlyHeadIsConsideredAndExpiryIsStrict();
    testFailedSendBackKeepsTheEntry();
    testSixteenCapPerPass();
    testRemoveOnlyWhenStillHead();
    testContainsMessageIsByQueueOffset();
    testEntriesViewIsUnionDeduped();
    testSweepScopeIsHeldQueuesOnly();
    testOrderlyConsumerIsNeverSwept();
    testConcurrentBatchSeesItselfInTheProcessQueue();
    testSendBackSkipsAMessageTheSweepAlreadyReclaimed();
    testConcurrentBatchDeregistersOnSuccessAndAcrossRounds();
    testTakeThenConsumeKeepsOneCopy();
    std::printf("clean expired msg: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
