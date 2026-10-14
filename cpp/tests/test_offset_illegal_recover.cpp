// Why this must be locked down offline: getting this path wrong fails **silently** in two extreme ways --
//   - only the pull cursor is moved to nextBeginOffset without dropping the queue or freezing: the broker has just
//     corrected the offset into a legal range, but an older batch already fetched and not yet acked on this queue
//     pushes the offset back to an illegal value when it acks, and the next pull is rejected by the broker again --
//     bouncing back and forth between the client and the broker, never stopping;
//   - the corrected offset is not persisted immediately: the process crashes before the next periodic flush (5s by default),
//     the broker still holds the illegal offset and a restart pulls from it -- the correction never really happened.
// Both directions are indistinguishable in a short live window (messages keep being consumed while bouncing; only a crash exposes it),
// so the assertions stay offline as much as possible; a separate live proof chain exists (examples/live_offset_illegal.cpp S1/S2:
#include <cstdio>
#include <optional>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/result.h"
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

const char* kGroup = "GID_OffsetIllegalCppUnit";
const char* kTopic = "OffsetIllegalCppUnitTopic";
const char* kBroker = "broker-a";

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

std::vector<MessageExt> offsetBatch(size_t n) {
    std::vector<MessageExt> out;
    for (size_t i = 0; i < n; i++) {
        out.push_back(ext(static_cast<int64_t>(i)));
    }
    return out;
}

// Listener that accepts the whole batch (ackIndex stays default); only records the call count
class RecordingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        ++calls;
        lastSize = msgs.size();
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
    int calls = 0;
    size_t lastSize = 0;
};

// A non-started consumer suffices: handleOffsetIllegal / consumeBatch / correctTagsOffset all work on local
// state; onQueuesRevoked returns immediately when mqClient_ is null (no real-RPC branch in this port).
class Harness {
public:
    Harness() : consumer_(kGroup), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        listener_ = std::make_shared<RecordingListener>();
        consumer_.setMessageListener(listener_);
        consumer_.setAssignedQueue(key_, queue0());
    }

    // Consume one batch and advance the offset in place: the offset table has no direct write surface, so a known offset is
    // produced through the real consumption path (offline sendMessageBack always fails, so only whole-batch-accepted batches are used here).
    void seedConsumed(int64_t uptoOffsetExclusive) {
        std::vector<MessageExt> batch;
        for (int64_t i = 0; i < uptoOffsetExclusive; i++) {
            batch.push_back(ext(i));
        }
        consumer_.consumeBatch(key_, queue0(), batch, consumer_.queueEpoch(key_));
    }

    void recover(int64_t corrected) { consumer_.handleOffsetIllegal(key_, corrected); }

    const std::string& key() const { return key_; }
    RecordingListener& listener() { return *listener_; }
    DefaultMQPushConsumer& consumer() { return consumer_; }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
    std::shared_ptr<RecordingListener> listener_;
};

std::string offText(const std::optional<int64_t>& v) {
    return v ? std::to_string(*v) : "none";
}

void expectOffset(Harness& h, std::optional<int64_t> want, const std::string& name) {
    const auto got = h.consumer().consumeOffset(h.key());
    expect(got == want, name, "want=" + offText(want) + " got=" + offText(got));
}

// ------------------------------------------------- recovery = freeze + drop queue + epoch +1

void testRecoverDropsQueueState() {
    Harness h;
    h.seedConsumed(3);                    // offset 3
    expectOffset(h, 3, "recover.seedOffset");
    h.consumer().setPendingMessages(h.key(), {ext(3), ext(4)});
    h.consumer().setLastPullAt(h.key(), 1234567890);

    h.recover(0);

    // All local queue state is invalidated: buffer, cursor, consumed offset, pull-time table (the rest is carried by the epoch and the freeze)
    expect(h.consumer().pendingMessages(h.key()).empty(), "recover.pendingCleared");
    expectOffset(h, std::nullopt, "recover.offsetTableCleared");
    expect(h.consumer().queueEpoch(h.key()) == 1, "recover.epochBumped",
           std::to_string(h.consumer().queueEpoch(h.key())));
    expect(h.consumer().offsetFrozen(h.key()), "recover.frozen");
    // Ownership revoked: after the time table is cleared the old pull loop fails ownsQueue on the next round and exits (this also
    // proves recovery goes through rebalance's retireQueueLocked instead of a separate half-baked cleanup)
    expect(h.consumer().lastPullAt(h.key()) == -1, "recover.ownershipDropped",
           std::to_string(h.consumer().lastPullAt(h.key())));
}

void testRecoverWithoutQueueIsNoop() {
    // The queue is no longer owned by this instance (concurrent revoke): without a MessageQueue there is no object to persist, and one must not
    // be invented out of thin air; the freeze and epoch bump still happen (the corrected value must still block any late ack), and the network
    // cleanup of this port returns right after the null sentinel check -- calling on a non-started consumer must not crash.
    Harness h;
    const std::string key = DefaultMQPushConsumer::offsetKey(MessageQueue("T2", kBroker, 7));
    h.consumer().handleOffsetIllegal(key, 5);
    expect(h.consumer().queueEpoch(key) == 1, "noQueue.epochBumped",
           std::to_string(h.consumer().queueEpoch(key)));
    expect(h.consumer().offsetFrozen(key), "noQueue.frozen");
    expectOffset(h, std::nullopt, "noQueue.noOffsetInvented");
    expect(!h.consumer().offsetFrozen(h.key()), "noQueue.siblingUntouched");
}

void testSecondRecoverKeepsEpochMonotonic() {
    // Two consecutive recoveries (the broker got reset again): the epoch may only increase; falling back to 1 would let the
    // batch dropped by the first correction suddenly "match the epoch again" and come back to life.
    Harness h;
    h.recover(0);
    h.recover(0);
    expect(h.consumer().queueEpoch(h.key()) == 2, "twice.epochMonotonic",
           std::to_string(h.consumer().queueEpoch(h.key())));
    expect(h.consumer().offsetFrozen(h.key()), "twice.stillFrozen");
}

// ------------------------------------------------- freeze: the corrected value is not overturned by in-flight acks

void testFrozenOffsetIgnoresAck() {
    Harness h;
    h.seedConsumed(3);                    // offset 3 (before correction)
    h.recover(0);                         // correct to 0 and freeze
    const int callsBefore = h.listener().calls;

    const uint64_t epoch = h.consumer().queueEpoch(h.key());
    h.consumer().consumeBatch(h.key(), queue0(), offsetBatch(3), epoch);

    expect(h.listener().calls > callsBefore, "frozenAck.listenerRan");
    expectOffset(h, std::nullopt, "frozenAck.offsetUntouched");
}

void testFrozenOffsetIgnoresCorrectTagsOffset() {
    Harness h;
    h.recover(0);
    h.consumer().correctTagsOffset(h.key(), PullStatus::NO_NEW_MSG, 110);
    expectOffset(h, std::nullopt, "frozenTags.offsetUntouched");
}

void testFrozenOffsetSurvivesTheDrop() {
    Harness h;
    h.recover(0);
    h.seedConsumed(2);                    // try to push the offset from "none" up to 2
    expectOffset(h, std::nullopt, "frozenSurvives.noResurrect");
    expect(h.consumer().offsetFrozen(h.key()), "frozenSurvives.stillFrozen");
}


void testConsumeBatchEpochGate() {
    {
        Harness h;
        const uint64_t epoch = h.consumer().queueEpoch(h.key());
        const bool done = h.consumer().consumeBatch(h.key(), queue0(), offsetBatch(2),
                                                    epoch + 7);
        expect(!done, "epochGate.staleReturnsFalse");
        expect(h.listener().calls == 0, "epochGate.staleNotConsumed",
               std::to_string(h.listener().calls));
        expectOffset(h, std::nullopt, "epochGate.staleNoAck");
    }
    {
        // matching epoch => consume normally and advance (the gate must not block the normal path either)
        Harness h;
        const uint64_t epoch = h.consumer().queueEpoch(h.key());
        const bool done = h.consumer().consumeBatch(h.key(), queue0(), offsetBatch(2), epoch);
        expect(done, "epochGate.currentConsumed");
        expect(h.listener().calls == 1, "epochGate.currentListenerRan");
        expectOffset(h, 2, "epochGate.currentAck");
    }
}

void testNoCrossQueueInterference() {
    // Recovery only touches this queue: sibling queues' epoch / freeze / ownership must stay as they were (an implementation
    // that "clears the whole table" would redeliver every other queue of the same instance).
    Harness h;
    const std::string sibling =
        DefaultMQPushConsumer::offsetKey(MessageQueue(kTopic, kBroker, 1));
    h.consumer().setAssignedQueue(sibling, MessageQueue(kTopic, kBroker, 1));
    h.consumer().setLastPullAt(sibling, 1234567890);
    const uint64_t siblingEpoch = h.consumer().queueEpoch(sibling);

    h.recover(0);

    expect(h.consumer().queueEpoch(sibling) == siblingEpoch, "cross.siblingEpoch");
    expect(!h.consumer().offsetFrozen(sibling), "cross.siblingNotFrozen");
    expect(h.consumer().lastPullAt(sibling) == 1234567890, "cross.siblingStillAssigned",
           std::to_string(h.consumer().lastPullAt(sibling)));
}

}  // namespace

int main() {
    testRecoverDropsQueueState();
    testRecoverWithoutQueueIsNoop();
    testSecondRecoverKeepsEpochMonotonic();
    testFrozenOffsetIgnoresAck();
    testFrozenOffsetIgnoresCorrectTagsOffset();
    testFrozenOffsetSurvivesTheDrop();
    testConsumeBatchEpochGate();
    testNoCrossQueueInterference();
    std::printf("offset illegal recover: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
