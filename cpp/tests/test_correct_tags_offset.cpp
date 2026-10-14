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

const char* kGroup = "GID_CorrectTagsOffsetCppUnit";
const char* kTopic = "CorrectTagsOffsetCppUnitTopic";
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

// A consumer that was never start()ed is enough: correctTagsOffset / takeBatchForConsume
// / finishBatchConsume all work on local state only and never touch the network.
class Harness {
public:
    explicit Harness() : consumer_(kGroup), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        consumer_.setConsumeMessageBatchMaxSize(2);
    }

    void seedPending(const std::vector<MessageExt>& msgs) {
        consumer_.setPendingMessages(key_, msgs);
    }

    void correct(PullStatus status, int64_t nextOffset) {
        consumer_.correctTagsOffset(key_, status, nextOffset);
    }

    std::optional<int64_t> offset() const { return consumer_.consumeOffset(key_); }

    std::vector<MessageExt> take() { return consumer_.takeBatchForConsume(key_); }
    void finish() { consumer_.finishBatchConsume(key_); }
    std::vector<MessageExt> pending() const { return consumer_.pendingMessages(key_); }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
};

std::string offText(const std::optional<int64_t>& v) {
    return v ? std::to_string(*v) : "none";
}

void expectOffset(const Harness& h, std::optional<int64_t> want, const std::string& name) {
    const auto got = h.offset();
    expect(got == want, name, "want=" + offText(want) + " got=" + offText(got));
}

void testStatuses() {
    // Both empty-response statuses advance the offset; every other status leaves it untouched
    for (PullStatus s : {PullStatus::NO_NEW_MSG, PullStatus::NO_MATCHED_MSG}) {
        Harness h;
        h.seedPending({});
        h.correct(s, 42);
        expectOffset(h, 42, std::string("empty.") + pullStatusName(s) + ".advance");
        h.correct(s, 43);
        expectOffset(h, 43, std::string("empty.") + pullStatusName(s) + ".advanceAgain");
    }
    for (PullStatus s : {PullStatus::FOUND, PullStatus::OFFSET_ILLEGAL}) {
        Harness h;
        h.seedPending({});
        h.correct(s, 42);
        expectOffset(h, std::nullopt, std::string("notEmpty.") + pullStatusName(s) + ".ignored");
    }
}

void testIncreaseOnly() {
    Harness h;
    h.seedPending({});
    h.correct(PullStatus::NO_NEW_MSG, 100);
    expectOffset(h, 100, "increaseOnly.seed");
    h.correct(PullStatus::NO_NEW_MSG, 50);
    expectOffset(h, 100, "increaseOnly.noRegress");
    h.correct(PullStatus::NO_NEW_MSG, 100);
    expectOffset(h, 100, "increaseOnly.equalStays");
    h.correct(PullStatus::NO_NEW_MSG, 101);
    expectOffset(h, 101, "increaseOnly.advance");
}

void testProcessQueueGuard() {
    {
        // The buffer still holds unconsumed messages: the gate stays closed
        Harness h;
        h.seedPending({ext(0)});
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "guard.pendingNonEmpty");
    }
    {
        // Buffer empty, but one message sits with the listener (removeMessage has not run): closed
        Harness h;
        h.seedPending({ext(0)});
        std::vector<MessageExt> batch = h.take();
        expect(batch.size() == 1, "guard.takeOne", std::to_string(batch.size()));
        h.correct(PullStatus::NO_MATCHED_MSG, 42);
        expectOffset(h, std::nullopt, "guard.inflightBlocks");

        // Once consume finishing ends (re-queue on exception included) the count is zero and the correction applies
        h.finish();
        h.correct(PullStatus::NO_MATCHED_MSG, 42);
        expectOffset(h, 42, "guard.afterFinish");
    }
    {
        // No pending entry at all (never pulled this queue) counts as empty; a missing entry must not skip it
        Harness h;
        h.correct(PullStatus::NO_NEW_MSG, 7);
        expectOffset(h, 7, "guard.missingPendingAllows");
    }
}

void testTakeFinishWiring() {
    {
        // takeBatchForConsume removes one batch sized by consumeMessageBatchMaxSize, and it must
        // register in-flight in the same critical section: seen via "the gate closes right after a take"
        Harness h;
        h.seedPending(offsetBatch(3));
        std::vector<MessageExt> batch = h.take();
        expect(batch.size() == 2, "wiring.batchSize", std::to_string(batch.size()));
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "wiring.takenBlocks");
        expect(h.pending().size() == 1, "wiring.pendingLeft",
               std::to_string(h.pending().size()));

        // Leftovers were not taken: the buffer is non-empty and blocks the same way
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "wiring.pendingStillBlocks");

        // Once the second batch finishes too, the gate opens fully
        std::vector<MessageExt> rest = h.take();
        expect(rest.size() == 1, "wiring.restSize", std::to_string(rest.size()));
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.drainedAllows");
    }
    {
        // Two batches in flight (two consume threads): the count accumulates, one finish cannot open the gate
        Harness h;
        h.seedPending(offsetBatch(4));
        h.take();      // 2 messages
        h.take();      // another 2: two batches in flight
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "wiring.stillOneInFlight");
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.bothFinishedAllows");
    }
    {
        // A key erased once its count hit zero must be re-registrable: the gate closes again while batch 2 runs
        Harness h;
        h.seedPending(offsetBatch(3));
        h.take();      // 2 messages
        h.finish();
        h.take();      // the 3rd message: in-flight counting starts over
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "wiring.reRegistered");
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.reRegisteredFinishAllows");
    }
    {
        // Taking from an empty queue must not record a phantom in-flight batch, else the gate closes itself
        Harness h;
        h.seedPending({});
        expect(h.take().empty(), "wiring.emptyTake");
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.emptyTakeNoLeak");
    }
    {
        // finish on an already-erased key is a no-op and must not resurrect the count
        Harness h;
        h.seedPending(offsetBatch(1));
        h.take();
        h.finish();
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.doubleFinishNoResurrect");
    }
}

}  // namespace

int main() {
    testStatuses();
    testIncreaseOnly();
    testProcessQueueGuard();
    testTakeFinishWiring();
    std::printf("correct tags offset: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
