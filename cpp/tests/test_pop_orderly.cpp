// Covers the semantics of POP orderly listening:
//   - submitPopConsumeRequest:161-166 -- dispatch into the orderly skeleton; the listener is not invoked and no ack happens
//     (the wait-ack backlog stays on pq = it only grows; the pop loop's wait-ack gate reports that
//      growing debt on the client-wide flow-control counter and pauses this queue's rounds)
//   - submitConsumeRequest:178-191 -- the dedup set compares (pq reference, mq): repeated submits of the same queue
//     enqueue only once; after rebalance installs a **new** PopProcessQueue (different pointer identity) it enqueues as usual
//   - ConsumeRequest.run:315-324 -- the request is removed only when pq is revoked; on a live queue it is a no-op
//   - the concurrent branch must stay unaffected by the split (regression guard: breaking the split means either orderly
//     messages get consumed by the wrong path or concurrent POP messages silently vanish)
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

const char* kGroup = "GID_PopOrderlyCpp";
const char* kTopic = "PopOrderlyCppTopic";
const char* kBroker = "broker-a";

MessageQueue queue0() { return MessageQueue(kTopic, kBroker, 0); }

MessageExt ext(int64_t queueOffset) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.msgId = "pop-orderly-" + std::to_string(queueOffset);
    m.body = "body-" + std::to_string(queueOffset);
    return m;
}

/// POP_CK of a "freshly popped" message: consumePopBatch's isPopTimeout check relies on it;
/// without it the concurrent branch discards the batch as timed out (the listener is never called).
void stampFreshPopCk(MessageExt& m) {
    m.putProperty(MessageConst::PROPERTY_POP_CK,
                  extra_info::buildExtraInfo(/*ckQueueOffset=*/m.queueOffset,
                                             /*popTime=*/UtilAll::currentTimeMillis(),
                                             /*invisibleTime=*/60000, /*reviveQid=*/0,
                                             kTopic, kBroker, /*queueId=*/0,
                                             /*msgQueueOffset=*/m.queueOffset));
}

class CountingOrderlyListener : public MessageListenerOrderly {
public:
    int* calls;
    explicit CountingOrderlyListener(int* sink) : calls(sink) {}
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>&,
                                        ConsumeOrderlyContext&) override {
        ++*calls;
        return ConsumeOrderlyStatus::SUCCESS;
    }
};

class CountingConcurrentListener : public MessageListenerConcurrently {
public:
    int* calls;
    explicit CountingConcurrentListener(int* sink) : calls(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        ++*calls;
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

std::vector<MessageExt> batch2() { return {ext(0), ext(1)}; }

// Silence the recorder for a round that deliberately trips a gate: the assertions below are on the
// retained backlog, the gate's answer and the shared hit counter; the log wording itself is captured
// by the flow-control suite, which redirects the recorder into a file under /tmp.
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

// ------------------------------------------------ dispatch: orderly enters the skeleton, listener not invoked

void testOrderlyDispatchNeverInvokesListener() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingOrderlyListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    // queuePopLoop already incFoundMsg before dispatch: the backlog goes up first
    pq->incFoundMsg(2);
    c.submitPopConsumeRequest(batch2(), pq, queue0());
    expect(calls == 0, "dispatch.orderly.listenerNotCalled", std::to_string(calls));
    // no ack and no renewal: the backlog stays on pq (the broker side waits for invisibleTime to expire, then revives and redelivers)
    expect(pq->waitAckCount() == 2, "dispatch.orderly.neverAcked",
           std::to_string(pq->waitAckCount()));
    expect(c.popOrderlyRequestCount() == 1, "dispatch.orderly.requestKept",
           std::to_string(c.popOrderlyRequestCount()));
}

// The skeleton never acks, so the backlog above is exactly what the pop loop's wait-ack gate has to
// report: instead of pausing the round silently, the queue over its own ceiling ticks the SAME
// client-wide flow-control counter the pull path's cache gate uses. Control at the end: a queue with no
// pop state left (revoked by rebalance) is not a flow-control trip.
void testSkeletonBacklogTripsTheSharedFlowControlCounter() {
    QuietLog quiet;
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingOrderlyListener>(&calls));
    c.setPopThresholdForQueue(1);
    auto pq = std::make_shared<PopProcessQueue>();
    pq->incFoundMsg(2);                       // queuePopLoop counts the batch before dispatch
    c.submitPopConsumeRequest(batch2(), pq, queue0());
    expect(pq->waitAckCount() == 2, "skeletonGate.backlogStaysUnacked",
           std::to_string(pq->waitAckCount()));
    expect(c.flowControlTriggered() == 0, "skeletonGate.dispatchAloneTicksNothing",
           std::to_string(c.flowControlTriggered()));
    // 2 over a ceiling of 1: the next round of this queue is a flow-control hit.
    expect(c.popFlowControlHitForTest(queue0(), pq), "skeletonGate.overOwnCeilingTrips");
    expect(c.flowControlTriggered() == 1, "skeletonGate.countedOnTheSharedCounter",
           std::to_string(c.flowControlTriggered()));
    // A second round on the same unacked backlog ticks again (the gate re-reads the debt every round,
    // it is not a one-shot latch) -- still this queue's own debt, still one counter.
    expect(c.popFlowControlHitForTest(queue0(), pq), "skeletonGate.tripsAgainNextRound");
    expect(c.flowControlTriggered() == 2, "skeletonGate.secondRoundSecondTick",
           std::to_string(c.flowControlTriggered()));
    expect(calls == 0, "skeletonGate.listenerStillNotCalled", std::to_string(calls));
    // Controls: the ceiling is not a target (exactly 1 unacked still pops), and a revoked queue with no
    // pop state at all never counts as a hit.
    auto atCeiling = std::make_shared<PopProcessQueue>();
    atCeiling->incFoundMsg(1);
    expect(!c.popFlowControlHitForTest(queue0(), atCeiling), "skeletonGate.atTheCeilingPops");
    expect(!c.popFlowControlHitForTest(queue0(), nullptr), "skeletonGate.noPopStateIsQuiet");
    expect(c.flowControlTriggered() == 2, "skeletonGate.quietRoundsAddNothing",
           std::to_string(c.flowControlTriggered()));
}

// ------------------------------------------------ split: the concurrent branch still consumes normally (regression guard)

void testConcurrentDispatchStillConsumes() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingConcurrentListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    pq->incFoundMsg(1);
    std::vector<MessageExt> msgs{ext(0)};
    stampFreshPopCk(msgs[0]);
    c.submitPopConsumeRequest(msgs, pq, queue0());
    expect(calls == 1, "dispatch.concurrent.listenerCalled", std::to_string(calls));
    // CONSUME_SUCCESS acks the whole batch: waitAckCount returns to 0 (ackPopMsg cannot go out while not started,
    // but pq->ack() still runs -- on a real machine the ack is confirmed by the broker; this locks the pq counter half only)
    expect(pq->waitAckCount() == 0, "dispatch.concurrent.acked",
           std::to_string(pq->waitAckCount()));
}

// ------------------------------------------------ dedup equality (pq reference + mq)

void testRequestDedup() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingOrderlyListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    // Repeated submit with the same (pq, mq): ConcurrentSet.add returns false, no duplicate enqueue
    c.submitPopOrderlyRequest(pq, queue0(), /*force=*/false);
    c.submitPopOrderlyRequest(pq, queue0(), /*force=*/false);
    expect(c.popOrderlyRequestCount() == 1, "dedup.samePqSameMq.once",
           std::to_string(c.popOrderlyRequestCount()));
    // different mq => a new request
    c.submitPopOrderlyRequest(pq, MessageQueue(kTopic, kBroker, 1), false);
    expect(c.popOrderlyRequestCount() == 2, "dedup.differentMq.counted",
           std::to_string(c.popOrderlyRequestCount()));
    // same mq but a **new** PopProcessQueue (rebalance revoked it and handed it back) => a new request
    auto fresh = std::make_shared<PopProcessQueue>();
    c.submitPopOrderlyRequest(fresh, queue0(), false);
    expect(c.popOrderlyRequestCount() == 3, "dedup.newPq.counted",
           std::to_string(c.popOrderlyRequestCount()));
    c.submitPopOrderlyRequest(pq, queue0(), /*force=*/true);
    expect(c.popOrderlyRequestCount() == 3, "dedup.force.staysSingle",
           std::to_string(c.popOrderlyRequestCount()));
}

// ------------------------------------------------ request removal: only when the pq is revoked

void testDroppedRequestIsRemoved() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingOrderlyListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    c.submitPopOrderlyRequest(pq, queue0(), false);
    expect(c.popOrderlyRequestCount() == 1, "dropped.primed",
           std::to_string(c.popOrderlyRequestCount()));
    // rebalance revokes -> pq marked dropped -> the request hits the dropped branch: erase itself + do not invoke the listener
    pq->setDropped(true);
    c.runPopOrderlyRequest(pq, queue0());
    expect(c.popOrderlyRequestCount() == 0, "dropped.removed",
           std::to_string(c.popOrderlyRequestCount()));
    expect(calls == 0, "dropped.listenerStillNotCalled", std::to_string(calls));
    // the submit path on a revoked queue erases itself the same way (run clears the entry before returning from the dropped branch)
    c.submitPopOrderlyRequest(pq, queue0(), false);
    expect(c.popOrderlyRequestCount() == 0, "dropped.submitAlsoCleans",
           std::to_string(c.popOrderlyRequestCount()));
}

// ------------------------------------------------ run on a live queue is a no-op

void testRunOnLiveQueueIsANoOp() {
    DefaultMQPushConsumer c(kGroup);
    int calls = 0;
    c.setMessageListener(std::make_shared<CountingOrderlyListener>(&calls));
    auto pq = std::make_shared<PopProcessQueue>();
    pq->incFoundMsg(3);
    c.submitPopOrderlyRequest(pq, queue0(), false);
    c.runPopOrderlyRequest(pq, queue0());
    expect(c.popOrderlyRequestCount() == 1, "noop.requestKept",
           std::to_string(c.popOrderlyRequestCount()));
    expect(pq->waitAckCount() == 3, "noop.untouched", std::to_string(pq->waitAckCount()));
    expect(calls == 0, "noop.listenerNotCalled", std::to_string(calls));
}

}  // namespace

int main() {
    testOrderlyDispatchNeverInvokesListener();
    testSkeletonBacklogTripsTheSharedFlowControlCounter();
    testConcurrentDispatchStillConsumes();
    testRequestDedup();
    testDroppedRequestIsRemoved();
    testRunOnLiveQueueIsANoOp();
    std::printf("pop orderly: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
