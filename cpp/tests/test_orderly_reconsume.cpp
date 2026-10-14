// Offline unit test for the orderly-consumption redelivery gate (checkReconsumeTimes / send-back) -- no cluster required.
//
//
// Plus the "lift into the request header" step of DefaultMQProducerImpl.sendKernelImpl:1004-1018: the broker
// handleRetryAndDLQ (SendMessageProcessor:197-210) reads reconsumeTimes / maxReconsumeTimes from the
// requestHeader when judging a dead letter, not from the message properties.
//
// Why all three places must be locked down offline: every breakage is **silent**. Without the +1 the threshold is never
// reached; lifting the wrong field makes the broker fall back to the consumer group default of 16; and if the queue stays
// suspended after a successful send-back, one poison message blocks that queue forever -- on a live cluster that looks
// exactly like "the consumer died".
//
// ⚠ Offline can only lock the "send-back failed" branch: a consumer that never started has no internal producer, so
// orderlySendMessageBack must return false. "send-back succeeded -> offset advances, queue no longer blocked" is proven on a live cluster by S12 of examples/live_redelivery.cpp.
#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

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

const char* kGroup = "GID_OrderlyReconsumeCpp";
const char* kTopic = "OrderlyReconsumeCppTopic";
const char* kBroker = "broker-a";
const int32_t kIntMax = std::numeric_limits<int32_t>::max();

std::string retryTopic() { return std::string("%RETRY%") + kGroup; }

MessageQueue queue0() { return MessageQueue(kTopic, kBroker, 0); }

MessageExt ext(int64_t queueOffset, int32_t reconsumeTimes = 0) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.reconsumeTimes = reconsumeTimes;
    m.msgId = "offset-msg-id-" + std::to_string(queueOffset);
    m.body = "body-" + std::to_string(queueOffset);
    return m;
}

class OrderlyListener : public MessageListenerOrderly {
public:
    explicit OrderlyListener(ConsumeOrderlyStatus status) : status_(status) {}

    OrderlyListener& manualCommit(bool autoCommit) {
        autoCommit_ = autoCommit;
        return *this;
    }

    // Set the suspend duration on the context (-1 = not set, falls back to the consumer configuration).
    OrderlyListener& asking(int32_t ms) {
        suspendMs_ = ms;
        return *this;
    }

    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>&,
                                        ConsumeOrderlyContext& ctx) override {
        if (autoCommit_.has_value()) ctx.autoCommit = *autoCommit_;
        if (suspendMs_.has_value()) ctx.suspendCurrentQueueTimeMillis = *suspendMs_;
        return status_;
    }

private:
    ConsumeOrderlyStatus status_;
    std::optional<bool> autoCommit_;
    std::optional<int32_t> suspendMs_;
};

// An orderly consumer that never touches the network: the offset and the pending buffer are wired up by hand.
class Harness {
public:
    explicit Harness(int32_t maxReconsumeTimes) : consumer_(kGroup), key_(
        DefaultMQPushConsumer::offsetKey(queue0())) {
        consumer_.setMaxReconsumeTimes(maxReconsumeTimes);
        consumer_.setSuspendCurrentQueueTimeMillis(0);  // unit tests do not wait 1s
        consumer_.setPendingMessages(key_, {});
    }

    bool run(const std::vector<MessageExt>& batch,
             ConsumeOrderlyStatus status = ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT) {
        consumer_.setMessageListener(std::make_shared<OrderlyListener>(status));
        return consumer_.consumeBatch(key_, queue0(), batch);
    }

    /// Runs one batch with a given listener (which may set autoCommit / the suspend duration).
    bool runWith(const std::vector<MessageExt>& batch,
                 const std::shared_ptr<MessageListenerOrderly>& listener) {
        consumer_.setMessageListener(listener);
        return consumer_.consumeBatch(key_, queue0(), batch);
    }

    std::vector<MessageExt> pending() const { return consumer_.pendingMessages(key_); }
    std::optional<int64_t> offset() const { return consumer_.consumeOffset(key_); }
    DefaultMQPushConsumer& c() { return consumer_; }

    std::string pendingShape() const {
        std::string s = "[";
        for (size_t i = 0; i < pending().size(); i++) {
            if (i) s += ",";
            s += std::to_string(pending()[i].queueOffset) + ":" +
                 std::to_string(pending()[i].reconsumeTimes);
        }
        return s + "]";
    }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
};

// ------------------------------------------------ the two meanings of -1 (orderly vs concurrent)

void testMinusOneMeansUnlimitedForOrderly() {
    DefaultMQPushConsumer c(kGroup);
    c.setMaxReconsumeTimes(-1);
    expect(c.orderlyMaxReconsumeTimes() == kIntMax, "orderly.minusOne.maxValue",
           std::to_string(c.orderlyMaxReconsumeTimes()));
    // The concurrent send-back fallback is still the broker default 16: merging the two into one constant would either invent dead letters
    // for orderly consumption, or let concurrent messages redeliver forever.
    expect(c.maxReconsumeTimesOrDefault() == 16, "concurrent.minusOne.still16",
           std::to_string(c.maxReconsumeTimesOrDefault()));
    c.setMaxReconsumeTimes(3);
    expect(c.orderlyMaxReconsumeTimes() == 3, "orderly.explicitUsedAsIs",
           std::to_string(c.orderlyMaxReconsumeTimes()));
}

void testDefaultCapNeverJudgesExhausted() {
    // Under the default configuration (-1) a poison message must never be judged "exhausted": the local counter can never catch up with MAX_VALUE.
    Harness h(-1);
    std::vector<MessageExt> batch;
    batch.push_back(ext(0, kIntMax - 1));
    expect(h.c().checkOrderlyReconsumeTimes(batch), "defaultCap.suspend");
    expect(batch[0].reconsumeTimes == kIntMax, "defaultCap.countedUp",
           std::to_string(batch[0].reconsumeTimes));
}

// ------------------------------------------------ the three branches of checkReconsumeTimes

void testBelowCapCountsLocallyAndSuspends() {
    Harness h(3);
    std::vector<MessageExt> batch;
    batch.push_back(ext(0, 0));
    batch.push_back(ext(1, 2));
    expect(h.c().checkOrderlyReconsumeTimes(batch), "belowCap.suspend");
    // The broker did not record this failure; without a local +1 the client never reaches the threshold
    expect(batch[0].reconsumeTimes == 1 && batch[1].reconsumeTimes == 3, "belowCap.countedUp",
           std::to_string(batch[0].reconsumeTimes) + "/" +
               std::to_string(batch[1].reconsumeTimes));
}

void testAtCapWithFailedSendBackStillSuspends() {
    Harness h(2);
    std::vector<MessageExt> batch;
    batch.push_back(ext(7, 2));
    expect(h.c().checkOrderlyReconsumeTimes(batch), "atCap.failedSendBack.suspend");
    expect(batch[0].reconsumeTimes == 3, "atCap.failedSendBack.countedUp",
           std::to_string(batch[0].reconsumeTimes));
}

void testEmptyBatchDoesNotSuspend() {
    Harness h(0);
    std::vector<MessageExt> empty;
    expect(!h.c().checkOrderlyReconsumeTimes(empty), "empty.noSuspend");
}

// ------------------------------------------------ what the sent-back message looks like

void testRetryMessageFields() {
    Harness h(2);
    MessageExt poison = ext(7, 2);
    poison.putProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED, "true");
    poison.putProperty(MessageConst::PROPERTY_KEYS, "k7");
    const Message m = h.c().buildRetryMessage(poison, h.c().orderlyMaxReconsumeTimes());
    expect(m.topic == retryTopic(), "retryMessage.topic", m.topic);
    expect(m.body == poison.body, "retryMessage.body", m.body);
    expect(m.getProperty(MessageConst::PROPERTY_KEYS) == "k7", "retryMessage.copyProps");
    expect(m.getProperty(MessageConst::PROPERTY_RETRY_TOPIC) == kTopic, "retryMessage.retryTopic",
           m.getProperty(MessageConst::PROPERTY_RETRY_TOPIC));
    expect(m.getProperty(MessageConst::PROPERTY_RECONSUME_TIME) == "3", "retryMessage.reconsumeTime",
           m.getProperty(MessageConst::PROPERTY_RECONSUME_TIME));
    expect(m.getProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES) == "2",
           "retryMessage.maxReconsumeTimes",
           m.getProperty(MessageConst::PROPERTY_MAX_RECONSUME_TIMES));
    expect(m.getProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL) == "5", "retryMessage.delayLevel",
           m.getProperty(MessageConst::PROPERTY_DELAY_TIME_LEVEL));  // 3 + 2
    expect(m.getProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID) == poison.msgId,
           "retryMessage.originMsgId", m.getProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID));
    // The half-message flag must be cleared, otherwise the broker handles it as a check-back message again
    expect(m.getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED).empty(),
           "retryMessage.transactionCleared",
           m.getProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED));
    expect(!m.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).empty(),
           "retryMessage.hasUniqKey");
}

// ------------------------------------------------ wiring inside the consumption loop

void testSuspendRequeuesBatchWhileBelowCap() {
    Harness h(3);
    std::vector<MessageExt> batch;
    batch.push_back(ext(0, 0));
    batch.push_back(ext(1, 1));
    // The next message is still at the queue tail (offset 2), so this batch must be inserted before it in the original order
    MessageExt tail = ext(2, 0);
    h.c().setPendingMessages(DefaultMQPushConsumer::offsetKey(queue0()), {tail});
    expect(!h.run(batch), "wire.requeue.notAdvanced");
    expect(!h.offset().has_value(), "wire.requeue.offsetFrozen",
           h.offset() ? std::to_string(*h.offset()) : "none");
    expect(h.pendingShape() == "[0:1,1:2,2:0]", "wire.requeue.order", h.pendingShape());
}

void testSuccessPathSkipsTheReconsumeGate() {
    // Threshold 0: once the gate is reached it sends back, so asserting "never sent back" here means the gate was never called.
    Harness h(0);
    std::vector<MessageExt> batch;
    batch.push_back(ext(0, 0));
    expect(h.run(batch, ConsumeOrderlyStatus::SUCCESS), "wire.success.advanced");
    expect(h.offset().has_value() && *h.offset() == 1, "wire.success.offset",
           h.offset() ? std::to_string(*h.offset()) : "none");
    expect(h.pending().empty(), "wire.success.noRequeue", h.pendingShape());
}

// ------------------------------------------------ suspend duration (context wins + clamping)

void testOrderlySuspendMillisResolvesThenClamps() {
    Harness h(0);
    h.c().setSuspendCurrentQueueTimeMillis(900);
    ConsumeOrderlyContext ctx;
    const auto resolve = [&](int32_t asked) {
        ctx.suspendCurrentQueueTimeMillis = asked;
        return h.c().orderlySuspendMillis(ctx);
    };
    expect(resolve(-1) == 900, "suspend.minusOneFallsBackToConfig",
           std::to_string(resolve(-1)));
    expect(resolve(70) == 70, "suspend.contextWins", std::to_string(resolve(70)));
    expect(resolve(0) == 10, "suspend.zeroClampsTo10", std::to_string(resolve(0)));
    expect(resolve(9) == 10, "suspend.belowFloor", std::to_string(resolve(9)));
    expect(resolve(10) == 10, "suspend.atFloor", std::to_string(resolve(10)));
    expect(resolve(30000) == 30000, "suspend.atCeiling", std::to_string(resolve(30000)));
    expect(resolve(30001) == 30000, "suspend.aboveCeiling", std::to_string(resolve(30001)));
    expect(resolve(std::numeric_limits<int32_t>::max()) == 30000, "suspend.intMaxClamped",
           std::to_string(resolve(std::numeric_limits<int32_t>::max())));
    h.c().setSuspendCurrentQueueTimeMillis(-1);
    ctx.suspendCurrentQueueTimeMillis = -1;
    expect(h.c().orderlySuspendMillis(ctx) == 10, "suspend.bothMinusOneHitsFloor",
           std::to_string(h.c().orderlySuspendMillis(ctx)));
}

// ------------------------------------------------ the illegal states under autoCommit=true

void testIllegalCommitAndRollbackUnderAutoCommitAreAcked() {
    for (ConsumeOrderlyStatus status :
         {ConsumeOrderlyStatus::COMMIT, ConsumeOrderlyStatus::ROLLBACK}) {
        Harness h(0);
        std::vector<MessageExt> batch{ext(0, 0), ext(1, 0)};
        h.c().setPendingMessages(DefaultMQPushConsumer::offsetKey(queue0()),
                                 {ext(2, 0)});
        expect(h.run(batch, status), "illegal.advanced");
        expect(h.offset().has_value() && *h.offset() == 2, "illegal.offset",
               h.offset() ? std::to_string(*h.offset()) : "none");
        expect(h.pendingShape() == "[2:0]", "illegal.noRequeue", h.pendingShape());
    }
}

// ------------------------------------------------ autoCommit=false (manual commit)

void testManualCommitAdvancesWithoutRequeueing() {
    Harness h(0);
    auto listener = std::make_shared<OrderlyListener>(ConsumeOrderlyStatus::COMMIT);
    listener->manualCommit(false);
    std::vector<MessageExt> batch{ext(0, 0), ext(1, 0)};
    expect(h.runWith(batch, listener), "manual.commit.advanced");
    expect(h.offset().has_value() && *h.offset() == 2, "manual.commit.offset",
           h.offset() ? std::to_string(*h.offset()) : "none");
    expect(h.pending().empty(), "manual.commit.noRequeue", h.pendingShape());
}

void testManualRollbackRequeuesAndHoldsTheOffset() {
    Harness h(0);
    auto listener = std::make_shared<OrderlyListener>(ConsumeOrderlyStatus::ROLLBACK);
    listener->manualCommit(false);
    std::vector<MessageExt> batch{ext(0, 0), ext(1, 0)};
    h.c().setPendingMessages(DefaultMQPushConsumer::offsetKey(queue0()), {ext(2, 0)});
    expect(!h.runWith(batch, listener), "manual.rollback.notAdvanced");
    expect(!h.offset().has_value(), "manual.rollback.offsetFrozen",
           h.offset() ? std::to_string(*h.offset()) : "none");
    expect(h.pendingShape() == "[0:0,1:0,2:0]", "manual.rollback.requeued", h.pendingShape());
}

void testManualSuccessHoldsTheBatchWithoutCommitting() {
    Harness h(0);
    auto listener = std::make_shared<OrderlyListener>(ConsumeOrderlyStatus::SUCCESS);
    listener->manualCommit(false);
    std::vector<MessageExt> batch{ext(0, 0)};
    expect(!h.runWith(batch, listener), "manual.success.notAdvanced");
    expect(!h.offset().has_value(), "manual.success.offsetFrozen",
           h.offset() ? std::to_string(*h.offset()) : "none");
    expect(h.pendingShape() == "[0:0]", "manual.success.held", h.pendingShape());
}

void testManualSuspendNeverCommitsTheOffset() {
    Harness h(0);
    auto listener = std::make_shared<OrderlyListener>(
        ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT);
    listener->manualCommit(false);
    std::vector<MessageExt> batch{ext(0, 0)};
    expect(!h.runWith(batch, listener), "manual.suspend.notAdvanced");
    expect(!h.offset().has_value(), "manual.suspend.offsetFrozen",
           h.offset() ? std::to_string(*h.offset()) : "none");
    expect(h.pendingShape() == "[0:1]", "manual.suspend.requeuedAndCounted", h.pendingShape());
}

// ------------------------------------------------ the 309 CONSUME_MESSAGE_DIRECTLY mapping

void testDirectConsumeOrderlyMapping() {
    const std::pair<ConsumeOrderlyStatus, const char*> cases[] = {
        {ConsumeOrderlyStatus::SUCCESS, "CR_SUCCESS"},
        {ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT, "CR_LATER"},
        {ConsumeOrderlyStatus::COMMIT, "CR_COMMIT"},
        {ConsumeOrderlyStatus::ROLLBACK, "CR_ROLLBACK"},
    };
    for (const auto& c : cases) {
        DefaultMQPushConsumer consumer(kGroup);
        auto listener = std::make_shared<OrderlyListener>(c.first);
        listener->manualCommit(false);
        consumer.setMessageListener(listener);
        ConsumeMessageDirectlyResult r = consumer.consumeMessageDirectly(ext(0), kBroker);
        expect(r.consumeResult == c.second, std::string("direct.orderly.") + c.second,
               r.consumeResult);
        expect(r.order, "direct.orderly.orderTrue");
        expect(!r.autoCommit, "direct.orderly.autoCommitFromContext");
    }
}

void testDirectConsumeWithoutListenerIsReturnNull() {
    DefaultMQPushConsumer consumer(kGroup);
    ConsumeMessageDirectlyResult r = consumer.consumeMessageDirectly(ext(0), kBroker);
    expect(r.consumeResult == "CR_RETURN_NULL", "direct.noListener.returnNull", r.consumeResult);
}

// ------------------------------------------------ lifting into the request header (sendKernelImpl:1004-1018)

// Only the header-building part is used: building a request touches no network and the whole lifting criterion is in its output.
std::shared_ptr<SendMessageRequestHeaderV2> buildHeader(MQClientInstance& inst,
                                                       const std::string& topic,
                                                       const PropertyMap& props,
                                                       PropertyMap* wire) {
    Message m;
    m.topic = topic;
    m.body = "b";
    m.properties = props;
    RemotingCommand request =
        inst.buildSendRequest("PG", m, MessageQueue(topic, kBroker, 0), /*sysFlag=*/0,
                             /*unitMode=*/false);
    // createRequestCommand only attaches the customHeader; extFields are filled at the encode step;
    // expanding it explicitly here is what makes the assertion cover the payload actually put on the wire.
    request.makeCustomHeaderToNet();
    if (wire != nullptr) {
        *wire = request.extFields;
    }
    return std::static_pointer_cast<SendMessageRequestHeaderV2>(request.customHeader);
}

void testRetryTopicPropertiesAreLiftedIntoTheHeader() {
    MQClientInstance inst("lift-unit-test", {});
    PropertyMap props;
    props[MessageConst::PROPERTY_RECONSUME_TIME] = "4";
    props[MessageConst::PROPERTY_MAX_RECONSUME_TIMES] = "6";
    PropertyMap wire;
    const auto header = buildHeader(inst, retryTopic(), props, &wire);
    // The broker judges dead letters from the request header; without this step it falls back to the subscription group default retryMaxTimes(16)
    expect(header->reconsumeTimes && *header->reconsumeTimes == 4, "lift.reconsumeTimes",
           header->reconsumeTimes ? std::to_string(*header->reconsumeTimes) : "none");
    expect(header->maxReconsumeTimes && *header->maxReconsumeTimes == 6, "lift.maxReconsumeTimes",
           header->maxReconsumeTimes ? std::to_string(*header->maxReconsumeTimes) : "none");
    // Short field names of V2: j=reconsumeTimes, l=maxReconsumeTimes, i=properties
    expect(wire["j"] == "4", "lift.wireJ", wire["j"]);
    expect(wire["l"] == "6", "lift.wireL", wire["l"]);
    expect(wire["i"].find("RECONSUME_TIME") != std::string::npos, "lift.wireKeepsProperty",
           wire["i"]);
}

void testOrdinaryTopicSendIsNotLifted() {
    MQClientInstance inst("lift-unit-test-2", {});
    PropertyMap props;
    props[MessageConst::PROPERTY_RECONSUME_TIME] = "4";
    props[MessageConst::PROPERTY_MAX_RECONSUME_TIMES] = "6";
    PropertyMap wire;
    const auto header = buildHeader(inst, kTopic, props, &wire);
    expect(header->reconsumeTimes && *header->reconsumeTimes == 0, "noLift.reconsumeTimes",
           header->reconsumeTimes ? std::to_string(*header->reconsumeTimes) : "none");
    expect(wire["j"] == "0", "noLift.wireJ", wire["j"]);
    // A non-%RETRY% send does not deliver maxReconsumeTimes: brokers >=V3_4_9 adopt it unconditionally,
    // so always sending 0 would make the very first delivery satisfy reconsumeTimes(0) >= 0 and go straight into %DLQ%.
    // ⚠ Do not write expect(..., wire["l"]): MSVC evaluates arguments right to left, so operator[] would first
    //    insert "l" into the map and the absence assertion would fail forever. find runs once, value stored then used.
    auto it = wire.find("l");
    expect(it == wire.end(), "noLift.wireLAbsent", it == wire.end() ? "(absent)" : it->second);
}

}  // namespace

int main() {
    testMinusOneMeansUnlimitedForOrderly();
    testDefaultCapNeverJudgesExhausted();
    testBelowCapCountsLocallyAndSuspends();
    testAtCapWithFailedSendBackStillSuspends();
    testEmptyBatchDoesNotSuspend();
    testRetryMessageFields();
    testSuspendRequeuesBatchWhileBelowCap();
    testSuccessPathSkipsTheReconsumeGate();
    testOrderlySuspendMillisResolvesThenClamps();
    testIllegalCommitAndRollbackUnderAutoCommitAreAcked();
    testManualCommitAdvancesWithoutRequeueing();
    testManualRollbackRequeuesAndHoldsTheOffset();
    testManualSuccessHoldsTheBatchWithoutCommitting();
    testManualSuspendNeverCommitsTheOffset();
    testDirectConsumeOrderlyMapping();
    testDirectConsumeWithoutListenerIsReturnNull();
    testRetryTopicPropertiesAreLiftedIntoTheHeader();
    testOrdinaryTopicSendIsNotLifted();
    std::printf("orderly reconsume: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
