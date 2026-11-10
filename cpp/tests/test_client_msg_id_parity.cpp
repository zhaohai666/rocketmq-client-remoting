// Java MessageClientExt#getMsgId() parity on consumed messages.
//
// Client-side decoding yields a MessageClientExt whose getMsgId() returns the UNIQ_KEY
// property and only falls back to the broker offset id. This port keeps that offset id
// in MessageExt::msgId and exposes the Java behaviour through clientMsgId(). Every
// consume-side call site (SubBefore bean, CONSUMER_SEND_MSG_BACK, retry ORIGIN_MESSAGE_ID)
// must go through it. Fully offline: a no-network TraceDispatcher and the pure
// buildRetryMessage helper are enough to pin both the UNIQ_KEY and the fallback halves.
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/trace.h"
#include "rocketmq/client/trace_dispatcher.h"
#include "rocketmq/client/trace_hook.h"
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

void expectStr(const std::string& actual, const std::string& expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=%s expected=%s)\n", name.c_str(), actual.c_str(),
                    expected.c_str());
    }
}

const char* kOffsetId = "0A0B0C0D00002A9F0000000000000EA7";
const char* kUniqId = "AC1E0A0B2C3D4E5F60718293A4B5C6D7";

MessageExt consumed(bool withUniq) {
    MessageExt msg(Message("TopicTest", Bytes{'h', 'i'}));
    msg.msgId = kOffsetId;
    msg.queueId = 0;
    msg.brokerName = "broker-a";
    msg.reconsumeTimes = 1;
    msg.storeTimestamp = 1700000000000;
    msg.storeSize = 42;
    if (withUniq) {
        msg.putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, kUniqId);
    }
    return msg;
}

// A pure in-memory TraceDispatcher: captures the contexts a hook appends, no network.
class CapturingDispatcher : public TraceDispatcher {
public:
    std::vector<std::shared_ptr<TraceContext>> appended;

    bool append(const std::shared_ptr<TraceContext>& ctx) override {
        appended.push_back(ctx);
        return true;
    }
    std::string getTraceTopicName() const override { return "RMQ_SYS_TRACE_TOPIC"; }
};

// ---------------------------------------------------------------- 1. the helper
void testHelperPrefersUniqKeyAndFallsBackToOffsetId() {
    expectStr(consumed(true).clientMsgId(), kUniqId, "helper: UNIQ_KEY wins");
    expectStr(consumed(false).clientMsgId(), kOffsetId, "helper: offset id is the fallback");
}

// ---------------------------------------------------------------- 2. consume-side SubBefore / SubAfter
void testConsumeTraceBeansCarryTheUniqKey() {
    CapturingDispatcher d;
    ConsumeMessageTraceHook hook(&d);
    ConsumeMessageContext ctx("CID_test", {consumed(true)},
                              MessageQueue("TopicTest", "broker-a", 0));
    ctx.props = "SUCCESS";
    ctx.success = true;

    hook.consumeMessageBefore(ctx);
    hook.consumeMessageAfter(ctx);

    expect(d.appended.size() == 2, "consume: SubBefore + SubAfter appended",
           "size=" + std::to_string(d.appended.size()));
    if (d.appended.size() != 2) return;
    expect(d.appended[0]->traceType == TraceType::SUB_BEFORE &&
               d.appended[1]->traceType == TraceType::SUB_AFTER,
           "consume: trace types");
    expect(!d.appended[0]->traceBeans.empty() && !d.appended[1]->traceBeans.empty(),
           "consume: beans present");
    if (d.appended[0]->traceBeans.empty() || d.appended[1]->traceBeans.empty()) return;
    expectStr(d.appended[0]->traceBeans[0].msgId, kUniqId, "SubBefore bean.msgId = UNIQ_KEY");
    expectStr(d.appended[1]->traceBeans[0].msgId, kUniqId, "SubAfter bean.msgId = UNIQ_KEY");
    expectStr(d.appended[1]->requestId, d.appended[0]->requestId,
              "SubBefore/SubAfter share one requestId");
}

// ---------------------------------------------------------------- 3. consume-side fallback
void testConsumeTraceFallsBackToOffsetIdWithoutUniqKey() {
    CapturingDispatcher d;
    ConsumeMessageTraceHook hook(&d);
    ConsumeMessageContext ctx("CID_test", {consumed(false)},
                              MessageQueue("TopicTest", "broker-a", 0));
    hook.consumeMessageBefore(ctx);
    expect(!d.appended.empty() && !d.appended[0]->traceBeans.empty(),
           "fallback: SubBefore appended with a bean");
    if (d.appended.empty() || d.appended[0]->traceBeans.empty()) return;
    expectStr(d.appended[0]->traceBeans[0].msgId, kOffsetId,
              "SubBefore bean.msgId falls back to the offset id");
}

// ---------------------------------------------------------------- 4. retry ORIGIN_MESSAGE_ID (push path)
void testRetryOriginMessageIdIsTheUniqKey() {
    DefaultMQPushConsumer c("GID_client_msg_id_parity");
    Message withUniq = c.buildRetryMessage(consumed(true), 16);
    expectStr(withUniq.getProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID), kUniqId,
              "retry ORIGIN_MESSAGE_ID = UNIQ_KEY");
    expect(withUniq.getProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID) != kOffsetId,
           "retry ORIGIN_MESSAGE_ID is not the offset id");
    Message withoutUniq = c.buildRetryMessage(consumed(false), 16);
    expectStr(withoutUniq.getProperty(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID), kOffsetId,
              "retry ORIGIN_MESSAGE_ID falls back to the offset id");
}

}  // namespace

int main() {
    testHelperPrefersUniqKeyAndFallsBackToOffsetId();
    testConsumeTraceBeansCarryTheUniqKey();
    testConsumeTraceFallsBackToOffsetIdWithoutUniqKey();
    testRetryOriginMessageIdIsTheUniqKey();
    std::printf("client_msg_id_parity: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
