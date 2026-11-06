// OpenTracing hook trio unit test: span names, tag sets, close-out rules and
// propagation write-back for the send / consume / end-transaction legs, on a
// recording fake tracer -- no cluster needed.
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/client/open_tracing_hook.h"
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

void expectEq(const std::string& actual, const std::string& expected, const std::string& name) {
    expect(actual == expected, name, "(actual=" + actual + " expected=" + expected + ")");
}

// ------------------------------------------------------------ fake tracer

class FakeSpanContext : public OpenTracingSpanContext {
public:
    explicit FakeSpanContext(int id) : id(id) {}
    int id;
};

class FakeSpan : public OpenTracingSpan {
public:
    FakeSpan(OpenTracingTracer* owner, std::string operation, OTTagMap tags,
             std::vector<std::shared_ptr<OpenTracingSpanContext>> refs)
        : owner(owner), operation(std::move(operation)), tags(std::move(tags)), refs(std::move(refs)) {}

    void setTag(const std::string& key, const std::string& value) override { tags[key] = value; }

    std::shared_ptr<OpenTracingSpanContext> context() override {
        ctx = std::make_shared<FakeSpanContext>(nextCtxId++);
        return ctx;
    }

    void finish() override { finished = true; }

    OpenTracingTracer* owner;
    std::string operation;
    OTTagMap tags;
    std::vector<std::shared_ptr<OpenTracingSpanContext>> refs;
    std::shared_ptr<FakeSpanContext> ctx;
    bool finished = false;
    static inline int nextCtxId = 1;
};

class FakeTracer : public OpenTracingTracer {
public:
    explicit FakeTracer(std::shared_ptr<OpenTracingSpanContext> upstream = nullptr)
        : upstream_(std::move(upstream)) {}

    std::shared_ptr<OpenTracingSpan> startSpan(const std::string& operation, const OTTagMap& tags,
                                               const std::vector<std::shared_ptr<OpenTracingSpanContext>>& refs) override {
        auto span = std::make_shared<FakeSpan>(this, operation, tags, refs);
        spans.push_back(span);
        return span;
    }

    std::shared_ptr<OpenTracingSpanContext> extract(const PropertyMap& carrier) override {
        (void)carrier;
        return upstream_;
    }

    void inject(const std::shared_ptr<OpenTracingSpanContext>& spanContext, PropertyMap& carrier) override {
        carrier["traceparent"] = "fake-trace-context";
        injected.push_back(spanContext);
    }

    std::vector<std::shared_ptr<FakeSpan>> spans;
    std::vector<std::shared_ptr<OpenTracingSpanContext>> injected;

private:
    std::shared_ptr<OpenTracingSpanContext> upstream_;
};

// ------------------------------------------------------------ helpers

Message makeSendMsg() {
    return Message("TopicTest", Bytes("hello-world"));
}

MessageExt makeExt(int storeSize, int retry) {
    MessageExt m;
    m.topic = "TopicTest";
    m.msgId = "EXT-MSG-ID";
    m.setStoreSize(storeSize);
    m.setReconsumeTimes(retry);
    m.setUserProperty(MessageConst::PROPERTY_MSG_REGION, "DefaultRegion");
    m.setUserProperty("TAGS", "TagC");
    m.setUserProperty("KEYS", "KeyC");
    return m;
}

const std::string& tag(const FakeSpan& s, const std::string& key) {
    static const std::string kEmpty;
    auto it = s.tags.find(key);
    return it == s.tags.end() ? kEmpty : it->second;
}

// ------------------------------------------------------------ send leg

void testSendBeforeOpensProducerSpanWithDocumentedTags() {
    auto tracer = std::make_shared<FakeTracer>();
    SendMessageOpenTracingHook hook(tracer);
    SendMessageContext ctx;
    auto msg = makeSendMsg();
    ctx.message = &msg;
    ctx.brokerAddr = "127.0.0.1:10911";
    ctx.msgType = 0;  // TraceMessageType::NORMAL
    hook.sendMessageBefore(ctx);

    expect(tracer->spans.size() == 1, "one span opened");
    const auto& span = tracer->spans[0];
    expectEq(span->operation, "To_TopicTest", "operation");
    expectEq(tag(*span, OT_SPAN_KIND), OT_SPAN_KIND_PRODUCER, "span.kind");
    expectEq(tag(*span, OT_PEER_SERVICE), "rocketmq", "peer.service");
    expectEq(tag(*span, OT_MESSAGE_BUS_DESTINATION), "TopicTest", "destination");
    expectEq(tag(*span, OT_ROCKETMQ_TAGS), "", "tags empty");
    expectEq(tag(*span, OT_ROCKETMQ_KEYS), "", "keys empty");
    expectEq(tag(*span, OT_ROCKETMQ_STORE_HOST), "127.0.0.1:10911", "store_host");
    expectEq(tag(*span, OT_ROCKETMQ_MSG_TYPE), "NORMAL_MSG", "msg_type");
    expectEq(tag(*span, OT_ROCKETMQ_BODY_LENGTH), "11", "body_length");
    expect(!span->finished, "span open");
    // the span rides in the mqTraceContext slot (Java setMqTraceContext)
    auto holder = std::dynamic_pointer_cast<OTSpanHolder>(ctx.mqTraceContext);
    expect(holder != nullptr && holder->span == span, "span stashed in slot");

    // close-out: success + msg_id + region_id
    SendResult sr;
    sr.sendStatus = SendStatus::SEND_OK;
    sr.msgId = "MSG-ID-1";
    sr.regionId = "DefaultRegion";
    ctx.sendResult = &sr;
    hook.sendMessageAfter(ctx);
    expectEq(tag(*span, OT_ROCKETMQ_SUCCESS), "true", "success");
    expectEq(tag(*span, OT_ROCKETMQ_MSG_ID), "MSG-ID-1", "msg_id");
    expectEq(tag(*span, OT_ROCKETMQ_REGION_ID), "DefaultRegion", "region_id");
    expect(span->finished, "span closed");
}

void testSendAfterNoResultOrRegionIdLeavesSpanOpen() {
    auto tracer = std::make_shared<FakeTracer>();
    SendMessageOpenTracingHook hook(tracer);
    SendMessageContext ctx;
    auto msg = makeSendMsg();
    ctx.message = &msg;
    hook.sendMessageBefore(ctx);
    hook.sendMessageAfter(ctx);  // no send result at all
    expect(!tracer->spans[0]->finished, "no result: span stays open");
    SendResult sr;
    sr.sendStatus = SendStatus::SEND_OK;
    sr.msgId = "M";
    sr.regionId = "";  // no region id
    ctx.sendResult = &sr;
    hook.sendMessageAfter(ctx);
    expect(!tracer->spans[0]->finished, "no region id: span stays open");
}

void testSendAfterFailureReportsSuccessFalse() {
    auto tracer = std::make_shared<FakeTracer>();
    SendMessageOpenTracingHook hook(tracer);
    SendMessageContext ctx;
    auto msg = makeSendMsg();
    ctx.message = &msg;
    hook.sendMessageBefore(ctx);
    SendResult sr;
    sr.sendStatus = SendStatus::FLUSH_DISK_TIMEOUT;
    sr.msgId = "M";
    sr.regionId = "R";
    ctx.sendResult = &sr;
    hook.sendMessageAfter(ctx);
    expectEq(tag(*tracer->spans[0], OT_ROCKETMQ_SUCCESS), "false", "failure success=false");
    expect(tracer->spans[0]->finished, "failure span closed");
}

void testSendDoesNotWriteBackPropertiesContextIsConst() {
    // cpp deviation: SendMessageContext.message is const, so the send leg is
    // extract-only -- no propagation write-back happens in the hook.
    auto tracer = std::make_shared<FakeTracer>();
    SendMessageOpenTracingHook hook(tracer);
    SendMessageContext ctx;
    auto msg = makeSendMsg();
    ctx.message = &msg;
    hook.sendMessageBefore(ctx);
    expect(tracer->injected.empty(), "send leg does not inject (const message)");
    expect(msg.getProperties().count("traceparent") == 0, "message properties untouched");
}

void testSendUpstreamContextBecomesRef() {
    auto upstream = std::make_shared<FakeSpanContext>(99);
    auto tracer = std::make_shared<FakeTracer>(upstream);
    SendMessageOpenTracingHook hook(tracer);
    SendMessageContext ctx;
    auto msg = makeSendMsg();
    ctx.message = &msg;
    hook.sendMessageBefore(ctx);
    expect(tracer->spans[0]->refs.size() == 1 && tracer->spans[0]->refs[0] == upstream,
           "upstream context used as ref");
}

void testSendMsgTypeTagUsesJavaEnumName() {
    auto tracer = std::make_shared<FakeTracer>();
    SendMessageOpenTracingHook hook(tracer);
    SendMessageContext ctx;
    auto msg = makeSendMsg();
    ctx.message = &msg;
    ctx.msgType = 1;  // TraceMessageType::TRANS
    hook.sendMessageBefore(ctx);
    expectEq(tag(*tracer->spans[0], OT_ROCKETMQ_MSG_TYPE), "TRANS_MSG_HALF", "msg_type TRANS_MSG_HALF");
    expectEq(otMsgTypeName(2), "TRANS_MSG_COMMIT", "ordinal 2");
    expectEq(otMsgTypeName(3), "DELAY_MSG", "ordinal 3");
    expectEq(otMsgTypeName(4), "ORDER_MSG", "ordinal 4");
    expectEq(otMsgTypeName(0), "NORMAL_MSG", "ordinal 0");
    expectEq(otMsgTypeName(42), "NORMAL_MSG", "unknown falls back");
}

// ------------------------------------------------------------ consume leg

void testConsumeBeforeOpensOneSpanPerMessage() {
    auto tracer = std::make_shared<FakeTracer>();
    ConsumeMessageOpenTracingHook hook(tracer);
    ConsumeMessageContext ctx("CID_test", {makeExt(42, 2), makeExt(42, 2)}, MessageQueue());
    hook.consumeMessageBefore(ctx);

    expect(tracer->spans.size() == 2, "one span per message");
    for (const auto& span : tracer->spans) {
        expectEq(span->operation, "From_TopicTest", "consume operation");
        expectEq(tag(*span, OT_SPAN_KIND), OT_SPAN_KIND_CONSUMER, "span.kind consumer");
        expectEq(tag(*span, OT_PEER_SERVICE), "rocketmq", "peer.service");
        expectEq(tag(*span, OT_MESSAGE_BUS_DESTINATION), "TopicTest", "destination");
        expectEq(tag(*span, OT_ROCKETMQ_MSG_ID), "EXT-MSG-ID", "msg_id");
        expectEq(tag(*span, OT_ROCKETMQ_TAGS), "TagC", "tags");
        expectEq(tag(*span, OT_ROCKETMQ_KEYS), "KeyC", "keys");
        expectEq(tag(*span, OT_ROCKETMQ_BODY_LENGTH), "42", "body_length = store size");
        expectEq(tag(*span, OT_ROCKETMQ_RETRY_TIMERS), "2", "retry_times");
        expectEq(tag(*span, OT_ROCKETMQ_REGION_ID), "DefaultRegion", "region_id");
        expect(!span->finished, "consume span open");
    }
    auto holder = std::dynamic_pointer_cast<OTConsumeSpanHolder>(ctx.mqTraceContext);
    expect(holder != nullptr && holder->spans.size() == 2, "spans stashed in slot");
}

void testConsumeAfterTagsSuccessAndFinishesAll() {
    auto tracer = std::make_shared<FakeTracer>();
    ConsumeMessageOpenTracingHook hook(tracer);
    ConsumeMessageContext ctx("CID_test", {makeExt(1, 0)}, MessageQueue());
    hook.consumeMessageBefore(ctx);
    ctx.success = false;
    hook.consumeMessageAfter(ctx);
    expectEq(tag(*tracer->spans[0], OT_ROCKETMQ_SUCCESS), "false", "consume failure tagged");
    expect(tracer->spans[0]->finished, "consume span closed");
}

void testConsumeEmptyMsgListIsNoop() {
    auto tracer = std::make_shared<FakeTracer>();
    ConsumeMessageOpenTracingHook hook(tracer);
    ConsumeMessageContext ctx("CID_test", {}, MessageQueue());
    hook.consumeMessageBefore(ctx);
    hook.consumeMessageAfter(ctx);
    expect(tracer->spans.empty(), "empty list: no spans");
    expect(ctx.mqTraceContext == nullptr, "empty list: nothing stashed");
}

// ------------------------------------------------------------ transaction leg

void testEndTransactionOpensAndClosesImmediately() {
    auto tracer = std::make_shared<FakeTracer>();
    EndTransactionOpenTracingHook hook(tracer);
    auto msg = makeSendMsg();
    msg.setUserProperty("TAGS", "TagT");
    msg.setUserProperty("KEYS", "KeyT");
    EndTransactionContext ctx;
    ctx.message = &msg;
    ctx.brokerAddr = "127.0.0.1:10911";
    ctx.msgId = "TX-MSG-ID";
    ctx.transactionId = "TX-ID-9";
    ctx.transactionState = "COMMIT_MESSAGE";
    ctx.fromTransactionCheck = true;
    hook.endTransaction(ctx);

    expect(tracer->spans.size() == 1, "one span opened");
    const auto& span = tracer->spans[0];
    expectEq(span->operation, "EndTransaction", "operation");
    expectEq(tag(*span, OT_SPAN_KIND), OT_SPAN_KIND_PRODUCER, "span.kind");
    expectEq(tag(*span, OT_MESSAGE_BUS_DESTINATION), "TopicTest", "destination");
    expectEq(tag(*span, OT_ROCKETMQ_TAGS), "TagT", "tags");
    expectEq(tag(*span, OT_ROCKETMQ_KEYS), "KeyT", "keys");
    expectEq(tag(*span, OT_ROCKETMQ_STORE_HOST), "127.0.0.1:10911", "store_host");
    expectEq(tag(*span, OT_ROCKETMQ_MSG_ID), "TX-MSG-ID", "msg_id");
    expectEq(tag(*span, OT_ROCKETMQ_MSG_TYPE), OT_TRANS_MSG_COMMIT, "msg_type");
    expectEq(tag(*span, OT_ROCKETMQ_TRANSACTION_ID), "TX-ID-9", "transaction_id");
    expectEq(tag(*span, OT_ROCKETMQ_TRANSACTION_STATE), "COMMIT_MESSAGE", "transaction_state");
    expectEq(tag(*span, OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK), "true", "from_check");
    expect(span->finished, "closed immediately");
}

void testEndTransactionWithoutMessageStillOpensSpan() {
    auto tracer = std::make_shared<FakeTracer>();
    EndTransactionOpenTracingHook hook(tracer);
    EndTransactionContext ctx;
    ctx.msgId = "TX";
    ctx.transactionState = "ROLLBACK_MESSAGE";
    hook.endTransaction(ctx);
    expectEq(tag(*tracer->spans[0], OT_MESSAGE_BUS_DESTINATION), "", "no message: destination empty");
    expectEq(tag(*tracer->spans[0], OT_ROCKETMQ_TRANSACTION_STATE), "ROLLBACK_MESSAGE", "state name");
    expect(tracer->spans[0]->finished, "span closed");
}

void testHookNamesMatchJavaImpls() {
    expect(SendMessageOpenTracingHook(nullptr).hookName() == "SendMessageOpenTracingHook", "send hook name");
    expect(ConsumeMessageOpenTracingHook(nullptr).hookName() == "ConsumeMessageOpenTracingHook", "consume hook name");
    expect(EndTransactionOpenTracingHook(nullptr).hookName() == "EndTransactionOpenTracingHook", "end tx hook name");
}

}  // namespace

int main() {
    testSendBeforeOpensProducerSpanWithDocumentedTags();
    testSendAfterNoResultOrRegionIdLeavesSpanOpen();
    testSendAfterFailureReportsSuccessFalse();
    testSendDoesNotWriteBackPropertiesContextIsConst();
    testSendUpstreamContextBecomesRef();
    testSendMsgTypeTagUsesJavaEnumName();
    testConsumeBeforeOpensOneSpanPerMessage();
    testConsumeAfterTagsSuccessAndFinishesAll();
    testConsumeEmptyMsgListIsNoop();
    testEndTransactionOpensAndClosesImmediately();
    testEndTransactionWithoutMessageStillOpensSpan();
    testHookNamesMatchJavaImpls();
    std::printf("open_tracing_hook: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
