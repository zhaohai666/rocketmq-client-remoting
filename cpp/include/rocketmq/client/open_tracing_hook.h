#ifndef ROCKETMQ_CLIENT_OPEN_TRACING_HOOK_H
#define ROCKETMQ_CLIENT_OPEN_TRACING_HOOK_H

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rocketmq/client/hook.h"
#include "rocketmq/client/trace.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/namespace_util.h"

namespace rocketmq {

// OpenTracing-style hook trio for the message lifecycle.
//
// Port of the Java client's SendMessageOpenTracingHookImpl /
// ConsumeMessageOpenTracingHookImpl / EndTransactionOpenTracingHookImpl (the
// same trio the PHP port carries as OpenTracingHook.php), on top of a
// duck-typed Tracer SPI with zero third-party deps.
//
// The in-flight span rides in the context's mqTraceContext slot, exactly like
// the Java original (context.setMqTraceContext(span)): before opens the span
// and stashes it, after reads it back and closes it. The stash is an
// OTSpanHolder / OTConsumeSpanHolder (TraceContext subclasses), so a context
// carrying some other hook's TraceContext is skipped rather than
// misinterpreted -- registering both the trace dispatcher hooks and these
// hooks on the same producer is not supported (same restriction as Java).
//
// Cross-process propagation rides on the message properties themselves: the
// upstream context is extracted from the property map and used as the span
// ref. One documented deviation from the PHP port: the cpp SendMessageContext
// carries a **const** Message (buildSendMessageContext takes const&), so the
// just-started span's context cannot be written back onto the properties
// here -- propagation injection must happen at the call site if needed.

// OpenTracing standard tag keys (io.opentracing.tag.Tags).
inline constexpr const char* OT_SPAN_KIND = "span.kind";
inline constexpr const char* OT_SPAN_KIND_PRODUCER = "producer";
inline constexpr const char* OT_SPAN_KIND_CONSUMER = "consumer";
inline constexpr const char* OT_PEER_SERVICE = "peer.service";
inline constexpr const char* OT_MESSAGE_BUS_DESTINATION = "message_bus.destination";

// The rocketmq.* tag keys: the Java TraceConstants literals (the mgs_id /
// mgs_type spellings are the Java protocol literals -- the typo is
// load-bearing, do not "fix" it).
inline constexpr const char* OT_ROCKETMQ_SERVICE = "rocketmq";
inline constexpr const char* OT_ROCKETMQ_SUCCESS = "rocketmq.success";
inline constexpr const char* OT_ROCKETMQ_TAGS = "rocketmq.tags";
inline constexpr const char* OT_ROCKETMQ_KEYS = "rocketmq.keys";
inline constexpr const char* OT_ROCKETMQ_STORE_HOST = "rocketmq.store_host";
inline constexpr const char* OT_ROCKETMQ_BODY_LENGTH = "rocketmq.body_length";
inline constexpr const char* OT_ROCKETMQ_MSG_ID = "rocketmq.mgs_id";
inline constexpr const char* OT_ROCKETMQ_MSG_TYPE = "rocketmq.mgs_type";
inline constexpr const char* OT_ROCKETMQ_REGION_ID = "rocketmq.region_id";
inline constexpr const char* OT_ROCKETMQ_TRANSACTION_ID = "rocketmq.transaction_id";
inline constexpr const char* OT_ROCKETMQ_TRANSACTION_STATE = "rocketmq.transaction_state";
inline constexpr const char* OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK = "rocketmq.is_from_transaction_check";
inline constexpr const char* OT_ROCKETMQ_RETRY_TIMERS = "rocketmq.retry_times";

// The transaction leg reports the commit state only.
inline constexpr const char* OT_TRANS_MSG_COMMIT = "Trans_msg_Commit";

using OTTagMap = std::map<std::string, std::string>;

// Duck-typed span context: an opaque object the tracer's inject() understands.
class OpenTracingSpanContext {
public:
    virtual ~OpenTracingSpanContext() = default;
};

// Duck-typed span. Tag values arrive as strings (the hook formats ints and
// bools: "true"/"false", std::to_string for lengths), keeping the SPI
// trivially implementable without a variant type.
class OpenTracingSpan {
public:
    virtual ~OpenTracingSpan() = default;
    virtual void setTag(const std::string& key, const std::string& value) = 0;
    virtual std::shared_ptr<OpenTracingSpanContext> context() = 0;
    virtual void finish() = 0;
};

// Duck-typed tracer:
//   - startSpan opens a span; refs carries the upstream context (from
//     extract) when there is one;
//   - extract reads the upstream context from the message property map (a
//     null return means "no upstream");
//   - inject writes the propagation keys of the just-started span into the
//     carrier, which the hook writes back onto the message.
class OpenTracingTracer {
public:
    virtual ~OpenTracingTracer() = default;
    virtual std::shared_ptr<OpenTracingSpan> startSpan(const std::string& operation,
                                                       const OTTagMap& tags,
                                                       const std::vector<std::shared_ptr<OpenTracingSpanContext>>& refs) = 0;
    virtual std::shared_ptr<OpenTracingSpanContext> extract(const PropertyMap& carrier) = 0;
    virtual void inject(const std::shared_ptr<OpenTracingSpanContext>& spanContext,
                        PropertyMap& carrier) = 0;
};

// TraceContext subclasses carrying the in-flight OT spans through the
// context's mqTraceContext slot.
class OTSpanHolder : public TraceContext {
public:
    std::shared_ptr<OpenTracingSpan> span;
};

class OTConsumeSpanHolder : public TraceContext {
public:
    std::vector<std::shared_ptr<OpenTracingSpan>> spans;
};

// Java parity: the tag value is the TraceMessageType ordinal mapped to the
// Java MessageType enum *name* (NORMAL_MSG...).
inline std::string otMsgTypeName(int32_t msgType) {
    switch (msgType) {
        case 1: return "TRANS_MSG_HALF";
        case 2: return "TRANS_MSG_COMMIT";
        case 3: return "DELAY_MSG";
        case 4: return "ORDER_MSG";
        default: return "NORMAL_MSG";
    }
}

inline std::string otBoolStr(bool b) { return b ? "true" : "false"; }

// ---------------------------------------------------------------- send leg

// Send leg: span "To_<topic>", kind producer, with the kind + service +
// destination + tags + keys + store_host + msg_type + body_length tags at
// open; success / msg_id / region_id at close. No result, or no region id on
// it: the span is left open, exactly like the Java close-out flow.
class SendMessageOpenTracingHook : public SendMessageHook {
public:
    explicit SendMessageOpenTracingHook(std::shared_ptr<OpenTracingTracer> tracer)
        : tracer_(std::move(tracer)) {}

    std::string hookName() const override { return "SendMessageOpenTracingHook"; }

    void sendMessageBefore(SendMessageContext& context) override {
        if (context.message == nullptr || tracer_ == nullptr) {
            return;
        }
        const Message& msg = *context.message;
        OTTagMap tags{{OT_SPAN_KIND, OT_SPAN_KIND_PRODUCER}};
        std::vector<std::shared_ptr<OpenTracingSpanContext>> refs;
        auto upstream = tracer_->extract(msg.getProperties());
        if (upstream) {
            refs.push_back(std::move(upstream));
        }
        auto span = tracer_->startSpan(std::string(TraceConstants::TO_PREFIX) + msg.getTopic(), tags, refs);
        span->setTag(OT_PEER_SERVICE, OT_ROCKETMQ_SERVICE);
        span->setTag(OT_MESSAGE_BUS_DESTINATION, msg.getTopic());
        span->setTag(OT_ROCKETMQ_TAGS, msg.getTags());
        span->setTag(OT_ROCKETMQ_KEYS, msg.getKeys());
        span->setTag(OT_ROCKETMQ_STORE_HOST, context.brokerAddr);
        span->setTag(OT_ROCKETMQ_MSG_TYPE, otMsgTypeName(context.msgType));
        span->setTag(OT_ROCKETMQ_BODY_LENGTH, std::to_string(msg.getBody().size()));
        // See the header note: context.message is const in cpp, so the
        // just-started span's context is NOT written back onto the properties.
        auto holder = std::make_shared<OTSpanHolder>();
        holder->span = std::move(span);
        context.mqTraceContext = std::move(holder);
    }

    void sendMessageAfter(SendMessageContext& context) override {
        auto holder = std::dynamic_pointer_cast<OTSpanHolder>(context.mqTraceContext);
        if (!holder || !holder->span) {
            return;
        }
        // No result, or no region id on it: nothing to report, span stays open.
        if (context.sendResult == nullptr || context.sendResult->regionId.empty()) {
            return;
        }
        holder->span->setTag(OT_ROCKETMQ_SUCCESS,
                             otBoolStr(context.sendResult->sendStatus == SendStatus::SEND_OK));
        holder->span->setTag(OT_ROCKETMQ_MSG_ID, context.sendResult->msgId);
        holder->span->setTag(OT_ROCKETMQ_REGION_ID, context.sendResult->regionId);
        holder->span->finish();
    }

private:
    std::shared_ptr<OpenTracingTracer> tracer_;
};

// ---------------------------------------------------------------- consume leg

// Consume leg: one span per message, "From_<topic>", kind consumer; msg_id /
// tags / keys / body_length (store size) / retry_times / region_id at open;
// success at close.
class ConsumeMessageOpenTracingHook : public ConsumeMessageHook {
public:
    explicit ConsumeMessageOpenTracingHook(std::shared_ptr<OpenTracingTracer> tracer)
        : tracer_(std::move(tracer)) {}

    std::string hookName() const override { return "ConsumeMessageOpenTracingHook"; }

    void consumeMessageBefore(ConsumeMessageContext& context) override {
        if (context.msgList.empty() || tracer_ == nullptr) {
            return;
        }
        auto holder = std::make_shared<OTConsumeSpanHolder>();
        for (const auto& msg : context.msgList) {
            OTTagMap tags{{OT_SPAN_KIND, OT_SPAN_KIND_CONSUMER}};
            std::vector<std::shared_ptr<OpenTracingSpanContext>> refs;
            auto upstream = tracer_->extract(msg.getProperties());
            if (upstream) {
                refs.push_back(std::move(upstream));
            }
            auto span = tracer_->startSpan(std::string(TraceConstants::FROM_PREFIX) + msg.getTopic(), tags, refs);
            span->setTag(OT_PEER_SERVICE, OT_ROCKETMQ_SERVICE);
            span->setTag(OT_MESSAGE_BUS_DESTINATION,
                         NamespaceUtil::withoutNamespace(msg.getTopic()));
            span->setTag(OT_ROCKETMQ_MSG_ID, msg.getMsgId());
            span->setTag(OT_ROCKETMQ_TAGS, msg.getTags());
            span->setTag(OT_ROCKETMQ_KEYS, msg.getKeys());
            span->setTag(OT_ROCKETMQ_BODY_LENGTH, std::to_string(msg.getStoreSize()));
            span->setTag(OT_ROCKETMQ_RETRY_TIMERS, std::to_string(msg.getReconsumeTimes()));
            span->setTag(OT_ROCKETMQ_REGION_ID, msg.getProperty(MessageConst::PROPERTY_MSG_REGION));
            holder->spans.push_back(std::move(span));
        }
        context.mqTraceContext = std::move(holder);
    }

    void consumeMessageAfter(ConsumeMessageContext& context) override {
        auto holder = std::dynamic_pointer_cast<OTConsumeSpanHolder>(context.mqTraceContext);
        if (!holder) {
            return;
        }
        for (auto& span : holder->spans) {
            span->setTag(OT_ROCKETMQ_SUCCESS, otBoolStr(context.success));
            span->finish();
        }
    }

private:
    std::shared_ptr<OpenTracingTracer> tracer_;
};

// ------------------------------------------------------------ transaction leg

// Transaction leg: span "EndTransaction" with the transaction identity keys,
// closed immediately. The message is left untouched on this leg (the send
// side already put the context on the properties).
class EndTransactionOpenTracingHook : public EndTransactionHook {
public:
    explicit EndTransactionOpenTracingHook(std::shared_ptr<OpenTracingTracer> tracer)
        : tracer_(std::move(tracer)) {}

    std::string hookName() const override { return "EndTransactionOpenTracingHook"; }

    void endTransaction(EndTransactionContext& context) override {
        if (tracer_ == nullptr) {
            return;
        }
        const Message* msg = context.message;
        OTTagMap tags{{OT_SPAN_KIND, OT_SPAN_KIND_PRODUCER}};
        std::vector<std::shared_ptr<OpenTracingSpanContext>> refs;
        if (msg != nullptr) {
            auto upstream = tracer_->extract(msg->getProperties());
            if (upstream) {
                refs.push_back(std::move(upstream));
            }
        }
        auto span = tracer_->startSpan(TraceConstants::END_TRANSACTION, tags, refs);
        span->setTag(OT_PEER_SERVICE, OT_ROCKETMQ_SERVICE);
        span->setTag(OT_MESSAGE_BUS_DESTINATION, msg ? msg->getTopic() : "");
        span->setTag(OT_ROCKETMQ_TAGS, msg ? msg->getTags() : "");
        span->setTag(OT_ROCKETMQ_KEYS, msg ? msg->getKeys() : "");
        span->setTag(OT_ROCKETMQ_STORE_HOST, context.brokerAddr);
        span->setTag(OT_ROCKETMQ_MSG_ID, context.msgId);
        span->setTag(OT_ROCKETMQ_MSG_TYPE, OT_TRANS_MSG_COMMIT);
        span->setTag(OT_ROCKETMQ_TRANSACTION_ID, context.transactionId);
        span->setTag(OT_ROCKETMQ_TRANSACTION_STATE, context.transactionState);
        span->setTag(OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK,
                     otBoolStr(context.fromTransactionCheck));
        span->finish();
    }

private:
    std::shared_ptr<OpenTracingTracer> tracer_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_OPEN_TRACING_HOOK_H
