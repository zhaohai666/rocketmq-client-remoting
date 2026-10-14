// The three hard rules are in the trace_hook.h header comment: trace messages are never traced, whether a record is written is decided by the broker / message
// property, and the consume-side msgId uses the offset-side ID (matching SendResult.offsetMsgId rather than SendResult.msgId /
// UNIQ_KEY).
#include "rocketmq/client/trace_hook.h"

#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/util_all.h"

namespace rocketmq {

namespace {

// ConsumeReturnType name -> ordinal (this order is the protocol's ConsumeReturnType enum ordinal,
// which is exactly what the trace SubAfter contextCode uses; it must not be reordered). Unknown names fall back to SUCCESS(0).
int32_t consumeReturnTypeOrdinal(const std::string& name) {
    if (name == "SUCCESS") return 0;
    if (name == "TIME_OUT") return 1;
    if (name == "EXCEPTION") return 2;
    if (name == "RETURNNULL") return 3;
    if (name == "FAILED") return 4;
    return 0;
}

bool startsWith(const std::string& s, const std::string& prefix) {
    if (prefix.size() > s.size()) return false;
    return s.compare(0, prefix.size(), prefix) == 0;
}

}  // namespace

// ---------------------------------------------------------------- SendMessage
void SendMessageTraceHook::sendMessageBefore(SendMessageContext& context) {
    if (dispatcher_ == nullptr || context.message == nullptr) return;
    const std::string& topic = context.message->topic;
    if (startsWith(topic, dispatcher_->getTraceTopicName())) return;  // trace messages are never traced

    auto traceContext = std::make_shared<TraceContext>();
    context.mqTraceContext = traceContext;
    traceContext->traceType = TraceType::PUB;
    traceContext->groupName = NamespaceUtil::withoutNamespace(context.producerGroup);

    TraceBean bean;
    bean.topic = NamespaceUtil::withoutNamespace(topic);
    bean.tags = context.message->getTags();
    bean.keys = context.message->getKeys();
    bean.storeHost = context.brokerAddr;
    bean.bodyLength = static_cast<int32_t>(context.message->body.size());
    bean.msgType = context.msgType;
    traceContext->traceBeans = {bean};
}

void SendMessageTraceHook::sendMessageAfter(SendMessageContext& context) {
    if (dispatcher_ == nullptr || context.message == nullptr) return;
    const std::string& topic = context.message->topic;
    if (startsWith(topic, dispatcher_->getTraceTopicName())) return;
    if (context.mqTraceContext == nullptr) return;
    if (context.sendResult == nullptr) return;
    const SendResult& result = *context.sendResult;
    // Nothing is written when the broker reports traceOn=false or returns no region
    if (result.regionId.empty() || !result.traceOn) return;

    TraceContext& traceContext = *context.mqTraceContext;
    if (traceContext.traceBeans.empty()) return;

    int64_t now = UtilAll::currentTimeMillis();
    int32_t costTime = static_cast<int32_t>((now - traceContext.timeStamp)
                                            / static_cast<int64_t>(traceContext.traceBeans.size()));
    traceContext.costTime = costTime;
    traceContext.isSuccess = (result.sendStatus == SendStatus::SEND_OK);
    traceContext.regionId = result.regionId;

    TraceBean& bean = traceContext.traceBeans[0];
    bean.msgId = result.msgId;               // client-side UNIQ_KEY
    bean.offsetMsgId = result.offsetMsgId;   // the broker's offset message ID
    bean.storeTime = traceContext.timeStamp + static_cast<int64_t>(costTime) / 2;
    dispatcher_->append(context.mqTraceContext);
}

// ---------------------------------------------------------------- ConsumeMessage
void ConsumeMessageTraceHook::consumeMessageBefore(ConsumeMessageContext& context) {
    if (dispatcher_ == nullptr || context.msgList.empty()) return;

    auto traceContext = std::make_shared<TraceContext>();
    context.mqTraceContext = traceContext;
    traceContext->traceType = TraceType::SUB_BEFORE;
    traceContext->groupName = NamespaceUtil::withoutNamespace(context.consumerGroup);

    std::vector<TraceBean> beans;
    for (const MessageExt& msg : context.msgList) {
        // Filter only on the per-message switch: no skip for empty topics (the C++ vector holds no null elements).
        std::string traceOn = msg.getProperty(TraceConstants::PROPERTY_TRACE_SWITCH);
        if (traceOn == "false") continue;  // tracing disabled per message
        TraceBean bean;
        bean.topic = NamespaceUtil::withoutNamespace(msg.topic);
        bean.msgId = msg.msgId;            // consume side uses the offset-side ID (matching SendResult.offsetMsgId)
        bean.tags = msg.getTags();
        bean.keys = msg.getKeys();
        bean.storeTime = msg.storeTimestamp;
        bean.bodyLength = msg.storeSize;
        bean.retryTimes = msg.reconsumeTimes;
        std::string region = msg.getProperty(MessageConst::PROPERTY_MSG_REGION);
        traceContext->regionId = region;   // overwritten per message; the last one wins
        beans.push_back(bean);
    }
    if (!beans.empty()) {
        traceContext->traceBeans = beans;
        traceContext->timeStamp = UtilAll::currentTimeMillis();
        dispatcher_->append(traceContext);
    }
}

void ConsumeMessageTraceHook::consumeMessageAfter(ConsumeMessageContext& context) {
    if (dispatcher_ == nullptr || context.msgList.empty()) return;
    if (context.mqTraceContext == nullptr) return;
    if (context.mqTraceContext->traceBeans.empty()) return;

    TraceContext& subBefore = *context.mqTraceContext;

    auto subAfter = std::make_shared<TraceContext>();
    subAfter->traceType = TraceType::SUB_AFTER;
    subAfter->regionId = subBefore.regionId;
    subAfter->groupName = NamespaceUtil::withoutNamespace(subBefore.groupName);
    subAfter->requestId = subBefore.requestId;        // shares one request_id with SubBefore
    subAfter->accessChannel = context.accessChannel;
    subAfter->isSuccess = context.success;
    int64_t now = UtilAll::currentTimeMillis();
    int32_t costTime = static_cast<int32_t>((now - subBefore.timeStamp)
                                            / static_cast<int64_t>(context.msgList.size()));
    subAfter->costTime = costTime;
    subAfter->traceBeans = subBefore.traceBeans;      // the same batch of messages, msgIds stay the offset-side ones

    // props carries the ConsumeReturnType **name** (the consumer looks the ordinal up by name); unknown values fall back to SUCCESS.
    subAfter->contextCode = consumeReturnTypeOrdinal(context.props);

    dispatcher_->append(subAfter);
}

// ---------------------------------------------------------------- EndTransaction
void EndTransactionTraceHook::endTransaction(EndTransactionContext& context) {
    if (dispatcher_ == nullptr || context.message == nullptr) return;
    const std::string& topic = context.message->topic;
    if (startsWith(topic, dispatcher_->getTraceTopicName())) return;  // trace messages are never traced

    auto traceContext = std::make_shared<TraceContext>();
    traceContext->traceType = TraceType::END_TRANSACTION;
    traceContext->groupName = NamespaceUtil::withoutNamespace(context.producerGroup);

    TraceBean bean;
    bean.topic = NamespaceUtil::withoutNamespace(topic);
    bean.tags = context.message->getTags();
    bean.keys = context.message->getKeys();
    bean.storeHost = context.brokerAddr;
    bean.msgType = static_cast<int32_t>(TraceMessageType::TRANS_COMMIT);
    bean.clientHost = dispatcher_->clientId();
    bean.msgId = context.msgId;
    bean.transactionState = context.transactionState;
    bean.transactionId = context.transactionId;
    bean.fromTransactionCheck = context.fromTransactionCheck;
    traceContext->traceBeans = {bean};
    traceContext->timeStamp = UtilAll::currentTimeMillis();

    std::string region = context.message->getProperty(MessageConst::PROPERTY_MSG_REGION);
    traceContext->regionId = region.empty() ? std::string(TraceConstants::DEFAULT_TRACE_REGION_ID)
                                            : region;
    dispatcher_->append(traceContext);
}

}  // namespace rocketmq
