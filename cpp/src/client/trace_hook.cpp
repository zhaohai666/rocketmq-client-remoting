// 消息轨迹钩子实现（对应 org.apache.rocketmq.client.trace.hook 包）。
//
// 三条硬性约定见 trace_hook.h 头注释：轨迹消息不再被追踪、是否落轨迹由 broker / 消息属性
// 决定、消费侧 msgId 用 offset 侧 ID（对齐 SendResult.offsetMsgId，而不是 SendResult.msgId /
// UNIQ_KEY）。
#include "rocketmq/client/trace_hook.h"

#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/util_all.h"

namespace rocketmq {

namespace {

// ConsumeReturnType 名字 -> ordinal（顺序即 Java ConsumeReturnType 的 ordinal，
// 轨迹 SubAfter 的 contextCode 用的正是它）。未知名字退化为 SUCCESS(0)。
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
    if (startsWith(topic, dispatcher_->getTraceTopicName())) return;  // 轨迹消息不再被追踪

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
    // broker 侧 traceOn=false 或没带回 region 时不落库（对齐 Java）
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
    bean.msgId = result.msgId;               // 客户端 UNIQ_KEY
    bean.offsetMsgId = result.offsetMsgId;   // broker 的 offset 消息 ID
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
        // Java 只跳过 null（C++ 的 vector 里没有 null，故不额外过滤），
        // 也不按空 topic 跳过 —— 与 Python 参考实现保持一致。
        std::string traceOn = msg.getProperty(TraceConstants::PROPERTY_TRACE_SWITCH);
        if (traceOn == "false") continue;  // 消息级关闭轨迹
        TraceBean bean;
        bean.topic = NamespaceUtil::withoutNamespace(msg.topic);
        bean.msgId = msg.msgId;            // 消费侧用 offset 侧 ID（对齐 SendResult.offsetMsgId）
        bean.tags = msg.getTags();
        bean.keys = msg.getKeys();
        bean.storeTime = msg.storeTimestamp;
        bean.bodyLength = msg.storeSize;
        bean.retryTimes = msg.reconsumeTimes;
        std::string region = msg.getProperty(MessageConst::PROPERTY_MSG_REGION);
        traceContext->regionId = region;   // 逐条覆盖（最后一条生效，对齐 Python/Java）
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
    subAfter->requestId = subBefore.requestId;        // 与 SubBefore 共用一个 request_id
    subAfter->accessChannel = context.accessChannel;
    subAfter->isSuccess = context.success;
    int64_t now = UtilAll::currentTimeMillis();
    int32_t costTime = static_cast<int32_t>((now - subBefore.timeStamp)
                                            / static_cast<int64_t>(context.msgList.size()));
    subAfter->costTime = costTime;
    subAfter->traceBeans = subBefore.traceBeans;      // 同一批消息，msgId 沿用 offset 侧 ID

    // props 里放的是 ConsumeReturnType 的**名字**（Java 按名查 ordinal），未知退化为 SUCCESS。
    subAfter->contextCode = consumeReturnTypeOrdinal(context.props);

    dispatcher_->append(subAfter);
}

// ---------------------------------------------------------------- EndTransaction
void EndTransactionTraceHook::endTransaction(EndTransactionContext& context) {
    if (dispatcher_ == nullptr || context.message == nullptr) return;
    const std::string& topic = context.message->topic;
    if (startsWith(topic, dispatcher_->getTraceTopicName())) return;  // 轨迹消息不再被追踪

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
