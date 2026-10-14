#ifndef ROCKETMQ_CLIENT_HOOK_H
#define ROCKETMQ_CLIENT_HOOK_H

#include <optional>
#include <string>
#include <vector>

#include "rocketmq/client/result.h"
#include "rocketmq/client/trace.h"
#include "rocketmq/common/message.h"

namespace rocketmq {

// ---------------------------------------------------------------- SendMessage
struct SendMessageContext {
    std::string producerGroup;
    const Message* message = nullptr;
    MessageQueue mq;
    std::string brokerAddr;
    std::string bornHost;
    int32_t msgType = 0;  // TraceMessageType ordinal
    const SendResult* sendResult = nullptr;
    std::string exception;
    // Hook-private state: before writes the TraceContext, after reads it back
    std::shared_ptr<TraceContext> mqTraceContext;
    std::string props;
    std::string ns;
};

class SendMessageHook {
public:
    virtual ~SendMessageHook();
    virtual std::string hookName() const = 0;
    virtual void sendMessageBefore(SendMessageContext& context) = 0;
    virtual void sendMessageAfter(SendMessageContext& context) = 0;
};

// ---------------------------------------------------------------- ConsumeMessage
struct ConsumeMessageContext {
    std::string consumerGroup;
    std::vector<MessageExt> msgList;
    MessageQueue mq;
    bool success = true;
    std::string status;
    std::shared_ptr<TraceContext> mqTraceContext;
    std::string props;
    std::optional<AccessChannel> accessChannel;

    ConsumeMessageContext() = default;
    ConsumeMessageContext(const std::string& group, const std::vector<MessageExt>& msgs,
                         const MessageQueue& queue)
        : consumerGroup(group), msgList(msgs), mq(queue) {}
};

class ConsumeMessageHook {
public:
    virtual ~ConsumeMessageHook();
    virtual std::string hookName() const = 0;
    virtual void consumeMessageBefore(ConsumeMessageContext& context) = 0;
    virtual void consumeMessageAfter(ConsumeMessageContext& context) = 0;
};

// ---------------------------------------------------------------- EndTransaction
struct EndTransactionContext {
    std::string producerGroup;
    const Message* message = nullptr;
    std::string brokerAddr;
    std::string msgId;
    std::string transactionId;
    std::string transactionState;  // LocalTransactionState name
    bool fromTransactionCheck = false;
    std::string ns;
};

class EndTransactionHook {
public:
    virtual ~EndTransactionHook();
    virtual std::string hookName() const = 0;
    virtual void endTransaction(EndTransactionContext& context) = 0;
};

enum class CommunicationMode { SYNC, ASYNC, ONEWAY };

// Key difference from SendMessageContext: **no sendResult** (nothing has been sent yet), plus an `arg`
// (the business parameter from send(msg, selector, arg)).
struct CheckForbiddenContext {
    std::string nameSrvAddr;
    std::string group;
    const Message* message = nullptr;
    MessageQueue mq;
    std::string brokerAddr;
    CommunicationMode communicationMode = CommunicationMode::SYNC;
    const SendResult* sendResult = nullptr;
    std::string exception;
    const std::string* arg = nullptr;
    bool unitMode = false;
};

class CheckForbiddenHook {
public:
    virtual ~CheckForbiddenHook();
    virtual std::string hookName() const = 0;
    virtual void checkForbidden(CheckForbiddenContext& context) = 0;
};

// `msgList` is **mutable**: messages a hook replaces or trims away are dropped outright by the client
// (pull path = silently skipped and the offset still advances; POP path = acked immediately).
struct FilterMessageContext {
    std::string consumerGroup;
    std::vector<MessageExt> msgList;
    MessageQueue mq;
    const std::string* arg = nullptr;
    bool unitMode = false;
};

class FilterMessageHook {
public:
    virtual ~FilterMessageHook();
    virtual std::string hookName() const = 0;
    virtual void filterMessage(FilterMessageContext& context) = 0;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_HOOK_H
