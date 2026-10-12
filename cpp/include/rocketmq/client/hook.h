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
    // 钩子私有状态：before 写入 TraceContext，after 取出
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
    std::string transactionState;  // LocalTransactionState 名字
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

// 与 SendMessageContext 的关键差别：**没有 sendResult**（此刻还没发），带上 `arg`
// （send(msg, selector, arg) 里的业务参数）。
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

// `msgList` 是**可变的**：钩子把它替换/裁剪掉的消息会被客户端直接丢弃
// （拉取路径 = 静默跳过、位点照常推进；POP 路径 = 立刻 ack）。
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
