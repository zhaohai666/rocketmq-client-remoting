// 客户端钩子（对应 org.apache.rocketmq.client.hook 包）。
//
// Java 侧钩子是「业务无关的切面」：生产者在发送前后各调一次 SendMessageHook，消费者在
// 投递 listener 前后各调一次 ConsumeMessageHook。消息轨迹（client.trace.hook.*）正是建在
// 这两个接口上的。
//
// 设计约定（与 Java 一致）：
//   * 钩子抛出的异常**必须被吞掉并记 warn**（见 producer/consumer 的调用点），
//     绝不能因为轨迹出错影响正常收发；
//   * ``mq_trace_context`` 是钩子自己的私有状态：before 写入、after 取出，
//     中间不允许别的钩子依赖它的具体类型。
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

// ---------------------------------------------------------------- 发送模式
// 对应 org.apache.rocketmq.client.impl.CommunicationMode（Java 是枚举，三个常量）。
enum class CommunicationMode { SYNC, ASYNC, ONEWAY };

// ---------------------------------------------------------------- CheckForbidden
// 对应 org.apache.rocketmq.client.hook.CheckForbiddenContext。
//
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
    // Java 是 Object arg；C++ 的 sendBySelector 用 std::string 承载，这里照此对齐，
    // 其余发送入口传 nullptr。
    const std::string* arg = nullptr;
    // 本项目无 unit mode（Java 的 isUnitMode() 恒为 false）
    bool unitMode = false;
};

// 对应 org.apache.rocketmq.client.hook.CheckForbiddenHook。
//
// ⚠ 与 Send/Consume 钩子**相反**：checkForbidden 抛出的异常**不会被吞掉**
// （Java 签名就是 `throws MQClientException`），而是沿发送重试链向上传播 ——
// 这正是"拦截"能力的实现方式。
class CheckForbiddenHook {
public:
    virtual ~CheckForbiddenHook();
    virtual std::string hookName() const = 0;
    virtual void checkForbidden(CheckForbiddenContext& context) = 0;
};

// ---------------------------------------------------------------- FilterMessage
// 对应 org.apache.rocketmq.client.hook.FilterMessageContext。
//
// `msgList` 是**可变的**：钩子把它替换/裁剪掉的消息会被客户端直接丢弃
// （拉取路径 = 静默跳过、位点照常推进；POP 路径 = 立刻 ack）。
struct FilterMessageContext {
    std::string consumerGroup;
    std::vector<MessageExt> msgList;
    MessageQueue mq;
    const std::string* arg = nullptr;
    bool unitMode = false;
};

// 对应 org.apache.rocketmq.client.hook.FilterMessageHook。
class FilterMessageHook {
public:
    virtual ~FilterMessageHook();
    virtual std::string hookName() const = 0;
    virtual void filterMessage(FilterMessageContext& context) = 0;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_HOOK_H
