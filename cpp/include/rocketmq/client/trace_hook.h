//   * SendMessageTraceHook    ← SendMessageTraceHookImpl
//   * ConsumeMessageTraceHook ← ConsumeMessageTraceHookImpl
//   * EndTransactionTraceHook ← EndTransactionTraceHookImpl
#ifndef ROCKETMQ_CLIENT_TRACE_HOOK_H
#define ROCKETMQ_CLIENT_TRACE_HOOK_H

#include <string>

#include "rocketmq/client/hook.h"
#include "rocketmq/client/trace.h"
#include "rocketmq/client/trace_dispatcher.h"

namespace rocketmq {

// 发送侧轨迹钩子。
class SendMessageTraceHook : public SendMessageHook {
public:
    explicit SendMessageTraceHook(TraceDispatcher* dispatcher) : dispatcher_(dispatcher) {}

    std::string hookName() const override { return "SendMessageTraceHook"; }

    void sendMessageBefore(SendMessageContext& context) override;
    void sendMessageAfter(SendMessageContext& context) override;

private:
    TraceDispatcher* dispatcher_;
};

// 消费侧轨迹钩子（SubBefore / SubAfter 两条记录共用同一个 request_id）。
class ConsumeMessageTraceHook : public ConsumeMessageHook {
public:
    explicit ConsumeMessageTraceHook(TraceDispatcher* dispatcher) : dispatcher_(dispatcher) {}

    std::string hookName() const override { return "ConsumeMessageTraceHook"; }

    void consumeMessageBefore(ConsumeMessageContext& context) override;
    void consumeMessageAfter(ConsumeMessageContext& context) override;

private:
    TraceDispatcher* dispatcher_;
};

// 事务收尾轨迹钩子（对应 EndTransactionTraceHookImpl）。
// broker 的事务回查（fromTransactionCheck=true）也会走这里。
class EndTransactionTraceHook : public EndTransactionHook {
public:
    explicit EndTransactionTraceHook(TraceDispatcher* dispatcher) : dispatcher_(dispatcher) {}

    std::string hookName() const override { return "EndTransactionTraceHook"; }

    void endTransaction(EndTransactionContext& context) override;

private:
    TraceDispatcher* dispatcher_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_TRACE_HOOK_H
