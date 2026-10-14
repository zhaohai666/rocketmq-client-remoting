//   * SendMessageTraceHook    — send-side trace records
//   * ConsumeMessageTraceHook — consume-side trace records
//   * EndTransactionTraceHook — end-transaction trace records
#ifndef ROCKETMQ_CLIENT_TRACE_HOOK_H
#define ROCKETMQ_CLIENT_TRACE_HOOK_H

#include <string>

#include "rocketmq/client/hook.h"
#include "rocketmq/client/trace.h"
#include "rocketmq/client/trace_dispatcher.h"

namespace rocketmq {

// Send-side trace hook.
class SendMessageTraceHook : public SendMessageHook {
public:
    explicit SendMessageTraceHook(TraceDispatcher* dispatcher) : dispatcher_(dispatcher) {}

    std::string hookName() const override { return "SendMessageTraceHook"; }

    void sendMessageBefore(SendMessageContext& context) override;
    void sendMessageAfter(SendMessageContext& context) override;

private:
    TraceDispatcher* dispatcher_;
};

// Consume-side trace hook (the SubBefore / SubAfter records share one request_id).
class ConsumeMessageTraceHook : public ConsumeMessageHook {
public:
    explicit ConsumeMessageTraceHook(TraceDispatcher* dispatcher) : dispatcher_(dispatcher) {}

    std::string hookName() const override { return "ConsumeMessageTraceHook"; }

    void consumeMessageBefore(ConsumeMessageContext& context) override;
    void consumeMessageAfter(ConsumeMessageContext& context) override;

private:
    TraceDispatcher* dispatcher_;
};

// Trace hook for the end of a transaction (commit/rollback).
// The broker's transaction check-back (fromTransactionCheck=true) also passes through here.
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
