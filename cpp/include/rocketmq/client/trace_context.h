// W3C Trace Context（traceparent）透传 —— OpenTracing/OTel 场景的消息级上下文。
//
// 格式：`00-<trace-id 32hex>-<parent-id 16hex>-<flags 2hex>`；id 不能全 0。
// 生产侧：opt-in（enableTraceContext / env ROCKETMQ_TRACE_CONTEXT_ENABLE），
// 发送路径注入根上下文；已有值**不覆盖**（上游传播优先）。键名沿用 W3C 小写
// `traceparent`。Java 客户端把这类注入交给外部链路追踪的 SendMessageHook，
// 本实现内建等价能力（有意差异：直接在发送路径注入，而非钩子 —— C++ 的
// SendMessageContext 持有 const Message*，钩子不便改写属性）。
#ifndef ROCKETMQ_CLIENT_TRACE_CONTEXT_H
#define ROCKETMQ_CLIENT_TRACE_CONTEXT_H

#include <string>

#include "rocketmq/common/message.h"

namespace rocketmq {

inline constexpr const char* kTraceContextProperty = "traceparent";
inline constexpr const char* kTraceStateProperty = "tracestate";

// 生成合法的根 traceparent：`00-<32hex>-<16hex>-01`（记录采样）。
std::string generateTraceparent();

// 按 W3C 语法与"不全 0"规则校验。宽松接受大写 hex（转发不重写）。
bool isValidTraceparent(const std::string& value);

// 同一 trace-id 下生成子 span（换 parent-id）；parent 非法返回空串。
std::string childTraceparent(const std::string& parent);

// 消息没有 traceparent 属性时注入根上下文；返回（注入后的）值。
std::string injectTraceContext(Message* msg);

// 从消息属性里取出 traceparent（未注入/为空返回空串）。
std::string extractTraceparent(const Message& msg);

// env `ROCKETMQ_TRACE_CONTEXT_ENABLE`。
bool traceContextEnabledFromEnv();

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_TRACE_CONTEXT_H
