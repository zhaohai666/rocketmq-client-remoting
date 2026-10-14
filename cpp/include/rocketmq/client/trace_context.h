// W3C Trace Context (traceparent) propagation — message-level context for OpenTracing/OTel scenarios.
#ifndef ROCKETMQ_CLIENT_TRACE_CONTEXT_H
#define ROCKETMQ_CLIENT_TRACE_CONTEXT_H

#include <string>

#include "rocketmq/common/message.h"

namespace rocketmq {

inline constexpr const char* kTraceContextProperty = "traceparent";
inline constexpr const char* kTraceStateProperty = "tracestate";

// Generates a valid root traceparent: `00-<32hex>-<16hex>-01` (sampled flag recorded).
std::string generateTraceparent();

// Validates W3C syntax and the "not all zeros" rule; uppercase hex is accepted leniently (forwarding does not rewrite).
bool isValidTraceparent(const std::string& value);

// Generates a child span (new parent-id) under the same trace-id; returns an empty string when the parent is invalid.
std::string childTraceparent(const std::string& parent);

// Injects a root context when the message has no traceparent property; returns the (injected) value.
std::string injectTraceContext(Message* msg);

// Reads the traceparent from the message properties (empty string when not injected or empty).
std::string extractTraceparent(const Message& msg);

// Environment variable `ROCKETMQ_TRACE_CONTEXT_ENABLE`.
bool traceContextEnabledFromEnv();

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_TRACE_CONTEXT_H
