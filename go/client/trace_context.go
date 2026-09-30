// W3C Trace Context (traceparent) propagation — the message-level context used
// by OpenTracing/OpenTelemetry integrations (Java leaves this to a
// SkyWalking/OTel SendMessageHook; the Python and Rust ports build the
// equivalent in, and so does this one).
//
//	traceparent: 00-<trace-id 32hex>-<parent-id 16hex>-<flags 2hex>
//
// Rules that matter:
//
//   - trace-id and parent-id must not be all zeroes; flags may be ("00" records
//     sampling off, and that is legal);
//   - the producer INJECTS a root span only when the message carries no
//     traceparent yet — a context propagated by the caller always wins;
//   - the consumer EXTRACTS it so business code can parent its own span.
//
// Opt-in via SetEnableTraceContext or the ROCKETMQ_TRACE_CONTEXT_ENABLE env
// var (same switch names as the other ports).
package client

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Trace context property names (W3C keys, lowercase like every HTTP header).
const (
	TraceContextProperty = "traceparent"
	TraceStateProperty   = "tracestate"
)

const traceContextEnableEnv = "ROCKETMQ_TRACE_CONTEXT_ENABLE"

func traceRandomHex(chars int) string {
	b := make([]byte, chars/2)
	// Go 1.24's crypto/rand.Read never returns an error (it crashes the process
	// if the system source is unusable), so there is nothing to branch on.
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// GenerateTraceparent mints a root traceparent: version 00, random ids, flags
// 01 (sampled).
func GenerateTraceparent() string {
	return "00-" + traceRandomHex(32) + "-" + traceRandomHex(16) + "-01"
}

// IsValidTraceparent checks the W3C syntax plus the all-zero rule. Uppercase
// hex is accepted (a forwarded value is never rewritten), matching the ports.
func IsValidTraceparent(value string) bool {
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) != 4 {
		return false
	}
	version, traceID, parentID, flags := parts[0], parts[1], parts[2], parts[3]
	if version != "00" && !(len(version) == 2 && isLowerHex(version)) {
		return false
	}
	// Version ff is forbidden by the spec (and reserved).
	if version == "ff" {
		return false
	}
	if len(traceID) != 32 || len(parentID) != 16 || len(flags) != 2 {
		return false
	}
	if !isLowerHex(traceID) || !isLowerHex(parentID) || !isLowerHex(flags) {
		return false
	}
	if traceID == strings.Repeat("0", 32) || parentID == strings.Repeat("0", 16) {
		return false
	}
	return true
}

func isLowerHex(s string) bool {
	for _, c := range strings.ToLower(s) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ChildTraceparent derives a child span (same trace-id, fresh parent-id) from a
// valid parent. ok=false means the parent was malformed.
func ChildTraceparent(parent string) (string, bool) {
	if !IsValidTraceparent(parent) {
		return "", false
	}
	parts := strings.Split(strings.TrimSpace(parent), "-")
	return "00-" + strings.ToLower(parts[1]) + "-" + traceRandomHex(16) + "-01", true
}

// InjectTraceContext writes a root traceparent into the message unless one is
// already present (caller-propagated contexts win) and returns the value now on
// the message.
func InjectTraceContext(msg *common.Message) string {
	if existing, ok := msg.GetProperty(TraceContextProperty); ok && existing != "" {
		return existing
	}
	tp := GenerateTraceparent()
	msg.PutProperty(TraceContextProperty, tp)
	return tp
}

// ExtractTraceparent reads the traceparent off a consumed message. ok=false
// means the producer never injected one.
func ExtractTraceparent(msg *common.MessageExt) (string, bool) {
	if msg == nil {
		return "", false
	}
	v, ok := msg.GetProperty(TraceContextProperty)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// TraceContextEnabledFromEnv reads ROCKETMQ_TRACE_CONTEXT_ENABLE (the env
// convention shared by the other ports).
func TraceContextEnabledFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(traceContextEnableEnv))) {
	case "1", "true", "yes":
		return true
	}
	return false
}
