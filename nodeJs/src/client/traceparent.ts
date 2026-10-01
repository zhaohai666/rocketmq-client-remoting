// W3C Trace Context (traceparent) propagation — the message-level context used
// by OpenTelemetry integrations (Java delegates this to an external
// SkyWalking/OTel SendMessageHook; the Go/Python/Rust ports build the
// equivalent in, and so does this one).
//
//	traceparent: 00-<trace-id 32hex>-<parent-id 16hex>-<flags 2hex>
//
// Rules that matter:
//
//   - trace-id and parent-id must not be all zeroes; flags "00" (sampling off)
//     is legal;
//   - the producer INJECTS a root span only when the message carries no
//     traceparent yet — a context propagated by the caller always wins;
//   - the consumer EXTRACTS it so business code can parent its own span.
//
// Opt-in via setEnableTraceContext or the ROCKETMQ_TRACE_CONTEXT_ENABLE env
// var (same switch names as the other ports).
import { randomBytes } from 'node:crypto';
import { Message } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';

export const TRACE_CONTEXT_PROPERTY = MessageConst.PROPERTY_TRACE_PARENT; // "traceparent"
export const TRACE_STATE_PROPERTY = 'tracestate';

const TRACE_CONTEXT_ENABLE_ENV = 'ROCKETMQ_TRACE_CONTEXT_ENABLE';

function traceRandomHex(chars: number): string {
  return randomBytes(chars / 2).toString('hex');
}

// generateTraceparent mints a root traceparent: version 00, random ids, flags
// 01 (sampled).
export function generateTraceparent(): string {
  return `00-${traceRandomHex(32)}-${traceRandomHex(16)}-01`;
}

function isHex(s: string): boolean {
  return /^[0-9a-fA-F]+$/.test(s);
}

// isValidTraceparent checks the W3C syntax plus the all-zero rule. Uppercase
// hex is accepted (a forwarded value is never rewritten), matching the ports.
export function isValidTraceparent(value: string): boolean {
  const parts = String(value).trim().split('-');
  if (parts.length !== 4) return false;
  const [version, traceID, parentID, flags] = parts;
  if (version !== '00' && !(version.length === 2 && isHex(version))) return false;
  // Version ff is forbidden by the spec (and reserved).
  if (version === 'ff') return false;
  if (traceID.length !== 32 || parentID.length !== 16 || flags.length !== 2) return false;
  if (!isHex(traceID) || !isHex(parentID) || !isHex(flags)) return false;
  if (/^0{32}$/.test(traceID) || /^0{16}$/.test(parentID)) return false;
  return true;
}

// childTraceparent derives a child span (same trace-id, fresh parent-id) from a
// valid parent. Returns null when the parent is malformed.
export function childTraceparent(parent: string): string | null {
  if (!isValidTraceparent(parent)) return null;
  const parts = String(parent).trim().split('-');
  return `00-${parts[1].toLowerCase()}-${traceRandomHex(16)}-01`;
}

// injectTraceContext writes a root traceparent into the message unless one is
// already present (caller-propagated contexts win) and returns the value now on
// the message.
export function injectTraceContext(msg: Message): string {
  const existing = msg.getProperty(TRACE_CONTEXT_PROPERTY);
  if (existing) return existing;
  const tp = generateTraceparent();
  msg.putProperty(TRACE_CONTEXT_PROPERTY, tp);
  return tp;
}

// extractTraceparent reads the traceparent off a consumed message. Returns
// null when the producer never injected one.
export function extractTraceparent(msg: Message): string | null {
  if (!msg) return null;
  const v = msg.getProperty(TRACE_CONTEXT_PROPERTY);
  return v ? v : null;
}

// traceContextEnabledFromEnv reads ROCKETMQ_TRACE_CONTEXT_ENABLE (the env
// convention shared by the other ports).
export function traceContextEnabledFromEnv(): boolean {
  const v = (process.env[TRACE_CONTEXT_ENABLE_ENV] || '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}
