// W3C Trace Context（traceparent）在消息上的传播，供 OpenTelemetry 一类集成使用。
//
//	traceparent: 00-<trace-id 32hex>-<parent-id 16hex>-<flags 2hex>
//
// 关键规则：
//
//   - trace-id 与 parent-id 不能全零；flags "00"（未采样）合法；
//   - producer 只有在消息**尚未携带 traceparent** 时才注入根 span——上游传播
//     过来的 context 永远优先；
//   - consumer 侧执行 EXTRACT，让业务代码能挂自己的子 span。
//
// 通过 setEnableTraceContext 或 ROCKETMQ_TRACE_CONTEXT_ENABLE 环境变量启用。
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

// isValidTraceparent 检查 W3C 语法加上"非全零"规则。大写 hex 也接受（转发过来的
// 值不做改写）。
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

// traceContextEnabledFromEnv 读取 ROCKETMQ_TRACE_CONTEXT_ENABLE 环境变量。
export function traceContextEnabledFromEnv(): boolean {
  const v = (process.env[TRACE_CONTEXT_ENABLE_ENV] || '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}
