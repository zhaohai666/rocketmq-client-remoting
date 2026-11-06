// OpenTracing-style hook trio for the message lifecycle.
//
// Port of the Java client's SendMessageOpenTracingHookImpl /
// ConsumeMessageOpenTracingHookImpl / EndTransactionOpenTracingHookImpl (the
// same trio the PHP port carries as OpenTracingHook.php), on top of a
// duck-typed Tracer SPI with zero third-party deps. The tracer is duck-typed
// on purpose: any io.opentracing-shaped tracer (or a test fake) plugs in.
//
// The span rides in the context's mqTraceContext slot, exactly like the Java
// original (context.setMqTraceContext(span)): before opens the span and
// stashes it, after reads it back and closes it. Registering both the trace
// dispatcher hooks and these hooks on the same producer is not supported
// (same restriction as Java).
//
// Cross-process propagation rides on the message properties themselves: the
// upstream context is extracted from the property map and the just-started
// span's context is injected back into it, so whichever keys the tracer's
// injector writes travel in-band with the message.
import { Message, MessageExt } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { NamespaceUtil } from '../remoting/namespace.ts';
import {
  ConsumeMessageContext, ConsumeMessageHook,
  EndTransactionContext, EndTransactionHook,
  SendMessageContext, SendMessageHook,
} from './hook.ts';

// OpenTracing standard tags (io.opentracing.tag.Tags).
export const OT_SPAN_KIND = 'span.kind';
export const OT_SPAN_KIND_PRODUCER = 'producer';
export const OT_SPAN_KIND_CONSUMER = 'consumer';
export const OT_PEER_SERVICE = 'peer.service';
export const OT_MESSAGE_BUS_DESTINATION = 'message_bus.destination';

// The rocketmq.* tag keys, mirroring the Java TraceConstants / the PHP
// Trace.php block (the mgs_id/mgs_type spellings are the Java protocol
// literals -- the typo is load-bearing, do not "fix" it).
export const OT_ROCKETMQ_SERVICE = 'rocketmq';
export const OT_ROCKETMQ_SUCCESS = 'rocketmq.success';
export const OT_ROCKETMQ_TAGS = 'rocketmq.tags';
export const OT_ROCKETMQ_KEYS = 'rocketmq.keys';
export const OT_ROCKETMQ_STORE_HOST = 'rocketmq.store_host';
export const OT_ROCKETMQ_BODY_LENGTH = 'rocketmq.body_length';
export const OT_ROCKETMQ_MSG_ID = 'rocketmq.mgs_id';
export const OT_ROCKETMQ_MSG_TYPE = 'rocketmq.mgs_type';
export const OT_ROCKETMQ_REGION_ID = 'rocketmq.region_id';
export const OT_ROCKETMQ_TRANSACTION_ID = 'rocketmq.transaction_id';
export const OT_ROCKETMQ_TRANSACTION_STATE = 'rocketmq.transaction_state';
export const OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK = 'rocketmq.is_from_transaction_check';
export const OT_ROCKETMQ_RETRY_TIMERS = 'rocketmq.retry_times';

const OT_TO_PREFIX = 'To_';
const OT_FROM_PREFIX = 'From_';
const OT_END_TRANSACTION = 'EndTransaction';
// The transaction leg reports the commit state only (Java MessageType
// .Trans_msg_Commit.name()).
const OT_TRANS_MSG_COMMIT = 'Trans_msg_Commit';

// Duck-typed tracer SPI: the fields/methods an io.opentracing-shaped tracer
// must expose for the trio to run.
export interface OTSpanLike {
  setTag(key: string, value: unknown): unknown;
  finish(): unknown;
}

export interface OTTracerLike {
  startSpan(operation: string, options?: { tags?: Record<string, unknown>; refs?: unknown[] }): OTSpanLike;
  extract(carrier: Record<string, string>): unknown;
  inject(spanContext: unknown, carrier: Record<string, string>): void;
}

function extractUpstream(tracer: OTTracerLike, props: Record<string, string> | null): unknown {
  if (props == null) return undefined;
  try {
    return tracer.extract(props);
  } catch {
    return undefined;
  }
}

function injectIntoMessage(tracer: OTTracerLike, span: OTSpanLike, msg: Message): void {
  // The just-started span's context is what travels: the downstream leg must
  // parent onto THIS send, not onto whatever upstream produced the context.
  // getProperties() returns the live object, so the injector's writes land on
  // the message directly.
  const spanAny = span as unknown as Record<string, unknown>;
  let spanContext: unknown = span;
  for (const getter of ['context', 'getContext']) {
    const fn = spanAny[getter];
    if (typeof fn === 'function') {
      const ctx = (fn as () => unknown).call(span);
      if (ctx != null) { spanContext = ctx; break; }
    }
  }
  try {
    tracer.inject(spanContext, msg.getProperties() as Record<string, string>);
  } catch {
    // propagation must never break the send
  }
}

// ---------------------------------------------------------------- send leg

// Send leg: span "To_<topic>", kind producer, with the kind + service +
// destination + tags + keys + store_host + msg_type + body_length tags at
// open; success / msg_id / region_id at close. No result, or no region id on
// it: the span is left open, exactly like the Java close-out flow.
export class SendMessageOpenTracingHook extends SendMessageHook {
  private tracer: OTTracerLike;

  constructor(tracer: OTTracerLike) {
    super();
    this.tracer = tracer;
  }

  override getHookName(): string { return 'SendMessageOpenTracingHook'; }

  override sendMessageBefore(ctx: SendMessageContext): void {
    if (ctx == null || ctx.message == null) return;
    const msg = ctx.message;
    const upstream = extractUpstream(this.tracer, msg.getProperties() as Record<string, string>);
    const tags: Record<string, unknown> = { [OT_SPAN_KIND]: OT_SPAN_KIND_PRODUCER };
    const options: { tags?: Record<string, unknown>; refs?: unknown[] } = { tags };
    if (upstream != null) options.refs = [upstream];
    const span = this.tracer.startSpan(OT_TO_PREFIX + msg.getTopic(), options);
    span.setTag(OT_PEER_SERVICE, OT_ROCKETMQ_SERVICE);
    span.setTag(OT_MESSAGE_BUS_DESTINATION, msg.getTopic());
    span.setTag(OT_ROCKETMQ_TAGS, msg.getTags() ?? '');
    span.setTag(OT_ROCKETMQ_KEYS, msg.getKeys() ?? '');
    span.setTag(OT_ROCKETMQ_STORE_HOST, ctx.brokerAddr ?? '');
    span.setTag(OT_ROCKETMQ_MSG_TYPE, ctx.msgType ?? 'NORMAL_MSG');
    const body = msg.getBody();
    span.setTag(OT_ROCKETMQ_BODY_LENGTH, body ? body.length : 0);
    injectIntoMessage(this.tracer, span, msg);
    (ctx as unknown as Record<string, unknown>).mqTraceContext = span;
  }

  override sendMessageAfter(ctx: SendMessageContext): void {
    if (ctx == null) return;
    const span = (ctx as unknown as Record<string, unknown>).mqTraceContext as OTSpanLike | undefined;
    if (span == null) return;
    const sr = ctx.sendResult;
    // No result, or no region id on it: nothing to report, span stays open.
    if (sr == null || sr.regionId == null) return;
    span.setTag(OT_ROCKETMQ_SUCCESS, sr.sendStatus === SendStatusCodeOK);
    span.setTag(OT_ROCKETMQ_MSG_ID, sr.msgId ?? '');
    span.setTag(OT_ROCKETMQ_REGION_ID, sr.regionId);
    span.finish();
  }
}

// SendStatus.SEND_OK (kept local so the hook module does not import the whole
// send_result surface for one constant).
const SendStatusCodeOK = 0;

// ---------------------------------------------------------------- consume leg

// Consume leg: one span per message, "From_<topic>", kind consumer; msg_id /
// tags / keys / body_length (store size) / retry_times / region_id at open;
// success at close.
export class ConsumeMessageOpenTracingHook extends ConsumeMessageHook {
  private tracer: OTTracerLike;

  constructor(tracer: OTTracerLike) {
    super();
    this.tracer = tracer;
  }

  override getHookName(): string { return 'ConsumeMessageOpenTracingHook'; }

  override consumeMessageBefore(ctx: ConsumeMessageContext): void {
    if (ctx == null || ctx.msgList == null || ctx.msgList.length === 0) return;
    const spans: OTSpanLike[] = [];
    for (const msg of ctx.msgList) {
      if (!(msg instanceof MessageExt)) continue;
      const upstream = extractUpstream(this.tracer, msg.getProperties() as Record<string, string>);
      const tags: Record<string, unknown> = { [OT_SPAN_KIND]: OT_SPAN_KIND_CONSUMER };
      const options: { tags?: Record<string, unknown>; refs?: unknown[] } = { tags };
      if (upstream != null) options.refs = [upstream];
      const span = this.tracer.startSpan(OT_FROM_PREFIX + msg.getTopic(), options);
      span.setTag(OT_PEER_SERVICE, OT_ROCKETMQ_SERVICE);
      span.setTag(OT_MESSAGE_BUS_DESTINATION, NamespaceUtil.withoutNamespace(msg.getTopic() ?? ''));
      span.setTag(OT_ROCKETMQ_MSG_ID, msg.msgId ?? '');
      span.setTag(OT_ROCKETMQ_TAGS, msg.getTags() ?? '');
      span.setTag(OT_ROCKETMQ_KEYS, msg.getKeys() ?? '');
      span.setTag(OT_ROCKETMQ_BODY_LENGTH, msg.getStoreSize());
      span.setTag(OT_ROCKETMQ_RETRY_TIMERS, msg.getReconsumeTimes());
      span.setTag(OT_ROCKETMQ_REGION_ID, msg.getProperty(MessageConst.PROPERTY_MSG_REGION) ?? '');
      spans.push(span);
    }
    (ctx as unknown as Record<string, unknown>).mqTraceContext = spans;
  }

  override consumeMessageAfter(ctx: ConsumeMessageContext): void {
    if (ctx == null || ctx.msgList == null || ctx.msgList.length === 0) return;
    const spans = (ctx as unknown as Record<string, unknown>).mqTraceContext as OTSpanLike[] | undefined;
    if (spans == null) return;
    // The delivery path stamps an explicit boolean (hookCtx.success) after the
    // listener returns; absent means success, like the other ends' default.
    const success = (ctx as unknown as Record<string, unknown>).success !== false;
    for (const span of spans) {
      span.setTag(OT_ROCKETMQ_SUCCESS, success);
      span.finish();
    }
  }
}

// ------------------------------------------------------------ transaction leg

// Transaction leg: span "EndTransaction" with the transaction identity keys,
// closed immediately. The message is left untouched on this leg (the send
// side already put the context on the properties).
export class EndTransactionOpenTracingHook extends EndTransactionHook {
  private tracer: OTTracerLike;

  constructor(tracer: OTTracerLike) {
    super();
    this.tracer = tracer;
  }

  override getHookName(): string { return 'EndTransactionOpenTracingHook'; }

  override endTransactionBefore(ctx: EndTransactionContext): void {
    if (ctx == null) return;
    const msg = ctx.message;
    const upstream = extractUpstream(this.tracer, msg ? msg.getProperties() as Record<string, string> : null);
    const tags: Record<string, unknown> = { [OT_SPAN_KIND]: OT_SPAN_KIND_PRODUCER };
    const options: { tags?: Record<string, unknown>; refs?: unknown[] } = { tags };
    if (upstream != null) options.refs = [upstream];
    const span = this.tracer.startSpan(OT_END_TRANSACTION, options);
    span.setTag(OT_PEER_SERVICE, OT_ROCKETMQ_SERVICE);
    span.setTag(OT_MESSAGE_BUS_DESTINATION, msg ? msg.getTopic() : '');
    span.setTag(OT_ROCKETMQ_TAGS, msg ? (msg.getTags() ?? '') : '');
    span.setTag(OT_ROCKETMQ_KEYS, msg ? (msg.getKeys() ?? '') : '');
    span.setTag(OT_ROCKETMQ_STORE_HOST, ctx.brokerAddr ?? '');
    span.setTag(OT_ROCKETMQ_MSG_ID, ctx.msgId ?? '');
    span.setTag(OT_ROCKETMQ_MSG_TYPE, OT_TRANS_MSG_COMMIT);
    span.setTag(OT_ROCKETMQ_TRANSACTION_ID, ctx.transactionId ?? '');
    span.setTag(OT_ROCKETMQ_TRANSACTION_STATE, transactionStateName(ctx.transactionState));
    span.setTag(OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK, ctx.fromTransactionCheck === true);
    span.finish();
  }

  override endTransactionAfter(_ctx: EndTransactionContext): void {
    // The Java/PHP leg opens and closes one span synchronously; the pair
    // method exists because the nodeJs EndTransactionHook SPI is two-sided.
  }
}

// LocalTransactionState arrives as either a numeric code or an enum object;
// both map onto the Java state names.
function transactionStateName(state: unknown): string {
  if (state == null) return '';
  if (typeof state === 'number') {
    switch (state) {
      case 0: return 'COMMIT_MESSAGE';
      case 1: return 'ROLLBACK_MESSAGE';
      case 2: return 'UNKNOW';
      default: return String(state);
    }
  }
  const name = (state as Record<string, unknown>).name;
  return typeof name === 'string' ? name : String(state);
}
