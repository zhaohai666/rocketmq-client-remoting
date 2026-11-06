// OpenTracing hook trio smoke test: span names, tag sets, close-out rules and
// propagation write-back for the send / consume / end-transaction legs, plus
// the send-message-type classification on the context.
// Run: node --experimental-strip-types test/open_tracing_hook_smoke.ts
import assert from 'node:assert';

import { Message, MessageExt } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import {
  ConsumeMessageContext, EndTransactionContext, SendMessageContext,
} from '../src/client/hook.ts';
import { classifySendMessageType, LocalTransactionState } from '../src/client/producer.ts';
import {
  ConsumeMessageOpenTracingHook, EndTransactionOpenTracingHook,
  OT_MESSAGE_BUS_DESTINATION, OT_PEER_SERVICE, OT_ROCKETMQ_BODY_LENGTH,
  OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK, OT_ROCKETMQ_KEYS, OT_ROCKETMQ_MSG_ID,
  OT_ROCKETMQ_MSG_TYPE, OT_ROCKETMQ_REGION_ID, OT_ROCKETMQ_RETRY_TIMERS,
  OT_ROCKETMQ_STORE_HOST, OT_ROCKETMQ_SUCCESS, OT_ROCKETMQ_TAGS,
  OT_ROCKETMQ_TRANSACTION_ID, OT_ROCKETMQ_TRANSACTION_STATE, OT_SPAN_KIND,
  OT_SPAN_KIND_CONSUMER, OT_SPAN_KIND_PRODUCER, SendMessageOpenTracingHook,
  type OTTracerLike,
} from '../src/client/open_tracing_hook.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean) {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name}`); }
}

interface FakeSpan {
  operation: string;
  tags: Record<string, unknown>;
  refs: unknown[];
  finished: boolean;
  context(): object;
  setTag(key: string, value: unknown): void;
  finish(): void;
}

interface RecordedTracer extends OTTracerLike {
  spans: FakeSpan[];
  injected: unknown[];
  upstream: unknown;
}

function fakeTracer(upstream: unknown = undefined): RecordedTracer {
  const spans: FakeSpan[] = [];
  const injected: unknown[] = [];
  return {
    spans,
    injected,
    upstream,
    startSpan(operation: string, options?: { tags?: Record<string, unknown>; refs?: unknown[] }): FakeSpan {
      const span: FakeSpan = {
        operation,
        tags: { ...(options?.tags ?? {}) },
        refs: [...(options?.refs ?? [])],
        finished: false,
        context() { return { spanId: spans.length }; },
        setTag(key: string, value: unknown) { span.tags[key] = value; },
        finish() { span.finished = true; },
      };
      spans.push(span);
      return span;
    },
    extract(carrier: Record<string, string>) {
      return this.upstream != null && carrier != null ? this.upstream : undefined;
    },
    inject(spanContext: unknown, carrier: Record<string, string>) {
      carrier['traceparent'] = 'fake-trace-context';
      injected.push(spanContext);
    },
  };
}

function sendMsg(): Message {
  const msg = new Message('TopicTest', Buffer.from('hello-world'), 'TagA', 'KeyA');
  return msg;
}

console.log('== send leg ==');
{
  const tracer = fakeTracer();
  const hook = new SendMessageOpenTracingHook(tracer);
  const ctx = new SendMessageContext('GID_test', sendMsg(), null, 0, '127.0.0.1:10911');
  ctx.msgType = 'NORMAL_MSG';
  hook.sendMessageBefore(ctx);

  check('one span opened', tracer.spans.length === 1);
  const span = tracer.spans[0];
  check('operation To_TopicTest', span.operation === 'To_TopicTest');
  check('span.kind producer', span.tags[OT_SPAN_KIND] === OT_SPAN_KIND_PRODUCER);
  check('peer.service rocketmq', span.tags[OT_PEER_SERVICE] === 'rocketmq');
  check('destination', span.tags[OT_MESSAGE_BUS_DESTINATION] === 'TopicTest');
  check('tags', span.tags[OT_ROCKETMQ_TAGS] === 'TagA');
  check('keys', span.tags[OT_ROCKETMQ_KEYS] === 'KeyA');
  check('store_host', span.tags[OT_ROCKETMQ_STORE_HOST] === '127.0.0.1:10911');
  check('msg_type NORMAL_MSG', span.tags[OT_ROCKETMQ_MSG_TYPE] === 'NORMAL_MSG');
  check('body_length', span.tags[OT_ROCKETMQ_BODY_LENGTH] === 'hello-world'.length);
  check('span stashed on mqTraceContext', (ctx as any).mqTraceContext === span);
  check('span open', !span.finished);

  check('propagation injected into live properties',
    ctx.message!.getProperty('traceparent') === 'fake-trace-context');
  check('inject received the span context', tracer.injected.length === 1);

  ctx.sendResult = { sendStatus: 0, msgId: 'MSG-ID-1', regionId: 'DefaultRegion' } as any;
  hook.sendMessageAfter(ctx);
  check('success true', span.tags[OT_ROCKETMQ_SUCCESS] === true);
  check('msg_id', span.tags[OT_ROCKETMQ_MSG_ID] === 'MSG-ID-1');
  check('region_id', span.tags[OT_ROCKETMQ_REGION_ID] === 'DefaultRegion');
  check('span closed', span.finished);
}
{
  const tracer = fakeTracer();
  const hook = new SendMessageOpenTracingHook(tracer);
  const ctx = new SendMessageContext('GID_test', sendMsg(), null, 0, '127.0.0.1:10911');
  ctx.msgType = 'NORMAL_MSG';
  hook.sendMessageBefore(ctx);
  hook.sendMessageAfter(ctx); // no send result
  check('no result: span stays open', !tracer.spans[0].finished);
  ctx.sendResult = { sendStatus: 0, msgId: 'M', regionId: null } as any;
  hook.sendMessageAfter(ctx);
  check('no region id: span stays open', !tracer.spans[0].finished);
}
{
  const tracer = fakeTracer();
  const hook = new SendMessageOpenTracingHook(tracer);
  const ctx = new SendMessageContext('GID_test', sendMsg(), null, 0, '127.0.0.1:10911');
  ctx.msgType = 'NORMAL_MSG';
  hook.sendMessageBefore(ctx);
  ctx.sendResult = { sendStatus: 1, msgId: 'M', regionId: 'R' } as any;
  hook.sendMessageAfter(ctx);
  check('failure reports success false', tracer.spans[0].tags[OT_ROCKETMQ_SUCCESS] === false);
}
{
  const upstream = { up: true };
  const tracer = fakeTracer(upstream);
  const hook = new SendMessageOpenTracingHook(tracer);
  const ctx = new SendMessageContext('GID_test', sendMsg(), null, 0, null);
  ctx.msgType = 'NORMAL_MSG';
  hook.sendMessageBefore(ctx);
  check('upstream context used as ref', tracer.spans[0].refs.length === 1 && tracer.spans[0].refs[0] === upstream);
}
{
  const tracer = fakeTracer();
  const hook = new SendMessageOpenTracingHook(tracer);
  const ctx = new SendMessageContext('GID_test', sendMsg(), null, 0, null);
  ctx.msgType = 'TRANS_MSG_HALF';
  hook.sendMessageBefore(ctx);
  check('msg_type TRANS_MSG_HALF', tracer.spans[0].tags[OT_ROCKETMQ_MSG_TYPE] === 'TRANS_MSG_HALF');
}

console.log('== consume leg ==');
function makeExt(): MessageExt {
  const msg = new MessageExt('TopicTest', Buffer.from('x'.repeat(10)), 'TagC', 'KeyC');
  msg.msgId = 'EXT-MSG-ID';
  msg.storeSize = 42;
  msg.reconsumeTimes = 2;
  msg.putProperty(MessageConst.PROPERTY_MSG_REGION, 'DefaultRegion');
  return msg;
}
{
  const tracer = fakeTracer();
  const hook = new ConsumeMessageOpenTracingHook(tracer);
  const ctx = new ConsumeMessageContext('CID_test', [makeExt(), makeExt()], 'TopicTest');
  hook.consumeMessageBefore(ctx);

  check('one span per message', tracer.spans.length === 2);
  for (const span of tracer.spans) {
    check('operation From_TopicTest', span.operation === 'From_TopicTest');
    check('span.kind consumer', span.tags[OT_SPAN_KIND] === OT_SPAN_KIND_CONSUMER);
    check('msg_id', span.tags[OT_ROCKETMQ_MSG_ID] === 'EXT-MSG-ID');
    check('body_length = store size', span.tags[OT_ROCKETMQ_BODY_LENGTH] === 42);
    check('retry_times', span.tags[OT_ROCKETMQ_RETRY_TIMERS] === 2);
    check('region_id', span.tags[OT_ROCKETMQ_REGION_ID] === 'DefaultRegion');
    check('consume span open', !span.finished);
  }
  check('spans stashed', Array.isArray((ctx as any).mqTraceContext)
    && (ctx as any).mqTraceContext.length === 2
    && (ctx as any).mqTraceContext[0] === tracer.spans[0]
    && (ctx as any).mqTraceContext[1] === tracer.spans[1]);
}
{
  const tracer = fakeTracer();
  const hook = new ConsumeMessageOpenTracingHook(tracer);
  const ctx = new ConsumeMessageContext('CID_test', [makeExt()], 'TopicTest');
  hook.consumeMessageBefore(ctx);
  (ctx as any).success = false;
  hook.consumeMessageAfter(ctx);
  check('consume failure tagged', tracer.spans[0].tags[OT_ROCKETMQ_SUCCESS] === false);
  check('consume spans closed', tracer.spans[0].finished);
}
{
  const tracer = fakeTracer();
  const hook = new ConsumeMessageOpenTracingHook(tracer);
  const ctx = new ConsumeMessageContext('CID_test', [], 'TopicTest');
  hook.consumeMessageBefore(ctx);
  hook.consumeMessageAfter(ctx);
  check('empty msg list noop', tracer.spans.length === 0);
}

console.log('== end transaction leg ==');
{
  const tracer = fakeTracer();
  const hook = new EndTransactionOpenTracingHook(tracer);
  const ctx = new EndTransactionContext('GID_test', sendMsg(), '127.0.0.1:10911');
  ctx.msgId = 'TX-MSG-ID';
  ctx.transactionId = 'TX-ID-9';
  ctx.transactionState = LocalTransactionState.COMMIT_MESSAGE;
  ctx.fromTransactionCheck = true;
  hook.endTransactionBefore(ctx);

  check('one span opened', tracer.spans.length === 1);
  const span = tracer.spans[0];
  check('operation EndTransaction', span.operation === 'EndTransaction');
  check('span.kind producer', span.tags[OT_SPAN_KIND] === OT_SPAN_KIND_PRODUCER);
  check('destination', span.tags[OT_MESSAGE_BUS_DESTINATION] === 'TopicTest');
  check('msg_id', span.tags[OT_ROCKETMQ_MSG_ID] === 'TX-MSG-ID');
  check('msg_type Trans_msg_Commit', span.tags[OT_ROCKETMQ_MSG_TYPE] === 'Trans_msg_Commit');
  check('transaction_id', span.tags[OT_ROCKETMQ_TRANSACTION_ID] === 'TX-ID-9');
  check('transaction_state', span.tags[OT_ROCKETMQ_TRANSACTION_STATE] === 'COMMIT_MESSAGE');
  check('from_check true', span.tags[OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK] === true);
  check('closed immediately', span.finished);
}
{
  const tracer = fakeTracer();
  const hook = new EndTransactionOpenTracingHook(tracer);
  const ctx = new EndTransactionContext('GID_test', null, null);
  ctx.transactionState = LocalTransactionState.ROLLBACK_MESSAGE;
  hook.endTransactionBefore(ctx);
  check('no message: destination empty', tracer.spans[0].tags[OT_MESSAGE_BUS_DESTINATION] === '');
  check('rollback state name', tracer.spans[0].tags[OT_ROCKETMQ_TRANSACTION_STATE] === 'ROLLBACK_MESSAGE');
}

console.log('== send message type classification ==');
{
  const msg = sendMsg();
  check('normal default', classifySendMessageType(msg) === 'NORMAL_MSG');
  msg.putProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED, 'true');
  check('prepared -> TRANS_MSG_HALF', classifySendMessageType(msg) === 'TRANS_MSG_HALF');
  const delay = sendMsg();
  delay.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, '3');
  check('delay level -> DELAY_MSG', classifySendMessageType(delay) === 'DELAY_MSG');
  const timer = sendMsg();
  timer.putProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, '123456');
  check('timer deliver -> DELAY_MSG', classifySendMessageType(timer) === 'DELAY_MSG');
}

console.log('== hook names ==');
{
  check('send hook name', new SendMessageOpenTracingHook(fakeTracer()).getHookName() === 'SendMessageOpenTracingHook');
  check('consume hook name', new ConsumeMessageOpenTracingHook(fakeTracer()).getHookName() === 'ConsumeMessageOpenTracingHook');
  check('end tx hook name', new EndTransactionOpenTracingHook(fakeTracer()).getHookName() === 'EndTransactionOpenTracingHook');
}

console.log(`\n${fail === 0 ? 'ALL GREEN' : fail + ' failure(s)'} (${pass} checks)`);
process.exit(fail === 0 ? 0 : 1);
