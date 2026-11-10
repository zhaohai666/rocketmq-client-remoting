// Java MessageClientExt.getMsgId() parity on consumed messages — offline.
//
//   node --experimental-strip-types test/client_msg_id_parity_smoke.ts
//
// Java client-side decoding yields MessageClientExt, whose getMsgId() returns
// the UNIQ_KEY property and only falls back to the broker offset id. The port
// keeps the decoded msg.msgId / msg.offsetMsgId as the offset id, so every place
// that in Java calls msg.getMsgId() on a *consumed* message must go through
// clientMsgId() instead of reading the offset id directly:
//
//   * ConsumeMessageTraceHookImpl -> SubBefore/SubAfter bean.msgId
//   * CONSUMER_SEND_MSG_BACK      -> header.originMsgId (push + pull)
//   * retry fallback copy         -> ORIGIN_MESSAGE_ID property
import { clientMsgId } from '../src/common/messageClientIdSetter.ts';
import { traceBeanFromMessageExt } from '../src/client/trace_context.ts';
import { ConsumeMessageTraceHookImpl } from '../src/client/trace_hook.ts';
import { ConsumeMessageContext } from '../src/client/hook.ts';
import { buildRetryMessage } from '../src/client/retry_message.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { MessageExt } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const UNIQ = 'AC1E0A0B2C3D4E5F60718293A4B5C6D7';
const OFFSET = '0A0B0C0D00002A9F0000000000000EA7';
const GROUP = 'GID_NodeJsClientMsgIdParity';
const TOPIC = 'NodeJsClientMsgIdParityTopic';
const ADDR = '127.0.0.1:10911';

function consumed(withUniq: boolean): MessageExt {
  const m = new MessageExt(TOPIC, Buffer.from('payload'), 'TagA', 'key-1', 7);
  m.brokerName = 'broker-a';
  m.queueId = 0;
  m.queueOffset = 7;
  m.commitLogOffset = 123456;
  m.msgId = OFFSET;
  m.reconsumeTimes = 1;
  if (withUniq) m.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, UNIQ);
  return m;
}

class CapturingDispatcher {
  appended: any[] = [];
  append(ctx: any): boolean { this.appended.push(ctx); return true; }
}

// A refused bounce is recorded (header) and degrades to one retry send (copy).
function fakeClient(answer: number) {
  const bounces: any[] = [];
  const client = {
    clientId: 'fake-client-id',
    findBrokerAddressInPublish(_broker: string) { return ADDR; },
    vipChannel(addr: string) { return addr; },
    remotingClient: {
      async invokeSync(_addr: string, request: any, _timeout: number) {
        bounces.push(request.customHeader);
        return { code: answer, remark: '' };
      },
    },
    async unregisterClientAllBrokers(_addr: string, _group: string) { /* no wire */ },
    unregisterConsumer(_group: string) { /* no wire */ },
  };
  return { client, bounces };
}

function fakeProducer() {
  const sent: any[] = [];
  const producer = {
    async sendBySelector(msg: any) { sent.push(msg); },
    async shutdown() { /* no wire */ },
  };
  return { producer, sent };
}

console.log('== helper: clientMsgId ==');
check('UNIQ_KEY preferred over offset id', clientMsgId(consumed(true)) === UNIQ);
check('falls back to offset id without UNIQ_KEY', clientMsgId(consumed(false)) === OFFSET);
check('null message -> null', clientMsgId(null) === null);

console.log('== traceBeanFromMessageExt (consumer mode = false) ==');
check('UNIQ_KEY mode takes UNIQ_KEY', traceBeanFromMessageExt(consumed(true), false).msgId === UNIQ);
check('UNIQ_KEY mode falls back to offset id', traceBeanFromMessageExt(consumed(false), false).msgId === OFFSET);
check('offset mode still takes the offset id', traceBeanFromMessageExt(consumed(true), true).msgId === OFFSET);

console.log('== SubBefore / SubAfter beans ==');
{
  const d = new CapturingDispatcher();
  const hook = new ConsumeMessageTraceHookImpl(d as any);
  const ctx = new ConsumeMessageContext(GROUP, [consumed(true)], TOPIC);
  ctx.props = { ConsumeContextType: 'SUCCESS' };
  (ctx as any).success = true;
  hook.consumeMessageBefore(ctx);
  hook.consumeMessageAfter(ctx);
  check('before + after appended 2 contexts', d.appended.length === 2, String(d.appended.length));
  check('SubBefore bean.msgId = UNIQ_KEY', d.appended[0]?.traceBeans[0]?.msgId === UNIQ);
  check('SubAfter bean.msgId = UNIQ_KEY', d.appended[1]?.traceBeans[0]?.msgId === UNIQ);
  check('SubBefore/SubAfter share requestId', d.appended[1]?.requestId === d.appended[0]?.requestId);
}
{
  const d = new CapturingDispatcher();
  new ConsumeMessageTraceHookImpl(d as any).consumeMessageBefore(
    new ConsumeMessageContext(GROUP, [consumed(false)], TOPIC));
  check('SubBefore without UNIQ_KEY falls back to offset id',
    d.appended[0]?.traceBeans[0]?.msgId === OFFSET);
}

console.log('== buildRetryMessage ORIGIN_MESSAGE_ID ==');
check('with UNIQ_KEY -> ORIGIN_MESSAGE_ID = UNIQ_KEY',
  buildRetryMessage(GROUP, consumed(true), 16).getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID) === UNIQ);
check('without UNIQ_KEY -> ORIGIN_MESSAGE_ID = offset id',
  buildRetryMessage(GROUP, consumed(false), 16).getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID) === OFFSET);

console.log('== send-back header + retry copy (push + pull) ==');
for (const [kind, makeConsumer] of [
  ['push', () => new DefaultMQPushConsumer(GROUP)],
  ['pull', () => new DefaultMQPullConsumer(GROUP)],
] as const) {
  const c: any = makeConsumer();
  const { client, bounces } = fakeClient(ResponseCode.SYSTEM_ERROR);
  const inner = fakeProducer();
  c.mqClient = client;
  c._producer = inner.producer;
  await c.sendMessageBack(consumed(true), 0);
  check(`${kind}: one bounce recorded`, bounces.length === 1, String(bounces.length));
  check(`${kind}: header.originMsgId = UNIQ_KEY`, bounces[0]?.originMsgId === UNIQ,
    String(bounces[0]?.originMsgId));
  check(`${kind}: header.originMsgId != offset id`, bounces[0]?.originMsgId !== OFFSET);
  check(`${kind}: one retry send`, inner.sent.length === 1, String(inner.sent.length));
  if (inner.sent.length === 1) {
    check(`${kind}: retry ORIGIN_MESSAGE_ID = UNIQ_KEY`,
      inner.sent[0].getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID) === UNIQ);
  }
}
{
  const c: any = new DefaultMQPushConsumer(GROUP);
  const { client, bounces } = fakeClient(ResponseCode.SUCCESS);
  c.mqClient = client;
  await c.sendMessageBack(consumed(false), 0);
  check('push without UNIQ_KEY: header falls back to offset id', bounces[0]?.originMsgId === OFFSET,
    String(bounces[0]?.originMsgId));
}

console.log(`\n${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
