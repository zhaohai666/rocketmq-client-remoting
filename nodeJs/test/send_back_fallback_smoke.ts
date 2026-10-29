// Send-back fallback on both consumer facades — offline, with the wire request
// and the internal producer's sends RECORDED instead of dialed.
//
//   node --experimental-strip-types test/send_back_fallback_smoke.ts
//
// A refused bounce (a non-success answer, a transport error, a missing master)
// must not lose the redelivery: it degrades to a normal send to %RETRY%<group>
// through the internal producer. The legs pinned here are:
//
//   1. the shared retry copy carries body/flag, RETRY_TOPIC, RECONSUME_TIME,
//      MAX_RECONSUME_TIMES, ORIGIN_MESSAGE_ID and DELAY — and DROPS the
//      half-message marker WITHOUT touching the source message;
//   2. push: a refused answer, a transport error and a missing master each fall
//      back to exactly one retry send, while the default -1 reports 16 on the
//      wire and the RPC keeps its 5000 ms budget;
//   3. push: when the fallback send fails too, the ORIGINAL error reaches the
//      caller;
//   4. the orderly path sends the same retry copy (no half-message marker on
//      either the copy or the source) and reports success only for a confirmed
//      send;
//   5. pull: a refused answer falls back with a 3000 ms RPC and 16 as the
//      default ceiling, and the maxReconsumeTimes knob moves what the request
//      carries (5 stays 5, -1 reports 16);
//   6. pull shutdown tears the lazily built fallback producer down.
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { buildRetryMessage } from '../src/client/retry_message.ts';
import { Message, MessageExt } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const GROUP = 'GID_NodeJsSendBackFallback';
const TOPIC = 'NodeJsSendBackFallbackTopic';
const BROKER = 'broker-a';
const ADDR = '127.0.0.1:10911';
const MSG_ID = '0A0B0C0D0000000000000000000EA7';

interface RecordedBounce {
  addr: string;
  code: number;
  header: any;
  timeout: number;
}

// fakeClient answers every CONSUMER_SEND_MSG_BACK with `answer` (a code, or an
// Error to throw as a transport failure) and records the request as it was sent.
function fakeClient(opts: { addr?: string | null; answer?: number | Error } = {}) {
  const bounces: RecordedBounce[] = [];
  const client = {
    clientId: 'fake-client-id',
    findBrokerAddressInPublish(_broker: string) {
      return opts.addr === undefined ? ADDR : opts.addr;
    },
    vipChannel(addr: string) { return addr; },
    remotingClient: {
      async invokeSync(addr: string, request: any, timeout: number) {
        bounces.push({ addr, code: request.code, header: request.customHeader, timeout });
        const answer = opts.answer === undefined ? ResponseCode.SUCCESS : opts.answer;
        if (answer instanceof Error) throw answer;
        return { code: answer, remark: answer === ResponseCode.SUCCESS ? '' : 'scripted refusal' };
      },
    },
    async unregisterClientAllBrokers(_addr: string, _group: string) { /* no wire */ },
    unregisterConsumer(_group: string) { /* no wire */ },
  };
  return { client, bounces };
}

// fakeProducer stands in for the internal producer: it records each normal send
// and can be told to fail (proving the original error survives the fallback).
function fakeProducer(opts: { fail?: boolean } = {}) {
  const sent: Message[] = [];
  let shutdowns = 0;
  const producer = {
    async sendBySelector(msg: Message, _selector?: unknown) {
      if (opts.fail) throw new Error('inner producer send failed');
      sent.push(msg);
    },
    async shutdown() { shutdowns++; },
  };
  return { producer, sent, shutdowns: () => shutdowns };
}

function message(reconsumeTimes = 0): MessageExt {
  const m = new MessageExt(TOPIC, Buffer.from('payload'), 'TagA', 'key-1', 7);
  m.brokerName = BROKER;
  m.queueId = 0;
  m.queueOffset = 7;
  m.commitLogOffset = 123456;
  m.msgId = MSG_ID;
  m.reconsumeTimes = reconsumeTimes;
  // A half-message marker the retry copy must not inherit.
  m.putProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED, 'true');
  return m;
}

// checkRetryCopy pins the whole retry-message contract in one place.
function checkRetryCopy(label: string, copy: Message, maxReconsume: number, group = GROUP): void {
  const p = copy.getProperties();
  check(`${label}: topic is %RETRY%<group>`, copy.getTopic() === MixAll.getRetryTopic(group), copy.getTopic());
  check(`${label}: RETRY_TOPIC keeps the origin topic`,
    p[MessageConst.PROPERTY_RETRY_TOPIC] === TOPIC, p[MessageConst.PROPERTY_RETRY_TOPIC]);
  check(`${label}: RECONSUME_TIME = 1`,
    p[MessageConst.PROPERTY_RECONSUME_TIME] === '1', p[MessageConst.PROPERTY_RECONSUME_TIME]);
  check(`${label}: MAX_RECONSUME_TIMES = ${maxReconsume}`,
    p[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] === String(maxReconsume),
    p[MessageConst.PROPERTY_MAX_RECONSUME_TIMES]);
  check(`${label}: ORIGIN_MESSAGE_ID carries the message id`,
    p[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID] === MSG_ID, p[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID]);
  check(`${label}: DELAY = 3`, p['DELAY'] === '3', p['DELAY']);
  check(`${label}: half-message marker dropped`,
    p[MessageConst.PROPERTY_TRANSACTION_PREPARED] === undefined,
    p[MessageConst.PROPERTY_TRANSACTION_PREPARED]);
  check(`${label}: body copied`, copy.getBody().toString() === 'payload', copy.getBody().toString());
  check(`${label}: flag copied`, copy.getFlag() === 7, String(copy.getFlag()));
}

console.log('== retry copy shape ==');
{
  const src = message();
  const copy = buildRetryMessage(GROUP, src, 16);
  checkRetryCopy('builder', copy, 16);
  check('builder: source keeps its half-message marker',
    src.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED) === 'true',
    String(src.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED)));
  check('builder: source topic untouched', src.getTopic() === TOPIC, src.getTopic());
}

console.log('== push: refused bounce falls back ==');
{
  const c = new DefaultMQPushConsumer(GROUP);
  const { client, bounces } = fakeClient({ answer: ResponseCode.SYSTEM_ERROR });
  const inner = fakeProducer();
  c.mqClient = client as any;
  (c as any)._producer = inner.producer;

  await c.sendMessageBack(message(), 3);
  check('push refused: exactly one bounce attempted', bounces.length === 1, String(bounces.length));
  check('push refused: request is CONSUMER_SEND_MSG_BACK',
    bounces[0].code === RequestCode.CONSUMER_SEND_MSG_BACK, String(bounces[0].code));
  check('push refused: default -1 reports 16 on the wire',
    bounces[0].header.maxReconsumeTimes === 16, String(bounces[0].header.maxReconsumeTimes));
  check('push refused: delayLevel passed through', bounces[0].header.delayLevel === 3,
    String(bounces[0].header.delayLevel));
  check('push refused: originMessageId is the message id',
    bounces[0].header.originMsgId === MSG_ID, String(bounces[0].header.originMsgId));
  check('push refused: RPC budget stays 5000 ms', bounces[0].timeout === 5000, String(bounces[0].timeout));
  check('push refused: one retry send', inner.sent.length === 1, String(inner.sent.length));
  if (inner.sent.length === 1) checkRetryCopy('push refused retry', inner.sent[0], 16);
}

console.log('== push: transport error falls back ==');
{
  const c = new DefaultMQPushConsumer(GROUP);
  const { client } = fakeClient({ answer: new Error('socket closed') });
  const inner = fakeProducer();
  c.mqClient = client as any;
  (c as any)._producer = inner.producer;

  await c.sendMessageBack(message(), 0);
  check('push transport: one retry send', inner.sent.length === 1, String(inner.sent.length));
  if (inner.sent.length === 1) checkRetryCopy('push transport retry', inner.sent[0], 16);
}

console.log('== push: missing master falls back ==');
{
  const c = new DefaultMQPushConsumer(GROUP);
  const { client, bounces } = fakeClient({ addr: null });
  const inner = fakeProducer();
  c.mqClient = client as any;
  (c as any)._producer = inner.producer;

  await c.sendMessageBack(message(), 0);
  check('push missing master: no RPC attempted', bounces.length === 0, String(bounces.length));
  check('push missing master: one retry send', inner.sent.length === 1, String(inner.sent.length));
  if (inner.sent.length === 1) checkRetryCopy('push missing-master retry', inner.sent[0], 16);
}

console.log('== push: a failing fallback rethrows the ORIGINAL error ==');
{
  const c = new DefaultMQPushConsumer(GROUP);
  const { client } = fakeClient({ answer: ResponseCode.SYSTEM_ERROR });
  c.mqClient = client as any;
  (c as any)._producer = fakeProducer({ fail: true }).producer;

  let caught: Error | null = null;
  try { await c.sendMessageBack(message(), 0); } catch (e) { caught = e as Error; }
  check('push double failure: rejects', caught !== null);
  check('push double failure: original broker error survives',
    caught !== null && caught.message.includes('CODE 1') && !caught.message.includes('inner producer send failed'),
    caught ? caught.message : 'no error');
}

console.log('== push: orderly send-back uses the same retry copy ==');
{
  const c = new DefaultMQPushConsumer(GROUP);
  const inner = fakeProducer();
  (c as any)._producer = inner.producer;

  const src = message();
  const ok = await (c as any)._doOrderlySendMessageBack(src);
  check('orderly: confirmed send reports true', ok === true);
  check('orderly: one retry send', inner.sent.length === 1, String(inner.sent.length));
  if (inner.sent.length === 1) checkRetryCopy('orderly retry', inner.sent[0], 2147483647);
  check('orderly: source keeps its half-message marker',
    src.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED) === 'true',
    String(src.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED)));

  const failing = new DefaultMQPushConsumer(GROUP);
  (failing as any)._producer = fakeProducer({ fail: true }).producer;
  const failed = await (failing as any)._doOrderlySendMessageBack(message());
  check('orderly: a failing send reports false', failed === false);
}

console.log('== pull: refused bounce falls back ==');
{
  const c = new DefaultMQPullConsumer(GROUP);
  const { client, bounces } = fakeClient({ answer: ResponseCode.SYSTEM_ERROR });
  const inner = fakeProducer();
  c.mqClient = client as any;
  (c as any)._producer = inner.producer;

  await c.sendMessageBack(message(), 3);
  check('pull refused: exactly one bounce attempted', bounces.length === 1, String(bounces.length));
  check('pull refused: request is CONSUMER_SEND_MSG_BACK',
    bounces[0].code === RequestCode.CONSUMER_SEND_MSG_BACK, String(bounces[0].code));
  check('pull refused: RPC budget is 3000 ms', bounces[0].timeout === 3000, String(bounces[0].timeout));
  check('pull refused: default ceiling 16', bounces[0].header.maxReconsumeTimes === 16,
    String(bounces[0].header.maxReconsumeTimes));
  check('pull refused: one retry send', inner.sent.length === 1, String(inner.sent.length));
  if (inner.sent.length === 1) checkRetryCopy('pull refused retry', inner.sent[0], 16);
}

console.log('== pull: maxReconsumeTimes knob ==');
{
  const c = new DefaultMQPullConsumer(GROUP);
  check('pull knob: default is 16', c.maxReconsumeTimes === 16, String(c.maxReconsumeTimes));
  c.setMaxReconsumeTimes(5);
  const { client, bounces } = fakeClient({ answer: ResponseCode.SYSTEM_ERROR });
  const inner = fakeProducer();
  c.mqClient = client as any;
  (c as any)._producer = inner.producer;
  await c.sendMessageBack(message(), 0);
  check('pull knob 5: request carries 5', bounces[0].header.maxReconsumeTimes === 5,
    String(bounces[0].header.maxReconsumeTimes));
  if (inner.sent.length === 1) checkRetryCopy('pull knob 5 retry', inner.sent[0], 5);

  const unlimited = new DefaultMQPullConsumer(GROUP + '_unlimited');
  unlimited.setMaxReconsumeTimes(-1);
  const u = fakeClient({ answer: ResponseCode.SYSTEM_ERROR });
  const uInner = fakeProducer();
  unlimited.mqClient = u.client as any;
  (unlimited as any)._producer = uInner.producer;
  await unlimited.sendMessageBack(message(), 0);
  check('pull knob -1: request reports 16', u.bounces[0].header.maxReconsumeTimes === 16,
    String(u.bounces[0].header.maxReconsumeTimes));
  if (uInner.sent.length === 1) checkRetryCopy('pull knob -1 retry', uInner.sent[0], 16, GROUP + '_unlimited');
}

console.log('== pull: missing master falls back ==');
{
  const c = new DefaultMQPullConsumer(GROUP);
  const { client, bounces } = fakeClient({ addr: null });
  const inner = fakeProducer();
  c.mqClient = client as any;
  (c as any)._producer = inner.producer;
  await c.sendMessageBack(message(), 0);
  check('pull missing master: no RPC attempted', bounces.length === 0, String(bounces.length));
  check('pull missing master: one retry send', inner.sent.length === 1, String(inner.sent.length));
}

console.log('== pull: shutdown tears the fallback producer down ==');
{
  const c = new DefaultMQPullConsumer(GROUP);
  const { client } = fakeClient();
  const inner = fakeProducer();
  c.mqClient = client as any;
  c.started = true;
  (c as any)._producer = inner.producer;
  await c.shutdown();
  check('pull shutdown: producer shut down once', inner.shutdowns() === 1, String(inner.shutdowns()));
  check('pull shutdown: producer reference cleared', (c as any)._producer === null);
  check('pull shutdown: consumer no longer started', c.started === false);
}

console.log(`\n${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
