// -*- coding: utf-8 -*-
// Auto-batch, sync-send retry, beat-period knobs and the transaction check-back
// hooks — offline tests of the producer send path against a recording fake broker.
//
// Behaviour baseline (what these tests hold the client to):
//   * autoBatch groups small messages by (topic, queue, waitStoreMsgOK, tag) and
//     the group leaves as ONE packed batch request; each caller still gets its
//     OWN SendResult, keyed by the member's UNIQ_KEY stamped before it joined,
//     and the queue offset it reports is the group's answer advanced by the
//     position the member holds inside the group;
//   * a group that crosses batchMaxBytes leaves at once, otherwise the
//     batchMaxDelayMs hold timer drains it; -1 keeps the built-in default;
//   * an unbatchable message (batch-in-batch, retry topic, prepared transaction,
//     delay/timer delivery, a body over the hold size) is sent on its own;
//   * a caller that passes an explicit timeout is never held for a group;
//   * shutting down flushes what is still buffered, and a stopped buffer takes
//     no new traffic (a group nobody will ever drain must not be entered);
//   * SYNC retry: a broker answer inside retryResponseCodes is worth another
//     attempt AWAY from that broker, an answer outside it is surfaced on the
//     first attempt, a transport failure is always retried, and when every
//     attempt is rejected by a broker the caller sees that broker's own answer;
//   * a stored-but-not-SEND_OK answer comes back as it is unless
//     retryAnotherBrokerWhenNotStoreOK asks for another broker (SYNC only);
//   * heartbeatIntervalMillis / pollNameServerIntervalMillis are the real periods
//     of the two instance loops, and a running loop is rebuilt for a new period;
//   * a transaction check-back runs the EndTransaction hooks around the
//     END_TRANSACTION oneway, and with no hook registered the commit still goes out.
//
// Run: node --experimental-strip-types test/auto_batch_and_retry_smoke.ts
import { MQClient } from '../src/client/mq_client.ts';
import { MQClientManager } from '../src/client/mq_client_manager.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { SendStatus, SendResult } from '../src/client/send_result.ts';
import { ProduceAccumulator, getOrCreateProduceAccumulator, splitSendResults,
  DEFAULT_HOLD_SIZE } from '../src/client/produce_accumulator.ts';
import { MQBrokerException, RemotingTimeoutException } from '../src/remoting/exception.ts';
import { Message, MessageBatch, MessageExt, MessageQueue } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { MessageAccessor } from '../src/common/message_accessor.ts';
import { MessageSysFlag } from '../src/common/sysflag.ts';
import { TopicRouteData, QueueData, BrokerData } from '../src/remoting/route.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { SendMessageRequestHeaderV2, SendMessageResponseHeader } from '../src/remoting/headers.ts';
import { decodeBatchMessages, encodeMessageExt, string2MessageProperties } from '../src/common/messageDecoder.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { EndTransactionHook, hookRegistry } from '../src/client/hook.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = ''): void {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

// Polls for recorded state. The deadline only bounds the wait; every assertion
// still inspects what was actually recorded.
async function until(cond: () => boolean, timeoutMs = 2000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (cond()) return true;
    await new Promise((r) => setTimeout(r, 5));
  }
  return cond();
}

function throwsOn(fn: () => void): boolean {
  try { fn(); return false; } catch (e) { return true; }
}

const NS_ADDR = '127.0.0.1:19200';
const ADDR_A = '127.0.0.1:19100';
const ADDR_B = '127.0.0.1:19101';

// One send frame the fake broker was asked to store.
type Frame = {
  addr: string;
  code: number;
  topic: string;
  brokerName: string;
  queueId: number;
  batch: boolean;
  properties: Record<string, string>;
  members: number;
  bodies: string[];
  uniqKeys: string[];
  attempt: number;
};
type Answer = { code: number; remark?: string; queueOffset?: number };
type QueueSpec = { broker: string; addr: string; queues: number };

let harnessSeq = 0;

// The whole transport the client sees: send frames are decoded and recorded the
// way a broker would, and the answer comes from the per-test responder.
class Harness {
  clientId: string;
  client: MQClient;
  producer: DefaultMQProducer;
  frames: Frame[] = [];
  beats: number[] = [];
  routes: string[] = [];
  oneway: RemotingCommand[] = [];
  trace: string[] = [];
  nsCode: number = ResponseCode.TOPIC_NOT_EXIST;
  responder: (frame: Frame) => Answer = () => ({ code: ResponseCode.SUCCESS });

  constructor(topic: string, queues: QueueSpec[], group: string,
    tune?: (p: DefaultMQProducer) => void) {
    const producer = new DefaultMQProducer(group);
    producer.setInstanceName(`node-wave1-${++harnessSeq}`);
    if (tune != null) tune(producer);
    // The instance is registered under the id the facade itself computes, so
    // start() adopts this harness instead of building a real transport.
    this.clientId = (producer as any)._buildClientId() as string;
    this.producer = producer;
    this.producer.setNamesrvAddr(NS_ADDR);

    const remoting: any = {
      rpcHooks: [],
      registerProcessor: (): void => { /* reverse requests are dispatched by hand here */ },
      unregisterProcessor: (): void => { /* ditto */ },
      shutdown: (): void => { /* no sockets to close */ },
      invokeOneway: (_addr: string, request: RemotingCommand): Promise<void> => {
        this.trace.push('oneway');
        this.oneway.push(request);
        return Promise.resolve();
      },
      invokeAsync: (addr: string, request: RemotingCommand, cb: any, timeout: number): void => {
        void timeout;
        // Answered off-loop, like a real response arriving on a later tick.
        setTimeout(() => {
          try { cb(this.dispatch(addr, request), null); } catch (e) { cb(null, e as Error); }
        }, 0);
      },
      invokeSync: async (addr: string, request: RemotingCommand, _timeout: number): Promise<RemotingCommand> =>
        this.dispatch(addr, request),
    };
    this.client = new MQClient(this.clientId, NS_ADDR, remoting);

    const addrs = new Map<string, string>();
    for (const q of queues) addrs.set(q.broker, q.addr);
    for (const [broker, addr] of addrs) {
      this.client.brokerAddrTable.set(broker, { [MixAll.MASTER_ID]: addr });
    }
    const trd = new TopicRouteData();
    trd.brokerDatas = [...addrs.entries()].map(([broker, addr]) =>
      new BrokerData(broker, broker, { [MixAll.MASTER_ID]: addr }));
    trd.queueDatas = queues.map((q) => new QueueData(q.broker, q.queues, q.queues, 6, 0));
    this.client.topicPublishInfoTable.set(topic, this.client.topicRouteData2TopicPublishInfo(topic, trd));
    MQClientManager.getInstance().getOrCreateMQClientInstance(this.clientId, () => this.client);
  }

  async start(): Promise<DefaultMQProducer> {
    await this.producer.start();
    return this.producer;
  }

  accumulator(): ProduceAccumulator { return getOrCreateProduceAccumulator(this.clientId); }

  private dispatch(addr: string, request: RemotingCommand): RemotingCommand {
    const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
    response.opaque = request.opaque;
    if (request.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      this.trace.push('route');
      this.routes.push(String(request.extFields?.['topic'] ?? ''));
      response.code = this.nsCode;
      response.body = Buffer.alloc(0);
      return response;
    }
    if (request.code === RequestCode.HEART_BEAT) {
      this.beats.push(Date.now());
      return response;
    }
    if (request.code !== RequestCode.SEND_MESSAGE_V2 && request.code !== RequestCode.SEND_BATCH_MESSAGE) {
      return response;
    }
    const frame = this.decodeSend(addr, request);
    this.frames.push(frame);
    this.trace.push('send');
    const answer = this.responder(frame);
    const stored = answer.code === ResponseCode.SUCCESS ||
      answer.code === ResponseCode.FLUSH_DISK_TIMEOUT ||
      answer.code === ResponseCode.FLUSH_SLAVE_TIMEOUT ||
      answer.code === ResponseCode.SLAVE_NOT_AVAILABLE;
    if (!stored) {
      response.code = answer.code;
      response.remark = answer.remark ?? `code ${answer.code}`;
      return response;
    }
    // A stored answer carries the SendStatus in the RESPONSE CODE itself: the
    // four codes the broker uses for "accepted, with a store caveat" all come
    // back with the normal send header.
    response.code = answer.code;
    const header = new SendMessageResponseHeader();
    header.msgId = `AC100001000012340000000000${String(1000 + frame.attempt)}`;
    header.queueId = frame.queueId;
    header.queueOffset = answer.queueOffset ?? 4000 + frame.attempt;
    header.transactionId = '';
    header.batchUniqId = frame.properties[MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX] ?? '';
    response.extFields = header.toExtFields();
    return response;
  }

  private decodeSend(addr: string, request: RemotingCommand): Frame {
    // What the transport does before the bytes leave: the header object is only
    // flattened into extFields here, not when the command was built.
    request.makeCustomHeaderToNet();
    const v2 = new SendMessageRequestHeaderV2();
    v2.fromExtFields(request.extFields);
    const v1 = v2.toV1();
    const body = (request.body as Buffer) ?? Buffer.alloc(0);
    const packed = request.code === RequestCode.SEND_BATCH_MESSAGE;
    // A packed body is the lightweight per-member framing, not the 17-field
    // stored format a consumer reads.
    const members = packed ? decodeBatchMessages(body) : [];
    const properties = string2MessageProperties(v1.properties);
    return {
      addr,
      code: request.code,
      topic: String(v1.topic),
      brokerName: String(v1.brokerName),
      queueId: Number(v1.queueId),
      batch: v1.batch === true,
      properties,
      members: packed ? members.length : 1,
      bodies: packed ? members.map((m) => m.getBody().toString()) : [body.toString()],
      uniqKeys: packed
        ? members.map((m) => String(m.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)))
        : [String(properties[MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX] ?? '')],
      attempt: this.frames.length + 1,
    };
  }

  counts(): string { return JSON.stringify(this.frames.map((f) => [f.code, f.members, f.addr])); }
}

function msgWithId(topic: string, body: string, tag: string | null, uniq: string): Message {
  const m = new Message(topic, Buffer.from(body, 'utf8'), tag ?? undefined);
  MessageAccessor.putProperty(m, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniq);
  return m;
}

const ONE_BROKER: QueueSpec[] = [{ broker: 'brokerB', addr: ADDR_B, queues: 1 }];
const TWO_BROKERS: QueueSpec[] = [
  { broker: 'brokerA', addr: ADDR_A, queues: 2 },
  { broker: 'brokerB', addr: ADDR_B, queues: 1 },
];

async function main(): Promise<void> {
  // The fake transport opens no socket, so nothing ref'd would keep the loop
  // alive across a `await send()` that only settles on the (unref'd, like every
  // background timer in this port) flush daemon. A real started producer always
  // has its broker connection holding the process; this stands in for it.
  const keepAlive = setInterval(() => { /* holds the event loop */ }, 1000);

  // ============================== 1. the accumulation rules
  console.log('== accumulation rules ==');
  {
    const mq = new MessageQueue('AccTopic', 'brokerA', 0);
    const acc = new ProduceAccumulator('acc-matrix', 1024, 10);
    const plain = new Message('AccTopic', Buffer.from('a'));
    check('a plain message is batchable', acc.isBatchable(plain) === true);
    check('a batch is never packed into a batch', acc.isBatchable(new MessageBatch([plain])) === false);
    check('a retry-topic message is not batched',
      acc.isBatchable(new Message('%RETRY%GID_x', Buffer.from('a'))) === false);
    const delayed = new Message('AccTopic', Buffer.from('a'));
    delayed.setDelayTimeLevel(3);
    check('a delayed message keeps its own schedule', acc.isBatchable(delayed) === false);
    for (const prop of [MessageConst.PROPERTY_TIMER_DELAY_SEC,
      MessageConst.PROPERTY_TIMER_DELAY_MS, MessageConst.PROPERTY_TIMER_DELIVER_MS]) {
      const m = new Message('AccTopic', Buffer.from('a'));
      MessageAccessor.putProperty(m, prop, '10');
      check(`a ${prop} message keeps its own timer`, acc.isBatchable(m) === false);
    }
    const prepared = new Message('AccTopic', Buffer.from('a'));
    MessageAccessor.putProperty(prepared, MessageConst.PROPERTY_TRANSACTION_PREPARED, 'true');
    check('a prepared half message stays individually addressable', acc.isBatchable(prepared) === false);
    const fat = new Message('AccTopic', Buffer.from('x'.repeat(1025)));
    check('a body over the hold size cannot fit a group', acc.isBatchable(fat) === false);
    check('the same body fits once the hold size grows',
      new ProduceAccumulator('acc-wide', 4096, 10).isBatchable(fat) === true);

    acc.accumulate(new Message('AccTopic', Buffer.from('a')), mq);
    acc.accumulate(new Message('AccTopic', Buffer.from('b')), mq);
    check('two messages of one key share a group', acc.size() === 1);
    acc.accumulate(msgWithId('AccTopic', 'c', 'tagB', 'UNIQ-TAG-B'), mq);
    check('a different tag is a different group', acc.size() === 2);
    const otherStore = new Message('AccTopic', Buffer.from('d'));
    otherStore.setWaitStoreMsgOK(false);
    acc.accumulate(otherStore, mq);
    check('a different waitStoreMsgOK is a different group', acc.size() === 3);
    acc.accumulate(new Message('AccTopic', Buffer.from('e')), new MessageQueue('AccTopic', 'brokerA', 1));
    check('a different queue is a different group', acc.size() === 4);

    const gated = new ProduceAccumulator('acc-gate', 100, 10);
    const g1 = gated.accumulate(new Message('AccTopic', Buffer.from('x'.repeat(60))), mq);
    const g2 = gated.accumulate(new Message('AccTopic', Buffer.from('y'.repeat(60))), mq);
    check('crossing the hold size hands back the finished group',
      g2.flushNow != null && g2.flushNow.count === 2 && gated.size() === 0,
      `${g2.flushNow?.count} / keys ${gated.size()}`);
    const g3 = gated.accumulate(new Message('AccTopic', Buffer.from('z')), mq);
    check('a group below the gate keeps waiting for the hold timer',
      g1.flushNow === null && g3.flushNow === null);

    check('batchMaxBytes below 1 is refused', throwsOn(() => acc.setMaxHoldSize(0)));
    check('batchMaxBytes over 2MB is refused', throwsOn(() => acc.setMaxHoldSize(2 * 1024 * 1024 + 1)));
    check('batchMaxDelayMs below 1 is refused', throwsOn(() => acc.setHoldMs(0)));
    check('batchMaxDelayMs over 30s is refused', throwsOn(() => acc.setHoldMs(30001)));
    acc.setMaxHoldSize(2048);
    acc.setHoldMs(20);
    check('both knobs keep the accepted value', acc.maxHoldSize === 2048 && acc.holdMs === 20);

    const stranded = new ProduceAccumulator('acc-strand', 1024, 10);
    const pending = stranded.accumulate(new Message('AccTopic', Buffer.from('a')), mq);
    stranded.stop();
    let strandedErr: Error | null = null;
    await pending.future.promise.catch((e: Error) => { strandedErr = e; });
    check('a buffered group with no sender fails its caller instead of hanging',
      strandedErr instanceof Error && stranded.size() === 0, String(strandedErr));
    check('a stopped accumulator refuses new members',
      throwsOn(() => stranded.accumulate(new Message('AccTopic', Buffer.from('b')), mq)));
    stranded.start();
    check('and takes traffic again once it is started',
      stranded.isStopped() === false &&
      stranded.accumulate(new Message('AccTopic', Buffer.from('c')), mq).future.promise instanceof Promise);
  }

  // ============================== 2. autoBatch on the wire
  console.log('== auto-batch send path ==');
  {
    const h = new Harness('BatchTopic', ONE_BROKER, 'GID_Batch', (p) => {
      p.setAutoBatch(true);
      p.setBatchMaxBytes(10 * 1024);
      p.setBatchMaxDelayMs(50);
    });
    const p = await h.start();
    check('the batch knobs reach the live accumulator',
      p.getBatchMaxBytes() === 10 * 1024 && p.getBatchMaxDelayMs() === 50);

    const results = await Promise.all([
      p.send(msgWithId('BatchTopic', 'body-one', 'tagA', 'UNIQ-M1')),
      p.send(msgWithId('BatchTopic', 'body-two', 'tagA', 'UNIQ-M2')),
      p.send(msgWithId('BatchTopic', 'body-three', 'tagA', 'UNIQ-M3')),
    ]);
    check('three sends of one key leave as one frame', h.frames.length === 1, h.counts());
    const frame = h.frames[0];
    check('the group leaves as a batch request',
      frame.code === RequestCode.SEND_BATCH_MESSAGE && frame.batch === true);
    check('the packed body holds all three members in order',
      frame.members === 3 && frame.bodies.join('|') === 'body-one|body-two|body-three',
      frame.bodies.join('|'));
    check('every member keeps its own UNIQ_KEY inside the group',
      frame.uniqKeys.join(',') === 'UNIQ-M1,UNIQ-M2,UNIQ-M3', frame.uniqKeys.join(','));
    check('each caller gets its OWN message id, not the batch answer',
      results.map((r) => r.msgId).join(',') === 'UNIQ-M1,UNIQ-M2,UNIQ-M3',
      results.map((r) => r.msgId).join(','));
    const envelopeId = frame.properties[MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX];
    check('and none of them is the envelope id',
      envelopeId != null && envelopeId.length > 0 && results.every((r) => r.msgId !== envelopeId),
      String(envelopeId));
    const base = results[0].queueOffset;
    check('the group shares one queue and one store answer',
      results.every((r) => r.sendStatus === SendStatus.SEND_OK && r.messageQueue != null &&
        r.messageQueue.getBrokerName() === 'brokerB' && r.offsetMsgId === results[0].offsetMsgId) &&
        base === 4000 + frame.attempt,
      JSON.stringify(results.map((r) => [r.queueOffset, r.offsetMsgId])));
    check('each member reports the offset it really holds inside the group',
      results.every((r, i) => r.queueOffset === base + i),
      JSON.stringify(results.map((r) => r.queueOffset)));
    const two = splitSendResults(new SendResult(SendStatus.SEND_OK, 'B1,B2', null, 90, '', 'O1,O2'),
      [msgWithId('BatchTopic', 'a', 'tagQ', 'UNIQ-Q1'), msgWithId('BatchTopic', 'b', 'tagQ', 'UNIQ-Q2')]);
    check('an answer that already carries one id per member is matched member by member',
      two.every((r, i) => r.msgId === `B${i + 1}` && r.offsetMsgId === `O${i + 1}`
        && r.queueOffset === 90 + i) && two[0].msgId !== 'UNIQ-Q1',
      JSON.stringify(two.map((r) => [r.msgId, r.offsetMsgId, r.queueOffset])));
    const measured = splitSendResults(
      new SendResult(SendStatus.SEND_OK, 'ENVELOPE', null, 90, '', 'O1,O2,O3'),
      [msgWithId('BatchTopic', 'a', 'tagR', 'UNIQ-R1'), msgWithId('BatchTopic', 'b', 'tagR', 'UNIQ-R2'),
        msgWithId('BatchTopic', 'c', 'tagR', 'UNIQ-R3')]);
    check('a group answer with only the offset list split is still per member, ids and offsets alike',
      measured.every((r, i) => r.msgId === `UNIQ-R${i + 1}` && r.offsetMsgId === `O${i + 1}`
        && r.queueOffset === 90 + i),
      JSON.stringify(measured.map((r) => [r.msgId, r.offsetMsgId, r.queueOffset])));

    h.frames.length = 0;
    const mixed = await Promise.all([p.send(msgWithId('BatchTopic', 'x', 'tagA', 'UNIQ-TA')),
      p.send(msgWithId('BatchTopic', 'x', 'tagB', 'UNIQ-TB'))]);
    check('two tags go out as two groups',
      h.frames.length === 2 && h.frames.every((f) => f.members === 1), h.counts());
    check('each group still settles its own caller',
      mixed[0].msgId === 'UNIQ-TA' && mixed[1].msgId === 'UNIQ-TB',
      mixed.map((r) => r.msgId).join(','));

    h.frames.length = 0;
    const both = await Promise.all([p.send(msgWithId('BatchTopic', 'b1', 'tagC', 'UNIQ-C1')),
      p.send(msgWithId('BatchTopic', 'b2', 'tagC', 'UNIQ-C2'), 3000)]);
    check('a send with its own timeout is not held for a group',
      h.frames.length === 2 &&
      h.frames.some((f) => f.code === RequestCode.SEND_MESSAGE_V2 && f.bodies[0] === 'b2') &&
      h.frames.some((f) => f.code === RequestCode.SEND_BATCH_MESSAGE && f.bodies[0] === 'b1'),
      h.counts());
    check('the bounded send keeps its own result',
      both[0].msgId === 'UNIQ-C1' && both[1].msgId === 'UNIQ-C2');

    h.frames.length = 0;
    const late = msgWithId('BatchTopic', 'late', 'tagD', 'UNIQ-D1');
    late.setDelayTimeLevel(2);
    const open = p.send(msgWithId('BatchTopic', 'open', 'tagD', 'UNIQ-D2'));
    const direct = await p.send(late);
    check('a delayed message is sent on its own, not held for a group',
      h.frames.some((f) => f.code === RequestCode.SEND_MESSAGE_V2 && f.bodies[0] === 'late'), h.counts());
    check('the direct send reports its own id', direct.msgId === 'UNIQ-D1');
    await open;
    check('the group the delayed message skipped still leaves as a batch',
      h.frames.some((f) => f.code === RequestCode.SEND_BATCH_MESSAGE && f.bodies[0] === 'open'), h.counts());

    h.frames.length = 0;
    const got: string[] = [];
    await Promise.all([
      p.sendAsync(msgWithId('BatchTopic', 'a1', 'tagE', 'UNIQ-E1'), (sr, err) => {
        got.push(err == null ? String(sr?.msgId) : `err:${err.message}`);
      }),
      p.sendAsync(msgWithId('BatchTopic', 'a2', 'tagE', 'UNIQ-E2'), (sr, err) => {
        got.push(err == null ? String(sr?.msgId) : `err:${err.message}`);
      }),
    ]);
    check('two async sends of one key ride one batch',
      await until(() => got.length === 2 && h.frames.length === 1, 1500),
      `got ${got.join(',')} / ${h.counts()}`);
    check('each callback got its own result',
      got.includes('UNIQ-E1') && got.includes('UNIQ-E2') && h.frames[0].members === 2, got.join(','));

    h.frames.length = 0;
    const group = new MessageBatch([msgWithId('BatchTopic', 'g1', 'tagF', 'UNIQ-F1'),
      msgWithId('BatchTopic', 'g2', 'tagF', 'UNIQ-F2')]);
    const single = p.send(msgWithId('BatchTopic', 'g3', 'tagF', 'UNIQ-F3'));
    const groupResult = await p.send(group);
    const ownBatch = h.frames.find((f) => f.members === 2);
    check('a hand-built batch leaves on its own', ownBatch != null, h.counts());
    check('a hand-built batch reports the envelope id the broker echoed',
      ownBatch != null && groupResult.msgId ===
        ownBatch.properties[MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX],
      `${groupResult.msgId} vs ${ownBatch?.properties['UNIQ_KEY']}`);
    await single;
    check('a hand-built batch is never merged into a pending group',
      h.frames.length === 2 && h.frames.every((f) => f.code === RequestCode.SEND_BATCH_MESSAGE),
      h.counts());
    p.shutdown();
  }

  // ============================== 3. byte gate and shutdown drain
  console.log('== batch gates and shutdown ==');
  {
    const h = new Harness('GateTopic', ONE_BROKER, 'GID_Gate', (p) => {
      p.setAutoBatch(true);
      p.setBatchMaxBytes(150);
      // Long enough that only the byte gate can explain an early flush.
      p.setBatchMaxDelayMs(5000);
    });
    const p = await h.start();
    const began = Date.now();
    const gate = await Promise.all([
      p.send(new Message('GateTopic', Buffer.from('a'.repeat(100)))),
      p.send(new Message('GateTopic', Buffer.from('b'.repeat(100)))),
    ]);
    const waited = Date.now() - began;
    check('crossing batchMaxBytes flushes without waiting for the hold timer',
      h.frames.length === 1 && h.frames[0].members === 2 && waited < 1000,
      `waited ${waited}ms / ${h.counts()}`);
    check('both gated callers still get their own result',
      gate.length === 2 && gate.every((r) => r.sendStatus === SendStatus.SEND_OK && r.msgId.length > 0));

    h.frames.length = 0;
    const overGate = await p.send(new Message('GateTopic', Buffer.from('z'.repeat(400))));
    check('a body over the hold size bypasses the group',
      h.frames.length === 1 && h.frames[0].code === RequestCode.SEND_MESSAGE_V2 &&
      overGate.msgId.length > 0, h.counts());

    h.frames.length = 0;
    // The caller's own outcome is captured the moment it is created: a send that
    // settles while the drain is still being polled must never leave its
    // rejection sitting without a handler.
    let settled: any = null;
    let settledErr: Error | null = null;
    const buffered = p.send(new Message('GateTopic', Buffer.from('held')));
    const tracked: Promise<void> = buffered.then(
      (r) => { settled = r; },
      (e: Error) => { settledErr = e; },
    );
    check('a group inside both gates is held',
      await until(() => h.accumulator().size() === 1 && h.frames.length === 0, 500), h.counts());
    p.shutdown();
    check('shutdown flushes the buffered group',
      await until(() => h.frames.length === 1, 1500), h.counts());
    await Promise.race([tracked, new Promise((r) => setTimeout(r, 1500))]);
    check('and its caller is answered rather than left hanging',
      settled != null && settledErr == null && settled.sendStatus === SendStatus.SEND_OK,
      `${JSON.stringify(settled)} / ${settledErr && settledErr.message}`);
    check('the stopped buffer takes no new traffic', h.accumulator().isStopped() === true);
    let afterShutdown: any = null;
    try { await p.send(new Message('GateTopic', Buffer.from('after'))); } catch (e) { afterShutdown = e; }
    check('a send after shutdown fails loudly instead of joining a dead buffer',
      afterShutdown instanceof Error && /not started/.test(String(afterShutdown.message)),
      String(afterShutdown && afterShutdown.message));
    check('nothing more reached the broker', h.frames.length === 1, h.counts());
  }

  // ============================== 4. the switch itself
  console.log('== auto-batch off / on over a started producer ==');
  {
    const h = new Harness('OffTopic', ONE_BROKER, 'GID_Off', (p) => { p.setBatchMaxDelayMs(50); });
    const p = await h.start();
    check('autoBatch is off by default', p.isAutoBatch() === false);
    await Promise.all([p.send(new Message('OffTopic', Buffer.from('o1'))),
      p.send(new Message('OffTopic', Buffer.from('o2')))]);
    check('without the switch every send is its own request',
      h.frames.length === 2 && h.frames.every((f) => f.code === RequestCode.SEND_MESSAGE_V2), h.counts());
    p.setAutoBatch(true);
    check('the switch can be turned on over a started producer', p.isAutoBatch() === true);
    h.frames.length = 0;
    await Promise.all([p.send(msgWithId('OffTopic', 'n1', 'tagZ', 'UNIQ-N1')),
      p.send(msgWithId('OffTopic', 'n2', 'tagZ', 'UNIQ-N2'))]);
    check('and enabling it after start() takes effect at once',
      h.frames.length === 1 && h.frames[0].members === 2 &&
      h.frames[0].uniqKeys.join(',') === 'UNIQ-N1,UNIQ-N2', h.counts());
    check('the hold period set before start is applied when the switch turns on',
      h.accumulator().holdMs === 50 && h.accumulator().maxHoldSize === DEFAULT_HOLD_SIZE,
      `holdMs ${h.accumulator().holdMs}, maxHoldSize ${h.accumulator().maxHoldSize}`);
    p.shutdown();
  }

  // ============================== 5. retryable broker answers
  console.log('== sync send retry response codes ==');
  {
    const defaultSet = [ResponseCode.SYSTEM_ERROR, ResponseCode.SYSTEM_BUSY,
      ResponseCode.SERVICE_NOT_AVAILABLE, ResponseCode.NO_PERMISSION,
      ResponseCode.TOPIC_NOT_EXIST, ResponseCode.NO_BUYER_ID,
      ResponseCode.NOT_IN_CURRENT_UNIT, ResponseCode.GO_AWAY];
    const probe = new DefaultMQProducer('GID_CodeSet');
    check('the default set is the broker-could-not-take-it family',
      defaultSet.every((c) => probe.isRetryResponseCode(c)),
      defaultSet.filter((c) => !probe.isRetryResponseCode(c)).join(','));
    check('a rejected message is not retried by default',
      probe.isRetryResponseCode(ResponseCode.MESSAGE_ILLEGAL) === false);
    check('a missing answer code is never retryable',
      probe.isRetryResponseCode(null) === false && probe.isRetryResponseCode(undefined) === false);
    check('the getter hands back a copy', (() => {
      const got = probe.getRetryResponseCodes();
      got.add(ResponseCode.MESSAGE_ILLEGAL);
      return probe.isRetryResponseCode(ResponseCode.MESSAGE_ILLEGAL) === false;
    })());

    const h = new Harness('RetryTopic', TWO_BROKERS, 'GID_Retry', (p) => {
      p.setRetryTimesWhenSendFailed(2);
    });
    const p = await h.start();

    h.responder = (f) => (f.attempt === 1
      ? { code: ResponseCode.SYSTEM_ERROR, remark: 'broker busy' } : { code: ResponseCode.SUCCESS });
    const moved = await p.send(new Message('RetryTopic', Buffer.from('r1')));
    check('a retryable broker answer costs a second attempt',
      h.frames.length === 2 && moved.sendStatus === SendStatus.SEND_OK, h.counts());
    check('the retry leaves the broker that answered badly',
      h.frames[0].addr !== h.frames[1].addr && h.frames[0].brokerName !== h.frames[1].brokerName,
      `${h.frames[0].brokerName} -> ${h.frames[1].brokerName}`);

    h.frames.length = 0;
    h.responder = (f) => (f.attempt <= 2
      ? { code: ResponseCode.SYSTEM_BUSY, remark: 'system busy' } : { code: ResponseCode.SUCCESS });
    const third = await p.send(new Message('RetryTopic', Buffer.from('r2')));
    check('three attempts never repeat a broker',
      h.frames.length === 3 && third.sendStatus === SendStatus.SEND_OK &&
      h.frames[0].addr !== h.frames[1].addr && h.frames[1].addr !== h.frames[2].addr, h.counts());

    h.frames.length = 0;
    h.responder = () => ({ code: ResponseCode.MESSAGE_ILLEGAL, remark: 'body is illegal' });
    let surfaced: any = null;
    try { await p.send(new Message('RetryTopic', Buffer.from('bad'))); } catch (e) { surfaced = e; }
    check('a rejected message surfaces without burning the retry budget',
      surfaced instanceof MQBrokerException && h.frames.length === 1,
      `frames ${h.frames.length}, err ${surfaced && surfaced.constructor.name}`);
    check('the surfaced answer keeps the broker code and remark',
      surfaced instanceof MQBrokerException && surfaced.responseCode === ResponseCode.MESSAGE_ILLEGAL &&
      String(surfaced.message).includes('body is illegal'), String(surfaced && surfaced.message));

    h.frames.length = 0;
    h.responder = () => ({ code: ResponseCode.MESSAGE_ILLEGAL, remark: 'still illegal' });
    p.addRetryResponseCode(ResponseCode.MESSAGE_ILLEGAL);
    let rejected: any = null;
    try { await p.send(new Message('RetryTopic', Buffer.from('bad2'))); } catch (e) { rejected = e; }
    check('declaring a code retryable spends the budget on another broker',
      h.frames.length === 3 && rejected instanceof MQBrokerException &&
      h.frames[0].addr !== h.frames[1].addr && h.frames[1].addr !== h.frames[2].addr, h.counts());

    h.frames.length = 0;
    p.removeRetryResponseCode(ResponseCode.MESSAGE_ILLEGAL);
    let afterRemove: any = null;
    try { await p.send(new Message('RetryTopic', Buffer.from('bad3'))); } catch (e) { afterRemove = e; }
    check('removing a code restores the immediate failure',
      h.frames.length === 1 && afterRemove instanceof MQBrokerException, `frames ${h.frames.length}`);

    h.frames.length = 0;
    p.setRetryResponseCodes([ResponseCode.SYSTEM_ERROR]);
    check('the replacement set holds exactly what it was given',
      p.getRetryResponseCodes().size === 1 && p.isRetryResponseCode(ResponseCode.SYSTEM_ERROR) &&
      !p.isRetryResponseCode(ResponseCode.SYSTEM_BUSY));
    h.responder = () => ({ code: ResponseCode.SYSTEM_BUSY, remark: 'busy now' });
    let busy: any = null;
    try { await p.send(new Message('RetryTopic', Buffer.from('r3'))); } catch (e) { busy = e; }
    check('a code dropped from the set fails at once again',
      h.frames.length === 1 && busy instanceof MQBrokerException, `frames ${h.frames.length}`);

    h.frames.length = 0;
    h.responder = () => ({ code: ResponseCode.SYSTEM_ERROR, remark: 'always busy' });
    let exhausted: any = null;
    try { await p.send(new Message('RetryTopic', Buffer.from('r4'))); } catch (e) { exhausted = e; }
    check('when the retries run out the caller sees the broker answer, not a generic failure',
      exhausted instanceof MQBrokerException && exhausted.responseCode === ResponseCode.SYSTEM_ERROR &&
      h.frames.length === 3, `${exhausted && exhausted.message} / frames ${h.frames.length}`);

    h.frames.length = 0;
    p.setRetryResponseCodes([]);
    h.responder = (f) => {
      if (f.attempt <= 2) throw new RemotingTimeoutException('connect timed out');
      return { code: ResponseCode.SUCCESS };
    };
    const afterTimeout = await p.send(new Message('RetryTopic', Buffer.from('r5')));
    check('a transport failure retries even with an empty code set',
      h.frames.length === 3 && afterTimeout.sendStatus === SendStatus.SEND_OK, `frames ${h.frames.length}`);

    h.frames.length = 0;
    p.setRetryResponseCodes([ResponseCode.SYSTEM_ERROR]);
    h.responder = () => ({ code: ResponseCode.SYSTEM_BUSY, remark: 'busy' });
    let asyncErr: Error | null = null;
    await p.sendAsync(new Message('RetryTopic', Buffer.from('r6')), (sr, err) => {
      if (err != null) asyncErr = err;
    });
    check('the async chain does not consult the retryable set',
      (await until(() => asyncErr != null, 1500)) && h.frames.length === 1 &&
      asyncErr instanceof MQBrokerException,
      `frames ${h.frames.length}, err ${asyncErr && `${asyncErr.constructor.name}: ${asyncErr.message}`}`);
    p.shutdown();
  }

  // ============================== 6. stored, but not stored well
  console.log('== retryAnotherBrokerWhenNotStoreOK ==');
  {
    const h = new Harness('StoreTopic', TWO_BROKERS, 'GID_Store', (p) => {
      p.setRetryTimesWhenSendFailed(2);
    });
    const p = await h.start();
    check('the flag is off by default', p.retryAnotherBrokerWhenNotStoreOK === false);
    h.responder = () => ({ code: ResponseCode.FLUSH_DISK_TIMEOUT, queueOffset: 77 });
    const kept = await p.send(new Message('StoreTopic', Buffer.from('s1')));
    check('without the flag a lagged store answer is returned as it is',
      h.frames.length === 1 && kept.sendStatus === SendStatus.FLUSH_DISK_TIMEOUT && kept.queueOffset === 77,
      `${kept.sendStatus} / frames ${h.frames.length}`);

    h.frames.length = 0;
    p.setRetryAnotherBrokerWhenNotStoreOK(true);
    h.responder = (f) => (f.attempt === 1
      ? { code: ResponseCode.FLUSH_SLAVE_TIMEOUT, queueOffset: 78 }
      : { code: ResponseCode.SUCCESS, queueOffset: 90 });
    const movedOn = await p.send(new Message('StoreTopic', Buffer.from('s2')));
    check('with the flag a FLUSH_SLAVE_TIMEOUT answer moves to another broker',
      h.frames.length === 2 && movedOn.sendStatus === SendStatus.SEND_OK &&
      h.frames[0].addr !== h.frames[1].addr, h.counts());

    h.frames.length = 0;
    h.responder = () => ({ code: ResponseCode.SLAVE_NOT_AVAILABLE, queueOffset: 81 });
    const tail = await p.send(new Message('StoreTopic', Buffer.from('s3')));
    check('when no broker stores it well the last answer is returned, not thrown',
      h.frames.length === 3 && tail.sendStatus === SendStatus.SLAVE_NOT_AVAILABLE,
      `${tail.sendStatus} / frames ${h.frames.length}`);

    h.frames.length = 0;
    h.responder = () => ({ code: ResponseCode.FLUSH_DISK_TIMEOUT, queueOffset: 82 });
    let asyncStatus: number | null = null;
    await p.sendAsync(new Message('StoreTopic', Buffer.from('s4')), (sr) => {
      if (sr != null) asyncStatus = sr.sendStatus;
    });
    check('the async chain reports a lagged store without re-sending',
      (await until(() => asyncStatus != null && h.frames.length >= 1, 1500)) &&
      h.frames.length === 1 && asyncStatus === SendStatus.FLUSH_DISK_TIMEOUT,
      `frames ${h.frames.length}, status ${asyncStatus}`);
    p.shutdown();
  }

  // ============================== 7. beat period knobs
  console.log('== heartbeat / route poll periods ==');
  {
    const h = new Harness('BeatTopic', ONE_BROKER, 'GID_Beat');
    const p = await h.start();
    check('the facade hands its beat period to the instance',
      p.getHeartbeatIntervalMillis() === 30_000 && h.client.heartbeatIntervalMillis === 30_000);
    const atStart = h.beats.length;
    check('the early beat at start reaches the registered broker', atStart >= 1, String(atStart));

    h.client.setHeartbeatIntervalMillis(30);
    const fast = await until(() => h.beats.length >= atStart + 3, 1500);
    const span = h.beats[h.beats.length - 1] - h.beats[atStart];
    check('a shorter beat period really drives the loop', fast && span < 1000,
      `${h.beats.length - atStart} beats in ${span}ms`);
    const before = h.client._heartbeatTimer;
    h.client.setHeartbeatIntervalMillis(40);
    check('a running loop is rebuilt for the new period',
      h.client._heartbeatTimer !== before && h.client._heartbeatTimer != null);

    h.client.setHeartbeatIntervalMillis(30_000);
    const slowBaseline = h.beats.length;
    check('back at the default period no extra beat is squeezed in',
      (await until(() => h.beats.length >= slowBaseline + 3, 400)) === false);

    check('a zero or negative beat period is refused',
      throwsOn(() => h.client.setHeartbeatIntervalMillis(0)) &&
      throwsOn(() => h.client.setHeartbeatIntervalMillis(-1)) &&
      throwsOn(() => h.client.setHeartbeatIntervalMillis(Number.NaN)));
    h.client.setHeartbeatIntervalMillis(1234.9);
    check('the period is rounded down to whole milliseconds', h.client.heartbeatIntervalMillis === 1234);

    p.setHeartbeatIntervalMillis(2500);
    check('the facade knob moves the live instance loop',
      p.getHeartbeatIntervalMillis() === 2500 && h.client.heartbeatIntervalMillis === 2500);

    h.routes.length = 0;
    h.client.setPollNameServerIntervalMillis(30);
    check('the route period is kept on the instance', h.client.pollNameServerIntervalMillis === 30);
    const polled = await until(() => h.routes.length >= 2, 1500);
    check('the refresh loop asks the name server again on the short period',
      polled && h.routes.includes('BeatTopic'), JSON.stringify(h.routes));
    const routeBefore = h.client._routeTimer;
    h.client.setPollNameServerIntervalMillis(40);
    check('a running refresh loop is rebuilt for the new period',
      h.client._routeTimer !== routeBefore && h.client._routeTimer != null);
    check('a zero route poll period is refused',
      throwsOn(() => h.client.setPollNameServerIntervalMillis(0)));
    h.client.setPollNameServerIntervalMillis(30_000);
    const quietBaseline = h.routes.length;
    check('back at the default period nothing extra is polled',
      (await until(() => h.routes.length >= quietBaseline + 2, 400)) === false);
    p.shutdown();

    // A period handed in BEFORE the loop is built has to be the period the loop
    // runs on: the setter only stores while no timer exists, and the beat loop
    // takes its period from that field when it is built.
    const solo = new Harness('BeatFastTopic', ONE_BROKER, 'GID_BeatFast');
    solo.client.setHeartbeatIntervalMillis(40);
    solo.client.setPollNameServerIntervalMillis(40);
    const sp = await solo.start();
    const earlyBeats = await until(() => solo.beats.length >= 4, 1500);
    const earlySpan = solo.beats[solo.beats.length - 1] - solo.beats[0];
    check('the loop is built on the period already on the instance',
      earlyBeats && earlySpan < 1000, `${solo.beats.length} beats in ${earlySpan}ms`);
    const earlyRoutes = await until(() => solo.routes.length >= 2, 1500);
    check('and so is the route refresh loop',
      earlyRoutes && solo.routes.includes('BeatFastTopic'), JSON.stringify(solo.routes));
    check('and the facade reports the period the instance really runs on',
      sp.getHeartbeatIntervalMillis() === 40);
    sp.shutdown();
  }

  // ============================== 8. transaction check-back hooks
  console.log('== check-back EndTransaction hooks ==');
  {
    const h = new Harness('TxTopic', ONE_BROKER, 'GID_TxHook');
    const p = await h.start();
    let state = 0; // COMMIT
    (p as any).transactionListener = {
      checkLocalTransaction: (_m: MessageExt) => state,
      executeLocalTransaction: () => state,
    };

    const half = new MessageExt();
    half.setTopic('TxTopic');
    half.setBody(Buffer.from('half'));
    half.putProperty(MessageConst.PROPERTY_PRODUCER_GROUP, 'GID_TxHook');
    half.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, 'UNIQ-TX-1');
    const cmd = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, null);
    cmd.body = encodeMessageExt(half, false);
    cmd.addExtField('tranStateTableOffset', '11');
    cmd.addExtField('commitLogOffset', '22');

    const seen: Array<Record<string, any>> = [];
    class Recorder extends EndTransactionHook {
      getHookName(): string { return 'Recorder'; }
      endTransactionBefore(ctx: any): void { h.trace.push('before'); seen.push({ phase: 'before', ...ctx }); }
      endTransactionAfter(ctx: any): void { h.trace.push('after'); seen.push({ phase: 'after', ...ctx }); }
    }
    const hook = new Recorder();
    hookRegistry.registerEndTransactionHook(hook);
    h.client.dispatchFacadeProcessor(cmd, ADDR_B);
    check('the check-back went out', await until(() => h.oneway.length === 1, 1500), h.trace.join(','));
    check('the hooks run before and after the oneway dispatch',
      h.trace.join(',') === 'before,oneway,after', h.trace.join(','));
    const before = seen.find((c) => c.phase === 'before');
    const after = seen.find((c) => c.phase === 'after');
    check('the context carries the check-back identity',
      before != null && before.producerGroup === 'GID_TxHook' && before.msgId === 'UNIQ-TX-1' &&
      before.transactionId === 'UNIQ-TX-1' && before.brokerAddr === ADDR_B &&
      before.fromTransactionCheck === true && before.transactionState === 0, JSON.stringify(before));
    check('the after hook sees the same context', after != null && after.msgId === 'UNIQ-TX-1');
    const ended = h.oneway[0];
    ended.headerEncode();
    check('the END_TRANSACTION frame is the commit the listener chose',
      ended.code === RequestCode.END_TRANSACTION &&
      ended.extFields['commitOrRollback'] === String(MessageSysFlag.TRANSACTION_COMMIT_TYPE) &&
      ended.extFields['fromTransactionCheck'] === 'true' &&
      ended.extFields['tranStateTableOffset'] === '11' &&
      ended.extFields['commitLogOffset'] === '22', JSON.stringify(ended.extFields));

    state = 1;
    h.oneway.length = 0; h.trace.length = 0; seen.length = 0;
    h.client.dispatchFacadeProcessor(cmd, ADDR_B);
    check('a rollback check-back fires the hooks too',
      (await until(() => h.oneway.length === 1, 1500)) && h.trace.join(',') === 'before,oneway,after',
      h.trace.join(','));
    const rollback = h.oneway[0];
    rollback.headerEncode();
    check('and carries the rollback type',
      rollback.extFields['commitOrRollback'] === String(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE),
      String(rollback.extFields['commitOrRollback']));

    const idx = hookRegistry.endTransactionHooks.indexOf(hook);
    if (idx >= 0) hookRegistry.endTransactionHooks.splice(idx, 1);
    check('the hook was unregistered', hookRegistry.endTransactionHooks.length === 0);
    state = 0;
    h.oneway.length = 0; h.trace.length = 0; seen.length = 0;
    h.client.dispatchFacadeProcessor(cmd, ADDR_B);
    check('with no hook registered the check-back still commits',
      (await until(() => h.oneway.length === 1, 1500)) && h.trace.join(',') === 'oneway' && seen.length === 0,
      h.trace.join(','));

    h.oneway.length = 0;
    const orphan = new MessageExt();
    orphan.setTopic('TxTopic');
    orphan.setBody(Buffer.from('orphan'));
    orphan.putProperty(MessageConst.PROPERTY_PRODUCER_GROUP, 'GID_Nobody');
    const stranger = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, null);
    stranger.body = encodeMessageExt(orphan, false);
    const reply = h.client.dispatchFacadeProcessor(stranger, ADDR_B);
    await until(() => h.oneway.length > 0, 100);
    check('a check-back for an unregistered group sends no END_TRANSACTION',
      h.oneway.length === 0 && reply != null && reply.code === ResponseCode.SUCCESS);
    p.shutdown();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  clearInterval(keepAlive);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
