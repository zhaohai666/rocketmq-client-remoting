// Classic namespace + recall contract — offline test against a recording
// transport (producer) and a fake TCP broker (consumers).
//
//   node --experimental-strip-types test/classic_namespace_smoke.ts
//
// Baseline held here:
//   * with a client-side namespace set, every resource the broker sees carries
//     the prefix — the producer/consumer group, the business topic, the retry
//     topic `%RETRY%<ns>%<group>` — while every caller and every delivered
//     message keeps the bare name;
//   * helper wrapping is idempotent, leaves system resources alone, and keeps
//     the RETRY/DLQ prefix in front of the namespace rather than behind it;
//   * recallMessage(topic, handle) validates its topic, refuses retry/DLQ
//     topics, warms the route, keeps the handle's broker with the topic's
//     publisher as fallback, spends the producer send budget, and answers with
//     the withdrawn message id — a non-success answer is a typed broker error.
import net from 'node:net';

import { MQClient } from '../src/client/mq_client.ts';
import { MQClientManager } from '../src/client/mq_client_manager.ts';
import { DefaultMQProducer, LocalTransactionState } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { Message, MessageExt, MessageQueue } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { encodeMessageExt, string2MessageProperties } from '../src/common/messageDecoder.ts';
import { RecallMessageHandle } from '../src/common/recall_message_handle.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { SendMessageRequestHeaderV2, SendMessageResponseHeader } from '../src/remoting/headers.ts';
import { ConsumeFromWhere, HeartbeatData } from '../src/remoting/heartbeat.ts';
import { MQBrokerException, MQClientException } from '../src/remoting/exception.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

async function until(cond: () => boolean, timeoutMs = 4000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (cond()) return true;
    await new Promise((r) => setTimeout(r, 10));
  }
  return cond();
}

async function throwsWith(fn: () => Promise<any>): Promise<Error | null> {
  try { await fn(); return null; } catch (e) { return e as Error; }
}

const NS = 'NSDEMO';
const BROKER = 'nsBroker';

// ---------------------------------------------------------------- producer side

type SendRec = {
  code: number;
  addr: string;
  topic: string;
  group: string;
  properties: Record<string, string>;
};
type CallRec = {
  code: number;
  addr: string;
  ext: Record<string, string>;
  timeout: number;
  request: RemotingCommand;
};

let wireSeq = 0;

// A recording transport that answers like the name server and one broker. It
// adopts the exact MQClient instance the facade resolves from its clientId, so
// start()/shutdown() run their real bookkeeping without opening a socket.
class ProducerWire {
  client: MQClient;
  producer: DefaultMQProducer;
  clientId: string;
  addr = '127.0.0.1:19150';
  rosterAddr = '127.0.0.1:19151';
  broker = 'brokerP';
  calls: CallRec[] = [];
  sends: SendRec[] = [];
  oneways: RemotingCommand[] = [];
  routes: string[] = [];
  beats: Array<{ clientID: string; producers: string[] }> = [];
  missingRoutes = new Set<string>();
  recallAnswer: { code: number; msgId?: string; remark?: string } =
    { code: ResponseCode.SUCCESS, msgId: 'RECALLED-MSG-ID' };

  constructor(group: string, namespace: string | null) {
    const producer = new DefaultMQProducer(group);
    producer.setInstanceName(`ns-prod-${++wireSeq}`);
    producer.setSendMsgTimeout(4321);
    if (namespace != null) producer.setNamespace(namespace);
    this.producer = producer;
    this.clientId = (producer as any)._buildClientId();
    producer.setNamesrvAddr(this.rosterAddr);

    const remoting: any = {
      rpcHooks: [],
      registerProcessor: (): void => { /* reverse requests are not part of this harness */ },
      unregisterProcessor: (): void => { /* ditto */ },
      shutdown: (): void => { /* no sockets */ },
      invokeOneway: (_addr: string, request: RemotingCommand): Promise<void> => {
        this.record(_addr, request, 0);
        this.oneways.push(request);
        return Promise.resolve();
      },
      invokeAsync: (addr: string, request: RemotingCommand, cb: any, timeout: number): void => {
        this.record(addr, request, timeout);
        setTimeout(() => {
          try { cb(this.answer(addr, request), null); } catch (e) { cb(null, e as Error); }
        }, 0);
      },
      invokeSync: async (addr: string, request: RemotingCommand, timeout: number): Promise<RemotingCommand> => {
        this.record(addr, request, timeout);
        return this.answer(addr, request);
      },
    };
    this.client = new MQClient(this.clientId, this.rosterAddr, remoting);
    MQClientManager.getInstance().getOrCreateMQClientInstance(this.clientId, () => this.client);
  }

  async start(): Promise<DefaultMQProducer> {
    await this.producer.start();
    return this.producer;
  }

  private record(addr: string, request: RemotingCommand, timeout: number): void {
    request.makeCustomHeaderToNet();
    const ext: Record<string, string> = { ...(request.extFields || {}) };
    this.calls.push({ code: request.code, addr, ext, timeout, request });
    if (request.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      this.routes.push(String(ext['topic'] ?? ''));
    }
    if (request.code === RequestCode.SEND_MESSAGE_V2 || request.code === RequestCode.SEND_BATCH_MESSAGE) {
      const v2 = new SendMessageRequestHeaderV2();
      try { v2.fromExtFields(ext); } catch (e) { /* a malformed frame fails the assertion below */ }
      const v1 = v2.toV1();
      this.sends.push({
        code: request.code,
        addr,
        topic: String(v1.topic ?? ''),
        group: String(v1.producerGroup ?? ''),
        properties: string2MessageProperties(String(v1.properties ?? '')),
      });
    }
  }

  private answer(addr: string, request: RemotingCommand): RemotingCommand {
    const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
    response.opaque = request.opaque;
    const ext = request.extFields || {};
    if (request.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const topic = String(ext['topic'] ?? '');
      if (this.missingRoutes.has(topic)) {
        response.code = ResponseCode.TOPIC_NOT_EXIST;
        return response;
      }
      response.body = Buffer.from(JSON.stringify({
        queueDatas: [{ brokerName: this.broker, readQueueNums: 2, writeQueueNums: 2, perm: 6, topicSysFlag: 0 }],
        brokerDatas: [{
          cluster: 'DefaultCluster', brokerName: this.broker,
          brokerAddrs: { '0': this.addr }, zoneName: null, enableActingMaster: false,
        }],
        filterServerTable: {},
      }), 'utf8');
      return response;
    }
    if (request.code === RequestCode.HEART_BEAT) {
      const hb = HeartbeatData.decode(request.body as Buffer);
      this.beats.push({
        clientID: hb.clientID,
        producers: hb.producerDataSet.map((p) => p.groupName),
      });
      return response;
    }
    if (request.code === RequestCode.SEND_MESSAGE_V2 || request.code === RequestCode.SEND_BATCH_MESSAGE) {
      const v2 = new SendMessageRequestHeaderV2();
      v2.fromExtFields(ext);
      const v1 = v2.toV1();
      const header = new SendMessageResponseHeader();
      header.msgId = `MSGID-${this.sends.length}`;
      header.queueId = Number(v1.queueId ?? 0);
      header.queueOffset = 4000 + this.sends.length;
      header.transactionId = '';
      header.batchUniqId = '';
      response.extFields = header.toExtFields();
      return response;
    }
    if (request.code === RequestCode.RECALL_MESSAGE) {
      response.code = this.recallAnswer.code;
      if (this.recallAnswer.remark != null) response.remark = this.recallAnswer.remark;
      if (this.recallAnswer.msgId != null) response.addExtField('msgId', this.recallAnswer.msgId);
      return response;
    }
    if (request.code === RequestCode.GET_MAX_OFFSET) {
      response.addExtField('offset', '100');
      return response;
    }
    void addr;
    return response;
  }

  producerBeats(): Array<{ clientID: string; producers: string[] }> { return this.beats; }
}

// ---------------------------------------------------------------- consumer side

const BROKER_NAME = BROKER;

type BeatRec = {
  clientID: string;
  producers: string[];
  consumers: Array<{ group: string; subs: Array<{ topic: string; expr: string }> }>;
};

class FakeBroker {
  server: net.Server;
  port = 0;
  routes = new Map<string, number>();
  routeLookups: string[] = [];
  beats: BeatRec[] = [];
  members = new Map<string, Set<string>>();
  sends: SendRec[] = [];
  pulls: Array<Record<string, string>> = [];
  minOffsets: Array<Record<string, string>> = [];
  maxOffsets: Array<Record<string, string>> = [];
  searches: Array<Record<string, string>> = [];
  queries: Array<Record<string, string>> = [];
  updates: Array<Record<string, string>> = [];
  bounces: Array<Record<string, string>> = [];
  unregisters: Array<Record<string, string>> = [];
  oneways: RemotingCommand[] = [];
  foundBodies: Buffer[] = [];

  constructor() { this.server = net.createServer((s) => this.onConnection(s)); }

  listen(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        this.port = (this.server.address() as net.AddressInfo).port;
        resolve();
      });
    });
  }

  get addr() { return `127.0.0.1:${this.port}`; }

  setRoute(topic: string, queues: number): void { this.routes.set(topic, queues); }

  lastBeatSubs(group: string): Map<string, string> {
    const own = this.beats.filter((b) => b.consumers.some((c) => c.group === group));
    const out = new Map<string, string>();
    if (own.length === 0) return out;
    const last = own[own.length - 1];
    const cd = last.consumers.find((c) => c.group === group);
    if (cd != null) for (const s of cd.subs) out.set(s.topic, s.expr);
    return out;
  }

  close(): void { this.server.close(); }

  private onConnection(sock: net.Socket): void {
    let buf = Buffer.alloc(0);
    sock.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      for (;;) {
        if (buf.length < 4) return;
        const total = buf.readInt32BE(0);
        if (total <= 0 || total > 20 * 1024 * 1024) { sock.destroy(); return; }
        if (buf.length < 4 + total) return;
        const frame = buf.subarray(0, 4 + total);
        buf = buf.subarray(4 + total);
        let req: RemotingCommand;
        try { req = RemotingCommand.decode(frame); } catch { sock.destroy(); return; }
        const resp = this.respond(req);
        if (resp) sock.write(resp.encode());
      }
    });
    sock.on('error', () => { /* teardown */ });
  }

  private respond(req: RemotingCommand): RemotingCommand | null {
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
    resp.opaque = req.opaque;
    resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
    const ext: Record<string, string> = { ...(req.extFields || {}) };

    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const topic = String(ext['topic'] ?? '');
      this.routeLookups.push(topic);
      const queues = this.routes.get(topic);
      if (queues == null) { resp.code = ResponseCode.TOPIC_NOT_EXIST; return resp; }
      resp.body = Buffer.from(JSON.stringify({
        queueDatas: [{ brokerName: BROKER_NAME, readQueueNums: queues, writeQueueNums: queues,
          perm: 6, topicSysFlag: 0 }],
        brokerDatas: [{
          cluster: 'DefaultCluster', brokerName: BROKER_NAME,
          brokerAddrs: { '0': this.addr }, zoneName: null, enableActingMaster: false,
        }],
        filterServerTable: {},
      }), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.HEART_BEAT) {
      const hb = HeartbeatData.decode(req.body as Buffer);
      const rec: BeatRec = {
        clientID: hb.clientID,
        producers: hb.producerDataSet.map((p) => p.groupName),
        consumers: hb.consumerDataSet.map((cd) => ({
          group: cd.groupName,
          subs: cd.subscriptionDataSet.map((s) => ({ topic: s.topic, expr: s.subString })),
        })),
      };
      this.beats.push(rec);
      for (const cd of rec.consumers) {
        let set = this.members.get(cd.group);
        if (set == null) { set = new Set(); this.members.set(cd.group, set); }
        set.add(rec.clientID);
      }
      return resp;
    }

    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      const ids = Array.from(this.members.get(String(ext['consumerGroup'] ?? '')) || []);
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: ids }), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.UNREGISTER_CLIENT) {
      this.unregisters.push(ext);
      return resp;
    }

    if (req.code === RequestCode.GET_MAX_OFFSET) {
      this.maxOffsets.push(ext);
      resp.addExtField('offset', '100');
      return resp;
    }
    if (req.code === RequestCode.GET_MIN_OFFSET) {
      this.minOffsets.push(ext);
      resp.addExtField('offset', '0');
      return resp;
    }
    if (req.code === RequestCode.SEARCH_OFFSET_BY_TIMESTAMP) {
      this.searches.push(ext);
      resp.addExtField('offset', '50');
      return resp;
    }
    if (req.code === RequestCode.QUERY_CONSUMER_OFFSET) {
      this.queries.push(ext);
      resp.code = ResponseCode.QUERY_NOT_FOUND;
      return resp;
    }
    if (req.code === RequestCode.UPDATE_CONSUMER_OFFSET) {
      this.updates.push(ext);
      return resp;
    }

    if (req.code === RequestCode.PULL_MESSAGE || req.code === RequestCode.LITE_PULL_MESSAGE) {
      this.pulls.push(ext);
      const offset = Number(ext['queueOffset'] ?? 0);
      const body = this.foundBodies.shift();
      if (body != null) {
        resp.body = body;
        resp.addExtField('nextBeginOffset', String(offset + 1));
      } else {
        resp.code = ResponseCode.PULL_NOT_FOUND;
        resp.addExtField('nextBeginOffset', String(offset));
      }
      resp.addExtField('minOffset', '0');
      resp.addExtField('maxOffset', '100');
      resp.addExtField('suggestWhichBrokerId', '0');
      return resp;
    }

    if (req.code === RequestCode.SEND_MESSAGE_V2 || req.code === RequestCode.SEND_BATCH_MESSAGE) {
      const v2 = new SendMessageRequestHeaderV2();
      v2.fromExtFields(ext);
      const v1 = v2.toV1();
      this.sends.push({
        code: req.code, addr: '', topic: String(v1.topic ?? ''),
        group: String(v1.producerGroup ?? ''), properties: string2MessageProperties(String(v1.properties ?? '')),
      });
      const header = new SendMessageResponseHeader();
      header.msgId = `SENT-${this.sends.length}`;
      header.queueId = Number(v1.queueId ?? 0);
      header.queueOffset = 10;
      resp.extFields = header.toExtFields();
      return resp;
    }

    if (req.code === RequestCode.CONSUMER_SEND_MSG_BACK) {
      this.bounces.push(ext);
      return resp;
    }

    if (req.code === RequestCode.RECALL_MESSAGE) {
      resp.addExtField('msgId', 'RECALLED-ID');
      return resp;
    }

    return resp;
  }
}

// A FOUND pull body carrying one stored message. The topic is the broker-side
// name; the caller must see the bare name after the pull.
function storedMessage(topic: string, tag = 'TagA'): Buffer {
  const m = new MessageExt(topic, Buffer.from('payload'), tag);
  m.setQueueId(0);
  m.setQueueOffset(1001);
  m.setStoreTimestamp(1700000000000);
  m.setBornTimestamp(1700000000000);
  m.setMsgId('STORED-1');
  m.putProperty(MessageConst.PROPERTY_TAGS, tag);
  return encodeMessageExt(m);
}

function bareQueue(queueId = 0): MessageQueue {
  return new MessageQueue('PlainTopic', BROKER_NAME, queueId);
}

function newMessageExt(topic: string, tag = 'TagA'): MessageExt {
  return new MessageExt(topic, Buffer.from('payload'), tag);
}

// ---------------------------------------------------------------- sections

async function helperContract(): Promise<void> {
  console.log('== helper wrapping ==');
  check('a business topic gains the prefix', MixAll.wrapNamespace(NS, 'Topic') === `${NS}%Topic`);
  check('wrapping is idempotent', MixAll.wrapNamespace(NS, MixAll.wrapNamespace(NS, 'Topic')) === `${NS}%Topic`);
  check('an empty namespace wraps nothing', MixAll.wrapNamespace('', 'Topic') === 'Topic');
  check('a retry topic keeps its prefix in front',
    MixAll.wrapNamespace(NS, '%RETRY%GID_a') === `%RETRY%${NS}%GID_a`);
  check('a DLQ topic keeps its prefix in front',
    MixAll.wrapNamespace(NS, '%DLQ%GID_a') === `%DLQ%${NS}%GID_a`);
  check('a system topic is never wrapped',
    MixAll.wrapNamespace(NS, MixAll.REAL_TRACE_TOPIC) === MixAll.REAL_TRACE_TOPIC,
    MixAll.wrapNamespace(NS, MixAll.REAL_TRACE_TOPIC));
  check('a system consumer group is never wrapped',
    MixAll.wrapNamespace(NS, 'CID_RMQ_SYS_TRANS') === 'CID_RMQ_SYS_TRANS',
    MixAll.wrapNamespace(NS, 'CID_RMQ_SYS_TRANS'));

  check('a prefixed topic unwraps', MixAll.withoutNamespace(`${NS}%Topic`, NS) === 'Topic');
  check('an empty namespace strips whatever is there', MixAll.withoutNamespace(`${NS}%Topic`, '') === 'Topic');
  check('a foreign namespace is left alone',
    MixAll.withoutNamespace('OTHER%Topic', NS) === 'OTHER%Topic');
  check('a retry topic unwraps back to its prefixed name',
    MixAll.withoutNamespace(`%RETRY%${NS}%GID_a`, NS) === '%RETRY%GID_a');
  check('wrapping then unwrapping is the identity',
    MixAll.withoutNamespace(MixAll.wrapNamespace(NS, '%RETRY%GID_a'), NS) === '%RETRY%GID_a');
}

async function producerWire(): Promise<void> {
  console.log('== producer namespace on the wire ==');
  const h = new ProducerWire('GID_ns_prod', NS);
  const p = await h.start();
  try {
    check('start() prefixes the producer group', p.getProducerGroup() === `${NS}%GID_ns_prod`,
      p.getProducerGroup());

    const sent = await p.send(new Message('PlainTopic', Buffer.from('one')));
    check('a send leaves under the prefixed topic',
      h.sends.length === 1 && h.sends[0].topic === `${NS}%PlainTopic`, h.sends[0]?.topic);
    check('the send header carries the prefixed producer group',
      h.sends[0]?.group === `${NS}%GID_ns_prod`, h.sends[0]?.group);
    check('the route was warmed for the prefixed topic', h.routes.includes(`${NS}%PlainTopic`),
      h.routes.join(','));
    check('the send still settles with a broker result', sent.sendStatus != null);

    await p.sendOneway(new Message('PlainTopic', Buffer.from('two')));
    const oneway = h.oneways.find((r) => r.code === RequestCode.SEND_MESSAGE_V2)!;
    oneway.makeCustomHeaderToNet();
    const v2w = new SendMessageRequestHeaderV2();
    v2w.fromExtFields(oneway.extFields);
    check('sendOneway wraps its topic too', v2w.toV1().topic === `${NS}%PlainTopic`,
      String(v2w.toV1().topic));

    let asyncResult: any = null;
    const sendsBeforeAsync = h.sends.length;
    await p.sendAsync(new Message('PlainTopic', Buffer.from('three')), (sr) => { asyncResult = sr; });
    check('sendAsync wraps its topic too',
      (await until(() => asyncResult != null, 1500)) &&
      h.sends.length === sendsBeforeAsync + 1 &&
      h.sends[h.sends.length - 1].topic === `${NS}%PlainTopic`,
      `${h.sends.length - sendsBeforeAsync} / ${h.sends[h.sends.length - 1]?.topic}`);

    await p.sendBySelector(new Message('PlainTopic', Buffer.from('four')), {
      select: (_m: any, mqs: MessageQueue[]) => mqs[0],
    }, null);
    check('sendBySelector wraps its topic',
      h.sends[h.sends.length - 1].topic === `${NS}%PlainTopic`, h.sends[h.sends.length - 1].topic);

    const halfMsg = new Message('PlainTopic', Buffer.from('tx'));
    await p.sendMessageInTransaction(halfMsg, {
      executeLocalTransaction: () => LocalTransactionState.COMMIT_MESSAGE,
      checkLocalTransaction: () => LocalTransactionState.COMMIT_MESSAGE,
    }, null);
    const half = h.sends.find((s) => s.properties[MessageConst.PROPERTY_TRANSACTION_PREPARED] === 'true');
    check('a half message leaves under the prefixed topic',
      half != null && half.topic === `${NS}%PlainTopic`, half?.topic);
    check('the half message names the prefixed producer group',
      half?.group === `${NS}%GID_ns_prod`, half?.group);
    const end = h.oneways.find((r) => r.code === RequestCode.END_TRANSACTION);
    end?.makeCustomHeaderToNet();
    check('the transaction end names the prefixed producer group',
      end != null && end.extFields['producerGroup'] === `${NS}%GID_ns_prod`,
      end?.extFields?.['producerGroup']);

    let requestFrame = false;
    const reqErr = await throwsWith(async () => {
      await p.request(new Message('PlainTopic', Buffer.from('ask')), 60);
    });
    requestFrame = h.sends.some((s) => s.properties[MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT] != null &&
      s.topic === `${NS}%PlainTopic`);
    check('a request wraps its topic before the wire', requestFrame && reqErr != null,
      `${requestFrame} / ${reqErr?.message}`);
    void reqErr;

    await p.createTopic('NewTopic', 'RouteKeyTopic');
    const created = h.calls.find((c) => c.code === RequestCode.UPDATE_AND_CREATE_TOPIC);
    check('createTopic prefixes only the new topic',
      created?.ext['topic'] === `${NS}%NewTopic`, created?.ext['topic']);
    check('createTopic leaves its route key alone',
      h.routes.filter((t) => t === MixAll.DEFAULT_TOPIC).length >= 1, h.routes.join(','));

    const max = await p.maxOffset('PlainTopic', 0);
    const maxCall = h.calls.filter((c) => c.code === RequestCode.GET_MAX_OFFSET).pop();
    check('maxOffset asks for the prefixed topic', max === 100 && maxCall?.ext['topic'] === `${NS}%PlainTopic`,
      `${max} / ${maxCall?.ext['topic']}`);

    await p.getMQClient()!.sendHeartbeatToAllBrokers();
    const beat = h.producerBeats();
    check('the heartbeat announces the prefixed producer group',
      beat.length > 0 && beat.some((b) => b.producers.includes(`${NS}%GID_ns_prod`)),
      JSON.stringify(beat.map((b) => b.producers)));
    check('buildHeartbeatData carries the prefixed group',
      p.getMQClient()!.buildHeartbeatData().producerDataSet.some((d) => d.groupName === `${NS}%GID_ns_prod`));

    console.log('== recall ==');
    const handle = RecallMessageHandle.encodeRecallMessageHandle(
      RecallMessageHandle.composeRecallMessageTopic('RecallTopic'), h.broker, '1700000000000', 'ORIGIN-1');
    const back = await p.recallMessage('RecallTopic', handle);
    const recall = h.calls.filter((c) => c.code === RequestCode.RECALL_MESSAGE).pop()!;
    check('recall answers with the withdrawn message id', back === 'RECALLED-MSG-ID', back);
    check('recall asks under the prefixed topic', recall.ext['topic'] === `${NS}%RecallTopic`, recall.ext['topic']);
    check('recall names the prefixed producer group', recall.ext['producerGroup'] === `${NS}%GID_ns_prod`,
      recall.ext['producerGroup']);
    check('recall keeps the handle as given', recall.ext['recallHandle'] === handle);
    check('recall sends the handle broker as bname', recall.ext['bname'] === h.broker, recall.ext['bname']);
    check('recall spends the producer send budget', recall.timeout === 4321, String(recall.timeout));
    check('recall warmed the route for the prefixed topic', h.routes.includes(`${NS}%RecallTopic`),
      h.routes.join(','));

    h.recallAnswer = { code: ResponseCode.SYSTEM_ERROR, remark: 'recall refused' };
    const refused = await throwsWith(() => p.recallMessage('RecallTopic', handle));
    check('a refused recall surfaces as a typed broker error',
      refused instanceof MQBrokerException && refused.responseCode === ResponseCode.SYSTEM_ERROR &&
      refused.message.includes('recall refused'), `${refused?.constructor.name}: ${refused?.message}`);
    h.recallAnswer = { code: ResponseCode.SUCCESS, msgId: 'RECALLED-MSG-ID' };

    const retryGuard = await throwsWith(() => p.recallMessage('%RETRY%GID_ns_prod', handle));
    check('a retry topic is refused before any RPC',
      retryGuard instanceof MQClientException && retryGuard.message.includes('topic is not supported'),
      retryGuard?.message);
    const dlqGuard = await throwsWith(() => p.recallMessage('%DLQ%GID_ns_prod', handle));
    check('a DLQ topic is refused before any RPC',
      dlqGuard instanceof MQClientException && dlqGuard.message.includes('topic is not supported'),
      dlqGuard?.message);
    const beforeBadHandle = h.calls.filter((c) => c.code === RequestCode.RECALL_MESSAGE).length;
    const badHandle = await throwsWith(() => p.recallMessage('RecallTopic', 'not-a-handle'));
    check('a malformed handle is refused without a recall RPC',
      badHandle instanceof MQClientException && badHandle.message.includes('recall handle is invalid') &&
      h.calls.filter((c) => c.code === RequestCode.RECALL_MESSAGE).length === beforeBadHandle,
      badHandle?.message);

    const goneHandle = RecallMessageHandle.encodeRecallMessageHandle(
      RecallMessageHandle.composeRecallMessageTopic('RecallTopic'), 'goneBroker', '1700000000000', 'ORIGIN-2');
    await p.recallMessage('RecallTopic', goneHandle);
    const fallback = h.calls.filter((c) => c.code === RequestCode.RECALL_MESSAGE).pop()!;
    check('a missing handle broker falls back to the topic publisher',
      fallback.addr === h.addr, `${fallback.addr} vs ${h.addr}`);
    check('the fallback keeps the handle broker as bname', fallback.ext['bname'] === 'goneBroker',
      fallback.ext['bname']);
  } finally {
    p.shutdown();
  }

  // A topic nobody routes (and no default-topic fallback) plus a handle broker
  // nobody knows: the recall must say so instead of dialling a guess.
  const lonely = new ProducerWire('GID_ns_prod_gone', NS);
  lonely.missingRoutes.add(`${NS}%GhostTopic`);
  lonely.missingRoutes.add(MixAll.DEFAULT_TOPIC);
  const lp = await lonely.start();
  try {
    const ghostHandle = RecallMessageHandle.encodeRecallMessageHandle(
      RecallMessageHandle.composeRecallMessageTopic('GhostTopic'), 'goneBroker', '1', 'X');
    const ghostErr = await throwsWith(() => lp.recallMessage('GhostTopic', ghostHandle));
    check('an unroutable topic with no publisher surfaces as not found',
      ghostErr instanceof MQClientException && ghostErr.message.includes('The broker service address not found'),
      ghostErr?.message);
  } finally {
    lp.shutdown();
  }

  console.log('== no namespace leaves names alone ==');
  const ctrl = new ProducerWire('GID_ns_ctrl', null);
  const cp = await ctrl.start();
  try {
    await cp.send(new Message('PlainTopic', Buffer.from('plain')));
    check('without a namespace a send keeps its topic', ctrl.sends[0]?.topic === 'PlainTopic',
      ctrl.sends[0]?.topic);
    check('without a namespace the group stays bare', cp.getProducerGroup() === 'GID_ns_ctrl',
      cp.getProducerGroup());
    const ctrlHandle = RecallMessageHandle.encodeRecallMessageHandle(
      RecallMessageHandle.composeRecallMessageTopic('PlainTopic'), ctrl.broker, '1', 'M');
    await cp.recallMessage('PlainTopic', ctrlHandle);
    check('without a namespace recall sends the bare topic',
      ctrl.calls.filter((c) => c.code === RequestCode.RECALL_MESSAGE).pop()?.ext['topic'] === 'PlainTopic');
  } finally {
    cp.shutdown();
  }
}

async function pushConsumer(): Promise<void> {
  console.log('== push consumer namespace ==');
  const broker = new FakeBroker();
  await broker.listen();
  broker.setRoute(`${NS}%PlainTopic`, 2);
  const c = new DefaultMQPushConsumer('GID_ns_push');
  c.setNamesrvAddr(broker.addr);
  c.setInstanceName('ns-push-1');
  c.setNamespace(NS);
  c.setPullInterval(50);
  c.subscribe('PlainTopic', 'TagA');
  const delivered: string[] = [];
  c.registerMessageListener((msgs: MessageExt[]) => {
    for (const m of msgs) delivered.push(m.getTopic());
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  try {
    await c.start();
    check('the push group is prefixed after start', c.consumerGroup === `${NS}%GID_ns_push`, c.consumerGroup);
    check('subscribe stores the prefixed topic', c.subscription.has(`${NS}%PlainTopic`),
      Array.from(c.subscription.keys()).join(','));
    check('the heartbeat names the prefixed group', await until(() => broker.beats.some((b) =>
      b.consumers.some((cd) => cd.group === `${NS}%GID_ns_push`))),
      JSON.stringify(broker.beats.map((b) => b.consumers.map((cd) => cd.group))));
    const subs = broker.lastBeatSubs(`${NS}%GID_ns_push`);
    check('the heartbeat carries the prefixed business topic', subs.get(`${NS}%PlainTopic`) === 'TagA',
      JSON.stringify([...subs]));
    check('the implicit retry topic is prefixed once',
      subs.has(`%RETRY%${NS}%GID_ns_push`), JSON.stringify([...subs]));
    check('the route was fetched under the prefixed topic',
      broker.routeLookups.includes(`${NS}%PlainTopic`), broker.routeLookups.join(','));
    check('rebalance assigns the prefixed queues',
      await until(() => c.assigned.length === 2, 4000) && c.assigned.every((mq) => mq.getTopic() === `${NS}%PlainTopic`),
      c.assigned.map((mq) => mq.getTopic()).join(','));

    broker.foundBodies.push(storedMessage(`${NS}%PlainTopic`));
    check('a delivered message reaches the listener under the bare topic',
      await until(() => delivered.length > 0, 4000) && delivered[0] === 'PlainTopic',
      delivered.join(','));
    const pull = broker.pulls.find((p) => p['topic'] === `${NS}%PlainTopic`);
    check('the push pull asked under the prefixed topic', pull != null, JSON.stringify(broker.pulls[0]));

    // The delivery-entry rewrites, pinned directly for the retry and POP naming
    // the pull path cannot easily produce here.
    const anyC = c as any;
    const retryMsg = newMessageExt(`%RETRY%${NS}%GID_ns_push`, '');
    retryMsg.putProperty(MessageConst.PROPERTY_RETRY_TOPIC, `${NS}%PlainTopic`);
    const plainMsg = newMessageExt(`${NS}%PlainTopic`, '');
    anyC._resetRetryTopicAndNamespace([retryMsg, plainMsg]);
    check('the retry delivery unwraps to the bare origin topic', retryMsg.getTopic() === 'PlainTopic',
      retryMsg.getTopic());
    check('a plain delivery unwraps its own topic', plainMsg.getTopic() === 'PlainTopic',
      plainMsg.getTopic());

    const popV1 = newMessageExt(`%RETRY%GID_ns_push_${NS}%PlainTopic`, '');
    const popV2 = newMessageExt(`%RETRY%GID_ns_push+${NS}%PlainTopic`, '');
    const popOther = newMessageExt(`${NS}%OtherTopic`, '');
    anyC._resetPopRetryTopic([popV1, popV2, popOther]);
    check('the POP retry name rewrites to the bare topic (v1 separator)',
      popV1.getTopic() === 'PlainTopic', popV1.getTopic());
    check('the POP retry name rewrites to the bare topic (v2 separator)',
      popV2.getTopic() === 'PlainTopic', popV2.getTopic());
    check('a non-retry POP row keeps its topic', popOther.getTopic() === `${NS}%OtherTopic`,
      popOther.getTopic());

    const bounceMsg = newMessageExt('PlainTopic');
    bounceMsg.brokerName = BROKER_NAME;
    bounceMsg.commitLogOffset = 7;
    bounceMsg.setMsgId('BOUNCE-1');
    await c.sendMessageBack(bounceMsg, 0);
    const bounce = broker.bounces[broker.bounces.length - 1];
    check('a bounced message names the prefixed origin topic',
      bounce != null && bounce['originTopic'] === `${NS}%PlainTopic`, bounce?.['originTopic']);
    check('a bounced message names the prefixed group',
      bounce != null && bounce['group'] === `${NS}%GID_ns_push`, bounce?.['group']);
  } finally {
    await c.shutdown().catch(() => {});
    broker.close();
  }
}

async function pullConsumer(): Promise<void> {
  console.log('== pull consumer namespace ==');
  const broker = new FakeBroker();
  await broker.listen();
  broker.setRoute(`${NS}%PlainTopic`, 2);
  const c = new DefaultMQPullConsumer('GID_ns_pull');
  c.setNamesrvAddr(broker.addr);
  c.setInstanceName('ns-pull-1');
  c.setNamespace(NS);
  c.subscribe('PlainTopic', '*');
  try {
    await c.start();
    check('the pull group is prefixed after start', c.consumerGroup === `${NS}%GID_ns_pull`, c.consumerGroup);
    check('the route was fetched under the prefixed topic',
      broker.routeLookups.includes(`${NS}%PlainTopic`), broker.routeLookups.join(','));
    const subs = broker.lastBeatSubs(`${NS}%GID_ns_pull`);
    check('the pull heartbeat carries the prefixed topic', subs.has(`${NS}%PlainTopic`),
      JSON.stringify([...subs]));

    const queues = await c.fetchSubscribeMessageQueues('PlainTopic');
    check('the subscribe view hands back bare queues',
      queues.length === 2 && queues.every((mq) => mq.getTopic() === 'PlainTopic'),
      queues.map((mq) => mq.getTopic()).join(','));

    broker.foundBodies.push(storedMessage(`${NS}%PlainTopic`));
    const result = await c.pull(bareQueue(0), '*', 0, 32);
    const pull = broker.pulls[broker.pulls.length - 1];
    check('the pull request names the prefixed topic', pull['topic'] === `${NS}%PlainTopic`, pull['topic']);
    check('the pulled message is handed back under the bare topic',
      result.msgFoundList.length === 1 && result.msgFoundList[0].getTopic() === 'PlainTopic',
      result.msgFoundList.map((m) => m.getTopic()).join(','));

    const max = await c.maxOffset(bareQueue(0));
    const maxCall = broker.maxOffsets[broker.maxOffsets.length - 1];
    check('maxOffset asks under the prefixed topic', max === 100 && maxCall['topic'] === `${NS}%PlainTopic`,
      `${max} / ${maxCall?.['topic']}`);

    c.updateConsumeOffset(bareQueue(0), 42);
    // persist() takes the stored (wrapped) queue: that is where updateConsumeOffset
    // put the offset, because the broker-side key carries the namespace.
    await (c as any).offsetStore.persist(new MessageQueue(`${NS}%PlainTopic`, BROKER_NAME, 0));
    const update = broker.updates[broker.updates.length - 1];
    check('a committed offset names the prefixed topic', update?.['topic'] === `${NS}%PlainTopic`,
      update?.['topic']);
    check('a committed offset names the prefixed group', update?.['consumerGroup'] === `${NS}%GID_ns_pull`,
      update?.['consumerGroup']);

    const fetched = await c.fetchConsumeOffset(bareQueue(1), true);
    const query = broker.queries[broker.queries.length - 1];
    check('a broker-side offset read names the prefixed topic',
      fetched === -1 && query?.['topic'] === `${NS}%PlainTopic`, `${fetched} / ${query?.['topic']}`);

    const view = await c.fetchMessageQueuesInBalance('PlainTopic');
    check('the balance view hands back bare queues',
      view.length === 2 && view.every((mq) => mq.getTopic() === 'PlainTopic'),
      view.map((mq) => mq.getTopic()).join(','));
    check('the balance view fetched the prefixed route',
      broker.routeLookups.filter((t) => t === `${NS}%PlainTopic`).length >= 2, broker.routeLookups.join(','));

    const bounceMsg = newMessageExt('PlainTopic');
    bounceMsg.brokerName = BROKER_NAME;
    bounceMsg.setMsgId('PULL-BOUNCE');
    await c.sendMessageBack(bounceMsg, 0);
    const bounce = broker.bounces[broker.bounces.length - 1];
    check('the pull send-back names the prefixed origin topic',
      bounce?.['originTopic'] === `${NS}%PlainTopic`, bounce?.['originTopic']);
    check('the pull send-back names the prefixed group',
      bounce?.['group'] === `${NS}%GID_ns_pull`, bounce?.['group']);
  } finally {
    await c.shutdown().catch(() => {});
    broker.close();
  }
}

async function liteAssign(): Promise<void> {
  console.log('== lite pull: assign mode ==');
  const broker = new FakeBroker();
  await broker.listen();
  broker.setRoute(`${NS}%PlainTopic`, 2);
  const c = new DefaultLitePullConsumer('GID_ns_lite');
  c.setNamesrvAddr(broker.addr);
  c.setInstanceName('ns-lite-1');
  c.setNamespace(NS);
  c.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
  c.assign([bareQueue(0)]);
  c.setSubExpressionForAssign('PlainTopic', '*');
  try {
    await c.start();
    check('the lite group is prefixed after start', c.consumerGroup === `${NS}%GID_ns_lite`, c.consumerGroup);
    check('an assigned bare queue becomes prefixed in the assignment view',
      c.assignment().length === 1 && c.assignment()[0].getTopic() === `${NS}%PlainTopic`,
      c.assignment().map((mq) => mq.getTopic()).join(','));
    check('the assign-mode route was fetched under the prefixed topic',
      broker.routeLookups.includes(`${NS}%PlainTopic`), broker.routeLookups.join(','));
    check('the lite heartbeat names the prefixed group',
      broker.beats.some((b) => b.consumers.some((cd) => cd.group === `${NS}%GID_ns_lite`)),
      JSON.stringify(broker.beats.map((b) => b.consumers.map((cd) => cd.group))));

    const fetched = await c.fetchMessageQueues('PlainTopic');
    check('fetchMessageQueues hands back bare queues',
      fetched.length === 2 && fetched.every((mq) => mq.getTopic() === 'PlainTopic'),
      fetched.map((mq) => mq.getTopic()).join(','));

    broker.foundBodies.push(storedMessage(`${NS}%PlainTopic`));
    const polled = await c.poll(1000);
    const pull = broker.pulls[broker.pulls.length - 1];
    check('the lite pull asks under the prefixed topic', pull['topic'] === `${NS}%PlainTopic`, pull['topic']);
    check('the lite pull hands messages back under the bare topic',
      polled.length === 1 && polled[0].getTopic() === 'PlainTopic', polled.map((m) => m.getTopic()).join(','));

    check('committed() maps a bare queue onto the pulled offset', await c.committed(bareQueue(0)) === 1,
      String(await c.committed(bareQueue(0))));
    const notSeen = await c.committed(bareQueue(1));
    const query = broker.queries[broker.queries.length - 1];
    check('committed() reads the broker under the prefixed topic',
      notSeen === -1 && query?.['topic'] === `${NS}%PlainTopic`, `${notSeen} / ${query?.['topic']}`);

    const at = await c.offsetForTimestamp(bareQueue(0), 1700000000000);
    const search = broker.searches[broker.searches.length - 1];
    check('offsetForTimestamp asks under the prefixed topic',
      at === 50 && search?.['topic'] === `${NS}%PlainTopic`, `${at} / ${search?.['topic']}`);
    check('offsetForTimestamp carries the instant', search?.['timestamp'] === '1700000000000',
      search?.['timestamp']);

    await c.seek(bareQueue(0), 10);
    check('seek checks the bounds under the prefixed topic',
      broker.minOffsets[broker.minOffsets.length - 1]?.['topic'] === `${NS}%PlainTopic` &&
      broker.maxOffsets[broker.maxOffsets.length - 1]?.['topic'] === `${NS}%PlainTopic`,
      `${broker.minOffsets.pop()?.['topic']} / ${broker.maxOffsets.pop()?.['topic']}`);
    const beforeSeekPull = broker.pulls.length;
    await c.poll(100);
    check('the pull after seek starts at the sought offset',
      broker.pulls.length > beforeSeekPull &&
      broker.pulls[broker.pulls.length - 1]['queueOffset'] === '10',
      broker.pulls[broker.pulls.length - 1]?.['queueOffset']);
    const illegal = await throwsWith(() => c.seek(bareQueue(0), 500));
    check('a seek past the max offset is refused', illegal != null && illegal.message.includes('Seek offset illegal'),
      illegal?.message);

    c.pause([bareQueue(0)]);
    const pausedAt = broker.pulls.length;
    const empty = await c.poll(60);
    check('a paused bare queue issues no pull',
      empty.length === 0 && broker.pulls.length === pausedAt, `${broker.pulls.length - pausedAt} pulls`);
    c.resume([bareQueue(0)]);
    await c.poll(100);
    check('resume puts the bare queue back in the round', broker.pulls.length > pausedAt,
      String(broker.pulls.length - pausedAt));
  } finally {
    await c.shutdown().catch(() => {});
    broker.close();
  }
}

async function liteSubscribe(): Promise<void> {
  console.log('== lite pull: subscribe mode ==');
  const broker = new FakeBroker();
  await broker.listen();
  broker.setRoute(`${NS}%PlainTopic`, 2);
  const c = new DefaultLitePullConsumer('GID_ns_lite_sub');
  c.setNamesrvAddr(broker.addr);
  c.setInstanceName('ns-lite-2');
  c.setNamespace(NS);
  c.subscribe('PlainTopic', 'TagA');
  try {
    await c.start();
    check('the lite subscribe heartbeat carries the prefixed topic',
      broker.lastBeatSubs(`${NS}%GID_ns_lite_sub`).get(`${NS}%PlainTopic`) === 'TagA',
      JSON.stringify([...broker.lastBeatSubs(`${NS}%GID_ns_lite_sub`)]));
    check('the subscribe rebalance assigns prefixed queues',
      c.assignment().length === 2 && c.assignment().every((mq) => mq.getTopic() === `${NS}%PlainTopic`),
      c.assignment().map((mq) => mq.getTopic()).join(','));

    const seen: Array<{ topic: string; queues: string[] }> = [];
    // The registration takes its first snapshot asynchronously; changing the
    // route before that settles would make the change invisible to the compare.
    await c.registerTopicMessageQueueChangeListener('PlainTopic', {
      onChanged: (topic: string, queues: MessageQueue[]) => {
        seen.push({ topic, queues: queues.map((q) => q.getTopic()) });
      },
    });
    broker.setRoute(`${NS}%PlainTopic`, 3);
    const fired = await c.fetchTopicMessageQueuesAndCompare();
    check('the queue-change listener fires for the prefixed topic',
      fired === 1 && seen.length === 1 && seen[0].topic === `${NS}%PlainTopic`,
      JSON.stringify(seen));
    check('the listener snapshot holds bare queues',
      seen.length === 1 && seen[0].queues.length === 3 && seen[0].queues.every((t) => t === 'PlainTopic'),
      JSON.stringify(seen[0]?.queues));

    c.unsubscribe('PlainTopic');
    check('unsubscribe removes the prefixed subscription', !c.subscription.has(`${NS}%PlainTopic`) &&
      c.subscription.size === 0, Array.from(c.subscription.keys()).join(','));
  } finally {
    await c.shutdown().catch(() => {});
    broker.close();
  }
}

async function main(): Promise<void> {
  await helperContract();
  await producerWire();
  await pushConsumer();
  await pullConsumer();
  await liteAssign();
  await liteSubscribe();

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
