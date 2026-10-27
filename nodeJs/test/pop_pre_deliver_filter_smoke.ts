// What a POP round does with a FOUND answer BEFORE it hands anything over —
// offline, against an in-process fake broker.
//
//   node --experimental-strip-types test/pop_pre_deliver_filter_smoke.ts
//
// A POP row is invisible to everyone else until this client answers for it, and
// the answer is per row. So a row this client is never going to consume has to
// be dealt with at the round, not left in the batch: the round filters it out,
// ACKs it, and books only what it actually delivered. Four rules, in this
// order, and all of them run before the round is booked:
//
//   (a) the exact-tag check: a non-empty tag set drops a row whose tag is not
//       in it, including a row that carries no tag at all; an empty tag set
//       (SUB_ALL) or a class/SQL92 expression switches the check off;
//   (b) the FilterMessageHook list over the kept list, a throwing hook skipped;
//   (c) the retry budget as an early guard: a row already STRICTLY past it is
//       dropped, one that has exactly reached it is delivered;
//   (d) everything (a)-(c) removed is ACKed at once — a row that is filtered
//       out but not acked simply comes back when its window closes;
//   (e) only what remains reaches the listener, the pull stats and the queue's
//       answer debt.
//
// The broker's ACK_MESSAGE(200051) and CHANGE_MESSAGE_INVISIBLETIME(200053)
// requests are read off the socket, so "the dropped row was acked" is a wire
// fact, not a client-side flag. Every positive rule is paired with the control
// that goes red if it is removed, moved after the stats, or applied to the
// wrong path.
import net from 'node:net';

import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MessageExt, MessageQueue } from '../src/common/message.ts';
import { encodeMessageExt } from '../src/common/messageDecoder.ts';
import { FilterAPI } from '../src/common/subscriptionData.ts';
import { PullResult, PullStatus, ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { FilterMessageContext, FilterMessageHook } from '../src/client/hook.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { mqKey } from '../src/client/offset_store.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'PopFilterTopic';
const BROKER = 'broker-a';
const INVISIBLE_MILLIS = 60000;

// row builds one message the fake broker can hand back as a pop result. The
// queue offset identifies the row in every assertion below.
function row(offset: number, tag: string | null, reconsumeTimes: number): MessageExt {
  const m = new MessageExt(TOPIC, Buffer.from('body-' + offset), tag);
  m.setQueueId(0);
  m.setQueueOffset(offset);
  m.setCommitLogOffset(offset);
  m.setReconsumeTimes(reconsumeTimes);
  m.setBornTimestamp(Date.now());
  m.setStoreHost('127.0.0.1');
  m.setBornHost('127.0.0.1');
  return m;
}

class FakePopBroker {
  server: net.Server;
  port = 0;
  queues = new Map<string, number>();
  cidList: string[] = [];
  // The FOUND answer to serve, once, for this topic.
  planTopic = '';
  planRows: MessageExt[] = [];
  served = false;
  pops: Array<Record<string, string>> = [];
  // Offsets the broker was asked to settle, per RPC, straight off the wire.
  acks: string[] = [];
  changes: string[] = [];
  ackHeaders: Array<Record<string, string>> = [];

  constructor() { this.server = net.createServer((s) => this.onConnection(s)); }

  listen(): Promise<number> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        this.port = (this.server.address() as net.AddressInfo).port;
        resolve(this.port);
      });
    });
  }

  get addr() { return `127.0.0.1:${this.port}`; }
  addTopic(topic: string, n: number) { this.queues.set(topic, n); }
  plan(topic: string, rows: MessageExt[]) { this.planTopic = topic; this.planRows = rows; }

  popsFor(topic: string): Array<Record<string, string>> {
    return this.pops.filter((p) => p['topic'] === topic);
  }

  private routeBody(topic: string, n: number): string {
    return JSON.stringify({
      orderTopicConf: null,
      queueDatas: [{ brokerName: BROKER, readQueueNums: n, writeQueueNums: n, perm: 6, topicSysFlag: 0 }],
      brokerDatas: [{
        cluster: 'DefaultCluster', brokerName: BROKER,
        brokerAddrs: { '0': this.addr }, zoneName: null, enableActingMaster: false,
      }],
      filterServerTable: {},
    });
  }

  onConnection(sock: net.Socket) {
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
    sock.on('error', () => { /* the client hangs up on teardown */ });
  }

  respond(req: RemotingCommand): RemotingCommand | null {
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!;
    resp.opaque = req.opaque;
    resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
    const ext = req.extFields || {};

    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const n = this.queues.get(ext['topic'] || '');
      if (n === undefined) { resp.code = ResponseCode.TOPIC_NOT_EXIST; return resp; }
      resp.body = Buffer.from(this.routeBody(ext['topic'], n), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.HEART_BEAT) {
      try {
        const hb = JSON.parse(req.body ? req.body.toString('utf8') : '{}');
        if (hb.clientID && !this.cidList.includes(String(hb.clientID))) {
          this.cidList.push(String(hb.clientID));
        }
      } catch { /* an unparseable beat leaves the group empty */ }
      return resp;
    }
    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: this.cidList }), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.POP_MESSAGE) {
      this.pops.push({ ...ext });
      if (!this.served && ext['topic'] === this.planTopic && ext['initMode'] != null) {
        this.served = true;
        resp.body = Buffer.concat(this.planRows.map((r) => encodeMessageExt(r)));
        // The checkpoint the client has to rebuild per row: popTime and
        // invisibleTime decide whether the batch is still inside its window.
        // The offset tables stay absent, so the client derives each row's
        // checkpoint from its own queue offset.
        resp.extFields['popTime'] = String(Date.now());
        resp.extFields['invisibleTime'] = String(INVISIBLE_MILLIS);
        resp.extFields['reviveQid'] = '0';
        resp.extFields['restNum'] = '0';
        return resp;
      }
      // Anything else: the polling queue is full, which parks this client's
      // round for its own backoff instead of spinning on the socket.
      resp.code = ResponseCode.POLLING_FULL;
      return resp;
    }
    if (req.code === RequestCode.ACK_MESSAGE) {
      this.acks.push(String(ext['offset']));
      this.ackHeaders.push({ ...ext });
      return resp;
    }
    if (req.code === RequestCode.CHANGE_MESSAGE_INVISIBLETIME) {
      this.changes.push(String(ext['offset']));
      resp.extFields['popTime'] = String(Date.now());
      resp.extFields['invisibleTime'] = String(INVISIBLE_MILLIS);
      resp.extFields['reviveQid'] = '0';
      return resp;
    }
    return resp;
  }

  close() { this.server.close(); }
}

async function waitFor(cond: () => boolean, timeoutMs = 4000): Promise<boolean> {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (cond()) return true;
    await new Promise<void>((r) => setTimeout(r, 10));
  }
  return cond();
}

// settle waits for the round's RPCs to arrive and then a short quiet window, so
// an "exactly these" assertion cannot be satisfied by a partial batch.
async function settle(broker: FakePopBroker, expectedRpc: number): Promise<boolean> {
  const done = await waitFor(() => broker.acks.length + broker.changes.length >= expectedRpc);
  await new Promise<void>((r) => setTimeout(r, 150));
  return done;
}

const FIVE_ROWS = () => [
  row(100, 'TagA', 0),    // matches the tag, inside the budget -> delivered
  row(101, 'TagB', 0),    // tag mismatch -> dropped by (a)
  row(102, null, 0),      // carries no tag at all -> dropped by (a)
  row(103, 'TagA', 2),    // exactly at the budget -> delivered (strict ">")
  row(104, 'TagA', 3),    // strictly past the budget -> dropped by (c)
];

// stage runs one POP round end to end and reports what the listener saw.
async function stage(opts: {
  group: string;
  expression?: string;
  classFilter?: boolean;
  maxReconsumeTimes?: number;
  rows: MessageExt[];
  hooks?: FilterMessageHook[];
  listenerStatus?: number;
  expectedRpc: number;
  threshold?: number;
}): Promise<{ broker: FakePopBroker; c: DefaultMQPushConsumer; delivered: MessageExt[]; settleOk: boolean }> {
  const broker = new FakePopBroker();
  await broker.listen();
  broker.addTopic(TOPIC, 1);
  broker.plan(TOPIC, opts.rows);

  const c = new DefaultMQPushConsumer(opts.group);
  c.setNamesrvAddr(broker.addr);
  c.setPopMode(true);
  c.subscribe(TOPIC, opts.expression ?? 'TagA');
  if (opts.classFilter) {
    // A class/SQL92 subscription carries the expression text with the tag set
    // filled in: the exact-tag step has to stand down for it.
    (c.subscription.get(TOPIC) as any).classFilterMode = true;
  }
  if (opts.maxReconsumeTimes !== undefined) c.setMaxReconsumeTimes(opts.maxReconsumeTimes);
  if (opts.threshold !== undefined) c.setPopThresholdForQueue(opts.threshold);
  for (const hook of opts.hooks ?? []) c.registerFilterMessageHook(hook);
  const delivered: MessageExt[] = [];
  c.registerMessageListener({
    consumeMessage: (msgs: MessageExt[]) => {
      delivered.push(...msgs);
      return opts.listenerStatus ?? ConsumeConcurrentlyStatus.RECONSUME_LATER;
    },
  });
  await c.start();
  await waitFor(() => broker.popsFor(TOPIC).length >= 1);
  const settleOk = await settle(broker, opts.expectedRpc);
  return { broker, c, delivered, settleOk };
}

function offsetsOf(list: MessageExt[]): string[] {
  return list.map((m) => String(m.getQueueOffset())).sort();
}
function sameSet(actual: string[], expected: number[]): boolean {
  const e = expected.map(String).sort();
  return actual.length === e.length && actual.every((v, i) => v === e[i]);
}

// pullPathRunsTheBudgetDrop asks the pull result processor the same question the
// POP round asks: does a row that is past the retry budget get dropped here?
function pullPathKeepsPastBudgetRow(expression: string): { kept: string[], droppedTagged: boolean } {
  const c = new DefaultMQPushConsumer('GID_pop_filter_pull_control');
  c.subscribe(TOPIC, expression);
  const sub = c.subscription.get(TOPIC)!;
  const mq = new MessageQueue(TOPIC, BROKER, 0);
  const rows = [row(500, 'TagA', 99), row(501, 'TagB', 0)];
  const result = new PullResult(PullStatus.FOUND, 0, 0, 0, rows, 0);
  const out = (c as any)._processPullResult(mq, result, sub) as PullResult;
  const kept = (out.msgFoundList ?? []).map((m) => String(m.getQueueOffset()));
  return { kept, droppedTagged: !kept.includes('501') };
}

class ThrowingHook extends FilterMessageHook {
  filterMessage(_ctx: FilterMessageContext): any[] | null {
    throw new Error('this hook always blows up');
  }
}

class DropFirstHook extends FilterMessageHook {
  filterMessage(ctx: FilterMessageContext): any[] | null {
    // The hook contract: it rewrites the list it was handed, and whatever it
    // removes is no longer the listener's problem.
    ctx.msgList = (ctx.msgList ?? []).slice(1);
    return ctx.msgList;
  }
}

async function main() {
  // ---- 1. the five-row stage: what is delivered, what is acked, what is booked
  {
    const s = await stage({
      group: 'GID_pop_filter_stage', rows: FIVE_ROWS(), maxReconsumeTimes: 2,
      expectedRpc: 5,
    });
    check('the round reaches the broker', s.settleOk && s.broker.popsFor(TOPIC).length >= 1);
    check('only the tag-matched rows inside the budget reach the listener',
      sameSet(offsetsOf(s.delivered), [100, 103]), offsetsOf(s.delivered).join(','));
    check('the removed rows are ACKed on the wire, each with its own checkpoint offset',
      sameSet(s.broker.acks, [101, 102, 104]), s.broker.acks.join(','));
    check('the delivered rows are held longer instead, never acked by the filter',
      sameSet(s.broker.changes, [100, 103]), s.broker.changes.join(','));
    check('every drop-ack names the topic, queue and group the row was popped under',
      s.broker.ackHeaders.every((h) => h['topic'] === TOPIC && h['queueId'] === '0'
        && h['consumerGroup'] === 'GID_pop_filter_stage' && h['bname'] === BROKER
        && String(h['extraInfo'] || '').split(' ').length === 8),
      JSON.stringify(s.broker.ackHeaders[0]));
    const stats = (s.c as any)._statsManager;
    const pullTps = stats.topicAndGroupPullTPS.find(`${TOPIC}@GID_pop_filter_stage`);
    check('the round books only what it delivered as pulled messages',
      pullTps != null && pullTps.value === 2, `pullTPS=${pullTps ? pullTps.value : 'none'}`);
    const pullRt = stats.topicAndGroupPullRT.find(`${TOPIC}@GID_pop_filter_stage`);
    check('and the round still counts as one pull for the latency figure',
      pullRt != null && pullRt.times >= 1, `pullRT times=${pullRt ? pullRt.times : 'none'}`);
    const pq = s.c.popQueueTable.get(mqKey(new MessageQueue(TOPIC, BROKER, 0)));
    check('the queue\'s answer debt holds only the delivered rows, and settles to zero',
      pq != null && pq.waitAckMsgCount() === 0, `debt=${pq ? pq.waitAckMsgCount() : 'no queue'}`);
    await s.c.shutdown();
    s.broker.close();
  }

  // ---- 2. SUB_ALL: the exact-tag step stands down, the budget step still acts
  {
    const s = await stage({
      group: 'GID_pop_filter_suball', expression: FilterAPI.SUB_ALL, maxReconsumeTimes: 2,
      rows: [row(300, 'TagA', 0), row(301, 'TagB', 0), row(302, 'TagB', 3)],
      expectedRpc: 3,
    });
    check('with a tag set of "*": every tag reaches the listener except a spent one',
      sameSet(offsetsOf(s.delivered), [300, 301]), offsetsOf(s.delivered).join(','));
    check('and the only wire ack is the row past the budget',
      sameSet(s.broker.acks, [302]), s.broker.acks.join(','));
    await s.c.shutdown();
    s.broker.close();
  }

  // ---- 3. class/SQL92 expression: the exact-tag step stands down as well
  {
    const s = await stage({
      group: 'GID_pop_filter_classmode', expression: 'TagA', classFilter: true,
      maxReconsumeTimes: 2, rows: [row(400, 'TagB', 0)], expectedRpc: 1,
    });
    check('a class/SQL92 subscription delivers a row whose tag is not in the set',
      sameSet(offsetsOf(s.delivered), [400]), offsetsOf(s.delivered).join(','));
    check('and puts no ack on the wire for it',
      s.broker.acks.length === 0, s.broker.acks.join(','));
    await s.c.shutdown();
    s.broker.close();
  }

  // ---- 4. a hook that throws costs the round nothing
  {
    const s = await stage({
      group: 'GID_pop_filter_hook_throws', maxReconsumeTimes: 2, rows: FIVE_ROWS(),
      hooks: [new ThrowingHook()], expectedRpc: 5,
    });
    check('a throwing filter hook still delivers the same rows',
      sameSet(offsetsOf(s.delivered), [100, 103]), offsetsOf(s.delivered).join(','));
    check('and still acks exactly the rows the tag and budget steps removed',
      sameSet(s.broker.acks, [101, 102, 104]), s.broker.acks.join(','));
    await s.c.shutdown();
    s.broker.close();
  }

  // ---- 5. a hook that drops a row hands it to the same ack path
  {
    const s = await stage({
      group: 'GID_pop_filter_hook_drops', maxReconsumeTimes: 2, rows: FIVE_ROWS(),
      hooks: [new DropFirstHook()], expectedRpc: 5,
    });
    check('the row a filter hook removed is not delivered',
      sameSet(offsetsOf(s.delivered), [103]), offsetsOf(s.delivered).join(','));
    check('and it is acked like any other removal',
      sameSet(s.broker.acks, [100, 101, 102, 104]), s.broker.acks.join(','));
    const stats = (s.c as any)._statsManager;
    const pullTps = stats.topicAndGroupPullTPS.find(`${TOPIC}@GID_pop_filter_hook_drops`);
    check('with a hook-only removal, the round books one delivered message',
      pullTps != null && pullTps.value === 1, `pullTPS=${pullTps ? pullTps.value : 'none'}`);
    await s.c.shutdown();
    s.broker.close();
  }

  // ---- 6. a whole FOUND answer filtered away: acked, booked as nothing
  {
    const s = await stage({
      group: 'GID_pop_filter_all_out', maxReconsumeTimes: 2,
      rows: [row(200, 'TagB', 0), row(201, null, 5)], expectedRpc: 2,
      threshold: 1,
    });
    check('nothing of a fully filtered answer reaches the listener',
      s.delivered.length === 0, offsetsOf(s.delivered).join(','));
    check('and every row of it is acked on the wire',
      sameSet(s.broker.acks, [200, 201]), s.broker.acks.join(','));
    const pq = s.c.popQueueTable.get(mqKey(new MessageQueue(TOPIC, BROKER, 0)));
    check('a filtered-away answer leaves no answer owed on the queue',
      pq != null && pq.waitAckMsgCount() === 0, `debt=${pq ? pq.waitAckMsgCount() : 'no queue'}`);
    check('so the queue never trips its own debt gate',
      s.c.flowControlTriggered === 0, `flowControlTriggered=${s.c.flowControlTriggered}`);
    const stats = (s.c as any)._statsManager;
    const pullTps = stats.topicAndGroupPullTPS.find(`${TOPIC}@GID_pop_filter_all_out`);
    check('and the round is booked with no pulled messages at all',
      pullTps == null || pullTps.value === 0, `pullTPS=${pullTps ? pullTps.value : 'none'}`);
    await s.c.shutdown();
    s.broker.close();
  }

  // ---- 7. the budget default: -1 means 16 here, so 16 is delivered, 17 is not
  {
    const s = await stage({
      group: 'GID_pop_filter_default16', maxReconsumeTimes: -1,
      rows: [row(600, 'TagA', 16), row(601, 'TagA', 17)], expectedRpc: 2,
    });
    check('with the budget unset, a row on the sixteenth attempt is still delivered',
      sameSet(offsetsOf(s.delivered), [600]), offsetsOf(s.delivered).join(','));
    check('and the seventeenth is acked away',
      sameSet(s.broker.acks, [601]), s.broker.acks.join(','));
    await s.c.shutdown();
    s.broker.close();
  }

  // ---- 8. the pull path keeps the exact-tag check and none of the budget drop
  {
    const withTags = pullPathKeepsPastBudgetRow('TagA');
    check('the pull path still applies the exact-tag check',
      withTags.droppedTagged, `kept=${withTags.kept.join(',')}`);
    check('control: the pull path does NOT drop a row for being past the retry budget',
      withTags.kept.includes('500'), `kept=${withTags.kept.join(',')}`);
    const subAll = pullPathKeepsPastBudgetRow(FilterAPI.SUB_ALL);
    check('control: with "*" the pull path hands over both rows untouched',
      subAll.kept.length === 2 && !subAll.droppedTagged, `kept=${subAll.kept.join(',')}`);
  }

  console.log(`\npop pre-delivery filter: ${pass} checks, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
