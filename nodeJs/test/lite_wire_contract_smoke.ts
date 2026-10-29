// Lite pull wire contract — offline test against a fake broker.
//
// Two behaviours are pinned here, both invisible to the in-memory facades:
//   * the LITE_PULL_MESSAGE(361) request carries the consumer's configured
//     broker-suspend budget, so the knob is live rather than only validated at
//     start(), while the suspend bit stays clear (a synchronous lite round asks
//     for a short poll);
//   * the heartbeat announces the set an application built for it, and a later
//     build that leaves a topic out stops announcing it — the broker filters a
//     group's pulls by the expressions it was last told.
//
// Run: node --experimental-strip-types test/lite_wire_contract_smoke.ts
import net from 'node:net';

import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { MessageSelector } from '../src/client/consumer_result.ts';
import { MessageQueue } from '../src/common/message.ts';
import { PullSysFlag } from '../src/common/sysflag.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { ConsumeFromWhere, HeartbeatData } from '../src/remoting/heartbeat.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

function sleep(ms: number): Promise<void> { return new Promise((r) => setTimeout(r, ms)); }
async function waitFor(cond: () => boolean, millis = 5000): Promise<boolean> {
  const deadline = Date.now() + millis;
  while (Date.now() < deadline) {
    if (cond()) return true;
    await sleep(20);
  }
  return cond();
}

const BROKER = 'liteWireBroker';
const TOPIC = 'LiteWireTopic';

type Beat = { group: string; subs: Array<{ topic: string; expr: string }> };

class FakeBroker {
  server: net.Server;
  port = 0;
  pullHeaders: Array<Record<string, string>> = [];
  beats: Beat[] = [];

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

  // lastBeatSubs is the newest heartbeat's subscription table, topic -> expression.
  lastBeatSubs(group: string): Map<string, string> {
    const own = this.beats.filter((b) => b.group === group);
    const out = new Map<string, string>();
    if (own.length === 0) return out;
    for (const sub of own[own.length - 1].subs) out.set(sub.topic, sub.expr);
    return out;
  }

  private onConnection(sock: net.Socket) {
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

  private respond(req: RemotingCommand): RemotingCommand | null {
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
    resp.opaque = req.opaque;
    resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
    const ext = req.extFields || {};

    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      resp.body = Buffer.from(JSON.stringify({
        orderTopicConf: null,
        queueDatas: [{
          brokerName: BROKER, readQueueNums: 1, writeQueueNums: 1, perm: 6, topicSysFlag: 0,
        }],
        brokerDatas: [{
          cluster: 'DefaultCluster', brokerName: BROKER,
          brokerAddrs: { '0': this.addr }, zoneName: null, enableActingMaster: false,
        }],
        filterServerTable: {},
      }), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.HEART_BEAT) {
      const hb = HeartbeatData.decode(req.body as Buffer);
      for (const cd of hb.consumerDataSet) {
        this.beats.push({
          group: cd.groupName,
          subs: cd.subscriptionDataSet.map((s) => ({ topic: s.topic, expr: s.subString })),
        });
      }
      return resp;
    }

    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: [] }), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.LITE_PULL_MESSAGE) {
      this.pullHeaders.push({ ...ext });
      // NO_NEW_MSG: the broker has nothing to hand out, and the offset stays put
      // so the consumer does not rewind.
      resp.code = ResponseCode.PULL_NOT_FOUND;
      resp.extFields = { nextBeginOffset: String(ext['queueOffset'] || '0') };
      return resp;
    }

    return resp;
  }

  close() { this.server.close(); }
}

function lite(group: string, addr: string): DefaultLitePullConsumer {
  const c = new DefaultLitePullConsumer(group);
  c.setNamesrvAddr(addr);
  return c;
}

async function main() {
  const broker = new FakeBroker();
  await broker.listen();
  try {
    // ---------------------------------------------------------- 1. the wire
    const c = lite('GID_lite_wire', broker.addr);
    // FIRST_OFFSET places the queue without a boundary RPC, so the first thing
    // the fake broker sees from this consumer is the pull itself.
    c.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    c.setBrokerSuspendMaxTimeMillis(8000);
    c.setConsumerTimeoutMillisWhenSuspend(20000);
    c.assign([new MessageQueue(TOPIC, BROKER, 0)]);
    // An assigned topic pulls with the expression registered for it.
    c.setSubExpressionForAssign(TOPIC, 'TagA');
    c.buildSubscriptionsForHeartbeat(new Map<string, MessageSelector>([
      ['A', MessageSelector.TAG('TagA')],
      ['B', MessageSelector.SQL92('a > 1')],
    ]));
    await c.start();
    // The lite round is driven by poll(): it drains within the deadline.
    await c.poll(1000);

    check('a lite pull went out', broker.pullHeaders.length > 0,
      String(broker.pullHeaders.length));
    const head = broker.pullHeaders[0] || {};
    check('LITE_PULL carried the configured suspend budget',
      head['suspendTimeoutMillis'] === '8000', head['suspendTimeoutMillis']);
    check('the suspend bit stays clear on a short poll',
      !PullSysFlag.hasSuspendFlag(parseInt(head['sysFlag'] || '0', 10)), head['sysFlag']);

    check('the heartbeat announced both built topics', await waitFor(() => {
      const subs = broker.lastBeatSubs('GID_lite_wire');
      return subs.get('A') === 'TagA' && subs.get('B') === 'a > 1';
    }), JSON.stringify([...broker.lastBeatSubs('GID_lite_wire')]));

    // A rebuild replaces the whole set: B is not announced any more.
    c.buildSubscriptionsForHeartbeat(new Map<string, MessageSelector>([
      ['A', MessageSelector.TAG('*')],
    ]));
    await c.mqClient!.sendHeartbeatToAllBrokers();
    check('the rebuilt beat drops the topic left out', await waitFor(() => {
      const subs = broker.lastBeatSubs('GID_lite_wire');
      return subs.get('A') === '*' && !subs.has('B');
    }), JSON.stringify([...broker.lastBeatSubs('GID_lite_wire')]));
    check('the snapshot lists the rebuilt set in order',
      c.getSubscriptionsForHeartbeat().map((s) => s.topic).join(',') === 'A');
    await c.shutdown();
  } finally {
    broker.close();
  }

  // -------------------------------------------------- 2. the assignment seam
  {
    const u = lite('GID_lite_wire_unit', '127.0.0.1:1');
    u.subscribe('T', 'TagA');
    u.buildSubscriptionsForHeartbeat(new Map<string, MessageSelector>([
      ['T', MessageSelector.TAG('TagZ')],
      ['X', MessageSelector.TAG('TagX')],
    ]));
    const subs = u.subscriptionDataSet.map((s) => `${s.topic}=${s.subString}`);
    check('a subscribed topic keeps the rebalance expression', subs.includes('T=TagA'), subs.join(','));
    check('a topic only the built set covers is announced',
      subs.includes('X=TagX') && subs.length === 2, subs.join(','));

    let msg = '';
    try {
      u.buildSubscriptionsForHeartbeat(new Map<string, MessageSelector>([['', MessageSelector.TAG('TagA')]]));
    } catch (e) { msg = (e as Error).message; }
    check('an empty topic is refused', msg.includes('Topic can not be null'), msg);
    msg = '';
    try {
      u.buildSubscriptionsForHeartbeat(new Map<string, MessageSelector>([['X', MessageSelector.SQL92('')]]));
    } catch (e) { msg = (e as Error).message; }
    check('an empty non-TAG expression is refused', msg.includes("Expression can't be null"), msg);
    check('a refused build leaves the set intact',
      u.getSubscriptionsForHeartbeat().map((s) => s.topic).join(',') === 'T,X');

    // A topic assigned in ASSIGN mode announces nothing until the application
    // opts in — the control that keeps the wire assertion from being vacuous.
    const a = lite('GID_lite_wire_assign', '127.0.0.1:1');
    a.assign([new MessageQueue('T', BROKER, 0)]);
    check('an assign-mode consumer announces nothing by default',
      a.subscriptionDataSet.length === 0, String(a.subscriptionDataSet.length));
    a.buildSubscriptionsForHeartbeat(new Map<string, MessageSelector>([['T', MessageSelector.TAG('TagA')]]));
    check('the opt-in set is what the assign-mode heartbeat announces',
      a.subscriptionDataSet.map((s) => `${s.topic}=${s.subString}`).join(',') === 'T=TagA');
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  if (fail > 0) process.exit(1);
}

main().catch((e) => { console.error(e); process.exit(1); });
