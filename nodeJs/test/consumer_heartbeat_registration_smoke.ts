// Push-consumer heartbeat registration — offline wire test.
//
// Behaviour baseline (what a broker does with the frames this client sends):
//   * start() must beat every broker of the route BEFORE the first
//     GET_CONSUMER_LIST_BY_GROUP(38). Rebalance asks the broker who belongs to
//     the group, and a broker that has never seen this clientId hands out
//     nothing. The instance-level beat cannot cover this window: it fires while
//     the broker table is still empty (so nothing leaves) and its next tick is
//     30s later — a consumer that skips its own first beat is only discovered
//     one full beat cycle after start().
//   * The consumer beat fans out to the slave as well as the master; only the
//     master answers the member list here, so the slave leg is proved from the
//     frames, not from an allocation.
//   * The beat carries the subscription sets (the topic tag expression plus the
//     implicit retry topic), which is what lets the broker filter for it.
//   * The facade keeps its own 30s beat loop so the registration survives a
//     broker restart between rebalances.
//   * sendHeartbeatNow() on a facade with no instance is a silent no-op.
//
// The fake broker answers 38 from the clientIds its HEART_BEAT bodies named,
// which is exactly how a broker's consumer manager builds that list. So "this
// consumer got 4 queues" is only reachable through the pre-rebalance beat, and
// the same table answers EMPTY for a group that never beat — the control that
// keeps the allocation assertion from being vacuous.
//
// Run: node --experimental-strip-types test/consumer_heartbeat_registration_smoke.ts
import net from 'node:net';

import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { HeartbeatData } from '../src/remoting/heartbeat.ts';
import { RemotingSerializable } from '../src/remoting/serialize.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

function sleep(ms: number): Promise<void> { return new Promise((r) => setTimeout(r, ms)); }
async function waitFor(cond: () => boolean, millis = 6000): Promise<boolean> {
  const deadline = Date.now() + millis;
  while (Date.now() < deadline) {
    if (cond()) return true;
    await sleep(20);
  }
  return cond();
}

const BROKER = 'hbRegBroker';
const TOPIC = 'HbRegTopic';
const GROUP = 'GID_hb_reg';
const TAGS = 'tagA||tagB';

type Beat = {
  clientID: string;
  group: string;
  withoutSub: boolean;
  fingerprint: number;
  subs: Array<{ topic: string; expr: string }>;
};

// One listening node: name server, master or slave. Which role it plays only
// changes what it is asked; every frame lands in the shared `order` log so the
// test can reason about cross-node request sequencing.
class FakeNode {
  role: string;
  order: Array<{ code: number; role: string }>;
  server: net.Server;
  port = 0;
  // Routed topics: topic -> { queues, brokerAddrs }.
  routes = new Map<string, { queues: number; addrs: Record<string, string> }>();
  beats: Beat[] = [];
  // group -> clientId -> subscriptions, the table a consumer manager keeps.
  members = new Map<string, Map<string, Array<{ topic: string; expr: string }>>>();
  // Every member list handed out, in order, for the ordering assertions.
  lists: string[][] = [];
  unregistered: string[] = [];

  constructor(role: string, order: Array<{ code: number; role: string }>) {
    this.role = role;
    this.order = order;
    this.server = net.createServer((s) => this.onConnection(s));
  }

  listen(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        this.port = (this.server.address() as net.AddressInfo).port;
        resolve();
      });
    });
  }

  get addr() { return `127.0.0.1:${this.port}`; }

  beatCount(group: string): number {
    return this.beats.filter((b) => b.group === group).length;
  }
  membersOf(group: string): string[] {
    const m = this.members.get(group);
    return m ? Array.from(m.keys()) : [];
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
    this.order.push({ code: req.code, role: this.role });
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
    resp.opaque = req.opaque;
    resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
    const ext = req.extFields || {};

    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const route = this.routes.get(String(ext['topic'] || ''));
      if (route == null) { resp.code = ResponseCode.TOPIC_NOT_EXIST; return resp; }
      resp.body = Buffer.from(JSON.stringify({
        orderTopicConf: null,
        queueDatas: [{
          brokerName: BROKER, readQueueNums: route.queues, writeQueueNums: route.queues,
          perm: 6, topicSysFlag: 0,
        }],
        brokerDatas: [{
          cluster: 'DefaultCluster', brokerName: BROKER, brokerAddrs: route.addrs,
          zoneName: null, enableActingMaster: false,
        }],
        filterServerTable: {},
      }), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.HEART_BEAT) {
      const hb = HeartbeatData.decode(req.body as Buffer);
      for (const cd of hb.consumerDataSet) {
        this.beats.push({
          clientID: hb.clientID,
          group: cd.groupName,
          withoutSub: hb.withoutSub,
          fingerprint: hb.heartbeatFingerprint,
          subs: cd.subscriptionDataSet.map((s) => ({ topic: s.topic, expr: s.subString })),
        });
        let byClient = this.members.get(cd.groupName);
        if (byClient == null) { byClient = new Map(); this.members.set(cd.groupName, byClient); }
        byClient.set(hb.clientID, cd.subscriptionDataSet.map((s) => ({ topic: s.topic, expr: s.subString })));
      }
      return resp;
    }

    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      const ids = this.membersOf(String(ext['consumerGroup'] || ''));
      this.lists.push(ids);
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: ids }), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.UNREGISTER_CLIENT) {
      const group = String(ext['consumerGroup'] || '');
      const clientId = String(ext['clientID'] || '');
      const byClient = this.members.get(group);
      if (byClient != null) byClient.delete(clientId);
      this.unregistered.push(`${group}@${clientId}`);
      return resp;
    }

    if (req.code === RequestCode.QUERY_CONSUMER_OFFSET) {
      // Nothing committed yet: the broker says so and the consumer falls back to
      // CONSUME_FROM_LAST_OFFSET, which asks for the max offset.
      resp.code = ResponseCode.QUERY_NOT_FOUND;
      return resp;
    }

    if (req.code === RequestCode.GET_MAX_OFFSET || req.code === RequestCode.GET_MIN_OFFSET
      || req.code === RequestCode.SEARCH_OFFSET_BY_TIMESTAMP) {
      resp.addExtField('offset', '10');
      return resp;
    }

    if (req.code === RequestCode.PULL_MESSAGE || req.code === RequestCode.LITE_PULL_MESSAGE) {
      resp.code = ResponseCode.PULL_NOT_FOUND;
      resp.addExtField('nextBeginOffset', String(Number(ext['queueOffset'] || 0)));
      resp.addExtField('minOffset', '0');
      resp.addExtField('maxOffset', '10');
      resp.addExtField('suggestWhichBrokerId', '0');
      return resp;
    }

    return resp; // offset commits, transaction and reply frames: accepted as-is
  }

  close() { this.server.close(); }
}

function indexOfFirst(order: Array<{ code: number; role: string }>, code: number, role?: string): number {
  return order.findIndex((e) => e.code === code && (role == null || e.role === role));
}
function countBefore(order: Array<{ code: number; role: string }>, end: number, code: number, role: string): number {
  let n = 0;
  for (let i = 0; i < end; i++) if (order[i].code === code && order[i].role === role) n++;
  return n;
}

async function main() {
  const order: Array<{ code: number; role: string }> = [];
  const ns = new FakeNode('ns', order);
  const master = new FakeNode('master', order);
  const slave = new FakeNode('slave', order);
  await Promise.all([ns.listen(), master.listen(), slave.listen()]);
  ns.routes.set(TOPIC, { queues: 4, addrs: { '0': master.addr, '1': slave.addr } });

  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(ns.addr);
  consumer.setInstanceName('hb_reg');
  // Long-poll misses must not spin the pull loops against the fake broker.
  consumer.setPullInterval(500);
  consumer.subscribe(TOPIC, TAGS);
  consumer.registerMessageListener(() => ConsumeConcurrentlyStatus.CONSUME_SUCCESS);

  try {
    await consumer.start();
    const client = consumer.mqClient!;

    // ------------------------------------ 1. the beat lands before the member list
    const firstList = indexOfFirst(order, RequestCode.GET_CONSUMER_LIST_BY_GROUP, 'master');
    const firstBeat = indexOfFirst(order, RequestCode.HEART_BEAT);
    check('start() beats the brokers before asking for the group member list',
      firstBeat >= 0 && firstList >= 0 && firstBeat < firstList,
      `firstHeartbeat=${firstBeat} firstList=${firstList}`);
    check('that first beat round reaches the master AND the slave',
      countBefore(order, firstList, RequestCode.HEART_BEAT, 'master') >= 1
      && countBefore(order, firstList, RequestCode.HEART_BEAT, 'slave') >= 1,
      `master=${countBefore(order, firstList, RequestCode.HEART_BEAT, 'master')} `
      + `slave=${countBefore(order, firstList, RequestCode.HEART_BEAT, 'slave')}`);

    // ------------------------------------------------ 2. the beat is what registers
    check('the member list that fed rebalance already contained this clientId',
      master.lists.length > 0 && master.lists[0].includes(consumer.clientID),
      `clientId=${consumer.clientID} firstList=${JSON.stringify(master.lists[0])}`);
    check('so the consumer is assigned every queue of the topic at start()',
      consumer.assigned.length === 4, `assigned=${consumer.assigned.length}`);

    // ---------------------------------- 3. the body carries the subscription sets
    const own = master.beats.filter((b) => b.group === GROUP && b.clientID === consumer.clientID);
    check('the beat names this consumer group with its subscriptions attached',
      own.length > 0 && own[0].subs.some((s) => s.topic === TOPIC && s.expr === TAGS),
      JSON.stringify(own[0] && own[0].subs));
    check('the implicit retry topic is part of the reported subscription set',
      own.length > 0 && own[0].subs.some((s) => s.topic === `%RETRY%${GROUP}`),
      JSON.stringify(own[0] && own[0].subs));
    check('a V1 beat carries no fingerprint and never drops the subscriptions',
      own.every((b) => b.fingerprint === 0 && b.withoutSub === false),
      JSON.stringify(own.map((b) => [b.fingerprint, b.withoutSub])));

    // --------------------------------------------------------- 4. paired controls
    // The same table answers EMPTY for a group that never beat: the allocation in
    // step 2 can therefore only come from a heartbeat, not from a canned answer.
    const ghost = await client.getConsumerListByGroup(master.addr, 'GID_hb_reg_ghost');
    const ghostIds = (RemotingSerializable.decode(ghost.body as Buffer) as any)['consumerIdList'];
    check('control: the same broker answers an empty list for a group that never beat',
      Array.isArray(ghostIds) && ghostIds.length === 0, JSON.stringify(ghostIds));
    // The slave holds the registration too — a slave that never got the beat would
    // answer the pull with "subscription not exist".
    check('the slave holds this group as well', slave.membersOf(GROUP).includes(consumer.clientID),
      JSON.stringify(slave.membersOf(GROUP)));

    // --------------------------------- 5. sendHeartbeatNow() beats every broker
    // start() beats twice over: the registration beat and the beat that the rebalance
    // round which just took the queues owes the brokers. The second one is
    // fire-and-forget, so a delta read too early would count it against this call.
    // Each round reaches every broker of the table and the master is beaten first, so
    // equal counts mean no round is still in flight.
    const settled = await waitFor(() => master.beatCount(GROUP) === slave.beatCount(GROUP), 3000);
    check('every beat round start() owes reaches both the master and the slave', settled,
      `master=${master.beatCount(GROUP)} slave=${slave.beatCount(GROUP)}`);
    const beforeMaster = master.beatCount(GROUP);
    const beforeSlave = slave.beatCount(GROUP);
    await consumer.sendHeartbeatNow();
    check('sendHeartbeatNow() issues one beat round over the whole broker table',
      master.beatCount(GROUP) === beforeMaster + 1 && slave.beatCount(GROUP) === beforeSlave + 1,
      `master=${beforeMaster}->${master.beatCount(GROUP)} slave=${beforeSlave}->${slave.beatCount(GROUP)}`);

    // ------------------------------------------ 6. the 30s loop is a live loop
    // Only the loop's own 30s tick is accelerated; every other sleep (pull backoff,
    // rebalance, offset flush) keeps its real period.
    const anyC = consumer as any;
    const realSleep = anyC._sleepOrStop.bind(anyC);
    const flagsBefore = new Set<string>(anyC._stopFlags);
    const beatsBefore = master.beatCount(GROUP);
    anyC._sleepOrStop = (ms: number, flag?: string) =>
      (ms === 30000 ? sleep(5).then(() => false) : realSleep(ms, flag));
    const loopDone = anyC._heartbeatLoop();
    const loopFlags = Array.from(anyC._stopFlags).filter((f: string) => !flagsBefore.has(f));
    const looped = await waitFor(() => master.beatCount(GROUP) >= beatsBefore + 2, 5000);
    // Retire the loop the way a revoked queue does: drop its stop flag.
    for (const f of loopFlags) anyC._stopFlags.delete(f);
    await loopDone;
    anyC._sleepOrStop = realSleep;
    check('the consumer heartbeat loop keeps beating while the consumer runs',
      looped && master.beatCount(GROUP) >= beatsBefore + 2,
      `before=${beatsBefore} after=${master.beatCount(GROUP)}`);
  } finally {
    await consumer.shutdown().catch(() => {});
  }

  // ------------------------------------------- 7. shutdown unregisters it again
  check('shutdown removes the client from the master and the slave tables',
      !master.membersOf(GROUP).includes(consumer.clientID)
      && !slave.membersOf(GROUP).includes(consumer.clientID),
    `master=${JSON.stringify(master.membersOf(GROUP))} slave=${JSON.stringify(slave.membersOf(GROUP))}`);

  // ------------------------- 8. a facade with no instance beats nothing (guard)
  {
    const bare = new DefaultMQPushConsumer('GID_hb_reg_bare');
    bare.subscribe(TOPIC, TAGS);
    bare.registerMessageListener(() => ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
    const framesBefore = order.length;
    let threw = '';
    try { await bare.sendHeartbeatNow(); } catch (e) { threw = (e as Error).message; }
    check('sendHeartbeatNow() on a not-started consumer is a silent no-op',
      bare.mqClient === null && threw === '' && order.length === framesBefore,
      `threw=${threw} frames=${order.length - framesBefore}`);
  }

  ns.close(); master.close(); slave.close();

  console.log(`\n== ${pass}/${pass + fail} passed ==`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
