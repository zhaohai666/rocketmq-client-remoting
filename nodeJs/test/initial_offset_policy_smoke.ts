// Where a consumer with no committed offset starts — offline wire test.
//
// Behaviour baseline, for BOTH the push and the lite pull consumer:
//   * A stored offset always wins; the policy only places a group that has
//     never committed one.
//   * CONSUME_FROM_FIRST_OFFSET starts at literal 0 and asks the broker for no
//     boundary at all (an overrun is healed by the OFFSET_ILLEGAL path).
//   * CONSUME_FROM_TIMESTAMP asks for the offset of the configured instant —
//     the instant parsed from consumeTimestamp, not "now" — except on a
//     %RETRY% queue, which is created by the first redelivery long after that
//     instant and therefore starts at the queue tail.
//   * CONSUME_FROM_LAST_OFFSET (and the deprecated *OVERMITTED aliases) skips
//     the backlog on a business queue but replays a %RETRY% queue from 0:
//     retried messages must all be retried.
//   * consumeTimestamp is parsed unconditionally at start-up. A malformed value
//     is a hard failure — falling back to "now" would silently turn the policy
//     into "skip the backlog".
//   * A store read that FAILS is not "nothing committed": the queue gets no
//     pull loop for that round, the read is retried on the next rebalance, and
//     the failed read never falls through to the tail.
//   * The default instant is boot time minus 30 minutes.
//
// Each "which RPC drove the start" claim is decided by what the fake broker
// actually sees on the socket, with a distinct offset per boundary RPC
// (tail=100, searched=200, min=50) so a wrong branch cannot pass by accident.
//
// Run: node --experimental-strip-types test/initial_offset_policy_smoke.ts
import net from 'node:net';

import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MixAll } from '../src/common/mixAll.ts';
import {
  defaultConsumeTimestamp, parseConsumeTimestamp, timeMillisToHumanString3,
} from '../src/common/utilAll.ts';
import { ConsumeFromWhere } from '../src/remoting/heartbeat.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'InitOffsetTopic';
const BROKER = 'b1';
const TAIL = 100;
const SEARCHED = 200;
const HEAD = 50;
const STORED = 7;
const CONFIGURED = '20230101000000';

class FakeCluster {
  server: net.Server;
  port = 0;
  queues = new Map<string, number>();
  cidList: string[] = [];
  // -1 makes QUERY_CONSUMER_OFFSET answer QUERY_NOT_FOUND: the group has
  // never committed, so the policy decides.
  storedOffset = -1;
  // offsetReadFails makes that same read answer SYSTEM_ERROR: the store exists
  // but cannot answer, which is NOT "nothing committed".
  offsetReadFails = false;
  offsetReadAttempts = 0;
  pulls: Array<{ topic: string; queueId: number; offset: number }> = [];
  searchTimestamps: number[] = [];
  minOffsetCalls = 0;
  maxOffsetCalls = 0;

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

  firstPullOffset(topic: string): number | undefined {
    const p = this.pulls.find((x) => x.topic === topic);
    return p ? p.offset : undefined;
  }
  pulledTopics(): string[] {
    const s = new Set<string>();
    for (const p of this.pulls) s.add(p.topic);
    return [...s];
  }
  searchedAt(): number | undefined {
    return this.searchTimestamps.length > 0 ? this.searchTimestamps[0] : undefined;
  }

  private routeBody(topic: string, n: number): string {
    return JSON.stringify({
      orderTopicConf: null,
      queueDatas: [{
        brokerName: BROKER, readQueueNums: n, writeQueueNums: n, perm: 6, topicSysFlag: 0,
      }],
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
    sock.on('error', () => { /* client hangs up on teardown */ });
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
      // An empty group list assigns nothing: the consumer would keep its old
      // (empty) slice and never pull. Record the beat's clientID so the group
      // really has one member, the way a live broker would.
      try {
        const hb = JSON.parse(req.body ? req.body.toString('utf8') : '{}');
        // Deduplicated: a repeated beat must not split the group into two
        // members and hand half of the queues to a phantom.
        if (hb.clientID && !this.cidList.includes(String(hb.clientID))) {
          this.cidList.push(String(hb.clientID));
        }
      } catch { /* an unparseable beat simply leaves the group empty */ }
      return resp;
    }
    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: this.cidList }), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.QUERY_CONSUMER_OFFSET) {
      this.offsetReadAttempts++;
      if (this.offsetReadFails) { resp.code = ResponseCode.SYSTEM_ERROR; return resp; }
      if (this.storedOffset < 0) { resp.code = ResponseCode.QUERY_NOT_FOUND; return resp; }
      resp.extFields['offset'] = String(this.storedOffset);
      return resp;
    }
    if (req.code === RequestCode.GET_MAX_OFFSET) {
      this.maxOffsetCalls++;
      resp.extFields['offset'] = String(TAIL);
      return resp;
    }
    if (req.code === RequestCode.GET_MIN_OFFSET) {
      this.minOffsetCalls++;
      resp.extFields['offset'] = String(HEAD);
      return resp;
    }
    if (req.code === RequestCode.SEARCH_OFFSET_BY_TIMESTAMP) {
      this.searchTimestamps.push(parseInt(ext['timestamp'] || '-1', 10));
      resp.extFields['offset'] = String(SEARCHED);
      return resp;
    }
    if (req.code === RequestCode.PULL_MESSAGE || req.code === RequestCode.LITE_PULL_MESSAGE) {
      const offset = parseInt(ext['queueOffset'] || '-1', 10);
      this.pulls.push({ topic: ext['topic'] || '', queueId: parseInt(ext['queueId'] || '-1', 10), offset });
      resp.code = ResponseCode.PULL_NOT_FOUND;
      // Echo the request back so the consumer's cursor does not drift and the
      // first recorded offset stays the one produced by the policy.
      resp.extFields['nextBeginOffset'] = String(offset);
      resp.extFields['minOffset'] = String(HEAD);
      resp.extFields['maxOffset'] = String(TAIL);
      resp.extFields['suggestWhichBrokerId'] = '0';
      return resp;
    }
    return resp;
  }

  close() { this.server.close(); }
}

async function waitFor(cond: () => boolean, timeoutMs = 3000): Promise<boolean> {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (cond()) return true;
    await new Promise<void>((r) => setTimeout(r, 10));
  }
  return cond();
}

// A lite consumer subscribed to both the business topic and the group's retry
// topic; returns the cluster so the caller reads the starts off the wire.
async function liteRun(group: string, policy: string, opts:
  { consumeTimestamp?: string; storedOffset?: number } = {}): Promise<FakeCluster> {
  const cluster = new FakeCluster();
  await cluster.listen();
  if (opts.storedOffset !== undefined) cluster.storedOffset = opts.storedOffset;
  cluster.addTopic(TOPIC, 1);
  cluster.addTopic(MixAll.getRetryTopic(group), 1);
  const c = new DefaultLitePullConsumer(group);
  c.setNamesrvAddr(cluster.addr);
  c.setConsumeFromWhere(policy);
  if (opts.consumeTimestamp !== undefined) c.setConsumeTimestamp(opts.consumeTimestamp);
  c.subscribe(TOPIC, '*');
  c.subscribe(MixAll.getRetryTopic(group), '*');
  await c.start();
  await c.poll(200);
  await c.shutdown();
  return cluster;
}

// The same for the push consumer, which pulls from its own loops.
async function pushRun(group: string, policy: string, opts:
  { consumeTimestamp?: string; storedOffset?: number } = {}): Promise<FakeCluster> {
  const cluster = new FakeCluster();
  await cluster.listen();
  if (opts.storedOffset !== undefined) cluster.storedOffset = opts.storedOffset;
  cluster.addTopic(TOPIC, 1);
  cluster.addTopic(MixAll.getRetryTopic(group), 1);
  const c = new DefaultMQPushConsumer(group);
  c.setNamesrvAddr(cluster.addr);
  c.setConsumeFromWhere(policy);
  if (opts.consumeTimestamp !== undefined) c.setConsumeTimestamp(opts.consumeTimestamp);
  c.subscribe(TOPIC, '*');
  c.registerMessageListener({ consumeMessage: () => 0 });
  await c.start();
  const seen = await waitFor(() => cluster.pulledTopics().length >= 2);
  await c.shutdown();
  if (!seen) throw new Error('the push consumer never pulled both queues');
  return cluster;
}

async function main() {
  // ------------------------------------------------- 1. the instant itself
  {
    check('the configured instant round-trips through the formatter',
      timeMillisToHumanString3(parseConsumeTimestamp(CONFIGURED)) === CONFIGURED,
      timeMillisToHumanString3(parseConsumeTimestamp(CONFIGURED)));

    check('one second of the instant is one millisecond',
      parseConsumeTimestamp('20230101000001') - parseConsumeTimestamp(CONFIGURED) === 1000,
      String(parseConsumeTimestamp('20230101000001') - parseConsumeTimestamp(CONFIGURED)));

    const def = defaultConsumeTimestamp();
    check('the default instant is a legal yyyyMMddHHmmss', /^\d{14}$/.test(def), def);
    const ageMinutes = (Date.now() - parseConsumeTimestamp(def)) / 60000;
    check('and sits half an hour behind boot time',
      ageMinutes >= 29 && ageMinutes <= 31, String(ageMinutes));

    for (const bad of ['2023010100000', '', 'not-a-time', '2023-01-01 00:00:00', '202313010000001']) {
      let msg = '';
      try { parseConsumeTimestamp(bad); } catch (e) { msg = (e as Error).message; }
      check(`a malformed instant is rejected: "${bad}"`,
        msg.includes('consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received')
        && msg.endsWith(bad), msg);
    }
    let ok = true;
    try { parseConsumeTimestamp(CONFIGURED); } catch { ok = false; }
    check('the paired control: a legal instant is accepted', ok);
  }

  // ------------------------------- 2. lite: CONSUME_FROM_LAST_OFFSET
  {
    const cluster = await liteRun('GID_off_last', ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    check('lite LAST: a business queue skips the backlog',
      cluster.firstPullOffset(TOPIC) === TAIL, String(cluster.firstPullOffset(TOPIC)));
    check('lite LAST: a retry queue replays from 0',
      cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_last')) === 0,
      String(cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_last'))));
    check('lite LAST: no backlog search is issued', cluster.searchTimestamps.length === 0);
    cluster.close();
  }

  // ----------------------------- 3. lite: the deprecated LAST alias
  {
    const cluster = await liteRun('GID_off_last_alias', 'CONSUME_FROM_LAST_OFFSET_OVERMITTED');
    check('lite LAST_OVERMITTED: still skips the backlog',
      cluster.firstPullOffset(TOPIC) === TAIL, String(cluster.firstPullOffset(TOPIC)));
    check('lite LAST_OVERMITTED: still replays the retry queue from 0',
      cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_last_alias')) === 0,
      String(cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_last_alias'))));
    cluster.close();
  }

  // ------------------------------ 4. lite: CONSUME_FROM_FIRST_OFFSET
  {
    const cluster = await liteRun('GID_off_first', ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    check('lite FIRST: a business queue starts at 0 without asking for minOffset',
      cluster.firstPullOffset(TOPIC) === 0, String(cluster.firstPullOffset(TOPIC)));
    check('lite FIRST: the retry queue starts at 0 too',
      cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_first')) === 0,
      String(cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_first'))));
    check('lite FIRST: no boundary RPC at all',
      cluster.minOffsetCalls === 0 && cluster.maxOffsetCalls === 0 && cluster.searchTimestamps.length === 0,
      `min=${cluster.minOffsetCalls} max=${cluster.maxOffsetCalls} search=${cluster.searchTimestamps.length}`);
    cluster.close();
  }

  // ---------------------------- 5. lite: CONSUME_FROM_TIMESTAMP
  {
    const cluster = await liteRun('GID_off_ts', ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
      { consumeTimestamp: CONFIGURED });
    check('lite TIMESTAMP: a business queue starts at the offset of the configured instant',
      cluster.firstPullOffset(TOPIC) === SEARCHED, String(cluster.firstPullOffset(TOPIC)));
    check('lite TIMESTAMP: the instant searched for is the configured one, not now',
      cluster.searchedAt() === parseConsumeTimestamp(CONFIGURED),
      `${cluster.searchedAt()} vs ${parseConsumeTimestamp(CONFIGURED)}`);
    check('lite TIMESTAMP: the retry queue cannot be searched, so it takes the tail',
      cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_ts')) === TAIL,
      String(cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_ts'))));
    check('lite TIMESTAMP: only the business queue is searched', cluster.searchTimestamps.length === 1,
      String(cluster.searchTimestamps.length));
    cluster.close();
  }

  // ------------------------------ 6. lite: the stored offset wins
  {
    const cluster = await liteRun('GID_off_stored', ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
      { consumeTimestamp: CONFIGURED, storedOffset: STORED });
    check('lite stored: a committed offset beats the policy on the business queue',
      cluster.firstPullOffset(TOPIC) === STORED, String(cluster.firstPullOffset(TOPIC)));
    check('lite stored: and beats it on the retry queue',
      cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_stored')) === STORED,
      String(cluster.firstPullOffset(MixAll.getRetryTopic('GID_off_stored'))));
    check('lite stored: no boundary RPC is issued',
      cluster.maxOffsetCalls === 0 && cluster.searchTimestamps.length === 0,
      `max=${cluster.maxOffsetCalls} search=${cluster.searchTimestamps.length}`);
    cluster.close();
  }

  // -------------------------------- 7. lite: start-up rejects a bad instant
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 1);
    const c = new DefaultLitePullConsumer('GID_off_bad_ts_lite');
    c.setNamesrvAddr(cluster.addr);
    c.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
    c.subscribe(TOPIC, '*');
    c.setConsumeTimestamp('202301010000');
    let msg = '';
    try { await c.start(); } catch (e) { msg = (e as Error).message; }
    check('lite: a malformed instant stops start-up', msg.includes('consumeTimestamp is invalid'), msg);
    check('lite: and the consumer is not left half started', !c.isStarted(), '');

    const c2 = new DefaultLitePullConsumer('GID_off_good_ts_lite');
    c2.setNamesrvAddr(cluster.addr);
    c2.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
    c2.subscribe(TOPIC, '*');
    c2.setConsumeTimestamp(CONFIGURED);
    let ok = true;
    try { await c2.start(); } catch { ok = false; }
    check('the paired control: a legal instant starts', ok);
    check('and the getter keeps the configured instant', c2.getConsumeTimestamp() === CONFIGURED);
    await c2.shutdown();
    cluster.close();
  }

  // ------------------------- 8-10. push: the same table, on the wire
  {
    const cluster = await pushRun('GID_push_last', ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    check('push LAST: a business queue skips the backlog',
      cluster.firstPullOffset(TOPIC) === TAIL, String(cluster.firstPullOffset(TOPIC)));
    check('push LAST: a retry queue replays from 0',
      cluster.firstPullOffset(MixAll.getRetryTopic('GID_push_last')) === 0,
      String(cluster.firstPullOffset(MixAll.getRetryTopic('GID_push_last'))));
    check('push LAST: no backlog search is issued', cluster.searchTimestamps.length === 0);
    cluster.close();
  }
  {
    const cluster = await pushRun('GID_push_ts', ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
      { consumeTimestamp: CONFIGURED });
    check('push TIMESTAMP: a business queue starts at the offset of the configured instant',
      cluster.firstPullOffset(TOPIC) === SEARCHED, String(cluster.firstPullOffset(TOPIC)));
    check('push TIMESTAMP: the instant searched for is the configured one',
      cluster.searchedAt() === parseConsumeTimestamp(CONFIGURED), String(cluster.searchedAt()));
    check('push TIMESTAMP: the retry queue takes the tail',
      cluster.firstPullOffset(MixAll.getRetryTopic('GID_push_ts')) === TAIL,
      String(cluster.firstPullOffset(MixAll.getRetryTopic('GID_push_ts'))));
    cluster.close();
  }
  {
    const cluster = await pushRun('GID_push_first', ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    check('push FIRST: a business queue starts at 0 without asking for a boundary',
      cluster.firstPullOffset(TOPIC) === 0 && cluster.minOffsetCalls === 0,
      `offset=${cluster.firstPullOffset(TOPIC)} min=${cluster.minOffsetCalls}`);
    check('push FIRST: the retry queue starts at 0 too',
      cluster.firstPullOffset(MixAll.getRetryTopic('GID_push_first')) === 0,
      String(cluster.firstPullOffset(MixAll.getRetryTopic('GID_push_first'))));
    cluster.close();
  }
  {
    const cluster = await pushRun('GID_push_stored', ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
      { consumeTimestamp: CONFIGURED, storedOffset: STORED });
    check('push stored: a committed offset beats the policy',
      cluster.firstPullOffset(TOPIC) === STORED, String(cluster.firstPullOffset(TOPIC)));
    check('push stored: no boundary RPC is issued',
      cluster.maxOffsetCalls === 0 && cluster.searchTimestamps.length === 0);
    cluster.close();
  }

  // -------------------------------- 11. push: start-up rejects a bad instant
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 1);
    const c = new DefaultMQPushConsumer('GID_push_bad_ts');
    c.setNamesrvAddr(cluster.addr);
    c.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
    c.subscribe(TOPIC, '*');
    c.registerMessageListener({ consumeMessage: () => 0 });
    c.setConsumeTimestamp('2023010100000');
    let msg = '';
    try { await c.start(); } catch (e) { msg = (e as Error).message; }
    check('push: a malformed instant stops start-up', msg.includes('consumeTimestamp is invalid'), msg);
    cluster.close();
  }

  // ------------------- 12. push: a failed store read starts nothing
  {
    const group = 'GID_push_read_fail';
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 1);
    cluster.addTopic(MixAll.getRetryTopic(group), 1);
    cluster.offsetReadFails = true;
    const c = new DefaultMQPushConsumer(group);
    c.setNamesrvAddr(cluster.addr);
    c.subscribe(TOPIC, '*');
    c.registerMessageListener({ consumeMessage: () => 0 });
    await c.start();
    try {
      check(`push read failure: the committed-offset read is attempted`,
        cluster.offsetReadAttempts > 0, String(cluster.offsetReadAttempts));

      // Watch the window in which a wrongly started loop would have pulled.
      const until = Date.now() + 1500;
      while (Date.now() < until && cluster.firstPullOffset(TOPIC) === undefined) {
        await new Promise<void>((r) => setTimeout(r, 20));
      }
      check(`push read failure: no PULL goes out for the queue the round could not place`,
        cluster.firstPullOffset(TOPIC) === undefined, String(cluster.firstPullOffset(TOPIC)));
      check(`push read failure: the failed read never falls back to the tail`,
        cluster.maxOffsetCalls === 0, String(cluster.maxOffsetCalls));

      // The store recovers: the next round reads the committed offset and the
      // queue starts exactly there.
      cluster.offsetReadFails = false;
      cluster.storedOffset = STORED;
      await c.doRebalance();
      const pulled = await waitFor(() => cluster.firstPullOffset(TOPIC) !== undefined);
      check(`push read failure: once the store answers, the queue pulls at the committed offset`,
        pulled && cluster.firstPullOffset(TOPIC) === STORED,
        String(cluster.firstPullOffset(TOPIC)));
    } finally {
      await c.shutdown();
      cluster.close();
    }
  }

  console.log(`\ninitial offset policy: ${pass} checks, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
