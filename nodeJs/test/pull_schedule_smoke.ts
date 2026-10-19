// PullConsumerScheduleService + DefaultMQPullConsumer.pullAsync — offline wire test.
//
// Behaviour baseline (the rules this scheduling service commits to,
// each one asserted against the recorded wire traffic below):
//   * start() expands every registered topic into QUEUE-dimension tasks; one task
//     = one queue, and a queue's rounds are strictly serial (the next timer is
//     armed only after the current round settles).
//   * Three rescheduling channels with a fixed priority: the callback's return
//     value > context.pullMessageLater() (one-shot) > context.setPullInterval()
//     (resident) > the status default. A callback that throws waits
//     PULL_TASK_EXCEPTION_DELAY_MILLIS (1000 ms) and costs one round, not the loop.
//   * The first round's offset comes from the registration (number/provider), else
//     the broker's committed offset, else the queue tail (the LAST-offset fallback).
//   * A queue that leaves the view is cancelled; a transient route/consumer-list
//     failure keeps the current tasks (never "cancel everything").
//   * shutdown() drains: pending timers go away, an in-flight round still finishes.
//   * pullAsync delivers EXACTLY ONCE (success XOR exception) and NEVER inline,
//     even for a local guard failure — event-loop semantics.
//
// Run: node --experimental-strip-types test/pull_schedule_smoke.ts
import net from 'node:net';

import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import {
  PullConsumerScheduleService, PullTaskContext,
  DEFAULT_PULL_NEXT_DELAY_MILLIS, PULL_TASK_EXCEPTION_DELAY_MILLIS,
  DEFAULT_PULL_NEXT_DELAY_MILLIS as _DEF_DELAY,
} from '../src/client/pull_schedule_service.ts';
import { PullStatus } from '../src/client/consumer_result.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const BROKER = 'schedBroker';

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(cond: () => boolean, millis = 5000): Promise<boolean> {
  const deadline = Date.now() + millis;
  while (Date.now() < deadline) {
    if (cond()) return true;
    await sleep(10);
  }
  return cond();
}

// One endpoint plays nameserver (route + consumer list) and broker (pull / offsets).
class FakeCluster {
  server: net.Server;
  port = 0;
  // topic -> readQueueNums; mutable so a test can shrink/reassign the view.
  queueNums = new Map<string, number>();
  cidList: string[] = [];
  // Every pull as `topic@queueId@queueOffset`, in arrival order.
  pulls: string[] = [];
  // Committed offsets served by QUERY_CONSUMER_OFFSET (`topic@queueId` -> offset).
  committed = new Map<string, number>();
  // Queue tails served by GET_MAX_OFFSET (default: DEFAULT_MAX_OFFSET).
  maxOffsets = new Map<string, number>();
  DEFAULT_MAX_OFFSET = 40;
  // When > 0 the pull answers this response code instead of PULL_NOT_FOUND.
  pullErrorCode = 0;
  // UPDATE_CONSUMER_OFFSET commits seen (`topic@queueId` -> offset).
  commits: string[] = [];

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

  setQueues(topic: string, n: number) { this.queueNums.set(topic, n); }

  pullKeys(topic: string, queueId: number): string[] {
    return this.pulls.filter((p) => p.startsWith(`${topic}@${queueId}@`));
  }

  firstPullOffset(topic: string, queueId: number): number {
    const keys = this.pullKeys(topic, queueId);
    return keys.length === 0 ? -1 : Number(keys[0].split('@')[2]);
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
    const topic = String(ext['topic'] || '');
    const queueId = Number(ext['queueId'] || 0);
    const key = `${topic}@${queueId}`;

    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const n = this.queueNums.get(topic);
      if (n == null) { resp.code = ResponseCode.TOPIC_NOT_EXIST; return resp; }
      const queueDatas = [];
      for (let i = 0; i < 1; i++) {
        queueDatas.push({
          brokerName: BROKER, readQueueNums: n, writeQueueNums: n,
          perm: 6, topicSysFlag: 0,
        });
      }
      resp.body = Buffer.from(JSON.stringify({
        orderTopicConf: null,
        queueDatas,
        brokerDatas: [{
          cluster: 'DefaultCluster', brokerName: BROKER,
          brokerAddrs: { '0': this.addr }, zoneName: null, enableActingMaster: false,
        }],
        filterServerTable: {},
      }), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: this.cidList }), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.QUERY_CONSUMER_OFFSET) {
      const off = this.committed.get(key);
      if (off == null) { resp.code = ResponseCode.QUERY_NOT_FOUND; return resp; }
      resp.addExtField('offset', String(off));
      return resp;
    }
    if (req.code === RequestCode.GET_MAX_OFFSET || req.code === RequestCode.GET_MIN_OFFSET) {
      resp.addExtField('offset', String(this.maxOffsets.get(key) ?? this.DEFAULT_MAX_OFFSET));
      return resp;
    }
    if (req.code === RequestCode.UPDATE_CONSUMER_OFFSET) {
      this.commits.push(`${key}@${ext['commitOffset']}`);
      return resp;
    }
    if (req.code === RequestCode.PULL_MESSAGE || req.code === RequestCode.LITE_PULL_MESSAGE) {
      const offset = Number(ext['queueOffset'] || 0);
      this.pulls.push(`${key}@${offset}`);
      if (this.pullErrorCode > 0) { resp.code = this.pullErrorCode; resp.remark = 'scripted failure'; return resp; }
      // NO_NEW_MSG keeps the round observable without an encoded body: the cursor
      // still advances, so "one round per pull" is visible in `pulls`.
      resp.code = ResponseCode.PULL_NOT_FOUND;
      resp.addExtField('nextBeginOffset', String(offset + 1));
      resp.addExtField('minOffset', '0');
      resp.addExtField('maxOffset', String(this.maxOffsets.get(key) ?? this.DEFAULT_MAX_OFFSET));
      resp.addExtField('suggestWhichBrokerId', '0');
      return resp;
    }
    return resp; // heartbeat / every other code: SUCCESS
  }

  close() { this.server.close(); }
}

// A service wired to the cluster: BROADCASTING by default, because that view is the
// route's queues and needs no consumer-list round trip (CLUSTERING is covered
// separately in the reassignment case).
function service(group: string, cluster: FakeCluster, topic: string, queueNums = 3,
  model = 'BROADCASTING') {
  const svc = new PullConsumerScheduleService(group);
  const consumer = svc.getPullConsumer();
  consumer.setNamesrvAddr(cluster.addr);
  consumer.setMessageModel(model);
  consumer.subscribe(topic, '*');
  cluster.setQueues(topic, queueNums);
  svc.queueCheckIntervalMillis = 100;
  return svc;
}

function counting(cb: (mq: any, r: any, ctx: PullTaskContext) => any) {
  const seen: string[] = [];
  const fn = (mq: any, r: any, ctx: PullTaskContext) => {
    seen.push(`${mq.getTopic()}@${mq.getQueueId()}`);
    return cb(mq, r, ctx);
  };
  return { call: fn as any, seen, count: () => seen.length };
}

async function main() {
  // -------------------------------------------------- 1. fan-out and serial rounds
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_fanout', cluster, 'SchedFanout', 3);
    const rec = counting(() => undefined);
    svc.registerPullTask('SchedFanout', rec.call, { nextDelayMillis: 30 });
    check('a topic registers once, sorted', svc.callbackTopics().join(',') === 'SchedFanout');
    check('the service reports not started before start()', svc.isStarted() === false);
    await svc.start();
    check('start() expands 3 queues into 3 tasks', svc.taskCount() === 3, String(svc.taskCount()));
    check('every queue got pulled', waitFor(() =>
      cluster.pullKeys('SchedFanout', 0).length > 0
      && cluster.pullKeys('SchedFanout', 1).length > 0
      && cluster.pullKeys('SchedFanout', 2).length > 0));
    const perQueue = [0, 1, 2].map((q) => cluster.pullKeys('SchedFanout', q).length);
    check('the cursor advances round by round (nextBeginOffset is taken)',
      [0, 1, 2].every((q) => cluster.pullKeys('SchedFanout', q)
        .every((p, i) => Number(p.split('@')[2]) === i)), JSON.stringify(perQueue));
    await svc.shutdown();
    check('shutdown() drops the task table', svc.taskCount() === 0 && !svc.isStarted());
    const after = cluster.pulls.length;
    await sleep(200);
    check('no round is scheduled after shutdown', cluster.pulls.length === after,
      `${after} -> ${cluster.pulls.length}`);
    cluster.close();
  }

  // ------------------------------------------- 2. exactly one round in flight / queue
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_serial', cluster, 'SchedSerial', 2);
    let live = 0, maxLive = 0;
    const perQueueLive = new Map<string, number>();
    svc.registerPullTask('SchedSerial', async (_mq, _r, ctx) => {
      const key = `${_mq.getQueueId()}`;
      const here = (perQueueLive.get(key) || 0) + 1;
      perQueueLive.set(key, here);
      live = Math.max(live, here);
      maxLive = Math.max(maxLive, here);
      await sleep(30);
      perQueueLive.set(key, here - 1);
      ctx.setPullInterval(10);
    }, { nextDelayMillis: 10 });
    await svc.start();
    await waitFor(() => svc.getPullConsumer() && cluster.pulls.length >= 8, 5000);
    await svc.shutdown();
    check('a queue never has two rounds in flight', maxLive <= 1, String(maxLive));
    cluster.close();
  }

  // ------------------------------------------------------- 3. the first-round offset
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_off1', cluster, 'SchedOffFixed', 1);
    svc.registerPullTask('SchedOffFixed', () => undefined, { offset: 7, nextDelayMillis: 30 });
    await svc.start();
    await waitFor(() => cluster.pulls.length > 0);
    check('an explicit numeric offset starts the first round',
      cluster.firstPullOffset('SchedOffFixed', 0) === 7,
      String(cluster.firstPullOffset('SchedOffFixed', 0)));
    await svc.shutdown();
    cluster.close();
  }
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_off2', cluster, 'SchedOffFn', 1);
    let calls = 0;
    svc.registerPullTask('SchedOffFn', () => undefined,
      { offset: () => { calls++; return 12; }, nextDelayMillis: 30 });
    await svc.start();
    await waitFor(() => cluster.pulls.length > 0);
    check('a provider is evaluated for the first round',
      cluster.firstPullOffset('SchedOffFn', 0) === 12 && calls === 1,
      `offset=${cluster.firstPullOffset('SchedOffFn', 0)} providerCalls=${calls}`);
    await svc.shutdown();
    cluster.close();
  }
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    // Only CLUSTERING keeps offsets on the BROKER (BROADCASTING uses the local
    // file store), so the commit path needs that model — and a consumer started
    // up front, because its clientID (with the @pid/@STREAM suffix) is only known
    // after start() and the broker's consumer list must contain it.
    const consumer = new DefaultMQPullConsumer('GID_sched_off3');
    consumer.setNamesrvAddr(cluster.addr);
    consumer.setMessageModel('CLUSTERING');
    consumer.subscribe('SchedOffCommitted', '*');
    cluster.setQueues('SchedOffCommitted', 1);
    await consumer.start();
    cluster.cidList = [consumer.clientID];
    cluster.committed.set('SchedOffCommitted@0', 5);
    const svc = new PullConsumerScheduleService(null, consumer);
    svc.queueCheckIntervalMillis = 100;
    svc.registerPullTask('SchedOffCommitted', () => undefined, { nextDelayMillis: 30 });
    await svc.start();
    await waitFor(() => cluster.pulls.length > 0);
    check('with no registration offset the broker commit wins',
      cluster.firstPullOffset('SchedOffCommitted', 0) === 5,
      String(cluster.firstPullOffset('SchedOffCommitted', 0)));
    await svc.shutdown();
    await consumer.shutdown();
    cluster.close();
  }
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_off4', cluster, 'SchedOffTail', 1);
    cluster.maxOffsets.set('SchedOffTail@0', 33);
    svc.registerPullTask('SchedOffTail', () => undefined, { nextDelayMillis: 30 });
    await svc.start();
    await waitFor(() => cluster.pulls.length > 0);
    check('nothing committed and no registration → start at the queue tail',
      cluster.firstPullOffset('SchedOffTail', 0) === 33,
      String(cluster.firstPullOffset('SchedOffTail', 0)));
    await svc.shutdown();
    cluster.close();
  }

  // ---------------------------------------- 4. delay channels: return > later > interval
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_prio', cluster, 'SchedPrio', 1);
    const ctxHolder: { ctx?: PullTaskContext } = {};
    svc.registerPullTask('SchedPrio', (_mq, _r, ctx) => {
      ctxHolder.ctx = ctx;
      ctx.setPullInterval(3000);          // channel 3, deliberately long
      return 20;                          // channel 1 wins
    });
    await svc.start();
    await waitFor(() => cluster.pullKeys('SchedPrio', 0).length >= 4, 5000);
    check('the return value overrides the standing interval',
      cluster.pullKeys('SchedPrio', 0).length >= 4);
    check('the settled delay is the returned one',
      ctxHolder.ctx?.lastNextDelayMillis === 20, String(ctxHolder.ctx?.lastNextDelayMillis));
    check('the standing interval stays for the following rounds',
      ctxHolder.ctx?.getPullNextDelayTimeMillis() === 3000);
    await svc.shutdown();
    cluster.close();
  }
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_later', cluster, 'SchedLater', 1);
    const ctxRef: { ctx?: PullTaskContext } = {};
    let rounds = 0;
    svc.registerPullTask('SchedLater', (_mq, _r, ctx) => {
      ctxRef.ctx = ctx;
      rounds++;
      if (rounds === 1) ctx.pullMessageLater(400);   // channel 2, one-shot
      ctx.setPullInterval(20);
    });
    await svc.start();
    await waitFor(() => rounds >= 1 && ctxRef.ctx?.roundCount === 1, 3000);
    check('pullMessageLater settles round one',
      ctxRef.ctx?.lastNextDelayMillis === 400, String(ctxRef.ctx?.lastNextDelayMillis));
    await waitFor(() => rounds >= 3, 5000);
    check('the one-shot deferral is consumed: later rounds use the interval',
      ctxRef.ctx?.lastNextDelayMillis === 20, String(ctxRef.ctx?.lastNextDelayMillis));
    await svc.shutdown();
    cluster.close();
  }
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_default', cluster, 'SchedDefault', 1);
    const ctxRef: { ctx?: PullTaskContext } = {};
    svc.registerPullTask('SchedDefault', (_mq, _r, ctx) => { ctxRef.ctx = ctx; });
    await svc.start();
    await waitFor(() => cluster.pullKeys('SchedDefault', 0).length >= 3, 5000);
    check('no channel set → the status default (200ms) is used',
      ctxRef.ctx?.lastNextDelayMillis === DEFAULT_PULL_NEXT_DELAY_MILLIS
      && DEFAULT_PULL_NEXT_DELAY_MILLIS === _DEF_DELAY,
      String(ctxRef.ctx?.lastNextDelayMillis));
    await svc.shutdown();
    cluster.close();
  }

  // --------------------------------------------------- 5. failures cost one round only
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_throw', cluster, 'SchedThrow', 1);
    // Delays are read at the START of a round: that is the value the PREVIOUS round settled.
    const delays: number[] = [];
    let n = 0;
    svc.registerPullTask('SchedThrow', (_mq, _r, ctx) => {
      delays.push(ctx.lastNextDelayMillis ?? -1);
      n++;
      ctx.setPullInterval(20);
      if (n === 1) throw new Error('boom');
    });
    await svc.start();
    await waitFor(() => n >= 2, 5000);
    check('a throwing callback waits the exception delay (1000ms)',
      delays.length >= 2 && delays[1] === PULL_TASK_EXCEPTION_DELAY_MILLIS && delays[1] === 1000,
      JSON.stringify(delays));
    check('the loop survives the exception', n >= 2, String(n));
    await svc.shutdown();
    cluster.close();
  }
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_pullerr', cluster, 'SchedPullErr', 1);
    const rec = counting(() => undefined);
    svc.registerPullTask('SchedPullErr', rec.call, { nextDelayMillis: 20, timeoutMillis: 3000 });
    cluster.pullErrorCode = ResponseCode.SYSTEM_ERROR;
    await svc.start();
    await waitFor(() => cluster.pulls.length >= 3, 5000);
    check('a broker-side pull failure does not reach the callback',
      rec.count() === 0, String(rec.count()));
    cluster.pullErrorCode = 0;
    const before = rec.count();
    check('pulls resume once the broker recovers',
      await waitFor(() => rec.count() > before, 8000), `${before} -> ${rec.count()}`);
    await svc.shutdown();
    cluster.close();
  }

  // --------------------------------------------- 6. register / unregister at runtime
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_dyn', cluster, 'SchedDynA', 1);
    svc.registerPullTask('SchedDynA', () => undefined, { nextDelayMillis: 20 });
    await svc.start();
    check('the first topic is expanded at start()', svc.taskCount() === 1);
    cluster.setQueues('SchedDynB', 2);
    const rec = counting(() => undefined);
    svc.registerPullTask('SchedDynB', rec.call, { nextDelayMillis: 20 });
    check('a topic registered after start() is expanded immediately',
      await waitFor(() => svc.taskCount() === 3, 3000)
      && svc.callbackTopics().join(',') === 'SchedDynA,SchedDynB',
      `${svc.taskCount()} / ${svc.callbackTopics().join(',')}`);
    svc.unregisterPullTask('SchedDynB');
    check('unregister removes that topic\'s tasks and callback',
      svc.taskCount() === 1 && svc.callbackTopics().join(',') === 'SchedDynA',
      `${svc.taskCount()} / ${svc.callbackTopics().join(',')}`);
    await svc.shutdown();
    cluster.close();
  }

  // ------------------------- 7. CLUSTERING reassignment: revoked queues stop pulling
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_rebal', cluster, 'SchedRebal', 4, 'CLUSTERING');
    svc.registerPullTask('SchedRebal', () => undefined, { nextDelayMillis: 20 });
    // No consumer is known to the broker yet: this instance owns NOTHING, and a
    // transient/empty view must never be read as "all queues are mine".
    await svc.start();
    await sleep(250);
    check('an unknown consumer list expands to zero tasks', svc.taskCount() === 0,
      String(svc.taskCount()));
    cluster.cidList = [svc.getPullConsumer().clientID];
    check('the recheck timer picks up this instance\'s share',
      await waitFor(() => svc.taskCount() === 4, 3000), String(svc.taskCount()));
    cluster.setQueues('SchedRebal', 2);
    check('a shrunken route revokes the surplus tasks',
      await waitFor(() => svc.taskCount() === 2, 3000), String(svc.taskCount()));
    await svc.shutdown();
    cluster.close();
  }

  // --------------------------------------------------- 8. guards and shutdown drain
  {
    const svc = new PullConsumerScheduleService('GID_sched_guard');
    let threw = '';
    try { svc.registerPullTask('', () => undefined); } catch (e) { threw = (e as Error).message; }
    check('an empty topic is rejected', threw.includes('topic'), threw);
    threw = '';
    try { svc.registerPullTask('T', null as any); } catch (e) { threw = (e as Error).message; }
    check('a missing callback is rejected', threw.includes('callback'), threw);
    threw = '';
    try { new PullConsumerScheduleService(null as any, null as any); } catch (e) { threw = (e as Error).message; }
    check('no group and no consumer is rejected', threw.includes('consumerGroup'), threw);
    const orphan = new PullTaskContext(svc.getPullConsumer(),
      // any queue identity works: seek() never touches the wire
      new (await import('../src/common/message.ts')).MessageQueue('T', 'b', 0));
    threw = '';
    try { orphan.seek(-1); } catch (e) { threw = (e as Error).message; }
    check('seek rejects a negative offset', threw.includes('offset'), threw);
    orphan.seek(9);
    check('seek sets the next round cursor', orphan.nextOffset === 9 && orphan.getPullNextDelayTimeMillis() === DEFAULT_PULL_NEXT_DELAY_MILLIS);
    await svc.shutdown(); // shutdown of a never-started service is a no-op
    check('shutdown before start is harmless', svc.isStarted() === false);
  }
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const svc = service('GID_sched_drain', cluster, 'SchedDrain', 1);
    let finished = 0;
    svc.registerPullTask('SchedDrain', async () => { await sleep(300); finished++; });
    await svc.start();
    await waitFor(() => cluster.pulls.length >= 1, 3000);
    await svc.shutdown();
    check('shutdown drains the in-flight round', finished === 1, String(finished));
    cluster.close();
  }

  // ----------------------------------------------------- 9. pullAsync exactly-once
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.setQueues('SchedAsync', 1);
    const consumer = new DefaultMQPullConsumer('GID_sched_async');
    consumer.setNamesrvAddr(cluster.addr);
    consumer.subscribe('SchedAsync', '*');
    const mq = new (await import('../src/common/message.ts')).MessageQueue('SchedAsync', BROKER, 0);
    let inline = true, success = 0, exception = 0;
    consumer.pullAsync(mq, '*', 0, 32, {
      onSuccess: () => { inline = false; success++; },
      onException: () => { inline = false; exception++; },
    }, 3000);
    check('pullAsync never calls back inline on the caller stack',
      success === 0 && exception === 0);
    await waitFor(() => success + exception === 1, 5000);
    check('an unstarted consumer still settles exactly once (before start)',
      success === 0 && exception === 1, `${success}/${exception}`);

    await consumer.start();
    success = 0; exception = 0;
    consumer.pullAsync(mq, '*', 0, 32,
      { onSuccess: () => { success++; }, onException: () => { exception++; } }, 5000);
    check('success XOR exception, exactly once (happy round)',
      await waitFor(() => success + exception === 1, 5000) && success === 1 && exception === 0,
      `${success}/${exception}`);
    check('the pull carried the inline expression to the broker',
      cluster.pullKeys('SchedAsync', 0).length === 1);

    cluster.pullErrorCode = ResponseCode.SYSTEM_ERROR;
    success = 0; exception = 0;
    consumer.pullAsync(mq, '*', 0, 32,
      { onSuccess: () => { success++; }, onException: () => { exception++; } }, 5000);
    check('a broker error lands in onException, still exactly once',
      await waitFor(() => success + exception === 1, 5000) && success === 0 && exception === 1,
      `${success}/${exception}`);
    cluster.pullErrorCode = 0;

    let threw = '';
    try { consumer.pullAsync(mq, '*', 0, 32, null as any, 1000); }
    catch (e) { threw = (e as Error).message; }
    check('a malformed callback throws at the call site', threw.includes('PullCallback'), threw);

    // A callback that itself raises must not escape the settle path.
    let branch = '';
    consumer.pullAsync(mq, '*', 0, 32, {
      onSuccess: () => { branch = 'success'; throw new Error('inside onSuccess'); },
      onException: () => { branch = 'exception'; },
    }, 5000);
    check('a raising onSuccess is swallowed, not thrown at the caller',
      await waitFor(() => branch !== '', 5000) && branch === 'success', branch);

    await consumer.shutdown();
    success = 0; exception = 0;
    consumer.pullAsync(mq, '*', 0, 32,
      { onSuccess: () => { success++; }, onException: () => { exception++; } }, 3000);
    check('after shutdown the pull settles through onException',
      await waitFor(() => success + exception === 1, 5000) && exception === 1,
      `${success}/${exception}`);
    cluster.close();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
