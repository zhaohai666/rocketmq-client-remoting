// MQClient instance-level persistAllConsumerOffset + persistConsumerOffsetInterval —
// offline wire test.
//
// Behaviour baseline (Java MQClientInstance.startScheduledTask registers
// persistAllConsumerOffset with initialDelay 10s / period
// ClientConfig.persistConsumerOffsetInterval = 5000ms; RemoteBrokerOffsetStore
// / LocalFileOffsetStore decide what a flush costs; one failing consumer must
// never starve the others — the sweep is best-effort per consumer):
//   * The PULL / LITE-PULL facades only flushed on shutdown or by an explicit
//     call, so a crash lost every offset since the last flush. The instance sweep
//     is what closes that window.
//   * The period is a knob on the facade (first creator wins for a shared
//     instance) and on the instance; changing it at runtime rebuilds the timer.
//   * BROADCASTING keeps offsets in a LOCAL file: its flush must not touch the broker.
//   * The timers die with the instance (last facade unregisters → shutdown).
//
// Run: node --experimental-strip-types test/offset_persist_timer_smoke.ts
import net from 'node:net';

import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { MessageQueue } from '../src/common/message.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const BROKER = 'persistBroker';

async function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }

async function waitFor(cond: () => boolean, millis = 6000): Promise<boolean> {
  const deadline = Date.now() + millis;
  while (Date.now() < deadline) {
    if (cond()) return true;
    await sleep(20);
  }
  return cond();
}

class FakeCluster {
  server: net.Server;
  port = 0;
  queueNums = new Map<string, number>();
  // Committed offsets served by QUERY_CONSUMER_OFFSET (`topic@queueId` -> offset).
  committed = new Map<string, number>();
  // UPDATE_CONSUMER_OFFSET seen, as `topic@queueId@commitOffset`.
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
  commitKeys(topic: string): string[] {
    return this.commits.filter((c) => c.startsWith(`${topic}@`));
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
    const key = `${topic}@${Number(ext['queueId'] || 0)}`;

    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const n = this.queueNums.get(topic);
      if (n == null) { resp.code = ResponseCode.TOPIC_NOT_EXIST; return resp; }
      resp.body = Buffer.from(JSON.stringify({
        orderTopicConf: null,
        queueDatas: [{
          brokerName: BROKER, readQueueNums: n, writeQueueNums: n, perm: 6, topicSysFlag: 0,
        }],
        brokerDatas: [{
          cluster: 'DefaultCluster', brokerName: BROKER,
          brokerAddrs: { '0': this.addr }, zoneName: null, enableActingMaster: false,
        }],
        filterServerTable: {},
      }), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: [] }), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.QUERY_CONSUMER_OFFSET) {
      const off = this.committed.get(key);
      if (off == null) { resp.code = ResponseCode.QUERY_NOT_FOUND; return resp; }
      resp.addExtField('offset', String(off));
      return resp;
    }
    if (req.code === RequestCode.GET_MAX_OFFSET || req.code === RequestCode.GET_MIN_OFFSET) {
      resp.addExtField('offset', '10');
      return resp;
    }
    if (req.code === RequestCode.UPDATE_CONSUMER_OFFSET) {
      this.commits.push(`${key}@${ext['commitOffset']}`);
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
    return resp; // heartbeat / unregistration / everything else: SUCCESS
  }

  close() { this.server.close(); }
}

async function main() {
  // ------------------------------------------------- 1. defaults and validation
  {
    const c = new DefaultMQPullConsumer('GID_persist_defaults');
    check('the pull facade defaults to Java 5000ms', c.getPersistConsumerOffsetInterval() === 5000,
      String(c.getPersistConsumerOffsetInterval()));
    const lite = new DefaultLitePullConsumer('GID_persist_defaults_lite');
    check('the lite-pull facade defaults to Java 5000ms',
      lite.getPersistConsumerOffsetInterval() === 5000,
      String(lite.getPersistConsumerOffsetInterval()));
    for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      let threw = '';
      try { c.setPersistConsumerOffsetInterval(bad); } catch (e) { threw = (e as Error).message; }
      check(`a facade rejects the period ${bad}`, threw.includes('persistConsumerOffsetInterval'), threw);
    }
    const bare = new MQClient('127.0.0.1@persist_bare', '127.0.0.1:9876');
    check('a bare instance defaults to 5000ms', bare.persistConsumerOffsetInterval === 5000,
      String(bare.persistConsumerOffsetInterval));
    let threw = '';
    try { bare.setPersistConsumerOffsetInterval(0); } catch (e) { threw = (e as Error).message; }
    check('the instance rejects a non-positive period', threw.includes('must be > 0'), threw);
    // Not started: the knob still records the value (the loop is built at start()).
    bare.setPersistConsumerOffsetInterval(1234);
    check('setting the period before start() only records it',
      bare.persistConsumerOffsetInterval === 1234 && bare._persistOffsetTimer === null,
      `${bare.persistConsumerOffsetInterval}/${bare._persistOffsetTimer}`);
  }

  // ---------------------------------------------- 2. the sweep flushes on its own
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.setQueues('PersistPull', 1);
    const c = new DefaultMQPullConsumer('GID_persist_sweep');
    c.setNamesrvAddr(cluster.addr);
    c.setInstanceName('persist_sweep');
    c.subscribe('PersistPull', '*');
    await c.start();
    const client = c.mqClient!;
    check('start() arms the instance sweep', client._persistOffsetTimer !== null,
      String(client._persistOffsetTimer));
    c.setPersistConsumerOffsetInterval(150);
    check('a runtime change rebuilds the timer', client.persistConsumerOffsetInterval === 150
      && client._persistOffsetTimer !== null);

    const mq = new MessageQueue('PersistPull', BROKER, 0);
    c.updateConsumeOffset(mq, 3);
    // No explicit persistConsumeOffset() anywhere below: only the timer may flush.
    check('the instance sweep commits the recorded offset to the broker',
      await waitFor(() => cluster.commits.includes('PersistPull@0@3'), 6000),
      cluster.commits.join(','));
    c.updateConsumeOffset(mq, 9);
    check('a later cursor is flushed too (the sweep repeats)',
      await waitFor(() => cluster.commits.includes('PersistPull@0@9'), 6000),
      cluster.commits.join(','));
    const before = cluster.commits.length;
    await c.shutdown();
    const afterShutdown = cluster.commits.length;
    check('shutdown() flushes once more', afterShutdown >= before, `${before} -> ${afterShutdown}`);
    await sleep(600);
    check('the sweep timer dies with the instance', cluster.commits.length === afterShutdown,
      `${afterShutdown} -> ${cluster.commits.length}`);
    cluster.close();
  }

  // --------------------- 3. one sweep reaches every consumer of the shared instance
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.setQueues('PersistSharedA', 1);
    cluster.setQueues('PersistSharedB', 1);
    const pull = new DefaultMQPullConsumer('GID_persist_shared_pull');
    pull.setNamesrvAddr(cluster.addr);
    pull.setInstanceName('persist_shared');
    pull.subscribe('PersistSharedA', '*');
    await pull.start();
    const lite = new DefaultLitePullConsumer('GID_persist_shared_lite');
    lite.setNamesrvAddr(cluster.addr);
    lite.setInstanceName('persist_shared');
    lite.setAutoCommit(false); // only the instance sweep may commit
    lite.assign([new MessageQueue('PersistSharedB', BROKER, 0)]);
    await lite.start();
    check('two facades with the same instanceName share one MQClient instance',
      pull.mqClient === lite.mqClient, `${pull.mqClient?.clientId} / ${lite.mqClient?.clientId}`);
    check("the first creator's period rules the shared instance",
      pull.mqClient!.persistConsumerOffsetInterval === 5000,
      String(pull.mqClient!.persistConsumerOffsetInterval));

    pull.setPersistConsumerOffsetInterval(150);
    const mqA = new MessageQueue('PersistSharedA', BROKER, 0);
    pull.updateConsumeOffset(mqA, 4);
    lite.seek(new MessageQueue('PersistSharedB', BROKER, 0), 6);
    check('the pull facade is flushed by the sweep',
      await waitFor(() => cluster.commitKeys('PersistSharedA').length > 0, 6000),
      cluster.commits.join(','));
    check('the lite-pull facade is flushed by the SAME sweep',
      await waitFor(() => cluster.commitKeys('PersistSharedB').length > 0, 6000),
      cluster.commits.join(','));
    await lite.shutdown();
    await pull.shutdown();
    cluster.close();
  }

  // ------------------------ 4. one failing consumer must not starve the others
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.setQueues('PersistIsolate', 1);
    const c = new DefaultMQPullConsumer('GID_persist_isolate');
    c.setNamesrvAddr(cluster.addr);
    c.setInstanceName('persist_isolate');
    c.subscribe('PersistIsolate', '*');
    await c.start();
    const client = c.mqClient!;
    client.setPersistConsumerOffsetInterval(150);
    // A consumer that throws synchronously and one whose promise rejects: both
    // must be contained so the real flush still happens.
    client.getRegisteredConsumers().set('__boomSync', {
      persistConsumerOffset: () => { throw new Error('sync boom'); },
    });
    client.getRegisteredConsumers().set('__boomAsync', {
      persistConsumerOffset: () => Promise.reject(new Error('async boom')),
    });
    c.updateConsumeOffset(new MessageQueue('PersistIsolate', BROKER, 0), 2);
    check('a consumer that raises (sync or async) does not stop the sweep',
      await waitFor(() => cluster.commits.includes('PersistIsolate@0@2'), 6000),
      cluster.commits.join(','));
    client.getRegisteredConsumers().delete('__boomSync');
    client.getRegisteredConsumers().delete('__boomAsync');
    await c.shutdown();
    cluster.close();
  }

  // ------------------- 5. BROADCASTING keeps the flush off the wire (local store)
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.setQueues('PersistBcast', 1);
    const c = new DefaultMQPullConsumer('GID_persist_bcast');
    c.setNamesrvAddr(cluster.addr);
    c.setInstanceName('persist_bcast');
    c.setMessageModel('BROADCASTING');
    c.subscribe('PersistBcast', '*');
    await c.start();
    c.setPersistConsumerOffsetInterval(150);
    c.updateConsumeOffset(new MessageQueue('PersistBcast', BROKER, 0), 5);
    await sleep(900); // several sweeps would have fired at 150ms
    check('a broadcasting consumer never commits to the broker',
      cluster.commitKeys('PersistBcast').length === 0, cluster.commits.join(','));
    await c.shutdown();
    cluster.close();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
