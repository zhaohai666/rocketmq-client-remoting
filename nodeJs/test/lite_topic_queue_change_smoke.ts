// DefaultLitePullConsumer topic queue-change listener — offline wire test.
//
// Baseline (Java 5.5.1 client, read line by line against
// DefaultLitePullConsumerImpl / DefaultLitePullConsumer, same judgements as
// cpp's test_lite_pull_topic_queue_change, python/tests/
// test_lite_topic_queue_change.py, go/client/lite_topic_queue_change_test.go
// and rust's pull_consumer tests):
//   * Impl:1267-1279 register → null topic/listener throws
//     "Topic or listener is null"; a duplicate registration WARNS and
//     OVERWRITES; only a RUNNING consumer snapshots the current queue set
//     (otherwise the first compare reports the set that was already there).
//   * Impl:1230-1244 compare → onChanged fires ONLY on a real set change, and a
//     failed route query for one topic must not abort the other topics.
//   * Impl:1246-1260 isSetEqual → "no snapshot" is never equal, so a listener
//     registered before start() fires once on the first round.
//   * Impl:382-393 + DefaultLitePullConsumer:160 → 10s first delay, then every
//     topicMetadataCheckIntervalMillis (default 30000).
//   ⚠ This port keys the listener map by the BARE topic (like its subscribe /
//     route / rebalance state) because namespaceV2 is applied on the wire by
//     NamespaceRpcHook, not by prefixing topic names — Java keys by the
//     namespaced topic. Test 6 pins the resulting behaviour.
//
// Run: node --experimental-strip-types test/lite_topic_queue_change_smoke.ts
import net from 'node:net';

import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import type { TopicMessageQueueChangeListener } from '../src/client/lite_pull_consumer.ts';
import { MessageQueue } from '../src/common/message.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'LiteQueueChangeTopic';
const OTHER = 'LiteQueueChangeOther';
const BROKER = 'b1';

class FakeCluster {
  server: net.Server;
  port = 0;
  queues = new Map<string, number>();

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
  scale(topic: string, n: number) { this.queues.set(topic, n); }

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
    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const topic = req.extFields['topic'] || '';
      const n = this.queues.get(topic);
      if (n === undefined) { resp.code = ResponseCode.TOPIC_NOT_EXIST; return resp; }
      resp.body = Buffer.from(this.routeBody(topic, n), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      // Only this consumer is in the group unless the test says otherwise.
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: this.cidList }), 'utf8');
      return resp;
    }
    return resp;
  }

  cidList: string[] = [];

  close() { this.server.close(); }
}

class Recorder implements TopicMessageQueueChangeListener {
  events: Array<[string, number[]]> = [];
  onChanged(topic: string, messageQueues: MessageQueue[]): void {
    this.events.push([topic, messageQueues.map((m) => m.getQueueId()).sort((a, b) => a - b)]);
  }
}

function started(group: string, cluster: FakeCluster, topic: string): Promise<DefaultLitePullConsumer> {
  const c = new DefaultLitePullConsumer(group);
  c.setNamesrvAddr(cluster.addr);
  c.subscribe(topic, '*');
  return c.start().then(() => c);
}

async function waitFor(cond: () => boolean, timeoutMs = 2000): Promise<boolean> {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (cond()) return true;
    await new Promise<void>((r) => setTimeout(r, 10));
  }
  return cond();
}

function last(ids: number[] | undefined) { return (ids || []).join(','); }

async function main() {
  // ------------------------------------------------ 1. register guards + interval
  {
    const c = new DefaultLitePullConsumer('GID_lite_qc_guard');
    let msg = '';
    try { await c.registerTopicMessageQueueChangeListener('', new Recorder()); }
    catch (e) { msg = (e as Error).message; }
    check('an empty topic is rejected like Java', msg.includes('Topic or listener is null'), msg);

    msg = '';
    try { await c.registerTopicMessageQueueChangeListener('T', null); }
    catch (e) { msg = (e as Error).message; }
    check('a missing listener is rejected like Java', msg.includes('Topic or listener is null'), msg);

    check('interval defaults to Java 30s', c.getTopicMetadataCheckIntervalMillis() === 30000,
      String(c.getTopicMetadataCheckIntervalMillis()));
    c.setTopicMetadataCheckIntervalMillis(0);
    check('a 0 interval is floored to 1s (0 makes Java throw)',
      c.getTopicMetadataCheckIntervalMillis() === 1000);
    c.setTopicMetadataCheckIntervalMillis(NaN);
    check('a non-numeric interval is floored too',
      c.getTopicMetadataCheckIntervalMillis() === 1000);
    c.setTopicMetadataCheckIntervalMillis(5000);
    check('a sane interval is kept', c.getTopicMetadataCheckIntervalMillis() === 5000);
  }

  // ---------------------- 2. registered BEFORE start: first round fires once
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 2);
    const c = new DefaultLitePullConsumer('GID_lite_qc_presnapshot');
    c.setNamesrvAddr(cluster.addr);
    c.subscribe(TOPIC, '*');
    const rec = new Recorder();
    await c.registerTopicMessageQueueChangeListener(TOPIC, rec);
    check('registering while stopped takes no snapshot', rec.events.length === 0);

    await c.start();
    const fired = await c.fetchTopicMessageQueuesAndCompare();
    check('the first round reports the current set once (no snapshot ⇒ not equal)',
      fired === 1 && rec.events.length === 1, String(fired));
    check('and it reports exactly the route queues',
      rec.events[0][0] === TOPIC && last(rec.events[0][1]) === '0,1', last(rec.events[0][1]));

    const again = await c.fetchTopicMessageQueuesAndCompare();
    check('an unchanged set fires nobody twice', again === 0 && rec.events.length === 1);
    await c.shutdown();
    cluster.close();
  }

  // ------------------- 3. registered while RUNNING: snapshot suppresses the
  //                       "change" that was already there, real changes fire
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 2);
    const c = await started('GID_lite_qc_running', cluster, TOPIC);
    const rec = new Recorder();
    await c.registerTopicMessageQueueChangeListener(TOPIC, rec);

    const fired = await c.fetchTopicMessageQueuesAndCompare();
    check('a running registration does NOT report the pre-existing queues',
      fired === 0 && rec.events.length === 0, String(fired));

    cluster.scale(TOPIC, 4);
    const grown = await c.fetchTopicMessageQueuesAndCompare();
    check('scale-out fires with the new set',
      grown === 1 && rec.events.length === 1 && last(rec.events[0][1]) === '0,1,2,3',
      last(rec.events[0][1]));

    cluster.scale(TOPIC, 1);
    const shrunk = await c.fetchTopicMessageQueuesAndCompare();
    check('scale-in fires too',
      shrunk === 1 && rec.events.length === 2 && last(rec.events[1][1]) === '0',
      last(rec.events[1][1]));

    const idle = await c.fetchTopicMessageQueuesAndCompare();
    check('the snapshot advanced, so a stable set is quiet again',
      idle === 0 && rec.events.length === 2);
    await c.shutdown();
    cluster.close();
  }

  // ------------------------------------- 4. re-registering overwrites the old one
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 2);
    const c = await started('GID_lite_qc_overwrite', cluster, TOPIC);
    const first = new Recorder();
    const second = new Recorder();
    await c.registerTopicMessageQueueChangeListener(TOPIC, first);
    await c.registerTopicMessageQueueChangeListener(TOPIC, second);

    cluster.scale(TOPIC, 3);
    const fired = await c.fetchTopicMessageQueuesAndCompare();
    check('only the NEW listener is called',
      fired === 1 && second.events.length === 1 && first.events.length === 0,
      `${fired} / ${first.events.length} / ${second.events.length}`);
    await c.shutdown();
    cluster.close();
  }

  // -------- 5. one topic's failed route query must not abort the whole round
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 2);
    cluster.addTopic(OTHER, 2);
    const c = await started('GID_lite_qc_isolate', cluster, TOPIC);
    const ghost = new Recorder();
    const ok = new Recorder();
    // OTHER has no route in the fake cluster for the first round: register both,
    // then scale TOPIC so the healthy topic still has something to report.
    cluster.queues.delete(OTHER);
    await c.registerTopicMessageQueueChangeListener(OTHER, ghost);
    await c.registerTopicMessageQueueChangeListener(TOPIC, ok);
    cluster.scale(TOPIC, 3);

    const fired = await c.fetchTopicMessageQueuesAndCompare();
    check('the unroutable topic is logged, not fatal', ghost.events.length === 0);
    check('and the healthy topic still gets its change',
      fired === 1 && last(ok.events[0] && ok.events[0][1]) === '0,1,2',
      `${fired} / ${last(ok.events[0] && ok.events[0][1])}`);
    await c.shutdown();
    cluster.close();
  }

  // ---- 5b. a route that answers with ZERO queues is "topic not found", not a
  // scale-in: fetchMessageQueues throws, the round skips the listener, and the
  // snapshot keeps the queues it had (otherwise the next round would "scale
  // back out" and fire a second bogus callback).
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 2);
    const c = await started('GID_lite_qc_empty', cluster, TOPIC);
    const rec = new Recorder();
    await c.registerTopicMessageQueueChangeListener(TOPIC, rec);
    check('healthy round has nothing to report',
      (await c.fetchTopicMessageQueuesAndCompare()) === 0 && rec.events.length === 0);

    cluster.scale(TOPIC, 0);
    let thrown = '';
    try { await c.fetchMessageQueues(TOPIC); } catch (e) { thrown = (e as Error).message; }
    check('an empty queue set throws instead of returning []',
      thrown.includes('Namesrv return empty'), thrown);
    check('the empty round does not call the listener',
      (await c.fetchTopicMessageQueuesAndCompare()) === 0 && rec.events.length === 0);

    cluster.scale(TOPIC, 2);
    check('the snapshot survived, so coming back is not a change',
      (await c.fetchTopicMessageQueuesAndCompare()) === 0 && rec.events.length === 0);
    await c.shutdown();
    cluster.close();
  }

  // -------- 6. bare topic key: the wire carries the namespace, not the name
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 2);
    const c = await started('GID_lite_qc_ns', cluster, TOPIC);
    c.setNamespaceV2('ns1');
    const rec = new Recorder();
    await c.registerTopicMessageQueueChangeListener(TOPIC, rec);
    cluster.scale(TOPIC, 3);
    await c.fetchTopicMessageQueuesAndCompare();
    check('the callback gets the topic it was registered for',
      rec.events.length === 1 && rec.events[0][0] === TOPIC,
      rec.events.map((e) => e[0]).join(','));
    await c.shutdown();
    cluster.close();
  }

  // ---------------------------------------- 7. the background loop really runs
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addTopic(TOPIC, 2);
    const c = await started('GID_lite_qc_loop', cluster, TOPIC);
    const rec = new Recorder();
    await c.registerTopicMessageQueueChangeListener(TOPIC, rec);
    check('start() arms the scheduled check', (c as any)._metadataTimer != null);

    cluster.scale(TOPIC, 3);
    // Java's period is fixed at start(); drive a short one the same way.
    clearInterval((c as any)._metadataTimer);
    clearTimeout((c as any)._metadataInitialTimer);
    (c as any)._metadataTimer = null;
    (c as any)._metadataInitialTimer = null;
    (c as any)._startMetadataLoop(20, 40);
    const sawChange = await waitFor(() => rec.events.length >= 1);
    check('the scheduled check notices the scale-out on its own',
      sawChange && last(rec.events[0] && rec.events[0][1]) === '0,1,2',
      last(rec.events[0] && rec.events[0][1]));

    await c.shutdown();
    check('shutdown clears both metadata timers',
      (c as any)._metadataTimer == null && (c as any)._metadataInitialTimer == null);
    cluster.close();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
