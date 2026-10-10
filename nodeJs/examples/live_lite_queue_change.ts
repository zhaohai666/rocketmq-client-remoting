// live_lite_queue_change is the Node.js DefaultLitePullConsumer topic
// queue-change listener smoke against a real RocketMQ 5.x cluster.
//
//   node --experimental-strip-types examples/live_lite_queue_change.ts --ns 127.0.0.1:9876
//
// Why it needs a live cluster: a check round re-queries the name server for the
// topic's queues, while the ordinary route cache only refreshes every 30s. A
// fake name server proves the comparison logic; only a real one proves the
// freshness. So the metadata check interval is pushed to its 1s floor, the
// route poll interval stays at the 30s default, and the topic is really
// scaled. Once the scheduled round is past its initial delay, the callback must
// land within a couple of 1s rounds of the moment the name server starts
// reporting the new count — reading the cache would push that gap past half a
// minute.
//
//   L1  quiet      queues unchanged ⇒ the listener is not disturbed
//   L1b first pass the scheduled round has really run and stayed quiet
//   L2  freshness  scale 2 -> 4, callback within a few rounds
//   L3  converged  the snapshot advances, so one change = one callback
//   L4  scale-in   4 -> 2 is reported the same way
//   L5  unknown    a topic with no queues is "not found", never "zero queues"
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import type { TopicMessageQueueChangeListener } from '../src/client/lite_pull_consumer.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { MessageQueue } from '../src/common/message.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = `NodeLiteQcLive_${STAMP}`;
const GROUP = `GID_NodeLiteQc_${STAMP}`;
const GHOST = `NodeLiteQcGhost_${STAMP}`;

const BASE_QUEUES = 2;
const SCALED_QUEUES = 4;
const CHECK_INTERVAL_MS = 1000;   // the floor: separates "re-query" from "30s cache"
const FIRST_DELAY_MS = 12000;     // scheduled round starts 10s after start()
const FRESH_WINDOW_MS = 5000;

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

class Recorder implements TopicMessageQueueChangeListener {
  events: string[] = [];
  onChanged(topic: string, messageQueues: MessageQueue[]): void {
    const ids = messageQueues.map((mq) => mq.getQueueId()).sort((a, b) => a - b);
    this.events.push(`${topic}=[${ids.join(',')}]`);
  }
}

async function waitQueueNum(c: DefaultLitePullConsumer, topic: string, want: number,
  timeoutMs: number): Promise<number> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const qs = await c.fetchMessageQueues(topic);
      if (qs.length === want) return Date.now() - start;
    } catch { /* route not updated yet */ }
    await sleep(250);
  }
  return -1;
}

async function waitEvents(rec: Recorder, want: number, timeoutMs: number): Promise<number> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (rec.events.length >= want) return Date.now() - start;
    await sleep(100);
  }
  return -1;
}

async function scaleTopic(queueNum: number, group: string): Promise<void> {
  const p = new DefaultMQProducer(group);
  p.setNamesrvAddr(NS);
  p.start();
  await p.createTopic(TOPIC, p.createTopicKey, queueNum);
  p.shutdown();
}

async function main(): Promise<void> {
  console.log(`nameserver=${NS} topic=${TOPIC} group=${GROUP}`);
  await scaleTopic(BASE_QUEUES, 'GID_NodeLiteQcPrepare');
  await sleep(2000);

  const c = new DefaultLitePullConsumer(GROUP);
  c.setNamesrvAddr(NS);
  c.setTopicMetadataCheckIntervalMillis(CHECK_INTERVAL_MS);
  check('L0 check interval floored at 1s',
    c.getTopicMetadataCheckIntervalMillis() === CHECK_INTERVAL_MS,
    String(c.getTopicMetadataCheckIntervalMillis()));
  c.subscribe(TOPIC, '*');
  await c.start();
  const loopStart = Date.now();

  check('L0 route visible (2 queues)',
    (await waitQueueNum(c, TOPIC, BASE_QUEUES, 30000)) >= 0);

  const rec = new Recorder();
  // Registering while RUNNING snapshots the current set ⇒ first round is quiet.
  await c.registerTopicMessageQueueChangeListener(TOPIC, rec);
  await sleep(3000);
  check('L1 queues unchanged ⇒ quiet', rec.events.length === 0, JSON.stringify(rec.events));

  const wait = FIRST_DELAY_MS - (Date.now() - loopStart);
  if (wait > 0) await sleep(wait);
  check('L1b the scheduled round ran and stayed quiet', rec.events.length === 0,
    JSON.stringify(rec.events));

  // ---- L2 scale out 2 -> 4
  await scaleTopic(SCALED_QUEUES, 'GID_NodeLiteQcUp');
  const nsMs = await waitQueueNum(c, TOPIC, SCALED_QUEUES, 45000);
  check('L2a name server reports 4 queues', nsMs >= 0, `${nsMs}ms`);
  if (nsMs >= 0) {
    const cb = await waitEvents(rec, 1, FRESH_WINDOW_MS);
    check('L2b check round re-queries the route (callback follows, no 30s cache)',
      cb >= 0 && rec.events[0] === `${TOPIC}=[0,1,2,3]`, `${cb}ms ${rec.events[0] || ''}`);
  }

  // ---- L3 snapshot advanced: one change, one callback
  await sleep(3000);
  check('L3 snapshot advanced ⇒ no repeat callback', rec.events.length === 1,
    `${rec.events.length}`);

  // ---- L4 scale back in 4 -> 2
  await scaleTopic(BASE_QUEUES, 'GID_NodeLiteQcDown');
  const nsDown = await waitQueueNum(c, TOPIC, BASE_QUEUES, 45000);
  check('L4a name server reports 2 queues', nsDown >= 0, `${nsDown}ms`);
  if (nsDown >= 0) {
    const cb = await waitEvents(rec, 2, FRESH_WINDOW_MS);
    check('L4b scale-in is seen the same way',
      cb >= 0 && rec.events[1] === `${TOPIC}=[0,1]`, `${cb}ms ${rec.events[1] || ''}`);
  }

  // ---- L5 an unroutable topic is "not found", not "zero queues"
  let raised = '';
  try {
    const qs = await c.fetchMessageQueues(GHOST);
    raised = `no error, queues=${qs.length}`;
  } catch (e) { raised = (e as Error).message; }
  // Either "no route at all" or "route with zero queues" counts: both mean the
  // topic's queue set could not be resolved, and handing back [] would read as
  // a scale-in.
  check('L5 unknown topic throws instead of returning []',
    raised.includes('Can not find Message'), raised);

  await c.shutdown();
  console.log(`\nNode.js lite queue-change live: PASS=${passCount} FAIL=${failCount}`);
  if (failCount > 0) process.exit(1);
}

main().catch((e) => { console.error(e); process.exit(1); });
