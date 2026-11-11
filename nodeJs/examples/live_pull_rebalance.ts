// Live verification of the pull-side rebalance (Java DefaultMQPullConsumer.enableRebalance).
//
// Java's pull consumer rebalances: DefaultMQPullConsumer.java:97 defaults enableRebalance
// to true, and the shared RebalanceService drives tryRebalance() every 20s, calling
// MessageQueueListener.messageQueueChanged(topic, mqAll, mqDivided) when the allocation
// changed. This probe proves against a real broker:
//
//   L1  two instances of one group split a multi-queue topic -- disjoint shares whose
//       union is the whole topic (only possible if both read the same broker-side
//       consumer list and run the same allocation formula);
//   L2  the listener really fired with mqAll = the topic's full queue set;
//   L3  enableRebalance=false stops the loop (no callbacks, empty table) while
//       fetchMessageQueuesInBalance keeps computing on the spot.
//
// Single broker, one topic with 8 queues -- enough for all three: the split is across
// queues within one broker, not across brokers.
//
//   node --experimental-strip-types examples/live_pull_rebalance.ts --ns 127.0.0.1:9876
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import type { MessageQueue } from '../src/common/message.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const PREFIX = `PullRebLive_${Date.now()}`;
const TOPIC = `${PREFIX}_Topic`;
const GROUP = `${PREFIX}_group`;
const N_QUEUE = 8;
// Shorter than Java's 20s so the probe finishes quickly; the loop shape is the point.
const WAIT_MILLIS = 3000;
const MIN_MILLIS = 500;

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`  [PASS] ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`  [FAIL] ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function until(pred: () => boolean, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (pred()) return true;
    await sleep(200);
  }
  return pred();
}

interface RecordedCall { topic: string; mqAll: MessageQueue[]; mqDivided: MessageQueue[]; }
type Recorder = { calls: RecordedCall[]; fn: (t: string, a: MessageQueue[], d: MessageQueue[]) => void };

function makeRecorder(): Recorder {
  const rec: Recorder = { calls: [], fn: () => {} };
  rec.fn = (topic, mqAll, mqDivided) => { rec.calls.push({ topic, mqAll, mqDivided }); };
  return rec;
}

// Identity by (brokerName, queueId): MessageQueue objects never compare by === across
// instances, so the share comparisons must go through the identity tuple.
const idOf = (mq: MessageQueue): string => `${mq.brokerName}#${mq.queueId}`;

function makeConsumer(inst: string, rec: Recorder | null, enable: boolean): DefaultMQPullConsumer {
  const c = new DefaultMQPullConsumer(GROUP);
  // A distinct instanceName gives a distinct clientId; the clientId is what the broker's
  // GET_CONSUMER_LIST_BY_GROUP returns, so without this both facades would be one client.
  c.setInstanceName(inst);
  c.setNamesrvAddr(NS);
  c.setRebalanceWaitMillis(WAIT_MILLIS);
  c.setRebalanceMinIntervalMillis(MIN_MILLIS);
  c.registerMessageQueueListener(TOPIC, rec ? rec.fn : null);
  c.setEnableRebalance(enable);
  return c;
}

async function main(): Promise<void> {
  console.log(`${PREFIX}: pull-side rebalance live (ns=${NS}, topic=${TOPIC}, ${N_QUEUE} queues)`);

  // Create the topic up front: a consumer gets no route for an uncreated topic
  // (TBW102 is a producer-side fallback only).
  const producer = new DefaultMQProducer(`${PREFIX}_prep`);
  producer.setNamesrvAddr(NS);
  producer.start();
  try {
    await producer.createTopic(TOPIC, producer.createTopicKey, N_QUEUE);
  } finally {
    await producer.shutdown();
  }
  // createTopic only reaches the broker; wait until the name server really serves the
  // topic's own route with all 8 read queues.
  {
    const waiter = makeConsumer(`${PREFIX}_wait`, null, true);
    await waiter.start();
    const deadline = Date.now() + 30000;
    let seen = 0;
    while (Date.now() < deadline) {
      try {
        seen = (await waiter.fetchSubscribeMessageQueues(TOPIC)).length;
        if (seen >= N_QUEUE) break;
      } catch { /* route not there yet */ }
      await sleep(300);
    }
    await waiter.shutdown();
    if (seen < N_QUEUE) {
      console.log(`FATAL route for ${TOPIC} never showed ${N_QUEUE} queues (last=${seen})`);
      process.exit(2);
    }
  }
  console.log(`  created ${TOPIC} with ${N_QUEUE} queues`);

  const recA = makeRecorder();
  const recB = makeRecorder();
  const a = makeConsumer(`${PREFIX}InstA`, recA, true);
  const b = makeConsumer(`${PREFIX}InstB`, recB, true);
  let c: DefaultMQPullConsumer | null = null;
  try {
    await a.start();
    await b.start();
    console.log('  started two instances, waiting for the first rebalance rounds...');

    const table = (cc: DefaultMQPullConsumer): MessageQueue[] => cc.getRebalanceTable().get(TOPIC) || [];

    // L1: both must end up with a non-empty share of the topic.
    const got = await until(() => table(a).length > 0 && table(b).length > 0, 30000);
    const shareA = table(a);
    const shareB = table(b);
    console.log(`  INFO share A=${shareA.length} queues, share B=${shareB.length} queues`);
    check('L1-both instances ended up with a share of the topic', got, `A=${shareA.length} B=${shareB.length}`);

    const idsA = new Set(shareA.map(idOf));
    const idsB = new Set(shareB.map(idOf));
    const overlap = [...idsA].filter((x) => idsB.has(x)).length;
    check('L1-the two shares are disjoint (one queue is never owned twice)', overlap === 0, `overlap=${overlap}`);
    const union = new Set([...idsA, ...idsB]);
    const full = Array.from({ length: N_QUEUE }, (_, i) => `broker-a#${i}`)
      .every((id) => union.has(id));
    check('L1-the two shares cover every queue of the topic exactly once',
      full && union.size === N_QUEUE, `union=${union.size} of ${N_QUEUE}`);

    // L2: the listener really ran, and mqAll was the topic's full queue set.
    check('L2-the allocation change was reported to the listener',
      recA.calls.length >= 1 && recB.calls.length >= 1,
      `A calls=${recA.calls.length} B calls=${recB.calls.length}`);
    const lastA = recA.calls[recA.calls.length - 1];
    check('L2-messageQueueChanged carried the topic\'s full queue set as mqAll',
      lastA !== undefined && lastA.mqAll.length === N_QUEUE,
      `mqAll=${lastA ? lastA.mqAll.length : -1}`);
    check('L2-mqDivided matches what this instance reports as its share',
      lastA !== undefined && new Set(lastA.mqDivided.map(idOf)).size === idsA.size &&
        lastA.mqDivided.every((mq) => idsA.has(idOf(mq))),
      `mqDivided=${lastA ? lastA.mqDivided.length : -1} table=${idsA.size}`);

    // The table is the background rebalance's product, so fetchMessageQueuesInBalance
    // must now answer from it (Java reads the process-queue keys).
    const inBalance = await a.fetchMessageQueuesInBalance(TOPIC);
    check('L1-fetchMessageQueuesInBalance answers from the rebalance table',
      inBalance.length === idsA.size && inBalance.every((mq) => idsA.has(idOf(mq))),
      `got=${inBalance.length} expect=${idsA.size}`);

    // L3: the switch really gates the loop.
    const recC = makeRecorder();
    c = makeConsumer(`${PREFIX}InstC`, recC, false);
    await c.start();
    console.log('  started a third instance with enableRebalance=false, waiting 2 rounds...');
    await sleep(WAIT_MILLIS * 2 + 4000);
    check('L3-enableRebalance=false stops the loop: no callback at all',
      recC.calls.length === 0, `calls=${recC.calls.length}`);
    check('L3-enableRebalance=false leaves the rebalance table empty',
      c.getRebalanceTable().size === 0, `table size=${c.getRebalanceTable().size}`);
    const onTheSpot = await c.fetchMessageQueuesInBalance(TOPIC);
    check('L3-with the loop off, fetchMessageQueuesInBalance still computes on the spot',
      onTheSpot.length > 0, `got=${onTheSpot.length} queues`);
  } finally {
    for (const cc of [c, b, a]) {
      if (cc) { try { await cc.shutdown(); } catch { /* best effort */ } }
    }
  }

  console.log(`\n${PREFIX}: ${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error('FATAL', e);
  process.exit(2);
});
