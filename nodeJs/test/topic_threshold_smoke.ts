// Topic-level cache budgets divide into the per-queue knobs AT REBALANCE —
// offline test of _divideTopicThresholds, its placement inside doRebalance,
// and the pull gate no longer aggregating sibling queues.
//
// Contract pinned here (each positive has a paired control that goes red when
// the division is reverted to the old pull-time topic aggregation):
//   * an enabled topic budget (-1 = off) is divided over ALL queues this
//     consumer owns right now — across every topic, not one topic's own — and
//     only its matching per-queue knob moves; an off budget leaves the user's
//     per-queue value alone (a default-on or unconditional division is caught
//     there);
//   * the quotient never drops below 1: a budget smaller than the queue count
//     still lets one queue cache a message;
//   * owning no queue divides nothing: both knobs keep exactly the user's values
//     (a missing zero-guard would be caught by the divide-by-zero path);
//   * the dividend is always the topic knob, never the current per-queue value,
//     so re-running with an unchanged queue count rewrites the same number
//     (1000/8 = 125, not the previous 250/8 = 31) and logs nothing new;
//   * under POP the divisor is popQueueTable, not processQueueTable;
//   * the division runs only on a CHANGED assignment and only AFTER
//     syncPullLoops rebuilt the pull state: an unchanged round leaves a hand-set
//     knob alone, and a round that shrinks 4 queues to 2 divides by 2 (500),
//     not by the stale 4 (250);
//   * _flowControlHit judges ONE queue's own buffer against the divided
//     per-queue knobs and NEVER adds up sibling queues: 4 queues at 250 each
//     under a 1000-count budget stay unpaused; the 251st message trips exactly
//     one queue with reason 'count' — the removed reasons 'topicCount' and
//     'topicSize' must not appear anywhere.
//
// Run: node --experimental-strip-types test/topic_threshold_smoke.ts
import { format } from 'node:util';
import { MessageQueue, MessageExt } from '../src/common/message.ts';
import { ProcessQueue } from '../src/client/process_queue.ts';
import { PopProcessQueue } from '../src/client/pop_process_queue.ts';
import { SubscriptionData } from '../src/common/subscriptionData.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MessageModel } from '../src/remoting/heartbeat.ts';
import { mqKey } from '../src/client/offset_store.ts';
import { getLogger } from '../src/logging.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'NodeJsTopicThresholdUnitTopic';
const OTHER_TOPIC = 'NodeJsTopicThresholdUnitOther';
const consumerLogger = getLogger('client.consumer');

function queue(topic = TOPIC, queueId = 0): MessageQueue {
  return new MessageQueue(topic, 'broker-a', queueId);
}

function msg(offset: number, bodyBytes = 10): MessageExt {
  const m = new MessageExt(TOPIC, Buffer.alloc(bodyBytes, 0x78));
  m.setQueueOffset(offset);
  m.setBrokerName('broker-a');
  m.storeSize = bodyBytes;
  return m;
}

function nMsgs(n: number): MessageExt[] {
  const out: MessageExt[] = [];
  for (let i = 0; i < n; i++) out.push(msg(i));
  return out;
}

// stagePQ puts a ProcessQueue holding `count` messages into the classic table.
function stagePQ(c: DefaultMQPushConsumer, mq: MessageQueue, count: number): ProcessQueue {
  const pq = new ProcessQueue(false);
  if (count > 0) pq.putMessage(nMsgs(count));
  c.processQueueTable.set(mqKey(mq), pq);
  return pq;
}

// ---------------------------------------------------------------------------
// 1) each enabled budget divides over the owned queues and only its own knob moves
// ---------------------------------------------------------------------------
{
  const c = new DefaultMQPushConsumer('G_nodejs_tt_count_only');
  c.pullThresholdForTopic = 1000;
  c.pullThresholdSizeForTopic = -1;
  c.pullThresholdForQueue = 1000;
  c.pullThresholdSizeForQueue = 55; // the user's own size knob, off-budget path
  for (const id of [0, 1, 2, 3]) stagePQ(c, queue(TOPIC, id), 0);
  (c as any)._divideTopicThresholds();
  check('count budget 1000 over 4 queues divides to 250', c.pullThresholdForQueue === 250,
    `got ${c.pullThresholdForQueue}`);
  // Control: an off (-1) size budget must leave the user's value — a division
  // applied to both knobs unconditionally is red here.
  check('the off size budget leaves the user value 55 alone', c.pullThresholdSizeForQueue === 55,
    `got ${c.pullThresholdSizeForQueue}`);
}
{
  const c = new DefaultMQPushConsumer('G_nodejs_tt_size_only');
  c.pullThresholdForTopic = -1;
  c.pullThresholdSizeForTopic = 80;
  c.pullThresholdForQueue = 7; // the user's own count knob, off-budget path
  c.pullThresholdSizeForQueue = 100;
  for (const id of [0, 1, 2, 3]) stagePQ(c, queue(TOPIC, id), 0);
  (c as any)._divideTopicThresholds();
  check('size budget 80 over 4 queues divides to 20', c.pullThresholdSizeForQueue === 20,
    `got ${c.pullThresholdSizeForQueue}`);
  // Control: the off count budget must not touch the user's 7.
  check('the off count budget leaves the user value 7 alone', c.pullThresholdForQueue === 7,
    `got ${c.pullThresholdForQueue}`);
}

// ---------------------------------------------------------------------------
// 2) both budgets off -> per-queue knobs untouched (reddens an unconditional division)
// ---------------------------------------------------------------------------
{
  const c = new DefaultMQPushConsumer('G_nodejs_tt_both_off');
  c.pullThresholdForTopic = -1;
  c.pullThresholdSizeForTopic = -1;
  c.pullThresholdForQueue = 7;
  c.pullThresholdSizeForQueue = 55;
  for (const id of [0, 1, 2, 3]) stagePQ(c, queue(TOPIC, id), 0);
  (c as any)._divideTopicThresholds();
  check('both budgets off: pullThresholdForQueue stays at the user value 7',
    c.pullThresholdForQueue === 7, `got ${c.pullThresholdForQueue}`);
  check('both budgets off: pullThresholdSizeForQueue stays at the user value 55',
    c.pullThresholdSizeForQueue === 55, `got ${c.pullThresholdSizeForQueue}`);
}

// ---------------------------------------------------------------------------
// 3) a budget smaller than the queue count floors at 1, never 0
// ---------------------------------------------------------------------------
{
  const c = new DefaultMQPushConsumer('G_nodejs_tt_floor_one');
  c.pullThresholdForTopic = 3;
  c.pullThresholdSizeForTopic = 2;
  for (let id = 0; id < 8; id++) stagePQ(c, queue(TOPIC, id), 0);
  (c as any)._divideTopicThresholds();
  check('count budget 3 over 8 queues floors at 1, not 0', c.pullThresholdForQueue === 1,
    `got ${c.pullThresholdForQueue}`);
  check('size budget 2 over 8 queues floors at 1, not 0', c.pullThresholdSizeForQueue === 1,
    `got ${c.pullThresholdSizeForQueue}`);
}

// ---------------------------------------------------------------------------
// 4) owning no queue -> both knobs left exactly as the user set them
// ---------------------------------------------------------------------------
{
  const c = new DefaultMQPushConsumer('G_nodejs_tt_no_queue');
  c.pullThresholdForTopic = 1000;
  c.pullThresholdSizeForTopic = 80;
  c.pullThresholdForQueue = 66;
  c.pullThresholdSizeForQueue = 77;
  (c as any)._divideTopicThresholds();
  check('zero owned queues leaves pullThresholdForQueue at 66', c.pullThresholdForQueue === 66,
    `got ${c.pullThresholdForQueue}`);
  check('zero owned queues leaves pullThresholdSizeForQueue at 77', c.pullThresholdSizeForQueue === 77,
    `got ${c.pullThresholdSizeForQueue}`);
}

// ---------------------------------------------------------------------------
// 5) idempotence + the divisor follows the queue count (1000/8 = 125, not 250/8 = 31)
// ---------------------------------------------------------------------------
{
  const c = new DefaultMQPushConsumer('G_nodejs_tt_idempotent');
  c.pullThresholdForTopic = 1000;
  c.pullThresholdSizeForTopic = -1;
  for (const id of [0, 1, 2, 3]) stagePQ(c, queue(TOPIC, id), 0);
  (c as any)._divideTopicThresholds();
  check('first division over 4 queues: 1000/4 = 250', c.pullThresholdForQueue === 250,
    `got ${c.pullThresholdForQueue}`);
  (c as any)._divideTopicThresholds();
  check('re-running with an unchanged count rewrites the same 250 (the dividend is the topic knob)',
    c.pullThresholdForQueue === 250, `got ${c.pullThresholdForQueue}`);
  // Grow to 8: dividing the CURRENT value (250/8=31) instead of the topic knob
  // (1000/8=125) is exactly the non-idempotent bug.
  for (const id of [4, 5, 6, 7]) stagePQ(c, queue(TOPIC, id), 0);
  (c as any)._divideTopicThresholds();
  check('8 queues divide 1000/8 = 125, not the stale 250/8 = 31',
    c.pullThresholdForQueue === 125, `got ${c.pullThresholdForQueue}`);
}

// ---------------------------------------------------------------------------
// 6) the divisor counts queues across ALL topics, not one topic's queues
// ---------------------------------------------------------------------------
{
  const c = new DefaultMQPushConsumer('G_nodejs_tt_all_topics');
  c.pullThresholdForTopic = 1000;
  c.pullThresholdSizeForTopic = -1;
  for (const id of [0, 1]) stagePQ(c, queue(TOPIC, id), 0);
  for (const id of [0, 1]) stagePQ(c, queue(OTHER_TOPIC, id), 0);
  (c as any)._divideTopicThresholds();
  // Dividing only TOPIC's 2 queues would give 500; the whole set of 4 gives 250.
  check('the divisor is the queue count across all topics (4 -> 250, not one topic\'s 2 -> 500)',
    c.pullThresholdForQueue === 250, `got ${c.pullThresholdForQueue}`);
}

// ---------------------------------------------------------------------------
// 7) POP mode divides using the POP state table
// ---------------------------------------------------------------------------
{
  const c = new DefaultMQPushConsumer('G_nodejs_tt_pop');
  c.setPopMode(true);
  c.pullThresholdForTopic = 1000;
  c.pullThresholdSizeForTopic = -1;
  for (const id of [0, 1, 2, 3]) c.popQueueTable.set(mqKey(queue(TOPIC, id)), new PopProcessQueue());
  // Control on the other table: 8 pull queues staged that must NOT be the
  // divisor under POP (using them would give 125).
  for (let id = 0; id < 8; id++) stagePQ(c, queue(TOPIC, id), 0);
  (c as any)._divideTopicThresholds();
  check('POP divides over popQueueTable (4 queues -> 250), not processQueueTable (8 -> 125)',
    c.pullThresholdForQueue === 250, `got ${c.pullThresholdForQueue}`);

  // Zero-owned control: in POP with an EMPTY popQueueTable the 8 pull queues
  // are meaningless and both knobs keep the user's values.
  const z = new DefaultMQPushConsumer('G_nodejs_tt_pop_empty');
  z.setPopMode(true);
  z.pullThresholdForTopic = 1000;
  z.pullThresholdSizeForTopic = 80;
  z.pullThresholdForQueue = 66;
  z.pullThresholdSizeForQueue = 77;
  for (let id = 0; id < 8; id++) stagePQ(z, queue(TOPIC, id), 0);
  (z as any)._divideTopicThresholds();
  check('POP with no owned queue leaves both knobs at the user values',
    z.pullThresholdForQueue === 66 && z.pullThresholdSizeForQueue === 77,
    `got ${z.pullThresholdForQueue}/${z.pullThresholdSizeForQueue}`);
}

// ---------------------------------------------------------------------------
// log capture for the INFO change lines
// ---------------------------------------------------------------------------
const infoLines: string[] = [];
const origInfo = consumerLogger.info;
consumerLogger.info = (...args: any[]) => { infoLines.push(format(...args)); };

// ---------------------------------------------------------------------------
// 8) the real doRebalance: only on a CHANGED assignment, only AFTER the
//    pull-state rebuild (divisor = this round's count)
// ---------------------------------------------------------------------------
async function rebalanceHarness(group: string): Promise<any> {
  const c: any = new DefaultMQPushConsumer(group);
  c.setMessageModel(MessageModel.BROADCASTING); // every route queue is ours, no broker member list
  c.subscription.set(TOPIC, new SubscriptionData(TOPIC, '*'));
  c.started = true;
  const holder = { queues: [] as MessageQueue[] };
  c.mqClient = {
    getTopicRouteData: () => ({ getAllSubscribeMessageQueue: () => holder.queues }),
    sendHeartbeatToAllBrokers: async () => {},
  };
  c.setQueues = (ids: number[]) => {
    holder.queues = ids.map((id) => new MessageQueue(TOPIC, 'broker-a', id));
  };
  // Offline stubs for the loops the rebalance kicks off: the pull kernel and
  // the initial-offset RPC are covered by other suites; the table rebuild —
  // the part the division's ORDER depends on — runs for real.
  c.computePullFromWhere = async () => -1;
  c._queuePullLoop = async () => {};
  return c;
}
{
  const c = await rebalanceHarness('G_nodejs_tt_rebalance');
  c.pullThresholdForTopic = 1000;
  c.pullThresholdSizeForTopic = -1;
  c.setQueues([0, 1, 2, 3]);
  await c.doRebalance();
  check('round 1 (4 queues): the real doRebalance divides to 250',
    c.pullThresholdForQueue === 250, `got ${c.pullThresholdForQueue}`);
  check('the rebuild precedes the division: 4 process queues are live',
    c.processQueueTable.size === 4, `got ${c.processQueueTable.size}`);

  // Unchanged round: a hand-set knob must survive, i.e. no division at all.
  c.pullThresholdForQueue = 42;
  infoLines.length = 0;
  await c.doRebalance();
  check('unchanged round: the hand-set 42 is left alone (division only on a change)',
    c.pullThresholdForQueue === 42, `got ${c.pullThresholdForQueue}`);
  check('unchanged round logs no threshold change',
    !infoLines.some((l) => l.includes('is changed from')), infoLines.join(' | '));

  // Shrinking round 4 -> 2: dividing AFTER syncPullLoops means the divisor is
  // the NEW count (1000/2 = 500). Dividing before the rebuild — over the
  // stale 4 — would give 250; dividing the current value would give 21.
  c.setQueues([0, 1]);
  infoLines.length = 0;
  await c.doRebalance();
  check('shrinking 4 -> 2 divides by the NEW count: 1000/2 = 500, not the stale 250',
    c.pullThresholdForQueue === 500, `got ${c.pullThresholdForQueue}`);
  check('the shrink rebuilt the table first: 2 process queues are live',
    c.processQueueTable.size === 2, `got ${c.processQueueTable.size}`);
  check('the change is logged as "The pullThresholdForQueue is changed from 42 to 500"',
    infoLines.some((l) => l === 'The pullThresholdForQueue is changed from 42 to 500'),
    infoLines.join(' | '));

  // Control with both topic budgets off on the SAME real path: the hand-set
  // knobs must survive a changed round untouched.
  const off = await rebalanceHarness('G_nodejs_tt_rebalance_off');
  off.pullThresholdForTopic = -1;
  off.pullThresholdSizeForTopic = -1;
  off.pullThresholdForQueue = 7;
  off.pullThresholdSizeForQueue = 55;
  off.setQueues([0, 1, 2, 3]);
  await off.doRebalance();
  check('control: changed round with both budgets off leaves the user knobs 7/55',
    off.pullThresholdForQueue === 7 && off.pullThresholdSizeForQueue === 55,
    `got ${off.pullThresholdForQueue}/${off.pullThresholdSizeForQueue}`);
}

// Real doRebalance on the POP path: syncPullLoops branches to the POP twin.
{
  const c = await rebalanceHarness('G_nodejs_tt_rebalance_pop');
  c.setPopMode(true);
  c._sleepOrStop = async () => true; // the background pop loop exits on its first wait
  c.mqClient.popMessage = async () => { throw new Error('no broker in this smoke'); };
  c.pullThresholdForTopic = 1000;
  c.pullThresholdSizeForTopic = -1;
  c.setQueues([0, 1, 2, 3]);
  await c.doRebalance();
  check('POP round 1: the real doRebalance divides over popQueueTable to 250',
    c.pullThresholdForQueue === 250 && c.popQueueTable.size === 4 && c.processQueueTable.size === 0,
    `threshold=${c.pullThresholdForQueue} pop=${c.popQueueTable.size} pull=${c.processQueueTable.size}`);
}

// ---------------------------------------------------------------------------
// 9) the pull gate never adds up sibling queues
// ---------------------------------------------------------------------------
const debugLines: string[] = [];
const origDebug = consumerLogger.debug;
consumerLogger.debug = (...args: any[]) => { debugLines.push(format(...args)); };

function hit(c: DefaultMQPushConsumer, mq: MessageQueue, pq: ProcessQueue): boolean {
  return (c as any)._flowControlHit(mq, pq);
}

{
  const c = new DefaultMQPushConsumer('G_nodejs_tt_gate_no_sum');
  // The topic budget stays ENABLED at 1000: the point is that the pull-time
  // gate does not look at it any more — only the divided per-queue knobs count.
  c.pullThresholdForTopic = 1000;
  c.pullThresholdSizeForTopic = -1;
  // The other two gates are parked out of reach rather than "disabled": 0 would
  // mean "cache nothing" and every queue below would trip them.
  c.pullThresholdSizeForQueue = 1 << 30;
  c.consumeConcurrentlyMaxSpan = 1 << 30;
  const queues = [0, 1, 2, 3].map((id) => queue(TOPIC, id));
  for (const mq of queues) stagePQ(c, mq, 250);
  (c as any)._divideTopicThresholds(); // 1000/4 -> pullThresholdForQueue = 250
  check('the divided knob is 250 before the gate runs', c.pullThresholdForQueue === 250);
  const atShare = queues.map((mq) => hit(c, mq, c.processQueueTable.get(mqKey(mq))!));
  // Old code: 4 x 250 = 1000 >= topicCount budget -> every queue paused. This
  // is the assertion that reddens the aggregation.
  check('4 queues at their divided share (250 each, budget 1000) none pauses',
    atShare.every((h) => !h), JSON.stringify(atShare));
  check('nothing was counted', c.flowControlTriggered === 0);

  // One queue at 251 trips on its OWN count; its 3 siblings at 250 stay open.
  debugLines.length = 0;
  const hot = queues[0];
  const hotPq = c.processQueueTable.get(mqKey(hot))!;
  hotPq.putMessage([msg(250)]); // fresh offset: 251 in this queue alone
  check('the queue at 251 pauses', hit(c, hot, hotPq));
  const reason = debugLines.find((l) => l.startsWith('flow control: queue')) || '';
  check('the pause reason names the count gate and the number measured',
    reason.includes(' count=251, pause pull'), reason);
  check('the removed topic reasons do not appear anywhere',
    !reason.includes('topicCount') && !reason.includes('topicSize'), reason);
  const siblings = queues.slice(1).map((mq) => hit(c, mq, c.processQueueTable.get(mqKey(mq))!));
  check('the 3 siblings at 250 stay unpaused while the topic total is 1001',
    siblings.every((h) => !h), JSON.stringify(siblings));
  check('exactly one judgment was counted', c.flowControlTriggered === 1);
}

consumerLogger.info = origInfo;
consumerLogger.debug = origDebug;

console.log(`\ntopic threshold division: ${pass} checks, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
