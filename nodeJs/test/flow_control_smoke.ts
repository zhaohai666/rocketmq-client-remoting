// Push-consumer cache flow control — offline test of the three per-queue gates
// (count / size / span) and the log cadence.
//
// Baseline this suite pins:
//   * every cache gate compares STRICTLY GREATER THAN its threshold, so a buffer that
//     has reached the threshold exactly is still allowed to pull. A threshold reads as
//     "how many may be cached", not "how many stop us";
//   * the byte gate is in MiB, not bytes — the single most expensive unit mistake here,
//     because a 100x error silently disables backpressure;
//   * the span gate measures the OFFSET span (max-min), so one message that keeps failing
//     cannot let the offset run away;
//   * the switches: 0 disables the queue-level byte gate, and a count threshold of 0
//     clamps to 1; the topic-level knobs (-1 = off) are NOT pull-time gates at all —
//     they are divided into the per-queue knobs at rebalance (topic_threshold_smoke);
//   * the gate measures the not-yet-dispatched remainder, and one judgment records exactly
//     one tick even when several gates match;
//   * the WARN fires on the 1st trip and every 1000th after, while the counter keeps
//     counting every trip;
//   * popThresholdForQueue defaults to 96, not to the pull-side 1000;
//   * the POP cache gate judges ONE queue's own outstanding-ACK debt, strictly
//     greater than popThresholdForQueue, and a trip is a flow-control event of
//     this client: it lands on the same counter and the same WARN cadence as the
//     pull gate, so flowControlTimes is one client-wide number over both paths;
//   * a stopped round waits with the right number: 50ms for this client's own cache
//     gate, 20ms for the broker's 215, 500ms for every other failed pull — and no
//     wait at all for an idle long-poll timeout. The broker's answer has to reach the
//     loop as a coded exception, or the short wait silently never applies.
//
// The LITE PULL legs below pin the same two rules on the path that has NO local
// cache and NO background pull task — DefaultLitePullConsumer.poll() pulls inline
// inside the caller's deadline and hands the messages straight back, so the only
// structural pause it can honour is the broker's own throttle answer:
//   * a 215 parks THAT queue for 20ms (the same exported constant the push path
//     uses) and the queue is pulled again once the park expires, while every other
//     code keeps today's behaviour — the idle single-queue guard, unchanged;
//   * a parked queue never holds up its siblings inside one poll() deadline, and
//     a round where every queue is parked waits out the SHORTEST remaining park
//     instead of burning the caller's deadline;
//   * the pull kernel hands the broker code to its caller on a typed exception, so
//     both branches above can actually read it.
// This path has no client-side cache gates, so it has no flow-control counters:
// flowControlTimes belongs to the push and POP gates asserted above and is not
// moved by anything below.
//
// Run: node --experimental-strip-types test/flow_control_smoke.ts
import net from 'node:net';

import { MessageQueue, MessageExt } from '../src/common/message.ts';
import { ProcessQueue } from '../src/client/process_queue.ts';
import { PopProcessQueue } from '../src/client/pop_process_queue.ts';
import { SubscriptionData } from '../src/common/subscriptionData.ts';
import { PullResult, PullStatus } from '../src/client/consumer_result.ts';
import { PopResult, processPopResponse } from '../src/client/pop_api.ts';
import { PopMessageResponseHeader } from '../src/remoting/headers.ts';
import { queuePopLoop, POP_DELAY_WHEN_EXCEPTION, POP_DELAY_WHEN_CACHE_FLOW_CONTROL } from '../src/client/pop_consumer.ts';
import { MQBrokerException, RemotingTimeoutException } from '../src/remoting/exception.ts';
import {
  DefaultMQPushConsumer, isFlowControlWarnTrip, pullRetryDelayMillisFor,
  PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL, PULL_TIME_DELAY_MILLS_WHEN_PULL_FAILED,
} from '../src/client/consumer.ts';
import { DefaultLitePullConsumer, litePullParkMillisFor } from '../src/client/lite_pull_consumer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { encodeMessageExt } from '../src/common/messageDecoder.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { mqKey } from '../src/client/offset_store.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'NodeJsFlowControlUnitTopic';
const OTHER_TOPIC = 'NodeJsFlowControlUnitOther';
const MIB = 1024 * 1024;

function queue(topic = TOPIC, queueId = 0): MessageQueue {
  return new MessageQueue(topic, 'broker-a', queueId);
}

function msg(offset: number, bodyBytes: number, topic = TOPIC): MessageExt {
  const m = new MessageExt(topic, Buffer.alloc(bodyBytes, 0x78));
  m.setQueueOffset(offset);
  m.setBrokerName('broker-a');
  m.storeSize = bodyBytes;
  return m;
}

// gatesOff leaves every gate disabled except the one under test: an earlier hit would mask
// the branch being asserted.
function gatesOff(c: DefaultMQPushConsumer): DefaultMQPushConsumer {
  c.pullThresholdForQueue = 2 ** 31 - 1;
  c.pullThresholdSizeForQueue = 0;
  c.consumeConcurrentlyMaxSpan = 0;
  c.pullThresholdForTopic = -1;
  c.pullThresholdSizeForTopic = -1;
  return c;
}

function hit(c: DefaultMQPushConsumer, mq: MessageQueue, msgs: MessageExt[]): boolean {
  const pq = new ProcessQueue(false);
  pq.putMessage(msgs);
  c.processQueueTable.set(mqKey(mq), pq);
  return (c as any)._flowControlHit(mq, pq);
}

function consumer(group: string): DefaultMQPushConsumer {
  return new DefaultMQPushConsumer(group);
}

// ---- defaults ----------------------------------------------------------------
{
  const c = consumer('G_nodejs_fc_defaults');
  check('pullThresholdForQueue defaults to 1000', c.pullThresholdForQueue === 1000);
  check('pullThresholdSizeForQueue defaults to 100 MiB', c.pullThresholdSizeForQueue === 100);
  check('consumeConcurrentlyMaxSpan defaults to 2000', c.consumeConcurrentlyMaxSpan === 2000);
  check('the topic-level thresholds default to off',
    c.pullThresholdForTopic === -1 && c.pullThresholdSizeForTopic === -1);
  // popThresholdForQueue is the POP backlog in messages, NOT the pull backlog: 96, not 1000.
  check('popThresholdForQueue defaults to 96', c.popThresholdForQueue === 96,
    `got ${c.popThresholdForQueue}`);
  check('a default-gated buffer of one small message does not hit', !hit(c, queue(), [msg(0, 100)]));
  check('an empty buffer does not hit', !hit(c, queue(), []));
  check('nothing was counted', c.flowControlTriggered === 0);
}

// ---- count gate --------------------------------------------------------------
{
  const c = gatesOff(consumer('G_nodejs_fc_count'));
  c.pullThresholdForQueue = 3;
  check('2 messages at threshold 3 do not hit', !hit(c, queue(), [msg(0, 10), msg(1, 10)]));
  // The boundary: reaching the threshold exactly still pulls.
  check('3 messages at threshold 3 do NOT hit (>, not >=)',
    !hit(c, queue(), [msg(0, 10), msg(1, 10), msg(2, 10)]));
  check('4 messages at threshold 3 hit',
    hit(c, queue(), [msg(0, 10), msg(1, 10), msg(2, 10), msg(3, 10)]));
  check('one judgment records one tick', c.flowControlTriggered === 1);

  // threshold 0 clamps to 1, which under ">" means the 2nd message trips.
  const z = gatesOff(consumer('G_nodejs_fc_count_zero'));
  z.pullThresholdForQueue = 0;
  check('threshold 0 lets one cached message through', !hit(z, queue(), [msg(0, 10)]));
  check('threshold 0 clamps to 1 and trips on the 2nd message', hit(z, queue(), [msg(0, 10), msg(1, 10)]));
}

// ---- size gate ---------------------------------------------------------------
{
  const c = gatesOff(consumer('G_nodejs_fc_size'));
  c.pullThresholdSizeForQueue = 1;
  check('900 bytes under a 1 MiB threshold do not hit', !hit(c, queue(), [msg(0, 300), msg(1, 300), msg(2, 300)]));
  check('exactly 1 MiB does NOT hit (>, not >=)', !hit(c, queue(), [msg(0, MIB)]));
  // One byte over the boundary has to hit: this separates ">" from a sloppy "much greater".
  check('1 MiB + 1 byte hits', hit(c, queue(), [msg(0, MIB + 1)]));
  check('2 MiB hits', hit(c, queue(), [msg(0, 2 * MIB)]));
  c.pullThresholdSizeForQueue = 0;
  check('threshold 0 disables the byte gate', !hit(c, queue(), [msg(0, 8 * MIB)]));
}

// ---- span gate ---------------------------------------------------------------
{
  const c = gatesOff(consumer('G_nodejs_fc_span'));
  c.consumeConcurrentlyMaxSpan = 10;
  check('a span of 100 over maxSpan 10 hits', hit(c, queue(), [msg(0, 1), msg(100, 1)]));
  c.consumeConcurrentlyMaxSpan = 100;
  check('a span of exactly 100 does not hit', !hit(c, queue(), [msg(0, 1), msg(100, 1)]));
  c.consumeConcurrentlyMaxSpan = 10;
  // Out-of-order buffering must still measure max-min, not first-last.
  check('an out-of-order buffer measures max-min', hit(c, queue(), [msg(50, 1), msg(5, 1), msg(7, 1)]));
  c.consumeConcurrentlyMaxSpan = 0;
  check('maxSpan 0 disables the span gate', !hit(c, queue(), [msg(0, 1), msg(1_000_000_000, 1)]));
}

// ---- topic budgets NEVER aggregate at pull time -----------------------------
// The old contract summed every queue of a topic on each pull judgment; the new
// one divides the topic budget into the per-queue knob at rebalance. A pull-time
// topic branch is exactly what must stay dead, so the sibling queues below are
// staged well past the topic budget and the judgment must not react.
{
  const c = gatesOff(consumer('G_nodejs_fc_topic'));
  c.pullThresholdForTopic = 2;
  c.pullThresholdSizeForTopic = 2;
  const q0 = queue(TOPIC, 0), q1 = queue(TOPIC, 1);
  check('one message in one queue does not hit', !hit(c, q0, [msg(0, 1)]));
  const sib = new ProcessQueue(false);
  sib.putMessage([msg(0, 1), msg(1, 1), msg(2, 1)]);
  c.processQueueTable.set(mqKey(q1), sib);
  check('sibling queues of the same topic no longer add up at pull time',
    !hit(c, q0, [msg(0, 1), msg(1, 1), msg(2, 1)]));
  check('the removed aggregation counted nothing', c.flowControlTriggered === 0);
  // Paired control on the SAME staged buffers: the per-queue count gate still
  // bites on the very message set that used to trip the topic branch — the
  // budget is now enforced only through the divided per-queue knob.
  c.pullThresholdForQueue = 2;
  check('the per-queue gate trips where the topic aggregation used to',
    hit(c, q0, [msg(0, 1), msg(1, 1), msg(2, 1)]));
  check('one judgment records one tick', c.flowControlTriggered === 1);
  // Another topic must never mix in either way.
  const c2 = gatesOff(consumer('G_nodejs_fc_topic_other'));
  c2.pullThresholdForTopic = 2;
  c2.processQueueTable.set(mqKey(queue(OTHER_TOPIC, 0)), new ProcessQueue(false));
  c2.processQueueTable.get(mqKey(queue(OTHER_TOPIC, 0)))!.putMessage([msg(0, 1), msg(1, 1), msg(2, 1)]);
  check("another topic's buffer does not count toward this topic", !hit(c2, q0, [msg(0, 1)]));
}

// ---- gate order and one tick -------------------------------------------------
{
  const c = consumer('G_nodejs_fc_all');
  c.pullThresholdForQueue = 1;
  c.pullThresholdSizeForQueue = 1;
  c.consumeConcurrentlyMaxSpan = 1;
  check('several matching gates still record one tick', hit(c, queue(), [msg(0, 2 * MIB), msg(100, 2 * MIB)]));
  check('the counter is exactly 1', c.flowControlTriggered === 1);
}

// ---- warn cadence ------------------------------------------------------------
{
  check('the 1st trip warns', isFlowControlWarnTrip(1));
  check('the 1001st trip warns', isFlowControlWarnTrip(1001));
  check('the 2001st trip warns', isFlowControlWarnTrip(2001));
  // Control: everything between the boundaries stays on the debug channel only, so a
  // cadence degraded to "warn always" or "warn once and stop" is caught here.
  for (const n of [0, 2, 999, 1000, 1002, 2000]) {
    check(`trip ${n} does not warn`, !isFlowControlWarnTrip(n));
  }

  // The cadence must be driven off the same counter the public field exposes.
  const c = gatesOff(consumer('G_nodejs_fc_warn'));
  c.pullThresholdForQueue = 1;
  const msgs = [msg(0, 10), msg(1, 10)];
  for (let n = 1; n <= 1000; n++) {
    hit(c, queue(), msgs);
    if (c.flowControlTriggered !== n) { fail++; console.log(`  FAIL counter drifted at trip ${n}`); break; }
  }
  check('1000 trips counted', c.flowControlTriggered === 1000);
  check('the 1000th trip does not warn', !isFlowControlWarnTrip(c.flowControlTriggered));
  hit(c, queue(), msgs);
  check('the 1001st trip warns', isFlowControlWarnTrip(c.flowControlTriggered));
}

// ---- the three different waits a throttled or failed round produces ---------
// 50 ms (this client's own cache gate), 20 ms (the broker's 215 answer) and
// 500 ms (every other failure) have to stay three separate numbers: collapse
// them and a throttled queue either hammers the broker or stalls the consumer.
// The mapping is driven through the real loop with the waits RECORDED instead of
// slept, so a branch that never runs cannot pass a value check by accident.
{
  check('only the broker 215 answer gets the short 20ms wait',
    pullRetryDelayMillisFor(215) === 20 && PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL === 20);
  check('every other broker answer gets the generic 500ms wait',
    pullRetryDelayMillisFor(2) === 500 && PULL_TIME_DELAY_MILLS_WHEN_PULL_FAILED === 500);
  // Controls on the wire code: neighbours of 215 and the codes that mean something
  // else on a pull answer must NOT be treated as throttling.
  for (const code of [1, 2, 24, 25, 26, 214, 216, 1006, 3000]) {
    check(`code ${code} does not get the 20ms wait`, pullRetryDelayMillisFor(code) === 500);
  }
  check('a failure that never reached the wire gets the generic wait',
    pullRetryDelayMillisFor(undefined) === 500);
  // The three waits stay apart (20 < 50 < 500): the cache gate's own value is
  // asserted from the loop below, where it is the only pause that can appear.
  check('the broker wait is shorter than the cache wait, the generic wait longer',
    pullRetryDelayMillisFor(215) < 50 && 50 < pullRetryDelayMillisFor(2));
}

function nMsgs(n: number): MessageExt[] {
  const out: MessageExt[] = [];
  for (let i = 0; i < n; i++) out.push(msg(i, 10));
  return out;
}

// drivePullLoop runs the real per-queue loop over one round. `threshold`/`cached`
// set up the cache gate, `failWith` is the answer the pull comes back with. The
// waits are recorded, never slept, and each recorded wait retires the queue's stop
// flag, so the loop exits right after the round under test. `rounds` counts the
// requests that reached the wire: a gate that stopped the pull must leave it at 0.
async function drivePullLoop(opts: {
  group: string, threshold?: number, cached?: number, failWith?: Error,
}): Promise<{ waits: number[], rounds: number }> {
  const c = gatesOff(consumer(opts.group));
  if (opts.threshold !== undefined) c.pullThresholdForQueue = opts.threshold;
  const mq = queue();
  const anyC = c as any;
  c.subscription.set(TOPIC, new SubscriptionData(TOPIC, '*'));
  const pq = new ProcessQueue(false);
  if (opts.cached) pq.putMessage(nMsgs(opts.cached));
  c.processQueueTable.set(mqKey(mq), pq);
  c.offsetTable.set(mqKey(mq), 0);
  c.started = true;
  c.mqClient = { updateTopicRouteInfoFromNameServer: async () => {} } as never;
  anyC._findBrokerAddressInSubscribe = () => ({ addr: '127.0.0.1:10911', isSlave: false });
  const flag = anyC._newStopFlag();
  let rounds = 0;
  anyC._pullKernel = async () => {
    rounds++;
    // Only the round under test fails; the next one retires the queue so a round
    // that asked for NO wait (the long-poll timeout) still terminates.
    if (rounds === 1 && opts.failWith) throw opts.failWith;
    anyC._stopFlags.delete(flag);
    return new PullResult(PullStatus.NO_NEW_MSG, 0, 0, 0, []);
  };
  const waits: number[] = [];
  anyC._sleepOrStop = async (ms: number) => {
    waits.push(ms);
    anyC._stopFlags.delete(flag);
    return true;
  };
  await anyC._queuePullLoop(mq, flag);
  return { waits, rounds };
}

{
  // The cache gate, driven end to end: a buffer of 2 over a threshold of 1.
  const gated = await drivePullLoop({ group: 'G_nodejs_fc_cache_pause', threshold: 1, cached: 2 });
  check('the cache gate re-schedules the queue after exactly 50ms',
    gated.waits.length === 1 && gated.waits[0] === 50, JSON.stringify(gated.waits));
  check('a round the cache gate stopped never puts a pull on the wire',
    gated.rounds === 0, `rounds=${gated.rounds}`);

  // Paired control on the same driver: a buffer that only REACHED the threshold
  // (strict >) pulls, and the round waits for nothing.
  const open = await drivePullLoop({ group: 'G_nodejs_fc_cache_open', threshold: 2, cached: 2 });
  check('control: a buffer at the threshold pulls with no cache wait at all',
    open.waits.length === 0 && open.rounds === 1, JSON.stringify(open.waits));

  const throttled = await drivePullLoop({
    group: 'G_nodejs_fc_broker_flow',
    failWith: new MQBrokerException(215, 'broker is flow controlling', '127.0.0.1:10911'),
  });
  check('a pull the broker throttled (215) is asked again after 20ms',
    throttled.waits.length === 1 && throttled.waits[0] === 20, JSON.stringify(throttled.waits));

  const broken = await drivePullLoop({
    group: 'G_nodejs_fc_pull_failed',
    failWith: new MQBrokerException(2, 'system error', '127.0.0.1:10911'),
  });
  check('a pull that failed for any other broker reason waits 500ms',
    broken.waits.length === 1 && broken.waits[0] === 500, JSON.stringify(broken.waits));

  // Control: a failure with no broker code behind it is a generic failure — the
  // 20ms branch may not swallow it.
  const plain = await drivePullLoop({
    group: 'G_nodejs_fc_pull_plain', failWith: new Error('connection reset'),
  });
  check('a failure carrying no broker code waits the generic 500ms',
    plain.waits.length === 1 && plain.waits[0] === 500, JSON.stringify(plain.waits));

  // Control: an idle long-poll timeout is normal, so it retries immediately.
  const timedOut = await drivePullLoop({
    group: 'G_nodejs_fc_pull_timeout',
    failWith: new RemotingTimeoutException('127.0.0.1:10911', 30000),
  });
  check('control: a long-poll timeout retries at once, asking for no wait',
    timedOut.waits.length === 0 && timedOut.rounds === 2, JSON.stringify(timedOut.waits));
}

// ---- the same discrimination on the POP round ------------------------------
// The POP loop reads the broker code off the field the remoting layer actually
// fills in; a wrong field name leaves the 20ms branch as dead code and every
// throttled queue silently waits the long error backoff instead.
async function drivePopLoop(group: string, failWith: Error): Promise<number[]> {
  const c = consumer(group);
  const mq = queue();
  const anyC = c as any;
  c.subscription.set(TOPIC, new SubscriptionData(TOPIC, '*'));
  c.popQueueTable.set(mqKey(mq), new PopProcessQueue());
  c.started = true;
  anyC._findBrokerAddressInSubscribe = () => ({ addr: '127.0.0.1:10911', isSlave: false });
  const flag = anyC._newStopFlag();
  let rounds = 0;
  c.mqClient = {
    popMessage: async () => {
      rounds++;
      if (rounds === 1) throw failWith;
      anyC._stopFlags.delete(flag);
      return new PopResult();
    },
  } as never;
  const waits: number[] = [];
  anyC._sleepOrStop = async (ms: number) => { waits.push(ms); return false; };
  await queuePopLoop(c, mq, flag);
  return waits;
}

{
  const throttled = await drivePopLoop('G_nodejs_pop_fc_broker',
    new MQBrokerException(215, 'broker is flow controlling', '127.0.0.1:10911'));
  check('a POP round the broker throttled (215) is asked again after 20ms',
    throttled.length === 1 && throttled[0] === 20, JSON.stringify(throttled));
  const broken = await drivePopLoop('G_nodejs_pop_fc_failed',
    new MQBrokerException(2, 'system error', '127.0.0.1:10911'));
  check('control: any other POP failure waits the long error backoff',
    broken.length === 1 && broken[0] === POP_DELAY_WHEN_EXCEPTION, JSON.stringify(broken));
}

// ---- the POP cache gate: one counter, one cadence, the same 50 ms ----------
// The POP loop's own gate sits on the ANSWER DEBT rather than on a buffer, but a
// trip is still a flow-control event of this client: it has to move the shared
// counter and warn on the shared cadence, or the operator sees a queue that
// pauses forever and a flowControlTimes figure of 0.

// popHit asks the consumer's gate about one queue that owes `debt` answers.
function popHit(c: DefaultMQPushConsumer, key: string, debt: number): boolean {
  const pq = new PopProcessQueue();
  pq.incFoundMsg(debt);
  c.popQueueTable.set(key, pq);
  return (c as any)._popFlowControlHit(key, pq);
}

// drivePopGate runs the real POP loop over one round with a queue that owes
// `debt` answers against `threshold`. The waits are RECORDED, never slept, and
// each recorded wait retires the loop's stop flag, so the gate's pause is the
// only number that can appear. `rounds` counts POP requests that reached the
// wire: a round the gate stopped must leave it at 0. `stamped` says whether the
// round touched the queue's pop timestamp BEFORE judging the gate — the stall
// criterion reads that stamp, so a gated round has to look alive.
async function drivePopGate(opts: {
  group: string, threshold: number, debt: number, queueId?: number,
}): Promise<{ waits: number[], rounds: number, c: DefaultMQPushConsumer, stamped: boolean }> {
  const c = consumer(opts.group);
  c.popThresholdForQueue = opts.threshold;
  const mq = queue(TOPIC, opts.queueId ?? 0);
  const anyC = c as any;
  c.subscription.set(TOPIC, new SubscriptionData(TOPIC, '*'));
  const pq = new PopProcessQueue();
  pq.incFoundMsg(opts.debt);
  // A timestamp from deep in the past: only this round's stamp can lift it.
  pq.setLastPopTimestamp(1);
  c.popQueueTable.set(mqKey(mq), pq);
  c.started = true;
  anyC._findBrokerAddressInSubscribe = () => ({ addr: '127.0.0.1:10911', isSlave: false });
  const flag = anyC._newStopFlag();
  let rounds = 0;
  c.mqClient = {
    popMessage: async () => {
      rounds++;
      anyC._stopFlags.delete(flag);
      return new PopResult();
    },
  } as never;
  const waits: number[] = [];
  anyC._sleepOrStop = async (ms: number) => {
    waits.push(ms);
    anyC._stopFlags.delete(flag);
    return true;
  };
  const begin = Date.now();
  await queuePopLoop(c, mq, flag);
  return { waits, rounds, c, stamped: pq.getLastPopTimestamp() >= begin };
}

{
  // The boundary, driven end to end. A debt that only REACHED the threshold
  // pops: the gate is a strict ">", so the threshold reads as "how many answers
  // may be outstanding", not "how many stop us".
  const open = await drivePopGate({ group: 'G_nodejs_pop_gate_open', threshold: 2, debt: 2 });
  check('control: a POP debt at the threshold pops with no flow-control pause at all',
    open.waits.length === 0 && open.rounds === 1, JSON.stringify(open.waits));
  check('control: a round that popped counted nothing', open.c.flowControlTriggered === 0);
  check('control: a round that popped stamped the queue', open.stamped);

  const gated = await drivePopGate({ group: 'G_nodejs_pop_gate_hot', threshold: 2, debt: 3 });
  check('a POP debt of threshold+1 re-schedules the queue after exactly the 50ms cache wait',
    gated.waits.length === 1 && gated.waits[0] === POP_DELAY_WHEN_CACHE_FLOW_CONTROL,
    JSON.stringify(gated.waits));
  check('the POP cache wait is the same 50ms number as the pull cache wait',
    POP_DELAY_WHEN_CACHE_FLOW_CONTROL === 50);
  check('a round the POP gate stopped never puts a pop request on the wire',
    gated.rounds === 0, `rounds=${gated.rounds}`);
  check('the POP gate counted exactly one trip on the shared counter',
    gated.c.flowControlTriggered === 1, `got ${gated.c.flowControlTriggered}`);
  check('a gated POP round still stamps the queue, so rebalance keeps it',
    gated.stamped);
  check('the first POP trip is a WARN trip of the shared cadence',
    isFlowControlWarnTrip(gated.c.flowControlTriggered));
}

// Per-queue isolation: the debts of sibling queues are never summed, so one
// runaway queue cannot hold up a queue that owes nothing.
{
  const c = consumer('G_nodejs_pop_gate_per_queue');
  c.popThresholdForQueue = 3;
  const hotKey = mqKey(queue(TOPIC, 0));
  const coldKey = mqKey(queue(TOPIC, 1));
  check('the queue whose own debt is over the pop threshold trips', popHit(c, hotKey, 4));
  // Paired control on the SAME consumer and the SAME staged debts: judged on a
  // summed debt (4 > 3) this empty sibling would trip too.
  check('a sibling queue that owes nothing is not judged by its neighbour\'s debt',
    !popHit(c, coldKey, 0));
  check('two judgments over one hot queue counted one trip', c.flowControlTriggered === 1,
    `got ${c.flowControlTriggered}`);
  check('a POP debt equal to the threshold does not trip (>, not >=)', !popHit(c, hotKey, 3));
  check('a POP debt of threshold+1 trips', popHit(c, hotKey, 4));
  check('the boundary pair added exactly one trip', c.flowControlTriggered === 2,
    `got ${c.flowControlTriggered}`);
}

// One counter across BOTH paths: the pull gate and the POP gate feed the same
// flowControlTimes figure.
{
  const c = gatesOff(consumer('G_nodejs_pop_gate_shared_counter'));
  c.pullThresholdForQueue = 1;
  c.popThresholdForQueue = 1;
  const mq = queue(TOPIC, 5);
  check('a pull cache hit is counted', hit(c, mq, [msg(0, 10), msg(1, 10)]));
  check('a POP debt hit is counted', popHit(c, mqKey(mq), 2));
  // Paired control: leave the POP trip out of the counter and both gates still
  // pause their queues while this total stops at 1.
  check('flowControlTimes is one client-wide count spanning the pull path and the POP path',
    c.flowControlTriggered === 2, `got ${c.flowControlTriggered}`);
}

// The cadence is driven off that shared counter, so POP trips shift it too.
// Pre-seeding the counter pins the boundary arithmetic without running 1000
// real trips.
{
  const c = consumer('G_nodejs_pop_gate_cadence');
  c.popThresholdForQueue = 0;
  const key = mqKey(queue(TOPIC, 0));
  check('the 1st POP trip warns', popHit(c, key, 1) && isFlowControlWarnTrip(c.flowControlTriggered));
  check('the 2nd POP trip stays on the debug channel',
    popHit(c, key, 1) && !isFlowControlWarnTrip(c.flowControlTriggered));
  c.flowControlTriggered = 999;
  popHit(c, key, 1);
  check('the 1000th trip of the shared counter does not warn',
    c.flowControlTriggered === 1000 && !isFlowControlWarnTrip(c.flowControlTriggered));
  popHit(c, key, 1);
  check('the 1001st trip of the shared counter warns',
    c.flowControlTriggered === 1001 && isFlowControlWarnTrip(c.flowControlTriggered));
}

// ======================= the LITE PULL path (poll, no cache) =======================
// LiteBroker plays nameserver AND broker on one real socket: it answers the route
// with N queues and lets the test script the pull answer of each queue, so the
// parking is pinned on wire traffic rather than on an injected kernel.
const LITE_TOPIC = 'NodeJsLiteFlowControlTopic';
const LITE_BROKER = 'liteFcBroker';
// The port's existing single-queue "nothing found" guard, asserted as the CONTROL
// value: a code that is not 215 must still land here.
const LITE_IDLE_GUARD_MILLIS = 200;

function storedLiteMessage(queueId: number, offset: number): MessageExt {
  const msg = new MessageExt(LITE_TOPIC, Buffer.from(`lite-fc-${queueId}-${offset}`, 'utf8'), 'tagA');
  msg.setBrokerName(LITE_BROKER);
  msg.setQueueId(queueId);
  msg.setQueueOffset(offset);
  msg.setFlag(0);
  msg.setSysFlag(0);
  msg.setBornTimestamp(1_700_000_000_000);
  msg.setStoreTimestamp(1_700_000_000_001);
  msg.setBornHost('127.0.0.1');
  msg.setStoreHost('127.0.0.1');
  msg.putProperty(MessageConst.PROPERTY_TAGS, 'tagA');
  return msg;
}

class LiteBroker {
  server: net.Server;
  port = 0;
  queueNums: number;
  // Every pull in arrival order: the SKIP and the ASK-AGAIN are read here, so the
  // test needs no clock assumptions about the client.
  pulls: Array<{ queueId: number, offset: number, at: number }> = [];
  private script = new Map<number, { code?: number, found?: boolean, left: number }>();

  constructor(queueNums: number) {
    this.queueNums = queueNums;
    this.server = net.createServer((s) => this.onConnection(s));
  }

  listen(): Promise<number> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        this.port = (this.server.address() as net.AddressInfo).port;
        resolve(this.port);
      });
    });
  }

  get addr() { return `127.0.0.1:${this.port}`; }
  close() { this.server.close(); }

  // throttle answers this queue's next `left` pulls with the broker code `code`.
  throttle(queueId: number, left: number, code: number): this {
    this.script.set(queueId, { code, left });
    return this;
  }

  // handBack answers this queue's next `left` pulls with one stored message, so a
  // poll() round can end on a real batch instead of on the deadline.
  handBack(queueId: number, left: number): this {
    this.script.set(queueId, { found: true, left });
    return this;
  }

  pullsOf(queueId: number) { return this.pulls.filter((p) => p.queueId === queueId); }

  private routeBody(): string {
    return JSON.stringify({
      orderTopicConf: null,
      queueDatas: [{
        brokerName: LITE_BROKER, readQueueNums: this.queueNums,
        writeQueueNums: this.queueNums, perm: 6, topicSysFlag: 0,
      }],
      brokerDatas: [{
        cluster: 'DefaultCluster', brokerName: LITE_BROKER,
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
    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      resp.body = Buffer.from(this.routeBody(), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.LITE_PULL_MESSAGE || req.code === RequestCode.PULL_MESSAGE) {
      return this.answerPull(req, resp);
    }
    return resp; // heartbeat / offset commit / every other code: SUCCESS
  }

  private answerPull(req: RemotingCommand, resp: RemotingCommand): RemotingCommand {
    const ext = req.extFields || {};
    const queueId = Number(ext['queueId'] || 0);
    const offset = Number(ext['queueOffset'] || 0);
    this.pulls.push({ queueId, offset, at: Date.now() });
    const s = this.script.get(queueId);
    const scripted = s != null && s.left > 0;
    if (scripted) s!.left--;
    if (scripted && s!.code !== undefined) {
      resp.code = s!.code!;
      resp.remark = 'the broker is flow controlling this queue';
      return resp;
    }
    if (scripted && s!.found) resp.code = ResponseCode.SUCCESS;
    else resp.code = ResponseCode.PULL_NOT_FOUND;
    if (scripted && s!.found) resp.body = encodeMessageExt(storedLiteMessage(queueId, offset));
    resp.addExtField('nextBeginOffset', String(offset + 1));
    resp.addExtField('minOffset', '0');
    resp.addExtField('maxOffset', String(offset + 10));
    resp.addExtField('suggestWhichBrokerId', '0');
    return resp;
  }
}

// driveLitePoll runs the REAL poll() round loop against a LiteBroker. The waits
// between rounds are RECORDED, never guessed, and each record also captures how
// long the queue under probe was still parked at that instant (0 = not parked) —
// so "the next round skipped ONLY that queue" is visible even when the sleep itself
// is faked. `sleepForReal` keeps the wall clock honest for the wire-gap assertions.
async function driveLitePoll(opts: {
  group: string, queueIds: number[], script: (b: LiteBroker) => void,
  pollMillis: number, batchSize?: number, sleepForReal?: boolean, probeQueueId?: number,
}): Promise<{ c: DefaultLitePullConsumer, broker: LiteBroker, waits: number[], parkLeft: number[] }> {
  const broker = new LiteBroker(opts.queueIds.length);
  await broker.listen();
  opts.script(broker);
  const c = new DefaultLitePullConsumer(opts.group);
  c.setNamesrvAddr(broker.addr);
  const mqs = opts.queueIds.map((q) => new MessageQueue(LITE_TOPIC, LITE_BROKER, q));
  c.assign(mqs);
  if (opts.batchSize !== undefined) c.setPullBatchSize(opts.batchSize);
  await c.start();
  // Pin the cursors so the rounds under test are the only RPCs the loop makes and
  // no offset-resolution branch can add a wait of its own.
  for (const mq of mqs) c.seek(mq, 0);
  const probe = mqs[(opts.queueIds.indexOf(opts.probeQueueId ?? opts.queueIds[0]) + opts.queueIds.length)
    % opts.queueIds.length];
  const waits: number[] = [];
  const parkLeft: number[] = [];
  (c as any)._sleepPollRound = async (ms: number) => {
    waits.push(ms);
    const until = c.getPullParkUntil(probe);
    parkLeft.push(until === 0 ? 0 : Math.max(1, until - Date.now()));
    if (opts.sleepForReal) await new Promise<void>((r) => setTimeout(r, ms));
  };
  await c.poll(opts.pollMillis);
  await c.shutdown();
  broker.close();
  return { c, broker, waits, parkLeft };
}

{
  // ---- the mapping, pinned where the value is decided ----
  check('a lite pull the broker throttled (215) parks its queue for the same 20ms',
    litePullParkMillisFor(ResponseCode.FLOW_CONTROL) === 20
    && litePullParkMillisFor(215) === PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL);
  // Paired control: every other code is NOT a park — today's behaviour stays.
  for (const code of [1, 2, 14, 19, 24, 26, 214, 216, 1006]) {
    check(`code ${code} parks nothing on the lite path`, litePullParkMillisFor(code) === 0);
  }
  check('a failure with no broker code behind it parks nothing',
    litePullParkMillisFor(undefined) === 0);
}

{
  // ---- 215 parks THAT queue for 20ms and the queue is pulled again after ----
  const throttled = await driveLitePoll({
    group: 'G_nodejs_lite_fc_park', queueIds: [0],
    script: (b) => b.throttle(0, 1, ResponseCode.FLOW_CONTROL),
    pollMillis: 400, sleepForReal: true,
  });
  const asked = throttled.broker.pullsOf(0);
  check('the throttled queue was asked again once its park expired',
    asked.length >= 2, `pulls=${asked.length}`);
  const parkGap = asked.length >= 2 ? asked[1].at - asked[0].at : -1;
  check('and it was asked again after the short 20ms broker pause, not before',
    parkGap >= 19 && parkGap <= 150, `gap=${parkGap}ms`);
  check('the round asked for the park itself, never for the 200ms idle guard',
    throttled.waits[0] > 0 && throttled.waits[0] <= 20
    && throttled.parkLeft[0] > 0 && throttled.parkLeft[0] <= 20,
    JSON.stringify(throttled.waits) + ' / ' + JSON.stringify(throttled.parkLeft));
  check('a throttled round moved no cursor: the queue is asked at the same offset',
    asked[0].offset === 0 && asked[1].offset === 0,
    asked.map((p) => p.offset).join(','));

  // Paired control on the SAME driver: SYSTEM_BUSY is not a broker throttle, so the
  // queue is not parked and today's single-queue idle guard is the only wait.
  const busy = await driveLitePoll({
    group: 'G_nodejs_lite_fc_busy', queueIds: [0],
    script: (b) => b.throttle(0, 1, ResponseCode.SYSTEM_BUSY),
    pollMillis: 400, sleepForReal: true,
  });
  const busyAsked = busy.broker.pullsOf(0);
  const busyGap = busyAsked.length >= 2 ? busyAsked[1].at - busyAsked[0].at : -1;
  check('control: SYSTEM_BUSY arms no park for the queue',
    busy.parkLeft[0] === 0, JSON.stringify(busy.parkLeft));
  check('control: SYSTEM_BUSY keeps today\'s 200ms single-queue idle guard',
    busy.waits[0] === LITE_IDLE_GUARD_MILLIS, JSON.stringify(busy.waits));
  check('control: so the retry is late, not at the 20ms throttle pace',
    busyGap >= 150, `gap=${busyGap}ms`);
}

{
  // ---- a parked queue never delays a sibling inside one poll() deadline ----
  // q0 answers 215 on every pull; q1 hands one message back. With a batch size of
  // 1 the round must end on q1's message WITHOUT taking any wait at all: a parked
  // queue is stepped over, not waited on, as long as another queue can answer.
  const sibling = await driveLitePoll({
    group: 'G_nodejs_lite_fc_sibling', queueIds: [0, 1],
    script: (b) => b.throttle(0, 99, ResponseCode.FLOW_CONTROL).handBack(1, 1),
    pollMillis: 1000, batchSize: 1,
  });
  const order = sibling.broker.pulls.map((p) => p.queueId).join(',');
  const asked0 = sibling.broker.pullsOf(0);
  const asked1 = sibling.broker.pullsOf(1);
  check('the throttled queue went first and the sibling was served in the same round',
    order === '0,1' && asked1.length === 1, order);
  check('serving the sibling cost no wait at all, park or idle guard',
    sibling.waits.length === 0, JSON.stringify(sibling.waits));
  const siblingGap = asked1.length === 1 && asked0.length >= 1
    ? asked1[0].at - asked0[0].at : -1;
  check('the sibling was not held by the parked queue\'s 20ms',
    siblingGap >= 0 && siblingGap <= 100, `gap=${siblingGap}ms`);
  check('and the parked queue itself was not asked a second time in that poll',
    asked0.length === 1, `asked=${asked0.length}`);
}

{
  // ---- the caller sees a typed broker exception, not a flattened string ----
  const broker = new LiteBroker(1);
  await broker.listen();
  broker.throttle(0, 4, ResponseCode.FLOW_CONTROL);
  const puller = new DefaultMQPullConsumer('G_nodejs_lite_fc_typed');
  puller.setNamesrvAddr(broker.addr);
  await puller.start();
  const mq = new MessageQueue(LITE_TOPIC, LITE_BROKER, 0);

  let thrown: any = null;
  try { await puller.pull(mq, '*', 0, 10); } catch (e) { thrown = e; }
  check('a non-SUCCESS pull answer surfaces as the port\'s typed broker exception',
    thrown instanceof MQBrokerException && thrown.name === 'MQBrokerException',
    thrown == null ? 'nothing thrown' : `${thrown.name}: ${thrown.message}`);
  check('and it carries the response code the caller branches on',
    thrown instanceof MQBrokerException && thrown.responseCode === ResponseCode.FLOW_CONTROL,
    String(thrown && (thrown as any).responseCode));
  check('while the message stays informative (code and the broker remark)',
    thrown != null && (thrown as Error).message.includes('215')
    && (thrown as Error).message.includes('flow controlling'), thrown && (thrown as Error).message);

  // A non-215 code has to survive the same way — the text alone is not enough.
  broker.throttle(0, 1, ResponseCode.SYSTEM_ERROR);
  let otherCode: any = null;
  try { await puller.pull(mq, '*', 0, 10); } catch (e) { otherCode = e; }
  check('a SYSTEM_ERROR pull also surfaces its code, not only its text',
    otherCode instanceof MQBrokerException && otherCode.responseCode === ResponseCode.SYSTEM_ERROR,
    otherCode == null ? 'nothing thrown' : `${otherCode.name}/` + (otherCode as any).responseCode);

  // pullAsync hands the SAME typed exception to the callback (it never re-wraps an
  // Error), so the scheduled pull can branch on the code too.
  broker.throttle(0, 1, ResponseCode.FLOW_CONTROL);
  const asyncErr: any = await new Promise((resolve) => {
    puller.pullAsync(mq, '*', 0, 10,
      { onSuccess: () => resolve(null), onException: (e) => resolve(e) }, 3000);
  });
  check('pullAsync delivers the typed exception with its code to onException',
    asyncErr instanceof MQBrokerException && asyncErr.responseCode === ResponseCode.FLOW_CONTROL,
    asyncErr == null ? 'success instead' : String(asyncErr.name));
  await puller.shutdown();

  // The POP sibling of this rule: a broker answer reaching the POP round as a bare
  // Error leaves its 20ms throttle branch dead code, so the response processing
  // must throw the typed exception with the code as well.
  let popErr: any = null;
  try {
    processPopResponse('broker-a', ResponseCode.FLOW_CONTROL, 'pop is flow controlling',
      null, new PopMessageResponseHeader(), LITE_TOPIC, '', false);
  } catch (e) { popErr = e; }
  check('a POP answer the broker throttled keeps its code on a typed exception',
    popErr instanceof MQBrokerException && popErr.responseCode === ResponseCode.FLOW_CONTROL,
    popErr == null ? 'nothing thrown' : `${popErr.name}: ${popErr.message}`);
  broker.close();
}

{
  // The lite path has no client-side cache gate, so it must not move the counter
  // the push and POP gates above own and assert.
  const c = new DefaultLitePullConsumer('G_nodejs_lite_fc_no_counter');
  check('the lite pull consumer has no flow-control counter of its own',
    (c as any).flowControlTriggered === undefined);
  check('and a parked queue is keyed like every other per-queue state',
    c.getPullParkUntil(new MessageQueue(LITE_TOPIC, LITE_BROKER, 7)) === 0
    && mqKey(new MessageQueue(LITE_TOPIC, LITE_BROKER, 7)).includes(LITE_BROKER));
  // The park knob is cancellable the way pause() is: a past instant is dropped.
  const mq = new MessageQueue(LITE_TOPIC, LITE_BROKER, 7);
  c.setPullParkUntil(mq, Date.now() + PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL);
  check('a park armed for the future holds the queue back', c.getPullParkUntil(mq) > 0);
  c.setPullParkUntil(mq, Date.now() - 1);
  check('an expired park arms nothing', c.getPullParkUntil(mq) === 0);
}

console.log(`\nflow control: ${pass} checks, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
