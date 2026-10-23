// Push-consumer cache flow control — offline test of the five gates and the log cadence.
//
// Baseline this suite pins:
//   * every cache gate compares STRICTLY GREATER THAN its threshold, so a buffer that
//     has reached the threshold exactly is still allowed to pull. A threshold reads as
//     "how many may be cached", not "how many stop us";
//   * the byte gate is in MiB, not bytes — the single most expensive unit mistake here,
//     because a 100x error silently disables backpressure;
//   * the span gate measures the OFFSET span (max-min), so one message that keeps failing
//     cannot let the offset run away;
//   * the switches: 0 disables the queue-level byte gate, -1 disables the topic-level ones,
//     and a count threshold of 0 clamps to 1;
//   * the gate measures the not-yet-dispatched remainder, and one judgment records exactly
//     one tick even when several gates match;
//   * the WARN fires on the 1st trip and every 1000th after, while the counter keeps
//     counting every trip;
//   * popThresholdForQueue defaults to 96, not to the pull-side 1000;
//   * a stopped round waits with the right number: 50ms for this client's own cache
//     gate, 20ms for the broker's 215, 500ms for every other failed pull — and no
//     wait at all for an idle long-poll timeout. The broker's answer has to reach the
//     loop as a coded exception, or the short wait silently never applies.
//
// Run: node --experimental-strip-types test/flow_control_smoke.ts
import { MessageQueue, MessageExt } from '../src/common/message.ts';
import { ProcessQueue } from '../src/client/process_queue.ts';
import { PopProcessQueue } from '../src/client/pop_process_queue.ts';
import { SubscriptionData } from '../src/common/subscriptionData.ts';
import { PullResult, PullStatus } from '../src/client/consumer_result.ts';
import { PopResult } from '../src/client/pop_api.ts';
import { queuePopLoop, POP_DELAY_WHEN_EXCEPTION } from '../src/client/pop_consumer.ts';
import { MQBrokerException, RemotingTimeoutException } from '../src/remoting/exception.ts';
import {
  DefaultMQPushConsumer, isFlowControlWarnTrip, pullRetryDelayMillisFor,
  PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL, PULL_TIME_DELAY_MILLS_WHEN_PULL_FAILED,
} from '../src/client/consumer.ts';
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

// ---- topic gates -------------------------------------------------------------
{
  const c = gatesOff(consumer('G_nodejs_fc_topic'));
  c.pullThresholdForTopic = 2;
  const q0 = queue(TOPIC, 0), q1 = queue(TOPIC, 1);
  check('one message in one queue leaves the topic total at 1', !hit(c, q0, [msg(0, 1)]));
  c.processQueueTable.set(mqKey(q1), new ProcessQueue(false));
  c.processQueueTable.get(mqKey(q1))!.putMessage([msg(0, 1)]);
  check('sibling queues of the same topic accumulate', hit(c, q0, [msg(0, 1)]));
  // Another topic must not mix in.
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

console.log(`\nflow control: ${pass} checks, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
