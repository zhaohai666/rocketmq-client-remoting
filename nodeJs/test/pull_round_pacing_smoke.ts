// How a PULL round is paced — offline, driving the real per-queue loop with its
// waits RECORDED (and, on two legs, waited out for real and timed between two
// consecutive fetches).
//
//   node --experimental-strip-types test/pull_round_pacing_smoke.ts
//
// One pull round answers with a status plus, for FOUND, a list. What the round
// does about its NEXT fetch:
//
//   * FOUND whose list still has rows when the round hands them over — after the
//     second client-side tag pass and after the FilterMessageHook rewrite, the very
//     list PULL_TPS counts: wait pullInterval, and only when that knob is above 0;
//     at 0 the queue asks again at once;
//   * FOUND whose list came back empty, or which the tag pass / the hook emptied:
//     re-pull IMMEDIATELY: the cadence paces deliveries, so a local wait on top of an
//     idle round only delays the next fetch;
//   * NO_NEW_MSG and NO_MATCHED_MSG: the same immediate re-pull, for the same
//     reason;
//   * a failed round keeps its own arms, untouched by the pacing above: 20 ms when
//     the broker's answer is the throttle code 215, otherwise this consumer's
//     generic error backoff (pullRetryDelayMillis, default 1000 ms), and a benign
//     idle long-poll timeout waits nothing at all;
//   * a queue with no cursor yet, and a paused consumer, keep their own waits (the
//     backoff and 1000 ms); the OFFSET_ILLEGAL round fixes the cursor and leaves the
//     loop without a pacing wait;
//   * a delivered round still books its pull RT and its delivered TPS, an emptied
//     one its RT only — pacing moved no counter.
//
// Every wait is read BACK as the number the loop asked for, compared against what
// the consumer itself reports (pullInterval / getPullRetryDelayMillis), and each
// rule has its paired control: FOUND with rows waits, the same FOUND fully
// filtered does not. The two measured legs wait for real, so the gap between two
// fetches reaching the wire is the pacing the loop actually applied.
import {
  DefaultMQPushConsumer, PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
  DEFAULT_PULL_RETRY_DELAY_MILLIS, pullRetryDelayMillisFor,
} from '../src/client/consumer.ts';
import { ProcessQueue } from '../src/client/process_queue.ts';
import { PullResult, PullStatus } from '../src/client/consumer_result.ts';
import { ConsumerStatsManager } from '../src/client/consumer_stats.ts';
import { FilterMessageContext, FilterMessageHook } from '../src/client/hook.ts';
import { FilterAPI } from '../src/common/subscriptionData.ts';
import { MessageExt, MessageQueue } from '../src/common/message.ts';
import { MQBrokerException, RemotingTimeoutException } from '../src/remoting/exception.ts';
import { ResponseCode } from '../src/remoting/codes.ts';
import { mqKey } from '../src/client/offset_store.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'PullPacingTopic';
const BROKER = 'broker-a';
const ADDR = '127.0.0.1:10911';
const PULL_INTERVAL_MS = 50;
// The pause a round waits when the consumer is suspended, and the one the cache
// gate takes — both fixed numbers the pacing rule must not displace.
const SUSPEND_WAIT_MS = 1000;

function row(offset: number, tag: string | null): MessageExt {
  const m = new MessageExt(TOPIC, Buffer.from('body-' + offset), tag);
  m.setQueueId(0);
  m.setQueueOffset(offset);
  m.setCommitLogOffset(offset);
  m.setBornTimestamp(Date.now());
  m.setStoreHost('127.0.0.1');
  m.setBornHost('127.0.0.1');
  return m;
}

function found(rows: MessageExt[]): PullResult {
  return new PullResult(PullStatus.FOUND, 100, 0, 1000, rows);
}
const IDLE = () => new PullResult(PullStatus.NO_NEW_MSG, 0, 0, 0, []);

class EmptyListHook extends FilterMessageHook {
  filterMessage(ctx: FilterMessageContext): any[] | null {
    ctx.msgList = [];
    return ctx.msgList;
  }
}

class DropFirstHook extends FilterMessageHook {
  filterMessage(ctx: FilterMessageContext): any[] | null {
    ctx.msgList = (ctx.msgList ?? []).slice(1);
    return ctx.msgList;
  }
}

// drive runs the REAL per-queue pull loop over the round under test and reports
// what it scheduled. `first` is that round's answer, `failWith` what it throws.
// Recorded mode patches the wait away and captures the number asked for, then ends
// the run; real mode lets the wait happen and returns the timestamps of the two
// fetches, so the caller measures the gap. A round that asks for no wait ends when
// the second fetch retires the queue's stop flag.
async function drive(opts: {
  group: string;
  expression?: string;
  first?: PullResult;
  failWith?: Error;
  hooks?: FilterMessageHook[];
  pullInterval?: number;
  retryDelayMillis?: number;
  paused?: boolean;
  noCursor?: boolean;
  realWait?: boolean;
}): Promise<{
  waits: number[], rounds: number, handed: MessageExt[][], at: number[],
  pullInterval: number, retryDelayMillis: number, cursorLeft: boolean,
  c: DefaultMQPushConsumer,
}> {
  const c = new DefaultMQPushConsumer(opts.group);
  const anyC = c as any;
  c.pullInterval = opts.pullInterval ?? PULL_INTERVAL_MS;
  if (opts.retryDelayMillis !== undefined) c.setPullRetryDelayMillis(opts.retryDelayMillis);
  // No cache gate within reach: the round under test is the only thing on trial.
  c.pullThresholdForQueue = 1 << 30;
  c.pullThresholdSizeForQueue = 1 << 30;
  c.consumeConcurrentlyMaxSpan = 1 << 30;
  c.subscription.set(TOPIC, FilterAPI.buildSubscriptionData(TOPIC, opts.expression ?? '*'));
  c.started = true;
  c._statsManager = new ConsumerStatsManager();
  for (const hook of opts.hooks ?? []) c.registerFilterMessageHook(hook);
  if (opts.paused) c.suspend();

  const mq = new MessageQueue(TOPIC, BROKER, 0);
  const key = mqKey(mq);
  const pq = new ProcessQueue(false);
  // Record the list each round hands the queue — the one that exists AFTER the
  // tag pass and the hook, which is exactly what the pacing decision reads.
  const handed: MessageExt[][] = [];
  const origPut = pq.putMessage.bind(pq);
  pq.putMessage = (msgs: MessageExt[]): boolean => { handed.push(msgs); return origPut(msgs); };
  c.processQueueTable.set(key, pq);
  if (!opts.noCursor) c.offsetTable.set(key, 0);
  c.mqClient = { updateTopicRouteInfoFromNameServer: async () => {} } as never;
  anyC._findBrokerAddressInSubscribe = () => ({ addr: ADDR, isSlave: false });

  const flag = anyC._newStopFlag();
  const at: number[] = [];
  let rounds = 0;
  anyC._pullKernel = async () => {
    rounds++;
    at.push(Date.now());
    if (rounds === 1) {
      if (opts.failWith) throw opts.failWith;
      return opts.first ?? IDLE();
    }
    // The second fetch only ends the run, with a status that schedules nothing.
    anyC._stopFlags.delete(flag);
    return IDLE();
  };
  const waits: number[] = [];
  if (!opts.realWait) {
    anyC._sleepOrStop = async (ms: number) => {
      waits.push(ms);
      anyC._stopFlags.delete(flag);
      return true;
    };
  }
  // The real wait unrefs its own timers, so with nothing else pending the process
  // would simply exit mid-wait: hold the event loop open across a measured round.
  const hold = opts.realWait ? setInterval(() => { /* keep the loop alive */ }, 10) : null;
  await anyC._queuePullLoop(mq, flag);
  if (hold) clearInterval(hold);
  return {
    waits, rounds, handed, at,
    pullInterval: c.pullInterval, retryDelayMillis: c.getPullRetryDelayMillis(),
    cursorLeft: c.offsetTable.has(key), c,
  };
}

// gapOf is the measured distance between the round under test and the fetch that
// followed it, in real-wait mode.
function gapOf(r: { at: number[] }): number {
  return r.at.length >= 2 ? r.at[1] - r.at[0] : -1;
}

async function main() {
  // ---- 1. a FOUND round that hands rows over waits pullInterval, once
  {
    const r = await drive({
      group: 'GID_pull_pace_delivered',
      first: found([row(1, 'TagA'), row(2, 'TagB'), row(3, null)]),
    });
    check('the wait is exactly the consumer\'s pullInterval, taken once',
      r.waits.length === 1 && r.waits[0] === r.pullInterval && r.pullInterval === PULL_INTERVAL_MS,
      `${JSON.stringify(r.waits)}/${r.pullInterval}`);
    check('and the round hands over all three rows',
      r.handed.length === 1 && r.handed[0].length === 3,
      r.handed.map((h) => h.length).join(','));
    const stats = (r.c as any)._statsManager;
    const rt = stats.topicAndGroupPullRT.find(`${TOPIC}@GID_pull_pace_delivered`);
    const tps = stats.topicAndGroupPullTPS.find(`${TOPIC}@GID_pull_pace_delivered`);
    check('a paced round still books one pull RT and its delivered TPS',
      rt != null && rt.times === 1 && tps != null && tps.value === 3,
      `rt=${rt ? rt.times : 'none'} tps=${tps ? tps.value : 'none'}`);
  }

  // ---- 2. pullInterval 0 on the same round: no wait at all
  {
    const r = await drive({
      group: 'GID_pull_pace_interval0', pullInterval: 0,
      first: found([row(1, 'TagA'), row(2, 'TagA')]),
    });
    check('pullInterval 0 asks again with no wait',
      r.waits.length === 0 && r.rounds === 2, `${JSON.stringify(r.waits)}/${r.rounds}`);
    check('and the rows are still handed over',
      r.handed.length === 1 && r.handed[0].length === 2,
      r.handed.map((h) => h.length).join(','));
  }

  // ---- 3. measured: the paced round really holds the next fetch back
  {
    const r = await drive({
      group: 'GID_pull_pace_real_delivered', realWait: true,
      first: found([row(1, 'TagA')]),
    });
    const gap = gapOf(r);
    check('with the wait taken for real the second fetch comes no earlier than pullInterval',
      r.rounds === 2 && gap >= PULL_INTERVAL_MS - 5 && gap <= 2000,
      `gap=${gap}ms rounds=${r.rounds}`);
  }

  // ---- 4. FOUND the tag pass emptied: re-pull at once (the discriminating leg)
  {
    const r = await drive({
      group: 'GID_pull_pace_tagemptied', expression: 'TagA',
      first: found([row(1, 'TagB'), row(2, 'TagC'), row(3, null)]),
    });
    check('a FOUND answer the tag pass emptied waits nothing',
      r.waits.length === 0 && r.rounds === 2, `${JSON.stringify(r.waits)}/${r.rounds}`);
    check('nothing of it is handed over', r.handed.length === 0,
      String(r.handed.length));
    const tps = (r.c as any)._statsManager.topicAndGroupPullTPS.find(`${TOPIC}@GID_pull_pace_tagemptied`);
    const rt = (r.c as any)._statsManager.topicAndGroupPullRT.find(`${TOPIC}@GID_pull_pace_tagemptied`);
    check('and it books no pulled message while still counting as one round for the latency figure',
      (tps == null || tps.value === 0) && rt != null && rt.times === 1,
      `tps=${tps ? tps.value : 'none'} rt=${rt ? rt.times : 'none'}`);
  }

  // ---- 5. FOUND the FilterMessageHook emptied: the same immediate re-pull
  {
    const r = await drive({
      group: 'GID_pull_pace_hookemptied', hooks: [new EmptyListHook()],
      first: found([row(1, 'TagA'), row(2, 'TagA')]),
    });
    check('a FOUND answer the hook emptied waits nothing either',
      r.waits.length === 0 && r.rounds === 2, `${JSON.stringify(r.waits)}/${r.rounds}`);
    check('and the emptied list is never handed to the queue', r.handed.length === 0,
      String(r.handed.length));
    const r2 = await drive({
      group: 'GID_pull_pace_hookpartial', expression: 'TagA', hooks: [new DropFirstHook()],
      first: found([row(1, 'TagA'), row(2, 'TagB'), row(3, 'TagA')]),
    });
    check('a round the hook only thinned still waits pullInterval for its surviving row',
      r2.waits.length === 1 && r2.waits[0] === r2.pullInterval && r2.pullInterval === PULL_INTERVAL_MS,
      JSON.stringify(r2.waits));
  }

  // ---- 6. measured: an emptied round's next fetch comes at once
  {
    const r = await drive({
      group: 'GID_pull_pace_real_emptied', expression: 'TagA', realWait: true,
      first: found([row(1, 'TagB'), row(2, 'TagC')]),
    });
    const gap = gapOf(r);
    check('with the same pullInterval set, a fully filtered round fetches again immediately',
      r.rounds === 2 && gap >= 0 && gap < PULL_INTERVAL_MS - 10,
      `gap=${gap}ms rounds=${r.rounds}`);
  }

  // ---- 7. FOUND with no rows of its own, and the two idle statuses
  {
    const empty = await drive({ group: 'GID_pull_pace_found_empty', first: found([]) });
    check('control: FOUND with an empty answer waits nothing',
      empty.waits.length === 0 && empty.rounds === 2, JSON.stringify(empty.waits));
    const noNew = await drive({ group: 'GID_pull_pace_nonew', first: IDLE() });
    check('NO_NEW_MSG re-pulls at once, with no wait and nothing handed over',
      noNew.waits.length === 0 && noNew.rounds === 2 && noNew.handed.length === 0,
      JSON.stringify(noNew.waits));
    const noMatched = await drive({
      group: 'GID_pull_pace_nomatched',
      first: new PullResult(PullStatus.NO_MATCHED_MSG, 100, 0, 1000, []),
    });
    check('NO_MATCHED_MSG re-pulls at once too',
      noMatched.waits.length === 0 && noMatched.rounds === 2, JSON.stringify(noMatched.waits));
  }

  // ---- 8. the error arms are untouched by the pacing rule
  {
    const throttled = await drive({
      group: 'GID_pull_pace_flow',
      failWith: new MQBrokerException(ResponseCode.FLOW_CONTROL, 'flow control', ADDR),
    });
    check('a pull the broker throttled (215) still waits 20 ms, not pullInterval',
      throttled.waits.length === 1 && throttled.waits[0] === 20
      && PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL === 20
      && throttled.waits[0] !== throttled.pullInterval,
      JSON.stringify(throttled.waits));
    const failed = await drive({
      group: 'GID_pull_pace_failed',
      failWith: new MQBrokerException(ResponseCode.SYSTEM_ERROR, 'system error', ADDR),
    });
    check('any other failure still waits the generic knob the loop was set up with',
      failed.waits.length === 1 && failed.waits[0] === failed.retryDelayMillis
      && failed.retryDelayMillis === DEFAULT_PULL_RETRY_DELAY_MILLIS,
      JSON.stringify(failed.waits));
    const moved = await drive({
      group: 'GID_pull_pace_knob', retryDelayMillis: 77,
      failWith: new Error('connection reset'),
    });
    check('a configured knob still moves that arm while pullInterval stays put',
      moved.waits.length === 1 && moved.waits[0] === 77 && moved.pullInterval === PULL_INTERVAL_MS,
      JSON.stringify(moved.waits));
    const movedFlow = await drive({
      group: 'GID_pull_pace_knob_flow', retryDelayMillis: 77,
      failWith: new MQBrokerException(ResponseCode.FLOW_CONTROL, 'flow control', ADDR),
    });
    check('and the 215 arm does not follow the knob',
      movedFlow.waits.length === 1 && movedFlow.waits[0] === 20
      && movedFlow.retryDelayMillis === 77, JSON.stringify(movedFlow.waits));
    const timedOut = await drive({
      group: 'GID_pull_pace_timeout',
      failWith: new RemotingTimeoutException(ADDR, 30000),
    });
    check('control: a benign long-poll timeout waits nothing and asks again at once',
      timedOut.waits.length === 0 && timedOut.rounds === 2, JSON.stringify(timedOut.waits));
    check('the two arms stay one function apart',
      pullRetryDelayMillisFor(215, 77) === 20 && pullRetryDelayMillisFor(2, 77) === 77);
  }

  // ---- 9. the remaining fixed waits, and OFFSET_ILLEGAL, keep their behaviour
  {
    const noCursor = await drive({ group: 'GID_pull_pace_no_cursor', noCursor: true });
    check('a round with no cursor for the queue still waits the generic backoff',
      noCursor.waits.length === 1 && noCursor.waits[0] === noCursor.retryDelayMillis
      && noCursor.rounds === 0, JSON.stringify(noCursor.waits));
    const paused = await drive({
      group: 'GID_pull_pace_paused', paused: true, first: found([row(1, 'TagA')]),
    });
    check('a paused consumer still parks its queue for 1000 ms and pulls nothing',
      paused.c.isPaused() && paused.waits.length === 1 && paused.waits[0] === SUSPEND_WAIT_MS
      && paused.rounds === 0, JSON.stringify(paused.waits));
    const illegal = await drive({
      group: 'GID_pull_pace_offset_illegal',
      first: new PullResult(PullStatus.OFFSET_ILLEGAL, 500, 0, 1000, []),
    });
    check('the OFFSET_ILLEGAL round retires its cursor and takes no pacing wait',
      illegal.waits.length === 0 && !illegal.cursorLeft && illegal.rounds === 1,
      `${JSON.stringify(illegal.waits)}/${illegal.cursorLeft}`);
  }

  console.log(`\npull round pacing: ${pass} checks, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
