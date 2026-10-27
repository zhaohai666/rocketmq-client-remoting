// The generic pull/POP error backoff as ONE configurable knob — offline, with the
// real per-queue loops and their waits RECORDED instead of slept.
//
//   node --experimental-strip-types test/pull_retry_delay_knob_smoke.ts
//
// pullRetryDelayMillis is the single wait a queue's loop takes before re-issuing a
// pull (or POP) request after a round that failed for any reason other than the
// broker's own flow control. Four legs are pinned here, each with a control:
//
//   1. a fresh consumer reports the default, 1000 ms, and a failed pull round
//      waits exactly that number;
//   2. the broker's own flow-control answer (215) still waits 20 ms, on the pull
//      path and on the POP path, so the generic arm cannot swallow it;
//   3. setPullRetryDelayMillis moves what BOTH paths wait: the same configured
//      number shows up in the recorded waits of the pull loop and of the POP loop,
//      while a loop already running keeps the value it was set up with;
//   4. a non-215 error and a POLLING_FULL round both take the knob's value, and an
//      idle long-poll timeout takes nothing at all.
//
// Every wait is read BACK against what the consumer reports through
// getPullRetryDelayMillis, so a field that is declared nowhere, or a loop that
// ignores the knob, fails here instead of passing quietly.
import { DefaultMQPushConsumer, pullRetryDelayMillisFor } from '../src/client/consumer.ts';
import { ProcessQueue } from '../src/client/process_queue.ts';
import { PopProcessQueue } from '../src/client/pop_process_queue.ts';
import { queuePopLoop, POP_DELAY_WHEN_BROKER_FLOW } from '../src/client/pop_consumer.ts';
import { PopResult, processPopResponse } from '../src/client/pop_api.ts';
import { PullResult, PullStatus, ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { PopMessageResponseHeader } from '../src/remoting/headers.ts';
import { SubscriptionData } from '../src/common/subscriptionData.ts';
import { MessageQueue } from '../src/common/message.ts';
import { MQBrokerException, RemotingTimeoutException } from '../src/remoting/exception.ts';
import { ResponseCode } from '../src/remoting/codes.ts';
import { mqKey } from '../src/client/offset_store.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'NodeJsPullRetryKnobTopic';
const BROKER = 'broker-a';
const ADDR = '127.0.0.1:10911';

function queue(): MessageQueue {
  return new MessageQueue(TOPIC, BROKER, 0);
}

// baseConsumer is a push facade wired just enough for one loop round: a
// subscription, an owned queue, no cache gate within reach.
function baseConsumer(group: string): DefaultMQPushConsumer {
  const c = new DefaultMQPushConsumer(group);
  c.pullThresholdForQueue = 1 << 30;
  c.pullThresholdSizeForQueue = 1 << 30;
  c.consumeConcurrentlyMaxSpan = 1 << 30;
  c.popThresholdForQueue = 1 << 30;
  c.subscription.set(TOPIC, new SubscriptionData(TOPIC, '*'));
  c.started = true;
  return c;
}

function resolvable(c: DefaultMQPushConsumer, ok = true): void {
  (c as any)._findBrokerAddressInSubscribe = () =>
    (ok ? { addr: ADDR, isSlave: false } : null);
}

// popAnswerOf builds the PopResult the client's own response processor makes from
// one answer code, so the POP loop is driven with a real answer shape.
function popAnswerOf(code: number): PopResult {
  const rh = new PopMessageResponseHeader();
  rh.popTime = Date.now();
  rh.invisibleTime = 60000;
  rh.reviveQid = 0;
  rh.restNum = 0;
  return processPopResponse(BROKER, code, '', null, rh, TOPIC, '', false);
}

// drivePull runs the REAL per-queue pull loop over one round and records the
// waits. `failWith` is what the pull answers with, `noOffset` leaves the queue
// without a cursor, `noBroker` makes the address unresolvable. Each recorded wait
// retires the queue's stop flag, so the loop ends right after the round under
// test; a round that asks for no wait ends when the second round retires the flag.
async function drivePull(c: DefaultMQPushConsumer, opts: {
  failWith?: Error, noOffset?: boolean, noBroker?: boolean,
}): Promise<number[]> {
  const anyC = c as any;
  resolvable(c, opts.noBroker !== true);
  const mq = queue();
  anyC.processQueueTable.set(mqKey(mq), new ProcessQueue(false));
  if (!opts.noOffset) c.offsetTable.set(mqKey(mq), 0);
  c.mqClient = { updateTopicRouteInfoFromNameServer: async () => {} } as never;
  const flag = anyC._newStopFlag();
  let rounds = 0;
  anyC._pullKernel = async () => {
    rounds++;
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
  return waits;
}

// drivePop runs the REAL per-queue POP loop over one answer: either a thrown
// `failWith`, or the PopResult `answer` built from a broker code.
async function drivePop(c: DefaultMQPushConsumer, opts: {
  failWith?: Error, answer?: PopResult,
}): Promise<number[]> {
  const anyC = c as any;
  resolvable(c);
  const mq = queue();
  c.popQueueTable.set(mqKey(mq), new PopProcessQueue());
  c.registerMessageListener({
    consumeMessage: () => ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
  });
  const flag = anyC._newStopFlag();
  let rounds = 0;
  c.mqClient = {
    popMessage: async () => {
      rounds++;
      if (opts.failWith && rounds === 1) throw opts.failWith;
      if (rounds > 1) {
        // The second answer ends the run without changing what the first one
        // scheduled.
        anyC._stopFlags.delete(flag);
        return new PopResult();
      }
      return opts.answer ? opts.answer : new PopResult();
    },
    ackMessage: async () => {},
    changeInvisibleTime: async () => ({ popTime: Date.now(), invisibleTime: 60000, reviveQid: 0 }),
  } as never;
  const waits: number[] = [];
  anyC._sleepOrStop = async (ms: number) => {
    waits.push(ms);
    anyC._stopFlags.delete(flag);
    return true;
  };
  await queuePopLoop(c, mq, flag);
  return waits;
}

async function main() {
  // ---- leg 1: the default is 1000 ms, and a failed pull round waits it
  {
    const c = baseConsumer('GID_knob_default');
    check('a fresh consumer reports the generic backoff as 1000 ms',
      c.getPullRetryDelayMillis() === 1000 && c.pullRetryDelayMillis === 1000,
      String(c.getPullRetryDelayMillis()));
    const waits = await drivePull(c, {
      failWith: new MQBrokerException(ResponseCode.SYSTEM_ERROR, 'system error', ADDR),
    });
    check('a failed pull round waits exactly what the consumer reports',
      waits.length === 1 && waits[0] === c.getPullRetryDelayMillis(), JSON.stringify(waits));

    // Control: the queue whose state is not OK yet (no cursor to pull from) takes
    // the same wait, because that round failed without ever reaching the wire.
    const noCursor = await drivePull(baseConsumer('GID_knob_no_cursor'), { noOffset: true });
    check('a round with no cursor for the queue waits the same generic backoff',
      noCursor.length === 1 && noCursor[0] === 1000, JSON.stringify(noCursor));

    // Control: a round whose broker cannot be resolved is a failed round too, and
    // the route refresh it already tried must not turn into a hot retry.
    const noBroker = await drivePull(baseConsumer('GID_knob_no_broker'), { noBroker: true });
    check('a round whose broker cannot be resolved takes the generic backoff',
      noBroker.length === 1 && noBroker[0] === 1000, JSON.stringify(noBroker));
  }

  // ---- leg 2: the broker's own throttle (215) keeps its 20 ms
  {
    const c = baseConsumer('GID_knob_broker_flow');
    const waits = await drivePull(c, {
      failWith: new MQBrokerException(ResponseCode.FLOW_CONTROL, 'flow control', ADDR),
    });
    check('a pull the broker throttled (215) waits 20 ms, not the knob',
      waits.length === 1 && waits[0] === 20 && waits[0] !== c.getPullRetryDelayMillis(),
      JSON.stringify(waits));
    const popWaits = await drivePop(baseConsumer('GID_knob_pop_broker_flow'), {
      failWith: new MQBrokerException(ResponseCode.FLOW_CONTROL, 'flow control', ADDR),
    });
    check('a POP the broker throttled (215) waits the same 20 ms',
      popWaits.length === 1 && popWaits[0] === POP_DELAY_WHEN_BROKER_FLOW
      && POP_DELAY_WHEN_BROKER_FLOW === 20, JSON.stringify(popWaits));
    check('the 215 arm stays distinguishable from the generic arm',
      pullRetryDelayMillisFor(215, 1000) === 20 && pullRetryDelayMillisFor(2, 1000) === 1000);
  }

  // ---- leg 3: setting the knob moves BOTH paths
  {
    const configured = 3750;
    const pullC = baseConsumer('GID_knob_set_pull');
    pullC.setPullRetryDelayMillis(configured);
    check('the getter reads the configured value back',
      pullC.getPullRetryDelayMillis() === configured && pullC.pullRetryDelayMillis === configured,
      String(pullC.getPullRetryDelayMillis()));
    const pullWaits = await drivePull(pullC, {
      failWith: new MQBrokerException(ResponseCode.SYSTEM_ERROR, 'system error', ADDR),
    });
    check('the pull loop waits the configured value, not the default',
      pullWaits.length === 1 && pullWaits[0] === configured, JSON.stringify(pullWaits));

    const popC = baseConsumer('GID_knob_set_pop');
    popC.setPullRetryDelayMillis(configured);
    const popWaits = await drivePop(popC, {
      failWith: new MQBrokerException(ResponseCode.SYSTEM_ERROR, 'system error', ADDR),
    });
    check('the POP loop waits the same configured value',
      popWaits.length === 1 && popWaits[0] === configured, JSON.stringify(popWaits));

    // Control on the consumer whose knob now reads 3750: the throttle arm is not
    // the generic arm.
    const stillShort = await drivePull(pullC, {
      failWith: new MQBrokerException(ResponseCode.FLOW_CONTROL, 'flow control', ADDR),
    });
    check('and a configured knob leaves the 215 arm at 20 ms',
      stillShort.length === 1 && stillShort[0] === 20, JSON.stringify(stillShort));
  }

  // ---- leg 3b: the value is taken at setup, not read every round
  {
    const c = baseConsumer('GID_knob_late_change');
    const anyC = c as any;
    resolvable(c);
    const mq = queue();
    anyC.processQueueTable.set(mqKey(mq), new ProcessQueue(false));
    c.offsetTable.set(mqKey(mq), 0);
    c.mqClient = { updateTopicRouteInfoFromNameServer: async () => {} } as never;
    const flag = anyC._newStopFlag();
    const waits: number[] = [];
    let rounds = 0;
    anyC._pullKernel = async () => {
      rounds++;
      if (rounds === 1) throw new MQBrokerException(ResponseCode.SYSTEM_ERROR, 'err', ADDR);
      anyC._stopFlags.delete(flag);
      return new PullResult(PullStatus.NO_NEW_MSG, 0, 0, 0, []);
    };
    anyC._sleepOrStop = async (ms: number) => {
      waits.push(ms);
      // The knob moves while this loop is already running: the wait it schedules
      // for THIS round stays the one the loop was set up with.
      c.setPullRetryDelayMillis(9999);
      return false;
    };
    await anyC._queuePullLoop(mq, flag);
    check('a running loop keeps the backoff it was set up with, and the knob still reads the new value',
      waits.length === 1 && waits[0] === 1000 && c.getPullRetryDelayMillis() === 9999,
      `${JSON.stringify(waits)}/${c.getPullRetryDelayMillis()}`);
  }

  // ---- leg 4: a non-215 error and a POLLING_FULL round both take the knob
  {
    const c = baseConsumer('GID_knob_error_and_full');
    const plain = await drivePull(c, { failWith: new Error('connection reset') });
    check('a pull failure carrying no broker code takes the knob',
      plain.length === 1 && plain[0] === c.getPullRetryDelayMillis(), JSON.stringify(plain));

    const full = await drivePop(baseConsumer('GID_knob_polling_full'),
      { answer: popAnswerOf(ResponseCode.POLLING_FULL) });
    check('a POLLING_FULL POP round takes the knob default',
      full.length === 1 && full[0] === 1000, JSON.stringify(full));

    const moved = baseConsumer('GID_knob_polling_full_moved');
    moved.setPullRetryDelayMillis(4242);
    const fullMoved = await drivePop(moved, { answer: popAnswerOf(ResponseCode.POLLING_FULL) });
    check('and a POLLING_FULL round follows a configured knob',
      fullMoved.length === 1 && fullMoved[0] === 4242, JSON.stringify(fullMoved));

    // A POP answer whose status this client does not know lands on the same arm.
    const odd = popAnswerOf(ResponseCode.SUCCESS);
    odd.popStatus = 'NOT_A_KNOWN_STATUS' as never;
    const unknown = await drivePop(baseConsumer('GID_knob_unknown_status'), { answer: odd });
    check('an unrecognised POP status takes the knob too',
      unknown.length === 1 && unknown[0] === 1000, JSON.stringify(unknown));

    // Control: an idle long-poll timeout is a normal answer on both paths, so it
    // asks again at once; the generic arm must not swallow it.
    const timedOut = await drivePull(baseConsumer('GID_knob_timeout'), {
      failWith: new RemotingTimeoutException(ADDR, 30000),
    });
    check('control: a pull long-poll timeout waits nothing',
      timedOut.length === 0, JSON.stringify(timedOut));
    const popTimedOut = await drivePop(baseConsumer('GID_knob_pop_timeout'), {
      failWith: new RemotingTimeoutException(ADDR, 30000),
    });
    check('control: a POP long-poll timeout waits nothing either',
      popTimedOut.length === 0, JSON.stringify(popTimedOut));
  }

  console.log(`\npull retry delay knob: ${pass} checks, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
