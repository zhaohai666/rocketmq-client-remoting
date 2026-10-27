// How a POP round is re-scheduled, judged per answer status — offline, with the
// real per-queue loop and the waits RECORDED instead of slept.
//
//   node --experimental-strip-types test/pop_round_reschedule_smoke.ts
//
// One POP round answers with a status, and the status decides when the queue
// asks again:
//
//   * FOUND whose list still has rows after the pre-delivery filter: book the
//     round (pull RT for the round, pull TPS and the queue's answer debt for the
//     number DELIVERED, not the number popped), submit, then wait pullInterval
//     — and only when pullInterval is above 0; at 0 the queue asks again at
//     once;
//   * FOUND whose list the filter emptied: re-pop IMMEDIATELY, with no TPS for
//     the round. This is the one status pair that a loop cannot tell apart by
//     looking at the broker's answer alone, so it is decided after the filter;
//   * NO_NEW_MSG and POLLING_NOT_FOUND: the broker already held the poll open,
//     so asking again at once costs nothing;
//   * POLLING_FULL and any status this client does not know: back off 3000 ms;
//   * a failed round: 20 ms when the broker's own answer is the throttle code
//     215, 3000 ms for any other failure.
//
// Every scenario below asserts the recorded wait list, so a branch that never
// runs cannot pass by accident, and each rule has its paired control: FOUND with
// rows waits, the same FOUND fully filtered does not.
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MessageExt, MessageQueue } from '../src/common/message.ts';
import { encodeMessageExt } from '../src/common/messageDecoder.ts';
import { FilterAPI } from '../src/common/subscriptionData.ts';
import { PopResult, PopStatus, processPopResponse } from '../src/client/pop_api.ts';
import { PopProcessQueue } from '../src/client/pop_process_queue.ts';
import {
  queuePopLoop, POP_DELAY_WHEN_EXCEPTION, POP_DELAY_WHEN_BROKER_FLOW,
} from '../src/client/pop_consumer.ts';
import { ConsumerStatsManager } from '../src/client/consumer_stats.ts';
import { PopMessageResponseHeader } from '../src/remoting/headers.ts';
import { ResponseCode } from '../src/remoting/codes.ts';
import { MQBrokerException, RemotingTimeoutException } from '../src/remoting/exception.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { mqKey } from '../src/client/offset_store.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'PopRescheduleTopic';
const BROKER = 'broker-a';
const PULL_INTERVAL_MS = 500;

function row(offset: number, tag: string | null, reconsumeTimes: number): MessageExt {
  const m = new MessageExt(TOPIC, Buffer.from('body-' + offset), tag);
  m.setQueueId(0);
  m.setQueueOffset(offset);
  m.setCommitLogOffset(offset);
  m.setReconsumeTimes(reconsumeTimes);
  m.setBornTimestamp(Date.now());
  m.setStoreHost('127.0.0.1');
  m.setBornHost('127.0.0.1');
  return m;
}

// popResult builds what the client's own response processor returns for one
// answer of the given code, so the loop is driven with the same PopResult shape
// a socket reply produces.
function popResult(code: number, rows: MessageExt[]): PopResult {
  const rh = new PopMessageResponseHeader();
  rh.popTime = Date.now();
  rh.invisibleTime = 60000;
  rh.reviveQid = 0;
  rh.restNum = 0;
  const body = rows.length > 0 ? Buffer.concat(rows.map((r) => encodeMessageExt(r))) : null;
  return processPopResponse(BROKER, code, '', body, rh, TOPIC, '', false);
}

// drive runs the real pop loop over two answers. The waits are recorded, never
// slept, and the second answer retires the queue's stop flag so the loop ends.
async function drive(opts: {
  group: string;
  expression: string;
  first: PopResult;
  pullInterval?: number;
  maxReconsumeTimes?: number;
  failWith?: Error;
}): Promise<{
  waits: number[], rounds: number, fed: number[], acks: string[],
  delivered: MessageExt[], debt: number[], debtOnFeed: number[],
  c: DefaultMQPushConsumer,
}> {
  const c = new DefaultMQPushConsumer(opts.group);
  const anyC = c as any;
  c.pullInterval = opts.pullInterval ?? PULL_INTERVAL_MS;
  if (opts.maxReconsumeTimes !== undefined) c.setMaxReconsumeTimes(opts.maxReconsumeTimes);
  c.subscription.set(TOPIC, FilterAPI.buildSubscriptionData(TOPIC, opts.expression));
  c.started = true;
  c._statsManager = new ConsumerStatsManager();

  const mq = new MessageQueue(TOPIC, BROKER, 0);
  const pq = new PopProcessQueue();
  // Record what the queue's answer debt is fed, and still let it accumulate.
  const fed: number[] = [];
  // The counter right after each feed: a round whose rows were partly filtered
  // must leave exactly the surviving rows owed, never the whole answer.
  const debtOnFeed: number[] = [];
  const origInc = PopProcessQueue.prototype.incFoundMsg;
  pq.incFoundMsg = function (n: number): void {
    fed.push(n);
    origInc.call(this, n);
    debtOnFeed.push(this.waitAckMsgCount());
  };
  c.popQueueTable.set(mqKey(mq), pq);

  const acks: string[] = [];
  anyC._findBrokerAddressInSubscribe = () => ({ addr: '127.0.0.1:10911', isSlave: false });
  const flag = anyC._newStopFlag();
  let rounds = 0;
  c.mqClient = {
    popMessage: async () => {
      rounds++;
      if (opts.failWith && rounds === 1) throw opts.failWith;
      if (rounds > 1) {
        // The second answer ends the run without changing what it schedules.
        anyC._stopFlags.delete(flag);
        return new PopResult();
      }
      return opts.first;
    },
    ackMessage: async (_addr: string, header: any) => {
      acks.push(String(header.offset));
    },
    changeInvisibleTime: async () => ({ popTime: Date.now(), invisibleTime: 60000, reviveQid: 0 }),
  } as never;

  const delivered: MessageExt[] = [];
  c.registerMessageListener({
    consumeMessage: (msgs: MessageExt[]) => {
      delivered.push(...msgs);
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    },
  });

  const waits: number[] = [];
  const debt: number[] = [];
  anyC._sleepOrStop = async (ms: number) => {
    waits.push(ms);
    debt.push(pq.waitAckMsgCount());
    // The run ends at the first scheduled wait; a scenario that must show "no
    // wait at all" simply never gets here.
    anyC._stopFlags.delete(flag);
    return true;
  };
  await queuePopLoop(c, mq, flag);
  return { waits, rounds, fed, acks, delivered, debt, debtOnFeed, c };
}

async function main() {
  // ---- 1. FOUND with rows left: booked, submitted, then waits pullInterval
  {
    const r = await drive({
      group: 'GID_pop_resched_found', expression: '*',
      first: popResult(ResponseCode.SUCCESS, [row(0, 'TagA', 0), row(1, 'TagB', 0), row(2, null, 0)]),
    });
    check('a FOUND round that delivered waits exactly pullInterval, once',
      r.waits.length === 1 && r.waits[0] === PULL_INTERVAL_MS, JSON.stringify(r.waits));
    check('and submits every row of it, since nothing is filtered',
      r.delivered.length === 3, String(r.delivered.length));
    check('the answer debt is fed with the delivered count',
      r.fed.length === 1 && r.fed[0] === 3, JSON.stringify(r.fed));
    const tps = (r.c as any)._statsManager.topicAndGroupPullTPS.find(`${TOPIC}@GID_pop_resched_found`);
    check('pull TPS records the delivered count', tps != null && tps.value === 3,
      `tps=${tps ? tps.value : 'none'}`);
    const rt = (r.c as any)._statsManager.topicAndGroupPullRT.find(`${TOPIC}@GID_pop_resched_found`);
    check('pull RT records one sample for the round', rt != null && rt.times === 1,
      `times=${rt ? rt.times : 'none'}`);
  }

  // ---- 2. the same round with pullInterval at 0 asks again at once
  {
    const r = await drive({
      group: 'GID_pop_resched_interval0', expression: '*', pullInterval: 0,
      first: popResult(ResponseCode.SUCCESS, [row(0, 'TagA', 0)]),
    });
    check('pullInterval 0 means no wait between FOUND rounds',
      r.waits.length === 0, JSON.stringify(r.waits));
    check('and the row is still delivered and booked',
      r.delivered.length === 1 && r.fed.length === 1 && r.fed[0] === 1,
      `${r.delivered.length}/${JSON.stringify(r.fed)}`);
  }

  // ---- 3. FOUND with a tag set: only the matches are booked and submitted
  {
    const r = await drive({
      group: 'GID_pop_resched_partly', expression: 'TagA', maxReconsumeTimes: 0,
      first: popResult(ResponseCode.SUCCESS,
        [row(0, 'TagA', 0), row(1, 'TagB', 0), row(2, 'TagA', 1)]),
    });
    check('a round that filtered part of its answer still waits pullInterval',
      r.waits.length === 1 && r.waits[0] === PULL_INTERVAL_MS, JSON.stringify(r.waits));
    check('and submits only the row that survived',
      r.delivered.length === 1 && r.delivered[0].getQueueOffset() === 0,
      r.delivered.map((m) => String(m.getQueueOffset())).join(','));
    check('the debt and TPS follow the surviving row, not the answer',
      r.fed.length === 1 && r.fed[0] === 1, JSON.stringify(r.fed));
    const tps = (r.c as any)._statsManager.topicAndGroupPullTPS.find(`${TOPIC}@GID_pop_resched_partly`);
    check('pull TPS for the round is the surviving count', tps != null && tps.value === 1,
      `tps=${tps ? tps.value : 'none'}`);
    check('both rows the filter removed were acked on the spot',
      r.acks.includes('1') && r.acks.includes('2'), r.acks.join(','));
    // Only the surviving row owes an answer: the rows the filter removed were
    // acked before the feed, so the counter never held them.
    check('and the answer debt is fed with the surviving row only',
      r.debtOnFeed.length === 1 && r.debtOnFeed[0] === 1, JSON.stringify(r.debtOnFeed));
  }

  // ---- 4. FOUND emptied by the filter: no wait, no TPS, no submit
  {
    const r = await drive({
      group: 'GID_pop_resched_emptied', expression: 'TagA',
      first: popResult(ResponseCode.SUCCESS, [row(0, 'TagB', 0), row(1, null, 0)]),
    });
    check('a FOUND answer whose rows were all filtered out re-pops at once',
      r.waits.length === 0, JSON.stringify(r.waits));
    check('nothing of it is submitted', r.delivered.length === 0,
      String(r.delivered.length));
    check('nothing is fed to the answer debt', r.fed.length === 0, JSON.stringify(r.fed));
    const tps = (r.c as any)._statsManager.topicAndGroupPullTPS.find(`${TOPIC}@GID_pop_resched_emptied`);
    check('and the round records no pulled message', tps == null || tps.value === 0,
      `tps=${tps ? tps.value : 'none'}`);
    const rt = (r.c as any)._statsManager.topicAndGroupPullRT.find(`${TOPIC}@GID_pop_resched_emptied`);
    check('while the round still counts as one pull for the latency figure',
      rt != null && rt.times === 1, `times=${rt ? rt.times : 'none'}`);
  }

  // ---- 5. FOUND with an empty answer of its own: the same immediate re-pop
  {
    const r = await drive({
      group: 'GID_pop_resched_empty_body', expression: '*',
      first: popResult(ResponseCode.SUCCESS, []),
    });
    check('control: FOUND with no message at all re-pops at once too',
      r.waits.length === 0 && r.rounds === 2, `${JSON.stringify(r.waits)}/${r.rounds}`);
  }

  // ---- 6. NO_NEW_MSG and POLLING_NOT_FOUND ask again at once
  {
    const noNew = new PopResult();
    noNew.popStatus = PopStatus.NO_NEW_MSG;
    const r1 = await drive({ group: 'GID_pop_resched_nonew', expression: '*', first: noNew });
    check('NO_NEW_MSG re-pops at once, with no wait and no submit',
      r1.waits.length === 0 && r1.delivered.length === 0 && r1.fed.length === 0,
      JSON.stringify(r1.waits));
    const r2 = await drive({
      group: 'GID_pop_resched_pollingnf', expression: '*',
      first: popResult(ResponseCode.PULL_NOT_FOUND, []),
    });
    check('POLLING_NOT_FOUND re-pops at once',
      r2.waits.length === 0 && r2.delivered.length === 0, JSON.stringify(r2.waits));
    check('POLLING_NOT_FOUND is what the broker\'s no-match answer becomes',
      r2.rounds === 2, String(r2.rounds));
  }

  // ---- 7. POLLING_FULL and an unknown status back off 3000 ms
  {
    const r = await drive({
      group: 'GID_pop_resched_pollingfull', expression: '*',
      first: popResult(ResponseCode.POLLING_FULL, []),
    });
    check('POLLING_FULL parks the queue for 3000 ms',
      r.waits.length === 1 && r.waits[0] === 3000, JSON.stringify(r.waits));
    check('and the poll-full wait is the exported backoff, unchanged',
      POP_DELAY_WHEN_EXCEPTION === 3000, String(POP_DELAY_WHEN_EXCEPTION));
    check('POLLING_FULL books nothing: no submit, no debt, no TPS',
      r.delivered.length === 0 && r.fed.length === 0
      && (r.c as any)._statsManager.topicAndGroupPullTPS.find(`${TOPIC}@GID_pop_resched_pollingfull`) == null);

    const odd = popResult(ResponseCode.SUCCESS, [row(0, 'TagA', 0)]);
    odd.popStatus = 'NOT_A_KNOWN_STATUS' as any;
    const r2 = await drive({ group: 'GID_pop_resched_unknown', expression: '*', first: odd });
    check('a status this client does not know is parked for the same 3000 ms',
      r2.waits.length === 1 && r2.waits[0] === 3000, JSON.stringify(r2.waits));
    check('control: an unknown status does not deliver the rows it carries',
      r2.delivered.length === 0 && r2.fed.length === 0,
      `${r2.delivered.length}/${JSON.stringify(r2.fed)}`);
  }

  // ---- 8. the failed round keeps its two waits apart
  {
    const throttled = await drive({
      group: 'GID_pop_resched_flow', expression: '*',
      first: new PopResult(), failWith: new MQBrokerException(215, 'flow', '127.0.0.1:10911'),
    });
    check('a POP the broker throttled (215) parks for 20 ms',
      throttled.waits.length === 1 && throttled.waits[0] === POP_DELAY_WHEN_BROKER_FLOW
      && POP_DELAY_WHEN_BROKER_FLOW === 20, JSON.stringify(throttled.waits));
    const failed = await drive({
      group: 'GID_pop_resched_failed', expression: '*',
      first: new PopResult(), failWith: new MQBrokerException(2, 'system error', '127.0.0.1:10911'),
    });
    check('any other POP failure parks for 3000 ms',
      failed.waits.length === 1 && failed.waits[0] === 3000, JSON.stringify(failed.waits));
    const timedOut = await drive({
      group: 'GID_pop_resched_timeout', expression: '*',
      first: new PopResult(), failWith: new RemotingTimeoutException('127.0.0.1:10911', 3000),
    });
    check('control: an idle long-poll timeout re-pops at once, with no wait',
      timedOut.waits.length === 0 && timedOut.rounds === 2, JSON.stringify(timedOut.waits));
    check('a failed round books nothing at all',
      throttled.fed.length === 0 && failed.fed.length === 0
      && (failed.c as any)._statsManager.topicAndGroupPullRT.find(`${TOPIC}@GID_pop_resched_failed`) == null);
  }

  console.log(`\npop round reschedule: ${pass} checks, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
