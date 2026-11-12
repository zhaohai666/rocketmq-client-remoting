// A queue that STAYS assigned can still lose its pull cursor: the self-heal
// sweep in syncPullLoops (pull expired) and the OFFSET_ILLEGAL fix both route
// through _retireQueue, which deletes the offsetTable entry. The next
// doRebalance must recompute the cursor for that queue — gated on "does the
// queue still have a live offset", never on "was it assigned before" — or the
// rebuilt pull loop reads offset === undefined, spins silently in its backoff
// branch, and the queue never pulls again (2026-11-12, live orderly/transaction
// run: queues stopped pulling after the first stall sweep).
//
// Contract pinned here:
//   * the self-heal round really deletes the cursor (the defect precondition);
//   * the NEXT round restores it from the committed offset in the store
//     (computePullFromWhere reads READ_FROM_STORE first), so the rebuilt loop
//     pulls from the corrected position, not from a silent spin;
//   * a healthy assigned queue's cursor is never recomputed (the control: the
//     compute stub fires only for the repaired queue);
//   * a queue whose offset CANNOT be resolved is dropped from that round's
//     assignment (skipped) rather than left owned with no cursor — the same
//     fail-visible semantics as a fresh assignment.
//
// Run: node --experimental-strip-types test/rebalance_stall_repair_smoke.ts
import { MessageQueue } from '../src/common/message.ts';
import { SubscriptionData } from '../src/common/subscriptionData.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { MessageModel } from '../src/remoting/heartbeat.ts';
import { TopicRouteData } from '../src/remoting/route.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { mqKey } from '../src/client/offset_store.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'NodeJsStallRepairTopic';
const BROKER = 'broker-a';
// A wall-clock offset sentinel: the "committed offset" the stubbed store returns.
const OFFSET = 42;

async function harness(group: string): Promise<any> {
  const remoting: any = {
    invokeSync: () => {
      throw new Error('no broker answers in this smoke');
    },
    invokeOneway: () => Promise.resolve(),
  };
  const client = new MQClient(`node-ssr-test@${process.pid}`, '127.0.0.1:1', remoting);
  client.brokerAddrTable.set(BROKER, { [MixAll.MASTER_ID]: '127.0.0.1:19760' });
  const c: any = new DefaultMQPushConsumer(group);
  // Broadcasting owns every route queue, so the assignment is exactly the route
  // and no broker member list can move it — same trick as rebalance_change_smoke.
  c.setMessageModel(MessageModel.BROADCASTING);
  c.subscription.set(TOPIC, new SubscriptionData(TOPIC, '*'));
  c.started = true;
  c.mqClient = client;
  client.registerConsumer(group, c);
  // The stubbed store: every compute reads back the committed offset. The counter
  // distinguishes "the healthy queue's cursor was left alone" (0 further calls)
  // from "the repaired queue's cursor was recomputed" (1 call).
  let computes = 0;
  c.computePullFromWhere = async (): Promise<number> => { computes++; return OFFSET; };
  c._queuePullLoop = async (): Promise<void> => {};
  c._sleepOrStop = async (): Promise<boolean> => true;
  client.popMessage = async (): Promise<never> => { throw new Error('no broker in this smoke'); };

  const h = {
    c, client,
    computes(): number { return computes; },
    resetComputes(): void { computes = 0; },
    cursor(key: string): number | undefined { return c.offsetTable.get(key); },
    // setQueues scripts the topic's route: N readable queues on one broker.
    setQueues(n: number) {
      client.topicRouteTable.set(TOPIC, TopicRouteData.decode(Buffer.from(JSON.stringify({
        orderTopicConf: null,
        queueDatas: [{ brokerName: BROKER, readQueueNums: n, writeQueueNums: n, perm: 6, topicSysFlag: 0 }],
        brokerDatas: [{
          cluster: 'DefaultCluster', brokerName: BROKER,
          brokerAddrs: { '0': '127.0.0.1:19760' }, zoneName: null, enableActingMaster: false,
        }],
        filterServerTable: {},
      }), 'utf8')));
      return h;
    },
    stall(key: string): void {
      const pq = c.processQueueTable.get(key);
      pq.lastPullTimestamp = 1; // a stamp so old the loop looks dead to the stall check
    },
    async settle(): Promise<void> { await new Promise<void>((r) => setImmediate(r)); },
  };

  // Round 1: the assignment moves from "nothing" to the route; every queue gets
  // its cursor from the stubbed store.
  h.setQueues(2);
  await c.doRebalance();
  await h.settle();
  return h;
}

(async () => {
  const key0 = mqKey(new MessageQueue(TOPIC, BROKER, 0));
  const key1 = mqKey(new MessageQueue(TOPIC, BROKER, 1));

  {
    const h = await harness('G_nodejs_ssr_repair');
    check('precondition: round 1 gave every assigned queue a cursor',
      h.cursor(key0) === OFFSET && h.cursor(key1) === OFFSET,
      `0=${h.cursor(key0)} 1=${h.cursor(key1)}`);

    // The self-heal round: queue 0's pull loop stalled. syncPullLoops retires it
    // (deleting the cursor) and rebuilds the ProcessQueue + pull loop.
    h.resetComputes();
    h.stall(key0);
    await h.c.doRebalance();
    await h.settle();
    check('the self-heal round really deleted the retired queue\'s cursor (defect precondition)',
      h.cursor(key0) === undefined && h.c.processQueueTable.has(key0),
      `cursor=${h.cursor(key0)}`);
    check('control: the sibling queue kept its cursor through the self-heal round',
      h.cursor(key1) === OFFSET, `cursor=${h.cursor(key1)}`);
    check('the self-heal round itself recomputes nothing (its precompute ran before the sweep)',
      h.computes() === 0, `computes=${h.computes()}`);

    // The next round: the assignment did NOT change, but the repaired queue has
    // no cursor. The fix recomputes it from the store; the old `was.has` gate
    // skipped exactly this queue and the rebuilt loop spun forever.
    await h.c.doRebalance();
    await h.settle();
    check('the next rebalance round restores the repaired queue\'s cursor',
      h.cursor(key0) === OFFSET, `cursor=${h.cursor(key0)}`);
    check('exactly one compute was spent on the repair',
      h.computes() === 1, `computes=${h.computes()}`);
    check('and the queue is still owned (not skipped) after the repair',
      h.c.processQueueTable.has(key0), 'missing from the process-queue table');
  }

  {
    // The unresolvable-offset path: the stall round retires the queue (its
    // precompute ran BEFORE the sweep, so the cursor is still there then); the
    // NEXT round consults the compute, and a compute that cannot resolve drops
    // the queue from that round (fail-visible) instead of owning it cursor-less.
    const h = await harness('G_nodejs_ssr_unresolvable');
    h.resetComputes();
    h.stall(key0);
    h.c.computePullFromWhere = async (): Promise<number> => -1; // this round cannot resolve it
    await h.c.doRebalance();
    await h.settle();
    check('the stall round rebuilds the queue but leaves it cursor-less (defect precondition)',
      h.c.processQueueTable.has(key0) && h.cursor(key0) === undefined,
      `owned=${h.c.processQueueTable.has(key0)} cursor=${h.cursor(key0)}`);
    await h.c.doRebalance();
    await h.settle();
    check('a queue whose cursor cannot be resolved is dropped from the next round',
      !h.c.processQueueTable.has(key0), `owned=${h.c.processQueueTable.has(key0)}`);
    check('control: the sibling queue stays owned through those rounds',
      h.c.processQueueTable.has(key1), 'sibling wrongly dropped');
  }

  console.log(`\nPASS=${pass} FAIL=${fail}`);
  process.exit(fail === 0 ? 0 : 1);
})();
