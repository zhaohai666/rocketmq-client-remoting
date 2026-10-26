// What a rebalance round whose queue set MOVED pays — offline test of
// _afterAssignmentChange and the per-topic change detection in front of it.
//
// Contract pinned here (every positive has a paired control that goes red when the
// step is dropped, widened to every topic, or spent on a round that did not move):
//   * the trigger is one topic's assignment SET changing. Comparing the total queue
//     count is not enough — losing queue 0 and gaining queue 2 on another broker
//     keeps the count while changing who owns what, and that IS a change;
//   * only the topics that moved re-share their subscription version, and the beat
//     that follows carries those versions ON THE WIRE: a broker that had suppressed
//     this client's unchanged subscriptions takes them again, which is what stops a
//     queue this client just picked up from being served under a stale filter;
//   * a topic that did NOT move keeps its pinned version, both in the subscription
//     table and in the beat body — a blanket restamp over every subscription is red
//     on that control;
//   * the brokers are beaten IMMEDIATELY, not on the 30s periodic round, exactly once
//     per changed round;
//   * the topic cache budgets are divided in the same package, AFTER the pull state was
//     rebuilt, so the divisor is this round's queue count;
//   * the application's queue listener hears about the moved topics only, once each,
//     and the unchanged round tells it nothing;
//   * a self-heal round — the queue stays assigned and only its stalled pull loop's
//     state was retired and rebuilt — is NOT a change: nothing is re-stamped, no beat
//     goes out, the listener hears nothing;
//   * under POP the same package runs once, from the pull-side rebalance, and the POP
//     loop rebuild spends nothing on top of it.
//
// Run: node --experimental-strip-types test/rebalance_change_smoke.ts
import { MessageQueue } from '../src/common/message.ts';
import { ProcessQueue } from '../src/client/process_queue.ts';
import { SubscriptionData } from '../src/common/subscriptionData.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { HeartbeatData, MessageModel } from '../src/remoting/heartbeat.ts';
import { TopicRouteData } from '../src/remoting/route.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { mqKey } from '../src/client/offset_store.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC_A = 'NodeJsRebalanceChangeTopicA';
const TOPIC_B = 'NodeJsRebalanceChangeTopicB';
const BROKER = 'broker-a';
const OTHER_BROKER = 'broker-b';
// A wall-clock version from the past. A re-shared version is then a comparison rather
// than a race against millisecond resolution, and a round that must NOT touch a
// subscription leaves this sentinel in place where the assertion can see it.
const STALE = 1_700_000_000_000;

// One heartbeat body as the broker reads it: the subVersion of every topic carried.
type Beat = { subs: Map<string, number> };

// BeatBroker stands in for the transport: it decodes each HEART_BEAT(34) body the way a
// broker would, so "the beat carries the re-shared version" is a wire assertion rather
// than an inspection of the client's own map.
class BeatBroker {
  addr: string;
  beats: Beat[] = [];

  constructor(addr: string) { this.addr = addr; }

  invokeSync(_addr: string, request: RemotingCommand): RemotingCommand {
    if (request.code !== RequestCode.HEART_BEAT) {
      throw new Error(`unexpected request code ${request.code} in the rebalance smoke`);
    }
    const hb = HeartbeatData.decode(request.body as Buffer);
    const subs = new Map<string, number>();
    for (const cd of hb.consumerDataSet) {
      for (const s of cd.subscriptionDataSet) subs.set(s.topic, s.subVersion);
    }
    this.beats.push({ subs });
    const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
    response.opaque = request.opaque;
    return response;
  }

  count(): number { return this.beats.length; }
  // last is safe on an empty recording: a missing beat reads as a beat that carries no
  // subscription, so the assertion below fails with a number instead of throwing and
  // taking the rest of the suite down with it.
  last(): Beat { return this.beats[this.beats.length - 1] ?? { subs: new Map<string, number>() }; }
  clear(): void { this.beats.length = 0; }
}

// harness wires a real MQClient (so the beat body is built by the real heartbeat
// builder) to a broadcast push consumer whose queue set the test controls. Every broker
// the instance knows gets its own recorded transport: "one beat" means one beat PER
// registered broker, which is what an immediate rebalance beat actually is.
async function harness(group: string, pop = false): Promise<any> {
  const brokers = new Map<string, BeatBroker>([
    [BROKER, new BeatBroker('127.0.0.1:19260')],
    [OTHER_BROKER, new BeatBroker('127.0.0.1:19261')],
  ]);
  const remoting: any = {
    invokeSync: (addr: string, request: RemotingCommand) => {
      for (const broker of brokers.values()) {
        if (broker.addr === addr) return broker.invokeSync(addr, request);
      }
      throw new Error(`no broker answers at ${addr}`);
    },
    invokeOneway: () => Promise.resolve(),
  };
  const client = new MQClient(`node-rc-test@${process.pid}`, '127.0.0.1:1', remoting);
  for (const [name, broker] of brokers) {
    client.brokerAddrTable.set(name, { [MixAll.MASTER_ID]: broker.addr });
  }
  const broker = brokers.get(BROKER)!;
  const c: any = new DefaultMQPushConsumer(group);
  // Broadcasting owns every route queue, so the assignment is exactly the route the
  // test scripts below and no broker member list can move it.
  c.setMessageModel(MessageModel.BROADCASTING);
  if (pop) c.setPopMode(true);
  c.subscription.set(TOPIC_A, new SubscriptionData(TOPIC_A, '*'));
  c.subscription.set(TOPIC_B, new SubscriptionData(TOPIC_B, '*'));
  c.started = true;
  c.mqClient = client;
  client.registerConsumer(group, c);
  const notifications: Array<{ topic: string, all: number, divided: number }> = [];
  c.setMessageQueueListener((topic: string, mqAll: MessageQueue[], mqDivided: MessageQueue[]) =>
    notifications.push({ topic, all: mqAll.length, divided: mqDivided.length }));
  // Offline stubs for the background work a rebuild kicks off; the table rebuild,
  // the version stamping, the beat and the notification all run for real.
  c.computePullFromWhere = async () => -1;
  c._queuePullLoop = async () => {};
  c._sleepOrStop = async () => true;
  client.popMessage = async () => { throw new Error('no broker in this smoke'); };

  const h = {
    c, client, broker, brokers, notifications,
    // setQueues scripts one topic's route: N readable queues on one broker.
    setQueues(topic: string, n: number, brokerName = BROKER) {
      const target = brokers.get(brokerName)!;
      const route = TopicRouteData.decode(Buffer.from(JSON.stringify({
        orderTopicConf: null,
        queueDatas: [{ brokerName, readQueueNums: n, writeQueueNums: n, perm: 6, topicSysFlag: 0 }],
        brokerDatas: [{
          cluster: 'DefaultCluster', brokerName,
          brokerAddrs: { '0': target.addr }, zoneName: null, enableActingMaster: false,
        }],
        filterServerTable: {},
      }), 'utf8'));
      client.topicRouteTable.set(topic, route);
      return h;
    },
    // pin parks each topic's subscription version at the sentinel.
    pin(...topics: string[]) {
      for (const topic of topics) c.subscription.get(topic)!.subVersion = STALE;
      return h;
    },
    version(topic: string): number { return c.subscription.get(topic)!.subVersion; },
    owned(topic: string): number {
      let n = 0;
      for (const key of c.processQueueTable.keys()) if (key.startsWith(`${topic}@`)) n++;
      for (const key of c.popQueueTable.keys()) if (key.startsWith(`${topic}@`)) n++;
      return n;
    },
    // rounds is how many beat ROUNDS reached the transport: one round is one beat at
    // every registered broker, so the per-broker counts have to agree.
    rounds(): number {
      const counts = new Set<number>();
      for (const b of brokers.values()) counts.add(b.count());
      return counts.size === 1 ? broker.count() : -1;
    },
    // settle lets the fire-and-forget beat reach the recorded transport. The beat BODY
    // is built synchronously by the rebalance package; only the delivery is awaited.
    async settle() { await new Promise<void>((r) => setImmediate(r)); return h; },
    reset() { for (const b of brokers.values()) b.clear(); notifications.length = 0; return h; },
  };
  // Round 1: both topics move from "no queues" to their routes, which is a change like
  // any other. The test then parks what round 1 did and judges the rounds after it.
  h.setQueues(TOPIC_A, 2).setQueues(TOPIC_B, 2);
  await c.doRebalance();
  await h.settle();
  h.reset();
  return h;
}

// ---------------------------------------------------------------------------
// 1) an unchanged round spends nothing, and the beat path is provably alive
// ---------------------------------------------------------------------------
{
  const h = await harness('G_nodejs_rc_unchanged');
  h.pin(TOPIC_A, TOPIC_B);
  await h.c.doRebalance();
  await h.settle();
  check('an unchanged round re-shares no subscription version',
    h.version(TOPIC_A) === STALE && h.version(TOPIC_B) === STALE,
    `${h.version(TOPIC_A)}/${h.version(TOPIC_B)}`);
  check('an unchanged round sends no heartbeat to any broker', h.rounds() === 0,
    `beats=${h.rounds()}`);
  check('an unchanged round notifies the queue listener nothing',
    h.notifications.length === 0, JSON.stringify(h.notifications));

  // Paired control on the same harness: a beat the caller asks for right now still
  // carries the PINNED version. So the zero above is "no rebalance beat", not "the
  // recorded transport never sees a heartbeat".
  await h.client.sendHeartbeatToAllBrokers();
  await h.settle();
  check('control: a plain beat reaches every registered broker', h.rounds() === 1,
    `rounds=${h.rounds()}`);
  check('control: and the beat body carries the pinned version of every topic',
    h.broker.last().subs.get(TOPIC_A) === STALE && h.broker.last().subs.get(TOPIC_B) === STALE,
    JSON.stringify([...h.broker.last().subs]));
}

// ---------------------------------------------------------------------------
// 2) a grown topic restamps only that topic, beats once, and the wire carries it
// ---------------------------------------------------------------------------
{
  const h = await harness('G_nodejs_rc_grow');
  h.pin(TOPIC_A, TOPIC_B);
  // Growing TOPIC_A to 3 queues also grows the total, so the paired control below
  // (TOPIC_B untouched) is what separates a per-topic restamp from a blanket one.
  h.setQueues(TOPIC_A, 3);
  h.c.pullThresholdForTopic = 1000;
  h.c.pullThresholdSizeForTopic = -1;
  await h.c.doRebalance();
  await h.settle();

  check('the moved topic re-shared its subscription version', h.version(TOPIC_A) > STALE,
    `version=${h.version(TOPIC_A)}`);
  check('control: the topic that did not move keeps its pinned version',
    h.version(TOPIC_B) === STALE, `version=${h.version(TOPIC_B)}`);
  check('a changed round beats every broker exactly once', h.rounds() === 1,
    `rounds=${h.rounds()}`);
  const beat = h.broker.last();
  check('the beat carries the freshly re-shared version on the wire',
    beat.subs.get(TOPIC_A) === h.version(TOPIC_A) && beat.subs.get(TOPIC_A)! > STALE,
    `${beat.subs.get(TOPIC_A)} vs ${h.version(TOPIC_A)}`);
  check('control: and the wire still shows the pinned version of the topic that did not move',
    beat.subs.get(TOPIC_B) === STALE, `${beat.subs.get(TOPIC_B)}`);
  // The division belongs to the same package and runs after the rebuild, so its divisor
  // is this round's queue count (3 + 2), not the previous round's (2 + 2).
  check('the cache budget was divided over the queues this round now owns (1000/5 = 200)',
    h.c.pullThresholdForQueue === 200, `got ${h.c.pullThresholdForQueue}`);
  check('the queue listener heard about the moved topic only',
    h.notifications.length === 1 && h.notifications[0].topic === TOPIC_A,
    JSON.stringify(h.notifications));
  check('and the notification carried this round\'s queues: 3 of 3',
    h.notifications.length === 1 && h.notifications[0].divided === 3
    && h.notifications[0].all === 3, JSON.stringify(h.notifications));
}

// ---------------------------------------------------------------------------
// 3) a queue SWAP that keeps the count is still a change
// ---------------------------------------------------------------------------
// The assignment count is the same before and after, so a size comparison would call
// this round unchanged: no restamp, no beat, and the broker keeps serving the queues
// this client just picked up under the filter of the ones it lost.
{
  const h = await harness('G_nodejs_rc_swap');
  h.pin(TOPIC_A, TOPIC_B);
  h.setQueues(TOPIC_A, 2, OTHER_BROKER); // 2 queues elsewhere instead of 2 here
  await h.c.doRebalance();
  await h.settle();
  check('a same-count queue swap restamps that topic', h.version(TOPIC_A) > STALE,
    `version=${h.version(TOPIC_A)}`);
  check('a same-count queue swap beats the brokers once', h.rounds() === 1,
    `rounds=${h.rounds()}`);
  check('the swapped-in queues are the ones this client now owns',
    h.owned(TOPIC_A) === 2 && ![...Array.from(h.c.processQueueTable.keys())]
      .some((k: string) => k.startsWith(`${TOPIC_A}@${BROKER}@`)),
    JSON.stringify([...h.c.processQueueTable.keys()]));
  check('control: the topic whose queues did not move is not restamped',
    h.version(TOPIC_B) === STALE, `version=${h.version(TOPIC_B)}`);
  check('and the listener hears the swapped topic with its new queues',
    h.notifications.length === 1 && h.notifications[0].topic === TOPIC_A
    && h.notifications[0].divided === 2, JSON.stringify(h.notifications));
}

// ---------------------------------------------------------------------------
// 4) a self-heal round is not a change
// ---------------------------------------------------------------------------
// syncPullLoops retires and rebuilds a queue whose pull loop stalled. The queue stays
// assigned, so nothing the broker knows about moved: the round that rebuilds it pays no
// version stamp, no beat and no notification — while the rebuild itself still happens.
{
  const h = await harness('G_nodejs_rc_selfheal');
  h.pin(TOPIC_A, TOPIC_B);
  const key = mqKey(new MessageQueue(TOPIC_A, BROKER, 0));
  const stalled = h.c.processQueueTable.get(key) as ProcessQueue;
  stalled.lastPullTimestamp = 1; // a stamp so old the loop looks dead to the stall check
  await h.c.doRebalance();
  await h.settle();
  const rebuilt = h.c.processQueueTable.get(key) as ProcessQueue;
  check('the stalled queue really was rebuilt',
    rebuilt !== stalled && rebuilt.lastPullTimestamp > 1_000_000_000_000,
    `stamp=${rebuilt && rebuilt.lastPullTimestamp}`);
  check('control: the rebuilt queue is still the one this client owns', h.owned(TOPIC_A) === 2,
    `owned=${h.owned(TOPIC_A)}`);
  check('a self-heal round sends no heartbeat', h.rounds() === 0, `rounds=${h.rounds()}`);
  check('a self-heal round re-shares no subscription version',
    h.version(TOPIC_A) === STALE && h.version(TOPIC_B) === STALE,
    `${h.version(TOPIC_A)}/${h.version(TOPIC_B)}`);
  check('a self-heal round notifies the queue listener nothing',
    h.notifications.length === 0, JSON.stringify(h.notifications));
}

// ---------------------------------------------------------------------------
// 5) the loss of a queue is a change too, and the topic vanishing entirely is one
// ---------------------------------------------------------------------------
{
  const h = await harness('G_nodejs_rc_shrink');
  h.pin(TOPIC_A, TOPIC_B);
  h.setQueues(TOPIC_A, 1);
  await h.c.doRebalance();
  await h.settle();
  check('a shrunk assignment restamps that topic and beats once',
    h.version(TOPIC_A) > STALE && h.rounds() === 1,
    `version=${h.version(TOPIC_A)} rounds=${h.rounds()}`);
  check('the revoked queue left the process-queue table', h.owned(TOPIC_A) === 1,
    `owned=${h.owned(TOPIC_A)}`);

  // A topic that disappears from the route entirely: its queues moved (to nobody), so
  // it is still a change — and the beat that follows covers the surviving topic's
  // subscription state as usual.
  const gone = await harness('G_nodejs_rc_topic_gone');
  gone.pin(TOPIC_A, TOPIC_B);
  gone.client.topicRouteTable.set(TOPIC_B, TopicRouteData.decode(Buffer.from(JSON.stringify({
    orderTopicConf: null, queueDatas: [], brokerDatas: [], filterServerTable: {},
  }), 'utf8')));
  await gone.c.doRebalance();
  await gone.settle();
  check('a topic whose queues all went away restamps it', gone.version(TOPIC_B) > STALE,
    `version=${gone.version(TOPIC_B)}`);
  check('control: a topic that kept its queues is not restamped by that round',
    gone.version(TOPIC_A) === STALE, `version=${gone.version(TOPIC_A)}`);
  check('and exactly one beat round goes out for the change', gone.rounds() === 1,
    `rounds=${gone.rounds()}`);
}

// ---------------------------------------------------------------------------
// 6) POP: one package, from the pull-side rebalance, no second notify
// ---------------------------------------------------------------------------
{
  const h = await harness('G_nodejs_rc_pop', true);
  h.pin(TOPIC_A, TOPIC_B);
  check('POP owns its queues from this rebalance',
    h.c.popQueueTable.size === 4 && h.c.processQueueTable.size === 0,
    `pop=${h.c.popQueueTable.size} pull=${h.c.processQueueTable.size}`);
  await h.c.doRebalance();
  await h.settle();
  check('control: an unchanged POP round spends no beat, no restamp, no notification',
    h.rounds() === 0 && h.version(TOPIC_A) === STALE && h.notifications.length === 0,
    `rounds=${h.rounds()} version=${h.version(TOPIC_A)} notes=${h.notifications.length}`);
  h.setQueues(TOPIC_A, 5);
  await h.c.doRebalance();
  await h.settle();
  check('a changed POP round restamps the moved topic once and beats once',
    h.version(TOPIC_A) > STALE && h.version(TOPIC_B) === STALE && h.rounds() === 1,
    `rounds=${h.rounds()} version=${h.version(TOPIC_A)}`);
  check('the changed POP round notifies the moved topic exactly once',
    h.notifications.length === 1 && h.notifications[0].topic === TOPIC_A
    && h.notifications[0].divided === 5, JSON.stringify(h.notifications));
}

console.log(`\nrebalance change: ${pass} checks, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
