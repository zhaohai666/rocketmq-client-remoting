// DefaultMQPullConsumer — pull-side background rebalance (offline).
//
// Java's pull consumer is NOT the "no rebalance" shape this port used to have: the
// shared RebalanceService thread calls `mqClientFactory.doRebalance()` every
// `rocketmq.client.rebalance.waitInterval` (20s), which walks the registered
// consumers and calls `tryRebalance()`. That gates on `enableRebalance` (default
// **true**, DefaultMQPullConsumer.java:97) and then runs
// `rebalanceImpl.doRebalance(false)` -> allocate -> `messageQueueChanged`.
//
// The assertions below pin the things that are easy to get wrong:
//   * `enableRebalance == false` must still report **balanced** (tryRebalance
//     returns true at DefaultMQPullConsumerImpl:398-402) — reporting "unbalanced"
//     would retry at minInterval forever and burn a round-trip per second.
//   * the listener fires **only when the allocation changed**, receiving
//     `(topic, mqAll, mqDivided)` — the same three arguments RebalancePullImpl
//     (:44-53) passes.
//   * a throwing listener must not escape (RebalancePullImpl:49-51).
//   * fetchMessageQueuesInBalance is a PURE query: it prefers the table the
//     background rebalance filled, computes on demand otherwise, and never
//     notifies or writes the table in either path.
//
// Run: node --experimental-strip-types test/pull_rebalance_smoke.ts
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { AllocateMessageQueueAveragely, type AllocateMessageQueueStrategy } from '../src/client/allocate.ts';
import { MessageQueue } from '../src/common/message.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'PullRebalanceTopic';
const BROKER = 'broker-a';

function queues(n: number, topic = TOPIC): MessageQueue[] {
  return Array.from({ length: n }, (_, i) => new MessageQueue(topic, BROKER, i));
}

function ids(mqs: MessageQueue[]): number[] { return mqs.map((m) => m.getQueueId()); }
function identity(mqs: MessageQueue[]): string[] {
  return mqs.map((m) => `${m.getBrokerName()}-${m.getQueueId()}`).sort();
}
function setOf(mqs: MessageQueue[]): Set<string> { return new Set(identity(mqs)); }

// Minimal stand-in for MQClient: a cached route (subscribe view) plus the group's
// consumer id list. `cids === null` plays "GET_CONSUMER_LIST_BY_GROUP unavailable".
class FakeClient {
  queues: MessageQueue[];
  cids: string[] | null;
  heartbeatCalls = 0;

  constructor(mqAll: MessageQueue[], cids: string[] | null) {
    this.queues = [...mqAll];
    this.cids = cids == null ? null : [...cids];
  }

  getTopicRouteData(_topic: string): any {
    return {
      brokerDatas: [{ brokerName: BROKER, brokerAddrs: { '0': '127.0.0.1:10911' } }],
      getAllSubscribeMessageQueue: (topic: string) =>
        this.queues.filter((mq) => mq.getTopic() === topic),
      getAllMessageQueue: (topic: string) =>
        this.queues.filter((mq) => mq.getTopic() === topic),
    };
  }

  async updateTopicRouteInfoFromNameServer(_topic: string, _force: boolean): Promise<void> {
    void _topic; void _force;
  }

  async sendHeartbeatToAllBrokers(): Promise<void> { this.heartbeatCalls++; }

  async getConsumerListByGroup(_addr: string, _group: string): Promise<any> {
    if (this.cids == null) return { code: 0, body: Buffer.alloc(0) };
    return { code: 0, body: Buffer.from(JSON.stringify({ consumerIdList: this.cids }), 'utf8') };
  }
}

// Wraps the real strategy so a test can prove it was (not) consulted.
class CountingStrategy implements AllocateMessageQueueStrategy {
  calls = 0;
  inner: AllocateMessageQueueStrategy;
  constructor(inner: AllocateMessageQueueStrategy) { this.inner = inner; }
  name(): string { return `counting(${this.inner.name()})`; }
  allocate(group: string, cid: string, mqAll: MessageQueue[], cidAll: string[]): MessageQueue[] {
    this.calls++;
    return this.inner.allocate(group, cid, mqAll, cidAll);
  }
}

type Change = { topic: string; mqAll: Set<string>; divided: Set<string> };

function consumer(mqAll: MessageQueue[], cids: string[] | null,
  opts: { model?: string; clientId?: string; listener?: (t: string, a: MessageQueue[], d: MessageQueue[]) => void;
    strategy?: AllocateMessageQueueStrategy } = {}): DefaultMQPullConsumer {
  const c = new DefaultMQPullConsumer('PG_PullReb');
  c.clientID = opts.clientId ?? 'cid-A';
  c.messageModel = opts.model ?? 'CLUSTERING';
  c.mqClient = new FakeClient(mqAll, cids) as any;
  c.started = true;
  c.subscribe(TOPIC, '*');
  if (opts.listener) c.setMessageQueueListener(opts.listener);
  if (opts.strategy) c.setAllocateMessageQueueStrategy(opts.strategy);
  return c;
}

function recorder(): { listener: (t: string, a: MessageQueue[], d: MessageQueue[]) => void; calls: Change[] } {
  const calls: Change[] = [];
  return {
    calls,
    listener: (topic, mqAll, mqDivided) => {
      calls.push({ topic, mqAll: setOf(mqAll), divided: setOf(mqDivided) });
    },
  };
}

async function main() {
  // ----------------------------------------------------------- switch defaults
  {
    const c = new DefaultMQPullConsumer('PG_PullReb');
    // Java DefaultMQPullConsumer.java:97 `private boolean enableRebalance = true`.
    check('enableRebalance defaults to Java\'s true', c.isEnableRebalance() === true);

    c.setEnableRebalance(false);
    check('setter round-trips off', c.isEnableRebalance() === false);
    c.setEnableRebalance(true);
    check('setter round-trips on', c.isEnableRebalance() === true);

    check('wait/min intervals default to 20000/1000',
      c.rebalanceWaitMillis === 20000 && c.rebalanceMinIntervalMillis === 1000,
      `${c.rebalanceWaitMillis}/${c.rebalanceMinIntervalMillis}`);
    c.setRebalanceWaitMillis(0);
    c.setRebalanceMinIntervalMillis(-5);
    check('a zero/negative period is clamped to 1ms, never a spin',
      c.rebalanceWaitMillis === 1 && c.rebalanceMinIntervalMillis === 1,
      `${c.rebalanceWaitMillis}/${c.rebalanceMinIntervalMillis}`);
  }

  // ------------------------------------------------------------ switch disabled
  {
    const c = consumer(queues(4), ['cid-A', 'cid-B']);
    const strategy = new CountingStrategy(c.allocateMessageQueueStrategy);
    c.setAllocateMessageQueueStrategy(strategy);
    c.setEnableRebalance(false);

    check('switch off reports balanced, not endless-unbalanced',
      (await c.doRebalance()) === true);
    check('switch off never runs an allocation', strategy.calls === 0, `calls=${strategy.calls}`);
    check('switch off caches nothing', c.getRebalanceTable().size === 0);
  }
  {
    const c = consumer(queues(4), ['cid-A', 'cid-B']);
    // A stale table must be ignored while the switch is off.
    (c as any).rebalanceTable.set(TOPIC, [new MessageQueue(TOPIC, BROKER, 3)]);
    c.setEnableRebalance(false);
    const got = await c.fetchMessageQueuesInBalance(TOPIC);
    check('switch off keeps fetch on the on-the-spot averaging',
      ids(got).join(',') === '0,1', ids(got).join(','));
  }

  // --------------------------------------------------------------- BROADCASTING
  {
    const rec = recorder();
    const c = consumer(queues(3), ['cid-A'], { model: 'BROADCASTING', listener: rec.listener });
    check('broadcast: first round reports unbalanced (empty table moved)',
      (await c.doRebalance()) === false);
    const all = [...rec.calls[0].mqAll].sort().join(',');
    const divided = [...rec.calls[0].divided].sort().join(',');
    check('broadcast: exactly one notification with all queues',
      rec.calls.length === 1 && rec.calls[0].topic === TOPIC
      && all === 'broker-a-0,broker-a-1,broker-a-2'
      && divided === 'broker-a-0,broker-a-1,broker-a-2',
      JSON.stringify(rec.calls));
  }
  {
    const rec = recorder();
    const c = consumer(queues(3), ['cid-A'], { model: 'BROADCASTING', listener: rec.listener });
    await c.doRebalance();
    check('broadcast: a second round is balanced', (await c.doRebalance()) === true);
    check('broadcast: nothing changed -> no second notification', rec.calls.length === 1,
      `calls=${rec.calls.length}`);
  }

  // ----------------------------------------------------------------- CLUSTERING
  {
    const c = consumer(queues(4), ['cid-A', 'cid-B']);
    await c.doRebalance();
    check('clustering: this instance takes the first half (AVG, sorted cids)',
      ids(c.getRebalanceTable().get(TOPIC)!).join(',') === '0,1',
      ids(c.getRebalanceTable().get(TOPIC)!).join(','));
  }
  {
    const a = consumer(queues(5), ['cid-A', 'cid-B'], { clientId: 'cid-A' });
    const b = consumer(queues(5), ['cid-A', 'cid-B'], { clientId: 'cid-B' });
    await a.doRebalance();
    await b.doRebalance();
    const sa = setOf(a.getRebalanceTable().get(TOPIC)!);
    const sb = setOf(b.getRebalanceTable().get(TOPIC)!);
    const overlap = [...sa].filter((x) => sb.has(x));
    const union = [...new Set([...sa, ...sb])].sort();
    check('clustering: two instances never own the same queue', overlap.length === 0,
      overlap.join(','));
    check('clustering: together they cover every queue',
      union.join(',') === 'broker-a-0,broker-a-1,broker-a-2,broker-a-3,broker-a-4',
      union.join(','));
  }
  {
    const rec = recorder();
    const c = consumer(queues(2), ['cid-A'], { listener: rec.listener });
    await c.doRebalance();
    const before = rec.calls.length;
    (c.mqClient as any).queues = queues(3); // a queue was added
    const balanced = await c.doRebalance();
    check('clustering: a new queue reports unbalanced', balanced === false);
    check('clustering: a new queue notifies exactly once more', rec.calls.length === before + 1,
      `${before} -> ${rec.calls.length}`);
    const last = rec.calls[rec.calls.length - 1];
    check('clustering: the change carries the full and the divided set',
      [...last.mqAll].sort().join(',') === 'broker-a-0,broker-a-1,broker-a-2'
      && [...last.divided].sort().join(',') === 'broker-a-0,broker-a-1,broker-a-2',
      JSON.stringify(last));
  }
  {
    const rec = recorder();
    const c = consumer(queues(3), null, { listener: rec.listener });
    // Java (:300-302) only warns; it does not notify and leaves balanced at true.
    check('clustering: a missing consumer list is balanced', (await c.doRebalance()) === true);
    check('clustering: a missing consumer list notifies nothing', rec.calls.length === 0);
    check('clustering: a missing consumer list caches nothing', c.getRebalanceTable().size === 0);
  }
  {
    // Java's updateProcessQueueTableInRebalance counts only table additions/removals as a
    // change: "nothing -> nothing" is no change, so an empty opening share fires no callback
    // and the round counts as balanced (otherwise the loop hammers minInterval).
    class EmptyStrategy implements AllocateMessageQueueStrategy {
      name(): string { return 'empty'; }
      allocate(_group: string, _cid: string,
        _mqAll: MessageQueue[], _cidAll: string[]): MessageQueue[] { return []; }
    }
    const rec = recorder();
    const c = consumer(queues(3), ['cid-A'],
      { listener: rec.listener, strategy: new EmptyStrategy() });
    check('an empty opening share is balanced', (await c.doRebalance()) === true);
    check('an empty opening share notifies nothing', rec.calls.length === 0);
    check('an empty opening share keeps the topic in the table with an empty list',
      c.getRebalanceTable().get(TOPIC) !== undefined
      && c.getRebalanceTable().get(TOPIC)!.length === 0);
    // The share appearing later IS a change (the table grew from nothing).
    c.setAllocateMessageQueueStrategy(new AllocateMessageQueueAveragely());
    check('the share appearing later reports unbalanced', (await c.doRebalance()) === false);
    check('the share appearing later notifies exactly once', rec.calls.length === 1);
  }
  {
    const rec = recorder();
    const c = consumer(queues(2), ['cid-A'], { listener: rec.listener });
    await c.doRebalance();
    const before = rec.calls.length;
    (c.mqClient as any).queues = []; // the topic disappeared from the route
    // Java (:286-294) notifies with two empty sets then `break`s: balanced stays TRUE.
    check('a vanished topic is not "unbalanced"', (await c.doRebalance()) === true);
    check('a vanished topic clears the table',
      c.getRebalanceTable().get(TOPIC)!.length === 0);
    const last = rec.calls[rec.calls.length - 1];
    check('a vanished topic notifies exactly once with two empty sets',
      rec.calls.length === before + 1 && last.mqAll.size === 0 && last.divided.size === 0,
      JSON.stringify(last));
  }

  // ------------------------------------------------------------- listener safety
  {
    const c = consumer(queues(2), ['cid-A'], {
      listener: () => { throw new Error('listener exploded'); },
    });
    // Java swallows it (RebalancePullImpl:49-51): the call must simply return.
    check('a throwing listener does not escape', (await c.doRebalance()) === false);
    check('a throwing listener does not stop the table update',
      c.getRebalanceTable().get(TOPIC)!.length === 2);
  }
  {
    const rec = recorder();
    const c = consumer(queues(3), ['cid-A'], { listener: rec.listener });
    await c.doRebalance();
    const before = rec.calls.length;
    (c.mqClient as any).queues = [...queues(3)].reverse();
    check('the same set in another order is balanced', (await c.doRebalance()) === true);
    check('the same set in another order does not notify', rec.calls.length === before,
      `${before} -> ${rec.calls.length}`);
  }

  // ------------------------------------- fetchMessageQueuesInBalance uses the table
  {
    const c = consumer(queues(4), ['cid-A', 'cid-B']);
    (c as any).rebalanceTable.set(TOPIC, [new MessageQueue(TOPIC, BROKER, 3)]);
    const got = await c.fetchMessageQueuesInBalance(TOPIC);
    check('the rebalance table beats the on-the-spot computation',
      ids(got).join(',') === '3', ids(got).join(','));
  }
  {
    const c = consumer(queues(4), ['cid-A']);
    (c as any).rebalanceTable.set(TOPIC, [
      new MessageQueue(TOPIC, 'broker-b', 1), new MessageQueue(TOPIC, 'broker-a', 2),
    ]);
    const got = await c.fetchMessageQueuesInBalance(TOPIC);
    check('the table answer is sorted',
      identity(got).join(',') === 'broker-a-2,broker-b-1', identity(got).join(','));
    got.length = 0;
    check('callers cannot mutate the table through the answer',
      c.getRebalanceTable().get(TOPIC)!.length === 2);
  }
  {
    const c = consumer(queues(2), ['cid-A']);
    await c.doRebalance();
    const snapshot = c.getRebalanceTable();
    snapshot.get(TOPIC)!.length = 0;
    check('getRebalanceTable returns a copy',
      c.getRebalanceTable().get(TOPIC)!.length === 2);
  }
  {
    const c = consumer(queues(4), ['cid-A', 'cid-B']);
    check('an unfilled topic starts with an empty table', c.getRebalanceTable().size === 0);
    const got = await c.fetchMessageQueuesInBalance(TOPIC);
    check('an unfilled topic still computes on the spot',
      ids(got).join(',') === '0,1', ids(got).join(','));
    check('the on-the-spot answer does not fill the table', c.getRebalanceTable().size === 0);
  }

  // --------------------------------------- registerMessageQueueListener registers
  {
    const c = new DefaultMQPullConsumer('PG_PullReb');
    const rec = recorder();
    c.registerMessageQueueListener(TOPIC, rec.listener);
    check('registerMessageQueueListener registers the topic',
      c.getRegisterTopics().has(TOPIC));
    check('registerMessageQueueListener installs the listener',
      (c as any).messageQueueListener === rec.listener);
  }
  {
    const c = new DefaultMQPullConsumer('PG_PullReb');
    const rec = recorder();
    c.setMessageQueueListener(rec.listener);
    c.registerMessageQueueListener(TOPIC, null);
    check('a null listener still registers the topic', c.getRegisterTopics().has(TOPIC));
    check('a null listener keeps the current one',
      (c as any).messageQueueListener === rec.listener);
  }
  {
    const c = new DefaultMQPullConsumer('PG_PullReb');
    const rec = recorder();
    c.registerMessageQueueListener('   ', rec.listener);
    check('a blank topic is ignored entirely',
      c.getRegisterTopics().size === 0 && (c as any).messageQueueListener === null);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
