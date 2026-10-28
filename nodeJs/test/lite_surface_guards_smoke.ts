// DefaultLitePullConsumer misuse guards - offline, nothing reaches the wire.
//
// Run: node --experimental-strip-types test/lite_surface_guards_smoke.ts
//
// The lite surface answers misuse with an error instead of a plausible-looking
// result:
//
//   * assign() with no queue is refused. Taken literally it would drop every queue
//     held so far, which is exactly what a route lookup that came back empty does by
//     accident; a refused call also leaves the mode and the assignment alone.
//   * subscribe() with a blank topic is refused rather than registered as a topic
//     named "".
//   * poll() and committed() refuse a consumer that never started. An empty poll and
//     an unknown committed offset are exactly what a queue sitting at its tail looks
//     like, which is how a missing start() stays hidden forever. Once running, a
//     negative poll timeout is refused too - and the state check comes first, so an
//     unstarted consumer is reported as that rather than as a timeout problem.
//   * fetchMessageQueues() refuses before start for the same reason: an empty list
//     would read as "this topic has no queues".
//   * subscribe() and assign() are mutually exclusive: the first one that claims the
//     consumer decides how its queues are chosen, and the other is refused.
//   * assign() holds a queue once (the list is a set) and drops every piece of
//     per-queue state of a queue it no longer names.
//   * seek() refuses a queue this consumer does not hold, a stopped consumer, and an
//     offset outside the queue's own [minOffset, maxOffset] range.
//   * committed() reads the broker, and a FAILED read is an error — never the -1 that
//     means "nobody has committed".
//
// `started` is set directly where a case needs "already running": a real start would
// need a cluster, and none of these guards reads the wire.
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { MessageQueue } from '../src/common/message.ts';
import { mqKey, ReadOffsetMode, RemoteBrokerOffsetStore } from '../src/client/offset_store.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

function labels(c: DefaultLitePullConsumer): string[] {
  return c.assignment().map((q) => `${q.getBrokerName()}@${q.getTopic()}-${q.getQueueId()}`);
}

/** Runs $call and reports whether it rejected/threw with a message containing $needle. */
async function checkThrows(needle: string, call: () => unknown, name: string): Promise<void> {
  let message = '';
  let threw = false;
  try {
    await call();
  } catch (e) {
    threw = true;
    message = e instanceof Error ? e.message : String(e);
  }
  check(name, threw && message.includes(needle), `got: ${message}`);
}

function consumer(): DefaultLitePullConsumer {
  const c = new DefaultLitePullConsumer('GID_lite_guards');
  c.setNamesrvAddr('127.0.0.1:9876');
  c.setAutoCommit(false);
  return c;
}

function markStarted(c: DefaultLitePullConsumer): void {
  // poll() answers as "running" only once the pull facade exists; a real start
  // builds it from the client instance. The guard under test reads the timeout
  // before anything touches it, so an empty stand-in is enough.
  (c as unknown as { started: boolean }).started = true;
  (c as unknown as { pullAPI: unknown }).pullAPI = {};
}

async function main(): Promise<void> {
  // ------------------------------- 1. assign
  {
    const c = consumer();
    const mq = new MessageQueue('GuardTopic', 'broker-a', 0);
    await checkThrows('can not be null or empty', () => c.assign([]),
      'assign([]) is refused instead of dropping every held queue');
    await checkThrows('can not be null or empty', () => c.assign(undefined as unknown as MessageQueue[]),
      'assign(undefined) is refused too');
    c.assign([mq]);
    check('a non-empty assign is accepted', labels(c).join(',') === 'broker-a@GuardTopic-0',
      labels(c).join(','));
    await checkThrows('can not be null or empty', () => c.assign([]),
      'assign([]) is still refused once queues are held');
    check('a refused assign leaves the assignment alone',
      labels(c).join(',') === 'broker-a@GuardTopic-0', labels(c).join(','));
  }

  // ------------------------------- 2. subscribe
  {
    const c = consumer();
    await checkThrows('Topic can not be null or empty', () => c.subscribe(''),
      'subscribe("") is refused');
    await checkThrows('Topic can not be null or empty', () => c.subscribe('   '),
      'subscribe("   ") is refused');
    const sub = (c as unknown as { subscription: Map<string, unknown> }).subscription;
    check('a refused subscribe registers nothing', sub.size === 0, String(sub.size));
    c.subscribe('GuardTopic', 'TagA');
    check('a real subscribe is accepted', sub.has('GuardTopic'), [...sub.keys()].join(','));
  }

  // ------------------------------- 3. poll / committed / fetchMessageQueues
  {
    const c = consumer();
    const mq = new MessageQueue('GuardTopic', 'broker-a', 0);
    await checkThrows('The consumer not running, please start it first', () => c.poll(),
      'poll() on a consumer that never started refuses');
    await checkThrows('not running', () => c.poll(-1),
      'the state check comes before the timeout check');
    await checkThrows('not running', () => c.committed(mq),
      'committed() on a consumer that never started refuses');
    await checkThrows('not running', () => c.fetchMessageQueues('GuardTopic'),
      'fetchMessageQueues() on a consumer that never started refuses');

    markStarted(c);
    await checkThrows('Timeout must not be negative', () => c.poll(-1),
      'a running consumer still refuses a negative timeout');
  }

  // ------------------------------- 4. subscribe / assign are mutually exclusive
  {
    const c = consumer();
    c.subscribe('ModeTopic', '*');
    c.subscribe('ModeTopic2', 'TagA');
    await checkThrows('Subscribe and assign are mutually exclusive',
      () => c.assign([new MessageQueue('ModeTopic', 'broker-a', 0)]),
      'assign() after subscribe() is refused');
    await checkThrows('Subscribe and assign are mutually exclusive',
      () => c.setSubExpressionForAssign('ModeTopic', 'TagA'),
      'setSubExpressionForAssign() after subscribe() is refused');
    check('a refused assign leaves subscribe mode untouched',
      c.assignment().length === 0 && c.subscriptionDataSet.length === 2,
      `${c.assignment().length}/${c.subscriptionDataSet.length}`);

    const a = consumer();
    a.setSubExpressionForAssign('ModeTopic', 'TagA');
    a.assign([new MessageQueue('ModeTopic', 'broker-a', 0)]);
    await checkThrows('Subscribe and assign are mutually exclusive',
      () => a.subscribe('ModeTopic', '*'), 'subscribe() after assign() is refused');
    check('assign mode advertises no subscription at all',
      a.subscriptionDataSet.length === 0, String(a.subscriptionDataSet.length));
    check('the assigned topic keeps its own expression',
      (a as unknown as { assignSubscription: Map<string, { subString: string }> })
        .assignSubscription.get('ModeTopic')?.subString === 'TagA');
  }

  // ------------------------------- 5. setSubExpressionForAssign
  {
    const c = consumer();
    await checkThrows('subExpression can not be null or empty',
      () => c.setSubExpressionForAssign('ExprTopic', '   '),
      'a blank assign expression is refused');
    c.setSubExpressionForAssign('ExprTopic', 'TagA');
    check('a pre-start assign expression is recorded',
      (c as unknown as { assignSubscription: Map<string, { subString: string }> })
        .assignSubscription.get('ExprTopic')?.subString === 'TagA');
    markStarted(c);
    await checkThrows('only can be called before start',
      () => c.setSubExpressionForAssign('ExprTopic', 'TagB'),
      'a running consumer refuses a new assign expression');
    check('the refused call changed nothing',
      (c as unknown as { assignSubscription: Map<string, { subString: string }> })
        .assignSubscription.get('ExprTopic')?.subString === 'TagA');
  }

  // ------------------------------- 6. assign dedupes and drops revoked state
  {
    const c = consumer();
    const a = new MessageQueue('RevokeTopic', 'broker-a', 0);
    const b = new MessageQueue('RevokeTopic', 'broker-a', 1);
    c.assign([a, a, b, a]);
    check('a queue named twice is held once',
      labels(c).join(',') === 'broker-a@RevokeTopic-0,broker-a@RevokeTopic-1', labels(c).join(','));
    const st = c as unknown as {
      pullCursor: Map<string, number>; paused: Set<string>;
    };
    st.pullCursor.set(mqKey(a), 5);
    st.pullCursor.set(mqKey(b), 6);
    c.setPullParkUntil(a, Date.now() + 60000);
    c.pause([a]);
    c.assign([b]);
    check('a revoked queue loses its cursor and its sibling keeps its own',
      !st.pullCursor.has(mqKey(a)) && st.pullCursor.get(mqKey(b)) === 6);
    check('a revoked queue loses its armed park', c.getPullParkUntil(a) === 0);
    check('a revoked queue is no longer paused', !st.paused.has(mqKey(a)));

    // unsubscribe() drops the topic's queues at once instead of waiting for the
    // next rebalance, and their state goes with them.
    c.assign([a, b]);
    st.pullCursor.set(mqKey(b), 9);
    c.unsubscribe('RevokeTopic');
    check('unsubscribe() revokes the topic\'s queues', c.assignment().length === 0,
      String(c.assignment().length));
    check('unsubscribe() drops their cursor too', !st.pullCursor.has(mqKey(b)));
  }

  // ------------------------------- 7. seek
  // The bounds are broker RPCs; this client answers them so the range guard can be
  // watched without a cluster.
  const boundsClient = (min: number, max: number) => ({
    updateTopicRouteInfoFromNameServer: async () => true,
    getTopicRouteData: () => ({
      brokerDatas: [{ brokerName: 'broker-a', brokerAddrs: { '0': '127.0.0.1:10911' } }],
    }),
    getMinOffset: async () => min,
    getMaxOffset: async () => max,
  });
  {
    const held = new MessageQueue('SeekTopic', 'broker-a', 0);

    const s = consumer();
    s.subscribe('SeekTopic', '*');
    await checkThrows('not in assigned list, may be rebalancing, message queue: MessageQueue [topic=SeekTopic',
      () => s.seek(held, 0),
      'subscribe mode: a queue outside the assignment is refused as possibly rebalancing');

    const a = consumer();
    a.assign([held]);
    await checkThrows('not in assigned list, message queue: MessageQueue [topic=SeekTopic, brokerName=broker-a, queueId=1]',
      () => a.seek(new MessageQueue('SeekTopic', 'broker-a', 1), 0),
      'assign mode: a stranger is refused without the rebalancing hint');
    await checkThrows('not running', () => a.seek(held, 0),
      'a held queue on a stopped consumer is refused as not running');
    check('a refused seek leaves no cursor', (a as unknown as { pullCursor: Map<string, number> }).pullCursor.size === 0);

    markStarted(a);
    (a as unknown as { mqClient: unknown }).mqClient = boundsClient(10, 20);
    await checkThrows('Seek offset illegal, seek offset = 9, min offset = 10, max offset = 20',
      () => a.seek(held, 9), 'an offset below minOffset is refused with all three numbers');
    await checkThrows('Seek offset illegal, seek offset = 21, min offset = 10, max offset = 20',
      () => a.seek(held, 21), 'an offset above maxOffset is refused too');
    check('refused seeks left no cursor',
      (a as unknown as { pullCursor: Map<string, number> }).pullCursor.size === 0);

    a.setPullParkUntil(held, Date.now() + 60000);
    await a.seek(held, 10);
    check('the range boundary is legal',
      (a as unknown as { pullCursor: Map<string, number> }).pullCursor.get(mqKey(held)) === 10);
    check('an explicit seek lifts the leftover throttle park', a.getPullParkUntil(held) === 0);
    await a.seek(held, 20);
    check('maxOffset is legal too',
      (a as unknown as { pullCursor: Map<string, number> }).pullCursor.get(mqKey(held)) === 20);

    (a as unknown as { mqClient: unknown }).mqClient = boundsClient(-1, 20);
    await checkThrows('Can not get min offset', () => a.seek(held, 10),
      'an unanswered minOffset refuses the seek instead of pinning -1');
    check('the refused seek left the cursor where it was',
      (a as unknown as { pullCursor: Map<string, number> }).pullCursor.get(mqKey(held)) === 20);
  }

  // ------------------------------- 8. committed is broker-backed
  {
    const c = consumer();
    markStarted(c);
    const mq = new MessageQueue('CommitTopic', 'broker-a', 0);
    const store = {
      readOffset: async () => -1,
      updateOffset() { /* no-op */ },
      persist: async () => undefined,
      persistAll: async () => undefined,
      load: async () => undefined,
    };
    (c as unknown as { offsetStore: unknown }).offsetStore = store;
    check('an unknown committed offset stays -1', (await c.committed(mq)) === -1);
    store.readOffset = async () => 42;
    check('the broker answer is reported as-is', (await c.committed(mq)) === 42);
    store.readOffset = async () => -2;
    await checkThrows('Fetch consume offset from broker exception', () => c.committed(mq),
      'a failed broker read is an error, not "never committed"');

    const failing = new RemoteBrokerOffsetStore({
      findBrokerAddrForQueue: () => '127.0.0.1:10911',
      queryConsumerOffset: async () => { throw new Error('boom'); },
    }, 'GID_lite_guards');
    check('a failed query reads as -2, not -1',
      (await failing.readOffset(mq, ReadOffsetMode.READ_FROM_STORE)) === -2);
    const lost = new RemoteBrokerOffsetStore({ findBrokerAddrForQueue: () => null }, 'GID_lite_guards');
    check('a queue whose broker is unknown reads as -2',
      (await lost.readOffset(mq, ReadOffsetMode.READ_FROM_STORE)) === -2);
    const fresh = new RemoteBrokerOffsetStore({
      findBrokerAddrForQueue: () => '127.0.0.1:10911',
      queryConsumerOffset: async () => ({ found: false, offset: -1 }),
    }, 'GID_lite_guards');
    check('QUERY_NOT_FOUND reads as -1',
      (await fresh.readOffset(mq, ReadOffsetMode.READ_FROM_STORE)) === -1);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
