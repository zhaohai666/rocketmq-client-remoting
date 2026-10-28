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
//
// `started` is set directly where a case needs "already running": a real start would
// need a cluster, and none of these guards reads the wire.
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { MessageQueue } from '../src/common/message.ts';

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

  console.log(`\n${pass} passed, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
