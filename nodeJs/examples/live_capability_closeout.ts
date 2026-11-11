// live_capability_closeout is the Node.js live counterpart of the offline
// capability_closeout suite: the capabilities added in the 2026-11-11 round,
// asserted against a real RocketMQ 5.x broker.
//
//   node --experimental-strip-types examples/live_capability_closeout.ts --ns 127.0.0.1:9876
//
// L1 ConsumeFromWhere's three "deprecated" values. Java folds
//   CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST / MIN_OFFSET / MAX_OFFSET
//   into the CONSUME_FROM_LAST_OFFSET branch of
//   RebalancePushImpl#computePullFromWhereWithException (lines 171-173), so all four
//   must start at the same place. The LAST_OFFSET baseline is MEASURED on this end,
//   not assumed: on a fresh topic the broker answers QUERY_CONSUMER_OFFSET with
//   SUCCESS offset=0 for a never-committed group (its V3_0_6 path) unless the client
//   sends setZeroIfNotFound=false, and Java sends no such field. Hardcoding either
//   outcome would turn a parity question into a spurious failure.
// L2 LitePull pull-from-which-node pin. pullAPI.recalculatePullFromWhichNode is
//   exactly what the pull path puts on the request (pull_api.ts resolves the broker
//   address with it), so reading it after start is a request-level assertion, not a
//   behavioural guess -- the fallback in findAddrInSubscribe(..., onlyThisBroker=false)
//   makes delivery identical whether or not the pin is honoured.
// L3 adminExtGroup identity: a custom group must still boot and serve an RPC.
// L4 awaitTerminationMillisWhenShutdown (Node-only knob): default 30000, and a small
//   budget must really shorten shutdown() instead of being stored and ignored.
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { Message, MessageQueue } from '../src/common/message.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = String(Date.now());
const PREFIX = `CapLive_${STAMP}`;
const N_MSG = 8;

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// pushLeg starts one push consumer at `where` and records arrivals for `window`.
// Every leg gets a fresh group: a group that already committed offsets reads them
// back whatever consumeFromWhere says.
async function pushLeg(topic: string, group: string, where: string, windowMs: number): Promise<number> {
  let got = 0;
  const c = new DefaultMQPushConsumer(group);
  c.setNamesrvAddr(NS);
  c.setConsumeFromWhere(where);
  c.subscribe(topic, '*');
  c.setConsumeThreadNums(1);
  c.registerMessageListenerConcurrently((msgs: any[]) => {
    got += msgs.length;
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await c.start();
  await sleep(windowMs);
  await c.shutdown();
  return got;
}

// liteLeg runs one LitePull subscribe-mode leg with the given pin.
async function liteLeg(topic: string, group: string, pin: boolean, brokerId: number,
                       want: number, windowMs: number): Promise<{ got: number; resolved: number | null; apiPin: boolean | null }> {
  const c = new DefaultLitePullConsumer(group);
  c.setNamesrvAddr(NS);
  c.subscribe(topic, '*');
  c.setConsumeFromWhere('CONSUME_FROM_FIRST_OFFSET');
  c.setConnectBrokerByUser(pin);
  c.setDefaultBrokerId(brokerId);
  await c.start();
  // Read the value the pull path would put on the request. With the pin off the
  // table has no suggestion for this queue yet, so MASTER_ID is the answer.
  const probeMq = new MessageQueue(topic, 'broker-a', 0);
  const apiPin = c.pullAPI ? c.pullAPI.connectBrokerByUser : null;
  const resolved = c.pullAPI ? c.pullAPI.recalculatePullFromWhichNode(probeMq) : null;
  let got = 0;
  const deadline = Date.now() + windowMs;
  while (Date.now() < deadline && got < want) {
    const msgs = await c.poll(1000);
    got += msgs.length;
  }
  await c.shutdown();
  return { got, resolved, apiPin };
}

async function main(): Promise<void> {
  const producer = new DefaultMQProducer(PREFIX + '_pg');
  producer.setNamesrvAddr(NS);
  producer.start();

  const createTopic = async (topic: string): Promise<void> => {
    await producer.createTopic(topic, producer.createTopicKey, 1);
    await sleep(3000);
  };
  const sendAll = async (topic: string): Promise<void> => {
    for (let i = 0; i < N_MSG; i++) {
      await producer.send(new Message(topic, Buffer.from(`cap-${String(i).padStart(2, '0')}`), 'TagCap', `k${i}`));
    }
  };

  // ---------------------------------------------------------------- L1
  console.log('\nL1 ConsumeFromWhere: the three deprecated values fold into LAST_OFFSET');
  const t1 = PREFIX + '_FromWhere';
  await createTopic(t1);
  await sendAll(t1);
  console.log(`  sent ${N_MSG} messages, no consumer has ever joined this topic`);

  const window = 12000;
  const nFirst = await pushLeg(t1, PREFIX + '_gFirst', 'CONSUME_FROM_FIRST_OFFSET', window);
  check('L1-positive control: FIRST_OFFSET delivers the whole backlog',
    nFirst === N_MSG, `received=${nFirst} expect=${N_MSG}`);

  // The four LAST-family values are measured, never compared to a constant, and **not**
  // required to equal each other. Delivery counts cannot discriminate the branch: the broker
  // answers QUERY_CONSUMER_OFFSET for a never-committed group with SUCCESS offset=0 on its
  // V3_0_6 path (minOffset<=0 and offset 0 still in memory) and with QUERY_NOT_FOUND
  // otherwise, and that answer flips between legs. Measured 2026-11-11 with the JAVA
  // reference client on one backlog in one process: LAST_OFFSET=0 while MIN_OFFSET=8 and
  // MAX_OFFSET=8. So this leg asserts what it can honestly assert -- every value is
  // accepted and the consumer starts -- and prints the counts as the parity record. The
  // "folds into the LAST_OFFSET branch" claim is pinned by the offline suite.
  const counts: Record<string, number> = {};
  for (const leg of [
    ['LAST_OFFSET', 'CONSUME_FROM_LAST_OFFSET'],
    ['MIN_OFFSET', 'CONSUME_FROM_MIN_OFFSET'],
    ['MAX_OFFSET', 'CONSUME_FROM_MAX_OFFSET'],
    ['LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST', 'CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST'],
  ]) {
    const n = await pushLeg(t1, PREFIX + '_g' + leg[0], leg[1], window);
    counts[leg[0]] = n;
    check(`L1-${leg[0]} accepted: the consumer started and received a sane count`,
      n >= 0 && n <= N_MSG, `received=${n} of ${N_MSG}`);
  }
  console.log(`  INFO LAST-family delivery counts on this end: ${JSON.stringify(counts)} `
    + '(not a parity oracle: the broker V3_0_6 offset answer is timing-dependent)');

  // ---------------------------------------------------------------- L2
  console.log('\nL2 LitePull pin (connectBrokerByUser / defaultBrokerId)');
  const t2 = PREFIX + '_Pin';
  await createTopic(t2);
  await sendAll(t2);
  console.log(`  sent ${N_MSG} messages to ${t2}`);

  const off = await liteLeg(t2, PREFIX + '_pinOff', false, 0, N_MSG, 20000);
  check('L2-pin off: pullAPI reports the switch off', off.apiPin === false, `apiPin=${String(off.apiPin)}`);
  check('L2-pin off: the request resolves to MASTER_ID', off.resolved === 0, `resolved=${String(off.resolved)}`);
  check('L2-pin off: delivers the backlog', off.got === N_MSG, `got=${off.got} expect=${N_MSG}`);

  const master = await liteLeg(t2, PREFIX + '_pinMaster', true, 0, N_MSG, 20000);
  check('L2-pin to the master (0): pullAPI reports pin on and the request resolves to 0',
    master.apiPin === true && master.resolved === 0, `apiPin=${String(master.apiPin)} resolved=${String(master.resolved)}`);
  check('L2-pin to the master (0): delivers the backlog', master.got === N_MSG,
    `got=${master.got} expect=${N_MSG}`);

  const slave = await liteLeg(t2, PREFIX + '_pinSlave', true, 1, N_MSG, 12000);
  check('L2-pin to a non-existent slave (1): the request resolves to 1',
    slave.apiPin === true && slave.resolved === 1, `apiPin=${String(slave.apiPin)} resolved=${String(slave.resolved)}`);
  check('L2-pin to a non-existent slave (1): findAddrInSubscribe(onlyThisBroker=false) falls back, '
    + 'so delivery still happens (Java parity)', slave.got === N_MSG, `got=${slave.got} expect=${N_MSG}`);

  // ---------------------------------------------------------------- L3
  console.log('\nL3 adminExtGroup (identity only, no wire effect)');
  const admin = new DefaultMQAdminExt();
  admin.setNamesrvAddr(NS);
  check('L3-default adminExtGroup is admin_ext_group', admin.getAdminExtGroup() === 'admin_ext_group',
    `value=${admin.getAdminExtGroup()}`);
  admin.setAdminExtGroup('cap_closeout_admin_' + PREFIX);
  check('L3-override takes', admin.getAdminExtGroup() === 'cap_closeout_admin_' + PREFIX,
    `value=${admin.getAdminExtGroup()}`);
  await (admin.start() as any);
  let found = false;
  let topicCount = 0;
  try {
    const list: any = await admin.fetchAllTopicList();
    const names: string[] = list?.topicList || [];
    topicCount = names.length;
    found = names.includes(t2);
  } finally {
    await (admin.shutdown() as any);
  }
  check('L3-a custom-group admin still boots and serves an RPC on the live cluster', found,
    `topics=${topicCount}`);

  // ---------------------------------------------------------------- L4
  console.log('\nL4 awaitTerminationMillisWhenShutdown (Node-only)');
  const probe = new DefaultMQPushConsumer(PREFIX + '_gAwait');
  check('L4-default budget is 30000 (Java 0 is a deliberate deviation)',
    probe.getAwaitTerminationMillisWhenShutdown() === 30000,
    `value=${probe.getAwaitTerminationMillisWhenShutdown()}`);
  probe.setAwaitTerminationMillisWhenShutdown(300);
  check('L4-setter takes effect', probe.getAwaitTerminationMillisWhenShutdown() === 300,
    `value=${probe.getAwaitTerminationMillisWhenShutdown()}`);

  const slow = new DefaultMQPushConsumer(PREFIX + '_gAwaitSlow');
  slow.setNamesrvAddr(NS);
  slow.subscribe(t2, '*');
  slow.registerMessageListenerConcurrently(() => ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
  slow.setAwaitTerminationMillisWhenShutdown(300);
  await slow.start();
  const began = Date.now();
  await slow.shutdown();
  const elapsed = Date.now() - began;
  check('L4-shutdown honours the small budget instead of stalling for the default',
    elapsed < 5000, `elapsed=${elapsed}ms budget=300ms`);

  producer.shutdown();
  console.log(`\n${PREFIX}: ${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
