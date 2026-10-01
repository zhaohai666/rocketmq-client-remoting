// live_pop is the Node.js POP-consumption end-to-end smoke against a real
// RocketMQ 5.5 cluster — the live counterpart of test/pop_smoke.ts. It
// verifies the three things an in-process fake broker cannot:
//   1. SET_MESSAGE_REQUEST_MODE(401) is accepted by the broker and the pop
//      loops really come up (popQueueTable grows past zero),
//   2. happy path: every popped message is ACKed exactly once at the broker,
//   3. the NACK path: a message the listener rejects comes back after the
//      first delay notch (broker redelivery), and ACK debt settles to 0.
//
//   node --experimental-strip-types examples/live_pop.ts --ns 127.0.0.1:9876
//
// Cross-port rules honoured here: consumer FIRST, then send (see
// live_consumer.ts rule #8); accounting by BODY, never by topic.
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { Message } from '../src/common/message.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodePop_${STAMP}`);
const GROUP = argOf('--group', `GID_NodePop_${STAMP}`);
const TOTAL = parseInt(argOf('--total', '20'), 10);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  const producer = new DefaultMQProducer('GID_NodePop_Producer');
  producer.setNamesrvAddr(NS);
  producer.start();
  await producer.createTopic(TOPIC, producer.createTopicKey, 4);
  await sleep(3000);

  // ---- 1. POP consumer FIRST (rule #8), then send ----
  const received = new Map<string, number>();
  const nackSeen = new Map<string, number>(); // body -> consume attempts
  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.subscribe(TOPIC, '*');
  consumer.setConsumeThreadNums(4);
  consumer.setPopMode(true);
  consumer.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) {
      const body = (m.getBody() || Buffer.alloc(0)).toString('utf8');
      if (body.startsWith('nack-')) {
        nackSeen.set(body, (nackSeen.get(body) || 0) + 1);
        // first sight: reject -> broker must redeliver after the delay notch
        if ((nackSeen.get(body) || 0) === 1) {
          return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
      }
      received.set(body, (received.get(body) || 0) + 1);
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await consumer.start();
  console.log(`POP consumer started group=${GROUP} topic=${TOPIC}`);
  // 401 goes out during start(); the pop loops come up on the first rebalance.
  const popDeadline = Date.now() + 60_000;
  while (Date.now() < popDeadline && consumer.popQueueTable.size === 0) await sleep(1000);
  check('POP mode active (pop loops up via 401 + rebalance)', consumer.popQueueTable.size > 0,
    `popQueues=${consumer.popQueueTable.size}`);

  // ---- 2. send happy-path batch + one NACK marker ----
  for (let i = 0; i < TOTAL; i++) {
    await producer.send(new Message(TOPIC, Buffer.from(`p-${i}`), 'TagP', `k${i}`));
  }
  await producer.send(new Message(TOPIC, Buffer.from('nack-0'), 'TagN', 'kn'));
  console.log(`sent ${TOTAL} + 1 NACK marker`);

  // ---- 3. wait: all TOTAL delivered once, NACK marker delivered TWICE ----
  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline) {
    if (received.size >= TOTAL + 1 && (nackSeen.get('nack-0') || 0) >= 2) break;
    await sleep(500);
  }
  const missing: number[] = [];
  for (let i = 0; i < TOTAL; i++) {
    if ((received.get(`p-${i}`) || 0) !== 1) missing.push(i);
  }
  check(`all ${TOTAL} messages delivered exactly once`, missing.length === 0,
    missing.length ? `missing/dup: ${missing.join(',')}` : '');
  check('NACK marker redelivered by broker (invisible-time path)',
    (nackSeen.get('nack-0') || 0) >= 2, `attempts=${nackSeen.get('nack-0') || 0}`);
  check('marker finally ACKed (exactly once after retry)',
    (received.get('nack-0') || 0) === 1, `delivered=${received.get('nack-0') || 0}`);

  // ---- 4. ACK debt settles to zero (every pop was acknowledged) ----
  await sleep(3000); // let trailing acks land
  let debt = 0;
  for (const pq of consumer.popQueueTable.values()) debt += pq.waitAckMsgCount();
  check('ACK debt settles to 0 (no leaked checkpoints)', debt === 0, `debt=${debt}`);

  await consumer.shutdown();
  producer.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
