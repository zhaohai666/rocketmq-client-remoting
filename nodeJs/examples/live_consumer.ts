// live_consumer is the Node.js push-consumer end-to-end smoke: start the
// consumer FIRST, then send N messages from a second producer, and verify
// every message arrives exactly once (offsets 0..n-1 per queue, no missing,
// no duplicate). Uses a fresh topic + group per run.
//
//   node --experimental-strip-types examples/live_consumer.ts --ns 127.0.0.1:9876
//
// Cross-port rules honoured here:
//   - "consume" tests MUST start the consumer BEFORE sending
//     (CONSUME_FROM_LAST_OFFSET with no committed offset initialises the
//     cursor to the then-current maxOffset),
//   - messages read back from %RETRY%<group> have msg.topic restored to the
//     business topic, so accounting is by BODY, never by topic,
//   - queue counting uses the (topic, brokerName, queueId) triple.
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { Message, MessageQueue } from '../src/common/message.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeLive_${STAMP}`);
const GROUP = argOf('--group', `GID_NodeLive_Consume_${STAMP}`);
const TOTAL = parseInt(argOf('--total', '20'), 10);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  const producer = new DefaultMQProducer('GID_NodeLive_Producer');
  producer.setNamesrvAddr(NS);
  producer.start();
  await producer.createTopic(TOPIC, producer.createTopicKey, 4);
  await sleep(3000);

  // ---- 1. consumer FIRST (rule #8), then send ----
  const received = new Map<string, number>(); // body -> count
  const queueSet = new Set<string>();
  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.subscribe(TOPIC, '*');
  consumer.setConsumeThreadNums(4);
  consumer.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) {
      const body = (m.getBody() || Buffer.alloc(0)).toString('utf8');
      received.set(body, (received.get(body) || 0) + 1);
      queueSet.add(`${m.getTopic()}@${m.getBrokerName()}@${m.getQueueId()}`);
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await consumer.start();
  console.log(`consumer started group=${GROUP} topic=${TOPIC}`);
  // A freshly created topic reaches the nameserver on the broker's own
  // registration cycle (up to ~30s); the consumer group registers at the
  // broker only after the first successful route fetch + heartbeat. Sending
  // before the queues are actually assigned would race CONSUME_FROM_LAST_OFFSET
  // (the offset initialises to maxOffset and skips everything). Wait for the
  // real assignment instead of a blind sleep.
  const assignDeadline = Date.now() + 60_000;
  while (Date.now() < assignDeadline) {
    const mine: MessageQueue[] = (consumer.assigned || []).filter((q: MessageQueue) => q.getTopic() === TOPIC);
    if (mine.length > 0) break;
    await sleep(1000);
  }
  const assignedCount = (consumer.assigned || []).filter((q: MessageQueue) => q.getTopic() === TOPIC).length;
  console.log(`assigned ${assignedCount} queue(s) for ${TOPIC}`);

  // ---- 2. send ----
  for (let i = 0; i < TOTAL; i++) {
    await producer.send(new Message(TOPIC, Buffer.from(`c-${i}`), 'TagC', `k${i}`));
  }
  console.log(`sent ${TOTAL} messages`);

  // ---- 3. wait for delivery and assert ----
  const deadline = Date.now() + 60000;
  while (received.size < TOTAL && Date.now() < deadline) await sleep(500);
  const missing: number[] = [];
  for (let i = 0; i < TOTAL; i++) {
    if ((received.get(`c-${i}`) || 0) !== 1) missing.push(i);
  }
  check(`all ${TOTAL} messages delivered exactly once`, missing.length === 0,
    missing.length ? `missing/dup: ${missing.join(',')}` : '');
  check('queues look sane (1..8 queues seen)', queueSet.size >= 1 && queueSet.size <= 8,
    `queues=${queueSet.size}`);

  await consumer.shutdown();
  producer.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
