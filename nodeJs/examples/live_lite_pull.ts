// live_lite_pull is the Node.js DefaultLitePullConsumer end-to-end smoke:
// subscribe (auto-rebalance) + poll(timeout) round-trip, seek backwards, and
// commitSync.
//
//   node --experimental-strip-types examples/live_lite_pull.ts --ns 127.0.0.1:9876
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { Message, MessageQueue } from '../src/common/message.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeLive_${STAMP}`);
const GROUP = argOf('--group', `GID_NodeLive_Lite_${STAMP}`);
const TOTAL = parseInt(argOf('--total', '8'), 10);

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
  await producer.createTopic(TOPIC, producer.createTopicKey, 2);
  await sleep(3000);

  for (let i = 0; i < TOTAL; i++) {
    await producer.send(new Message(TOPIC, Buffer.from(`l-${i}`), 'TagL', `k${i}`));
  }
  console.log(`sent ${TOTAL} messages to ${TOPIC}`);

  const consumer = new DefaultLitePullConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.subscribe(TOPIC, '*');
  // The messages are sent BEFORE the consumer starts: with the Java default
  // CONSUME_FROM_LAST_OFFSET the initial offset is maxOffset and everything
  // pre-sent would be skipped. FIRST_OFFSET makes the read deterministic.
  consumer.setConsumeFromWhere('CONSUME_FROM_FIRST_OFFSET');
  consumer.setAutoCommit(false);
  await consumer.start();
  await sleep(3000); // assignment needs a rebalance round

  // ---- poll everything ----
  const seen = new Set<string>(); // queueOffset repeats per queue — account by body
  let firstSeen: { mq: MessageQueue; offset: number; body: string } | null = null;
  const deadline = Date.now() + 30000;
  while (seen.size < TOTAL && Date.now() < deadline) {
    const msgs = await consumer.poll(2000);
    for (const m of msgs) {
      const body = (m.getBody() || Buffer.alloc(0)).toString('utf8');
      seen.add(body);
      if (firstSeen == null) {
        firstSeen = {
          mq: new MessageQueue(m.getTopic(), m.getBrokerName(), m.getQueueId()),
          offset: m.getQueueOffset(),
          body,
        };
      }
    }
  }
  check(`poll delivered all ${TOTAL}`, seen.size === TOTAL, `unique=${seen.size}`);

  // ---- seek backwards to the FIRST message and re-read it ----
  const first = await (async () => {
    if (firstSeen == null) return null;
    consumer.seek(firstSeen.mq, firstSeen.offset);
    const again = await consumer.poll(3000);
    for (const m of again) {
      if ((m.getBody() || Buffer.alloc(0)).toString('utf8') === firstSeen.body) return firstSeen.body;
    }
    return null;
  })();
  check('seek re-reads the first message', first != null, `first=${first}`);

  await consumer.commitSync();
  check('commitSync', true);

  await consumer.shutdown();
  producer.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
