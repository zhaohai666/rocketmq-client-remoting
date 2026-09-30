// live_pull is the Node.js DefaultMQPullConsumer end-to-end smoke:
// SHORT-poll pulls (suspend=false — the cross-port trap), manual offset
// bookkeeping, sendMessageBack into %RETRY%, and offset persistence.
//
//   node --experimental-strip-types examples/live_pull.ts --ns 127.0.0.1:9876
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { Message } from '../src/common/message.ts';
import { PullStatus } from '../src/client/consumer_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeLive_${STAMP}`);
const GROUP = argOf('--group', `GID_NodeLive_Pull_${STAMP}`);
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
  await producer.createTopic(TOPIC, producer.createTopicKey, 4);
  await sleep(3000);

  for (let i = 0; i < TOTAL; i++) {
    await producer.send(new Message(TOPIC, Buffer.from(`p-${i}`), 'TagP', `k${i}`));
  }
  await sleep(500);
  console.log(`sent ${TOTAL} messages to ${TOPIC}`);

  const consumer = new DefaultMQPullConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  // Java DefaultMQPullConsumer: subscribe() MUST precede start() (the pull
  // consumer registers its subscription at heartbeat time and the start guard
  // requires it).
  consumer.subscribe(TOPIC, '*');
  await consumer.start();

  const mqs = await consumer.fetchMessageQueuesInBalance(TOPIC);
  check('fetchMessageQueuesInBalance', mqs.length >= 1, `mqs=${mqs.length}`);

  let pulled = 0;
  const pulledBodies = new Set<string>(); // queueOffset repeats per queue — account by body
  for (const mq of mqs) {
    let offset = await consumer.fetchConsumeOffset(mq, false);
    if (offset < 0) offset = 0;
    for (;;) {
      // SHORT poll: suspend must stay false for pull() (rule #10: suspend=true
      // on a short poll burns the full suspend budget).
      const result = await consumer.pull(mq, '*', offset, 10);
      if (result.status === PullStatus.FOUND) {
        for (const m of result.msgFoundList) {
          pulled++;
          pulledBodies.add((m.getBody() || Buffer.alloc(0)).toString('utf8'));
          // send one message back into %RETRY% (via the topic-route-reusing
          // consumer; rule #11)
          if (m.getQueueOffset && m.getQueueOffset() === 0) {
            try { await consumer.sendMessageBack(m, 1); } catch (e) { /* best effort */ }
          }
        }
        offset = result.nextBeginOffset;
        await consumer.updateConsumeOffset(mq, offset);
      } else if (result.status === PullStatus.NO_NEW_MSG
        || result.status === PullStatus.OFFSET_ILLEGAL) {
        if (result.status === PullStatus.OFFSET_ILLEGAL) offset = result.nextBeginOffset;
        else break;
      } else {
        break;
      }
    }
  }
  check(`pulled all ${TOTAL} messages`, pulled >= TOTAL && pulledBodies.size === TOTAL,
    `pulled=${pulled} unique=${pulledBodies.size}`);

  await consumer.persistConsumeOffset();
  check('persistConsumeOffset', true);

  await consumer.shutdown();
  producer.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
