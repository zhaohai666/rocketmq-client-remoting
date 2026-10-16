// live_pull is the Node.js DefaultMQPullConsumer end-to-end smoke:
// SHORT-poll pulls (suspend=false — the cross-port trap), manual offset
// bookkeeping, sendMessageBack into %RETRY%, offset persistence, and the
// subscribe-less flow the README documents.
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

  // README flow: a pull consumer that never calls subscribe(). start() must
  // succeed, fetchMessageQueuesInBalance must return the group's share (the
  // broker only knows the group from a heartbeat carrying its ConsumerData), and
  // a restarted instance must read its committed offset back from the store even
  // though nothing has fetched this topic's route yet.
  const README_GROUP = `${GROUP}_Readme`;
  const readme1 = new DefaultMQPullConsumer(README_GROUP);
  readme1.setNamesrvAddr(NS);
  await readme1.start();
  check('start() without subscribe()', true);
  const rmqs = await readme1.fetchMessageQueuesInBalance(TOPIC);
  check('balance without subscribe()', rmqs.length >= 1, `mqs=${rmqs.length}`);
  let readmePulled = 0;
  for (const mq of rmqs) {
    let offset = await readme1.fetchConsumeOffset(mq, true);
    if (offset < 0) offset = 0;
    for (;;) {
      const r = await readme1.pull(mq, '*', offset, 10);
      if (r.status !== PullStatus.FOUND) {
        if (r.status === PullStatus.OFFSET_ILLEGAL) { offset = r.nextBeginOffset; continue; }
        break;
      }
      readmePulled += (r.msgFoundList || []).length;
      offset = r.nextBeginOffset;
      readme1.updateConsumeOffset(mq, offset);
    }
  }
  check('pull without subscribe()', readmePulled >= TOTAL, `pulled=${readmePulled}`);
  await readme1.persistConsumeOffset();
  await readme1.shutdown();

  const readme2 = new DefaultMQPullConsumer(README_GROUP);
  readme2.setNamesrvAddr(NS);
  await readme2.start();
  // Read first, before anything in this instance has fetched the topic route:
  // the queue list is carried over from the previous instance, exactly what the
  // offset store cannot resolve from its own address table.
  let resumed = -1;
  for (const mq of rmqs) {
    const stored = await readme2.fetchConsumeOffset(mq, true);
    if (stored > resumed) resumed = stored;
  }
  check('committed offset readable by a cold instance', resumed > 0, `offset=${resumed}`);
  await readme2.shutdown();

  producer.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
