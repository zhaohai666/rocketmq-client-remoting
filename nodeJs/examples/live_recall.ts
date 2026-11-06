// live_recall is the Node.js recall (RECALL_MESSAGE(370)) live smoke: a timer
// message carries a recallHandle, %RETRY% topics and garbage handles are
// rejected locally, the recall returns the withdrawn message's id, and the
// recalled message is never delivered while its control timers arrive.
//
//   node --experimental-strip-types examples/live_recall.ts --ns 127.0.0.1:9876
//
// The broker's recallMessageEnable is flipped on for the run and restored to
// its original value at the end. The consume-observation window is longer than
// the timer delay: the control message must arrive on time, the recalled one
// must stay absent for the whole window.
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { ConsumeFromWhere } from '../src/remoting/heartbeat.ts';
import { Message } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeRecallLive_${STAMP}`);
const GROUP = argOf('--group', `GID_NodeRecallLive_${STAMP}`);
const DELAY_SEC = parseInt(argOf('--delay-sec', '12'), 10);
const WINDOW_SEC = DELAY_SEC + 20;
const RECALL_KEY = 'recallMessageEnable';

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  const admin = new DefaultMQAdminExt(null);
  admin.setNamesrvAddr(NS);
  await admin.start();

  const producer = new DefaultMQProducer('GID_NodeRecallSend_' + STAMP);
  producer.setNamesrvAddr(NS);
  await producer.start();

  // ---- R0: recallMessageEnable readable + flipped on ----
  const cfg = await (admin as any).getBrokerConfig('127.0.0.1:10911');
  const original = cfg?.[RECALL_KEY];
  check('R0 the broker\'s recallMessageEnable is readable', original != null, `value=${original}`);
  let addr = '127.0.0.1:10911';
  let flipped = false;
  if (original == null || original !== 'true') {
    await (admin as any).updateBrokerConfig(addr, { [RECALL_KEY]: 'true' });
    await sleep(500);
    const cfg2 = await (admin as any).getBrokerConfig(addr);
    flipped = cfg2?.[RECALL_KEY] === 'true';
    check('R0 recallMessageEnable flipped on', flipped, `was=${original}`);
  }

  // ---- R1: only a timer message carries a recallHandle ----
  await producer.createTopic(TOPIC, producer.createTopicKey, 2);
  await sleep(3000);

  const timerMsg = new Message(TOPIC, Buffer.from('to-recall'));
  timerMsg.putProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC, String(DELAY_SEC));
  const controlMsg = new Message(TOPIC, Buffer.from('control'));
  controlMsg.putProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC, String(DELAY_SEC));
  const plainMsg = new Message(TOPIC, Buffer.from('plain'));

  const rRecall = await producer.send(timerMsg);
  const rControl = await producer.send(controlMsg);
  const rPlain = await producer.send(plainMsg);
  const handle = rRecall?.recallHandle ?? null;
  check('R1 the timer message came back with a recallHandle', handle != null && handle !== '',
    `handle=${handle}`);
  check('R1 the ordinary message carries no recallHandle', rPlain?.recallHandle == null);
  check('R1 the two timer messages have different handles',
    handle != null && rControl?.recallHandle !== handle);

  // ---- R4/R5: local validation before touching the network ----
  let retryRejected = false;
  let retryErr = '';
  try {
    await producer.recallMessage('%RETRY%' + GROUP, handle ?? '');
  } catch (e) {
    retryRejected = true;
    retryErr = (e as Error).message;
  }
  check('R4 a %RETRY% topic is rejected', retryRejected && retryErr.includes('topic is not supported'),
    retryErr);
  const began = Date.now();
  let illegalRejected = false;
  let illegalErr = '';
  try {
    await producer.recallMessage(TOPIC, 'not-a-handle');
  } catch (e) {
    illegalRejected = true;
    illegalErr = (e as Error).message;
  }
  check('R5 an illegal handle is rejected locally',
    illegalRejected && (Date.now() - began) < 200,
    `${illegalErr} (${Date.now() - began}ms)`);

  // ---- R3: the recall returns the withdrawn message's id ----
  let recalledID = '';
  let recallErr = '';
  try {
    recalledID = await producer.recallMessage(TOPIC, handle ?? '');
  } catch (e) {
    recallErr = (e as Error).message;
  }
  check('R3 recallMessage returns the withdrawn message\'s id',
    recallErr === '' && recalledID === (rRecall?.msgId ?? ''),
    `resp=${recalledID} send=${rRecall?.msgId} ${recallErr}`);

  // ---- R6: the control timers arrive, the recalled one never does ----
  const received = new Set<string>();
  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
  consumer.subscribe(TOPIC, '*');
  consumer.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) {
      received.add((m.getBody() || Buffer.alloc(0)).toString('utf8'));
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await consumer.start();
  await sleep(WINDOW_SEC * 1000);
  await consumer.shutdown();

  check('R6 the control timer message was delivered on time', received.has('control'),
    `got=${[...received].sort().join(',')}`);
  check('R6 the ordinary message was delivered', received.has('plain'));
  check('R6 the recalled timer message was never delivered', !received.has('to-recall'),
    `got=${[...received].sort().join(',')}`);

  // ---- R7: restore the flag ----
  let restored = false;
  try {
    await (admin as any).updateBrokerConfig(addr, { [RECALL_KEY]: String(original) });
    await sleep(500);
    const cfg3 = await (admin as any).getBrokerConfig(addr);
    restored = cfg3?.[RECALL_KEY] === String(original);
  } catch { /* restore failed */ }
  check(`R7 the broker's recallMessageEnable was restored to ${original}`, restored);

  await producer.shutdown();
  await admin.shutdown();

  console.log(`\nPASS=${passCount} FAIL=${failCount}`);
  if (failCount > 0) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
