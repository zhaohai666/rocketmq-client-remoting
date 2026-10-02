// live_admin_ns verifies the admin-only wire round-trips against a real
// cluster:
//   - 318 UPDATE_NAMESRV_CONFIG / 319 GET_NAMESRV_CONFIG against the nameserver
//   - 309 CONSUME_MESSAGE_DIRECTLY: the broker relays the message to a LIVE
//     consumer and answers with its verdict
//
//   node --experimental-strip-types examples/live_admin_ns.ts --ns 127.0.0.1:9876
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { Message } from '../src/common/message.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeAdminNS_${STAMP}`);
const GROUP = argOf('--group', `GID_NodeAdminNS_${STAMP}`);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  const admin = new DefaultMQAdminExt();
  admin.adminExtGroup = `GID_NodeAdminExt_${STAMP}`;
  admin.setNamesrvAddr(NS);
  admin.start();

  // ---- 1. 318 + 319 round-trip ----
  // The namesrv only persists fields that exist on NamesrvConfig (unknown keys
  // are dropped by Configuration.update), so flip a real boolean and read it
  // back, then restore.
  const probeKey = 'orderMessageEnable';
  let originalValue: string | undefined;
  try {
    const before = await admin.getNameServerConfig(null);
    originalValue = (Object.values(before)[0] || {})[probeKey];
    await admin.updateNameServerConfig({ [probeKey]: 'true' });
    check('318 updateNameServerConfig accepted', true);
  } catch (e) {
    check('318 updateNameServerConfig accepted', false, (e as Error).message);
  }
  let readBack: string | undefined;
  try {
    const configs = await admin.getNameServerConfig(null);
    const first = Object.values(configs)[0] || {};
    readBack = first[probeKey];
  } catch (e) {
    console.log(`  319 read failed: ${(e as Error).message}`);
  }
  check('319 getNameServerConfig reads back the 318 value', readBack === 'true',
    `readBack=${readBack} (was ${originalValue})`);
  try {
    await admin.updateNameServerConfig({ [probeKey]: originalValue || 'false' });
  } catch { /* restore best effort */ }

  // ---- 2. 309 with a live consumer ----
  const producer = new DefaultMQProducer('GID_NodeAdminNS_Prod');
  producer.setNamesrvAddr(NS);
  producer.start();
  await producer.createTopic(TOPIC, producer.createTopicKey, 4);
  await sleep(3000);

  const deliveries: string[] = [];
  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.subscribe(TOPIC, '*');
  consumer.setConsumeThreadNums(1);
  consumer.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) deliveries.push((m.getBody() || Buffer.alloc(0)).toString('utf8'));
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await consumer.start();
  const assignDeadline = Date.now() + 60_000;
  while (Date.now() < assignDeadline && (consumer.assigned || []).filter((q) => q.getTopic() === TOPIC).length === 0) {
    await sleep(1000);
  }

  const sendResult = await producer.send(new Message(TOPIC, Buffer.from('directly-0'), 'TagD', 'kd'));
  const clientId = consumer.mqClient != null ? consumer.mqClient.clientId : '';
  check('consumer clientId available for 309 targeting', clientId !== '', clientId);

  try {
    const result = await admin.consumeMessageDirectly(GROUP, clientId, TOPIC, sendResult.msgId);
    check('309 consumeMessageDirectly CONSUME_SUCCESS',
      result.consumeResult === 'CONSUME_SUCCESS',
      `result=${result.consumeResult} remark=${result.remark || ''} spent=${result.spentTimeMills ?? ''}`);
  } catch (e) {
    check('309 consumeMessageDirectly CONSUME_SUCCESS', false, (e as Error).message);
  }
  // The 309 delivery runs the SAME listener as the push delivery — the probe
  // body must have been seen twice (1 push + 1 direct).
  await sleep(2000);
  const direct = deliveries.filter((b) => b === 'directly-0').length;
  check('309 really drove the consumer listener (push + direct)', direct >= 2, `deliveries=${direct}`);

  await consumer.shutdown();
  producer.shutdown();
  admin.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
