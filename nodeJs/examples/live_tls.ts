// live_tls verifies the TLS transport against a broker started with
// `-Dtls.enable=true` (server side; the broker's own namesrv connection stays
// plaintext — that is governed by nettyClientConfig.useTLS, not this flag).
// The node client wraps the socket with node:tls (rejectUnauthorized=false,
// matching a self-signed server cert).
//
//   node --experimental-strip-types examples/live_tls.ts --ns 127.0.0.1:9876
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
const TOPIC = argOf('--topic', `NodeTls_${STAMP}`);
const GROUP = argOf('--group', `GID_NodeTls_${STAMP}`);
const TOTAL = parseInt(argOf('--total', '5'), 10);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  // consumer FIRST (rule #8) — it speaks TLS on the broker port too.
  const received = new Map<string, number>();
  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.setTlsEnable(true);
  consumer.subscribe(TOPIC, '*');
  consumer.setConsumeThreadNums(2);
  consumer.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) {
      const body = (m.getBody() || Buffer.alloc(0)).toString('utf8');
      received.set(body, (received.get(body) || 0) + 1);
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await consumer.start();

  const producer = new DefaultMQProducer('GID_NodeTls_Prod');
  producer.setNamesrvAddr(NS);
  producer.setTlsEnable(true);
  producer.start();
  await producer.createTopic(TOPIC, producer.createTopicKey, 4);
  await sleep(3000);
  const assignDeadline = Date.now() + 60_000;
  while (Date.now() < assignDeadline && (consumer.assigned || []).filter((q) => q.getTopic() === TOPIC).length === 0) {
    await sleep(1000);
  }

  let sent = 0;
  try {
    for (let i = 0; i < TOTAL; i++) {
      await producer.send(new Message(TOPIC, Buffer.from(`tls-${i}`), 'TagT', `k${i}`));
      sent++;
    }
  } catch (e) {
    console.log(`  tls send failed: ${(e as Error).message}`);
  }
  check(`TLS producer sent ${TOTAL}`, sent === TOTAL, `sent=${sent}`);

  const deadline = Date.now() + 60_000;
  while (received.size < TOTAL && Date.now() < deadline) await sleep(500);
  const missing: number[] = [];
  for (let i = 0; i < TOTAL; i++) {
    if ((received.get(`tls-${i}`) || 0) !== 1) missing.push(i);
  }
  check('TLS consumer received all exactly once', missing.length === 0,
    missing.length ? `missing: ${missing.join(',')}` : '');

  await consumer.shutdown();
  producer.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
