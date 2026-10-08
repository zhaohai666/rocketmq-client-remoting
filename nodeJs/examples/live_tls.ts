// live_tls verifies the TLS transport against a broker started with
// `-Dtls.enable=true`. Three legs (mirrors php/examples/live_tls.php):
//   plain_tls : tlsEnable only — trust the broker's self-signed cert
//               (Java tls.test.mode.enable=true semantics)
//   ca_verify : + setTlsOptions({caCert, serverName}) — STRICT verification:
//               the broker cert must chain to the CA and the hostname/SAN
//               must match (Java tls.test.mode.enable=false semantics)
//   mtls      : ca_verify + client certificate (broker -Dtls.server.authClient=true)
//
//   node --experimental-strip-types examples/live_tls.ts --ns 127.0.0.1:9876 \
//        --leg ca_verify --caCert /tmp/.../ca.crt --serverName 127.0.0.1
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
const LEG = argOf('--leg', 'plain_tls');
const CA_CERT = argOf('--caCert', '');
const SERVER_NAME = argOf('--serverName', '127.0.0.1');
const CLIENT_CERT = argOf('--clientCert', '');
const CLIENT_KEY = argOf('--clientKey', '');
const TOPIC = argOf('--topic', `NodeTls_${LEG}_${STAMP}`);
const GROUP = argOf('--group', `GID_NodeTls_${LEG}_${STAMP}`);
const TOTAL = parseInt(argOf('--total', '5'), 10);

if (!['plain_tls', 'ca_verify', 'mtls'].includes(LEG)) {
  console.error(`unknown leg: ${LEG} (plain_tls|ca_verify|mtls)`);
  process.exit(2);
}
const tlsOptions = LEG === 'plain_tls' ? null : {
  caCert: CA_CERT, serverName: SERVER_NAME,
  ...(LEG === 'mtls' ? { clientCert: CLIENT_CERT, clientKey: CLIENT_KEY } : {}),
};

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  console.log(`=== node TLS live: ns=${NS} leg=${LEG} topic=${TOPIC} ===`);
  // consumer FIRST (rule #8) — it speaks TLS on the broker port too.
  const received = new Map<string, number>();
  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.setTlsEnable(true);
  if (tlsOptions != null) consumer.setTlsOptions(tlsOptions);
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
  if (tlsOptions != null) producer.setTlsOptions(tlsOptions);
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
