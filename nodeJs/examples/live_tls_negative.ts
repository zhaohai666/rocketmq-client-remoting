// Negative probe for route-error transparency (#25):
// strict-CA TLS producer pointed at a HEALTHY TLS cluster but trusting the
// WRONG CA. Before the fix the producer surfaced a bare
// "No route info of this topic" (black box — user chases the topic, the real
// problem is the certificate). After the fix the error must carry the route
// fetch failure chain including the certificate text.
//
//   node --experimental-strip-types examples/live_tls_negative.ts \
//        --ns 127.0.0.1:9876 --caCert /tmp/.../ca-wrong.crt --serverName 127.0.0.1
import { DefaultMQProducer } from '../src/client/producer.ts';
import { Message } from '../src/common/message.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const WRONG_CA = argOf('--caCert', '');
const SERVER_NAME = argOf('--serverName', '127.0.0.1');
const TOPIC = argOf('--topic', 'TlsNegProbe');
if (WRONG_CA === '') { console.error('--caCert required'); process.exit(2); }

function chainText(e: any, depth = 0): string {
  if (e == null) return '';
  const self = `${e.name ?? 'Error'}: ${e.message}`;
  const cause = depth < 6 ? chainText(e.cause, depth + 1) : '';
  return cause ? `${self} <-- ${cause}` : self;
}

async function main(): Promise<void> {
  console.log(`=== node TLS negative probe: ns=${NS} caCert=${WRONG_CA} (wrong CA) ===`);
  const producer = new DefaultMQProducer('GID_NodeTlsNeg');
  producer.setNamesrvAddr(NS);
  producer.setTlsEnable(true);
  producer.setTlsOptions({ caCert: WRONG_CA, serverName: SERVER_NAME });

  const errors: Error[] = [];
  try { await producer.start(); } catch (e) { errors.push(e as Error); }
  try { await producer.send(new Message(TOPIC, Buffer.from('neg-probe'))); } catch (e) { errors.push(e as Error); }
  try { producer.shutdown(); } catch { /* best effort */ }

  const text = errors.map((e) => chainText(e)).join('\n');
  console.log(`--- captured error(s) ---\n${text || '(none!)'}`);

  let pass = true;
  if (errors.length === 0) { console.log('FAIL  expected an error, got none'); pass = false; }
  if (!text.includes('No route info')) {
    console.log('FAIL  error should mention "No route info"'); pass = false;
  }
  if (!text.includes('route fetch failed')) {
    console.log('FAIL  missing "route fetch failed" suffix (#25 transparency)'); pass = false;
  }
  if (!/(certificate|cert|CERT|SSL|ssl|verify|CERTIFICATE)/.test(text)) {
    console.log('FAIL  no certificate/CA marker anywhere in the error chain'); pass = false;
  }
  console.log(pass ? 'TLS_NEG_PROBE PASS' : 'TLS_NEG_PROBE FAIL');
  process.exit(pass ? 0 : 1);
}
main().catch((e) => { console.error(e); process.exit(1); });
