// live_acl verifies authentication against a broker started with
// `authenticationEnabled = true` (the 5.5 auth framework — its remoting path
// is wire-compatible with the classic AccessKey/Signature protocol:
// content = sorted extFields values minus Signature + body, signed with
// HMAC-SHA1 and sent as Base64; the broker verifies against the user's
// stored password, e.g. the super user injected via
// `initAuthenticationUser = {"username":"rocketmq","password":"12345678"}`).
//
//   node --experimental-strip-types examples/live_acl.ts --ns 127.0.0.1:9876 \
//        --ak rocketmq --sk 12345678
import { DefaultMQProducer } from '../src/client/producer.ts';
import { Message } from '../src/common/message.ts';
import { AclRPCHook } from '../src/remoting/acl.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeAcl_${STAMP}`);
const AK = argOf('--ak', 'rocketmq');
const SK = argOf('--sk', '12345678');

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  // ---- 1. signed producer: topic create + sends must succeed ----
  const good = new DefaultMQProducer(`GID_NodeAcl_Good_${STAMP}`);
  good.setNamesrvAddr(NS);
  good.rpcHook = new AclRPCHook(AK, SK);
  good.start();
  let created = false;
  for (let i = 0; i < 30 && !created; i++) {
    try { await good.createTopic(TOPIC, good.createTopicKey, 4); created = true; }
    catch { await sleep(1000); }
  }
  try {
    await sleep(3000);
    let sent = 0;
    for (let i = 0; i < 3; i++) {
      await good.send(new Message(TOPIC, Buffer.from(`acl-${i}`), 'TagA', `k${i}`));
      sent++;
    }
    check(`signed producer: createTopic(${created}) + send x3 succeed`, created && sent === 3, `sent=${sent}`);
  } catch (e) {
    check(`signed producer: createTopic(${created}) + send x3 succeed`, false, (e as Error).message);
  }
  await good.shutdown();
  await sleep(500);

  // ---- 2. wrong secret: broker must reject with an AUTH error ----
  const bad = new DefaultMQProducer(`GID_NodeAcl_Bad_${STAMP}`);
  bad.setNamesrvAddr(NS);
  bad.rpcHook = new AclRPCHook(AK, SK === '12345678' ? 'wrong-secret' : '12345678');
  bad.start();
  let rejectedWrong = false;
  let wrongRemark = '';
  try {
    await bad.send(new Message(TOPIC, Buffer.from('acl-bad'), 'TagA', 'kbad'));
  } catch (e) {
    const msg = (e as Error).message || '';
    // a genuine auth rejection, not a route-level miss
    rejectedWrong = !msg.includes('No route info') && !msg.includes('no route');
    wrongRemark = msg;
  }
  check('wrong secret rejected by broker (auth failure)', rejectedWrong, wrongRemark.slice(0, 120));
  await bad.shutdown();
  await sleep(500);

  // ---- 3. unsigned producer: broker must reject with an AUTH error ----
  const anon = new DefaultMQProducer(`GID_NodeAcl_Anon_${STAMP}`);
  anon.setNamesrvAddr(NS);
  anon.start();
  let rejectedAnon = false;
  let anonRemark = '';
  try {
    await anon.send(new Message(TOPIC, Buffer.from('acl-anon'), 'TagA', 'kanon'));
  } catch (e) {
    const msg = (e as Error).message || '';
    rejectedAnon = !msg.includes('No route info') && !msg.includes('no route');
    anonRemark = msg;
  }
  check('unsigned (no AccessKey) rejected by broker (auth failure)', rejectedAnon, anonRemark.slice(0, 120));
  await anon.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
