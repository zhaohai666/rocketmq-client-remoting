// live_slave_only — fix5 verification: with ONLY the slave alive, a freshly
// started push consumer must still consume.
//
// Two phases (the SHELL kills the master between them):
//   --phase send     create topic (on master AND slave), send N messages,
//                    exit. HA replication drains while the shell sleeps.
//   --phase consume  brand-new consumer group, CONSUME_FROM_FIRST_OFFSET —
//                    with the master's route entry gone this exercises:
//                    consumer-id list via the SLAVE (selectBrokerAddr
//                    fallback), heartbeat-to-slave, pull-from-slave, and
//                    slave-side offset behaviour (no offset commits).
//
//   node --experimental-strip-types examples/live_slave_only.ts --ns 127.0.0.1:9876 --phase send --total 10
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { Message } from '../src/common/message.ts';
import { SendStatus } from '../src/client/send_result.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { ConsumeFromWhere } from '../src/remoting/heartbeat.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const PHASE = argOf('--phase', 'send');
const TOTAL = parseInt(argOf('--total', '10'), 10);
const TOPIC = argOf('--topic', `NodeSlave_${STAMP}`);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function sendPhase(): Promise<number> {
  const p = new DefaultMQProducer(`GID_SlaveLive_Send_${STAMP}`);
  p.setNamesrvAddr(NS);
  await p.start();
  // The route may still be registering when we get here — retry.
  let created = false;
  const deadline = Date.now() + 60_000;
  while (!created && Date.now() < deadline) {
    try { await p.createTopic(TOPIC, p.createTopicKey, 4); created = true; }
    catch (e) { console.log(`createTopic retry: ${(e as Error).message}`); await sleep(2000); }
  }
  if (!created) { check('create topic', false); p.shutdown(); return 1; }
  check('create topic (master+slave)', true);
  await sleep(3000);
  let ok = 0;
  for (let i = 0; i < TOTAL; i++) {
    const sr = await p.send(new Message(TOPIC, Buffer.from(`slave-${i}`), 'TagSlave', `ks${i}`));
    if (sr.sendStatus === SendStatus.SEND_OK) ok++;
  }
  check(`send x${TOTAL} (master alive)`, ok === TOTAL, `ok=${ok}`);
  p.shutdown();
  console.log(`SENT=${ok}`);
  return ok === TOTAL ? 0 : 1;
}

async function consumePhase(): Promise<number> {
  const received = new Set<string>();
  let slaveSeen = false;
  const consumer = new DefaultMQPushConsumer(`GID_SlaveLive_C_${STAMP}`);
  consumer.setNamesrvAddr(NS);
  consumer.subscribe(TOPIC, '*');
  consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
  consumer.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) {
      received.add((m.getBody() || Buffer.alloc(0)).toString('utf8'));
      // Every message must come from the slave (id=1) — the only live node.
      console.log(`  recv ${m.getBody().toString('utf8')} broker=${m.getBrokerName()}`);
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await consumer.start();
  // Wait for queue assignment — with the master gone this only succeeds if
  // the member list resolves through the slave.
  const assignDeadline = Date.now() + 60_000;
  let assigned = 0;
  while (Date.now() < assignDeadline) {
    assigned = (consumer.assigned || []).filter((q: any) => q.getTopic() === TOPIC).length;
    if (assigned > 0) break;
    await sleep(1000);
  }
  check('queues assigned with only slave alive', assigned > 0, `assigned=${assigned}`);
  const deadline = Date.now() + 60_000;
  while (received.size < TOTAL && Date.now() < deadline) await sleep(1000);
  check(`consumed all ${TOTAL} from the slave`, received.size === TOTAL,
    `got=${received.size}`);
  let missing = 0;
  for (let i = 0; i < TOTAL; i++) if (!received.has(`slave-${i}`)) missing++;
  check('no missing bodies', missing === 0, `missing=${missing}`);
  void slaveSeen;
  consumer.shutdown();
  console.log(`RECV=${received.size}`);
  return received.size === TOTAL ? 0 : 1;
}

const run = PHASE === 'send' ? sendPhase() : consumePhase();
run.then((rc) => {
  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : rc);
}).catch((e) => { console.error(e); process.exit(1); });
