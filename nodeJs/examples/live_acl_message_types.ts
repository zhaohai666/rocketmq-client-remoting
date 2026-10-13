// live_acl_message_types is the one-command, ACL-enforcing end-to-end matrix
// for this port: EVERY message type the client supports is SENT through a
// broker started with `authenticationEnabled=true` (signed via --ak/--sk) and
// then PROVEN by reading it back from the broker.
//
//   node --experimental-strip-types examples/live_acl_message_types.ts \
//        --ns 127.0.0.1:9876 --ak <accessKey> --sk <secretKey>
//
// Env fallbacks: ROCKETMQ_ACL_AK / ROCKETMQ_ACL_SK. The example insists on
// credentials: without them the negative leg proves nothing.
//
// Coverage (each row = send under ACL + read-back assertion):
//   - sync normal            x2   read-back by body
//   - async                  x1   callback result + read-back
//   - oneway                 x1   read-back count (fire-and-forget has no ack)
//   - batch (SEND_BATCH)     x3   read-back of all three bodies
//   - delay level (3 = 10s)  x1   storeTimestamp - bornTimestamp >= 7s
//   - timer (TIMER_DELIVER_MS +5s) x1  same assertion >= 3s
//   - transaction half->commit x1 read-back proves the committed message is
//                                consumable (an un-committed half never ships)
//   - orderly via selector   x5   same hash key -> one queue, orderly listener
//                                sees them in send order
//   - tag-filtered consume   x2+x2 the filtered group gets exactly the
//                                TagFilterA pair and never sees TagFilterB
//   - keys + server query    x1   QUERY_MESSAGE by key returns the stored msg
//   - user property pass-through x1 the consumer sees the custom property
//
// Paired negative assertion (the anti-false-pass): BEFORE any signed client
// boots, an UNSIGNED producer send must be rejected with broker response
// code 16 (NO_PERMISSION). "Some error happened" is not proof — the code is
// asserted exactly. The probe shuts down and a settle pause follows; this
// port keys each MQClientInstance by `<ip>@<instanceName>` and rewrites the
// DEFAULT instance name per client to `<pid>#<nanotime>`, so one client's
// hook can never shadow another's — the probe-first ordering is kept anyway
// so the run also holds when the caller pins a shared instanceName.
import { DefaultMQProducer, TransactionMQProducer, TransactionListener,
  LocalTransactionState, SelectMessageQueueByHash } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { Message, MessageBatch, MessageExt, MessageQueue } from '../src/common/message.ts';
import { SendStatus } from '../src/client/send_result.ts';
import { ConsumeConcurrentlyStatus, ConsumeOrderlyStatus } from '../src/client/consumer_result.ts';
import { AclRPCHook } from '../src/remoting/acl.ts';
import { ResponseCode } from '../src/remoting/codes.ts';
import { MQBrokerException } from '../src/client/exception.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeAclTypes_${STAMP}`);
const AK = argOf('--ak', process.env['ROCKETMQ_ACL_AK'] || '');
const SK = argOf('--sk', process.env['ROCKETMQ_ACL_SK'] || '');

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const hook = () => new AclRPCHook(AK, SK);

// Commit-immediately listener; the half->commit (END_TRANSACTION) leg only.
class CommitListener extends TransactionListener {
  executed = 0;
  executeLocalTransaction(_msg: Message, _arg: any): number {
    this.executed++;
    return LocalTransactionState.COMMIT_MESSAGE;
  }
  checkLocalTransaction(_msg: Message): number {
    return LocalTransactionState.COMMIT_MESSAGE;
  }
}

async function waitQueuesAssigned(consumer: DefaultMQPushConsumer, timeoutMs: number): Promise<number> {
  // A fresh topic reaches the nameserver on the broker's registration cycle
  // and group registration needs the first heartbeat; poll for the real
  // assignment instead of a blind sleep (consume tests must start the
  // consumer BEFORE sending, with queues already assigned).
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const mine = (consumer.assigned || []).filter((q: MessageQueue) => q.getTopic() === TOPIC);
    if (mine.length > 0) return mine.length;
    await sleep(1000);
  }
  return 0;
}

async function main(): Promise<void> {
  if (AK === '' || SK === '') {
    console.error('this example verifies an ACL-enforcing cluster: --ak/--sk '
      + '(or ROCKETMQ_ACL_AK/ROCKETMQ_ACL_SK) are required');
    process.exit(2);
  }

  // ---- 0. negative probe: UNSIGNED send must die with NO_PERMISSION (16) ----
  const probe = new DefaultMQProducer('GID_NodeAclTypes_Probe');
  probe.setNamesrvAddr(NS);
  probe.start();
  let probeCode = -1;
  let probeDetail = 'unexpected SUCCESS without credentials (cluster enforces nothing?)';
  try {
    await probe.send(new Message(TOPIC, Buffer.from('nocred'), 'TagNoCred', 'knocred'));
  } catch (e) {
    const err = e as MQBrokerException;
    probeCode = typeof err.responseCode === 'number' ? err.responseCode : -1;
    probeDetail = `code=${probeCode} remark=${err.message || ''}`;
  }
  check('unsigned send rejected with broker code 16 (NO_PERMISSION)',
    probeCode === ResponseCode.NO_PERMISSION, probeDetail.slice(0, 160));
  probe.shutdown();
  await sleep(1000); // unload the probe's client instance before signed clients

  // ---- 1. signed producer + topic ----
  const producer = new DefaultMQProducer('GID_NodeAclTypes_Producer');
  producer.setNamesrvAddr(NS);
  producer.rpcHook = hook();
  producer.start();
  try {
    await producer.createTopic(TOPIC, producer.createTopicKey, 4);
    check('create topic (signed admin rpc on producer)', true);
  } catch (e) {
    check('create topic (signed admin rpc on producer)', false, String((e as Error).message));
    process.exit(1); // nothing below can run without the topic
  }
  await sleep(3000); // route registration takes a moment

  // ---- 2. consumers FIRST (rule: start before sending) ----
  // main: sees everything ('*'), records the full MessageExt for read-back.
  const received = new Map<string, MessageExt>();
  const mainConsumer = new DefaultMQPushConsumer(`GID_NodeAclTypes_Main_${STAMP}`);
  mainConsumer.setNamesrvAddr(NS);
  mainConsumer.rpcHook = hook();
  mainConsumer.subscribe(TOPIC, '*');
  mainConsumer.registerMessageListenerConcurrently((msgs: MessageExt[]) => {
    for (const m of msgs) {
      const body = (m.getBody() || Buffer.alloc(0)).toString('utf8');
      if (!received.has(body)) received.set(body, m);
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await mainConsumer.start();

  // tag-filtered: subscribes ONLY to TagFilterA; TagFilterB must never show up.
  const tagSeen = new Set<string>();
  const tagConsumer = new DefaultMQPushConsumer(`GID_NodeAclTypes_Tag_${STAMP}`);
  tagConsumer.setNamesrvAddr(NS);
  tagConsumer.rpcHook = hook();
  tagConsumer.subscribe(TOPIC, 'TagFilterA');
  tagConsumer.registerMessageListenerConcurrently((msgs: MessageExt[]) => {
    for (const m of msgs) tagSeen.add((m.getBody() || Buffer.alloc(0)).toString('utf8'));
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await tagConsumer.start();

  // orderly: subscribes to TagOrder and records the arrival sequence per
  // queue; in-orderly mode one queue is consumed by one thread at a time, so
  // the recorded order is the broker's storage order.
  const orderLog: { body: string; queue: string }[] = [];
  const orderConsumer = new DefaultMQPushConsumer(`GID_NodeAclTypes_Order_${STAMP}`);
  orderConsumer.setNamesrvAddr(NS);
  orderConsumer.rpcHook = hook();
  orderConsumer.subscribe(TOPIC, 'TagOrder');
  orderConsumer.registerMessageListenerOrderly((msgs: MessageExt[]) => {
    for (const m of msgs) {
      orderLog.push({
        body: (m.getBody() || Buffer.alloc(0)).toString('utf8'),
        queue: `${m.getBrokerName()}@${m.getQueueId()}`,
      });
    }
    return ConsumeOrderlyStatus.SUCCESS;
  });
  await orderConsumer.start();

  for (const c of [mainConsumer, tagConsumer, orderConsumer]) {
    const n = await waitQueuesAssigned(c, 60_000);
    console.log(`consumer ${c.consumerGroup} assigned ${n} queue(s)`);
  }

  // ---- 3. every message type, signed ----
  const expectBodies = new Set<string>();
  const note = (b: string) => expectBodies.add(b);

  // sync normal x2
  let syncOk = 0;
  for (let i = 0; i < 2; i++) {
    const body = `norm-${i}`;
    note(body);
    const r = await producer.send(new Message(TOPIC, Buffer.from(body), 'TagSync', `knorm-${i}`));
    if (r.sendStatus === SendStatus.SEND_OK) syncOk++;
  }
  check('sync normal send x2 accepted (SEND_OK)', syncOk === 2, `ok=${syncOk}`);

  // async x1
  const asyncResult = await new Promise<SendStatus | string>((resolve) => {
    note('async-0');
    producer.sendAsync(new Message(TOPIC, Buffer.from('async-0'), 'TagAsync', 'kasync'),
      (r, err) => resolve(err != null ? `err:${err.message || err}` : (r ? r.sendStatus : 'null')));
  });
  check('async send accepted (callback SEND_OK)', asyncResult === SendStatus.SEND_OK, `got=${String(asyncResult)}`);

  // oneway x1 — no ack exists; the read-back count below is the proof
  note('oneway-0');
  await producer.sendOneway(new Message(TOPIC, Buffer.from('oneway-0'), 'TagOneway', 'koneway'));
  console.log('INFO  oneway sent; proven by read-back count only');

  // batch x3 (SEND_BATCH_MESSAGE)
  const batch = new MessageBatch([0, 1, 2].map((i) => {
    note(`batch-${i}`);
    return new Message(TOPIC, Buffer.from(`batch-${i}`), 'TagBatch', `kbatch${i}`);
  }));
  const batchResult = await producer.send(batch);
  check('batch send x3 accepted (SEND_OK)', batchResult.sendStatus === SendStatus.SEND_OK,
    `status=${batchResult.sendStatus}`);

  // delay level 3 (the default ladder is 1s 5s 10s ... -> 10s)
  note('delay-0');
  const delayMsg = new Message(TOPIC, Buffer.from('delay-0'), 'TagDelay', 'kdelay');
  delayMsg.setDelayTimeLevel(3);
  const delaySend = await producer.send(delayMsg);
  check('delay-level send accepted', delaySend.sendStatus === SendStatus.SEND_OK);

  // timer message: TIMER_DELIVER_MS 5s ahead
  note('timer-0');
  const deliverMs = Date.now() + 5000;
  const timerMsg = new Message(TOPIC, Buffer.from('timer-0'), 'TagTimer', 'ktimer');
  timerMsg.setDeliverTimeMs(deliverMs);
  const timerSend = await producer.send(timerMsg);
  check('timer (TIMER_DELIVER_MS +5s) send accepted', timerSend.sendStatus === SendStatus.SEND_OK);

  // transaction: half message -> END_TRANSACTION commit
  const txListener = new CommitListener();
  const tx = new TransactionMQProducer(`GID_NodeAclTypes_Tx`, txListener);
  tx.setNamesrvAddr(NS);
  tx.rpcHook = hook();
  await tx.start();
  note('tx-0');
  const txResult = await tx.sendMessageInTransaction(
    new Message(TOPIC, Buffer.from('tx-0'), 'TagTx', 'ktx'), null);
  check('transaction half->commit accepted',
    txResult.sendStatus === SendStatus.SEND_OK && txListener.executed === 1,
    `state=${txResult.localTransactionState}`);

  // orderly: five messages through the hash selector, same key -> same queue
  const selector = new SelectMessageQueueByHash();
  const ordQueues = new Set<string>();
  for (let i = 0; i < 5; i++) {
    const body = `ord-${i}`;
    note(body);
    const r = await producer.sendBySelector(
      new Message(TOPIC, Buffer.from(body), 'TagOrder', 'ordkey'), selector, 'ordkey');
    if (r.sendStatus === SendStatus.SEND_OK) ordQueues.add(`${r.messageQueue.getBrokerName()}@${r.messageQueue.getQueueId()}`);
  }
  check('orderly send x5 via selector landed on ONE queue', ordQueues.size === 1,
    `queues=${[...ordQueues].join(',')}`);

  // tag-filtered source: two A, two B
  for (let i = 0; i < 2; i++) {
    note(`tA-${i}`);
    await producer.send(new Message(TOPIC, Buffer.from(`tA-${i}`), 'TagFilterA', `kta${i}`));
    note(`tB-${i}`);
    await producer.send(new Message(TOPIC, Buffer.from(`tB-${i}`), 'TagFilterB', `ktb${i}`));
  }

  // user property pass-through
  note('prop-0');
  const propMsg = new Message(TOPIC, Buffer.from('prop-0'), 'TagProp', 'kprop');
  propMsg.putProperty('userProp', 'uv-42');
  await producer.send(propMsg);

  // keys + later server-side query by key
  note('query-0');
  await producer.send(new Message(TOPIC, Buffer.from('query-0'), 'TagQuery', 'aclQueryKey'));

  // ---- 4. read-back waits (delay 10s + timer 5s ride on these deadlines) ----
  const allDeadline = Date.now() + 120_000;
  while (received.size < expectBodies.size && Date.now() < allDeadline) await sleep(500);
  const missing = [...expectBodies].filter((b) => !received.has(b));
  check(`read-back: all ${expectBodies.size} committed messages consumed`,
    missing.length === 0, missing.length ? `missing: ${missing.join(',')}` : '');

  const got = (b: string): MessageExt | undefined => received.get(b);
  check('oneway proven by read-back', got('oneway-0') != null);

  // delay: the message is re-appended when it matures, so
  // storeTimestamp - bornTimestamp spans the delay window.
  const delayMsg2 = got('delay-0');
  const delayDelta = delayMsg2 ? delayMsg2.getStoreTimestamp() - delayMsg2.getBornTimestamp() : -1;
  check('delay level 3 (10s): store-born gap >= 7s', delayDelta >= 7000, `delta=${delayDelta}ms`);

  const timerMsg2 = got('timer-0');
  const timerDelta = timerMsg2 ? timerMsg2.getStoreTimestamp() - timerMsg2.getBornTimestamp() : -1;
  check('timer (+5s): store-born gap >= 3s', timerDelta >= 3000, `delta=${timerDelta}ms`);

  check('transaction commit is consumable (read-back)', got('tx-0') != null);

  // order preserved on read-back: the orderly listener logged ord-0..ord-4
  const ordSeq = orderLog.filter((e) => e.body.startsWith('ord-'));
  const ordOrderOk = ordSeq.length === 5 && ordSeq.every((e, i) => e.body === `ord-${i}`);
  const ordSameQueue = new Set(ordSeq.map((e) => e.queue)).size === 1;
  check('orderly read-back: order preserved on one queue', ordOrderOk && ordSameQueue,
    `seq=${ordSeq.map((e) => e.body).join(',')} queues=${new Set(ordSeq.map((e) => e.queue)).size}`);

  // tag filter: exactly the two A bodies, never a B — give the filtered
  // group the same settle window the main group already consumed.
  const tagDeadline = Date.now() + 15_000;
  while (tagSeen.size < 2 && Date.now() < tagDeadline) await sleep(500);
  const sawB = [...tagSeen].some((b) => b.startsWith('tB-'));
  check('tag-filtered consumption: got tA pair, never tB',
    tagSeen.has('tA-0') && tagSeen.has('tA-1') && !sawB, `seen=${[...tagSeen].join(',')}`);

  // user property survived the broker round trip
  const propMsg2 = got('prop-0');
  check('user property pass-through', propMsg2?.getProperty('userProp') === 'uv-42',
    `userProp=${propMsg2?.getProperty('userProp') ?? 'null'}`);

  // server-side query by KEY (topic index). Index building lags the commit
  // by a tick; retry up to 30s.
  const admin = new DefaultMQAdminExt(hook());
  admin.setNamesrvAddr(NS);
  admin.start();
  let queryHits: MessageExt[] = [];
  const queryDeadline = Date.now() + 30_000;
  while (queryHits.length === 0 && Date.now() < queryDeadline) {
    try {
      const res = await admin.queryMessageFromRoute(TOPIC, 'aclQueryKey');
      queryHits = res.messageList;
    } catch { /* QUERY_NOT_FOUND until the key index lands — retry */ }
    if (queryHits.length === 0) await sleep(1000);
  }
  const queryBody = queryHits.length > 0
    ? (queryHits[0].getBody() || Buffer.alloc(0)).toString('utf8') : '';
  check('server-side query by key returns the stored message',
    queryHits.length >= 1 && queryBody === 'query-0', `hits=${queryHits.length} body=${queryBody}`);

  // ---- 5. teardown ----
  await mainConsumer.shutdown();
  await tagConsumer.shutdown();
  await orderConsumer.shutdown();
  tx.shutdown();
  producer.shutdown();
  admin.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
