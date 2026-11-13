// live_orderly_transaction is the Node.js orderly + transaction consumption
// probe against a real RocketMQ 5.x cluster.
//
//   node --experimental-strip-types examples/live_orderly_transaction.ts --ns 127.0.0.1:9876
//
// Scenarios (fresh topics + fresh groups per run; every topic is created
// explicitly — B1: the consumer has no default-topic fallback):
//
//   O1  ORDERLY: a single-queue topic, 10 messages consumed by an orderly
//       listener; the arrival order of (queueOffset) must be exactly 0..9 —
//       orderly delivers one queue's batches in order, so this is the
//       invariant to pin.
//   O2  exactly-once for the orderly leg (no missing, no duplicate).
//   T1  transaction COMMIT: executeLocalTransaction commits immediately; the
//       consumer must receive the message.
//   T2  transaction UNKNOW -> broker check-back (CHECK_TRANSACTION_STATE 39):
//       executeLocalTransaction answers UNKNOW, the broker checks back and
//       checkLocalTransaction commits; the consumer must receive the message.
//   T3  transaction ROLLBACK: check-back answers ROLLBACK; the consumer must
//       NOT receive the message (the half message goes to %DLQ%/trans half
//       discard, never the queue).
//
// Closing line `PASS=<n> FAIL=<n>`; the process exits non-zero on any FAIL.
import { DefaultMQProducer, TransactionMQProducer, LocalTransactionState, TransactionListener } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { Message } from '../src/common/message.ts';
import { ConsumeConcurrentlyStatus, ConsumeOrderlyStatus } from '../src/client/consumer_result.ts';
import { SendStatus } from '../src/client/send_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = Date.now();
const ORDERLY_TOPIC = `NodeOrderlyTx_${STAMP}`;
const TX_TOPIC = `NodeTxMsg_${STAMP}`;
const ORDERLY_GROUP = `GID_Node_orderly_tx_${STAMP}`;
const TX_GROUP = `GID_Node_tx_${STAMP}`;
const ORDERLY_COUNT = 10;

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor(timeoutMs: number, cond: () => boolean): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (cond()) return true;
    await sleep(300);
  }
  return cond();
}

// The collector the transaction legs share: counts deliveries by body.
const txReceived = new Map<string, number>();

async function main(): Promise<void> {
  // ---- shared infra: producer for topic creation and plain sends ----
  const producer = new DefaultMQProducer(`GID_Node_otx_prod_${STAMP}`);
  producer.setNamesrvAddr(NS);
  producer.start();
  await producer.createTopic(ORDERLY_TOPIC, producer.createTopicKey, 1);
  await producer.createTopic(TX_TOPIC, producer.createTopicKey, 2);
  await sleep(3000);

  // ================= O1/O2 orderly =================
  const orderlyArrivals: number[] = [];
  const orderlyConsumer = new DefaultMQPushConsumer(ORDERLY_GROUP);
  orderlyConsumer.setNamesrvAddr(NS);
  orderlyConsumer.subscribe(ORDERLY_TOPIC, '*');
  orderlyConsumer.setConsumeThreadNums(1);
  orderlyConsumer.registerMessageListenerOrderly((msgs: any[]) => {
    for (const m of msgs) {
      orderlyArrivals.push(m.getQueueOffset());
    }
    return ConsumeOrderlyStatus.SUCCESS;
  });
  await orderlyConsumer.start();
  // consumer first (B5): wait for the assignment before sending
  const assignDeadline = Date.now() + 60_000;
  while (Date.now() < assignDeadline) {
    const mine = (orderlyConsumer.assigned || []).filter((q: any) => q.getTopic() === ORDERLY_TOPIC);
    if (mine.length > 0) break;
    await sleep(500);
  }
  console.log(`orderly assigned=${(orderlyConsumer.assigned || []).filter((q: any) => q.getTopic() === ORDERLY_TOPIC).length}`);

  for (let i = 0; i < ORDERLY_COUNT; i++) {
    await producer.send(new Message(ORDERLY_TOPIC, Buffer.from(`orderly-${i}`)));
  }
  const orderlyDone = await waitFor(30_000, () => orderlyArrivals.length >= ORDERLY_COUNT);
  check('O1 the orderly messages were all delivered', orderlyDone,
    `arrivals=${orderlyArrivals.length}`);
  const sorted = [...orderlyArrivals].sort((a, b) => a - b);
  check('O1 the orderly arrival order is exactly the queue offset order 0..9',
    sorted.length === ORDERLY_COUNT && orderlyArrivals.every((v, i) => v === sorted[i])
    && orderlyArrivals[0] === 0 && orderlyArrivals[ORDERLY_COUNT - 1] === ORDERLY_COUNT - 1,
    `order=${JSON.stringify(orderlyArrivals)}`);
  check('O2 the orderly leg is exactly-once (no missing, no duplicate)',
    new Set(orderlyArrivals).size === ORDERLY_COUNT,
    `distinct=${new Set(orderlyArrivals).size}`);

  // ================= T1/T2/T3 transaction =================
  // Body prefixes identify the legs even after %RETRY% topic restore (C3).
  // One listener dispatching on the body prefix: the COMMIT leg commits in
  // executeLocalTransaction; the UNKNOW leg parks the half message for the
  // broker's check-back (which then commits); the ROLLBACK leg rolls back on
  // check-back.
  const bodies = {
    commit: `tx-commit-${STAMP}`,
    unknow: `tx-unknow-${STAMP}`,
    rollback: `tx-rollback-${STAMP}`,
  };
  const txListener = new (class extends TransactionListener {
    executeLocalTransaction(msg: Message): number {
      const body = (msg.getBody() || Buffer.alloc(0)).toString('utf8');
      return body === bodies.commit ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.UNKNOW;
    }
    checkLocalTransaction(msg: Message): number {
      const body = (msg.getBody() || Buffer.alloc(0)).toString('utf8');
      return body === bodies.rollback ? LocalTransactionState.ROLLBACK_MESSAGE : LocalTransactionState.COMMIT_MESSAGE;
    }
  })();
  const txProducer = new TransactionMQProducer(`GID_Node_tx_prod_${STAMP}`, txListener);
  txProducer.setNamesrvAddr(NS);
  txProducer.start();

  const txConsumer = new DefaultMQPushConsumer(TX_GROUP);
  txConsumer.setNamesrvAddr(NS);
  txConsumer.subscribe(TX_TOPIC, '*');
  txConsumer.registerMessageListenerConcurrently((msgs: any[]) => {
    for (const m of msgs) {
      const body = (m.getBody() || Buffer.alloc(0)).toString('utf8');
      txReceived.set(body, (txReceived.get(body) || 0) + 1);
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await txConsumer.start();
  const txAssignDeadline = Date.now() + 60_000;
  while (Date.now() < txAssignDeadline) {
    const mine = (txConsumer.assigned || []).filter((q: any) => q.getTopic() === TX_TOPIC);
    if (mine.length > 0) break;
    await sleep(500);
  }
  console.log(`tx assigned=${(txConsumer.assigned || []).filter((q: any) => q.getTopic() === TX_TOPIC).length}`);

  // The listener is resolved from the producer's own field; bodies.dispatch above.
  const r1 = await txProducer.sendMessageInTransaction(new Message(TX_TOPIC, Buffer.from(bodies.commit)));
  check('T1 the COMMIT leg sent (endTransaction COMMIT, status SEND_OK)',
    r1 && r1.sendStatus === SendStatus.SEND_OK && r1.localTransactionState === LocalTransactionState.COMMIT_MESSAGE,
    r1 ? `status=${r1.sendStatus} state=${r1.localTransactionState}` : 'no result');

  const r2 = await txProducer.sendMessageInTransaction(new Message(TX_TOPIC, Buffer.from(bodies.unknow)));
  check('T2 the UNKNOW leg sent (half message parked)',
    r2 && r2.sendStatus === SendStatus.SEND_OK && r2.localTransactionState === LocalTransactionState.UNKNOW,
    r2 ? `status=${r2.sendStatus} state=${r2.localTransactionState}` : 'no result');

  // T4: while the UNKNOW half is parked, read RMQ_SYS_TRANS_HALF_TOPIC directly.
  // The broker only files a row there when the send request's sysFlag carries
  // TRANSACTION_PREPARED_TYPE (0x1 << 2 = 0x4, Java DefaultMQProducerImpl:951-954);
  // a missing bit stores the message as NORMAL and the check-back never fires.
  const halfSeen = { bodies: new Map<string, number>() };
  const halfReader = new DefaultMQPushConsumer(`GID_Node_half_read_${STAMP}`);
  halfReader.setNamesrvAddr(NS);
  halfReader.setConsumeFromWhere('CONSUME_FROM_FIRST_OFFSET');
  halfReader.subscribe('RMQ_SYS_TRANS_HALF_TOPIC', '*');
  halfReader.registerMessageListenerConcurrently((msgs: any[]) => {
    for (const m of msgs) {
      const body = (m.getBody() || Buffer.alloc(0)).toString('utf8');
      halfSeen.bodies.set(body, (halfSeen.bodies.get(body) || 0) + 1);
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await halfReader.start();
  const halfInTopic = await waitFor(15_000, () => (halfSeen.bodies.get(bodies.unknow) || 0) >= 1);
  check('T4 the parked half message is really in RMQ_SYS_TRANS_HALF_TOPIC (sysFlag bit 0x4 honored)',
    halfInTopic, `bodies=${Array.from(halfSeen.bodies.keys()).join(',')}`);
  await halfReader.shutdown();

  const r3 = await txProducer.sendMessageInTransaction(new Message(TX_TOPIC, Buffer.from(bodies.rollback)));
  check('T3 the ROLLBACK leg sent (half message parked)',
    r3 && r3.sendStatus === SendStatus.SEND_OK && r3.localTransactionState === LocalTransactionState.UNKNOW,
    r3 ? `status=${r3.sendStatus} state=${r3.localTransactionState}` : 'no result');

  const commitSeen = await waitFor(30_000, () => (txReceived.get(bodies.commit) || 0) >= 1);
  check('T1 the committed transaction message was consumed', commitSeen,
    `received=${txReceived.get(bodies.commit) || 0}`);

  // The broker's transaction check-back interval is seconds-scale (default 6s);
  // give the UNKNOW leg a generous window for at least one check.
  const unknowSeen = await waitFor(90_000, () => (txReceived.get(bodies.unknow) || 0) >= 1);
  check('T2 the check-back committed transaction message was consumed', unknowSeen,
    `received=${txReceived.get(bodies.unknow) || 0}`);

  // The rollback leg must never arrive. The observation window must outlast the
  // broker's check interval so a wrongly-committed half has time to surface.
  await sleep(20_000);
  check('T3 the rolled-back transaction message was never delivered',
    (txReceived.get(bodies.rollback) || 0) === 0,
    `received=${txReceived.get(bodies.rollback) || 0}`);

  await orderlyConsumer.shutdown();
  await txConsumer.shutdown();
  txProducer.shutdown();
  producer.shutdown();

  console.log(`\n############ PASS=${passCount} FAIL=${failCount} ############`);
  process.exit(failCount === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error('FATAL', e);
  process.exit(2);
});
