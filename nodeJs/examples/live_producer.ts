// live_producer is the Node.js end-to-end smoke test against a real RocketMQ
// 5.x cluster: sync / batch / oneway sends, the async chain, the queue
// selector, the two-phase transaction flow, and the broker's transaction
// check-back.
//
//   node --experimental-strip-types examples/live_producer.ts --ns 127.0.0.1:9876
//
// Every check prints PASS/FAIL and the process exits non-zero if any failed.
// Use a fresh topic per run (the default embeds a timestamp) so leftover state
// from an earlier run cannot make a check pass.
//
// The last line is `SENT=<n>`: how many messages the broker accepted. The
// read-back check in the other language needs that number, and deriving it
// here means adding a check here can never silently make the read-back
// expectation wrong.
import { DefaultMQProducer, TransactionMQProducer, TransactionListener,
  LocalTransactionState, SelectMessageQueueByHash } from '../src/client/producer.ts';
import { Message, MessageBatch } from '../src/common/message.ts';
import { SendStatus } from '../src/client/send_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeLive_${STAMP}`);

let passCount = 0;
let failCount = 0;
let sent = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// txListener records what happened so the check-back can be observed.
class TxListener extends TransactionListener {
  executeRun = 0;
  checkRun = 0;
  executeRet = LocalTransactionState.COMMIT_MESSAGE;
  checkRet = LocalTransactionState.COMMIT_MESSAGE;
  executeLocalTransaction(msg: Message, arg: any): number {
    this.executeRun++;
    void arg;
    // 'trans' = 'unknow' → leave the half message unresolved so the broker
    // check-back path gets exercised; anything else commits immediately.
    if (msg.getProperty('trans') === 'unknow') return LocalTransactionState.UNKNOW;
    return this.executeRet;
  }
  checkLocalTransaction(msg: Message): number {
    this.checkRun++;
    void msg;
    return this.checkRet;
  }
}

async function main(): Promise<void> {
  const producer = new DefaultMQProducer('GID_NodeLive_Producer');
  producer.setNamesrvAddr(NS);
  producer.start();
  console.log(`producer started ns=${NS} topic=${TOPIC}`);

  // 0. pre-create the topic (deterministic queue count; the consumer-side rule
  // "create topic BEFORE starting consumers" applies to the read-back too).
  try {
    await producer.createTopic(TOPIC, producer.createTopicKey, 4);
    check('create topic', true);
  } catch (e) {
    check('create topic', false, String((e as Error).message));
  }
  await sleep(3000); // route registration on 5.5.1 takes a moment

  // 1. sync send (10 msgs)
  let syncOk = 0;
  for (let i = 0; i < 10; i++) {
    const msg = new Message(TOPIC, Buffer.from(`sync-${i}`), 'TagSync', `k${i}`);
    try {
      const result = await producer.send(msg);
      if (result.sendStatus === SendStatus.SEND_OK) { syncOk++; sent++; }
      else check(`sync send ${i}`, false, `status=${result.sendStatus}`);
    } catch (e) {
      check(`sync send ${i}`, false, String((e as Error).message));
    }
  }
  check('sync send x10', syncOk === 10, `ok=${syncOk}`);

  // 2. batch send (one 3-message batch = 3 accepted)
  try {
    const batch = new MessageBatch([
      new Message(TOPIC, Buffer.from('batch-0'), 'TagBatch', 'kb0'),
      new Message(TOPIC, Buffer.from('batch-1'), 'TagBatch', 'kb1'),
      new Message(TOPIC, Buffer.from('batch-2'), 'TagBatch', 'kb2'),
    ]);
    const result = await producer.send(batch);
    check('batch send x3', result.sendStatus === SendStatus.SEND_OK, `status=${result.sendStatus}`);
    if (result.sendStatus === SendStatus.SEND_OK) sent += 3;
  } catch (e) {
    check('batch send x3', false, String((e as Error).message));
  }

  // 3. oneway send (no ack — success = no throw)
  try {
    await producer.sendOneway(new Message(TOPIC, Buffer.from('oneway-0'), 'TagOneway', 'ko'));
    check('oneway send', true);
    sent++; // oneway cannot be confirmed; count optimistically like the Go port
  } catch (e) {
    check('oneway send', false, String((e as Error).message));
  }

  // 4. async send (callback)
  await new Promise<void>((resolve) => {
    producer.sendAsync(new Message(TOPIC, Buffer.from('async-0'), 'TagAsync', 'ka'),
      (result, err) => {
        if (err != null || result == null) check('async send', false, String(err));
        else { check('async send', result.sendStatus === SendStatus.SEND_OK, `status=${result.sendStatus}`); sent++; }
        resolve();
      });
  });

  // 5. queue selector (hash on the key lands on one queue)
  try {
    const result = await producer.sendBySelector(
      new Message(TOPIC, Buffer.from('selector-0'), 'TagSel', 'ks'),
      new SelectMessageQueueByHash(), 'ks');
    check('selector send', result.sendStatus === SendStatus.SEND_OK);
    if (result.sendStatus === SendStatus.SEND_OK) sent++;
  } catch (e) {
    check('selector send', false, String((e as Error).message));
  }

  // 6. two-phase transaction: COMMIT on execute.
  const tx = new TransactionMQProducer('GID_NodeLive_Tx', new TxListener());
  tx.setNamesrvAddr(NS);
  await tx.start();
  try {
    const txMsg = new Message(TOPIC, Buffer.from('tx-commit-0'), 'TagTx', 'ktx');
    txMsg.putProperty('trans', 'commit');
    const result = await tx.sendMessageInTransaction(txMsg, null);
    check('transaction COMMIT', result.sendStatus === SendStatus.SEND_OK, `state=${result.localTransactionState}`);

    // 7. half message left UNKNOW → broker check-back resolves it.
    const listener = tx.transactionListener as TxListener;
    listener.checkRet = LocalTransactionState.ROLLBACK_MESSAGE;
    const halfMsg = new Message(TOPIC, Buffer.from('tx-unknow-0'), 'TagTx', 'ktx2');
    halfMsg.putProperty('trans', 'unknow');
    await tx.sendMessageInTransaction(halfMsg, null);
    // broker 巡检半消息的周期是 BrokerConfig#transactionCheckInterval（5.5.1 默认
    // 30s），8s 的固定等待必然早于首次回查。改成等"回查发生"事件，最多 90s。
    const deadline = Date.now() + 90_000;
    while (listener.checkRun === 0 && Date.now() < deadline) await sleep(1000);
    check('transaction check-back ran', listener.checkRun > 0, `checks=${listener.checkRun}`);
    sent += 1; // commit half counts; the unknow one rolls back
  } catch (e) {
    check('transaction flow', false, String((e as Error).message));
  }
  tx.shutdown();
  producer.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  console.log(`SENT=${sent}`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
