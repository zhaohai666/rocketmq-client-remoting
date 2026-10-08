// live_fixes2 — end-to-end verification of the five live-reported fixes:
//   fix1  transaction half message VISIBLE right after COMMIT (observation
//         window << the 30s broker check-back),
//   fix2  admin query-by-key (K index), by-uniq-key (U index + UNIQUE flag),
//         and route-wide query — the body is binary stored records,
//   fix3  producer compression entry: LZ4 / ZSTD / ZLIB send + consume with
//         byte-identical bodies,
//   fix4  message trace: producer-side dispatcher + Pub record msgId=UNIQ_KEY
//         + consumer SubBefore/SubAfter, read back from RMQ_SYS_TRACE_TOPIC.
//   (fix5, the slave-only leg, needs a master+slave cluster — see
//   scripts/run_node_slave_live.sh.)
//
//   node --experimental-strip-types examples/live_fixes2.ts --ns 127.0.0.1:9876
import { DefaultMQProducer, TransactionMQProducer, TransactionListener,
  LocalTransactionState } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { Message } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { SendStatus } from '../src/client/send_result.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { DecodeTraceDataString } from '../src/client/trace_context.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TX_TOPIC = `NodeFix2Tx_${STAMP}`;
const COMP_TOPIC = `NodeFix2Comp_${STAMP}`;
const TRACE_TOPIC_BIZ = `NodeFix2Trace_${STAMP}`;
const TRACE_TOPIC = 'RMQ_SYS_TRACE_TOPIC';

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

class TxCommitListener extends TransactionListener {
  executeLocalTransaction(_msg: Message, _arg: any): number {
    return LocalTransactionState.COMMIT_MESSAGE;
  }
  checkLocalTransaction(_msg: Message): number {
    return LocalTransactionState.COMMIT_MESSAGE;
  }
}

function bigBody(tag: string, size: number): Buffer {
  // Compressible but not run-of-one (deflate/lz4 both bite; no RLE edge).
  const parts: Buffer[] = [];
  let i = 0;
  while (parts.reduce((a, b) => a + b.length, 0) < size) {
    parts.push(Buffer.from(`${tag}-${i++}-${(i * 7919) % 100000}-abcdefghijklmnopqrstuv\n`));
  }
  return Buffer.concat(parts);
}

async function main(): Promise<void> {
  // ---------- setup ----------
  const setup = new DefaultMQProducer('GID_Fix2_Setup');
  setup.setNamesrvAddr(NS);
  await setup.start();
  for (const t of [TX_TOPIC, COMP_TOPIC, TRACE_TOPIC_BIZ, TRACE_TOPIC]) {
    try { await setup.createTopic(t, setup.createTopicKey, 4); } catch (e) {
      console.log(`createTopic ${t}: ${(e as Error).message}`);
    }
  }
  await sleep(3000); // route registration

  // NOTE: DefaultMQAdminExt's FIRST ctor arg is the rpcHook — pass null and
  // set the group via the third arg (a string here would be registered as a
  // "hook" and blow up on doBeforeRequest).
  const admin = new DefaultMQAdminExt(null, 20000, 'GID_Fix2_Admin');
  admin.namesrvAddr = NS;
  admin.start();

  // ---------- fix1: transaction commit visible in the observation window ----------
  console.log('--- fix1: transaction COMMIT visibility ---');
  {
    const tx = new TransactionMQProducer(`GID_Fix2_Tx_${STAMP}`, new TxCommitListener());
    tx.setNamesrvAddr(NS);
    await tx.start();
    const key = `fix2tx-${STAMP}`;
    const msg = new Message(TX_TOPIC, Buffer.from('tx-visible-now'), 'TagFix2Tx', key);
    const result = await tx.sendMessageInTransaction(msg, null);
    check('tx half message sent', result.sendStatus === SendStatus.SEND_OK,
      `state=${result.localTransactionState}`);
    // The commit was sent oneway BEFORE this returns; poll the key index —
    // the message must show up well before the broker's first check-back
    // (transactionCheckInterval default 30s).
    let hit: any = null;
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline && hit == null) {
      try {
        const qr = await admin.queryMessageFromRoute(TX_TOPIC, key, 32);
        if (qr.messageList.length > 0) hit = qr.messageList[0];
        else await sleep(500);
      } catch (e) { await sleep(500); }
    }
    check('fix1: committed message visible by KEY within 15s', hit != null,
      hit ? '' : 'no index hit in window');
    if (hit != null) {
      check('fix1: visible body matches', hit.getBody().toString('utf8') === 'tx-visible-now',
        hit.getBody().toString('utf8'));
    }
    tx.shutdown();
  }

  // ---------- fix2: key query (K + U + route) ----------
  console.log('--- fix2: admin query by key ---');
  {
    const p = new DefaultMQProducer(`GID_Fix2_Query_${STAMP}`);
    p.setNamesrvAddr(NS);
    await p.start();
    const kmsg = new Message(TX_TOPIC, Buffer.from('query-by-key-body'), 'TagFix2Q', `fix2k-${STAMP}`);
    const sr = await p.send(kmsg);
    check('query-leg send OK', sr.sendStatus === SendStatus.SEND_OK);
    await sleep(2000); // index build

    const kq = await admin.queryMessageFromRoute(TX_TOPIC, `fix2k-${STAMP}`, 32);
    check('fix2: queryMessageFromRoute (K) hit', kq.messageList.length >= 1,
      `hits=${kq.messageList.length}`);
    const uniq = kmsg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) || '';
    check('sent message carries UNIQ_KEY', uniq.length > 0);
    if (uniq) {
      // route must resolve a broker address for the U query
      await (admin as any)._requireClient().updateTopicRouteInfoFromNameServer(TX_TOPIC, false);
      const route = (admin as any)._requireClient().getTopicRouteData(TX_TOPIC);
      const bd = (route?.brokerDatas || [])[0];
      const addr = bd?.brokerAddrs?.['0'];
      if (addr) {
        const uq = await admin.queryMessageByUniqKey(addr, TX_TOPIC, uniq, 32);
        check('fix2: queryMessageByUniqKey (U + UNIQUE flag) hit', uq != null
          && uq.getBody().toString('utf8') === 'query-by-key-body');
      } else {
        check('fix2: route resolves broker addr', false);
      }
    }
    p.shutdown();
  }

  // ---------- fix3: compression selection ----------
  console.log('--- fix3: LZ4 / ZSTD / ZSTD send + consume ---');
  {
    const received = new Map<string, string>(); // tag -> body
    const consumer = new DefaultMQPushConsumer(`GID_Fix2_CompC_${STAMP}`);
    consumer.setNamesrvAddr(NS);
    consumer.subscribe(COMP_TOPIC, '*');
    consumer.registerMessageListenerConcurrently((msgs) => {
      for (const m of msgs) {
        received.set(String(m.getProperty('comp')), (m.getBody() || Buffer.alloc(0)).toString('utf8'));
      }
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    await consumer.start();
    const assignDeadline = Date.now() + 60_000;
    while (Date.now() < assignDeadline) {
      const mine = (consumer.assigned || []).filter((q: any) => q.getTopic() === COMP_TOPIC);
      if (mine.length > 0) break;
      await sleep(1000);
    }
    const algos = ['LZ4', 'ZSTD', 'ZLIB'];
    for (const algo of algos) {
      const p = new DefaultMQProducer(`GID_Fix2_Comp_${algo}_${STAMP}`);
      p.setNamesrvAddr(NS);
      p.setCompressType(algo);
      p.setCompressMsgBodyOverHowmuch(2048);
      await p.start();
      const body = bigBody(algo, 16 * 1024);
      const m = new Message(COMP_TOPIC, body, 'TagComp', `comp-${algo}`);
      m.putProperty('comp', algo);
      const sr = await p.send(m);
      check(`fix3: ${algo} 16KB send`, sr.sendStatus === SendStatus.SEND_OK, `status=${sr.sendStatus}`);
      p.shutdown();
    }
    const deadline = Date.now() + 30_000;
    while (received.size < algos.length && Date.now() < deadline) await sleep(500);
    for (const algo of algos) {
      const back = received.get(algo);
      check(`fix3: ${algo} body survives the roundtrip`, back != null
        && back === bigBody(algo, 16 * 1024).toString('utf8'),
        back ? `len=${back.length}` : 'not received');
    }
    consumer.shutdown();
  }

  // ---------- fix4: message trace ----------
  console.log('--- fix4: trace pub+sub records ---');
  {
    const traceRaw: string[] = [];
    const reader = new DefaultMQPushConsumer(`GID_Fix2_TraceR_${STAMP}`);
    reader.setNamesrvAddr(NS);
    reader.subscribe(TRACE_TOPIC, '*');
    reader.registerMessageListenerConcurrently((msgs) => {
      for (const m of msgs) traceRaw.push((m.getBody() || Buffer.alloc(0)).toString('utf8'));
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    await reader.start(); // reader FIRST: trace records flushed later must be seen

    const p = new DefaultMQProducer(`GID_Fix2_TraceP_${STAMP}`);
    p.setNamesrvAddr(NS);
    p.setEnableTrace(true);
    await p.start();
    const c = new DefaultMQPushConsumer(`GID_Fix2_TraceC_${STAMP}`);
    c.setNamesrvAddr(NS);
    c.setEnableTrace(true);
    c.subscribe(TRACE_TOPIC_BIZ, '*');
    c.registerMessageListenerConcurrently((msgs) => {
      void msgs;
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    await c.start();
    const assignDeadline = Date.now() + 60_000;
    while (Date.now() < assignDeadline) {
      const mine = (c.assigned || []).filter((q: any) => q.getTopic() === TRACE_TOPIC_BIZ);
      if (mine.length > 0) break;
      await sleep(1000);
    }
    const tmsg = new Message(TRACE_TOPIC_BIZ, Buffer.from('trace-me'), 'TagTrace', `fix4-${STAMP}`);
    const sr = await p.send(tmsg);
    check('fix4: traced send OK', sr.sendStatus === SendStatus.SEND_OK);
    const uniq = tmsg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) || '';
    // dispatcher flush timer is 5s; the consumer trace rides the same cadence.
    await sleep(9000);
    // Decode every record we collected.
    let pubHit = false; let subBefore = false; let subAfter = false;
    for (const raw of traceRaw) {
      for (const ctx of DecodeTraceDataString(raw)) {
        if (ctx.traceType === 'Pub' && ctx.traceBeans.some((b: any) => b.msgId === uniq)) pubHit = true;
        if (ctx.traceType === 'SubBefore' && ctx.groupName === `GID_Fix2_TraceC_${STAMP}`) subBefore = true;
        if (ctx.traceType === 'SubAfter' && ctx.groupName === `GID_Fix2_TraceC_${STAMP}`) subAfter = true;
      }
    }
    check('fix4: Pub record with UNIQ_KEY msgId', pubHit, `rawRecords=${traceRaw.length}`);
    check('fix4: SubBefore record from consumer group', subBefore);
    check('fix4: SubAfter record from consumer group', subAfter);
    if (!pubHit || !subBefore || !subAfter) {
      console.log(`  sendResult.msgId='${sr.msgId}' offsetMsgId='${sr.offsetMsgId}' uniq='${uniq}'`);
      traceRaw.forEach((raw, i) => {
        console.log(`  -- raw[${i}] (${raw.length}B) --`);
        for (const c of DecodeTraceDataString(raw)) {
          const b = c.traceBeans[0] || {};
          console.log(`     ${c.traceType} group=${c.groupName} msgId=${b.msgId} keys=${b.keys}`);
        }
      });
    }
    c.shutdown(); p.shutdown(); reader.shutdown();
  }

  admin.shutdown();
  setup.shutdown();
  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
