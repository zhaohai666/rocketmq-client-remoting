// transaction_sysflag_smoke — a half message must carry sysFlag's
// TRANSACTION_PREPARED_TYPE (0x1 << 2 = 0x4) on the wire.
//
//   node --experimental-strip-types test/transaction_sysflag_smoke.ts
//
// Java reference: sendMessageInTransaction stamps TRAN_MSG="true"
// (DefaultMQProducerImpl:1445) and the send kernel reads it back into
// sysFlag before building the request header (:951-954,
// `sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE`). Without that bit
// the broker stores the row as a NORMAL message: it never enters
// RMQ_SYS_TRANS_HALF_TOPIC and endTransaction cannot find the prepared
// record (the whole transaction half of this port was silently broken).
//
// A plain MQClient instance is used so the real buildSendRequest -> header
// path is asserted; only the network call is stubbed (no broker).
import assert from 'node:assert';
import { Buffer } from 'node:buffer';

import { Message, MessageQueue } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { MessageAccessor } from '../src/common/message_accessor.ts';
import { MessageSysFlag } from '../src/common/sysflag.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { CommunicationMode } from '../src/client/hook.ts';
import { SendStatus } from '../src/client/send_result.ts';

let checks = 0;
let failed = 0;
function check(name: string, ok: boolean, detail?: string): void {
  checks++;
  if (ok) { console.log(`  ok    ${name}`); }
  else { failed++; console.log(`  FAIL  ${name}${detail ? ` — ${detail}` : ''}`); }
}

// Build a producer whose client is a real MQClient but with the network call
// replaced by a capture, so the send kernel runs end-to-end offline.
function makeProducer(captured: { sysFlags: number[] }): DefaultMQProducer {
  const p = new DefaultMQProducer('GID_TxSysFlag');
  const client = new MQClient('tx-sysflag@instance', null, null);
  client.publishAddrFor = (_mq) => '127.0.0.1:10911';
  client.sendMessage = async (_addr, request) => {
    // buildSendRequest wraps the v2 header as customHeader; its wire name for
    // sysFlag is 'f' (headers.ts). Guard against a missing header: reading a
    // sentinel -1 must NOT be allowed to slip through bitwise checks (-1 & bit
    // is always truthy in JS).
    const h: any = (request as any).customHeader;
    if (h == null || (h.f == null && h.sysFlag == null)) {
      throw new Error('send request has no SendMessageRequestHeaderV2');
    }
    captured.sysFlags.push(Number(h.f ?? h.sysFlag));
    const { SendResult } = await import('../src/client/send_result.ts');
    return new SendResult(SendStatus.SEND_OK, 'uniq-1');
  };
  (p as any).client = client;
  return p;
}

async function main(): Promise<void> {
  console.log('== transaction half-message sysFlag ==');

  // 0. constant parity (Java MessageSysFlag:36)
  check('TRANSACTION_PREPARED_TYPE == 0x4 (Java 0x1 << 2)',
    MessageSysFlag.TRANSACTION_PREPARED_TYPE === 0x4,
    `got 0x${MessageSysFlag.TRANSACTION_PREPARED_TYPE.toString(16)}`);

  // 1. a half message carries the prepared bit in the request header sysFlag.
  const half: { sysFlags: number[] } = { sysFlags: [] };
  const ph = makeProducer(half);
  const halfMsg = new Message('T', Buffer.from('half'));
  MessageAccessor.setTransactionPrepared(halfMsg);
  const mq = new MessageQueue('T', 'broker-a', 0);
  await ph.sendKernelImpl(halfMsg, mq, CommunicationMode.SYNC, null);
  check('half message: header sysFlag carries TRANSACTION_PREPARED_TYPE',
    half.sysFlags.length === 1 && (half.sysFlags[0] & MessageSysFlag.TRANSACTION_PREPARED_TYPE) !== 0,
    `sysFlags=${half.sysFlags}`);

  // 2. a normal message keeps the transaction field at NOT_TYPE (0).
  const normal: { sysFlags: number[] } = { sysFlags: [] };
  const pn = makeProducer(normal);
  const plain = new Message('T', Buffer.from('plain'));
  await pn.sendKernelImpl(plain, mq, CommunicationMode.SYNC, null);
  check('normal message: transaction field is 0',
    normal.sysFlags.length === 1
    && (normal.sysFlags[0] & MessageSysFlag.TRANSACTION_ROLLBACK_TYPE) === 0
    && MessageSysFlag.getTransactionValue(normal.sysFlags[0]) === 0,
    `sysFlags=${normal.sysFlags}`);

  // 3. compression and the prepared bit coexist (kernel = compression first,
  //    prepared second, one OR each — Java :946/:953 order).
  const both: { sysFlags: number[] } = { sysFlags: [] };
  const pb = makeProducer(both);
  const big = new Message('T', Buffer.alloc(64 * 1024, 0x61));
  MessageAccessor.setTransactionPrepared(big);
  (pb as any).tryToCompressMessage(big);
  await pb.sendKernelImpl(big, mq, CommunicationMode.SYNC, null);
  check('compressed half message: COMPRESSED_FLAG and PREPARED both set',
    both.sysFlags.length === 1
    && (both.sysFlags[0] & MessageSysFlag.COMPRESSED_FLAG) !== 0
    && (both.sysFlags[0] & MessageSysFlag.TRANSACTION_PREPARED_TYPE) !== 0,
    `sysFlag=0x${both.sysFlags[0].toString(16)}`);

  // 4. idempotent: re-sending the same prepared message does not double-set
  //    anything (the field is a 2-bit enum, OR stays stable).
  const again: { sysFlags: number[] } = { sysFlags: [] };
  const pa = makeProducer(again);
  await pa.sendKernelImpl(halfMsg, mq, CommunicationMode.SYNC, null);
  check('re-send keeps exactly one prepared bit set',
    again.sysFlags.length === 1 && half.sysFlags.length === 1
    && again.sysFlags[0] === half.sysFlags[0],
    `first=0x${half.sysFlags[0]?.toString(16)} again=0x${again.sysFlags[0]?.toString(16)}`);

  console.log(`\n${checks} checks, ${failed} failures`);
  if (failed > 0) process.exit(1);
  console.log('PASS');
}

main().catch((e) => { console.error(e); process.exit(1); });
