// Smoke: the five live-reported fixes —
//   1. transaction END_TRANSACTION commitOrRollback mapping (sysflag types, not ordinals)
//   2. queryMessage binary stored-record body decoding
//   3. producer compression-type entry (ZLIB/LZ4/ZSTD) + decoder support
//   4. trace wiring: UNIQ_KEY msgId on Pub records + producer dispatcher + EndTransaction record
//   5. slave fallback: consumer-id list address selection + all-broker heartbeat enumeration
// Run: node --experimental-strip-types test/fixes2_smoke.ts
import assert from 'node:assert';
import nodeZlib from 'node:zlib';
import { DefaultMQProducer, LocalTransactionState } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { Message, MessageExt, MessageQueue } from '../src/common/message.ts';
import { MessageSysFlag } from '../src/common/sysflag.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { MessageAccessor } from '../src/common/message_accessor.ts';
import { encodeMessageExt, decodeMessage, decodeMessages, decompressBody, createMessageId } from '../src/common/messageDecoder.ts';
import { ipAndPortToBytes } from '../src/common/utilAll.ts';
import { lz4CompressFrame, lz4DecompressFrame, zstdCompressRaw, zstdDecompressFrame, decompressFor } from '../src/common/compress.ts';
import { SendMessageTraceHookImpl } from '../src/client/trace_hook.ts';
import { AsyncTraceDispatcher } from '../src/client/trace_dispatcher.ts';
import { SendMessageContext } from '../src/client/hook.ts';
import { DecodeTraceDataString } from '../src/client/trace_context.ts';
import { createUniqID } from '../src/common/messageClientIdSetter.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, detail = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${detail}`); }
}

// ---------------------------------------------------------------- 1. transaction
console.log('== transaction END_TRANSACTION mapping ==');
{
  const oneway: RemotingCommand[] = [];
  const p = new DefaultMQProducer('GID_TxSmoke');
  (p as any).client = {
    publishAddrFor: (_mq: MessageQueue) => '127.0.0.1:10911',
    remotingClient: { invokeOneway: (_addr: string, req: RemotingCommand) => { oneway.push(req); return Promise.resolve(); } },
  };
  const msg = new Message('T', Buffer.from('x'));
  const uniq = createUniqID();
  MessageAccessor.putProperty(msg, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniq);
  const mq = new MessageQueue('T', 'b', 0);
  // A REAL decodable offsetMsgId: physical commitlog offset 123456789 on 127.0.0.1:10911.
  const PHYS_OFFSET = 123456789;
  const offsetMsgId = createMessageId(ipAndPortToBytes('127.0.0.1', 10911), PHYS_OFFSET);
  const sendResult: any = {
    sendStatus: 'SEND_OK', msgId: uniq, messageQueue: mq, queueOffset: 42,
    transactionId: 'tid-1', offsetMsgId, regionId: '', traceOn: true,
  };
  await (p as any)._endTransaction(sendResult, LocalTransactionState.COMMIT_MESSAGE, msg);
  await (p as any)._endTransaction(sendResult, LocalTransactionState.ROLLBACK_MESSAGE, msg);
  await (p as any)._endTransaction(sendResult, LocalTransactionState.UNKNOW, msg);
  check('3 oneway END_TRANSACTION captured', oneway.length === 3);
  // extFields materialize lazily inside headerEncode() — force it, like the
  // real transport does, before asserting the wire fields.
  const exts = oneway.map((r) => { r.headerEncode(); return r.extFields; });
  // Wire fields are stringified (Java JSON-header convention) — compare numerically.
  check('COMMIT -> TRANSACTION_COMMIT_TYPE(8)', Number(exts[0]['commitOrRollback']) === MessageSysFlag.TRANSACTION_COMMIT_TYPE,
    `got ${exts[0]['commitOrRollback']}`);
  check('ROLLBACK -> TRANSACTION_ROLLBACK_TYPE(12)', Number(exts[1]['commitOrRollback']) === MessageSysFlag.TRANSACTION_ROLLBACK_TYPE,
    `got ${exts[1]['commitOrRollback']}`);
  check('UNKNOW -> TRANSACTION_NOT_TYPE(0)', Number(exts[2]['commitOrRollback']) === MessageSysFlag.TRANSACTION_NOT_TYPE,
    `got ${exts[2]['commitOrRollback']}`);
  check('commitLogOffset = PHYSICAL offset decoded from offsetMsgId', exts[0]['commitLogOffset'] === String(PHYS_OFFSET),
    `got ${exts[0]['commitLogOffset']}`);
  check('tranStateTableOffset = queueOffset', exts[0]['tranStateTableOffset'] === '42',
    `got ${exts[0]['tranStateTableOffset']}`);
  check('topic passthrough', exts[0]['topic'] === 'T', `got ${exts[0]['topic']}`);
  check('msgId = UNIQ_KEY property', exts[0]['msgId'] === uniq, `got ${exts[0]['msgId']}`);
  check('fromTransactionCheck=false', exts[0]['fromTransactionCheck'] === 'false');
  check('transactionId passthrough', exts[0]['transactionId'] === 'tid-1');
}

// ---------------------------------------------------------------- 2. queryMessage decode
console.log('== queryMessage binary body decoding ==');
{
  const m1 = new MessageExt();
  m1.setTopic('T'); m1.setBody(Buffer.from('body-one')); m1.setQueueId(0);
  m1.setQueueOffset(1); m1.setCommitLogOffset(100); m1.setSysFlag(0);
  m1.setBornTimestamp(Date.now()); m1.setStoreTimestamp(Date.now());
  m1.setBornHost('127.0.0.1'); m1.setStoreHost('127.0.0.1');
  m1.setReconsumeTimes(0); m1.setFlag(0); m1.setBodyCrc(0);
  MessageAccessor.putProperty(m1, MessageConst.PROPERTY_KEYS, 'k1');
  const m2 = new MessageExt();
  m2.setTopic('T'); m2.setBody(Buffer.from('body-two')); m2.setQueueId(1);
  m2.setQueueOffset(2); m2.setCommitLogOffset(300); m2.setSysFlag(0);
  m2.setBornTimestamp(Date.now()); m2.setStoreTimestamp(Date.now());
  m2.setBornHost('127.0.0.1'); m2.setStoreHost('127.0.0.1');
  m2.setReconsumeTimes(0); m2.setFlag(0); m2.setBodyCrc(0);
  const body = Buffer.concat([encodeMessageExt(m1, false), encodeMessageExt(m2, false)]);
  check('decodeMessages parses concatenated stored records', decodeMessages(body, true).length === 2);

  const admin = new DefaultMQAdminExt('admin_smoke');
  let sentCode: number | null = null;
  (admin as any).client = {
    remotingClient: {
      invokeSync: async (_addr: string, req: RemotingCommand, _t: number) => {
        sentCode = req.code;
        return { code: ResponseCode.SUCCESS, extFields: { indexLastUpdateTimestamp: '12345' }, body, remark: null };
      },
    },
  };
  (admin as any)._started = true;
  const qr = await admin.queryMessage('127.0.0.1:10911', 'T', 'k1');
  check('queryMessage returns decoded MessageExt list', qr.messageList.length === 2
    && qr.messageList[0].getBody().toString() === 'body-one');
  check('indexLastUpdateTimestamp from response header ext', qr.indexLastUpdateTimestamp === 12345);
  check('QUERY_MESSAGE request code used', sentCode === 12);
}

// ---------------------------------------------------------------- 3. compression entry
console.log('== producer compression selection ==');
{
  const p = new DefaultMQProducer('GID_CompSmoke');
  p.compressMsgBodyOverHowmuch = 100;
  const payload = Buffer.from('rocketmq-payload-'.repeat(200)); // 3400B, compressible
  for (const [name, typeVal] of [['LZ4', 1], ['ZSTD', 2], ['ZLIB', 3]] as const) {
    p.setCompressType(name);
    check(`setCompressType(${name})`, p.compressType === typeVal);
    const msg = new Message('T', payload);
    const ok = p.tryToCompressMessage(msg);
    const sysFlag = (msg as any)._sysFlag;
    // On Node >= 23.8 ZSTD is node:zlib's real codec (compressed blocks, size
    // gain); on older runtimes it degrades to a legal Raw-block frame, which is
    // interop-correct but stores the body. Assert the invariant both paths
    // share: compressed flag set, body replaced, and it round-trips.
    check(`${name} body compressed`, ok && (name === 'ZSTD' ? msg.getBody().length > 0
      : msg.getBody().length < payload.length));
    check(`${name} sysFlag COMPRESSED+type`, MessageSysFlag.isCompressed(sysFlag)
      && MessageSysFlag.getCompressionType(sysFlag) === typeVal);
    const back = decompressBody(msg.getBody(), typeVal);
    check(`${name} roundtrip via decoder`, back.equals(payload));
  }
  // decodeMessage integration: encode a MessageExt with LZ4-flagged compressed body.
  const me = new MessageExt();
  me.setTopic('T'); me.setSysFlag(MessageSysFlag.setCompressionType(MessageSysFlag.COMPRESSED_FLAG, 1));
  me.setBody(lz4CompressFrame(payload));
  me.setQueueId(0); me.setQueueOffset(0); me.setCommitLogOffset(0);
  me.setBornTimestamp(0); me.setStoreTimestamp(0);
  me.setBornHost('127.0.0.1'); me.setStoreHost('127.0.0.1');
  me.setReconsumeTimes(0); me.setFlag(0); me.setBodyCrc(0);
  const raw = encodeMessageExt(me, false);
  const back = decodeMessage(raw, true, true);
  check('decodeMessage inflates LZ4-flagged body', back != null && back.getBody().equals(payload));
  const ze = new MessageExt();
  ze.setTopic('T'); ze.setSysFlag(MessageSysFlag.setCompressionType(MessageSysFlag.COMPRESSED_FLAG, 2));
  ze.setBody(zstdCompressRaw(payload));
  ze.setQueueId(0); ze.setQueueOffset(0); ze.setCommitLogOffset(0);
  ze.setBornTimestamp(0); ze.setStoreTimestamp(0);
  ze.setBornHost('127.0.0.1'); ze.setStoreHost('127.0.0.1');
  ze.setReconsumeTimes(0); ze.setFlag(0); ze.setBodyCrc(0);
  const zback = decodeMessage(encodeMessageExt(ze, false), true, true);
  check('decodeMessage inflates ZSTD-flagged body', zback != null && zback.getBody().equals(payload));

  // External real-compressed frame: Java's zstd-jni (and this repo's cpp / rust
  // / csharp ports) emit Compressed blocks, not Raw blocks. Failing to decode
  // them means a cross-language consumer silently gets nothing, so this is the
  // load-bearing interop assertion. Fixture = `zstd -3 --no-check` over the
  // payload above.
  const zstdCliFrame = Buffer.from(
    '28b52ffd60480ccd000088726f636b65746d712d7061796c6f61642d0100694afe5c02', 'hex');
  const hasNodeZstd = typeof (nodeZlib as unknown as {
    zstdDecompressSync?: unknown;
  }).zstdDecompressSync === 'function';
  if (hasNodeZstd) {
    check('zstd decodes an external Compressed-block frame', decompressFor(zstdCliFrame, 2).equals(payload));
  } else {
    let threw = false;
    try { decompressFor(zstdCliFrame, 2); } catch { threw = true; }
    check('zstd fallback rejects Compressed blocks instead of passing them through', threw);
  }
}

// ---------------------------------------------------------------- 4. trace
console.log('== trace wiring ==');
{
  const appended: any[] = [];
  const fakeDispatcher: any = { append: (ctx: any) => { appended.push(ctx); return true; } };
  const hook = new SendMessageTraceHookImpl(fakeDispatcher);
  const msg = new Message('T', Buffer.from('b'));
  const uniq = createUniqID();
  MessageAccessor.putProperty(msg, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniq);
  MessageAccessor.putProperty(msg, MessageConst.PROPERTY_KEYS, 'k1 k2');
  const ctx = new SendMessageContext('G', msg, new MessageQueue('T', 'b', 0), 0, '127.0.0.1:10911');
  hook.sendMessageBefore(ctx);
  const sendResult: any = {
    sendStatus: 'SEND_OK', msgId: uniq, messageQueue: new MessageQueue('T', 'b', 0),
    queueOffset: 0, transactionId: null, offsetMsgId: 'OFF', regionId: '', traceOn: true,
  };
  ctx.sendResult = sendResult;
  hook.sendMessageAfter(ctx);
  check('Pub record appended', appended.length === 1 && appended[0].traceType === 'Pub');
  check('Pub msgId = UNIQ_KEY (not undefined)', appended[0].traceBeans[0].msgId === uniq,
    `got '${appended[0].traceBeans[0].msgId}'`);

  // EndTransaction record via the dispatcher helper, then decode round-trip.
  const dispatcher = new AsyncTraceDispatcher('G', 'PRODUCER');
  const sink: any[] = [];
  (dispatcher as any)._producer = { send: async () => {} };
  dispatcher.append = (c: any) => { sink.push(c); return true; };
  dispatcher.appendEndTransaction('G', 'T', 'OFF1', 'tid-9', LocalTransactionState.COMMIT_MESSAGE, false, 'b:10911');
  check('EndTransaction record appended', sink.length === 1 && sink[0].traceType === 'EndTransaction');
  check('EndTransaction state name', sink[0].traceBeans[0].transactionState === 'COMMIT_MESSAGE');
  const { EncodeTraceContext } = await import('../src/client/trace_context.ts');
  const tb = EncodeTraceContext(sink[0]);
  const decoded = DecodeTraceDataString(tb!.transData);
  check('EndTransaction record decodes', decoded.length === 1
    && decoded[0].traceBeans[0].transactionId === 'tid-9');
}

// ---------------------------------------------------------------- 5. slave fallback
console.log('== slave fallback ==');
{
  // 5a. consumer-id list address selection (master preferred, else lowest id).
  const c = new DefaultMQPushConsumer('GID_SlaveSmoke');
  let askedAddr: string | null = null;
  (c as any).mqClient = {
    getTopicRouteData: (_t: string) => ({
      brokerDatas: [{ brokerName: 'b', brokerAddrs: { 1: '127.0.0.1:10913' } }],
    }),
    getConsumerListByGroup: async (addr: string, _g: string) => {
      askedAddr = addr;
      const { RemotingSerializable } = await import('../src/remoting/serialize.ts');
      return { code: 0, body: RemotingSerializable.encode({ consumerIdList: ['cid-1'] }) };
    },
  };
  const r = await (c as any)._getConsumerIdListByGroup('T');
  check('slave-only route still resolves member list', r.answered && r.cidAll[0] === 'cid-1');
  check('asked the SLAVE addr (id=1)', askedAddr === '127.0.0.1:10913', `asked ${askedAddr}`);

  // 5b. heartbeat / unregister enumeration covers slaves.
  const { MQClient } = await import('../src/client/mq_client.ts');
  const inst = new MQClient('cid', null, null);
  inst.brokerAddrTable.set('b', { 0: '127.0.0.1:10911', 1: '127.0.0.1:10913' });
  const entries = inst.getAllBrokerAddrEntries();
  check('all-broker entries include slave', entries.length === 2
    && entries.some((e) => e.id === 1 && e.addr === '127.0.0.1:10913'));
  let beats = 0;
  (inst as any).sendHeartbeat = async (_addr: string) => { beats++; return {}; };
  await (inst as any).sendHeartbeatToAllBrokers();
  check('heartbeat sent to slave too', beats === 2, `beats=${beats}`);
}

console.log(`\n${fail === 0 ? 'ALL GREEN' : fail + ' failure(s)'} (${pass} checks)`);
process.exit(fail === 0 ? 0 : 1);
