// Smoke test for the POP consumption surface (protocol layer + checkpoint
// rebuild + consumer-side accounting). Run:
//   node --experimental-strip-types test/pop_smoke.ts
//
// Wire-level POP against a real 5.5.1 broker is covered by the live scripts;
// this smoke pins the logic that makes every ACK addressable.
import assert from 'node:assert';
import { MessageExt } from '../src/common/message.ts';
import { encodeMessageExt } from '../src/common/messageDecoder.ts';
import { ResponseCode } from '../src/remoting/codes.ts';
import { PopMessageResponseHeader } from '../src/remoting/headers.ts';
import {
  BitSet, BatchAck, BatchAckMessageRequestBody, buildBatchAckMessageRequestBody,
  SetMessageRequestModeRequestBody, MESSAGE_REQUEST_MODE_POP,
} from '../src/remoting/pop_bodies.ts';
import { PopProcessQueue } from '../src/client/pop_process_queue.ts';
import {
  PopStatus, processPopResponse, POP_DELAY_LEVEL, popDelayLevelSeconds,
  popDelayLevelForElapsed, popBatchTimedOut, popInvisibleTimeOf,
  getRealTopicFromCk, ckParse,
} from '../src/client/pop_api.ts';
import { MixAll } from '../src/common/mixAll.ts';

// 1) Checkpoint rebuild, table path: the CK comes from startOffsetInfo /
//    msgOffsetInfo, segment 7 is the message's OWN offset, and the retry
//    marker is resolved from the message's (physical) topic.
{
  const msgs: MessageExt[] = [];
  for (let i = 0; i < 3; i++) {
    const m = new MessageExt('T', Buffer.from('m' + i));
    m.setQueueId(2);
    m.setQueueOffset(100 + i);
    msgs.push(m);
  }
  const body = Buffer.concat(msgs.map((m) => encodeMessageExt(m)));
  const rh = new PopMessageResponseHeader();
  rh.popTime = 1727700000000;
  rh.invisibleTime = 60000;
  rh.reviveQid = 1;
  rh.restNum = 5;
  rh.startOffsetInfo = '0 2 100';           // retry=0, queueId=2, startOffset=100
  rh.msgOffsetInfo = '0 2 100,101,102';
  const result = processPopResponse('broker-a', ResponseCode.SUCCESS, '', body, rh, 'T', '', false);
  assert.strictEqual(result.popStatus, PopStatus.FOUND);
  assert.strictEqual(result.restNum, 5);
  assert.strictEqual(result.msgFoundList.length, 3);
  for (let i = 0; i < 3; i++) {
    const msg = result.msgFoundList[i];
    const parts = msg.getProperty('POP_CK')!.split(' ');
    assert.strictEqual(parts.length, 8);
    assert.strictEqual(parts[0], '100');           // ckQueueOffset from the table
    assert.strictEqual(parts[1], '1727700000000');
    assert.strictEqual(parts[2], '60000');
    assert.strictEqual(parts[3], '1');
    assert.strictEqual(parts[4], '0');             // retry marker from topic T
    assert.strictEqual(parts[5], 'broker-a');
    assert.strictEqual(parts[6], '2');
    assert.strictEqual(parts[7], String(100 + i)); // THE ack offset
    assert.strictEqual(msg.getProperty('1ST_POP_TIME'), '1727700000000');
    assert.strictEqual(msg.getBrokerName(), 'broker-a');
  }
  console.log('OK: POP_CK rebuilt from offset tables, segment 7 = own offset');
}

// 2) Retry-topic messages keep the broker's armoured marker: the CK lookup key
//    uses the message's own topic (%RETRY%g_t -> marker 1), and the rebuilt CK
//    carries "1" so the later ACK resolves the %RETRY% topic.
{
  const m = new MessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + 'g_t', Buffer.from('r'));
  m.setQueueId(0);
  m.setQueueOffset(7);
  const body = encodeMessageExt(m);
  const rh = new PopMessageResponseHeader();
  rh.popTime = 1727700000000;
  rh.invisibleTime = 60000;
  rh.reviveQid = 0;
  rh.startOffsetInfo = '1 0 7';
  rh.msgOffsetInfo = '1 0 7';
  const result = processPopResponse('broker-a', ResponseCode.SUCCESS, '', body, rh, 'T', '', false);
  const parts = result.msgFoundList[0].getProperty('POP_CK')!.split(' ');
  assert.strictEqual(parts[4], '1');
  // ACK topic resolution through the marker:
  assert.strictEqual(getRealTopicFromCk(parts.join(' '), 'T', 'g'),
    MixAll.RETRY_GROUP_TOPIC_PREFIX + 'g_T');
  console.log('OK: retry-topic checkpoint keeps marker 1 and ACKs on %RETRY%');
}

// 3) No-table path: CK built from the message's own queue offset, cached per
//    (topic, queueId) — segment 0 equals the FIRST message's offset.
{
  const msgs: MessageExt[] = [];
  for (let i = 0; i < 2; i++) {
    const m = new MessageExt('T', Buffer.from('x' + i));
    m.setQueueId(1);
    m.setQueueOffset(50 + i);
    msgs.push(m);
  }
  const body = Buffer.concat(msgs.map((m) => encodeMessageExt(m)));
  const rh = new PopMessageResponseHeader();
  rh.popTime = 1727700000000;
  rh.invisibleTime = 60000;
  rh.reviveQid = 3;
  const result = processPopResponse('broker-b', ResponseCode.SUCCESS, '', body, rh, 'T', '', false);
  const p0 = result.msgFoundList[0].getProperty('POP_CK')!.split(' ');
  const p1 = result.msgFoundList[1].getProperty('POP_CK')!.split(' ');
  assert.strictEqual(p0[0], '50');      // cached batch-start
  assert.strictEqual(p1[0], '50');
  assert.strictEqual(p0[7], '50');      // per-message segment 7
  assert.strictEqual(p1[7], '51');
  console.log('OK: no-table path caches the batch-start checkpoint');
}

// 4) Status mapping: POLLING_FULL / POLLING_TIMEOUT / PULL_NOT_FOUND map to
//    PopStatus values and carry no messages.
{
  const rh = new PopMessageResponseHeader();
  assert.strictEqual(processPopResponse('b', ResponseCode.POLLING_FULL, '', null, rh, 'T', '', false).popStatus,
    PopStatus.POLLING_FULL);
  assert.strictEqual(processPopResponse('b', ResponseCode.POLLING_TIMEOUT, '', null, rh, 'T', '', false).popStatus,
    PopStatus.POLLING_NOT_FOUND);
  assert.strictEqual(processPopResponse('b', ResponseCode.PULL_NOT_FOUND, '', null, rh, 'T', '', false).popStatus,
    PopStatus.POLLING_NOT_FOUND);
  const empty = processPopResponse('b', ResponseCode.SUCCESS, '', null, rh, 'T', '', false);
  assert.strictEqual(empty.popStatus, PopStatus.FOUND);
  assert.strictEqual(empty.msgFoundList.length, 0);
  assert.throws(() => processPopResponse('b', 999, 'boom', null, rh, 'T', '', false), /999/);
  console.log('OK: POP status mapping');
}

// 5) Namespace stripping on the delivered topic.
{
  const m = new MessageExt('NS%T', Buffer.from('n'));
  m.setQueueId(0);
  m.setQueueOffset(1);
  const body = encodeMessageExt(m);
  const rh = new PopMessageResponseHeader();
  rh.popTime = 1727700000000;
  rh.invisibleTime = 60000;
  rh.reviveQid = 0;
  rh.startOffsetInfo = '0 0 1';
  rh.msgOffsetInfo = '0 0 1';
  const result = processPopResponse('b', ResponseCode.SUCCESS, '', body, rh, 'NS%T', 'NS', false);
  // The topic handed to the listener is the SUBSCRIBED topic with namespace
  // stripped (the request topic), not the physical topic.
  assert.strictEqual(result.msgFoundList[0].getTopic(), 'T');
  console.log('OK: namespace stripped on delivery');
}

// 6) Delay-level table: clamping and the two sentinels of the elapsed search.
{
  assert.strictEqual(POP_DELAY_LEVEL.length, 16);
  assert.strictEqual(popDelayLevelSeconds(0), 10);
  assert.strictEqual(popDelayLevelSeconds(3), 120);
  assert.strictEqual(popDelayLevelSeconds(-5), 10);              // clamped
  assert.strictEqual(popDelayLevelSeconds(99), 7200);            // clamped to last
  assert.strictEqual(popDelayLevelForElapsed(0), -1);            // below 10s: Java AIOOBE, caller clamps
  assert.strictEqual(popDelayLevelForElapsed(9_999), -1);
  assert.strictEqual(popDelayLevelForElapsed(10_000), 1);        // notch 0 elapsed -> wait notch 1
  assert.strictEqual(popDelayLevelForElapsed(120_000), 4);       // notch 3 (120s) elapsed -> wait notch 4
  assert.strictEqual(popDelayLevelForElapsed(7200_000), 16);     // beyond last -> clamped by Seconds()
  assert.strictEqual(popDelayLevelSeconds(popDelayLevelForElapsed(7200_000)), 7200);
  console.log('OK: POP delay-level table semantics');
}

// 7) Batch timeout: missing/short CK counts as timed out; a live window does not.
{
  const live = new MessageExt('T', Buffer.from('a'));
  live.setQueueId(0);
  live.putProperty('POP_CK', `${0} ${Date.now()} 60000 0 0 b 0 0`);
  assert.strictEqual(popBatchTimedOut([live]), false);
  assert.strictEqual(popInvisibleTimeOf([live]), 60000);

  const stale = new MessageExt('T', Buffer.from('b'));
  stale.putProperty('POP_CK', `${0} ${Date.now() - 61_000} 60000 0 0 b 0 0`);
  assert.strictEqual(popBatchTimedOut([stale]), true);

  const noCk = new MessageExt('T', Buffer.from('c'));
  assert.strictEqual(popBatchTimedOut([noCk]), true);
  const shortCk = new MessageExt('T', Buffer.from('d'));
  shortCk.putProperty('POP_CK', '0 1 2');
  assert.strictEqual(popBatchTimedOut([shortCk]), true);
  assert.strictEqual(popBatchTimedOut([]), true);
  console.log('OK: popBatchTimedOut guards');
}

// 8) PopProcessQueue: ack() returns the PRE-decrement value, decFoundMsg ADDS
//    (the "hand the debt back" undo path).
{
  const pq = new PopProcessQueue();
  assert.strictEqual(pq.waitAckMsgCount(), 0);
  pq.incFoundMsg(3);
  assert.strictEqual(pq.waitAckMsgCount(), 3);
  assert.strictEqual(pq.ack(), 3);   // pre-decrement
  assert.strictEqual(pq.ack(), 2);
  pq.decFoundMsg(-1);                // give the last one back
  assert.strictEqual(pq.waitAckMsgCount(), 0);
  assert.strictEqual(pq.isDropped(), false);
  pq.setDropped();
  assert.strictEqual(pq.isDropped(), true);
  assert.strictEqual(pq.isPullExpired(), false);
  pq.setLastPopTimestamp(Date.now() - 121_000);
  assert.strictEqual(pq.isPullExpired(), true);
  console.log('OK: PopProcessQueue ack/debt semantics');
}

// 9) BatchAck body: checkpoints sharing (retry, queueId, startOffset, popTime)
//    collapse into one entry; the bit index is msgQueueOffset - ckQueueOffset.
{
  const body = buildBatchAckMessageRequestBody('T', 'G', [
    '100 1727700000000 60000 0 0 b 2 100',
    '100 1727700000000 60000 0 0 b 2 101',
    '100 1727700000000 60000 0 0 b 2 103',
  ]);
  assert.strictEqual(body.brokerName, 'b');
  assert.strictEqual(body.acks.length, 1);
  const ack = body.acks[0];
  assert.strictEqual(ack.startOffset, 100);
  assert.strictEqual(ack.queueId, 2);
  assert.strictEqual(ack.popTime, 1727700000000);
  assert.strictEqual(ack.bitSet.popCount(), 3);

  // Wire form: JSON with the fastjson2 short keys, bitset base64.
  const decoded = JSON.parse(body.encode().toString('utf-8'));
  assert.strictEqual(decoded.brokerName, 'b');
  assert.strictEqual(decoded.acks[0].c, 'G');
  assert.strictEqual(decoded.acks[0].t, 'T');
  assert.strictEqual(decoded.acks[0].so, 100);
  assert.ok(decoded.acks[0].b.length > 0);

  // Round-trip the BitSet alone: base64 -> same bits.
  const bs = new BitSet();
  bs.set(0); bs.set(1); bs.set(7); bs.set(8); bs.set(9);
  const bs2 = new BitSet();
  bs2.decodeBase64(bs.encodeBase64());
  assert.strictEqual(bs2.popCount(), 5);
  console.log('OK: BatchAck grouping + BitSet round-trip');
}

// 10) SetMessageRequestMode body: enum NAME on the wire, PULL default.
{
  const b = new SetMessageRequestModeRequestBody();
  assert.strictEqual(b.mode, 'PULL');
  b.topic = 'T';
  b.consumerGroup = 'G';
  b.mode = MESSAGE_REQUEST_MODE_POP;
  b.popShareQueueNum = 0;
  const decoded = JSON.parse(b.encode().toString('utf-8'));
  assert.strictEqual(decoded.mode, 'POP');
  assert.strictEqual(decoded.topic, 'T');
  assert.strictEqual(decoded.popShareQueueNum, 0);
  console.log('OK: SET_MESSAGE_REQUEST_MODE body');
}

// 11) ckParse: full checkpoint parses, short ones return null (never ack).
{
  const parsed = ckParse('100 1727700000000 60000 0 0 b 2 100');
  assert.deepStrictEqual(parsed, { brokerName: 'b', queueId: 2, queueOffset: 100 });
  assert.strictEqual(ckParse('100 1727700000000'), null);
  assert.strictEqual(ckParse(''), null);
  console.log('OK: ckParse guards');
}

console.log('POP smoke: all assertions passed');
