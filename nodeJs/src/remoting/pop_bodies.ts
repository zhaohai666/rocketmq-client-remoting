// -*- coding: utf-8 -*-
// POP wire bodies — port of go/client pop_bodies subset
// (Java SetMessageRequestModeRequestBody / BatchAck / BatchAckMessageRequestBody).
//
// NOTE (Java fidelity): the CLASSIC Java client never sends BATCH_ACK_MESSAGE
// (200151). Its POP path acks one message at a time through
// DefaultMQPushConsumerImpl#ackAsync. Batch ack is exported here because the
// wire capability is real (the broker implements it and the next-gen/proxy
// clients use it). Do NOT "finish" the POP consumer by routing its acks
// through this — that would be inventing client behaviour.
import { RemotingSerializable } from './serialize.ts';

export const MESSAGE_REQUEST_MODE_PULL = 'PULL';
export const MESSAGE_REQUEST_MODE_POP = 'POP';

// SetMessageRequestModeRequestBody is the body of SET_MESSAGE_REQUEST_MODE
// (401). The broker has no header for this code — everything rides in the body.
export class SetMessageRequestModeRequestBody {
  topic = '';
  consumerGroup = '';
  mode = MESSAGE_REQUEST_MODE_PULL; // Java's field default: a body that never set the mode asks for PULL.
  popShareQueueNum = 0;

  toDict(): Record<string, any> {
    return {
      topic: this.topic,
      consumerGroup: this.consumerGroup,
      mode: this.mode || MESSAGE_REQUEST_MODE_PULL,
      popShareQueueNum: this.popShareQueueNum,
    };
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
}

// java.util.BitSet over the wire: toByteArray() -> Base64. Only the bytes the
// highest set bit needs are sent (BitSet.toByteArray drops the zero tail).
export class BitSet {
  bytes: Uint8Array | null = null;

  // set(bitIndex): bit 0 is the LSB of byte 0 (Java's little-endian bit order).
  set(bitIndex: number): void {
    if (bitIndex < 0) throw new Error('bitIndex < 0: ' + bitIndex);
    const byteIndex = bitIndex >> 3;
    const need = byteIndex + 1;
    const cur = this.bytes && this.bytes.length > need ? this.bytes : new Uint8Array(need);
    if (this.bytes) cur.set(this.bytes.subarray(0, Math.min(this.bytes.length, cur.length)));
    cur[byteIndex] |= (1 << (bitIndex & 7)) & 0xff;
    this.bytes = cur;
  }

  popCount(): number {
    if (!this.bytes) return 0;
    let n = 0;
    for (const v of this.bytes) {
      let x = v;
      while (x !== 0) { n += x & 1; x >>= 1; }
    }
    return n;
  }

  encodeBase64(): string {
    if (!this.bytes || this.bytes.length === 0) return '';
    return Buffer.from(trimZeroTail(this.bytes)).toString('base64');
  }

  decodeBase64(encoded: string): void {
    if (!encoded) { this.bytes = null; return; }
    this.bytes = new Uint8Array(Buffer.from(encoded, 'base64'));
  }
}

function trimZeroTail(bytes: Uint8Array): Uint8Array {
  let end = bytes.length;
  while (end > 0 && bytes[end - 1] === 0) end--;
  return bytes.subarray(0, end);
}

// BatchAck mirrors Java org.apache.rocketmq.remoting.protocol.body.BatchAck.
// One entry covers every acknowledged message that shares the same
// (retry, queueId, startOffset, popTime) group. The short key names ("c","t",
// "r","so","q","rq","pt","it","b") are what fastjson2 writes on the wire.
export class BatchAck {
  consumerGroup = '';
  topic = '';
  retry = '';
  startOffset = 0;
  queueId = 0;
  reviveQueueId = 0;
  popTime = 0;
  invisibleTime = 0;
  bitSet = new BitSet();

  toDict(): Record<string, any> {
    return {
      c: this.consumerGroup, t: this.topic, r: this.retry, so: this.startOffset,
      q: this.queueId, rq: this.reviveQueueId, pt: this.popTime, it: this.invisibleTime,
      b: this.bitSet.encodeBase64(),
    };
  }
}

// BatchAckMessageRequestBody is the body of BATCH_ACK_MESSAGE (200151).
export class BatchAckMessageRequestBody {
  brokerName = '';
  acks: BatchAck[] = [];

  toDict(): Record<string, any> {
    return { brokerName: this.brokerName, acks: this.acks.map((a) => a.toDict()) };
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
}

// buildBatchAckMessageRequestBody mirrors the group-and-bitmap step of
// MQClientAPIImpl.batchAckMessageAsync: checkpoints sharing
// (retry, queueId, ckQueueOffset, popTime) collapse into one BatchAck, and the
// bit index is (msgQueueOffset - ckQueueOffset).
//
// topic/consumerGroup are the caller's; every other value is read back out of
// the checkpoint, never guessed.
export function buildBatchAckMessageRequestBody(
  topic: string, consumerGroup: string, extraInfos: string[],
): BatchAckMessageRequestBody {
  const body = new BatchAckMessageRequestBody();
  const index = new Map<string, BatchAck>();
  for (const extraInfo of extraInfos) {
    const parts = splitCk(extraInfo);
    if (parts.length < 8) throw new Error('batchAck: checkpoint too short: ' + extraInfo);
    const brokerName = parts[5];
    if (!body.brokerName) body.brokerName = brokerName;
    const retry = parts[4];
    const queueId = parseInt(parts[6], 10);
    const msgQueueOffset = parseInt(parts[7], 10);
    const ckQueueOffset = parseInt(parts[0], 10);
    const popTime = parseInt(parts[1], 10);
    const invisibleTime = parseInt(parts[2], 10);
    const reviveQueueId = parseInt(parts[3], 10);

    const key = [retry, queueId, ckQueueOffset, popTime].join('|');
    let ack = index.get(key);
    if (!ack) {
      ack = new BatchAck();
      ack.consumerGroup = consumerGroup;
      ack.topic = topic;
      ack.retry = retry;
      ack.startOffset = ckQueueOffset;
      ack.queueId = queueId;
      ack.reviveQueueId = reviveQueueId;
      ack.popTime = popTime;
      ack.invisibleTime = invisibleTime;
      index.set(key, ack);
      body.acks.push(ack);
    }
    ack.bitSet.set(msgQueueOffset - ckQueueOffset);
  }
  return body;
}

// Local split (trailing empties dropped) to avoid importing extra_info here.
function splitCk(extraInfo: string): string[] {
  const parts = extraInfo.split(' ');
  while (parts.length && parts[parts.length - 1] === '') parts.pop();
  return parts;
}
