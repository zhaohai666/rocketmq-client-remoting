// -*- coding: utf-8 -*-
// POP protocol wire bodies: SetMessageRequestModeRequestBody / BatchAck /
// BatchAckMessageRequestBody.
//
// Note: consumers of the classic remoting protocol never send BATCH_ACK_MESSAGE (200151);
// their POP path acknowledges messages one by one via ackAsync. We still export batch ack only
// because that wire capability genuinely exists (the broker implements it, and newer/proxy clients
// use it). Do NOT route POP consumer acks here — that would invent client behavior out of thin air.
import { RemotingSerializable } from './serialize.ts';

export const MESSAGE_REQUEST_MODE_PULL = 'PULL';
export const MESSAGE_REQUEST_MODE_POP = 'POP';

// SetMessageRequestModeRequestBody is the body of SET_MESSAGE_REQUEST_MODE
// (401). The broker has no header for this code — everything rides in the body.
export class SetMessageRequestModeRequestBody {
  topic = '';
  consumerGroup = '';
  mode = MESSAGE_REQUEST_MODE_PULL; // Field default: a body without an explicit mode requests PULL.
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

// Wire form of java.util.BitSet: toByteArray() -> Base64. Only the bytes required by the highest set
// bit are sent (toByteArray drops trailing zero bytes).
export class BitSet {
  bytes: Uint8Array | null = null;

  // set(bitIndex): bit 0 is the least significant bit of byte 0 (the protocol's bit ordering convention).
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

// BatchAck: one record covers all acknowledged messages sharing the same
// (retry, queueId, startOffset, popTime) group. The short key names ("c","t",
// "r","so","q","rq","pt","it","b") are the protocol field names on the wire.
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

// buildBatchAckMessageRequestBody performs the grouping + bitmap steps required for batch ack:
// checkpoints sharing (retry, queueId, ckQueueOffset, popTime) collapse into one BatchAck,
// with the bit index (msgQueueOffset - ckQueueOffset).
//
// topic/consumerGroup are supplied by the caller; all other values are read back from the checkpoint, never guessed.
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

// ---------------------------------------------------------------------------
// QUERY_ASSIGNMENT(400) body: QueryAssignmentRequestBody /
// QueryAssignmentResponseBody + MessageQueueAssignment. Pure JSON in both directions.
// ---------------------------------------------------------------------------

export class QueryAssignmentRequestBody {
  topic = '';
  consumerGroup = '';
  clientId = '';
  messageModel = 'CLUSTERING';
  strategyName = '';
  // The protocol also carries extraInfo (POP context) — always sent, may be empty.
  extraInfo = '';

  toDict(): Record<string, any> {
    return {
      topic: this.topic,
      consumerGroup: this.consumerGroup,
      clientId: this.clientId,
      messageModel: this.messageModel,
      strategyName: this.strategyName,
      extraInfo: this.extraInfo,
    };
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
}

export class MessageQueueAssignment {
  mq: { topic: string; brokerName: string; queueId: number };
  mode: string;

  constructor(topic: string, brokerName: string, queueId: number, mode: string) {
    this.mq = { topic, brokerName, queueId };
    this.mode = mode;
  }
}

export class QueryAssignmentResponseBody {
  // assignments === null means "the result is invalid, skip this update". An empty set
  // (the broker explicitly assigned no queues) is a genuine response.
  static decode(body: Buffer | null): Array<MessageQueueAssignment> | null {
    if (body == null || body.length === 0) return null;
    let obj: any;
    try { obj = JSON.parse(body.toString('utf-8')); } catch (e) { return null; }
    const raw = obj != null && Array.isArray(obj['messageQueueAssignments'])
      ? obj['messageQueueAssignments']
      : (Array.isArray(obj) ? obj : null);
    if (raw == null) return null;
    const out: MessageQueueAssignment[] = [];
    for (const a of raw) {
      const mqa = a && a['messageQueue'] ? a['messageQueue'] : a;
      if (mqa == null || mqa['topic'] == null) continue;
      out.push(new MessageQueueAssignment(
        String(mqa['topic']),
        String(mqa['brokerName'] ?? ''),
        Number(mqa['queueId'] ?? 0) | 0,
        String(a && a['mode'] ? a['mode'] : 'PULL'),
      ));
    }
    return out;
  }
}
