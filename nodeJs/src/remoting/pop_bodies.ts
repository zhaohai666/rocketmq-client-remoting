// -*- coding: utf-8 -*-
// POP 协议 wire body：SetMessageRequestModeRequestBody / BatchAck /
// BatchAckMessageRequestBody。
//
// 注意：经典 remoting 协议的消费者从不发送 BATCH_ACK_MESSAGE（200151），
// 其 POP 路径通过 ackAsync 逐条确认。这里导出批量确认只是因为该 wire 能力
// 真实存在（broker 实现了它，新一代/proxy 客户端在使用）。不要把 POP 消费者的
// 确认改走这里——那等于凭空发明客户端行为。
import { RemotingSerializable } from './serialize.ts';

export const MESSAGE_REQUEST_MODE_PULL = 'PULL';
export const MESSAGE_REQUEST_MODE_POP = 'POP';

// SetMessageRequestModeRequestBody is the body of SET_MESSAGE_REQUEST_MODE
// (401). The broker has no header for this code — everything rides in the body.
export class SetMessageRequestModeRequestBody {
  topic = '';
  consumerGroup = '';
  mode = MESSAGE_REQUEST_MODE_PULL; // 字段默认值：未显式设置 mode 的 body 请求 PULL。
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

// java.util.BitSet 的 wire 形式：toByteArray() -> Base64。只发送最高置位
// 所需的字节（toByteArray 会丢弃尾部零字节）。
export class BitSet {
  bytes: Uint8Array | null = null;

  // set(bitIndex)：bit 0 是 byte 0 的最低位（协议的位序约定）。
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

// BatchAck：一条记录覆盖所有共享同一
// (retry, queueId, startOffset, popTime) 分组的已确认消息。短键名（"c","t",
// "r","so","q","rq","pt","it","b"）就是 wire 上的协议字段名。
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

// buildBatchAckMessageRequestBody 执行批量确认要求的分组+位图步骤：共享
// (retry, queueId, ckQueueOffset, popTime) 的 checkpoint 折叠为一条 BatchAck，
// 位索引为 (msgQueueOffset - ckQueueOffset)。
//
// topic/consumerGroup 由调用方给出；其余值全部从 checkpoint 读回，不做猜测。
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
// QUERY_ASSIGNMENT(400) 的 body：QueryAssignmentRequestBody /
// QueryAssignmentResponseBody + MessageQueueAssignment。双向纯 JSON。
// ---------------------------------------------------------------------------

export class QueryAssignmentRequestBody {
  topic = '';
  consumerGroup = '';
  clientId = '';
  messageModel = 'CLUSTERING';
  strategyName = '';
  // 协议还携带 extraInfo（POP 上下文）——总是发送，可以为空。
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
  // assignments 为 null 表示"结果无效，跳过本次更新"。空集合
  // （broker 明确没分配任何队列）则是一次真实应答。
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
