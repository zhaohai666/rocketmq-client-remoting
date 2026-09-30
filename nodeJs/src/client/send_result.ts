// -*- coding: utf-8 -*-
// Send result model + SendStatus enum (org.apache.rocketmq.client.producer.SendResult).
// Faithful port of python/rocketmq/client/send_result.py.
import { MessageQueue } from '../common/message.ts';

export const SendStatus = {
  SEND_OK: 0,
  FLUSH_DISK_TIMEOUT: 1,
  FLUSH_SLAVE_TIMEOUT: 2,
  SLAVE_NOT_AVAILABLE: 3,
  fromCode(code: number): number {
    switch (code) {
      case SendStatus.SEND_OK: return SendStatus.SEND_OK;
      case SendStatus.FLUSH_DISK_TIMEOUT: return SendStatus.FLUSH_DISK_TIMEOUT;
      case SendStatus.FLUSH_SLAVE_TIMEOUT: return SendStatus.FLUSH_SLAVE_TIMEOUT;
      case SendStatus.SLAVE_NOT_AVAILABLE: return SendStatus.SLAVE_NOT_AVAILABLE;
      default: return SendStatus.SEND_OK;
    }
  },
};

export class SendResult {
  sendStatus: number;
  msgId: string;
  messageQueue: MessageQueue | null;
  queueOffset: number;
  transactionId: string | null;
  offsetMsgId: string | null;
  regionId: string | null;
  traceOn: boolean;
  recallHandle: string | null;

  constructor(
    sendStatus: number = SendStatus.SEND_OK,
    msgId: string = '',
    messageQueue: MessageQueue | null = null,
    queueOffset: number = 0,
    transactionId: string | null = null,
    offsetMsgId: string | null = null,
    regionId: string | null = null,
    traceOn: boolean = true,
    recallHandle: string | null = null,
  ) {
    this.sendStatus = sendStatus;
    this.msgId = msgId;
    this.messageQueue = messageQueue;
    this.queueOffset = queueOffset;
    this.transactionId = transactionId;
    this.offsetMsgId = offsetMsgId;
    this.regionId = regionId;
    this.traceOn = traceOn;
    this.recallHandle = recallHandle;
  }

  getSendStatus(): number { return this.sendStatus; }
  setSendStatus(s: number): void { this.sendStatus = s; }
  getMsgId(): string { return this.msgId; }
  setMsgId(id: string): void { this.msgId = id; }
  getMessageQueue(): MessageQueue | null { return this.messageQueue; }
  setMessageQueue(mq: MessageQueue | null): void { this.messageQueue = mq; }
  getQueueOffset(): number { return this.queueOffset; }
  setQueueOffset(o: number): void { this.queueOffset = o; }
  getTransactionId(): string | null { return this.transactionId; }
  setTransactionId(id: string | null): void { this.transactionId = id; }
  getOffsetMsgId(): string | null { return this.offsetMsgId; }
  setOffsetMsgId(id: string | null): void { this.offsetMsgId = id; }
  getRegionId(): string | null { return this.regionId; }
  setRegionId(r: string | null): void { this.regionId = r; }
  isTraceOn(): boolean { return this.traceOn; }
  setTraceOn(t: boolean): void { this.traceOn = t; }
  getRecallHandle(): string | null { return this.recallHandle; }
  setRecallHandle(h: string | null): void { this.recallHandle = h; }

  toString(): string {
    return `SendResult [sendStatus=${this.sendStatus}, msgId=${this.msgId}, ` +
      `offsetMsgId=${this.offsetMsgId}, messageQueue=${this.messageQueue}, queueOffset=${this.queueOffset}, ` +
      `regionId=${this.regionId}, traceOn=${this.traceOn}, recallHandle=${this.recallHandle}]`;
  }
}

// Transaction send result (org.apache.rocketmq.client.producer.SendResult + transaction fields).
export class TransactionSendResult extends SendResult {
  localTransactionState: number;
  constructor(
    sendStatus: number = SendStatus.SEND_OK,
    msgId: string = '',
    messageQueue: MessageQueue | null = null,
    queueOffset: number = 0,
    transactionId: string | null = null,
    offsetMsgId: string | null = null,
    regionId: string | null = null,
    traceOn: boolean = true,
    recallHandle: string | null = null,
  ) {
    super(sendStatus, msgId, messageQueue, queueOffset, transactionId, offsetMsgId, regionId, traceOn, recallHandle);
    this.localTransactionState = 0;
  }
  getLocalTransactionState(): number { return this.localTransactionState; }
  setLocalTransactionState(s: number): void { this.localTransactionState = s; }
}

export default { SendStatus, SendResult, TransactionSendResult };
