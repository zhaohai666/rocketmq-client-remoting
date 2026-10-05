// -*- coding: utf-8 -*-
// Consumer-side result models (org.apache.rocketmq.client.consumer.*).
// Faithful port of python/client/consumer_result.py.
import { MessageQueue } from '../common/message.ts';
import { MessageExt } from '../common/message.ts';

export const PullStatus = {
  FOUND: 0,
  NO_NEW_MSG: 1,
  NO_MATCHED_MSG: 2,
  OFFSET_ILLEGAL: 3,
  fromCode(code: number): number {
    return code;
  },
};

export class PullResult {
  status: number;
  nextBeginOffset: number;
  minOffset: number;
  maxOffset: number;
  msgFoundList: MessageExt[] | null;
  suggestWhichBrokerId: number;

  constructor(
    status: number,
    nextBeginOffset: number,
    minOffset: number,
    maxOffset: number,
    msgFoundList: MessageExt[] | null,
    suggestWhichBrokerId: number = 0,
  ) {
    this.status = status;
    this.nextBeginOffset = nextBeginOffset;
    this.minOffset = minOffset;
    this.maxOffset = maxOffset;
    this.msgFoundList = msgFoundList;
    this.suggestWhichBrokerId = suggestWhichBrokerId;
  }

  getStatus(): number { return this.status; }
  setStatus(s: number): void { this.status = s; }
  getNextBeginOffset(): number { return this.nextBeginOffset; }
  setNextBeginOffset(o: number): void { this.nextBeginOffset = o; }
  getMinOffset(): number { return this.minOffset; }
  setMinOffset(o: number): void { this.minOffset = o; }
  getMaxOffset(): number { return this.maxOffset; }
  setMaxOffset(o: number): void { this.maxOffset = o; }
  getMsgFoundList(): MessageExt[] | null { return this.msgFoundList; }
  setMsgFoundList(l: MessageExt[] | null): void { this.msgFoundList = l; }
  getSuggestWhichBrokerId(): number { return this.suggestWhichBrokerId; }
  setSuggestWhichBrokerId(id: number): void { this.suggestWhichBrokerId = id; }
}

export const ConsumeConcurrentlyStatus = {
  CONSUME_SUCCESS: 0,
  RECONSUME_LATER: 1,
};

export const ConsumeOrderlyStatus = {
  SUCCESS: 0,
  ROLLBACK: 1,
  COMMIT: 2,
  SUSPEND_CURRENT_QUEUE_A_MOMENT: 3,
};

export const consumeStatusName = {
  [ConsumeConcurrentlyStatus.CONSUME_SUCCESS]: 'CONSUME_SUCCESS',
  [ConsumeConcurrentlyStatus.RECONSUME_LATER]: 'RECONSUME_LATER',
  [ConsumeOrderlyStatus.SUCCESS]: 'SUCCESS',
  [ConsumeOrderlyStatus.ROLLBACK]: 'ROLLBACK',
  [ConsumeOrderlyStatus.COMMIT]: 'COMMIT',
  [ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT]: 'SUSPEND_CURRENT_QUEUE_A_MOMENT',
};

export class ConsumeConcurrentlyContext {
  messageQueue: MessageQueue;
  delayLevelWhenNextConsume: number;
  ackIndex: number;
  constructor(messageQueue: MessageQueue, delayLevelWhenNextConsume: number = 0) {
    this.messageQueue = messageQueue;
    this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    this.ackIndex = Math.pow(2, 31) - 1;
  }
  getMessageQueue(): MessageQueue { return this.messageQueue; }
  setMessageQueue(mq: MessageQueue): void { this.messageQueue = mq; }
  getDelayLevelWhenNextConsume(): number { return this.delayLevelWhenNextConsume; }
  setDelayLevelWhenNextConsume(d: number): void { this.delayLevelWhenNextConsume = d; }
  getAckIndex(): number { return this.ackIndex; }
  setAckIndex(i: number): void { this.ackIndex = i; }
}

export class ConsumeOrderlyContext {
  messageQueue: MessageQueue;
  autoCommit: boolean;
  suspendCurrentQueueTimeMillis: number;
  constructor(messageQueue: MessageQueue, autoCommit: boolean = true) {
    this.messageQueue = messageQueue;
    this.autoCommit = autoCommit;
    this.suspendCurrentQueueTimeMillis = -1;
  }
  getMessageQueue(): MessageQueue { return this.messageQueue; }
  setMessageQueue(mq: MessageQueue): void { this.messageQueue = mq; }
  isAutoCommit(): boolean { return this.autoCommit; }
  setAutoCommit(a: boolean): void { this.autoCommit = a; }
  getSuspendCurrentQueueTimeMillis(): number { return this.suspendCurrentQueueTimeMillis; }
  setSuspendCurrentQueueTimeMillis(t: number): void { this.suspendCurrentQueueTimeMillis = t; }
}

// Message selector (org.apache.rocketmq.client.consumer.MessageSelector).
export class MessageSelector {
  type: string;
  expression: string;
  constructor(type: string, expression: string) {
    this.type = type;
    this.expression = expression;
  }
  static TAG(expression: string): MessageSelector {
    return new MessageSelector('TAG', expression);
  }
  static SQL92(expression: string): MessageSelector {
    return new MessageSelector('SQL92', expression);
  }
  getType(): string { return this.type; }
  getExpression(): string { return this.expression; }
}

// MessageQueueListener (org.apache.rocketmq.client.consumer.listener.MessageQueueListener).
export abstract class MessageQueueListener {
  // Subclasses override.
  messageQueueChanged(topic: string, mqAll: MessageQueue[], mqDivided: MessageQueue[]): void {
    void topic; void mqAll; void mqDivided;
  }
}

// MessageListener interfaces (callback shapes invoked by the consumer agent).
export abstract class MessageListenerConcurrently {
  consumeMessage(msgs: MessageExt[], context: ConsumeConcurrentlyContext): number {
    void msgs; void context;
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  }
}

export abstract class MessageListenerOrderly {
  consumeMessage(msgs: MessageExt[], context: ConsumeOrderlyContext): number {
    void msgs; void context;
    return ConsumeOrderlyStatus.SUCCESS;
  }
}

export abstract class MessageListener {
  consumeMessage(msgs: MessageExt[], context: any): number {
    void msgs; void context;
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  }
}

export default {
  PullStatus, PullResult, ConsumeConcurrentlyStatus, ConsumeOrderlyStatus, consumeStatusName,
  ConsumeConcurrentlyContext, ConsumeOrderlyContext, MessageSelector, MessageQueueListener,
  MessageListenerConcurrently, MessageListenerOrderly, MessageListener,
};
