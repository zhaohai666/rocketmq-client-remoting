// -*- coding: utf-8 -*-
// Hook interfaces + default no-op implementations + a shared registry.
// Faithful port of python/rocketmq/client/hook.py.
//
// Ordering contract (mirrors the Java client):
//   * CheckForbiddenHook runs INSIDE sendKernelImpl, right after compression/sysflag, and a
//     thrown exception is NOT swallowed (it aborts the send).
//   * SendMessageHook runs around the actual network send (before/after).
//   * FilterMessageHook is swallowed: any exception falls back to the original msg list.
import { Message } from '../common/message.ts';
import { MessageQueue } from '../common/message.ts';
import { SendResult } from './send_result.ts';

export const CommunicationMode = {
  SYNC: 0,
  ASYNC: 1,
  ONEWAY: 2,
};

export class SendMessageContext {
  producerGroup: string | null;
  message: Message | null;
  messageQueue: MessageQueue | null;
  brokerAddr: string | null;
  bornHost: string | null;
  communicationMode: number;
  sendResult: SendResult | null;
  exception: Error | null;
  msgId: string | null;
  namespace: string | null;

  constructor(producerGroup: string | null = null, message: Message | null = null,
    messageQueue: MessageQueue | null = null, communicationMode: number = CommunicationMode.SYNC,
    brokerAddr: string | null = null) {
    this.producerGroup = producerGroup;
    this.message = message;
    this.messageQueue = messageQueue;
    this.communicationMode = communicationMode;
    this.brokerAddr = brokerAddr;
    this.bornHost = null;
    this.sendResult = null;
    this.exception = null;
    this.msgId = null;
    this.namespace = null;
  }
}

export class ConsumeMessageContext {
  consumerGroup: string | null;
  msgList: any[] | null;
  topic: string | null;
  status: number | null;
  mq: MessageQueue | null;
  namespace: string | null;
  props: Record<string, string> | null;

  constructor(consumerGroup: string | null = null, msgList: any[] | null = null, topic: string | null = null) {
    this.consumerGroup = consumerGroup;
    this.msgList = msgList;
    this.topic = topic;
    this.status = null;
    this.mq = null;
    this.namespace = null;
    this.props = null;
  }
}

export class EndTransactionContext {
  producerGroup: string | null;
  message: Message | null;
  msgId: string | null;
  transactionId: string | null;
  brokerAddr: string | null;
  localTransactionState: number | null;
  fromTransactionCheck: boolean;

  constructor(producerGroup: string | null = null, message: Message | null = null, brokerAddr: string | null = null) {
    this.producerGroup = producerGroup;
    this.message = message;
    this.msgId = null;
    this.transactionId = null;
    this.brokerAddr = brokerAddr;
    this.localTransactionState = null;
    this.fromTransactionCheck = false;
  }
}

export class CheckForbiddenContext {
  namespace: string | null;
  producerGroup: string | null;
  topic: string | null;
  message: Message | null;
  brokerAddr: string | null;
  forbid: boolean;

  constructor(namespace: string | null = null, producerGroup: string | null = null, topic: string | null = null,
    message: Message | null = null) {
    this.namespace = namespace;
    this.producerGroup = producerGroup;
    this.topic = topic;
    this.message = message;
    this.brokerAddr = null;
    this.forbid = false;
  }
}

export class FilterMessageContext {
  namespace: string | null;
  consumerGroup: string | null;
  topic: string | null;
  msgList: any[] | null;
  unitMode: boolean;
  propertyKeys: string[] | null;

  constructor(consumerGroup: string | null = null, topic: string | null = null, msgList: any[] | null = null) {
    this.namespace = null;
    this.consumerGroup = consumerGroup;
    this.topic = topic;
    this.msgList = msgList;
    this.unitMode = false;
    this.propertyKeys = null;
  }
}

// ---- interfaces ----
export abstract class SendMessageHook {
  getHookName(): string { return 'SendMessageHook'; }
  abstract sendMessageBefore(ctx: SendMessageContext): void;
  abstract sendMessageAfter(ctx: SendMessageContext): void;
}

export abstract class ConsumeMessageHook {
  getHookName(): string { return 'ConsumeMessageHook'; }
  abstract consumeMessageBefore(ctx: ConsumeMessageContext): void;
  abstract consumeMessageAfter(ctx: ConsumeMessageContext): void;
}

export abstract class EndTransactionHook {
  getHookName(): string { return 'EndTransactionHook'; }
  abstract endTransactionBefore(ctx: EndTransactionContext): void;
  abstract endTransactionAfter(ctx: EndTransactionContext): void;
}

export abstract class CheckForbiddenHook {
  getHookName(): string { return 'CheckForbiddenHook'; }
  abstract checkForbidden(ctx: CheckForbiddenContext): void;
}

export abstract class FilterMessageHook {
  getHookName(): string { return 'FilterMessageHook'; }
  abstract filterMessage(ctx: FilterMessageContext): any[] | null;
}

// ---- default no-op implementations ----
export class DefaultSendMessageHook extends SendMessageHook {
  getHookName(): string { return 'DefaultSendMessageHook'; }
  sendMessageBefore(_ctx: SendMessageContext): void {}
  sendMessageAfter(_ctx: SendMessageContext): void {}
}

export class DefaultConsumeMessageHook extends ConsumeMessageHook {
  getHookName(): string { return 'DefaultConsumeMessageHook'; }
  consumeMessageBefore(_ctx: ConsumeMessageContext): void {}
  consumeMessageAfter(_ctx: ConsumeMessageContext): void {}
}

export class DefaultEndTransactionHook extends EndTransactionHook {
  getHookName(): string { return 'DefaultEndTransactionHook'; }
  endTransactionBefore(_ctx: EndTransactionContext): void {}
  endTransactionAfter(_ctx: EndTransactionContext): void {}
}

export class DefaultCheckForbiddenHook extends CheckForbiddenHook {
  getHookName(): string { return 'DefaultCheckForbiddenHook'; }
  checkForbidden(_ctx: CheckForbiddenContext): void {}
}

export class DefaultFilterMessageHook extends FilterMessageHook {
  getHookName(): string { return 'DefaultFilterMessageHook'; }
  filterMessage(ctx: FilterMessageContext): any[] | null {
    return ctx.msgList;
  }
}

// ---- shared registry ----
class HookRegistry {
  sendMessageHooks: SendMessageHook[] = [];
  consumeMessageHooks: ConsumeMessageHook[] = [];
  checkForbiddenHooks: CheckForbiddenHook[] = [];
  filterMessageHooks: FilterMessageHook[] = [];
  endTransactionHooks: EndTransactionHook[] = [];

  registerSendMessageHook(h: SendMessageHook): void { this.sendMessageHooks.push(h); }
  registerConsumeMessageHook(h: ConsumeMessageHook): void { this.consumeMessageHooks.push(h); }
  registerCheckForbiddenHook(h: CheckForbiddenHook): void { this.checkForbiddenHooks.push(h); }
  registerFilterMessageHook(h: FilterMessageHook): void { this.filterMessageHooks.push(h); }
  registerEndTransactionHook(h: EndTransactionHook): void { this.endTransactionHooks.push(h); }
}

export const hookRegistry = new HookRegistry();

export default {
  CommunicationMode, SendMessageContext, ConsumeMessageContext, EndTransactionContext,
  CheckForbiddenContext, FilterMessageContext, SendMessageHook, ConsumeMessageHook,
  EndTransactionHook, CheckForbiddenHook, FilterMessageHook,
  DefaultSendMessageHook, DefaultConsumeMessageHook, DefaultEndTransactionHook,
  DefaultCheckForbiddenHook, DefaultFilterMessageHook, hookRegistry,
};
