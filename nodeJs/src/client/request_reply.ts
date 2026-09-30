// -*- coding: utf-8 -*-
// Request-reply support (org.apache.rocketmq.client.producer.RequestReplyProducer).
// Faithful port of python/rocketmq/client/request_reply.py.
//
// A request producer sends a message carrying a correlation id + reply-to topic; the replying
// side creates a reply message and the future is resolved. This module holds the in-flight
// future table and the message helpers. Actual network round-trips are performed by the producer.
import { Message } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MessageAccessor } from '../common/message_accessor.ts';
import { MixAll } from '../common/mixAll.ts';

let _correlationSeq = 0;
export function createCorrelationId(): string {
  _correlationSeq = (_correlationSeq + 1) & 0x7fffffff;
  return `${Date.now()}${_correlationSeq.toString(16)}`;
}

export class RequestResponseFuture {
  correlationId: string;
  requestMsg: Message;
  timeoutMillis: number;
  beginTimestamp: number;
  done: boolean;
  _resolve: ((msg: Message) => void) | null;
  _reject: ((err: Error) => void) | null;
  _promise: Promise<Message>;

  constructor(correlationId: string, requestMsg: Message, timeoutMillis: number) {
    this.correlationId = correlationId;
    this.requestMsg = requestMsg;
    this.timeoutMillis = timeoutMillis;
    this.beginTimestamp = Date.now();
    this.done = false;
    this._resolve = null;
    this._reject = null;
    this._promise = new Promise<Message>((resolve, reject) => {
      this._resolve = resolve;
      this._reject = reject;
    });
  }

  promise(): Promise<Message> { return this._promise; }

  complete(replyMsg: Message): void {
    if (this.done) return;
    this.done = true;
    if (this._resolve) this._resolve(replyMsg);
  }

  completeExceptionally(err: Error): void {
    if (this.done) return;
    this.done = true;
    if (this._reject) this._reject(err);
  }

  isDone(): boolean { return this.done; }

  isTimeout(): boolean {
    return Date.now() - this.beginTimestamp >= this.timeoutMillis;
  }
}

export class RequestFutureHolder {
  private static _instance: RequestFutureHolder | null = null;
  private table: Map<string, RequestResponseFuture> = new Map();

  static getInstance(): RequestFutureHolder {
    if (RequestFutureHolder._instance == null) {
      RequestFutureHolder._instance = new RequestFutureHolder();
    }
    return RequestFutureHolder._instance;
  }

  putRequest(correlationId: string, future: RequestResponseFuture): void {
    this.table.set(correlationId, future);
  }

  getRequest(correlationId: string): RequestResponseFuture | null {
    return this.table.get(correlationId) != null ? this.table.get(correlationId)! : null;
  }

  // Remove and return the future (mirrors Java's putResponse which removes).
  removeRequest(correlationId: string): RequestResponseFuture | null {
    const f = this.table.get(correlationId);
    if (f != null) this.table.delete(correlationId);
    return f != null ? f : null;
  }

  size(): number { return this.table.size; }
}

export const REQUEST_FUTURE_HOLDER = RequestFutureHolder.getInstance();

// Build a reply message for the given request message, copying the correlation id and reply-to.
export function createReplyMessage(requestMessage: Message, body: Buffer | string): Message {
  const reply = new Message();
  if (typeof body === 'string') reply.setBody(Buffer.from(body, 'utf8'));
  else reply.setBody(body);
  const correlationId = MessageAccessor.getProperty(requestMessage, MessageConst.PROPERTY_CORRELATION_ID);
  const replyTo = MessageAccessor.getProperty(requestMessage, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
  if (correlationId != null) MessageAccessor.putProperty(reply, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
  if (replyTo != null) reply.setTopic(replyTo);
  MessageAccessor.setMessageType(reply, 'Reply_Msg');
  return reply;
}

export function isReplyMessage(msg: Message): boolean {
  const t = MessageAccessor.getMessageType(msg);
  return t === 'Reply_Msg';
}

export abstract class RequestCallback {
  onSuccess(replyMsg: Message): void { void replyMsg; }
  onException(e: Error): void { void e; }
}

// Compose the reply topic for a cluster (used by producer.request).
export function getReplyTopic(clusterName: string): string {
  return MixAll.getReplyTopic(clusterName);
}

export default {
  createCorrelationId, RequestResponseFuture, RequestFutureHolder, REQUEST_FUTURE_HOLDER,
  createReplyMessage, isReplyMessage, RequestCallback, getReplyTopic,
};
