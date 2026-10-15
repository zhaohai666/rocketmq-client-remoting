// -*- coding: utf-8 -*-
// Request-response support.
//
// A request producer's message carries a correlation id and a reply-to topic;
// the responder generates a reply message from them and settles the
// corresponding future. This module only maintains the in-flight future table
// and message helpers; the actual network round trip is done by the producer.
import { Message } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MessageAccessor } from '../common/message_accessor.ts';
import { MixAll } from '../common/mixAll.ts';
import { MQClientException } from '../remoting/exception.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.request_reply');

// Request callback: either a RequestCallback object (onSuccess/onException)
// or a node-style (replyMsg, err) => void function.
export type RequestCallbackLike = RequestCallback |
  ((replyMsg: Message | null, err: Error | null) => void);

let _correlationSeq = 0;
export function createCorrelationId(): string {
  _correlationSeq = (_correlationSeq + 1) & 0x7fffffff;
  return `${Date.now()}${_correlationSeq.toString(16)}`;
}

export class RequestResponseFuture {
  correlationId: string;
  requestMsg: Message | null;
  timeoutMillis: number;
  beginTimestamp: number;
  done: boolean;
  // The async callback (null in the synchronous form), the reply message,
  // whether the request was sent, the failure cause, and the "callback runs
  // exactly once" latch.
  requestCallback: RequestCallbackLike | null;
  responseMsg: Message | null;
  sendRequestOk: boolean;
  cause: Error | null;
  private _callbackExecuted: boolean;
  _resolve: ((msg: Message) => void) | null;
  _reject: ((err: Error) => void) | null;
  _promise: Promise<Message>;

  constructor(correlationId: string, requestMsg: Message | null, timeoutMillis: number,
    requestCallback: RequestCallbackLike | null = null) {
    this.correlationId = correlationId;
    this.requestMsg = requestMsg;
    this.timeoutMillis = timeoutMillis;
    this.beginTimestamp = Date.now();
    this.done = false;
    this.requestCallback = requestCallback;
    this.responseMsg = null;
    this.sendRequestOk = true;
    this.cause = null;
    this._callbackExecuted = false;
    this._resolve = null;
    this._reject = null;
    this._promise = new Promise<Message>((resolve, reject) => {
      this._resolve = resolve;
      this._reject = reject;
    });
  }

  promise(): Promise<Message> { return this._promise; }

  // Stores the reply message; the latch here is carried by the promise settling, triggered by complete().
  putResponseMessage(responseMsg: Message | null): void {
    this.responseMsg = responseMsg;
  }

  setSendRequestOk(sendRequestOk: boolean): void { this.sendRequestOk = sendRequestOk; }
  setCause(cause: Error | null): void { this.cause = cause; }

  // Fires the async callback EXACTLY ONCE: guaranteed by the _callbackExecuted
  // latch — the reply-arrival path and the timeout-scan path cannot both
  // deliver. onSuccess runs only when the request was sent and no cause is
  // recorded; otherwise onException runs.
  executeRequestCallback(): void {
    const cb: any = this.requestCallback;
    if (cb == null) return;
    if (this._callbackExecuted) return;
    this._callbackExecuted = true;
    if (this.sendRequestOk && this.cause == null) {
      if (typeof cb === 'function') cb(this.responseMsg, null);
      else cb.onSuccess(this.responseMsg);
    } else {
      const err = this.cause != null ? this.cause : new Error('request failed');
      if (typeof cb === 'function') cb(null, err);
      else cb.onException(err);
    }
  }

  complete(replyMsg: Message): void {
    if (this.done) return;
    this.done = true;
    this.responseMsg = replyMsg;
    if (this._resolve) this._resolve(replyMsg);
    this.executeRequestCallback();
  }

  completeExceptionally(err: Error): void {
    if (this.done) return;
    this.done = true;
    this.cause = err;
    if (this._reject) this._reject(err);
    this.executeRequestCallback();
  }

  isDone(): boolean { return this.done; }

  isTimeout(): boolean {
    return Date.now() - this.beginTimestamp > this.timeoutMillis;
  }
}

export class RequestFutureHolder {
  private static _instance: RequestFutureHolder | null = null;
  private table: Map<string, RequestResponseFuture> = new Map();
  // TTL scan: periodically clean up timed-out requests as long as at least one producer is running (3s initial delay, 1s interval).
  private producerSet: Set<any> = new Set();
  private _sweepTimer: NodeJS.Timeout | null = null;

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

  // Take the future out and remove it from the table (once the reply arrives, tracking is no longer needed).
  removeRequest(correlationId: string): RequestResponseFuture | null {
    const f = this.table.get(correlationId);
    if (f != null) this.table.delete(correlationId);
    return f != null ? f : null;
  }

  // scanExpiredRequest: remove every TTL-expired future from the table first,
  // then fire their callbacks uniformly with a timeout cause. REMOVE FROM
  // TABLE BEFORE CALLBACK — the reply-arrival path also removes from the
  // table, so removing first avoids a late reply re-delivering after the
  // request was already marked failed.
  scanExpiredRequest(): void {
    const expired: RequestResponseFuture[] = [];
    for (const [key, rep] of this.table.entries()) {
      if (rep.isTimeout()) {
        this.table.delete(key);
        expired.push(rep);
        logger.warning('remove timeout request, CorrelationId=%s', rep.correlationId);
      }
    }
    for (const rf of expired) {
      try {
        rf.setCause(new MQClientException('request timeout, no reply message.'));
        rf.executeRequestCallback();
      } catch (e) {
        logger.warning('scanExpiredRequest callback error: %s', (e as Error).message);
      }
    }
  }

  // Reference-counted TTL scan: the first round runs 3s after the first producer starts, then every 1s.
  startScheduledTask(producer: any): void {
    this.producerSet.add(producer);
    if (this._sweepTimer == null) {
      const tick = () => {
        try { this.scanExpiredRequest(); } catch (e) { /* never break the client */ }
        this._sweepTimer = setTimeout(tick, 1000);
        if (typeof this._sweepTimer.unref === 'function') this._sweepTimer.unref();
      };
      this._sweepTimer = setTimeout(tick, 3000);
      if (typeof this._sweepTimer.unref === 'function') this._sweepTimer.unref();
    }
  }

  // Drop one producer reference; the TTL scan stops only when the last producer goes away.
  shutdown(producer: any): void {
    this.producerSet.delete(producer);
    if (this.producerSet.size <= 0 && this._sweepTimer != null) {
      clearTimeout(this._sweepTimer);
      this._sweepTimer = null;
    }
  }

  size(): number { return this.table.size; }
}

export const REQUEST_FUTURE_HOLDER = RequestFutureHolder.getInstance();

// Build a reply message from the request message: the reply's TOPIC is 5.x's
// REPLY topic (`<cluster>_REPLY_TOPIC`, where cluster comes from the request's
// CLUSTER property, stamped in by the broker when the request arrives); the
// requester's clientId rides along via the REPLY_TO_CLIENT property, and the
// broker uses it to find the requester's channel and push 326 straight back.
// Throws when the request lacks the CLUSTER property.
export function createReplyMessage(requestMessage: Message, body: Buffer | string): Message {
  const cluster = MessageAccessor.getProperty(requestMessage, MessageConst.PROPERTY_CLUSTER);
  if (cluster == null || cluster === '') {
    throw new MQClientException('create reply message fail, requestMessage error, property[CLUSTER] is null.');
  }
  const replyTo = MessageAccessor.getProperty(requestMessage, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
  const correlationId = MessageAccessor.getProperty(requestMessage, MessageConst.PROPERTY_CORRELATION_ID);
  const ttl = MessageAccessor.getProperty(requestMessage, MessageConst.PROPERTY_MESSAGE_TTL);
  const reply = new Message();
  if (typeof body === 'string') reply.setBody(Buffer.from(body, 'utf8'));
  else reply.setBody(body);
  reply.setTopic(MixAll.getReplyTopic(cluster));
  MessageAccessor.putProperty(reply, MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG);
  if (correlationId != null) MessageAccessor.putProperty(reply, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
  if (replyTo != null) MessageAccessor.putProperty(reply, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, replyTo);
  if (ttl != null) MessageAccessor.putProperty(reply, MessageConst.PROPERTY_MESSAGE_TTL, ttl);
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
