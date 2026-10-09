// -*- coding: utf-8 -*-
// Request-reply support (org.apache.rocketmq.client.producer.RequestReplyProducer).
// Faithful port of python/client/request_reply.py, aligned with Java
// RequestResponseFuture / RequestFutureHolder (client/producer/*.java).
//
// A request producer sends a message carrying a correlation id + reply-to topic; the replying
// side creates a reply message and the future is resolved. This module holds the in-flight
// future table and the message helpers. Actual network round-trips are performed by the producer.
import { Message } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MessageAccessor } from '../common/message_accessor.ts';
import { MixAll } from '../common/mixAll.ts';
import { MQClientException } from '../remoting/exception.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.request_reply');

// A request callback is either a Java-style RequestCallback object
// (onSuccess(replyMsg) / onException(err)) or a node-style
// (replyMsg, err) => void function.
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
  // Java RequestResponseFuture state: the async callback (null for the sync
  // form), the reply message, whether the request itself was sent, the failure
  // cause, and the execute-request-callback-once latch.
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

  // Java putResponseMessage — stores the reply (the Java latch countDown is
  // node's promise resolution, done by complete()).
  putResponseMessage(responseMsg: Message | null): void {
    this.responseMsg = responseMsg;
  }

  setSendRequestOk(sendRequestOk: boolean): void { this.sendRequestOk = sendRequestOk; }
  setCause(cause: Error | null): void { this.cause = cause; }

  // Java executeRequestCallback: fire the async callback EXACTLY once (Java
  // guards with an AtomicBoolean CAS so the reply-arrival path and the
  // timeout-scan path cannot both deliver). onSuccess only when the request
  // was sent and no cause was recorded; otherwise onException.
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
  // Java producerSet + scheduledExecutorService: the TTL sweep runs while at
  // least one producer is started (3s initial delay, 1s interval).
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

  // Remove and return the future (mirrors Java's putResponse which removes).
  removeRequest(correlationId: string): RequestResponseFuture | null {
    const f = this.table.get(correlationId);
    if (f != null) this.table.delete(correlationId);
    return f != null ? f : null;
  }

  // Java RequestFutureHolder.scanExpiredRequest: remove every future whose TTL
  // elapsed, then fail its callback with a timeout cause. Removing BEFORE the
  // callback keeps the reply-arrival path (which also removes) from delivering
  // a late reply to an already-failed request.
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

  // Java startScheduledTask(DefaultMQProducerImpl): refcounted sweep. The first
  // scan runs 3s after the first producer starts; the interval is 1s.
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

  // Java shutdown(DefaultMQProducerImpl): drop one producer reference; stop the
  // sweep only when the last producer is gone.
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

// Build a reply message for the given request message — Java MessageUtil.
// createReplyMessage: the reply's TOPIC is the 5.x REPLY topic
// (`<cluster>_REPLY_TOPIC`, cluster read from the request's CLUSTER property,
// which the broker stamps on receipt); the requester's clientId rides along as
// the REPLY_TO_CLIENT property — the broker uses it to find the requestor's
// channel and push 326 directly. Throws when the request carries no CLUSTER
// property (Java does the same).
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
