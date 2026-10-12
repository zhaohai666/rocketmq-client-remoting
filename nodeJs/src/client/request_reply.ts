// -*- coding: utf-8 -*-
// 请求-响应支持。
//
// 请求 producer 发出的消息带一个关联 id 和 reply-to topic；应答端据此生成一条
// 应答消息、把对应 future 兑现。本模块只维护在途 future 表和消息辅助函数，真正的
// 网络往返由 producer 完成。
import { Message } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MessageAccessor } from '../common/message_accessor.ts';
import { MixAll } from '../common/mixAll.ts';
import { MQClientException } from '../remoting/exception.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.request_reply');

// 请求回调：既可以是 RequestCallback 对象（onSuccess/onException），
// 也可以是 node 风格的 (replyMsg, err) => void 函数。
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
  // 异步回调（同步形态下为 null）、应答消息、请求是否已发出、失败原因，
  // 以及"回调只跑一次"闩。
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

  // 存放应答消息；闩在这里由 promise 兑现来承担，由 complete() 触发。
  putResponseMessage(responseMsg: Message | null): void {
    this.responseMsg = responseMsg;
  }

  setSendRequestOk(sendRequestOk: boolean): void { this.sendRequestOk = sendRequestOk; }
  setCause(cause: Error | null): void { this.cause = cause; }

  // 触发异步回调**恰好一次**：由 _callbackExecuted 闩保证——应答到达路径与超时扫描
  // 路径不会都投递。请求已发出且没记录 cause 时才走 onSuccess，否则走 onException。
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
  // TTL 扫描：只要至少还有一个 producer 在跑就定时清理超时请求（首次延迟 3s，间隔 1s）。
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

  // 取出并把 future 从表里移除（应答到达即不再需要跟踪）。
  removeRequest(correlationId: string): RequestResponseFuture | null {
    const f = this.table.get(correlationId);
    if (f != null) this.table.delete(correlationId);
    return f != null ? f : null;
  }

  // scanExpiredRequest：把 TTL 到期的 future 全部先摘表，再统一以超时 cause 触发回调。
  // **先摘表再回调**——应答到达路径也会摘表，先摘能避免已经把请求标失败之后又被
  // 迟到应答二次投递。
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

  // 引用计数的 TTL 扫描：首个 producer 启动后 3s 跑第一轮，之后每 1s 一轮。
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

  // 少一个 producer 引用；只有最后一个 producer 走掉才停 TTL 扫描。
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

// 依据请求消息构造应答消息：应答的 TOPIC 是 5.x 的 REPLY topic
// （`<cluster>_REPLY_TOPIC`，cluster 取自请求的 CLUSTER 属性，由 broker 在收到请求
// 时盖章写入）；请求方的 clientId 通过 REPLY_TO_CLIENT 属性带过去，broker 用它
// 找到请求者所在的 channel 直接推回 326。请求缺少 CLUSTER 属性时抛错。
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
