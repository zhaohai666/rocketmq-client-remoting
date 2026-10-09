// AsyncTraceDispatcher (Java
// org.apache.rocketmq.client.trace.AsyncTraceDispatcher) — batches TraceContext
// records, encodes them with the wire codec and ships them to the trace topic
// through an internal producer.
//
// Degrade-first policy: trace must NEVER break the client. Broker down, topic
// missing, producer failure — everything logs and drops; the data path keeps
// running.
import { Buffer } from 'node:buffer';
import {
  TraceContext, TraceBean, TraceType, TraceMsgType,
  TraceTransferBean, EncodeTraceContext, TraceGroupNamePrefix,
} from './trace_context.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.trace_dispatcher');

export type TraceDispatcherType = 'PRODUCER' | 'CONSUME';

export class AsyncTraceDispatcher {
  groupName: string;
  traceType: TraceDispatcherType;
  batchNum: number;
  traceTopic: string;
  regionId = '';
  // Java AsyncTraceDispatcher.namespaceV2 (+ get/set): propagated to the
  // internal trace producer in start() (Java AsyncTraceDispatcher:155) so the
  // trace rpcs carry the nsd/ns extFields too.
  namespaceV2: string | null = null;
  // Java AsyncTraceDispatcher.maxMsgSize (default 128000): the trace payload is
  // split into messages no larger than this (the internal trace producer's
  // maxMessageSize is set to the same value in Java).
  maxMsgSize = 128000;
  // The internal producer is created lazily on start() (zero-cost when trace
  // is disabled).
  private _producer: any = null;
  private _queue: TraceContext[] = [];
  private _queueMax = 2048;
  private _flushTimer: NodeJS.Timeout | null = null;
  private _running = false;
  private _nameSrvAddr = '';

  constructor(groupName: string, traceType: TraceDispatcherType = 'PRODUCER',
    batchNum = 10, traceTopic?: string) {
    this.groupName = groupName;
    this.traceType = traceType;
    this.batchNum = Math.min(Math.max(1, batchNum), 20);
    // Default trace topic is RMQ_SYS_TRACE_TOPIC; a custom topic overrides it
    // (Java's behaviour when setTraceTopicName is used).
    this.traceTopic = traceTopic || 'RMQ_SYS_TRACE_TOPIC';
  }

  setHostConsumer(_hostConsumer: any): void { /* reserved for future use */ }
  setHostProducer(_hostProducer: any): void { /* reserved for future use */ }

  // Java AsyncTraceDispatcher#getNamespaceV2 / #setNamespaceV2.
  setNamespaceV2(namespaceV2: string | null): void { this.namespaceV2 = namespaceV2; }
  getNamespaceV2(): string | null { return this.namespaceV2; }

  async start(nameSrvAddr?: string): Promise<void> {
    if (nameSrvAddr) this._nameSrvAddr = nameSrvAddr;
    if (this._running) return;
    this._running = true;
    try {
      const { DefaultMQProducer } = await import('./producer.ts');
      // Java names the internal producer group <group>_INNER_TRACE_PRODUCER.
      const producer = new DefaultMQProducer(`${this.groupName}${TraceGroupNamePrefix}`);
      producer.setNamesrvAddr(this._nameSrvAddr);
      // Java start():155 — traceProducer.setNamespaceV2(namespaceV2): without
      // this the trace writes of a namespaceV2 client are the only requests
      // the broker cannot attribute to the instance.
      producer.setNamespaceV2(this.namespaceV2);
      // Java keeps hook lists PER PRODUCER — the inner trace producer's own
      // list is empty, so its sends are NOT traced. node's SendMessageHook
      // registry is global; this flag restores Java's semantics by exempting
      // the inner producer from the shared hooks (otherwise every trace-topic
      // write would itself emit a spurious Pub record).
      (producer as any)._skipSendHooks = true;
      await producer.start();
      this._producer = producer;
    } catch (e) {
      logger.warning('trace internal producer start failed (trace disabled): %s', (e as Error).message);
      this._producer = null;
    }
    this._flushTimer = setInterval(() => {
      this.flush().catch((e) => logger.debug('trace flush error: %s', (e as Error).message));
    }, 5000);
    if (typeof this._flushTimer.unref === 'function') this._flushTimer.unref();
  }

  // append buffers one event; the queue is bounded so a slow broker cannot
  // grow memory without limit (oldest records are dropped first).
  append(ctx: TraceContext | null): boolean {
    if (ctx == null) return false;
    if (this._queue.length >= this._queueMax) {
      this._queue.shift();
    }
    this._queue.push(ctx);
    if (this._queue.length >= this.batchNum) {
      this.flush().catch((e) => logger.debug('trace flush error: %s', (e as Error).message));
    }
    return true;
  }

  // appendEndTransaction builds the EndTransaction record of a transaction
  // message (Java EndTransactionTraceHookImpl.endTransactionAfter). The
  // transactionState is the LocalTransactionState enum NAME (wire literal).
  appendEndTransaction(producerGroup: string, topic: string, msgId: string,
    transactionId: string | null, localTransactionState: number,
    fromTransactionCheck: boolean, brokerAddr: string): boolean {
    try {
      const ctx = new TraceContext();
      ctx.traceType = TraceType.END_TRANSACTION;
      ctx.timeStamp = Date.now();
      ctx.isSuccess = true;
      ctx.accessChannel = 'LOCAL';
      ctx.regionId = brokerAddr || '';
      ctx.groupName = producerGroup || '';
      const bean = new TraceBean();
      bean.topic = topic || '';
      bean.msgId = msgId || '';
      bean.msgType = TraceMsgType.TRANSACTION;
      bean.transactionId = transactionId || '';
      // LocalTransactionState ordinal -> Java enum name (TraceView lookups).
      bean.transactionState =
        localTransactionState === 0 ? 'COMMIT_MESSAGE'
          : localTransactionState === 1 ? 'ROLLBACK_MESSAGE' : 'UNKNOW';
      bean.fromTransactionCheck = fromTransactionCheck;
      ctx.traceBeans = [bean];
      return this.append(ctx);
    } catch (e) {
      logger.debug('append end-transaction trace failed: %s', (e as Error).message);
      return false;
    }
  }

  // flush encodes every buffered context and sends the joined payload. Each
  // chunk's message keys are the transKey set (the console resolves a
  // message's trace by these).
  async flush(): Promise<void> {
    if (this._queue.length === 0) return;
    if (!this._producer) {
      this._queue = []; // no producer: drop, never block the data path
      return;
    }
    // Drain the queue under a snapshot; appends during the send are kept for
    // the next round.
    const batch = this._queue.splice(0, this._queue.length);
    // Java AsyncDataSendTask.flushData: accumulate encoded records and send a
    // chunk every time the buffer reaches maxMsgSize — one oversized batch
    // would otherwise be rejected by the broker's max message size check.
    let buffer = '';
    const keys = new Set<string>();
    const sendChunk = async (data: string, chunkKeys: Set<string>): Promise<void> => {
      if (!data) return;
      const { Message } = await import('../common/message.ts');
      const msg = new Message(this.traceTopic, Buffer.from(data, 'utf-8'));
      if (chunkKeys.size > 0) msg.setKeys(Array.from(chunkKeys).slice(0, 100).join(' '));
      await this._producer.send(msg);
    };
    for (const ctx of batch) {
      const tb: TraceTransferBean | null = EncodeTraceContext(ctx);
      if (tb == null || !tb.transData) continue;
      buffer += tb.transData;
      for (const k of tb.transKey) keys.add(k);
      if (buffer.length >= this.maxMsgSize) {
        const chunk = buffer;
        const chunkKeys = keys;
        buffer = '';
        keys.clear();
        try { await sendChunk(chunk, chunkKeys); } catch (e) {
          logger.debug('trace send failed (records dropped): %s', (e as Error).message);
        }
      }
    }
    if (buffer) {
      try { await sendChunk(buffer, keys); } catch (e) {
        logger.debug('trace send failed (records dropped): %s', (e as Error).message);
      }
    }
  }

  async shutdown(): Promise<void> {
    if (!this._running) return;
    this._running = false;
    if (this._flushTimer) { clearInterval(this._flushTimer); this._flushTimer = null; }
    await this.flush(); // last records must land before the producer dies
    if (this._producer) {
      try { this._producer.shutdown(); } catch (e) { /* ignore */ }
      this._producer = null;
    }
  }
}
