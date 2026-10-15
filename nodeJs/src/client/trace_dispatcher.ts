// AsyncTraceDispatcher: batches TraceContext records, assembles them with the
// wire codecs, and sends them into the trace topic via an internal producer.
//
// Degradation first: tracing must never take the client down. Broker offline,
// missing topic, producer failure — all are logged and dropped, and the data
// plane keeps running.
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
  // namespaceV2 goes out with the internal trace producer so trace rpcs also carry the nsd/ns ext fields, letting the broker side tell instances apart.
  namespaceV2: string | null = null;
  // The trace payload is sent in slices no larger than maxMsgSize; the internal trace producer's maxMessageSize takes the same value, default 128000.
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
    // The default trace topic is RMQ_SYS_TRACE_TOPIC; an explicit traceTopic overrides the default.
    this.traceTopic = traceTopic || 'RMQ_SYS_TRACE_TOPIC';
  }

  setHostConsumer(_hostConsumer: any): void { /* reserved for future use */ }
  setHostProducer(_hostProducer: any): void { /* reserved for future use */ }

  setNamespaceV2(namespaceV2: string | null): void { this.namespaceV2 = namespaceV2; }
  getNamespaceV2(): string | null { return this.namespaceV2; }

  async start(nameSrvAddr?: string): Promise<void> {
    if (nameSrvAddr) this._nameSrvAddr = nameSrvAddr;
    if (this._running) return;
    this._running = true;
    try {
      const { DefaultMQProducer } = await import('./producer.ts');
      // The internal producer group is named <group>_INNER_TRACE_PRODUCER.
      const producer = new DefaultMQProducer(`${this.groupName}${TraceGroupNamePrefix}`);
      producer.setNamesrvAddr(this._nameSrvAddr);
      // If the client configures namespaceV2, the internal trace producer must carry it too; otherwise trace rpcs become the only requests the broker cannot attribute to an instance.
      producer.setNamespaceV2(this.namespaceV2);
      // The hook list is supposed to hang on each producer — the internal trace
      // producer's own hook list is empty, so its messages are not traced. On
      // the node side SendMessageHook is a global registry, so a skip flag
      // exempts the internal producer from the shared hooks; otherwise every
      // write to the trace topic would spawn one extra fake Pub record.
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

  // appendEndTransaction builds a transactional message's EndTransaction
  // record. transactionState uses the LocalTransactionState enum name (a wire
  // literal).
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
      // LocalTransactionState ordinal -> state name (for console queries).
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
    // Accumulate encoded records, slicing and sending whenever the buffer hits
    // maxMsgSize — one big batch exceeding the broker's maxMessageSize would be
    // rejected by the broker.
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
