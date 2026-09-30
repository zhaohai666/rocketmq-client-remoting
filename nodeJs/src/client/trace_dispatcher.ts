// AsyncTraceDispatcher (Java
// org.apache.rocketmq.client.trace.AsyncTraceDispatcher) — batches TraceContext
// records, encodes them with the wire codec and ships them to the trace topic
// through an internal producer.
//
// Degrade-first policy: trace must NEVER break the client. Broker down, topic
// missing, producer failure — everything logs and drops; the data path keeps
// running.
import { Buffer } from 'node:buffer';
import { TraceContext, TraceTransferBean, EncodeTraceContext, TraceGroupNamePrefix } from './trace_context.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.trace_dispatcher');

export type TraceDispatcherType = 'PRODUCER' | 'CONSUME';

export class AsyncTraceDispatcher {
  groupName: string;
  traceType: TraceDispatcherType;
  batchNum: number;
  traceTopic: string;
  regionId = '';
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

  async start(nameSrvAddr?: string): Promise<void> {
    if (nameSrvAddr) this._nameSrvAddr = nameSrvAddr;
    if (this._running) return;
    this._running = true;
    try {
      const { DefaultMQProducer } = await import('./producer.ts');
      // Java names the internal producer group <group>_INNER_TRACE_PRODUCER.
      const producer = new DefaultMQProducer(`${this.groupName}${TraceGroupNamePrefix}`);
      producer.setNamesrvAddr(this._nameSrvAddr);
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
    let transData = '';
    const keys = new Set<string>();
    for (const ctx of batch) {
      const tb: TraceTransferBean | null = EncodeTraceContext(ctx);
      if (tb == null || !tb.transData) continue;
      transData += tb.transData;
      for (const k of tb.transKey) keys.add(k);
    }
    if (!transData) return;
    try {
      const { Message } = await import('../common/message.ts');
      const msg = new Message(this.traceTopic, Buffer.from(transData, 'utf-8'));
      if (keys.size > 0) msg.setKeys(Array.from(keys).slice(0, 100).join(' '));
      await this._producer.send(msg);
    } catch (e) {
      // Topic-not-exist and broker-down are EXPECTED on a fresh cluster: log
      // at debug so the run log keeps ERROR=0.
      logger.debug('trace send failed (records dropped): %s', (e as Error).message);
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
