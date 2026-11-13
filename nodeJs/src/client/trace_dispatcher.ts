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
  TraceContentSplitor, TraceTopicPrefix, AccessChannelLocal, AccessChannelCloud,
  DefaultTraceRegionId,
} from './trace_context.ts';
import { getLogger } from '../logging.ts';
import type { AclRPCHook } from '../remoting/acl.ts';

const logger = getLogger('client.trace_dispatcher');

export type TraceDispatcherType = 'PRODUCER' | 'CONSUME';

export class AsyncTraceDispatcher {
  groupName: string;
  traceType: TraceDispatcherType;
  batchNum: number;
  traceTopic: string;
  // Java AsyncTraceDispatcher#accessChannel (default LOCAL). CLOUD routes records to
  // TraceTopicPrefix + regionId; the facade's setAccessChannel value arrives via start().
  accessChannel: string = AccessChannelLocal;
  // The host's ACL hook. Java builds the internal trace producer with
  // `new DefaultMQProducer(rpcHook)`: without it an ACL-enabled broker rejects every
  // trace request and no record lands (neither in LOCAL nor in CLOUD mode).
  rpcHook: AclRPCHook | null = null;
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

  async start(nameSrvAddr?: string, accessChannel?: string): Promise<void> {
    if (nameSrvAddr) this._nameSrvAddr = nameSrvAddr;
    // Java AsyncTraceDispatcher#start(nameSrvAddr, accessChannel) (:151-159): remember
    // the channel even when the worker was already running. A blank argument keeps the
    // current value so repeated starts from a facade cannot silently reset CLOUD.
    if (accessChannel) this.accessChannel = accessChannel;
    if (this._running) return;
    this._running = true;
    try {
      const producer = await this.createInternalProducerForTest();
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

  // Test-only factory: build the internal producer exactly the way start() does, without
  // starting it. Kept public so the offline suite can assert the ACL hook / namespace are
  // really attached (a trace producer without the host's signature writes nothing).
  async createInternalProducerForTest(): Promise<any> {
    const { DefaultMQProducer } = await import('./producer.ts');
    // The internal producer group is named <group>_INNER_TRACE_PRODUCER.
    const producer = new DefaultMQProducer(`${this.groupName}${TraceGroupNamePrefix}`);
    producer.setNamesrvAddr(this._nameSrvAddr);
    // If the client configures namespaceV2, the internal trace producer must carry it too; otherwise trace rpcs become the only requests the broker cannot attribute to an instance.
    producer.setNamespaceV2(this.namespaceV2);
    // Java getAndCreateTraceProducer (:167-177) is `new DefaultMQProducer(rpcHook)`.
    // The host's ACL hook must sign trace requests as well, otherwise an ACL broker
    // rejects every send and the trace topic stays empty.
    producer.rpcHook = this.rpcHook;
    // The hook list is supposed to hang on each producer — the internal trace
    // producer's own hook list is empty, so its messages are not traced. On
    // the node side SendMessageHook is a global registry, so a skip flag
    // exempts the internal producer from the shared hooks; otherwise every
    // write to the trace topic would spawn one extra fake Pub record.
    (producer as any)._skipSendHooks = true;
    return producer;
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
      // The dispatcher's channel (Java falls back to it when a context carries none),
      // not a hard-coded LOCAL, otherwise a CLOUD facade's transaction records would
      // still land in RMQ_SYS_TRACE_TOPIC.
      ctx.accessChannel = this.accessChannel;
      ctx.regionId = brokerAddr || DefaultTraceRegionId;
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
  //
  // Java AsyncTraceDispatcher#sendTraceData (:309-334): every TraceContext picks its
  // own accessChannel, falling back to the dispatcher's when it carries none; CLOUD
  // sends to "rmq_sys_TRACE_DATA_" + regionId instead of traceTopic. Records are
  // grouped by (business topic, trace topic) BEFORE encoding/chunking, so a CLOUD
  // record can never be flushed into the LOCAL topic. Contexts with no region or no
  // beans are skipped (Java :316-318) -- every hook fills regionId for that reason.
  async flush(): Promise<void> {
    if (this._queue.length === 0) return;
    if (!this._producer) {
      this._queue = []; // no producer: drop, never block the data path
      return;
    }
    // Drain the queue under a snapshot; appends during the send are kept for
    // the next round.
    const batch = this._queue.splice(0, this._queue.length);
    interface TraceGroup { traceTopic: string; buffer: string; keys: Set<string>; }
    const groups = new Map<string, TraceGroup>();
    const sendChunk = async (g: TraceGroup): Promise<void> => {
      if (!g.buffer) return;
      const data = g.buffer;
      const chunkKeys = new Set(g.keys);
      g.buffer = '';
      g.keys.clear();
      const { Message } = await import('../common/message.ts');
      const msg = new Message(g.traceTopic, Buffer.from(data, 'utf-8'));
      if (chunkKeys.size > 0) msg.setKeys(Array.from(chunkKeys).slice(0, 100).join(' '));
      await this._producer.send(msg);
    };
    for (const ctx of batch) {
      const channel = ctx.accessChannel || this.accessChannel;
      const regionId = ctx.regionId;
      if (!regionId || !ctx.traceBeans || ctx.traceBeans.length === 0) continue;
      const traceTopic = channel === AccessChannelCloud
        ? TraceTopicPrefix + regionId
        : this.traceTopic;
      const businessTopic = ctx.traceBeans[0].topic || '';
      // TraceConstants.CONTENT_SPLITOR is the Java grouping delimiter.
      const key = businessTopic + TraceContentSplitor + traceTopic;
      let g = groups.get(key);
      if (!g) {
        g = { traceTopic, buffer: '', keys: new Set<string>() };
        groups.set(key, g);
      }
      const tb: TraceTransferBean | null = EncodeTraceContext(ctx);
      if (tb == null || !tb.transData) continue;
      g.buffer += tb.transData;
      for (const k of tb.transKey) g.keys.add(k);
      if (g.buffer.length >= this.maxMsgSize) {
        // One big batch exceeding the broker's maxMessageSize would be rejected.
        try { await sendChunk(g); } catch (e) {
          logger.debug('trace send failed (records dropped): %s', (e as Error).message);
        }
      }
    }
    for (const g of groups.values()) {
      if (!g.buffer) continue;
      try { await sendChunk(g); } catch (e) {
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
