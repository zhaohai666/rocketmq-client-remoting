// AsyncTraceDispatcher：批量攒 TraceContext 记录，用线上编解码拼好之后经内部
// producer 送进轨迹 topic。
//
// 降级优先：轨迹绝不能拖垮客户端。broker 掉线、topic 缺失、producer 失败——
// 全部记日志并丢弃，数据面继续跑。
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
  // namespaceV2 会随内部 trace producer 一起下发，让轨迹 rpcs 也带上 nsd/ns 扩展字段，
  // 便于 broker 侧按实例区分。
  namespaceV2: string | null = null;
  // trace 载荷按不超过 maxMsgSize 的大小切片发送；内部 trace producer 的
  // maxMessageSize 与这里取同值，默认 128000。
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
    // 默认轨迹 topic 为 RMQ_SYS_TRACE_TOPIC；显式指定 traceTopic 时覆盖默认。
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
      // 内部 producer group 走 <group>_INNER_TRACE_PRODUCER 命名。
      const producer = new DefaultMQProducer(`${this.groupName}${TraceGroupNamePrefix}`);
      producer.setNamesrvAddr(this._nameSrvAddr);
      // 若客户端配置了 namespaceV2，内部 trace producer 也必须带上，否则轨迹 rpcs
      // 就成了 broker 唯一无法归属到实例的请求。
      producer.setNamespaceV2(this.namespaceV2);
      // hook 列表本应挂在每个 producer 上——内部 trace producer 自己的 hook 列表是
      // 空的，它发的消息不再被 trace。node 端 SendMessageHook 是全局注册表，
      // 因此用一个 skip 标记把内部 producer 从共享 hook 里豁免掉；否则每一条写
      // trace topic 的动作都会再多出一条伪 Pub 记录。
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

  // appendEndTransaction 构造事务消息的 EndTransaction 记录。transactionState 使用
  // LocalTransactionState 枚举名（线上字面量）。
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
      // LocalTransactionState 序号 -> 状态名（供控制台查询使用）。
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
    // 累积编码记录，缓冲区每次触顶 maxMsgSize 就切片发出——一大块超过 broker
    // maxMessageSize 的批量会被 broker 拒收。
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
