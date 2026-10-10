// -*- coding: utf-8 -*-
// DefaultMQProducer — the default message producer (org.apache.rocketmq.client.producer.*).
// Faithful port of python/client/producer.py built on top of MQClient.
//
// Send ordering (mirrors Java, enforced by the cross-port rules):
//   send() -> _send_default_impl (retry loop) -> _send_with_hooks (SendMessageHook before/after)
//         -> sendKernelImpl (compress -> set UNIQ_KEY -> CheckForbiddenHook [NOT swallowed]
//            -> build request -> resolve broker addr -> actual transport via MQClient).
import { RemotingClient } from '../remoting/client.ts';
import { RemotingCommand } from '../remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { EndTransactionRequestHeader } from '../remoting/headers.ts';
import { Message, MessageQueue, MessageBatch, MessageExt } from '../common/message.ts';
import { MessageSysFlag } from '../common/sysflag.ts';
import { compressionTypeByName, compressFor } from '../common/compress.ts';
import { decodeMessageId } from '../common/messageDecoder.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MessageAccessor } from '../common/message_accessor.ts';
import { MessageType } from '../common/messageType.ts';
import { MixAll } from '../common/mixAll.ts';
import { NamespaceUtil } from '../remoting/namespace.ts';
import { createUniqID } from '../common/messageClientIdSetter.ts';
import { TopicValidator } from '../common/topic_validator.ts';
import { Validators } from '../common/validators.ts';
import { RecallMessageHandle } from '../common/recall_message_handle.ts';
import { AclRPCHook } from '../remoting/acl.ts';
import { registerRpcHooks } from '../remoting/rpc_hooks.ts';
import { getLogger } from '../logging.ts';
import {
  MQClientException, MQBrokerException,
} from '../remoting/exception.ts';
import { SendResult, TransactionSendResult, SendStatus } from './send_result.ts';
import { MQClient, TopicPublishInfo } from './mq_client.ts';
import {
  CommunicationMode, SendMessageContext, CheckForbiddenContext, SendMessageHook, hookRegistry,
} from './hook.ts';
import { MQFaultStrategy, tcpDetect } from './latency.ts';
import { getOrCreateProduceAccumulator, ProduceAccumulator } from './produce_accumulator.ts';
import { FairSemaphore } from './backpressure.ts';
import { injectTraceContext, traceContextEnabledFromEnv } from './traceparent.ts';
import {
  TraceContext, TraceBean, TraceType, TraceMsgType,
} from './trace_context.ts';
import type { AsyncTraceDispatcher } from './trace_dispatcher.ts';
import {
  createCorrelationId, createReplyMessage, isReplyMessage,
  REQUEST_FUTURE_HOLDER, getReplyTopic, RequestResponseFuture,
} from './request_reply.ts';
// 类型必须单独 import：--experimental-strip-types 会把非 `import type` 的导入原样
// 留在运行时代码里，而 request_reply.ts 只导出类型，值导入会在加载期报
// "does not provide an export named ..."。
import type { RequestCallbackLike } from './request_reply.ts';

const logger = getLogger('producer');

// ---- selector / callback / transaction types ----
export abstract class MessageQueueSelector {
  abstract select(msgs: Message | Message[], mqList: MessageQueue[], arg: any): MessageQueue | null;
}

export class SelectMessageQueueByHash extends MessageQueueSelector {
  select(_msgs: Message | Message[], mqList: MessageQueue[], arg: any): MessageQueue | null {
    if (mqList == null || mqList.length === 0) return null;
    const s = arg != null ? String(arg) : '';
    let hash = 0;
    for (let i = 0; i < s.length; i++) hash = (Math.imul(31, hash) + s.charCodeAt(i)) | 0;
    if (hash < 0) hash = -hash;
    return mqList[hash % mqList.length];
  }
}

export class SelectMessageQueueByRandom extends MessageQueueSelector {
  select(_msgs: Message | Message[], mqList: MessageQueue[], _arg: any): MessageQueue | null {
    if (mqList == null || mqList.length === 0) return null;
    const idx = Math.floor(Math.random() * mqList.length);
    return mqList[idx];
  }
}

// SelectMessageQueueByMachineRoom picks the first queue whose brokerName
// starts with arg (broker names are expected to be "<idc>-<broker>"); with no
// match it falls back to the first queue. Mirrors Go's
// SelectMessageQueueByMachineRoom and Java's producer-side room routing.
export class SelectMessageQueueByMachineRoom extends MessageQueueSelector {
  select(_msgs: Message | Message[], mqList: MessageQueue[], arg: any): MessageQueue | null {
    if (mqList == null || mqList.length === 0) return null;
    const room = arg != null ? String(arg) : '';
    for (const mq of mqList) {
      if (mq.getBrokerName().startsWith(room)) return mq;
    }
    return mqList[0];
  }
}

export type SendCallback = (sendResult: SendResult | null, err: Error | null) => void;

export const LocalTransactionState = {
  COMMIT_MESSAGE: 0,
  ROLLBACK_MESSAGE: 1,
  UNKNOW: 2,
};

export abstract class TransactionListener {
  // Returns the local transaction state after executing the local transaction.
  executeLocalTransaction(msg: Message, arg: any): number { void msg; void arg; return LocalTransactionState.UNKNOW; }
  // Called by the broker to check the state of a half message.
  checkLocalTransaction(msg: Message): number { void msg; return LocalTransactionState.UNKNOW; }
}

export class DefaultMQProducer {
  producerGroup: string;
  createTopicKey: string;
  defaultTopicQueueNums: number;
  sendMsgTimeout: number;
  compressMsgBodyOverHowmuch: number;
  maxMessageSize: number;
  retryTimesWhenSendFailed: number;
  retryAnotherBrokerWhenNotStoreOK: boolean;
  namespace: string | null;
  // Java ClientConfig#namespaceV2 — the SERVER-side namespace (Aliyun-style
  // serverless instance id). When non-empty, NamespaceRpcHook stamps every
  // request with nsd=true / ns=<value> extFields. Independent from
  // `namespace`, which rewrites resource names on the client side.
  namespaceV2: string | null;
  namesrvAddr: string | null;
  tlsEnable: boolean;
  // TLS 细项（见 setTlsOptions）；null = test-mode（信任自签）。
  tlsOptions: { caCert?: string; clientCert?: string; clientKey?: string; serverName?: string } | null;
  rpcHook: AclRPCHook | null;
  unitName: string | null;
  sendLatencyFaultEnable: boolean;
  heartbeatIntervalMillis: number;
  autoBatch: boolean;
  enableTrace: boolean;
  // Custom trace topic (Java setTraceTopicName); null = RMQ_SYS_TRACE_TOPIC.
  traceTopic: string | null;
  // Compression algorithm for oversized bodies: MessageSysFlag type value
  // (1=LZ4, 2=ZSTD, 3=ZLIB). Default ZLIB for backward compatibility; Java 5.x
  // defaults to LZ4. Select via setCompressType('ZLIB'|'LZ4'|'ZSTD').
  compressType: number;
  // Java DefaultMQProducer.sendMessageWithVIPChannel (default false): send
  // rpcs target the broker's VIP port (listen port - 2).
  sendMessageWithVIPChannel: boolean;
  // ASYNC retry budget (DefaultMQProducer.retryTimesWhenSendAsyncFailed, default 2):
  // the first attempt is NOT counted — a value of 2 means up to 3 total attempts.
  // Deliberately a SEPARATE knob from retryTimesWhenSendFailed: the async chain
  // never reads the sync one (Java MQClientAPIImpl.sendMessageAsync/onExceptionImpl).
  retryTimesWhenSendAsyncFailed: number;
  // Java DefaultMQProducer.compressLevel (Deflater.BEST_SPEED+... default 5):
  // ZLIB compression level for oversized bodies. LZ4/ZSTD ignore it.
  compressLevel: number;
  // Java DefaultMQProducer.startDetectorEnable (via ClientConfig, default false):
  // arms the MQFaultStrategy probe thread that re-checks isolated brokers.
  startDetectorEnable: boolean;
  // W3C traceparent passthrough switch (Java delegates this to an external
  // OTel/SkyWalking hook); independent of enableTrace, seeded from
  // ROCKETMQ_TRACE_CONTEXT_ENABLE like every other port.
  enableTraceContext: boolean;

  client: MQClient | null;
  mqFaultStrategy: MQFaultStrategy;
  producerClientId: string;
  // Message-trace dispatcher (Java AsyncTraceDispatcher) — created in start()
  // only when enableTrace is on, torn down in shutdown().
  _traceDispatcher: AsyncTraceDispatcher | null;
  private _traceHook: SendMessageHook | null;
  private _recallTraceHook: any | null;
  private _accumulator: ProduceAccumulator | null;
  // Async-send backpressure (Java DefaultMQProducer backPressureForAsyncSendNum
  // / Size + the two fair semaphores built in initAsyncConfig). The gate is
  // enableBackpressureForAsyncMode (Java DefaultMQProducer, default false) —
  // with the gate off the semaphores are never consulted.
  enableBackpressureForAsyncMode: boolean;
  backPressureForAsyncSendNum: number;
  backPressureForAsyncSendSize: number;
  private _semaphoreAsyncSendNum: FairSemaphore;
  private _semaphoreAsyncSendSize: FairSemaphore;

  constructor(producerGroup: string = MixAll.DEFAULT_PRODUCER_GROUP) {
    this.producerGroup = producerGroup;
    this.createTopicKey = MixAll.DEFAULT_TOPIC;
    this.defaultTopicQueueNums = MixAll.DEFAULT_TOPIC_QUEUE_NUMS;
    this.sendMsgTimeout = 3000;
    this.compressMsgBodyOverHowmuch = 4 * 1024;
    this.maxMessageSize = 4 * 1024 * 1024;
    this.retryTimesWhenSendFailed = 2;
    this.retryAnotherBrokerWhenNotStoreOK = false;
    this.namespace = null;
    this.namespaceV2 = null;
    this.namesrvAddr = null;
    this.tlsEnable = false;
    this.tlsOptions = null;
    this.rpcHook = null;
    this.unitName = null;
    this.sendLatencyFaultEnable = false;
    this.heartbeatIntervalMillis = 30 * 1000;
    this.autoBatch = false;
    this.enableTrace = false;
    this.traceTopic = null;
    this.compressType = MessageSysFlag.ZLIB_TYPE;
    this.sendMessageWithVIPChannel = false;
    this.retryTimesWhenSendAsyncFailed = 2;
    this.compressLevel = 5;
    this.startDetectorEnable = false;
    this.enableTraceContext = traceContextEnabledFromEnv();

    this.client = null;
    this.mqFaultStrategy = new MQFaultStrategy(this.sendLatencyFaultEnable);
    this.producerClientId = this._buildClientId();
    this._traceDispatcher = null;
    this._traceHook = null;    this._recallTraceHook = null;
    this._accumulator = null;
    this.enableBackpressureForAsyncMode = false;
    this.backPressureForAsyncSendNum = 1024;
    this.backPressureForAsyncSendSize = 100 * 1024 * 1024;
    this._semaphoreAsyncSendNum = new FairSemaphore(this.backPressureForAsyncSendNum);
    this._semaphoreAsyncSendSize = new FairSemaphore(this.backPressureForAsyncSendSize);
  }

  private _buildClientId(): string {
    const instanceName = MixAll.changeInstanceNameToPid(MixAll.DEFAULT_INSTANCE_NAME);
    return MixAll.clientIdFor(instanceName, this.unitName);
  }

  // ---- namespace / topic helpers ----
  _withNamespace(resource: string): string {
    if (!this.namespace) return resource;
    return NamespaceUtil.wrapNamespace(this.namespace, resource);
  }

  _withoutNamespace(resource: string): string {
    if (!this.namespace) return resource;
    return NamespaceUtil.withoutNamespace(resource, this.namespace);
  }

  _checkLegalTopic(topic: string): void {
    TopicValidator.validateTopic(topic);
    if (TopicValidator.isNotAllowedSendTopic(topic)) {
      throw new MQClientException(`topic[${topic}] is not allowed to send`);
    }
  }

  // ---- lifecycle ----
  // ---- fluent config (mirror the consumer) ----
  setNamesrvAddr(addr: string): this { this.namesrvAddr = addr; return this; }
  setNamespace(ns: string): this { this.namespace = ns; return this; }
  // Java ClientConfig#setNamespaceV2/getNamespaceV2. The value is read live by
  // the NamespaceRpcHook on every request, so setting it after start() takes
  // effect too — exactly like Java.
  setNamespaceV2(ns: string | null): this { this.namespaceV2 = ns; return this; }
  getNamespaceV2(): string | null { return this.namespaceV2; }
  setUnitName(name: string): this { this.unitName = name; return this; }
  setTlsEnable(enable: boolean): this { this.tlsEnable = enable; return this; }
  // TLS 细项（caCert=严格 CA 校验；clientCert/clientKey=mTLS；serverName=主机名覆盖），
  // 对齐 PHP tlsOptions / Java TlsSystemConfig certPath 族。不设 caCert = test-mode。
  setTlsOptions(opts: { caCert?: string; clientCert?: string; clientKey?: string; serverName?: string } | null): this {
    this.tlsOptions = opts; return this;
  }
  setSendMsgTimeout(ms: number): this { this.sendMsgTimeout = ms; return this; }
  setEnableTrace(enable: boolean): this { this.enableTrace = enable; return this; }
  setTraceTopic(topic: string): this { this.traceTopic = topic; return this; }
  // compressType: 'ZLIB' | 'LZ4' | 'ZSTD' (Java DefaultMQProducer.setCompressType)
  // or the raw MessageSysFlag type value (1/2/3).
  setCompressType(compressType: string | number): this {
    this.compressType = compressionTypeByName(compressType);
    return this;
  }
  setCompressMsgBodyOverHowmuch(howmuch: number): this { this.compressMsgBodyOverHowmuch = howmuch; return this; }
  setSendMessageWithVIPChannel(enable: boolean): this { this.sendMessageWithVIPChannel = enable; return this; }
  isSendMessageWithVIPChannel(): boolean { return this.sendMessageWithVIPChannel; }
  // ASYNC retry budget (first attempt not counted). Only the async chain reads it.
  setRetryTimesWhenSendAsyncFailed(n: number): this { this.retryTimesWhenSendAsyncFailed = n; return this; }
  getRetryTimesWhenSendAsyncFailed(): number { return this.retryTimesWhenSendAsyncFailed; }
  // ZLIB compression level 0-9 (Java DefaultMQProducer.setCompressLevel, default 5).
  setCompressLevel(level: number): this { this.compressLevel = level; return this; }
  getCompressLevel(): number { return this.compressLevel; }
  // Arms the MQFaultStrategy reachability probe (Java ClientConfig.startDetectorEnable).
  setStartDetectorEnable(enable: boolean): this { this.startDetectorEnable = enable; return this; }
  // Async-send backpressure gate (Java DefaultMQProducer.setEnableBackpressureForAsyncMode,
  // default false): when off, the in-flight num/size semaphores are not consulted.
  setEnableBackpressureForAsyncMode(enable: boolean): this { this.enableBackpressureForAsyncMode = enable; return this; }
  isEnableBackpressureForAsyncMode(): boolean { return this.enableBackpressureForAsyncMode; }
  setEnableTraceContext(enable: boolean): this { this.enableTraceContext = enable; return this; }
  isEnableTraceContext(): boolean { return this.enableTraceContext; }

  // Runtime backpressure knobs (Java documents them as RUNTIME tunables).
  setBackPressureForAsyncSendNum(num: number): this {
    this.backPressureForAsyncSendNum = num;
    this._semaphoreAsyncSendNum.setTotalPermits(num);
    return this;
  }
  setBackPressureForAsyncSendSize(size: number): this {
    this.backPressureForAsyncSendSize = size;
    this._semaphoreAsyncSendSize.setTotalPermits(size);
    return this;
  }

  // Acquire both async-send permits or throw synchronously (Java's bounded
  // async queue rejects with RemotingTooMuchRequestException when full). A
  // partial acquisition is rolled back so permits never leak.
  private _acquireAsyncPermits(msg: Message): void {
    if (!this._semaphoreAsyncSendNum.tryAcquire()) {
      throw new MQClientException(
        `async send rejected: in-flight num over backPressureForAsyncSendNum(${this.backPressureForAsyncSendNum})`);
    }
    const bodyLen = msg.getBody() != null ? msg.getBody().length : 0;
    if (!this._semaphoreAsyncSendSize.tryAcquireFor(bodyLen)) {
      this._semaphoreAsyncSendNum.release();
      throw new MQClientException(
        `async send rejected: in-flight size over backPressureForAsyncSendSize(${this.backPressureForAsyncSendSize})`);
    }
  }

  private _releaseAsyncPermits(msg: Message): void {
    const bodyLen = msg.getBody() != null ? msg.getBody().length : 0;
    this._semaphoreAsyncSendSize.releaseFor(bodyLen);
    this._semaphoreAsyncSendNum.release();
  }

  async start(): Promise<void> {
    if (this.producerGroup == null || !String(this.producerGroup).trim()) {
      throw new MQClientException('producer group is blank');
    }
    Validators.checkGroup(this.producerGroup);
    if (this.producerGroup === MixAll.DEFAULT_PRODUCER_GROUP) {
      throw new MQClientException('producer group can not equal DEFAULT_PRODUCER');
    }
    const instanceName = MixAll.changeInstanceNameToPid(MixAll.DEFAULT_INSTANCE_NAME);
    const clientId = MixAll.clientIdFor(instanceName, this.unitName);
    this.producerClientId = clientId;

    const remotingClient = new RemotingClient({ tlsEnable: this.tlsEnable, tlsOptions: this.tlsOptions });
    this.client = new MQClient(clientId, this.namesrvAddr, remotingClient);
    // Java MQClientAPIImpl:329-335 registration order: Namespace -> Stream ->
    // user (ACL) hook -> DynamicalExtField. The namespace extFields must be
    // written BEFORE the ACL hook so the signature covers nsd/ns.
    registerRpcHooks(this.client.remotingClient, {
      namespaceV2: () => this.namespaceV2,
      userHook: this.rpcHook,
    });
    this.client.start();
    this.client.registerProducer(this.producerGroup, this);
    this.mqFaultStrategy = new MQFaultStrategy(this.sendLatencyFaultEnable);
    // Fault-strategy probe thread (Java MQFaultStrategy implements StartAndShutdown;
    // DefaultMQProducerImpl.start() calls startDetector()): resolver maps broker
    // name -> publish addr, detector is a plain TCP connect probe. Both no-ops
    // unless startDetectorEnable is turned on.
    if (this.startDetectorEnable) {
      this.mqFaultStrategy.setResolver((brokerName: string): string | null =>
        this.client != null ? this.client.findBrokerAddressInPublish(brokerName) : null);
      this.mqFaultStrategy.setServiceDetector((addr: string, timeout: number): boolean => tcpDetect(addr, timeout));
      this.mqFaultStrategy.setStartDetectorEnable(true);
    }
    this.mqFaultStrategy.startDetector();
    // RequestFutureHolder housekeeping (Java DefaultMQProducerImpl.start() ->
    // RequestFutureHolder.startScheduledTask): the TTL sweep that fails
    // request-reply futures nobody will ever answer.
    REQUEST_FUTURE_HOLDER.startScheduledTask(this);

    if (this.autoBatch) {
      this._accumulator = getOrCreateProduceAccumulator(clientId);
      this._accumulator.setSender(async (acc) => {
        await this._flushAccumulation(acc);
      });
      this._accumulator.start();
    }

    // Message trace (Java DefaultMQProducerImpl.initTraceDispatcher + the
    // DefaultMQProducer constructor registering SendMessageTraceHookImpl):
    // without this wiring enableTrace is a silent no-op on the produce side.
    if (this.enableTrace) {
      try {
        const { AsyncTraceDispatcher } = await import('./trace_dispatcher.ts');
        const { SendMessageTraceHookImpl } = await import('./trace_hook.ts');
        const dispatcher = new AsyncTraceDispatcher(
          this.producerGroup, 'PRODUCER', 10, this.traceTopic || undefined);
        dispatcher.setHostProducer(this);
        // Java AsyncTraceDispatcher.start:155 — the namespace is propagated to
        // the internal trace producer so its rpcs carry nsd/ns as well.
        dispatcher.setNamespaceV2(this.namespaceV2);
        this._traceDispatcher = dispatcher;
        this._traceHook = new SendMessageTraceHookImpl(dispatcher);
        hookRegistry.registerSendMessageHook(this._traceHook);
        // Java DefaultMQProducer also registers DefaultRecallMessageTraceHook
        // (an RPCHook on the remoting client) here — recall rpcs get their own
        // Recall trace records; gated on the
        // com.rocketmq.recall.default.trace.enable sysprop, default off.
        const { DefaultRecallMessageTraceHook } = await import('./trace_hook.ts');
        this._recallTraceHook = new DefaultRecallMessageTraceHook(dispatcher);
        this.client.remotingClient.registerRpcHook(this._recallTraceHook);
        await dispatcher.start(this.namesrvAddr || undefined);
      } catch (e) {
        logger.warn('trace dispatcher start failed (trace disabled): %s', (e as Error).message);
        this._traceDispatcher = null;
        this._traceHook = null;
        this._recallTraceHook = null;
      }
    }
    logger.info(`producer ${this.producerGroup} started, clientId=${clientId}`);
  }

  shutdown(): void {
    REQUEST_FUTURE_HOLDER.shutdown(this);
    this.mqFaultStrategy.shutdown();
    if (this._recallTraceHook != null && this.client != null) {
      const idx = this.client.remotingClient.rpcHooks.indexOf(this._recallTraceHook);
      if (idx >= 0) this.client.remotingClient.rpcHooks.splice(idx, 1);
      this._recallTraceHook = null;
    }
    if (this._accumulator != null) this._accumulator.stop();
    if (this._traceHook != null) {
      const idx = hookRegistry.sendMessageHooks.indexOf(this._traceHook);
      if (idx >= 0) hookRegistry.sendMessageHooks.splice(idx, 1);
      this._traceHook = null;
    }
    if (this._traceDispatcher != null) {
      const d = this._traceDispatcher;
      this._traceDispatcher = null;
      void d.shutdown().catch(() => { /* best effort */ });
    }
    if (this.client != null) {
      this.client.unregisterProducer(this.producerGroup);
      this.client.shutdown();
    }
    this.client = null;
  }

  // ---- compression (algorithm selectable; degrade on failure, never throw) ----
  tryToCompressMessage(msg: Message): boolean {
    // Java sendKernelImpl guards with `!(msg instanceof MessageBatch)` before
    // compressing: the batch body is the aggregate envelope the broker splits
    // back apart — compressing it would corrupt that contract.
    if (msg instanceof MessageBatch) return false;
    const body = msg.getBody();
    if (body == null || body.length < this.compressMsgBodyOverHowmuch) return false;
    if (MessageSysFlag.isCompressed((msg as any)._sysFlag != null ? (msg as any)._sysFlag : 0)) return false;
    try {
      const compressed = compressFor(body, this.compressType, this.compressLevel);
      (msg as any)._sysFlag = MessageSysFlag.setCompressionType(
        ((msg as any)._sysFlag != null ? (msg as any)._sysFlag : 0) | MessageSysFlag.COMPRESSED_FLAG,
        this.compressType,
      );
      msg.setBody(compressed);
      return true;
    } catch (e) {
      logger.warn('compress message body failed, send without compression');
      return false;
    }
  }

  // ---- core send path ----
  // Java DefaultMQProducerImpl.tryToFindTopicPublishInfo: first fetch the
  // EXACT topic route; only when that yields no usable info, retry via the
  // DEFAULT topic (TBW102) so the broker auto-creates on first send. The
  // fallback-derived info is tagged (`fromDefaultTopic`) so _sendDefaultImpl
  // refetches the exact route before using its queue list.
  async _tryToFindTopicPublishInfo(topic: string): Promise<TopicPublishInfo | null> {
    if (this.client == null) throw new MQClientException('producer not started');
    let info = this.client.getTopicPublishInfo(topic);
    if (info != null && info.ok() && !(info as any).fromDefaultTopic) return info;
    this._lastRouteError = null;
    try {
      await this.client.updateTopicRouteInfoFromNameServer(topic, false);
    } catch (e) {
      // 路由拉不到的原因要留着：TLS 握手 CA 校验失败这类错误若在这里被吞，
      // 上层只会看到 "No route info of this topic"，黑盒（用户往"建 topic"方向查，
      // 真正坏的是证书/CA）。挂到 _lastRouteError，由 No-route 抛出点带出。
      this._lastRouteError = e instanceof Error ? e : new Error(String(e));
      logger.warn(`find topic publish info for ${topic} failed: ${(e as Error).message}`);
    }
    info = this.client.getTopicPublishInfo(topic);
    if (info != null && info.ok() && !(info as any).fromDefaultTopic) return info;
    // Second step: DEFAULT-topic fallback (auto-create path).
    try {
      await this.client.updateTopicRouteInfoFromNameServer(topic, true);
    } catch (e) {
      if (this._lastRouteError == null) {
        this._lastRouteError = e instanceof Error ? e : new Error(String(e));
      }
      logger.warn(`find default-topic publish info for ${topic} failed: ${(e as Error).message}`);
    }
    info = this.client.getTopicPublishInfo(topic);
    if (info != null && info.ok()) return info;
    return info;
  }

  // 上面两步路由尝试中最后一次失败的原因（成功即清空）。用于把 TLS/CA、
  // 连接拒绝这类真实根因带进 "No route info" 报错，而不是黑盒。
  private _lastRouteError: Error | null = null;

  private _routeFailureSuffix(): string {
    const e = this._lastRouteError;
    if (e == null) return '';
    const chain: string[] = [];
    for (let x: any = e; x != null; x = x.cause) chain.push(`${x.name ?? 'Error'}: ${x.message}`);
    return ` (route fetch failed: ${chain.join(' <-- ')})`;
  }

  _selectOneMessageQueue(tpInfo: TopicPublishInfo, lastBrokerName: string | null): MessageQueue | null {
    if (this.sendLatencyFaultEnable) {
      return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }
    return tpInfo.selectOneMessageQueue(lastBrokerName);
  }

  // Build the on-wire request (sets UNIQ_KEY + producer-group correction). Exposed for the
  // smoke test / advanced callers; returns the RemotingCommand.
  buildSendRequest(msg: Message, mq: MessageQueue): RemotingCommand {
    if (this.client == null) throw new MQClientException('producer not started');
    if (!msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)) {
      MessageAccessor.putProperty(msg, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
    }
    MessageAccessor.setCorrectionBeforePublish(msg, this.producerGroup);
    return this.client.buildSendRequest(msg, mq, this.producerGroup, this.namespace);
  }

  // Java MQClientAPIImpl wraps every send invoke with
  // MixAll.brokerVIPChannel(isVipChannelEnabled, addr).
  private _sendAddr(addr: string | null): string | null {
    if (addr == null) return null;
    if (this.sendMessageWithVIPChannel) return MixAll.brokerVipChannel(true, addr);
    return addr;
  }

  // Send kernel: compress -> UNIQ_KEY -> CheckForbiddenHook (NOT swallowed) -> transport.
  async sendKernelImpl(
    msg: Message,
    mq: MessageQueue,
    communicationMode: number,
    sendCallback: SendCallback | null,
    tpInfo: TopicPublishInfo,
  ): Promise<SendResult | null> {
    if (this.client == null) throw new MQClientException('producer not started');
    // 1) compression already applied by caller (tryToCompressMessage); ensure sysFlag present.
    let sysFlag = (msg as any)._sysFlag != null ? (msg as any)._sysFlag : 0;

    // 2) CheckForbiddenHook — injected AFTER compression/sysflag, exception is NOT swallowed.
    const forbidCtx = new CheckForbiddenContext(this.namespace, this.producerGroup, msg.getTopic(), msg);
    forbidCtx.brokerAddr = this.client.publishAddrFor(mq);
    forbidCtx.message = msg;
    for (const hook of hookRegistry.checkForbiddenHooks) {
      hook.checkForbidden(forbidCtx);
    }
    if (forbidCtx.forbid) {
      throw new MQClientException('the message is forbidden by CheckForbiddenHook');
    }

    if (!msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)) {
      MessageAccessor.putProperty(msg, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
    }
    MessageAccessor.setCorrectionBeforePublish(msg, this.producerGroup);
    (msg as any)._sysFlag = sysFlag;

    // W3C traceparent passthrough (opt-in): a message without one gets a root
    // span; a caller-propagated value is never overwritten. Java leaves this
    // to an external OTel hook — the ports build the equivalent in, and it
    // runs in the same position (after the forbidden hooks, before the request
    // is built, so the trace hook sees the property).
    if (this.enableTraceContext) {
      injectTraceContext(msg);
    }

    const request = this.client.buildSendRequest(msg, mq, this.producerGroup, this.namespace);
    const addr = this._sendAddr(this.client.publishAddrFor(mq));
    if (addr == null) {
      throw new MQClientException(`no broker address for mq ${mq.toString()}`);
    }

    if (communicationMode === CommunicationMode.SYNC) {
      return this.client.sendMessage(addr, request, mq, this.sendMsgTimeout, msg);
    } else if (communicationMode === CommunicationMode.ASYNC) {
      // Java MQClientAPIImpl.sendMessage(ASYNC) -> sendMessageAsync/onExceptionImpl:
      // the ASYNC path owns its own retry chain (retryTimesWhenSendAsyncFailed,
      // shared time budget, re-select a queue that avoids the failed broker).
      this._sendAsyncKernel(msg, mq, tpInfo, sendCallback, request, addr);
      return null;
    } else {
      this.client.sendMessageOneway(addr, request);
      return null;
    }
  }

  // Async send retry chain (Java MQClientAPIImpl.sendMessageAsync + onExceptionImpl,
  // MQClientAPIImpl.java:614-740). One logical send may issue up to
  // 1 + retryTimesWhenSendAsyncFailed attempts against the SAME shared budget:
  //   - transport-level failures (timeout / send failure / connection closed)
  //     retry on a queue chosen to avoid the failed broker (needRetry=true);
  //   - a broker answer that fails response parsing (MQBrokerException — error
  //     code) does NOT retry (needRetry=false, Java operationSucceed catch);
  //   - a parsed SendResult is delivered as-is. Java's async chain never consults
  //     retryAnotherBrokerWhenNotStoreOK (DefaultMQProducerImpl.java:799 is
  //     sync-only) — FLUSH_DISK_TIMEOUT etc. go to the caller's onSuccess.
  // Every attempt updates the fault item; when the budget or the retry budget
  // runs out the user callback fires exactly once with the error.
  private _sendAsyncKernel(
    msg: Message,
    mq: MessageQueue,
    tpInfo: TopicPublishInfo,
    sendCallback: SendCallback | null,
    request: RemotingCommand,
    addr: string,
  ): void {
    void addr; // kept for signature symmetry with sendKernelImpl
    const timesTotal = Math.max(0, this.retryTimesWhenSendAsyncFailed);
    let times = 0;
    const self = this;
    const attempt = (brokerName: string | null, attemptMq: MessageQueue, budgetMs: number): void => {
      const attemptAddr = brokerName != null
        ? self._sendAddr(self.client!.findBrokerAddressInPublish(brokerName))
        : self._sendAddr(self.client!.publishAddrFor(attemptMq));
      const attemptBegin = Date.now();
      const onExceptionImpl = (err: Error, needRetry: boolean, reachable: boolean): void => {
        // Java async chain: callback failures mark isolation with reachable=true;
        // a synchronous throw (never reached the transport) sets reachable=false.
        if (self.sendLatencyFaultEnable) {
          self.mqFaultStrategy.updateFaultItem(attemptMq.getBrokerName(), Date.now() - attemptBegin, true, reachable);
        }
        times += 1;
        const remaining = budgetMs - (Date.now() - attemptBegin);
        if (needRetry && times <= timesTotal && remaining > 0) {
          let retryBrokerName = brokerName; // by default, retry the same broker
          if (tpInfo != null) {
            const mqChosen = self._selectOneMessageQueue(tpInfo, brokerName);
            if (mqChosen != null) retryBrokerName = mqChosen.getBrokerName();
          }
          logger.warning('async send msg by retry %d times. topic=%s, brokerName=%s, err=%s',
            times, msg.getTopic(), retryBrokerName != null ? retryBrokerName : '', err.message);
          // Java reuses the request with a fresh request id (setOpaque(createNewRequestId())):
          // a retried request must not collide with the old opaque in the response table.
          request.opaque = RemotingCommand.createNewRequestId();
          attempt(retryBrokerName, attemptMq, remaining);
        } else {
          if (sendCallback) sendCallback(null, err);
        }
      };
      if (attemptAddr == null) {
        onExceptionImpl(new MQClientException(`no broker address for mq ${attemptMq.toString()}`), true, false);
        return;
      }
      try {
        self.client!.sendMessageAsync(attemptAddr, request, attemptMq, budgetMs, (sr, err) => {
          if (err != null) {
            // Java distinguishes transport failure (retry) from a broker answer
            // that failed response parsing (MQBrokerException — no retry).
            onExceptionImpl(err, !(err instanceof MQBrokerException), true);
            return;
          }
          if (self.sendLatencyFaultEnable) {
            self.mqFaultStrategy.updateFaultItem(attemptMq.getBrokerName(), Date.now() - attemptBegin, false, true);
          }
          if (sendCallback) sendCallback(sr, null);
        }, msg);
      } catch (e) {
        onExceptionImpl(e as Error, true, false);
      }
    };
    attempt(mq.getBrokerName(), mq, this.sendMsgTimeout);
  }

  // Wrap SendMessageHook around the kernel send.
  async _sendWithHooks(
    msg: Message,
    mq: MessageQueue,
    communicationMode: number,
    sendCallback: SendCallback | null,
    tpInfo: TopicPublishInfo,
  ): Promise<SendResult | null> {
    const ctx = new SendMessageContext(this.producerGroup, msg, mq, communicationMode, this.client!.publishAddrFor(mq));
    // Java registers SendMessageHooks per producer; node's registry is global.
    // The inner trace producer opts out (_skipSendHooks) so trace-topic writes
    // are not themselves traced (Java parity).
    const sendHooks: SendMessageHook[] = (this as any)._skipSendHooks
      ? [] : hookRegistry.sendMessageHooks;
    for (const hook of sendHooks) hook.sendMessageBefore(ctx);
    let sendResult: SendResult | null = null;
    try {
      sendResult = await this.sendKernelImpl(msg, mq, communicationMode, sendCallback, tpInfo);
      ctx.sendResult = sendResult;
      ctx.exception = null;
    } catch (e) {
      ctx.exception = e as Error;
      ctx.sendResult = null;
      for (const hook of sendHooks) hook.sendMessageAfter(ctx);
      throw e;
    }
    for (const hook of sendHooks) hook.sendMessageAfter(ctx);
    return sendResult;
  }

  // Retry-loop default send implementation.
  async _sendDefaultImpl(
    msg: Message,
    communicationMode: number,
    sendCallback: SendCallback | null,
    timeoutMillis: number,
  ): Promise<SendResult | null> {
    // Java sendDefaultImpl entry: Validators.checkMessage(msg, this.defaultMQProducer)
    // — topic legality plus body null/empty/maxMessageSize (and the LMQ
    // INNER_MULTI_DISPATCH separator rule) before any route lookup.
    Validators.checkMessage(msg, this.maxMessageSize);
    this._checkLegalTopic(msg.getTopic());
    let tpInfo = await this._tryToFindTopicPublishInfo(msg.getTopic());
    if (tpInfo == null || !tpInfo.ok()) {
      // 带上路由拉取的真实失败原因（TLS/CA、连接拒绝…），没有失败记录时
      // 保持原文案（topic 真不存在的语义）。
      throw new MQClientException(
        `No route info of this topic: ${msg.getTopic()}${this._routeFailureSuffix()}`,
        this._lastRouteError ?? undefined,
      );
    }
    // A publish info synthesized from the DEFAULT topic (TBW102) carries
    // TBW102's queue list — sending against it yields "request queueId[N] is
    // illegal". Refetch the EXACT topic route before giving the queues to the
    // broker (the tag is set by updateTopicRouteInfoFromNameServer's fallback).
    if ((tpInfo as any).fromDefaultTopic) {
      try {
        await this.client!.updateTopicRouteInfoFromNameServer(msg.getTopic(), false);
        const refreshed = this.client!.getTopicPublishInfo(msg.getTopic());
        if (refreshed != null && refreshed.ok() && !(refreshed as any).fromDefaultTopic) tpInfo = refreshed;
      } catch (e) { /* keep the fallback info */ }
    }
    let lastBrokerName: string | null = null;
    let sendResult: SendResult | null = null;
    // Java: `timesTotal = SYNC ? 1 + retryTimesWhenSendFailed : 1` — only the
    // SYNC path retries inside this loop. ASYNC owns its own chain
    // (_sendAsyncKernel, retryTimesWhenSendAsyncFailed); ONEWAY never retries.
    const maxTimes = communicationMode === CommunicationMode.SYNC
      ? this.retryTimesWhenSendFailed + 1 : 1;
    for (let times = 0; times < maxTimes; times++) {
      const mq = this._selectOneMessageQueue(tpInfo, lastBrokerName);
      if (mq == null) continue;
      lastBrokerName = mq.getBrokerName();
      try {
        this.tryToCompressMessage(msg);
        sendResult = await this._sendWithHooks(msg, mq, communicationMode, sendCallback, tpInfo);
        // Java semantics: ASYNC / ONEWAY succeed as soon as no exception was
        // thrown (sendKernelImpl returns null for both); only SYNC uses the
        // SendResult as the retry-driving value.
        if (communicationMode === CommunicationMode.ASYNC || communicationMode === CommunicationMode.ONEWAY) {
          return null;
        }
        if (sendResult != null) {
          if (this.sendLatencyFaultEnable) {
            this.mqFaultStrategy.updateFaultItem(mq.getBrokerName(), 0, false, true);
          }
          return sendResult;
        }
      } catch (e) {
        logger.warn(`send to ${mq.toString()} failed (attempt ${times + 1}): ${(e as Error).message}`);
        if (this.sendLatencyFaultEnable) {
          this.mqFaultStrategy.updateFaultItem(mq.getBrokerName(), this.sendMsgTimeout, true, false);
        }
        lastBrokerName = null;
        // A failure may mean the cached publish info is still the default-topic
        // fallback (route registered late). Refetch the exact route between
        // attempts so the next try does not repeat the same illegal queue.
        if ((tpInfo as any).fromDefaultTopic) {
          try {
            await this.client!.updateTopicRouteInfoFromNameServer(msg.getTopic(), false);
            const refreshed = this.client!.getTopicPublishInfo(msg.getTopic());
            if (refreshed != null && refreshed.ok() && !(refreshed as any).fromDefaultTopic) tpInfo = refreshed;
          } catch (e2) { /* keep the fallback info */ }
        }
      }
    }
    if (sendResult == null) {
      throw new MQClientException('failed to send message after retries');
    }
    return sendResult;
  }

  // ---- public send API ----
  async send(msg: Message, timeoutMillis: number = this.sendMsgTimeout): Promise<SendResult> {
    const r = await this._sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeoutMillis);
    if (r == null) throw new MQClientException('null send result');
    return r;
  }

  async sendOneway(msg: Message, timeoutMillis: number = this.sendMsgTimeout): Promise<void> {
    await this._sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, timeoutMillis);
  }

  async sendAsync(msg: Message, sendCallback: SendCallback, timeoutMillis: number = this.sendMsgTimeout): Promise<void> {
    // Async-send backpressure (Java initAsyncConfig + executeAsyncMessageSend):
    // the two fair semaphores bound the in-flight async sends, but ONLY while
    // enableBackpressureForAsyncMode is on (Java default false). A full
    // semaphore throws synchronously. Permits return when the transport-level
    // callback fires — exactly once — or when the send fails before reaching it.
    if (this.enableBackpressureForAsyncMode) {
      this._acquireAsyncPermits(msg);
    }
    let permitsHeld = true;
    const releaseOnce = () => {
      if (permitsHeld) {
        permitsHeld = false;
        this._releaseAsyncPermits(msg);
      }
    };
    const wrapped: SendCallback = (sr, err) => {
      releaseOnce();
      if (sendCallback) sendCallback(sr, err);
    };
    try {
      await this._sendDefaultImpl(msg, CommunicationMode.ASYNC, wrapped, timeoutMillis);
    } catch (e) {
      releaseOnce();
      throw e;
    }
  }

  // Java sendSelectImpl: route lookup + selector pick + kernel send, shared by
  // sendBySelector and the two selector-shaped request forms.
  async _sendSelectImpl(
    msg: Message,
    selector: MessageQueueSelector,
    arg: any,
    communicationMode: number,
    sendCallback: SendCallback | null,
    timeoutMillis: number,
  ): Promise<SendResult | null> {
    // Java sendSelectImpl: Validators.checkMessage before the route lookup.
    Validators.checkMessage(msg, this.maxMessageSize);
    this._checkLegalTopic(msg.getTopic());
    const tpInfo = await this._tryToFindTopicPublishInfo(msg.getTopic());
    if (tpInfo == null || !tpInfo.ok()) {
      throw new MQClientException(
        `No route info of this topic: ${msg.getTopic()}${this._routeFailureSuffix()}`,
        this._lastRouteError ?? undefined,
      );
    }
    const mq = selector.select(msg, tpInfo.msgQueueList, arg);
    if (mq == null) throw new MQClientException('failed to select a message queue');
    this.tryToCompressMessage(msg);
    const r = await this._sendWithHooks(msg, mq, communicationMode, sendCallback, tpInfo);
    if (communicationMode === CommunicationMode.SYNC) {
      if (r == null) throw new MQClientException('null send result');
      return r;
    }
    return null;
  }

  async sendBySelector(
    msg: Message,
    selector: MessageQueueSelector,
    arg: any,
    timeoutMillis: number = this.sendMsgTimeout,
  ): Promise<SendResult> {
    return this._sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeoutMillis);
  }

  // ---- transaction ----
  // Java shape: sendMessageInTransaction(msg, arg) resolves the listener from
  // the producer's own field; also accept it explicitly as the 2nd parameter.
  async sendMessageInTransaction(
    msg: Message,
    listener: TransactionListener | null = null,
    arg: any = null,
    timeoutMillis: number = this.sendMsgTimeout,
  ): Promise<TransactionSendResult> {
    listener = listener ?? (this as any).transactionListener ?? null;
    if (listener == null) throw new MQClientException('transaction listener is null');
    MessageAccessor.setTransactionPrepared(msg);
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.producerGroup);
    const sendResult = await this._sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeoutMillis);
    if (sendResult == null) throw new MQClientException('send half message failed');
    // Java DefaultMQProducerImpl.sendMessageInTransaction: a listener exception
    // becomes ROLLBACK_MESSAGE (it never escapes); the half message must not
    // be left for the broker's check thread on a local failure we already know.
    let localState: number;
    try {
      localState = listener.executeLocalTransaction(msg, arg);
    } catch (e) {
      logger.warn('executeLocalTransaction raised, rolling back half message: %s', (e as Error).message);
      localState = LocalTransactionState.ROLLBACK_MESSAGE;
    }
    await this._endTransaction(sendResult, localState, msg);
    const tsr = new TransactionSendResult(
      sendResult.sendStatus, sendResult.msgId, sendResult.messageQueue, sendResult.queueOffset,
      sendResult.transactionId, sendResult.offsetMsgId, sendResult.regionId, sendResult.traceOn, sendResult.recallHandle,
    );
    tsr.setLocalTransactionState(localState);
    return tsr;
  }

  private async _endTransaction(sendResult: SendResult, localTransactionState: number,
    msg: Message | null = null): Promise<void> {
    if (this.client == null) return;
    const brokerAddr = this.client.publishAddrFor(sendResult.messageQueue!);
    if (brokerAddr == null) return;
    const header = new EndTransactionRequestHeader();
    header.producerGroup = this.producerGroup;
    header.transactionId = sendResult.transactionId != null ? sendResult.transactionId : sendResult.msgId;
    // ⚠ commitOrRollback is a MessageSysFlag transaction TYPE on the wire
    // (NOT=0 / COMMIT=8 / ROLLBACK=12), NOT the LocalTransactionState ordinal.
    // Sending the raw ordinal (COMMIT_MESSAGE=0) makes the broker read
    // TRANSACTION_NOT_TYPE — the half message is never committed and stays
    // invisible until the (much later) check-back. Java maps the enum.
    // ⚠ offset semantics (Java endTransaction, verified against 5.x):
    //   - tranStateTableOffset = sendResult.queueOffset — the half message's
    //     offset in RMQ_SYS_TRANS_HALF_TOPIC's queue. @CFNotNull on the wire;
    //     the broker's checkPrepareMessage compares it to the half msg's
    //     queueOffset and REJECTS the commit on mismatch/null.
    //   - commitLogOffset = the PHYSICAL commitlog offset, decoded from the
    //     offsetMsgId (falling back to msgId) — NOT queueOffset (the old code
    //     sent the half-queue offset here, so every producer-side COMMIT was
    //     rejected with "The commit log offset wrong" and the message stayed
    //     invisible until the 30s check-back).
    //   - topic is compared to PROPERTY_REAL_TOPIC in 5.x checkPrepareMessage.
    if (localTransactionState === LocalTransactionState.COMMIT_MESSAGE) {
      header.commitOrRollback = MessageSysFlag.TRANSACTION_COMMIT_TYPE;
    } else if (localTransactionState === LocalTransactionState.ROLLBACK_MESSAGE) {
      header.commitOrRollback = MessageSysFlag.TRANSACTION_ROLLBACK_TYPE;
    } else {
      header.commitOrRollback = MessageSysFlag.TRANSACTION_NOT_TYPE;
    }
    header.tranStateTableOffset = sendResult.queueOffset;
    try {
      const decoded = decodeMessageId(sendResult.offsetMsgId != null
        ? sendResult.offsetMsgId : sendResult.msgId);
      header.commitLogOffset = decoded.offset;
    } catch (e) {
      logger.warn(`decode offsetMsgId failed: ${(e as Error).message}`);
      header.commitLogOffset = sendResult.queueOffset;
    }
    header.topic = msg != null ? msg.getTopic() : null;
    header.fromTransactionCheck = false;
    const uniqKey = msg != null
      ? msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
      : null;
    header.msgId = uniqKey != null ? String(uniqKey) : sendResult.msgId;
    header.bname = sendResult.messageQueue != null ? sendResult.messageQueue.getBrokerName() : null;
    const request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, header);
    try {
      this.client.remotingClient.invokeOneway(brokerAddr, request);
    } catch (e) {
      logger.warn(`end transaction failed: ${(e as Error).message}`);
    }
    // Trace: Java fires the EndTransactionTraceHook from endTransaction — the
    // EndTransaction record of a transaction message.
    if (this._traceDispatcher != null) {
      try {
        this._traceDispatcher.appendEndTransaction(
          this.producerGroup, msg != null ? msg.getTopic() : '',
          header.msgId || '', header.transactionId, localTransactionState,
          false, brokerAddr);
      } catch (e) { /* trace never breaks the data path */ }
    }
  }

  // ---- request-reply ----
  // Java DefaultMQProducer surface: 3 sync forms (msg / msg+selector / msg+mq)
  // and 3 async forms (msg+callback / msg+selector+callback / msg+mq+callback).
  // prepareSendRequest: CORRELATION_ID + REPLY_TO_CLIENT (the requester's
  // clientId — the broker uses it to route the reply back via 326) + TTL, then
  // make sure the topic route exists (Java prepares it so the ASYNC send does
  // not race a cold cache). The reply returns as a
  // PUSH_REPLY_MESSAGE_TO_CLIENT(326) push handled by
  // MQClient._registerReplyMessageProcessor.
  async _prepareSendRequest(msg: Message, timeoutMillis: number): Promise<void> {
    const correlationId = createCorrelationId();
    const requestClientId = this.client != null ? this.client.clientId : this.producerClientId;
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String(timeoutMillis));
    MessageAccessor.setMessageType(msg, MessageType.REQUEST_REPLY);
    if (this.client != null && this.client.getTopicRouteData(msg.getTopic()) == null) {
      await this._tryToFindTopicPublishInfo(msg.getTopic());
    }
  }

  // Java waitResponse: the reply must arrive within (timeout - cost); a null
  // reply distinguishes send-OK-but-reply-timeout from send-failure.
  async _waitResponse(msg: Message, timeoutMillis: number, future: RequestResponseFuture,
    cost: number): Promise<Message> {
    let timer: NodeJS.Timeout | null = null;
    try {
      return await new Promise<Message>((resolve, reject) => {
        timer = setTimeout(() => {
          if (future.isSendRequestOk()) {
            reject(new MQClientException(
              `send request message to <${msg.getTopic()}> OK, but wait reply message timeout, ${timeoutMillis} ms.`));
          } else {
            reject(new MQClientException(`send request message to <${msg.getTopic()}> fail`));
          }
          future.completeExceptionally(new MQClientException('request timeout, no reply message.'));
        }, Math.max(1, timeoutMillis - cost));
        future.promise().then(resolve, reject);
      });
    } finally {
      if (timer != null) clearTimeout(timer);
    }
  }

  // Sync request (Java request(msg, timeout), sendDefaultImpl ASYNC + wait).
  async request(msg: Message, timeoutMillis: number = 3000): Promise<Message> {
    await this._prepareSendRequest(msg, timeoutMillis);
    const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID) as string;
    const future = new RequestResponseFuture(correlationId, msg, timeoutMillis);
    REQUEST_FUTURE_HOLDER.putRequest(correlationId, future);
    const begin = Date.now();
    try {
      await this._sendDefaultImpl(msg, CommunicationMode.ASYNC, (sr, err) => {
        // Java SendCallback: only mark the request as sent — the caller is
        // unblocked by the reply arrival, the timeout scan, or requestFail.
        if (err != null) {
          future.setSendRequestOk(false);
          future.putResponseMessage(null);
          future.setCause(err);
        } else {
          future.setSendRequestOk(true);
        }
      }, timeoutMillis);
      return await this._waitResponse(msg, timeoutMillis, future, Date.now() - begin);
    } finally {
      REQUEST_FUTURE_HOLDER.removeRequest(correlationId);
    }
  }

  // Sync request through a selector (Java request(msg, selector, arg, timeout) —
  // sendSelectImpl ASYNC + wait).
  async requestBySelector(
    msg: Message,
    selector: MessageQueueSelector,
    arg: any,
    timeoutMillis: number = 3000,
  ): Promise<Message> {
    await this._prepareSendRequest(msg, timeoutMillis);
    const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID) as string;
    const future = new RequestResponseFuture(correlationId, msg, timeoutMillis);
    REQUEST_FUTURE_HOLDER.putRequest(correlationId, future);
    const begin = Date.now();
    try {
      await this._sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, (sr, err) => {
        if (err != null) {
          future.setSendRequestOk(false);
          future.putResponseMessage(null);
          future.setCause(err);
        } else {
          future.setSendRequestOk(true);
        }
      }, timeoutMillis);
      return await this._waitResponse(msg, timeoutMillis, future, Date.now() - begin);
    } finally {
      REQUEST_FUTURE_HOLDER.removeRequest(correlationId);
    }
  }

  // Sync request to a pinned queue (Java request(msg, mq, timeout) —
  // sendKernelImpl ASYNC + wait).
  async requestByMq(msg: Message, mq: MessageQueue, timeoutMillis: number = 3000): Promise<Message> {
    if (this.client == null) throw new MQClientException('producer not started');
    await this._prepareSendRequest(msg, timeoutMillis);
    const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID) as string;
    const future = new RequestResponseFuture(correlationId, msg, timeoutMillis);
    REQUEST_FUTURE_HOLDER.putRequest(correlationId, future);
    const begin = Date.now();
    try {
      this.tryToCompressMessage(msg);
      // Java passes topicPublishInfo=null here — a failed attempt can only be
      // retried on the SAME broker (no route to pick another one from).
      await this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, (sr, err) => {
        if (err != null) {
          future.setSendRequestOk(false);
          future.putResponseMessage(null);
          future.setCause(err);
        } else {
          future.setSendRequestOk(true);
        }
      }, null as unknown as TopicPublishInfo);
      return await this._waitResponse(msg, timeoutMillis, future, Date.now() - begin);
    } finally {
      REQUEST_FUTURE_HOLDER.removeRequest(correlationId);
    }
  }

  // Async request (Java request(msg, requestCallback, timeout)). The callback
  // fires exactly once: on reply arrival, on the TTL sweep, or on send failure.
  requestAsync(msg: Message, requestCallback: RequestCallbackLike, timeoutMillis: number = 3000): void {
    void this._requestAsyncDefault(msg, requestCallback, timeoutMillis);
  }

  async _requestAsyncDefault(
    msg: Message,
    requestCallback: RequestCallbackLike,
    timeoutMillis: number,
  ): Promise<void> {
    await this._prepareSendRequest(msg, timeoutMillis);
    const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID) as string;
    const future = new RequestResponseFuture(correlationId, msg, timeoutMillis, requestCallback);
    REQUEST_FUTURE_HOLDER.putRequest(correlationId, future);
    // Callback-form futures are driven by the callback; silence the promise so
    // a rejected reply never surfaces as an unhandled rejection.
    future.promise().catch(() => { /* driven via requestCallback */ });
    try {
      await this._sendDefaultImpl(msg, CommunicationMode.ASYNC, (sr, err) => {
        if (err != null) {
          future.setCause(err);
          this._requestFail(correlationId);
        } else {
          future.setSendRequestOk(true);
        }
      }, timeoutMillis);
    } catch (e) {
      // Pre-dispatch failure (no route / validation): Java routes it to the callback.
      future.setCause(e as Error);
      this._requestFail(correlationId);
    }
  }

  requestAsyncBySelector(
    msg: Message,
    selector: MessageQueueSelector,
    arg: any,
    requestCallback: RequestCallbackLike,
    timeoutMillis: number = 3000,
  ): void {
    void this._requestAsyncBySelector(msg, selector, arg, requestCallback, timeoutMillis);
  }

  async _requestAsyncBySelector(
    msg: Message,
    selector: MessageQueueSelector,
    arg: any,
    requestCallback: RequestCallbackLike,
    timeoutMillis: number,
  ): Promise<void> {
    await this._prepareSendRequest(msg, timeoutMillis);
    const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID) as string;
    const future = new RequestResponseFuture(correlationId, msg, timeoutMillis, requestCallback);
    REQUEST_FUTURE_HOLDER.putRequest(correlationId, future);
    future.promise().catch(() => { /* driven via requestCallback */ });
    try {
      await this._sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, (sr, err) => {
        if (err != null) {
          future.setCause(err);
          this._requestFail(correlationId);
        } else {
          future.setSendRequestOk(true);
        }
      }, timeoutMillis);
    } catch (e) {
      future.setCause(e as Error);
      this._requestFail(correlationId);
    }
  }

  requestAsyncByMq(
    msg: Message,
    mq: MessageQueue,
    requestCallback: RequestCallbackLike,
    timeoutMillis: number = 3000,
  ): void {
    void this._requestAsyncByMq(msg, mq, requestCallback, timeoutMillis);
  }

  async _requestAsyncByMq(
    msg: Message,
    mq: MessageQueue,
    requestCallback: RequestCallbackLike,
    timeoutMillis: number,
  ): Promise<void> {
    if (this.client == null) throw new MQClientException('producer not started');
    await this._prepareSendRequest(msg, timeoutMillis);
    const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID) as string;
    const future = new RequestResponseFuture(correlationId, msg, timeoutMillis, requestCallback);
    REQUEST_FUTURE_HOLDER.putRequest(correlationId, future);
    future.promise().catch(() => { /* driven via requestCallback */ });
    this.tryToCompressMessage(msg);
    try {
      await this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, (sr, err) => {
        if (err != null) {
          future.setCause(err);
          this._requestFail(correlationId);
        } else {
          future.setSendRequestOk(true);
        }
      }, null as unknown as TopicPublishInfo);
    } catch (e) {
      future.setCause(e as Error);
      this._requestFail(correlationId);
    }
  }

  // Java requestFail: remove the future, mark send-not-ok, fire the callback
  // (once) with the recorded cause.
  _requestFail(correlationId: string): void {
    const future = REQUEST_FUTURE_HOLDER.removeRequest(correlationId);
    if (future != null) {
      future.setSendRequestOk(false);
      future.putResponseMessage(null);
      try {
        future.executeRequestCallback();
      } catch (e) {
        logger.warn('execute requestCallback in requestFail, and callback throw: %s', (e as Error).message);
      }
    }
  }

  // Resolve a reply future (called by the reply-message processor in the consumer agent).
  static resolveRequestReply(correlationId: string, replyMsg: Message): void {
    const future = REQUEST_FUTURE_HOLDER.removeRequest(correlationId);
    if (future != null) future.complete(replyMsg);
  }

  // Java DefaultMQProducer.reply: build the reply from the request (topic =
  // <cluster>_REPLY_TOPIC, REPLY_TO_CLIENT = the requestor's clientId — the
  // broker uses it to find the requestor's channel and push 326) and send it
  // through the normal send path.
  async reply(requestMsg: Message, body: Buffer | string, timeoutMillis: number = this.sendMsgTimeout): Promise<SendResult> {
    const replyMsg = createReplyMessage(requestMsg, body);
    return this.send(replyMsg, timeoutMillis);
  }

  // ---- recall ----
  async recallMessage(recallHandle: string): Promise<SendResult> {
    if (this.client == null) throw new MQClientException('producer not started');
    const handle = RecallMessageHandle.parseRecallMessageHandle(recallHandle);
    const topic = RecallMessageHandle.getOriginTopicFromRecallTopic(handle.topic);
    const addr = this.client.findBrokerAddressInPublish(handle.brokerName);
    if (addr == null) throw new MQClientException(`no broker address for ${handle.brokerName}`);
    return this.client.recallMessage(addr, this.producerGroup, topic, recallHandle);
  }

  // ---- topic / offset helpers ----
  // Java DefaultMQProducerImpl.createTopic: fetch the route of `key` from the
  // nameserver FIRST, then issue UPDATE_AND_CREATE_TOPIC to every master broker
  // in that route. Never rely on the client-side broker cache here — right
  // after start() it is empty and would wrongly report "no broker available".
  async createTopic(topic: string, key: string = this.createTopicKey, queueNums: number = this.defaultTopicQueueNums): Promise<void> {
    if (this.client == null) throw new MQClientException('producer not started');
    await this.client.updateTopicRouteInfoFromNameServer(key, true);
    const route = this.client.getTopicRouteData(key);
    const brokerDatas: any[] = (route as any)?.brokerDatas || [];
    let sent = false;
    // Java DefaultMQAdminExtImpl.createAndUpdateTopicConfig sends the config
    // to EVERY broker id in the route (master AND slaves) — a slave that never
    // receives it answers pull with "topic not exist", which kills slave-only
    // consumption. Sending master-only was a node-port gap.
    for (const bd of brokerDatas) {
      const addrs: Record<string, string> = bd.brokerAddrs || {};
      for (const addr of Object.values(addrs)) {
        if (addr == null) continue;
        const resp = await this.client.createTopicInBroker(addr, topic, queueNums);
        if (resp.code !== ResponseCode.SUCCESS) {
          throw new MQClientException(`create topic ${topic} failed on ${addr}: code=${resp.code} remark=${resp.remark || ''}`);
        }
        sent = true;
      }
    }
    if (!sent) throw new MQClientException('Not enough info to create topic');
  }

  async maxOffset(topic: string, queueId: number): Promise<number> {
    const info = await this._tryToFindTopicPublishInfo(topic);
    if (info == null) throw new MQClientException(`no route for ${topic}`);
    const mq = new MessageQueue(topic, info.msgQueueList.length ? info.msgQueueList[0].getBrokerName() : '', queueId);
    const addr = this.client!.publishAddrFor(mq);
    if (addr == null) throw new MQClientException('no broker address');
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, null);
    request.addExtField('topic', this._withoutNamespace(topic));
    request.addExtField('queueId', String(queueId));
    const resp = await this.client!.remotingClient.invokeSync(addr, request, 3000);
    if (resp.code === ResponseCode.SUCCESS) return resp.extFields['offset'] != null ? parseInt(resp.extFields['offset'], 10) : 0;
    throw new MQBrokerException(resp.code, resp.remark || 'GET_MAX_OFFSET failed', addr);
  }

  // ---- batch ----
  private async _flushAccumulation(acc: any): Promise<void> {
    const msgs = acc.messages as Message[];
    if (msgs.length === 0) return;
    const topic = msgs[0].getTopic();
    const mq = new MessageQueue(topic, '', 0);
    const tpInfo = await this._tryToFindTopicPublishInfo(topic);
    if (tpInfo == null || !tpInfo.ok()) return;
    const selected = this._selectOneMessageQueue(tpInfo, null);
    if (selected == null) return;
    const batch = MessageBatch.generateFromList(msgs);
    await this._sendWithHooks(batch, selected, CommunicationMode.SYNC, null, tpInfo);
  }

  // ---- accessors ----
  getMQClient(): MQClient | null { return this.client; }
  getProducerGroup(): string { return this.producerGroup; }
}

// Transaction producer (a DefaultMQProducer that always carries a transaction listener).
export class TransactionMQProducer extends DefaultMQProducer {
  transactionListener: TransactionListener | null;
  constructor(producerGroup: string = MixAll.DEFAULT_PRODUCER_GROUP, transactionListener: TransactionListener | null = null) {
    super(producerGroup);
    this.transactionListener = transactionListener;
  }
}

export default {
  DefaultMQProducer, TransactionMQProducer, MessageQueueSelector,
  SelectMessageQueueByHash, SelectMessageQueueByRandom, LocalTransactionState,
  TransactionListener,
};
