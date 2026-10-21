// -*- coding: utf-8 -*-
// DefaultMQProducer — the default message producer, built on top of MQClient.
//
// Send ordering:
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
import type { SocksProxyConfig } from '../remoting/socks_proxy.ts';
import { getLogger } from '../logging.ts';
import {
  MQClientException, MQBrokerException,
} from '../remoting/exception.ts';
import { SendResult, TransactionSendResult, SendStatus } from './send_result.ts';
import { MQClient, TopicPublishInfo, DEFAULT_CONCURRENT_HEARTBEAT_THREAD_POOL_SIZE } from './mq_client.ts';
import { MQClientManager } from './mq_client_manager.ts';
import {
  CommunicationMode, SendMessageContext, CheckForbiddenContext, SendMessageHook,
  EndTransactionContext, hookRegistry,
} from './hook.ts';
import { MQFaultStrategy, tcpDetect } from './latency.ts';
import {
  getOrCreateProduceAccumulator, ProduceAccumulator, MessageAccumulation,
} from './produce_accumulator.ts';
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
// Types must be imported separately: --experimental-strip-types leaves
// non-`import type` imports in the runtime code as-is, and request_reply.ts
// exports types only, so a value import fails at load time with
// "does not provide an export named ...".
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
// match it falls back to the first queue.
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
  namespace: string | null;
  // Server-side namespaceV2 (Aliyun-style serverless instance id). When
  // non-empty, NamespaceRpcHook stamps every request with nsd=true / ns=<value>
  // extFields. Independent from `namespace`, which rewrites resource names on
  // the client side.
  namespaceV2: string | null;
  namesrvAddr: string | null;
  tlsEnable: boolean;
  // TLS details (see setTlsOptions); null = test-mode (trust the self-signed cert).
  tlsOptions: { caCert?: string; clientCert?: string; clientKey?: string; serverName?: string } | null;
  socksProxyConfig: SocksProxyConfig | null;
  rpcHook: AclRPCHook | null;
  unitName: string | null;
  instanceName: string;
  sendLatencyFaultEnable: boolean;
  heartbeatIntervalMillis: number;
  autoBatch: boolean;
  // Auto-batch accumulator knobs, pushed into the accumulator when start()
  // wires it: a buffered key flushes when its group reaches batchMaxBytes or
  // when the batchMaxDelayMs hold timer drains it. -1 means "leave the
  // accumulator's own default" (32 KB / 10 ms).
  batchMaxBytes: number;
  batchMaxDelayMs: number;
  // Broker response codes that make a SYNC send worth another attempt on a
  // DIFFERENT broker: the answer says the broker itself could not take the
  // message (busy / internal error), so the next broker may well succeed. Any
  // other broker answer (a rejected message, a permission failure...) is the
  // caller's to see, so it is raised on the first attempt.
  retryResponseCodes: Set<number>;
  // When a send was accepted but the store answer is not SEND_OK (the disk or
  // the slave lagged), re-pick another broker and retry; with the flag off the
  // not-stored result goes back to the caller unchanged. SYNC only.
  retryAnotherBrokerWhenNotStoreOK: boolean;
  enableTrace: boolean;
  // Custom trace topic; null = RMQ_SYS_TRACE_TOPIC.
  traceTopic: string | null;
  // Compression algorithm for oversized bodies: MessageSysFlag type value
  // (1=LZ4, 2=ZSTD, 3=ZLIB). Default ZLIB for backward compatibility with
  // older brokers. Select via setCompressType('ZLIB'|'LZ4'|'ZSTD').
  compressType: number;
  // When true, send rpcs target the broker's VIP port (listen port - 2).
  // Default false.
  sendMessageWithVIPChannel: boolean;
  // ASYNC retry budget (default 2): the first attempt is NOT counted — a value
  // of 2 means up to 3 total attempts. Deliberately a SEPARATE knob from
  // retryTimesWhenSendFailed: the async chain never reads the sync one.
  retryTimesWhenSendAsyncFailed: number;
  // ZLIB compression level for oversized bodies, 0-9 (default 5).
  // LZ4/ZSTD ignore it.
  compressLevel: number;
  // Arms the MQFaultStrategy probe thread that re-checks isolated brokers.
  // Default false.
  startDetectorEnable: boolean;
  // W3C traceparent passthrough switch. Independent of enableTrace, seeded
  // from ROCKETMQ_TRACE_CONTEXT_ENABLE.
  enableTraceContext: boolean;
  // Heartbeat V2: off by default, so heartbeats keep the V1 shape unless enabled.
  useHeartbeatV2: boolean;
  // Materialise received message bodies during batch decode (default true).
  decodeReadBody: boolean;
  // Internal-API timeout (heartbeat, route fetch, check client, unregister and
  // consumer-list RPCs), in milliseconds. Default 3000.
  mqClientApiTimeout: number;
  // Concurrent heartbeat round (ignored while heartbeat V2 is on).
  enableConcurrentHeartbeat: boolean;
  concurrentHeartbeatThreadPoolSize: number;
  // Page size of the paged metadata admin calls (maxTopicNum / maxGroupNum).
  maxPageSizeInGetMetadata: number;

  client: MQClient | null;
  mqFaultStrategy: MQFaultStrategy;
  producerClientId: string;
  // Message-trace dispatcher — created in start()
  // only when enableTrace is on, torn down in shutdown().
  _traceDispatcher: AsyncTraceDispatcher | null;
  private _traceHook: SendMessageHook | null;
  private _recallTraceHook: any | null;
  private _accumulator: ProduceAccumulator | null;
  // Async-send backpressure: two fair semaphores gate the pending byte and
  // message counts. The gate is enableBackpressureForAsyncMode (default false)
  // — with the gate off the semaphores are never consulted.
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
    this.retryResponseCodes = new Set([
      ResponseCode.SYSTEM_ERROR, ResponseCode.SYSTEM_BUSY,
      ResponseCode.SERVICE_NOT_AVAILABLE, ResponseCode.NO_PERMISSION,
      ResponseCode.TOPIC_NOT_EXIST, ResponseCode.NO_BUYER_ID,
      ResponseCode.NOT_IN_CURRENT_UNIT, ResponseCode.GO_AWAY,
    ]);
    this.namespace = null;
    this.namespaceV2 = null;
    this.namesrvAddr = null;
    this.tlsEnable = false;
    this.tlsOptions = null;
    this.socksProxyConfig = null;
    this.rpcHook = null;
    this.unitName = null;
    this.instanceName = MixAll.DEFAULT_INSTANCE_NAME;
    this.sendLatencyFaultEnable = false;
    this.heartbeatIntervalMillis = 30 * 1000;
    this.autoBatch = false;
    this.batchMaxBytes = -1;
    this.batchMaxDelayMs = -1;
    this.enableTrace = false;
    this.traceTopic = null;
    this.compressType = MessageSysFlag.ZLIB_TYPE;
    this.sendMessageWithVIPChannel = false;
    this.retryTimesWhenSendAsyncFailed = 2;
    this.compressLevel = 5;
    this.startDetectorEnable = false;
    this.enableTraceContext = traceContextEnabledFromEnv();
    this.useHeartbeatV2 = false;
    this.decodeReadBody = true;
    this.mqClientApiTimeout = 3000;
    this.enableConcurrentHeartbeat = false;
    this.concurrentHeartbeatThreadPoolSize = DEFAULT_CONCURRENT_HEARTBEAT_THREAD_POOL_SIZE;
    this.maxPageSizeInGetMetadata = 2000;

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
    const instanceName = MixAll.changeInstanceNameToPid(this.instanceName);
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
  // Server-side namespace accessor. The value is read live by the
  // NamespaceRpcHook on every request, so setting it after start() takes
  // effect too.
  setNamespaceV2(ns: string | null): this { this.namespaceV2 = ns; return this; }
  getNamespaceV2(): string | null { return this.namespaceV2; }
  setUnitName(name: string): this { this.unitName = name; return this; }
  setInstanceName(name: string): this { this.instanceName = name; return this; }
  getInstanceName(): string { return this.instanceName; }
  setTlsEnable(enable: boolean): this { this.tlsEnable = enable; return this; }
  // TLS details (caCert=strict CA verification; clientCert/clientKey=mTLS; serverName=hostname override).
  // Omitting caCert = test-mode (trust the self-signed cert).
  setTlsOptions(opts: { caCert?: string; clientCert?: string; clientKey?: string; serverName?: string } | null): this {
    this.tlsOptions = opts; return this;
  }
  setSocksProxyConfig(config: SocksProxyConfig | null): this { this.socksProxyConfig = config; return this; }
  getSocksProxyConfig(): SocksProxyConfig | null { return this.socksProxyConfig; }
  setSendMsgTimeout(ms: number): this { this.sendMsgTimeout = ms; return this; }
  setEnableTrace(enable: boolean): this { this.enableTrace = enable; return this; }
  // Heartbeat V2 switch. A broker that does
  // not answer the V2 ext fields keeps receiving subscription-carrying bodies, so
  // turning this on against an old broker is harmless.
  setUseHeartbeatV2(enable: boolean): this {
    this.useHeartbeatV2 = enable === true;
    if (this.client) this.client.setUseHeartbeatV2(this.useHeartbeatV2);
    return this;
  }
  getUseHeartbeatV2(): boolean { return this.useHeartbeatV2; }
  // Received-body decode switch. Pushed through to the shared instance so the
  // pull / POP decode paths honour it immediately.
  setDecodeReadBody(enable: boolean): this {
    this.decodeReadBody = enable === true;
    if (this.client) this.client.setDecodeReadBody(this.decodeReadBody);
    return this;
  }
  getDecodeReadBody(): boolean { return this.decodeReadBody; }
  // Internal-API timeout, in milliseconds. An explicit per-call timeout passed
  // by a caller still wins.
  setMqClientApiTimeout(ms: number): this {
    if (!Number.isFinite(ms) || ms <= 0) throw new Error(`mqClientApiTimeout must be > 0, got ${ms}`);
    this.mqClientApiTimeout = Math.floor(ms);
    if (this.client) this.client.setMqClientApiTimeout(this.mqClientApiTimeout);
    return this;
  }
  getMqClientApiTimeout(): number { return this.mqClientApiTimeout; }
  // Concurrent heartbeat round; heartbeat V2 still wins when it is enabled.
  setEnableConcurrentHeartbeat(enable: boolean): this {
    this.enableConcurrentHeartbeat = enable === true;
    if (this.client) this.client.setEnableConcurrentHeartbeat(this.enableConcurrentHeartbeat);
    return this;
  }
  getEnableConcurrentHeartbeat(): boolean { return this.enableConcurrentHeartbeat; }
  setConcurrentHeartbeatThreadPoolSize(size: number): this {
    if (!Number.isInteger(size) || size <= 0) {
      throw new Error(`concurrentHeartbeatThreadPoolSize must be a positive integer, got ${size}`);
    }
    this.concurrentHeartbeatThreadPoolSize = size;
    if (this.client) this.client.setConcurrentHeartbeatThreadPoolSize(size);
    return this;
  }
  getConcurrentHeartbeatThreadPoolSize(): number { return this.concurrentHeartbeatThreadPoolSize; }
  setMaxPageSizeInGetMetadata(size: number): this {
    if (!Number.isInteger(size) || size <= 0) {
      throw new Error(`maxPageSizeInGetMetadata must be a positive integer, got ${size}`);
    }
    this.maxPageSizeInGetMetadata = size;
    if (this.client) this.client.setMaxPageSizeInGetMetadata(size);
    return this;
  }
  getMaxPageSizeInGetMetadata(): number { return this.maxPageSizeInGetMetadata; }
  setTraceTopic(topic: string): this { this.traceTopic = topic; return this; }
  // compressType: 'ZLIB' | 'LZ4' | 'ZSTD'
  // or the raw MessageSysFlag type value (1/2/3).
  setCompressType(compressType: string | number): this {
    this.compressType = compressionTypeByName(compressType);
    return this;
  }
  setCompressMsgBodyOverHowmuch(howmuch: number): this { this.compressMsgBodyOverHowmuch = howmuch; return this; }
  setSendMessageWithVIPChannel(enable: boolean): this {
    this.sendMessageWithVIPChannel = enable;
    // The offset / lock / consumer-list RPCs are issued on the shared instance,
    // so keep its VIP view in step with this facade's send switch.
    if (this.client) this.client.setVipChannelEnabled(enable);
    return this;
  }
  isSendMessageWithVIPChannel(): boolean { return this.sendMessageWithVIPChannel; }
  // ASYNC retry budget (first attempt not counted). Only the async chain reads it.
  setRetryTimesWhenSendAsyncFailed(n: number): this { this.retryTimesWhenSendAsyncFailed = n; return this; }
  getRetryTimesWhenSendAsyncFailed(): number { return this.retryTimesWhenSendAsyncFailed; }
  // SYNC retry budget (first attempt not counted). Only the sync loop reads it.
  setRetryTimesWhenSendFailed(n: number): this { this.retryTimesWhenSendFailed = n; return this; }
  getRetryTimesWhenSendFailed(): number { return this.retryTimesWhenSendFailed; }
  // Whether a "stored, but not stored well" answer makes the sync loop move on to
  // another broker. SYNC only — the async chain delivers it as-is.
  setRetryAnotherBrokerWhenNotStoreOK(enable: boolean): this {
    this.retryAnotherBrokerWhenNotStoreOK = enable === true;
    return this;
  }
  getRetryAnotherBrokerWhenNotStoreOK(): boolean { return this.retryAnotherBrokerWhenNotStoreOK; }
  // Auto-batch switch. Enabling it after start() wires the accumulator right
  // away: a switch that only worked before start() would be a silent no-op for
  // the caller that flipped it on a live producer.
  setAutoBatch(enable: boolean): this {
    this.autoBatch = enable === true;
    if (this.autoBatch && this.client != null) this._wireAccumulator();
    return this;
  }
  isAutoBatch(): boolean { return this.autoBatch; }
  // Beat period of the instance-level heartbeat loop. A live producer applies it
  // to the shared instance at once (which rebuilds its timer); before start() it
  // is the value handed over at registration.
  setHeartbeatIntervalMillis(ms: number): this {
    this.heartbeatIntervalMillis = ms;
    if (this.client != null) this.client.setHeartbeatIntervalMillis(ms);
    return this;
  }
  getHeartbeatIntervalMillis(): number {
    return this.client != null ? this.client.heartbeatIntervalMillis : this.heartbeatIntervalMillis;
  }
  // Replace the whole retryable-code set (a fresh Set; the old contents are not merged).
  setRetryResponseCodes(codes: Iterable<number>): this {
    this.retryResponseCodes = new Set(codes);
    return this;
  }
  getRetryResponseCodes(): Set<number> { return new Set(this.retryResponseCodes); }
  addRetryResponseCode(code: number): this { this.retryResponseCodes.add(code); return this; }
  removeRetryResponseCode(code: number): this { this.retryResponseCodes.delete(code); return this; }
  isRetryResponseCode(code: number | null | undefined): boolean {
    return code != null && this.retryResponseCodes.has(code);
  }
  // Auto-batch gates. -1 keeps the accumulator's own default (32 KB / 10 ms);
  // any other value is range-checked by the accumulator and, when one is
  // already wired, applied to it in place (the hold period rebuilds its timer).
  setBatchMaxBytes(bytes: number): this {
    if (bytes > -1) this._accumulator?.setMaxHoldSize(bytes);
    this.batchMaxBytes = bytes;
    return this;
  }
  getBatchMaxBytes(): number { return this._accumulator?.maxHoldSize ?? this.batchMaxBytes; }
  setBatchMaxDelayMs(ms: number): this {
    if (ms > -1) this._accumulator?.setHoldMs(ms);
    this.batchMaxDelayMs = ms;
    return this;
  }
  getBatchMaxDelayMs(): number { return this._accumulator?.holdMs ?? this.batchMaxDelayMs; }
  // ZLIB compression level 0-9 (default 5).
  setCompressLevel(level: number): this { this.compressLevel = level; return this; }
  getCompressLevel(): number { return this.compressLevel; }
  // Arms the MQFaultStrategy reachability probe. Default off.
  setStartDetectorEnable(enable: boolean): this { this.startDetectorEnable = enable; return this; }
  // Async-send backpressure gate (default false): when off, the in-flight
  // num/size semaphores are not consulted.
  setEnableBackpressureForAsyncMode(enable: boolean): this { this.enableBackpressureForAsyncMode = enable; return this; }
  isEnableBackpressureForAsyncMode(): boolean { return this.enableBackpressureForAsyncMode; }
  setEnableTraceContext(enable: boolean): this { this.enableTraceContext = enable; return this; }
  isEnableTraceContext(): boolean { return this.enableTraceContext; }

  // Runtime backpressure knobs — take effect on the live semaphores.
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

  // Acquire both async-send permits or throw synchronously: a bounded
  // async queue rejects with RemotingTooMuchRequestException when full. A
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
    const instanceName = MixAll.changeInstanceNameToPid(this.instanceName);
    const clientId = MixAll.clientIdFor(instanceName, this.unitName);
    this.producerClientId = clientId;

    const manager = MQClientManager.getInstance();
    const { instance: client, created } = manager.getOrCreateMQClientInstance(clientId, () => {
      const remotingClient = new RemotingClient({
        tlsEnable: this.tlsEnable, tlsOptions: this.tlsOptions, socksProxyConfig: this.socksProxyConfig,
      });
      return new MQClient(clientId, this.namesrvAddr, remotingClient);
    });
    this.client = client;
    manager.registerClient(clientId, client);
    // Keep the shared instance's VIP view aligned with this facade's send switch,
    // so the instance-level offset / lock / consumer-list RPCs honour it too.
    client.setVipChannelEnabled(this.sendMessageWithVIPChannel);
    if (created) {
      if (this.useHeartbeatV2) client.setUseHeartbeatV2(true);
      client.setDecodeReadBody(this.decodeReadBody);
      client.setMqClientApiTimeout(this.mqClientApiTimeout);
      client.setEnableConcurrentHeartbeat(this.enableConcurrentHeartbeat);
      client.setConcurrentHeartbeatThreadPoolSize(this.concurrentHeartbeatThreadPoolSize);
      client.setMaxPageSizeInGetMetadata(this.maxPageSizeInGetMetadata);
      // Pass this facade's configured beat period into the shared instance ONCE
      // at registration: the instance-level heartbeat timer and the consumer
      // facades' own beats read it from there.
      client.setHeartbeatIntervalMillis(this.heartbeatIntervalMillis);
      registerRpcHooks(client.remotingClient, {
        namespaceV2: () => this.namespaceV2,
        userHook: this.rpcHook,
      });
    }
    // Register before start(): the early beat fired by client.start() builds its
    // HEARTBEAT from producerTable, so registering afterwards makes that beat carry
    // no ProducerData and the broker never learns this producer's channel.
    client.registerProducer(this.producerGroup, this);
    client.start();
    this.mqFaultStrategy = new MQFaultStrategy(this.sendLatencyFaultEnable);
    // Fault-strategy probe thread: MQFaultStrategy carries a start/stop
    // detector; resolver maps broker name -> publish addr, detector is a plain
    // TCP connect probe. Both are no-ops unless startDetectorEnable is turned on.
    if (this.startDetectorEnable) {
      this.mqFaultStrategy.setResolver((brokerName: string): string | null =>
        this.client != null ? this.client.findBrokerAddressInPublish(brokerName) : null);
      this.mqFaultStrategy.setServiceDetector((addr: string, timeout: number): Promise<boolean> => tcpDetect(addr, timeout));
      this.mqFaultStrategy.setStartDetectorEnable(true);
    }
    this.mqFaultStrategy.startDetector();
    // RequestFutureHolder housekeeping: the TTL sweep that fails
    // request-reply futures nobody will ever answer.
    REQUEST_FUTURE_HOLDER.startScheduledTask(this);

    if (this.autoBatch) this._wireAccumulator();

    // Message trace wiring: without this, enableTrace is a silent no-op on the
    // produce side (SendMessageHook fires before/after every send).
    if (this.enableTrace) {
      try {
        const { AsyncTraceDispatcher } = await import('./trace_dispatcher.ts');
        const { SendMessageTraceHookImpl } = await import('./trace_hook.ts');
        const dispatcher = new AsyncTraceDispatcher(
          this.producerGroup, 'PRODUCER', 10, this.traceTopic || undefined);
        dispatcher.setHostProducer(this);
        // Propagate the namespace to the internal trace producer so its rpcs
        // carry nsd/ns as well.
        dispatcher.setNamespaceV2(this.namespaceV2);
        this._traceDispatcher = dispatcher;
        this._traceHook = new SendMessageTraceHookImpl(dispatcher);
        hookRegistry.registerSendMessageHook(this._traceHook);
        // Default recall-message trace hook (an RPCHook on the remoting
        // client) gives recall rpcs their own Recall trace records. Gated on this
        // client's own env key com.rocketmq.recall.default.trace.enable, default off.
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
    // NOTE: the accumulator is NOT stopped here — it is instance-level (keyed by
    // clientId) and stops when the shared MQClient really shuts down (refcount 0).
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
      // Hand back to the manager: reference count -1; the REAL shutdown (stop
      // heartbeat/route timers, drop connections) happens only when it hits
      // ZERO. Other facades sharing the same clientId keep the instance alive.
      MQClientManager.getInstance().unregisterClient(this.client.clientId, this.client);
    }
    this.client = null;
  }

  // ---- compression (algorithm selectable; degrade on failure, never throw) ----
  tryToCompressMessage(msg: Message): boolean {
    // sendKernelImpl guards compression with `!(msg instanceof MessageBatch)`:
    // the batch body is the aggregate envelope the broker splits
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
  // tryToFindTopicPublishInfo: first fetch the
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
      // Keep the reason the route could not be fetched: if an error like a TLS
      // handshake CA verification failure were swallowed here, the layer above
      // would only see "No route info of this topic" — a black box (the user
      // hunts toward "create topic" while the real culprit is the
      // certificate/CA). Stash it in _lastRouteError and let the No-route
      // throw site carry it out.
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

  // The cause of the last failure across the two route attempts above (cleared
  // on success). Used to bring the real root cause — TLS/CA, connection
  // refused, etc. — into the "No route info" error instead of black-boxing it.
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

  // Every send invoke wraps the address with
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
    // span; a caller-propagated value is never overwritten. This port builds the
    // equivalent of an OTel hook inline, and it
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
      // The ASYNC path owns its own retry chain (retryTimesWhenSendAsyncFailed,
      // shared time budget, re-select a queue that avoids the failed broker).
      this._sendAsyncKernel(msg, mq, tpInfo, sendCallback, request, addr);
      return null;
    } else {
      await this.client.sendMessageOneway(addr, request);
      return null;
    }
  }

  // Async send retry chain. One logical send may issue up to
  // 1 + retryTimesWhenSendAsyncFailed attempts against the SAME shared budget:
  //   - transport-level failures (timeout / send failure / connection closed)
  //     retry on a queue chosen to avoid the failed broker (needRetry=true);
  //   - a broker answer that fails response parsing (MQBrokerException — error
  //     code) does NOT retry (needRetry=false);
  //   - a parsed SendResult is delivered as-is. The async chain never consults
  //     retryAnotherBrokerWhenNotStoreOK (that flag is sync-only) —
  //     FLUSH_DISK_TIMEOUT etc. go to the caller's onSuccess.
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
        // Async chain: callback failures mark isolation with reachable=true;
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
          // A retried request must reuse the request body but with a FRESH
          // opaque id: the retried request must not collide with the old
          // opaque in the response table.
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
            // Distinguish transport failure (retry) from a broker answer
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
    // This port's SendMessageHook registry is global, not per producer.
    // The inner trace producer opts out (_skipSendHooks) so trace-topic writes
    // are not themselves traced.
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
    // Entry: Validators.checkMessage — topic legality plus body
    // null/empty/maxMessageSize (and the LMQ INNER_MULTI_DISPATCH separator
    // rule) before any route lookup.
    Validators.checkMessage(msg, this.maxMessageSize);
    this._checkLegalTopic(msg.getTopic());
    let tpInfo = await this._tryToFindTopicPublishInfo(msg.getTopic());
    if (tpInfo == null || !tpInfo.ok()) {
      // Carry the real route-fetch failure reason (TLS/CA, connection
      // refused...); with no failure recorded, keep the original wording (the
      // topic genuinely does not exist).
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
    // When every attempt failed with a broker ANSWER (not a transport error),
    // the caller needs that answer — response code and remark — not a generic
    // retry-exhausted message. Auth rejections (NO_PERMISSION) in particular
    // are only provable through the code.
    let lastBrokerErr: MQBrokerException | null = null;
    // Contract: `timesTotal = SYNC ? 1 + retryTimesWhenSendFailed : 1` — only
    // the SYNC path retries inside this loop. ASYNC owns its own chain
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
        // ASYNC / ONEWAY succeed as soon as no exception was
        // thrown (sendKernelImpl returns null for both); only SYNC uses the
        // SendResult as the retry-driving value.
        if (communicationMode === CommunicationMode.ASYNC || communicationMode === CommunicationMode.ONEWAY) {
          return null;
        }
        if (sendResult != null) {
          if (this.sendLatencyFaultEnable) {
            this.mqFaultStrategy.updateFaultItem(mq.getBrokerName(), 0, false, true);
          }
          // "Stored, but not stored well" (flush timeouts) is a finished send unless the
          // caller asked to move on for it. lastBrokerName already holds this broker, so
          // the next pick avoids it; once attempts run out the tail returns this result.
          if (sendResult.sendStatus !== SendStatus.SEND_OK && this.retryAnotherBrokerWhenNotStoreOK) {
            continue;
          }
          return sendResult;
        }
      } catch (e) {
        logger.warn(`send to ${mq.toString()} failed (attempt ${times + 1}): ${(e as Error).message}`);
        if (e instanceof MQBrokerException) {
          lastBrokerErr = e;
          // A broker answer outside the retryable set is a verdict about THIS
          // request, not a sick broker; another broker would give the same
          // answer, so surface it now instead of spending the remaining attempts.
          if (!this.isRetryResponseCode(e.responseCode)) throw e;
        }
        if (this.sendLatencyFaultEnable) {
          this.mqFaultStrategy.updateFaultItem(mq.getBrokerName(), this.sendMsgTimeout, true, false);
        }
        // lastBrokerName keeps THIS broker: the next pick is steered away from the
        // one that just failed, which is the whole point of retrying. Clearing it
        // would leave the next attempt to plain round-robin and could re-send to
        // the same broker while another one is healthy.
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
      // Surface the broker's own answer (response code + remark) when every
      // attempt was rejected BY THE BROKER — a generic "retries exhausted"
      // would erase the reason (e.g. ACL NO_PERMISSION 16) that the caller
      // must assert on. Transport-style failures keep the original message.
      if (lastBrokerErr != null) throw lastBrokerErr;
      throw new MQClientException('failed to send message after retries');
    }
    return sendResult;
  }

  // ---- public send API ----
  async send(msg: Message, timeoutMillis?: number): Promise<SendResult> {
    // autoBatch: a message with no explicit timeout rides a group and this call
    // awaits that group's send. An explicit timeout opts out — the hold timer
    // would add latency the caller already bounded.
    if (timeoutMillis === undefined && this._canAutoBatch(msg)) {
      const batched = await this._sendByAccumulator(msg, null, null);
      if (batched != null) return batched;
    }
    const r = await this._sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeoutMillis ?? this.sendMsgTimeout);
    if (r == null) throw new MQClientException('null send result');
    return r;
  }

  async sendOneway(msg: Message, timeoutMillis?: number): Promise<void> {
    await this._sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, timeoutMillis ?? this.sendMsgTimeout);
  }

  async sendAsync(msg: Message, sendCallback: SendCallback, timeoutMillis?: number): Promise<void> {
    // autoBatch takes async sends the same way: join the group, and the group's
    // send settles this callback. Nothing is acquired from the async backpressure
    // semaphores, because the batch leaves through the sync path.
    if (timeoutMillis === undefined && this._canAutoBatch(msg)) {
      await this._sendByAccumulator(msg, null, sendCallback);
      return;
    }
    // Async-send backpressure: the two fair semaphores bound the in-flight
    // async sends, but ONLY while enableBackpressureForAsyncMode is on
    // (default false). A full
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
      await this._sendDefaultImpl(msg, CommunicationMode.ASYNC, wrapped, timeoutMillis ?? this.sendMsgTimeout);
    } catch (e) {
      // A sendAsync caller hands over a SendCallback and normally does not attach
      // a handler to the returned promise, so re-throwing here would turn an
      // ordinary send failure into an unhandled rejection that kills the process.
      // With a callback the callback owns the error; a callback-less caller still
      // gets the rejection.
      if (sendCallback) {
        releaseOnce();
        try {
          sendCallback(null, e as Error);
        } catch (cbErr) {
          logger.warn('execute sendCallback in sendAsync failure, and callback throw: %s',
            (cbErr as Error).message);
        }
        return;
      }
      releaseOnce();
      throw e;
    }
  }

  // sendSelectImpl: route lookup + selector pick + kernel send, shared by
  // sendBySelector and the two selector-shaped request forms.
  async _sendSelectImpl(
    msg: Message,
    selector: MessageQueueSelector,
    arg: any,
    communicationMode: number,
    sendCallback: SendCallback | null,
    timeoutMillis: number,
  ): Promise<SendResult | null> {
    // sendSelectImpl validates via Validators.checkMessage before the route
    // lookup.
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
  // sendMessageInTransaction(msg, arg) resolves the listener from
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
    // sendMessageInTransaction contract: a listener exception
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
    // invisible until the (much later) check-back. The wire protocol requires
    // the enum be mapped to the sys-flag value.
    // ⚠ offset semantics on the endTransaction wire contract:
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
    // EndTransactionHook wraps the one request that actually ends the
    // transaction on the wire. Same discipline as the send-hook call site:
    // before hooks run outside the try, after hooks run on both the success
    // and the exception path, and a synchronous throw is re-raised after the
    // after hooks have seen the context.
    const endCtx = new EndTransactionContext(this.producerGroup, msg, brokerAddr);
    endCtx.msgId = header.msgId != null ? String(header.msgId) : null;
    endCtx.transactionId = header.transactionId;
    endCtx.transactionState = localTransactionState;
    endCtx.fromTransactionCheck = false;
    const endHooks = hookRegistry.endTransactionHooks;
    for (const hook of endHooks) hook.endTransactionBefore(endCtx);
    try {
      // A synchronous try around an async call catches nothing: the rejection would
      // escape as an unhandled rejection and kill the process.
      this.client.remotingClient.invokeOneway(brokerAddr, request)
        .catch((e) => { logger.warn(`end transaction failed: ${(e as Error).message}`); });
      // Trace: fire the EndTransactionTraceHook from endTransaction — the
      // EndTransaction record of a transaction message.
      if (this._traceDispatcher != null) {
        try {
          this._traceDispatcher.appendEndTransaction(
            this.producerGroup, msg != null ? msg.getTopic() : '',
            header.msgId || '', header.transactionId, localTransactionState,
            false, brokerAddr);
        } catch (e) { /* trace never breaks the data path */ }
      }
    } catch (e) {
      for (const hook of endHooks) hook.endTransactionAfter(endCtx);
      throw e;
    }
    for (const hook of endHooks) hook.endTransactionAfter(endCtx);
  }

  // ---- request-reply ----
  // Public surface: 3 sync forms (msg / msg+selector / msg+mq)
  // and 3 async forms (msg+callback / msg+selector+callback / msg+mq+callback).
  // prepareSendRequest: CORRELATION_ID + REPLY_TO_CLIENT (the requester's
  // clientId — the broker uses it to route the reply back via 326) + TTL, then
  // make sure the topic route exists (prepared eagerly so the ASYNC send does
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
    // The broker routes the reply back by looking REPLY_TO_CLIENT up in its
    // ProducerManager, which is filled only by a HEARTBEAT carrying this
    // producer's ProducerData. Neither the beat of start() (no route yet, so no
    // broker to beat to) nor the 10s/30s refresh reaches a request issued right
    // after start(), so the reply would be undeliverable and the request would
    // only ever time out. Warm the route, then beat once, before the caller
    // registers the future and sends.
    if (this.client != null) {
      if (this.client.getTopicRouteData(msg.getTopic()) == null
        || this.client.getAllBrokerAddrEntries().length === 0) {
        await this._tryToFindTopicPublishInfo(msg.getTopic());
      }
      await this.client.sendHeartbeatToAllBrokers();
    }
  }

  // waitResponse contract: the reply must arrive within (timeout - cost); a
  // null reply distinguishes send-OK-but-reply-timeout from send-failure.
  async _waitResponse(msg: Message, timeoutMillis: number, future: RequestResponseFuture,
    cost: number): Promise<Message> {
    let timer: NodeJS.Timeout | null = null;
    try {
      return await new Promise<Message>((resolve, reject) => {
        timer = setTimeout(() => {
          // This body runs on Node's timer stack: anything thrown here is an
          // uncaughtException and kills the process, and the awaiting caller is
          // never told. Settle the future first, then reject.
          const err = future.sendRequestOk
            ? new MQClientException(
              `send request message to <${msg.getTopic()}> OK, but wait reply message timeout, ${timeoutMillis} ms.`)
            : new MQClientException(`send request message to <${msg.getTopic()}> fail`);
          future.completeExceptionally(new MQClientException('request timeout, no reply message.'));
          reject(err);
        }, Math.max(1, timeoutMillis - cost));
        future.promise().then(resolve, reject);
      });
    } finally {
      if (timer != null) clearTimeout(timer);
    }
  }

  // Sync request: request(msg, timeout) — sendDefaultImpl ASYNC + wait.
  async request(msg: Message, timeoutMillis: number = 3000): Promise<Message> {
    await this._prepareSendRequest(msg, timeoutMillis);
    const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID) as string;
    const future = new RequestResponseFuture(correlationId, msg, timeoutMillis);
    REQUEST_FUTURE_HOLDER.putRequest(correlationId, future);
    const begin = Date.now();
    try {
      await this._sendDefaultImpl(msg, CommunicationMode.ASYNC, (sr, err) => {
        // SendCallback contract: only mark the request as sent — the caller is
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

  // Sync request through a selector — sendSelectImpl ASYNC + wait.
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

  // Sync request to a pinned queue — sendKernelImpl ASYNC + wait.
  async requestByMq(msg: Message, mq: MessageQueue, timeoutMillis: number = 3000): Promise<Message> {
    if (this.client == null) throw new MQClientException('producer not started');
    await this._prepareSendRequest(msg, timeoutMillis);
    const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID) as string;
    const future = new RequestResponseFuture(correlationId, msg, timeoutMillis);
    REQUEST_FUTURE_HOLDER.putRequest(correlationId, future);
    const begin = Date.now();
    try {
      this.tryToCompressMessage(msg);
      // topicPublishInfo=null here: a failed attempt can only be
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

  // Async request: request(msg, requestCallback, timeout). The callback
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
      // Pre-dispatch failure (no route / validation) routes to the callback.
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

  // requestFail: remove the future, mark send-not-ok, fire the callback
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

  // reply(): build the reply from the request (topic =
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
  // createTopic: fetch the route of `key` from the
  // nameserver FIRST, then issue UPDATE_AND_CREATE_TOPIC to every master broker
  // in that route. Never rely on the client-side broker cache here — right
  // after start() it is empty and would wrongly report "no broker available".
  async createTopic(topic: string, key: string = this.createTopicKey, queueNums: number = this.defaultTopicQueueNums): Promise<void> {
    if (this.client == null) throw new MQClientException('producer not started');
    await this.client.updateTopicRouteInfoFromNameServer(key, true);
    const route = this.client.getTopicRouteData(key);
    const brokerDatas: any[] = (route as any)?.brokerDatas || [];
    let sent = false;
    // createAndUpdateTopicConfig must send the config
    // to EVERY broker id in the route (master AND slaves) — a slave that never
    // receives it answers pull with "topic not exist", which kills slave-only
    // consumption.
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
  // Wire this facade's group buffer onto the shared instance. The accumulator is
  // instance-level (keyed by clientId) so several producers over one instance
  // share one buffer, and it is registered on the instance so a real shutdown
  // stops it and flushes what is still held.
  private _wireAccumulator(): void {
    const client = this.client!;
    this._accumulator = getOrCreateProduceAccumulator(client.clientId);
    // Applied before the timer starts so a flush goes out on the configured
    // period rather than on the accumulator's default.
    if (this.batchMaxBytes > -1) this._accumulator.setMaxHoldSize(this.batchMaxBytes);
    if (this.batchMaxDelayMs > -1) this._accumulator.setHoldMs(this.batchMaxDelayMs);
    this._accumulator.setSender(async (acc) => { await this._flushAccumulation(acc); });
    this._accumulator.start();
    const accs: ProduceAccumulator[] = ((client as any)._accumulators ||= []);
    if (!accs.includes(this._accumulator)) accs.push(this._accumulator);
  }

  // Whether auto-batch takes this message. False sends it the plain way: no
  // accumulator is wired (autoBatch off, or start() not done), or the message
  // must not ride a group.
  private _canAutoBatch(msg: Message): boolean {
    const accumulator = this._accumulator;
    return this.autoBatch && accumulator != null && !accumulator.isStopped() && accumulator.isBatchable(msg);
  }

  // Auto-batch entry: hand one message to the group of its (topic, queue,
  // waitStoreMsgOK, tag) key and wait for that group's send. With a callback the
  // outcome belongs to that callback and the returned promise resolves as soon
  // as the message joined the group; without one the caller awaits this
  // message's own SendResult.
  //
  // A caller that passed an explicit timeout is NOT routed here: the hold timer
  // would add latency the caller already bounded.
  private async _sendByAccumulator(
    msg: Message,
    mq: MessageQueue | null,
    sendCallback: SendCallback | null,
  ): Promise<SendResult | null> {
    const accumulator = this._accumulator;
    if (accumulator == null) throw new MQClientException('producer not started');
    try {
      // Same entry guard as the direct path: an illegal topic or body must not
      // reach the accumulator's table.
      Validators.checkMessage(msg, this.maxMessageSize);
      this._checkLegalTopic(msg.getTopic());
      // UNIQ_KEY is stamped BEFORE joining: the group leaves as one packed
      // message with one answer, and that answer is split back per member by
      // this key. The direct path stamps it inside sendKernelImpl, which a batch
      // never reaches per member.
      if (!msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)) {
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
      }
      const tpInfo = await this._tryToFindTopicPublishInfo(msg.getTopic());
      if (tpInfo == null || !tpInfo.ok()) {
        throw new MQClientException(
          `No route info of this topic: ${msg.getTopic()}${this._routeFailureSuffix()}`,
          this._lastRouteError ?? undefined,
        );
      }
      const target = mq ?? this._selectOneMessageQueue(tpInfo, null);
      if (target == null) throw new MQClientException(`no writable queue for topic ${msg.getTopic()}`);
      const { future, flushNow } = accumulator.accumulate(msg, target);
      // Crossing the byte gate means this group is complete: it leaves now
      // instead of waiting for the hold timer.
      if (flushNow != null) void this._flushAccumulation(flushNow);
      if (sendCallback == null) return await future.promise;
      future.promise.then(
        (sr) => sendCallback(sr, null),
        (e) => sendCallback(null, e as Error),
      ).catch((cbErr) => logger.warn('accumulator send callback threw: %s', (cbErr as Error).message));
      return null;
    } catch (e) {
      // A callback-owning caller must not see a rejection on the returned
      // promise; the callback gets the error instead.
      if (sendCallback == null) throw e;
      try {
        sendCallback(null, e instanceof Error ? e : new Error(String(e)));
      } catch (cbErr) {
        logger.warn('accumulator send callback threw: %s', (cbErr as Error).message);
      }
      return null;
    }
  }

  // Send one buffered group as a single packed message and settle every caller
  // waiting on it. Every exit settles: no route, no queue, a broker refusal and
  // a successful send all reach the group's futures, because a group left
  // unsettled would keep its callers' promises pending forever.
  private async _flushAccumulation(acc: MessageAccumulation): Promise<void> {
    if (acc.isEmpty()) return;
    const msgs = acc.messages;
    const topic = msgs[0].getTopic();
    try {
      // Read the cached route first and only fall back to the (awaiting) lookup
      // on a miss. A drain triggered by shutdown has to reach the transport
      // before the facade detaches its client, and an await in between would
      // turn "flush what is still buffered" into "fail it with not started".
      const cached = this.client != null ? this.client.getTopicPublishInfo(topic) : null;
      const tpInfo = cached != null && cached.ok() && !(cached as any).fromDefaultTopic
        ? cached
        : await this._tryToFindTopicPublishInfo(topic);
      if (tpInfo == null || !tpInfo.ok()) {
        throw new MQClientException(
          `No route info of this topic: ${topic}${this._routeFailureSuffix()}`,
          this._lastRouteError ?? undefined,
        );
      }
      // A route lookup that had to go to the name server leaves a gap the
      // caller's shutdown can fall into; the transport calls below all need the
      // instance, so say what actually happened instead of dereferencing null.
      if (this.client == null) throw new MQClientException('producer not started');
      // The group was keyed around this queue; picking another one would merge
      // queues the callers deliberately kept apart. A queue that left the route
      // makes this send fail, and the group's callers learn that failure.
      const target = acc.entries[0].mq;
      const batch = MessageBatch.generateFromList(msgs);
      const result = await this._sendWithHooks(batch, target, CommunicationMode.SYNC, null, tpInfo);
      if (result == null) throw new MQClientException('null send result');
      acc.complete(result);
    } catch (e) {
      acc.fail(e instanceof Error ? e : new Error(String(e)));
    }
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
