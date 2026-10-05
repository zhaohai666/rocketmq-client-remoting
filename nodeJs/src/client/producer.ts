// -*- coding: utf-8 -*-
// DefaultMQProducer — the default message producer (org.apache.rocketmq.client.producer.*).
// Faithful port of python/client/producer.py built on top of MQClient.
//
// Send ordering (mirrors Java, enforced by the cross-port rules):
//   send() -> _send_default_impl (retry loop) -> _send_with_hooks (SendMessageHook before/after)
//         -> sendKernelImpl (compress -> set UNIQ_KEY -> CheckForbiddenHook [NOT swallowed]
//            -> build request -> resolve broker addr -> actual transport via MQClient).
import zlib from 'node:zlib';
import { RemotingClient } from '../remoting/client.ts';
import { RemotingCommand } from '../remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { EndTransactionRequestHeader } from '../remoting/headers.ts';
import { Message, MessageQueue, MessageBatch, MessageExt } from '../common/message.ts';
import { MessageSysFlag } from '../common/sysflag.ts';
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
import { getLogger } from '../logging.ts';
import {
  MQClientException, MQBrokerException,
} from '../remoting/exception.ts';
import { SendResult, TransactionSendResult, SendStatus } from './send_result.ts';
import { MQClient, TopicPublishInfo } from './mq_client.ts';
import {
  CommunicationMode, SendMessageContext, CheckForbiddenContext, hookRegistry,
} from './hook.ts';
import { MQFaultStrategy } from './latency.ts';
import { getOrCreateProduceAccumulator, ProduceAccumulator } from './produce_accumulator.ts';
import { FairSemaphore } from './backpressure.ts';
import { injectTraceContext, traceContextEnabledFromEnv } from './traceparent.ts';
import {
  createCorrelationId, createReplyMessage, isReplyMessage, RequestCallback,
  REQUEST_FUTURE_HOLDER, getReplyTopic, RequestResponseFuture,
} from './request_reply.ts';

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
  namesrvAddr: string | null;
  tlsEnable: boolean;
  rpcHook: AclRPCHook | null;
  unitName: string | null;
  sendLatencyFaultEnable: boolean;
  heartbeatIntervalMillis: number;
  autoBatch: boolean;
  enableTrace: boolean;
  // Java DefaultMQProducer.sendMessageWithVIPChannel (default false): send
  // rpcs target the broker's VIP port (listen port - 2).
  sendMessageWithVIPChannel: boolean;
  // W3C traceparent passthrough switch (Java delegates this to an external
  // OTel/SkyWalking hook); independent of enableTrace, seeded from
  // ROCKETMQ_TRACE_CONTEXT_ENABLE like every other port.
  enableTraceContext: boolean;

  client: MQClient | null;
  mqFaultStrategy: MQFaultStrategy;
  producerClientId: string;
  private _accumulator: ProduceAccumulator | null;
  // Async-send backpressure (Java DefaultMQProducer backPressureForAsyncSendNum
  // / Size + the two fair semaphores built in initAsyncConfig).
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
    this.namesrvAddr = null;
    this.tlsEnable = false;
    this.rpcHook = null;
    this.unitName = null;
    this.sendLatencyFaultEnable = false;
    this.heartbeatIntervalMillis = 30 * 1000;
    this.autoBatch = false;
    this.enableTrace = false;
    this.sendMessageWithVIPChannel = false;
    this.enableTraceContext = traceContextEnabledFromEnv();

    this.client = null;
    this.mqFaultStrategy = new MQFaultStrategy(this.sendLatencyFaultEnable);
    this.producerClientId = this._buildClientId();
    this._accumulator = null;
    this.backPressureForAsyncSendNum = 1000;
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
  setUnitName(name: string): this { this.unitName = name; return this; }
  setTlsEnable(enable: boolean): this { this.tlsEnable = enable; return this; }
  setSendMsgTimeout(ms: number): this { this.sendMsgTimeout = ms; return this; }
  setEnableTrace(enable: boolean): this { this.enableTrace = enable; return this; }
  setSendMessageWithVIPChannel(enable: boolean): this { this.sendMessageWithVIPChannel = enable; return this; }
  isSendMessageWithVIPChannel(): boolean { return this.sendMessageWithVIPChannel; }
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

  start(): void {
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

    const remotingClient = new RemotingClient({ tlsEnable: this.tlsEnable });
    this.client = new MQClient(clientId, this.namesrvAddr, remotingClient);
    if (this.rpcHook != null) {
      this.client.remotingClient.registerRpcHook(this.rpcHook);
    }
    this.client.start();
    this.client.registerProducer(this.producerGroup, this);
    this.mqFaultStrategy = new MQFaultStrategy(this.sendLatencyFaultEnable);

    if (this.autoBatch) {
      this._accumulator = getOrCreateProduceAccumulator(clientId);
      this._accumulator.setSender(async (acc) => {
        await this._flushAccumulation(acc);
      });
      this._accumulator.start();
    }
    logger.info(`producer ${this.producerGroup} started, clientId=${clientId}`);
  }

  shutdown(): void {
    if (this._accumulator != null) this._accumulator.stop();
    if (this.client != null) {
      this.client.unregisterProducer(this.producerGroup);
      this.client.shutdown();
    }
    this.client = null;
  }

  // ---- compression (ZLIB only; degrade on failure, never throw) ----
  tryToCompressMessage(msg: Message): boolean {
    const body = msg.getBody();
    if (body == null || body.length < this.compressMsgBodyOverHowmuch) return false;
    if (MessageSysFlag.isCompressed((msg as any)._sysFlag != null ? (msg as any)._sysFlag : 0)) return false;
    try {
      const compressed = zlib.deflateSync(body);
      (msg as any)._sysFlag = MessageSysFlag.setCompressionType(
        ((msg as any)._sysFlag != null ? (msg as any)._sysFlag : 0) | MessageSysFlag.COMPRESSED_FLAG,
        MessageSysFlag.ZLIB_TYPE,
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
    try {
      await this.client.updateTopicRouteInfoFromNameServer(topic, false);
    } catch (e) {
      logger.warn(`find topic publish info for ${topic} failed: ${(e as Error).message}`);
    }
    info = this.client.getTopicPublishInfo(topic);
    if (info != null && info.ok() && !(info as any).fromDefaultTopic) return info;
    // Second step: DEFAULT-topic fallback (auto-create path).
    try {
      await this.client.updateTopicRouteInfoFromNameServer(topic, true);
    } catch (e) {
      logger.warn(`find default-topic publish info for ${topic} failed: ${(e as Error).message}`);
    }
    info = this.client.getTopicPublishInfo(topic);
    if (info != null && info.ok()) return info;
    return info;
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
    _tpInfo: TopicPublishInfo,
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
      return this.client.sendMessage(addr, request, mq, this.sendMsgTimeout);
    } else if (communicationMode === CommunicationMode.ASYNC) {
      this.client.sendMessageAsync(addr, request, mq, this.sendMsgTimeout, (sr, err) => {
        if (sendCallback) sendCallback(sr, err);
      });
      return null;
    } else {
      this.client.sendMessageOneway(addr, request);
      return null;
    }
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
    for (const hook of hookRegistry.sendMessageHooks) hook.sendMessageBefore(ctx);
    let sendResult: SendResult | null = null;
    try {
      sendResult = await this.sendKernelImpl(msg, mq, communicationMode, sendCallback, tpInfo);
      ctx.sendResult = sendResult;
      ctx.exception = null;
    } catch (e) {
      ctx.exception = e as Error;
      ctx.sendResult = null;
      for (const hook of hookRegistry.sendMessageHooks) hook.sendMessageAfter(ctx);
      throw e;
    }
    for (const hook of hookRegistry.sendMessageHooks) hook.sendMessageAfter(ctx);
    return sendResult;
  }

  // Retry-loop default send implementation.
  async _sendDefaultImpl(
    msg: Message,
    communicationMode: number,
    sendCallback: SendCallback | null,
    timeoutMillis: number,
  ): Promise<SendResult | null> {
    this._checkLegalTopic(msg.getTopic());
    let tpInfo = await this._tryToFindTopicPublishInfo(msg.getTopic());
    if (tpInfo == null || !tpInfo.ok()) {
      throw new MQClientException(`No route info of this topic: ${msg.getTopic()}`);
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
    const maxTimes = this.retryTimesWhenSendFailed + 1;
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
    // Async-send backpressure (Java initAsyncConfig + onExceptionImpl): the
    // two fair semaphores bound the in-flight async sends; a full semaphore
    // throws synchronously. Permits return when the transport-level callback
    // fires — exactly once — or when the send fails before reaching it.
    this._acquireAsyncPermits(msg);
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

  async sendBySelector(
    msg: Message,
    selector: MessageQueueSelector,
    arg: any,
    timeoutMillis: number = this.sendMsgTimeout,
  ): Promise<SendResult> {
    this._checkLegalTopic(msg.getTopic());
    const tpInfo = await this._tryToFindTopicPublishInfo(msg.getTopic());
    if (tpInfo == null || !tpInfo.ok()) {
      throw new MQClientException(`No route info of this topic: ${msg.getTopic()}`);
    }
    const mq = selector.select(msg, tpInfo.msgQueueList, arg);
    if (mq == null) throw new MQClientException('failed to select a message queue');
    this.tryToCompressMessage(msg);
    const r = await this._sendWithHooks(msg, mq, CommunicationMode.SYNC, null, tpInfo);
    if (r == null) throw new MQClientException('null send result');
    return r;
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
    const localState = listener.executeLocalTransaction(msg, arg);
    await this._endTransaction(sendResult, localState);
    const tsr = new TransactionSendResult(
      sendResult.sendStatus, sendResult.msgId, sendResult.messageQueue, sendResult.queueOffset,
      sendResult.transactionId, sendResult.offsetMsgId, sendResult.regionId, sendResult.traceOn, sendResult.recallHandle,
    );
    tsr.setLocalTransactionState(localState);
    return tsr;
  }

  private async _endTransaction(sendResult: SendResult, localTransactionState: number): Promise<void> {
    if (this.client == null) return;
    const brokerAddr = this.client.publishAddrFor(sendResult.messageQueue!);
    if (brokerAddr == null) return;
    const header = new EndTransactionRequestHeader();
    header.producerGroup = this.producerGroup;
    header.transactionId = sendResult.transactionId != null ? sendResult.transactionId : sendResult.msgId;
    header.commitOrRollback = localTransactionState;
    header.fromTransactionCheck = false;
    header.msgId = sendResult.msgId;
    header.bname = sendResult.messageQueue != null ? sendResult.messageQueue.getBrokerName() : null;
    const request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, header);
    try {
      this.client.remotingClient.invokeOneway(brokerAddr, request);
    } catch (e) {
      logger.warn(`end transaction failed: ${(e as Error).message}`);
    }
  }

  // ---- request-reply ----
  // Java prepareSendRequest: CORRELATION_ID + REPLY_TO_CLIENT (the requester's
  // clientId — the broker uses it to route the reply back via 326) + TTL. The
  // reply itself returns as a PUSH_REPLY_MESSAGE_TO_CLIENT(326) push handled by
  // MQClient._registerReplyMessageProcessor.
  async request(msg: Message, timeoutMillis: number = 3000): Promise<Message> {
    const correlationId = createCorrelationId();
    const requestClientId = this.client != null ? this.client.clientId : this.producerClientId;
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String(timeoutMillis));
    MessageAccessor.setMessageType(msg, MessageType.REQUEST_REPLY);

    const future = new RequestResponseFuture(correlationId, msg, timeoutMillis);
    REQUEST_FUTURE_HOLDER.putRequest(correlationId, future);

    try {
      await this.send(msg, timeoutMillis);
    } catch (e) {
      REQUEST_FUTURE_HOLDER.removeRequest(correlationId);
      throw e;
    }
    const timer = setTimeout(() => {
      if (!future.isDone()) {
        REQUEST_FUTURE_HOLDER.removeRequest(correlationId);
        future.completeExceptionally(new MQClientException('request timeout'));
      }
    }, timeoutMillis);
    try {
      return await future.promise();
    } finally {
      clearTimeout(timer);
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
    for (const bd of brokerDatas) {
      const addrs: Record<string, string> = bd.brokerAddrs || {};
      // selectBrokerAddr: master (brokerId 0) preferred, else any.
      const addr = addrs['0'] != null ? addrs['0'] : Object.values(addrs)[0];
      if (addr != null) {
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
