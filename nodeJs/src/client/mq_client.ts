// -*- coding: utf-8 -*-
// MQClient — the central client manager (org.apache.rocketmq.client.impl.MQClientInstance).
// Faithful port of python/rocketmq/client/mq_client.py. It owns: the name-server address list,
// the topic-route cache, the per-topic publish-info table (with the SHARED round-robin cursor),
// the broker-address table, and the low-level send / heartbeat / recall primitives.
//
// NOTE: this module is the single dependency of the producer; it must NOT import producer (the
// consumer/admin agents will import it too). Hooks/transaction policy live in the producer.
import { RemotingClient } from '../remoting/client.ts';
import { RemotingCommand, CURRENT_VERSION } from '../remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import {
  TopicRouteData, findBrokerAddressInPublish, brokerDataList2Map,
} from '../remoting/route.ts';
import {
  SendMessageRequestHeader, SendMessageRequestHeaderV2, SendMessageResponseHeader,
  RecallMessageRequestHeader, EndTransactionRequestHeader,
  QueryConsumerOffsetRequestHeader, QueryConsumerOffsetResponseHeader,
  UpdateConsumerOffsetRequestHeader,
  GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader,
  SearchOffsetRequestHeader, GetEarliestMsgStoretimeRequestHeader,
  PopMessageRequestHeader, PopMessageResponseHeader, AckMessageRequestHeader,
  ChangeInvisibleTimeRequestHeader, ChangeInvisibleTimeResponseHeader,
} from '../remoting/headers.ts';
import { SetMessageRequestModeRequestBody, BatchAckMessageRequestBody } from '../remoting/pop_bodies.ts';
import { PopResult, processPopResponse } from './pop_api.ts';
import {
  HeartbeatData, ProducerData, ConsumerData, ConsumeType, MessageModel, ConsumeFromWhere,
} from '../remoting/heartbeat.ts';
import { NamespaceUtil } from '../remoting/namespace.ts';
import {
  Message, MessageQueue, MessageBatch, MessageExt,
} from '../common/message.ts';
import { MessageSysFlag } from '../common/sysflag.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MessageAccessor } from '../common/message_accessor.ts';
import { MessageType } from '../common/messageType.ts';
import { MixAll } from '../common/mixAll.ts';
import { createUniqID } from '../common/messageClientIdSetter.ts';
import { messageProperties2String, decodeMessage, string2MessageProperties } from '../common/messageDecoder.ts';
import { RecallMessageHandle } from '../common/recall_message_handle.ts';
import { getLogger } from '../logging.ts';
import {
  MQClientException, MQBrokerException, RemotingException,
} from '../remoting/exception.ts';
import { SendResult } from './send_result.ts';
import { ConsumerStatsManager } from './consumer_stats.ts';
import { REQUEST_FUTURE_HOLDER } from './request_reply.ts';
import zlib from 'node:zlib';

const logger = getLogger('mqclient');

// ---------------------------------------------------------------------------
// TopicPublishInfo — holds the shared round-robin cursor used across calls.
// ---------------------------------------------------------------------------
export class TopicPublishInfo {
  orderTopic: boolean;
  msgQueueList: MessageQueue[];
  _index: number;            // shared cursor (mirrors Python sendWhichQueue)
  haveTopicRouterInfo: boolean;

  constructor() {
    this.orderTopic = false;
    this.msgQueueList = [];
    this._index = 0;
    this.haveTopicRouterInfo = false;
  }

  ok(): boolean {
    return this.msgQueueList.length > 0;
  }

  // Round-robin selection. With no lastBrokerName, returns the next queue in rotation
  // (distinct across consecutive calls). When lastBrokerName is given, tries to avoid it.
  selectOneMessageQueue(lastBrokerName: string | null = null, resetIndex: boolean = false): MessageQueue | null {
    const list = this.msgQueueList;
    if (list.length === 0) return null;
    if (resetIndex) this._index = 0;
    if (lastBrokerName == null) {
      const idx = this._index % list.length;
      this._index = (this._index + 1) % list.length;
      return list[idx];
    }
    for (let i = 0; i < list.length; i++) {
      const idx = this._index % list.length;
      this._index = (this._index + 1) % list.length;
      const mq = list[idx];
      if (mq.getBrokerName() !== lastBrokerName) return mq;
    }
    // all queues belong to lastBrokerName: fall back to first
    return list[this._index % list.length];
  }

  resetIndex(): void {
    this._index = 0;
  }
}

// ---------------------------------------------------------------------------
// MQClient
// ---------------------------------------------------------------------------
export class MQClient {
  clientId: string;
  nameServerAddrList: string[];
  nameServerAddress: string | null;
  remotingClient: RemotingClient;
  topicRouteTable: Map<string, TopicRouteData>;
  topicPublishInfoTable: Map<string, TopicPublishInfo>;
  brokerAddrTable: Map<string, Record<number, string>>;
  consumerTable: Map<string, any>;
  producerTable: Map<string, any>;
  heartbeatIntervalMillis: number;
  // Consumer stats (Java MQClientFactory.getConsumerStatsManager — shared at
  // the instance level; ONE sampler timer, 10s precision).
  consumerStatsManager: ConsumerStatsManager;
  _heartbeatTimer: NodeJS.Timeout | null;
  _routeTimer: NodeJS.Timeout | null;
  _routeInitialTimer: NodeJS.Timeout | null;
  _running: boolean;

  constructor(
    clientId: string,
    nameServerAddr: string | null = null,
    remotingClient: RemotingClient | null = null,
  ) {
    this.clientId = clientId;
    this.nameServerAddrList = nameServerAddr ? nameServerAddr.split(';').filter((s) => s.length > 0) : [];
    this.nameServerAddress = nameServerAddr;
    this.remotingClient = remotingClient != null ? remotingClient : new RemotingClient({});
    this.topicRouteTable = new Map();
    this.topicPublishInfoTable = new Map();
    this.brokerAddrTable = new Map();
    this.consumerTable = new Map();
    this.producerTable = new Map();
    this.heartbeatIntervalMillis = 30 * 1000;
    this.consumerStatsManager = new ConsumerStatsManager();
    this._heartbeatTimer = null;
    this._routeTimer = null;
    this._routeInitialTimer = null;
    this._running = false;
  }

  // ---- name server ----
  updateNameServerAddressList(addrs: string): void {
    this.nameServerAddress = addrs;
    this.nameServerAddrList = addrs.split(';').filter((s) => s.length > 0);
  }

  getNameServerAddressList(): string[] {
    return this.nameServerAddrList;
  }

  private _randomNameServer(): string | null {
    const list = this.nameServerAddrList;
    if (list.length === 0) return null;
    return list[Math.floor(Math.random() * list.length)];
  }

  // ---- route management ----
  // Build publish info from a TopicRouteData (mirrors
  // MQClientInstance.topicRouteData2TopicPublishInfo). Returns a TopicPublishInfo whose
  // msgQueueList is the master-writable queue set.
  topicRouteData2TopicPublishInfo(topic: string, routeData: TopicRouteData): TopicPublishInfo {
    const info = new TopicPublishInfo();
    info.orderTopic = false;
    info.msgQueueList = routeData.getAllMessageQueue(topic);
    info.haveTopicRouterInfo = true;
    this.topicRouteTable.set(topic, routeData);
    this._refreshBrokerAddrTable(routeData);
    return info;
  }

  private _refreshBrokerAddrTable(routeData: TopicRouteData): void {
    for (const bd of routeData.brokerDatas) {
      this.brokerAddrTable.set(bd.brokerName, Object.assign({}, bd.brokerAddrs));
    }
  }

  // Register a route directly (used by the producer's pre-warmed cache / tests).
  updateTopicPublishInfo(topic: string, info: TopicPublishInfo): void {
    this.topicPublishInfoTable.set(topic, info);
  }

  getTopicRouteData(topic: string): TopicRouteData | null {
    return this.topicRouteTable.get(topic) != null ? this.topicRouteTable.get(topic)! : null;
  }

  getTopicPublishInfo(topic: string): TopicPublishInfo | null {
    return this.topicPublishInfoTable.get(topic) != null ? this.topicPublishInfoTable.get(topic)! : null;
  }

  // Resolve route from the name server (network). Java semantics: isDefault
  // only changes WHICH topic's route is fetched (TBW102 instead of `topic`);
  // the TBW102 fallback for a missing topic is the PRODUCER's
  // tryToFindTopicPublishInfo second step — never an automatic behaviour of
  // this method. A fallback-derived publish info is TAGGED (`fromDefaultTopic`):
  // its queue list is TBW102's, not the real topic's — the send path uses the
  // tag to refetch the exact route before sending (a stale fallback yields
  // "request queueId[N] is illegal" on every attempt otherwise).
  async updateTopicRouteInfoFromNameServer(topic: string, isDefault: boolean = false): Promise<boolean> {
    const ns = this._randomNameServer();
    if (ns == null) throw new MQClientException('No name server address, please set it first.');
    const realTopic = isDefault ? MixAll.DEFAULT_TOPIC : topic;
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
    request.addExtField('topic', realTopic);
    const response = await this.remotingClient.invokeSync(ns, request, 3000);
    const code = response.code;
    if (code === ResponseCode.SUCCESS) {
      const routeData = TopicRouteData.decode(response.body as Buffer);
      const info = this.topicRouteData2TopicPublishInfo(topic, routeData);
      if (isDefault && topic !== MixAll.DEFAULT_TOPIC) (info as any).fromDefaultTopic = true;
      else (info as any).fromDefaultTopic = false;
      this.topicPublishInfoTable.set(topic, info);
      return true;
    }
    // TOPIC_NOT_EXIST (or anything else): NO automatic fallback to TBW102 —
    // Java returns false here. A silent fallback would poison the consumer's
    // rebalance with TBW102's 8-queue layout ("queueId[N] is illegal").
    return false;
  }

  // ---- broker address lookup (publish side = master only) ----
  findBrokerAddrByTopic(topic: string): string | null {
    const info = this.topicPublishInfoTable.get(topic);
    if (info != null && info.ok()) {
      const mq = info.selectOneMessageQueue(null, true);
      if (mq != null) return this.findBrokerAddressInPublish(mq.getBrokerName());
    }
    const route = this.topicRouteTable.get(topic);
    if (route != null) {
      for (const bd of route.brokerDatas) {
        const master = bd.brokerAddrs[MixAll.MASTER_ID];
        if (master != null) return master;
      }
    }
    return null;
  }

  findBrokerAddressInPublish(brokerName: string): string | null {
    const map = this.brokerAddrTable.get(brokerName);
    if (map && map[MixAll.MASTER_ID] != null) return map[MixAll.MASTER_ID];
    // fall back to route table
    const route = this._routeForBroker(brokerName);
    if (route != null) return findBrokerAddressInPublish(brokerName, route);
    return null;
  }

  private _routeForBroker(brokerName: string): TopicRouteData | null {
    for (const route of this.topicRouteTable.values()) {
      for (const bd of route.brokerDatas) {
        if (bd.brokerName === brokerName) return route;
      }
    }
    return null;
  }

  // Publish address for a specific queue (master only).
  publishAddrFor(mq: MessageQueue): string | null {
    return this.findBrokerAddressInPublish(mq.getBrokerName());
  }

  getBrokerAddrTable(): Map<string, Record<number, string>> { return this.brokerAddrTable; }

  getRouteOfAllBrokers(): TopicRouteData[] {
    return Array.from(this.topicRouteTable.values());
  }

  getAllBrokerAddrs(): Record<string, string> {
    const out: Record<string, string> = {};
    for (const [name, addrs] of this.brokerAddrTable.entries()) {
      if (addrs[MixAll.MASTER_ID] != null) out[name] = addrs[MixAll.MASTER_ID];
    }
    return out;
  }

  // ---- low-level send primitives ----
  // Build the on-wire request command for a message (mirrors Java buildSendRequest /
  // _build_send_request). The UNIQ_KEY must already be set on the message by the caller.
  buildSendRequest(
    msg: Message,
    mq: MessageQueue,
    producerGroup: string,
    namespace: string | null,
  ): RemotingCommand {
    const topic = msg.getTopic();
    const isBatch = msg instanceof MessageBatch;
    const isReply =
      MessageAccessor.getMessageType(msg) === MessageType.REQUEST_REPLY ||
      msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID) != null;
    const properties = msg.getProperties();
    const propsStr = messageProperties2String(properties);
    const sysFlag = (msg as any)._sysFlag != null ? (msg as any)._sysFlag : 0;

    const header = new SendMessageRequestHeader();
    header.producerGroup = producerGroup;
    header.topic = topic;
    header.defaultTopic = MixAll.DEFAULT_TOPIC;
    header.defaultTopicQueueNums = MixAll.DEFAULT_TOPIC_QUEUE_NUMS;
    header.queueId = mq.getQueueId();
    header.sysFlag = sysFlag;
    header.bornTimestamp = Date.now();
    header.flag = msg.getFlag();
    header.properties = propsStr;
    header.reconsumeTimes = null;
    header.unitMode = false;
    header.maxReconsumeTimes = null;
    header.brokerName = mq.getBrokerName();

    if (MixAll.isRetryTopic(topic)) {
      const rt = msg.getProperty(MessageConst.PROPERTY_RECONSUME_TIME);
      const mt = msg.getProperty(MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
      header.reconsumeTimes = rt != null ? parseInt(rt, 10) : 0;
      header.maxReconsumeTimes = mt != null ? parseInt(mt, 10) : 0;
    }

    let code: number;
    if (isReply) code = RequestCode.SEND_REPLY_MESSAGE_V2;
    else if (isBatch) code = RequestCode.SEND_BATCH_MESSAGE;
    else code = RequestCode.SEND_MESSAGE_V2;

    const v2 = SendMessageRequestHeaderV2.createV2(header);
    const request = RemotingCommand.createRequestCommand(code, v2);
    request.body = isBatch ? (msg as MessageBatch).encode() : (msg.getBody() || Buffer.alloc(0));
    return request;
  }

  // Parse a send response into a SendResult (mirrors Java _parse_send_response).
  parseSendResponse(response: RemotingCommand, mq: MessageQueue, brokerAddr: string): SendResult {
    const code = response.code;
    const remark = response.remark;
    const ext = response.extFields || {};
    switch (code) {
      case ResponseCode.SUCCESS:
      case ResponseCode.FLUSH_DISK_TIMEOUT:
      case ResponseCode.FLUSH_SLAVE_TIMEOUT:
      case ResponseCode.SLAVE_NOT_AVAILABLE: {
        const header = new SendMessageResponseHeader();
        header.fromExtFields(ext);
        let status: number;
        if (code === ResponseCode.SUCCESS) status = 0;
        else if (code === ResponseCode.FLUSH_DISK_TIMEOUT) status = 1;
        else if (code === ResponseCode.FLUSH_SLAVE_TIMEOUT) status = 2;
        else status = 3;
        const msgId = header.msgId != null ? header.msgId : '';
        const regionId = ext['MSG_REGION'] != null ? ext['MSG_REGION'] : MixAll.DEFAULT_TRACE_REGION_ID;
        const traceOn = ext['TRACE_ON'] != null ? ext['TRACE_ON'] !== 'false' : true;
        return new SendResult(
          status, msgId, mq, header.queueOffset != null ? header.queueOffset : 0,
          header.transactionId, msgId, regionId, traceOn, header.recallHandle,
        );
      }
      default:
        throw new MQBrokerException(code, remark || `CODE: ${code}`, brokerAddr);
    }
  }

  // Synchronous send to a broker address.
  // NOTE: invokeSync is async — this MUST await it, otherwise parseSendResponse
  // receives a Promise and `response.code` is undefined (surfaced as
  // "CODE: undefined" on every real send).
  async sendMessage(addr: string, request: RemotingCommand, mq: MessageQueue, timeoutMillis: number): Promise<SendResult> {
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    return this.parseSendResponse(response, mq, addr);
  }

  // Asynchronous send. sendCallback = (sendResult: SendResult|null, err: Error|null) => void.
  sendMessageAsync(
    addr: string,
    request: RemotingCommand,
    mq: MessageQueue,
    timeoutMillis: number,
    sendCallback: (sendResult: SendResult | null, err: Error | null) => void,
  ): void {
    this.remotingClient.invokeAsync(addr, request, (response: RemotingCommand | null, err: Error | null) => {
      if (err != null || response == null) {
        sendCallback(null, err != null ? err : new RemotingException('null response'));
        return;
      }
      try {
        sendCallback(this.parseSendResponse(response, mq, addr), null);
      } catch (e) {
        sendCallback(null, e as Error);
      }
    }, timeoutMillis);
  }

  sendMessageOneway(addr: string, request: RemotingCommand): void {
    this.remotingClient.invokeOneway(addr, request);
  }

  // ---- recall ----
  async recallMessage(brokerAddr: string, producerGroup: string, topic: string, recallHandle: string): Promise<SendResult> {
    const handle = RecallMessageHandle.parseRecallMessageHandle(recallHandle);
    const header = new RecallMessageRequestHeader();
    header.producerGroup = producerGroup;
    header.topic = topic;
    header.recallHandle = recallHandle;
    header.bname = handle.brokerName;
    const request = RemotingCommand.createRequestCommand(RequestCode.RECALL_MESSAGE, header);
    return this.sendMessage(brokerAddr, request, new MessageQueue(topic, handle.brokerName, 0), 3000);
  }

  // ---- heartbeat ----
  buildHeartbeatData(): HeartbeatData {
    const hb = new HeartbeatData(this.clientId);
    for (const [group, producer] of this.producerTable.entries()) {
      const pd = new ProducerData(group);
      pd.enable = true;
      hb.producerDataSet.push(pd);
      void producer;
    }
    for (const [group, consumer] of this.consumerTable.entries()) {
      const cd = new ConsumerData(
        group,
        consumer.consumeType != null ? consumer.consumeType : ConsumeType.CONSUME_PASSIVELY,
        consumer.messageModel != null ? consumer.messageModel : MessageModel.CLUSTERING,
        consumer.consumeFromWhere != null ? consumer.consumeFromWhere : ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
      );
      cd.subscriptionDataSet = consumer.subscriptionDataSet || [];
      cd.unitMode = consumer.unitMode === true;
      hb.consumerDataSet.push(cd);
    }
    return hb;
  }

  // Send a heartbeat to a single broker (HEART_BEAT=34, body = heartbeat JSON; fingerprint 0).
  async sendHeartbeat(brokerAddr: string): Promise<RemotingCommand> {
    const hb = this.buildHeartbeatData();
    const request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
    request.body = hb.encode();
    return this.remotingClient.invokeSync(brokerAddr, request, 3000);
  }

  async unregisterClient(brokerAddr: string, producerGroup: string, consumerGroup: string): Promise<RemotingCommand> {
    const request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, null);
    request.addExtField('clientID', this.clientId);
    request.addExtField('producerGroup', producerGroup);
    request.addExtField('consumerGroup', consumerGroup);
    return this.remotingClient.invokeSync(brokerAddr, request, 3000);
  }

  async unregisterClientAllBrokers(producerGroup: string, consumerGroup: string): Promise<void> {
    for (const addr of Object.values(this.getAllBrokerAddrs())) {
      try { await this.unregisterClient(addr, producerGroup, consumerGroup); } catch (e) { /* best effort */ }
    }
  }

  // ---- create topic ----
  // Java MQClientAPIImpl.createTopic (UpdateAndCreateTopicRequestHeader):
  // topic / defaultTopic / readQueueNums / writeQueueNums / perm /
  // topicFilterType / attributes. NOTE: `defaultTopicQueueNums` is a SEND-path
  // field — it is NOT part of this request, and omitting read/writeQueueNums
  // makes the broker decode -1 queues and silently fail to create anything.
  async createTopicInBroker(brokerAddr: string, topic: string, queueNums: number, defaultTopic: string = MixAll.DEFAULT_TOPIC): Promise<RemotingCommand> {
    const request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, null);
    request.addExtField('topic', topic);
    request.addExtField('defaultTopic', defaultTopic);
    request.addExtField('readQueueNums', String(queueNums));
    request.addExtField('writeQueueNums', String(queueNums));
    request.addExtField('perm', '6');
    request.addExtField('topicFilterType', 'SINGLE_TAG');
    request.addExtField('order', 'false');
    return this.remotingClient.invokeSync(brokerAddr, request, 3000);
  }

  async getConsumerListByGroup(brokerAddr: string, consumerGroup: string): Promise<RemotingCommand> {
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, null);
    request.addExtField('consumerGroup', consumerGroup);
    return this.remotingClient.invokeSync(brokerAddr, request, 3000);
  }

  // ---- offset helpers (used by offset store / consumers / admin) ----
  // Master publish addr first, then any addr for this broker in a cached route
  // (Java persistAll's findBrokerAddrByTopic fallback).
  findBrokerAddrForQueue(mq: MessageQueue): string | null {
    const master = this.findBrokerAddressInPublish(mq.getBrokerName());
    if (master) return master;
    const route = this.getTopicRouteData(mq.getTopic());
    if (route) {
      for (const bd of route.brokerDatas || []) {
        if (bd.brokerName === mq.getBrokerName()) {
          const addrs = Object.values(bd.brokerAddrs || {});
          if (addrs.length) return addrs[0];
        }
      }
    }
    return null;
  }

  async queryConsumerOffset(brokerAddr: string, consumerGroup: string, mq: MessageQueue,
    setZeroIfNotFound = false, timeoutMillis = 5000): Promise<{ found: boolean; offset: number }> {
    const h = new QueryConsumerOffsetRequestHeader();
    h.consumerGroup = consumerGroup;
    h.topic = mq.getTopic();
    h.queueId = mq.getQueueId();
    h.setZeroIfNotFound = setZeroIfNotFound;
    const request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, h);
    const response = await this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
    if (response.code === ResponseCode.QUERY_NOT_FOUND) return { found: false, offset: -1 };
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || '', brokerAddr);
    }
    const rh = new QueryConsumerOffsetResponseHeader();
    rh.fromExtFields(response.extFields || {});
    return { found: true, offset: rh.offset == null ? -1 : rh.offset };
  }

  async updateConsumerOffset(brokerAddr: string, consumerGroup: string, mq: MessageQueue,
    offset: number, timeoutMillis = 5000): Promise<void> {
    const h = new UpdateConsumerOffsetRequestHeader();
    h.consumerGroup = consumerGroup;
    h.topic = mq.getTopic();
    h.queueId = mq.getQueueId();
    h.commitOffset = offset;
    const request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, h);
    const response = await this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || '', brokerAddr);
    }
  }

  async getMaxOffset(brokerAddr: string, topic: string, queueId: number, timeoutMillis = 5000): Promise<number> {
    const h = new GetMaxOffsetRequestHeader();
    h.topic = topic;
    h.queueId = queueId;
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, h);
    const response = await this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || '', brokerAddr);
    }
    return (response.extFields && response.extFields['offset'] != null)
      ? parseInt(response.extFields['offset'], 10) : -1;
  }

  async getMinOffset(brokerAddr: string, topic: string, queueId: number, timeoutMillis = 5000): Promise<number> {
    const h = new GetMinOffsetRequestHeader();
    h.topic = topic;
    h.queueId = queueId;
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, h);
    const response = await this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || '', brokerAddr);
    }
    return (response.extFields && response.extFields['offset'] != null)
      ? parseInt(response.extFields['offset'], 10) : -1;
  }

  async searchOffsetByTimestamp(brokerAddr: string, topic: string, queueId: number, timestamp: number,
    timeoutMillis = 5000): Promise<number> {
    const h = new SearchOffsetRequestHeader();
    h.topic = topic;
    h.queueId = queueId;
    h.timestamp = timestamp;
    const request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, h);
    const response = await this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || '', brokerAddr);
    }
    return (response.extFields && response.extFields['offset'] != null)
      ? parseInt(response.extFields['offset'], 10) : -1;
  }

  async getEarliestMsgStoretime(brokerAddr: string, topic: string, queueId: number,
    timeoutMillis = 5000): Promise<number> {
    const h = new GetEarliestMsgStoretimeRequestHeader();
    h.topic = topic;
    h.queueId = queueId;
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, h);
    const response = await this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || '', brokerAddr);
    }
    return (response.extFields && response.extFields['timestamp'] != null)
      ? parseInt(response.extFields['timestamp'], 10) : -1;
  }

  // ---------------------------------------------------------------- POP path
  // (Java MQClientAPIImpl popAsync / ackMessageAsync / changeInvisibleTimeAsync
  // / batchAckMessageAsync / setMessageRequestMode; port of go/client/pop_api.go.)

  // popMessage sends POP_MESSAGE(200050) to one broker and turns the reply into
  // a PopResult with a POP_CK stamped on every message.
  //
  // brokerName is the LOGICAL name the checkpoint must carry (segment 5) and
  // must be the same name the ACK later addresses — sending the physical broker
  // name here makes every ACK unresolvable.
  async popMessage(brokerName: string, addr: string, header: PopMessageRequestHeader,
    namespace: string, timeoutMillis: number): Promise<PopResult> {
    const request = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, header);
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    const respHeader = new PopMessageResponseHeader();
    respHeader.fromExtFields(response.extFields || {});
    return processPopResponse(brokerName, response.code, response.remark || '',
      response.body || null, respHeader, header.topic || '', namespace, header.order === true);
  }

  // ackMessage sends ACK_MESSAGE(200051) synchronously. `offset` must be the
  // checkpoint's segment 7 (the message's own queue offset).
  async ackMessage(addr: string, header: AckMessageRequestHeader, timeoutMillis: number): Promise<void> {
    const request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, header);
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      // Java maps every non-SUCCESS code to AckStatus.NO_EXIST — the broker
      // could not find the checkpoint, which is a warning, not a hard error.
      throw new MQBrokerException(response.code, response.remark || '', addr);
    }
  }

  // batchAckMessage sends BATCH_ACK_MESSAGE(200151) with a pre-built body.
  //
  // NOTE (Java fidelity): the CLASSIC Java client never calls this. Its POP
  // path acks one message at a time through DefaultMQPushConsumerImpl#ackAsync.
  // It is exported because the wire capability is real (the broker implements
  // it and the next-gen/proxy clients use it). Do NOT "finish" the POP
  // consumer by routing its acks through this — that would be inventing client
  // behaviour.
  async batchAckMessage(addr: string, body: BatchAckMessageRequestBody, timeoutMillis: number): Promise<void> {
    // No custom header — Java passes null and rides the body only.
    const request = RemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);
    request.setBody(body.encode());
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || '', addr);
    }
  }

  // changeInvisibleTime sends CHANGE_MESSAGE_INVISIBLETIME(200053) and returns
  // the NEW popTime / invisibleTime the broker assigned.
  //
  // The returned values are not cosmetic: Java rebuilds the checkpoint from
  // them so a later ACK still matches the (now longer) invisibility window.
  async changeInvisibleTime(addr: string, header: ChangeInvisibleTimeRequestHeader,
    timeoutMillis: number): Promise<ChangeInvisibleTimeResponseHeader> {
    const request = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, header);
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || '', addr);
    }
    const rh = new ChangeInvisibleTimeResponseHeader();
    rh.fromExtFields(response.extFields || {});
    return rh;
  }

  // setMessageRequestMode sends SET_MESSAGE_REQUEST_MODE(401) — the
  // broker-side switch that decides whether a (group, topic) pair is served in
  // POP or PULL mode.
  async setMessageRequestMode(addr: string, topic: string, consumerGroup: string,
    mode: string, popShareQueueNum: number, timeoutMillis: number): Promise<void> {
    const body = new SetMessageRequestModeRequestBody();
    body.topic = topic;
    body.consumerGroup = consumerGroup;
    body.mode = mode;
    body.popShareQueueNum = popShareQueueNum;
    const request = RemotingCommand.createRequestCommand(RequestCode.SET_MESSAGE_REQUEST_MODE, null);
    request.setBody(body.encode());
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQClientException(
        `setMessageRequestMode failed, code=${response.code} remark=${response.remark || ''}`);
    }
  }

  // ---- producer / consumer registration (for the eventual consumer/admin agents) ----
  registerProducer(group: string, producer: any): void {
    this.producerTable.set(group, producer);
  }

  unregisterProducer(group: string): void {
    this.producerTable.delete(group);
  }

  registerConsumer(group: string, consumer: any): void {
    this.consumerTable.set(group, consumer);
  }

  unregisterConsumer(group: string): void {
    this.consumerTable.delete(group);
  }

  getRegisteredConsumers(): Map<string, any> { return this.consumerTable; }
  getRegisteredProducers(): Map<string, any> { return this.producerTable; }

  // ---- lifecycle ----
  start(): void {
    this._running = true;
    this._registerTransactionCheckProcessor();
    this._registerReplyMessageProcessor();
    this._startHeartbeatLoop();
    this._startRouteRefreshLoop();
    // Java ConsumerStatsManager.start() is empty (sampling hangs off each
    // StatsItem's scheduler); the unified sampler timer starts here.
    this.consumerStatsManager.start();
  }

  // Java startScheduledTask: MQClientInstance.updateTopicRouteInfoFromNameServer()
  // every pollNameServerInterval (30s), initial delay 10s. Refreshes the route
  // of EVERY topic the consumers subscribe to and the producers publish to —
  // without this, a route registered at the nameserver AFTER the client cached
  // it (e.g. topic created 3s before consumer start; broker registers its
  // topic config on its own cycle) never replaces the stale cache and the
  // rebalance keeps assigning phantom queues.
  private _startRouteRefreshLoop(): void {
    if (this._routeTimer != null) return;
    const beat = () => { void this.updateAllTopicRoutesFromNameServer(); };
    const initial = setTimeout(beat, 10_000);
    if (typeof initial.unref === 'function') initial.unref();
    this._routeTimer = setInterval(beat, 30_000);
    if (typeof this._routeTimer.unref === 'function') this._routeTimer.unref();
    this._routeInitialTimer = initial;
  }

  async updateAllTopicRoutesFromNameServer(): Promise<void> {
    const topics = new Set<string>();
    // Consumer subscriptions (includes the implicit %RETRY%<group>).
    for (const consumer of this.consumerTable.values()) {
      const subs: any = consumer.subscription;
      if (subs != null && typeof subs.keys === 'function') {
        for (const topic of subs.keys()) topics.add(topic);
      }
    }
    // Producer publish topics (everything we ever fetched a publish info for).
    for (const topic of this.topicPublishInfoTable.keys()) topics.add(topic);
    let refreshed = false;
    for (const topic of topics) {
      try {
        if (await this.updateTopicRouteInfoFromNameServer(topic, false)) refreshed = true;
      } catch (e) { /* best effort */ }
    }
    // The first heartbeats of a fresh client go nowhere: brokerAddrTable is
    // only populated by route fetches, and until the broker registers a new
    // topic (up to its 30s cycle) the consumer group is unknown to the broker
    // either. As soon as the first route lands, send the heartbeat at once so
    // the consumer group registers without waiting a full 30s cadence.
    if (refreshed) void this.sendHeartbeatToAllBrokers();
  }

  // Java MQClientInstance schedules sendHeartbeatToAllBrokerWithLock() every
  // 30s (+ an early beat at start). Without a client-level heartbeat the
  // broker's ProducerManager has no channel for the producer group and can
  // never send CHECK_TRANSACTION_STATE back — the transaction check-back path
  // silently dies ("Check transaction failed, channel table is empty").
  // Consumers keep their own loop too; duplicate beats are idempotent.
  private _startHeartbeatLoop(): void {
    if (this._heartbeatTimer != null) return;
    void this.sendHeartbeatToAllBrokers();
    this._heartbeatTimer = setInterval(() => { void this.sendHeartbeatToAllBrokers(); }, 30_000);
    if (typeof (this._heartbeatTimer as any).unref === 'function') this._heartbeatTimer.unref();
  }

  private async sendHeartbeatToAllBrokers(): Promise<void> {
    const addrs = Object.values(this.getAllBrokerAddrs());
    logger.debug('heartbeat beat to %d broker(s): %j', addrs.length, addrs);
    for (const addr of addrs) {
      try { await this.sendHeartbeat(addr); } catch (e) { /* best effort */ }
    }
  }

  // Java ClientRemotingProcessor.checkTransactionState + DefaultMQProducerImpl
  // .checkTransactionState: the broker's transaction-poll thread sends
  // CHECK_TRANSACTION_STATE to the client; the client replies SUCCESS at once,
  // then runs the listener check OFF the read thread (hard rule: a
  // broker-initiated request handler must NEVER issue a synchronous request
  // inline — self deadlock) and finally fires END_TRANSACTION oneway.
  private _registerTransactionCheckProcessor(): void {
    this.remotingClient.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, (cmd: RemotingCommand, addr: string) => {
      const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '');
      let msg: MessageExt | null = null;
      try {
        msg = (cmd.body != null && cmd.body.length > 0) ? decodeMessage(cmd.body, true, false) : null;
      } catch (e) { msg = null; }
      if (msg == null) {
        response.code = ResponseCode.SYSTEM_ERROR;
        response.remark = 'decode message failed';
        return response;
      }
      // transactionId = UNIQ_KEY user property (Java checkTransactionState).
      const uniqKey = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
      if (uniqKey != null) (msg as any).transactionId = uniqKey;
      const group = msg.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
      const producer: any = group != null ? this.producerTable.get(group) : null;
      if (producer == null || typeof producer.transactionListener?.checkLocalTransaction !== 'function') {
        logger.warning('transaction check: no producer registered for group %s', group);
        return response;
      }
      setImmediate(() => {
        try {
          const state = producer.transactionListener.checkLocalTransaction(msg!) | 0;
          const header = new EndTransactionRequestHeader();
          header.producerGroup = group;
          header.tranStateTableOffset = cmd.extFields['tranStateTableOffset'] != null ? parseInt(cmd.extFields['tranStateTableOffset'], 10) : 0;
          header.commitLogOffset = cmd.extFields['commitLogOffset'] != null ? parseInt(cmd.extFields['commitLogOffset'], 10) : 0;
          header.fromTransactionCheck = true;
          header.msgId = uniqKey != null ? String(uniqKey)
            : ((msg as any).msgId != null ? (msg as any).msgId : cmd.extFields['msgId']);
          header.transactionId = (msg as any).transactionId != null ? (msg as any).transactionId
            : (cmd.extFields['transactionId'] != null ? cmd.extFields['transactionId'] : null);
          // LocalTransactionState: COMMIT_MESSAGE=0 / ROLLBACK_MESSAGE=1 / UNKNOW=2
          // (constants live in producer.ts; importing it here would be a cycle).
          if (state === 0) header.commitOrRollback = MessageSysFlag.TRANSACTION_COMMIT_TYPE;
          else if (state === 1) header.commitOrRollback = MessageSysFlag.TRANSACTION_ROLLBACK_TYPE;
          else header.commitOrRollback = MessageSysFlag.TRANSACTION_NOT_TYPE;
          const request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, header);
          this.remotingClient.invokeOneway(addr, request).catch((e) => {
            logger.warning('transaction check END_TRANSACTION oneway failed: %s', (e as Error).message);
          });
        } catch (e) {
          logger.warning('transaction check listener raised: %s', (e as Error).message);
        }
      });
      return response;
    });
  }

  // Java ClientRemotingProcessor.processReplyMsg: the broker pushes the REPLY
  // message of a request(326) to the requesting client. The wire is NOT the 17
  // segment stored format — the message fields ride in the
  // ReplyMessageRequestHeader extFields and only the body is binary. The
  // handler rebuilds the MessageExt, extracts the CORRELATION_ID and resolves
  // the in-flight request future (atomically removed so the timeout-scan path
  // cannot double-resolve).
  private _registerReplyMessageProcessor(): void {
    this.remotingClient.registerProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, (cmd: RemotingCommand, _addr: string) => {
      const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '');
      try {
        const ext = cmd.extFields || {};
        const msg = new MessageExt();
        if (ext['topic'] != null) msg.setTopic(String(ext['topic']));
        msg.setProperties(string2MessageProperties(ext['properties'] != null ? String(ext['properties']) : null));
        msg.setFlag(ext['flag'] != null ? parseInt(ext['flag'], 10) : 0);
        msg.setSysFlag(ext['sysFlag'] != null ? parseInt(ext['sysFlag'], 10) : 0);
        msg.setBornTimestamp(ext['bornTimestamp'] != null ? parseInt(ext['bornTimestamp'], 10) : 0);
        msg.setReconsumeTimes(ext['reconsumeTimes'] != null ? parseInt(ext['reconsumeTimes'], 10) : 0);
        if (ext['bornHost'] != null) { msg.setBornHost(String(ext['bornHost'])); }
        if (ext['storeHost'] != null) { msg.setStoreHost(String(ext['storeHost'])); }
        msg.putProperty(MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME, String(Date.now()));
        // The broker compresses a reply body exactly like a send: sysFlag says so.
        let body = cmd.body != null ? cmd.body : Buffer.alloc(0);
        if (MessageSysFlag.isCompressed(msg.getSysFlag()) && body.length > 0) {
          body = zlib.inflateSync(body);
        }
        msg.setBody(body);
        const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);
        if (correlationId == null) {
          response.code = ResponseCode.SYSTEM_ERROR;
          response.remark = 'reply message has no correlation id';
          return response;
        }
        // Atomically remove so only one of the reply-arrival path and the
        // timeout-scan path can take ownership (Java processReplyMessage).
        const future = REQUEST_FUTURE_HOLDER.removeRequest(correlationId);
        if (future == null) {
          logger.warning('receive reply message, but not matched any request, CorrelationId: %s', correlationId);
          return response;
        }
        // Java wakes the sync caller off the Netty thread — same hard rule here.
        setImmediate(() => future.complete(msg));
      } catch (e) {
        logger.warning('process reply message failed: %s', (e as Error).message);
        response.code = ResponseCode.SYSTEM_ERROR;
        response.remark = 'process reply message fail';
      }
      return response;
    });
  }

  shutdown(): void {
    this._running = false;
    if (this._heartbeatTimer != null) { clearInterval(this._heartbeatTimer); this._heartbeatTimer = null; }
    if (this._routeTimer != null) { clearInterval(this._routeTimer); this._routeTimer = null; }
    if (this._routeInitialTimer != null) { clearTimeout(this._routeInitialTimer); this._routeInitialTimer = null; }
    this.consumerStatsManager.shutdown();
    try { this.remotingClient.shutdown(); } catch (e) { /* ignore */ }
    for (const acc of (this as any)._accumulators || []) { try { acc.stop(); } catch (e) {} }
  }

  isRunning(): boolean { return this._running; }
}

export default { MQClient, TopicPublishInfo };
