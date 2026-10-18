// -*- coding: utf-8 -*-
// MQClient — the central client manager. It owns: the name-server address list,
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
import { CheckClientRequestBody } from '../remoting/bodies.ts';
import { PopResult, processPopResponse } from './pop_api.ts';
import {
  HeartbeatData, ProducerData, ConsumerData, ConsumeType, MessageModel, ConsumeFromWhere,
  decodeHeartbeatV2Flags,
} from '../remoting/heartbeat.ts';
import { NamespaceUtil } from '../remoting/namespace.ts';
import {
  Message, MessageQueue, MessageBatch, MessageExt,
} from '../common/message.ts';
import { MessageSysFlag } from '../common/sysflag.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MessageAccessor } from '../common/message_accessor.ts';
import { MessageType } from '../common/messageType.ts';
import { ExpressionType } from '../common/subscriptionData.ts';
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
import { decompressFor } from '../common/compress.ts';
import { DefaultTopAddressing } from './top_addressing.ts';
import {
  QueryAssignmentRequestBody, QueryAssignmentResponseBody,
} from '../remoting/pop_bodies.ts';

const logger = getLogger('mqclient');

// ---------------------------------------------------------------------------
// TopicPublishInfo — holds the shared round-robin cursor used across calls.
// ---------------------------------------------------------------------------
export type FacadeProcessor = (cmd: RemotingCommand, addr: string) => RemotingCommand | null;

export class TopicPublishInfo {
  orderTopic: boolean;
  msgQueueList: MessageQueue[];
  _index: number;            // shared round-robin cursor
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
  topicEndPointsTable: Map<string, Map<string, string>>;
  brokerAddrTable: Map<string, Record<number, string>>;
  consumerTable: Map<string, any>;
  producerTable: Map<string, any>;
  heartbeatIntervalMillis: number;
  // Heartbeat V2. Off by default — the reference switch is a JVM system property
  // that reads "false", and a beat with fingerprint 0 makes the broker take its
  // plain registration path, exactly as before.
  useHeartbeatV2: boolean;
  // Addresses whose HEART_BEAT response declared IS_SUPPORT_HEART_BEAT_V2.
  _brokerSupportV2HeartbeatSet: Set<string>;
  // Last fingerprint accepted per broker address; a beat whose fingerprint
  // matches this cache may drop its subscription sets.
  _brokerAddrHeartbeatFingerprintTable: Map<string, number>;
  // Client-level API timeout (default 3000 ms): the timeout used by
  // the instance-level admin calls, including CHECK_CLIENT_CONFIG(46).
  mqClientApiTimeout: number;
  // Consumer stats manager — shared at the instance level; ONE sampler timer,
  // 10s precision.
  consumerStatsManager: ConsumerStatsManager;
  _heartbeatTimer: NodeJS.Timeout | null;
  _routeTimer: NodeJS.Timeout | null;
  _routeInitialTimer: NodeJS.Timeout | null;
  // Dynamic name-server addressing: fetchNameServerAddr via TopAddressing
  // every 2min, initial delay 10s — ONLY when the name server address was not
  // explicitly set.
  _topAddressing: DefaultTopAddressing | null;
  _namesrvTimer: NodeJS.Timeout | null;
  _namesrvInitialTimer: NodeJS.Timeout | null;
  // adjustThreadPool task: every 1 minute every
  // consumer of this instance is offered a pool-size adjustment decision.
  _adjustThreadPoolTimer: NodeJS.Timeout | null;
  // persistAllConsumerOffset task (Java ClientConfig.persistConsumerOffsetInterval,
  // default 5000ms): the INSTANCE-level offset flush. The push consumer keeps its
  // own 5s loop, but the pull / lite-pull facades only flushed on shutdown —
  // a crash lost every offset since the start. Java schedules the first tick
  // after a 10s initial delay, then every persistConsumerOffsetInterval.
  persistConsumerOffsetInterval: number;
  _persistOffsetTimer: NodeJS.Timeout | null;
  _persistOffsetInitialTimer: NodeJS.Timeout | null;
  // Nameserver sticky/round-robin semantics: reuse the last NS that answered.
  // Prefer it; only fall back to the next one when it fails. Without this stickiness + rotation,
  // when one of several NS is down every route fetch would pick randomly and fail on the dead one.
  _namesrvChosen: string | null;
  _running: boolean;
  private _facadeProcessors: Map<number, FacadeProcessor[]>;

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
    this.topicEndPointsTable = new Map();
    this.brokerAddrTable = new Map();
    this.consumerTable = new Map();
    this.producerTable = new Map();
    this.heartbeatIntervalMillis = 30 * 1000;
    this.useHeartbeatV2 = false;
    this._brokerSupportV2HeartbeatSet = new Set();
    this._brokerAddrHeartbeatFingerprintTable = new Map();
    this.mqClientApiTimeout = 3000;
    this.consumerStatsManager = new ConsumerStatsManager();
    this._heartbeatTimer = null;
    this._routeTimer = null;
    this._routeInitialTimer = null;
    this._topAddressing = null;
    this._namesrvTimer = null;
    this._namesrvInitialTimer = null;
    this._adjustThreadPoolTimer = null;
    this.persistConsumerOffsetInterval = 5000;
    this._persistOffsetTimer = null;
    this._persistOffsetInitialTimer = null;
    this._namesrvChosen = null;
    this._running = false;
    this._facadeProcessors = new Map();
  }

  // ---- name server ----
  updateNameServerAddressList(addrs: string): void {
    this.nameServerAddress = addrs;
    this.nameServerAddrList = addrs.split(';').filter((s) => s.length > 0);
  }

  getNameServerAddressList(): string[] {
    return this.nameServerAddrList;
  }

  // Apply a TopAddressing push of a new nameserver list.
  // Returns the accepted address (null/unchanged -> no-op).
  onNameServerAddressChange(namesrvAddress: string | null): string | null {
    if (namesrvAddress != null && namesrvAddress !== this.nameServerAddress) {
      logger.info('name server address changed, old=%s, new=%s', this.nameServerAddress, namesrvAddress);
      this.updateNameServerAddressList(namesrvAddress);
      return this.nameServerAddress;
    }
    return this.nameServerAddress;
  }

  // Poll the TopAddressing HTTP endpoint for the current nameserver list;
  // a changed answer is pushed through the change callback.
  async fetchNameServerAddr(): Promise<string | null> {
    if (this._topAddressing == null) return this.nameServerAddress;
    try {
      return await this._topAddressing.fetchNsAddr();
    } catch (e) {
      logger.debug('fetchNameServerAddr exception: %s', (e as Error).message);
      return this.nameServerAddress;
    }
  }

  private _randomNameServer(): string | null {
    const list = this.nameServerAddrList;
    if (list.length === 0) return null;
    return list[Math.floor(Math.random() * list.length)];
  }

  // Nameserver call stickiness/rotation: the last NS that answered (_namesrvChosen) is preferred;
  // if it was never configured or has failed, start from a random position and
  // walk the **entire list**, electing the first one to answer and sticking to it. When one of several
  // NS is down, the old implementation (a single random pick) hit the dead one and the whole route
  // fetch failed — with 3 up and 1 down that was ~33% of calls dead, and a fresh process hitting the dead
  // one on its first route fetch showed up as "no route info" / zero-consumption timeouts.
  private async _invokeSyncNameServer(request: RemotingCommand, timeoutMillis: number): Promise<RemotingCommand> {
    const list = this.nameServerAddrList;
    if (list.length === 0) throw new MQClientException('No name server address, please set it first.');
    const chosenIdx = this._namesrvChosen != null ? list.indexOf(this._namesrvChosen) : -1;
    const begin = chosenIdx >= 0 ? chosenIdx : Math.floor(Math.random() * list.length);
    let lastErr: Error | null = null;
    for (let i = 0; i < list.length; i++) {
      const ns = list[(begin + i) % list.length];
      try {
        const resp = await this.remotingClient.invokeSync(ns, request, timeoutMillis);
        this._namesrvChosen = ns;
        return resp;
      } catch (e) {
        lastErr = e instanceof Error ? e : new Error(String(e));
        // The elected one died: drop stickiness so the next round restarts from the next server in the list.
        if (this._namesrvChosen === ns) this._namesrvChosen = null;
        logger.warn('name server %s unreachable (%s), trying next', ns, lastErr.message);
      }
    }
    throw lastErr != null ? lastErr : new MQClientException('all name servers failed');
  }

  topicRouteData2TopicPublishInfo(topic: string, routeData: TopicRouteData): TopicPublishInfo {
    const info = new TopicPublishInfo();
    info.orderTopic = false;
    info.msgQueueList = routeData.getAllMessageQueue(topic);
    info.haveTopicRouterInfo = true;
    this.topicRouteTable.set(topic, routeData);
    const endpoints = routeData.getStaticTopicEndPoints(topic);
    if (endpoints != null) {
      const byKey = new Map<string, string>();
      for (const [mq, real] of endpoints.entries()) {
        byKey.set(`${mq.topic}@${mq.brokerName}@${mq.queueId}`, real);
      }
      this.topicEndPointsTable.set(topic, byKey);
    } else {
      this.topicEndPointsTable.delete(topic);
    }
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

  // Resolve route from the name server (network). Contract: isDefault
  // only changes WHICH topic's route is fetched (TBW102 instead of `topic`);
  // the TBW102 fallback for a missing topic is the PRODUCER's
  // tryToFindTopicPublishInfo second step — never an automatic behaviour of
  // this method. A fallback-derived publish info is TAGGED (`fromDefaultTopic`):
  // its queue list is TBW102's, not the real topic's — the send path uses the
  // tag to refetch the exact route before sending (a stale fallback yields
  // "request queueId[N] is illegal" on every attempt otherwise).
  async updateTopicRouteInfoFromNameServer(topic: string, isDefault: boolean = false): Promise<boolean> {
    const realTopic = isDefault ? MixAll.DEFAULT_TOPIC : topic;
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
    request.addExtField('topic', realTopic);
    // NS failover happens inside _invokeSyncNameServer (it rotates through all
    // NS, see its comment); here we only distinguish "a reachable NS explicitly
    // says the topic does not exist" from "no NS is reachable": the former
    // returns false (no automatic TBW102 fallback), the latter rethrows to the
    // caller — the thrown exception carries the last NS's failure reason
    // (connection refused / TLS handshake failure...), and producers/consumers
    // attach it to their own route error instead of black-boxing it as
    // "No route info".
    const response = await this._invokeSyncNameServer(request, 3000);
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
    // return false. A silent fallback would poison the consumer's
    // rebalance with TBW102's 8-queue layout ("queueId[N] is illegal").
    return false;
  }

  // ---- broker address lookup (publish side = master only) ----
  findBrokerAddrByTopic(topic: string): string | null {
    const info = this.topicPublishInfoTable.get(topic);
    if (info != null && info.ok()) {
      const mq = info.selectOneMessageQueue(null, true);
      if (mq != null) return this.findBrokerAddressInPublish(this.getBrokerNameFromMessageQueue(mq));
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

  getBrokerNameFromMessageQueue(mq: MessageQueue): string {
    const endpoints = this.topicEndPointsTable.get(mq.topic);
    if (endpoints != null && endpoints.size > 0) {
      const real = endpoints.get(`${mq.topic}@${mq.brokerName}@${mq.queueId}`);
      if (real != null) return real;
    }
    return mq.brokerName;
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

  publishAddrFor(mq: MessageQueue): string | null {
    return this.findBrokerAddressInPublish(this.getBrokerNameFromMessageQueue(mq));
  }

  getBrokerAddrTable(): Map<string, Record<number, string>> { return this.brokerAddrTable; }

  // Every (id, addr) pair of every broker we know — masters AND slaves.
  // Heartbeat and unregister iterate the whole brokerAddrTable; restricting
  // to masters breaks slave-only topologies
  // (a slave that never received a heartbeat answers pulls with
  // "subscription not exist").
  getAllBrokerAddrEntries(): Array<{ brokerName: string; id: number; addr: string }> {
    const out: Array<{ brokerName: string; id: number; addr: string }> = [];
    for (const [name, addrs] of this.brokerAddrTable.entries()) {
      for (const [idStr, addr] of Object.entries(addrs)) {
        out.push({ brokerName: name, id: parseInt(idStr, 10), addr });
      }
    }
    return out;
  }

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

  // ---- CHECK_CLIENT_CONFIG(46) ----
  // Pre-flight broker-side subscription validation. Only non-TAG
  // subscriptions go on the wire: a broker that does not know 46 answers with
  // a remoting error, and a silently-broken SQL92 filter would otherwise
  // degrade into "subscribe to everything" (ExpressionMessageFilter admits all
  // messages when the expression does not compile). Sending 46 turns that into
  // a start-up failure.
  async checkClientInBroker(): Promise<void> {
    for (const [group, consumer] of this.consumerTable.entries()) {
      const subs: Map<string, any> | undefined = consumer.subscription;
      if (subs == null || subs.size === 0) {
        // Return (do not `continue`) on a subscription-less consumer — the
        // whole instance is not ready yet.
        return;
      }
      await this.checkSubscriptionsInBroker(group, subs, this.mqClientApiTimeout);
    }
  }

  // The inner loop, exposed for callers that keep consumers out of the table.
  async checkSubscriptionsInBroker(
    group: string,
    subs: Map<string, any>,
    timeoutMillis: number,
  ): Promise<void> {
    for (const sub of subs.values()) {
      // TAG classification: null / "" / "TAG" all count as TAG.
      const exprType = sub.expressionType;
      if (exprType == null || exprType === '' || exprType === ExpressionType.TAG) continue;
      // One broker per cluster is checked; the rest are assumed identical.
      const addr = this.findBrokerAddrByTopic(sub.topic);
      if (addr == null) continue;
      try {
        await this.checkClientConfig(addr, group, sub, timeoutMillis);
      } catch (e) {
        if (e instanceof MQClientException) throw e;
        // Transport-class failure (a broker too old to know 46) uses the fixed
        // wording below — callers match on it. The failure also takes the
        // consumer start down.
        throw new MQClientException(
          `Check client in broker error, maybe because you use ${exprType} to filter message, `
          + 'but server has not been upgraded to support!'
          + 'This error would not affect the launch of consumer, but may has impact on message '
          + 'receiving if you have use the new features which are not supported by server, '
          + 'please check the log!',
          e as Error,
        );
      }
    }
  }

  // One CHECK_CLIENT_CONFIG(46) round trip. The custom header is null and the
  // body carries CheckClientRequestBody; any non-SUCCESS code becomes an
  // MQClientException carrying the broker's own response code.
  async checkClientConfig(
    addr: string,
    consumerGroup: string,
    sub: any,
    timeoutMillis: number,
  ): Promise<void> {
    const request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);
    const body = new CheckClientRequestBody();
    body.clientId = this.clientId;
    body.group = consumerGroup;
    body.subscriptionData = sub.toDict ? sub.toDict() : sub;
    request.body = body.encode();
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQClientException(
        `CODE: ${response.code} exception message: ${response.remark ?? ''}`,
      );
    }
  }

  // ---- low-level send primitives ----
  // Build the on-wire request command for a message. The UNIQ_KEY must
  // already be set on the message by the caller.
  buildSendRequest(
    msg: Message,
    mq: MessageQueue,
    producerGroup: string,
    namespace: string | null,
  ): RemotingCommand {
    const topic = msg.getTopic();
    const isBatch = msg instanceof MessageBatch;
    // Wire rule: only a REPLY (MSG_TYPE == "reply") goes out as
    // SEND_REPLY_MESSAGE_V2. A REQUEST carries MSG_TYPE=Request_Reply_Msg and
    // CORRELATION_ID but must go as a NORMAL send — the reply processor does
    // not stamp CLUSTER and would push the "reply" straight back to the
    // client named by REPLY_TO_CLIENT (i.e. the requestor itself).
    const isReply = MessageAccessor.getMessageType(msg) === MessageType.REPLY;
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
    // The broker only takes the batch path when the header says so: requestCode
    // alone leaves the packed body stored as ONE ordinary message, which no
    // consumer can read back as its individual members.
    header.batch = isBatch;

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

  // Parse a send response into a SendResult. Wire rule: for a single
  // (non-batch) message the SendResult.msgId is the UNIQ_KEY client id
  // (offsetMsgId keeps the physical broker id). The trace Pub record and the
  // console join on this.
  parseSendResponse(response: RemotingCommand, mq: MessageQueue, brokerAddr: string,
    msg: Message | null = null): SendResult {
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
        const offsetMsgId = header.msgId != null ? header.msgId : '';
        const uniqKey = msg != null
          ? msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
          : null;
        const msgId = uniqKey != null && String(uniqKey).length > 0 ? String(uniqKey) : offsetMsgId;
        const regionId = ext['MSG_REGION'] != null ? ext['MSG_REGION'] : MixAll.DEFAULT_TRACE_REGION_ID;
        const traceOn = ext['TRACE_ON'] != null ? ext['TRACE_ON'] !== 'false' : true;
        return new SendResult(
          status, msgId, mq, header.queueOffset != null ? header.queueOffset : 0,
          header.transactionId, offsetMsgId, regionId, traceOn, header.recallHandle,
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
  async sendMessage(addr: string, request: RemotingCommand, mq: MessageQueue, timeoutMillis: number,
    msg: Message | null = null): Promise<SendResult> {
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    return this.parseSendResponse(response, mq, addr, msg);
  }

  // Asynchronous send. sendCallback = (sendResult: SendResult|null, err: Error|null) => void.
  sendMessageAsync(
    addr: string,
    request: RemotingCommand,
    mq: MessageQueue,
    timeoutMillis: number,
    sendCallback: (sendResult: SendResult | null, err: Error | null) => void,
    msg: Message | null = null,
  ): void {
    this.remotingClient.invokeAsync(addr, request, (response: RemotingCommand | null, err: Error | null) => {
      if (err != null || response == null) {
        sendCallback(null, err != null ? err : new RemotingException('null response'));
        return;
      }
      try {
        sendCallback(this.parseSendResponse(response, mq, addr, msg), null);
      } catch (e) {
        sendCallback(null, e as Error);
      }
    }, timeoutMillis);
  }

  sendMessageOneway(addr: string, request: RemotingCommand): Promise<void> {
    // Returned, not dropped: a connection failure here must reach the awaiting
    // producer.sendOneway caller instead of surfacing as an unhandled rejection.
    return this.remotingClient.invokeOneway(addr, request);
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
  // buildHeartbeatData(withoutSub) mirrors the reference's two heartbeat
  // renderings: withoutSub keeps every consumer's identity but empties its
  // subscription set, which is what lets a V2 broker skip registration work it
  // already did.
  buildHeartbeatData(withoutSub = false): HeartbeatData {
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
      if (!withoutSub) cd.subscriptionDataSet = consumer.subscriptionDataSet || [];
      cd.unitMode = consumer.unitMode === true;
      hb.consumerDataSet.push(cd);
    }
    hb.withoutSub = withoutSub;
    return hb;
  }

  setUseHeartbeatV2(enable: boolean): this {
    if (this.useHeartbeatV2 === (enable === true)) return this;
    this.useHeartbeatV2 = enable === true;
    // Turning the switch either way invalidates what the caches mean, so start the
    // conversation with every broker over again.
    this._brokerSupportV2HeartbeatSet.clear();
    this._brokerAddrHeartbeatFingerprintTable.clear();
    return this;
  }

  // Send one heartbeat to one broker (HEART_BEAT=34, body = heartbeat JSON).
  //
  // V1 (the default): the body carries subscriptions and fingerprint 0, so a broker
  // takes its plain registration path and never answers the V2 flags.
  //
  // V2: the fingerprint of this client's subscription snapshot rides on the body —
  // a non-zero fingerprint is exactly what makes the broker answer
  // IS_SUPPORT_HEART_BEAT_V2 / IS_SUB_CHANGE. Once THIS address has declared
  // support and the fingerprint it last accepted is unchanged, the beat goes out
  // without subscription sets; a response saying the subscriptions changed drops
  // the cached fingerprint so the next beat reports them again.
  async sendHeartbeat(brokerAddr: string): Promise<RemotingCommand> {
    const hb = this.buildHeartbeatData(false);
    if (!this.useHeartbeatV2) return this._sendHeartbeatBody(brokerAddr, hb);
    const fingerprint = hb.computeHeartbeatFingerprint();
    hb.heartbeatFingerprint = fingerprint;
    const withoutSub = this.buildHeartbeatData(true);
    withoutSub.heartbeatFingerprint = fingerprint;
    const skipSub = this._brokerSupportV2HeartbeatSet.has(brokerAddr) &&
      this._brokerAddrHeartbeatFingerprintTable.get(brokerAddr) === fingerprint;
    const response = await this._sendHeartbeatBody(brokerAddr, skipSub ? withoutSub : hb);
    const flags = decodeHeartbeatV2Flags(response);
    if (flags.supportV2) {
      this._brokerSupportV2HeartbeatSet.add(brokerAddr);
      if (flags.subChange) this._brokerAddrHeartbeatFingerprintTable.delete(brokerAddr);
      else this._brokerAddrHeartbeatFingerprintTable.set(brokerAddr, fingerprint);
    }
    return response;
  }

  private async _sendHeartbeatBody(brokerAddr: string, hb: HeartbeatData): Promise<RemotingCommand> {
    const request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
    request.body = hb.encode();
    // The reference sends the beat with the client-level API timeout, not a fixed
    // one, so setMqClientApiTimeout governs the heartbeat too.
    return this.remotingClient.invokeSync(brokerAddr, request, this.mqClientApiTimeout);
  }

  async unregisterClient(brokerAddr: string, producerGroup: string, consumerGroup: string): Promise<RemotingCommand> {
    const request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, null);
    request.addExtField('clientID', this.clientId);
    request.addExtField('producerGroup', producerGroup);
    request.addExtField('consumerGroup', consumerGroup);
    return this.remotingClient.invokeSync(brokerAddr, request, 3000);
  }

  async unregisterClientAllBrokers(producerGroup: string, consumerGroup: string): Promise<void> {
    // Unregister goes to every broker entry (incl. slaves).
    for (const { addr } of this.getAllBrokerAddrEntries()) {
      try { await this.unregisterClient(addr, producerGroup, consumerGroup); } catch (e) { /* best effort */ }
    }
  }

  // ---- create topic ----
  // UPDATE_AND_CREATE_TOPIC(17) extFields:
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
  // (persistAll's findBrokerAddrByTopic fallback). Static-topic logical queues
  // resolve their real broker name first (endpoints table).
  findBrokerAddrForQueue(mq: MessageQueue): string | null {
    const master = this.findBrokerAddressInPublish(this.getBrokerNameFromMessageQueue(mq));
    if (master) return master;
    const realName = this.getBrokerNameFromMessageQueue(mq);
    const route = this.getTopicRouteData(mq.getTopic());
    if (route) {
      for (const bd of route.brokerDatas || []) {
        if (bd.brokerName === realName) {
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
  // POP/ACK/CHANGE_INVISIBLE/BATCH_ACK/SET_MESSAGE_REQUEST_MODE primitives.

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
      // Every non-SUCCESS code means the broker could not find the checkpoint
      // (AckStatus.NO_EXIST on the wire) — a warning, not a hard error.
      throw new MQBrokerException(response.code, response.remark || '', addr);
    }
  }

  // batchAckMessage sends BATCH_ACK_MESSAGE(200151) with a pre-built body.
  //
  // NOTE: the classic remoting POP path acks ONE message at a time via
  // ACK_MESSAGE(200051). This primitive is exported because the broker-side
  // capability is real and next-gen/proxy clients use it. Do NOT "finish" the
  // POP consumer by routing its acks through this — that would be inventing
  // client behaviour.
  async batchAckMessage(addr: string, body: BatchAckMessageRequestBody, timeoutMillis: number): Promise<void> {
    // No custom header — the request rides the body only.
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
  // The returned values are not cosmetic: the caller rebuilds the checkpoint
  // from them so a later ACK still matches the (now longer) invisibility
  // window.
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
    request.body = body.encode();
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQClientException(
        `setMessageRequestMode failed, code=${response.code} remark=${response.remark || ''}`);
    }
  }

  // ---- QUERY_ASSIGNMENT(400) — broker-side queue assignment. Used by the
  // push consumer's rebalance when clientRebalance is OFF: the broker decides
  // which queues (and in which MessageRequestMode) this client consumes.
  // Returns null when the broker answered with an invalid/empty result — the
  // caller treats null as "skip the update", never "I own nothing".
  async queryAssignment(addr: string, topic: string, consumerGroup: string, clientId: string,
    strategyName: string, messageModel: string, timeoutMillis = 3000,
  ): Promise<Array<{ mq: MessageQueue; mode: string }> | null> {
    const body = new QueryAssignmentRequestBody();
    body.topic = topic;
    body.consumerGroup = consumerGroup;
    body.clientId = clientId;
    body.messageModel = messageModel;
    body.strategyName = strategyName;
    const request = RemotingCommand.createRequestCommand(RequestCode.QUERY_ASSIGNMENT, null);
    request.body = body.encode();
    const response = await this.remotingClient.invokeSync(addr, request, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || 'queryAssignment failed', addr);
    }
    return QueryAssignmentResponseBody.decode(response.body as Buffer);
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

  // ---- facade-level reverse-request processors (shared-instance multicast) ----
  // Facades register reverse-request processors on the instance (under a shared
  // instance they fan out by registration count and never overwrite each other).
  // Note: do NOT mix remotingClient.registerProcessor direct slots under the
  // same code — a direct slot would displace the dispatcher here. The
  // instance's built-in 39/326 processors are registered through here as well.
  registerFacadeProcessor(requestCode: number, handler: FacadeProcessor): void {
    let list = this._facadeProcessors.get(requestCode);
    if (list == null) {
      list = [];
      this._facadeProcessors.set(requestCode, list);
      this.remotingClient.registerProcessor(requestCode, (cmd: RemotingCommand, addr: string) =>
        this.dispatchFacadeProcessor(cmd, addr));
    }
    list.push(handler);
  }

  // Remove your own processors when a facade exits; the last remover unloads the transport-layer slot.
  unregisterFacadeProcessor(requestCode: number, handler: FacadeProcessor): void {
    const list = this._facadeProcessors.get(requestCode);
    if (list == null) return;
    const idx = list.indexOf(handler);
    if (idx >= 0) list.splice(idx, 1);
    if (list.length === 0) {
      this._facadeProcessors.delete(requestCode);
      this.remotingClient.unregisterProcessor(requestCode);
    }
  }

  dispatchFacadeProcessor(cmd: RemotingCommand, addr: string): RemotingCommand | null {
    const list = this._facadeProcessors.get(cmd.code);
    if (list == null || list.length === 0) return null;
    let first: RemotingCommand | null = null;
    for (const handler of list.slice()) {
      try {
        const resp = handler(cmd, addr);
        if (resp != null && first == null) first = resp;
      } catch (e) {
        logger.warning('facade processor for code %s failed: %s', cmd.code, (e as Error).message);
      }
    }
    return first;
  }

  // How many facade processors a given code currently has (test observability: the non-overwriting landing spots under a shared instance).
  facadeProcessorCount(requestCode: number): number {
    const list = this._facadeProcessors.get(requestCode);
    return list != null ? list.length : 0;
  }

  start(): void {
    if (this._running) return;
    this._running = true;
    this._registerTransactionCheckProcessor();
    this._registerReplyMessageProcessor();
    this._startHeartbeatLoop();
    this._startRouteRefreshLoop();
    this._startNamesrvAddressingLoop();
    this._startAdjustThreadPoolLoop();
    this._startPersistConsumerOffsetLoop();
    // The stats manager's own start() is empty (sampling hangs off each
    // StatsItem's scheduler); the unified sampler timer starts here.
    this.consumerStatsManager.start();
  }

  // The TopAddressing branch runs ONLY when no name server address was
  // explicitly configured: scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min).
  private _startNamesrvAddressingLoop(): void {
    if (this.nameServerAddress != null) return; // explicitly set — skip the task
    this._topAddressing = new DefaultTopAddressing(null, null);
    this._topAddressing.registerChangeCallBack((addrs) => this.onNameServerAddressChange(addrs));
    const beat = () => { void this.fetchNameServerAddr(); };
    const initial = setTimeout(beat, 10_000);
    if (typeof initial.unref === 'function') initial.unref();
    this._namesrvTimer = setInterval(beat, 2 * 60_000);
    if (typeof this._namesrvTimer.unref === 'function') this._namesrvTimer.unref();
    this._namesrvInitialTimer = initial;
  }

  // Periodic adjustThreadPool sweep every 1 minute (initial delay 1 min).
  // The consumer-side inc/dec is a deliberate no-op (see DefaultMQPushConsumer
  // .adjustThreadPool), so this only measures.
  private _startAdjustThreadPoolLoop(): void {
    this._adjustThreadPoolTimer = setInterval(() => { this.adjustThreadPool(); }, 60_000);
    if (typeof this._adjustThreadPoolTimer.unref === 'function') this._adjustThreadPoolTimer.unref();
  }

  adjustThreadPool(): void {
    for (const consumer of this.consumerTable.values()) {
      if (consumer != null && typeof consumer.adjustThreadPool === 'function') {
        try { consumer.adjustThreadPool(); } catch (e) { /* best effort — never let one consumer break the sweep */ }
      }
    }
  }

  // Periodic offset flush (Java MQClientInstance: scheduleAtFixedRate(
  // persistAllConsumerOffset, 10s, persistConsumerOffsetInterval=5s)). The
  // interval timer alone would fire at +5s already, so the 10s initial tick
  // only matters for a configured interval LONGER than 10s — kept anyway for
  // shape parity with the Java schedule.
  private _startPersistConsumerOffsetLoop(): void {
    if (this._persistOffsetTimer != null) return;
    const beat = () => { this.persistAllConsumerOffset(); };
    const initial = setTimeout(beat, 10_000);
    if (typeof initial.unref === 'function') initial.unref();
    this._persistOffsetInitialTimer = initial;
    this._persistOffsetTimer = setInterval(beat, this.persistConsumerOffsetInterval);
    if (typeof this._persistOffsetTimer.unref === 'function') this._persistOffsetTimer.unref();
  }

  // Java MQClientInstance.persistAllConsumerOffset: sweep EVERY consumer
  // registered on this instance. Best effort per consumer — one failing flush
  // must never starve the others (same isolation rule as adjustThreadPool).
  persistAllConsumerOffset(): void {
    for (const consumer of this.consumerTable.values()) {
      if (consumer == null || typeof consumer.persistConsumerOffset !== 'function') continue;
      try {
        const r = consumer.persistConsumerOffset();
        if (r != null && typeof r.then === 'function') {
          r.catch((e: Error) => logger.debug('persistConsumerOffset failed: %s', e.message));
        }
      } catch (e) {
        logger.debug('persistConsumerOffset raised: %s', (e as Error).message);
      }
    }
  }

  // Runtime knob for the flush period (Java ClientConfig.setPersistConsumerOffsetInterval).
  // When the loop is already running the interval timer is rebuilt so the new
  // period takes effect from the next tick; the 10s initial tick is left alone.
  setPersistConsumerOffsetInterval(ms: number): void {
    if (!Number.isFinite(ms) || ms <= 0) {
      throw new Error(`persistConsumerOffsetInterval must be > 0, got ${ms}`);
    }
    this.persistConsumerOffsetInterval = Math.floor(ms);
    if (this._persistOffsetTimer != null) {
      clearInterval(this._persistOffsetTimer);
      this._persistOffsetTimer = setInterval(() => { this.persistAllConsumerOffset(); },
        this.persistConsumerOffsetInterval);
      if (typeof this._persistOffsetTimer.unref === 'function') this._persistOffsetTimer.unref();
    }
  }

  // Route refresh every pollNameServerInterval (30s), initial delay 10s.
  // Refreshes the route
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

  // Client-level heartbeat beat every 30s (+ an early beat at start). Without
  // it the broker's ProducerManager has no channel for the producer group and
  // can never send CHECK_TRANSACTION_STATE back — the transaction check-back
  // path silently dies ("Check transaction failed, channel table is empty").
  // Consumers keep their own loop too; duplicate beats are idempotent.
  private _startHeartbeatLoop(): void {
    if (this._heartbeatTimer != null) return;
    void this.sendHeartbeatToAllBrokers();
    this._heartbeatTimer = setInterval(() => { void this.sendHeartbeatToAllBrokers(); }, 30_000);
    if (typeof (this._heartbeatTimer as any).unref === 'function') this._heartbeatTimer.unref();
  }

  // Public: facade start() (pull / lite-pull consumer), route refresh and the
  // push consumer's rebalance all call this directly. `isRebalance` is the push
  // consumer's assignment-changed signal: it drops every cached fingerprint so
  // the next beat re-reports subscriptions, the way the reference resets the
  // table before beating when a rebalance moved the queue set.
  async sendHeartbeatToAllBrokers(isRebalance = false): Promise<void> {
    const entries = this.getAllBrokerAddrEntries();
    // Same two guards the reference applies before it walks the broker table: a
    // client that registers nothing has nothing to report, and a producer-only
    // client beats the MASTER only — heartbeating a slave with no consumer data
    // would leave the slave holding a producer registration it can never use.
    const beat = this.buildHeartbeatData(false);
    const producerEmpty = beat.producerDataSet.length === 0;
    const consumerEmpty = beat.consumerDataSet.length === 0;
    if (producerEmpty && consumerEmpty) {
      logger.warning('heartbeat skipped: no producer and no consumer registered');
      return;
    }
    if (isRebalance) this._brokerAddrHeartbeatFingerprintTable.clear();
    logger.debug('heartbeat beat to %d broker(s)', entries.length);
    for (const { addr, id } of entries) {
      if (consumerEmpty && id !== MixAll.MASTER_ID) continue;
      try { await this.sendHeartbeat(addr); } catch (e) { /* best effort */ }
    }
  }

  private _registerTransactionCheckProcessor(): void {
    this.registerFacadeProcessor(RequestCode.CHECK_TRANSACTION_STATE, (cmd: RemotingCommand, addr: string): RemotingCommand | null => {
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
      // transactionId = UNIQ_KEY user property (broker-side check contract).
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

  // Broker-initiated reply delivery (PUSH_REPLY_MESSAGE_TO_CLIENT, 326): the
  // broker pushes the REPLY
  // message of a request to the requesting client. The wire is NOT the 17
  // segment stored format — the message fields ride in the
  // ReplyMessageRequestHeader extFields and only the body is binary. The
  // handler rebuilds the MessageExt, extracts the CORRELATION_ID and resolves
  // the in-flight request future (atomically removed so the timeout-scan path
  // cannot double-resolve).
  private _registerReplyMessageProcessor(): void {
    this.registerFacadeProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, (cmd: RemotingCommand, _addr: string): RemotingCommand | null => {
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
        // The broker compresses a reply body exactly like a send: sysFlag says
        // which algorithm — don't assume ZLIB (an LZ4-flagged reply would
        // otherwise crash the inflate).
        let body = cmd.body != null ? cmd.body : Buffer.alloc(0);
        if (MessageSysFlag.isCompressed(msg.getSysFlag()) && body.length > 0) {
          body = decompressFor(body, MessageSysFlag.getCompressionType(msg.getSysFlag()));
        }
        msg.setBody(body);
        const correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);
        if (correlationId == null) {
          response.code = ResponseCode.SYSTEM_ERROR;
          response.remark = 'reply message has no correlation id';
          return response;
        }
        // Atomically remove so only one of the reply-arrival path and the
        // timeout-scan path can take ownership.
        const future = REQUEST_FUTURE_HOLDER.removeRequest(correlationId);
        if (future == null) {
          logger.warning('receive reply message, but not matched any request, CorrelationId: %s', correlationId);
          return response;
        }
        // Wake the sync caller off the read thread — same hard rule as the
        // transaction-check processor: never run user code inline.
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
    if (this._namesrvTimer != null) { clearInterval(this._namesrvTimer); this._namesrvTimer = null; }
    if (this._namesrvInitialTimer != null) { clearTimeout(this._namesrvInitialTimer); this._namesrvInitialTimer = null; }
    if (this._topAddressing != null) { this._topAddressing.clearChangeCallBack(); this._topAddressing = null; }
    if (this._adjustThreadPoolTimer != null) { clearInterval(this._adjustThreadPoolTimer); this._adjustThreadPoolTimer = null; }
    if (this._persistOffsetTimer != null) { clearInterval(this._persistOffsetTimer); this._persistOffsetTimer = null; }
    if (this._persistOffsetInitialTimer != null) { clearTimeout(this._persistOffsetInitialTimer); this._persistOffsetInitialTimer = null; }
    this.consumerStatsManager.shutdown();
    try { this.remotingClient.shutdown(); } catch (e) { /* ignore */ }
    for (const acc of (this as any)._accumulators || []) { try { acc.stop(); } catch (e) {} }
  }

  isRunning(): boolean { return this._running; }
}

export default { MQClient, TopicPublishInfo };
