// DefaultMQPullConsumer — the pull consumer
// (Java org.apache.rocketmq.client.consumer.DefaultMQPullConsumer +
// DefaultMQPullConsumerImpl + PullAPIWrapper).
//
// CRITICAL (cross-port rule #10): `pull()` is a SHORT poll — suspend=false in
// the sysFlag and suspendTimeoutMillis=0 on the wire. Only `pullBlockIfNotFound`
// sets suspend=true (and a broker that holds the request ~5s then answers
// NO_NEW_MSG). Writing suspend=true unconditionally manifests as a 5s timeout
// on every idle pull.
import type { MQClient } from './mq_client.ts';
import { PullAPI } from './pull_api.ts';
import { MQClient } from './mq_client.ts';
import { registerRpcHooks } from '../remoting/rpc_hooks.ts';
import {
  LocalFileOffsetStore, RemoteBrokerOffsetStore, ReadOffsetMode, mqKey,
} from './offset_store.ts';
import type { OffsetStore } from './offset_store.ts';
import { PullStatus, PullResult } from './consumer_result.ts';
import { MessageModel, ConsumeType } from '../remoting/heartbeat.ts';
import { MessageQueue, MessageExt } from '../common/message.ts';
import { MixAll } from '../common/mixAll.ts';
import { FilterAPI, SubscriptionData, ExpressionType } from '../common/subscriptionData.ts';
import { PullSysFlag } from '../common/sysflag.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { RemotingCommand } from '../remoting/remotingCommand.ts';
import { RemotingSerializable } from '../remoting/serialize.ts';
import { ConsumerSendMsgBackRequestHeader } from '../remoting/headers.ts';
import { Validators } from '../common/validators.ts';
import type { TopicRouteData } from '../remoting/route.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.pull_consumer');

const DEFAULT_PULL_BATCH_SIZE = 32;
const DEFAULT_PULL_TIMEOUT_MILLIS = 20000;

export class DefaultMQPullConsumer {
  consumerGroup: string;
  namespace = '';
  // Java ClientConfig#namespaceV2 — the SERVER-side namespace (nsd/ns
  // extFields stamped by NamespaceRpcHook), independent from `namespace`.
  namespaceV2: string | null = null;
  nameServerAddr = '';
  messageModel: string = MessageModel.CLUSTERING;
  unitMode = false;
  // Java ClientConfig#instanceName / #unitName / #enableStreamRequestType. The
  // pull consumer turns the stream flag ON in every constructor
  // (DefaultMQPullConsumer:113/:126), which both appends `@STREAM` to the
  // clientId — Java's stated reason is to keep a pull consumer from silently
  // sharing an MQClientInstance with a push consumer in the same process — and
  // stamps ReqT=0 on every request inside the ACL signature.
  instanceName = 'DEFAULT';
  unitName = '';
  enableStreamRequestType = true;
  // wire fields read by MQClient.buildHeartbeatData:
  consumeType: string = ConsumeType.CONSUME_ACTIVELY;
  get subscriptionDataSet(): SubscriptionData[] { return [...this.subscription.values()]; }

  clientID = '';
  mqClient: MQClient | null = null;
  pullAPI: PullAPI | null = null;
  offsetStore: OffsetStore | null = null;

  pullBatchSize = DEFAULT_PULL_BATCH_SIZE;
  pullTimeoutMillis = DEFAULT_PULL_TIMEOUT_MILLIS;
  maxReconsumeTimes = -1;

  private subscription = new Map<string, SubscriptionData>();
  // Last computed share per topic — this port's stand-in for Java's
  // processQueueTable, and what fetchMessageQueuesInBalance keeps when the
  // route or the consumer list cannot be had (see that method's comment).
  private balancedQueues = new Map<string, MessageQueue[]>();
  started = false;

  constructor(consumerGroup = 'DEFAULT_CONSUMER') {
    this.consumerGroup = consumerGroup || 'DEFAULT_CONSUMER';
  }

  setNamesrvAddr(addr: string): this { this.nameServerAddr = addr; return this; }
  setNamespace(ns: string): this { this.namespace = ns; return this; }
  // Java ClientConfig#setNamespaceV2/getNamespaceV2 (read live per request).
  setNamespaceV2(ns: string | null): this { this.namespaceV2 = ns; return this; }
  getNamespaceV2(): string | null { return this.namespaceV2; }
  setMessageModel(model: string): this { this.messageModel = model; return this; }
  setInstanceName(name: string): this { this.instanceName = name; return this; }
  setUnitName(name: string): this { this.unitName = name; return this; }
  setPullBatchSize(n: number): this { this.pullBatchSize = n; return this; }
  setPullTimeoutMillis(ms: number): this { this.pullTimeoutMillis = ms; return this; }
  setMaxReconsumeTimes(n: number): this { this.maxReconsumeTimes = n; return this; }
  getConsumerGroup(): string { return this.consumerGroup; }
  isStarted(): boolean { return this.started; }

  subscribe(topic: string, subExpression = '*'): this {
    if (!topic || !topic.trim()) throw new Error('subscription topic is empty');
    const expr = subExpression || '*';
    this.subscription.set(topic, FilterAPI.buildSubscriptionData(topic, expr));
    return this;
  }

  async start(): Promise<void> {
    if (this.started) return;
    Validators.checkGroup(this.consumerGroup);
    if (!this.nameServerAddr) throw new Error('name server address is not set');
    if (this.subscription.size === 0) {
      throw new Error('subscription is not set, call subscribe() first');
    }
    // Java DefaultMQPullConsumerImpl#start:712-716 — only CLUSTERING rewrites
    // the DEFAULT instance name (a BROADCASTING group keeps "DEFAULT" so same
    // process instances share one client instance), and the clientId is
    // ClientConfig#buildMQClientId's `<ip>@<instanceName>[@<unitName>][@STREAM]`.
    // ⚠ A pid-only name collides between two pull consumers in one process, and
    // allocate() then hands both the SAME slice — duplicate consumption.
    if (this.messageModel === MessageModel.CLUSTERING) {
      this.instanceName = MixAll.changeInstanceNameToPid(this.instanceName);
    }
    this.clientID = MixAll.clientIdFor(
      this.instanceName, this.unitName || null, this.enableStreamRequestType);
    const client = new MQClient(this.clientID, this.nameServerAddr);
    // Java MQClientAPIImpl:329 — NamespaceRpcHook first on the remoting
    // client, then StreamTypeRPCHook (pull consumers enable it), before any
    // user hook (this facade has none today).
    registerRpcHooks(client.remotingClient, {
      namespaceV2: () => this.namespaceV2,
      enableStreamRequestType: this.enableStreamRequestType,
    });
    this.mqClient = client;
    client.registerConsumer(this.consumerGroup, this);
    client.start();
    if (this.messageModel === MessageModel.BROADCASTING) {
      this.offsetStore = new LocalFileOffsetStore(this.clientID, this.consumerGroup);
    } else {
      this.offsetStore = new RemoteBrokerOffsetStore(client, this.consumerGroup);
    }
    this.pullAPI = new PullAPI(client, this.consumerGroup);
    await this.offsetStore.load().catch((e) =>
      logger.warning('load offset store failed: %s', (e as Error).message));
    for (const topic of this.subscription.keys()) {
      await client.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
    }
    // Register the group at the broker immediately (see lite_pull_consumer:
    // the client heartbeat loop's 30s cadence is too slow for first rebalance).
    await client.sendHeartbeatToAllBrokers().catch(() => {});
    this.started = true;
  }

  async shutdown(): Promise<void> {
    if (!this.started) return;
    try { await this.persistConsumeOffset(); } catch (e) { /* best effort */ }
    if (this.mqClient) {
      await this.mqClient.unregisterClientAllBrokers('', this.consumerGroup).catch(() => {});
      this.mqClient.unregisterConsumer(this.consumerGroup);
      this.mqClient.shutdown();
    }
    this.started = false;
  }

  // fetchPublishMessageQueues is Java DefaultMQPullConsumerImpl:137 — the
  // PUBLISH view (write perm, writeQueueNums, master required; MQClientInstance
  // #topicRouteData2TopicPublishInfo). ⚠ It used to hand back the SUBSCRIBE
  // view: with readQueueNums != writeQueueNums, or a broker with no master in
  // the route, the two lists differ, and a caller sizing its shard by this list
  // then reads queues it was never given (or misses ones it was).
  async fetchPublishMessageQueues(topic: string): Promise<MessageQueue[]> {
    return (await this.routeOf(topic, 'publish')).getAllMessageQueue(topic);
  }

  // fetchSubscribeMessageQueues is Java :142 — the SUBSCRIBE view (read perm,
  // readQueueNums, no master filter). This is the list a pull caller iterates.
  async fetchSubscribeMessageQueues(topic: string): Promise<MessageQueue[]> {
    return (await this.routeOf(topic, 'subscribe')).getAllSubscribeMessageQueue(topic);
  }

  // routeOf refreshes then returns the topic's route; a route that cannot be
  // had is Java's "topic not exist" and throws rather than returning [].
  private async routeOf(topic: string, view: 'publish' | 'subscribe'): Promise<TopicRouteData> {
    if (!this.mqClient) throw new Error('consumer not started');
    await this.mqClient.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
    const route = this.mqClient.getTopicRouteData(topic);
    if (!route) {
      throw new Error(`Can not find MessageQueue for topic: ${topic}`);
    }
    return route;
  }

  // pull is a SHORT poll (rule #10): suspend=false, suspendTimeoutMillis=0.
  async pull(mq: MessageQueue, subExpression: string | null, offset: number, maxNums: number,
    timeoutMillis = this.pullTimeoutMillis): Promise<PullResult> {
    return this._pullImpl(mq, subExpression, offset, maxNums, false, 0, timeoutMillis);
  }

  // pullBlockIfNotFound is a LONG poll: suspend=true. The broker holds the
  // request up to brokerSuspendMaxTimeMillis and answers NO_NEW_MSG when
  // nothing arrives in time — a benign timeout on the client is NORMAL here.
  async pullBlockIfNotFound(mq: MessageQueue, subExpression: string | null, offset: number,
    maxNums: number, timeoutMillis = this.pullTimeoutMillis,
    suspendTimeoutMillis = 20000): Promise<PullResult> {
    return this._pullImpl(mq, subExpression, offset, maxNums, true, suspendTimeoutMillis, timeoutMillis);
  }

  private async _pullImpl(mq: MessageQueue, subExpression: string | null, offset: number,
    maxNums: number, suspend: boolean, suspendTimeoutMillis: number,
    timeoutMillis: number): Promise<PullResult> {
    if (!this.pullAPI) throw new Error('consumer not started');
    const topic = mq.getTopic();
    let sub: SubscriptionData;
    if (subExpression == null) {
      const cached = this.subscription.get(topic);
      if (!cached) throw new Error(`subscription for topic ${topic} is not set`);
      sub = cached;
    } else {
      sub = FilterAPI.buildSubscriptionData(topic, subExpression || '*');
    }
    // Pull / lite-pull consumers ALWAYS send the expression (rule #21):
    // subscription bit on.
    const sysFlag = PullSysFlag.buildSysFlag(false, suspend, true, false);
    let result = await this.pullAPI.pullKernel(mq, offset, sub, sysFlag, 0,
      maxNums > 0 ? maxNums : this.pullBatchSize, suspendTimeoutMillis, timeoutMillis);
    result = this.pullAPI.processPullResult(mq, result, sub);
    return result;
  }

  // updateConsumeOffset records the consumed offset (the store persists it).
  updateConsumeOffset(mq: MessageQueue, offset: number): void {
    if (!this.offsetStore) throw new Error('consumer not started');
    if (offset < 0) throw new Error(`offset must be >= 0, got ${offset}`);
    this.offsetStore.updateOffset(mq, offset, true);
  }

  // fetchConsumeOffset reads the consumed offset (-1 when unknown).
  async fetchConsumeOffset(mq: MessageQueue, fromStore: boolean): Promise<number> {
    if (!this.offsetStore) throw new Error('consumer not started');
    return this.offsetStore.readOffset(mq,
      fromStore ? ReadOffsetMode.READ_FROM_STORE : ReadOffsetMode.READ_FROM_MEMORY);
  }

  // persistConsumeOffset flushes all recorded offsets.
  async persistConsumeOffset(): Promise<void> {
    if (!this.offsetStore) return;
    const snap = (this.offsetStore as any).tableSnapshot
      ? (this.offsetStore as any).tableSnapshot() as Map<string, number> : new Map();
    const mqs: MessageQueue[] = [];
    for (const key of snap.keys()) {
      const i1 = key.indexOf('@');
      const i2 = key.lastIndexOf('@');
      if (i1 > 0 && i2 > i1) {
        mqs.push(new MessageQueue(key.slice(0, i1), key.slice(i1 + 1, i2), parseInt(key.slice(i2 + 1), 10) || 0));
      }
    }
    await this.offsetStore.persistAll(mqs);
  }

  // fetchMessageQueuesInBalance is Java MQPullConsumer:187 →
  // DefaultMQPullConsumerImpl:120-135: the queues THIS instance owns once the
  // group is balanced — what example/simple/PullConsumer.java:62 iterates before
  // pulling, so taking the whole topic here is the classic duplicate-consumption
  // bug in a multi-instance group.
  //
  // Java reads rebalanceImpl.getProcessQueueTable(), a table its background
  // rebalance thread fills. This port runs no pull-side rebalance thread, so the
  // view is computed on demand with the exact formula RebalanceImpl
  // #rebalanceByTopic uses — the same one this port's push consumer rebalances
  // with, so the two can never disagree and hand one queue to two instances: the
  // subscribe view as mqAll, the group's clientIds from
  // GET_CONSUMER_LIST_BY_GROUP(38) as cidAll, both sorted, then the AVG strategy
  // slices out my share. Under BROADCASTING Java never asks for the cid list.
  //
  // "No route / no answered consumer list" means CANNOT compute, not "my share is
  // empty": the last computed assignment is kept (Java's table still holds it
  // then), or — before the first computation — the queues already being pulled.
  // Falling back to "all of them" would make every co-instance read the same
  // messages twice. A share that genuinely computes to empty (more consumers
  // than queues) returns [], exactly like Java's empty table at that moment.
  async fetchMessageQueuesInBalance(topic: string): Promise<MessageQueue[]> {
    if (!this.mqClient) throw new Error('consumer not started');
    const client = this.mqClient;
    const keep = (why: string): MessageQueue[] => {
      logger.debug(`fetchMessageQueuesInBalance: ${why} for ${this.consumerGroup}/${topic}`
        + ', keep current assignment');
      const last = this.balancedQueues.get(topic);
      if (last) return last;
      const api = this.pullAPI;
      if (!api) return [];
      return api.pulledQueues().filter(mq => mq.getTopic() === topic)
        .sort((a, b) => a.compareTo(b));
    };

    let mqAll: MessageQueue[];
    try {
      mqAll = (await this.routeOf(topic, 'subscribe')).getAllSubscribeMessageQueue(topic);
    } catch (e) {
      return keep(`no route (${(e as Error).message})`);
    }
    mqAll.sort((a, b) => a.compareTo(b));
    if (this.messageModel === MessageModel.BROADCASTING) {
      if (!mqAll.length) return keep('no readable queue in the route');
      this.balancedQueues.set(topic, mqAll);
      return mqAll;
    }
    if (!mqAll.length) return keep('no readable queue in the route');

    const route = client.getTopicRouteData(topic)!;
    let addr: string | null = null;
    for (const bd of route.brokerDatas || []) {
      const m = bd.brokerAddrs || {};
      if (MixAll.MASTER_ID in m) { addr = m[MixAll.MASTER_ID]; break; }
    }
    if (!addr) return keep('no master broker in the route');

    let ids: string[];
    try {
      const response = await client.getConsumerListByGroup(addr, this.consumerGroup);
      if (response.code !== ResponseCode.SUCCESS) return keep(`broker answered ${response.code}`);
      const body = response.body && response.body.length
        ? RemotingSerializable.decode(response.body) : null;
      const raw = Array.isArray(body) ? body
        : (body && Array.isArray(body['consumerIdList'])) ? body['consumerIdList'] : [];
      ids = raw.map(String);
    } catch (e) {
      return keep(`consumer list query failed: ${(e as Error).message}`);
    }
    if (!ids.length) return keep('the group is unknown to the broker');

    const { AllocateMessageQueueAveragely } = await import('./allocate.ts');
    const got = new AllocateMessageQueueAveragely().allocate(
      this.consumerGroup, this.clientID, mqAll, ids.slice().sort());
    // Java :128-131 compares every table key's topic: never let another topic
    // leak into the caller's pull loop.
    const mine = (got || []).filter(mq => mq.getTopic() === topic)
      .sort((a, b) => a.compareTo(b));
    this.balancedQueues.set(topic, mine);
    return mine;
  }

  // sendMessageBack redelivers one message via CONSUMER_SEND_MSG_BACK(36).
  // `offset` is the message's commitLogOffset, NOT its queueOffset.
  async sendMessageBack(msg: MessageExt, delayLevel = 0): Promise<void> {
    if (!this.mqClient) throw new Error('consumer not started');
    const addr = this.mqClient.findBrokerAddressInPublish(msg.getBrokerName());
    if (!addr) throw new Error(`Broker[${msg.getBrokerName()}] master node does not exist`);
    const h = new ConsumerSendMsgBackRequestHeader();
    h.offset = msg.getCommitLogOffset();
    h.group = this.consumerGroup;
    h.delayLevel = delayLevel;
    h.originMsgId = msg.getMsgId();
    h.originTopic = msg.getTopic();
    h.unitMode = this.unitMode;
    h.maxReconsumeTimes = this.maxReconsumeTimes === -1 ? 16 : this.maxReconsumeTimes;
    const request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, h);
    const response = await this.mqClient.remotingClient.invokeSync(addr, request, 5000);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new Error(`sendMessageBack failed: CODE ${response.code} ${response.remark || ''}`);
    }
  }
}
