// DefaultMQPullConsumer —— 拉取消费者。
//
// 关键约束：`pull()` 是 SHORT 拉取——sysFlag 里 suspend=false，线路上
// suspendTimeoutMillis=0。只有 `pullBlockIfNotFound` 才置 suspend=true
// （broker 挂起请求约 5s 后应答 NO_NEW_MSG）。无条件写 suspend=true 的表现
// 是每次空闲 pull 都超时 5s。
import type { MQClient } from './mq_client.ts';
import { PullAPI } from './pull_api.ts';
import { MQClient } from './mq_client.ts';
import { MQClientManager } from './mq_client_manager.ts';
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
  // namespaceV2：服务端侧命名空间（由 NamespaceRpcHook 打 nsd/ns 扩展字段），
  // 与客户端侧的 `namespace` 相互独立。
  namespaceV2: string | null = null;
  nameServerAddr = '';
  messageModel: string = MessageModel.CLUSTERING;
  unitMode = false;
  // instanceName / unitName / enableStreamRequestType：pull 消费者默认打开
  // stream 标记，一是让 clientId 追加 `@STREAM`——避免同进程里 pull 消费者和
  // push 消费者悄悄共用同一个客户端实例——二是让每个请求在 ACL 签名里带
  // ReqT=0。
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
  // 每个 topic 最近一次算出的份额——本实现里充当 rebalance 结果表，
  // fetchMessageQueuesInBalance 在拿不到路由或消费者列表时保留的就是它
  // （见该方法的注释）。
  private balancedQueues = new Map<string, MessageQueue[]>();
  started = false;

  constructor(consumerGroup = 'DEFAULT_CONSUMER') {
    this.consumerGroup = consumerGroup || 'DEFAULT_CONSUMER';
  }

  setNamesrvAddr(addr: string): this { this.nameServerAddr = addr; return this; }
  setNamespace(ns: string): this { this.namespace = ns; return this; }
  // 设置/读取 namespaceV2（每个请求实时读取）。
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
    // 只有 CLUSTERING 才把默认 instanceName 改写掉（BROADCASTING 保留
    // "DEFAULT"，让同进程实例共享一个客户端实例），clientId 形如
    // `<ip>@<instanceName>[@<unitName>][@STREAM]`。⚠ 只用 pid 作实例名会在
    // 同进程两个 pull 消费者之间撞车，allocate() 把同一片队列分给两边——
    // 重复消费。
    if (this.messageModel === MessageModel.CLUSTERING) {
      this.instanceName = MixAll.changeInstanceNameToPid(this.instanceName);
    }
    this.clientID = MixAll.clientIdFor(
      this.instanceName, this.unitName || null, this.enableStreamRequestType);
    // 跨 facade 复用（Java MQClientManager.factoryTable）：同 clientId 的门面共享
    // 一个实例，引用计数归零才真关闭；hook 链只由创建者注册（first-wins 快照）。
    const manager = MQClientManager.getInstance();
    const { instance: client, created } = manager.getOrCreateMQClientInstance(
      this.clientID, () => new MQClient(this.clientID, this.nameServerAddr));
    if (created) {
      // Java MQClientAPIImpl:329 — NamespaceRpcHook first on the remoting
      // client, then StreamTypeRPCHook (pull consumers enable it), before any
      // user hook (this facade has none today).
      registerRpcHooks(client.remotingClient, {
        namespaceV2: () => this.namespaceV2,
        enableStreamRequestType: this.enableStreamRequestType,
      });
    }
    manager.registerClient(this.clientID, client);
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
      // 归还给 manager：计数 -1，归零才真正 shutdown（同 clientId 还有别的门面时
      // 实例与其后台定时器继续存活）。
      MQClientManager.getInstance().unregisterClient(this.mqClient.clientId, this.mqClient);
    }
    this.started = false;
  }

  // fetchPublishMessageQueues：PUBLISH 视图（写权限、writeQueueNums、必须有
  // master）。⚠ 这里曾经错误地返回 SUBSCRIBE 视图：当 readQueueNums !=
  // writeQueueNums、或路由里没有 master broker 时两个列表不同，按这个列表
  // 定分片的调用方会读到没分给自己的队列（或漏掉分到的）。
  async fetchPublishMessageQueues(topic: string): Promise<MessageQueue[]> {
    return (await this.routeOf(topic, 'publish')).getAllMessageQueue(topic);
  }

  // fetchSubscribeMessageQueues：SUBSCRIBE 视图（读权限、readQueueNums、不做
  // master 过滤）。拉取方要遍历的就是这个列表。
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
