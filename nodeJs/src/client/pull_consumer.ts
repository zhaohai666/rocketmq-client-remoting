// DefaultMQPullConsumer — the pull consumer.
//
// Key constraint: `pull()` is a SHORT pull — suspend=false in sysFlag and
// suspendTimeoutMillis=0 on the wire. Only `pullBlockIfNotFound` sets
// suspend=true (the broker suspends the request for about 5s before answering
// NO_NEW_MSG). Unconditionally writing suspend=true manifests as a 5s timeout
// on every idle pull.
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
  // namespaceV2: the server-side namespace (NamespaceRpcHook stamps the
  // nsd/ns ext fields), independent of the client-side `namespace`.
  namespaceV2: string | null = null;
  nameServerAddr = '';
  messageModel: string = MessageModel.CLUSTERING;
  unitMode = false;
  // instanceName / unitName / enableStreamRequestType: pull consumers enable
  // the stream flag by default — first so clientId gets `@STREAM` appended,
  // preventing a pull consumer and a push consumer in the same process from
  // silently sharing one client instance; second so every request carries
  // ReqT=0 in the ACL signature.
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
  // Each topic's most recently computed share — serving as the rebalance result
  // table in this implementation; fetchMessageQueuesInBalance preserves exactly
  // this when it cannot get the route or the consumer list (see that method's
  // comment).
  private balancedQueues = new Map<string, MessageQueue[]>();
  started = false;

  constructor(consumerGroup = 'DEFAULT_CONSUMER') {
    this.consumerGroup = consumerGroup || 'DEFAULT_CONSUMER';
  }

  setNamesrvAddr(addr: string): this { this.nameServerAddr = addr; return this; }
  setNamespace(ns: string): this { this.namespace = ns; return this; }
  // Set/read namespaceV2 (read live per request).
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
    // Only CLUSTERING rewrites the default instanceName (BROADCASTING keeps
    // "DEFAULT", letting same-process instances share one client instance);
    // clientId has the form `<ip>@<instanceName>[@<unitName>][@STREAM]`. ⚠ Using
    // pid alone as the instance name collides between two pull consumers in the
    // same process — allocate() hands the same queue slice to both, causing
    // duplicate consumption.
    if (this.messageModel === MessageModel.CLUSTERING) {
      this.instanceName = MixAll.changeInstanceNameToPid(this.instanceName);
    }
    this.clientID = MixAll.clientIdFor(
      this.instanceName, this.unitName || null, this.enableStreamRequestType);
    const manager = MQClientManager.getInstance();
    const { instance: client, created } = manager.getOrCreateMQClientInstance(
      this.clientID, () => new MQClient(this.clientID, this.nameServerAddr));
    if (created) {
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
      // Hand back to the manager: count -1; the real shutdown happens only when
      // it hits zero (when other facades share the same clientId, the instance
      // and its background timers keep running).
      MQClientManager.getInstance().unregisterClient(this.mqClient.clientId, this.mqClient);
    }
    this.started = false;
  }

  // fetchPublishMessageQueues: the PUBLISH view (write perm, writeQueueNums,
  // must have a master). ⚠ This once wrongly returned the SUBSCRIBE view: when
  // readQueueNums != writeQueueNums, or when the route has no master broker,
  // the two lists differ, and a caller sharding by this list would read queues
  // not assigned to it (or miss the ones that were).
  async fetchPublishMessageQueues(topic: string): Promise<MessageQueue[]> {
    return (await this.routeOf(topic, 'publish')).getAllMessageQueue(topic);
  }

  // fetchSubscribeMessageQueues: the SUBSCRIBE view (read perm, readQueueNums,
  // no master filtering). This is the list a puller iterates.
  async fetchSubscribeMessageQueues(topic: string): Promise<MessageQueue[]> {
    return (await this.routeOf(topic, 'subscribe')).getAllSubscribeMessageQueue(topic);
  }

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
