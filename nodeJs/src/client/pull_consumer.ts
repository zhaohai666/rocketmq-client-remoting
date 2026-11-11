// DefaultMQPullConsumer — the pull consumer.
//
// Key constraint: `pull()` is a SHORT pull — suspend=false in sysFlag and
// suspendTimeoutMillis=0 on the wire. Only `pullBlockIfNotFound` sets
// suspend=true, which makes the request wait for the window it carries instead
// of answering at once). Unconditionally writing suspend=true manifests as a 5s timeout
// on every idle pull.
import { PullAPI } from './pull_api.ts';
import { MQClient, DEFAULT_CONCURRENT_HEARTBEAT_THREAD_POOL_SIZE } from './mq_client.ts';
import { MQClientManager } from './mq_client_manager.ts';
import { registerRpcHooks } from '../remoting/rpc_hooks.ts';
import {
  LocalFileOffsetStore, RemoteBrokerOffsetStore, ReadOffsetMode, mqKey,
} from './offset_store.ts';
import type { OffsetStore } from './offset_store.ts';
import { PullStatus, PullResult, MessageSelector } from './consumer_result.ts';
import { AllocateMessageQueueAveragely } from './allocate.ts';
import type { AllocateMessageQueueStrategy } from './allocate.ts';
import { MessageModel, ConsumeType } from '../remoting/heartbeat.ts';
import { MessageQueue, MessageExt } from '../common/message.ts';
import { clientMsgId } from '../common/messageClientIdSetter.ts';
import { MixAll } from '../common/mixAll.ts';
import { FilterAPI, SubscriptionData, ExpressionType } from '../common/subscriptionData.ts';
import { PullSysFlag } from '../common/sysflag.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { RemotingCommand } from '../remoting/remotingCommand.ts';
import { RemotingSerializable } from '../remoting/serialize.ts';
import { ConsumerSendMsgBackRequestHeader } from '../remoting/headers.ts';
import { Validators } from '../common/validators.ts';
import { MQBrokerException, MQClientException } from '../remoting/exception.ts';
import type { TopicRouteData } from '../remoting/route.ts';
import type { DefaultMQProducer } from './producer.ts';
import { buildRetryMessage } from './retry_message.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.pull_consumer');

const DEFAULT_PULL_BATCH_SIZE = 32;
const DEFAULT_PULL_TIMEOUT_MILLIS = 20000;

// PullCallback -- the asynchronous form of pull(mq, subExpression, offset,
// maxNums, PullCallback, timeout): delivered EXACTLY ONCE per pullAsync call,
// never inline on the caller's stack -- the settle path hands over via setImmediate.
export interface PullCallback {
  onSuccess(pullResult: PullResult): void;
  onException(e: Error): void;
}

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
  // Retry ceiling reported with message redelivery: pull mode's own default is
  // 16, and -1 ("unlimited") is reported as 16 as well.
  maxReconsumeTimes = 16;
  // Offset flush period (default 5000 ms): applied to
  // the shared MQClient instance when THIS facade creates it (first creator's
  // config wins). The instance-level persistAllConsumerOffset sweep is what
  // flushes this facade's offsets between the explicit shutdown/revoke flushes.
  persistConsumerOffsetInterval = 5000;
  // Heartbeat V2: off by default, so heartbeats keep the V1 shape unless enabled.
  useHeartbeatV2 = false;
  // Materialise received message bodies during batch decode (default true).
  decodeReadBody = true;
  // Internal-API timeout (heartbeat, route fetch, check client, unregister and
  // consumer-list RPCs), in milliseconds. Default 3000.
  mqClientApiTimeout = 3000;
  // Concurrent heartbeat round (ignored while heartbeat V2 is on).
  enableConcurrentHeartbeat = false;
  concurrentHeartbeatThreadPoolSize = DEFAULT_CONCURRENT_HEARTBEAT_THREAD_POOL_SIZE;
  // Page size of the paged metadata admin calls (maxTopicNum / maxGroupNum).
  maxPageSizeInGetMetadata = 2000;

  // ---- pull-side rebalance (Java DefaultMQPullConsumer.java:97 / impl :388-406) ----
  // Java's pull consumer DOES rebalance: the shared RebalanceService thread calls
  // `mqClientFactory.doRebalance()` every `waitInterval` (20s, system property
  // `rocketmq.client.rebalance.waitInterval`), which walks the registered consumers
  // and calls `tryRebalance()`; that gates on `enableRebalance` (default true) and
  // then runs `rebalanceImpl.doRebalance(false)` -> allocate -> `messageQueueChanged`.
  //
  // Deviation (documented): Java runs ONE RebalanceService per client instance, this
  // port gives each facade its own timer. The observable behaviour -- which queues
  // this instance owns and when the listener fires -- is the same.
  enableRebalance = true;
  rebalanceWaitMillis = 20000;
  rebalanceMinIntervalMillis = 1000;
  // The allocation strategy: used by the background rebalance and by
  // fetchMessageQueuesInBalance's on-the-spot computation, so both always agree on
  // which queues are mine (diverging would have two instances claim one queue).
  allocateMessageQueueStrategy: AllocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
  // messageQueueChanged callback, in this port's function shape (the push consumer
  // uses the same one). Java passes two Sets; arrays with stable order are used here.
  messageQueueListener:
    ((topic: string, mqAll: MessageQueue[], mqDivided: MessageQueue[]) => void) | null = null;
  // The product of the background rebalance: topic -> the queues allocated to this
  // instance. Java reads the keys of the rebalance's process-queue table; here it is
  // a plain table, and fetchMessageQueuesInBalance prefers it when it is warm.
  private rebalanceTable = new Map<string, MessageQueue[]>();
  // Topics registered for rebalance. Java's registerMessageQueueListener(topic,
  // listener) fills this; subscribe() fills `subscription`. The rebalance walks the
  // union, so either registration path gets an allocation.
  private registerTopics = new Set<string>();
  private _rebalanceTimer: NodeJS.Timeout | null = null;
  private _rebalanceActive = false;

  private subscription = new Map<string, SubscriptionData>();
  started = false;
  // The internal producer the send-back fallback sends through; built lazily on
  // the first refused bounce and torn down with the consumer.
  private _producer: DefaultMQProducer | null = null;

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
  // Flush period for consumed offsets, in milliseconds; must be greater than 0.
  // When already started the change is pushed through to the shared instance's
  // sweep timer immediately.
  setPersistConsumerOffsetInterval(ms: number): this {
    if (!Number.isFinite(ms) || ms <= 0) throw new Error(`persistConsumerOffsetInterval must be > 0, got ${ms}`);
    this.persistConsumerOffsetInterval = Math.floor(ms);
    if (this.mqClient) this.mqClient.setPersistConsumerOffsetInterval(this.persistConsumerOffsetInterval);
    return this;
  }
  getPersistConsumerOffsetInterval(): number { return this.persistConsumerOffsetInterval; }
  // Heartbeat V2 switch; pushed through to
  // the shared instance when this facade already owns one.
  setUseHeartbeatV2(enable: boolean): this {
    this.useHeartbeatV2 = enable === true;
    if (this.mqClient) this.mqClient.setUseHeartbeatV2(this.useHeartbeatV2);
    return this;
  }
  getUseHeartbeatV2(): boolean { return this.useHeartbeatV2; }
  // Received-body decode switch, mirrored onto the shared instance.
  setDecodeReadBody(enable: boolean): this {
    this.decodeReadBody = enable === true;
    if (this.mqClient) this.mqClient.setDecodeReadBody(this.decodeReadBody);
    return this;
  }
  getDecodeReadBody(): boolean { return this.decodeReadBody; }
  // Internal-API timeout, in milliseconds. An explicit per-call timeout passed
  // by a caller still wins.
  setMqClientApiTimeout(ms: number): this {
    if (!Number.isFinite(ms) || ms <= 0) throw new Error(`mqClientApiTimeout must be > 0, got ${ms}`);
    this.mqClientApiTimeout = Math.floor(ms);
    if (this.mqClient) this.mqClient.setMqClientApiTimeout(this.mqClientApiTimeout);
    return this;
  }
  getMqClientApiTimeout(): number { return this.mqClientApiTimeout; }
  // Concurrent heartbeat round; heartbeat V2 still wins when it is enabled.
  setEnableConcurrentHeartbeat(enable: boolean): this {
    this.enableConcurrentHeartbeat = enable === true;
    if (this.mqClient) this.mqClient.setEnableConcurrentHeartbeat(this.enableConcurrentHeartbeat);
    return this;
  }
  getEnableConcurrentHeartbeat(): boolean { return this.enableConcurrentHeartbeat; }
  setConcurrentHeartbeatThreadPoolSize(size: number): this {
    if (!Number.isInteger(size) || size <= 0) {
      throw new Error(`concurrentHeartbeatThreadPoolSize must be a positive integer, got ${size}`);
    }
    this.concurrentHeartbeatThreadPoolSize = size;
    if (this.mqClient) this.mqClient.setConcurrentHeartbeatThreadPoolSize(size);
    return this;
  }
  getConcurrentHeartbeatThreadPoolSize(): number { return this.concurrentHeartbeatThreadPoolSize; }
  setMaxPageSizeInGetMetadata(size: number): this {
    if (!Number.isInteger(size) || size <= 0) {
      throw new Error(`maxPageSizeInGetMetadata must be a positive integer, got ${size}`);
    }
    this.maxPageSizeInGetMetadata = size;
    if (this.mqClient) this.mqClient.setMaxPageSizeInGetMetadata(size);
    return this;
  }
  getMaxPageSizeInGetMetadata(): number { return this.maxPageSizeInGetMetadata; }

  // ---- pull-side rebalance configuration ----

  // Java DefaultMQPullConsumer#setEnableRebalance (:507). Off means
  // doRebalance/tryRebalance return without allocating anything, so the caller's
  // own fetchMessageQueuesInBalance keeps computing on the spot.
  setEnableRebalance(enable: boolean): this { this.enableRebalance = enable === true; return this; }
  isEnableRebalance(): boolean { return this.enableRebalance; }
  // Round period: Java system property `rocketmq.client.rebalance.waitInterval` (20000).
  setRebalanceWaitMillis(ms: number): this {
    this.rebalanceWaitMillis = Number.isFinite(ms) ? Math.max(1, Math.floor(ms)) : 1;
    return this;
  }
  // Lower bound between two rounds: Java `rocketmq.client.rebalance.minInterval` (1000).
  setRebalanceMinIntervalMillis(ms: number): this {
    this.rebalanceMinIntervalMillis = Number.isFinite(ms) ? Math.max(1, Math.floor(ms)) : 1;
    return this;
  }
  setAllocateMessageQueueStrategy(strategy: AllocateMessageQueueStrategy): this {
    // start()/the rebalance would otherwise fail on every round; like Java, an
    // explicit null is rejected rather than silently replaced.
    if (strategy == null) throw new Error('allocateMessageQueueStrategy is null');
    this.allocateMessageQueueStrategy = strategy;
    return this;
  }
  setMessageQueueListener(
    listener: ((topic: string, mqAll: MessageQueue[], mqDivided: MessageQueue[]) => void) | null): this {
    this.messageQueueListener = listener;
    return this;
  }
  // Java DefaultMQPullConsumer#registerMessageQueueListener (:328-335): registers the
  // topic AND installs the listener in one call. The topic matters because the
  // background rebalance only visits registered topics; a listener installed without
  // one would silently never fire. A null listener keeps the current one.
  registerMessageQueueListener(topic: string,
    listener: ((topic: string, mqAll: MessageQueue[], mqDivided: MessageQueue[]) => void) | null): this {
    if (topic == null || !String(topic).trim()) return this;
    this.registerTopics.add(this._withNamespace(String(topic)));
    if (listener != null) this.messageQueueListener = listener;
    return this;
  }
  getRegisterTopics(): Set<string> { return new Set(this.registerTopics); }
  // Snapshot of the background rebalance's product. Callers get fresh arrays, so
  // mutating the answer cannot change which queues this instance owns.
  getRebalanceTable(): Map<string, MessageQueue[]> {
    const out = new Map<string, MessageQueue[]>();
    for (const [topic, mqs] of this.rebalanceTable) out.set(topic, [...mqs]);
    return out;
  }
  getConsumerGroup(): string { return this.consumerGroup; }
  isStarted(): boolean { return this.started; }

  // subscribe registers a topic subscription. A string is a tag expression
  // ("*" = all); a MessageSelector selects the expression type — bySql rides the
  // wire as expressionType=SQL92 and is evaluated BY THE BROKER: this
  // client has no SQL92 engine, and FilterAPI.build keeps tagsSet
  // EMPTY so the client-side tag filter stays off.
  subscribe(topic: string, subExpression: string | MessageSelector = '*'): this {
    if (!topic || !topic.trim()) throw new Error('subscription topic is empty');
    topic = this._withNamespace(topic);
    this.subscription.set(topic, this._buildSub(topic, subExpression));
    return this;
  }

  // ---- namespace (client-side prefix) ----
  // With a namespace set the broker-side name of every resource is the prefixed
  // one: subscriptions, the group, route lookups and pull requests all use it,
  // and delivered messages get the bare topic back before the caller sees them.
  _withNamespace(resource: string): string {
    return MixAll.wrapNamespace(this.namespace, resource);
  }

  private _queueWithNamespace(mq: MessageQueue): MessageQueue {
    if (!this.namespace) return mq;
    const topic = this._withNamespace(mq.getTopic());
    return topic === mq.getTopic() ? mq : new MessageQueue(topic, mq.getBrokerName(), mq.getQueueId());
  }

  private _bareQueue(mq: MessageQueue): MessageQueue {
    if (!this.namespace) return mq;
    const topic = MixAll.withoutNamespace(mq.getTopic(), this.namespace);
    return topic === mq.getTopic() ? mq : new MessageQueue(topic, mq.getBrokerName(), mq.getQueueId());
  }

  private _buildSub(topic: string, subExpression: string | MessageSelector): SubscriptionData {
    if (typeof subExpression === 'string') {
      return FilterAPI.buildSubscriptionData(topic, subExpression || '*');
    }
    return FilterAPI.build(topic, subExpression.expression, subExpression.type);
  }

  async start(): Promise<void> {
    if (this.started) return;
    // The broker-side group carries the prefix; wrapNamespace is idempotent, so
    // a restart after shutdown does not stack prefixes.
    this.consumerGroup = this._withNamespace(this.consumerGroup);
    Validators.checkGroup(this.consumerGroup);
    if (!this.nameServerAddr) throw new Error('name server address is not set');
    // No subscription is required to start: a pull consumer names the topic in
    // fetchSubscribeMessageQueues/pull and may pass the expression inline, so
    // subscribe() is only one of the ways to get one.
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
      client.setPersistConsumerOffsetInterval(this.persistConsumerOffsetInterval);
      if (this.useHeartbeatV2) client.setUseHeartbeatV2(true);
      client.setDecodeReadBody(this.decodeReadBody);
      client.setMqClientApiTimeout(this.mqClientApiTimeout);
      client.setEnableConcurrentHeartbeat(this.enableConcurrentHeartbeat);
      client.setConcurrentHeartbeatThreadPoolSize(this.concurrentHeartbeatThreadPoolSize);
      client.setMaxPageSizeInGetMetadata(this.maxPageSizeInGetMetadata);
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
    for (const topic of this._rebalanceTopics()) {
      await client.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
    }
    // Register the group at the broker immediately (see lite_pull_consumer:
    // the client heartbeat loop's 30s cadence is too slow for first rebalance).
    await client.sendHeartbeatToAllBrokers().catch(() => {});
    this.started = true;
    // Java registers the pull consumer's rebalance with the shared RebalanceService
    // in start(); here the facade drives its own unref'd timer (see the field block).
    this._rebalanceActive = true;
    this._startRebalanceLoop();
  }

  async shutdown(): Promise<void> {
    if (!this.started) return;
    // Stop the rebalance loop first: it calls back into the client instance which is
    // about to be handed back to the manager.
    this._rebalanceActive = false;
    if (this._rebalanceTimer) { clearTimeout(this._rebalanceTimer); this._rebalanceTimer = null; }
    try { await this.persistConsumeOffset(); } catch (e) { /* best effort */ }
    if (this.mqClient) {
      await this.mqClient.unregisterClientAllBrokers('', this.consumerGroup).catch(() => {});
      this.mqClient.unregisterConsumer(this.consumerGroup);
      // Hand back to the manager: count -1; the real shutdown happens only when
      // it hits zero (when other facades share the same clientId, the instance
      // and its background timers keep running).
      MQClientManager.getInstance().unregisterClient(this.mqClient.clientId, this.mqClient);
    }
    // The lazily built fallback producer owns its own client slot, so it is
    // torn down explicitly rather than left to the manager's refcount.
    if (this._producer) {
      try { await this._producer.shutdown(); } catch (e) { /* ignore */ }
      this._producer = null;
    }
    this.started = false;
  }

  // fetchPublishMessageQueues: the PUBLISH view (write perm, writeQueueNums,
  // must have a master). ⚠ This once wrongly returned the SUBSCRIBE view: when
  // readQueueNums != writeQueueNums, or when the route has no master broker,
  // the two lists differ, and a caller sharding by this list would read queues
  // not assigned to it (or miss the ones that were).
  async fetchPublishMessageQueues(topic: string): Promise<MessageQueue[]> {
    const full = this._withNamespace(topic);
    return (await this.routeOf(full, 'publish')).getAllMessageQueue(full);
  }

  // fetchSubscribeMessageQueues: the SUBSCRIBE view (read perm, readQueueNums,
  // no master filtering). This is the list a puller iterates.
  async fetchSubscribeMessageQueues(topic: string): Promise<MessageQueue[]> {
    const full = this._withNamespace(topic);
    const queues = (await this.routeOf(full, 'subscribe')).getAllSubscribeMessageQueue(full);
    // The queue list is the caller's view: hand back the bare topic, as the
    // pull/offset APIs accept it and prefix it again internally.
    return queues.map((mq) => this._bareQueue(mq));
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
  async pull(mq: MessageQueue, subExpression: string | MessageSelector | null, offset: number, maxNums: number,
    timeoutMillis = this.pullTimeoutMillis): Promise<PullResult> {
    return this._pullImpl(mq, subExpression, offset, maxNums, false, 0, timeoutMillis);
  }

  // pullAsync -- the callback form of pull(mq, subExpression, offset,
  // maxNums, PullCallback, timeout). Event-loop semantics: the callback fires
  // EXACTLY ONCE (success XOR exception) and NEVER inline -- even an immediate
  // local failure (consumer not started, no subscription) is delivered through
  // setImmediate, so the caller's statement always completes first.
  pullAsync(mq: MessageQueue, subExpression: string | MessageSelector | null, offset: number,
    maxNums: number, callback: PullCallback, timeoutMillis = this.pullTimeoutMillis): void {
    if (callback == null || typeof callback.onSuccess !== 'function'
      || typeof callback.onException !== 'function') {
      throw new Error('pullAsync requires a PullCallback with onSuccess/onException');
    }
    let delivered = false;
    const deliver = (fn: () => void): void => {
      if (delivered) return; // exactly once, even if both settle paths raced
      delivered = true;
      setImmediate(() => {
        try { fn(); } catch (e) {
          logger.warning('pullAsync callback raised: %s', (e as Error).message);
        }
      });
    };
    this._pullImpl(mq, subExpression, offset, maxNums, false, 0, timeoutMillis).then(
      (r) => deliver(() => callback.onSuccess(r)),
      (e) => deliver(() => callback.onException(e instanceof Error ? e : new Error(String(e)))),
    );
  }

  // pullBlockIfNotFound is a LONG poll: suspend=true, so the request carries a
  // suspend window (suspendTimeoutMillis) instead of answering at once, and a
  // round that finds nothing reports an idle status — a benign timeout on the
  // client is NORMAL here.
  async pullBlockIfNotFound(mq: MessageQueue, subExpression: string | MessageSelector | null, offset: number,
    maxNums: number, timeoutMillis = this.pullTimeoutMillis,
    suspendTimeoutMillis = 20000): Promise<PullResult> {
    return this._pullImpl(mq, subExpression, offset, maxNums, true, suspendTimeoutMillis, timeoutMillis);
  }

  private async _pullImpl(mq: MessageQueue, subExpression: string | MessageSelector | null, offset: number,
    maxNums: number, suspend: boolean, suspendTimeoutMillis: number,
    timeoutMillis: number): Promise<PullResult> {
    // The pull entry checks the service state first: a shutdown facade must report,
    // never keep pulling through the (still open) client instance it shared.
    if (!this.started || !this.pullAPI) throw new Error('consumer not started');
    if (offset < 0) throw new MQClientException('offset < 0');
    if (maxNums <= 0) throw new MQClientException('maxNums <= 0');
    const fullMq = this._queueWithNamespace(mq);
    const topic = fullMq.getTopic();
    let sub: SubscriptionData;
    if (subExpression == null) {
      const cached = this.subscription.get(topic);
      if (!cached) throw new Error(`subscription for topic ${topic} is not set`);
      sub = cached;
    } else {
      sub = this._buildSub(topic, subExpression);
    }
    // Pull / lite-pull consumers ALWAYS send the expression (rule #21):
    // subscription bit on.
    const sysFlag = PullSysFlag.buildSysFlag(false, suspend, true, false);
    let result = await this.pullAPI.pullKernel(fullMq, offset, sub, sysFlag, 0,
      maxNums, suspendTimeoutMillis, timeoutMillis);
    result = this.pullAPI.processPullResult(fullMq, result, sub);
    if (this.namespace) {
      // The caller handed in the bare topic; hand it back on the messages too.
      for (const msg of result.msgFoundList) {
        msg.setTopic(MixAll.withoutNamespace(msg.getTopic(), this.namespace));
      }
    }
    return result;
  }

  // updateConsumeOffset records the consumed offset (the store persists it).
  updateConsumeOffset(mq: MessageQueue, offset: number): void {
    if (!this.offsetStore) throw new Error('consumer not started');
    if (offset < 0) throw new Error(`offset must be >= 0, got ${offset}`);
    this.offsetStore.updateOffset(this._queueWithNamespace(mq), offset, true);
  }

  // fetchConsumeOffset reads the consumed offset (-1 when unknown).
  async fetchConsumeOffset(mq: MessageQueue, fromStore: boolean): Promise<number> {
    if (!this.offsetStore) throw new Error('consumer not started');
    const fullMq = this._queueWithNamespace(mq);
    if (fromStore) await this._warmQueueRoute(fullMq.getTopic());
    return this.offsetStore.readOffset(fullMq,
      fromStore ? ReadOffsetMode.READ_FROM_STORE : ReadOffsetMode.READ_FROM_MEMORY_THEN_STORE);
  }

  // The offset store resolves a queue's broker address from the cached route
  // table, and a consumer started without subscribe() has nothing cached: a cold
  // store read then silently answers -1 — the caller restarts at zero and
  // re-consumes the whole topic — while a cold persist throws.
  private async _warmQueueRoute(topic: string): Promise<void> {
    if (!this.mqClient || !topic) return;
    if (this.mqClient.getTopicRouteData(topic)) return;
    await this.mqClient.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
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
    for (const topic of new Set(mqs.map(mq => mq.getTopic()))) {
      await this._warmQueueRoute(topic);
    }
    await this.offsetStore.persistAll(mqs);
  }

  // persistConsumerOffset — the MQConsumerInner contract name the MQClient's
  // instance-level persistAllConsumerOffset sweep calls every
  // persistConsumerOffsetInterval; delegates to persistConsumeOffset.
  async persistConsumerOffset(): Promise<void> {
    return this.persistConsumeOffset();
  }

  // maxOffset -- the queue tail on the MASTER broker, i.e. the highest
  // offset ever written to that queue. Throws when the route/broker is unknown;
  // the pull schedule context treats that as "start from 0".
  async maxOffset(mq: MessageQueue): Promise<number> {
    if (!this.mqClient) throw new Error('consumer not started');
    const fullMq = this._queueWithNamespace(mq);
    await this._warmQueueRoute(fullMq.getTopic());
    const addrs = this.mqClient.getBrokerAddrTable().get(fullMq.getBrokerName());
    const addr = addrs != null ? addrs[MixAll.MASTER_ID] : null;
    if (addr == null) throw new Error(`Broker[${fullMq.getBrokerName()}] master node does not exist`);
    return this.mqClient.getMaxOffset(addr, fullMq.getTopic(), fullMq.getQueueId());
  }

  // fetchMessageQueuesInBalance is Java DefaultMQPullConsumerImpl's pure query
  // (:408-...): when the background rebalance has already produced this topic's
  // share, that table IS the answer. Otherwise (rebalance off, or the table is
  // still cold) the same allocation is computed on the spot. Neither path fires
  // the listener or writes the table — a read must not look like a queue change.
  async fetchMessageQueuesInBalance(topic: string): Promise<MessageQueue[]> {
    const queues = await this._fetchMessageQueuesInBalance(this._withNamespace(topic));
    return queues.map((mq) => this._bareQueue(mq));
  }

  private async _fetchMessageQueuesInBalance(topic: string): Promise<MessageQueue[]> {
    if (!this.mqClient) throw new Error('consumer not started');
    // A warm table is authoritative. While the switch is off the stale table must
    // be ignored: the caller asked for the on-the-spot allocation.
    if (this.enableRebalance) {
      const cached = this.rebalanceTable.get(topic);
      if (cached !== undefined) {
        return [...cached].sort((a, b) => a.compareTo(b));
      }
    }
    const keep = (why: string): MessageQueue[] => {
      logger.debug(`fetchMessageQueuesInBalance: ${why} for ${this.consumerGroup}/${topic}`
        + ', keep current assignment');
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
    mqAll = [...mqAll].sort((a, b) => a.compareTo(b));
    if (!mqAll.length) return keep('no readable queue in the route');
    if (this.messageModel === MessageModel.BROADCASTING) return mqAll;

    // The broker learns about a group only from a HEARTBEAT carrying its
    // ConsumerData. start() beats once before any topic route is cached, so a
    // consumer that starts without subscribe() has an empty address table and
    // that beat reaches nobody. This one-shot query beats again; the background
    // rebalance does not (it is the heartbeat loop's job, as in Java).
    const ids = await this._getConsumerIdsFor(topic, true);
    if (!ids) return keep('the group is unknown to the broker');
    try {
      const got = this.allocateMessageQueueStrategy.allocate(
        this.consumerGroup, this.clientID, mqAll, [...ids].sort());
      return (got || []).filter(mq => mq.getTopic() === topic)
        .sort((a, b) => a.compareTo(b));
    } catch (e) {
      return keep(`allocation failed (${(e as Error).message})`);
    }
  }

  // _getConsumerIdsFor asks one master broker for the group's consumer id list.
  // `null` means "unavailable" (no route, no master, a failed query or an empty
  // list) — both callers keep the existing assignment and never read it as
  // "I am the only one, so I own everything".
  private async _getConsumerIdsFor(topic: string, sendHeartbeat = false): Promise<string[] | null> {
    const client = this.mqClient;
    if (!client) return null;
    const route = client.getTopicRouteData(topic);
    if (!route) return null;
    let addr: string | null = null;
    for (const bd of route.brokerDatas || []) {
      const m = bd.brokerAddrs || {};
      if (MixAll.MASTER_ID in m) { addr = m[MixAll.MASTER_ID]; break; }
    }
    if (!addr) return null;
    if (sendHeartbeat) await client.sendHeartbeatToAllBrokers().catch(() => {});
    try {
      const response = await client.getConsumerListByGroup(addr, this.consumerGroup);
      if (response.code !== ResponseCode.SUCCESS) return null;
      const body = response.body && response.body.length
        ? RemotingSerializable.decode(response.body) : null;
      const raw = Array.isArray(body) ? body
        : (body && Array.isArray(body['consumerIdList'])) ? body['consumerIdList'] : [];
      const ids = raw.map(String);
      return ids.length ? ids : null;
    } catch (e) {
      return null;
    }
  }

  // ---------------------------------------------------------------- rebalance

  // _rebalanceTopics is the union of subscribe()'d topics and topics named by
  // registerMessageQueueListener(): Java stores them in two tables and the pull
  // rebalance must serve either registration path.
  private _rebalanceTopics(): string[] {
    return [...new Set([...this.subscription.keys(), ...this.registerTopics])].sort();
  }

  // _startRebalanceLoop drives one rebalance round per timer tick, unref'd so a
  // live consumer never keeps the Node process alive on its own (shutdown clears
  // the handle explicitly). Deviation from Java's shared RebalanceService is
  // documented on the enableRebalance field.
  private _startRebalanceLoop(): void {
    if (this._rebalanceTimer) return;
    const schedule = (delayMillis: number): void => {
      if (!this._rebalanceActive) return;
      this._rebalanceTimer = setTimeout(() => {
        this._rebalanceTimer = null;
        if (!this._rebalanceActive) return;
        void (async () => {
          let balanced = true;
          try {
            balanced = await this.doRebalance();
          } catch (e) {
            logger.debug('rebalance loop error: %s', (e as Error).message);
            balanced = false;
          }
          // balanced -> the full period; otherwise retry quickly so a freshly
          // appeared queue is claimed without waiting out 20s.
          schedule(balanced ? this.rebalanceWaitMillis : this.rebalanceMinIntervalMillis);
        })();
      }, delayMillis);
      // unref(): a pending rebalance round must not pin the event loop at exit.
      if (typeof this._rebalanceTimer.unref === 'function') this._rebalanceTimer.unref();
    };
    schedule(this.rebalanceWaitMillis);
  }

  // doRebalance is Java DefaultMQPullConsumerImpl#tryRebalance (:398-406).
  // With enableRebalance off Java returns TRUE ("nothing left to retry"); returning
  // false would pin the loop at minInterval and burn a round-trip every second.
  async doRebalance(): Promise<boolean> {
    if (!this.enableRebalance) return true;
    if (!this.mqClient) return true;
    let balanced = true;
    for (const topic of this._rebalanceTopics()) {
      try {
        if (!(await this.rebalanceByTopic(topic))) balanced = false;
      } catch (e) {
        logger.debug('rebalance topic %s failed: %s', topic, (e as Error).message);
        balanced = false;
      }
    }
    return balanced;
  }

  // rebalanceByTopic is Java RebalanceImpl#rebalanceByTopic (pull path) plus
  // RebalancePullImpl#messageQueueChanged. Returns whether this topic is already
  // balanced. The listener fires ONLY when the allocation changed, receiving
  // (topic, mqAll, mqDivided) — the same three arguments RebalancePullImpl
  // (:44-53) passes.
  //
  // Deviation (documented): Java compares allocateResultSet with the freshly
  // updated process-queue table, so a "changed then already in place" round can
  // report balanced immediately. Here balanced means "identical to the previous
  // round", which costs at most one extra minInterval confirmation round.
  async rebalanceByTopic(topic: string): Promise<boolean> {
    const client = this.mqClient;
    if (!client) return true;
    // Java reads the cached subscribe-info table (the route refresh loop keeps it
    // warm); warm it here too when a listener-only topic has never been routed.
    let route = client.getTopicRouteData(topic);
    if (!route) {
      await client.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
      route = client.getTopicRouteData(topic);
    }
    const mqAll = (route ? route.getAllSubscribeMessageQueue(topic) : [])
      .slice().sort((a, b) => a.compareTo(b));
    if (mqAll.length === 0) {
      // Java warns "topic not exist" and notifies with two empty sets, then
      // `break`s — which leaves balanced at TRUE. Returning false here would
      // retry at minInterval forever while the topic is merely absent.
      const previous = this.rebalanceTable.get(topic);
      if (previous && previous.length > 0) {
        this.rebalanceTable.set(topic, []);
        logger.warning('doRebalance, %s, topic not exist: %s', this.consumerGroup, topic);
        this.notifyQueueChanged(topic, [], []);
      }
      return true;
    }

    let divided: MessageQueue[];
    if (this.messageModel === MessageModel.BROADCASTING) {
      divided = [...mqAll];
    } else {
      const ids = await this._getConsumerIdsFor(topic);
      if (!ids) {
        // Java warns "get consumer id list failed" and NOTIFIES NOTHING, leaving
        // balanced at true so the next round waits the full interval.
        logger.warning('doRebalance, %s %s, get consumer id list failed',
          this.consumerGroup, topic);
        return true;
      }
      let allocated: MessageQueue[];
      try {
        allocated = this.allocateMessageQueueStrategy.allocate(
          this.consumerGroup, this.clientID, mqAll, [...ids].sort());
      } catch (e) {
        logger.error('allocate message queue exception. strategy name: %s, ex: %s',
          this.allocateMessageQueueStrategy.name(), (e as Error).message);
        return false;
      }
      divided = (allocated || []).filter(mq => mq.getTopic() === topic)
        .sort((a, b) => a.compareTo(b));
    }

    const previous = this.rebalanceTable.get(topic);
    // First sight of the topic: Java's updateProcessQueueTableInRebalance counts only table
    // additions/removals as a change, so "nothing -> nothing" is no change -- an empty
    // opening share stays silent and the round counts as balanced.
    const balanced = previous === undefined
        ? divided.length === 0
        : this._sameMqSet(previous, divided);
    this.rebalanceTable.set(topic, [...divided]);
    if (!balanced) {
      logger.info('client rebalanced result changed. Group: %s, topic: %s, '
        + 'mqAllSize: %d, rebalanceResultSize: %d',
      this.consumerGroup, topic, mqAll.length, divided.length);
      this.notifyQueueChanged(topic, mqAll, divided);
    }
    return balanced;
  }

  // _sameMqSet compares queue identities, not order: losing one queue and gaining
  // another keeps the count while changing who owns what.
  private _sameMqSet(left: MessageQueue[], right: MessageQueue[]): boolean {
    if (left.length !== right.length) return false;
    const seen = new Set(left.map(mqKey));
    for (const mq of right) if (!seen.has(mqKey(mq))) return false;
    return true;
  }

  // notifyQueueChanged is RebalancePullImpl#messageQueueChanged (:44-53): a null
  // listener is fine, and a throwing one is logged and swallowed — one bad
  // callback must not kill the loop and freeze every allocation.
  notifyQueueChanged(topic: string, mqAll: MessageQueue[], mqDivided: MessageQueue[]): void {
    const listener = this.messageQueueListener;
    if (!listener) return;
    try {
      listener(topic, mqAll, mqDivided);
    } catch (e) {
      logger.error('messageQueueChanged exception: %s', (e as Error).message);
    }
  }

  // sendMessageBack redelivers one message via CONSUMER_SEND_MSG_BACK(36).
  // `offset` is the message's commitLogOffset, NOT its queueOffset.
  //
  // A refused bounce — a missing master, a transport error or a non-success
  // answer — degrades to re-sending the message as a normal message to
  // %RETRY%<group> through the internal producer, so the redelivery is not
  // lost; only when that send fails too does the original error reach the
  // caller.
  async sendMessageBack(msg: MessageExt, delayLevel = 0): Promise<void> {
    if (!this.mqClient) throw new Error('consumer not started');
    const maxReconsume = this.maxReconsumeTimes === -1 ? 16 : this.maxReconsumeTimes;
    try {
      const addr = this.mqClient.findBrokerAddressInPublish(msg.getBrokerName());
      if (!addr) throw new Error(`Broker[${msg.getBrokerName()}] master node does not exist`);
      const h = new ConsumerSendMsgBackRequestHeader();
      h.offset = msg.getCommitLogOffset();
      h.group = this.consumerGroup;
      h.delayLevel = delayLevel;
      h.originMsgId = clientMsgId(msg);
      h.originTopic = this._withNamespace(msg.getTopic());
      h.unitMode = this.unitMode;
      h.maxReconsumeTimes = maxReconsume;
      const request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, h);
      const response = await this.mqClient.remotingClient.invokeSync(this.mqClient.vipChannel(addr), request, 3000);
      if (response.code !== ResponseCode.SUCCESS) {
        // Same rule as the pull kernel: the broker code rides on the typed
        // exception, so a caller can branch on it instead of parsing the text.
        throw new MQBrokerException(response.code,
          `sendMessageBack failed: CODE ${response.code} ${response.remark || ''}`, addr);
      }
    } catch (e) {
      logger.error('Failed to send message back, consumerGroup=%s, brokerName=%s, msg=%s: %s; fallback to normal send',
        this.consumerGroup, msg.getBrokerName(), msg.getMsgId(), (e as Error).message);
      try {
        await this._sendRetryMessage(msg, maxReconsume);
      } catch (fallbackError) {
        logger.error('Fallback normal send failed, consumerGroup=%s, msg=%s: %s',
          this.consumerGroup, msg.getMsgId(), (fallbackError as Error).message);
        throw e;
      }
    }
  }

  // _sendRetryMessage sends a failed delivery as a normal message to
  // %RETRY%<group> through the internal producer.
  private async _sendRetryMessage(msg: MessageExt, maxReconsume: number): Promise<void> {
    const producer = await this._innerProducer();
    if (!producer) throw new Error('inner producer is not available');
    await producer.sendBySelector(buildRetryMessage(this.consumerGroup, msg, maxReconsume), undefined);
  }

  // _innerProducer lazily builds the CLIENT_INNER_PRODUCER the send-back
  // fallback sends through.
  private async _innerProducer(): Promise<DefaultMQProducer | null> {
    if (this._producer) return this._producer;
    try {
      const { DefaultMQProducer: P } = await import('./producer.ts');
      const producer = new P(MixAll.CLIENT_INNER_PRODUCER_GROUP || 'CLIENT_INNER_PRODUCER');
      producer.setNamesrvAddr(this.nameServerAddr);
      await producer.start();
      this._producer = producer;
      return producer;
    } catch (e) {
      logger.debug('inner producer start failed: %s', (e as Error).message);
      return null;
    }
  }

  // viewMessage is Java DefaultMQPullConsumerImpl#viewMessage
  // (DefaultMQPullConsumerImpl.java:838) parity: the MQAdminImpl single-leg
  // path — offset msgId only, no uniq-key fallback at this layer.
  async viewMessage(topic: string, msgId: string): Promise<MessageExt> {
    if (!this.mqClient) throw new Error('consumer not started');
    return this.mqClient.viewMessage(topic, msgId);
  }
}
