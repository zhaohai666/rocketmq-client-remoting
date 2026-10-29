// DefaultLitePullConsumer — the lite pull consumer.
//
// Two assignment modes:
//  - subscribe(topic, expr): the assignment is rebalance-driven (AVG over the
//    group's consumer list).
//  - assign(mqs): fully manual, no broker coordination.
//
// The pull is always a SHORT poll via LITE_PULL_MESSAGE(361) with no inline
// offset commit (the consumer owns its own commit path), and the expression is
// ALWAYS sent (this port never drops the subscription field on lite pull).
import { MQClient, DEFAULT_CONCURRENT_HEARTBEAT_THREAD_POOL_SIZE } from './mq_client.ts';
import { MQClientManager } from './mq_client_manager.ts';
import { PullAPI } from './pull_api.ts';
import { registerRpcHooks } from '../remoting/rpc_hooks.ts';
import {
  LocalFileOffsetStore, RemoteBrokerOffsetStore, ReadOffsetMode, mqKey,
} from './offset_store.ts';
import type { OffsetStore } from './offset_store.ts';
import { PullStatus } from './consumer_result.ts';
import { MessageModel, ConsumeType, ConsumeFromWhere } from '../remoting/heartbeat.ts';
import { MessageQueue, MessageExt } from '../common/message.ts';
import { AllocateMessageQueueAveragely } from './allocate.ts';
import type { AllocateMessageQueueStrategy } from './allocate.ts';
import { MixAll } from '../common/mixAll.ts';
import { FilterAPI, SubscriptionData } from '../common/subscriptionData.ts';
import type { MessageSelector } from './consumer_result.ts';
import { PullSysFlag } from '../common/sysflag.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { MQBrokerException } from '../remoting/exception.ts';
import { PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL } from './consumer.ts';
import { Validators } from '../common/validators.ts';
import { defaultConsumeTimestamp, parseConsumeTimestamp } from '../common/utilAll.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.lite_pull_consumer');

const DEFAULT_PULL_BATCH_SIZE = 10;
const DEFAULT_POLL_TIMEOUT_MILLIS = 5000;
// Shortest auto-commit interval setAutoCommitIntervalMillis accepts; below it a setting is ignored.
const MIN_AUTO_COMMIT_INTERVAL_MILLIS = 1000;
// Single queue, nothing found: the wait between two rounds of the same poll()
// call, so an idle queue does not hot-loop against the broker.
const IDLE_POLL_RETRY_DELAY_MILLIS = 200;
// Default value of topicMetadataCheckIntervalMillis: the watched topic's queue
// set is compared once every 30s.
const DEFAULT_TOPIC_METADATA_CHECK_INTERVAL_MILLIS = 30 * 1000;
// The period is clamped to a 1s floor: a timer with a 0 period makes the
// background scheduler fail outright.
const MIN_TOPIC_METADATA_CHECK_INTERVAL_MILLIS = 1000;
// The first comparison only runs 10s after start(), not immediately.
const METADATA_CHECK_FIRST_DELAY_MILLIS = 1000 * 10;

// TopicMessageQueueChangeListener: the callback fires only when the topic's queue
// **set** changes (scale-in or scale-out), and it carries the new set; with no
// change there is no callback.
export interface TopicMessageQueueChangeListener {
  onChanged(topic: string, messageQueues: MessageQueue[]): void | Promise<void>;
}

// Queue-set equality: "never seen this topic before" (no snapshot) is not equal
// to any set — fetchMessageQueues always returns a non-empty list, so the first
// round after registering a listener always fires once.
function isQueueSetEqual(
  oldQueues: MessageQueue[] | undefined, newQueues: MessageQueue[]): boolean {
  if (oldQueues === undefined) return false;
  if (oldQueues.length !== newQueues.length) return false;
  const oldSet = new Set(oldQueues.map(mqKey));
  return newQueues.every((mq) => oldSet.has(mqKey(mq)));
}

// litePullParkMillisFor maps a FAILED lite pull round to how long THIS queue is
// parked before poll() asks it again:
//   * the broker's own flow-control answer (215): the short pause — nothing was
//     handed to the application and the queue is readable again as soon as the
//     broker's pressure drops;
//   * every other code (and a failure that never reached the wire, which carries
//     no code at all): 0, i.e. no park — that is today's behaviour and it stays
//     whatever the port already does for a failed lite pull.
//
// The park is per queue: a throttled queue is stepped over while its siblings are
// still served, exactly like a queue the application paused.
export function litePullParkMillisFor(responseCode?: number): number {
  return responseCode === ResponseCode.FLOW_CONTROL
    ? PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL : 0;
}

export class DefaultLitePullConsumer {
  consumerGroup: string;
  // namespaceV2: the server-side namespace; when non-empty NamespaceRpcHook tags
  // every request with nsd=true / ns=<value>.
  namespaceV2: string | null = null;
  nameServerAddr = '';
  messageModel: string = MessageModel.CLUSTERING;
  // wire fields read by MQClient.buildHeartbeatData:
  consumeType: string = ConsumeType.CONSUME_ACTIVELY;
  // The topics assigned in ASSIGN mode are not a topic-wide subscription, so they
  // register with the broker only when the application opts in through
  // buildSubscriptionsForHeartbeat(); subscribe() mode's topics always ride the
  // heartbeat, and a topic both cover keeps the rebalance expression.
  get subscriptionDataSet(): SubscriptionData[] {
    const subs: SubscriptionData[] = [...this.subscription.values()];
    for (const [topic, sub] of this.heartbeatSubscriptions) {
      if (!this.subscription.has(topic)) subs.push(sub);
    }
    return subs;
  }
  // The extra set announced on top of the rebalance subscriptions.
  private heartbeatSubscriptions = new Map<string, SubscriptionData>();
  unitMode = false;
  // instanceName / unitName / enableStreamRequestType: lite pull turns the stream
  // marker on by default — `@STREAM` is appended to the clientId and every
  // request carries ReqT=0.
  instanceName = 'DEFAULT';
  unitName = '';
  enableStreamRequestType = true;

  clientID = '';
  mqClient: MQClient | null = null;
  pullAPI: PullAPI | null = null;
  offsetStore: OffsetStore | null = null;

  pullBatchSize = DEFAULT_PULL_BATCH_SIZE;
  pollTimeoutMillis = DEFAULT_POLL_TIMEOUT_MILLIS;
  autoCommit = true;
  autoCommitIntervalMillis = 5000;
  // Default CONSUME_FROM_LAST_OFFSET: a queue with no committed offset starts at
  // the broker's maxOffset.
  consumeFromWhere: string = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
  // The start instant of CONSUME_FROM_TIMESTAMP, in "yyyyMMddHHmmss". The default is boot time minus
  // 30 minutes: an empty value would fail at start(), and silently falling back to "now" would turn
  // the policy into "skip the backlog".
  consumeTimestamp: string = defaultConsumeTimestamp();
  // The period at which a background task compares the queue set of every
  // watched topic.
  topicMetadataCheckIntervalMillis = DEFAULT_TOPIC_METADATA_CHECK_INTERVAL_MILLIS;
  // Offset flush period (default 5000 ms): applied to
  // the shared MQClient instance when THIS facade creates it; drives the
  // instance-level persistAllConsumerOffset sweep (which calls this facade's
  // persistConsumerOffset -> commitSync).
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
  // Queue allocation used by subscribe() mode.
  allocateMessageQueueStrategy: AllocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
  // Long-polling bounds. They are validated at start(); a consumer configured
  // to time out before the broker's suspend window would give up on every
  // parked pull.
  brokerSuspendMaxTimeMillis = 20000;
  consumerTimeoutMillisWhenSuspend = 30000;

  setConsumeFromWhere(where: string): this { this.consumeFromWhere = where; return this; }
  // The CONSUME_FROM_TIMESTAMP start instant ("yyyyMMddHHmmss"); the format is checked at start().
  setConsumeTimestamp(ts: string): this { this.consumeTimestamp = ts; return this; }
  getConsumeTimestamp(): string { return this.consumeTimestamp; }

  private subscription = new Map<string, SubscriptionData>();
  // Which of subscribe()/assign() this consumer claimed. Whichever came first
  // holds; the other is refused, so an assignment is never silently switched out
  // from under the queues a caller already pinned.
  private mode: 'none' | 'subscribe' | 'assign' = 'none';
  private assignedManual: MessageQueue[] | null = null; // assign() mode
  private assignedAuto: MessageQueue[] = [];            // subscribe() mode
  // ASSIGN mode's per-topic expression. The caller names it with
  // setSubExpressionForAssign(); assign() seeds '*' for a topic nobody named yet,
  // which is the default the pull task builds when no expression was set.
  private assignSubscription = new Map<string, SubscriptionData>();
  private paused = new Set<string>();
  private pullCursor = new Map<string, number>();       // mqKey -> next pull offset
  // mqKey -> the instant (epoch ms) until which poll() skips this queue, armed by
  // the broker's own flow-control answer (215). Keyed exactly like the other
  // per-queue state, and read by every round of poll().
  private pullParkUntil = new Map<string, number>();
  // Each topic's listener plus the queue set last seen.
  // The key is the **bare** topic, consistent with this implementation's other
  // lite pull state (subscribe / route cache / rebalance) — namespaceV2 takes
  // effect at the wire layer via NamespaceRpcHook rather than by prefixing topic
  // names.
  private topicChangeListeners = new Map<string, TopicMessageQueueChangeListener>();
  private messageQueuesForTopic = new Map<string, MessageQueue[]>();
  private started = false;
  // Start state machine: a consumer can only be started from CREATE_JUST.
  // Every other state refuses, so a second start() can never run a half-built
  // second incarnation.
  private _serviceState: 'CREATE_JUST' | 'RUNNING' | 'START_FAILED' | 'SHUTDOWN_ALREADY' = 'CREATE_JUST';
  private _rebalanceTimer: NodeJS.Timeout | null = null;
  private _autoCommitTimer: NodeJS.Timeout | null = null;
  private _metadataTimer: NodeJS.Timeout | null = null;
  private _metadataInitialTimer: NodeJS.Timeout | null = null;

  constructor(consumerGroup = 'DEFAULT_CONSUMER') {
    this.consumerGroup = consumerGroup || 'DEFAULT_CONSUMER';
  }

  setNamesrvAddr(addr: string): this { this.nameServerAddr = addr; return this; }
  // Sets/reads namespaceV2 (read live on every request).
  setNamespaceV2(ns: string | null): this { this.namespaceV2 = ns; return this; }
  getNamespaceV2(): string | null { return this.namespaceV2; }
  setMessageModel(model: string): this { this.messageModel = model; return this; }
  setInstanceName(name: string): this { this.instanceName = name; return this; }
  setUnitName(name: string): this { this.unitName = name; return this; }
  setPullBatchSize(n: number): this { this.pullBatchSize = n; return this; }
  setPollTimeoutMillis(ms: number): this { this.pollTimeoutMillis = ms; return this; }
  setAutoCommit(enable: boolean): this { this.autoCommit = enable; return this; }
  // Set the auto-commit interval. A value below the floor is ignored outright, so the
  // interval that was already running keeps running instead of being pulled down to the floor.
  setAutoCommitIntervalMillis(ms: number): this {
    if (Number.isFinite(ms) && ms >= MIN_AUTO_COMMIT_INTERVAL_MILLIS) this.autoCommitIntervalMillis = ms;
    return this;
  }
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
  getConsumerGroup(): string { return this.consumerGroup; }
  isStarted(): boolean { return this.started; }
  getServiceState(): string { return this._serviceState; }

  setAllocateMessageQueueStrategy(strategy: AllocateMessageQueueStrategy): this {
    this.allocateMessageQueueStrategy = strategy;
    return this;
  }
  getAllocateMessageQueueStrategy(): AllocateMessageQueueStrategy {
    return this.allocateMessageQueueStrategy;
  }
  setBrokerSuspendMaxTimeMillis(ms: number): this { this.brokerSuspendMaxTimeMillis = ms; return this; }
  getBrokerSuspendMaxTimeMillis(): number { return this.brokerSuspendMaxTimeMillis; }
  setConsumerTimeoutMillisWhenSuspend(ms: number): this {
    this.consumerTimeoutMillisWhenSuspend = ms;
    return this;
  }
  getConsumerTimeoutMillisWhenSuspend(): number { return this.consumerTimeoutMillisWhenSuspend; }

  // Sets the topic metadata comparison period (anything below the floor or
  // otherwise illegal is clamped to the floor).
  setTopicMetadataCheckIntervalMillis(ms: number): this {
    this.topicMetadataCheckIntervalMillis =
      Number.isFinite(ms) && ms >= MIN_TOPIC_METADATA_CHECK_INTERVAL_MILLIS
        ? Math.floor(ms) : MIN_TOPIC_METADATA_CHECK_INTERVAL_MILLIS;
    return this;
  }
  getTopicMetadataCheckIntervalMillis(): number { return this.topicMetadataCheckIntervalMillis; }

  // subscribe() and assign() are mutually exclusive: the first claim holds, and the
  // other side is refused with the same message however it is reached.
  private claimMode(mode: 'subscribe' | 'assign'): void {
    if (this.mode === 'none') { this.mode = mode; return; }
    if (this.mode !== mode) throw new Error('Subscribe and assign are mutually exclusive.');
  }

  subscribe(topic: string, subExpression = '*'): this {
    if (!topic || !topic.trim()) throw new Error('Topic can not be null or empty.');
    this.claimMode('subscribe');
    const expr = subExpression || '*';
    this.subscription.set(topic, FilterAPI.buildSubscriptionData(topic, expr));
    // A subscription added while RUNNING needs its route and a heartbeat now:
    // the rebalance loop only reads the cached route, and the broker learns the
    // expression only from a fresh heartbeat.
    const client = this.mqClient;
    if (this.started && client) {
      void client.updateTopicRouteInfoFromNameServer(topic, false).catch((e) =>
        logger.debug('lite subscribe: route refresh for %s failed: %s', topic, (e as Error).message));
      void client.sendHeartbeatToAllBrokers().catch(() => {});
    }
    return this;
  }

  unsubscribe(topic: string): void {
    this.subscription.delete(topic);
    // The topic's queues leave with it, without waiting for the next rebalance,
    // and their per-queue state goes with them — the same revocation path a
    // rebalance that no longer covers them takes.
    const keep = (mq: MessageQueue) => mq.getTopic() !== topic;
    for (const mq of this._assignedQueues().filter((mq) => !keep(mq))) this.dropQueueState(mq);
    this.assignedAuto = this.assignedAuto.filter(keep);
    if (this.assignedManual != null) this.assignedManual = this.assignedManual.filter(keep);
  }

  // setSubExpressionForAssign names the tag/SQL expression one assigned topic pulls
  // with. It is a pre-start setting: a running consumer has already told the broker
  // what filter this queue uses.
  setSubExpressionForAssign(topic: string, subExpression: string): this {
    if (!subExpression || !subExpression.trim()) {
      throw new Error('subExpression can not be null or empty.');
    }
    if (this.started) throw new Error('setSubExpressionForAssign only can be called before start.');
    if (!topic || !topic.trim()) throw new Error('Topic can not be null or empty.');
    this.claimMode('assign');
    this.assignSubscription.set(topic, FilterAPI.buildSubscriptionData(topic, subExpression));
    return this;
  }

  // buildSubscriptionsForHeartbeat replaces the topics this consumer announces in
  // its heartbeat beyond the rebalance subscriptions. An assign-mode consumer has
  // no topic-wide subscription of its own, yet the broker filters the group's
  // pulls by the expression it was last told, so this is how such an application
  // announces the filters its queues need.
  //
  // Every call replaces the whole set (a topic left out stops being announced),
  // a nil or empty map clears it, a topic the rebalance subscriptions cover keeps
  // their expression, and a rejected selector leaves the previous set intact.
  buildSubscriptionsForHeartbeat(selectors: Map<string, MessageSelector> | null): this {
    const built = new Map<string, SubscriptionData>();
    for (const [topic, selector] of selectors ?? new Map<string, MessageSelector>()) {
      if (!topic || !topic.trim()) throw new Error('Topic can not be null or empty.');
      built.set(topic, FilterAPI.build(topic,
        selector.getExpression(), selector.getType()));
    }
    this.heartbeatSubscriptions = built;
    return this;
  }

  // getSubscriptionsForHeartbeat returns the built set, sorted by topic; a
  // snapshot, not a live view.
  getSubscriptionsForHeartbeat(): SubscriptionData[] {
    return [...this.heartbeatSubscriptions.keys()].sort()
      .map((topic) => this.heartbeatSubscriptions.get(topic)!);
  }

  // assign switches to MANUAL mode: the given queues are polled directly.
  // An empty list is refused: taken literally it would drop every queue held so far,
  // which is exactly what a route lookup that came back empty does by accident.
  // The list is a set: a queue named twice is held once.
  assign(mqs: MessageQueue[]): this {
    if (!mqs || mqs.length === 0) {
      throw new Error('Message queues can not be null or empty.');
    }
    this.claimMode('assign');
    const seen = new Set<string>();
    const kept: MessageQueue[] = [];
    for (const mq of mqs) {
      const key = mqKey(mq);
      if (seen.has(key)) continue;
      seen.add(key);
      kept.push(mq);
      if (!this.assignSubscription.has(mq.getTopic())) {
        this.assignSubscription.set(mq.getTopic(), FilterAPI.buildSubscriptionData(mq.getTopic(), '*'));
      }
    }
    // A queue this call no longer names loses its cursors, its armed park and its
    // paused flag with the assignment; nothing stale may hold up a later round.
    for (const mq of this.assignedManual ?? []) {
      if (!seen.has(mqKey(mq))) this.dropQueueState(mq);
    }
    this.assignedManual = kept;
    return this;
  }

  // Drop every piece of per-queue state this consumer keeps: the pull cursor, the
  // armed 215 park and the paused flag.
  private dropQueueState(mq: MessageQueue): void {
    const key = mqKey(mq);
    this.pullCursor.delete(key);
    this.pullParkUntil.delete(key);
    this.paused.delete(key);
  }

  // _checkConfig validates the pre-start configuration. Subscription is NOT a
  // precondition here: queues can also be named after start() via assign().
  private _checkConfig(): void {
    Validators.checkGroup(this.consumerGroup);
    if (this.consumerGroup === MixAll.DEFAULT_CONSUMER_GROUP) {
      throw new Error(`consumerGroup can not equal ${MixAll.DEFAULT_CONSUMER_GROUP}`);
    }
    if (!this.messageModel) throw new Error('messageModel can not be null');
    if (!this.allocateMessageQueueStrategy) throw new Error('allocateMessageQueueStrategy can not be null');
    if (this.consumerTimeoutMillisWhenSuspend < this.brokerSuspendMaxTimeMillis) {
      throw new Error('Long polling mode, the consumer consumerTimeoutMillisWhenSuspend '
        + 'must greater than brokerSuspendMaxTimeMillis');
    }
  }

  async start(): Promise<void> {
    switch (this._serviceState) {
      case 'CREATE_JUST':
        // Leave CREATE_JUST before anything can fail, so a failed start is not
        // retried with half of the state already built.
        this._serviceState = 'START_FAILED';
        this._checkConfig();
        break;
      case 'RUNNING':
      case 'START_FAILED':
      case 'SHUTDOWN_ALREADY':
        throw new Error(`The PullConsumer service state not OK, maybe started once, ${this._serviceState}`);
      default:
        break;
    }
    if (!this.nameServerAddr) throw new Error('name server address is not set');
    // The start instant is checked whatever the policy is: a malformed value must fail here rather
    // than surface mid-consumption as a failed offset resolution on one queue.
    parseConsumeTimestamp(this.consumeTimestamp);
    // CLUSTERING rewrites the default instanceName, after which the clientId looks
    // like `<ip>@<instanceName>[@<unitName>][@STREAM]`. ⚠ If only the pid is used
    // as the instance name, every lite pull consumer in one process shares a
    // clientId, allocate() hands the same set of queues to two instances, and
    // both read exactly the same messages.
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
    // Warm the route cache for every topic this consumer will pull: the
    // subscribed topics in subscribe mode, the assigned queues' topics in assign
    // mode (which advertises no subscription at all).
    const routeTopics = this.mode === 'assign'
      ? [...new Set((this.assignedManual ?? []).map((mq) => mq.getTopic()))]
      : [...this.subscription.keys()];
    for (const topic of routeTopics) {
      await client.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
    }
    // Once the queues are resolved but before the group is registered at the broker,
    // send one CHECK_CLIENT_IN_BROKER(46) pre-check.
    try {
      await client.checkClientInBroker();
    } catch (e) {
      logger.warning('Start the lite pull consumer %s fail: %s', this.consumerGroup, (e as Error).message);
      // started is still false here, so shutdown() would no-op — unwind the
      // registration this start() already did by hand (the balancing return of
      // registerClient: unregisterClient counts -1, and only hitting zero really
      // shuts down).
      client.unregisterConsumer(this.consumerGroup);
      manager.unregisterClient(client.clientId, client);
      throw e;
    }
    // Register the consumer group at the broker right away: the first beat of the
    // client's heartbeat loop may already have run before any route was cached,
    // and waiting for the 30s period would delay group registration — and hence
    // the first rebalance — by half a minute.
    // So start() proactively sends one extra heartbeat.
    await client.sendHeartbeatToAllBrokers().catch(() => {});
    this.started = true;
    this._serviceState = 'RUNNING';
    // The metadata comparison loop starts unconditionally, whether or not any
    // listener was registered.
    this._startMetadataLoop();
    if (this.mode === 'subscribe') {
      await this._doRebalance();
      this._rebalanceTimer = setInterval(() => {
        this._doRebalance().catch((e) =>
          logger.debug('lite pull rebalance error: %s', (e as Error).message));
      }, 20000);
      if (typeof this._rebalanceTimer.unref === 'function') this._rebalanceTimer.unref();
    }
    if (this.autoCommit) {
      this._autoCommitTimer = setInterval(() => {
        this.commitSync().catch((e) =>
          logger.debug('lite pull auto commit error: %s', (e as Error).message));
      }, this.autoCommitIntervalMillis);
      if (typeof this._autoCommitTimer.unref === 'function') this._autoCommitTimer.unref();
    }
  }

  async shutdown(): Promise<void> {
    if (!this.started) return;
    if (this._rebalanceTimer) { clearInterval(this._rebalanceTimer); this._rebalanceTimer = null; }
    if (this._autoCommitTimer) { clearInterval(this._autoCommitTimer); this._autoCommitTimer = null; }
    if (this._metadataTimer) { clearInterval(this._metadataTimer); this._metadataTimer = null; }
    if (this._metadataInitialTimer) { clearTimeout(this._metadataInitialTimer); this._metadataInitialTimer = null; }
    try { await this.commitSync(); } catch (e) { /* best effort */ }
    if (this.mqClient) {
      await this.mqClient.unregisterClientAllBrokers('', this.consumerGroup).catch(() => {});
      this.mqClient.unregisterConsumer(this.consumerGroup);
      // Handed back to the manager: the count goes -1, and only at zero does it really
      // shut down.
      MQClientManager.getInstance().unregisterClient(this.mqClient.clientId, this.mqClient);
    }
    this.started = false;
    this._serviceState = 'SHUTDOWN_ALREADY';
  }

  // _doRebalance assigns queues only in subscribe() mode.
  private async _doRebalance(): Promise<void> {
    if (this.mode !== 'subscribe' || !this.mqClient) return;
    const client = this.mqClient;
    const assigned: MessageQueue[] = [];
    for (const topic of this.subscription.keys()) {
      await client.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
      const route = client.getTopicRouteData(topic);
      const mqAll = route ? route.getAllSubscribeMessageQueue(topic) : [];
      if (!mqAll.length) continue;
      if (this.messageModel === MessageModel.BROADCASTING) {
        assigned.push(...mqAll);
        continue;
      }
      // Ask one master for the group's consumer id list; on failure KEEP the
      // current assignment (never "I own everything").
      let addr: string | null = null;
      for (const bd of route.brokerDatas || []) {
        const m = bd.brokerAddrs || {};
        if (MixAll.MASTER_ID in m) { addr = m[MixAll.MASTER_ID]; break; }
      }
      if (!addr) continue;
      try {
        const response = await client.getConsumerListByGroup(addr, this.consumerGroup);
        if (response.code !== 0) continue;
        const { RemotingSerializable } = await import('../remoting/serialize.ts');
        const body = response.body && response.body.length
          ? RemotingSerializable.decode(response.body) : null;
        const ids = Array.isArray(body) ? body
          : (body && Array.isArray(body['consumerIdList'])) ? body['consumerIdList'] : [];
        if (!ids.length) continue;
        const sorted = [...mqAll].sort((a, b) => a.compareTo(b));
        assigned.push(...this.allocateMessageQueueStrategy.allocate(
          this.consumerGroup, this.clientID, sorted, ids.map(String).sort()));
      } catch (e) {
        logger.debug('lite pull rebalance: no consumer id list for %s: %s', topic, (e as Error).message);
      }
    }
    assigned.sort((a, b) => a.compareTo(b));
    // A queue the new allocation no longer covers is revoked: its cursor, its
    // armed park and its paused flag go with it, so a later round cannot keep
    // serving a queue this consumer no longer owns.
    const next = new Set(assigned.map(mqKey));
    for (const mq of this.assignedAuto) {
      if (!next.has(mqKey(mq))) this.dropQueueState(mq);
    }
    this.assignedAuto = assigned;
  }

  // fetchMessageQueues returns the SUBSCRIBE view (read perm, readQueueNums, NO
  // master filter). An unroutable topic throws: handing back [] would read as
  // "this topic has zero queues", so a nameserver blip would look like a
  // scale-in and the queue-change listener would happily report it as one.
  async fetchMessageQueues(topic: string): Promise<MessageQueue[]> {
    if (!this.mqClient) throw new Error('The consumer not running, please start it first.');
    await this.mqClient.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
    const route = this.mqClient.getTopicRouteData(topic);
    if (!route) throw new Error(`Can not find MessageQueue for topic: ${topic}`);
    const queues = route.getAllSubscribeMessageQueue(topic);
    if (queues.length === 0) {
      throw new Error(`Can not find Message Queue for this topic, ${topic} Namesrv return empty`);
    }
    return queues;
  }

  fetchSubscribeMessageQueues(topic: string): Promise<MessageQueue[]> {
    return this.fetchMessageQueues(topic);
  }

  // Constraints, one by one:
  //  * empty topic / missing listener -> report "Topic or listener is null";
  //  * re-registering the same topic logs a WARN and overwrites the old listener;
  //  * only a consumer in the RUNNING state takes the snapshot immediately —
  //    otherwise the first round would report the set that "was already there"
  //    as a change.
  // A snapshot is one route RPC, hence the await here; registering before start()
  // is fine too, because the comparison loop starts taking snapshots at
  // start().
  async registerTopicMessageQueueChangeListener(
    topic: string, listener: TopicMessageQueueChangeListener | null | undefined): Promise<void> {
    if (!topic || !topic.trim() || listener == null) {
      throw new Error('Topic or listener is null');
    }
    if (this.topicChangeListeners.has(topic)) {
      logger.warning('Topic %s had been registered, new listener will overwrite the old one', topic);
    }
    this.topicChangeListeners.set(topic, listener);
    if (!this.started) return;
    try {
      this.messageQueuesForTopic.set(topic, await this.fetchMessageQueues(topic));
    } catch (e) {
      logger.debug('register listener: fetch queues for %s failed: %s', topic, (e as Error).message);
    }
  }

  // For each watched topic: compare the route's current queue set against the one
  // seen last time; onChanged fires only on a real change, and only for that
  // topic — one topic's route lookup failing must not abort the whole round (the
  // catch sits inside each topic's loop and only logs).
  // Returns the number of listeners notified (0 when nothing changed).
  async fetchTopicMessageQueuesAndCompare(): Promise<number> {
    let fired = 0;
    // Snapshot the entries: an onChanged implementation may register or drop
    // listeners, and JS Map iteration would then see the mutation.
    for (const [topic, listener] of [...this.topicChangeListeners.entries()]) {
      try {
        const newQueues = await this.fetchMessageQueues(topic);
        if (isQueueSetEqual(this.messageQueuesForTopic.get(topic), newQueues)) continue;
        this.messageQueuesForTopic.set(topic, newQueues);
        await listener.onChanged(topic, newQueues);
        fired++;
      } catch (e) {
        logger.error('ScheduledTask fetchMessageQueuesAndCompare exception: %s', (e as Error).message);
      }
    }
    return fired;
  }

  // Fixed-rate scheduling: the first round is delayed 10s, then one beat per
  // interval. The period is read once at start() and never changes afterwards;
  // the 10s first-round delay keeps a just-started consumer from answering route
  // lookups before the broker has registered the queues.
  private _startMetadataLoop(firstDelayMillis = METADATA_CHECK_FIRST_DELAY_MILLIS,
    periodMillis = this.topicMetadataCheckIntervalMillis): void {
    if (this._metadataTimer != null) return;
    const beat = () => {
      this.fetchTopicMessageQueuesAndCompare().catch((e) =>
        logger.debug('lite pull topic queue change check error: %s', (e as Error).message));
    };
    const initial = setTimeout(beat, firstDelayMillis);
    if (typeof initial.unref === 'function') initial.unref();
    this._metadataInitialTimer = initial;
    this._metadataTimer = setInterval(beat, periodMillis);
    if (typeof this._metadataTimer.unref === 'function') this._metadataTimer.unref();
  }

  // poll fetches the next batch from any assigned, non-paused queue.
  //
  // The cursor starts at the committed offset (or 0 for a %RETRY% topic) and
  // advances by nextBeginOffset after every round.
  //
  // A queue the broker throttled (215) is skipped until its short park expires;
  // the round steps over it and keeps serving its siblings, and a round where
  // NOTHING is pullable waits out the shortest remaining park rather than burning
  // the caller's deadline on a spin.
  async poll(timeoutMillis = this.pollTimeoutMillis): Promise<MessageExt[]> {
    if (!this.started || !this.pullAPI) throw new Error('The consumer not running, please start it first.');
    if (timeoutMillis < 0) throw new Error('Timeout must not be negative');
    const deadline = Date.now() + Math.max(1, timeoutMillis);
    const queues = this._activeQueues();
    if (queues.length === 0) return [];
    const out: MessageExt[] = [];
    let idx = 0;
    while (Date.now() < deadline && out.length < this.pullBatchSize) {
      // Round-robin across queues so no single queue starves the rest.
      let pos = idx % queues.length;
      let steppedOver = 0;
      while (steppedOver < queues.length && this._parkMillisLeft(queues[pos]) > 0) {
        pos = (pos + 1) % queues.length;
        steppedOver++;
      }
      if (steppedOver >= queues.length) {
        // Every queue is parked: the shortest remaining park is the soonest any
        // answer can arrive, so waiting it out respects both the broker's pause
        // and the caller's deadline.
        let shortest = this._parkMillisLeft(queues[0]);
        for (const mq of queues) shortest = Math.min(shortest, this._parkMillisLeft(mq));
        await this._sleepPollRound(Math.min(shortest, Math.max(1, deadline - Date.now())));
        continue;
      }
      idx = pos + 1;
      const mq = queues[pos];
      const key = mqKey(mq);
      const sub = this._subscriptionFor(mq.getTopic());
      if (!sub) continue;
      let offset = this.pullCursor.get(key);
      if (offset === undefined) {
        const committed = await this.offsetStore!.readOffset(mq, ReadOffsetMode.READ_FROM_MEMORY_THEN_STORE);
        // A stored offset always wins; only a group that has never committed one
        // is placed by the consumeFromWhere policy.
        if (committed >= 0) offset = committed;
        else if (this.consumeFromWhere === ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET) offset = 0;
        else if (this.consumeFromWhere === ConsumeFromWhere.CONSUME_FROM_TIMESTAMP) {
          // The policy comes before the retry-topic rule: a retry queue is created by the first
          // redelivery, long after the configured instant, so searching there answers nothing and
          // its replay starts at the tail.
          offset = MixAll.isRetryTopic(mq.getTopic())
            ? await this._maxOffsetFor(mq)
            : await this._searchOffsetFor(mq, parseConsumeTimestamp(this.consumeTimestamp));
        } else {
          // CONSUME_FROM_LAST_OFFSET (and the deprecated variants): a retry queue replays from 0
          // because retried messages must all be retried; a business queue skips the backlog.
          offset = MixAll.isRetryTopic(mq.getTopic()) ? 0 : await this._maxOffsetFor(mq);
        }
        if (offset < 0) continue; // unknown offset; nothing to pull from
        this.pullCursor.set(key, offset);
      }
      try {
        // LITE_PULL_MESSAGE(361), SHORT poll, expression ALWAYS sent, and the
        // lite-pull bit set in sysFlag.
        const sysFlag = PullSysFlag.buildSysFlag(false, false, true, false, true);
        let result = await this.pullAPI.pullKernelLite(mq, offset, sub, sysFlag,
          this.pullBatchSize - out.length, this.brokerSuspendMaxTimeMillis,
          Math.max(1, deadline - Date.now()));
        result = this.pullAPI.processPullResult(mq, result, sub);
        this.pullCursor.set(key, result.nextBeginOffset);
        if (result.status === PullStatus.FOUND && result.msgFoundList.length > 0) {
          out.push(...result.msgFoundList);
          if (this.offsetStore) this.offsetStore.updateOffset(mq, result.nextBeginOffset, true);
          if (out.length >= this.pullBatchSize) break;
        } else if (result.status === PullStatus.OFFSET_ILLEGAL) {
          // The broker corrected the offset: adopt it.
          this.pullCursor.set(key, result.nextBeginOffset);
          if (this.offsetStore) this.offsetStore.updateOffset(mq, result.nextBeginOffset, false);
        }
      } catch (e) {
        logger.debug('lite pull error for %s: %s', key, (e as Error).message);
        // A short poll error: fall through so the remaining queues still get
        // their turn within the deadline. The ONE exception is the broker's own
        // throttle answer, which parks just this queue for the short 20ms pause —
        // retrying it hot is exactly what the broker is asking for not to happen.
        const parkMillis = litePullParkMillisFor(
          e instanceof MQBrokerException ? e.responseCode : undefined);
        if (parkMillis > 0) this.setPullParkUntil(mq, Date.now() + parkMillis);
      }
      if (queues.length === 1 && out.length === 0 && this._parkMillisLeft(mq) <= 0) {
        // Single queue and nothing found: don't hot-loop against the broker. A
        // queue this round parked on 215 waits its own short pause above instead,
        // so the idle guard must not swallow it.
        await this._sleepPollRound(IDLE_POLL_RETRY_DELAY_MILLIS);
      }
    }
    return out;
  }

  // commitSync persists the offsets of every assigned queue.
  async commitSync(): Promise<void> {
    if (!this.offsetStore) return;
    const queues = this._activeQueues();
    await this.offsetStore.persistAll(queues);
  }

  // persistConsumerOffset — the MQConsumerInner contract name the MQClient's
  // instance-level persistAllConsumerOffset sweep calls every
  // persistConsumerOffsetInterval; it is only a thin wrapper
  // over commitSync.
  async persistConsumerOffset(): Promise<void> {
    return this.commitSync();
  }

  // seek repositions one queue: the next poll of it starts at `offset`.
  //
  // The queue must be one this consumer holds — writing a cursor for a stranger
  // would leave an entry nothing pulls, so the caller would watch an offset
  // change that no round ever honours — and the offset is held to the queue's own
  // [minOffset, maxOffset] range. Reading those two bounds is also where the
  // running-state check happens: they are broker RPCs, and a stopped consumer has
  // no broker to ask.
  async seek(mq: MessageQueue, offset: number): Promise<void> {
    const held = this._assignedQueues().some((q) => mqKey(q) === mqKey(mq));
    if (!held) {
      throw new Error(this.mode === 'assign'
        ? `The message queue is not in assigned list, message queue: ${mq}`
        : `The message queue is not in assigned list, may be rebalancing, message queue: ${mq}`);
    }
    if (!this.started || !this.mqClient) {
      throw new Error('The consumer not running, please start it first.');
    }
    // A bound the broker could not answer must not become a cursor: a negative
    // cursor would only come back as OFFSET_ILLEGAL on the next poll.
    const minOffset = await this._boundaryOffset(mq, 'min');
    if (minOffset < 0) throw new Error(`Can not get min offset of queue ${mqKey(mq)}`);
    const maxOffset = await this._boundaryOffset(mq, 'max');
    if (maxOffset < 0) throw new Error(`Can not get max offset of queue ${mqKey(mq)}`);
    if (offset < minOffset || offset > maxOffset) {
      throw new Error(
        `Seek offset illegal, seek offset = ${offset}, min offset = ${minOffset}, max offset = ${maxOffset}`);
    }
    const key = mqKey(mq);
    this.pullCursor.set(key, offset);
    // An explicit repositioning lifts a leftover throttle park: it belonged to
    // the position that is being left behind.
    this.pullParkUntil.delete(key);
    if (this.offsetStore) this.offsetStore.updateOffset(mq, offset, false);
  }

  // assignment(): returns the queues currently assigned.
  assignment(): MessageQueue[] {
    return [...(this.assignedManual != null ? this.assignedManual : this.assignedAuto)];
  }

  // The min/max offset on the **master** broker that owns the queue (returns -1
  // when the route or broker is unknown — the caller skips the queue this round
  // instead of seeking to a fake cursor). Management queries only accept the
  // master address.
  private async _boundaryOffset(mq: MessageQueue, kind: 'min' | 'max' | 'search',
    timestamp = 0): Promise<number> {
    if (!this.mqClient) return -1;
    await this.mqClient.updateTopicRouteInfoFromNameServer(mq.getTopic(), false).catch(() => {});
    const route = this.mqClient.getTopicRouteData(mq.getTopic());
    if (!route) return -1;
    for (const bd of route.brokerDatas || []) {
      const addrs = bd.brokerAddrs || {};
      const addr = addrs['0'] != null ? addrs['0'] : Object.values(addrs)[0];
      if (addr != null && bd.brokerName === mq.getBrokerName()) {
        try {
          if (kind === 'min') {
            return await this.mqClient.getMinOffset(addr, mq.getTopic(), mq.getQueueId());
          }
          if (kind === 'search') {
            return await this.mqClient.searchOffsetByTimestamp(
              addr, mq.getTopic(), mq.getQueueId(), timestamp);
          }
          return await this.mqClient.getMaxOffset(addr, mq.getTopic(), mq.getQueueId());
        } catch (e) { return -1; }
      }
    }
    return -1;
  }

  private _maxOffsetFor(mq: MessageQueue): Promise<number> {
    return this._boundaryOffset(mq, 'max');
  }

  private _searchOffsetFor(mq: MessageQueue, timestamp: number): Promise<number> {
    return this._boundaryOffset(mq, 'search', timestamp);
  }

  // seekToBegin: read minOffset first, then seek; seekToEnd: read maxOffset first,
  // then seek.
  // When the broker cannot supply the boundary value it throws instead of seeking
  // to -1: a negative cursor would come back as OFFSET_ILLEGAL on the next poll,
  // which amounts to a silent error.
  async seekToBegin(mq: MessageQueue): Promise<void> {
    if (!this.started) throw new Error('The consumer not running, please start it first.');
    const begin = await this._boundaryOffset(mq, 'min');
    if (begin < 0) throw new Error(`seekToBegin: minOffset unavailable for ${mqKey(mq)}`);
    await this.seek(mq, begin);
  }

  async seekToEnd(mq: MessageQueue): Promise<void> {
    if (!this.started) throw new Error('The consumer not running, please start it first.');
    const end = await this._boundaryOffset(mq, 'max');
    if (end < 0) throw new Error(`seekToEnd: maxOffset unavailable for ${mqKey(mq)}`);
    await this.seek(mq, end);
  }

  // committed(mq): the offset last committed for this queue — the in-memory
  // commit table first, then the broker — with -1 for "nobody has committed".
  // A broker read that FAILED is not "nobody committed": it throws, so a broken
  // read cannot be mistaken for a fresh consumer.
  async committed(mq: MessageQueue): Promise<number> {
    if (!this.started || !this.offsetStore) {
      throw new Error('The consumer not running, please start it first.');
    }
    const offset = await this.offsetStore.readOffset(mq, ReadOffsetMode.READ_FROM_MEMORY_THEN_STORE);
    if (offset === -2) throw new Error('Fetch consume offset from broker exception');
    return offset;
  }

  pause(mqs: MessageQueue[]): void {
    for (const mq of mqs) this.paused.add(mqKey(mq));
  }

  resume(mqs: MessageQueue[]): void {
    for (const mq of mqs) this.paused.delete(mqKey(mq));
  }

  // setPullParkUntil makes this queue's next pull wait until `untilMillis` (epoch
  // ms) — the knob behind the broker 215 short pause, exposed so an application (or
  // a test) can hold ONE queue back without pausing its siblings. A value not in
  // the future is dropped, and clearing an unknown queue is a no-op.
  setPullParkUntil(mq: MessageQueue, untilMillis: number): void {
    const key = mqKey(mq);
    if (!Number.isFinite(untilMillis) || untilMillis <= Date.now()) {
      this.pullParkUntil.delete(key);
      return;
    }
    this.pullParkUntil.set(key, Math.floor(untilMillis));
  }

  // getPullParkUntil reads back the instant one queue is parked until, or 0 when
  // nothing holds it back. An already-expired park reads as 0 and is pruned here,
  // so the map cannot drift away from the queues it can actually hold back.
  getPullParkUntil(mq: MessageQueue): number {
    const key = mqKey(mq);
    const until = this.pullParkUntil.get(key);
    if (until === undefined) return 0;
    if (until <= Date.now()) {
      this.pullParkUntil.delete(key);
      return 0;
    }
    return until;
  }

  // Remaining park for one queue, in ms (0 when it is pullable now).
  private _parkMillisLeft(mq: MessageQueue): number {
    return this.getPullParkUntil(mq) - Date.now();
  }

  // _sleepPollRound is the one place a poll() round waits between two asks. It is
  // a method so the pause a round asked for can be RECORDED instead of slept by
  // the offline tests — the same handle the push path's _sleepOrStop gives its
  // gate tests. A real wait can only be late, never early, so the short park keeps
  // its lower bound.
  private async _sleepPollRound(millis: number): Promise<void> {
    await new Promise<void>((r) => setTimeout(r, Math.max(1, Math.floor(millis))));
  }

  // _subscriptionFor picks the expression a queue pulls with: the assigned-topic
  // expression in assign mode (subscribe() is refused there), the subscription
  // table in subscribe mode.
  private _subscriptionFor(topic: string): SubscriptionData | undefined {
    return this.mode === 'assign'
      ? this.assignSubscription.get(topic)
      : this.subscription.get(topic);
  }

  private _assignedQueues(): MessageQueue[] {
    return this.assignedManual != null ? this.assignedManual : this.assignedAuto;
  }

  private _activeQueues(): MessageQueue[] {
    return this._assignedQueues().filter((mq) => !this.paused.has(mqKey(mq)));
  }
}
