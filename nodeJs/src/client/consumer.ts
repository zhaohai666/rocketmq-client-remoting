// DefaultMQPushConsumer — the push consumer
// (Java org.apache.rocketmq.client.consumer.DefaultMQPushConsumer +
// DefaultMQPushConsumerImpl + RebalancePushImpl, merged the same way the
// Python/Go/C++/.NET ports merge them).
//
// Shape of the thing, per client instance:
//
//  rebalance loop (20s, or 2s while starting up with nothing assigned)
//    -> assigns queues, then syncPullLoops() retires/adds
//  per-queue pull async-loop (long poll, suspend=true)
//    -> buffers messages into that queue's processQueue
//  one dispatch async-loop
//    -> hands batches to concurrent listeners (bounded by corePoolSize),
//       or consumes orderly batches inline (serialised)
//  instance-scheduled tasks
//    -> heartbeat (30s, MQClient), offset persist (5s), clean-expire, lock
//
// Ordering rules that are load-bearing (see Go consumer.go for the full
// rationale — they are identical here):
//  - The instance's heartbeat puts this group into the broker's
//    ConsumerManager; without it GET_CONSUMER_LIST_BY_GROUP answers empty and
//    rebalance KEEPS the previous assignment (never "I own every queue").
//  - Retiring a queue persists its CONSUMED offset BEFORE the new owner starts.
//  - The initial offset is resolved at ASSIGNMENT time, not lazily at the
//    first pull.
//  - Messages pulled from %RETRY%<group> get msg.topic restored to the
//    business topic BEFORE the listener (resetRetryTopicAndNamespace).
import { Buffer } from 'node:buffer';
import { MQClient, TopicPublishInfo } from './mq_client.ts';
import { ProcessQueue } from './process_queue.ts';
import { PopProcessQueue } from './pop_process_queue.ts';
import {
  syncPopLoops, retirePopQueue, setMessageRequestModeOnBroker,
} from './pop_consumer.ts';
import {
  LocalFileOffsetStore, RemoteBrokerOffsetStore, ReadOffsetMode, mqKey,
} from './offset_store.ts';
import type {
  OffsetStore,
} from './offset_store.ts';
import {
  AllocateMessageQueueAveragely,
} from './allocate.ts';
import type {
  AllocateMessageQueueStrategy,
} from './allocate.ts';
import {
  PullStatus, PullResult, ConsumeConcurrentlyStatus, ConsumeOrderlyStatus,
  ConsumeConcurrentlyContext, ConsumeOrderlyContext, MessageSelector,
} from './consumer_result.ts';
import { ConsumeMessageContext, FilterMessageContext, FilterMessageHook } from './hook.ts';
import { SendMessageRequestHeader, PullMessageRequestHeader } from '../remoting/headers.ts';
import {
  ConsumerSendMsgBackRequestHeader, LockBatchMqRequestHeader, UnlockBatchMqRequestHeader,
  GetConsumerRunningInfoRequestHeader,
} from '../remoting/headers.ts';
import { RemotingCommand, CURRENT_VERSION } from '../remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { RemotingSerializable } from '../remoting/serialize.ts';
import { TopicRouteData } from '../remoting/route.ts';
import {
  ConsumeType, MessageModel, ConsumeFromWhere,
} from '../remoting/heartbeat.ts';
import { PullSysFlag } from '../common/sysflag.ts';
import { MessageExt, MessageQueue, Message } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MixAll } from '../common/mixAll.ts';
import { FilterAPI, SubscriptionData, ExpressionType } from '../common/subscriptionData.ts';
import { decodeMessages } from '../common/messageDecoder.ts';
import { Validators } from '../common/validators.ts';
import { DefaultMQProducer } from './producer.ts';
import { getLogger } from '../logging.ts';
import { RemotingTimeoutException } from '../remoting/exception.ts';

const logger = getLogger('client.consumer');

// Consumer defaults (Java DefaultMQPushConsumer 5.x).
const DEFAULT_PULL_BATCH_SIZE = 32;
const DEFAULT_CONSUME_MESSAGE_BATCH_MAX_SIZE = 1;
const DEFAULT_CONSUME_THREAD_MIN = 20;
const DEFAULT_CONSUME_THREAD_MAX = 20;
const DEFAULT_ADJUST_THREAD_POOL_NUMS_THRESH = 100000;
const DEFAULT_CONSUME_CONCURRENTLY_MAX_SPAN = 2000;
const DEFAULT_PULL_THRESHOLD_FOR_QUEUE = 1000;
const DEFAULT_PULL_THRESHOLD_SIZE_FOR_QUEUE = 100;
const DEFAULT_PULL_THRESHOLD_FOR_TOPIC = -1;
const DEFAULT_PULL_THRESHOLD_SIZE_FOR_TOPIC = -1;
const DEFAULT_PULL_INTERVAL = 0;
const DEFAULT_PULL_TIMEOUT_MILLIS = 30000;
const DEFAULT_PULL_SUSPEND_TIMEOUT_MILLIS = 20000;
const DEFAULT_PULL_BATCH_SIZE_IN_BYTES = 256 * 1024;
const DEFAULT_MAX_RECONSUME_TIMES = -1;
const DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS = 1000;
const DEFAULT_CONSUME_TIMEOUT = 15;
// rebalanceInterval is Java RebalanceService's 20s. While the consumer is young
// (60s) and has NOTHING assigned, retry every 2s so a consumer that starts
// before the topic exists is not stalled for a long time.
const REBALANCE_INTERVAL = 20 * 1000;
const REBALANCE_INTERVAL_DURING_STARTUP = 2 * 1000;
const REBALANCE_STARTUP_WINDOW = 60 * 1000;
const PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
const PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION = 3000;
const JAVA_INT_MAX = 2147483647;
const DEFAULT_CONSUMER_GROUP = 'DEFAULT_CONSUMER';

export { MessageSelector };

function defaultConsumeTimestamp(): string {
  const d = new Date(Date.now() - 30 * 60 * 1000);
  const p = (n: number, w = 2) => String(n).padStart(w, '0');
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
}

// ConsumeReturnType — the ORDINAL is a wire-format value: a SubAfter trace
// record stores it as contextCode. Do not reorder.
export const ConsumeReturnType = {
  SUCCESS: 0, TIME_OUT: 1, EXCEPTION: 2, RETURNNULL: 3, FAILED: 4,
} as const;
const CONSUME_RETURN_TYPE_NAMES = ['SUCCESS', 'TIME_OUT', 'EXCEPTION', 'RETURNNULL', 'FAILED'];

function consumeReturnTypeName(t: number): string {
  return CONSUME_RETURN_TYPE_NAMES[t] || 'SUCCESS';
}

export class DefaultMQPushConsumer {
  consumerGroup: string;
  namespace = '';
  instanceName = 'DEFAULT';
  clientID = '';
  unitMode = false;
  tlsEnable: boolean | null = null;

  messageModel: string = MessageModel.CLUSTERING;
  consumeFromWhere: string = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
  consumeTimestamp: string = defaultConsumeTimestamp();

  consumeThreadMin = DEFAULT_CONSUME_THREAD_MIN;
  consumeThreadMax = DEFAULT_CONSUME_THREAD_MAX;
  corePoolSize = DEFAULT_CONSUME_THREAD_MIN;
  adjustThreadPoolNumsThreshold = DEFAULT_ADJUST_THREAD_POOL_NUMS_THRESH;

  consumeConcurrentlyMaxSpan = DEFAULT_CONSUME_CONCURRENTLY_MAX_SPAN;
  pullThresholdForQueue = DEFAULT_PULL_THRESHOLD_FOR_QUEUE;
  pullThresholdSizeForQueue = DEFAULT_PULL_THRESHOLD_SIZE_FOR_QUEUE;
  pullThresholdForTopic = DEFAULT_PULL_THRESHOLD_FOR_TOPIC;
  pullThresholdSizeForTopic = DEFAULT_PULL_THRESHOLD_SIZE_FOR_TOPIC;
  pullInterval = DEFAULT_PULL_INTERVAL;
  pullTimeoutMillis = DEFAULT_PULL_TIMEOUT_MILLIS;
  pullSuspendTimeoutMillis = DEFAULT_PULL_SUSPEND_TIMEOUT_MILLIS;
  consumeMessageBatchMaxSize = DEFAULT_CONSUME_MESSAGE_BATCH_MAX_SIZE;
  pullBatchSize = DEFAULT_PULL_BATCH_SIZE;
  pullBatchSizeInBytes = DEFAULT_PULL_BATCH_SIZE_IN_BYTES;
  postSubscriptionWhenPull = false;
  maxReconsumeTimes = DEFAULT_MAX_RECONSUME_TIMES;
  suspendCurrentQueueTimeMillis = DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS;
  consumeTimeout = DEFAULT_CONSUME_TIMEOUT;
  allocateStrategy: AllocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
  messageQueueListener: ((topic: string, mqAll: MessageQueue[], mqDivided: MessageQueue[]) => void) | null = null;

  nameServerAddr = '';
  // wire fields read by MQClient.buildHeartbeatData:
  consumeType: string = ConsumeType.CONSUME_PASSIVELY;
  unitModeFlag = false;
  get unitModeValue(): boolean { return this.unitMode; }

  subscription = new Map<string, SubscriptionData>();
  get subscriptionDataSet(): SubscriptionData[] { return this.subscriptions(); }

  listener: any = null;
  orderly = false;

  consumeMessageHookList: any[] = [];
  filterMessageHookList: FilterMessageHook[] = [];

  enableTrace = false;
  traceMsgBatchNum = 10;
  traceTopic = '';
  traceDispatcher: any = null;

  mqClient: MQClient | null = null;
  pullFromWhichNode = new Map<string, number>(); // mqKey -> brokerId
  offsetStore: OffsetStore | null = null;

  started = false;
  private _stopFlags = new Set<string>();
  private _startTime = 0;

  // processQueueTable: the queues this instance currently owns. Membership is
  // also "which queues may be pulled".
  processQueueTable = new Map<string, ProcessQueue>();
  private _queueStopFlags = new Map<string, string>();
  assigned: MessageQueue[] = [];

  // offsetTable is the pull cursor (Java's PullRequest.nextOffset), distinct
  // from the consumed offset the store holds.
  offsetTable = new Map<string, number>();
  private _frozenOffsets = new Set<string>();
  private _queueEpoch = new Map<string, number>();
  private _msgAccCnt = new Map<string, number>();

  private _dispatchSem = 0;
  private _draining = false;
  private _inFlight = 0;
  private _inFlightWaiters: Array<() => void> = [];
  private _timers: NodeJS.Timeout[] = [];
  private _rebalanceNow = false;

  private _producer: DefaultMQProducer | null = null;

  flowControlTriggered = 0;

  // ---- POP mode (Java DefaultMQPushConsumer pop fields; port of
  // go/client/pop_consumer.go). When popMode is on the broker serves this
  // (group, topic) pair in POP mode: no client cursor, ACK per message.
  popMode = false;
  popInvisibleTime = 60000;
  popBatchNums = 32;
  popShareQueueNum = 0;
  popThresholdForQueue = 1000;
  // popQueueTable is the POP counterpart of processQueueTable. Only one of the
  // two is populated at a time; POP has no offset cursor, so the only state is
  // the outstanding-ACK debt per queue.
  popQueueTable = new Map<string, PopProcessQueue>();
  popQueueStopFlags = new Map<string, string>();

  // Consumer-side stats (Java ConsumerStatsManager, shared on the MQClient
  // instance; assigned in start()). Differential-window model — see
  // consumer_stats.ts.
  _statsManager: import('./consumer_stats.ts').ConsumerStatsManager | null = null;

  constructor(consumerGroup = 'DEFAULT_CONSUMER') {
    this.consumerGroup = consumerGroup || 'DEFAULT_CONSUMER';
  }

  // ------------------------------------------------------------- config
  setNamesrvAddr(addr: string): this { this.nameServerAddr = addr; return this; }
  setNamespace(ns: string): this { this.namespace = ns; return this; }
  setInstanceName(name: string): this { this.instanceName = name; return this; }
  setUnitMode(mode: boolean): this { this.unitMode = mode; return this; }
  setTlsEnable(enable: boolean): this { this.tlsEnable = enable; return this; }
  setMessageModel(model: string): this { this.messageModel = model; return this; }
  setConsumeFromWhere(where: string): this { this.consumeFromWhere = where; return this; }
  setConsumeTimestamp(ts: string): this { this.consumeTimestamp = ts; return this; }
  setConsumeThreadMin(n: number): this { this.consumeThreadMin = n; this.corePoolSize = n; return this; }
  setConsumeThreadMax(n: number): this { this.consumeThreadMax = n; return this; }
  setConsumeThreadNums(n: number): this {
    this.consumeThreadMin = n; this.consumeThreadMax = n; this.corePoolSize = n; return this;
  }
  setPullBatchSize(n: number): this { this.pullBatchSize = n; return this; }
  setPullBatchSizeInBytes(n: number): this { this.pullBatchSizeInBytes = n; return this; }
  setPullInterval(ms: number): this { this.pullInterval = ms; return this; }
  setPullTimeoutMillis(ms: number): this { this.pullTimeoutMillis = ms; return this; }
  setPullSuspendTimeoutMillis(ms: number): this { this.pullSuspendTimeoutMillis = ms; return this; }
  setConsumeMessageBatchMaxSize(n: number): this { this.consumeMessageBatchMaxSize = n; return this; }
  setPostSubscriptionWhenPull(enable: boolean): this { this.postSubscriptionWhenPull = enable; return this; }
  setMaxReconsumeTimes(n: number): this { this.maxReconsumeTimes = n; return this; }
  setSuspendCurrentQueueTimeMillis(ms: number): this { this.suspendCurrentQueueTimeMillis = ms; return this; }
  setConsumeTimeout(minutes: number): this { this.consumeTimeout = minutes; return this; }
  setConsumeConcurrentlyMaxSpan(v: number): this { this.consumeConcurrentlyMaxSpan = v; return this; }
  // SetPopMode switches this consumer to POP mode (Java's broker-side
  // MessageRequestMode). A POP consumer must not be orderly: Java leaves
  // orderly POP as a TODO stub and there is no queue lock in the POP protocol
  // to serialise on — checkConfig rejects the combination at start().
  setPopMode(enable: boolean): this { this.popMode = enable; return this; }
  get isPopMode(): boolean { return this.popMode; }
  // SetPopInvisibleTime sets how long a popped batch stays invisible (ms).
  setPopInvisibleTime(ms: number): this { this.popInvisibleTime = ms; return this; }
  setPopBatchNums(n: number): this { this.popBatchNums = n; return this; }
  setPopShareQueueNum(n: number): this { this.popShareQueueNum = n; return this; }
  setPopThresholdForQueue(n: number): this { this.popThresholdForQueue = n; return this; }
  setPullThresholdForQueue(v: number): this { this.pullThresholdForQueue = v; return this; }
  setPullThresholdSizeForQueue(v: number): this { this.pullThresholdSizeForQueue = v; return this; }
  setPullThresholdForTopic(v: number): this { this.pullThresholdForTopic = v; return this; }
  setPullThresholdSizeForTopic(v: number): this { this.pullThresholdSizeForTopic = v; return this; }
  setAllocateMessageQueueStrategy(s: AllocateMessageQueueStrategy): this { this.allocateStrategy = s; return this; }
  setMessageQueueListener(l: any): this { this.messageQueueListener = l; return this; }
  registerConsumeMessageHook(hook: any): this {
    if (hook) this.consumeMessageHookList.push(hook);
    return this;
  }
  registerFilterMessageHook(hook: FilterMessageHook): this {
    if (hook) this.filterMessageHookList.push(hook);
    return this;
  }
  setEnableTrace(enable: boolean): this { this.enableTrace = enable; return this; }
  setTraceTopic(topic: string): this { this.traceTopic = topic; return this; }

  isOrderly(): boolean { return this.orderly; }
  isStarted(): boolean { return this.started; }
  getConsumerGroup(): string { return this.consumerGroup; }
  getClientId(): string { return this.clientID; }

  // Subscribe registers a tag subscription. An empty expression defaults to
  // "*". A selector object {type, expression} selects the expression type.
  subscribe(topic: string, subExpression: string | MessageSelector = '*'): this {
    if (!topic || !topic.trim()) throw new Error('subscription topic is empty');
    let expr: string, exprType: string;
    if (typeof subExpression === 'string') {
      expr = subExpression || '*';
      exprType = ExpressionType.TAG;
    } else {
      expr = subExpression.expression || '*';
      exprType = subExpression.type || ExpressionType.TAG;
    }
    const sub = FilterAPI.buildSubscriptionData(topic, expr);
    sub.expressionType = exprType;
    const existed = this.subscription.has(topic);
    this.subscription.set(topic, sub);
    if (existed && this.started) {
      // Java's RebalancePushImpl.messageQueueChanged: re-run immediately so a
      // changed expression takes effect without waiting out the 20s timer.
      this.rebalanceImmediately();
    }
    return this;
  }

  unsubscribe(topic: string): void {
    const existed = this.subscription.delete(topic);
    if (existed && this.started) this.rebalanceImmediately();
  }

  subscriptions(): SubscriptionData[] {
    return Array.from(this.subscription.values()).sort((a, b) => a.topic.localeCompare(b.topic));
  }

  // SetMessageListener installs the listener: a function (msgs, ctx) => status
  // or an object with a consumeMessage method. Default is CONCURRENT; use
  // setOrderly(true) / the second arg for orderly mode.
  registerMessageListener(listener: any, orderly = false): this {
    this.listener = listener;
    this.orderly = !!orderly;
    return this;
  }
  registerMessageListenerConcurrently(listener: any): this { return this.registerMessageListener(listener, false); }
  registerMessageListenerOrderly(listener: any): this { return this.registerMessageListener(listener, true); }

  private _listenerCall(msgs: MessageExt[], ctx: ConsumeConcurrentlyContext | ConsumeOrderlyContext): { status: number; threw: boolean } {
    const fn = typeof this.listener === 'function'
      ? this.listener
      : (this.listener && typeof this.listener.consumeMessage === 'function' ? this.listener.consumeMessage.bind(this.listener) : null);
    if (!fn) throw new Error('listener is not callable');
    try {
      return { status: fn(msgs, ctx), threw: false };
    } catch (e) {
      // Java's exception rule: a throw is RECONSUME_LATER (concurrent) or
      // suspend-and-retry-in-place (orderly) — never a crash.
      logger.debug('listener error: %s', (e as Error).message);
      return { status: -1, threw: true };
    }
  }

  // ------------------------------------------------- broker-initiated requests

  // RebalanceImmediately is the NOTIFY_CONSUMER_IDS_CHANGED(40) fan-out target.
  // It must only set a flag — this runs on the remoting read path, and any
  // synchronous request here would deadlock the connection.
  rebalanceImmediately(): void {
    this._rebalanceNow = true;
  }

  // ---------------------------------------------------------------- lifecycle

  // Start brings the consumer up following Java DefaultMQPushConsumerImpl.start.
  async start(): Promise<void> {
    if (this.started) return;
    if (this.namespace) {
      // Must run BEFORE the retry topic is derived: the retry topic is
      // %RETRY% + the WRAPPED group name.
      this.consumerGroup = MixAll.wrapNamespace
        ? MixAll.wrapNamespace(this.namespace, this.consumerGroup)
        : this.consumerGroup;
    }
    Validators.checkGroup(this.consumerGroup);
    if (this.consumerGroup === DEFAULT_CONSUMER_GROUP) {
      throw new Error(`consumerGroup can not equal ${DEFAULT_CONSUMER_GROUP}, please specify another one.`);
    }
    if (!this.nameServerAddr) throw new Error('name server address is not set');
    if (this.subscription.size === 0) {
      throw new Error('subscription is not set, call subscribe() first');
    }
    if (this.listener == null) throw new Error('message listener is not set');
    this._parseConsumeTimestamp(this.consumeTimestamp); // hard fail on bad format
    this._checkConfigRanges();
    if (this.messageModel === MessageModel.CLUSTERING) {
      // Java start:934-936 — only CLUSTERING rewrites DEFAULT to pid#nanotime.
      if (this.instanceName === 'DEFAULT') {
        this.instanceName = `DEFAULT${process.pid}`;
      }
    }
    this.clientID = MixAll.buildMqClientId
      ? MixAll.buildMqClientId(this.instanceName)
      : `${this.instanceName}@${Date.now()}`;

    const client = new MQClient(this.clientID, this.nameServerAddr);
    if (this.tlsEnable != null) client.remotingClient.tlsEnable = this.tlsEnable;
    this.mqClient = client;
    // Java MQClientFactory.getConsumerStatsManager — instance-level shared.
    this._statsManager = client.consumerStatsManager;
    client.registerConsumer(this.consumerGroup, this);
    client.start();

    // Clustering subscribes its retry topic automatically; the broker
    // redelivers into it. SUB_ALL keeps tagsSet EMPTY (see FilterAPI).
    if (this.messageModel !== MessageModel.BROADCASTING) {
      const retryTopic = MixAll.getRetryTopic(this.consumerGroup);
      if (!this.subscription.has(retryTopic)) {
        this.subscription.set(retryTopic, FilterAPI.buildSubscriptionData(retryTopic, '*'));
      }
    }
    if (this.messageModel === MessageModel.BROADCASTING) {
      this.offsetStore = new LocalFileOffsetStore(this.clientID, this.consumerGroup);
    } else {
      this.offsetStore = new RemoteBrokerOffsetStore(client, this.consumerGroup);
    }
    this.corePoolSize = this.consumeThreadMin;
    this._dispatchSem = 0;
    this._draining = false;
    this._inFlight = 0;
    this._stopFlags.clear();
    this._queueStopFlags.clear();
    this._startTime = Date.now();
    this.started = true;

    await this.offsetStore.load().catch((e) => {
      logger.warning('load offset store failed for %s: %s', this.consumerGroup, (e as Error).message);
    });
    for (const topic of this.subscription.keys()) {
      await client.updateTopicRouteInfoFromNameServer(topic, false).catch((e) => {
        logger.debug('initial route refresh failed for %s: %s', topic, (e as Error).message);
      });
    }
    // Register broker-initiated request processors BEFORE the first heartbeat.
    this._registerProcessors();
    // Heartbeat must precede rebalance: rebalance asks the broker for the
    // group's client list.
    await this.sendHeartbeatNow();
    // The first assignment is computed synchronously; otherwise the pull loops
    // would spin on an empty assignment until the first timer tick.
    await this.doRebalance();

    // POP mode: tell the broker to serve this (group, topic) pair as POP. Java
    // does this out of band (mqadmin / console); doing it here keeps the client
    // self-contained. A failure is logged, not fatal — a broker that already
    // has the group in POP mode answers SUCCESS anyway, and an old broker
    // without POP support must not take the consumer down.
    if (this.popMode) {
      const err = await setMessageRequestModeOnBroker(this, 3000);
      if (err) logger.warning('enable POP on broker failed for group %s: %s', this.consumerGroup, err.message);
    }

    this._goLoop(() => this._rebalanceLoop());
    this._goLoop(() => this._dispatchLoop());
    if (!this.orderly && !this.popMode) this._goLoop(() => this._cleanExpireLoop());
    if (this.orderly && this.messageModel !== MessageModel.BROADCASTING) {
      this._goLoop(() => this._lockLoop());
    }
    this._goLoop(() => this._persistOffsetLoop());
    this._goLoop(() => this._heartbeatLoop());
    if (this.enableTrace) {
      this._startTraceDispatcher();
    }
  }

  // Shutdown unwinds in Java's order: persist offsets (while started is still
  // true), unlock if orderly, unregister at every broker, then stop the loops.
  async shutdown(): Promise<void> {
    if (!this.started) return;
    this._draining = true;
    const client = this.mqClient;
    try { await this.persistConsumerOffset(); } catch (e) { /* best effort */ }
    if (this.orderly && this.messageModel !== MessageModel.BROADCASTING) {
      await this.unlockAssigned();
    }
    if (client) {
      await client.unregisterClientAllBrokers('', this.consumerGroup).catch(() => {});
      client.unregisterConsumer(this.consumerGroup);
    }
    // Stop all loops.
    for (const flag of Array.from(this._stopFlags)) {
      this._stopFlags.delete(flag); // delete = stop signal for sleepers
    }
    this._stopFlags.add('__stopped');
    // POP loops ride the same stop-flag set; drop their queue state so an
    // in-flight batch is marked dropped and never ACKs a batch the broker has
    // taken back.
    for (const key of Array.from(this.popQueueTable.keys())) {
      retirePopQueue(this, key);
    }
    for (const t of this._timers) clearTimeout(t);
    this._timers = [];
    // Wait (bounded) for in-flight batches — the listener call and its
    // send-back. Without this window an immediate process exit can cut a
    // CONSUMER_SEND_MSG_BACK(36) short and the message never reaches
    // %RETRY%/%DLQ%.
    await this._waitInFlightDrain(30000);
    // Java persists a second time from MQClientInstance.shutdown: offsets the
    // drain just advanced must land too.
    try { await this.persistConsumerOffset(); } catch (e) { /* best effort */ }
    this.started = false;
    if (this._producer) {
      try { this._producer.shutdown(); } catch (e) { /* ignore */ }
      this._producer = null;
    }
    if (client) client.shutdown();
    this._statsManager = null; // manager timer stopped by client.shutdown()
    if (this.traceDispatcher) {
      try { this.traceDispatcher.shutdown(); } catch (e) { /* ignore */ }
      this.traceDispatcher = null;
    }
  }

  private _startTraceDispatcher(): void {
    // Java DefaultMQPushConsumer.start:180-190: failures only log — a broken
    // trace stack must never take the consumer down.
    try {
      // Lazy wiring; the trace dispatcher lives in trace_dispatcher.ts.
      // Imported lazily to avoid a hard dependency cycle for consumers that
      // never enable trace.
      import('./trace_dispatcher.ts').then((m: any) => {
        if (!this.started || this.traceDispatcher) return;
        const dispatcher = new m.AsyncTraceDispatcher(
          this.consumerGroup, 'CONSUME', this.traceMsgBatchNum, this.traceTopic || undefined);
        dispatcher.setHostConsumer(this);
        this.traceDispatcher = dispatcher;
        if (this.consumerGroup) {
          import('./trace_hook.ts').then((th: any) => {
            this.registerConsumeMessageHook(new th.ConsumeMessageTraceHookImpl(dispatcher));
          }).catch((e) => logger.warning('trace hook load failed: %s', (e as Error).message));
        }
        dispatcher.start(this.nameServerAddr).catch((e: Error) =>
          logger.warning('trace dispatcher start failed: %s', e.message));
      }).catch((e) => logger.warning('trace dispatcher load failed: %s', (e as Error).message));
    } catch (e) {
      logger.warning('trace dispatcher init failed: %s', (e as Error).message);
    }
  }

  // ---------------------------------------------------------------- loops

  private _goLoop(fn: () => Promise<void>): void {
    fn().catch((e) => logger.error('consumer loop error: %s', (e as Error).message || e));
  }

  private _newStopFlag(): string {
    const flag = `sf-${Math.random().toString(36).slice(2)}-${Date.now()}`;
    this._stopFlags.add(flag);
    return flag;
  }

  private _isStopped(flag?: string): boolean {
    if (this._stopFlags.has('__stopped')) return true;
    if (flag && !this._stopFlags.has(flag)) return true; // retired queue
    return false;
  }

  // sleepOrStop waits, and reports whether it was interrupted.
  private _sleepOrStop(ms: number, flag?: string): Promise<boolean> {
    if (ms <= 0) return Promise.resolve(this._isStopped(flag));
    return new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => {
        clearInterval(poll);
        resolve(false);
      }, ms);
      const poll = setInterval(() => {
        if (this._isStopped(flag)) {
          clearTimeout(timer);
          clearInterval(poll);
          resolve(true);
        }
      }, 20);
      if (poll && typeof poll.unref === 'function') poll.unref();
      if (typeof timer.unref === 'function') timer.unref();
    });
  }

  private _beginInFlight(): boolean {
    if (this._draining) return false;
    this._inFlight++;
    return true;
  }

  private _endInFlight(): void {
    this._inFlight = Math.max(0, this._inFlight - 1);
    if (this._inFlight === 0 && this._inFlightWaiters.length) {
      const waiters = this._inFlightWaiters;
      this._inFlightWaiters = [];
      for (const w of waiters) w();
    }
  }

  private _waitInFlightDrain(budgetMs: number): Promise<void> {
    if (this._inFlight === 0) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        logger.warning('consumer shutdown drain timed out after %d ms, group=%s; in-flight batches detached',
          budgetMs, this.consumerGroup);
        resolve();
      }, budgetMs);
      this._inFlightWaiters.push(() => {
        clearTimeout(timer);
        resolve();
      });
    });
  }

  // Immediate full-broker heartbeat (Java sendHeartbeatToAllBrokerWithLock).
  async sendHeartbeatNow(): Promise<void> {
    if (this.client == null) return;
    await this.client.sendHeartbeatToAllBrokers();
  }

  private async _heartbeatLoop(): Promise<void> {
    const flag = this._newStopFlag();
    while (!this._isStopped(flag)) {
      if (!(await this._sleepOrStop(30000, flag))) {
        try { await this.sendHeartbeatNow(); } catch (e) { /* best effort */ }
      }
    }
  }

  private async _persistOffsetLoop(): Promise<void> {
    const flag = this._newStopFlag();
    while (!this._isStopped(flag)) {
      if (!(await this._sleepOrStop(5000, flag))) {
        try { await this.persistConsumerOffset(); } catch (e) { /* best effort */ }
      }
    }
  }

  private async _rebalanceLoop(): Promise<void> {
    const flag = this._newStopFlag();
    while (!this._isStopped(flag)) {
      const startingUp = (Date.now() - this._startTime) < REBALANCE_STARTUP_WINDOW;
      const interval = (startingUp && this.assigned.length === 0)
        ? REBALANCE_INTERVAL_DURING_STARTUP : REBALANCE_INTERVAL;
      const stopped = await this._sleepOrStop(interval, flag);
      if (stopped || !this.started) return;
      // NOTIFY_CONSUMER_IDS_CHANGED path: the flag was set on the read thread
      // and is consumed here, on the rebalance loop.
      this._rebalanceNow = false;
      try { await this.doRebalance(); } catch (e) {
        logger.debug('rebalance error: %s', (e as Error).message);
      }
    }
  }

  // ------------------------------------------------------------- rebalance

  // doRebalance is Java RebalanceImpl#rebalanceByTopic for every subscribed
  // topic, then syncPullLoops.
  //
  // BROADCASTING: every queue is ours, no broker coordination.
  // CLUSTERING: ask the broker for the group's client list -> sort cidAll and
  // mqAll -> run the strategy -> take our slice.
  //
  // A missing consumer list KEEPS the current assignment (Java warns). Never
  // degrade to "I own everything": co-instances would duplicate every message.
  async doRebalance(): Promise<void> {
    if (!this.mqClient || !this.started) return;
    const client = this.mqClient;
    const was = new Set(this.assigned.map(mqKey));
    const topics = Array.from(this.subscription.keys()).sort();
    const assigned: MessageQueue[] = [];
    for (const topic of topics) {
      const route = client.getTopicRouteData(topic);
      const info: MessageQueue[] = route ? route.getAllSubscribeMessageQueue(topic) : [];
      if (info.length === 0) {
        logger.debug('rebalance: no subscribe info for topic %s', topic);
      }
      if (this.messageModel === MessageModel.BROADCASTING) {
        assigned.push(...info);
        continue;
      }
      const mqAll = [...info].sort((a, b) => a.compareTo(b));
      if (mqAll.length === 0) continue;
      const { cidAll, answered } = await this._getConsumerIdListByGroup(topic);
      if (!answered || cidAll.length === 0) {
        logger.debug('rebalance: no consumer id list for %s/%s, keep current', this.consumerGroup, topic);
        for (const mq of this.assigned) {
          if (mq.getTopic() === topic) assigned.push(mq);
        }
        continue;
      }
      cidAll.sort();
      const strategy = this.allocateStrategy || new AllocateMessageQueueAveragely();
      const got = strategy.allocate(this.consumerGroup, this.clientID, mqAll, cidAll);
      if (!got || got.length === 0) {
        logger.warning('allocate message queue returned nothing, strategy=%s group=%s',
          strategy.name(), this.consumerGroup);
      }
      assigned.push(...got);
    }
    assigned.sort((a, b) => a.compareTo(b));
    this.assigned = assigned;
    const now = new Set(assigned.map(mqKey));
    if (now.size !== was.size) {
      logger.info('rebalance result changed, group=%s clientId=%s assigned=%d',
        this.consumerGroup, this.clientID, assigned.length);
    }
    // Resolve the initial offset for freshly assigned queues NOW, not lazily
    // on the first pull: CONSUME_FROM_LAST_OFFSET means "the newest offset as
    // of the moment the queue was assigned".
    for (const mq of assigned) {
      if (was.has(mqKey(mq))) continue;
      if (!this.subscription.has(mq.getTopic())) continue;
      if (this.offsetTable.has(mqKey(mq))) continue;
      try {
        const next = await this.computePullFromWhere(mq);
        if (next < 0) continue;
        if (!this.offsetTable.has(mqKey(mq))) this.offsetTable.set(mqKey(mq), next);
        if (this.offsetStore) this.offsetStore.updateOffset(mq, next, false);
      } catch (e) {
        logger.debug('resolve initial offset for %s failed: %s', mqKey(mq), (e as Error).message);
      }
    }
    this.syncPullLoops();
  }

  // _getConsumerIdListByGroup is Java MQClientInstance#findConsumerIdList: ask
  // one master of the topic's route. Every client heartbeats to every broker,
  // so any one broker holds the COMPLETE list for the group.
  private async _getConsumerIdListByGroup(topic: string): Promise<{ cidAll: string[]; answered: boolean }> {
    if (!this.mqClient) return { cidAll: [], answered: false };
    const client = this.mqClient;
    const route = client.getTopicRouteData(topic);
    if (!route) return { cidAll: [], answered: false };
    let addr: string | null = null;
    for (const bd of route.brokerDatas || []) {
      const m = bd.brokerAddrs || {};
      if (MixAll.MASTER_ID in m) { addr = m[MixAll.MASTER_ID]; break; }
    }
    if (!addr) return { cidAll: [], answered: false };
    try {
      const response = await client.getConsumerListByGroup(addr, this.consumerGroup);
      if (response.code !== ResponseCode.SUCCESS) return { cidAll: [], answered: false };
      const body = response.body && response.body.length
        ? RemotingSerializable.decode(response.body) : null;
      const ids = Array.isArray(body) ? body
        : (body && Array.isArray(body['consumerIdList'])) ? body['consumerIdList'] : [];
      return { cidAll: ids.map(String), answered: true };
    } catch (e) {
      logger.debug('getConsumerIdListByGroup failed, %s %s: %s', addr, this.consumerGroup, (e as Error).message);
      return { cidAll: [], answered: false };
    }
  }

  // syncPullLoops mirrors Java RebalanceImpl#updateProcessQueueTableInRebalance:
  // retire what we no longer own, then start a pull loop per new queue.
  //
  // Revocation has three mandatory steps — persist the CONSUMED offset, drop
  // the ProcessQueue, and UNLOCK_BATCH_MQ for an orderly clustering consumer.
  // Order matters: retire -> settle -> add. Reversed, the new loop would start
  // from a stale offset and write the smaller value back.
  syncPullLoops(): void {
    // POP mode branches here too (Go does the same): the POP twin retires and
    // adds pop loops instead of pull loops — no offset to settle, only the
    // dropped marker so in-flight batches abort instead of ACKing.
    if (this.popMode) {
      syncPopLoops(this);
      return;
    }
    const current = new Set(this.assigned.map(mqKey));
    const revoked: Array<{ mq: MessageQueue; offset: number; hasOffset: boolean }> = [];
    for (const [key, pq] of Array.from(this.processQueueTable.entries())) {
      if (!current.has(key)) {
        revoked.push(this._retireQueue(key));
        continue;
      }
      if (this.started && pq.isPullExpired()) {
        logger.error('[BUG]doRebalance, %s, try remove unnecessary mq, %s, because pull is pause, so try to fixed it',
          this.consumerGroup, key);
        revoked.push(this._retireQueue(key));
      }
    }
    if (revoked.length > 0) {
      this._persistRevoked(revoked);
    }
    for (const mq of this.assigned) {
      const key = mqKey(mq);
      if (this.processQueueTable.has(key)) continue;
      const pq = new ProcessQueue(this.orderly);
      this.processQueueTable.set(key, pq);
      // A fresh ProcessQueue lifts the freeze (Java removeProcessQueue's
      // removeOffset): the rebuilt queue advances from the corrected offset.
      this._frozenOffsets.delete(key);
      pq.touchPull();
      // A rebuilt queue starts over from the master instead of the slave the
      // previous incarnation had drifted to.
      this.pullFromWhichNode.delete(key);
      const stopFlag = this._newStopFlag();
      this._queueStopFlags.set(key, stopFlag);
      this._goLoop(() => this._queuePullLoop(mq, stopFlag));
    }
    if (revoked.length > 0) {
      logger.info('queues revoked, group=%s count=%d', this.consumerGroup, revoked.length);
    }
    this._notifyQueueChanged();
  }

  // _retireQueue drops one queue's local state.
  //
  // The epoch bump invalidates every in-flight batch's ack (Java's
  // setDropped(true)); the freeze marker is KEPT until the queue is rebuilt, so
  // a corrected offset cannot be overwritten by an old ack.
  private _retireQueue(key: string): { mq: MessageQueue; offset: number; hasOffset: boolean } {
    const stopFlag = this._queueStopFlags.get(key);
    if (stopFlag) {
      this._stopFlags.delete(stopFlag); // signal the pull loop to exit
      this._queueStopFlags.delete(key);
    }
    const pq = this.processQueueTable.get(key);
    const [topic, brokerName, queueId] = parseMqKey(key);
    const mq = new MessageQueue(topic, brokerName, queueId);
    let offset = 0;
    let hasOffset = false;
    if (this.offsetStore && typeof (this.offsetStore as any).tableSnapshot === 'function') {
      const snap = (this.offsetStore as any).tableSnapshot() as Map<string, number>;
      const v = snap.get(key);
      if (v !== undefined && v >= 0) { offset = v; hasOffset = true; }
    }
    if (pq) {
      pq.setDropped();
      pq.clear();
    }
    this.processQueueTable.delete(key);
    this.offsetTable.delete(key);
    this._msgAccCnt.delete(key);
    this.pullFromWhichNode.delete(key);
    this._queueEpoch.set(key, (this._queueEpoch.get(key) || 0) + 1);
    return { mq, offset, hasOffset };
  }

  // _persistRevoked is Java RebalanceImpl#removeUnnecessaryMessageQueue.
  private _persistRevoked(revoked: Array<{ mq: MessageQueue; offset: number; hasOffset: boolean }>): void {
    if (!this.offsetStore) return;
    if (this.messageModel === MessageModel.BROADCASTING) {
      // Broadcast offsets live in a local file. Java persists BEFORE
      // removeOffset so the value stays on disk and a rebuilt queue can
      // continue from it.
      this.offsetStore.persistAll([]).catch((e) =>
        logger.debug('persist local offsets on revoke failed: %s', (e as Error).message));
      return;
    }
    for (const q of revoked) {
      if (q.hasOffset) {
        // Java persist(mq): write the value now, do not wait for the 5s
        // periodic flush — the new owner may start pulling any moment.
        this.offsetStore.updateOffset(q.mq, q.offset, false);
        this.offsetStore.persist(q.mq).catch((e) =>
          logger.debug('persist offset on revoke failed for %s: %s', mqKey(q.mq), (e as Error).message));
      }
      if (this.orderly && this.mqClient) {
        // Orderly: release the broker queue lock so the new owner can start.
        this._unlockBatchMQ([q.mq]).catch(() => {});
      }
    }
  }

  private _notifyQueueChanged(): void {
    const listener = this.messageQueueListener;
    const client = this.mqClient;
    const assigned = [...this.assigned];
    if (!listener || !client) return;
    const byTopic = new Map<string, MessageQueue[]>();
    for (const mq of assigned) {
      const list = byTopic.get(mq.getTopic()) || [];
      list.push(mq);
      byTopic.set(mq.getTopic(), list);
    }
    for (const [topic, divided] of byTopic) {
      const route = client.getTopicRouteData(topic);
      listener(topic, route ? route.getAllSubscribeMessageQueue(topic) : [], divided);
    }
  }

  // computePullFromWhere is Java RebalancePushImpl#computePullFromWhere.
  //
  // READ_FROM_STORE first, then the per-policy fallback. Two items are
  // deliberate: CONSUME_FROM_FIRST_OFFSET returns 0 WITHOUT querying minOffset
  // (the offset gets fixed by the OFFSET_ILLEGAL path); and a %RETRY% topic
  // with no committed offset starts at 0 rather than maxOffset, because
  // retried messages must all be retried.
  async computePullFromWhere(mq: MessageQueue): Promise<number> {
    const store = this.offsetStore;
    const client = this.mqClient;
    if (!store || !client) return -1;
    const last = await store.readOffset(mq, ReadOffsetMode.READ_FROM_STORE);
    if (last >= 0) return last;
    const where = this.consumeFromWhere;
    if (where === ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET) return 0;
    if (where === ConsumeFromWhere.CONSUME_FROM_TIMESTAMP) {
      if (MixAll.isRetryTopic(mq.getTopic())) {
        return this._getMaxOffsetForMq(mq);
      }
      const ts = this._parseConsumeTimestamp(this.consumeTimestamp);
      return this._searchOffsetForMq(mq, ts);
    }
    // CONSUME_FROM_LAST_OFFSET (and the two deprecated variants).
    if (MixAll.isRetryTopic(mq.getTopic())) return 0;
    return this._getMaxOffsetForMq(mq);
  }

  private async _getMaxOffsetForMq(mq: MessageQueue): Promise<number> {
    if (!this.mqClient) return -1;
    const addr = this.mqClient.findBrokerAddrForQueue(mq);
    if (!addr) return -1;
    try {
      return await this.mqClient.getMaxOffset(addr, mq.getTopic(), mq.getQueueId());
    } catch (e) { return -1; }
  }

  private async _searchOffsetForMq(mq: MessageQueue, timestamp: number): Promise<number> {
    if (!this.mqClient) return -1;
    const addr = this.mqClient.findBrokerAddrForQueue(mq);
    if (!addr) return -1;
    try {
      return await this.mqClient.searchOffsetByTimestamp(addr, mq.getTopic(), mq.getQueueId(), timestamp);
    } catch (e) { return -1; }
  }

  // ------------------------------------------------------------- pull loop

  // _queuePullLoop pulls one queue until it is retired or the consumer stops.
  //
  // Why one loop PER QUEUE rather than Java's shared PullMessageService
  // thread: the push consumer long-polls (suspend=true), so an idle queue
  // parks its request for up to ~15s. A shared loop would serialise those
  // parks and starve every other queue.
  private async _queuePullLoop(mq: MessageQueue, stopFlag: string): Promise<void> {
    const key = mqKey(mq);
    while (!this._isStopped(stopFlag)) {
      const pq = this.processQueueTable.get(key);
      if (!pq || pq.isDropped()) return;
      // Java DefaultMQPushConsumerImpl.pullMessage:253 — the timestamp is
      // stamped when a pull is STARTED, before the flow-control/lock
      // decisions: the stall detector asks "is this loop alive".
      pq.touchPull();

      const sub = this.subscription.get(mq.getTopic());
      if (!sub) return;
      if (this.orderly && !pq.isLocked()) {
        // Orderly in CLUSTERING: do not pull before LOCK_BATCH_MQ granted
        // this queue, or we would consume what another instance is about to
        // lock.
        if (await this._sleepOrStop(200, stopFlag)) return;
        continue;
      }
      if (this._flowControlHit(mq, pq)) {
        if (await this._sleepOrStop(PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, stopFlag)) return;
        continue;
      }
      let offset = this.offsetTable.get(key);
      if (offset === undefined) {
        if (await this._sleepOrStop(1000, stopFlag)) return;
        continue;
      }

      // Java pullMessage:458-468 — the expression goes on the wire only when
      // postSubscriptionWhenPull is on AND the subscription is not a class
      // filter. Off by default: the broker then does not filter and the
      // client's second-stage tag check covers it.
      const withExpr = this.postSubscriptionWhenPull && !sub.classFilterMode;
      let sysFlag = PullSysFlag.buildSysFlag(false, true, withExpr, false);
      let commitOffsetValue = 0;
      const client = this.mqClient;
      if (!client) return;

      // Resolve the broker address by the pullFromWhichNode id (rule #22):
      // every response rewrites the table, and a MISSING field means master(0)
      // — not "keep the previous value".
      let brokerId = this.pullFromWhichNode.get(key);
      if (brokerId === undefined) brokerId = MixAll.MASTER_ID;
      let resolved = this._findBrokerAddressInSubscribe(mq.getBrokerName(), brokerId, false);
      if (!resolved) {
        await client.updateTopicRouteInfoFromNameServer(mq.getTopic(), false).catch(() => {});
        resolved = this._findBrokerAddressInSubscribe(mq.getBrokerName(), brokerId, false);
        if (!resolved) {
          logger.debug('The broker[%s] not exist', mq.getBrokerName());
          if (await this._sleepOrStop(500, stopFlag)) return;
          continue;
        }
      }
      let sysFlagFinal = sysFlag;
      if (resolved.isSlave) {
        // Java pullKernelImpl:219-221 — a slave does not accept offset commits.
        sysFlagFinal = PullSysFlag.clearCommitOffsetFlag(sysFlag);
      }

      // The pull RT is measured from just before the request, so it covers the
      // long-poll hold too.
      const begin = Date.now();
      let result: PullResult | null = null;
      try {
        result = await this._pullKernel(mq, offset, sub, sysFlagFinal, commitOffsetValue,
          this.pullBatchSize, this.pullBatchSizeInBytes, this.pullSuspendTimeoutMillis,
          this.pullTimeoutMillis, resolved.addr);
      } catch (e) {
        if (e instanceof RemotingTimeoutException) {
          // A long poll timing out while suspended is NORMAL: the broker
          // clamps the suspend time to its own brokerSuspendMaxTimeMillis
          // (~15s) and ignores what we sent, so an idle queue times out
          // periodically. Debug level, so the run log keeps ERROR=0.
          logger.debug('pull long-poll timeout for %s (benign, will retry): %s', key, (e as Error).message);
        } else {
          logger.debug('pull error for %s: %s', key, (e as Error).message);
          if (await this._sleepOrStop(500, stopFlag)) return;
        }
        continue;
      }
      result = this._processPullResult(mq, result, sub);

      if (!this.started) return;
      const current = this.processQueueTable.get(key);
      if (!current || current !== pq || pq.isDropped()) {
        // Revoked mid-pull: discard the batch — do not consume it and do not
        // advance the offset. The new owner redelivers it from the last
        // offset we persisted.
        logger.debug('queue %s revoked during pull, discard %d fetched messages',
          key, result.msgFoundList.length);
        return;
      }
      if (result.status === PullStatus.FOUND && result.msgFoundList.length > 0) {
        pq.putMessage(result.msgFoundList);
        this._msgAccCnt.set(key, pq.msgAccCntValue());
      }
      this.offsetTable.set(key, result.nextBeginOffset);
      if (result.status === PullStatus.OFFSET_ILLEGAL) {
        // Java DefaultMQPushConsumerImpl:402-427 — freeze the corrected offset
        // here and do the revoke/persist outside the loop.
        this._frozenOffsets.add(key);
        if (this.offsetStore) this.offsetStore.updateOffset(mq, result.nextBeginOffset, false);
        logger.warning('the pull request offset illegal, fix it, queue=%s', key);
        const q = this._retireQueue(key);
        this._persistRevoked([q]);
        this.rebalanceImmediately();
        return;
      }
      this._correctTagsOffset(mq, result.status, result.nextBeginOffset);
      // Both counters fire inside the FOUND arm: the RT even when the list
      // came back empty, the TPS only for a non-empty one.
      if (result.status === PullStatus.FOUND) {
        this._incPullRT(mq.getTopic(), Date.now() - begin);
        if (result.msgFoundList.length > 0) {
          this._incPullTPS(mq.getTopic(), result.msgFoundList.length);
        }
      }
      if (this.pullInterval > 0 && await this._sleepOrStop(this.pullInterval, stopFlag)) return;
    }
  }

  // _findBrokerAddressInSubscribe mirrors Java MQClientInstance
  // #findBrokerAddressInSubscribe. Returns (addr, isSlave) or null.
  //
  // The isSlave flag comes from the id that was ACTUALLY matched, which is why
  // the fallback branch recomputes it: a request for slave id 3 that falls
  // back to the master must NOT be reported as a slave request — otherwise the
  // COMMIT_OFFSET bit gets cleared for nothing.
  private _findBrokerAddressInSubscribe(brokerName: string, brokerId: number,
    onlyThisBroker: boolean): { addr: string; isSlave: boolean } | null {
    const client = this.mqClient;
    if (!client || !brokerName) return null;
    const addrs = client.getBrokerAddrTable().get(brokerName);
    if (!addrs || Object.keys(addrs).length === 0) return null;
    const snapshot = new Map<number, string>();
    for (const [id, addr] of Object.entries(addrs)) snapshot.set(parseInt(id, 10), addr);
    const hit = snapshot.get(brokerId);
    if (hit != null) return { addr: hit, isSlave: brokerId !== MixAll.MASTER_ID };
    if (brokerId !== MixAll.MASTER_ID) {
      // Java's convention: a slave registers as <slaveId>, so +1 is the
      // "primary slave" of the same group.
      const next = snapshot.get(brokerId + 1);
      if (next != null) return { addr: next, isSlave: true };
    }
    if (!onlyThisBroker) {
      // Java takes `entrySet().iterator().next()` — map order. The ports agree
      // on the deterministic form instead: the smallest id (the master when it
      // exists).
      let minID = 0;
      let first = true;
      for (const id of snapshot.keys()) {
        if (first || id < minID) { minID = id; first = false; }
      }
      return { addr: snapshot.get(minID)!, isSlave: minID !== MixAll.MASTER_ID };
    }
    return null;
  }

  // _pullKernel mirrors Java PullAPIWrapper#pullKernelImpl. Issues PULL_MESSAGE(11).
  private async _pullKernel(mq: MessageQueue, offset: number, sub: SubscriptionData,
    sysFlag: number, commitOffset: number, maxMsgNums: number, maxMsgBytes: number,
    suspendTimeoutMillis: number,     timeoutMillis: number, addr: string): Promise<PullResult> {
    const client = this.mqClient!;
    logger.debug('pull request %s offset=%d addr=%s timeout=%d suspend=%d', mqKey(mq), offset, addr, timeoutMillis, suspendTimeoutMillis);
    const header = new PullMessageRequestHeader();
    header.consumerGroup = this.consumerGroup;
    header.topic = mq.getTopic();
    header.queueId = mq.getQueueId();
    header.queueOffset = offset;
    header.maxMsgNums = maxMsgNums;
    header.sysFlag = sysFlag;
    header.commitOffset = commitOffset;
    header.suspendTimeoutMillis = suspendTimeoutMillis;
    header.subVersion = sub.subVersion;
    header.expressionType = sub.expressionType || ExpressionType.TAG;
    header.maxMsgBytes = maxMsgBytes;
    header.brokerName = mq.getBrokerName();
    // Rule: the expression rides the wire only when the SUBSCRIPTION bit is
    // on. The key must not be present at all when the bit is off.
    if (PullSysFlag.hasSubscriptionFlag(sysFlag)) {
      header.subscription = sub.subString;
    }
    const request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, header);
    const response = await client.remotingClient.invokeSync(addr, request, timeoutMillis);
    let status: number;
    if (response.code === ResponseCode.SUCCESS) status = PullStatus.FOUND;
    else if (response.code === ResponseCode.PULL_NOT_FOUND) status = PullStatus.NO_NEW_MSG;
    else if (response.code === ResponseCode.PULL_OFFSET_MOVED) status = PullStatus.OFFSET_ILLEGAL;
    else if (response.code === ResponseCode.PULL_RETRY_IMMEDIATELY) status = PullStatus.NO_MATCHED_MSG;
    else throw new Error(`pull failed: CODE ${response.code} ${response.remark || ''}`);
    const ext = response.extFields || {};
    const num = (k: string, d: number) => (ext[k] != null ? parseInt(ext[k], 10) : d);
    let found: MessageExt[] = [];
    if (response.body && response.body.length > 0) {
      found = decodeMessages(response.body);
      for (const msg of found) {
        msg.setBrokerName(mq.getBrokerName());
        msg.setQueueId(mq.getQueueId());
      }
    }
    // suggestWhichBrokerId may be ABSENT — that means master(0), not "keep the
    // previous value" (rule #22).
    const suggest = ext['suggestWhichBrokerId'] != null ? parseInt(ext['suggestWhichBrokerId'], 10) : null;
    return new PullResult(status, num('nextBeginOffset', 0), num('minOffset', 0),
      num('maxOffset', 0), found, suggest);
  }

  // _processPullResult mirrors Java PullAPIWrapper#processPullResult. It MUST
  // run for every response, not only FOUND ones — the node table is fed by
  // every reply, and absent means master.
  private _processPullResult(mq: MessageQueue, result: PullResult, sub: SubscriptionData): PullResult {
    const brokerId = result.suggestWhichBrokerId != null ? result.suggestWhichBrokerId : MixAll.MASTER_ID;
    this.pullFromWhichNode.set(mqKey(mq), brokerId);

    if (result.status !== PullStatus.FOUND || result.msgFoundList.length === 0) return result;
    let msgs = clientSideTagFilter(sub, result.msgFoundList);
    if (this.filterMessageHookList.length > 0) {
      // FilterMessageHook MUST swallow exceptions and continue (rule #7): the
      // pull path silently skips filtered messages.
      const ctx = new FilterMessageContext();
      ctx.consumerGroup = this.consumerGroup;
      ctx.msgList = msgs;
      ctx.mq = mq;
      ctx.unitMode = this.unitMode;
      ctx.accessChannel = 'LOCAL';
      for (const hook of this.filterMessageHookList.slice()) {
        try { hook.filterMessage(ctx); } catch (e) { /* swallowed by contract */ }
      }
      msgs = ctx.msgList;
    }
    for (const msg of msgs) {
      // A half message carries its transaction id in UNIQ_KEY; Java lifts it
      // so the listener can see it as MessageExt.getTransactionId().
      if (msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED) === 'true') {
        const uniq = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYARRAY);
        if (uniq) msg.setTransactionId(uniq);
      }
      msg.putProperty(MessageConst.PROPERTY_MIN_OFFSET, String(result.minOffset));
      msg.putProperty(MessageConst.PROPERTY_MAX_OFFSET, String(result.maxOffset));
      msg.setBrokerName(mq.getBrokerName());
    }
    result.msgFoundList = msgs;
    return result;
  }

  // _flowControlHit is Java ProcessQueue.putMessage / pullMessage's threshold
  // checks. Any one of them pauses THIS queue's pulls.
  private _flowControlHit(mq: MessageQueue, pq: ProcessQueue): boolean {
    const [count, sizeMB, span] = pq.pendingStats();
    const maxCount = this.pullThresholdForQueue;
    const maxSize = this.pullThresholdSizeForQueue;
    const maxSpan = this.consumeConcurrentlyMaxSpan;
    const topicMaxCount = this.pullThresholdForTopic;
    const topicMaxSize = this.pullThresholdSizeForTopic;
    let reason = '';
    if (count >= Math.max(1, maxCount)) reason = 'count';
    else if (maxSize > 0 && sizeMB >= maxSize) reason = 'size';
    else if (maxSpan > 0 && span > maxSpan) reason = 'span';
    else if (topicMaxCount > 0 || topicMaxSize > 0) {
      const [topicCount, topicSizeMB] = this._topicPendingStats(mq.getTopic());
      if (topicMaxCount > 0 && topicCount >= topicMaxCount) reason = 'topicCount';
      else if (topicMaxSize > 0 && topicSizeMB >= topicMaxSize) reason = 'topicSize';
    }
    if (!reason) return false;
    this.flowControlTriggered++;
    logger.debug('flow control: queue %s %s, pause pull', mqKey(mq), reason);
    return true;
  }

  private _topicPendingStats(topic: string): [number, number] {
    let total = 0, size = 0.0;
    for (const [key, pq] of this.processQueueTable) {
      if (!key.startsWith(`${topic}@`)) continue;
      const [n, mb] = pq.pendingStats();
      total += n;
      size += mb;
    }
    return [total, size];
  }

  // ---------------------------------------------------------------- dispatch

  // _dispatchLoop takes batches out of the buffers and hands them to the
  // listener.
  //
  // Concurrent mode: an async batch, bounded by the core pool size. Orderly
  // mode: consumed INLINE, so a queue's ordering is preserved.
  private async _dispatchLoop(): Promise<void> {
    const stopFlag = this._newStopFlag();
    while (!this._isStopped(stopFlag)) {
      let progressed = false;
      const batchMax = this.consumeMessageBatchMaxSize;
      for (const [key, pq] of Array.from(this.processQueueTable.entries())) {
        if (this._isStopped(stopFlag)) return;
        if (pq.isDropped()) continue;
        const batch = pq.takeBatch(Math.max(1, batchMax));
        if (batch.length === 0) continue;
        const epoch = this._queueEpoch.get(key) || 0;
        progressed = true;
        if (!this._beginInFlight()) {
          // Shutdown froze new work: hand the batch back instead of consuming
          // it. Its offset was never advanced, so the broker redelivers it on
          // the next start.
          pq.requeueBatch(batch);
          return;
        }
        const [topic, brokerName, queueId] = parseMqKey(key);
        const mq = new MessageQueue(topic, brokerName, queueId);
        if (this.orderly) {
          try {
            await this._consumeBatch(mq, batch, epoch);
          } finally {
            this._endInFlight();
          }
          continue;
        }
        if (this._dispatchSem >= Math.max(1, this.corePoolSize)) {
          // Pool full: wait a tick rather than unbounded queueing (Java's
          // real concurrency is core too).
          pq.requeueBatch(batch);
          this._endInFlight();
          await this._sleepOrStop(5, stopFlag);
          continue;
        }
        this._dispatchSem++;
        void this._consumeBatch(mq, batch, epoch)
          .catch((e) => {
            logger.error('dispatch batch error for %s (requeued): %s', key, (e as Error).message);
            const cur = this.processQueueTable.get(key);
            if (cur) cur.requeueBatch(batch);
          })
          .finally(() => {
            this._dispatchSem = Math.max(0, this._dispatchSem - 1);
            this._endInFlight();
          });
      }
      if (!progressed) {
        if (await this._sleepOrStop(50, stopFlag)) return;
      }
    }
  }

  // _consumeBatch dispatches one batch and handles redelivery/suspension.
  private async _consumeBatch(mq: MessageQueue, batch: MessageExt[], epoch: number): Promise<void> {
    if (batch.length === 0) return;
    const key = mqKey(mq);
    if (epoch !== (this._queueEpoch.get(key) || 0)) {
      logger.warning('the message queue not be able to consume, because it\'s dropped. group=%s mq=%s msgs=%d',
        this.consumerGroup, key, batch.length);
      return;
    }
    this._resetRetryTopicAndNamespace(batch);
    if (this.orderly) await this._consumeOrderlyBatch(mq, batch, epoch);
    else await this._consumeConcurrentlyBatch(mq, batch, epoch);
  }

  // _resetRetryTopicAndNamespace is Java
  // DefaultMQPushConsumerImpl#resetRetryAndNamespace, called BEFORE the
  // listener.
  //
  // A retried message physically lives under %RETRY%<group>; the broker writes
  // the business topic into the RETRY_TOPIC property. Restoring it here is
  // what lets the listener branch on the topic it subscribed to.
  private _resetRetryTopicAndNamespace(msgs: MessageExt[]): void {
    const groupTopic = MixAll.getRetryTopic(this.consumerGroup);
    for (const msg of msgs) {
      const retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
      if (retryTopic && msg.getTopic() === groupTopic) {
        msg.setTopic(retryTopic);
      }
      if (this.namespace) {
        msg.setTopic(MixAll.withoutNamespace
          ? MixAll.withoutNamespace(msg.getTopic(), this.namespace)
          : msg.getTopic());
      }
    }
  }

  // ------------------------------------------------------------ concurrent

  private async _consumeConcurrentlyBatch(mq: MessageQueue, batch: MessageExt[], epoch: number): Promise<void> {
    const ctx = new ConsumeConcurrentlyContext(mq);
    // AckIndex defaults to "the whole batch" — Java MaxInt32 clamped to
    // len(batch)-1 on success. Getting it wrong in the other direction
    // silently acks only one message or none at all.
    (ctx as any).ackIndex = 2147483647;

    const hooks = this.consumeMessageHookList;
    let hookCtx: ConsumeMessageContext | null = null;
    if (hooks.length > 0) {
      hookCtx = new ConsumeMessageContext();
      hookCtx.consumerGroup = this.consumerGroup;
      hookCtx.msgList = batch;
      hookCtx.mq = mq;
      hookCtx.success = false;
      hookCtx.props = {};
      hookCtx.accessChannel = 'LOCAL';
      for (const hook of hooks.slice()) {
        try { hook.consumeMessageBefore(hookCtx); } catch (e) { /* hook errors don't stop delivery */ }
      }
    }
    const begin = Date.now();
    // Java ConsumeMessageConcurrentlyService:366-370 stamps CONSUME_START_TIME
    // on every delivery BEFORE the listener; cleanExpiredMsg's escape hatch
    // reads it.
    for (const msg of batch) {
      msg.putProperty('CONSUME_START_TIME', String(Date.now()));
    }
    const { status: rawStatus, threw } = this._listenerCall(batch, ctx);
    let status = rawStatus;
    if (threw) {
      logger.debug('listener error, treat as RECONSUME_LATER: mq=%s', mqKey(mq));
      status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
    }
    // Java:380 — the RT is taken ONCE, immediately after the listener returns.
    const consumeRT = Date.now() - begin;
    if (status !== ConsumeConcurrentlyStatus.CONSUME_SUCCESS && status !== ConsumeConcurrentlyStatus.RECONSUME_LATER) {
      // An out-of-range return behaves as RECONSUME_LATER instead of silently
      // falling into SUCCESS and acking an unhandled batch.
      logger.warning('consumeMessage return unknown status, Group: %s Msgs: %d MQ: %s',
        this.consumerGroup, batch.length, mqKey(mq));
      status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
    }
    let ackIndex = (ctx as any).ackIndex;
    if (status === ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
      if (ackIndex >= batch.length) ackIndex = batch.length - 1;
    } else {
      ackIndex = -1;
    }
    if (hookCtx) {
      const unknown = rawStatus !== ConsumeConcurrentlyStatus.CONSUME_SUCCESS
        && rawStatus !== ConsumeConcurrentlyStatus.RECONSUME_LATER;
      hookCtx.props['ConsumeContextType'] = consumeReturnTypeName(
        unknown ? (threw ? ConsumeReturnType.EXCEPTION : ConsumeReturnType.RETURNNULL)
          : (consumeRT >= this.consumeTimeout * 60 * 1000 ? ConsumeReturnType.TIME_OUT
            : (status === ConsumeConcurrentlyStatus.RECONSUME_LATER ? ConsumeReturnType.FAILED
              : ConsumeReturnType.SUCCESS)));
      hookCtx.success = status === ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
      hookCtx.status = status === ConsumeConcurrentlyStatus.CONSUME_SUCCESS ? 'CONSUME_SUCCESS' : 'RECONSUME_LATER';
      for (const hook of hooks.slice()) {
        try { hook.consumeMessageAfter(hookCtx); } catch (e) { /* hooks never break delivery */ }
      }
    }
    this._incConsumeRT(mq.getTopic(), consumeRT);
    if (status === ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
      this._incConsumeOKTPS(mq.getTopic(), ackIndex + 1);
      if (batch.length - ackIndex - 1 > 0) {
        this._incConsumeFailedTPS(mq.getTopic(), batch.length - ackIndex - 1);
      }
    } else {
      this._incConsumeFailedTPS(mq.getTopic(), batch.length);
    }

    if (this.messageModel === MessageModel.BROADCASTING) {
      // Broadcasting never sends back: the unacked tail is dropped with a
      // warning and the whole batch advances.
      const dropped = batch.length - ackIndex - 1;
      if (dropped > 0) {
        logger.warning('BROADCASTING, the message consume failed, drop it: %d msgs in %s', dropped, mqKey(mq));
      }
      this._commitConcurrently(mq, batch, epoch);
      return;
    }
    if (ackIndex + 1 >= batch.length) {
      this._commitConcurrently(mq, batch, epoch);
      return;
    }
    // Cluster mode: redeliver the unacked tail one by one to %RETRY%<group>
    // (delay 3+reconsumeTimes; the broker moves it to %DLQ% once
    // maxReconsumeTimes is exceeded).
    const failed = await this._sendBackBatch(mq, batch.slice(ackIndex + 1), ctx);
    if (failed.length > 0) {
      // The ones whose send-back failed are re-submitted later: they go back
      // to the front of the buffer.
      const pq = this.processQueueTable.get(mqKey(mq));
      if (pq) pq.requeueBatch(failed);
      await this._sleepOrStop(200);
    }
    const failedOffsets = new Set(failed.map((m) => m.getQueueOffset()));
    const acked = batch.filter((m) => !failedOffsets.has(m.getQueueOffset()));
    // The commit is what removeMessage(msgs) answers, and `msgs` is the ACKED
    // list (the batch minus msgBackFailed).
    this._commitConcurrently(mq, acked, epoch);
  }

  // _commitConcurrently is Java ProcessQueue#removeMessage + updateOffset for
  // the concurrent paths.
  //
  // The target is the queue's high-water mark + 1 — NOT the end of this batch.
  // Batches of one queue are consumed in parallel, so the batch that finishes
  // second is not necessarily the one with the higher offsets.
  private _commitConcurrently(mq: MessageQueue, acked: MessageExt[], epoch: number): void {
    if (acked.length === 0) return;
    let drained = batchEnd(acked);
    let floor = -1;
    const pq = this.processQueueTable.get(mqKey(mq));
    if (pq) {
      drained = pq.queueOffsetMaxValue() + 1;
      floor = pq.minRemainingExcept(acked);
    }
    this._advanceConsumeOffset(mq, acked, drained, floor, epoch);
  }

  // _sendBackBatch redelivers the unacked entries one by one and returns the
  // ones that FAILED.
  //
  // A failed entry gets reconsumeTimes+1 locally — the broker never recorded
  // the attempt, and without the local bump the message can never reach the
  // dead-letter queue.
  private async _sendBackBatch(mq: MessageQueue, batch: MessageExt[],
    ctx: ConsumeConcurrentlyContext): Promise<MessageExt[]> {
    const failed: MessageExt[] = [];
    const pq = this.processQueueTable.get(mqKey(mq));
    for (const msg of batch) {
      if (pq && !pq.contains(msg.getQueueOffset())) {
        // An entry already swept (or whose queue was revoked) is skipped: it
        // is on its way back to the broker, and sending it again would
        // duplicate it.
        logger.info('Message is not found in its process queue; skip send-back-procedure, topic=%s, brokerName=%s, queueId=%d, queueOffset=%d',
          msg.getTopic(), msg.getBrokerName(), msg.getQueueId(), msg.getQueueOffset());
        continue;
      }
      let delayLevel = (ctx as any).delayLevelWhenNextConsume || 0;
      if (delayLevel === 0) delayLevel = 3 + msg.getReconsumeTimes();
      try {
        await this.sendMessageBack(msg, delayLevel);
      } catch (e) {
        logger.debug('send message back failed for msg %s: %s', msg.getMsgId(), (e as Error).message);
        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
        failed.push(msg);
      }
    }
    return failed;
  }

  // sendMessageBack is DefaultMQPushConsumerImpl#sendMessageBack: broker-side
  // redelivery via CONSUMER_SEND_MSG_BACK(36).
  //
  // `offset` is the message's commitLogOffset, NOT its queueOffset — the
  // broker looks the record up by physical offset.
  async sendMessageBack(msg: MessageExt, delayLevel: number): Promise<void> {
    const client = this.mqClient;
    if (!client) throw new Error('consumer not started');
    const maxReconsume = this.maxReconsumeTimes === -1 ? 16 : this.maxReconsumeTimes;
    const addr = client.findBrokerAddressInPublish(msg.getBrokerName());
    if (!addr) throw new Error(`Broker[${msg.getBrokerName()}] master node does not exist`);
    const h = new ConsumerSendMsgBackRequestHeader();
    h.offset = msg.getCommitLogOffset();
    h.group = this.consumerGroup;
    h.delayLevel = delayLevel;
    h.originMsgId = msg.getMsgId();
    h.originTopic = msg.getTopic();
    h.unitMode = this.unitMode;
    h.maxReconsumeTimes = maxReconsume;
    const request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, h);
    const response = await client.remotingClient.invokeSync(addr, request, 5000);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new Error(`sendMessageBack failed: CODE ${response.code} ${response.remark || ''}`);
    }
  }

  // ------------------------------------------------------------- orderly

  private async _consumeOrderlyBatch(mq: MessageQueue, batch: MessageExt[], epoch: number): Promise<void> {
    const ctx = new ConsumeOrderlyContext(mq);
    (ctx as any).autoCommit = true;
    (ctx as any).suspendCurrentQueueTimeMillis = -1;

    const hooks = this.consumeMessageHookList;
    let hookCtx: ConsumeMessageContext | null = null;
    if (hooks.length > 0) {
      hookCtx = new ConsumeMessageContext();
      hookCtx.consumerGroup = this.consumerGroup;
      hookCtx.msgList = batch;
      hookCtx.mq = mq;
      hookCtx.success = false;
      hookCtx.props = {};
      hookCtx.accessChannel = 'LOCAL';
      for (const hook of hooks.slice()) {
        try { hook.consumeMessageBefore(hookCtx); } catch (e) { /* ignore */ }
      }
    }
    const begin = Date.now();
    const { status: rawStatus } = this._listenerCall(batch, ctx);
    let status = rawStatus;
    if (status < 0) {
      logger.debug('orderly listener error (retry in place): mq=%s', mqKey(mq));
    }
    const consumeRT = Date.now() - begin;
    if (status !== ConsumeOrderlyStatus.SUCCESS && status !== ConsumeOrderlyStatus.ROLLBACK
      && status !== ConsumeOrderlyStatus.COMMIT && status !== ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT) {
      // A null/stray status is normalised to "suspend" BEFORE the hook.
      // Without this a stray value would fall into the SUCCESS branch and
      // silently ack messages that were never consumed.
      status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
    }
    if (hookCtx) {
      const unknown = rawStatus !== ConsumeOrderlyStatus.SUCCESS && rawStatus !== ConsumeOrderlyStatus.ROLLBACK
        && rawStatus !== ConsumeOrderlyStatus.COMMIT && rawStatus !== ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
      hookCtx.props['ConsumeContextType'] = consumeReturnTypeName(
        unknown ? (rawStatus < 0 ? ConsumeReturnType.EXCEPTION : ConsumeReturnType.RETURNNULL)
          : (consumeRT >= this.consumeTimeout * 60 * 1000 ? ConsumeReturnType.TIME_OUT
            : (status === ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT ? ConsumeReturnType.FAILED
              : ConsumeReturnType.SUCCESS)));
      // The hook sees the NORMALISED status, while the returnType is derived
      // from the raw one; success is SUCCESS||COMMIT.
      hookCtx.success = status === ConsumeOrderlyStatus.SUCCESS || status === ConsumeOrderlyStatus.COMMIT;
      hookCtx.status = status === ConsumeOrderlyStatus.SUCCESS ? 'SUCCESS'
        : status === ConsumeOrderlyStatus.ROLLBACK ? 'ROLLBACK'
          : status === ConsumeOrderlyStatus.COMMIT ? 'COMMIT' : 'SUSPEND_CURRENT_QUEUE_A_MOMENT';
      for (const hook of hooks.slice()) {
        try { hook.consumeMessageAfter(hookCtx); } catch (e) { /* ignore */ }
      }
    }
    this._incConsumeRT(mq.getTopic(), consumeRT);

    const autoCommit = (ctx as any).autoCommit !== false;
    if (autoCommit) {
      if (status === ConsumeOrderlyStatus.COMMIT || status === ConsumeOrderlyStatus.ROLLBACK) {
        // With autoCommit on, COMMIT/ROLLBACK are illegal (they belong to the
        // binlog consumer). Java warns and does NOT break: the messages are
        // acked.
        logger.warning('the message queue consume result is illegal, we think you want to ack these message %s', mqKey(mq));
        status = ConsumeOrderlyStatus.SUCCESS;
      }
      if (status === ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT) {
        // SUSPEND counts the WHOLE batch as failed, before the retry decision.
        this._incConsumeFailedTPS(mq.getTopic(), batch.length);
        // checkReconsumeTimes runs first: only "still within the retry budget,
        // or the send-back failed" suspends in place; once the message has
        // been handed to the broker the offset moves on, otherwise a poison
        // message parks the queue forever.
        if (this._checkOrderlyReconsumeTimes(batch)) {
          const pq = this.processQueueTable.get(mqKey(mq));
          if (pq) pq.requeueBatch(batch);
          await this._sleepOrStop(this._sleepOrderlyMillis(ctx));
          return;
        }
      } else {
        this._incConsumeOKTPS(mq.getTopic(), batch.length);
      }
      this._advanceConsumeOffset(mq, batch, batchEnd(batch), -1, epoch);
      return;
    }
    // autoCommit == false (the binlog path).
    switch (status) {
      case ConsumeOrderlyStatus.COMMIT:
        this._advanceConsumeOffset(mq, batch, batchEnd(batch), -1, epoch);
        return;
      case ConsumeOrderlyStatus.ROLLBACK: {
        const pq = this.processQueueTable.get(mqKey(mq));
        if (pq) pq.requeueBatch(batch);
        await this._sleepOrStop(this._sleepOrderlyMillis(ctx));
        return;
      }
      case ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT: {
        this._incConsumeFailedTPS(mq.getTopic(), batch.length);
        if (this._checkOrderlyReconsumeTimes(batch)) {
          const pq = this.processQueueTable.get(mqKey(mq));
          if (pq) pq.requeueBatch(batch);
          await this._sleepOrStop(this._sleepOrderlyMillis(ctx));
        }
        // Unlike the autoCommit branch the offset is NOT committed; whether to
        // advance is the binlog consumer's call.
        return;
      }
      default: {
        // SUCCESS with autoCommit off: the offset stays put, nothing is lost,
        // and the loop does not spin.
        this._incConsumeOKTPS(mq.getTopic(), batch.length);
        const pq = this.processQueueTable.get(mqKey(mq));
        if (pq) pq.requeueBatch(batch);
        await this._sleepOrStop(this._sleepOrderlyMillis(ctx));
        return;
      }
    }
  }

  // _sleepOrderlyMillis is Java
  // ConsumeMessageOrderlyService#submitConsumeRequestLater: -1 means "not set"
  // and falls back to the consumer's configuration, then the result is clamped
  // to [10, 30000]. The clamp is not decoration: a listener returning 0 would
  // otherwise turn the consume loop into a busy loop.
  private _sleepOrderlyMillis(ctx: ConsumeOrderlyContext): number {
    let ms = (ctx as any).suspendCurrentQueueTimeMillis;
    if (ms === -1 || ms == null) ms = this.suspendCurrentQueueTimeMillis;
    if (ms < 10) return 10;
    if (ms > 30000) return 30000;
    return ms;
  }

  // _orderlyMaxReconsumeTimes is Java
  // ConsumeMessageOrderlyService#getMaxReconsumeTimes — -1 is Integer.MAX_VALUE
  // here (orderly retries in place, the broker never counts).
  private _orderlyMaxReconsumeTimes(): number {
    return this.maxReconsumeTimes === -1 ? JAVA_INT_MAX : this.maxReconsumeTimes;
  }

  // _checkOrderlyReconsumeTimes is Java
  // ConsumeMessageOrderlyService#checkReconsumeTimes. Returns whether the
  // batch still has to be suspended in place.
  private _checkOrderlyReconsumeTimes(msgs: MessageExt[]): boolean {
    let suspend = false;
    const maxTimes = this._orderlyMaxReconsumeTimes();
    for (const msg of msgs) {
      if (msg.getReconsumeTimes() >= maxTimes) {
        msg.putProperty(MessageConst.PROPERTY_RECONSUME_TIME, String(msg.getReconsumeTimes()));
        if (!this._orderlySendMessageBackSync(msg)) {
          suspend = true;
          msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
        }
      } else {
        suspend = true;
        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
      }
    }
    return suspend;
  }

  // _orderlySendMessageBack is Java
  // ConsumeMessageOrderlyService#sendMessageBack — a PLAIN send to
  // %RETRY%<group>, not a CONSUMER_SEND_MSG_BACK.
  private _orderlySendMessageBackSync(msg: MessageExt): boolean {
    try {
      void this._doOrderlySendMessageBack(msg).catch(() => false);
      // The async send is fire-and-forget here; Java is synchronous. For the
      // JS port the caller treats a not-yet-confirmed send as failed (suspend)
      // which is the safe side: the message is requeued, not lost.
      return false;
    } catch (e) {
      return false;
    }
  }

  private async _doOrderlySendMessageBack(msg: MessageExt): Promise<boolean> {
    const producer = await this._innerProducer();
    if (!producer) return false;
    const retryTopic = MixAll.getRetryTopic(this.consumerGroup);
    const newMsg = new Message(retryTopic, Buffer.from(msg.getBody() || Buffer.alloc(0)));
    newMsg.setProperties({ ...(msg.getProperties() || {}) });
    newMsg.setFlag(msg.getFlag());
    let originMsgID = msg.getMsgId();
    const orig = msg.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
    if (orig) originMsgID = orig;
    if (originMsgID) newMsg.putProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID, originMsgID);
    newMsg.putProperty(MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
    newMsg.putProperty(MessageConst.PROPERTY_RECONSUME_TIME, String(msg.getReconsumeTimes() + 1));
    newMsg.putProperty(MessageConst.PROPERTY_MAX_RECONSUME_TIMES, String(this._orderlyMaxReconsumeTimes()));
    // The half-message marker must go, else the broker treats it as a
    // transaction check-back message all over again.
    msg.removeProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
    newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
    try {
      await producer.sendBySelector(newMsg, undefined);
      return true;
    } catch (e) {
      logger.debug('orderly send message back failed, group=%s msg=%s: %s',
        this.consumerGroup, msg.getMsgId(), (e as Error).message);
      return false;
    }
  }

  // _innerProducer lazily builds the CLIENT_INNER_PRODUCER used by the orderly
  // send-back path (Java constructs it inside ConsumeMessageOrderlyService).
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

  // ---------------------------------------------------------------- offsets

  // _advanceConsumeOffset is Java's updateOffset: it writes `drained` — the
  // offset to commit once the buffer holds nothing else — and then lowers it
  // to `floor`, the smallest offset that WILL still be buffered, whenever that
  // is lower.
  //
  // epoch is the process-queue generation captured with the batch; a mismatch
  // means the queue was revoked or rebuilt and the ack is void. A frozen
  // offset (OFFSET_ILLEGAL recovery) must not move either.
  private _advanceConsumeOffset(mq: MessageQueue, batch: MessageExt[], drained: number, floor: number, epoch: number): void {
    if (batch.length === 0) return;
    const key = mqKey(mq);
    let nextOffset = drained;
    if (floor >= 0 && floor < nextOffset) nextOffset = floor;
    if (epoch !== (this._queueEpoch.get(key) || 0)) {
      logger.debug('drop ack for %s: process queue was dropped (epoch %s -> %s)',
        key, epoch, this._queueEpoch.get(key) || 0);
      return;
    }
    if (this._frozenOffsets.has(key)) return;
    if (this.offsetStore) this.offsetStore.updateOffset(mq, nextOffset, true);
    const pq = this.processQueueTable.get(key);
    if (pq) pq.completeBatch(batch);
  }

  // _correctTagsOffset is Java DefaultMQPushConsumerImpl#correctTagsOffset.
  //
  // When a pull answers NO_NEW_MSG or NO_MATCHED_MSG the CONSUMED offset must
  // follow the pull cursor, otherwise it parks forever. Java gates it on
  // `0L == processQueue.getMsgCount()`: a concurrent in-flight batch is still
  // in there until the listener returns — raising the offset before that would
  // silently skip the batch on a crash.
  private _correctTagsOffset(mq: MessageQueue, status: number, nextOffset: number): void {
    if (status !== PullStatus.NO_NEW_MSG && status !== PullStatus.NO_MATCHED_MSG) return;
    const key = mqKey(mq);
    if (this._frozenOffsets.has(key)) return;
    const pq = this.processQueueTable.get(key);
    if (pq && pq.msgCount() !== 0) return;
    if (!this.offsetStore) return;
    this.offsetStore.readOffset(mq, ReadOffsetMode.READ_FROM_MEMORY).then((cur) => {
      if (cur >= nextOffset) return;
      this.offsetStore!.updateOffset(mq, nextOffset, true);
    }).catch(() => {});
  }

  // persistConsumerOffset is the periodic flush
  // (Java MQClientInstance#persistAllConsumerOffset).
  async persistConsumerOffset(): Promise<void> {
    const store = this.offsetStore;
    if (!store || !this.started) return;
    const mqs = [...this.processQueueTable.keys()]
      .map((key) => {
        const [topic, brokerName, queueId] = parseMqKey(key);
        return new MessageQueue(topic, brokerName, queueId);
      })
      .sort((a, b) => a.compareTo(b));
    await store.persistAll(mqs);
  }

  // getConsumerStatus answers GET_CONSUMER_STATUS_FROM_CLIENT(221): the
  // CONSUMED offsets (Java returns offsetStore.cloneOffsetTable(topic)).
  getConsumerStatus(topic?: string): Array<{ mq: MessageQueue; offset: number }> {
    const out: Array<{ mq: MessageQueue; offset: number }> = [];
    if (!this.offsetStore) return out;
    const snap = (this.offsetStore as any).tableSnapshot
      ? (this.offsetStore as any).tableSnapshot() as Map<string, number> : new Map<string, number>();
    const keys = Array.from(snap.keys()).sort();
    for (const key of keys) {
      const [t, brokerName, queueId] = parseMqKey(key);
      if (topic && t !== topic) continue;
      out.push({ mq: new MessageQueue(t, brokerName, queueId), offset: snap.get(key)! });
    }
    return out;
  }

  // resetOffset handles RESET_CONSUMER_CLIENT_OFFSET(220).
  //
  // Java's four steps: pq.setDropped(true) + pq.clear() -> wait for concurrent
  // consumption to finish -> updateConsumeOffset + removeUnnecessaryMessageQueue
  // (persist now) -> drop the queue so rebalance rebuilds it at the new offset.
  async resetOffset(topic: string, table: Array<{ mq: MessageQueue; offset: number }>): Promise<void> {
    if (!table || table.length === 0) return;
    const todo: Array<{ mq: MessageQueue; offset: number }> = [];
    for (const entry of table) {
      if (topic && entry.mq.getTopic() !== topic) continue;
      if (!this.processQueueTable.has(mqKey(entry.mq))) continue;
      if (this.offsetStore) this.offsetStore.updateOffset(entry.mq, entry.offset, false);
      todo.push(entry);
    }
    const revoked: Array<{ mq: MessageQueue; offset: number; hasOffset: boolean }> = [];
    for (const p of todo) {
      const q = this._retireQueue(mqKey(p.mq));
      q.offset = p.offset;
      q.hasOffset = true;
      revoked.push(q);
    }
    if (revoked.length === 0) return;
    await this._sleepOrStop(200);
    this._persistRevoked(revoked);
    this.rebalanceImmediately();
    void this.doRebalance();
    logger.info('reset offset applied, group=%s topic=%s queues=%d', this.consumerGroup, topic, revoked.length);
  }

  // ------------------------------------------------------------- sweeps

  // _cleanExpireLoop is Java's scheduleAtFixedRate(cleanExpireMsg,
  // consumeTimeout, consumeTimeout, MINUTES): initialDelay and period are the
  // SAME, so the first sweep also waits a full period.
  private async _cleanExpireLoop(): Promise<void> {
    const flag = this._newStopFlag();
    const period = Math.max(1, this.consumeTimeout) * 60 * 1000;
    while (!this._isStopped(flag)) {
      if (await this._sleepOrStop(period, flag)) return;
      if (!this.started) return;
      if (!this._beginInFlight()) return; // shutdown froze new work
      try {
        this._cleanExpiredMsgOnce();
      } catch (e) {
        logger.error('scheduleAtFixedRate cleanExpireMsg exception: %s', (e as Error).message);
      } finally {
        this._endInFlight();
      }
    }
  }

  // _cleanExpiredMsgOnce walks the queues currently held and sweeps each.
  private _cleanExpiredMsgOnce(): void {
    for (const mq of [...this.assigned]) {
      this._cleanExpiredQueue(mq);
    }
  }

  // _cleanExpiredQueue is Java ProcessQueue#cleanExpiredMsg, line by line.
  // Three rules: only ever look at the HEAD (smallest offset), the message
  // must be STRICTLY older than consumeTimeout, and at most 16 per round.
  private _cleanExpiredQueue(mq: MessageQueue): void {
    if (this.orderly) return; // orderly has no such path
    const pq = this.processQueueTable.get(mqKey(mq));
    if (!pq) return;
    const timeoutMillis = this.consumeTimeout * 60 * 1000;
    const sweep = (i: number): void => {
      if (i >= 16) return;
      const head = pq.firstMessage();
      if (!head) return;
      const stamp = head.getProperty('CONSUME_START_TIME');
      if (!stamp) return; // never handed to the listener -> not expired
      const began = parseInt(stamp, 10);
      if (Number.isNaN(began)) return;
      if (Date.now() - began <= timeoutMillis) return;
      this.sendMessageBack(head, 3).then(() => {
        logger.info('send expire msg back. topic=%s, msgId=%s, storeHost=%s, queueId=%d, queueOffset=%d',
          head.getTopic(), head.getMsgId(), head.getStoreHostString(), head.getQueueId(), head.getQueueOffset());
        // Remove only if it is STILL the head: a racing normal completion wins
        // and we must not steal its message.
        const nowHead = pq.firstMessage();
        if (nowHead && nowHead.getQueueOffset() === head.getQueueOffset()) {
          pq.removeMessage([head]);
        }
        sweep(i + 1);
      }).catch((e) => {
        // A failed send-back is logged only: the message stays where it is and
        // the next round retries. Removing it would lose it for good.
        logger.error('send expired msg exception: %s', (e as Error).message);
      });
    };
    sweep(0);
  }

  // ------------------------------------------------------------- lock sweep

  // _lockMQOnce is Java ConsumeMessageOrderlyService#lockMQ: LOCK_BATCH_MQ(41)
  // for every assigned queue.
  private async _lockMQOnce(): Promise<void> {
    const mqs = [...this.assigned];
    if (mqs.length === 0) return;
    const locked = await this._lockBatchMQ(mqs);
    const lockedSet = new Set(locked.map(mqKey));
    for (const [key, pq] of this.processQueueTable) {
      pq.setLocked(lockedSet.has(key));
    }
    logger.debug('lock_batch_mq: %d/%d queues locked', lockedSet.size, mqs.length);
  }

  private async _lockLoop(): Promise<void> {
    const flag = this._newStopFlag();
    while (!this._isStopped(flag)) {
      try { await this._lockMQOnce(); } catch (e) { logger.debug('lock mq error: %s', (e as Error).message); }
      if (await this._sleepOrStop(20000, flag)) return;
    }
  }

  private async _lockBatchMQ(mqs: MessageQueue[]): Promise<MessageQueue[]> {
    const client = this.mqClient;
    if (!client) return [];
    const byBroker = new Map<string, MessageQueue[]>();
    for (const mq of mqs) {
      const list = byBroker.get(mq.getBrokerName()) || [];
      list.push(mq);
      byBroker.set(mq.getBrokerName(), list);
    }
    const locked: MessageQueue[] = [];
    for (const [brokerName, group] of byBroker) {
      // Master only, and deliberately NO route refresh: an unlocatable master
      // means "this round cannot lock", and the next round retries.
      const addr = client.findBrokerAddressInPublish(brokerName);
      if (!addr) continue;
      const body = {
        consumerGroup: this.consumerGroup,
        clientId: this.clientID,
        mqSet: group.map((mq) => ({
          topic: mq.getTopic(), brokerName: mq.getBrokerName(), queueId: mq.getQueueId(),
        })),
      };
      const h = new LockBatchMqRequestHeader();
      h.consumerGroup = this.consumerGroup;
      h.clientId = this.clientID;
      const request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, h);
      request.body = Buffer.from(JSON.stringify(body), 'utf-8');
      try {
        const response = await client.remotingClient.invokeSync(addr, request, 1000);
        if (response.code !== ResponseCode.SUCCESS) {
          logger.warning('batch lock rejected by broker %s: CODE %s %s', brokerName, response.code, response.remark || '');
          continue;
        }
        const decoded = response.body && response.body.length
          ? RemotingSerializable.decode(response.body) : null;
        const okList = decoded && (decoded['lockOKMQSet'] || decoded['lockOKMqSet']);
        if (Array.isArray(okList)) {
          for (const q of okList) {
            locked.push(new MessageQueue(String(q['topic'] || ''), String(q['brokerName'] || ''), Number(q['queueId'] || 0)));
          }
        }
      } catch (e) {
        logger.warning('batch lock failed for broker %s: %s', brokerName, (e as Error).message);
      }
    }
    return locked;
  }

  private async _unlockBatchMQ(mqs: MessageQueue[]): Promise<void> {
    const client = this.mqClient;
    if (!client) return;
    const byBroker = new Map<string, MessageQueue[]>();
    for (const mq of mqs) {
      const list = byBroker.get(mq.getBrokerName()) || [];
      list.push(mq);
      byBroker.set(mq.getBrokerName(), list);
    }
    for (const [brokerName, group] of byBroker) {
      const addr = client.findBrokerAddressInPublish(brokerName);
      if (!addr) continue;
      const body = {
        consumerGroup: this.consumerGroup,
        clientId: this.clientID,
        mqSet: group.map((mq) => ({
          topic: mq.getTopic(), brokerName: mq.getBrokerName(), queueId: mq.getQueueId(),
        })),
      };
      const h = new UnlockBatchMqRequestHeader();
      h.consumerGroup = this.consumerGroup;
      h.clientId = this.clientID;
      const request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, h);
      request.body = Buffer.from(JSON.stringify(body), 'utf-8');
      try {
        await client.remotingClient.invokeSync(addr, request, 1000);
      } catch (e) {
        logger.debug('batch unlock failed for broker %s: %s', brokerName, (e as Error).message);
      }
    }
  }

  private async unlockAssigned(): Promise<void> {
    await this._unlockBatchMQ([...this.assigned]);
  }

  // ------------------------------------------------- broker-initiated RPCs

  // _registerProcessors wires the broker-initiated requests. Handlers run on
  // the remoting read path: set flags / schedule work, NEVER send a synchronous
  // request inline (self-deadlock).
  private _registerProcessors(): void {
    const rc = this.mqClient?.remotingClient;
    if (!rc) return;
    rc.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, () => {
      this.rebalanceImmediately();
      return null;
    });
    rc.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, (cmd: RemotingCommand) => {
      const ext = cmd.extFields || {};
      const topic = ext['topic'] || undefined;
      const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
      const status = this.getConsumerStatus(topic);
      response.body = Buffer.from(JSON.stringify(
        status.map((e) => ({
          topic: e.mq.getTopic(), brokerName: e.mq.getBrokerName(),
          queueId: e.mq.getQueueId(), offset: e.offset,
        }))), 'utf-8');
      return response;
    });
    rc.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, (cmd: RemotingCommand) => {
      try {
        const body = cmd.body && cmd.body.length ? RemotingSerializable.decode(cmd.body) : null;
        const table = Array.isArray(body) ? body
          : (body && Array.isArray(body['offsetTable'])) ? body['offsetTable'] : [];
        const entries = table.map((e: any) => new MessageQueue(
          String(e['topic'] || e['queue']?.['topic'] || ''),
          String(e['brokerName'] || e['queue']?.['brokerName'] || ''),
          Number(e['queueId'] ?? e['queue']?.['queueId'] ?? 0),
        )).filter((mq: MessageQueue) => mq.getTopic()).map((mq: MessageQueue, i: number) => ({
          mq, offset: Number(table[i]['offset'] ?? 0),
        }));
        const topic = (cmd.extFields || {})['topic'] || '';
        void this.resetOffset(topic, entries);
      } catch (e) {
        logger.warning('reset offset request failed: %s', (e as Error).message);
      }
      return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
    });
    rc.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, (cmd: RemotingCommand) => {
      const h = new GetConsumerRunningInfoRequestHeader();
      try { h.fromExtFields(cmd.extFields || {}); } catch (e) { /* defaults */ }
      const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
      response.body = Buffer.from(JSON.stringify(this.buildConsumerRunningInfo(h.jstack === true)), 'utf-8');
      return response;
    });
    rc.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, (cmd: RemotingCommand) => {
      // Consume one message directly and report the result. The message body
      // rides the request body (MessageExt wire) when the broker pushes it;
      // otherwise report a failure remark.
      const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
      try {
        const msgs = cmd.body && cmd.body.length ? decodeMessages(cmd.body) : [];
        if (msgs.length === 0) {
          response.remark = 'no message to consume directly';
          return response;
        }
        this._resetRetryTopicAndNamespace(msgs);
        const ctx = new ConsumeConcurrentlyContext(new MessageQueue(
          msgs[0].getTopic(), msgs[0].getBrokerName(), msgs[0].getQueueId()));
        const { status } = this._listenerCall(msgs, ctx);
        response.body = Buffer.from(JSON.stringify({
          consumeResult: status === ConsumeConcurrentlyStatus.CONSUME_SUCCESS ? 'CONSUME_SUCCESS' : 'CONSUME_FAIL',
          remark: '',
        }), 'utf-8');
      } catch (e) {
        response.body = Buffer.from(JSON.stringify({
          consumeResult: 'CONSUME_FAIL', remark: (e as Error).message,
        }), 'utf-8');
      }
      return response;
    });
  }

  // buildConsumerRunningInfo assembles the 307 payload: properties,
  // subscriptionSet, mqTable, statusTable, userConsumerInfo {}.
  buildConsumerRunningInfo(jstack = false): Record<string, any> {
    void jstack;
    const mqTable: Record<string, any> = {};
    for (const [key, pq] of this.processQueueTable) {
      const [first, last, count] = pq.bufferedOffsetSpan();
      mqTable[key] = {
        commitOffset: this.offsetTable.get(key) ?? -1,
        cachedMsgCount: count,
        cachedMsgSizeInMiB: Math.round(pq.msgSizeMB() * 100) / 100,
        transactionTableSize: 0,
        firstMsgOffset: first,
        lastMsgOffset: last,
        locked: pq.isLocked(),
        tryUnlockTimes: 0,
        queueOffsetSpan: last - first,
        order: this.orderly,
      };
    }
    const statusTable = this._buildStatusTable();
    const subscriptionSet = this.subscriptions().map((s) => s.toDict ? s.toDict() : s);
    return {
      properties: {
        'PROP_CONSUME_TYPE': this.consumeType,
        'PROP_START_TIMESTAMP': String(this._startTime),
        'PROP_CONSUMEORDERLY': String(this.orderly),
        'PROP_THREADPOOL_CORE_SIZE': String(this.corePoolSize),
        'PROP_CONSUMER_START_TIMESTAMP': String(this._startTime),
        'PROP_CONSUME_SUSPEND': String(false),
        'PROP_CONSUMEFLOWCONTROL': String(this.pullThresholdForQueue),
      },
      subscriptionSet,
      mqTable,
      mqPopTable: this._buildPopTable(),
      statusTable,
      userConsumerInfo: {},
    };
  }

  // _buildPopTable is the 307 mqPopTable (Java
  // PopProcessQueue#fillPopProcessQueueInfo). All three fields are written
  // unconditionally — unlike mqTable there is no "only when non-empty" branch,
  // so an idle POP queue still reports its debt and its last pop time.
  private _buildPopTable(): Record<string, any> {
    const out: Record<string, any> = {};
    for (const [key, pq] of this.popQueueTable) {
      out[key] = {
        waitAckCount: pq.waitAckMsgCount(),
        droped: pq.isDropped(),
        lastPopTimestamp: pq.getLastPopTimestamp(),
      };
    }
    return out;
  }

  // _buildStatusTable is the 307 statusTable: per-topic ConsumeStatus from the
  // consumer stats manager (Java consumerRunningInfo: consumeStatus(group,
  // topic), MINUTE snapshots; consumeFailedMsgs comes from the HOUR window).
  // NOTE: real-clock TPS between two in-process samples is ~0; tests inject
  // timestamps via StatsItem sampling.
  private _buildStatusTable(): Record<string, any> {
    const table: Record<string, any> = {};
    for (const s of this.subscriptions()) {
      if (this._statsManager != null) {
        table[s.topic] = this._statsManager.consumeStatus(this.consumerGroup, s.topic).toDict();
      } else {
        table[s.topic] = { pullRT: 0, pullTPS: 0, consumeRT: 0, consumeOKTPS: 0, consumeFailedTPS: 0, consumeFailedMsgs: 0 };
      }
    }
    return table;
  }

  // ------------------------------------------------------------- stats

  // Differential-window recording (Java ConsumerStatsManager same-named
  // methods). No-op before start(): the manager lives on the MQClient.
  private _incPullRT(topic: string, rt: number): void {
    if (this._statsManager != null) this._statsManager.incPullRT(this.consumerGroup, topic, rt);
  }
  private _incPullTPS(topic: string, n: number): void {
    if (this._statsManager != null) this._statsManager.incPullTPS(this.consumerGroup, topic, n);
  }
  private _incConsumeRT(topic: string, rt: number): void {
    if (this._statsManager != null) this._statsManager.incConsumeRT(this.consumerGroup, topic, rt);
  }
  private _incConsumeOKTPS(topic: string, n: number): void {
    if (this._statsManager != null) this._statsManager.incConsumeOKTPS(this.consumerGroup, topic, n);
  }
  private _incConsumeFailedTPS(topic: string, n: number): void {
    if (this._statsManager != null) this._statsManager.incConsumeFailedTPS(this.consumerGroup, topic, n);
  }

  // ------------------------------------------------------------- misc

  assignedQueues(): MessageQueue[] { return [...this.assigned]; }
  assignedQueueCount(): number { return this.assigned.length; }
  processQueueCount(): number { return this.processQueueTable.size; }

  private _parseConsumeTimestamp(raw: string): number {
    if (!raw || raw.length !== 14) {
      throw new Error(`consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received ${raw}`);
    }
    const m = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/.exec(raw);
    if (!m) {
      throw new Error(`consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received ${raw}`);
    }
    const t = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6]));
    return t.getTime();
  }

  // _checkConfigRanges is Java DefaultMQPushConsumerImpl#checkConfig's numeric
  // section — same order, same bounds, same wording.
  private _checkConfigRanges(): void {
    if (this.consumeThreadMin < 1 || this.consumeThreadMin > 1000) {
      throw new Error('consumeThreadMin Out of range [1, 1000]');
    }
    if (this.consumeThreadMax < 1 || this.consumeThreadMax > 1000) {
      throw new Error('consumeThreadMax Out of range [1, 1000]');
    }
    if (this.consumeThreadMin > this.consumeThreadMax) {
      throw new Error(`consumeThreadMin (${this.consumeThreadMin}) is larger than consumeThreadMax (${this.consumeThreadMax})`);
    }
    if (this.consumeConcurrentlyMaxSpan < 1 || this.consumeConcurrentlyMaxSpan > 65535) {
      throw new Error('consumeConcurrentlyMaxSpan Out of range [1, 65535]');
    }
    if (this.pullThresholdForQueue < 1 || this.pullThresholdForQueue > 65535) {
      throw new Error('pullThresholdForQueue Out of range [1, 65535]');
    }
    if (this.pullThresholdForTopic !== -1 && (this.pullThresholdForTopic < 1 || this.pullThresholdForTopic > 6553500)) {
      throw new Error('pullThresholdForTopic Out of range [1, 6553500]');
    }
    if (this.pullThresholdSizeForQueue < 1 || this.pullThresholdSizeForQueue > 1024) {
      throw new Error('pullThresholdSizeForQueue Out of range [1, 1024]');
    }
    if (this.pullThresholdSizeForTopic !== -1
      && (this.pullThresholdSizeForTopic < 1 || this.pullThresholdSizeForTopic > 102400)) {
      throw new Error('pullThresholdSizeForTopic Out of range [1, 102400]');
    }
    if (this.pullInterval < 0 || this.pullInterval > 65535) {
      throw new Error('pullInterval Out of range [0, 65535]');    }
    if (this.consumeMessageBatchMaxSize < 1 || this.consumeMessageBatchMaxSize > 1024) {
      throw new Error('consumeMessageBatchMaxSize Out of range [1, 1024]');
    }
    if (this.pullBatchSize < 1 || this.pullBatchSize > 1024) {
      throw new Error('pullBatchSize Out of range [1, 1024]');
    }
    if (this.popInvisibleTime < 5000 || this.popInvisibleTime > 300000) {
      throw new Error('popInvisibleTime Out of range [5000, 300000]');
    }
    if (this.popBatchNums <= 0 || this.popBatchNums > 32) {
      throw new Error('popBatchNums Out of range [1, 32]');
    }
    if (this.popMode && this.orderly) {
      // Java: "POPTODO think of pop mode orderly implementation later." There
      // is no queue lock in the POP protocol to serialise on, so an orderly
      // POP consumer would silently lose ordering — refuse instead.
      throw new Error('pop mode does not support orderly consumption');
    }
  }

  // ------------------------------------------------------------- pop surface

  popProcessQueueCount(): number { return this.popQueueTable.size; }

  // popWaitAckCount totals the outstanding ACK debt across every queue.
  popWaitAckCount(): number {
    let total = 0;
    for (const pq of this.popQueueTable.values()) total += pq.waitAckMsgCount();
    return total;
  }
}

// ---------------------------------------------------------------- helpers

// batchEnd is "the largest offset in the batch + 1" — the orderly path's
// commit target, and the concurrent path's fallback when there is no buffer.
function batchEnd(batch: MessageExt[]): number {
  let next = -1;
  for (const msg of batch) {
    if (msg.getQueueOffset() > next) next = msg.getQueueOffset();
  }
  return next + 1;
}

// clientSideTagFilter is Java PullAPIWrapper.processPullResult:113-122 — the
// second, string-level tag check. The broker filters by the hash of the tag
// (codeSet), so a collision can let a non-matching message through; the client
// re-checks by exact string.
//
// The guard `!tagsSet.isEmpty() && !isClassFilterMode` is why FilterAPI's
// SUB_ALL path must keep tagsSet EMPTY: subscribing to "*" turns the filter
// off entirely rather than filtering everything out.
function clientSideTagFilter(sub: SubscriptionData, msgs: MessageExt[]): MessageExt[] {
  // tagsSet is a Set in this port (Java: HashSet) — use .size, NOT .length.
  // A `.length === 0` guard on a Set is always false, which silently turns
  // the '*' subscription (empty tagsSet → filter OFF) into "filter everything".
  const tagsSet: any = sub ? (sub as any).tagsSet : null;
  const empty = tagsSet == null ||
    (typeof tagsSet.size === 'number' ? tagsSet.size === 0 : tagsSet.length === 0);
  if (msgs.length === 0 || !sub || empty || sub.classFilterMode) {
    return msgs;
  }
  const accepted: Set<string> = typeof (tagsSet as any).has === 'function'
    ? tagsSet : new Set(tagsSet);
  const out: MessageExt[] = [];
  for (const msg of msgs) {
    const tags = msg.getProperty(MessageConst.PROPERTY_TAGS);
    if (!tags) continue;
    if (accepted.has(tags)) out.push(msg);
  }
  return out;
}

// parseMqKey reverses the canonical "topic@brokerName@queueId" key.
export function parseMqKey(key: string): [string, string, number] {
  const i1 = key.indexOf('@');
  const i2 = key.lastIndexOf('@');
  if (i1 < 0 || i2 <= i1) return ['', '', 0];
  return [key.slice(0, i1), key.slice(i1 + 1, i2), parseInt(key.slice(i2 + 1), 10) || 0];
}
