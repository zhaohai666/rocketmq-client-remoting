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
import { MQClient } from './mq_client.ts';
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
import { MixAll } from '../common/mixAll.ts';
import { FilterAPI, SubscriptionData } from '../common/subscriptionData.ts';
import { PullSysFlag } from '../common/sysflag.ts';
import { RequestCode } from '../remoting/codes.ts';
import { Validators } from '../common/validators.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.lite_pull_consumer');

const DEFAULT_PULL_BATCH_SIZE = 10;
const DEFAULT_POLL_TIMEOUT_MILLIS = 5000;
// topicMetadataCheckIntervalMillis 的默认值：30s 比对一次被监听 topic 的队列集合。
const DEFAULT_TOPIC_METADATA_CHECK_INTERVAL_MILLIS = 30 * 1000;
// 周期下限钳到 1s：0 周期的定时任务会让后台调度直接报错。
const MIN_TOPIC_METADATA_CHECK_INTERVAL_MILLIS = 1000;
// 首轮比对在 start() 之后 10s 才跑，而不是立刻。
const METADATA_CHECK_FIRST_DELAY_MILLIS = 1000 * 10;

// TopicMessageQueueChangeListener：仅当 topic 的队列【集合】发生变化（扩容
// 或缩容）时才回调，并带上新集合；没有变化就不回调。
export interface TopicMessageQueueChangeListener {
  onChanged(topic: string, messageQueues: MessageQueue[]): void | Promise<void>;
}

// 队列集合相等判断："从没见过这个 topic"（没有快照）不等于任何集合——
// fetchMessageQueues 总是返回非空列表，所以刚注册监听器的第一轮必定触发一次。
function isQueueSetEqual(
  oldQueues: MessageQueue[] | undefined, newQueues: MessageQueue[]): boolean {
  if (oldQueues === undefined) return false;
  if (oldQueues.length !== newQueues.length) return false;
  const oldSet = new Set(oldQueues.map(mqKey));
  return newQueues.every((mq) => oldSet.has(mqKey(mq)));
}

export class DefaultLitePullConsumer {
  consumerGroup: string;
  // namespaceV2：服务端侧命名空间；非空时 NamespaceRpcHook 给每个请求打上
  // nsd=true / ns=<值>。
  namespaceV2: string | null = null;
  nameServerAddr = '';
  messageModel: string = MessageModel.CLUSTERING;
  // wire fields read by MQClient.buildHeartbeatData:
  consumeType: string = ConsumeType.CONSUME_ACTIVELY;
  get subscriptionDataSet(): SubscriptionData[] { return [...this.subscription.values()]; }
  unitMode = false;
  // instanceName / unitName / enableStreamRequestType：lite pull 默认打开 stream
  // 标记——clientId 追加 `@STREAM`，且每个请求带 ReqT=0。
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
  // 默认 CONSUME_FROM_LAST_OFFSET：没有已提交位点的队列从 broker 的 maxOffset
  // 起步。
  consumeFromWhere: string = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
  // 后台任务比对每个被监听 topic 的队列集合的周期。
  topicMetadataCheckIntervalMillis = DEFAULT_TOPIC_METADATA_CHECK_INTERVAL_MILLIS;

  setConsumeFromWhere(where: string): this { this.consumeFromWhere = where; return this; }

  private subscription = new Map<string, SubscriptionData>();
  private assignedManual: MessageQueue[] | null = null; // assign() mode
  private assignedAuto: MessageQueue[] = [];            // subscribe() mode
  private paused = new Set<string>();
  private pullCursor = new Map<string, number>();       // mqKey -> next pull offset
  // 每个 topic 的监听器 + 上次见到的队列集合。
  // 键用【裸】topic，与本实现其他 lite pull 状态（subscribe / 路由缓存 /
  // rebalance）一致——namespaceV2 由 NamespaceRpcHook 在线路层生效，不靠给
  // topic 名加前缀。
  private topicChangeListeners = new Map<string, TopicMessageQueueChangeListener>();
  private messageQueuesForTopic = new Map<string, MessageQueue[]>();
  private started = false;
  private _rebalanceTimer: NodeJS.Timeout | null = null;
  private _autoCommitTimer: NodeJS.Timeout | null = null;
  private _metadataTimer: NodeJS.Timeout | null = null;
  private _metadataInitialTimer: NodeJS.Timeout | null = null;

  constructor(consumerGroup = 'DEFAULT_CONSUMER') {
    this.consumerGroup = consumerGroup || 'DEFAULT_CONSUMER';
  }

  setNamesrvAddr(addr: string): this { this.nameServerAddr = addr; return this; }
  // 设置/读取 namespaceV2（每个请求实时读取）。
  setNamespaceV2(ns: string | null): this { this.namespaceV2 = ns; return this; }
  getNamespaceV2(): string | null { return this.namespaceV2; }
  setMessageModel(model: string): this { this.messageModel = model; return this; }
  setInstanceName(name: string): this { this.instanceName = name; return this; }
  setUnitName(name: string): this { this.unitName = name; return this; }
  setPullBatchSize(n: number): this { this.pullBatchSize = n; return this; }
  setPollTimeoutMillis(ms: number): this { this.pollTimeoutMillis = ms; return this; }
  setAutoCommit(enable: boolean): this { this.autoCommit = enable; return this; }
  setAutoCommitIntervalMillis(ms: number): this { this.autoCommitIntervalMillis = ms; return this; }
  getConsumerGroup(): string { return this.consumerGroup; }
  isStarted(): boolean { return this.started; }

  // 设置 topic 元数据比对周期（低于下限或非法值一律钳到下限）。
  setTopicMetadataCheckIntervalMillis(ms: number): this {
    this.topicMetadataCheckIntervalMillis =
      Number.isFinite(ms) && ms >= MIN_TOPIC_METADATA_CHECK_INTERVAL_MILLIS
        ? Math.floor(ms) : MIN_TOPIC_METADATA_CHECK_INTERVAL_MILLIS;
    return this;
  }
  getTopicMetadataCheckIntervalMillis(): number { return this.topicMetadataCheckIntervalMillis; }

  subscribe(topic: string, subExpression = '*'): this {
    if (!topic || !topic.trim()) throw new Error('subscription topic is empty');
    const expr = subExpression || '*';
    this.subscription.set(topic, FilterAPI.buildSubscriptionData(topic, expr));
    this.assignedManual = null;
    return this;
  }

  unsubscribe(topic: string): void {
    this.subscription.delete(topic);
  }

  // assign switches to MANUAL mode: the given queues are polled directly.
  assign(mqs: MessageQueue[]): this {
    this.assignedManual = [...mqs];
    for (const mq of mqs) {
      if (!this.subscription.has(mq.getTopic())) {
        this.subscription.set(mq.getTopic(), FilterAPI.buildSubscriptionData(mq.getTopic(), '*'));
      }
    }
    return this;
  }

  async start(): Promise<void> {
    if (this.started) return;
    Validators.checkGroup(this.consumerGroup);
    if (!this.nameServerAddr) throw new Error('name server address is not set');
    if (this.subscription.size === 0 && !this.assignedManual) {
      throw new Error('subscription is not set, call subscribe() or assign() first');
    }
    // CLUSTERING 会把默认的 instanceName 改写掉，随后 clientId 形如
    // `<ip>@<instanceName>[@<unitName>][@STREAM]`。⚠ 只用 pid 作实例名的话，
    // 同进程内所有 lite pull 消费者共用一个 clientId，allocate() 会把同一片
    // 队列分给两个实例，双方读到完全相同的消息。
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
    // 队列解析完、组还没在 broker 注册之前，先发一次 CHECK_CLIENT_IN_BROKER(46)
    // 预检。
    try {
      await client.checkClientInBroker();
    } catch (e) {
      logger.warning('Start the lite pull consumer %s fail: %s', this.consumerGroup, (e as Error).message);
      // started is still false here, so shutdown() would no-op — unwind the
      // registration this start() already did by hand (registerClient 的平衡
      // 归还：unregisterClient 计数 -1，归零才真 shutdown)。
      client.unregisterConsumer(this.consumerGroup);
      manager.unregisterClient(client.clientId, client);
      throw e;
    }
    // 立即在 broker 注册消费组：客户端心跳循环的第一拍可能在任何路由被缓存之前
    // 就跑完了，再等 30s 的周期会把组注册——进而把首轮 rebalance——拖后半分钟。
    // 所以 start() 内主动补发一次心跳。
    await client.sendHeartbeatToAllBrokers().catch(() => {});
    this.started = true;
    // 元数据比对循环无条件启动，与是否注册了监听器无关。
    this._startMetadataLoop();
    if (this.assignedManual == null) {
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
      // 归还给 manager：计数 -1，归零才真正 shutdown。
      MQClientManager.getInstance().unregisterClient(this.mqClient.clientId, this.mqClient);
    }
    this.started = false;
  }

  // _doRebalance assigns queues only in subscribe() mode.
  private async _doRebalance(): Promise<void> {
    if (this.assignedManual != null || !this.mqClient) return;
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
        const { AllocateMessageQueueAveragely } = await import('./allocate.ts');
        const sorted = [...mqAll].sort((a, b) => a.compareTo(b));
        assigned.push(...new AllocateMessageQueueAveragely().allocate(
          this.consumerGroup, this.clientID, sorted, ids.map(String).sort()));
      } catch (e) {
        logger.debug('lite pull rebalance: no consumer id list for %s: %s', topic, (e as Error).message);
      }
    }
    assigned.sort((a, b) => a.compareTo(b));
    this.assignedAuto = assigned;
  }

  // fetchMessageQueues returns the SUBSCRIBE view (read perm, readQueueNums, NO
  // master filter). An unroutable topic throws: handing back [] would read as
  // "this topic has zero queues", so a nameserver blip would look like a
  // scale-in and the queue-change listener would happily report it as one.
  async fetchMessageQueues(topic: string): Promise<MessageQueue[]> {
    if (!this.mqClient) throw new Error('consumer not started');
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

  // 逐条约束：
  //  * topic 为空 / 监听器缺失 → 报 "Topic or listener is null"；
  //  * 重复注册同一 topic 时 WARN 并覆盖旧监听器；
  //  * 只有 RUNNING 状态的消费者才立刻取快照——否则第一轮会把"本来就在那儿
  //    的集合"当成一次变化上报。
  // 快照是一次路由 RPC，所以这里要 await；start() 之前注册也没问题，比对循环
  // 会在 start() 时开始取快照。
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

  // 对每个被监听的 topic：把路由里的当前队列集合与上次见到的比对；只有真的
  // 变了才触发 onChanged，且只针对该 topic——一个 topic 路由查询失败不能打断
  // 整轮（异常捕获放在每个 topic 内部，只记日志）。
  // 返回被通知的监听器数量（没变化时为 0）。
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

  // 固定速率调度：首轮延迟 10s，之后每 interval 一拍。周期在 start() 时读取
  // 一次，之后不再变；10s 首轮延迟避免刚启动的消费者在 broker 还没登记完
  // 队列时就去应答路由查询。
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
  async poll(timeoutMillis = this.pollTimeoutMillis): Promise<MessageExt[]> {
    if (!this.started || !this.pullAPI) throw new Error('consumer not started');
    const deadline = Date.now() + Math.max(1, timeoutMillis);
    const queues = this._activeQueues();
    if (queues.length === 0) return [];
    const out: MessageExt[] = [];
    let idx = 0;
    while (Date.now() < deadline && out.length < this.pullBatchSize) {
      // Round-robin across queues so no single queue starves the rest.
      const mq = queues[idx % queues.length];
      idx++;
      const key = mqKey(mq);
      const sub = this.subscription.get(mq.getTopic());
      if (!sub) continue;
      let offset = this.pullCursor.get(key);
      if (offset === undefined) {
        const committed = await this.offsetStore!.readOffset(mq, ReadOffsetMode.READ_FROM_MEMORY_THEN_STORE);
        // A %RETRY% topic with no committed offset starts at 0: retried
        // messages must all be retried.
        if (committed >= 0) offset = committed;
        else if (MixAll.isRetryTopic(mq.getTopic())) offset = 0;
        else if (this.consumeFromWhere === ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET) offset = 0;
        else {
          // CONSUME_FROM_LAST_OFFSET（默认）：从 maxOffset 起步。
          offset = await this._maxOffsetFor(mq);
        }
        if (offset < 0) continue; // unknown offset; nothing to pull from
        this.pullCursor.set(key, offset);
      }
      try {
        // LITE_PULL_MESSAGE(361), SHORT poll, expression ALWAYS sent, and the
        // lite-pull bit set in sysFlag.
        const sysFlag = PullSysFlag.buildSysFlag(false, false, true, false, true);
        let result = await this.pullAPI.pullKernelLite(mq, offset, sub, sysFlag,
          this.pullBatchSize - out.length, Math.max(1, deadline - Date.now()));
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
        // their turn within the deadline.
      }
      if (queues.length === 1 && out.length === 0) {
        // Single queue and nothing found: don't hot-loop against the broker.
        await new Promise<void>((r) => setTimeout(r, 200));
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

  // seek moves the pull cursor for one queue (the next poll starts there).
  seek(mq: MessageQueue, offset: number): void {
    if (!this.started) throw new Error('consumer not started');
    if (offset < 0) throw new Error(`offset must be >= 0, got ${offset}`);
    this.pullCursor.set(mqKey(mq), offset);
    if (this.offsetStore) this.offsetStore.updateOffset(mq, offset, false);
  }

  // assignment()：返回当前分配到的队列。
  assignment(): MessageQueue[] {
    return [...(this.assignedManual != null ? this.assignedManual : this.assignedAuto)];
  }

  // 队列所在 **master** broker 上的 min/max 位点（路由或 broker 未知时返回
  // -1——调用方本轮跳过该队列，而不是 seek 到一个假游标）。管控查询只认
  // master 地址。
  private async _boundaryOffset(mq: MessageQueue, kind: 'min' | 'max'): Promise<number> {
    if (!this.mqClient) return -1;
    await this.mqClient.updateTopicRouteInfoFromNameServer(mq.getTopic(), false).catch(() => {});
    const route = this.mqClient.getTopicRouteData(mq.getTopic());
    if (!route) return -1;
    for (const bd of route.brokerDatas || []) {
      const addrs = bd.brokerAddrs || {};
      const addr = addrs['0'] != null ? addrs['0'] : Object.values(addrs)[0];
      if (addr != null && bd.brokerName === mq.getBrokerName()) {
        try {
          return kind === 'min'
            ? await this.mqClient.getMinOffset(addr, mq.getTopic(), mq.getQueueId())
            : await this.mqClient.getMaxOffset(addr, mq.getTopic(), mq.getQueueId());
        } catch (e) { return -1; }
      }
    }
    return -1;
  }

  private _maxOffsetFor(mq: MessageQueue): Promise<number> {
    return this._boundaryOffset(mq, 'max');
  }

  // seekToBegin：先取 minOffset 再 seek；seekToEnd：先取 maxOffset 再 seek。
  // broker 答不出边界值时直接抛错而不是 seek(-1)：负游标会在下一次 poll 时以
  // OFFSET_ILLEGAL 弹回来，等于静默地错。
  async seekToBegin(mq: MessageQueue): Promise<void> {
    if (!this.started) throw new Error('consumer not started');
    const begin = await this._boundaryOffset(mq, 'min');
    if (begin < 0) throw new Error(`seekToBegin: minOffset unavailable for ${mqKey(mq)}`);
    this.seek(mq, begin);
  }

  async seekToEnd(mq: MessageQueue): Promise<void> {
    if (!this.started) throw new Error('consumer not started');
    const end = await this._boundaryOffset(mq, 'max');
    if (end < 0) throw new Error(`seekToEnd: maxOffset unavailable for ${mqKey(mq)}`);
    this.seek(mq, end);
  }

  // committed(mq)：该队列最后一次提交的位点（从未提交过时为 -1）。
  committed(mq: MessageQueue): number {
    if (!this.offsetStore) return -1;
    const snap = (this.offsetStore as any).tableSnapshot?.() as Map<string, number> | undefined;
    const v = snap?.get(mqKey(mq));
    return v !== undefined ? v : -1;
  }

  pause(mqs: MessageQueue[]): void {
    for (const mq of mqs) this.paused.add(mqKey(mq));
  }

  resume(mqs: MessageQueue[]): void {
    for (const mq of mqs) this.paused.delete(mqKey(mq));
  }

  private _activeQueues(): MessageQueue[] {
    const all = this.assignedManual != null ? this.assignedManual : this.assignedAuto;
    return all.filter((mq) => !this.paused.has(mqKey(mq)));
  }
}
