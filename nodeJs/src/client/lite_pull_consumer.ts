// DefaultLitePullConsumer — the lite pull consumer
// (Java org.apache.rocketmq.client.consumer.DefaultLitePullConsumer).
//
// Two assignment modes, like Java:
//  - subscribe(topic, expr): the assignment is rebalance-driven (AVG over the
//    group's consumer list).
//  - assign(mqs): fully manual, no broker coordination.
//
// The pull is always a SHORT poll via LITE_PULL_MESSAGE(361) with no inline
// offset commit (the consumer owns its own commit path), and the expression is
// ALWAYS sent (cross-port rule #21: pull / lite pull consumers never drop the
// subscription field).
import { MQClient } from './mq_client.ts';
import { PullAPI } from './pull_api.ts';
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

export class DefaultLitePullConsumer {
  consumerGroup: string;
  nameServerAddr = '';
  messageModel: string = MessageModel.CLUSTERING;
  // wire fields read by MQClient.buildHeartbeatData:
  consumeType: string = ConsumeType.CONSUME_ACTIVELY;
  get subscriptionDataSet(): SubscriptionData[] { return [...this.subscription.values()]; }
  unitMode = false;

  clientID = '';
  mqClient: MQClient | null = null;
  pullAPI: PullAPI | null = null;
  offsetStore: OffsetStore | null = null;

  pullBatchSize = DEFAULT_PULL_BATCH_SIZE;
  pollTimeoutMillis = DEFAULT_POLL_TIMEOUT_MILLIS;
  autoCommit = true;
  autoCommitIntervalMillis = 5000;
  // Java DefaultLitePullConsumer default: CONSUME_FROM_LAST_OFFSET — a queue
  // with NO committed offset starts at the broker's max offset.
  consumeFromWhere: string = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

  setConsumeFromWhere(where: string): this { this.consumeFromWhere = where; return this; }

  private subscription = new Map<string, SubscriptionData>();
  private assignedManual: MessageQueue[] | null = null; // assign() mode
  private assignedAuto: MessageQueue[] = [];            // subscribe() mode
  private paused = new Set<string>();
  private pullCursor = new Map<string, number>();       // mqKey -> next pull offset
  private started = false;
  private _rebalanceTimer: NodeJS.Timeout | null = null;
  private _autoCommitTimer: NodeJS.Timeout | null = null;

  constructor(consumerGroup = 'DEFAULT_CONSUMER') {
    this.consumerGroup = consumerGroup || 'DEFAULT_CONSUMER';
  }

  setNamesrvAddr(addr: string): this { this.nameServerAddr = addr; return this; }
  setMessageModel(model: string): this { this.messageModel = model; return this; }
  setPullBatchSize(n: number): this { this.pullBatchSize = n; return this; }
  setPollTimeoutMillis(ms: number): this { this.pollTimeoutMillis = ms; return this; }
  setAutoCommit(enable: boolean): this { this.autoCommit = enable; return this; }
  setAutoCommitIntervalMillis(ms: number): this { this.autoCommitIntervalMillis = ms; return this; }
  getConsumerGroup(): string { return this.consumerGroup; }
  isStarted(): boolean { return this.started; }

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
    this.clientID = `LITE${process.pid}`;
    const client = new MQClient(this.clientID, this.nameServerAddr);
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
    // Register the group at the broker NOW: the client heartbeat loop's first
    // beat may have run before any route was cached, and waiting for the 30s
    // cadence delays group registration — and therefore the first rebalance —
    // by half a minute. Java sends the heartbeat inside consumer start().
    await client.sendHeartbeatToAllBrokers().catch(() => {});
    this.started = true;
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
    try { await this.commitSync(); } catch (e) { /* best effort */ }
    if (this.mqClient) {
      await this.mqClient.unregisterClientAllBrokers('', this.consumerGroup).catch(() => {});
      this.mqClient.unregisterConsumer(this.consumerGroup);
      this.mqClient.shutdown();
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
          // CONSUME_FROM_LAST_OFFSET (Java default): start at maxOffset.
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

  // Java DefaultLitePullConsumer.assignment(): the currently assigned queues.
  assignment(): MessageQueue[] {
    return [...(this.assignedManual != null ? this.assignedManual : this.assignedAuto)];
  }

  // CONSUME_FROM_LAST_OFFSET initial position: the queue's max offset on its
  // master broker (-1 when the route/broker is unknown — the queue is skipped
  // this round and retried on the next poll).
  private async _maxOffsetFor(mq: MessageQueue): Promise<number> {
    if (!this.mqClient) return -1;
    await this.mqClient.updateTopicRouteInfoFromNameServer(mq.getTopic(), false).catch(() => {});
    const route = this.mqClient.getTopicRouteData(mq.getTopic());
    if (!route) return -1;
    for (const bd of route.brokerDatas || []) {
      const addrs = bd.brokerAddrs || {};
      const addr = addrs['0'] != null ? addrs['0'] : Object.values(addrs)[0];
      if (addr != null && bd.brokerName === mq.getBrokerName()) {
        try { return await this.mqClient.getMaxOffset(addr, mq.getTopic(), mq.getQueueId()); }
        catch (e) { return -1; }
      }
    }
    return -1;
  }

  // Java DefaultLitePullConsumer.committed(mq): the last committed offset of
  // the queue (-1 when never committed).
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
