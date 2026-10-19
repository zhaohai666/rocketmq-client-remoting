// -*- coding: utf-8 -*-
// PullConsumerScheduleService -- a scheduled pull facade pairing a
// DefaultMQPullConsumer with a per-topic callback table (PullTaskCallback /
// PullTaskContext), in the "service pulls, callback consumes" shape:
// the service owns the per-queue pull round and hands the PullResult to the
// topic's callback.
//
// Threading model: no thread pool. Each queue task is a self-rescheduling
// setTimeout chain driven by the single event loop, and that chain still
// gives the guarantees a dedicated pool with pullThreadNums workers would
// give:
//   * queue-dimension SERIAL execution — a queue's next round is scheduled only
//     after its previous round (pull + callback) fully settles;
//   * cross-queue concurrency — independent chains interleave on the loop;
//   * every round rides DefaultMQPullConsumer.pullAsync, whose exactly-once
//     setImmediate delivery keeps one pull from ever double-settling a round.
//
// Rescheduling delay precedence (three channels, first hit wins):
//   1. the callback's numeric return value (this round only),
//   2. context.pullMessageLater(ms)          (one-shot, consumed after use),
//   3. context.setPullInterval(ms)           (the resident interval).
// A failed pull or a throwing callback falls back to
// PULL_TASK_EXCEPTION_DELAY_MILLIS, the fixed one-second retry delay.
//
// The consumer facade underneath goes through MQClientManager like every other
// facade (instance reuse + refcounted shutdown) — this service never builds an
// MQClient of its own.
import { DefaultMQPullConsumer } from './pull_consumer.ts';
import type { PullResult } from './consumer_result.ts';
import { MessageQueue } from '../common/message.ts';
import { MessageModel } from '../remoting/heartbeat.ts';
import { mqKey } from './offset_store.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.pull_schedule_service');

export const DEFAULT_PULL_NEXT_DELAY_MILLIS = 200;
export const PULL_TASK_EXCEPTION_DELAY_MILLIS = 1000;
export const DEFAULT_QUEUE_CHECK_INTERVAL_MILLIS = 20000;
// shutdown() waits at most this long for in-flight rounds to settle; a bounded
// drain keeps a stalled round from blocking shutdown forever.
export const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MILLIS = 5000;

// PullTaskCallback -- consumes one queue's PullResult. The optional numeric
// return value (or resolved promise value) is rescheduling channel #1.
export type PullTaskCallback = (
  mq: MessageQueue,
  pullResult: PullResult,
  context: PullTaskContext,
) => number | void | Promise<number | void>;

export interface PullTaskOptions {
  // Tag expression or '*' (default). SQL92 goes through the consumer's
  // subscribe(topic, MessageSelector.SQL92(...)) instead — broker-side filter,
  // covering the same subscription scope as a tag expression.
  subExpression?: string;
  // First-round start offset: a number, a provider function, or null (default)
  // = broker committed offset, falling back to the queue tail (LAST semantics).
  offset?: number | (() => number) | null;
  // Resident rescheduling interval (channel #3). Default 200 ms, the
  // idle-round backoff between two completed rounds.
  nextDelayMillis?: number;
  maxMsgNums?: number;
  timeoutMillis?: number | null;
}

export class PullTaskContext {
  pullConsumer: DefaultMQPullConsumer;
  messageQueue: MessageQueue;
  subscriptionExpression: string;
  maxMsgNums: number;
  timeoutMillis: number | null;
  // Cursor for the next round; null until the first round resolves it from the
  // registered source (see resolveOffset).
  nextOffset: number | null = null;
  // Observation: completed pull rounds and the delay actually used last round —
  // tests and troubleshooting read these instead of wall-clock timing.
  roundCount = 0;
  lastNextDelayMillis: number | null = null;

  private _pullNextDelayMillis: number;
  private _offsetSource: number | (() => number) | null;
  private _oneShotDelayMillis: number | null = null;

  constructor(pullConsumer: DefaultMQPullConsumer, messageQueue: MessageQueue,
    subscriptionExpression = '*',
    offset: number | (() => number) | null = null,
    nextDelayMillis = DEFAULT_PULL_NEXT_DELAY_MILLIS,
    maxMsgNums = 32,
    timeoutMillis: number | null = null) {
    this.pullConsumer = pullConsumer;
    this.messageQueue = messageQueue;
    this.subscriptionExpression = subscriptionExpression;
    this._offsetSource = offset;
    this._pullNextDelayMillis = Math.max(0, Math.floor(nextDelayMillis));
    this.maxMsgNums = maxMsgNums;
    this.timeoutMillis = timeoutMillis;
  }

  // ---------------- offset ----------------

  // Starting offset of the next pull round: reuse the cursor when present; the
  // first round evaluates the registered source — explicit value/provider, or
  // the broker committed offset, or the queue tail when nothing is committed.
  async resolveOffset(): Promise<number> {
    if (this.nextOffset != null) return this.nextOffset;
    const src = this._offsetSource;
    if (src != null) {
      const v = typeof src === 'function' ? Number(src()) : Number(src);
      this.nextOffset = Number.isFinite(v) ? Math.max(0, Math.floor(v)) : 0;
      return this.nextOffset;
    }
    let committed = -1;
    try {
      committed = await this.pullConsumer.fetchConsumeOffset(this.messageQueue, true);
    } catch (e) {
      logger.debug('fetchConsumeOffset failed for %s: %s',
        mqKey(this.messageQueue), (e as Error).message);
    }
    if (committed >= 0) {
      this.nextOffset = committed;
      return this.nextOffset;
    }
    try {
      const max = await this.pullConsumer.maxOffset(this.messageQueue);
      this.nextOffset = max >= 0 ? max : 0;
    } catch (e) {
      logger.debug('maxOffset failed for %s: %s',
        mqKey(this.messageQueue), (e as Error).message);
      this.nextOffset = 0;
    }
    return this.nextOffset;
  }

  // seek manually overrides the next round's start (a callback deciding where
  // to re-read from, given the result).
  seek(offset: number): void {
    if (!Number.isFinite(offset) || offset < 0) {
      throw new Error(`offset must be >= 0, got ${offset}`);
    }
    this.nextOffset = Math.floor(offset);
  }

  // Advance the cursor after a successful pull (on OFFSET_ILLEGAL the broker's
  // suggested nextBeginOffset IS the correction).
  recordPullResult(result: PullResult): void {
    this.nextOffset = result.nextBeginOffset;
    this.roundCount += 1;
  }

  // ---------------- next-round delay ----------------

  getPullNextDelayTimeMillis(): number { return this._pullNextDelayMillis; }

  // Channel #3: the resident interval for every following round.
  setPullInterval(millis: number): void {
    this._pullNextDelayMillis = Math.max(0, Math.floor(millis));
  }

  // An alternate spelling of setPullInterval — both write the same
  // resident interval field, so a callback may use either name.
  setPullNextDelayTimeMillis(millis: number): void {
    this.setPullInterval(millis);
  }

  // Channel #2: defer the NEXT round by millis (one-shot; the round after that
  // falls back to the resident interval).
  pullMessageLater(millis: number): void {
    this._oneShotDelayMillis = Math.max(0, Math.floor(millis));
  }

  // Settle this round's delay: explicit callback return value > one-shot
  // deferral (consumed) > resident interval.
  consumeNextDelayMillis(returnedDelay: number | null): number {
    let delay: number;
    if (returnedDelay != null && Number.isFinite(returnedDelay)) {
      delay = Math.floor(returnedDelay);
    } else if (this._oneShotDelayMillis != null) {
      delay = this._oneShotDelayMillis;
      this._oneShotDelayMillis = null;
    } else {
      delay = this._pullNextDelayMillis;
    }
    delay = Math.max(0, delay);
    this.lastNextDelayMillis = delay;
    return delay;
  }
}

// One topic's registration entry: the callback plus the task template params.
class TaskSpec {
  callback: PullTaskCallback;
  subscriptionExpression: string;
  offset: number | (() => number) | null;
  nextDelayMillis: number;
  maxMsgNums: number;
  timeoutMillis: number | null;

  constructor(callback: PullTaskCallback, opts: PullTaskOptions) {
    this.callback = callback;
    this.subscriptionExpression = opts.subExpression != null ? opts.subExpression : '*';
    this.offset = opts.offset != null ? opts.offset : null;
    this.nextDelayMillis = opts.nextDelayMillis != null
      ? opts.nextDelayMillis : DEFAULT_PULL_NEXT_DELAY_MILLIS;
    this.maxMsgNums = opts.maxMsgNums != null ? opts.maxMsgNums : 32;
    this.timeoutMillis = opts.timeoutMillis != null ? opts.timeoutMillis : null;
  }

  newContext(pullConsumer: DefaultMQPullConsumer, mq: MessageQueue): PullTaskContext {
    return new PullTaskContext(pullConsumer, mq, this.subscriptionExpression,
      this.offset, this.nextDelayMillis, this.maxMsgNums, this.timeoutMillis);
  }
}

// One queue's scheduling state: the context plus cancel/in-flight gates and the
// pending timer (queue-dimension serialization).
class PullTask {
  context: PullTaskContext;
  cancelled = false;
  inFlight = false;
  timer: NodeJS.Timeout | null = null;
  constructor(context: PullTaskContext) { this.context = context; }
}

export class PullConsumerScheduleService {
  // Queue-view recheck period: this facade has no rebalance thread that is
  // notified when a topic's queue set changes,
  // so the view is reconciled on a timer instead.
  queueCheckIntervalMillis = DEFAULT_QUEUE_CHECK_INTERVAL_MILLIS;
  shutdownDrainTimeoutMillis = DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MILLIS;

  private _pullConsumer: DefaultMQPullConsumer;
  private _ownConsumer: boolean;
  private _callbackTable = new Map<string, TaskSpec>();
  private _taskTable = new Map<string, PullTask>(); // mqKey -> task
  private _queueCheckTimer: NodeJS.Timeout | null = null;
  private _started = false;

  constructor(consumerGroup: string | null = null,
    pullConsumer: DefaultMQPullConsumer | null = null) {
    if (pullConsumer == null) {
      if (consumerGroup == null || !String(consumerGroup).trim()) {
        throw new Error('consumerGroup is required when pullConsumer is not given');
      }
      this._pullConsumer = new DefaultMQPullConsumer(consumerGroup);
      this._ownConsumer = true;
    } else {
      this._pullConsumer = pullConsumer;
      this._ownConsumer = false;
    }
  }

  // ---------------- public API ----------------

  getPullConsumer(): DefaultMQPullConsumer { return this._pullConsumer; }
  isStarted(): boolean { return this._started; }
  taskCount(): number { return this._taskTable.size; }
  callbackTopics(): string[] { return [...this._callbackTable.keys()].sort(); }

  // Register (or replace) a topic's pull callback. When already started the
  // topic's queues are expanded into tasks immediately.
  registerPullTask(topic: string, callback: PullTaskCallback,
    opts: PullTaskOptions = {}): void {
    if (topic == null || !String(topic).trim()) throw new Error('topic is empty');
    if (typeof callback !== 'function') throw new Error('callback is required');
    this._callbackTable.set(topic, new TaskSpec(callback, opts));
    if (this._started) void this._putTasksForTopic(topic);
  }

  unregisterPullTask(topic: string): void {
    this._callbackTable.delete(topic);
    if (!this._started) return;
    for (const [key, task] of [...this._taskTable]) {
      if (task.context.messageQueue.getTopic() === topic) {
        task.cancelled = true;
        if (task.timer != null) { clearTimeout(task.timer); task.timer = null; }
        this._taskTable.delete(key);
      }
    }
  }

  async start(): Promise<void> {
    if (this._started) return;
    // The facade's own start() goes through MQClientManager (instance reuse +
    // refcount); nothing here may bypass it.
    await this._pullConsumer.start();
    this._started = true;
    for (const topic of [...this._callbackTable.keys()]) {
      await this._putTasksForTopic(topic);
    }
    // Deliberately NOT unref'd: a started service must keep the process alive
    // until shutdown() clears it, so this timer holds the loop open.
    this._queueCheckTimer = setInterval(() => { void this._recheckQueues(); },
      this.queueCheckIntervalMillis);
    logger.info('PullConsumerScheduleService start OK, group=%s topics=%s',
      this._pullConsumer.consumerGroup, this.callbackTopics().join(','));
  }

  // Shutdown DRAINS: pending timers are cancelled, but in-flight rounds (pull +
  // callback) are awaited up to shutdownDrainTimeoutMillis before the task
  // table is dropped — a callback mid-flight always sees its round finished.
  async shutdown(): Promise<void> {
    if (!this._started) return;
    this._started = false;
    if (this._queueCheckTimer != null) {
      clearInterval(this._queueCheckTimer);
      this._queueCheckTimer = null;
    }
    for (const task of this._taskTable.values()) {
      task.cancelled = true;
      if (task.timer != null) { clearTimeout(task.timer); task.timer = null; }
    }
    const deadline = Date.now() + this.shutdownDrainTimeoutMillis;
    while (Date.now() < deadline) {
      let inFlight = 0;
      for (const task of this._taskTable.values()) if (task.inFlight) inFlight++;
      if (inFlight === 0) break;
      await new Promise((r) => setTimeout(r, 20));
    }
    this._taskTable.clear();
    if (this._ownConsumer) {
      await this._pullConsumer.shutdown().catch((e) =>
        logger.warning('pull consumer shutdown raised: %s', (e as Error).message));
    }
    logger.info('PullConsumerScheduleService shutdown OK, group=%s',
      this._pullConsumer.consumerGroup);
  }

  // ---------------- queue expansion ----------------

  private async _recheckQueues(): Promise<void> {
    if (!this._started) return;
    for (const topic of [...this._callbackTable.keys()]) {
      await this._putTasksForTopic(topic);
    }
  }

  private async _queuesOfTopic(topic: string): Promise<MessageQueue[]> {
    if (this._pullConsumer.messageModel === MessageModel.BROADCASTING) {
      return this._pullConsumer.fetchSubscribeMessageQueues(topic);
    }
    // CLUSTERING: this instance's share — fetchMessageQueuesInBalance keeps the
    // existing assignment when the route/consumer-list view is unavailable, so
    // a failed query never drops or duplicates queues.
    return this._pullConsumer.fetchMessageQueuesInBalance(topic);
  }

  // Align one topic's queue set with the task table: cancel tasks whose queue
  // left the view, enqueue newly appeared queues with zero delay.
  private async _putTasksForTopic(topic: string): Promise<void> {
    const spec = this._callbackTable.get(topic);
    if (spec == null || !this._started) return;
    let queues: MessageQueue[];
    try {
      queues = await this._queuesOfTopic(topic);
    } catch (e) {
      // View unavailable: KEEP the existing tasks — never cancel everything on
      // a transient route/consumer-list failure.
      logger.debug('queue view unavailable for %s: %s', topic, (e as Error).message);
      return;
    }
    if (!this._started) return;
    const newSet = new Map<string, MessageQueue>();
    for (const mq of queues) {
      if (mq.getTopic() === topic) newSet.set(mqKey(mq), mq);
    }
    for (const [key, task] of [...this._taskTable]) {
      if (task.context.messageQueue.getTopic() !== topic) continue;
      if (!newSet.has(key)) {
        task.cancelled = true;
        if (task.timer != null) { clearTimeout(task.timer); task.timer = null; }
        this._taskTable.delete(key);
      }
    }
    for (const [key, mq] of newSet) {
      if (this._taskTable.has(key)) continue;
      const task = new PullTask(spec.newContext(this._pullConsumer, mq));
      this._taskTable.set(key, task);
      this._schedule(task, 0);
    }
  }

  // ---------------- one queue's round ----------------

  private _schedule(task: PullTask, delayMillis: number): void {
    if (task.cancelled || !this._started) return;
    const delay = Math.max(0, Math.floor(delayMillis));
    // NOT unref'd — see start(): a live service keeps the process alive.
    task.timer = setTimeout(() => { task.timer = null; void this._runTask(task); }, delay);
  }

  // One round: pull exactly once through pullAsync, hand the result to the
  // callback, settle the next delay, reschedule. Serial per queue by
  // construction — the next timer is only armed after this round settles.
  private async _runTask(task: PullTask): Promise<void> {
    const context = task.context;
    const mq = context.messageQueue;
    if (task.cancelled || !this._started) return;
    task.inFlight = true;
    let result: PullResult | null = null;
    let returnedDelay: number | null = null;
    let failed = false;
    try {
      const offset = await context.resolveOffset();
      result = await this._pullOnce(context, offset);
    } catch (e) {
      failed = true;
      logger.error('pull schedule: pull %s failed: %s', mqKey(mq), (e as Error).message);
    }
    if (task.cancelled) {
      task.inFlight = false;
      logger.warning('The pull task is cancelled after doPullTask, %s', mqKey(mq));
      return;
    }
    if (!failed && result != null) {
      context.recordPullResult(result);
      const spec = this._callbackTable.get(mq.getTopic());
      if (spec == null) {
        logger.warning('Pull task callback not exist, topic=%s', mq.getTopic());
        this._taskTable.delete(mqKey(mq));
        task.inFlight = false;
        return;
      }
      try {
        const returned = await spec.callback(mq, result, context);
        returnedDelay = (typeof returned === 'number' && Number.isFinite(returned))
          ? returned : null;
      } catch (e) {
        logger.error('doPullTask exception, mq=%s: %s', mqKey(mq), (e as Error).message);
        returnedDelay = PULL_TASK_EXCEPTION_DELAY_MILLIS;
      }
    } else {
      returnedDelay = PULL_TASK_EXCEPTION_DELAY_MILLIS;
    }
    const delay = context.consumeNextDelayMillis(returnedDelay);
    task.inFlight = false;
    if (task.cancelled || !this._started) {
      logger.warning('The pull task is cancelled after doPullTask, %s', mqKey(mq));
      return;
    }
    if (this._taskTable.get(mqKey(mq)) !== task) return; // replaced by a re-expansion
    this._schedule(task, delay);
  }

  // One wire round through pullAsync — the exactly-once setImmediate contract
  // maps 1:1 onto a promise settle, so success XOR exception fires once.
  private _pullOnce(context: PullTaskContext, offset: number): Promise<PullResult> {
    const consumer = context.pullConsumer;
    const timeout = context.timeoutMillis != null ? context.timeoutMillis : consumer.pullTimeoutMillis;
    return new Promise<PullResult>((resolve, reject) => {
      consumer.pullAsync(context.messageQueue, context.subscriptionExpression,
        offset, context.maxMsgNums, {
          onSuccess: (r) => resolve(r),
          onException: (e) => reject(e),
        }, timeout);
    });
  }
}

export default {
  PullConsumerScheduleService, PullTaskContext,
  DEFAULT_PULL_NEXT_DELAY_MILLIS, PULL_TASK_EXCEPTION_DELAY_MILLIS,
  DEFAULT_QUEUE_CHECK_INTERVAL_MILLIS, DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MILLIS,
};
