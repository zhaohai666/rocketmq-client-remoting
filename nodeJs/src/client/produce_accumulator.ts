// -*- coding: utf-8 -*-
// Batch accumulator: groups small messages into one batch by aggregation key
// before the real send. On the node side an async daemon replaces the thread.
// The concrete sender is wired in from the producer side.
//
// A message handed to the accumulator does NOT go out on its own: it joins the
// group of its (topic, queue, waitStoreMsgOK, tag) key and the caller waits on
// the future `accumulate` returns. That future settles with THIS message's own
// SendResult, built when the group leaves as one batch — never with the batch
// answer shared across the group.
import { Message } from '../common/message.ts';
import { MessageQueue } from '../common/message.ts';
import { isWaitStoreMsgOK } from '../common/message.ts';
import { MessageBatch } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MessageAccessor } from '../common/message_accessor.ts';
import { MixAll } from '../common/mixAll.ts';
import { SendResult } from './send_result.ts';

export const DEFAULT_TOTAL_HOLD_SIZE = 32 * 1024 * 1024; // 32 MB
export const DEFAULT_HOLD_SIZE = 32 * 1024;             // 32 KB
export const DEFAULT_HOLD_MS = 10;

export class AggregateKey {
  topic: string;
  mq: MessageQueue;
  waitStoreMsgOK: boolean;
  tag: string | null;

  constructor(topic: string, mq: MessageQueue, waitStoreMsgOK: boolean, tag: string | null) {
    this.topic = topic;
    this.mq = mq;
    this.waitStoreMsgOK = waitStoreMsgOK;
    this.tag = tag;
  }

  // Keys are comparable by (topic, queue hashcode, waitStoreMsgOK, tag).
  keyString(): string {
    return `${this.topic}\u0000${this.mq.hashcode()}\u0000${this.waitStoreMsgOK ? 1 : 0}\u0000${this.tag ?? ''}`;
  }

  equals(other: AggregateKey): boolean {
    return this.topic === other.topic &&
      this.mq.equals(other.mq) &&
      this.waitStoreMsgOK === other.waitStoreMsgOK &&
      this.tag === other.tag;
  }
}

// One message that joined the accumulator, plus the future its own caller
// waits on. The batch answer is split back onto every member, so a caller that
// handed over a single message always gets that message's result — not the
// group's.
export class AccumulatedSend {
  readonly message: Message;
  readonly mq: MessageQueue;
  readonly promise: Promise<SendResult>;
  private settleResult: (result: SendResult) => void = () => { /* replaced below */ };
  private settleError: (err: Error) => void = () => { /* replaced below */ };

  constructor(message: Message, mq: MessageQueue) {
    this.message = message;
    this.mq = mq;
    this.promise = new Promise<SendResult>((resolve, reject) => {
      this.settleResult = resolve;
      this.settleError = reject;
    });
  }

  complete(result: SendResult): void {
    this.settleResult(result);
  }

  completeExceptionally(err: Error): void {
    this.settleError(err);
  }
}

export class MessageAccumulation {
  entries: AccumulatedSend[];
  totalSize: number;

  constructor() {
    this.entries = [];
    this.totalSize = 0;
  }

  add(entry: AccumulatedSend, size: number): void {
    this.entries.push(entry);
    this.totalSize += size;
  }

  get messages(): Message[] { return this.entries.map((entry) => entry.message); }
  get count(): number { return this.entries.length; }
  get size(): number { return this.totalSize; }
  isEmpty(): boolean { return this.entries.length === 0; }
  clear(): void { this.entries = []; this.totalSize = 0; }

  // Settle the whole group with the answer its batch got: each member receives
  // its own SendResult built from the shared batch outcome.
  complete(batchResult: SendResult): void {
    const results = splitSendResults(batchResult, this.messages);
    for (let i = 0; i < this.entries.length; i++) this.entries[i].complete(results[i]);
  }

  // The group could not leave (no route, no queue, the broker refused it):
  // every waiting caller learns that instead of hanging.
  fail(err: Error): void {
    for (const entry of this.entries) entry.completeExceptionally(err);
  }
}

export class ProduceAccumulator {
  maxHoldSize: number;
  holdMs: number;
  clientId: string;
  accumulator: Map<string, MessageAccumulation>;
  sender: ((acc: MessageAccumulation) => Promise<void>) | null;
  private timer: ReturnType<typeof setInterval> | null;
  // True between stop() and the next start(). A group opened after a stop has no
  // timer left to drain it, so nothing may hand a caller's message to it.
  private stopped: boolean;

  constructor(clientId: string, maxHoldSize: number = DEFAULT_HOLD_SIZE, holdMs: number = DEFAULT_HOLD_MS) {
    this.clientId = clientId;
    this.maxHoldSize = maxHoldSize;
    this.holdMs = holdMs;
    this.accumulator = new Map();
    this.sender = null;
    this.timer = null;
    this.stopped = false;
  }

  setSender(fn: (acc: MessageAccumulation) => Promise<void>): void {
    this.sender = fn;
  }

  // The two gates are protocol-facing limits (a batch outside them is not going
  // to be accepted downstream), so an out-of-range value is refused here rather
  // than quietly moving traffic to another path.
  setMaxHoldSize(size: number): void {
    if (size < 1 || size > 2 * 1024 * 1024) {
      throw new Error(`batchMaxBytes expect between 1B and 2MB, but get ${size}!`);
    }
    this.maxHoldSize = size;
  }

  setHoldMs(ms: number): void {
    if (ms < 1 || ms > 30 * 1000) {
      throw new Error(`batchMaxDelayMs expect between 1ms and 30s, but get ${ms}!`);
    }
    this.holdMs = ms;
    // The period is captured when the timer is built, so a running daemon is
    // rebuilt for the new period to take effect from the next tick.
    if (this.timer != null) {
      clearInterval(this.timer);
      this.timer = null;
      this.start();
    }
  }

  // Whether a message may ride a batch at all. A packed batch is never packed
  // again, a delayed / timed message has to reach its own schedule, a
  // transaction half message has to stay individually addressable for the
  // broker's check-back, a retry-topic message belongs to one consumer group,
  // and a body over the hold size can never fit inside a group that is flushed
  // at that size — all of those go out on their own.
  isBatchable(msg: Message): boolean {
    if (msg instanceof MessageBatch) return false;
    if (MixAll.isRetryTopic(msg.getTopic())) return false;
    if (MessageAccessor.isTransactionPrepared(msg)) return false;
    // A message already stamped with a producer group is the half-message kind:
    // the broker locates its transactional connection by group, so it cannot
    // ride an envelope addressed by whoever else's group the batch happens to get.
    if (msg.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP) != null) return false;
    const delayLevel = msg.getDelayTimeLevel();
    if (delayLevel != null && parseInt(delayLevel, 10) > 0) return false;
    // Value checks, not presence: a 0 under one of these names asks for no schedule, so the
    // message still belongs in a group.
    for (const raw of [msg.getDelayTimeSec(), msg.getDelayTimeMs(), msg.getDeliverTimeMs()]) {
      if (raw != null && parseInt(raw, 10) > 0) return false;
    }
    return msg.getBody().length <= this.maxHoldSize;
  }

  // Hand one message to the group of its key. `future` settles with this
  // message's own result; `flushNow` is the group that crossed the hold size
  // (removed from the table) and should leave immediately instead of waiting
  // for the hold timer.
  //
  // A stopped accumulator has no timer left, so a group opened now would never
  // drain and its caller would wait forever; refusing here is what lets the
  // caller take the direct send path instead.
  accumulate(msg: Message, mq: MessageQueue): { future: AccumulatedSend; flushNow: MessageAccumulation | null } {
    if (this.stopped) throw new Error('producer accumulator is stopped');
    const tag = msg.getTags();
    const key = new AggregateKey(msg.getTopic(), mq, isWaitStoreMsgOK(msg), tag);
    const k = key.keyString();
    let acc = this.accumulator.get(k);
    if (acc == null) {
      acc = new MessageAccumulation();
      this.accumulator.set(k, acc);
    }
    const future = new AccumulatedSend(msg, mq);
    const size = msg.getBody() ? msg.getBody().length : 0;
    acc.add(future, size);
    if (acc.size >= this.maxHoldSize) {
      this.accumulator.delete(k);
      return { future, flushNow: acc };
    }
    return { future, flushNow: null };
  }

  // Drain all pending accumulations (used when flushing on a timer or shutdown).
  drain(): MessageAccumulation[] {
    const out: MessageAccumulation[] = [];
    for (const acc of this.accumulator.values()) {
      if (!acc.isEmpty()) out.push(acc);
    }
    this.accumulator.clear();
    return out;
  }

  size(): number { return this.accumulator.size; }

  isStopped(): boolean { return this.stopped; }

  start(): void {
    // (Re)starting always clears the stopped mark: a facade may wire a fresh
    // sender onto an accumulator that an earlier shutdown stopped, and the
    // buffer has to accept traffic again.
    this.stopped = false;
    if (this.timer != null) return;
    this.timer = setInterval(() => { void this.flushPending(); }, Math.max(1, this.holdMs));
    // Keep the daemon off the event loop so a settled producer cannot by itself
    // hold the process open.
    const timer = this.timer as any;
    if (typeof timer.unref === 'function') timer.unref();
  }

  // Hand every buffered group to the sender. Each group is settled exactly once:
  // the sender settles it with the batch answer, and only if the sender itself
  // throws (before settling) does the group fail with that error — a stranded
  // group would leave its callers' send promises pending forever.
  async flushPending(): Promise<void> {
    if (this.sender == null) return;
    for (const acc of this.drain()) {
      try {
        await this.sender(acc);
      } catch (e) {
        acc.fail(e instanceof Error ? e : new Error(String(e)));
      }
    }
  }

  // Stopping flushes what is still buffered rather than dropping it: without a
  // sender there is nothing left that could settle these groups, so they fail
  // instead of hanging.
  stop(): void {
    this.stopped = true;
    if (this.timer != null) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.sender == null) {
      for (const acc of this.drain()) acc.fail(new Error('producer accumulator stopped with the batch unsent'));
      return;
    }
    void this.flushPending();
  }
}

const _ACCUMULATORS: Map<string, ProduceAccumulator> = new Map();

export function getOrCreateProduceAccumulator(clientId: string): ProduceAccumulator {
  let acc = _ACCUMULATORS.get(clientId);
  if (acc == null) {
    acc = new ProduceAccumulator(clientId);
    _ACCUMULATORS.set(clientId, acc);
  }
  return acc;
}

// Split one batch SendResult into per-message SendResults: the group shares the
// queue and the store status, every member gets the offsets its own stored
// record carries, and every member keeps its OWN client message id (its
// UNIQ_KEY, stamped before it joined the group).
export function splitSendResults(batchResult: SendResult, msgs: Message[]): SendResult[] {
  // The store answers a group with one entry per member for the two id kinds
  // independently: the message-id field can stay the single envelope id while
  // the physical-offset field already lists one id per member. Each list is
  // matched member by member only when it really has one element per member.
  const idParts = String(batchResult.msgId ?? '').split(',');
  const offsetParts = String(batchResult.offsetMsgId ?? '').split(',');
  const idsSplit = idParts.length === msgs.length && msgs.length > 1;
  const offsetsSplit = offsetParts.length === msgs.length && msgs.length > 1;
  const out: SendResult[] = [];
  for (let i = 0; i < msgs.length; i++) {
    const msg = msgs[i];
    const uniqKey = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    const msgId = idsSplit
      ? idParts[i]
      : (uniqKey != null && String(uniqKey).length > 0 ? String(uniqKey) : batchResult.msgId);
    // The group occupies consecutive queue offsets in its queue, the envelope's
    // answer being the first of them; member i sits i further along, which is
    // the offset a consumer later reads back for that body.
    const queueOffset = batchResult.queueOffset + i;
    out.push(new SendResult(
      batchResult.sendStatus,
      msgId,
      batchResult.messageQueue,
      queueOffset,
      batchResult.transactionId,
      offsetsSplit ? offsetParts[i] : batchResult.offsetMsgId,
      batchResult.regionId,
      batchResult.traceOn,
      batchResult.recallHandle,
    ));
  }
  return out;
}

export default {
  ProduceAccumulator, AggregateKey, MessageAccumulation,
  getOrCreateProduceAccumulator, splitSendResults,
  DEFAULT_TOTAL_HOLD_SIZE, DEFAULT_HOLD_SIZE, DEFAULT_HOLD_MS,
};
