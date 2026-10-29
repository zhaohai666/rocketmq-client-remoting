// Two flags carry the rebalance contract:
//
//   - dropped: the queue was revoked (rebalance, OFFSET_ILLEGAL recovery,
//     reset-offset). An in-flight batch from a dropped queue must neither be
//     consumed nor acked — the new owner redelivers it from the last committed
//     offset. Acking it would push the offset back to a value the broker just
//     corrected.
//   - locked: only meaningful for ORDERLY consumption in CLUSTERING mode; the
//     broker granted LOCK_BATCH_MQ for this queue. Pulling before the lock is
//     confirmed would consume messages another instance is about to lock.
import { MessageExt } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';

export const PULL_MAX_IDLE_TIME_MS = 120_000;
// A granted lock only shields the queue for this long; the periodic lock sweep
// re-grants it, so a stale grant must not gate pulling forever.
export const LOCK_MAX_LIVE_TIME_MS = 30_000;

// processQueue is the per-queue state.
export class ProcessQueue {
  dropped = false;
  locked = false;
  lastLockTimestamp = 0; // unix millis of the last LOCK_BATCH_MQ grant
  tryUnlockTimes = 0;

  // msgs holds every pulled-but-unacked message keyed by queueOffset.
  private msgs = new Map<number, MessageExt>();
  private order: number[] = []; // ascending queueOffsets, kept in sync with msgs
  // dispatched marks entries already handed to a listener.
  private dispatched = new Set<number>();

  private msgAccCnt = 0;

  private queueOffsetMax = 0;

  lastPullTimestamp = 0; // unix millis
  lastConsumeTimestamp = 0; // unix millis of the last completion

  consumeOrderly: boolean;

  constructor(consumeOrderly = false) {
    this.consumeOrderly = consumeOrderly;
  }

  putMessage(msgs: MessageExt[]): boolean {
    const wasEmpty = this.msgs.size === 0;
    for (const msg of msgs) {
      const offset = msg.getQueueOffset();
      if (!this.msgs.has(offset)) {
        insertOffset(this.order, offset);
      }
      this.msgs.set(offset, msg);
      if (offset > this.queueOffsetMax) this.queueOffsetMax = offset;
    }
    this.msgAccCnt = computeMsgAccCnt(this.msgAccCnt, msgs);
    return wasEmpty && this.msgs.size > 0;
  }

  queueOffsetMaxValue(): number {
    return this.queueOffsetMax;
  }

  // MsgAccCnt returns the backlog computed by the last PutMessage.
  msgAccCntValue(): number {
    return this.msgAccCnt;
  }

  // HeldStats is the flow-control triple (count, whole MiB of message BODY, offset
  // span) over EVERYTHING this queue still holds: the undispatched buffer plus the
  // batches that are already in the listener's hands. A dispatched batch has only
  // moved, it has not left the queue — it leaves when it settles (completeBatch /
  // removeMessage) — so a gate that read the buffer alone would let a queue whose
  // listener is wedged keep pulling, which is exactly what the cache thresholds
  // exist to stop.
  //
  // The byte sum is the BODY the client keeps in memory after decoding, never the
  // wire-side store size, and it is divided by whole MiB: a partially filled MiB
  // costs nothing toward the budget.
  heldStats(): [number, number, number] {
    let count = 0;
    let bytes = 0;
    let minOffset = 0;
    let maxOffset = 0;
    let first = true;
    for (const offset of this.order) {
      const msg = this.msgs.get(offset);
      if (!msg) continue;
      count++;
      const body = msg.getBody();
      bytes += body ? body.length : 0;
      if (first || offset < minOffset) minOffset = offset;
      if (first || offset > maxOffset) maxOffset = offset;
      first = false;
    }
    const span = count > 0 ? maxOffset - minOffset : 0;
    return [count, Math.floor(bytes / (1024 * 1024)), span];
  }

  msgCount(): number {
    return this.msgs.size;
  }

  // PendingCount is the number of pulled-and-not-yet-dispatched messages, i.e.
  // the retry buffer. It is NOT what flow control reads: a batch handed to a
  // listener has only moved, it has not left the queue, so the gates judge
  // heldStats() — the whole held set, listener-held batches included.
  pendingCount(): number {
    return this.msgs.size - this.dispatched.size;
  }

  msgSizeMB(): number {
    let total = 0;
    for (const msg of this.msgs.values()) {
      const body = msg.getBody();
      total += body ? body.length : 0;
    }
    return total / (1024.0 * 1024.0);
  }

  maxSpan(): number {
    if (this.order.length === 0) return 0;
    return this.order[this.order.length - 1] - this.order[0];
  }

  // FirstOffset is the smallest buffered offset (-1 when empty).
  firstOffset(): number {
    return this.order.length > 0 ? this.order[0] : -1;
  }

  // FirstMessage is the smallest buffered message (cleanExpiredMsg's "head
  // only" rule).
  firstMessage(): MessageExt | null {
    if (this.order.length === 0) return null;
    return this.msgs.get(this.order[0]) || null;
  }

  contains(offset: number): boolean {
    return this.msgs.has(offset);
  }

  // TakeBatch returns up to max buffered messages in offset order and marks
  // them dispatched, so they cannot be handed out twice.
  takeBatch(max: number): MessageExt[] {
    if (max <= 0) max = 1;
    const batch: MessageExt[] = [];
    for (const offset of this.order) {
      if (batch.length >= max) break;
      if (this.dispatched.has(offset)) continue;
      const msg = this.msgs.get(offset);
      if (!msg) continue;
      this.dispatched.add(offset);
      batch.push(msg);
    }
    if (batch.length > 0) this.lastConsumeTimestamp = Date.now();
    return batch;
  }

  // CompleteBatch drops the batch from the buffer (the ack path).
  completeBatch(batch: MessageExt[]): void {
    for (const msg of batch) {
      this.msgs.delete(msg.getQueueOffset());
      this.dispatched.delete(msg.getQueueOffset());
    }
    this.rebuildOrderLocked();
  }

  // RequeueBatch makes the batch eligible for dispatch again (rollback /
  // suspend / a send-back that failed).
  requeueBatch(batch: MessageExt[]): void {
    for (const msg of batch) {
      this.dispatched.delete(msg.getQueueOffset());
    }
    this.lastConsumeTimestamp = Date.now();
  }

  // RemoveMessage drops the given messages and returns the smallest offset
  // still buffered (-1 when nothing is left). It is cleanExpiredMsg's remover:
  // that caller throws the result away, because the sweep only runs when the
  // entry really is the head. The ACK path must NOT use it — it has to consult
  // the buffer BEFORE mutating it (the removal and the commit write have to
  // stay on opposite sides of the consumer's lock), which is what
  // minRemainingExcept is for.
  removeMessage(batch: MessageExt[]): number {
    for (const msg of batch) {
      this.msgs.delete(msg.getQueueOffset());
      this.dispatched.delete(msg.getQueueOffset());
    }
    this.rebuildOrderLocked();
    return this.order.length > 0 ? this.order[0] : -1;
  }

  minRemainingExcept(skip: MessageExt[]): number {
    const skipSet = new Set<number>();
    for (const msg of skip) skipSet.add(msg.getQueueOffset());
    for (const offset of this.order) {
      if (skipSet.has(offset)) continue;
      return offset;
    }
    return -1;
  }

  clear(): void {
    this.msgs.clear();
    this.dispatched.clear();
    this.order = [];
    this.queueOffsetMax = 0;
  }

  private rebuildOrderLocked(): void {
    this.order = Array.from(this.msgs.keys());
    this.order.sort((a, b) => a - b);
  }

  setDropped(): void { this.dropped = true; }
  isDropped(): boolean { return this.dropped; }

  // SetLocked records the LOCK_BATCH_MQ outcome for orderly consumption. The
  // grant timestamp is stamped separately by the sweep.
  setLocked(locked: boolean): void { this.locked = locked; }
  isLocked(): boolean { return this.locked; }

  setLastLockTimestamp(ts: number): void { this.lastLockTimestamp = ts; }
  lastLockTimestampValue(): number { return this.lastLockTimestamp; }

  // A lock grant older than the live time is treated as unlocked at the pull
  // gate; the periodic sweep is what renews it.
  isLockExpired(): boolean {
    return Date.now() - this.lastLockTimestamp > LOCK_MAX_LIVE_TIME_MS;
  }

  incTryUnlockTimes(): void { this.tryUnlockTimes++; }

  // ConsumingStats is [min, max, count] over the batches handed to a listener
  // but not settled yet (the orderly transaction view).
  consumingStats(): [number, number, number] {
    let min = -1;
    let max = -1;
    let count = 0;
    for (const offset of this.order) {
      if (!this.dispatched.has(offset)) continue;
      if (min < 0 || offset < min) min = offset;
      if (offset > max) max = offset;
      count++;
    }
    return [min, max, count];
  }

  // TouchPull stamps "a pull was started", the isPullExpired input.
  touchPull(): void { this.lastPullTimestamp = Date.now(); }
  lastPullTimestampValue(): number { return this.lastPullTimestamp; }

  isPullExpired(): boolean {
    if (this.lastPullTimestamp === 0) return false;
    return Date.now() - this.lastPullTimestamp > PULL_MAX_IDLE_TIME_MS;
  }

  // BufferedOffsetSpan is a tiny helper for logs.
  bufferedOffsetSpan(): [number, number, number] {
    if (this.order.length === 0) return [0, 0, 0];
    return [this.order[0], this.order[this.order.length - 1], this.order.length];
  }
}

// ---------------------------------------------------------------- helpers

function insertOffset(sorted: number[], v: number): void {
  let lo = 0, hi = sorted.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (sorted[mid] < v) lo = mid + 1;
    else hi = mid;
  }
  sorted.splice(lo, 0, v);
}

function computeMsgAccCnt(current: number, msgs: MessageExt[]): number {
  if (msgs.length === 0) return current;
  const last = msgs[msgs.length - 1];
  const raw = last.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
  if (raw != null) {
    const maxOffset = parseInt(String(raw).trim(), 10);
    if (!Number.isNaN(maxOffset)) {
      const acc = maxOffset - last.getQueueOffset();
      if (acc > 0) return acc;
    }
  }
  return current;
}
