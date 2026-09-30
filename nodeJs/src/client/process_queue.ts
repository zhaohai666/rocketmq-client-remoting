// ProcessQueue — the per-queue buffer and bookkeeping
// (Java org.apache.rocketmq.client.impl.consumer.ProcessQueue).
//
// Java keeps ONE ConcurrentSkipListMap<queueOffset, MessageExt> holding both
// "pulled, not yet dispatched" and "dispatched, not yet acked" messages, plus a
// separate consumingMsgOrderlyTreeMap for the orderly path. This port keeps the
// single sorted map and marks dispatched entries with a flag instead: the
// observables that matter (msgCount, msgSize, maxSpan, "is this message still
// mine", removeMessage's floor) are identical, and the same model is used by
// the Python/C++/.NET/Go ports, which keeps cross-language behaviour
// comparable.
//
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

// processQueue is the per-queue state.
export class ProcessQueue {
  dropped = false;
  locked = false;

  // msgs holds every pulled-but-unacked message keyed by queueOffset.
  private msgs = new Map<number, MessageExt>();
  private order: number[] = []; // ascending queueOffsets, kept in sync with msgs
  // dispatched marks entries already handed to a listener.
  private dispatched = new Set<number>();

  private msgAccCnt = 0;

  // queueOffsetMax is Java ProcessQueue#queueOffsetMax: the highest offset
  // ever put into the buffer (cleared by clear()). Java's removeMessage falls
  // back to it when the buffer drains, which is how a batch that completes out
  // of order still lets the cursor reach the end of the queue.
  private queueOffsetMax = 0;

  lastPullTimestamp = 0; // unix millis
  lastConsumeTimestamp = 0; // unix millis of the last completion

  // consumeOrderly mirrors the consumer's mode at creation time (Java reads
  // the consumer's flag inside cleanExpiredMsg).
  consumeOrderly: boolean;

  constructor(consumeOrderly = false) {
    this.consumeOrderly = consumeOrderly;
  }

  // PutMessage mirrors Java ProcessQueue#putMessage. Returns true when the
  // queue went from empty to non-empty, i.e. "there is something to dispatch".
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

  // QueueOffsetMax is Java ProcessQueue#queueOffsetMax, kept as a running max
  // rather than "the last offset of the last put": a re-pulled batch always
  // moves forwards on a consume queue, so the two agree there, and the running
  // max cannot be dragged backwards by an out-of-order put.
  queueOffsetMaxValue(): number {
    return this.queueOffsetMax;
  }

  // MsgAccCnt returns the backlog computed by the last PutMessage.
  msgAccCntValue(): number {
    return this.msgAccCnt;
  }

  // PendingStats is the flow-control triple (count, size in MB, offset span)
  // computed over the NOT-yet-dispatched entries only.
  //
  // Java measures msgCount/msgSize/maxSpan over the whole msgTreeMap, which
  // also holds messages a listener is currently chewing on. Measuring only the
  // pending buffer is what the Python/C++/.NET/Go ports do, and it is the more
  // useful signal: a slow listener must not look like a backlog and stall the
  // pull. Kept consistent across the ports so the numbers are comparable.
  pendingStats(): [number, number, number] {
    let count = 0;
    let size = 0;
    let minOffset = 0;
    let maxOffset = 0;
    let first = true;
    for (const offset of this.order) {
      if (this.dispatched.has(offset)) continue;
      const msg = this.msgs.get(offset);
      if (!msg) continue;
      count++;
      const body = msg.getBody();
      size += body ? body.length : 0;
      if (first || offset < minOffset) minOffset = offset;
      if (first || offset > maxOffset) maxOffset = offset;
      first = false;
    }
    const span = count > 0 ? maxOffset - minOffset : 0;
    return [count, size / (1024.0 * 1024.0), span];
  }

  // MsgCount is Java ProcessQueue#getMsgCount: everything pulled but not
  // acked, dispatched included.
  msgCount(): number {
    return this.msgs.size;
  }

  // PendingCount is the number of pulled-and-not-yet-dispatched messages, i.e.
  // the retry buffer. Flow control is measured on this one so that a slow
  // listener does not look like a backlog.
  pendingCount(): number {
    return this.msgs.size - this.dispatched.size;
  }

  // MsgSizeMB is Java ProcessQueue#getMsgSize expressed in MB (the threshold
  // the consumer compares against is in MB too).
  msgSizeMB(): number {
    let total = 0;
    for (const msg of this.msgs.values()) {
      const body = msg.getBody();
      total += body ? body.length : 0;
    }
    return total / (1024.0 * 1024.0);
  }

  // MaxSpan is Java ProcessQueue#getMaxSpan: lastOffset - firstOffset. It
  // exists so one message that keeps failing cannot let the offset span run
  // away.
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

  // Contains reports whether the offset is still buffered. Java uses
  // msgTreeMap.containsValue(msg); identity does not survive a redelivery, so
  // the offset key is the honest test.
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

  // MinRemainingExcept is Java ProcessQueue#removeMessage's first half: the
  // smallest buffered offset that is NOT in `skip`, i.e. what the head of the
  // buffer WILL be once the entries about to be acked are gone; -1 when
  // nothing would be left. `skip` must therefore be the ACKED set. Passing the
  // FAILED set instead inverts the guard — the floor would step over exactly
  // the entries that have to hold the cursor.
  minRemainingExcept(skip: MessageExt[]): number {
    const skipSet = new Set<number>();
    for (const msg of skip) skipSet.add(msg.getQueueOffset());
    for (const offset of this.order) {
      if (skipSet.has(offset)) continue;
      return offset;
    }
    return -1;
  }

  // Clear empties the buffer (Java ProcessQueue#clear, which also zeroes
  // queueOffsetMax).
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

  // SetDropped marks the queue revoked. Irreversible, same as Java.
  setDropped(): void { this.dropped = true; }
  isDropped(): boolean { return this.dropped; }

  // SetLocked records the LOCK_BATCH_MQ outcome for orderly consumption.
  setLocked(locked: boolean): void { this.locked = locked; }
  isLocked(): boolean { return this.locked; }

  // TouchPull stamps "a pull was started", the isPullExpired input.
  touchPull(): void { this.lastPullTimestamp = Date.now(); }
  lastPullTimestampValue(): number { return this.lastPullTimestamp; }

  // IsPullExpired is Java ProcessQueue#isPullExpired: no pull started within
  // PULL_MAX_IDLE_TIME. A queue that never pulled does not count as expired.
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

// computeMsgAccCnt is Java ProcessQueue#putMessage's tail: backlog size taken
// from the LAST message of the batch, as MAX_OFFSET - queueOffset, only when
// positive.
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
