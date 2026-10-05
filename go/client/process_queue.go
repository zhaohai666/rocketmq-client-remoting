// ProcessQueue — the per-queue buffer and bookkeeping
// (Java org.apache.rocketmq.client.impl.consumer.ProcessQueue).
//
// Java keeps ONE ConcurrentSkipListMap<queueOffset, MessageExt> holding both
// "pulled, not yet dispatched" and "dispatched, not yet acked" messages, plus a
// separate consumingMsgOrderlyTreeMap for the orderly path. This port keeps the
// single sorted map and marks dispatched entries with a flag instead: the
// observables that matter (msgCount, msgSize, maxSpan, "is this message still
// mine", removeMessage's floor) are identical, and the same model is used by
// the Python/C++/C# ports, which is what keeps cross-language behaviour
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
package client

import (
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// processQueue is the per-queue state.
type processQueue struct {
	mu sync.Mutex

	dropped bool
	locked  bool

	// msgs holds every pulled-but-unacked message keyed by queueOffset.
	msgs  map[int64]*common.MessageExt
	order []int64 // ascending queueOffsets, kept in sync with msgs
	// dispatched marks entries already handed to a listener.
	dispatched map[int64]struct{}

	msgAccCnt int64

	// queueOffsetMax is Java ProcessQueue#queueOffsetMax: the highest offset
	// ever put into the buffer (cleared by clear()). Java's removeMessage falls
	// back to it when the buffer drains, which is how a batch that completes out
	// of order still lets the cursor reach the end of the queue.
	queueOffsetMax int64

	lastPullTimestamp    int64 // unix millis
	lastConsumeTimestamp int64 // unix millis of the last completion

	// tryUnlockTimes / lastLockTimestamp are Java ProcessQueue's
	// AtomicLong tryUnlockTimes + volatile lastLockTimestamp. Both ride inside
	// ProcessQueueInfo in the 307 answer, which is the only reason they exist.
	//
	// lastLockTimestamp is stamped at construction and re-stamped every time the
	// broker CONFIRMS the queue lock (Java: setLocked(true) is always paired with
	// setLastLockTimestamp(now) in RebalanceImpl.lock/lockAll; the failure branch
	// calls setLocked(false) WITHOUT touching the stamp, which is why the stamp
	// lives in the true branch here too).
	//
	// tryUnlockTimes counts "the queue could not be released cleanly" — Java
	// increments it when removeUnnecessaryMessageQueue fails to take the 500ms
	// consume-lock tryLock, or throws. This port retires a queue under its own
	// mutex with nothing to block on, so there is no failing branch to count and
	// the value stays 0; the field is kept so the wire shape matches and so a
	// future blocking release has somewhere to record it.
	lastLockTimestamp int64
	tryUnlockTimes    int64

	// consumeOrderly mirrors the consumer's mode at creation time (Java reads
	// the consumer's flag inside cleanExpiredMsg).
	consumeOrderly bool
}

func newProcessQueue(consumeOrderly bool) *processQueue {
	return &processQueue{
		msgs:              map[int64]*common.MessageExt{},
		dispatched:        map[int64]struct{}{},
		consumeOrderly:    consumeOrderly,
		lastLockTimestamp: common.CurrentTimeMillis(),
	}
}

// PutMessage mirrors Java ProcessQueue#putMessage. Returns true when the queue
// went from empty to non-empty, i.e. "there is something to dispatch".
func (pq *processQueue) PutMessage(msgs []*common.MessageExt) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	wasEmpty := len(pq.msgs) == 0
	valid := 0
	for _, msg := range msgs {
		offset := msg.QueueOffset
		if _, exists := pq.msgs[offset]; !exists {
			valid++
			pq.order = insertOffset(pq.order, offset)
		}
		pq.msgs[offset] = msg
		if offset > pq.queueOffsetMax {
			pq.queueOffsetMax = offset
		}
	}
	_ = valid
	pq.msgAccCnt = computeMsgAccCnt(pq.msgAccCnt, msgs)
	return wasEmpty && len(pq.msgs) > 0
}

// QueueOffsetMax is Java ProcessQueue#queueOffsetMax, kept as a running max
// rather than "the last offset of the last put": a re-pulled batch always moves
// forwards on a consume queue, so the two agree there, and the running max
// cannot be dragged backwards by an out-of-order put.
func (pq *processQueue) QueueOffsetMax() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.queueOffsetMax
}

// computeMsgAccCnt is Java ProcessQueue#putMessage's tail: backlog size taken
// from the LAST message of the batch, as MAX_OFFSET - queueOffset, only when
// positive.
func computeMsgAccCnt(current int64, msgs []*common.MessageExt) int64 {
	if len(msgs) == 0 {
		return current
	}
	last := msgs[len(msgs)-1]
	if raw, ok := last.GetProperty(common.PropertyMaxOffset); ok {
		if maxOffset, err := parseInt64(raw); err == nil {
			if acc := maxOffset - last.QueueOffset; acc > 0 {
				return acc
			}
		}
	}
	return current
}

// MsgAccCnt returns the backlog computed by the last PutMessage.
func (pq *processQueue) MsgAccCnt() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.msgAccCnt
}

// PendingStats is the flow-control triple (count, size in MB, offset span)
// computed over the NOT-yet-dispatched entries only.
//
// Java measures msgCount/msgSize/maxSpan over the whole msgTreeMap, which also
// holds messages a listener is currently chewing on. Measuring only the pending
// buffer is what the Python/C++/C# ports do, and it is the more useful signal:
// a slow listener must not look like a backlog and stall the pull. Kept
// consistent across the four ports so the numbers are comparable.
func (pq *processQueue) PendingStats() (int, float64, int64) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	count := 0
	var size int64
	var minOffset, maxOffset int64
	first := true
	for _, offset := range pq.order {
		if _, inflight := pq.dispatched[offset]; inflight {
			continue
		}
		msg, ok := pq.msgs[offset]
		if !ok {
			continue
		}
		count++
		size += int64(len(msg.GetBody()))
		if first || offset < minOffset {
			minOffset = offset
		}
		if first || offset > maxOffset {
			maxOffset = offset
		}
		first = false
	}
	span := int64(0)
	if count > 0 {
		span = maxOffset - minOffset
	}
	return count, float64(size) / (1024.0 * 1024.0), span
}

// MsgCount is Java ProcessQueue#getMsgCount: everything pulled but not acked,
// dispatched included.
func (pq *processQueue) MsgCount() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.msgs)
}

// PendingCount is the number of pulled-and-not-yet-dispatched messages, i.e.
// the retry buffer. Flow control is measured on this one so that a slow
// listener does not look like a backlog.
func (pq *processQueue) PendingCount() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.msgs) - len(pq.dispatched)
}

// MsgSizeMB is Java ProcessQueue#getMsgSize expressed in MB (the threshold the
// consumer compares against is in MB too).
func (pq *processQueue) MsgSizeMB() float64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	var total int64
	for _, msg := range pq.msgs {
		total += int64(len(msg.GetBody()))
	}
	return float64(total) / (1024.0 * 1024.0)
}

// MaxSpan is Java ProcessQueue#getMaxSpan: lastOffset - firstOffset. It exists
// so one message that keeps failing cannot let the offset span run away.
func (pq *processQueue) MaxSpan() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.order) == 0 {
		return 0
	}
	return pq.order[len(pq.order)-1] - pq.order[0]
}

// FirstOffset is the smallest buffered offset.
func (pq *processQueue) FirstOffset() (int64, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.order) == 0 {
		return 0, false
	}
	return pq.order[0], true
}

// FirstMessage is the smallest buffered message (cleanExpiredMsg's "head only"
// rule).
func (pq *processQueue) FirstMessage() (*common.MessageExt, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.order) == 0 {
		return nil, false
	}
	msg, ok := pq.msgs[pq.order[0]]
	return msg, ok
}

// Contains reports whether the offset is still buffered. Java uses
// msgTreeMap.containsValue(msg); identity does not survive a redelivery, so the
// offset key is the honest test.
func (pq *processQueue) Contains(offset int64) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	_, ok := pq.msgs[offset]
	return ok
}

// TakeBatch returns up to max buffered messages in offset order and marks them
// dispatched, so they cannot be handed out twice.
func (pq *processQueue) TakeBatch(max int) []*common.MessageExt {
	if max <= 0 {
		max = 1
	}
	pq.mu.Lock()
	defer pq.mu.Unlock()
	var batch []*common.MessageExt
	for _, offset := range pq.order {
		if len(batch) >= max {
			break
		}
		if _, inflight := pq.dispatched[offset]; inflight {
			continue
		}
		msg, ok := pq.msgs[offset]
		if !ok {
			continue
		}
		pq.dispatched[offset] = struct{}{}
		batch = append(batch, msg)
	}
	if len(batch) > 0 {
		pq.lastConsumeTimestamp = common.CurrentTimeMillis()
	}
	return batch
}

// CompleteBatch drops the batch from the buffer (the ack path).
func (pq *processQueue) CompleteBatch(batch []*common.MessageExt) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	for _, msg := range batch {
		delete(pq.msgs, msg.QueueOffset)
		delete(pq.dispatched, msg.QueueOffset)
	}
	pq.rebuildOrderLocked()
}

// RequeueBatch makes the batch eligible for dispatch again (rollback / suspend /
// a send-back that failed).
func (pq *processQueue) RequeueBatch(batch []*common.MessageExt) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	for _, msg := range batch {
		delete(pq.dispatched, msg.QueueOffset)
	}
	pq.lastConsumeTimestamp = common.CurrentTimeMillis()
}

// RemoveMessage drops the given messages and returns the smallest offset still
// buffered (-1 when nothing is left). It is cleanExpiredMsg's remover: that
// caller throws the result away, because the sweep only runs when the entry
// really is the head. The ACK path must NOT use it — it has to consult the
// buffer BEFORE mutating it (the removal and the commit write have to stay on
// opposite sides of the consumer's lock), which is what MinRemainingExcept is
// for.
func (pq *processQueue) RemoveMessage(batch []*common.MessageExt) int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	for _, msg := range batch {
		delete(pq.msgs, msg.QueueOffset)
		delete(pq.dispatched, msg.QueueOffset)
	}
	pq.rebuildOrderLocked()
	if len(pq.order) == 0 {
		return -1
	}
	return pq.order[0]
}

// MinRemainingExcept is Java ProcessQueue#removeMessage's first half: the
// smallest buffered offset that is NOT in `skip`, i.e. what the head of the
// buffer WILL be once the entries about to be acked are gone; -1 when nothing
// would be left. `skip` must therefore be the ACKED set. Passing the FAILED set
// instead inverts the guard — the floor would step over exactly the entries that
// have to hold the cursor.
func (pq *processQueue) MinRemainingExcept(skip []*common.MessageExt) int64 {
	skipSet := make(map[int64]struct{}, len(skip))
	for _, msg := range skip {
		skipSet[msg.QueueOffset] = struct{}{}
	}
	pq.mu.Lock()
	defer pq.mu.Unlock()
	for _, offset := range pq.order {
		if _, skipped := skipSet[offset]; skipped {
			continue
		}
		return offset
	}
	return -1
}

// Clear empties the buffer (Java ProcessQueue#clear, which also zeroes
// queueOffsetMax).
func (pq *processQueue) Clear() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.msgs = map[int64]*common.MessageExt{}
	pq.dispatched = map[int64]struct{}{}
	pq.order = nil
	pq.queueOffsetMax = 0
}

func (pq *processQueue) rebuildOrderLocked() {
	pq.order = pq.order[:0]
	for offset := range pq.msgs {
		pq.order = append(pq.order, offset)
	}
	sort.Slice(pq.order, func(i, j int) bool { return pq.order[i] < pq.order[j] })
}

// SetDropped marks the queue revoked. Irreversible, same as Java.
func (pq *processQueue) SetDropped() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.dropped = true
}

// IsDropped reports the dropped flag.
func (pq *processQueue) IsDropped() bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.dropped
}

// SetLocked records the LOCK_BATCH_MQ outcome for orderly consumption. A grant
// also re-stamps lastLockTimestamp (Java's lock()/lockAll(), which pair
// setLocked(true) with setLastLockTimestamp(now)); a refusal leaves the stamp
// alone, again as Java does.
func (pq *processQueue) SetLocked(locked bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.locked = locked
	if locked {
		pq.lastLockTimestamp = common.CurrentTimeMillis()
	}
}

// TryUnlockTimes is the failed-release counter (see the field comment).
func (pq *processQueue) TryUnlockTimes() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.tryUnlockTimes
}

// LastLockTimestamp is the last confirmed-lock stamp.
func (pq *processQueue) LastLockTimestamp() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.lastLockTimestamp
}

// LastConsumeTimestamp is the last handed-to-listener stamp.
func (pq *processQueue) LastConsumeTimestamp() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.lastConsumeTimestamp
}

// FillProcessQueueInfo mirrors Java ProcessQueue#fillProcessQueueInfo
// (:432-465) for the 307 answer.
//
// Three of the fields are CONDITIONAL in Java and the difference is observable:
// cachedMsgMinOffset/MaxOffset/Count are only written when the buffer is
// non-empty (an empty queue reports 0/0/0 rather than a stale range), and the
// transaction triple only when messages are actually in flight on the ORDERLY
// path (Java's consumingMsgOrderlyTreeMap). cachedMsgSizeInMiB is written
// unconditionally.
func (pq *processQueue) FillProcessQueueInfo(info *remoting.ProcessQueueInfo) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.order) > 0 {
		info.CachedMsgMinOffset = pq.order[0]
		info.CachedMsgMaxOffset = pq.order[len(pq.order)-1]
		info.CachedMsgCount = int32(len(pq.order))
	}
	var size int64
	for _, msg := range pq.msgs {
		size += int64(len(msg.GetBody()))
	}
	info.CachedMsgSizeInMiB = int32(size / (1024 * 1024))

	// Java's consumingMsgOrderlyTreeMap only ever receives entries when
	// consumeOrderly is set (takeMessages), so the concurrent path must leave
	// the triple at zero.
	if pq.consumeOrderly {
		inflightMin, inflightMax, inflightCount := pq.inflightOrderlyRangeLocked()
		if inflightCount > 0 {
			info.TransactionMsgMinOffset = inflightMin
			info.TransactionMsgMaxOffset = inflightMax
			info.TransactionMsgCount = int32(inflightCount)
		}
	}

	info.Locked = pq.locked
	info.TryUnlockTimes = pq.tryUnlockTimes
	info.LastLockTimestamp = pq.lastLockTimestamp

	info.Droped = pq.dropped
	info.LastPullTimestamp = pq.lastPullTimestamp
	info.LastConsumeTimestamp = pq.lastConsumeTimestamp
}

// inflightOrderlyRangeLocked is the (min, max, count) of the entries currently
// handed to an orderly listener. Caller must hold pq.mu.
func (pq *processQueue) inflightOrderlyRangeLocked() (int64, int64, int) {
	var minOff, maxOff int64
	count := 0
	for _, offset := range pq.order {
		if _, inflight := pq.dispatched[offset]; !inflight {
			continue
		}
		if count == 0 || offset < minOff {
			minOff = offset
		}
		if count == 0 || offset > maxOff {
			maxOff = offset
		}
		count++
	}
	return minOff, maxOff, count
}

// IsLocked reports the orderly lock state.
func (pq *processQueue) IsLocked() bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.locked
}

// TouchPull stamps "a pull was started", the isPullExpired input.
func (pq *processQueue) TouchPull() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.lastPullTimestamp = common.CurrentTimeMillis()
}

// LastPullTimestamp returns the stamp (tests/diagnostics).
func (pq *processQueue) LastPullTimestamp() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.lastPullTimestamp
}

// IsPullExpired is Java ProcessQueue#isPullExpired: no pull started within
// PULL_MAX_IDLE_TIME. A queue that never pulled does not count as expired.
func (pq *processQueue) IsPullExpired() bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if pq.lastPullTimestamp == 0 {
		return false
	}
	return common.CurrentTimeMillis()-pq.lastPullTimestamp > pullMaxIdleTime.Milliseconds()
}

// BufferedOffsetSpan is a tiny helper for logs.
func (pq *processQueue) BufferedOffsetSpan() (int64, int64, int) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.order) == 0 {
		return 0, 0, 0
	}
	return pq.order[0], pq.order[len(pq.order)-1], len(pq.order)
}

// ---------------------------------------------------------------- helpers

func insertOffset(sorted []int64, v int64) []int64 {
	i := sort.Search(len(sorted), func(i int) bool { return sorted[i] >= v })
	sorted = append(sorted, 0)
	copy(sorted[i+1:], sorted[i:])
	sorted[i] = v
	return sorted
}

// parseInt64 parses a property value the way Java's Long.parseLong does; the
// caller decides what a failure means.
func parseInt64(raw string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
}
