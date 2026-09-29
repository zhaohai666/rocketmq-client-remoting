// ProcessQueue — the per-queue buffer and bookkeeping
// (Java org.apache.rocketmq.client.impl.consumer.ProcessQueue).
//
// Java keeps ONE ConcurrentSkipListMap<queueOffset, MessageExt> holding both
// "pulled, not yet dispatched" and "dispatched, not yet acked" messages, plus a
// separate consumingMsgOrderlyTreeMap for the orderly path. This port keeps the
// single sorted map and marks dispatched entries with a flag instead: the
// observables that matter (msgCount, msgSize, maxSpan, "is this message still
// mine", removeMessage's floor) are identical, and the same model is used by
// the Python/C++/.NET ports, which is what keeps cross-language behaviour
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

	lastPullTimestamp    int64 // unix millis
	lastConsumeTimestamp int64 // unix millis of the last completion

	// consumeOrderly mirrors the consumer's mode at creation time (Java reads
	// the consumer's flag inside cleanExpiredMsg).
	consumeOrderly bool
}

func newProcessQueue(consumeOrderly bool) *processQueue {
	return &processQueue{
		msgs:           map[int64]*common.MessageExt{},
		dispatched:     map[int64]struct{}{},
		consumeOrderly: consumeOrderly,
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
	}
	_ = valid
	pq.msgAccCnt = computeMsgAccCnt(pq.msgAccCnt, msgs)
	return wasEmpty && len(pq.msgs) > 0
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
// buffer is what the Python/C++/.NET ports do, and it is the more useful signal:
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

// RemoveMessage mirrors Java ProcessQueue#removeMessage: drop the given
// messages and return the smallest offset STILL buffered, or -1 when nothing is
// left. The caller uses it as the commit floor — without it the offset would
// jump past messages that are still pending.
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

// MinRemainingExcept is the smallest buffered offset that is NOT in `skip`; -1
// when there is none. Used as the commit floor after a partly-failed send-back.
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

// Clear empties the buffer (Java ProcessQueue#clear).
func (pq *processQueue) Clear() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.msgs = map[int64]*common.MessageExt{}
	pq.dispatched = map[int64]struct{}{}
	pq.order = nil
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

// SetLocked records the LOCK_BATCH_MQ outcome for orderly consumption.
func (pq *processQueue) SetLocked(locked bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.locked = locked
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
