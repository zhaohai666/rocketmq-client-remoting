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

	queueOffsetMax int64

	lastPullTimestamp    int64 // unix millis
	lastConsumeTimestamp int64 // unix millis of the last completion

	lastLockTimestamp int64
	tryUnlockTimes    int64

	consumeOrderly bool
}

// rebalanceLockMaxLiveTimeMillis is how long an orderly queue lock stays valid
// after the broker granted it: a grant older than this is treated as unlocked,
// because the broker may already have handed the queue to another instance.
const rebalanceLockMaxLiveTimeMillis int64 = 30_000

func newProcessQueue(consumeOrderly bool) *processQueue {
	return &processQueue{
		msgs:              map[int64]*common.MessageExt{},
		dispatched:        map[int64]struct{}{},
		consumeOrderly:    consumeOrderly,
		lastLockTimestamp: common.CurrentTimeMillis(),
	}
}

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

func (pq *processQueue) QueueOffsetMax() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.queueOffsetMax
}

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

// CacheStats is the flow-control triple the cache gates judge: the number of messages this queue
// still HOLDS, their total BODY bytes, and the offset span -- all over the same held set.
//
// "Held" is everything pulled and not yet dropped from the buffer, including the batches already
// handed to a listener: a gate that skipped those would let a wedged listener keep pulling, since
// every held message has already left the undispatched remainder on its way to the listener.
//
// The fourth result tells the gate whether the span term applies at all. It does not, for ORDERLY
// consumption: an ordered queue's pull cursor cannot run ahead while the batch in the listener's
// hands is unsettled, so there is no run-away span to guard against and the span is never compared.
func (pq *processQueue) CacheStats() (count int, bodyBytes int64, span int64, judgeSpan bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	var minOffset, maxOffset int64
	for _, offset := range pq.order {
		msg, ok := pq.msgs[offset]
		if !ok {
			continue
		}
		bodyBytes += int64(len(msg.GetBody()))
		count++
		if count == 1 || offset < minOffset {
			minOffset = offset
		}
		if count == 1 || offset > maxOffset {
			maxOffset = offset
		}
	}
	if count > 0 {
		span = maxOffset - minOffset
	}
	judgeSpan = !pq.consumeOrderly
	return
}

func (pq *processQueue) MsgCount() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.msgs)
}

// PendingCount is the number of pulled-and-not-yet-dispatched messages, i.e. the
// retry buffer. The cache gates do NOT read it: they judge the whole held set,
// which is what CacheStats reports.
func (pq *processQueue) PendingCount() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.msgs) - len(pq.dispatched)
}

func (pq *processQueue) MsgSizeMB() float64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	var total int64
	for _, msg := range pq.msgs {
		total += int64(len(msg.GetBody()))
	}
	return float64(total) / (1024.0 * 1024.0)
}

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

// IsLockExpired reports whether the last confirmed grant is older than the
// lock lifetime, so pulling from this queue would race the next owner.
func (pq *processQueue) IsLockExpired() bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return common.CurrentTimeMillis()-pq.lastLockTimestamp > rebalanceLockMaxLiveTimeMillis
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

func parseInt64(raw string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
}
