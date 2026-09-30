package client

import (
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// popProcessQueue mirrors Java
// org.apache.rocketmq.client.impl.consumer.PopProcessQueue.
//
// It is deliberately NOT the ordinary processQueue: POP has no offset cursor and
// no message buffer. The broker hands out an invisible batch and expects an ACK
// (or an invisibility extension) per message; all the client tracks is how many
// it still owes an answer for. That counter is what flow control reads.
//
// The one trap in the Java class: `ack()` returns the value BEFORE decrementing,
// while `decFoundMsg()` ADD (not subtract) — it is a "give the messages back"
// undo path, and its name says the opposite. Both are reproduced here, with the
// Java behaviour, because ConsumeMessagePopConcurrentlyService relies on the
// sign.
type popProcessQueue struct {
	lastPopTimestamp int64
	waitAckCounter   int64
	dropped          int32
}

// popPullMaxIdleTime is Java's PULL_MAX_IDLE_TIME (overridable via the
// rocketmq.client.pull.pullMaxIdleTime system property).
var popPullMaxIdleTime = func() int64 {
	raw := os.Getenv("ROCKETMQ_CLIENT_PULL_MAX_IDLE_TIME")
	if raw == "" {
		return 120_000
	}
	if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
		return v
	}
	return 120_000
}()

func newPopProcessQueue() *popProcessQueue {
	return &popProcessQueue{lastPopTimestamp: commonCurrentTimeMillis()}
}

func (q *popProcessQueue) LastPopTimestamp() int64 { return atomic.LoadInt64(&q.lastPopTimestamp) }

func (q *popProcessQueue) SetLastPopTimestamp(ts int64) { atomic.StoreInt64(&q.lastPopTimestamp, ts) }

// IncFoundMsg mirrors Java incFoundMsg: the broker just made this many messages
// invisible and we owe an answer for each.
func (q *popProcessQueue) IncFoundMsg(count int) { atomic.AddInt64(&q.waitAckCounter, int64(count)) }

// Ack mirrors Java ack(): it returns the value BEFORE the decrement (Java uses
// AtomicInteger.getAndDecrement, not decrementAndGet). Callers in the ack loop
// ignore the result; the live verifiers use it to assert that the debt reaches
// zero, so the pre-decrement value is the one they read.
func (q *popProcessQueue) Ack() int64 { return atomic.AddInt64(&q.waitAckCounter, -1) + 1 }

// DecFoundMsg mirrors Java decFoundMsg, which ADDS. It is the "abort this batch,
// hand the debt back" path used when a batch turns out to be undeliverable
// (dropped queue / popped out of its invisibility window). Passing a negative
// count therefore returns the debt, which is exactly how Java calls it:
// `processQueue.decFoundMsg(-msgs.size())`.
func (q *popProcessQueue) DecFoundMsg(count int) { atomic.AddInt64(&q.waitAckCounter, int64(count)) }

// WaitAckMsgCount is the outstanding-ACK count (Java's misspelled
// getWaiAckMsgCount).
func (q *popProcessQueue) WaitAckMsgCount() int { return int(atomic.LoadInt64(&q.waitAckCounter)) }

func (q *popProcessQueue) IsDropped() bool { return atomic.LoadInt32(&q.dropped) != 0 }

func (q *popProcessQueue) SetDropped(dropped bool) {
	if dropped {
		atomic.StoreInt32(&q.dropped, 1)
		return
	}
	atomic.StoreInt32(&q.dropped, 0)
}

// IsPullExpired mirrors Java isPullExpired. The POP rebalance uses it to retire
// a queue whose pop loop has gone quiet (the broker would otherwise keep
// reviving its batch).
func (q *popProcessQueue) IsPullExpired() bool {
	return commonCurrentTimeMillis()-q.LastPopTimestamp() > popPullMaxIdleTime
}

// commonCurrentTimeMillis is a short local alias so this file does not need the
// common import for one call site.
func commonCurrentTimeMillis() int64 { return time.Now().UnixMilli() }
