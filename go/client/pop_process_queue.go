package client

import (
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// It is deliberately NOT the ordinary processQueue: POP has no offset cursor and
// no message buffer. The broker hands out an invisible batch and expects an ACK
// (or an invisibility extension) per message; all the client tracks is how many
// it still owes an answer for. That counter is what flow control reads.
type popProcessQueue struct {
	lastPopTimestamp int64
	waitAckCounter   int64
	dropped          int32
}

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

func (q *popProcessQueue) IncFoundMsg(count int) { atomic.AddInt64(&q.waitAckCounter, int64(count)) }

func (q *popProcessQueue) Ack() int64 { return atomic.AddInt64(&q.waitAckCounter, -1) + 1 }

func (q *popProcessQueue) DecFoundMsg(count int) { atomic.AddInt64(&q.waitAckCounter, int64(count)) }

func (q *popProcessQueue) WaitAckMsgCount() int { return int(atomic.LoadInt64(&q.waitAckCounter)) }

func (q *popProcessQueue) IsDropped() bool { return atomic.LoadInt32(&q.dropped) != 0 }

func (q *popProcessQueue) SetDropped(dropped bool) {
	if dropped {
		atomic.StoreInt32(&q.dropped, 1)
		return
	}
	atomic.StoreInt32(&q.dropped, 0)
}

func (q *popProcessQueue) IsPullExpired() bool {
	return commonCurrentTimeMillis()-q.LastPopTimestamp() > popPullMaxIdleTime
}

func (q *popProcessQueue) FillPopProcessQueueInfo(info *remoting.PopProcessQueueInfo) {
	info.WaitAckCount = int32(q.WaitAckMsgCount())
	info.Droped = q.IsDropped()
	info.LastPopTimestamp = q.LastPopTimestamp()
}

// commonCurrentTimeMillis is a short local alias so this file does not need the
// common import for one call site.
func commonCurrentTimeMillis() int64 { return time.Now().UnixMilli() }
