// Consumer-side listener SPIs and result beans (Java
// org.apache.rocketmq.client.consumer.listener.*).
package client

import (
	"math"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ConsumeConcurrentlyStatus mirrors Java ConsumeConcurrentlyStatus.
type ConsumeConcurrentlyStatus int

const (
	// ConsumeSuccess: the batch was handled; ack up to ctx.AckIndex.
	ConsumeSuccess ConsumeConcurrentlyStatus = iota
	// ReconsumeLater: the batch must come back; ackIndex is forced to -1.
	ReconsumeLater
)

func (s ConsumeConcurrentlyStatus) String() string {
	if s == ReconsumeLater {
		return "RECONSUME_LATER"
	}
	return "CONSUME_SUCCESS"
}

// ConsumeOrderlyStatus mirrors Java ConsumeOrderlyStatus.
type ConsumeOrderlyStatus int

const (
	OrderlySuccess ConsumeOrderlyStatus = iota
	OrderlyRollback
	OrderlyCommit
	OrderlySuspendCurrentQueueAMoment
)

func (s ConsumeOrderlyStatus) String() string {
	switch s {
	case OrderlyRollback:
		return "ROLLBACK"
	case OrderlyCommit:
		return "COMMIT"
	case OrderlySuspendCurrentQueueAMoment:
		return "SUSPEND_CURRENT_QUEUE_A_MOMENT"
	default:
		return "SUCCESS"
	}
}

// ConsumeReturnType mirrors Java
// org.apache.rocketmq.client.consumer.listener.ConsumeReturnType.
//
// The ORDINAL is a wire-format value: a SubAfter trace record stores it as
// contextCode (Java ConsumeMessageTraceHookImpl:113). Reordering the constants
// would silently relabel every consume outcome in the console.
type ConsumeReturnType int

const (
	ConsumeReturnSuccess ConsumeReturnType = iota
	ConsumeReturnTimeout
	ConsumeReturnException
	ConsumeReturnNull
	ConsumeReturnFailed
)

var consumeReturnTypeNames = [...]string{"SUCCESS", "TIME_OUT", "EXCEPTION", "RETURNNULL", "FAILED"}

func (r ConsumeReturnType) String() string {
	if r >= 0 && int(r) < len(consumeReturnTypeNames) {
		return consumeReturnTypeNames[r]
	}
	return "SUCCESS"
}

// ConsumeReturnTypeByName is the reverse lookup the hook context performs: the
// consumer stores the NAME under common.ConsumeContextType and the trace hook
// maps it back to the ordinal (Java ConsumeReturnType.valueOf).
func ConsumeReturnTypeByName(name string) (ConsumeReturnType, bool) {
	for i, n := range consumeReturnTypeNames {
		if n == name {
			return ConsumeReturnType(i), true
		}
	}
	return ConsumeReturnSuccess, false
}

// ConsumeConcurrentlyContext is handed to a concurrent listener and read back
// afterwards (Java ConsumeConcurrentlyContext).
type ConsumeConcurrentlyContext struct {
	MessageQueue common.MessageQueue
	// AckIndex: only messages [0, AckIndex] are acknowledged. Defaults to
	// MaxInt32, which the success path clamps to len(batch)-1 (i.e. "the whole
	// batch"); RECONSUME_LATER forces -1 (Java processConsumeResult:207-229).
	//
	// Getting the default wrong in the OTHER direction — leaving it at 0 or -1
	// — silently acks only one message or none at all.
	AckIndex int
	// DelayLevelWhenNextConsume: 0 means "3 + reconsumeTimes"; any other value
	// is the delay level handed to the broker.
	DelayLevelWhenNextConsume int32
}

// NewConsumeConcurrentlyContext builds the Java default state.
func NewConsumeConcurrentlyContext(mq common.MessageQueue) *ConsumeConcurrentlyContext {
	return &ConsumeConcurrentlyContext{MessageQueue: mq, AckIndex: math.MaxInt32}
}

// ConsumeOrderlyContext is handed to an orderly listener
// (Java ConsumeOrderlyContext).
type ConsumeOrderlyContext struct {
	MessageQueue common.MessageQueue
	// AutoCommit defaults to true. With it on, COMMIT/ROLLBACK are ILLEGAL
	// (Java logs a warning and treats them as SUCCESS) — they exist for the
	// binlog consumer, which turns autoCommit off.
	AutoCommit bool
	// SuspendCurrentQueueTimeMillis: -1 means "not set"; the consumer then uses
	// its own configuration, and the result is clamped to [10, 30000] after
	// substitution. The clamp is not decoration: a listener returning 0 would
	// otherwise turn the consume thread into a busy loop.
	SuspendCurrentQueueTimeMillis int64
}

// NewConsumeOrderlyContext builds the Java default state.
func NewConsumeOrderlyContext(mq common.MessageQueue) *ConsumeOrderlyContext {
	return &ConsumeOrderlyContext{MessageQueue: mq, AutoCommit: true, SuspendCurrentQueueTimeMillis: -1}
}

// MessageListenerConcurrently is the concurrent listener SPI.
//
// The implementation MUST NOT return an error — a panic is caught and treated
// as RECONSUME_LATER, exactly like Java's thrown exception.
type MessageListenerConcurrently interface {
	ConsumeMessage(msgs []*common.MessageExt, ctx *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus
}

// MessageListenerOrderly is the orderly listener SPI. A panic is caught and
// treated as "suspend and retry in place" — orderly consumption has NO
// "exception means ack" path.
type MessageListenerOrderly interface {
	ConsumeMessage(msgs []*common.MessageExt, ctx *ConsumeOrderlyContext) ConsumeOrderlyStatus
}

// ConsumeType / MessageModel / ConsumeFromWhere wire strings.
const (
	ConsumeTypePassively = "CONSUME_PASSIVELY"
	ConsumeTypeActively  = "CONSUME_ACTIVELY"

	// ConsumeFromWhere* match Java ConsumeFromWhere enum names.
	ConsumeFromWhereLastOffset    = "CONSUME_FROM_LAST_OFFSET"
	ConsumeFromWhereFirstOffset   = "CONSUME_FROM_FIRST_OFFSET"
	ConsumeFromWhereTimestamp     = "CONSUME_FROM_TIMESTAMP"
	ConsumeFromWhereLastOffsetNow = "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST"
	ConsumeFromWhereMinOffsetBoot = "CONSUME_FROM_MIN_OFFSET_WHEN_BOOT_FIRST"

	// MessageModel* match Java MessageModel enum names.
	MessageModelClustering   = "CLUSTERING"
	MessageModelBroadcasting = "BROADCASTING"
)

// PULL_MAX_IDLE_TIME is Java ProcessQueue.PULL_MAX_IDLE_TIME
// (`rocketmq.client.pull.pullMaxIdleTime`, default 120000ms): a queue still
// assigned to this instance that has not started a pull for this long has a
// dead or stuck loop, and rebalance tears it down and rebuilds it.
const pullMaxIdleTime = 120 * time.Second

// Java's re-schedule delays for a pull request that is not sent this round:
// 50ms when the local cache hits a flow-control gate
// (DefaultMQPushConsumerImpl:105), 1000ms while the consumer is suspended
// (:113, the flag Suspend()/Resume() drive). Java's third branch — 20ms when the
// BROKER answers the pull with flow control (:109, applied at :446) — has no
// dedicated delay here: the pull loop treats any broker rejection as the generic
// 500ms error backoff, while the POP loop does single out RespFlowControl
// (pop_consumer.go).
const (
	pullTimeDelayMillsWhenFlowControl = 50 * time.Millisecond
	pullTimeDelayMillsWhenException   = 3000 * time.Millisecond
	pullTimeDelayMillsWhenSuspend     = 1000 * time.Millisecond
)
