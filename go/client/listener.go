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

// PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL / _WHEN_EXCEPTION are Java's
// re-schedule delays.
const (
	pullTimeDelayMillsWhenFlowControl = 50 * time.Millisecond
	pullTimeDelayMillsWhenException   = 3000 * time.Millisecond
)
