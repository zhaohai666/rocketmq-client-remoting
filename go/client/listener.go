package client

import (
	"math"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

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

func ConsumeReturnTypeByName(name string) (ConsumeReturnType, bool) {
	for i, n := range consumeReturnTypeNames {
		if n == name {
			return ConsumeReturnType(i), true
		}
	}
	return ConsumeReturnSuccess, false
}

type ConsumeConcurrentlyContext struct {
	MessageQueue common.MessageQueue
	// Getting the default wrong in the OTHER direction — leaving it at 0 or -1
	// — silently acks only one message or none at all.
	AckIndex int
	// DelayLevelWhenNextConsume: 0 means "3 + reconsumeTimes"; any other value
	// is the delay level handed to the broker.
	DelayLevelWhenNextConsume int32
}

func NewConsumeConcurrentlyContext(mq common.MessageQueue) *ConsumeConcurrentlyContext {
	return &ConsumeConcurrentlyContext{MessageQueue: mq, AckIndex: math.MaxInt32}
}

type ConsumeOrderlyContext struct {
	MessageQueue common.MessageQueue
	AutoCommit   bool
	// SuspendCurrentQueueTimeMillis: -1 means "not set"; the consumer then uses
	// its own configuration, and the result is clamped to [10, 30000] after
	// substitution. The clamp is not decoration: a listener returning 0 would
	// otherwise turn the consume thread into a busy loop.
	SuspendCurrentQueueTimeMillis int64
}

func NewConsumeOrderlyContext(mq common.MessageQueue) *ConsumeOrderlyContext {
	return &ConsumeOrderlyContext{MessageQueue: mq, AutoCommit: true, SuspendCurrentQueueTimeMillis: -1}
}

// MessageListenerConcurrently is the concurrent listener SPI.
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

	ConsumeFromWhereLastOffset    = "CONSUME_FROM_LAST_OFFSET"
	ConsumeFromWhereFirstOffset   = "CONSUME_FROM_FIRST_OFFSET"
	ConsumeFromWhereTimestamp     = "CONSUME_FROM_TIMESTAMP"
	ConsumeFromWhereLastOffsetNow = "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST"
	ConsumeFromWhereMinOffsetBoot = "CONSUME_FROM_MIN_OFFSET_WHEN_BOOT_FIRST"

	MessageModelClustering   = "CLUSTERING"
	MessageModelBroadcasting = "BROADCASTING"
)

const pullMaxIdleTime = 120 * time.Second

const (
	// pullTimeDelayMillsWhenFlowControl is the wait after this client's OWN
	// backlog crossed a pull threshold: the messages are already buffered here,
	// so the loop only has to give the consume side a moment.
	pullTimeDelayMillsWhenFlowControl = 50 * time.Millisecond
	// pullTimeDelayMillsWhenBrokerFlow is the wait after the BROKER refused a
	// pull because it is flow-controlling its own queues (response code 215).
	// Shorter than the local one on purpose: the request already round-tripped,
	// nothing landed in the local buffer, and the queue is readable again as
	// soon as the broker's pressure drops.
	pullTimeDelayMillsWhenBrokerFlow = 20 * time.Millisecond
	// pullTimeDelayMillsWhenPullFailed is the wait for every other failed pull:
	// a system error, a busy answer, a dead connection. Retrying those hot adds
	// pressure to whatever is already wrong.
	pullTimeDelayMillsWhenPullFailed = 500 * time.Millisecond
	pullTimeDelayMillsWhenException  = 3000 * time.Millisecond
	pullTimeDelayMillsWhenSuspend    = 1000 * time.Millisecond
)
