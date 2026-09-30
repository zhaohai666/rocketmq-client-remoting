// DefaultMQPushConsumer — the push consumer
// (Java org.apache.rocketmq.client.consumer.DefaultMQPushConsumer +
// DefaultMQPushConsumerImpl + RebalancePushImpl, merged the same way the
// Python/C++/.NET ports merge them).
//
// Shape of the thing, per client instance:
//
//	rebalance loop (20s, or 2s while starting up with nothing assigned)
//	  -> assigns queues, then syncPullLoops() retires/adds
//	per-queue pull goroutine (long poll, suspend=true)
//	  -> buffers messages into that queue's processQueue
//	one dispatch goroutine
//	  -> hands batches to concurrent listeners in goroutines bounded by
//	     corePoolSize, or consumes orderly batches inline (serialised)
//	instance-scheduled tasks
//	  -> heartbeat (30s), route refresh (30s), offset persist (10s+5s)
//
// Ordering rules that are load-bearing:
//
//   - The instance's heartbeat is what puts this group into the broker's
//     ConsumerManager; without it GET_CONSUMER_LIST_BY_GROUP(38) answers empty
//     and rebalance keeps the previous assignment (it must NEVER fall back to
//     "I own every queue" — co-instances would then duplicate each other).
//   - Retiring a queue persists its CONSUMED offset BEFORE the new owner starts
//     pulling. Reversed, the new loop starts from a stale offset and the old
//     instance's in-flight ack writes the smaller value back — duplicate
//     delivery.
//   - The initial offset is resolved at ASSIGNMENT time, not lazily at the first
//     pull: CONSUME_FROM_LAST_OFFSET means "the newest offset as of the moment
//     the queue was assigned", so a lazy resolution would skip everything
//     produced in between.
package client

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// Consumer defaults (Java DefaultMQPushConsumer 5.x).
const (
	defaultPullBatchSize               = int32(32)
	defaultConsumeMessageBatchMaxSize  = 1
	defaultConsumeThreadMin            = 20
	defaultConsumeThreadMax            = 20
	defaultAdjustThreadPoolNumsThresh  = int64(100000)
	defaultConsumeConcurrentlyMaxSpan  = int64(2000)
	defaultPullThresholdForQueue       = int64(1000)
	defaultPullThresholdSizeForQueue   = int64(100)
	defaultPullThresholdForTopic       = int64(-1)
	defaultPullThresholdSizeForTopic   = int64(-1)
	defaultPullInterval                = int64(0)
	defaultPullTimeoutMillis           = int64(30000)
	defaultPullSuspendTimeoutMillis    = int64(20000)
	defaultPullBatchSizeInBytes        = int32(256 * 1024)
	defaultMaxReconsumeTimes           = int32(-1)
	defaultSuspendCurrentQueueTimeMs   = int64(1000)
	defaultConsumeTimeout              = int64(15)
	defaultConsumerPollNamesrvInterval = int64(30000)
	// POP defaults (Java DefaultMQPushConsumer.popThresholdForQueue /
	// popInvisibleTime / popBatchNums). The invisible time and batch size are
	// range-checked at startup: [5000, 300000] ms and [1, 32].
	defaultPopThresholdForQueue = 96
	defaultPopInvisibleTime     = int64(60000)
	defaultPopBatchNums         = int32(32)
	minPopInvisibleTime         = int64(5000)
	maxPopInvisibleTime         = int64(300000)
	maxPopBatchNums             = int32(32)
	// rebalanceInterval is Java RebalanceService's 20s.
	rebalanceInterval = 20 * time.Second
	// rebalanceIntervalDuringStartup is the Python/Rust fast retry: while the
	// consumer is young (60s) and has NOTHING assigned, retry every 2s. The
	// consumer may legitimately start before the topic exists (autoCreateTopic
	// builds it on the producer's first send), and consumers deliberately do
	// NOT fall back to the default topic — so a 20s wait would stall consumption
	// for a long time. Bounded to the startup window so a long-lived consumer
	// subscribed to a non-existent topic does not hammer the name server.
	rebalanceIntervalDuringStartup = 2 * time.Second
	rebalanceStartupWindow         = 60 * time.Second
)

// MessageSelector mirrors Java MessageSelector.
type MessageSelector struct {
	Type       string
	Expression string
}

// ByTag builds a TAG selector.
func byTagSelector(tag string) MessageSelector {
	return MessageSelector{Type: remoting.ExpressionTypeTag, Expression: tag}
}

// BySQL builds a SQL92 selector.
func bySQLSelector(sql string) MessageSelector {
	return MessageSelector{Type: remoting.ExpressionTypeSQL92, Expression: sql}
}

// MessageQueueListener is notified when the assigned queue set changes.
type MessageQueueListener interface {
	MessageQueueChanged(topic string, mqAll, mqDivided []common.MessageQueue)
}

// DefaultMQPushConsumer is the push consumer.
type DefaultMQPushConsumer struct {
	mu sync.Mutex

	consumerGroup string
	namespace     string
	namespaceV2   string
	instanceName  string
	clientID      string
	unitName      string
	unitMode      bool
	streamRequest bool
	tlsEnable     *bool
	rpcHook       remoting.RPCHook

	messageModel     string
	consumeFromWhere string
	consumeTimestamp string

	consumeThreadMin              int
	consumeThreadMax              int
	corePoolSize                  int
	adjustThreadPoolNumsThreshold int64

	consumeConcurrentlyMaxSpan int64
	pullThresholdForQueue      int64
	pullThresholdSizeForQueue  int64
	pullThresholdForTopic      int64
	pullThresholdSizeForTopic  int64
	pullInterval               int64
	pullTimeoutMillis          int64
	pullSuspendTimeoutMillis   int64
	consumeMessageBatchMaxSize int
	pullBatchSize              int32
	pullBatchSizeInBytes       int32
	postSubscriptionWhenPull   bool
	maxReconsumeTimes          int32
	suspendCurrentQueueTimeMs  int64
	consumeTimeout             int64
	allocateStrategy           AllocateMessageQueueStrategy
	messageQueueListener       MessageQueueListener

	// POP mode (Java's broker-side MessageRequestMode). When popMode is on the
	// consumer stops reading the topic with PULL_MESSAGE and pops it instead:
	// no client offset, no processQueue — the queue is tracked by a
	// popProcessQueue holding only the outstanding-ACK debt.
	popMode              bool
	popInvisibleTime     int64
	popBatchNums         int32
	popThresholdForQueue int
	// popShareQueueNum is forwarded verbatim by SET_MESSAGE_REQUEST_MODE(401):
	// it lets N following consumers in the cid list share this one's queues.
	popShareQueueNum int32
	// paused mirrors Java DefaultMQPushConsumerImpl.pause (SUSPEND_CONSUMER).
	paused bool

	nameServerAddrs []string
	subscription    map[string]*remoting.SubscriptionData
	listener        any
	orderly         bool

	consumeMessageHookList []ConsumeMessageHook
	filterMessageHookList  []FilterMessageHook

	// Client-side trace (Java DefaultMQPushConsumer.enableTrace /
	// DefaultMQPushConsumerImpl.traceDispatcher). Start builds a CONSUME
	// dispatcher and registers its ConsumeMessageTraceHook; Shutdown flushes it
	// last.
	enableTrace      bool
	traceMsgBatchNum int
	traceTopic       string
	traceDispatcher  *AsyncTraceDispatcher

	instance    *Instance
	pullAPI     *pullAPI
	offsetStore OffsetStore

	started   bool
	stopCh    chan struct{}
	stopOnce  sync.Once
	startTime time.Time

	// processQueueTable is Java's ProcessQueueTable: the queues this instance
	// currently owns. Membership is also "which queues may be pulled", so every
	// path that drops a queue must go through retireQueueLocked.
	processQueueTable map[common.MessageQueue]*processQueue
	queueStop         map[common.MessageQueue]chan struct{}
	assigned          []common.MessageQueue

	// popQueueTable is the POP counterpart of processQueueTable. Only one of the
	// two is ever non-empty, because the mode is fixed for the consumer's
	// lifetime (Java's mode is per (group,topic) and read from the assignment,
	// but a single consumer cannot be half-pulled and half-popped without a
	// second offset store).
	popQueueTable map[common.MessageQueue]*popProcessQueue

	// offsetTable is the pull cursor (Java's PullRequest.nextOffset), distinct
	// from the consumed offset the store holds.
	//
	// There is deliberately NO consumer-side copy of the pullFromWhichNode
	// table: Java keeps exactly one, inside PullAPIWrapper, and a second copy
	// here would be a second source of truth for which broker to ask. It is
	// reached through c.pullAPI.
	offsetTable   map[common.MessageQueue]int64
	frozenOffsets map[common.MessageQueue]bool
	queueEpoch    map[common.MessageQueue]uint64
	msgAccCnt     map[common.MessageQueue]int64

	rebalanceNow chan struct{}
	dispatchSem  chan struct{}

	// draining stops new batches from starting during shutdown; inFlight counts
	// the registered batches (listener call + its send-back) that must land
	// before the instance is torn down. Shutdown flips draining under drainMu —
	// after that flip no new Add can race in — and then waits on inFlight.
	drainMu  sync.Mutex
	draining bool
	inFlight sync.WaitGroup

	innerProducerMu sync.Mutex
	producer        *DefaultMQProducer

	flowControlTriggered int64
	consumedCount        int64
}

// NewDefaultMQPushConsumer builds a consumer. The group must not be blank.
func NewDefaultMQPushConsumer(consumerGroup string) (*DefaultMQPushConsumer, error) {
	if strings.TrimSpace(consumerGroup) == "" {
		return nil, common.ClientError("consumerGroup is empty")
	}
	return &DefaultMQPushConsumer{
		consumerGroup:                 consumerGroup,
		instanceName:                  common.DefaultInstanceName,
		messageModel:                  MessageModelClustering,
		consumeFromWhere:              ConsumeFromWhereLastOffset,
		consumeTimestamp:              defaultConsumeTimestamp(),
		consumeThreadMin:              defaultConsumeThreadMin,
		consumeThreadMax:              defaultConsumeThreadMax,
		adjustThreadPoolNumsThreshold: defaultAdjustThreadPoolNumsThresh,
		consumeConcurrentlyMaxSpan:    defaultConsumeConcurrentlyMaxSpan,
		pullThresholdForQueue:         defaultPullThresholdForQueue,
		pullThresholdSizeForQueue:     defaultPullThresholdSizeForQueue,
		pullThresholdForTopic:         defaultPullThresholdForTopic,
		pullThresholdSizeForTopic:     defaultPullThresholdSizeForTopic,
		pullInterval:                  defaultPullInterval,
		pullTimeoutMillis:             defaultPullTimeoutMillis,
		pullSuspendTimeoutMillis:      defaultPullSuspendTimeoutMillis,
		consumeMessageBatchMaxSize:    defaultConsumeMessageBatchMaxSize,
		pullBatchSize:                 defaultPullBatchSize,
		pullBatchSizeInBytes:          defaultPullBatchSizeInBytes,
		maxReconsumeTimes:             defaultMaxReconsumeTimes,
		suspendCurrentQueueTimeMs:     defaultSuspendCurrentQueueTimeMs,
		consumeTimeout:                defaultConsumeTimeout,
		traceMsgBatchNum:              defaultTraceMsgBatchNum,
		popThresholdForQueue:          defaultPopThresholdForQueue,
		popInvisibleTime:              defaultPopInvisibleTime,
		popBatchNums:                  defaultPopBatchNums,
		allocateStrategy:              AllocateMessageQueueAveragely{},
		subscription:                  map[string]*remoting.SubscriptionData{},
		processQueueTable:             map[common.MessageQueue]*processQueue{},
		popQueueTable:                 map[common.MessageQueue]*popProcessQueue{},
		queueStop:                     map[common.MessageQueue]chan struct{}{},
		offsetTable:                   map[common.MessageQueue]int64{},
		frozenOffsets:                 map[common.MessageQueue]bool{},
		queueEpoch:                    map[common.MessageQueue]uint64{},
		msgAccCnt:                     map[common.MessageQueue]int64{},
		rebalanceNow:                  make(chan struct{}, 1),
		stopCh:                        make(chan struct{}),
	}, nil
}

// MustNewDefaultMQPushConsumer panics on a bad group (builder convenience).
func MustNewDefaultMQPushConsumer(consumerGroup string) *DefaultMQPushConsumer {
	c, err := NewDefaultMQPushConsumer(consumerGroup)
	if err != nil {
		panic(err)
	}
	return c
}

// defaultConsumeTimestamp is Java's "30 minutes ago" in yyyyMMddHHmmss.
func defaultConsumeTimestamp() string {
	return time.Now().Add(-30 * time.Minute).Format("20060102150405")
}

// ---------------------------------------------------------------- config

// ConsumerGroup returns the (possibly namespace-wrapped) group.
func (c *DefaultMQPushConsumer) ConsumerGroup() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumerGroup
}

// ClientID returns the client id (empty before Start).
func (c *DefaultMQPushConsumer) ClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

// SetNameServerAddr parses "host:port;host:port".
func (c *DefaultMQPushConsumer) SetNameServerAddr(addr string) {
	var addrs []string
	for _, a := range strings.Split(addr, ";") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	c.SetNameServerAddresses(addrs)
}

// SetNameServerAddresses sets the name server list.
func (c *DefaultMQPushConsumer) SetNameServerAddresses(addrs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nameServerAddrs = append([]string(nil), addrs...)
}

// NameServerAddresses snapshots the list.
func (c *DefaultMQPushConsumer) NameServerAddresses() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.nameServerAddrs...)
}

// SetNamespace sets the namespace (the group is prefixed in Start).
func (c *DefaultMQPushConsumer) SetNamespace(ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespace = ns
}

// SetInstanceName sets the instance name.
func (c *DefaultMQPushConsumer) SetInstanceName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.instanceName = name
}

// SetUnitName / SetUnitMode / SetEnableStreamRequestType mirror ClientConfig.
func (c *DefaultMQPushConsumer) SetUnitName(unit string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unitName = unit
}

// SetUnitMode toggles unit mode (rides heartbeats and request headers).
func (c *DefaultMQPushConsumer) SetUnitMode(mode bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unitMode = mode
}

// SetEnableStreamRequestType adds the @STREAM clientId suffix and ReqT field.
func (c *DefaultMQPushConsumer) SetEnableStreamRequestType(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streamRequest = enable
}

// SetRpcHook installs the user's RPC hook (typically an ACL hook built with
// remoting.NewAclClientRPCHook). Without it a consumer cannot authenticate
// against an ACL-enabled cluster.
func (c *DefaultMQPushConsumer) SetRpcHook(hook remoting.RPCHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rpcHook = hook
}

// SetNamespaceV2 mirrors ClientConfig#setNamespaceV2: the SERVER-side namespace
// sent as `ns`/`nsd` by NamespaceRpcHook, as opposed to SetNamespace, which
// mangles topic names client-side.
func (c *DefaultMQPushConsumer) SetNamespaceV2(ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespaceV2 = ns
}

// SetTLSEnable opts into TLS (nil falls back to ROCKETMQ_TLS_ENABLE).
func (c *DefaultMQPushConsumer) SetTLSEnable(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tlsEnable = &enable
}

// SetMessageModel selects CLUSTERING (default) or BROADCASTING.
func (c *DefaultMQPushConsumer) SetMessageModel(model string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messageModel = model
}

// SetConsumeFromWhere selects the CONSUME_FROM_* policy.
func (c *DefaultMQPushConsumer) SetConsumeFromWhere(where string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeFromWhere = where
}

// SetConsumeTimestamp sets the CONSUME_FROM_TIMESTAMP start (yyyyMMddHHmmss).
func (c *DefaultMQPushConsumer) SetConsumeTimestamp(ts string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeTimestamp = ts
}

// SetConsumeThreadNums sets both bounds (the old 4.x convenience setter).
//
// The three thread setters deliberately do NOT clamp: Java assigns the raw
// value and lets checkConfig reject it at Start with
// "consumeThreadMin Out of range [1, 1000]". Clamping here would silently turn
// an invalid configuration into a valid one and make the range check dead code.
func (c *DefaultMQPushConsumer) SetConsumeThreadNums(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeThreadMin, c.consumeThreadMax, c.corePoolSize = n, n, n
}

// SetConsumeThreadMin bounds the consume pool from below (and sets core).
func (c *DefaultMQPushConsumer) SetConsumeThreadMin(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeThreadMin = n
	c.corePoolSize = n
}

// SetConsumeThreadMax bounds the consume pool from above.
func (c *DefaultMQPushConsumer) SetConsumeThreadMax(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeThreadMax = n
}

// SetAdjustThreadPoolNumsThreshold mirrors the Java knob.
func (c *DefaultMQPushConsumer) SetAdjustThreadPoolNumsThreshold(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.adjustThreadPoolNumsThreshold = v
}

// CorePoolSize reads the effective consume concurrency.
func (c *DefaultMQPushConsumer) CorePoolSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.corePoolSize > 0 {
		return c.corePoolSize
	}
	return c.consumeThreadMin
}

// UpdateCorePoolSize is Java AbstractConsumeMessageService#updateCorePoolSize.
//
// The guards are copied verbatim (:63-71): ownsConsumeExecutor (always true
// here) && 0 < n <= Short.MAX_VALUE && n < consumeThreadMax. Any failure is
// SILENTLY ignored, exactly like Java. The return value exists only so tests
// can assert whether it took effect.
func (c *DefaultMQPushConsumer) UpdateCorePoolSize(n int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n <= 0 || n > math.MaxInt16 {
		return false
	}
	if n >= c.consumeThreadMax {
		return false
	}
	c.corePoolSize = n
	return true
}

// SetPullBatchSize sets how many messages one pull may return.
func (c *DefaultMQPushConsumer) SetPullBatchSize(n int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullBatchSize = n
}

// SetPullBatchSizeInBytes sets the byte ceiling for one pull.
func (c *DefaultMQPushConsumer) SetPullBatchSizeInBytes(n int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullBatchSizeInBytes = n
}

// SetPullInterval adds a delay between pulls (0 = back to back).
func (c *DefaultMQPushConsumer) SetPullInterval(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullInterval = ms
}

// SetPullTimeoutMillis sets the pull RPC timeout.
func (c *DefaultMQPushConsumer) SetPullTimeoutMillis(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullTimeoutMillis = ms
}

// SetPullSuspendTimeoutMillis sets the long-poll suspend budget.
func (c *DefaultMQPushConsumer) SetPullSuspendTimeoutMillis(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullSuspendTimeoutMillis = ms
}

// SetConsumeMessageBatchMaxSize sets the listener batch size.
func (c *DefaultMQPushConsumer) SetConsumeMessageBatchMaxSize(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeMessageBatchMaxSize = n
}

// SetPostSubscriptionWhenPull sends the expression on every pull (off by
// default; the broker then does not filter and the client's second-stage tag
// filter covers it).
func (c *DefaultMQPushConsumer) SetPostSubscriptionWhenPull(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postSubscriptionWhenPull = enable
}

// ---------------------------------------------------------------- pop config

// SetPopMode turns POP consumption on for this consumer.
//
// This is the CLIENT half of the switch. Java's mode lives on the broker
// (MessageRequestMode per (group, topic)) and the client learns it from
// MessageQueueAssignment.mode during rebalance; the classic client has no
// setter for it. A consumer that only sets this flag keeps popping a broker
// that still serves the group in PULL mode, which is why Start also sends
// SET_MESSAGE_REQUEST_MODE(401) for every subscribed topic — see
// enablePopModeOnBroker.
//
// A POP consumer must not be orderly: Java leaves orderly POP as a TODO stub and
// mixing the two here would need a queue lock the POP protocol does not have.
func (c *DefaultMQPushConsumer) SetPopMode(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.popMode = enable
}

// IsPopMode reports whether POP mode is on.
func (c *DefaultMQPushConsumer) IsPopMode() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.popMode
}

// SetPopInvisibleTime sets how long a popped batch stays invisible (ms). Java
// requires [5000, 300000]; out of range fails at Start.
func (c *DefaultMQPushConsumer) SetPopInvisibleTime(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.popInvisibleTime = ms
}

// SetPopBatchNums sets the max messages per POP. Java requires [1, 32].
func (c *DefaultMQPushConsumer) SetPopBatchNums(n int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.popBatchNums = n
}

// SetPopThresholdForQueue sets the outstanding-ACK debt that trips per-queue
// flow control (Java default 96).
func (c *DefaultMQPushConsumer) SetPopThresholdForQueue(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.popThresholdForQueue = n
}

// SetPopShareQueueNum sets how many following consumers in the cid list may
// share this consumer's queues (forwarded by SET_MESSAGE_REQUEST_MODE).
func (c *DefaultMQPushConsumer) SetPopShareQueueNum(n int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.popShareQueueNum = n
}

// SetPause mirrors Java DefaultMQPushConsumerImpl.setPause (driven by the
// SUSPEND_CONSUMER admin request). A paused consumer keeps its assignment but
// stops issuing requests.
func (c *DefaultMQPushConsumer) SetPause(pause bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = pause
}

// IsPaused reports the pause flag.
func (c *DefaultMQPushConsumer) IsPaused() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused
}

// SetMaxReconsumeTimes sets the retry ceiling (-1 = Java default).
func (c *DefaultMQPushConsumer) SetMaxReconsumeTimes(n int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxReconsumeTimes = n
}

// SetConsumeTimeout sets the minutes after which a stuck message is sent back.
func (c *DefaultMQPushConsumer) SetConsumeTimeout(minutes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeTimeout = minutes
}

// SetSuspendCurrentQueueTimeMillis sets the orderly suspend default.
func (c *DefaultMQPushConsumer) SetSuspendCurrentQueueTimeMillis(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.suspendCurrentQueueTimeMs = ms
}

// SetConsumeConcurrentlyMaxSpan sets the offset-span flow-control threshold.
func (c *DefaultMQPushConsumer) SetConsumeConcurrentlyMaxSpan(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeConcurrentlyMaxSpan = v
}

// SetPullThresholdForQueue / Size / Topic variants.
func (c *DefaultMQPushConsumer) SetPullThresholdForQueue(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullThresholdForQueue = v
}

// SetPullThresholdSizeForQueue sets the per-queue byte threshold (MB).
func (c *DefaultMQPushConsumer) SetPullThresholdSizeForQueue(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullThresholdSizeForQueue = v
}

// SetPullThresholdForTopic sets the per-topic count threshold (-1 = off).
func (c *DefaultMQPushConsumer) SetPullThresholdForTopic(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullThresholdForTopic = v
}

// SetPullThresholdSizeForTopic sets the per-topic byte threshold (-1 = off).
func (c *DefaultMQPushConsumer) SetPullThresholdSizeForTopic(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullThresholdSizeForTopic = v
}

// SetAllocateMessageQueueStrategy installs the rebalance strategy.
func (c *DefaultMQPushConsumer) SetAllocateMessageQueueStrategy(s AllocateMessageQueueStrategy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.allocateStrategy = s
}

// SetMessageQueueListener installs the assignment-change callback.
func (c *DefaultMQPushConsumer) SetMessageQueueListener(l MessageQueueListener) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messageQueueListener = l
}

// RegisterConsumeMessageHook appends a consume hook.
func (c *DefaultMQPushConsumer) RegisterConsumeMessageHook(hook ConsumeMessageHook) {
	if hook == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeMessageHookList = append(c.consumeMessageHookList, hook)
}

// ---------------------------------------------------------------- trace

// SetEnableTrace turns on message tracing (Java setEnableTrace): Start then
// brings up a CONSUME-mode AsyncTraceDispatcher whose ConsumeMessageTraceHook
// records every delivered batch. Off by default.
func (c *DefaultMQPushConsumer) SetEnableTrace(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enableTrace = enable
}

// IsEnableTrace is Java isEnableTrace.
func (c *DefaultMQPushConsumer) IsEnableTrace() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enableTrace
}

// SetTraceMsgBatchNum sets how many trace records are batched before a flush
// (the dispatcher caps it at 20).
func (c *DefaultMQPushConsumer) SetTraceMsgBatchNum(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traceMsgBatchNum = n
}

// SetTraceTopic overrides the trace topic (default common.TraceTopic).
func (c *DefaultMQPushConsumer) SetTraceTopic(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traceTopic = topic
}

// TraceDispatcher exposes the running dispatcher, or nil when tracing is off.
func (c *DefaultMQPushConsumer) TraceDispatcher() *AsyncTraceDispatcher {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.traceDispatcher
}

// RegisterFilterMessageHook appends a pre-delivery filter hook.
func (c *DefaultMQPushConsumer) RegisterFilterMessageHook(hook FilterMessageHook) {
	if hook == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.filterMessageHookList = append(c.filterMessageHookList, hook)
}

// Subscribe registers a tag subscription.
func (c *DefaultMQPushConsumer) Subscribe(topic, subExpression string) error {
	return c.SubscribeWithSelector(topic, byTagSelector(subExpression))
}

// SubscribeWithSelector registers a subscription with an explicit expression
// type. An empty expression defaults to "*".
func (c *DefaultMQPushConsumer) SubscribeWithSelector(topic string, selector MessageSelector) error {
	if strings.TrimSpace(topic) == "" {
		return common.ClientError("subscription topic is empty")
	}
	expr := selector.Expression
	if expr == "" {
		expr = "*"
	}
	exprType := selector.Type
	if exprType == "" {
		exprType = remoting.ExpressionTypeTag
	}
	sub := remoting.NewSubscriptionData(topic, expr)
	sub.ExpressionType = exprType
	if exprType == remoting.ExpressionTypeTag {
		built, err := remoting.FilterAPI{}.BuildSubscriptionData(topic, expr)
		if err != nil {
			return err
		}
		sub.TagsSet = built.TagsSet
		sub.CodeSet = built.CodeSet
		sub.SubString = built.SubString
		sub.ClassFilterMode = built.ClassFilterMode
	} else {
		sub.ClassFilterMode = exprType == remoting.ExpressionTypeClassFilter
	}
	c.mu.Lock()
	_, existed := c.subscription[topic]
	c.subscription[topic] = sub
	started := c.started
	c.mu.Unlock()
	if existed && started {
		// Java's RebalancePushImpl.messageQueueChanged: re-run immediately so a
		// changed expression takes effect without waiting out the 20s timer.
		c.RebalanceImmediately()
	}
	return nil
}

// Unsubscribe drops a topic's subscription.
func (c *DefaultMQPushConsumer) Unsubscribe(topic string) {
	c.mu.Lock()
	_, existed := c.subscription[topic]
	delete(c.subscription, topic)
	started := c.started
	c.mu.Unlock()
	if existed && started {
		c.RebalanceImmediately()
	}
}

// Subscriptions snapshots the subscription set (sorted for stable heartbeats).
func (c *DefaultMQPushConsumer) Subscriptions() []*remoting.SubscriptionData {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscriptionsLocked()
}

func (c *DefaultMQPushConsumer) subscriptionsLocked() []*remoting.SubscriptionData {
	topics := make([]string, 0, len(c.subscription))
	for t := range c.subscription {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	out := make([]*remoting.SubscriptionData, 0, len(topics))
	for _, t := range topics {
		out = append(out, c.subscription[t])
	}
	return out
}

// SetMessageListener installs the listener. It must implement exactly one of
// MessageListenerConcurrently / MessageListenerOrderly.
func (c *DefaultMQPushConsumer) SetMessageListener(listener any) error {
	orderly := false
	switch listener.(type) {
	case MessageListenerConcurrently:
	case MessageListenerOrderly:
		orderly = true
	default:
		return common.ClientError("listener must implement MessageListenerConcurrently or MessageListenerOrderly")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listener = listener
	c.orderly = orderly
	return nil
}

// SetConcurrentlyListener installs a concurrent listener.
func (c *DefaultMQPushConsumer) SetConcurrentlyListener(l MessageListenerConcurrently) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listener = l
	c.orderly = false
}

// SetOrderlyListener installs an orderly listener.
func (c *DefaultMQPushConsumer) SetOrderlyListener(l MessageListenerOrderly) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listener = l
	c.orderly = true
}

// IsOrderly reports the consume mode.
func (c *DefaultMQPushConsumer) IsOrderly() bool { return c.isOrderly() }

func (c *DefaultMQPushConsumer) isOrderly() bool { return c.orderly }

// IsStarted reports the lifecycle state.
func (c *DefaultMQPushConsumer) IsStarted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

// ------------------------------------------------- instance.Consumer interface

// ConsumeType is always CONSUME_PASSIVELY for a push consumer.
func (c *DefaultMQPushConsumer) ConsumeType() string { return ConsumeTypePassively }

// MessageModel feeds ConsumerData.
func (c *DefaultMQPushConsumer) MessageModel() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.messageModel
}

// ConsumeFromWhere feeds ConsumerData.
func (c *DefaultMQPushConsumer) ConsumeFromWhere() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumeFromWhere
}

// IsUnitMode feeds ConsumerData.
func (c *DefaultMQPushConsumer) IsUnitMode() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unitMode
}

// RebalanceImmediately is the NOTIFY_CONSUMER_IDS_CHANGED(40) fan-out target.
// It must only signal — this runs on the remoting read goroutine, and any
// synchronous request here would deadlock the connection.
func (c *DefaultMQPushConsumer) RebalanceImmediately() {
	select {
	case c.rebalanceNow <- struct{}{}:
	default:
	}
}

// AdjustThreadPool runs the elasticity sweep.
//
// Java's automatic inc/dec is an EMPTY implementation in 5.5.1
// (AbstractConsumeMessageService:70-75). The computation is kept for
// observability; do NOT "fix it by accident".
func (c *DefaultMQPushConsumer) AdjustThreadPool() {
	total := c.computeAccumulationTotal()
	if total > c.adjustThreadPoolNumsThreshold {
		common.LogWarnf("the consumer [%s] accumulation is %d, exceeds the threshold %d (auto adjust is a no-op in Java 5.x)",
			c.consumerGroup, total, c.adjustThreadPoolNumsThreshold)
	}
}

// computeAccumulationTotal is Java computeAccumulationTotal: the sum of every
// queue's msgAccCnt.
func (c *DefaultMQPushConsumer) computeAccumulationTotal() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total int64
	for _, v := range c.msgAccCnt {
		total += v
	}
	return total
}

// MsgAccCnt reads one queue's backlog (or the total when mq is nil).
func (c *DefaultMQPushConsumer) MsgAccCnt(mq *common.MessageQueue) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if mq == nil {
		var total int64
		for _, v := range c.msgAccCnt {
			total += v
		}
		return total
	}
	return c.msgAccCnt[*mq]
}

// PersistConsumerOffset is the periodic flush
// (Java MQClientInstance#persistAllConsumerOffset ->
// offsetStore.persistAll(processQueueTable.keySet())).
func (c *DefaultMQPushConsumer) PersistConsumerOffset() error {
	c.mu.Lock()
	store := c.offsetStore
	mqs := make([]common.MessageQueue, 0, len(c.processQueueTable))
	for mq := range c.processQueueTable {
		mqs = append(mqs, mq)
	}
	started := c.started
	c.mu.Unlock()
	if store == nil || !started {
		return nil
	}
	sort.Slice(mqs, func(i, j int) bool { return mqs[i].CompareTo(mqs[j]) < 0 })
	return store.PersistAll(mqs)
}

// GetConsumerStatus answers GET_CONSUMER_STATUS_FROM_CLIENT(221): the CONSUMED
// offsets (Java returns offsetStore.cloneOffsetTable(topic), not the pull
// cursor).
func (c *DefaultMQPushConsumer) GetConsumerStatus(topic *string) remoting.MQOffsetTable {
	c.mu.Lock()
	store := c.offsetStore
	mqs := make([]common.MessageQueue, 0, len(c.processQueueTable))
	for mq := range c.processQueueTable {
		mqs = append(mqs, mq)
	}
	c.mu.Unlock()
	sort.Slice(mqs, func(i, j int) bool { return mqs[i].CompareTo(mqs[j]) < 0 })

	c.mu.Lock()
	defer c.mu.Unlock()
	var out remoting.MQOffsetTable
	for _, mq := range mqs {
		if topic != nil && *topic != "" && mq.Topic != *topic {
			continue
		}
		var offset int64 = -1
		if store != nil {
			if v, err := store.ReadOffset(mq, ReadFromMemory); err == nil {
				offset = v
			}
		}
		if offset < 0 {
			continue
		}
		out = append(out, remoting.MQOffsetTableEntry{Queue: mq, Offset: offset})
	}
	return out
}

// ResetOffset handles RESET_CONSUMER_CLIENT_OFFSET(220).
//
// Java's four steps: pq.setDropped(true) + pq.clear() (in-flight acks and the
// buffer both die) -> wait up to RESET_OFFSET_MAX_WAIT (10s) for concurrent
// consumption to finish -> updateConsumeOffset + removeUnnecessaryMessageQueue
// (persist now, unlock if orderly) -> drop the queue so rebalance rebuilds it at
// the new offset.
//
// This port keeps the new offset in the store, retires the queue (epoch bump =
// setDropped) and persists. The wait is shortened to 200ms on purpose: the epoch
// already invalidates every in-flight ack, so the delay is not what prevents the
// race.
func (c *DefaultMQPushConsumer) ResetOffset(topic string, table remoting.MQOffsetTable) {
	if table == nil {
		return
	}
	type pending struct {
		mq     common.MessageQueue
		offset int64
	}
	var todo []pending
	c.mu.Lock()
	for _, entry := range table {
		mq := entry.Queue
		if topic != "" && mq.Topic != topic {
			continue
		}
		if _, ok := c.processQueueTable[mq]; !ok {
			continue
		}
		if c.offsetStore != nil {
			c.offsetStore.UpdateOffset(mq, entry.Offset, false)
		}
		todo = append(todo, pending{mq: mq, offset: entry.Offset})
	}
	revoked := make([]revokedQueue, 0, len(todo))
	for _, p := range todo {
		q := c.retireQueueLocked(p.mq)
		q.offset, q.hasOffset = p.offset, true
		revoked = append(revoked, q)
	}
	c.mu.Unlock()
	if len(revoked) == 0 {
		return
	}
	select {
	case <-c.stopCh:
	case <-time.After(200 * time.Millisecond):
	}
	c.persistRevoked(revoked)
	c.RebalanceImmediately()
	go c.doRebalance()
	common.LogInfof("reset offset applied, group=%s topic=%s queues=%d", c.consumerGroup, topic, len(revoked))
}

// ---------------------------------------------------------------- lifecycle

// Start brings the consumer up following Java DefaultMQPushConsumerImpl.start:
// validate everything locally, refresh routes, check the subscription at the
// broker, send one heartbeat, then rebalance BEFORE the consume loops start.
func (c *DefaultMQPushConsumer) Start() error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	if c.namespace != "" {
		// Must run BEFORE the retry topic is derived: the retry topic is
		// %RETRY% + the WRAPPED group name.
		c.consumerGroup = common.WrapNamespace(c.namespace, c.consumerGroup)
	}
	if err := common.CheckGroup(c.consumerGroup); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.consumerGroup == common.DefaultConsumerGroup {
		group := c.consumerGroup
		c.mu.Unlock()
		return common.ClientError(fmt.Sprintf(
			"consumerGroup can not equal %s, please specify another one.", group))
	}
	if len(c.nameServerAddrs) == 0 {
		c.mu.Unlock()
		return common.ClientError("name server address is not set")
	}
	if len(c.subscription) == 0 {
		c.mu.Unlock()
		return common.ClientError("subscription is not set, call subscribe() first")
	}
	if c.listener == nil {
		c.mu.Unlock()
		return common.ClientError("message listener is not set")
	}
	// Both helpers below have a *Locked variant because Start() is already
	// inside its own critical section here: calling the locking wrapper would
	// self-deadlock, and it did.
	if _, err := c.consumeTimestampMillisLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.allocateStrategy == nil {
		c.mu.Unlock()
		return common.ClientError("allocateMessageQueueStrategy is null")
	}
	if err := c.checkConfigRangesLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.messageModel == MessageModelClustering {
		// Java start:934-936 — only CLUSTERING rewrites DEFAULT to pid#nanotime
		// so that same-process broadcast consumers share one instance.
		c.instanceName = common.ChangeInstanceNameToPID(c.instanceName)
	}
	c.clientID = common.BuildMQClientID(common.CachedIPStr(), c.instanceName, c.unitName, c.streamRequest)
	cfg := NewClientInstanceConfig()
	instance := CreateOrGetInstance(c.clientID, c.nameServerAddrs, cfg)
	c.instance = instance
	namespaceV2 := c.namespaceV2
	userHook := c.rpcHook
	c.mu.Unlock()

	// Namespace + stream + user(ACL) + zone, in Java's order, once per instance.
	// Without the user hook a consumer cannot authenticate against an ACL
	// cluster; with it in the wrong position every signature fails to verify.
	instance.EnsureRPCHooks(namespaceV2, c.streamRequest, userHook)

	if err := instance.Start(); err != nil {
		c.mu.Lock()
		c.instance = nil
		c.mu.Unlock()
		return err
	}
	instance.RegisterConsumer(c.consumerGroup, c)

	// Clustering subscribes its retry topic automatically; the broker redelivers
	// into it. SUB_ALL keeps tagsSet EMPTY (see FilterAPI).
	c.mu.Lock()
	if c.messageModel != MessageModelBroadcasting {
		retryTopic := common.GetRetryTopic(c.consumerGroup)
		if _, ok := c.subscription[retryTopic]; !ok {
			sub, err := remoting.FilterAPI{}.BuildSubscriptionData(retryTopic, "*")
			if err != nil {
				c.mu.Unlock()
				return err
			}
			c.subscription[retryTopic] = sub
		}
	}
	if c.messageModel == MessageModelBroadcasting {
		c.offsetStore = NewLocalFileOffsetStore(c.clientID, c.consumerGroup)
	} else {
		c.offsetStore = NewRemoteBrokerOffsetStore(instance, c.consumerGroup)
	}
	store := c.offsetStore
	c.pullAPI = newPullAPI(instance, c.consumerGroup)
	c.pullAPI.unitMode = c.unitMode
	c.pullAPI.hooks = c.filterMessageHookList
	c.corePoolSize = c.consumeThreadMin
	c.dispatchSem = make(chan struct{}, maxInt(1, c.corePoolSize))
	c.stopCh = make(chan struct{})
	c.stopOnce = sync.Once{}
	c.drainMu.Lock()
	c.draining = false
	c.drainMu.Unlock()
	c.startTime = time.Now()
	c.started = true
	topics := make([]string, 0, len(c.subscription))
	for t := range c.subscription {
		topics = append(topics, t)
	}
	c.mu.Unlock()

	if err := store.Load(); err != nil {
		common.LogWarnf("load offset store failed for %s: %v", c.consumerGroup, err)
	}
	for _, topic := range topics {
		instance.RegisterTopicInUse(topic)
		if _, err := instance.UpdateTopicRouteInfoFromNameServer(topic, 5000, false); err != nil {
			common.LogDebugf("initial route refresh failed for %s: %v", topic, err)
		}
	}
	// Java start:1013-1020 — validate non-TAG subscriptions at the broker BEFORE
	// the first heartbeat. A broken SQL92 filter compiles to nothing broker-side
	// and ExpressionMessageFilter then admits EVERY message; only this call turns
	// it into a startup error.
	if err := instance.CheckClientInBroker(); err != nil {
		c.Shutdown()
		return err
	}
	// Heartbeat must precede rebalance: rebalance asks the broker for the
	// group's client list.
	if err := c.sendHeartbeatNow(); err != nil {
		common.LogDebugf("initial heartbeat failed: %v", err)
	}
	// The first assignment is computed synchronously; otherwise the pull loops
	// would spin on an empty assignment until the first timer tick.
	if err := c.doRebalance(); err != nil {
		common.LogWarnf("initial rebalance failed: %v", err)
	}

	// POP mode: tell the broker to serve this (group, topic) pair as POP. Java
	// does this out of band (mqadmin / console); doing it here keeps the client
	// self-contained. A failure is logged, not fatal — a broker that already
	// has the group in POP mode answers SUCCESS anyway, and an old broker
	// without POP support must not take the consumer down.
	if c.IsPopMode() {
		if err := c.SetMessageRequestModeOnBroker(3000); err != nil {
			common.LogWarnf("enable POP on broker failed for group %s: %v", c.consumerGroup, err)
		}
	}

	if c.IsPopMode() {
		// POP has no processQueue to dispatch from: the pop loops hand each
		// batch straight to the listener. It also has no expire sweep
		// (cleanExpiredMsg reads a buffer POP does not keep) and no queue lock
		// (orderly POP is rejected at config time).
		c.goLoop(c.rebalanceLoop)
		c.startTraceDispatcher()
		return nil
	}

	c.goLoop(c.dispatchLoop)
	// The suspend sweep only exists on the classic concurrent path: Java builds
	// it in ConsumeMessageConcurrentlyService, ProcessQueue.cleanExpiredMsg
	// returns immediately for orderly, and the POP service has no equivalent.
	if !c.isOrderly() {
		c.goLoop(c.cleanExpireLoop)
	}
	if c.isOrderly() && c.messageModel != MessageModelBroadcasting {
		c.goLoop(c.lockLoop)
	}
	c.goLoop(c.rebalanceLoop)
	c.startTraceDispatcher()
	return nil
}

// startTraceDispatcher is Java DefaultMQPushConsumer.start:180-190 (Python
// consumer.py `_start_trace_dispatcher`): with enableTrace on, build a CONSUME
// dispatcher, register its consume hook, then start it. Failures only log — a
// broken trace stack must never take the consumer down.
//
// It runs OUTSIDE c.mu on purpose: registering the hook takes c.mu itself.
func (c *DefaultMQPushConsumer) startTraceDispatcher() {
	c.mu.Lock()
	enable := c.enableTrace
	batchNum := c.traceMsgBatchNum
	topic := c.traceTopic
	group := c.consumerGroup
	nameSrv := strings.Join(c.nameServerAddrs, ";")
	rpcHook := c.rpcHook
	dispatcher := c.traceDispatcher
	if enable && dispatcher == nil {
		dispatcher = NewAsyncTraceDispatcher(group, TraceDispatcherConsume, batchNum, topic, rpcHook)
		dispatcher.SetHostConsumer(c)
		c.traceDispatcher = dispatcher
	}
	c.mu.Unlock()

	if enable && group != "" {
		c.RegisterConsumeMessageHook(NewConsumeMessageTraceHook(dispatcher))
	}
	if dispatcher != nil {
		if err := dispatcher.Start(nameSrv, AccessChannelLocal); err != nil {
			common.LogWarnf("trace dispatcher start failed: %v", err)
		}
	}
}

// shutdownTraceDispatcher flushes and joins the dispatcher. Called LAST from
// Shutdown, outside c.mu: it must wait for the final SubBefore/SubAfter records
// to land, and it sends through its own internal producer.
func (c *DefaultMQPushConsumer) shutdownTraceDispatcher() {
	c.mu.Lock()
	dispatcher := c.traceDispatcher
	c.traceDispatcher = nil
	c.mu.Unlock()
	if dispatcher != nil {
		dispatcher.Shutdown()
	}
}

// Shutdown unwinds in Java's order: persist offsets (while _started is still
// true), unlock if orderly, unregister at every broker, then stop the loops.
func (c *DefaultMQPushConsumer) Shutdown() {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return
	}
	c.stopOnce.Do(func() { close(c.stopCh) })
	instance := c.instance
	c.mu.Unlock()

	// Freeze new consume work right away: batches still in flight are waited
	// for below, anything the dispatch loops would have started next is handed
	// back instead (its offset was never advanced, so the broker redelivers).
	c.freezeInFlight()

	if err := c.PersistConsumerOffset(); err != nil {
		common.LogDebugf("persist offsets on shutdown failed: %v", err)
	}
	if c.isOrderly() && c.messageModel != MessageModelBroadcasting {
		c.unlockAssigned()
	}
	if instance != nil {
		// Graceful unregister: without it the broker's ConsumerManager keeps the
		// clientId until the channel scan (~120s) and keeps routing
		// rebalance notifications and transaction check-backs at a dead client.
		instance.UnregisterClientAllBrokers("", c.consumerGroup)
		instance.UnregisterConsumer(c.consumerGroup)
	}
	c.stopQueueLoops()

	// Wait (bounded) for in-flight batches — the listener call and its
	// send-back — while the instance is still alive. Without this window an
	// immediate process exit can cut a CONSUMER_SEND_MSG_BACK(36) short and the
	// message never reaches %RETRY%/%DLQ% (Python joins with 2s, Rust finalizes
	// within a 30s budget; this is the same contract).
	c.waitInFlightDrain()
	// Java persists a second time from MQClientInstance.shutdown
	// (persistAllConsumerOffset): offsets the drain just advanced must land too.
	if err := c.PersistConsumerOffset(); err != nil {
		common.LogDebugf("post-drain persist failed: %v", err)
	}

	c.mu.Lock()
	c.started = false
	producer := c.producer
	c.producer = nil
	c.mu.Unlock()
	if producer != nil {
		producer.Shutdown()
	}
	if instance != nil {
		instance.Shutdown()
		instance.DetachFromRegistryIfLastTenant()
	}
	// Trace last, mirroring Java's DefaultMQPushConsumer.shutdown ordering: the
	// dispatcher flushes what the (now drained) consume loops queued and waits
	// for those sends to land before returning.
	c.shutdownTraceDispatcher()
}

func (c *DefaultMQPushConsumer) goLoop(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				common.LogErrorf("consumer loop panicked: %v", r)
			}
		}()
		fn()
	}()
}

func (c *DefaultMQPushConsumer) stopQueueLoops() {
	c.mu.Lock()
	stops := make([]chan struct{}, 0, len(c.queueStop))
	for _, ch := range c.queueStop {
		stops = append(stops, ch)
	}
	c.queueStop = map[common.MessageQueue]chan struct{}{}
	c.mu.Unlock()
	for _, ch := range stops {
		close(ch)
	}
}

// freezeInFlight flips the draining flag: after it, no new consume batch or
// expire sweep starts, and everything already running is accounted in inFlight.
func (c *DefaultMQPushConsumer) freezeInFlight() {
	c.drainMu.Lock()
	c.draining = true
	c.drainMu.Unlock()
}

// beginInFlight registers one unit of consume work. Once Shutdown has frozen
// the consumer it returns false and the caller must hand the batch back
// (RequeueBatch), never consume it.
//
// The Add happens under the same mutex as the draining flip, so no Add can
// race in after Shutdown observed the freeze — waitInFlightDrain's Wait is
// therefore not racy against a zero counter.
func (c *DefaultMQPushConsumer) beginInFlight() bool {
	c.drainMu.Lock()
	defer c.drainMu.Unlock()
	if c.draining {
		return false
	}
	c.inFlight.Add(1)
	return true
}

// shutdownDrainBudget bounds how long Shutdown waits for in-flight consume
// work (the listener call plus its send-back). Bounded so a hung listener
// cannot hang shutdown, generous enough for a send-back round trip.
var shutdownDrainBudget = 30 * time.Second

// waitInFlightDrain waits (bounded) for the batches registered by beginInFlight
// to finish. Must be called after freezeInFlight, while the client instance is
// still alive — that window is what lets their send-backs land.
func (c *DefaultMQPushConsumer) waitInFlightDrain() {
	drained := make(chan struct{})
	go func() {
		c.inFlight.Wait()
		close(drained)
	}()
	timer := time.NewTimer(shutdownDrainBudget)
	defer timer.Stop()
	select {
	case <-drained:
	case <-timer.C:
		common.LogWarnf("consumer shutdown drain timed out after %s, group=%s; in-flight batches detached",
			shutdownDrainBudget, c.consumerGroup)
	}
}

// sendHeartbeatNow forces one heartbeat; the instance's 30s task continues
// afterwards.
func (c *DefaultMQPushConsumer) sendHeartbeatNow() error {
	c.mu.Lock()
	instance := c.instance
	c.mu.Unlock()
	if instance == nil {
		return common.ClientError("consumer not started")
	}
	instance.SendHeartbeatToAllBroker(5000)
	return nil
}

// consumeTimestampMillis reads the configured value and parses it.
//
// NOT safe to call with c.mu held — Start() validates the timestamp while still
// inside its own critical section, and must use consumeTimestampMillisLocked.
func (c *DefaultMQPushConsumer) consumeTimestampMillis() (int64, error) {
	c.mu.Lock()
	raw := c.consumeTimestamp
	c.mu.Unlock()
	return parseConsumeTimestamp(raw)
}

// consumeTimestampMillisLocked is the c.mu-held variant of
// consumeTimestampMillis.
func (c *DefaultMQPushConsumer) consumeTimestampMillisLocked() (int64, error) {
	return parseConsumeTimestamp(c.consumeTimestamp)
}

// parseConsumeTimestamp is Java UtilAll.parseDate(consumeTimestamp,
// yyyyMMddHHmmss) in the LOCAL wall clock.
//
// A parse failure must HARD FAIL (Java throws in checkConfig): silently falling
// back to "now - 30min" would move the start point with nobody noticing.
func parseConsumeTimestamp(raw string) (int64, error) {
	if len(raw) != 14 {
		return 0, common.ClientError(fmt.Sprintf(
			"consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received %s", raw))
	}
	t, err := time.ParseInLocation("20060102150405", raw, time.Local)
	if err != nil {
		return 0, common.ClientError(fmt.Sprintf(
			"consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received %s", raw))
	}
	return t.UnixMilli(), nil
}

// checkConfigRanges is the locking wrapper; see checkConfigRangesLocked.
func (c *DefaultMQPushConsumer) checkConfigRanges() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkConfigRangesLocked()
}

// checkConfigRangesLocked is Java DefaultMQPushConsumerImpl#checkConfig's
// numeric section (:1099-1209) — same order, same bounds, same wording.
//
// These values drive the buffer watermarks and the pool size. Without the gate,
// a 0 or a 2^31-1 turns every pull into "backlogged" (the consumer silently
// stops receiving) or overflows the drop/flow-control arithmetic. Reporting it
// from the broker instead is several beats too late.
//
// pullThresholdForTopic / pullThresholdSizeForTopic use -1 as Java's "unset,
// use the per-queue thresholds" sentinel and are NOT range-checked.
//
// Caller must hold c.mu (Start() runs it inside its own critical section).
func (c *DefaultMQPushConsumer) checkConfigRangesLocked() error {
	switch {
	case c.consumeThreadMin < 1 || c.consumeThreadMin > 1000:
		return common.ClientError("consumeThreadMin Out of range [1, 1000]")
	case c.consumeThreadMax < 1 || c.consumeThreadMax > 1000:
		return common.ClientError("consumeThreadMax Out of range [1, 1000]")
	case c.consumeThreadMin > c.consumeThreadMax:
		return common.ClientError(fmt.Sprintf("consumeThreadMin (%d) is larger than consumeThreadMax (%d)",
			c.consumeThreadMin, c.consumeThreadMax))
	case c.consumeConcurrentlyMaxSpan < 1 || c.consumeConcurrentlyMaxSpan > 65535:
		return common.ClientError("consumeConcurrentlyMaxSpan Out of range [1, 65535]")
	case c.pullThresholdForQueue < 1 || c.pullThresholdForQueue > 65535:
		return common.ClientError("pullThresholdForQueue Out of range [1, 65535]")
	case c.pullThresholdForTopic != -1 && (c.pullThresholdForTopic < 1 || c.pullThresholdForTopic > 6553500):
		return common.ClientError("pullThresholdForTopic Out of range [1, 6553500]")
	case c.pullThresholdSizeForQueue < 1 || c.pullThresholdSizeForQueue > 1024:
		return common.ClientError("pullThresholdSizeForQueue Out of range [1, 1024]")
	case c.pullThresholdSizeForTopic != -1 && (c.pullThresholdSizeForTopic < 1 || c.pullThresholdSizeForTopic > 102400):
		return common.ClientError("pullThresholdSizeForTopic Out of range [1, 102400]")
	case c.pullInterval < 0 || c.pullInterval > 65535:
		return common.ClientError("pullInterval Out of range [0, 65535]")
	case c.consumeMessageBatchMaxSize < 1 || c.consumeMessageBatchMaxSize > 1024:
		return common.ClientError("consumeMessageBatchMaxSize Out of range [1, 1024]")
	case c.pullBatchSize < 1 || c.pullBatchSize > 1024:
		return common.ClientError("pullBatchSize Out of range [1, 1024]")
	case c.popInvisibleTime < minPopInvisibleTime || c.popInvisibleTime > maxPopInvisibleTime:
		return common.ClientError(fmt.Sprintf("popInvisibleTime Out of range [%d, %d]",
			minPopInvisibleTime, maxPopInvisibleTime))
	case c.popBatchNums <= 0 || c.popBatchNums > maxPopBatchNums:
		return common.ClientError(fmt.Sprintf("popBatchNums Out of range [1, %d]", maxPopBatchNums))
	case c.popMode && c.orderly:
		// Java: "POPTODO think of pop mode orderly implementation later." There
		// is no queue lock in the POP protocol to serialise on, so an orderly
		// POP consumer would silently lose ordering — refuse instead.
		return common.ClientError("pop mode does not support orderly consumption")
	}
	return nil
}

// ---------------------------------------------------------------- rebalance

// doRebalance is Java RebalanceImpl#rebalanceByTopic for every subscribed topic,
// then syncPullLoops.
//
// BROADCASTING: every queue is ours, no broker coordination.
// CLUSTERING: ask the broker for the group's client list -> sort cidAll and
// mqAll -> run the strategy -> take our slice.
//
// A missing consumer list KEEPS the current assignment (Java warns). Never
// degrade to "I own everything": co-instances would duplicate every message.
func (c *DefaultMQPushConsumer) doRebalance() error {
	c.mu.Lock()
	instance := c.instance
	started := c.started
	topics := make([]string, 0, len(c.subscription))
	for t := range c.subscription {
		topics = append(topics, t)
	}
	c.mu.Unlock()
	sort.Strings(topics)
	if instance == nil || !started {
		return nil
	}
	was := c.assignedSet()
	var assigned []common.MessageQueue
	for _, topic := range topics {
		info := instance.GetTopicSubscribeInfo(topic)
		if len(info) == 0 {
			common.LogDebugf("rebalance: no subscribe info for topic %s", topic)
		}
		if c.messageModel == MessageModelBroadcasting {
			assigned = append(assigned, info...)
			continue
		}
		mqAll := append([]common.MessageQueue(nil), info...)
		sort.Slice(mqAll, func(i, j int) bool { return mqAll[i].CompareTo(mqAll[j]) < 0 })
		if len(mqAll) == 0 {
			continue
		}
		cidAll, answered := instance.GetConsumerIDListByGroup(topic, c.consumerGroup, 5000)
		if !answered || len(cidAll) == 0 {
			common.LogDebugf("rebalance: no consumer id list for %s/%s, keep current", c.consumerGroup, topic)
			for mq := range was {
				if mq.Topic == topic {
					assigned = append(assigned, mq)
				}
			}
			continue
		}
		sort.Strings(cidAll)
		strategy := c.allocateStrategy
		if strategy == nil {
			strategy = AllocateMessageQueueAveragely{}
		}
		got := strategy.Allocate(c.consumerGroup, c.clientID, mqAll, cidAll)
		if got == nil {
			common.LogWarnf("allocate message queue returned nothing, strategy=%s group=%s",
				strategyName(strategy), c.consumerGroup)
		}
		assigned = append(assigned, got...)
	}
	sort.Slice(assigned, func(i, j int) bool { return assigned[i].CompareTo(assigned[j]) < 0 })
	c.mu.Lock()
	c.assigned = assigned
	c.mu.Unlock()
	now := c.assignedSet()
	if len(now) != len(was) {
		common.LogInfof("rebalance result changed, group=%s clientId=%s assigned=%d",
			c.consumerGroup, c.clientID, len(assigned))
	}
	// Resolve the initial offset for freshly assigned queues NOW, not lazily on
	// the first pull (see the package comment).
	//
	// POP has no client-side offset at all — the broker's revive queue is the
	// cursor — so this whole block only applies to the pull path. Resolving
	// offsets in POP mode would also issue QUERY_CONSUMER_OFFSET for a group
	// whose offsets are never committed, wasting a round trip per queue.
	if !c.IsPopMode() {
		for _, mq := range assigned {
			if _, already := was[mq]; already {
				continue
			}
			c.mu.Lock()
			_, has := c.offsetTable[mq]
			_, hasSub := c.subscription[mq.Topic]
			store := c.offsetStore
			c.mu.Unlock()
			if has || !hasSub {
				continue
			}
			next, err := c.computePullFromWhere(mq)
			if err != nil {
				common.LogDebugf("resolve initial offset for %v failed: %v", mq, err)
				continue
			}
			if next < 0 {
				continue
			}
			c.mu.Lock()
			if _, ok := c.offsetTable[mq]; !ok {
				c.offsetTable[mq] = next
			}
			c.mu.Unlock()
			if store != nil {
				store.UpdateOffset(mq, next, false)
			}
		}
	}
	c.syncPullLoops()
	return nil
}

type revokedQueue struct {
	mq        common.MessageQueue
	offset    int64
	hasOffset bool
}

// assignedSet snapshots the assignment.
func (c *DefaultMQPushConsumer) assignedSet() map[common.MessageQueue]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[common.MessageQueue]struct{}, len(c.assigned))
	for _, mq := range c.assigned {
		out[mq] = struct{}{}
	}
	return out
}

// assignedQueues snapshots the assignment as a slice.
func (c *DefaultMQPushConsumer) assignedQueues() []common.MessageQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]common.MessageQueue(nil), c.assigned...)
}

// AssignedQueueCount / AssignedQueues expose the current assignment.
func (c *DefaultMQPushConsumer) AssignedQueueCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.assigned)
}

// AssignedQueues is a copy of the current assignment.
func (c *DefaultMQPushConsumer) AssignedQueues() []common.MessageQueue { return c.assignedQueues() }

// SubscribeQueuesOf is the queues this client currently sees for one topic
// (Java MQClientInstance#getTopicSubscribeInfo). Rebalance reads the same
// source every round; this accessor exists so a live test can tell "the
// strategy split badly" apart from "the two instances saw different routes".
func (c *DefaultMQPushConsumer) SubscribeQueuesOf(topic string) []common.MessageQueue {
	c.mu.Lock()
	instance := c.instance
	c.mu.Unlock()
	if instance == nil {
		return nil
	}
	return instance.GetTopicSubscribeInfo(topic)
}

// ProcessQueueCount is the number of queues actually being pulled.
func (c *DefaultMQPushConsumer) ProcessQueueCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.processQueueTable)
}

// FlowControlTriggered counts flow-control pauses (live diagnostics).
func (c *DefaultMQPushConsumer) FlowControlTriggered() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flowControlTriggered
}

// syncPullLoops mirrors Java RebalanceImpl#updateProcessQueueTableInRebalance:
// retire what we no longer own, then start a pull goroutine per new queue.
//
// Revocation has three mandatory steps — persist the CONSUMED offset, drop the
// ProcessQueue (in-flight messages stop being consumed and go back to the new
// owner), and UNLOCK_BATCH_MQ for an orderly clustering consumer. Missing any
// one of them lets the old instance keep consuming the revoked queue's in-flight
// messages, duplicating against the new owner.
//
// Java also self-heals in the same pass: a queue still assigned to us whose pull
// has stalled beyond PULL_MAX_IDLE_TIME is retired too, and the add-branch
// immediately rebuilds it — that is how a dead consumer loop recovers.
//
// Order matters: retire -> settle -> add. Reversed, the new loop would start
// from a stale offset and write the smaller value back.
func (c *DefaultMQPushConsumer) syncPullLoops() {
	if c.IsPopMode() {
		c.syncPopLoops()
		return
	}
	current := map[common.MessageQueue]common.MessageQueue{}
	for _, mq := range c.assignedQueues() {
		current[mq] = mq
	}
	c.mu.Lock()
	var revoked []revokedQueue
	for mq, pq := range c.processQueueTable {
		if _, ok := current[mq]; !ok {
			revoked = append(revoked, c.retireQueueLocked(mq))
			continue
		}
		if c.started && pq.IsPullExpired() {
			// Java RebalanceImpl:449's warning text, kept for field debugging.
			common.LogErrorf("[BUG]doRebalance, %s, try remove unnecessary mq, %v, because pull is pause, so try to fixed it",
				c.consumerGroup, mq)
			revoked = append(revoked, c.retireQueueLocked(mq))
		}
	}
	c.mu.Unlock()
	if len(revoked) > 0 {
		c.persistRevoked(revoked)
	}

	c.mu.Lock()
	for mq := range current {
		if _, ok := c.processQueueTable[mq]; ok {
			continue
		}
		pq := newProcessQueue(c.isOrderly())
		c.processQueueTable[mq] = pq
		// A fresh ProcessQueue lifts the freeze (Java removeProcessQueue's
		// removeOffset): the rebuilt queue advances from the corrected offset.
		delete(c.frozenOffsets, mq)
		pq.TouchPull()
		// A rebuilt queue starts over from the master instead of the slave the
		// previous incarnation had drifted to.
		if c.pullAPI != nil {
			c.pullAPI.forgetPullFromWhichNode(mq)
		}
		stop := make(chan struct{})
		c.queueStop[mq] = stop
		go c.queuePullLoop(mq, stop)
	}
	c.mu.Unlock()

	if len(revoked) > 0 {
		common.LogInfof("queues revoked, group=%s count=%d", c.consumerGroup, len(revoked))
	}
	c.notifyQueueChanged()
}

// retireQueueLocked drops one queue's local state. Caller must hold c.mu.
//
// The epoch bump invalidates every in-flight batch's ack (Java's
// setDropped(true)); the freeze marker is KEPT until the queue is rebuilt, so a
// corrected offset cannot be overwritten by an old ack.
func (c *DefaultMQPushConsumer) retireQueueLocked(mq common.MessageQueue) revokedQueue {
	if stop, ok := c.queueStop[mq]; ok {
		close(stop)
		delete(c.queueStop, mq)
	}
	pq := c.processQueueTable[mq]
	offset, hasOffset := int64(0), false
	if c.offsetStore != nil {
		if v, err := c.offsetStore.ReadOffset(mq, ReadFromMemory); err == nil && v >= 0 {
			offset, hasOffset = v, true
		}
	}
	if pq != nil {
		pq.SetDropped()
		pq.Clear()
	}
	delete(c.processQueueTable, mq)
	delete(c.offsetTable, mq)
	delete(c.msgAccCnt, mq)
	if c.pullAPI != nil {
		c.pullAPI.forgetPullFromWhichNode(mq)
	}
	c.queueEpoch[mq]++
	return revokedQueue{mq: mq, offset: offset, hasOffset: hasOffset}
}

// retireQueue is retireQueueLocked with the lock taken for you.
func (c *DefaultMQPushConsumer) retireQueue(mq common.MessageQueue) revokedQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.retireQueueLocked(mq)
}

// persistRevoked is Java RebalanceImpl#removeUnnecessaryMessageQueue.
func (c *DefaultMQPushConsumer) persistRevoked(revoked []revokedQueue) {
	c.mu.Lock()
	store := c.offsetStore
	broadcast := c.messageModel == MessageModelBroadcasting
	clientID := c.clientID
	c.mu.Unlock()
	if store == nil {
		return
	}
	if broadcast {
		// Broadcast offsets live in a local file. Java persists BEFORE
		// removeOffset so the value stays on disk and a rebuilt queue can
		// continue from it; skipping this makes the queue rescan from
		// consumeFromWhere.
		if err := store.PersistAll(nil); err != nil {
			common.LogDebugf("persist local offsets on revoke failed: %v", err)
		}
		return
	}
	for _, q := range revoked {
		if q.hasOffset {
			// Java persist(mq): write the value now, do not wait for the 5s
			// periodic flush — the new owner may start pulling any moment.
			store.UpdateOffset(q.mq, q.offset, false)
			if err := store.Persist(q.mq); err != nil {
				common.LogDebugf("persist offset on revoke failed for %v: %v", q.mq, err)
			}
		}
		if c.isOrderly() && c.instance != nil {
			// Orderly: release the broker queue lock so the new owner can start.
			c.instance.UnlockBatchMQ(c.consumerGroup, clientID, []common.MessageQueue{q.mq}, 1000)
		}
	}
}

// notifyQueueChanged is Java RebalanceImpl#messageQueueChanged.
func (c *DefaultMQPushConsumer) notifyQueueChanged() {
	c.mu.Lock()
	listener := c.messageQueueListener
	instance := c.instance
	assigned := append([]common.MessageQueue(nil), c.assigned...)
	c.mu.Unlock()
	if listener == nil || instance == nil {
		return
	}
	byTopic := map[string][]common.MessageQueue{}
	for _, mq := range assigned {
		byTopic[mq.Topic] = append(byTopic[mq.Topic], mq)
	}
	for topic, divided := range byTopic {
		listener.MessageQueueChanged(topic, instance.GetTopicSubscribeInfo(topic), divided)
	}
}

// rebalanceLoop is Java RebalanceService (20s) plus the startup fast retry.
func (c *DefaultMQPushConsumer) rebalanceLoop() {
	for {
		startingUp := time.Since(c.startTime) < rebalanceStartupWindow
		interval := rebalanceInterval
		if startingUp && len(c.assignedQueues()) == 0 {
			interval = rebalanceIntervalDuringStartup
		}
		timer := time.NewTimer(interval)
		select {
		case <-c.stopCh:
			timer.Stop()
			return
		case <-c.rebalanceNow:
			timer.Stop()
		case <-timer.C:
		}
		if !c.IsStarted() {
			return
		}
		if err := c.doRebalance(); err != nil {
			common.LogDebugf("rebalance error: %v", err)
		}
	}
}

// computePullFromWhere is Java RebalancePushImpl#computePullFromWhere /
// computePullFromWhereWithException.
//
// READ_FROM_STORE first, then the per-policy fallback. Two items are
// deliberate: CONSUME_FROM_FIRST_OFFSET returns 0 WITHOUT querying minOffset
// (Java's comment says the offset gets fixed by the OFFSET_ILLEGAL path) — a
// minOffset round trip is not just slower, it goes through MQAdminImpl which
// only talks to the master, so with the master down a fresh consumer would
// receive nothing at all, while starting from 0 works through a slave and lets
// the broker correct the offset with PULL_OFFSET_MOVED; and a %RETRY% topic with
// no committed offset starts at 0 rather than maxOffset, because retried
// messages must all be retried.
func (c *DefaultMQPushConsumer) computePullFromWhere(mq common.MessageQueue) (int64, error) {
	c.mu.Lock()
	store := c.offsetStore
	instance := c.instance
	where := c.consumeFromWhere
	c.mu.Unlock()
	if store == nil {
		return -1, common.ClientError("consumer not started")
	}
	last := int64(-1)
	if v, err := store.ReadOffset(mq, ReadFromStore); err == nil {
		last = v
	}
	if last >= 0 {
		return last, nil
	}
	switch where {
	case ConsumeFromWhereFirstOffset:
		return 0, nil
	case ConsumeFromWhereTimestamp:
		if common.IsRetryTopic(mq.Topic) {
			return instance.GetMaxOffset(mq, 5000)
		}
		ts, err := c.consumeTimestampMillis()
		if err != nil {
			return -1, err
		}
		return instance.SearchOffsetByTimestamp(mq, ts, 5000)
	default:
		// CONSUME_FROM_LAST_OFFSET (and the two deprecated variants).
		if common.IsRetryTopic(mq.Topic) {
			return 0, nil
		}
		return instance.GetMaxOffset(mq, 5000)
	}
}
