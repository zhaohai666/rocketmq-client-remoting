// It sits between the push consumer and the classic pull consumer: the client
// still owns the pull (a background SHORT-poll loop over every assigned
// queue), but the CALLER pulls the results out at its own pace through Poll.
// Two modes share one engine:
//
//   - subscribe: Subscribe(topic, expr) → the background loop re-balances every
//     second with the AllocateMessageQueueStrategy and pulls what it got;
//   - assign: Assign(mqs) pins the queue set by hand — no rebalance at all
//     (SetSubExpressionForAssign still registers a TAG expression so the
//     heartbeat and the broker-side filter keep working).
//
// The two rules that are easy to get wrong and expensive to debug:
package client

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const (
	// DefaultLitePullBatchSize is pullBatchSize — messages per pull request. The
	// lite consumer's default is 10, not the push consumer's 32, and since the
	// aggregate gate budgets a whole batch per pending pull round it is also the
	// multiplier of that budget.
	DefaultLitePullBatchSize = int32(10)
	// DefaultLitePollTimeoutMillis is pollTimeoutMillis — how long Poll waits
	// for the buffer to become non-empty.
	DefaultLitePollTimeoutMillis = int64(5000)
	// DefaultLiteAutoCommitIntervalMillis is autoCommitIntervalMillis.
	DefaultLiteAutoCommitIntervalMillis = int64(5000)
	// MinLiteAutoCommitIntervalMillis is the shortest auto-commit interval the
	// knob accepts: a setting below it is ignored, and the interval already in
	// place keeps running.
	MinLiteAutoCommitIntervalMillis = int64(1000)
	// DefaultLitePullIntervalMillis is pullIntervalMillis — the idle backoff
	// between pull rounds when nothing came in.
	DefaultLitePullIntervalMillis = int64(50)
	// litePullTimeoutMillis is the wire budget for one pull request.
	litePullTimeoutMillis = int64(30000)
	// litePullRPCTimeoutMillis is the budget for the offset/min/max RPCs.
	litePullRPCTimeoutMillis = int64(5000)
	// liteHeartbeatIntervalMillis is the self-heartbeat period.
	liteHeartbeatIntervalMillis = int64(5000)
	// liteHeartbeatTimeoutMillis is one heartbeat's wire budget.
	liteHeartbeatTimeoutMillis = int64(5000)
	// liteRebalanceIntervalMillis is how often subscribe mode re-balances.
	liteRebalanceIntervalMillis = int64(1000)
	// liteGotAnyBackoffMillis is the immediate follow-up when a round pulled
	// something (drain the queue, don't wait out the idle interval).
	liteGotAnyBackoffMillis                     = int64(5)
	maxPollBatchSize                            = 1024
	DefaultLiteTopicMetadataCheckIntervalMillis = int64(30 * 1000)
	liteMetadataFirstDelayMillis                = int64(10 * 1000)

	// The four cache budgets a lite consumer guards its local buffer with. A
	// background pull round keeps filling the buffer until the application
	// drains it with Poll, so without these budgets a slow (or absent) poller
	// turns the buffer into an unbounded queue.
	//
	// DefaultLitePullThresholdForAll is pullThresholdForAll — the budget of
	// PULL CAPACITY still waiting to be drained: the pending pull rounds of every
	// queue, each budgeted a whole pullBatchSize, compared against this number.
	DefaultLitePullThresholdForAll = int64(10000)
	// DefaultLitePullThresholdForQueue is pullThresholdForQueue — messages ONE
	// queue may still hold in the buffer.
	DefaultLitePullThresholdForQueue = int64(1000)
	// DefaultLitePullThresholdSizeForQueue is pullThresholdSizeForQueue — MiB
	// ONE queue may still hold, counted as message BODY bytes (what the client
	// keeps in memory after decoding), never the wire-side store size.
	DefaultLitePullThresholdSizeForQueue = int64(100)
	// DefaultLiteConsumeMaxSpan is consumeMaxSpan — the queueOffset span ONE
	// queue's cached messages may cover.
	DefaultLiteConsumeMaxSpan = int64(2000)

	// bodyMiBBytes is the unit the size gate divides by. The division is an
	// integer one, so a partial MiB never counts toward the budget.
	bodyMiBBytes = int64(1024 * 1024)

	// The names the cache gates judge under, one per budget. "count" and "size"
	// share a trip counter — both mean "this queue holds too much of the buffer".
	liteGateAll   = "all"
	liteGateCount = "count"
	liteGateSize  = "size"
	liteGateSpan  = "span"
)

// The lite consumer is in exactly one of these modes for its whole life. A call
// that would switch it is refused: subscribe mode and assign mode build
// different subscription tables, pull tasks and heartbeats, so a mid-flight
// switch would leave the running halves describing two different consumers.
const (
	liteModeNone = iota
	liteModeSubscribe
	liteModeAssign
)

type TopicMessageQueueChangeListener interface {
	OnChanged(topic string, messageQueues []common.MessageQueue)
}

// DefaultLitePullConsumer polls with a background SHORT-pull loop and hands
// batches to the caller through Poll. Create one, configure it, Subscribe or
// Assign, then Start.
type DefaultLitePullConsumer struct {
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
	socksProxy    *remoting.SocksProxyConfig
	rpcHook       remoting.RPCHook

	nameServerAddrs []string
	pollNamesrvIntv int64

	// Instance knobs seeded at Start and applied live when the shared instance
	// already exists: receive-body decode, internal-API timeout and the
	// concurrent heartbeat round.
	decodeReadBody                    bool
	mqClientAPITimeoutMillis          int64
	enableConcurrentHeartbeat         bool
	concurrentHeartbeatThreadPoolSize int

	messageModel     string
	consumeFromWhere string
	consumeTimestamp string

	pullBatchSize            int32
	pollTimeoutMillis        int64
	autoCommit               bool
	autoCommitIntervalMillis int64
	pullIntervalMillis       int64

	// The cache budgets, all four judged per queue BEFORE a pull request is
	// built. They are plain budgets: no clamping, and a value of 0 trips on the
	// first cached message. pullThresholdForAll is the only one that spans every
	// queue; the other three judge this queue alone. Guarded by mu.
	pullThresholdForAll       int64
	pullThresholdForQueue     int64
	pullThresholdSizeForQueue int64
	consumeMaxSpan            int64

	// Cache-gate trip counters, one per budget shape (the count and the size
	// gate share one because both mean "this queue holds too much"). Guarded by mu.
	flowControlAllTriggered   int64
	flowControlCacheTriggered int64
	flowControlSpanTriggered  int64

	// pullPause holds the per-queue re-schedule a refusal left behind, keyed by
	// queue so a parked queue sits its own window out while its siblings keep
	// pulling. Guarded by mu.
	pullPause map[common.MessageQueue]litePullPause

	allocateStrategy AllocateMessageQueueStrategy
	queueListener    MessageQueueListener

	topicChangeListeners       map[string]TopicMessageQueueChangeListener
	messageQueuesForTopic      map[string][]common.MessageQueue
	topicMetadataCheckInterval int64

	// ---- consumer state, all guarded by mu ----
	// subscription is topic -> raw expression (subscribe mode).
	subscription map[string]string
	// subscriptionData feeds the self-heartbeat (both modes register here:
	// assign mode needs the broker-side filter just as much).
	subscriptionData map[string]*remoting.SubscriptionData
	// assignSubExpr is the per-topic TAG expression for assign mode.
	assignSubExpr          map[string]string
	mode                   int
	assigned               map[common.MessageQueue]struct{}
	nextOffset             map[common.MessageQueue]int64
	consumeOffset          map[common.MessageQueue]int64
	offsetTable            map[common.MessageQueue]int64
	seekOffset             map[common.MessageQueue]int64
	paused                 map[common.MessageQueue]struct{}
	nextAutoCommitDeadline int64
	lastRebalanceTs        int64

	// ---- runtime ----
	instance *Instance
	pullAPI  *pullAPI
	started  bool
	stopCh   chan struct{}

	// buffer + its own wakeup channel: Poll waits on bufNotify, the pull
	// loop drops one token in per enqueue. A single token is enough — every
	// waiter re-checks the buffer before waiting again.
	bufMu     sync.Mutex
	buffer    []*common.MessageExt
	bufNotify chan struct{}
	// cacheStats is the buffer seen per queue: the same entries, counted
	// incrementally so a per-queue verdict costs no scan of the buffer. It is
	// only ever written by the buffer's own primitives below, under bufMu, in the
	// same step that moves the entries — so it cannot report a backlog the buffer
	// does not hold.
	cacheStats map[common.MessageQueue]*liteQueueCache
}

func NewDefaultLitePullConsumer(group string) (*DefaultLitePullConsumer, error) {
	if err := common.CheckGroup(group); err != nil {
		return nil, err
	}
	return &DefaultLitePullConsumer{
		consumerGroup:              group,
		instanceName:               common.DefaultInstanceName,
		messageModel:               MessageModelClustering,
		consumeFromWhere:           ConsumeFromWhereLastOffset,
		consumeTimestamp:           defaultConsumeTimestamp(),
		pollNamesrvIntv:            defaultConsumerPollNamesrvInterval,
		decodeReadBody:             true,
		mqClientAPITimeoutMillis:   DefaultMQClientAPITimeoutMillis,
		pullBatchSize:              DefaultLitePullBatchSize,
		pollTimeoutMillis:          DefaultLitePollTimeoutMillis,
		autoCommit:                 true,
		autoCommitIntervalMillis:   DefaultLiteAutoCommitIntervalMillis,
		pullIntervalMillis:         DefaultLitePullIntervalMillis,
		pullThresholdForAll:        DefaultLitePullThresholdForAll,
		pullThresholdForQueue:      DefaultLitePullThresholdForQueue,
		pullThresholdSizeForQueue:  DefaultLitePullThresholdSizeForQueue,
		consumeMaxSpan:             DefaultLiteConsumeMaxSpan,
		pullPause:                  map[common.MessageQueue]litePullPause{},
		streamRequest:              true,
		allocateStrategy:           AllocateMessageQueueAveragely{},
		subscription:               map[string]string{},
		subscriptionData:           map[string]*remoting.SubscriptionData{},
		assignSubExpr:              map[string]string{},
		assigned:                   map[common.MessageQueue]struct{}{},
		nextOffset:                 map[common.MessageQueue]int64{},
		consumeOffset:              map[common.MessageQueue]int64{},
		offsetTable:                map[common.MessageQueue]int64{},
		seekOffset:                 map[common.MessageQueue]int64{},
		paused:                     map[common.MessageQueue]struct{}{},
		nextAutoCommitDeadline:     -1,
		topicChangeListeners:       map[string]TopicMessageQueueChangeListener{},
		messageQueuesForTopic:      map[string][]common.MessageQueue{},
		topicMetadataCheckInterval: DefaultLiteTopicMetadataCheckIntervalMillis,
		stopCh:                     make(chan struct{}),
		bufNotify:                  make(chan struct{}, 1),
		cacheStats:                 map[common.MessageQueue]*liteQueueCache{},
	}, nil
}

// MustNewDefaultLitePullConsumer panics instead of returning an error; for
// package-level wiring and tests where an invalid group is a programming error.
func MustNewDefaultLitePullConsumer(group string) *DefaultLitePullConsumer {
	c, err := NewDefaultLitePullConsumer(group)
	if err != nil {
		panic(err)
	}
	return c
}

// ---------------------------------------------------------------- configuration

func (c *DefaultLitePullConsumer) ConsumerGroup() string { return c.consumerGroup }

// ClientID is the computed clientId (populated by Start).
func (c *DefaultLitePullConsumer) ClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

// SetNameServerAddr parses "host:port;host:port".
func (c *DefaultLitePullConsumer) SetNameServerAddr(addr string) {
	var addrs []string
	for _, a := range strings.Split(addr, ";") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	c.SetNameServerAddresses(addrs)
}

func (c *DefaultLitePullConsumer) SetNameServerAddresses(addrs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nameServerAddrs = append([]string(nil), addrs...)
}

func (c *DefaultLitePullConsumer) SetInstanceName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.instanceName = name
}

func (c *DefaultLitePullConsumer) SetNamespace(ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespace = ns
}

func (c *DefaultLitePullConsumer) SetNamespaceV2(ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespaceV2 = ns
}

func (c *DefaultLitePullConsumer) SetUnitName(unit string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unitName = unit
}

func (c *DefaultLitePullConsumer) SetUnitMode(mode bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unitMode = mode
}

func (c *DefaultLitePullConsumer) SetEnableStreamRequestType(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streamRequest = enable
}

func (c *DefaultLitePullConsumer) SetRpcHook(hook remoting.RPCHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rpcHook = hook
}

// SetTlsEnable mirrors ClientConfig#setUseTLS (nil = leave the instance default).
func (c *DefaultLitePullConsumer) SetTlsEnable(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tlsEnable = &enable
}

func (c *DefaultLitePullConsumer) SetSocksProxy(proxy *remoting.SocksProxyConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.socksProxy = proxy
}

// SetDecodeReadBody seeds the shared instance's receive-body decode switch:
// when off, pull frames are walked without materialising bodies. It takes
// effect on a running instance immediately.
func (c *DefaultLitePullConsumer) SetDecodeReadBody(enable bool) {
	c.mu.Lock()
	c.decodeReadBody = enable
	inst := c.instance
	c.mu.Unlock()
	if inst != nil {
		inst.SetDecodeReadBody(enable)
	}
}

// SetMQClientAPITimeoutMillis sets the internal-API budget used by the
// self-heartbeat, route fetches and consumer-list RPCs. Non-positive values
// restore the 3000ms default.
func (c *DefaultLitePullConsumer) SetMQClientAPITimeoutMillis(timeoutMillis int64) {
	if timeoutMillis <= 0 {
		timeoutMillis = DefaultMQClientAPITimeoutMillis
	}
	c.mu.Lock()
	c.mqClientAPITimeoutMillis = timeoutMillis
	inst := c.instance
	c.mu.Unlock()
	if inst != nil {
		inst.SetMQClientAPITimeoutMillis(timeoutMillis)
	}
}

// SetEnableConcurrentHeartbeat turns the concurrent heartbeat round on or off;
// a V2 heartbeat still takes precedence when both are enabled.
func (c *DefaultLitePullConsumer) SetEnableConcurrentHeartbeat(enable bool) {
	c.mu.Lock()
	c.enableConcurrentHeartbeat = enable
	inst := c.instance
	c.mu.Unlock()
	if inst != nil {
		inst.SetEnableConcurrentHeartbeat(enable)
	}
}

// SetConcurrentHeartbeatThreadPoolSize sets the width of the concurrent
// heartbeat pool; non-positive values restore the available-processor default.
func (c *DefaultLitePullConsumer) SetConcurrentHeartbeatThreadPoolSize(size int) {
	if size <= 0 {
		size = runtime.NumCPU()
	}
	c.mu.Lock()
	c.concurrentHeartbeatThreadPoolSize = size
	inst := c.instance
	c.mu.Unlock()
	if inst != nil {
		inst.SetConcurrentHeartbeatThreadPoolSize(size)
	}
}

func (c *DefaultLitePullConsumer) SetMessageModel(model string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messageModel = model
}

func (c *DefaultLitePullConsumer) SetConsumeFromWhere(where string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeFromWhere = where
}

// SetConsumeTimestamp sets the CONSUME_FROM_TIMESTAMP start (yyyyMMddHHmmss).
func (c *DefaultLitePullConsumer) SetConsumeTimestamp(ts string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeTimestamp = ts
}

// SetPollNameServerInterval is ClientConfig#pollNameServerInterval.
func (c *DefaultLitePullConsumer) SetPollNameServerInterval(millis int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pollNamesrvIntv = millis
}

// SetPullBatchSize sets how many messages one pull request asks for. The value is
// not only the request size: the whole-buffer budget charges every pending pull
// round this many messages, so raising it raises the aggregate's pressure.
func (c *DefaultLitePullConsumer) SetPullBatchSize(n int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n < 1 {
		n = 1
	}
	c.pullBatchSize = n
}

func (c *DefaultLitePullConsumer) PullBatchSize() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pullBatchSize
}

func (c *DefaultLitePullConsumer) SetPollTimeoutMillis(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ms < 0 {
		ms = 0
	}
	c.pollTimeoutMillis = ms
}

// SetAutoCommit toggles the commit-on-delivery mode. Note what it does NOT
// change: Shutdown persists whatever the commit table holds either way —
// autoCommit=false only stops Poll's periodic flush.
func (c *DefaultLitePullConsumer) SetAutoCommit(auto bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.autoCommit = auto
}

// SetAutoCommitIntervalMillis sets the auto-commit interval. A value below
// MinLiteAutoCommitIntervalMillis is ignored outright — the previous interval
// keeps running rather than being clamped to the floor.
func (c *DefaultLitePullConsumer) SetAutoCommitIntervalMillis(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ms < MinLiteAutoCommitIntervalMillis {
		return
	}
	c.autoCommitIntervalMillis = ms
}

func (c *DefaultLitePullConsumer) SetPullIntervalMillis(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ms < 0 {
		ms = 0
	}
	c.pullIntervalMillis = ms
}

// ---------------------------------------------------------------- cache budgets

// SetPullThresholdForAll sets the budget of PULL CAPACITY still waiting to be
// drained: every queue's pull rounds whose messages the application has not
// polled yet count, each round budgeted a whole pullBatchSize, and the product is
// compared against this number. The aggregate gate every pull round judges first.
// A plain budget — no clamping, so 0 means "trip on the first pending round",
// which is a legal configuration.
func (c *DefaultLitePullConsumer) SetPullThresholdForAll(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullThresholdForAll = n
}

func (c *DefaultLitePullConsumer) PullThresholdForAll() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pullThresholdForAll
}

// SetPullThresholdForQueue sets how many messages ONE queue may still hold in
// the buffer. It is judged on that queue's own count and never on the sum of its
// siblings.
func (c *DefaultLitePullConsumer) SetPullThresholdForQueue(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullThresholdForQueue = n
}

func (c *DefaultLitePullConsumer) PullThresholdForQueue() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pullThresholdForQueue
}

// SetPullThresholdSizeForQueue sets how many MiB ONE queue may still hold,
// measured as the message BODY bytes — the payload the application actually gets,
// never the wire-side store size — so a few large messages weigh as much as many
// small ones. The gate divides by whole MiB, so a partial MiB costs nothing.
func (c *DefaultLitePullConsumer) SetPullThresholdSizeForQueue(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullThresholdSizeForQueue = n
}

func (c *DefaultLitePullConsumer) PullThresholdSizeForQueue() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pullThresholdSizeForQueue
}

// SetConsumeMaxSpan sets the queueOffset span ONE queue's cached messages may
// cover: a runaway cursor is caught even while the count stays small.
func (c *DefaultLitePullConsumer) SetConsumeMaxSpan(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeMaxSpan = n
}

func (c *DefaultLitePullConsumer) ConsumeMaxSpan() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumeMaxSpan
}

func (c *DefaultLitePullConsumer) SetAllocateMessageQueueStrategy(s AllocateMessageQueueStrategy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.allocateStrategy = s
}

// SetMessageQueueListener is notified when subscribe-mode rebalance changes
// the assignment of a topic.
func (c *DefaultLitePullConsumer) SetMessageQueueListener(l MessageQueueListener) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueListener = l
}

func (c *DefaultLitePullConsumer) SetTopicMetadataCheckIntervalMillis(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ms < 1000 {
		ms = 1000
	}
	c.topicMetadataCheckInterval = ms
}

// TopicMetadataCheckIntervalMillis reports the current period.
func (c *DefaultLitePullConsumer) TopicMetadataCheckIntervalMillis() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.topicMetadataCheckInterval
}

func (c *DefaultLitePullConsumer) RegisterTopicMessageQueueChangeListener(
	topic string, listener TopicMessageQueueChangeListener) error {
	if strings.TrimSpace(topic) == "" || listener == nil {
		return common.ClientError("Topic or listener is null")
	}
	c.mu.Lock()
	if _, dup := c.topicChangeListeners[topic]; dup {
		common.LogWarnf("Topic %s had been registered, new listener will overwrite the old one", topic)
	}
	c.topicChangeListeners[topic] = listener
	started := c.started
	c.mu.Unlock()
	if !started {
		return nil
	}
	queues, err := c.FetchMessageQueues(topic)
	if err != nil {
		common.LogDebugf("register listener: fetch queues for %s failed: %v", topic, err)
		return nil
	}
	c.mu.Lock()
	c.messageQueuesForTopic[topic] = queues
	c.mu.Unlock()
	return nil
}

func (c *DefaultLitePullConsumer) FetchTopicMessageQueuesAndCompare() int {
	c.mu.Lock()
	topics := make([]string, 0, len(c.topicChangeListeners))
	for topic := range c.topicChangeListeners {
		topics = append(topics, topic)
	}
	c.mu.Unlock()
	sort.Strings(topics)

	fired := 0
	for _, topic := range topics {
		c.mu.Lock()
		listener := c.topicChangeListeners[topic]
		c.mu.Unlock()
		if listener == nil {
			continue
		}
		newQueues, err := c.FetchMessageQueues(topic)
		if err != nil {
			common.LogErrorf("ScheduledTask fetchMessageQueuesAndCompare for %s failed: %v", topic, err)
			continue
		}
		c.mu.Lock()
		oldQueues, hadSnapshot := c.messageQueuesForTopic[topic]
		changed := !isQueueSetEqual(oldQueues, hadSnapshot, newQueues)
		if changed {
			c.messageQueuesForTopic[topic] = newQueues
		}
		c.mu.Unlock()
		if !changed {
			continue
		}
		listener.OnChanged(topic, newQueues)
		fired++
	}
	return fired
}

func isQueueSetEqual(oldQueues []common.MessageQueue, hadSnapshot bool,
	newQueues []common.MessageQueue) bool {
	if !hadSnapshot {
		return false
	}
	if len(oldQueues) != len(newQueues) {
		return false
	}
	set := make(map[common.MessageQueue]struct{}, len(oldQueues))
	for _, mq := range oldQueues {
		set[mq] = struct{}{}
	}
	for _, mq := range newQueues {
		if _, ok := set[mq]; !ok {
			return false
		}
	}
	return true
}

func (c *DefaultLitePullConsumer) metadataLoop(stop chan struct{}, firstDelay, period time.Duration) {
	timer := time.NewTimer(firstDelay)
	defer timer.Stop()
	for {
		select {
		case <-stop:
			return
		case <-timer.C:
		}
		c.FetchTopicMessageQueuesAndCompare()
		timer.Reset(period)
	}
}

// IsStarted reports whether Start succeeded and Shutdown has not run.
func (c *DefaultLitePullConsumer) IsStarted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

// ---------------------------------------------------------------- subscribe / assign

// Subscribe switches back to subscribe mode and registers a TAG expression.
// An unparsable expression drops only the wire subscription (the raw one is
// still recorded), matching the push consumer's Subscribe.
func (c *DefaultLitePullConsumer) Subscribe(topic, subExpression string) error {
	if strings.TrimSpace(topic) == "" {
		return common.ClientError("Topic can not be null or empty.")
	}
	if subExpression == "" {
		subExpression = "*"
	}
	sub, err := remoting.FilterAPI{}.BuildSubscriptionData(topic, subExpression)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.claimModeLocked(liteModeSubscribe); err != nil {
		return err
	}
	c.subscription[topic] = subExpression
	c.subscriptionData[topic] = sub
	return nil
}

// Unsubscribe drops a topic from both tables AND the queues this instance holds
// for it. Leaving the queues behind would keep pulling a topic the caller has
// just left; waiting for the next rebalance would keep doing it until then.
func (c *DefaultLitePullConsumer) Unsubscribe(topic string) {
	if strings.TrimSpace(topic) == "" {
		return
	}
	c.mu.Lock()
	delete(c.subscription, topic)
	if _, ok := c.assignSubExpr[topic]; !ok {
		delete(c.subscriptionData, topic)
	}
	var revoked []common.MessageQueue
	for mq := range c.assigned {
		if mq.Topic != topic {
			continue
		}
		delete(c.assigned, mq)
		delete(c.nextOffset, mq)
		delete(c.consumeOffset, mq)
		delete(c.seekOffset, mq)
		revoked = append(revoked, mq)
	}
	c.mu.Unlock()
	for _, mq := range revoked {
		c.dropQueueCache(mq)
	}
}

// SetSubExpressionForAssign gives one topic its TAG expression in assign mode
// (the pull request needs it, and the raw table entry is what a diagnostics read
// returns). Only a real expression is accepted: a blank one would silently mean
// "everything", and the caller would never learn that its filter did not apply.
// It is also a pre-start setting, because the running consumer already told the
// broker which expression this topic has.
func (c *DefaultLitePullConsumer) SetSubExpressionForAssign(topic, subExpression string) error {
	if strings.TrimSpace(subExpression) == "" {
		return common.ClientError("subExpression can not be null or empty.")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return common.ClientError("setSubExpressionForAssign only can be called before start.")
	}
	if strings.TrimSpace(topic) == "" {
		return common.ClientError("Topic can not be null or empty.")
	}
	if err := c.claimModeLocked(liteModeAssign); err != nil {
		return err
	}
	c.assignSubExpr[topic] = subExpression
	sub, err := remoting.FilterAPI{}.BuildSubscriptionData(topic, subExpression)
	if err != nil {
		common.LogDebugf("lite assign expr: build subscription data failed: %v", err)
		delete(c.subscriptionData, topic)
		return err
	}
	c.subscriptionData[topic] = sub
	return nil
}

// Assign names the queues this instance consumes, explicitly (no rebalance). An
// empty list is refused rather than taken: it would revoke every queue the
// consumer holds, which is what a caller with a route lookup that returned
// nothing does by accident.
func (c *DefaultLitePullConsumer) Assign(mqs []common.MessageQueue) error {
	if len(mqs) == 0 {
		return common.ClientError("Message queues can not be null or empty.")
	}
	c.mu.Lock()
	if err := c.claimModeLocked(liteModeAssign); err != nil {
		c.mu.Unlock()
		return err
	}
	keep := make(map[common.MessageQueue]struct{}, len(mqs))
	for _, mq := range mqs {
		keep[mq] = struct{}{}
	}
	var revoked []common.MessageQueue
	for mq := range c.assigned {
		if _, ok := keep[mq]; ok {
			continue
		}
		delete(c.nextOffset, mq)
		delete(c.consumeOffset, mq)
		delete(c.seekOffset, mq)
		revoked = append(revoked, mq)
	}
	c.assigned = keep
	c.mu.Unlock()

	// A queue this consumer no longer owns must also lose its paused flag, its
	// cached messages and its share of the backlog accounting: all three belong
	// to a queue that is not ours to judge or to serve any more.
	for _, mq := range revoked {
		c.dropQueueCache(mq)
	}
	return nil
}

// Assignment snapshots the assigned queues (sorted for stable callers).
func (c *DefaultLitePullConsumer) Assignment() []common.MessageQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	return sortedQueues(c.assigned)
}

// IsAssignMode reports which mode the consumer is in.
func (c *DefaultLitePullConsumer) IsAssignMode() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode == liteModeAssign
}

// claimModeLocked pins the consumer's mode on first use and refuses any later
// call that would change it. Callers hold mu.
func (c *DefaultLitePullConsumer) claimModeLocked(mode int) error {
	switch {
	case c.mode == liteModeNone:
		c.mode = mode
		return nil
	case c.mode == mode:
		return nil
	default:
		return common.ClientError("Subscribe and assign are mutually exclusive.")
	}
}

// SubscriptionExpressions snapshots topic -> raw expression (subscribe mode).
func (c *DefaultLitePullConsumer) SubscriptionExpressions() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]string, len(c.subscription))
	for t, e := range c.subscription {
		out[t] = e
	}
	return out
}

// Subscriptions feeds nothing on the wire (the self-heartbeat reads the same
// table directly); it exists for tests and diagnostics, sorted for stability.
func (c *DefaultLitePullConsumer) Subscriptions() []*remoting.SubscriptionData {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*remoting.SubscriptionData, 0, len(c.subscriptionData))
	for _, sub := range c.subscriptionData {
		out = append(out, sub)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}

func (c *DefaultLitePullConsumer) Pause(mqs []common.MessageQueue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, mq := range mqs {
		c.paused[mq] = struct{}{}
	}
}

// Resume un-does Pause.
func (c *DefaultLitePullConsumer) Resume(mqs []common.MessageQueue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, mq := range mqs {
		delete(c.paused, mq)
	}
}

// ---------------------------------------------------------------- lifecycle

// Start validates the config, opens the instance, resolves initial offsets
// for assign-mode queues, registers the group, and spawns the two background
// loops (self-heartbeat + pull). Idempotent.
func (c *DefaultLitePullConsumer) Start() error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	if err := c.checkConfigLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.namespace != "" {
		// Before %RETRY% derivations could ever matter: the wire group is the
		// WRAPPED one (same as the push/pull consumers' Start).
		c.consumerGroup = common.WrapNamespace(c.namespace, c.consumerGroup)
	}
	if c.messageModel == MessageModelClustering {
		c.instanceName = common.ChangeInstanceNameToPID(c.instanceName)
	}
	if c.clientID == "" {
		c.clientID = common.ClientIDFor(c.instanceName, c.unitName, c.streamRequest)
	}
	cfg := NewClientInstanceConfig()
	if c.tlsEnable != nil {
		cfg.TLSEnable = *c.tlsEnable
	}
	cfg.SocksProxy = c.socksProxy
	cfg.PollNameServerIntervalMillis = c.pollNamesrvIntv
	cfg.DecodeReadBody = c.decodeReadBody
	cfg.MQClientAPITimeoutMillis = c.mqClientAPITimeoutMillis
	cfg.EnableConcurrentHeartbeat = c.enableConcurrentHeartbeat
	cfg.ConcurrentHeartbeatThreadPoolSize = c.concurrentHeartbeatThreadPoolSize
	instance := CreateOrGetInstance(c.clientID, c.nameServerAddrs, cfg)
	c.instance = instance
	namespaceV2 := c.namespaceV2
	userHook := c.rpcHook
	stream := c.streamRequest
	c.mu.Unlock()

	instance.EnsureRPCHooks(namespaceV2, stream, userHook)
	if err := instance.Start(); err != nil {
		c.mu.Lock()
		c.instance = nil
		c.mu.Unlock()
		return err
	}

	c.mu.Lock()
	instance.RegisterConsumerGroup(c.consumerGroup)
	c.pullAPI = newPullAPI(instance, c.consumerGroup)
	c.started = true
	topics := make([]string, 0, len(c.subscription))
	for t := range c.subscription {
		topics = append(topics, t)
	}
	for mq := range c.assigned {
		if _, ok := c.subscription[mq.Topic]; !ok {
			topics = append(topics, mq.Topic)
		}
	}
	c.mu.Unlock()

	// Refresh routes FIRST so the assign-mode offset resolution below (and the
	// synchronous heartbeat after it) can actually reach a broker: the
	// heartbeat only goes to brokers already in the route table, and a fresh
	// instance's table is empty.
	for _, topic := range topics {
		instance.RegisterTopicInUse(topic)
		if _, err := instance.UpdateTopicRouteInfoFromNameServer(topic, 0, false); err != nil {
			common.LogDebugf("lite start: route refresh for %s failed: %v", topic, err)
		}
	}
	c.resolveOffsetsFor(c.Assignment())
	c.sendHeartbeatToAllBrokers()
	c.mu.Lock()
	stop := c.stopCh
	period := c.topicMetadataCheckInterval
	c.mu.Unlock()
	go c.heartbeatLoop(stop)
	go c.pullServiceLoop(stop)
	go c.metadataLoop(stop, time.Duration(liteMetadataFirstDelayMillis)*time.Millisecond,
		time.Duration(period)*time.Millisecond)
	return nil
}

func (c *DefaultLitePullConsumer) checkConfigLocked() error {
	if err := common.CheckGroup(c.consumerGroup); err != nil {
		return err
	}
	if c.consumerGroup == common.DefaultConsumerGroup {
		return common.ClientError(fmt.Sprintf(
			"consumerGroup can not equal %s, please specify another one.", common.DefaultConsumerGroup))
	}
	if len(c.nameServerAddrs) == 0 {
		return common.ClientError("name server address is not set")
	}
	if len(c.subscription) == 0 && c.mode != liteModeAssign {
		return common.ClientError("subscription is not set, call subscribe() or assign() first")
	}
	if _, err := parseConsumeTimestamp(c.consumeTimestamp); err != nil {
		return err
	}
	return nil
}

func (c *DefaultLitePullConsumer) Shutdown() {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return
	}
	c.started = false
	close(c.stopCh)
	// Wake a Poll parked on the buffer so it can drain what is left.
	c.notifyBufferLocked()
	autoCommit := c.autoCommit
	assigned := sortedQueues(c.assigned)
	instance := c.instance
	group := c.consumerGroup
	c.mu.Unlock()

	if autoCommit {
		c.mu.Lock()
		for _, mq := range assigned {
			off, ok := c.consumeOffset[mq]
			if ok && off != -1 {
				c.offsetTable[mq] = off
			}
		}
		c.mu.Unlock()
	}
	scope := c.Assignment()
	if err := c.persistOffsetTable(scope); err != nil {
		common.LogDebugf("lite shutdown: final offset persist failed: %v", err)
	}

	if instance != nil {
		instance.UnregisterConsumerGroup(group)
		instance.UnregisterClientAllBrokers("", group)
		instance.Shutdown()
		instance.DetachFromRegistryIfLastTenant()
	}
	c.mu.Lock()
	c.instance = nil
	c.pullAPI = nil
	c.mu.Unlock()
}

// notifyBufferLocked drops one token into the wakeup channel (non-blocking —
// a full channel means a waiter is already awake and will re-check).
func (c *DefaultLitePullConsumer) notifyBufferLocked() {
	select {
	case c.bufNotify <- struct{}{}:
	default:
	}
}

// ---------------------------------------------------------------- poll

// Poll waits up to pollTimeoutMillis for messages and returns up to
// maxPollBatchSize of them (nil when the timeout elapsed with nothing).
// A consumer that is not running answers with an error instead of an empty
// round: an empty round is exactly what a queue at its tail looks like, so the
// caller would wait forever on a Start() it never called.
func (c *DefaultLitePullConsumer) Poll() ([]*common.MessageExt, error) {
	c.mu.Lock()
	timeout := c.pollTimeoutMillis
	c.mu.Unlock()
	return c.PollWithTimeout(timeout)
}

func (c *DefaultLitePullConsumer) PollWithTimeout(timeoutMillis int64) ([]*common.MessageExt, error) {
	if !c.IsStarted() {
		return nil, common.ClientError("The consumer not running, please start it first.")
	}
	if timeoutMillis < 0 {
		return nil, common.ClientError("Timeout must not be negative")
	}
	if c.autoCommitOn() {
		c.maybeAutoCommit()
	}
	deadline := time.Now().Add(time.Duration(timeoutMillis) * time.Millisecond)
	for {
		if msgs := c.tryDrain(); len(msgs) > 0 {
			// Delivery is what advances the CONSUME cursor (rule 1).
			c.advanceConsumeOffset(msgs)
			return msgs, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}
		timer := time.NewTimer(remaining)
		select {
		case <-c.bufNotify:
			timer.Stop()
		case <-timer.C:
			return nil, nil
		}
	}
}

func (c *DefaultLitePullConsumer) autoCommitOn() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.autoCommit
}

// tryDrain takes up to maxPollBatchSize messages off the buffer. The per-queue
// backlog accounting retires in the same step (see drainBufferLocked), so what
// the gates measure is what the application has not polled away yet.
func (c *DefaultLitePullConsumer) tryDrain() []*common.MessageExt {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()
	return c.drainBufferLocked(maxPollBatchSize)
}

func (c *DefaultLitePullConsumer) advanceConsumeOffset(msgs []*common.MessageExt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	held := make(map[common.MessageQueue]struct{}, len(c.nextOffset))
	for mq := range c.nextOffset {
		if _, ok := c.assigned[mq]; ok {
			held[mq] = struct{}{}
		}
	}
	for _, m := range msgs {
		key := liteQueueKey(m)
		if _, ok := held[key]; !ok {
			continue
		}
		if nxt := m.QueueOffset + 1; nxt > c.consumeOffset[key] {
			c.consumeOffset[key] = nxt
		}
	}
}

// BufferedMessageCount reports the undelivered backlog (tests/diagnostics).
func (c *DefaultLitePullConsumer) BufferedMessageCount() int {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()
	return len(c.buffer)
}

// FlowControlAllTriggered counts the rounds the whole-buffer budget refused
// (live diagnostics).
func (c *DefaultLitePullConsumer) FlowControlAllTriggered() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flowControlAllTriggered
}

// FlowControlCacheTriggered counts the rounds one queue was over its own message
// count OR its own stored-bytes budget: one number for the two ways a queue can
// simply hold too much.
func (c *DefaultLitePullConsumer) FlowControlCacheTriggered() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flowControlCacheTriggered
}

// FlowControlSpanTriggered counts the rounds one queue's cached offset span was
// over consumeMaxSpan.
func (c *DefaultLitePullConsumer) FlowControlSpanTriggered() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flowControlSpanTriggered
}

// PullPauseMillisOf reports the length of the re-schedule a queue is currently
// sitting out (0 when it has no pause window). Cache-gate pauses are
// pullTimeDelayMillsWhenFlowControl and a broker throttle is
// pullTimeDelayMillsWhenBrokerFlow, so the number also says which of the two
// parked it (tests/diagnostics).
func (c *DefaultLitePullConsumer) PullPauseMillisOf(mq common.MessageQueue) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pullPause[mq]
	if !ok || common.CurrentTimeMillis() >= p.readyAt {
		return 0
	}
	return p.delayMillis
}

// PullCursorOf reads the pull cursor of one queue (-1 when it has none).
func (c *DefaultLitePullConsumer) PullCursorOf(mq common.MessageQueue) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if off, ok := c.nextOffset[mq]; ok {
		return off
	}
	return -1
}

// ConsumeCursorOf reads the consume cursor of one queue (-1 when Poll has not
// delivered anything for it yet).
func (c *DefaultLitePullConsumer) ConsumeCursorOf(mq common.MessageQueue) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if off, ok := c.consumeOffset[mq]; ok {
		return off
	}
	return -1
}

// PendingCommitOf reads the commit-table cell (-1 when there is none) — the
// value a persist=false Commit left behind.
func (c *DefaultLitePullConsumer) PendingCommitOf(mq common.MessageQueue) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if off, ok := c.offsetTable[mq]; ok {
		return off
	}
	return -1
}

// ---------------------------------------------------------------- seek / cursors

// Seek moves one queue's pull cursor to `offset`. A queue this consumer does not
// hold is refused first: writing a cursor for it would leave an entry nothing
// pulls, so the caller would watch an offset change that no round ever honours.
// The offset itself is then held to the queue's own [minOffset, maxOffset] range,
// which is also where the running-state check happens.
func (c *DefaultLitePullConsumer) Seek(mq common.MessageQueue, offset int64) error {
	c.mu.Lock()
	_, held := c.assigned[mq]
	subscribeMode := c.mode == liteModeSubscribe
	c.mu.Unlock()
	if !held {
		if subscribeMode {
			return common.ClientError(
				"The message queue is not in assigned list, may be rebalancing, message queue: " + mq.String())
		}
		return common.ClientError("The message queue is not in assigned list, message queue: " + mq.String())
	}
	instance, err := c.requireInstance()
	if err != nil {
		return err
	}
	minOffset, err := instance.GetMinOffset(mq, litePullRPCTimeoutMillis)
	if err != nil {
		return err
	}
	maxOffset, err := instance.GetMaxOffset(mq, litePullRPCTimeoutMillis)
	if err != nil {
		return err
	}
	if offset < minOffset || offset > maxOffset {
		return common.ClientError(fmt.Sprintf(
			"Seek offset illegal, seek offset = %d, min offset = %d, max offset = %d",
			offset, minOffset, maxOffset))
	}
	c.mu.Lock()
	c.seekOffset[mq] = offset
	c.nextOffset[mq] = offset
	c.consumeOffset[mq] = offset
	c.mu.Unlock()
	c.bufMu.Lock()
	// Rewinding the cursor drops what this queue has already cached below the
	// new offset; the accumulator is rebuilt from what the buffer keeps, so the
	// two never part company (pruneBufferLocked is the only place that does both).
	c.pruneBufferLocked(mq, offset)
	c.bufMu.Unlock()
	return nil
}

// SeekToBegin pins the cursor at the queue's minimum offset.
func (c *DefaultLitePullConsumer) SeekToBegin(mq common.MessageQueue) error {
	instance, err := c.requireInstance()
	if err != nil {
		return err
	}
	offset, err := instance.GetMinOffset(mq, litePullRPCTimeoutMillis)
	if err != nil {
		return err
	}
	return c.Seek(mq, offset)
}

// SeekToEnd pins the cursor at the queue's maximum offset.
func (c *DefaultLitePullConsumer) SeekToEnd(mq common.MessageQueue) error {
	instance, err := c.requireInstance()
	if err != nil {
		return err
	}
	offset, err := instance.GetMaxOffset(mq, litePullRPCTimeoutMillis)
	if err != nil {
		return err
	}
	return c.Seek(mq, offset)
}

// Committed reads the queue's committed offset: the in-memory commit table
// first (a persist=false Commit's value counts), then the broker, with the
// broker's answer backfilled into the table. -1 means nobody has committed.
// A consumer that never started has no committed offset to report -- the table
// belongs to a session that is not running, so it is not read.
func (c *DefaultLitePullConsumer) Committed(mq common.MessageQueue) (int64, error) {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return -1, common.ClientError("The consumer not running, please start it first.")
	}
	if off, ok := c.offsetTable[mq]; ok {
		c.mu.Unlock()
		return off, nil
	}
	instance := c.instance
	group := c.consumerGroup
	c.mu.Unlock()
	if instance == nil {
		return -1, common.ClientError("The consumer not running, please start it first.")
	}
	offset, found, err := instance.QueryConsumerOffset(group, mq, litePullRPCTimeoutMillis, "", false)
	if err != nil || !found {
		return -1, err
	}
	c.mu.Lock()
	if _, ok := c.offsetTable[mq]; !ok {
		c.offsetTable[mq] = offset
	}
	c.mu.Unlock()
	return offset, nil
}

// OffsetForTimestamp is the MQAdmin pass-through (search offset by time).
func (c *DefaultLitePullConsumer) OffsetForTimestamp(mq common.MessageQueue, timestamp int64) (int64, error) {
	instance, err := c.requireInstance()
	if err != nil {
		return 0, err
	}
	return instance.SearchOffsetByTimestamp(mq, timestamp, litePullRPCTimeoutMillis)
}

// FetchMessageQueues lists the topic's queues from the SUBSCRIBE info (read
// queues, no master filtering — the set a caller Assigns from).
func (c *DefaultLitePullConsumer) FetchMessageQueues(topic string) ([]common.MessageQueue, error) {
	instance, err := c.requireInstance()
	if err != nil {
		return nil, err
	}
	if _, err := instance.UpdateTopicRouteInfoFromNameServer(topic,
		0, false); err != nil {
		// Keep answering from the cached route when the nameserver is briefly
		// unreachable; the listener's per-topic catch still sees the empty case.
		common.LogDebugf("fetch queues: route refresh for %s failed: %v", topic, err)
	}
	queues := instance.GetTopicSubscribeInfo(topic)
	if len(queues) == 0 {
		return nil, common.ClientError("Can not find Message Queue for this topic, " +
			topic + " Namesrv return empty")
	}
	return queues, nil
}

func (c *DefaultLitePullConsumer) requireInstance() (*Instance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started || c.instance == nil {
		return nil, common.ClientError("The consumer not running, please start it first.")
	}
	return c.instance, nil
}

// ---------------------------------------------------------------- commit

func (c *DefaultLitePullConsumer) Commit() error {
	targets := make(map[common.MessageQueue]int64)
	c.mu.Lock()
	for mq := range c.assigned {
		// A queue Poll never delivered from has NO cursor yet — the map's zero
		// value 0 must not sneak in as a "committed at 0"; the -1 check below
		// relies on absent cursors staying out of targets.
		if off, ok := c.consumeOffset[mq]; ok {
			targets[mq] = off
		}
	}
	c.mu.Unlock()
	return c.commitTargets(targets, sortedQueueKeys(targets), true)
}

func (c *DefaultLitePullConsumer) CommitOffsets(offsets map[common.MessageQueue]int64, persist bool) error {
	if len(offsets) == 0 {
		common.LogWarnf("MessageQueues is empty, Ignore this commit ")
		return nil
	}
	scope := sortedQueueKeys(offsets)
	return c.commitTargets(offsets, scope, persist)
}

func (c *DefaultLitePullConsumer) CommitQueues(mqs []common.MessageQueue, persist bool) error {
	if len(mqs) == 0 {
		return nil
	}
	scope := sortedQueues(toSet(mqs))
	targets := make(map[common.MessageQueue]int64, len(scope))
	c.mu.Lock()
	for _, mq := range scope {
		if off, ok := c.consumeOffset[mq]; ok {
			targets[mq] = off
		}
	}
	c.mu.Unlock()
	return c.commitTargets(targets, sortedQueueKeys(targets), persist)
}

func (c *DefaultLitePullConsumer) commitTargets(targets map[common.MessageQueue]int64,
	scope []common.MessageQueue, persist bool) error {
	c.mu.Lock()
	for mq, off := range targets {
		if off == -1 {
			common.LogErrorf("consumerOffset is -1 in messageQueue [%s@%s-%d].",
				c.consumerGroup, mq.Topic, mq.QueueID)
			continue
		}
		c.offsetTable[mq] = off
	}
	c.mu.Unlock()
	if !persist {
		return nil
	}
	return c.persistOffsetTable(scope)
}

func (c *DefaultLitePullConsumer) persistOffsetTable(scope []common.MessageQueue) error {
	if len(scope) == 0 {
		return nil
	}
	wanted := make(map[common.MessageQueue]struct{}, len(scope))
	for _, mq := range scope {
		wanted[mq] = struct{}{}
	}
	c.mu.Lock()
	var toSend []struct {
		mq     common.MessageQueue
		offset int64
	}
	for mq, off := range c.offsetTable {
		if _, ok := wanted[mq]; !ok {
			delete(c.offsetTable, mq)
			continue
		}
		toSend = append(toSend, struct {
			mq     common.MessageQueue
			offset int64
		}{mq, off})
	}
	instance := c.instance
	group := c.consumerGroup
	c.mu.Unlock()
	if instance == nil {
		// Shutdown already flipped `started`; the FINAL persist still has to
		// use the not-yet-closed instance, which is why this reads the field
		// instead of requireInstance().
		return nil
	}
	sort.Slice(toSend, func(i, j int) bool { return toSend[i].mq.CompareTo(toSend[j].mq) < 0 })
	var firstErr error
	for _, e := range toSend {
		if err := instance.UpdateConsumerOffset(group, e.mq, e.offset, litePullRPCTimeoutMillis, ""); err != nil {
			common.LogDebugf("lite persist failed for %v: %v", e.mq, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (c *DefaultLitePullConsumer) persistOneOffset(mq common.MessageQueue) {
	c.mu.Lock()
	off, ok := c.offsetTable[mq]
	instance := c.instance
	group := c.consumerGroup
	c.mu.Unlock()
	if !ok || instance == nil {
		return
	}
	if err := instance.UpdateConsumerOffset(group, mq, off, litePullRPCTimeoutMillis, ""); err != nil {
		common.LogDebugf("lite persist failed for %v: %v", mq, err)
	}
}

func (c *DefaultLitePullConsumer) maybeAutoCommit() {
	c.mu.Lock()
	interval := c.autoCommitIntervalMillis
	now := common.CurrentTimeMillis()
	if now < c.nextAutoCommitDeadline {
		c.mu.Unlock()
		return
	}
	c.nextAutoCommitDeadline = now + interval
	c.mu.Unlock()
	if err := c.Commit(); err != nil {
		common.LogDebugf("lite auto-commit failed: %v", err)
	}
}

// ---------------------------------------------------------------- rebalance

// Rebalance recomputes the assignment for subscribe mode (assign mode is a
// no-op — the caller owns that queue set). Exposed for callers that want an
// assignment NOW instead of waiting out the 1s loop tick (Pause/Seek-by-queue
// right after Start).
func (c *DefaultLitePullConsumer) Rebalance() {
	instance, err := c.requireInstance()
	if err != nil {
		return
	}
	c.mu.Lock()
	group := c.consumerGroup
	clientID := c.clientID
	strategy := c.allocateStrategy
	if strategy == nil {
		strategy = AllocateMessageQueueAveragely{}
	}
	var topics []string
	for t := range c.subscription {
		topics = append(topics, t)
	}
	baseline := sortedQueues(c.assigned)
	c.mu.Unlock()

	newAssigned := make(map[common.MessageQueue]struct{})
	type topicChange struct {
		topic   string
		mqAll   []common.MessageQueue
		divided []common.MessageQueue
	}
	var changes []topicChange
	for _, topic := range topics {
		mqAll := instance.GetTopicSubscribeInfo(topic)
		sort.Slice(mqAll, func(i, j int) bool { return mqAll[i].CompareTo(mqAll[j]) < 0 })
		if len(mqAll) == 0 {
			common.LogDebugf("lite rebalance: no subscribe info for topic %s", topic)
			continue
		}
		cidAll, answered := instance.GetConsumerIDListByGroup(topic, group, 0)
		if !answered || len(cidAll) == 0 {
			// Same rule as the push consumer's rebalance: a group whose
			// membership is unknown keeps its current slice. Falling back to
			// "I own everything" makes co-instances duplicate each other.
			common.LogDebugf("lite rebalance: no consumer id list for %s/%s, keep current", group, topic)
			for _, mq := range baseline {
				if mq.Topic == topic {
					newAssigned[mq] = struct{}{}
				}
			}
			continue
		}
		sort.Strings(cidAll)
		got := strategy.Allocate(group, clientID, mqAll, cidAll)
		if reporter, ok := strategy.(AllocateErrReporter); ok {
			if err := reporter.AllocateErr(); err != nil {
				common.LogErrorf("lite rebalance: allocate failed, strategy=%s group=%s: %v, keep current",
					strategyName(strategy), group, err)
				for _, mq := range baseline {
					if mq.Topic == topic {
						newAssigned[mq] = struct{}{}
					}
				}
				continue
			}
		}
		for _, mq := range got {
			newAssigned[mq] = struct{}{}
		}
		changes = append(changes, topicChange{topic: topic, mqAll: mqAll, divided: got})
	}

	var added []common.MessageQueue
	var revoked []common.MessageQueue
	var changedTopics []topicChange
	c.mu.Lock()
	old := make(map[common.MessageQueue]struct{}, len(c.assigned))
	for mq := range c.assigned {
		old[mq] = struct{}{}
	}
	for mq := range newAssigned {
		if _, ok := old[mq]; !ok {
			added = append(added, mq)
		}
	}
	for mq := range old {
		if _, ok := newAssigned[mq]; !ok {
			revoked = append(revoked, mq)
			delete(c.nextOffset, mq)
			delete(c.consumeOffset, mq)
			delete(c.seekOffset, mq)
		}
	}
	changed := len(added) > 0 || len(revoked) > 0
	if changed {
		c.assigned = newAssigned
	}
	if changed {
		for _, tc := range changes {
			if len(tc.divided) == 0 {
				still := false
				for mq := range c.assigned {
					if mq.Topic == tc.topic {
						still = true
						break
					}
				}
				if !still {
					continue
				}
			}
			changedTopics = append(changedTopics, tc)
		}
	}
	c.mu.Unlock()

	sort.Slice(revoked, func(i, j int) bool { return revoked[i].CompareTo(revoked[j]) < 0 })
	for _, mq := range revoked {
		// Last commit for a surrendered queue, then its table cell goes away —
		// and so does its share of the backlog accounting, which no queue we no
		// longer own may use to refuse a pull.
		c.persistOneOffset(mq)
		c.mu.Lock()
		delete(c.offsetTable, mq)
		c.mu.Unlock()
		c.dropQueueCache(mq)
	}
	if len(added) > 0 {
		c.resolveOffsetsFor(added)
	}
	if len(changedTopics) > 0 {
		c.mu.Lock()
		listener := c.queueListener
		c.mu.Unlock()
		if listener != nil {
			for _, tc := range changedTopics {
				listener.MessageQueueChanged(tc.topic, tc.mqAll, tc.divided)
			}
		}
	}
}

func (c *DefaultLitePullConsumer) resolveOffsetsFor(mqs []common.MessageQueue) {
	instance, err := c.requireInstance()
	if err != nil {
		return
	}
	c.mu.Lock()
	group := c.consumerGroup
	model := c.messageModel
	fromWhere := c.consumeFromWhere
	ts := c.consumeTimestamp
	pending := make([]common.MessageQueue, 0, len(mqs))
	for _, mq := range mqs {
		if _, ok := c.nextOffset[mq]; !ok {
			pending = append(pending, mq)
		}
	}
	c.mu.Unlock()

	for _, mq := range pending {
		offset, err := c.resolveInitialOffset(instance, group, mq, model, fromWhere, ts)
		if err != nil {
			common.LogDebugf("lite: resolve initial offset failed for %v: %v", mq, err)
			continue
		}
		c.mu.Lock()
		if _, ok := c.nextOffset[mq]; !ok {
			c.nextOffset[mq] = offset
		}
		c.mu.Unlock()
	}
}

func (c *DefaultLitePullConsumer) resolveInitialOffset(instance *Instance, group string,
	mq common.MessageQueue, model, fromWhere, consumeTS string) (int64, error) {
	c.mu.Lock()
	if off, ok := c.seekOffset[mq]; ok {
		c.mu.Unlock()
		return off, nil
	}
	c.mu.Unlock()
	// found=false (QUERY_NOT_FOUND) is NOT a committed 0 — it must fall through
	// to the consumeFromWhere branch, or a fresh group would start at offset 0
	// even under CONSUME_FROM_LAST_OFFSET.
	if offset, found, err := instance.QueryConsumerOffset(group, mq, litePullRPCTimeoutMillis, "", false); err != nil {
		common.LogDebugf("lite query offset failed for %v: %v", mq, err)
	} else if found {
		return offset, nil
	}
	if fromWhere == ConsumeFromWhereFirstOffset {
		return 0, nil
	}
	if fromWhere == ConsumeFromWhereTimestamp {
		// A retry queue is created by the first redelivery, long after the configured
		// instant, so searching there answers nothing; its replay starts at the tail.
		if common.IsRetryTopic(mq.Topic) {
			return instance.GetMaxOffset(mq, litePullRPCTimeoutMillis)
		}
		ts, err := parseConsumeTimestamp(consumeTS)
		if err != nil {
			return 0, err
		}
		return instance.SearchOffsetByTimestamp(mq, ts, litePullRPCTimeoutMillis)
	}
	// First consumption of a retry queue replays from 0 instead of jumping to the tail.
	if common.IsRetryTopic(mq.Topic) {
		return 0, nil
	}
	return instance.GetMaxOffset(mq, litePullRPCTimeoutMillis)
}

// ---------------------------------------------------------------- background loops

// subscriptionFor is the per-pull expression: subscribe table → assign table
// → "*" (an assign without an explicit expression pulls everything).
func (c *DefaultLitePullConsumer) subscriptionFor(topic string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if expr, ok := c.subscription[topic]; ok {
		return expr
	}
	if expr, ok := c.assignSubExpr[topic]; ok {
		return expr
	}
	return "*"
}

// heartbeatLoop sends the SELF heartbeat (one ConsumerData, CONSUME_ACTIVELY,
// own subscription set) to every broker address, master and slave. A slave
// that never hears the beat answers pulls with SUBSCRIPTION_NOT_EXIST.
func (c *DefaultLitePullConsumer) heartbeatLoop(stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		c.sendHeartbeatToAllBrokers()
		timer := time.NewTimer(time.Duration(liteHeartbeatIntervalMillis) * time.Millisecond)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// sendHeartbeatToAllBrokers builds and fans the heartbeat out; the return
// value counts the brokers that accepted it (0 when not started — the loop
// keeps ticking instead of exiting).
func (c *DefaultLitePullConsumer) sendHeartbeatToAllBrokers() int {
	c.mu.Lock()
	if c.instance == nil {
		c.mu.Unlock()
		return 0
	}
	hb := remoting.NewHeartbeatData(c.clientID)
	cd := remoting.NewConsumerData(c.consumerGroup, ConsumeTypeActively, c.messageModel, c.consumeFromWhere)
	cd.UnitMode = c.unitMode
	// Only rebalance subscriptions are advertised. An assign-mode expression names
	// the filter for the queues this instance was handed, not a topic-wide
	// subscription, so announcing it would make the broker treat the group as a
	// subscriber of the whole topic.
	for topic := range c.subscription {
		if sub, ok := c.subscriptionData[topic]; ok {
			cd.AddSubscriptionData(sub)
		}
	}
	hb.AddConsumerData(cd)
	addrs := c.instance.GetAllBrokerAddrs()
	instance := c.instance
	c.mu.Unlock()

	ok := 0
	for _, addr := range addrs {
		if err := instance.SendHeartbeat(addr, hb, 0); err != nil {
			common.LogDebugf("lite heartbeat to %s failed: %v", addr, err)
			continue
		}
		ok++
	}
	return ok
}

// pullServiceLoop is the single pull engine: re-balance (subscribe mode) every
// liteRebalanceIntervalMillis, then one SHORT-pull round over every assigned,
// non-paused queue. A round that pulled something follows up after 5ms; an
// empty round backs off pullIntervalMillis.
//
// A queue its own cache gate (or the broker) re-scheduled sits out the rounds
// inside its pause window while its siblings keep pulling — the pause is
// per-queue state, never a sleep in this loop, so one throttled queue cannot
// delay the others. The round's wait is cut short when a parked queue is due
// sooner than the idle backoff, so a 50ms pause is not silently stretched to
// whatever pullIntervalMillis happens to be.
func (c *DefaultLitePullConsumer) pullServiceLoop(stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		c.mu.Lock()
		assignMode := c.mode == liteModeAssign
		lastTs := c.lastRebalanceTs
		c.mu.Unlock()
		now := common.CurrentTimeMillis()
		if !assignMode && (lastTs == 0 || now-lastTs > liteRebalanceIntervalMillis) {
			c.Rebalance()
			c.mu.Lock()
			c.lastRebalanceTs = common.CurrentTimeMillis()
			c.mu.Unlock()
		}

		c.mu.Lock()
		targets := make([]common.MessageQueue, 0, len(c.assigned))
		for mq := range c.assigned {
			if _, ok := c.paused[mq]; ok {
				continue
			}
			if c.queuePausedLocked(mq) {
				continue
			}
			targets = append(targets, mq)
		}
		c.mu.Unlock()
		sort.Slice(targets, func(i, j int) bool { return targets[i].CompareTo(targets[j]) < 0 })

		gotAny := false
		for _, mq := range targets {
			select {
			case <-stop:
				return
			default:
			}
			if c.pullOne(mq) {
				gotAny = true
			}
		}

		backoff := time.Duration(c.pullIntervalValue()) * time.Millisecond
		if gotAny {
			backoff = time.Duration(liteGotAnyBackoffMillis) * time.Millisecond
		}
		if until, ok := c.nextPullPauseDeadline(); ok {
			switch remaining := time.Duration(until-common.CurrentTimeMillis()) * time.Millisecond; {
			case remaining <= 0:
				backoff = 0
			case remaining < backoff:
				backoff = remaining
			}
		}
		timer := time.NewTimer(backoff)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (c *DefaultLitePullConsumer) pullIntervalValue() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pullIntervalMillis
}

// pullOne runs one short-pull round for one queue. true = something entered
// the buffer (drives the loop's backoff choice).
//
// The cache gates are judged first, before anything is read off the cursor: a
// refused round builds no request at all, so the broker sees no pull, commits
// nothing and moves nothing.
func (c *DefaultLitePullConsumer) pullOne(mq common.MessageQueue) bool {
	if c.flowControlHit(mq) {
		return false
	}
	c.mu.Lock()
	api := c.pullAPI
	group := c.consumerGroup
	namespace := c.namespace
	batchSize := c.pullBatchSize
	offset, ok := c.nextOffset[mq]
	c.mu.Unlock()
	if api == nil {
		return false
	}
	if !ok {
		instance, err := c.requireInstance()
		if err != nil {
			return false
		}
		c.mu.Lock()
		model, fromWhere, ts := c.messageModel, c.consumeFromWhere, c.consumeTimestamp
		c.mu.Unlock()
		offset, err = c.resolveInitialOffset(instance, group, mq, model, fromWhere, ts)
		if err != nil {
			common.LogDebugf("lite: resolve offset failed for %v: %v", mq, err)
			return false
		}
		c.mu.Lock()
		if _, already := c.nextOffset[mq]; !already {
			c.nextOffset[mq] = offset
		}
		c.mu.Unlock()
	}

	expr := c.subscriptionFor(mq.Topic)
	sub, err := remoting.FilterAPI{}.BuildSubscriptionData(mq.Topic, expr)
	if err != nil {
		common.LogDebugf("lite pull: build subscription for %s failed: %v", mq.Topic, err)
		return false
	}
	// Same wire rule as the classic pull consumer: a TAG expression travels
	// with SubVersion 0 (the broker re-derives the filter from SubString).
	if sub.ExpressionType == remoting.ExpressionTypeTag {
		sub.SubVersion = 0
	}
	// Short poll: no suspend, no inline commit (the lite consumer commits from
	// its own consume cursor); the SUBSCRIPTION bit carries the expression and
	// the LITE bit tags the request as lite traffic.
	sysFlag := common.BuildSysFlag(false, false, true, false, true)

	result, err := api.pullKernelLite(group, mq, offset, sub, sysFlag, batchSize, litePullTimeoutMillis)
	if err != nil {
		common.LogDebugf("lite pull failed for %v@%d: %v", mq, offset, err)
		// The broker's own throttle (FLOW_CONTROL 215) is not a failure of the
		// same kind as the rest: the request round-tripped, nothing landed in the
		// buffer, and the queue is readable again as soon as the broker's pressure
		// drops — so THIS queue waits the short broker pause. Every other answer
		// keeps the lite path's existing handling (debug log, ride the round's
		// wait), because retrying hot at an unhealthy broker adds pressure.
		if isBrokerFlowControlError(err) {
			c.schedulePullPause(mq, pullTimeDelayMillsWhenBrokerFlow)
		}
		return false
	}
	// Every response feeds processPullResult (suggest-which-broker bookkeeping),
	// and FOUND lists come back tag-filtered — the same call the classic pull
	// consumer makes.
	result = api.processPullResult(mq, result, sub)

	c.mu.Lock()
	intact := c.nextOffset[mq] == offset
	if intact {
		c.nextOffset[mq] = result.NextBeginOffset
	}
	c.mu.Unlock()
	if !intact {
		return false
	}
	if result.PullStatus != PullFound || len(result.MsgFoundList) == 0 {
		return false
	}
	msgs := result.MsgFoundList
	if namespace != "" {
		for _, m := range msgs {
			m.Topic = common.WithoutNamespace(m.Topic, namespace)
		}
	}
	c.enqueue(msgs)
	return true
}

func (c *DefaultLitePullConsumer) enqueue(msgs []*common.MessageExt) {
	c.bufMu.Lock()
	c.appendBufferLocked(msgs)
	c.bufMu.Unlock()
	c.notifyBufferLocked()
}

// ------------------------------------------------------- cached backlog accounting

// liteQueueCache is ONE queue's share of the local buffer: how many of its
// messages are still cached, how many BODY bytes they cost, which offsets they
// cover, and how many of its pull rounds are still waiting for the application.
// It exists so a per-queue verdict reads numbers instead of walking the buffer,
// and it only ever moves inside the primitives that move the buffer itself
// (appendBufferLocked / drainBufferLocked / pruneBufferLocked), under bufMu, so it
// cannot drift from what is actually cached.
type liteQueueCache struct {
	bytes int64
	// offsets holds this queue's cached queueOffsets in ascending order, one
	// entry per cached message, so the count is its length and the span is
	// first-to-last. Pull results arrive ascending, so the common update is an
	// append; a rewound cursor (Seek) inserts in place.
	offsets []int64
	// rounds holds, per pull round of this queue, how many of its messages are STILL in the
	// buffer, oldest round first; a round drops out as soon as its last message is polled. The
	// aggregate gate budgets one whole batch per round still represented here rather than the
	// messages that round happened to bring back, so it needs the round count, not the message
	// count. This queue's own messages always leave in round order — the shared buffer is drained
	// from its head and a rewind drops a prefix of this queue's offsets — so only the front
	// round's remainder ever has to be tracked.
	rounds []int
}

// beginRound records that one pull round of this queue left count messages in the buffer.
func (s *liteQueueCache) beginRound(count int) {
	if count > 0 {
		s.rounds = append(s.rounds, count)
	}
}

// pendingRounds is how many of this queue's pull rounds still hold messages in the buffer.
func (s *liteQueueCache) pendingRounds() int { return len(s.rounds) }

func (s *liteQueueCache) add(m *common.MessageExt) {
	s.bytes += int64(len(m.GetBody()))
	off := m.QueueOffset
	n := len(s.offsets)
	if n == 0 || off >= s.offsets[n-1] {
		s.offsets = append(s.offsets, off)
		return
	}
	i := sort.Search(n, func(j int) bool { return s.offsets[j] >= off })
	s.offsets = append(s.offsets, 0)
	copy(s.offsets[i+1:], s.offsets[i:])
	s.offsets[i] = off
}

// remove retires one cached message. An offset that is not in the set means the
// entry belongs to a queue that no longer has an accumulator (its messages
// outlive the accounting until the application polls them away), so there is
// nothing to retire here.
func (s *liteQueueCache) remove(m *common.MessageExt) {
	// Whatever left the buffer belongs to the oldest round still represented here.
	if len(s.rounds) > 0 {
		s.rounds[0]--
		if s.rounds[0] <= 0 {
			s.rounds = s.rounds[1:]
		}
	}
	off := m.QueueOffset
	i := sort.Search(len(s.offsets), func(j int) bool { return s.offsets[j] >= off })
	if i == len(s.offsets) || s.offsets[i] != off {
		return
	}
	s.offsets = append(s.offsets[:i], s.offsets[i+1:]...)
	s.bytes -= int64(len(m.GetBody()))
	if len(s.offsets) == 0 {
		// Nothing of this queue is cached any more: the numbers reset, so an empty cache reads
		// as "0 count, 0 bytes, 0 span" and never as a stale remainder.
		s.bytes = 0
		s.rounds = nil
	}
}

func (s *liteQueueCache) count() int { return len(s.offsets) }

// span is the distance between the lowest and the highest cached offset; an
// empty or single-message cache spans nothing.
func (s *liteQueueCache) span() int64 {
	if len(s.offsets) < 2 {
		return 0
	}
	return s.offsets[len(s.offsets)-1] - s.offsets[0]
}

// liteQueueKey is the queue a cached message belongs to — the same key the
// cursors are keyed by, so an accumulator and a cursor always name one queue.
func liteQueueKey(m *common.MessageExt) common.MessageQueue {
	return common.MessageQueue{Topic: m.Topic, BrokerName: m.BrokerName, QueueID: m.QueueID}
}

// cacheOfLocked returns this queue's accumulator, creating it on first use.
// Callers hold bufMu.
func (c *DefaultLitePullConsumer) cacheOfLocked(mq common.MessageQueue) *liteQueueCache {
	s, ok := c.cacheStats[mq]
	if !ok {
		s = &liteQueueCache{}
		c.cacheStats[mq] = s
	}
	return s
}

// appendBufferLocked adds the fetched messages to the buffer and to their
// queues' accounting in the same step, and books ONE pending pull round per
// queue the round touched: the aggregate gate budgets the round, not the messages
// that happened to come back with it. Callers hold bufMu.
func (c *DefaultLitePullConsumer) appendBufferLocked(msgs []*common.MessageExt) {
	c.buffer = append(c.buffer, msgs...)
	perQueue := make(map[common.MessageQueue]int, 1)
	for _, m := range msgs {
		mq := liteQueueKey(m)
		c.cacheOfLocked(mq).add(m)
		perQueue[mq]++
	}
	for mq, count := range perQueue {
		c.cacheOfLocked(mq).beginRound(count)
	}
}

// drainBufferLocked hands the application up to n messages from the front of the
// buffer and retires exactly those from their queues' accounting. Callers hold
// bufMu.
func (c *DefaultLitePullConsumer) drainBufferLocked(n int) []*common.MessageExt {
	if len(c.buffer) == 0 {
		return nil
	}
	take := len(c.buffer)
	if take > n {
		take = n
	}
	out := append([]*common.MessageExt(nil), c.buffer[:take]...)
	for _, m := range out {
		if s := c.cacheStats[liteQueueKey(m)]; s != nil {
			s.remove(m)
		}
	}
	c.buffer = c.buffer[take:]
	return out
}

// pruneBufferLocked drops this queue's cached messages below `from` (what Seek
// does when the cursor rewinds) and rebuilds this queue's accounting from the
// messages the buffer keeps. The walk over the buffer is the rewind's own cost,
// not something a pull round pays. Callers hold bufMu.
func (c *DefaultLitePullConsumer) pruneBufferLocked(mq common.MessageQueue, from int64) {
	kept := c.buffer[:0:0]
	pruned := 0
	for _, m := range c.buffer {
		if liteQueueKey(m) == mq && m.QueueOffset < from {
			pruned++
			continue
		}
		kept = append(kept, m)
	}
	c.buffer = kept

	rebuilt := &liteQueueCache{}
	for _, m := range c.buffer {
		if liteQueueKey(m) == mq {
			rebuilt.add(m)
		}
	}
	// The pruned messages are this queue's OLDEST cached ones, so they came out of its oldest
	// rounds: the round list is retired from the front exactly as poll() drains it, and the
	// aggregate gate keeps counting the rounds whose messages the application can still poll.
	if old := c.cacheStats[mq]; old != nil {
		rebuilt.rounds = retireRounds(old.rounds, pruned)
	}
	if rebuilt.count() == 0 {
		delete(c.cacheStats, mq)
		return
	}
	c.cacheStats[mq] = rebuilt
}

// retireRounds removes the first n cached messages from the round list and returns
// what is left: a round drops out once all of its messages are gone.
func retireRounds(rounds []int, n int) []int {
	out := rounds
	for n > 0 && len(out) > 0 {
		if out[0] > n {
			rest := make([]int, len(out))
			copy(rest, out)
			rest[0] -= n
			return rest
		}
		n -= out[0]
		out = out[1:]
	}
	if n > 0 {
		return nil
	}
	return out
}

// dropQueueCache retires a queue this consumer no longer owns: its accounting,
// its paused flag, its pending pull pause and any messages it already cached.
// The cached rows go with the queue rather than waiting for the application:
// nothing will replace them, no later round can cover them, and delivering from
// a queue this consumer has already handed back would serve messages the group
// no longer owns.
func (c *DefaultLitePullConsumer) dropQueueCache(mq common.MessageQueue) {
	c.mu.Lock()
	delete(c.pullPause, mq)
	delete(c.paused, mq)
	c.mu.Unlock()
	c.bufMu.Lock()
	kept := c.buffer[:0:0]
	for _, m := range c.buffer {
		if liteQueueKey(m) != mq {
			kept = append(kept, m)
		}
	}
	c.buffer = kept
	delete(c.cacheStats, mq)
	c.bufMu.Unlock()
}

// liteCacheStats is one judgment's measurements: this queue's own cached count, body bytes and
// offset range, plus the pull capacity the whole buffer has reserved.
type liteCacheStats struct {
	// rounds is the number of pull rounds still waiting for the application across EVERY queue,
	// and reserved is what the aggregate gate compares: rounds x pullBatchSize. The gate budgets a
	// round, not the messages that round happened to bring back.
	rounds    int
	reserved  int64
	batch     int64
	count     int
	bytes     int64
	minOffset int64
	maxOffset int64
	span      int64
}

// sizeMiB is the size gate's unit: whole MiB of message BODY, integer division, so a partial MiB
// does not count toward the budget.
func (s liteCacheStats) sizeMiB() int64 { return s.bytes / bodyMiBBytes }

// roundsFitInAll is the aggregate threshold translated back into rounds: the budget is stated in
// messages while the gate counts rounds, so the warn line says how many rounds still fit.
func (s liteCacheStats) roundsFitInAll(all int64) int64 {
	if s.batch <= 0 {
		return all
	}
	return (all + s.batch - 1) / s.batch
}

// cacheStatsOf reads this queue's share of the backlog plus the rounds every queue still has
// waiting. The per-queue numbers come from the accumulator, so a verdict costs no scan of the
// buffer; the round sum walks the accumulators, one entry per owned queue.
func (c *DefaultLitePullConsumer) cacheStatsOf(mq common.MessageQueue) liteCacheStats {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()
	c.mu.Lock()
	batch := int64(c.pullBatchSize)
	c.mu.Unlock()
	st := liteCacheStats{batch: batch}
	for _, s := range c.cacheStats {
		st.rounds += s.pendingRounds()
	}
	st.reserved = int64(st.rounds) * batch
	s := c.cacheStats[mq]
	if s == nil {
		return st
	}
	st.count = s.count()
	st.bytes = s.bytes
	if st.count > 0 {
		st.minOffset = s.offsets[0]
		st.maxOffset = s.offsets[st.count-1]
		st.span = s.span()
	}
	return st
}

// ------------------------------------------------------------- cache-flow gates

// cacheGateReason names the budget this queue is over, in the judgment's exact
// order — the pull capacity reserved across every queue, then this queue's count,
// its body size and its offset span — or "" when the queue may pull.
//
// Gates two to four read only THIS queue's accumulator, so a sibling that is over
// its budget never stops this one; the first gate is the one budget that spans
// every queue. All four compare strictly greater, so a queue sitting exactly on a
// threshold keeps pulling, and none of them is switched off by a zero value: a
// threshold of 0 trips on the first cached message.
func (c *DefaultLitePullConsumer) cacheGateReason(mq common.MessageQueue) (string, int64, liteCacheStats) {
	st := c.cacheStatsOf(mq)
	c.mu.Lock()
	all := c.pullThresholdForAll
	count := c.pullThresholdForQueue
	size := c.pullThresholdSizeForQueue
	span := c.consumeMaxSpan
	c.mu.Unlock()

	switch {
	case st.reserved > all:
		return liteGateAll, all, st
	case int64(st.count) > count:
		return liteGateCount, count, st
	case st.sizeMiB() > size:
		return liteGateSize, size, st
	case st.span > span:
		return liteGateSpan, span, st
	}
	return "", 0, st
}

// flowControlHit judges this queue's cached backlog before its pull round and,
// when it is over budget, records the trip, logs it and re-schedules THIS
// queue's pull for pullTimeDelayMillsWhenFlowControl later. The request is not
// built, so the broker sees no pull on a refused round; nothing is committed, the
// cursor does not move and the messages stay in the buffer for the application to
// poll — which is the only way the backlog can drain.
//
// The pause is per-queue state rather than a sleep, so a throttled queue cannot
// delay its siblings, and the loop's own wait is what shutdown interrupts.
func (c *DefaultLitePullConsumer) flowControlHit(mq common.MessageQueue) bool {
	reason, threshold, st := c.cacheGateReason(mq)
	if reason == "" {
		return false
	}
	times := c.tickFlowControl(reason)
	common.LogDebugf("lite flow control: queue %v gate=%s pendingRounds=%d reserved=%d count=%d size=%d MiB span=%d threshold=%d"+
		", pause pull for %s (flowControlTimes=%d)",
		mq, reason, st.rounds, st.reserved, st.count, st.sizeMiB(), st.span, threshold,
		pullTimeDelayMillsWhenFlowControl, times)
	if isFlowControlWarnTrip(times) {
		switch reason {
		case liteGateAll:
			// The budget is stated in messages while the gate counts rounds, so the warn
			// translates it back into the number of rounds that fit.
			common.LogWarnf("the consume request count exceeds threshold %d, so do flow control, "+
				"consume request count=%d, flowControlTimes=%d",
				st.roundsFitInAll(threshold), st.rounds, times)
		case liteGateCount:
			common.LogWarnf("The cached message count exceeds the threshold %d, so do flow control, "+
				"minOffset=%d, maxOffset=%d, count=%d, size=%d MiB, flowControlTimes=%d",
				threshold, st.minOffset, st.maxOffset, st.count, st.sizeMiB(), times)
		case liteGateSize:
			common.LogWarnf("The cached message size exceeds the threshold %d MiB, so do flow control, "+
				"minOffset=%d, maxOffset=%d, count=%d, size=%d MiB, flowControlTimes=%d",
				threshold, st.minOffset, st.maxOffset, st.count, st.sizeMiB(), times)
		case liteGateSpan:
			// maxSpan is the span this queue's cache actually has, not the threshold.
			common.LogWarnf("The queue's messages, span too long, so do flow control, "+
				"minOffset=%d, maxOffset=%d, maxSpan=%d, flowControlTimes=%d",
				st.minOffset, st.maxOffset, st.span, times)
		}
	}
	c.schedulePullPause(mq, pullTimeDelayMillsWhenFlowControl)
	return true
}

// tickFlowControl records one refusal on the counter of the gate that caused it
// and returns the counter's new value: the whole-buffer gate has its own, the
// span gate has its own, and the two "this queue holds too much" gates (count and
// size) share one. Every refusal is exactly one tick, whatever else the round did.
func (c *DefaultLitePullConsumer) tickFlowControl(reason string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch reason {
	case liteGateAll:
		c.flowControlAllTriggered++
		return c.flowControlAllTriggered
	case liteGateCount, liteGateSize:
		c.flowControlCacheTriggered++
		return c.flowControlCacheTriggered
	default:
		c.flowControlSpanTriggered++
		return c.flowControlSpanTriggered
	}
}

// ------------------------------------------------------------ per-queue pull pause

// litePullPause is one queue's re-scheduled pull round: the instant it may pull
// again, plus the length of the wait that produced it (the cache-gate pause or the
// short broker one), which is what the diagnostics and the tests read.
type litePullPause struct {
	readyAt     int64
	delayMillis int64
}

// schedulePullPause parks ONE queue's pull round for d. Sibling queues are not
// touched and keep pulling every round.
func (c *DefaultLitePullConsumer) schedulePullPause(mq common.MessageQueue, d time.Duration) {
	delay := int64(d / time.Millisecond)
	c.mu.Lock()
	c.pullPause[mq] = litePullPause{readyAt: common.CurrentTimeMillis() + delay, delayMillis: delay}
	c.mu.Unlock()
}

// queuePausedLocked reports whether this queue's round is still inside its pause
// window, retiring the entry once the window has run out. Callers hold mu.
func (c *DefaultLitePullConsumer) queuePausedLocked(mq common.MessageQueue) bool {
	p, ok := c.pullPause[mq]
	if !ok {
		return false
	}
	if common.CurrentTimeMillis() >= p.readyAt {
		delete(c.pullPause, mq)
		return false
	}
	return true
}

// nextPullPauseDeadline is the soonest instant any parked queue may pull again,
// so the round's wait never stretches a pause that was meant to be short.
func (c *DefaultLitePullConsumer) nextPullPauseDeadline() (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var soonest int64
	found := false
	for _, p := range c.pullPause {
		if !found || p.readyAt < soonest {
			soonest, found = p.readyAt, true
		}
	}
	return soonest, found
}

// ---------------------------------------------------------------- small helpers

func sortedQueues(set map[common.MessageQueue]struct{}) []common.MessageQueue {
	out := make([]common.MessageQueue, 0, len(set))
	for mq := range set {
		out = append(out, mq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CompareTo(out[j]) < 0 })
	return out
}

func sortedQueueKeys(m map[common.MessageQueue]int64) []common.MessageQueue {
	out := make([]common.MessageQueue, 0, len(m))
	for mq := range m {
		out = append(out, mq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CompareTo(out[j]) < 0 })
	return out
}

func toSet(mqs []common.MessageQueue) map[common.MessageQueue]struct{} {
	out := make(map[common.MessageQueue]struct{}, len(mqs))
	for _, mq := range mqs {
		out[mq] = struct{}{}
	}
	return out
}
