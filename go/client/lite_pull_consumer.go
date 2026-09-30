// DefaultLitePullConsumer — the LIGHT pull consumer (Java
// org.apache.rocketmq.client.consumer.DefaultLitePullConsumer +
// impl.consumer.DefaultLitePullConsumerImpl + RebalanceLitePullImpl).
//
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
//
//  1. There are TWO cursors per queue and collapsing them loses or duplicates
//     messages. nextOffset is the PULL cursor ("where the next pull goes");
//     consumeOffset is the CONSUME cursor ("what Poll has handed to the
//     caller") and is the only thing commit ever reads. Messages still sitting
//     in the local buffer are NOT consumed — committing from the pull cursor
//     would silently skip them after a restart (Java's
//     updateConsumeOffset(mq, processQueue.removeMessage(msgs)) is the same
//     line drawn differently: only delivery advances the commitable offset).
//  2. The pull cursor follows nextBeginOffset after EVERY round, whatever the
//     status (Java PullTaskImpl.run:982-998). NO_MATCHED_MSG would otherwise
//     re-scan the same unmatched window forever, and OFFSET_ILLEGAL carries the
//     broker's corrected offset in that field. The one brake: an in-flight
//     round must not overwrite a cursor that a concurrent Seek (or a revoke)
//     moved while the request was on the wire — the request-offset comparison
//     below is Java's seekOffset==-1 + isDropped checks folded into one.
//
// Wired differently from Java on purpose:
//
//   - The consumer is NOT registered in the instance's consumer table (it
//     cannot answer the broker's reverse requests the way a push consumer
//     does); it registers the GROUP only and sends its OWN heartbeat — one
//     ConsumerData with ConsumeType CONSUME_ACTIVELY (Java
//     DefaultLitePullConsumerImpl.consumeType():1111, the 5.4.0+ value) and
//     its own subscription set, fanned out to every broker address including
//     slaves (a slave that never hears the beat answers pulls with
//     SUBSCRIPTION_NOT_EXIST).
//   - The pull goes out as LITE_PULL_MESSAGE(361) with the
//     FLAG_LITE_PULL_MESSAGE sysFlag bit — the broker tags lite traffic with
//     them (Java DefaultLitePullConsumerImpl.PullTaskImpl:820-828).
//   - Shutdown persists the final offsets INLINE (Go has no async-runtime
//     constraint; the Rust/Python ports must spawn theirs), but in Java's
//     order: persistConsumerOffset first, unregister + factory shutdown last.
//   - Rebalance keeps the CURRENT assignment for a topic whose consumer-id
//     list could not be fetched — the same rule the push consumer follows
//     (Instance.GetConsumerIDListByGroup's "never fall back to I own every
//     queue"; the Rust/Python ports instead add themselves to an empty list,
//     which duplicates work between co-located instances after a blip).
package client

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// Lite-pull timings and limits (Java DefaultLitePullConsumerImpl + the shared
// port values the Rust/Python/C++/.NET consumers use).
const (
	// DefaultLitePullBatchSize is pullBatchSize — messages per pull request.
	DefaultLitePullBatchSize = int32(32)
	// DefaultLitePollTimeoutMillis is pollTimeoutMillis — how long Poll waits
	// for the buffer to become non-empty.
	DefaultLitePollTimeoutMillis = int64(5000)
	// DefaultLiteAutoCommitIntervalMillis is autoCommitIntervalMillis.
	DefaultLiteAutoCommitIntervalMillis = int64(5000)
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
	liteGotAnyBackoffMillis = int64(5)
	// maxPollBatchSize caps one Poll's return (Java MAX_POLL_BATCH_SIZE).
	maxPollBatchSize = 1024
)

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
	rpcHook       remoting.RPCHook

	nameServerAddrs []string
	pollNamesrvIntv int64

	messageModel     string
	consumeFromWhere string
	consumeTimestamp string

	pullBatchSize            int32
	pollTimeoutMillis        int64
	autoCommit               bool
	autoCommitIntervalMillis int64
	pullIntervalMillis       int64

	allocateStrategy AllocateMessageQueueStrategy
	queueListener    MessageQueueListener

	// ---- consumer state, all guarded by mu ----
	// subscription is topic -> raw expression (subscribe mode).
	subscription map[string]string
	// subscriptionData feeds the self-heartbeat (both modes register here:
	// assign mode needs the broker-side filter just as much).
	subscriptionData map[string]*remoting.SubscriptionData
	// assignSubExpr is the per-topic TAG expression for assign mode.
	assignSubExpr map[string]string
	assignMode    bool
	assigned      map[common.MessageQueue]struct{}
	// nextOffset is the PULL cursor; consumeOffset the CONSUME cursor (see
	// rule 1 in the file header); offsetTable is the commit target (Java
	// RemoteBrokerOffsetStore.offsetTable's role); seekOffset pins Seek's
	// value ahead of the first pull.
	nextOffset    map[common.MessageQueue]int64
	consumeOffset map[common.MessageQueue]int64
	offsetTable   map[common.MessageQueue]int64
	seekOffset    map[common.MessageQueue]int64
	paused        map[common.MessageQueue]struct{}
	// nextAutoCommitDeadline starts at -1: 0 would be "never again" only until
	// 1970, but -1 makes the FIRST MaybeAutoCommit fire immediately (Java's
	// nextAutoCommitDeadline initialises the same way).
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
}

// NewDefaultLitePullConsumer mirrors the Java constructor: the group is
// mandatory. STREAM request type defaults to ON (every Java lite constructor
// sets enableStreamRequestType = true, like the classic pull consumer).
func NewDefaultLitePullConsumer(group string) (*DefaultLitePullConsumer, error) {
	if err := common.CheckGroup(group); err != nil {
		return nil, err
	}
	return &DefaultLitePullConsumer{
		consumerGroup:            group,
		instanceName:             common.DefaultInstanceName,
		messageModel:             MessageModelClustering,
		consumeFromWhere:         ConsumeFromWhereLastOffset,
		consumeTimestamp:         defaultConsumeTimestamp(),
		pollNamesrvIntv:          defaultConsumerPollNamesrvInterval,
		pullBatchSize:            DefaultLitePullBatchSize,
		pollTimeoutMillis:        DefaultLitePollTimeoutMillis,
		autoCommit:               true,
		autoCommitIntervalMillis: DefaultLiteAutoCommitIntervalMillis,
		pullIntervalMillis:       DefaultLitePullIntervalMillis,
		streamRequest:            true,
		allocateStrategy:         AllocateMessageQueueAveragely{},
		subscription:             map[string]string{},
		subscriptionData:         map[string]*remoting.SubscriptionData{},
		assignSubExpr:            map[string]string{},
		assigned:                 map[common.MessageQueue]struct{}{},
		nextOffset:               map[common.MessageQueue]int64{},
		consumeOffset:            map[common.MessageQueue]int64{},
		offsetTable:              map[common.MessageQueue]int64{},
		seekOffset:               map[common.MessageQueue]int64{},
		paused:                   map[common.MessageQueue]struct{}{},
		nextAutoCommitDeadline:   -1,
		stopCh:                   make(chan struct{}),
		bufNotify:                make(chan struct{}, 1),
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

// SetPullBatchSize clamps at 1 (Java setPullBatchSize).
func (c *DefaultLitePullConsumer) SetPullBatchSize(n int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n < 1 {
		n = 1
	}
	c.pullBatchSize = n
}

// SetPollTimeoutMillis clamps at 0 (Java setPollTimeoutMillis).
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

func (c *DefaultLitePullConsumer) SetAutoCommitIntervalMillis(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ms < 0 {
		ms = 0
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

// SetAllocateMessageQueueStrategy mirrors the Java setter; Start refuses nil.
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
		return common.ClientError("subscription topic is empty")
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
	c.assignMode = false
	c.subscription[topic] = subExpression
	c.subscriptionData[topic] = sub
	return nil
}

// Unsubscribe drops a topic from both tables.
func (c *DefaultLitePullConsumer) Unsubscribe(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscription, topic)
	delete(c.subscriptionData, topic)
}

// SetSubExpressionForAssign gives one topic its TAG expression in assign mode
// (the broker-side filter and the heartbeat still need it).
func (c *DefaultLitePullConsumer) SetSubExpressionForAssign(topic, subExpression string) {
	if subExpression == "" {
		subExpression = "*"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.assignSubExpr[topic] = subExpression
	sub, err := remoting.FilterAPI{}.BuildSubscriptionData(topic, subExpression)
	if err == nil {
		c.subscriptionData[topic] = sub
	} else {
		common.LogDebugf("lite assign expr: build subscription data failed: %v", err)
		delete(c.subscriptionData, topic)
	}
}

// Assign switches to assign mode with an explicit queue set — no rebalance.
// Dropped queues lose their cursors entirely (Java
// AssignedMessageQueue.updateAssignedMessageQueue removes the whole
// MessageQueueState); the commit table is deliberately NOT touched, the
// persisted values only age out through Commit's persistAll scope rules.
// Queues already holding a pull cursor keep it (Java's
// `if (!nextPullOffsetMap.containsKey(mq))` on the resolve side).
func (c *DefaultLitePullConsumer) Assign(mqs []common.MessageQueue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.assignMode = true
	keep := make(map[common.MessageQueue]struct{}, len(mqs))
	for _, mq := range mqs {
		keep[mq] = struct{}{}
	}
	for mq := range c.assigned {
		if _, ok := keep[mq]; ok {
			continue
		}
		delete(c.nextOffset, mq)
		delete(c.consumeOffset, mq)
	}
	c.assigned = keep
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
	return c.assignMode
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

// Pause stops the pull loop from pulling these queues (buffered messages are
// unaffected — Java pauses pulls, not deliveries).
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
		// Java start:288-291 — only CLUSTERING rewrites DEFAULT to pid#nanotime.
		c.instanceName = common.ChangeInstanceNameToPID(c.instanceName)
	}
	if c.clientID == "" {
		c.clientID = common.ClientIDFor(c.instanceName, c.unitName, c.streamRequest)
	}
	cfg := NewClientInstanceConfig()
	if c.tlsEnable != nil {
		cfg.TLSEnable = *c.tlsEnable
	}
	cfg.PollNameServerIntervalMillis = c.pollNamesrvIntv
	instance := CreateOrGetInstance(c.clientID, c.nameServerAddrs, cfg)
	c.instance = instance
	namespaceV2 := c.namespaceV2
	userHook := c.rpcHook
	stream := c.streamRequest
	c.mu.Unlock()

	// Namespace + stream + user(ACL) + zone, in Java's order, once per instance.
	instance.EnsureRPCHooks(namespaceV2, stream, userHook)
	if err := instance.Start(); err != nil {
		c.mu.Lock()
		c.instance = nil
		c.mu.Unlock()
		return err
	}

	c.mu.Lock()
	// Java start:339 registers with the factory; this port registers the GROUP
	// only (see the file header) — the heartbeat task must not pick a lite
	// consumer up as if it could answer reverse requests.
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
		if _, err := instance.UpdateTopicRouteInfoFromNameServer(topic, litePullRPCTimeoutMillis, false); err != nil {
			common.LogDebugf("lite start: route refresh for %s failed: %v", topic, err)
		}
	}
	c.resolveOffsetsFor(c.Assignment())
	c.sendHeartbeatToAllBrokers()
	c.mu.Lock()
	stop := c.stopCh
	c.mu.Unlock()
	go c.heartbeatLoop(stop)
	go c.pullServiceLoop(stop)
	return nil
}

// checkConfigLocked is Java DefaultLitePullConsumerImpl.checkConfig:413 — the
// group checks first, then the address, then the subscription gate, then the
// timestamp format (a bad format would otherwise fail silently later and
// degrade CONSUME_FROM_TIMESTAMP into "consume from max offset").
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
	if len(c.subscription) == 0 && !c.assignMode {
		return common.ClientError("subscription is not set, call subscribe() or assign() first")
	}
	if _, err := parseConsumeTimestamp(c.consumeTimestamp); err != nil {
		return err
	}
	return nil
}

// Shutdown stops the loops, persists the final offsets (Java
// persistConsumerOffset, plus the autoCommit catch-up flush of
// "delivered but not yet deadline-reached" offsets), then unregisters the
// group and shuts the instance down — in that order. Idempotent.
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
		// Java shutdown → commitAll: copy the consume cursor into the commit
		// table for every held queue. The -1 guard matters: a queue Poll never
		// delivered from must not enter the table (a -1 on the wire would
		// replay the whole queue from zero after a restart).
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
func (c *DefaultLitePullConsumer) Poll() []*common.MessageExt {
	c.mu.Lock()
	timeout := c.pollTimeoutMillis
	c.mu.Unlock()
	return c.PollWithTimeout(timeout)
}

// PollWithTimeout is Poll with an explicit budget (Java poll(timeout)).
func (c *DefaultLitePullConsumer) PollWithTimeout(timeoutMillis int64) []*common.MessageExt {
	// Java poll() opens with maybeAutoCommit BEFORE touching the buffer: the
	// commit sends an RPC, and holding the buffer lock across it would stall
	// the enqueue side for the round trip.
	if c.autoCommitOn() {
		c.maybeAutoCommit()
	}
	deadline := time.Now().Add(time.Duration(maxInt64(timeoutMillis, 0)) * time.Millisecond)
	for {
		if msgs := c.tryDrain(); len(msgs) > 0 {
			// Delivery is what advances the CONSUME cursor (rule 1).
			c.advanceConsumeOffset(msgs)
			return msgs
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		timer := time.NewTimer(remaining)
		select {
		case <-c.bufNotify:
			timer.Stop()
		case <-timer.C:
			return nil
		}
	}
}

func (c *DefaultLitePullConsumer) autoCommitOn() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.autoCommit
}

// tryDrain takes up to maxPollBatchSize messages off the buffer.
func (c *DefaultLitePullConsumer) tryDrain() []*common.MessageExt {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()
	if len(c.buffer) == 0 {
		return nil
	}
	take := len(c.buffer)
	if take > maxPollBatchSize {
		take = maxPollBatchSize
	}
	out := append([]*common.MessageExt(nil), c.buffer[:take]...)
	c.buffer = c.buffer[take:]
	return out
}

// advanceConsumeOffset moves the CONSUME cursor past every delivered message.
// Only queues that are CURRENTLY assigned AND already pulled count (Java's
// updateConsumeOffset no-ops when the MessageQueueState is gone — a message
// whose queue was revoked mid-flight must not resurrect an offset).
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
		key := common.MessageQueue{Topic: m.Topic, BrokerName: m.BrokerName, QueueID: m.QueueID}
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

// Seek pins the cursors at offset and drops buffered messages of this queue
// that sit below it (Java: seekOffset then the next pull's nextPullOffset
// writes it through; the port writes all three cells at once — same effect,
// no window where a replayed message would be skipped by the committed
// offset).
func (c *DefaultLitePullConsumer) Seek(mq common.MessageQueue, offset int64) {
	c.mu.Lock()
	c.seekOffset[mq] = offset
	c.nextOffset[mq] = offset
	c.consumeOffset[mq] = offset
	c.mu.Unlock()
	c.bufMu.Lock()
	kept := c.buffer[:0:0]
	for _, m := range c.buffer {
		if m.Topic == mq.Topic && m.BrokerName == mq.BrokerName && m.QueueID == mq.QueueID && m.QueueOffset < offset {
			continue
		}
		kept = append(kept, m)
	}
	c.buffer = kept
	c.bufMu.Unlock()
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
	c.Seek(mq, offset)
	return nil
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
	c.Seek(mq, offset)
	return nil
}

// Committed reads the queue's committed offset: the in-memory commit table
// first (a persist=false Commit's value counts), then the broker, with the
// broker's answer backfilled into the table. -1 means nobody has committed.
func (c *DefaultLitePullConsumer) Committed(mq common.MessageQueue) (int64, error) {
	c.mu.Lock()
	if off, ok := c.offsetTable[mq]; ok {
		c.mu.Unlock()
		return off, nil
	}
	instance := c.instance
	group := c.consumerGroup
	c.mu.Unlock()
	if instance == nil {
		return -1, common.ClientError("consumer not started, call Start() first")
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
	return instance.GetTopicSubscribeInfo(topic), nil
}

func (c *DefaultLitePullConsumer) requireInstance() (*Instance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started || c.instance == nil {
		return nil, common.ClientError("consumer not started, call Start() first")
	}
	return c.instance, nil
}

// ---------------------------------------------------------------- commit

// Commit submits every assigned queue's CONSUME cursor (Java commitAll).
// Committing from the pull cursor here is the classic lite-pull data-loss bug:
// buffered-but-undelivered messages would be skipped after a restart.
func (c *DefaultLitePullConsumer) Commit() error {
	targets := make(map[common.MessageQueue]int64)
	c.mu.Lock()
	for mq := range c.assigned {
		// A queue Poll never delivered from has NO cursor yet — the map's zero
		// value 0 must not sneak in as a "committed at 0" (Rust's
		// unwrap_or(-1) guards the same hole against the -1 check below).
		if off, ok := c.consumeOffset[mq]; ok {
			targets[mq] = off
		}
	}
	c.mu.Unlock()
	return c.commitTargets(targets, sortedQueueKeys(targets), true)
}

// CommitOffsets is the caller-specified form (Java
// commit(Map<MessageQueue, Long>, persist)): ONLY the commit table moves,
// neither cursor does. An empty map is Java's
// "MessageQueues is empty, Ignore this commit" — a warn and no table access,
// so a previously accumulated persist=false value survives.
func (c *DefaultLitePullConsumer) CommitOffsets(offsets map[common.MessageQueue]int64, persist bool) error {
	if len(offsets) == 0 {
		common.LogWarnf("MessageQueues is empty, Ignore this commit ")
		return nil
	}
	scope := sortedQueueKeys(offsets)
	return c.commitTargets(offsets, scope, persist)
}

// CommitQueues commits only the named queues, from their current CONSUME
// cursor. An empty list is a silent no-op (Java commit(List, persist)).
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
	// Java persistAll over exactly the commit's own keys — see commitTargets.
	return c.commitTargets(targets, sortedQueueKeys(targets), persist)
}

// commitTargets is the shared body of the three commit entries: guarded write
// into the commit table, then (persist) a flush of exactly the commit's own
// keys. That scope is a SWEEP, not just a filter: Java RemoteBrokerOffsetStore
// persistAll deletes every table cell OUTSIDE the given set ("offset is not in
// mqs, remove it"), which is why a persist=false CommitOffsets' accumulated
// value on an unrelated queue disappears when a later commit(persist=true)
// does not name it.
func (c *DefaultLitePullConsumer) commitTargets(targets map[common.MessageQueue]int64,
	scope []common.MessageQueue, persist bool) error {
	c.mu.Lock()
	for mq, off := range targets {
		if off == -1 {
			// Java: "consumerOffset is -1" → error log, skip. A -1 on the wire
			// would replay the whole queue from zero on the next start.
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

// persistOffsetTable is Java RemoteBrokerOffsetStore#persistAll(Set): every
// in-scope table cell goes to its broker, and entries OUTSIDE the scope are
// dropped from the table on the way out ("remove unused mq"). The second half
// is why a partial Commit(persist=true) must hand in the full queue set — the
// unsent remainder of the table would be discarded.
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

// persistOneOffset is Java OffsetStore#persist(mq): one queue's commit-table
// cell goes to its broker, no cleanup. Rebalance uses it for surrendered
// queues, BEFORE their table cell is deleted (Java's removeUnnecessaryMessage
// Queue order: persist(mq) → removeOffset(mq)).
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

// maybeAutoCommit is Java maybeAutoCommit: one global deadline, checked (and
// re-armed) at the top of every Poll. Errors only log — an auto-commit that
// failed will retry on the next deadline.
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
		cidAll, answered := instance.GetConsumerIDListByGroup(topic, group, litePullRPCTimeoutMillis)
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
		for _, mq := range got {
			newAssigned[mq] = struct{}{}
		}
		changes = append(changes, topicChange{topic: topic, mqAll: mqAll, divided: got})
	}

	// Apply the union, dropping revoked queues' cursors with them (Java
	// removeUnnecessaryMessageQueue = persist(mq) then removeOffset(mq) — the
	// commit cell must SURVIVE until the persist went out, which is why the
	// revoked list is collected here and flushed below, outside the lock).
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
		// Last commit for a surrendered queue, then its table cell goes away.
		c.persistOneOffset(mq)
		c.mu.Lock()
		delete(c.offsetTable, mq)
		c.mu.Unlock()
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

// resolveOffsetsFor fills the pull cursor for queues that have none: seek pin
// → broker-committed offset (restart continuity) → CONSUME_FROM_FIRST_OFFSET
// means 0 (no minOffset RPC — Java's RebalanceLitePullImpl branch is a plain
// `result = 0L`, and the master-only min lookup would starve a new consumer
// while the master is down; the broker's OFFSET_ILLEGAL self-heals the rest)
// → CONSUME_FROM_TIMESTAMP → max offset. Failures only log: the next pull
// round retries resolution.
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
		ts, err := parseConsumeTimestamp(consumeTS)
		if err != nil {
			return 0, err
		}
		return instance.SearchOffsetByTimestamp(mq, ts, litePullRPCTimeoutMillis)
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
// keeps ticking like the Rust/Python ports instead of exiting).
func (c *DefaultLitePullConsumer) sendHeartbeatToAllBrokers() int {
	c.mu.Lock()
	if c.instance == nil {
		c.mu.Unlock()
		return 0
	}
	hb := remoting.NewHeartbeatData(c.clientID)
	cd := remoting.NewConsumerData(c.consumerGroup, ConsumeTypeActively, c.messageModel, c.consumeFromWhere)
	cd.UnitMode = c.unitMode
	for _, sub := range c.subscriptionData {
		cd.AddSubscriptionData(sub)
	}
	hb.AddConsumerData(cd)
	addrs := c.instance.GetAllBrokerAddrs()
	instance := c.instance
	c.mu.Unlock()

	ok := 0
	for _, addr := range addrs {
		if err := instance.SendHeartbeat(addr, hb, liteHeartbeatTimeoutMillis); err != nil {
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
func (c *DefaultLitePullConsumer) pullServiceLoop(stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		c.mu.Lock()
		assignMode := c.assignMode
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
// Cursor discipline (rule 2 in the file header): nextBeginOffset wins after
// EVERY round — FOUND, NO_NEW_MSG, NO_MATCHED_MSG, OFFSET_ILLEGAL alike —
// unless a concurrent Seek/Assign moved the cursor while the request was on
// the wire (the "intact" check folds Java's seekOffset==-1 and isDropped
// guards into one comparison).
func (c *DefaultLitePullConsumer) pullOne(mq common.MessageQueue) bool {
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
		// Java PullTaskImpl's resetTopic: the caller sees un-wrapped topics.
		for _, m := range msgs {
			m.Topic = common.WithoutNamespace(m.Topic, namespace)
		}
	}
	c.enqueue(msgs)
	return true
}

func (c *DefaultLitePullConsumer) enqueue(msgs []*common.MessageExt) {
	c.bufMu.Lock()
	c.buffer = append(c.buffer, msgs...)
	c.bufMu.Unlock()
	c.notifyBufferLocked()
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
