// DefaultMQPullConsumer — the ACTIVE pull consumer over the classic remoting
// protocol.
//
// The difference from DefaultMQPushConsumer is not "who calls the network", it
// is who owns the CURSOR and the QUEUE SET:
//
//   - push: the client rebalances, owns a process queue per assignment, pulls in
//     a background goroutine, and re-sends/commits by itself.
//   - pull(this): the CALLER decides which queue to read and passes the offset
//     back in on every RoundTrip. `Pull` is a SHORT poll (sysFlag suspend=false),
//     `PullBlockIfNotFound` is a long poll (suspend=true). Nothing is pulled in
//     the background and no offset is committed unless the caller asks.
//
// Three consequences that are easy to get wrong:
//
//  1. `Pull` must NOT suspend. The pull sysFlag is built as
//     `PullSysFlag.buildSysFlag(false, block, true, false)` with `block=false`
//     for pull() and true only for pullBlockIfNotFound(). Writing suspend=true
//     on the short path makes the broker hold the request until
//     brokerSuspendMaxTimeMillis (20s) while the client gives up at
//     consumerPullTimeoutMillis (10s) — a guaranteed timeout against a real
//     broker, on an empty queue only, which is why it survives casual testing.
//  2. The offset the caller passes is authoritative. There is no local cursor to
//     fall back to, and `FetchConsumeOffset` reads the STORE, so a caller that
//     never calls UpdateConsumeOffset sees -1 forever.
//  3. The retry ceiling is 16 for this consumer, NOT the push
//     consumer's unlimited -1. The broker takes the maxReconsumeTimes field at
//     face value for any client version >= V3_4_9, and
//     `reconsumeTimes(0) >= -1` is true, so -1 would send EVERY bounced message
//     straight to %DLQ%.
//
// The queue set is maintained by a pull-side rebalance, the same shape Java
// uses: EnableRebalance (default true, DefaultMQPullConsumer.java:97) gates a
// background goroutine that periodically runs doRebalance(), allocating this
// instance's share per topic and calling the queue listener only when the
// allocation changed. FetchMessageQueuesInBalance reads that table when it is
// warm and otherwise computes the same allocation on the spot.
//
// Deviation (documented): Java shares one RebalanceService per client instance;
// this port gives each facade its own goroutine, the same shape as the heartbeat
// loop. Which queues this instance owns, and when the listener fires, is
// unchanged. The caller still keeps the offsets itself — which is exactly what
// the class is for.
package client

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// Pull-consumer defaults.
const (
	// DefaultConsumerPullTimeoutMillis is the budget for the SHORT pull().
	DefaultConsumerPullTimeoutMillis = int64(10000)
	// DefaultBrokerSuspendMaxTimeMillis is how long the broker may hold a
	// long-poll request.
	DefaultBrokerSuspendMaxTimeMillis = int64(20000)
	// DefaultConsumerTimeoutMillisWhenSuspend is the CLIENT budget for the
	// long poll. Start() refuses a value below brokerSuspendMaxTimeMillis.
	DefaultConsumerTimeoutMillisWhenSuspend = int64(30000)
	// DefaultPullConsumerMaxReconsumeTimes: see rule 3 in the file header —
	// this is 16, not the push consumer's -1.
	DefaultPullConsumerMaxReconsumeTimes = int32(16)
	// DefaultPullMaxMsgNums matches the push consumer's pullBatchSize default.
	DefaultPullMaxMsgNums = int32(32)
	// defaultPullSendBackTimeoutMillis is the send-back RPC timeout on the
	// pull path (the push path uses 5000).
	defaultPullSendBackTimeoutMillis = int64(3000)

	// DefaultRebalanceWaitMillis is the round period of the pull-side
	// rebalance (Java `rocketmq.client.rebalance.waitInterval`).
	DefaultRebalanceWaitMillis = int64(20000)
	// DefaultRebalanceMinIntervalMillis is the retry period after an
	// unbalanced round (Java `rocketmq.client.rebalance.minInterval`).
	DefaultRebalanceMinIntervalMillis = int64(1000)
	// pullRebalanceShutdownWait bounds how long Shutdown waits for the
	// rebalance goroutine to notice the stop signal.
	pullRebalanceShutdownWait = 2 * time.Second
)

// DefaultMQPullConsumer is the active pull consumer.
type DefaultMQPullConsumer struct {
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

	messageModel     string
	consumeFromWhere string

	nameServerAddrs []string
	pollNamesrvIntv int64

	// Instance knobs seeded at Start and applied live when the shared instance
	// already exists: receive-body decode, internal-API timeout and the
	// concurrent heartbeat round.
	decodeReadBody                    bool
	mqClientAPITimeoutMillis          int64
	enableConcurrentHeartbeat         bool
	concurrentHeartbeatThreadPoolSize int

	// Pull timings — see the file header for how the three relate.
	brokerSuspendMaxTimeMillis       int64
	consumerPullTimeoutMillis        int64
	consumerTimeoutMillisWhenSuspend int64
	maxReconsumeTimes                int32

	// allocateStrategy is validated at Start and drives the pull-side
	// rebalance's per-topic allocation.
	allocateStrategy AllocateMessageQueueStrategy
	queueListener    MessageQueueListener

	// enableRebalance is Java DefaultMQPullConsumer#enableRebalance (default
	// true): off makes doRebalance report "balanced" without allocating
	// anything, so the caller keeps computing on the spot through
	// FetchMessageQueuesInBalance.
	enableRebalance bool
	// rebalanceWaitMillis / rebalanceMinIntervalMillis mirror RebalanceService:
	// the full wait after a balanced round, the short retry otherwise.
	rebalanceWaitMillis        int64
	rebalanceMinIntervalMillis int64
	// rebalanceTable is the product of the background rebalance: topic -> the
	// queues allocated to this instance. Java reads the keys of the rebalance's
	// process-queue table; this port keeps an equivalent per-topic table, which
	// FetchMessageQueuesInBalance prefers when it is warm. Guarded by mu.
	rebalanceTable map[string][]common.MessageQueue
	// rebalanceStopCh / rebalanceStopOnce / rebalanceWG drive one goroutine per
	// facade (see the file header's deviation note); rebalanceWakeCh is the
	// NOTIFY_CONSUMER_IDS_CHANGED kick, coalesced to capacity 1 so a burst of
	// notifications costs one round.
	rebalanceStopCh   chan struct{}
	rebalanceStopOnce sync.Once
	rebalanceWakeCh   chan struct{}
	rebalanceWG       sync.WaitGroup

	// registerTopics: every one of them is subscribed
	// with SUB_ALL at Start before the client registers, which is what
	// puts them in the heartbeat. Topics reached only through Pull() are added
	// lazily by subscriptionAutomatically.
	registerTopics map[string]struct{}
	subscription   map[string]*remoting.SubscriptionData

	filterMessageHooks  []FilterMessageHook
	consumeMessageHooks []ConsumeMessageHook

	instance    *Instance
	offsetStore OffsetStore
	pullAPI     *pullAPI

	// producerMu/producer back the send-back fallback: a failed
	// CONSUMER_SEND_MSG_BACK is re-sent as a normal message to %RETRY%<group>
	// through a CLIENT_INNER_PRODUCER built from this consumer's config (the
	// same internal producer the push path uses). Built lazily on the first
	// failed send-back and torn down in Shutdown.
	producerMu sync.Mutex
	producer   *DefaultMQProducer

	// pullAsyncPool / pullCallbackPool back PullAsync (see pull_async.go):
	// the first runs the RPC off the caller's goroutine, the second delivers
	// the user callback. Built in Start, torn down in Shutdown — the same
	// lifecycle the producer's async send executors have. Swapped to nil on
	// shutdown, so a late submit is refused synchronously instead of queueing
	// onto a dead consumer.
	pullAsyncPool    atomic.Pointer[boundedPool]
	pullCallbackPool atomic.Pointer[boundedPool]

	// offsetQueues is the local-offset queue set: every queue whose offset the
	// caller has parked in offsetStore. The rebalance table describes queue
	// OWNERSHIP only and carries no offsets, so this set is still tracked here.
	// It matters
	// because the broker-backed offset store's PersistAll REMOVES the entries
	// for queues
	// it is not handed, so an incomplete set silently discards the caller's
	// progress. Guarded by mu.
	offsetQueues map[common.MessageQueue]struct{}

	started bool

	// startTime is the consumer start timestamp, stamped in Start() and
	// reported as PROP_CONSUMER_START_TIMESTAMP in the 307 answer. This pull
	// path writes that property and nothing else beyond the
	// object2Properties dump, so it is the one per-consumer property this type
	// contributes.
	startTime time.Time
}

// NewDefaultMQPullConsumer requires the group up front: an empty one is refused
// here rather than at Start.
func NewDefaultMQPullConsumer(group string) (*DefaultMQPullConsumer, error) {
	if err := common.CheckGroup(group); err != nil {
		return nil, err
	}
	return &DefaultMQPullConsumer{
		consumerGroup:                    group,
		instanceName:                     common.DefaultInstanceName,
		messageModel:                     MessageModelClustering,
		consumeFromWhere:                 ConsumeFromWhereLastOffset,
		pollNamesrvIntv:                  defaultConsumerPollNamesrvInterval,
		decodeReadBody:                   true,
		mqClientAPITimeoutMillis:         DefaultMQClientAPITimeoutMillis,
		brokerSuspendMaxTimeMillis:       DefaultBrokerSuspendMaxTimeMillis,
		consumerPullTimeoutMillis:        DefaultConsumerPullTimeoutMillis,
		consumerTimeoutMillisWhenSuspend: DefaultConsumerTimeoutMillisWhenSuspend,
		maxReconsumeTimes:                DefaultPullConsumerMaxReconsumeTimes,
		registerTopics:                   map[string]struct{}{},
		subscription:                     map[string]*remoting.SubscriptionData{},
		offsetQueues:                     map[common.MessageQueue]struct{}{},
		// Pull-side rebalance defaults: on, 20s period, 1s retry.
		enableRebalance:            true,
		rebalanceWaitMillis:        DefaultRebalanceWaitMillis,
		rebalanceMinIntervalMillis: DefaultRebalanceMinIntervalMillis,
		rebalanceTable:             map[string][]common.MessageQueue{},
		rebalanceStopCh:            make(chan struct{}),
		rebalanceWakeCh:            make(chan struct{}, 1),
		// Average allocation is the default, and Start refuses a nil strategy,
		// so it must never be nil by default.
		allocateStrategy: AllocateMessageQueueAveragely{},
		// Every pull consumer uses a stream request type — the `@STREAM`
		// clientId suffix is not optional here, unlike on the producer.
		streamRequest: true,
	}, nil
}

// MustNewDefaultMQPullConsumer panics instead of returning an error; for
// package-level wiring and tests where an invalid group is a programming error.
func MustNewDefaultMQPullConsumer(group string) *DefaultMQPullConsumer {
	c, err := NewDefaultMQPullConsumer(group)
	if err != nil {
		panic(err)
	}
	return c
}

// ---------------------------------------------------------------- configuration

func (c *DefaultMQPullConsumer) ConsumerGroup() string { return c.consumerGroup }

// SetNameServerAddresses replaces the nameserver address list.
func (c *DefaultMQPullConsumer) SetNameServerAddresses(addrs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nameServerAddrs = append([]string(nil), addrs...)
}

func (c *DefaultMQPullConsumer) NameServerAddresses() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.nameServerAddrs...)
}

func (c *DefaultMQPullConsumer) SetInstanceName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.instanceName = name
}

func (c *DefaultMQPullConsumer) SetClientID(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clientID = id
}

func (c *DefaultMQPullConsumer) ClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

// SetUnitName / SetUnitMode / SetEnableStreamRequestType set the client config
// fields of the same names.
func (c *DefaultMQPullConsumer) SetUnitName(unit string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unitName = unit
}

func (c *DefaultMQPullConsumer) SetUnitMode(mode bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unitMode = mode
}

func (c *DefaultMQPullConsumer) SetEnableStreamRequestType(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streamRequest = enable
}

// SetNamespace is the CLIENT-side namespace: topic names are wrapped before they
// go on the wire and unwrapped again on the way back (resetTopic on pull
// results).
func (c *DefaultMQPullConsumer) SetNamespace(ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespace = ns
}

// SetNamespaceV2 is the SERVER-side namespace (the `ns` extField), which does
// not mangle topic names.
func (c *DefaultMQPullConsumer) SetNamespaceV2(ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespaceV2 = ns
}

func (c *DefaultMQPullConsumer) SetRpcHook(hook remoting.RPCHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rpcHook = hook
}

// SetTlsEnable turns TLS per consumer (nil = leave the instance default).
func (c *DefaultMQPullConsumer) SetTlsEnable(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tlsEnable = &enable
}

func (c *DefaultMQPullConsumer) SetSocksProxy(proxy *remoting.SocksProxyConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.socksProxy = proxy
}

// SetDecodeReadBody seeds the shared instance's receive-body decode switch:
// when off, pull frames are walked without materialising bodies. It takes
// effect on a running instance immediately.
func (c *DefaultMQPullConsumer) SetDecodeReadBody(enable bool) {
	c.mu.Lock()
	c.decodeReadBody = enable
	inst := c.instance
	c.mu.Unlock()
	if inst != nil {
		inst.SetDecodeReadBody(enable)
	}
}

// SetMQClientAPITimeoutMillis sets the internal-API budget used by heartbeats,
// route fetches, unregister and consumer-list RPCs. Non-positive values
// restore the 3000ms default.
func (c *DefaultMQPullConsumer) SetMQClientAPITimeoutMillis(timeoutMillis int64) {
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
func (c *DefaultMQPullConsumer) SetEnableConcurrentHeartbeat(enable bool) {
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
func (c *DefaultMQPullConsumer) SetConcurrentHeartbeatThreadPoolSize(size int) {
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

func (c *DefaultMQPullConsumer) SetMessageModel(model string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messageModel = model
}

func (c *DefaultMQPullConsumer) MessageModel() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.messageModel
}

func (c *DefaultMQPullConsumer) ConsumeFromWhere() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumeFromWhere
}

func (c *DefaultMQPullConsumer) IsUnitMode() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unitMode
}

// SetPollNameServerInterval sets the nameserver poll interval, read once
// when the instance is created.
func (c *DefaultMQPullConsumer) SetPollNameServerInterval(millis int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pollNamesrvIntv = millis
}

func (c *DefaultMQPullConsumer) SetBrokerSuspendMaxTimeMillis(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.brokerSuspendMaxTimeMillis = v
}

func (c *DefaultMQPullConsumer) SetConsumerPullTimeoutMillis(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumerPullTimeoutMillis = v
}

func (c *DefaultMQPullConsumer) SetConsumerTimeoutMillisWhenSuspend(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumerTimeoutMillisWhenSuspend = v
}

// SetMaxReconsumeTimes sets the retry ceiling carried on bounced messages. See
// rule 3 in the file header — the pull default is 16, not -1.
func (c *DefaultMQPullConsumer) SetMaxReconsumeTimes(v int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxReconsumeTimes = v
}

// BrokerSuspendMaxTimeMillis returns the broker-side suspend budget.
func (c *DefaultMQPullConsumer) BrokerSuspendMaxTimeMillis() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.brokerSuspendMaxTimeMillis
}

// ConsumerPullTimeoutMillis returns the SHORT-pull budget.
func (c *DefaultMQPullConsumer) ConsumerPullTimeoutMillis() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumerPullTimeoutMillis
}

// ConsumerTimeoutMillisWhenSuspend returns the CLIENT budget for the long poll.
func (c *DefaultMQPullConsumer) ConsumerTimeoutMillisWhenSuspend() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumerTimeoutMillisWhenSuspend
}

// MaxReconsumeTimes returns the retry ceiling carried on bounced messages.
func (c *DefaultMQPullConsumer) MaxReconsumeTimes() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxReconsumeTimes
}

// SetAllocateMessageQueueStrategy installs the allocation strategy; Start
// refuses nil.
func (c *DefaultMQPullConsumer) SetAllocateMessageQueueStrategy(s AllocateMessageQueueStrategy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.allocateStrategy = s
}

// SetMessageQueueListener installs the queue-change listener: it is
// notified when the topic's queue set (as this client sees it) changes.
func (c *DefaultMQPullConsumer) SetMessageQueueListener(l MessageQueueListener) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueListener = l
}

// SetEnableRebalance is Java DefaultMQPullConsumer#setEnableRebalance: off means
// doRebalance/tryRebalance return without allocating anything, so the caller's
// own FetchMessageQueuesInBalance keeps computing on the spot.
func (c *DefaultMQPullConsumer) SetEnableRebalance(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enableRebalance = enable
}

// IsEnableRebalance reports the switch (default true).
func (c *DefaultMQPullConsumer) IsEnableRebalance() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enableRebalance
}

// SetRebalanceWaitMillis sets the round period (Java
// `rocketmq.client.rebalance.waitInterval`, 20000). Non-positive values fall
// back to 1ms: a zero period would spin the loop.
func (c *DefaultMQPullConsumer) SetRebalanceWaitMillis(millis int64) {
	if millis <= 0 {
		millis = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebalanceWaitMillis = millis
}

// RebalanceWaitMillis returns the round period.
func (c *DefaultMQPullConsumer) RebalanceWaitMillis() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rebalanceWaitMillis
}

// SetRebalanceMinIntervalMillis sets the retry period (Java
// `rocketmq.client.rebalance.minInterval`, 1000). Non-positive values fall back
// to 1ms.
func (c *DefaultMQPullConsumer) SetRebalanceMinIntervalMillis(millis int64) {
	if millis <= 0 {
		millis = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebalanceMinIntervalMillis = millis
}

// RebalanceMinIntervalMillis returns the retry period.
func (c *DefaultMQPullConsumer) RebalanceMinIntervalMillis() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rebalanceMinIntervalMillis
}

// GetRebalanceTable returns a snapshot of the background rebalance's product:
// topic -> the queues allocated to this instance. Both the map and every slice
// are copied, so a caller can neither mutate the table nor observe it changing
// underneath.
func (c *DefaultMQPullConsumer) GetRebalanceTable() map[string][]common.MessageQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][]common.MessageQueue, len(c.rebalanceTable))
	for topic, mqs := range c.rebalanceTable {
		out[topic] = append([]common.MessageQueue(nil), mqs...)
	}
	return out
}

// RegisterFilterMessageHook appends a filter hook; the hooks run
// inside processPullResult, i.e. on the pull path before the caller sees the
// messages.
func (c *DefaultMQPullConsumer) RegisterFilterMessageHook(hook FilterMessageHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.filterMessageHooks = append(c.filterMessageHooks, hook)
}

func (c *DefaultMQPullConsumer) HasFilterMessageHook() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.filterMessageHooks) > 0
}

// RegisterConsumeMessageHook appends a consume hook. The pair
// fires around the pull result, not around user
// consumption — the pull consumer has no consume service to hang it off.
func (c *DefaultMQPullConsumer) RegisterConsumeMessageHook(hook ConsumeMessageHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeMessageHooks = append(c.consumeMessageHooks, hook)
}

// ConsumeType is always CONSUME_ACTIVELY for this class.
func (c *DefaultMQPullConsumer) ConsumeType() string { return ConsumeTypeActively }

// Subscriptions feeds ConsumerData.subscriptionDataSet in the heartbeat.
func (c *DefaultMQPullConsumer) Subscriptions() []*remoting.SubscriptionData {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*remoting.SubscriptionData, 0, len(c.subscription))
	for _, sub := range c.subscription {
		out = append(out, sub)
	}
	// Map order is random; a stable order keeps wire diffs and test
	// expectations readable — the heartbeat is where a flapping order shows up
	// as a diff on every beat.
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}

// RegisterTopic adds a topic to the register set: it is subscribed
// with SUB_ALL at Start so it reaches the broker's
// subscription group — pulling a topic without registering it works, but the
// broker then has no subscription entry and the retry topic never gets created.
func (c *DefaultMQPullConsumer) RegisterTopic(topic string) {
	if topic == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registerTopics[topic] = struct{}{}
}

// Subscribe is the explicit form of subscriptionAutomatically: it registers the
// topic and installs the subExpression filter.
func (c *DefaultMQPullConsumer) Subscribe(topic, subExpression string) error {
	if err := common.CheckTopic(topic); err != nil {
		return err
	}
	sub, err := remoting.FilterAPI{}.BuildSubscriptionData(topic, subExpression)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscription[topic] = sub
	c.registerTopics[topic] = struct{}{}
	return nil
}

// Unsubscribe drops the subscription and the register entry for the topic.
func (c *DefaultMQPullConsumer) Unsubscribe(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscription, topic)
	delete(c.registerTopics, topic)
}

// subscriptionAutomatically registers any topic pulled for the first time:
// pulling a topic
// this client has never heard of must still register it, or the heartbeat never
// mentions it and the broker's consumer manager cannot reach the group.
func (c *DefaultMQPullConsumer) subscriptionAutomatically(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.subscription[topic]; ok {
		return
	}
	sub, err := remoting.FilterAPI{}.BuildSubscriptionData(topic, "*")
	if err != nil {
		// An unparsable auto-subscription is swallowed — it must
		// not fail an otherwise valid pull.
		return
	}
	c.subscription[topic] = sub
}

// ---------------------------------------------------------------- pull-side rebalance
//
// Java's pull consumer DOES rebalance: the shared RebalanceService thread calls
// mqClientFactory.doRebalance() every waitInterval (20s), which walks every
// registered consumer and calls tryRebalance(); that gates on enableRebalance
// (default true) and then runs rebalanceImpl.doRebalance(false) -> allocate ->
// messageQueueChanged(topic, mqAll, mqDivided). This port drives the same
// allocation formula from one goroutine per facade (the documented deviation).

// rebalanceLoop is Java RebalanceService#run: wait, rebalance, then pick the
// next delay. `balanced` -> wait the full waitInterval; otherwise retry after
// minInterval so a freshly appeared queue is claimed quickly. A wake (the
// NOTIFY_CONSUMER_IDS_CHANGED kick) cuts the wait short and runs a round now.
func (c *DefaultMQPullConsumer) rebalanceLoop() {
	defer c.rebalanceWG.Done()
	c.mu.Lock()
	interval := c.rebalanceWaitMillis
	stop := c.rebalanceStopCh
	c.mu.Unlock()
	for {
		timer := time.NewTimer(time.Duration(interval) * time.Millisecond)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-c.rebalanceWakeCh:
			timer.Stop()
		case <-timer.C:
		}
		if !c.IsStarted() {
			return
		}
		balanced := c.doRebalance()
		c.mu.Lock()
		if balanced {
			interval = c.rebalanceWaitMillis
		} else {
			interval = c.rebalanceMinIntervalMillis
		}
		c.mu.Unlock()
	}
}

// stopRebalanceLoop closes the stop channel and waits — bounded — for the loop
// to return. Called before the client instance is torn down: the loop calls
// back into it for subscribe info and the group's consumer list.
func (c *DefaultMQPullConsumer) stopRebalanceLoop() {
	c.mu.Lock()
	stop := c.rebalanceStopCh
	c.mu.Unlock()
	if stop != nil {
		c.rebalanceStopOnce.Do(func() { close(stop) })
	}
	done := make(chan struct{})
	go func() {
		c.rebalanceWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(pullRebalanceShutdownWait):
		common.LogWarnf("pull consumer %s: timed out waiting for the rebalance loop to stop", c.consumerGroup)
	}
}

// doRebalance is Java DefaultMQPullConsumerImpl#tryRebalance (:398-406): walk
// every registered topic and report whether the round left everything balanced.
//
// With enableRebalance == false Java returns **true** (its "nothing left to
// retry" answer). Returning false instead would pin the loop at minInterval and
// burn a round every second for nothing.
func (c *DefaultMQPullConsumer) doRebalance() bool {
	c.mu.Lock()
	enabled := c.enableRebalance
	started := c.started
	instance := c.instance
	topics := make([]string, 0, len(c.registerTopics))
	for topic := range c.registerTopics {
		topics = append(topics, topic)
	}
	c.mu.Unlock()
	if !enabled {
		return true
	}
	if !started || instance == nil {
		return true
	}
	sort.Strings(topics)
	balanced := true
	for _, topic := range topics {
		if !c.rebalanceTopicSafely(topic, instance) {
			balanced = false
		}
	}
	return balanced
}

// rebalanceTopicSafely isolates one topic's round: Java wraps rebalanceByTopic
// in try/catch inside doRebalance, so one broken strategy or a bad allocation
// cannot stop the remaining topics from being balanced.
func (c *DefaultMQPullConsumer) rebalanceTopicSafely(topic string, instance *Instance) (balanced bool) {
	defer func() {
		if r := recover(); r != nil {
			common.LogErrorf("rebalance topic %s failed: %v", topic, r)
			balanced = false
		}
	}()
	return c.rebalanceByTopic(topic, instance)
}

// rebalanceByTopic is Java RebalanceImpl#rebalanceByTopic (pull path) plus
// messageQueueChanged. It returns whether the topic is already balanced.
//
// The listener fires **only when the allocation changed**, with
// (topic, mqAll, mqDivided) — exactly the three arguments
// RebalancePullImpl#messageQueueChanged (:44-53) passes.
//
// Deviation (documented): Java compares allocateResultSet against the freshly
// updated process-queue table, so a "changed then already in place" round can
// report balanced immediately. Here balanced means "identical to the previous
// round", which costs at most one extra minInterval confirmation round and never
// hammers.
func (c *DefaultMQPullConsumer) rebalanceByTopic(topic string, instance *Instance) bool {
	c.mu.Lock()
	model := c.messageModel
	group := c.consumerGroup
	clientID := c.clientID
	strategy := c.allocateStrategy
	c.mu.Unlock()
	if strategy == nil {
		strategy = AllocateMessageQueueAveragely{}
	}

	mqAll := append([]common.MessageQueue(nil), instance.GetTopicSubscribeInfo(topic)...)
	sort.Slice(mqAll, func(i, j int) bool { return mqAll[i].CompareTo(mqAll[j]) < 0 })
	if len(mqAll) == 0 {
		// Java RebalanceImpl:276-294 notifies with two empty sets and warns
		// when the topic's route disappears, then leaves `balanced` at its
		// initial true. Reporting unbalanced here would pin the loop at
		// minInterval and spin once a second while the topic is briefly gone.
		c.mu.Lock()
		hadQueues := len(c.rebalanceTable[topic]) > 0
		if hadQueues {
			c.rebalanceTable[topic] = []common.MessageQueue{}
		}
		c.mu.Unlock()
		if hadQueues {
			common.LogWarnf("doRebalance, %s, topic %s not exist, notify empty queue set", group, topic)
			c.notifyQueueChanged(topic, []common.MessageQueue{}, []common.MessageQueue{})
		}
		return true
	}

	var divided []common.MessageQueue
	if model == MessageModelBroadcasting {
		// BROADCASTING: every queue, without consulting the consumer list.
		divided = append([]common.MessageQueue(nil), mqAll...)
	} else {
		cidAll, answered := instance.GetConsumerIDListByGroup(topic, group, 0)
		if !answered || len(cidAll) == 0 {
			// Java (:300-302) warns "get consumer id list failed" and NOTIFIES
			// NOTHING, leaving balanced at true so the next round waits the full
			// interval.
			common.LogDebugf("doRebalance, %s %s, get consumer id list failed", group, topic)
			return true
		}
		sort.Strings(cidAll)
		got := strategy.Allocate(group, clientID, mqAll, cidAll)
		if reporter, ok := strategy.(AllocateErrReporter); ok {
			if err := reporter.AllocateErr(); err != nil {
				common.LogErrorf("allocate message queue exception. strategy name: %s, group=%s: %v",
					strategyName(strategy), group, err)
				return false
			}
		}
		// Never let a strategy's surprising return value leak another topic
		// into this topic's assignment.
		for _, mq := range got {
			if mq.Topic == topic {
				divided = append(divided, mq)
			}
		}
		sort.Slice(divided, func(i, j int) bool { return divided[i].CompareTo(divided[j]) < 0 })
	}

	c.mu.Lock()
	previous, hadPrevious := c.rebalanceTable[topic]
	var balanced bool
	if !hadPrevious {
		// First sight of this topic: Java's updateProcessQueueTableInRebalance counts only
		// table additions/removals as a change, so "nothing -> nothing" is no change -- an
		// empty opening share stays silent and the round counts as balanced.
		balanced = len(divided) == 0
	} else {
		balanced = sameMQSet(previous, divided)
	}
	c.rebalanceTable[topic] = append([]common.MessageQueue(nil), divided...)
	c.mu.Unlock()
	if !balanced {
		common.LogInfof("client rebalanced result changed. Group: %s, topic: %s, mqAllSize: %d, rebalanceResultSize: %d",
			group, topic, len(mqAll), len(divided))
		c.notifyQueueChanged(topic, mqAll, divided)
	}
	return balanced
}

// notifyQueueChanged is Java RebalancePullImpl#messageQueueChanged (:44-53):
// the listener is optional, and a failing listener is logged and swallowed so
// one bad callback cannot kill the loop and freeze every allocation.
func (c *DefaultMQPullConsumer) notifyQueueChanged(topic string, mqAll, mqDivided []common.MessageQueue) {
	c.mu.Lock()
	listener := c.queueListener
	c.mu.Unlock()
	if listener == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			common.LogErrorf("messageQueueChanged exception: %v", r)
		}
	}()
	listener.MessageQueueChanged(topic, mqAll, mqDivided)
}

// sameMQSet reports whether two allocations hold the same queues. Order carries
// no meaning, and "lost one, gained another" must count as a change even though
// the count is unchanged (the rule both Java's allocateResultSet comparison and
// the push-side rebalance use).
func sameMQSet(left, right []common.MessageQueue) bool {
	if len(left) != len(right) {
		return false
	}
	leftSet := make(map[common.MessageQueue]struct{}, len(left))
	for _, mq := range left {
		leftSet[mq] = struct{}{}
	}
	rightSet := make(map[common.MessageQueue]struct{}, len(right))
	for _, mq := range right {
		rightSet[mq] = struct{}{}
	}
	if len(leftSet) != len(rightSet) {
		return false
	}
	for mq := range rightSet {
		if _, ok := leftSet[mq]; !ok {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------- lifecycle

// Start brings the consumer up. The caller still drives every pull through
// Pull; Start additionally installs the pull-side rebalance goroutine
// (EnableRebalance, 20s period) so the queue set is kept balanced in the
// background, the same shape Java's RebalanceService gives this consumer.
func (c *DefaultMQPullConsumer) Start() error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	if err := c.checkConfigLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	// Every registered topic becomes SUB_ALL before the client instance is
	// created, so the first heartbeat already carries it.
	for topic := range c.registerTopics {
		if _, ok := c.subscription[topic]; ok {
			continue
		}
		sub, err := remoting.FilterAPI{}.BuildSubscriptionData(topic, "*")
		if err != nil {
			c.mu.Unlock()
			return common.ClientError(fmt.Sprintf("subscription exception: %v", err))
		}
		c.subscription[topic] = sub
	}
	if c.namespace != "" {
		// Must run BEFORE the group is used to derive %RETRY%: the retry topic
		// is %RETRY% + the WRAPPED group name.
		c.consumerGroup = common.WrapNamespace(c.namespace, c.consumerGroup)
	}
	if c.messageModel == MessageModelClustering {
		// Only CLUSTERING rewrites the DEFAULT instance name to the pid plus a nanotime.
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
	c.mu.Unlock()

	// Namespace + stream + user(ACL) + zone, composed in a fixed order and once
	// per instance — the signature has to cover exactly these fields.
	instance.EnsureRPCHooks(namespaceV2, c.streamRequest, userHook)
	if err := instance.Start(); err != nil {
		c.mu.Lock()
		c.instance = nil
		c.mu.Unlock()
		return err
	}

	c.mu.Lock()
	// Offset store choice: a caller-supplied store wins, otherwise CLUSTERING gets the
	// remote one and BROADCASTING the local file one.
	if c.messageModel == MessageModelBroadcasting {
		c.offsetStore = NewLocalFileOffsetStore(c.clientID, c.consumerGroup)
	} else {
		c.offsetStore = NewRemoteBrokerOffsetStore(instance, c.consumerGroup)
	}
	c.pullAPI = newPullAPI(instance, c.consumerGroup)
	c.pullAPI.unitMode = c.unitMode
	c.pullAPI.hooks = c.filterMessageHooks
	store := c.offsetStore
	c.startTime = time.Now()
	// The async-pull executors are built here (not in the constructor) so a
	// consumer that never starts owns no live goroutine pools; Shutdown's
	// destroyPullAsyncExecutors is the mirror image.
	c.initPullAsyncExecutors()
	c.started = true
	// A restart gets a fresh stop channel (the previous Shutdown closed the old
	// one); the wake channel is reused, so drain any kick left from before.
	c.rebalanceStopCh = make(chan struct{})
	c.rebalanceStopOnce = sync.Once{}
	select {
	case <-c.rebalanceWakeCh:
	default:
	}
	topics := make([]string, 0, len(c.subscription))
	for t := range c.subscription {
		topics = append(topics, t)
	}
	c.mu.Unlock()

	_ = store.Load()
	// RegisterConsumer stores the consumer in the instance so the
	// heartbeat task picks it up; without it the broker never learns the group
	// is online and %RETRY% is never created.
	instance.RegisterConsumer(c.consumerGroup, c)
	for _, topic := range topics {
		instance.RegisterTopicInUse(topic)
		if _, err := instance.UpdateTopicRouteInfoFromNameServer(topic, 0, false); err != nil {
			common.LogDebugf("initial route refresh failed for %s: %v", topic, err)
		}
	}
	// Java registers the pull consumer with the shared RebalanceService in
	// start(); here the facade owns the loop (see the deviation note).
	c.rebalanceWG.Add(1)
	go c.rebalanceLoop()
	return nil
}

// checkConfigLocked runs the fixed check list — same order, same wording.
// Must be called with c.mu held.
// initPullAsyncExecutors builds the two pools PullAsync runs on (the
// pullAsyncPool / pullCallbackPool fields). Mirrors the producer's async send
// executors: built in Start, torn down in Shutdown, so a consumer that never
// starts owns no live goroutine pools and Shutdown leaves nothing running.
func (c *DefaultMQPullConsumer) initPullAsyncExecutors() {
	workers := runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}
	c.pullAsyncPool.Store(newBoundedPool("PullAsyncExecutor", workers, 1<<16))
	c.pullCallbackPool.Store(newBoundedPool("PullCallbackExecutor", workers, 1<<16))
}

// destroyPullAsyncExecutors drains both pools (ExecutorService.shutdown
// semantics: stop accepting, let queued tasks finish). Must be called WITHOUT
// c.mu held — draining waits for in-flight tasks that take c.mu themselves.
func (c *DefaultMQPullConsumer) destroyPullAsyncExecutors() {
	if pool := c.pullAsyncPool.Swap(nil); pool != nil {
		pool.Shutdown()
	}
	if pool := c.pullCallbackPool.Swap(nil); pool != nil {
		pool.Shutdown()
	}
}

func (c *DefaultMQPullConsumer) checkConfigLocked() error {
	if err := common.CheckGroup(c.consumerGroup); err != nil {
		return err
	}
	if c.consumerGroup == common.DefaultConsumerGroup {
		return common.ClientError(fmt.Sprintf(
			"consumerGroup can not equal %s, please specify another one.", common.DefaultConsumerGroup))
	}
	if c.messageModel == "" {
		return common.ClientError("messageModel is null")
	}
	if c.allocateStrategy == nil {
		return common.ClientError("allocateMessageQueueStrategy is null")
	}
	if len(c.nameServerAddrs) == 0 {
		return common.ClientError("name server address is not set")
	}
	// The long poll can only work if the CLIENT waits longer than the
	// BROKER is allowed to hold the request.
	if c.consumerTimeoutMillisWhenSuspend < c.brokerSuspendMaxTimeMillis {
		return common.ClientError(
			"Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis")
	}
	return nil
}

// Shutdown persists the offsets while the consumer still counts as
// running, unregisters the group at every broker
// (the broker would otherwise keep routing rebalance notifications at a dead
// client for ~120s), then shuts the factory down.
//
// The ORDER inside is load-bearing: the offsets must be persisted while the
// consumer still counts as RUNNING, i.e. BEFORE leaving that state.
// Flipping `started` up front (the obvious way to write this)
// makes PersistConsumerOffset's own isRunning() guard fire and the caller's last
// offsets are silently lost on every clean shutdown.
func (c *DefaultMQPullConsumer) Shutdown() {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	// Stop the pull-side rebalance first: its rounds call back into the client
	// instance (subscribe info / consumer list) and into the user listener, so
	// it must be gone before anything below is torn down. Wait bounded.
	c.stopRebalanceLoop()

	// Drain the async-pull executors BEFORE leaving the RUNNING state (same
	// ordering reason as the persist below): the in-flight pulls still pass
	// requireClient, and when this returns no async attempt of ours can be
	// running against the instance we are about to detach. Deliberately NOT
	// under c.mu — the draining workers take it themselves.
	c.destroyPullAsyncExecutors()

	if err := c.PersistConsumerOffset(); err != nil {
		common.LogDebugf("persist offsets on shutdown failed: %v", err)
	}

	c.mu.Lock()
	if !c.started {
		// A concurrent Shutdown got here first and is already tearing down.
		c.mu.Unlock()
		return
	}
	c.started = false
	instance := c.instance
	group := c.consumerGroup
	producer := c.producer
	c.producer = nil
	c.mu.Unlock()

	// The lazily built send-back fallback producer owns its own client slot, so
	// it is torn down explicitly before the shared instance goes away.
	if producer != nil {
		producer.Shutdown()
	}
	if instance != nil {
		instance.UnregisterClientAllBrokers("", group)
		instance.UnregisterConsumer(group)
		instance.Shutdown()
		instance.DetachFromRegistryIfLastTenant()
	}
	c.mu.Lock()
	c.instance = nil
	c.mu.Unlock()
}

// IsStarted reports whether Start succeeded and Shutdown has not run.
func (c *DefaultMQPullConsumer) IsStarted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

func (c *DefaultMQPullConsumer) requireClient() (*Instance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started || c.instance == nil {
		return nil, common.ClientError("consumer not started, call Start() first")
	}
	return c.instance, nil
}

// ViewMessage is Java DefaultMQPullConsumerImpl#viewMessage
// (DefaultMQPullConsumerImpl.java:838) parity: the MQAdminImpl single-leg
// path — offset msgId only, no uniq-key fallback at this layer.
func (c *DefaultMQPullConsumer) ViewMessage(topic, msgID string) (*common.MessageExt, error) {
	client, err := c.requireClient()
	if err != nil {
		return nil, err
	}
	return client.ViewMessage(topic, msgID, defaultViewMessageTimeoutMillis)
}

// Instance exposes the underlying client instance (diagnostics/tests).
func (c *DefaultMQPullConsumer) Instance() *Instance {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.instance
}

// ---------------------------------------------------------------- pull

// Pull is a SHORT poll at an explicit offset, with the caller's own maxNums.
// `offset < 0` and `maxNums <= 0` are refused before anything goes on the wire.
func (c *DefaultMQPullConsumer) Pull(mq common.MessageQueue, subExpression string, offset int64, maxNums int32) (*PullResult, error) {
	c.mu.Lock()
	timeout := c.consumerPullTimeoutMillis
	c.mu.Unlock()
	return c.PullWithTimeout(mq, subExpression, offset, maxNums, timeout)
}

// PullWithTimeout is Pull with an explicit budget.
func (c *DefaultMQPullConsumer) PullWithTimeout(mq common.MessageQueue, subExpression string,
	offset int64, maxNums int32, timeoutMillis int64) (*PullResult, error) {
	sub, err := buildPullSubscription(mq, subExpression)
	if err != nil {
		return nil, err
	}
	return c.pullSync(mq, sub, offset, maxNums, false, timeoutMillis)
}

// PullBySelector is the MessageSelector flavour of Pull: a SQL92
// expression travels with its own expressionType, and a TAG selector keeps the
// subVersion at 0.
func (c *DefaultMQPullConsumer) PullBySelector(mq common.MessageQueue, selector MessageSelector,
	offset int64, maxNums int32) (*PullResult, error) {
	c.mu.Lock()
	timeout := c.consumerPullTimeoutMillis
	c.mu.Unlock()
	return c.PullBySelectorWithTimeout(mq, selector, offset, maxNums, timeout)
}

func (c *DefaultMQPullConsumer) PullBySelectorWithTimeout(mq common.MessageQueue, selector MessageSelector,
	offset int64, maxNums int32, timeoutMillis int64) (*PullResult, error) {
	sub, err := buildPullSelectorSubscription(mq, selector)
	if err != nil {
		return nil, err
	}
	return c.pullSync(mq, sub, offset, maxNums, false, timeoutMillis)
}

// PullBlockIfNotFound is the LONG poll: suspend=true, so the broker
// holds the request until a message arrives or brokerSuspendMaxTimeMillis
// elapses. The client budget switches to consumerTimeoutMillisWhenSuspend.
func (c *DefaultMQPullConsumer) PullBlockIfNotFound(mq common.MessageQueue, subExpression string,
	offset int64, maxNums int32) (*PullResult, error) {
	sub, err := buildPullSubscription(mq, subExpression)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	timeout := c.consumerTimeoutMillisWhenSuspend
	c.mu.Unlock()
	return c.pullSync(mq, sub, offset, maxNums, true, timeout)
}

// PullBlockIfNotFoundBySelector is the selector flavour of the long poll.
func (c *DefaultMQPullConsumer) PullBlockIfNotFoundBySelector(mq common.MessageQueue,
	selector MessageSelector, offset int64, maxNums int32) (*PullResult, error) {
	sub, err := buildPullSelectorSubscription(mq, selector)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	timeout := c.consumerTimeoutMillisWhenSuspend
	c.mu.Unlock()
	return c.pullSync(mq, sub, offset, maxNums, true, timeout)
}

// buildPullSubscription builds the ad-hoc subscription for one pull: a null mq
// is an error, and a parse failure is reported as "parse subscription error"
// rather than leaking the FilterAPI text.
func buildPullSubscription(mq common.MessageQueue, subExpression string) (*remoting.SubscriptionData, error) {
	if mq.Topic == "" {
		return nil, common.ClientError("mq is null")
	}
	sub, err := remoting.FilterAPI{}.BuildSubscriptionData(mq.Topic, subExpression)
	if err != nil {
		return nil, common.ClientError(fmt.Sprintf("parse subscription error: %v", err))
	}
	return sub, nil
}

// buildPullSelectorSubscription builds a SubscriptionData straight from a
// MessageSelector (expression + expressionType).
//
// This port has no FilterAPI.build; the shape is copied from
// DefaultMQPushConsumer.SubscribeWithSelector, which had to solve the same
// problem: a TAG selector still needs tagsSet/codeSet from the FilterAPI, while
// a non-TAG one carries only its expressionType (and ClassFilterMode for the
// class-filter form).
func buildPullSelectorSubscription(mq common.MessageQueue, selector MessageSelector) (*remoting.SubscriptionData, error) {
	if mq.Topic == "" {
		return nil, common.ClientError("mq is null")
	}
	return buildSelectorSubscription(mq.Topic, selector)
}

// buildSelectorSubscription builds the subscription a selector stands for, for
// a topic that is not tied to a queue.
func buildSelectorSubscription(topic string, selector MessageSelector) (*remoting.SubscriptionData, error) {
	if topic == "" {
		return nil, common.ClientError("topic is empty")
	}
	exprType := selector.Type
	if exprType == "" {
		exprType = remoting.ExpressionTypeTag
	}
	expr := selector.Expression
	if exprType == remoting.ExpressionTypeTag {
		if expr == "" {
			expr = "*"
		}
		built, err := remoting.FilterAPI{}.BuildSubscriptionData(topic, expr)
		if err != nil {
			return nil, common.ClientError(fmt.Sprintf("parse subscription error: %v", err))
		}
		return built, nil
	}
	// Any other language is passed through verbatim, so an empty expression would
	// ask the broker to filter with nothing at all -- refuse instead of silently
	// widening it to every message.
	if expr == "" {
		return nil, common.ClientError(fmt.Sprintf("expression can not be null at this type %s", exprType))
	}
	sub := remoting.NewSubscriptionData(topic, expr)
	sub.ExpressionType = exprType
	sub.ClassFilterMode = exprType == remoting.ExpressionTypeClassFilter
	return sub, nil
}

func (c *DefaultMQPullConsumer) pullSync(mq common.MessageQueue, sub *remoting.SubscriptionData,
	offset int64, maxNums int32, block bool, timeoutMillis int64) (*PullResult, error) {
	return c.pullSyncWithMaxBytes(mq, sub, offset, maxNums, -1, block, timeoutMillis)
}

func (c *DefaultMQPullConsumer) pullSyncWithMaxBytes(mq common.MessageQueue, sub *remoting.SubscriptionData,
	offset int64, maxNums int32, maxMsgBytes int32, block bool, timeoutMillis int64) (*PullResult, error) {

	if _, err := c.requireClient(); err != nil {
		return nil, err
	}
	if mq.Topic == "" {
		return nil, common.ClientError("mq is null")
	}
	if offset < 0 {
		return nil, common.ClientError("offset < 0")
	}
	if maxNums <= 0 {
		return nil, common.ClientError("maxNums <= 0")
	}
	// Pulling a topic the caller never registered still has to reach the
	// heartbeat, otherwise the broker has no subscription entry for us.
	c.subscriptionAutomatically(mq.Topic)

	c.mu.Lock()
	api := c.pullAPI
	brokerSuspend := c.brokerSuspendMaxTimeMillis
	namespace := c.namespace
	consumeHooks := c.consumeMessageHooks
	group := c.consumerGroup
	c.mu.Unlock()
	if api == nil {
		return nil, common.ClientError("consumer not started, call Start() first")
	}

	// commitOffset=false (the caller commits), suspend=block,
	// subscription=true, classFilter=false.
	sysFlag := common.BuildSysFlagBasic(false, block, true, false)

	// For a TAG subscription the subVersion travels as 0; only an
	// expression with its own version (SQL92/class filter) sends the real one.
	// pullKernel writes sub.SubVersion straight into the header, so the zeroing
	// has to happen on the value it will read.
	if sub.ExpressionType == remoting.ExpressionTypeTag && sub.SubVersion != 0 {
		local := *sub
		local.SubVersion = 0
		sub = &local
	}
	result, err := api.pullKernel(group, mq, offset, sub, sysFlag, 0, maxNums,
		maxMsgBytes, brokerSuspend, timeoutMillis)
	if err != nil {
		return nil, err
	}
	result = api.processPullResult(mq, result, sub)
	resetTopicForNamespace(result.MsgFoundList, namespace)

	// The consume hook pair runs around the pull RESULT (the pull
	// consumer has no consume service), and it is marked succeeded as soon as
	// the pull returned: the caller's own handling is not observable here.
	if len(consumeHooks) > 0 {
		ctx := &ConsumeMessageContext{
			ConsumerGroup: group,
			MQ:            mq,
			MsgList:       result.MsgFoundList,
			Success:       true,
			Status:        "CONSUME_SUCCESS",
			AccessChannel: AccessChannelLocal,
			Namespace:     namespace,
		}
		executeConsumeHookBefore(consumeHooks, ctx)
		executeConsumeHookAfter(consumeHooks, ctx)
	}
	return result, nil
}

// resetTopicForNamespace un-wraps every returned message's topic before the
// caller sees it when a namespace is in play.
func resetTopicForNamespace(msgs []*common.MessageExt, namespace string) {
	if namespace == "" {
		return
	}
	for _, msg := range msgs {
		msg.Topic = common.WithoutNamespace(msg.Topic, namespace)
	}
}

// ---------------------------------------------------------------- offsets

// FetchConsumeOffset reads one queue's offset. `fromStore` picks READ_FROM_STORE
// (a
// broker round trip) over MEMORY_FIRST (the cache, then the broker). -1 means
// the broker has no record for this queue.
func (c *DefaultMQPullConsumer) FetchConsumeOffset(mq common.MessageQueue, fromStore bool) (int64, error) {
	c.mu.Lock()
	store := c.offsetStore
	c.mu.Unlock()
	if _, err := c.requireClient(); err != nil {
		return -1, err
	}
	if store == nil {
		return -1, common.ClientError("consumer not started, call Start() first")
	}
	mode := ReadMemoryFirst
	if fromStore {
		mode = ReadFromStore
	}
	offset, err := store.ReadOffset(mq, mode)
	if err != nil {
		return -1, err
	}
	if fromStore {
		// READ_FROM_STORE routes through fetchConsumeOffsetFromBroker, which
		// updates the local table on the way out,
		// so the queue now has a local entry whether it had one or not.
		c.rememberOffsetQueue(mq)
	}
	return offset, nil
}

// rememberOffsetQueue registers mq as "this consumer holds a local offset for
// it", so PersistConsumerOffset keeps it (see the offsetQueues field).
func (c *DefaultMQPullConsumer) rememberOffsetQueue(mq common.MessageQueue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.offsetQueues == nil {
		c.offsetQueues = map[common.MessageQueue]struct{}{}
	}
	c.offsetQueues[mq] = struct{}{}
}

// UpdateConsumeOffset parks one offset locally with increaseOnly=false,
// i.e. a backwards seek IS allowed. Nothing is sent to the broker; the periodic
// persist (or PersistConsumerOffset) does that.
func (c *DefaultMQPullConsumer) UpdateConsumeOffset(mq common.MessageQueue, offset int64) error {
	c.mu.Lock()
	store := c.offsetStore
	c.mu.Unlock()
	if _, err := c.requireClient(); err != nil {
		return err
	}
	if store == nil {
		return common.ClientError("consumer not started, call Start() first")
	}
	store.UpdateOffset(mq, offset, false)
	c.rememberOffsetQueue(mq)
	return nil
}

// UpdateConsumeOffsetToBroker writes one offset straight to
// the broker, skipping the store's periodic flush.
//
// The protocol's `isOneway` flag has no counterpart in this client's offset
// store (every
// write is a synchronous UPDATE_CONSUMER_OFFSET), so the round trip is always
// confirmed before this returns.
func (c *DefaultMQPullConsumer) UpdateConsumeOffsetToBroker(mq common.MessageQueue, offset int64) error {
	c.mu.Lock()
	store := c.offsetStore
	c.mu.Unlock()
	if _, err := c.requireClient(); err != nil {
		return err
	}
	if remote, ok := store.(*RemoteBrokerOffsetStore); ok {
		if err := remote.updateConsumeOffsetToBroker(mq, offset); err != nil {
			return err
		}
		// The DEFAULT (confirmed-write) path of the store does NOT touch the
		// local table — a broker write only calls updateOffset when it is
		// oneway. Register the queue anyway so a later persistAll
		// cannot drop an entry another call created for it.
		c.rememberOffsetQueue(mq)
		return nil
	}
	if store == nil {
		return common.ClientError("consumer not started, call Start() first")
	}
	// A file store has no broker to write to; persist locally instead of
	// pretending the call reached one.
	store.UpdateOffset(mq, offset, false)
	c.rememberOffsetQueue(mq)
	return store.Persist(mq)
}

// PersistConsumerOffset flushes every queue this consumer knows about. The
// rebalance table describes queue ownership, not offsets (the caller owns the
// cursor), so the flush set is the union of the two ways a queue can enter this
// consumer's world — the queues it has PULLED (pullFromWhichNode) and the queues
// whose offset the CALLER parked locally (offsetQueues). Both halves are needed:
// the remote store's PersistAll
// drops the entries for queues left out, so a short set is silent data loss,
// while a queue with no local entry costs nothing (persistAll skips it —
// the store only sends entries it holds).
func (c *DefaultMQPullConsumer) PersistConsumerOffset() error {
	c.mu.Lock()
	store := c.offsetStore
	api := c.pullAPI
	started := c.started
	queued := make([]common.MessageQueue, 0, len(c.offsetQueues))
	for mq := range c.offsetQueues {
		queued = append(queued, mq)
	}
	c.mu.Unlock()
	if !started || store == nil {
		// A strict reading would fail the flush when the consumer is not running;
		// this client's periodic
		// caller treats a not-yet-started consumer as "nothing to do".
		return nil
	}
	seen := make(map[common.MessageQueue]struct{}, len(queued))
	mqs := make([]common.MessageQueue, 0, len(queued)+4)
	for _, mq := range queued {
		seen[mq] = struct{}{}
		mqs = append(mqs, mq)
	}
	for _, mq := range api.pulledQueues() {
		if _, dup := seen[mq]; dup {
			continue
		}
		seen[mq] = struct{}{}
		mqs = append(mqs, mq)
	}
	sort.Slice(mqs, func(i, j int) bool { return mqs[i].CompareTo(mqs[j]) < 0 })
	return store.PersistAll(mqs)
}

// MaxOffset / MinOffset / SearchOffset / EarliestMsgStoreTime are admin
// pass-throughs: they go through the
// instance's master-only offset lookup, NOT through the store.
func (c *DefaultMQPullConsumer) MaxOffset(mq common.MessageQueue) (int64, error) {
	instance, err := c.requireClient()
	if err != nil {
		return 0, err
	}
	return instance.GetMaxOffset(mq, 5000)
}

func (c *DefaultMQPullConsumer) MinOffset(mq common.MessageQueue) (int64, error) {
	instance, err := c.requireClient()
	if err != nil {
		return 0, err
	}
	return instance.GetMinOffset(mq, 5000)
}

func (c *DefaultMQPullConsumer) SearchOffset(mq common.MessageQueue, timestamp int64) (int64, error) {
	instance, err := c.requireClient()
	if err != nil {
		return 0, err
	}
	return instance.SearchOffsetByTimestamp(mq, timestamp, 5000)
}

func (c *DefaultMQPullConsumer) EarliestMsgStoreTime(mq common.MessageQueue) (int64, error) {
	instance, err := c.requireClient()
	if err != nil {
		return 0, err
	}
	addr, err := instance.adminPublishAddr(mq)
	if err != nil {
		return 0, err
	}
	response, err := instance.invokeSync(instance.brokerVIPAddr(addr),
		adminRequest(remoting.ReqGetEarliestMsgStoretime,
			adminExt("topic", mq.Topic, "queueId", mq.QueueID, "brokerName", mq.BrokerName)), 5000)
	if err != nil {
		return 0, err
	}
	if err := instance.checkResponse(response); err != nil {
		return 0, err
	}
	return extInt64(response, "timestamp"), nil
}

// ---------------------------------------------------------------- queues

// FetchSubscribeMessageQueues returns the SUBSCRIBE info (read
// queues, no master filtering), which is what a caller iterates to pull.
func (c *DefaultMQPullConsumer) FetchSubscribeMessageQueues(topic string) ([]common.MessageQueue, error) {
	instance, err := c.requireClient()
	if err != nil {
		return nil, err
	}
	return instance.GetTopicSubscribeInfo(topic), nil
}

// FetchPublishMessageQueues returns the PUBLISH info (write queues,
// master only).
func (c *DefaultMQPullConsumer) FetchPublishMessageQueues(topic string) ([]common.MessageQueue, error) {
	instance, err := c.requireClient()
	if err != nil {
		return nil, err
	}
	info, err := instance.GetTopicPublishInfo(topic, false)
	if err != nil {
		return nil, err
	}
	return info.MsgQueueList(), nil
}

// ParseSubscribeMessageQueues keeps only the queues whose topic
// this consumer is actually subscribed to.
func (c *DefaultMQPullConsumer) ParseSubscribeMessageQueues(queues []common.MessageQueue) []common.MessageQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]common.MessageQueue, 0, len(queues))
	for _, mq := range queues {
		if _, ok := c.subscription[mq.Topic]; ok {
			out = append(out, mq)
		}
	}
	return out
}

// FetchMessageQueuesInBalance returns the queues THIS instance owns once the
// group is balanced — the set a caller must iterate before
// pulling, so pulling "all queues" instead is the classic duplicate-consumption
// bug in a multi-instance group.
//
// When EnableRebalance is on (the default, as in Java
// DefaultMQPullConsumer.java:97) the background rebalance keeps
// rebalanceTable[topic] up to date and this method returns that table's answer,
// which is what Java's "read the keys of the process-queue table" does. Only
// when the switch is off — or the table has not been filled for this topic yet
// — is the share computed on demand with the exact allocation formula the push
// consumer's doRebalance() runs.
// One formula, two call sites: if they ever disagree, a
// single queue gets claimed by two instances.
//   - BROADCASTING: every queue, without consulting the consumer list;
//   - CLUSTERING: the subscribe info (read queues, no master filter — the very
//     source FetchSubscribeMessageQueues uses) as mqAll, the group's clientIds
//     from GET_CONSUMER_LIST_BY_GROUP(38) as cidAll, both sorted, then this
//     consumer's allocate strategy slices out my share.
//
// "No route" or "no answered consumer list" means CANNOT compute, not "my share
// is empty", so the current assignment is kept: the queues already being pulled
// (PulledQueues()). Falling back to
// "I own everything" would make every co-instance read the same messages twice.
// A share that genuinely computes to empty (the queues all went to someone
// else) returns an empty slice — an honest "nothing to pull right now".
func (c *DefaultMQPullConsumer) FetchMessageQueuesInBalance(topic string) ([]common.MessageQueue, error) {
	instance, err := c.requireClient()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	model := c.messageModel
	group := c.consumerGroup
	clientID := c.clientID
	strategy := c.allocateStrategy
	var cached []common.MessageQueue
	var hasCached bool
	if c.enableRebalance {
		// A warm table is authoritative: it is the background rebalance's
		// product, not a second guess at the same allocation.
		cached, hasCached = c.rebalanceTable[topic]
	}
	c.mu.Unlock()
	if hasCached {
		out := append([]common.MessageQueue(nil), cached...)
		sort.Slice(out, func(i, j int) bool { return out[i].CompareTo(out[j]) < 0 })
		return out, nil
	}
	if strategy == nil {
		strategy = AllocateMessageQueueAveragely{}
	}

	pulled := make([]common.MessageQueue, 0)
	for _, mq := range c.PulledQueues() {
		if mq.Topic == topic {
			pulled = append(pulled, mq)
		}
	}

	mqAll := append([]common.MessageQueue(nil), instance.GetTopicSubscribeInfo(topic)...)
	sort.Slice(mqAll, func(i, j int) bool { return mqAll[i].CompareTo(mqAll[j]) < 0 })
	if model == MessageModelBroadcasting {
		// BROADCASTING never asks for the cid list.
		if len(mqAll) == 0 {
			return pulled, nil
		}
		return mqAll, nil
	}

	cidAll, answered := instance.GetConsumerIDListByGroup(topic, group, 0)
	if len(mqAll) == 0 || !answered || len(cidAll) == 0 {
		common.LogDebugf("fetchMessageQueuesInBalance: no route/consumer list for %s/%s, keep current assignment",
			group, topic)
		return pulled, nil
	}
	sort.Strings(cidAll)
	got := strategy.Allocate(group, clientID, mqAll, cidAll)
	if reporter, ok := strategy.(AllocateErrReporter); ok {
		if err := reporter.AllocateErr(); err != nil {
			// The rebalance loop's error discipline: log and keep the current
			// assignment — a broken strategy must never revoke queues.
			common.LogErrorf("allocate message queue failed, strategy=%s group=%s: %v, keep current",
				strategyName(strategy), group, err)
			return pulled, nil
		}
	}
	// Guard every returned queue's topic: never let a strategy's
	// surprising return value leak another topic into the caller's pull loop.
	out := make([]common.MessageQueue, 0, len(got))
	for _, mq := range got {
		if mq.Topic == topic {
			out = append(out, mq)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CompareTo(out[j]) < 0 })
	return out, nil
}

// PulledQueues reports the queues this consumer has pulled at least once
// (the pull-API's per-queue node table key set) — the assignment
// FetchMessageQueuesInBalance falls back to when it cannot compute.
func (c *DefaultMQPullConsumer) PulledQueues() []common.MessageQueue {
	c.mu.Lock()
	api := c.pullAPI
	c.mu.Unlock()
	if api == nil {
		return nil
	}
	mqs := api.pulledQueues()
	sort.Slice(mqs, func(i, j int) bool { return mqs[i].CompareTo(mqs[j]) < 0 })
	return mqs
}

// ---------------------------------------------------------------- send back

// SendMessageBack bounces a failed message back to its broker for retry.
// Two documented behaviours:
//
//  1. the address comes from the PUBLISH table (master only), so the caller must
//     have pulled the topic through THIS consumer first — a queue reached
//     through another instance is not in this instance's route cache;
//  2. a failed bounce degrades to re-sending the message as a normal message to
//     %RETRY%<group> through the internal producer. Only when that fails too is
//     the error reported to the caller.
func (c *DefaultMQPullConsumer) SendMessageBack(msg *common.MessageExt, delayLevel int32, brokerName string) error {
	instance, err := c.requireClient()
	if err != nil {
		return err
	}
	if msg == nil {
		return common.ClientError("msg is null")
	}
	c.mu.Lock()
	group := c.consumerGroup
	unitMode := c.unitMode
	maxReconsume := c.maxReconsumeTimes
	namespace := c.namespace
	c.mu.Unlock()
	// The topic is un-wrapped even when the send threw (finally semantics).
	defer func() { msg.Topic = common.WithoutNamespace(msg.Topic, namespace) }()

	dest := brokerName
	if dest == "" {
		dest = msg.BrokerName
	}
	addr, ok := instance.FindBrokerAddressInPublish(dest)
	if !ok || addr == "" {
		// The error string is a fixed contract wording for "no master".
		err = common.ClientError(fmt.Sprintf("Broker[%s] master node does not exist", dest))
	} else {
		err = instance.sendMessageBack(group, msg, delayLevel, maxReconsume, unitMode,
			defaultPullSendBackTimeoutMillis)
	}
	if err == nil {
		return nil
	}
	common.LogErrorf("Failed to send message back, consumerGroup=%s, brokerName=%s, msg=%s: %v; fallback to normal send",
		group, dest, msg.MsgID, err)
	return c.sendRetryMessage(msg, group, maxReconsume)
}

// sendRetryMessage re-sends a failed send-back as a normal message to
// %RETRY%<group>, carrying the retry bookkeeping properties the broker would
// have written itself.
func (c *DefaultMQPullConsumer) sendRetryMessage(msg *common.MessageExt, group string, maxReconsume int32) error {
	producer, err := c.innerProducer()
	if err != nil {
		return err
	}
	newMsg := buildRetryMessage(msg, group, maxReconsume)
	publish, err := c.instance.GetTopicPublishInfo(newMsg.Topic, true)
	if err != nil {
		return err
	}
	mq, ok, err := publish.SelectOneMessageQueue(nil)
	if err != nil || !ok {
		return common.ClientError(fmt.Sprintf("no writable queue for retry topic %s", newMsg.Topic))
	}
	_, err = producer.SendToQueue(newMsg, mq)
	return err
}

// buildRetryMessage constructs the normal message that carries a failed
// delivery to %RETRY%<group> for the given consumer group.
func buildRetryMessage(msg *common.MessageExt, group string, maxReconsume int32) *common.Message {
	newMsg := common.NewMessage(common.GetRetryTopic(group), append([]byte(nil), msg.GetBody()...))
	if msg.Properties != nil {
		newMsg.SetProperties(msg.Properties.Clone())
	}
	newMsg.Flag = msg.Flag
	originMsgID := common.ClientMsgID(msg)
	if v, ok := msg.GetProperty(common.PropertyOriginMessageID); ok && v != "" {
		originMsgID = v
	}
	if originMsgID != "" {
		newMsg.PutProperty(common.PropertyOriginMessageID, originMsgID)
	}
	newMsg.PutProperty(common.PropertyRetryTopic, msg.Topic)
	newMsg.PutProperty(common.PropertyReconsumeTime, i32Text(msg.ReconsumeTimes+1))
	newMsg.PutProperty(common.PropertyMaxReconsumeTimes, i32Text(maxReconsume))
	// The half-message marker must go, else the broker treats it as a
	// transaction check-back message all over again.
	newMsg.RemoveProperty(common.PropertyTransactionPrepared)
	newMsg.SetDelayTimeLevel(3 + msg.ReconsumeTimes)
	return newMsg
}

func (c *DefaultMQPullConsumer) innerProducer() (*DefaultMQProducer, error) {
	c.producerMu.Lock()
	defer c.producerMu.Unlock()
	if c.producer != nil {
		return c.producer, nil
	}
	c.mu.Lock()
	addrs := append([]string(nil), c.nameServerAddrs...)
	unitName := c.unitName
	unitMode := c.unitMode
	stream := c.streamRequest
	tls := c.tlsEnable
	c.mu.Unlock()
	producer, err := NewDefaultMQProducer(common.ClientInnerProducerGroup)
	if err != nil {
		return nil, err
	}
	producer.SetNameServerAddresses(addrs)
	producer.SetUnitName(unitName)
	producer.SetUnitMode(unitMode)
	producer.SetEnableStreamRequestType(stream)
	if tls != nil {
		producer.SetTLSEnable(*tls)
	}
	if err := producer.Start(); err != nil {
		return nil, err
	}
	c.producer = producer
	return producer, nil
}

// CreateTopic creates a topic on every broker in `key`'s route
// (UPDATE_AND_CREATE_TOPIC). `key` is the topic whose route names the brokers to
// push the new topic to — in practice the default topic (TBW102), which is what
// this port keys the lookup on, so the argument is unused exactly as in
// DefaultMQAdminExt.CreateTopic. `queueNum` is used for BOTH the read and the
// write count.
//
// ⚠ It is NOT the attributes field of UPDATE_AND_CREATE_TOPIC: passing a
// topic name there makes the broker answer `kv string format wrong` (it parses
// attributes as `k=v;k=v`).
func (c *DefaultMQPullConsumer) CreateTopic(key, newTopic string, queueNum, topicSysFlag int32) error {
	instance, err := c.requireClient()
	if err != nil {
		return err
	}
	_ = key
	// The protocol's TopicConfig default perm is 6 (read|write); the create
	// call above does not touch it.
	return instance.CreateTopicInRoute(newTopic, queueNum, queueNum, 6, topicSysFlag, "", 5000)
}

// ---------------------------------------------------------------- Consumer impl

// RebalanceImmediately is Java DefaultMQPullConsumerImpl#rebalanceImmediately:
// the NOTIFY_CONSUMER_IDS_CHANGED(40) fan-out target. It kicks the background
// rebalance, which then runs the same doRebalance() every periodic round uses —
// one path, not a second ad-hoc computation.
//
// Like the push consumer's method it only SIGNALS: this runs on the remoting
// read goroutine, so any synchronous RPC here (route refresh, consumer list)
// would deadlock the connection. The wake channel is capacity 1, so a burst of
// notifications coalesces into one round.
func (c *DefaultMQPullConsumer) RebalanceImmediately() {
	c.mu.Lock()
	started := c.started
	wake := c.rebalanceWakeCh
	c.mu.Unlock()
	if !started || wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

// AdjustThreadPool is the periodic elasticity sweep. A pull consumer has no
// consume executor to resize, so it is a no-op here.
func (c *DefaultMQPullConsumer) AdjustThreadPool() {}

// ResetOffset handles RESET_CONSUMER_CLIENT_OFFSET(220). The effect is the
// store-side half (update + persist); the process-queue half of the classic
// reset does not exist for this consumer.
func (c *DefaultMQPullConsumer) ResetOffset(topic string, table remoting.MQOffsetTable) {
	if table == nil {
		return
	}
	c.mu.Lock()
	store := c.offsetStore
	c.mu.Unlock()
	if store == nil {
		return
	}
	mqs := make([]common.MessageQueue, 0, len(table))
	for _, entry := range table {
		if topic != "" && entry.Queue.Topic != topic {
			continue
		}
		store.UpdateOffset(entry.Queue, entry.Offset, false)
		c.rememberOffsetQueue(entry.Queue)
		mqs = append(mqs, entry.Queue)
	}
	if len(mqs) == 0 {
		return
	}
	sort.Slice(mqs, func(i, j int) bool { return mqs[i].CompareTo(mqs[j]) < 0 })
	if err := store.PersistAll(mqs); err != nil {
		common.LogWarnf("persist after reset offset failed: %v", err)
	}
}

// GetConsumerStatus answers GET_CONSUMER_STATUS_FROM_CLIENT(221) with a copy of
// the offset table. This port's OffsetStore
// interface has no table clone, so it enumerates the queues the consumer knows
// about — the ones holding a local offset, the ones already pulled, and the
// subscribed topic's queues — and reads each one. The first group is what makes
// the answer correct for a caller that updates offsets without ever pulling
// (a table clone would return exactly that group).
func (c *DefaultMQPullConsumer) GetConsumerStatus(topic *string) remoting.MQOffsetTable {
	c.mu.Lock()
	store := c.offsetStore
	api := c.pullAPI
	topics := make([]string, 0, len(c.subscription))
	for t := range c.subscription {
		topics = append(topics, t)
	}
	local := make([]common.MessageQueue, 0, len(c.offsetQueues))
	for mq := range c.offsetQueues {
		local = append(local, mq)
	}
	instance := c.instance
	c.mu.Unlock()
	if store == nil {
		return nil
	}
	if topic != nil && *topic != "" {
		topics = []string{*topic}
	}
	seen := map[common.MessageQueue]struct{}{}
	for _, mq := range local {
		seen[mq] = struct{}{}
	}
	if api != nil {
		for _, mq := range api.pulledQueues() {
			seen[mq] = struct{}{}
		}
	}
	if instance != nil {
		for _, t := range topics {
			for _, mq := range instance.GetTopicSubscribeInfo(t) {
				seen[mq] = struct{}{}
			}
		}
	}
	mqs := make([]common.MessageQueue, 0, len(seen))
	for mq := range seen {
		if topic != nil && *topic != "" && mq.Topic != *topic {
			continue
		}
		mqs = append(mqs, mq)
	}
	sort.Slice(mqs, func(i, j int) bool { return mqs[i].CompareTo(mqs[j]) < 0 })

	var out remoting.MQOffsetTable
	for _, mq := range mqs {
		offset, err := store.ReadOffset(mq, ReadFromMemory)
		if err != nil || offset < 0 {
			continue
		}
		out = append(out, remoting.MQOffsetTableEntry{Queue: mq, Offset: offset})
	}
	return out
}

// pulledQueues is the pullFromWhichNode key set, sorted for stable callers.
func (p *pullAPI) pulledQueues() []common.MessageQueue {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]common.MessageQueue, 0, len(p.pullFromWhichNode))
	for mq := range p.pullFromWhichNode {
		out = append(out, mq)
	}
	return out
}
