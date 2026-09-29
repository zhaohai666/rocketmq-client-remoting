// DefaultMQPullConsumer — the ACTIVE pull consumer (Java
// org.apache.rocketmq.client.consumer.DefaultMQPullConsumer +
// impl.consumer.DefaultMQPullConsumerImpl + impl.consumer.RebalancePullImpl).
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
// Three consequences that are easy to get wrong, all reproduced here:
//
//  1. `Pull` must NOT suspend. Java builds the sysFlag as
//     `PullSysFlag.buildSysFlag(false, block, true, false)` with `block=false`
//     for pull() and true only for pullBlockIfNotFound(). Writing suspend=true
//     on the short path makes the broker hold the request until
//     brokerSuspendMaxTimeMillis (20s) while the client gives up at
//     consumerPullTimeoutMillis (10s) — a guaranteed timeout against a real
//     broker, on an empty queue only, which is why it survives casual testing.
//  2. The offset the caller passes is authoritative. There is no local cursor to
//     fall back to, and `FetchConsumeOffset` reads the STORE, so a caller that
//     never calls UpdateConsumeOffset sees -1 forever.
//  3. The retry ceiling is Java DefaultMQPullConsumer's 16, NOT the push
//     consumer's -1. The broker takes the field at face value for any client
//     version >= V3_4_9 (AbstractSendMessageProcessor:173-179), and
//     `reconsumeTimes(0) >= -1` is true, so -1 would send EVERY bounced message
//     straight to %DLQ%.
//
// Deliberate simplification, matching python/rocketmq/client/consumer.py: this
// port does not install a pull-side rebalance, so `AllocateMessageQueueStrategy`
// exists as configuration and is validated at Start but is not used to compute
// an assignment (there is no process-queue table on this path either). The
// caller manages the queue set through FetchSubscribeMessageQueues and keeps the
// offsets itself — which is exactly what the class is for. The extras Java hangs
// off RebalancePullImpl (fetchMessageQueuesInBalance, the message-queue listener
// fan-out, persistConsumerOffset over the process queue table) are therefore
// reported as "this port has no such table" rather than faked.
package client

import (
	"fmt"
	"sort"
	"sync"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// Pull-consumer defaults (Java DefaultMQPullConsumer 5.x).
const (
	// DefaultConsumerPullTimeoutMillis is consumerPullTimeoutMillis:98 — the
	// budget for the SHORT pull().
	DefaultConsumerPullTimeoutMillis = int64(10000)
	// DefaultBrokerSuspendMaxTimeMillis is brokerSuspendMaxTimeMillis:100 — how
	// long the broker may hold a long-poll request.
	DefaultBrokerSuspendMaxTimeMillis = int64(20000)
	// DefaultConsumerTimeoutMillisWhenSuspend is
	// consumerTimeoutMillisWhenSuspend:102 — the CLIENT budget for the long
	// poll. Start() refuses a value below brokerSuspendMaxTimeMillis (:811).
	DefaultConsumerTimeoutMillisWhenSuspend = int64(30000)
	// DefaultPullConsumerMaxReconsumeTimes is DefaultMQPullConsumer:95. See
	// rule 3 in the file header: this is 16, not the push consumer's -1.
	DefaultPullConsumerMaxReconsumeTimes = int32(16)
	// DefaultPullMaxMsgNums matches the push consumer's pullBatchSize default.
	DefaultPullMaxMsgNums = int32(32)
	// defaultPullSendBackTimeoutMillis is the timeout Java passes to
	// consumerSendMessageBack (3000 in the pull impl, 5000 in the push one).
	defaultPullSendBackTimeoutMillis = int64(3000)
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
	rpcHook       remoting.RPCHook

	messageModel     string
	consumeFromWhere string

	nameServerAddrs []string
	pollNamesrvIntv int64

	// Pull timings — see the file header for how the three relate.
	brokerSuspendMaxTimeMillis       int64
	consumerPullTimeoutMillis        int64
	consumerTimeoutMillisWhenSuspend int64
	maxReconsumeTimes                int32

	// allocateStrategy is validated at Start (Java checkConfig:803) and kept for
	// the caller to use; this port does not run a pull-side rebalance.
	allocateStrategy AllocateMessageQueueStrategy
	queueListener    MessageQueueListener

	// registerTopics is Java's registerTopics: every one of them is subscribed
	// with SUB_ALL by copySubscription() before the client starts, which is what
	// puts them in the heartbeat. Topics reached only through Pull() are added
	// lazily by subscriptionAutomatically.
	registerTopics map[string]struct{}
	subscription   map[string]*remoting.SubscriptionData

	filterMessageHooks  []FilterMessageHook
	consumeMessageHooks []ConsumeMessageHook

	instance    *Instance
	offsetStore OffsetStore
	pullAPI     *pullAPI

	// offsetQueues is the local-offset queue set: every queue whose offset the
	// caller has parked in offsetStore. Java gets the equivalent set for free
	// from rebalanceImpl.getProcessQueueTable(), which this port deliberately
	// does not build (no pull-side rebalance, same as Python). It matters
	// because RemoteBrokerOffsetStore.PersistAll REMOVES the entries for queues
	// it is not handed, so an incomplete set silently discards the caller's
	// progress. Guarded by mu.
	offsetQueues map[common.MessageQueue]struct{}

	started bool
}

// NewDefaultMQPullConsumer mirrors Java's constructor: the group is mandatory
// and an empty one is refused here rather than at Start.
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
		brokerSuspendMaxTimeMillis:       DefaultBrokerSuspendMaxTimeMillis,
		consumerPullTimeoutMillis:        DefaultConsumerPullTimeoutMillis,
		consumerTimeoutMillisWhenSuspend: DefaultConsumerTimeoutMillisWhenSuspend,
		maxReconsumeTimes:                DefaultPullConsumerMaxReconsumeTimes,
		registerTopics:                   map[string]struct{}{},
		subscription:                     map[string]*remoting.SubscriptionData{},
		offsetQueues:                     map[common.MessageQueue]struct{}{},
		// Java field initialiser :89 is AllocateMessageQueueAveragely, and
		// checkConfig refuses a null strategy, so it must never be nil by
		// default.
		allocateStrategy: AllocateMessageQueueAveragely{},
		// Java sets enableStreamRequestType = true in EVERY pull-consumer
		// constructor (:113/:126) — the `@STREAM` clientId suffix is not
		// optional here, unlike on the producer.
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

// SetNameServerAddresses mirrors ClientConfig#setNameServerAddress.
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

// SetUnitName / SetUnitMode / SetEnableStreamRequestType mirror ClientConfig.
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
// go on the wire and unwrapped again on the way back (Java's resetTopic).
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

// SetTlsEnable mirrors ClientConfig#setUseTLS (nil = leave the instance default).
func (c *DefaultMQPullConsumer) SetTlsEnable(enable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tlsEnable = &enable
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

// SetPollNameServerInterval is ClientConfig#pollNameServerInterval, read once
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

// SetMaxReconsumeTimes is DefaultMQPullConsumer#setMaxReconsumeTimes:496. See
// rule 3 in the file header — the pull default is 16, not -1.
func (c *DefaultMQPullConsumer) SetMaxReconsumeTimes(v int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxReconsumeTimes = v
}

// BrokerSuspendMaxTimeMillis is DefaultMQPullConsumer#getBrokerSuspendMaxTimeMillis:376.
func (c *DefaultMQPullConsumer) BrokerSuspendMaxTimeMillis() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.brokerSuspendMaxTimeMillis
}

// ConsumerPullTimeoutMillis is DefaultMQPullConsumer#getConsumerPullTimeoutMillis:384.
func (c *DefaultMQPullConsumer) ConsumerPullTimeoutMillis() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumerPullTimeoutMillis
}

// ConsumerTimeoutMillisWhenSuspend is
// DefaultMQPullConsumer#getConsumerTimeoutMillisWhenSuspend:392.
func (c *DefaultMQPullConsumer) ConsumerTimeoutMillisWhenSuspend() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumerTimeoutMillisWhenSuspend
}

// MaxReconsumeTimes is DefaultMQPullConsumer#getMaxReconsumeTimes:504.
func (c *DefaultMQPullConsumer) MaxReconsumeTimes() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxReconsumeTimes
}

// SetAllocateMessageQueueStrategy mirrors the Java setter; Start refuses nil.
func (c *DefaultMQPullConsumer) SetAllocateMessageQueueStrategy(s AllocateMessageQueueStrategy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.allocateStrategy = s
}

// SetMessageQueueListener mirrors Java setMessageQueueListener: the listener is
// notified when the topic's queue set (as this client sees it) changes.
func (c *DefaultMQPullConsumer) SetMessageQueueListener(l MessageQueueListener) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueListener = l
}

// RegisterFilterMessageHook mirrors the Java hook registration; the hooks run
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

// RegisterConsumeMessageHook mirrors DefaultMQPullConsumerImpl:92. The pair
// fires around the pull result (pullSyncImpl:270-283), not around user
// consumption — the pull consumer has no consume service to hang it off.
func (c *DefaultMQPullConsumer) RegisterConsumeMessageHook(hook ConsumeMessageHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumeMessageHooks = append(c.consumeMessageHooks, hook)
}

// ConsumeType is always CONSUME_ACTIVELY for this class (Java
// DefaultMQPullConsumerImpl:348).
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
	// expectations readable (Java's HashMap has no order either, but the
	// heartbeat is where a flapping order shows up as a diff on every beat).
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}

// RegisterTopic mirrors Java getRegisterTopics().add(): the topic is subscribed
// with SUB_ALL by Start (copySubscription:819-831) so it reaches the broker's
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

// Subscribe is the explicit form of subscriptionAutomatically (Java
// DefaultMQPullConsumer#subscribe:(topic, subExpression)).
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

// Unsubscribe mirrors DefaultMQPullConsumerImpl:311 — it only drops the
// subscription; the topic stays registered until the next Start.
func (c *DefaultMQPullConsumer) Unsubscribe(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscription, topic)
	delete(c.registerTopics, topic)
}

// subscriptionAutomatically is DefaultMQPullConsumerImpl:301: pulling a topic
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
		// Java swallows this too (:306) — an unparsable auto-subscription must
		// not fail an otherwise valid pull.
		return
	}
	c.subscription[topic] = sub
}

// ---------------------------------------------------------------- lifecycle

// Start mirrors DefaultMQPullConsumerImpl:703-770. Unlike the push consumer
// there is no rebalance loop, no consume service and no pull goroutine: after
// Start the caller drives everything through Pull.
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
	// copySubscription (:819-831) — every registerTopic becomes SUB_ALL before
	// the client is created, so the first heartbeat already carries it.
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
		// Java :712-714 — only CLUSTERING rewrites DEFAULT to pid#nanotime.
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
	c.mu.Unlock()

	// Namespace + stream + user(ACL) + zone, in Java's order and once per
	// instance — the signature has to cover exactly the fields Java signs.
	instance.EnsureRPCHooks(namespaceV2, c.streamRequest, userHook)
	if err := instance.Start(); err != nil {
		c.mu.Lock()
		c.instance = nil
		c.mu.Unlock()
		return err
	}

	c.mu.Lock()
	// Java :728-742: a caller-supplied store wins, otherwise CLUSTERING gets the
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
	c.started = true
	topics := make([]string, 0, len(c.subscription))
	for t := range c.subscription {
		topics = append(topics, t)
	}
	c.mu.Unlock()

	_ = store.Load()
	// Java registerConsumer (:746) stores the consumer in the instance so the
	// heartbeat task picks it up; without it the broker never learns the group
	// is online and %RETRY% is never created.
	instance.RegisterConsumer(c.consumerGroup, c)
	for _, topic := range topics {
		instance.RegisterTopicInUse(topic)
		if _, err := instance.UpdateTopicRouteInfoFromNameServer(topic, 5000, false); err != nil {
			common.LogDebugf("initial route refresh failed for %s: %v", topic, err)
		}
	}
	return nil
}

// checkConfigLocked is Java DefaultMQPullConsumerImpl:772-817 — same checks, in
// same order, same wording. Must be called with c.mu held.
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
	// :811 — the long poll can only work if the CLIENT waits longer than the
	// BROKER is allowed to hold the request.
	if c.consumerTimeoutMillisWhenSuspend < c.brokerSuspendMaxTimeMillis {
		return common.ClientError(
			"Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis")
	}
	return nil
}

// Shutdown mirrors DefaultMQPullConsumerImpl:685-701: persist the offsets while
// the consumer still counts as running, unregister the group at every broker
// (the broker would otherwise keep routing rebalance notifications at a dead
// client for ~120s), then shut the factory down.
//
// The ORDER inside is load-bearing: Java's RUNNING branch calls
// persistConsumerOffset() as its first statement, i.e. BEFORE leaving the
// RUNNING state. Flipping `started` up front (the obvious way to write this)
// makes PersistConsumerOffset's own isRunning() guard fire and the caller's last
// offsets are silently lost on every clean shutdown.
func (c *DefaultMQPullConsumer) Shutdown() {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

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
	c.mu.Unlock()

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

// Instance exposes the underlying client instance (diagnostics/tests).
func (c *DefaultMQPullConsumer) Instance() *Instance {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.instance
}

// ---------------------------------------------------------------- pull

// Pull is Java DefaultMQPullConsumerImpl.pull:178 — a SHORT poll at an explicit
// offset, with the caller's own maxNums. `offset < 0` and `maxNums <= 0` are
// refused before anything goes on the wire (:238-244).
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

// PullBySelector is the MessageSelector flavour of Pull (Java :189-198): a SQL92
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

// PullBlockIfNotFound is the LONG poll (Java :586): suspend=true, so the broker
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

// buildPullSubscription is Java getSubscriptionData:200-212 — a null mq is an
// error, and a parse failure is reported as "parse subscription error" rather
// than leaking the FilterAPI text.
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

// buildPullSelectorSubscription is Java getSubscriptionData(mq, messageSelector)
// (:214-227) → FilterAPI.build(topic, expression, expressionType).
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
	expr := selector.Expression
	if expr == "" {
		expr = "*"
	}
	exprType := selector.Type
	if exprType == "" {
		exprType = remoting.ExpressionTypeTag
	}
	sub := remoting.NewSubscriptionData(mq.Topic, expr)
	sub.ExpressionType = exprType
	if exprType == remoting.ExpressionTypeTag {
		built, err := remoting.FilterAPI{}.BuildSubscriptionData(mq.Topic, expr)
		if err != nil {
			return nil, common.ClientError(fmt.Sprintf("parse subscription error: %v", err))
		}
		sub.TagsSet = built.TagsSet
		sub.CodeSet = built.CodeSet
		sub.SubString = built.SubString
		sub.ClassFilterMode = built.ClassFilterMode
	} else {
		sub.ClassFilterMode = exprType == remoting.ExpressionTypeClassFilter
	}
	return sub, nil
}

// pullSync is Java pullSyncImpl:229-285.
func (c *DefaultMQPullConsumer) pullSync(mq common.MessageQueue, sub *remoting.SubscriptionData,
	offset int64, maxNums int32, block bool, timeoutMillis int64) (*PullResult, error) {

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
	// :246 — pulling a topic the caller never registered still has to reach the
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

	// :248 — commitOffset=false (the caller commits), suspend=block,
	// subscription=true, classFilter=false.
	sysFlag := common.BuildSysFlagBasic(false, block, true, false)

	// :252-257 — for a TAG subscription the subVersion travels as 0; only an
	// expression with its own version (SQL92/class filter) sends the real one.
	// pullKernel writes sub.SubVersion straight into the header, so the zeroing
	// has to happen on the value it will read.
	if sub.ExpressionType == remoting.ExpressionTypeTag && sub.SubVersion != 0 {
		local := *sub
		local.SubVersion = 0
		sub = &local
	}
	result, err := api.pullKernel(group, mq, offset, sub, sysFlag, 0, maxNums,
		-1, brokerSuspend, timeoutMillis)
	if err != nil {
		return nil, err
	}
	result = api.processPullResult(mq, result, sub)
	resetTopicForNamespace(result.MsgFoundList, namespace)

	// :270-283 — the consume hook pair runs around the pull RESULT (the pull
	// consumer has no consume service), and Java marks it succeeded as soon as
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

// resetTopicForNamespace is Java resetTopic:287-299 — with a namespace in play
// every returned message's topic is un-wrapped before the caller sees it.
func resetTopicForNamespace(msgs []*common.MessageExt, namespace string) {
	if namespace == "" {
		return
	}
	for _, msg := range msgs {
		msg.Topic = common.WithoutNamespace(msg.Topic, namespace)
	}
}

// ---------------------------------------------------------------- offsets

// FetchConsumeOffset is Java :115-118. `fromStore` picks READ_FROM_STORE (a
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
		// calls updateOffset on the way out (Java RemoteBrokerOffsetStore:213),
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

// UpdateConsumeOffset is Java :833-836 — updateOffset(..., increaseOnly=false),
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

// UpdateConsumeOffsetToBroker is Java :641-644 — write one offset straight to
// the broker, skipping the store's periodic flush.
//
// Java's `isOneway` flag has no counterpart in this port's offset store (every
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
		// Java's DEFAULT (isOneway=false) path does NOT touch the local table —
		// updateConsumeOffsetToBroker only calls updateOffset when isOneway is
		// true (:140-145). Register the queue anyway so a later persistAll
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

// PersistConsumerOffset is Java :410-420. Java passes the process-queue table's
// keys; this port has no pull-side rebalance, so the set is the union of the two
// ways a queue can enter this consumer's world — the queues it has PULLED
// (pullFromWhichNode) and the queues whose offset the CALLER parked locally
// (offsetQueues). Both halves are needed: RemoteBrokerOffsetStore.PersistAll
// drops the entries for queues left out, so a short set is silent data loss,
// while a queue with no local entry costs nothing (persistAll skips it —
// Java RemoteBrokerOffsetStore:152 only sends entries it holds).
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
		// Java's isRunning() would throw; the periodic caller treats a
		// not-yet-started consumer as "nothing to do".
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

// MaxOffset / MinOffset / SearchOffset / EarliestMsgStoreTime are the Java
// DefaultMQPullConsumer pass-throughs to MQAdminImpl: they go through the
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
	response, err := instance.invokeSync(addr, adminRequest(remoting.ReqGetEarliestMsgStoretime,
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

// FetchSubscribeMessageQueues mirrors Java :142 — the SUBSCRIBE info (read
// queues, no master filtering), which is what a caller iterates to pull.
func (c *DefaultMQPullConsumer) FetchSubscribeMessageQueues(topic string) ([]common.MessageQueue, error) {
	instance, err := c.requireClient()
	if err != nil {
		return nil, err
	}
	return instance.GetTopicSubscribeInfo(topic), nil
}

// FetchPublishMessageQueues mirrors Java :137 — the PUBLISH info (write queues,
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

// ParseSubscribeMessageQueues is Java :153-161: keep only the queues whose topic
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

// PulledQueues reports the queues this consumer has pulled at least once
// (Java's pullFromWhichNodeTable key set) — the port's answer to
// fetchMessageQueuesInBalance.
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

// SendMessageBack is Java :636-683's happy path, with the two documented
// differences the other ports already carry:
//
//  1. the address comes from the PUBLISH table (master only), so the caller must
//     have pulled the topic through THIS consumer first — a queue reached
//     through another instance is not in this instance's route cache;
//  2. Java's catch falls back to re-sending the message to %RETRY%group through
//     an internal producer. This port reports the failure instead of silently
//     switching transports — a bounce that failed is something the caller needs
//     to see. (python/rocketmq/client/consumer.py:3890 documents the same.)
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

	dest := brokerName
	if dest == "" {
		dest = msg.BrokerName
	}
	addr, ok := instance.FindBrokerAddressInPublish(dest)
	if !ok || addr == "" {
		// Java uses the same wording for the deprecated path's "no master".
		return common.ClientError(fmt.Sprintf("Broker[%s] master node does not exist", dest))
	}
	if err := instance.sendMessageBack(group, msg, delayLevel, maxReconsume, unitMode,
		defaultPullSendBackTimeoutMillis); err != nil {
		return err
	}
	// Java's finally (:681) un-wraps the topic even when the send threw.
	msg.Topic = common.WithoutNamespace(msg.Topic, namespace)
	return nil
}

// CreateTopic mirrors Java :97-104 → MQAdminImpl.createTopic(key, newTopic,
// queueNum, topicSysFlag). `key` is the topic whose route names the brokers to
// push the new topic to — in practice the default topic (TBW102), which is what
// this port keys the lookup on, so the argument is unused exactly as in
// DefaultMQAdminExt.CreateTopic. `queueNum` is used for BOTH the read and the
// write count, as Java does.
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
	// Java's TopicConfig default perm is 6 (read|write); MQAdminImpl.createTopic
	// does not touch it.
	return instance.CreateTopicInRoute(newTopic, queueNum, queueNum, 6, topicSysFlag, "", 5000)
}

// ---------------------------------------------------------------- Consumer impl

// RebalanceImmediately is the fan-out of broker's NOTIFY_CONSUMER_IDS_CHANGED
// (40). A pull consumer has no assignment to recompute, so this refreshes the
// routes of the subscribed topics and tells the listener what the queue set
// looks like now — the same thing RebalancePullImpl.messageQueueChanged does.
func (c *DefaultMQPullConsumer) RebalanceImmediately() {
	instance := c.Instance()
	if instance == nil {
		return
	}
	c.mu.Lock()
	topics := make([]string, 0, len(c.subscription))
	for t := range c.subscription {
		topics = append(topics, t)
	}
	listener := c.queueListener
	c.mu.Unlock()
	sort.Strings(topics)

	for _, topic := range topics {
		if _, err := instance.UpdateTopicRouteInfoFromNameServer(topic, 5000, false); err != nil {
			common.LogDebugf("route refresh failed for %s: %v", topic, err)
		}
		if listener == nil {
			continue
		}
		mqAll := instance.GetTopicSubscribeInfo(topic)
		if len(mqAll) == 0 {
			continue
		}
		// mqDivided == mqAll: this port assigns nothing, and telling the caller
		// that a subset is "its" would be a lie it might act on.
		listener.MessageQueueChanged(topic, mqAll, mqAll)
	}
}

// AdjustThreadPool is the periodic elasticity sweep. Java's pull consumer has no
// consume executor to resize, so it is a no-op there too.
func (c *DefaultMQPullConsumer) AdjustThreadPool() {}

// ResetOffset handles RESET_CONSUMER_CLIENT_OFFSET(220). This port keeps Java's
// store-side effect (update + persist) and drops the process-queue half, which
// this consumer does not have.
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

// GetConsumerStatus answers GET_CONSUMER_STATUS_FROM_CLIENT(221).
//
// Java answers `offsetStore.cloneOffsetTable(topic)`. This port's OffsetStore
// interface has no table clone, so it enumerates the queues the consumer knows
// about — the ones holding a local offset, the ones already pulled, and the
// subscribed topic's queues — and reads each one. The first group is what makes
// the answer correct for a caller that updates offsets without ever pulling
// (and it is the group Java's cloneOffsetTable would return).
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
