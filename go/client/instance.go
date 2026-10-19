// Instance: the shared client factory behind every producer and consumer on
// one clientId.
//
// Responsibilities:
//   - the topic route cache (route table + publish info + flat broker address
//     table) and its nameserver refresh;
//   - the address lookup family, each with its own master/slave discipline:
//     publish lookups are MASTER-ONLY (slaves reject writes with SYSTEM_BUSY),
//     offset reads may fall back to slaves, heartbeat/unregister fan out to
//     EVERY broker id;
//   - consumer heartbeats (34) — including slaves: each broker keeps its own
//     ConsumerManager, and a slave that never sees a heartbeat rejects pulls
//     with SUBSCRIPTION_NOT_EXIST;
//   - the broker->client handlers wired at factory construction
//     (40/220/221/326; 307/309 land with the push consumer, whose running-info
//     beans they need);
//   - the scheduled tasks with scheduleAtFixedRate anchoring (task n
//     fires at initialDelay + (n-1)*period, never initialDelay + period).
package client

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// DefaultMQClientAPITimeoutMillis is the instance-level API timeout
// (the classic remoting protocol's 3s default).
const DefaultMQClientAPITimeoutMillis = int64(3000)

// ClientInstanceConfig carries the instance-scoped knobs (the client config
// fields the instance actually reads).
type ClientInstanceConfig struct {
	ConnectTimeoutMillis int64
	InvokeTimeoutMillis  int64
	TLSEnable            bool
	// NamesrvWsAddr is the address-server URL; when set and no nameserver is
	// configured, start() fetches addresses from it (TopAddressing).
	NamesrvWsAddr string
	// PollNameServerIntervalMillis drives the route-refresh period
	// (getPollNameServerInterval, default 30s).
	PollNameServerIntervalMillis int64
	// NamesrvRefreshIntervalMillis drives the address-server re-fetch
	// (2 minutes).
	NamesrvRefreshIntervalMillis int64
	// HeartbeatIntervalMillis (getHeartbeatBrokerInterval, default 30s).
	HeartbeatIntervalMillis int64
	// PersistConsumerOffsetIntervalMillis (default 5s).
	PersistConsumerOffsetIntervalMillis int64
	SocksProxy                          *remoting.SocksProxyConfig
	// UseHeartbeatV2 is ClientConfig.useHeartbeatV2 — the
	// `com.rocketmq.heartbeat.v2` switch. OFF by default, exactly like the
	// reference: the beat then stays on the plain HEART_BEAT path with
	// fingerprint 0. When on, an instance that talks to a V2-capable broker
	// stops re-sending unchanged subscriptions (see
	// SendHeartbeatToAllBrokerV2).
	UseHeartbeatV2 bool
	// VipChannelEnabled is ClientConfig.vipChannelEnabled — the single
	// broker-facing VIP switch. OFF by default: the broker's listen port serves
	// every request unchanged. When ON, the RPCs the broker only answers on its
	// VIP port are addressed at `port - 2` of the route address. It is read at
	// the invoke boundary, one request at a time, so the cached route keeps the
	// address the nameserver published, and it never applies to nameserver
	// traffic or to the RPCs the broker serves on the plain port only
	// (heartbeat, unregister, pull/POP/ACK, recall and the oneway
	// END_TRANSACTION).
	VipChannelEnabled bool
}

// NewClientInstanceConfig returns the classic defaults.
func NewClientInstanceConfig() ClientInstanceConfig {
	return ClientInstanceConfig{
		ConnectTimeoutMillis:                3000,
		InvokeTimeoutMillis:                 3000,
		PollNameServerIntervalMillis:        30_000,
		NamesrvRefreshIntervalMillis:        120_000,
		HeartbeatIntervalMillis:             30_000,
		PersistConsumerOffsetIntervalMillis: 5_000,
		UseHeartbeatV2:                      false,
		VipChannelEnabled:                   false,
	}
}

// Consumer is the instance's view of one registered consumer (MQConsumerInner).
// Every method is driven by the instance: heartbeat assembly, offset
// persistence, broker pushes.
type Consumer interface {
	// ConsumerGroup is ConsumerData.groupName.
	ConsumerGroup() string
	// ConsumeType / MessageModel / ConsumeFromWhere / IsUnitMode feed the
	// heartbeat's ConsumerData.
	ConsumeType() string
	MessageModel() string
	ConsumeFromWhere() string
	IsUnitMode() bool
	// Subscriptions feeds ConsumerData.subscriptionDataSet.
	Subscriptions() []*remoting.SubscriptionData
	// RebalanceImmediately is the fan-out of broker's
	// NOTIFY_CONSUMER_IDS_CHANGED(40).
	RebalanceImmediately()
	// AdjustThreadPool is the periodic elasticity sweep (a no-op by design).
	AdjustThreadPool()
	// ResetOffset handles RESET_CONSUMER_CLIENT_OFFSET(220); the instance
	// already runs it off the remoting read goroutine.
	ResetOffset(topic string, table remoting.MQOffsetTable)
	// GetConsumerStatus answers GET_CONSUMER_STATUS_FROM_CLIENT(221). A nil
	// topic means the request header carried none.
	GetConsumerStatus(topic *string) remoting.MQOffsetTable
	// ConsumerRunningInfo answers GET_CONSUMER_RUNNING_INFO(307). The body's
	// shape is per consumer TYPE (consumerRunningInfo): push adds
	// mqTable/mqPopTable/statusTable, pull stops at properties +
	// subscriptionSet. The instance adds the three instance-level properties on
	// top, which is why this method must NOT do that itself.
	ConsumerRunningInfo() *remoting.ConsumerRunningInfo
	// PersistConsumerOffset is the periodic offset flush.
	PersistConsumerOffset() error
}

// ---------------------------------------------------------------- registry

var (
	registryMu sync.Mutex
	registry   = map[string]*Instance{}
)

// CreateOrGetInstance is the classic instance registry lookup: an existing live instance
// with the same clientId is REUSED (the first caller's config wins).
func CreateOrGetInstance(clientID string, nameServerAddrs []string, config ClientInstanceConfig) *Instance {
	registryMu.Lock()
	defer registryMu.Unlock()
	if existing, ok := registry[clientID]; ok && !existing.shutdownDone() {
		return existing
	}
	inst := newInstance(clientID, nameServerAddrs, config)
	registry[clientID] = inst
	return inst
}

// FindInstance looks a live instance up by clientId (read side of the
// registry; tests and admin helpers use it).
func FindInstance(clientID string) *Instance {
	registryMu.Lock()
	defer registryMu.Unlock()
	if inst, ok := registry[clientID]; ok && !inst.shutdownDone() {
		return inst
	}
	return nil
}

func removeFromRegistry(clientID string, inst *Instance) {
	registryMu.Lock()
	defer registryMu.Unlock()
	// Only remove the entry still pointing at this instance: a newer instance
	// may have overwritten the key.
	if cur, ok := registry[clientID]; ok && cur == inst {
		delete(registry, clientID)
	}
}

// ---------------------------------------------------------------- Instance

type Instance struct {
	clientID string
	config   ClientInstanceConfig

	remoting *remoting.RemotingClient

	mu                    sync.Mutex
	nameServerAddrs       []string
	topicRouteTable       map[string]*TopicRouteData
	topicPublishInfoTable map[string]*TopicPublishInfo
	topicEndPointsTable   map[string]map[common.MessageQueue]string
	brokerAddrTable       map[string]map[int64]string
	topicsInUse           map[string]struct{}

	started atomic.Bool

	// vipChannelEnabled is the live copy of ClientInstanceConfig.VipChannelEnabled:
	// a client that owns this instance can flip the switch after Start, and the
	// next broker-facing request already carries the VIP port.
	vipChannelEnabled atomic.Bool

	consumerTable      map[string]Consumer
	producerTable      map[string]struct{}
	consumerGroupTable map[string]struct{}
	tablesMu           sync.Mutex

	// consumerStats is the shared consumer stats manager
	// (consumerStatsManager). Registered consumers record into
	// it; GET_CONSUMER_RUNNING_INFO(307) reads it back per subscribed topic.
	consumerStats *consumerStatsManager

	consumerIDsChanged atomic.Int64

	// heartbeatMu is the reference's lockHeartbeat: at most one fan-out at a
	// time, a caller that finds it busy skips its beat instead of queueing.
	heartbeatMu sync.Mutex
	// heartbeatStateMu guards the three V2 bookkeeping maps below. They are
	// written by the heartbeat task and read by tests / diagnostics, never held
	// across a network call.
	heartbeatStateMu sync.Mutex
	// heartbeatFingerprintTable is brokerAddr -> the fingerprint that broker
	// last acknowledged. An address missing from it gets the full
	// subscription-carrying body.
	heartbeatFingerprintTable map[string]int32
	// heartbeatV2Brokers is the set of addresses that answered
	// IS_SUPPORT_HEART_BEAT_V2. A broker not in it is treated as V1-only.
	heartbeatV2Brokers map[string]struct{}
	// brokerVersionTable is brokerAddr -> the protocol version its last
	// heartbeat response carried.
	brokerVersionTable map[string]int32

	// hooksInstalled guards EnsureRPCHooks. The hook chain belongs to the
	// INSTANCE (it is built once, at transport construction), and
	// Start() may be called more than once by the same or by a second tenant,
	// so it must not be appended to twice.
	hooksInstalled bool

	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

func newInstance(clientID string, nameServerAddrs []string, config ClientInstanceConfig) *Instance {
	inst := &Instance{
		clientID:                  clientID,
		config:                    config,
		nameServerAddrs:           append([]string(nil), nameServerAddrs...),
		topicRouteTable:           map[string]*TopicRouteData{},
		topicPublishInfoTable:     map[string]*TopicPublishInfo{},
		topicEndPointsTable:       map[string]map[common.MessageQueue]string{},
		brokerAddrTable:           map[string]map[int64]string{},
		topicsInUse:               map[string]struct{}{},
		consumerTable:             map[string]Consumer{},
		producerTable:             map[string]struct{}{},
		consumerGroupTable:        map[string]struct{}{},
		consumerStats:             newConsumerStatsManager(),
		heartbeatFingerprintTable: map[string]int32{},
		heartbeatV2Brokers:        map[string]struct{}{},
		brokerVersionTable:        map[string]int32{},
		stop:                      make(chan struct{}),
	}
	inst.vipChannelEnabled.Store(config.VipChannelEnabled)
	inst.remoting = remoting.NewRemotingClientWithConfig(remoting.ClientConfig{
		ConnectTimeoutMillis:     config.ConnectTimeoutMillis,
		InvokeTimeoutMillis:      config.InvokeTimeoutMillis,
		TLSEnable:                config.TLSEnable,
		TLSTestMode:              true,
		EnableReconnectForGoAway: true,
		SocksProxy:               config.SocksProxy,
	})
	inst.registerClientProcessors()
	return inst
}

// ClientID returns the instance's client id.
func (i *Instance) ClientID() string { return i.clientID }

// EnsureRPCHooks installs the RPC hook chain exactly once, in the classic
// order (built with the transport):
//
//	NamespaceRpcHook -> StreamTypeRPCHook (if enabled) -> user hook -> DynamicalExtFieldRPCHook
//
// The order decides what the ACL signature covers: the namespace and ReqT
// fields must be present BEFORE the user's hook runs, while the zone fields
// are added after it and stay unsigned. See remoting/rpchooks.go.
//
// The classic remoting client ties the chain to the shared instance, which is
// keyed by clientId and created once; the first creator's hook wins and later
// tenants share it. This port mirrors that: the first call installs, later calls
// are no-ops. A second tenant therefore does NOT get its own user hook on a
// shared instance — that is the intended behaviour, and silently stacking a
// second ACL hook would corrupt every signature instead.
func (i *Instance) EnsureRPCHooks(namespaceV2 string, stream bool, userHook remoting.RPCHook) {
	i.mu.Lock()
	if i.hooksInstalled {
		i.mu.Unlock()
		return
	}
	i.hooksInstalled = true
	i.mu.Unlock()

	rc := i.remoting
	rc.RegisterRPCHook(remoting.NewNamespaceRpcHook(namespaceV2))
	if stream {
		rc.RegisterRPCHook(remoting.StreamTypeRPCHook{})
	}
	if userHook != nil {
		rc.RegisterRPCHook(userHook)
	}
	rc.RegisterRPCHook(remoting.DynamicalExtFieldRPCHook{})
}

// Remoting exposes the shared transport (producers register their RPC hooks
// on it; the offset store rides its invoke path).
func (i *Instance) Remoting() *remoting.RemotingClient { return i.remoting }

// Config exposes the instance-scoped configuration.
func (i *Instance) Config() ClientInstanceConfig { return i.config }

// SetVipChannelEnabled is the one broker-facing VIP switch of this client
// instance. When it is on, the requests the broker only accepts on its VIP port
// are sent to `port - 2` of the route address; when it is off — the default —
// every address goes out exactly as the nameserver published it. The switch is
// live: flipping it takes effect on the next request.
func (i *Instance) SetVipChannelEnabled(enable bool) { i.vipChannelEnabled.Store(enable) }

// VipChannelEnabled reports the current VIP switch.
func (i *Instance) VipChannelEnabled() bool { return i.vipChannelEnabled.Load() }

// brokerVIPAddr is the invoke-boundary rewrite: it returns the VIP address for
// one resolved broker address when the switch is on, and the address itself
// otherwise. Call it per request, on a locally resolved address only — the
// cached route must keep the address the nameserver published, and a nameserver
// address must never be handed here.
func (i *Instance) brokerVIPAddr(addr string) string {
	if !i.vipChannelEnabled.Load() {
		return addr
	}
	return common.BrokerVIPChannel(true, addr)
}

// NameServerAddrs snapshots the configured nameserver list.
func (i *Instance) NameServerAddrs() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]string(nil), i.nameServerAddrs...)
}

func (i *Instance) setNameServerAddrs(addrs []string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.nameServerAddrs = append([]string(nil), addrs...)
}

// IsStarted reports whether start() took effect.
func (i *Instance) IsStarted() bool { return i.started.Load() }

// ConsumerIDsChangedCount counts received NOTIFY_CONSUMER_IDS_CHANGED(40)
// pushes (a live-verification hook: only brokers emit them).
func (i *Instance) ConsumerIDsChangedCount() int64 { return i.consumerIDsChanged.Load() }

// ---------------- consumer / producer registration ----------------

// RegisterConsumer installs a consumer under its group with a plain map
// assignment — no conflict check.
func (i *Instance) RegisterConsumer(group string, consumer Consumer) {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	i.consumerTable[group] = consumer
}

// UnregisterConsumer removes the group's consumer.
func (i *Instance) UnregisterConsumer(group string) {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	delete(i.consumerTable, group)
}

// FindConsumer looks the group's consumer up.
func (i *Instance) FindConsumer(group string) (Consumer, bool) {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	c, ok := i.consumerTable[group]
	return c, ok
}

func (i *Instance) consumersSnapshot() []Consumer {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	groups := make([]string, 0, len(i.consumerTable))
	for g := range i.consumerTable {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	out := make([]Consumer, 0, len(groups))
	for _, g := range groups {
		out = append(out, i.consumerTable[g])
	}
	return out
}

// RegisterProducer records a producer group so the shutdown guard sees it.
// (The producer sends its own heartbeat; the instance only needs the tenancy.)
func (i *Instance) RegisterProducer(group string) {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	i.producerTable[group] = struct{}{}
}

// UnregisterProducer must run BEFORE Shutdown (the classic client shutdown
// ordering), so the departing producer does not block the factory's teardown.
func (i *Instance) UnregisterProducer(group string) {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	delete(i.producerTable, group)
}

// HasProducer reports whether the group is still registered.
func (i *Instance) HasProducer(group string) bool {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	_, ok := i.producerTable[group]
	return ok
}

// RegisterConsumerGroup / UnregisterConsumerGroup are the pull-consumer /
// lite-pull variants: only the group name is registered (their heartbeats and
// offsets ride their own loops; the entry exists for the shutdown guard).
func (i *Instance) RegisterConsumerGroup(group string) {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	i.consumerGroupTable[group] = struct{}{}
}

// UnregisterConsumerGroup removes a pull-style group registration.
func (i *Instance) UnregisterConsumerGroup(group string) {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	delete(i.consumerGroupTable, group)
}

// HasConsumerGroup reports whether the pull-style group is registered.
func (i *Instance) HasConsumerGroup(group string) bool {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	_, ok := i.consumerGroupTable[group]
	return ok
}

// tenantCount is the shutdown guard's three tables summed (the guard refuses
// teardown while any producer/consumer is still registered).
func (i *Instance) tenantCount() int {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	return len(i.consumerTable) + len(i.producerTable) + len(i.consumerGroupTable)
}

// RebalanceImmediately pokes every registered consumer (the classic design
// wakes a shared rebalance service; without such a shared thread each
// consumer wakes its own loop).
func (i *Instance) RebalanceImmediately() {
	for _, c := range i.consumersSnapshot() {
		c.RebalanceImmediately()
	}
}

// RegisterTopicInUse marks a topic for the periodic route refresh.
func (i *Instance) RegisterTopicInUse(topic string) {
	if topic == "" {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.topicsInUse[topic] = struct{}{}
}

// ---------------- broker -> client handlers ----------------

// registerClientProcessors wires the instance-level processors — the classic
// remoting client builds its client processor once at transport construction
// (326/220/221/307/309/40), not per consumer.
func (i *Instance) registerClientProcessors() {
	i.remoting.RegisterProcessor(remoting.ReqNotifyConsumerIDsChanged, i.processNotifyConsumerIDsChanged)
	i.remoting.RegisterProcessor(remoting.ReqResetConsumerClientOffset, i.processResetOffset)
	i.remoting.RegisterProcessor(remoting.ReqGetConsumerStatusFromClient, i.processGetConsumerStatus)
	i.remoting.RegisterProcessor(remoting.ReqGetConsumerRunningInfo, i.processGetConsumerRunningInfo)
	i.remoting.RegisterProcessor(remoting.ReqConsumeMessageDirectly, i.processConsumeMessageDirectly)
	i.remoting.RegisterProcessor(remoting.ReqPushReplyMessageToClient, i.processReplyMessage)
	i.remoting.RegisterProcessor(remoting.ReqNotifyUnsubscribeLite, i.processNotifyUnsubscribeLite)
}

// processNotifyUnsubscribeLite (200073) is registered for exactly one reason:
// to keep it off the "no processor for request code" WARN path.
//
// In the classic remoting protocol this code is registered against the shared
// client processor, but that processor's switch has NO case for it — it falls
// through to `default: break;` and answers nothing, and the broker only ever
// sends it oneway, so nothing is waiting for an answer. In other words the
// whole feature is vestigial on the classic-remoting path (only the gRPC
// proxy's client processor acts on it). Following the protocol means doing
// nothing here — deliberately NOT even calling sink.Respond, because the
// classic processor would not reply to a sync sender either.
func (i *Instance) processNotifyUnsubscribeLite(_ *remoting.RemotingCommand, addr string, _ *remoting.ResponseSink) {
	common.LogDebugf("remoting: NOTIFY_UNSUBSCRIBE_LITE(200073) from %s; nothing to do on the classic path", addr)
}

// ackIfWanted answers the push. The classic processor returns no reply for
// 40/220 and the broker sends both oneway; Respond itself drops replies for
// oneway pushes, so a hypothetical sync pusher still gets its SUCCESS.
func (i *Instance) ackIfWanted(sink *remoting.ResponseSink) {
	sink.Respond(remoting.CreateResponseCommand(remoting.RespSuccess, ""))
}

// processNotifyConsumerIdsChanged: logs the broker's notification with the
// classic remoting protocol's fixed wording, then rebalance
// immediately. Failures must not surface to the broker connection.
func (i *Instance) processNotifyConsumerIDsChanged(request *remoting.RemotingCommand, addr string, sink *remoting.ResponseSink) {
	var header remoting.NotifyConsumerIdsChangedRequestHeader
	header.FromExtFields(request.ExtFields())
	group := ""
	if header.ConsumerGroup != nil {
		group = *header.ConsumerGroup
	}
	i.consumerIDsChanged.Add(1)
	common.LogInfof("receive broker's notification[%s], the consumer group: %s changed, rebalance immediately", addr, group)
	i.RebalanceImmediately()
	i.ackIfWanted(sink)
}

// processResetOffset (220): no reply (the classic processor answers none);
// the actual reset
// runs on its own goroutine because rebalance/lock/batch inside it make sync
// calls that must not block the remoting read loop.
func (i *Instance) processResetOffset(request *remoting.RemotingCommand, _ string, sink *remoting.ResponseSink) {
	var header remoting.ResetOffsetRequestHeader
	header.FromExtFields(request.ExtFields())
	group := ""
	if header.Group != nil {
		group = *header.Group
	}
	consumer, ok := i.FindConsumer(group)
	if !ok {
		common.LogWarnf("RESET_CONSUMER_CLIENT_OFFSET: no consumer for group=%q", group)
		return
	}
	table := remoting.MQOffsetTable(nil)
	if body := request.Body; len(body) > 0 {
		parsed, err := remoting.ParseResetOffsetTable(body)
		if err != nil {
			common.LogWarnf("RESET_CONSUMER_CLIENT_OFFSET: bad body: %v", err)
			return
		}
		table = parsed
	}
	topic := ""
	if header.Topic != nil {
		topic = *header.Topic
	}
	tableCopy := append(remoting.MQOffsetTable(nil), table...)
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		consumer.ResetOffset(topic, tableCopy)
	}()
	i.ackIfWanted(sink)
}

// processGetConsumerStatus (221): the consumer's consumed-offset table, or
// SYSTEM_ERROR when no consumer covers the group.
func (i *Instance) processGetConsumerStatus(request *remoting.RemotingCommand, _ string, sink *remoting.ResponseSink) {
	var header remoting.GetConsumerStatusRequestHeader
	header.FromExtFields(request.ExtFields())
	group := ""
	if header.Group != nil {
		group = *header.Group
	}
	consumer, ok := i.FindConsumer(group)
	if !ok {
		sink.Respond(remoting.CreateResponseCommand(remoting.RespSystemError, fmt.Sprintf("no consumer for group=%s", group)))
		return
	}
	body := &remoting.GetConsumerStatusBody{
		MessageQueueTable: consumer.GetConsumerStatus(header.Topic),
	}
	response := remoting.CreateResponseCommand(remoting.RespSuccess, "")
	response.SetBody(body.Encode())
	sink.Respond(response)
}

// processGetConsumerRunningInfo (307) answers with this client's
// ConsumerRunningInfo, per the classic remoting protocol's processor contract.
//
// The reply shape is a three-way contract:
//   - an unknown group is SYSTEM_ERROR with the protocol's fixed remark text
//     (the admin surfaces it verbatim, so it is not paraphrased);
//   - a known group gets SUCCESS with the body from Instance.consumerRunningInfo;
//   - jstack is only collected when the request asked for it, and its presence
//     in the body is what tells the admin the dump is real — an absent key is
//     how the protocol reports "not collected".
//
// The body must be encoded with EncodeInlineKeyJSON: mqTable/mqPopTable key their
// entries by MessageQueue as a raw inline object, and encoding/json cannot emit
// that (it validates Marshaler output). See remoting.mqKeyedJSON.
func (i *Instance) processGetConsumerRunningInfo(request *remoting.RemotingCommand, _ string, sink *remoting.ResponseSink) {
	var header remoting.GetConsumerRunningInfoRequestHeader
	header.FromExtFields(request.ExtFields())
	group := ""
	if header.ConsumerGroup != nil {
		group = *header.ConsumerGroup
	}

	info := i.consumerRunningInfo(group)
	if info == nil {
		sink.Respond(remoting.CreateResponseCommand(remoting.RespSystemError,
			fmt.Sprintf("The Consumer Group <%s> not exist in this consumer", group)))
		return
	}
	if header.JstackEnable != nil && *header.JstackEnable {
		// The protocol's jstack field is a full thread dump; Go's analogue
		// is every goroutine. The format differs (it is a goroutine stack, not
		// a thread dump), but the field exists precisely so an operator can see
		// where the client is stuck, and an empty string would say
		// "not collected" instead.
		info.Jstack = collectGoroutineStacks()
		info.HasJstack = true
	}

	response := remoting.CreateResponseCommand(remoting.RespSuccess, "")
	response.SetBody(info.Encode())
	sink.Respond(response)
}

// processConsumeMessageDirectly (309) hands one broker-supplied message to the
// local listener and returns its verdict.
//
// The classic processor answers SYSTEM_ERROR with
// the same "not exist" remark when the instance-level running-info lookup
// fails, and its consumeMessageDirectly dispatch returns nothing for any
// consumer that is not a push consumer — so a pull or lite consumer
// group produces that same error, not a silent success.
//
// The listener runs on its own goroutine. The classic client runs it on the
// Netty handler thread, which this port must not copy: the remoting read loop
// is shared with every other response on the connection, and a listener that
// makes a
// every other response on the connection, and a listener that makes a
// synchronous request (sendMessageBack, a downstream call) would wait on a
// response only that loop can deliver. The reply still goes out strictly after
// the listener returns, so the wire ordering the broker sees is unchanged.
func (i *Instance) processConsumeMessageDirectly(request *remoting.RemotingCommand, _ string, sink *remoting.ResponseSink) {
	var header remoting.ConsumeMessageDirectlyResultRequestHeader
	header.FromExtFields(request.ExtFields())
	group := ""
	if header.ConsumerGroup != nil {
		group = *header.ConsumerGroup
	}
	brokerName := ""
	if header.BrokerName != nil {
		brokerName = *header.BrokerName
	}

	notExist := func() {
		sink.Respond(remoting.CreateResponseCommand(remoting.RespSystemError,
			fmt.Sprintf("The Consumer Group <%s> not exist in this consumer", group)))
	}

	consumer, ok := i.FindConsumer(group)
	if !ok {
		notExist()
		return
	}
	push, ok := consumer.(*DefaultMQPushConsumer)
	if !ok {
		notExist()
		return
	}
	if len(request.Body) == 0 {
		sink.Respond(remoting.CreateResponseCommand(remoting.RespSystemError, "empty message body"))
		return
	}
	// The classic client decodes this body with its client-side message decoder,
	// readBody=true, i.e.
	// decode(bb, readBody=true, deCompressBody=true, isClient=true,
	// isSetPropertiesString=false, checkCRC=false) — exactly
	// common.DefaultDecodeOptions(). So the body is one stored message with its
	// body present, DECOMPRESSED if the sysFlag says so, and with IsClient set
	// (a MessageClientExt, i.e. MsgID also lands in OffsetMsgID).
	// (Plain non-client decode, by contrast, passes isClient=false; using it
	// here would drop OffsetMsgID from every 309 delivery.)
	msg, err := common.DecodeMessage(request.Body)
	if err != nil {
		sink.Respond(remoting.CreateResponseCommand(remoting.RespSystemError,
			fmt.Sprintf("decode message failed: %v", err)))
		return
	}

	go func() {
		result := push.consumeMessageDirectly(msg, brokerName)
		response := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		response.SetBody(result.Encode())
		sink.Respond(response)
	}()
}

// collectGoroutineStacks returns all goroutine stacks, growing the buffer if the
// first attempt was truncated (runtime.Stack reports the byte count written and
// stops at the buffer size).
func collectGoroutineStacks() string {
	buf := make([]byte, 64*1024)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		if len(buf) >= 16*1024*1024 {
			// Pathological: stop growing rather than risk an unbounded alloc.
			return string(buf[:n])
		}
		buf = make([]byte, len(buf)*2)
	}
}

// processReplyMessage (326) routes a broker-pushed reply to the producer
// future waiting on its correlationId. The holder removes the future first, so
// a late or duplicate push lands in the unmatched branch — a normal race,
// warned about with the protocol's fixed wording. Decode success answers
// SUCCESS either way.
func (i *Instance) processReplyMessage(request *remoting.RemotingCommand, _ string, sink *remoting.ResponseSink) {
	var header remoting.ReplyMessageRequestHeader
	header.FromExtFields(request.ExtFields())
	msg, err := buildReplyMessageExt(&header, request.Body)
	if err != nil {
		common.LogWarnf("unknown err when receiveReplyMsg: %v", err)
		sink.Respond(remoting.CreateResponseCommand(remoting.RespSystemError,
			fmt.Sprintf("process reply message fail: %v", err)))
		return
	}
	correlationID, _ := msg.GetProperty(common.PropertyCorrelationID)
	if GetRequestFutureHolder().PutResponse(correlationID, msg) == nil {
		common.LogWarnf("receive reply message, but not matched any request, CorrelationId: %q, reply from host: %q",
			correlationID, strValue(header.BornHost))
	}
	sink.Respond(remoting.CreateResponseCommand(remoting.RespSuccess, ""))
}

// ---------------- lifecycle ----------------

// Start flips started and spawns the scheduled tasks. Multiple
// producers/consumers on one clientId all call it; only the first has effect
// (a compareAndSet on the started flag).
//
// When no nameserver is configured but the address-server URL is, the first
// fetch happens synchronously; a failed or empty fetch tears the instance
// down and reports error code 10004 (NoNameServerException) rather than letting
// every later send fail with the misleading no-route (10005).
func (i *Instance) Start() error {
	if !i.started.CompareAndSwap(false, true) {
		return nil
	}
	i.remoting.Start()
	if len(i.NameServerAddrs()) == 0 && i.config.NamesrvWsAddr != "" {
		if err := i.FetchNameServerAddr(); err != nil {
			i.shutdownFactory()
			return common.ClientErrorCode(common.NoNameServerException,
				fmt.Sprintf("name server address is not set and address server (%s) failed: %v", i.config.NamesrvWsAddr, err))
		}
		if len(i.NameServerAddrs()) == 0 {
			ws := i.config.NamesrvWsAddr
			i.shutdownFactory()
			return common.ClientErrorCode(common.NoNameServerException,
				fmt.Sprintf("name server address is not set and address server (%s) returned none", ws))
		}
		// scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min): this task is
		// only scheduled when the address server is in play.
		i.spawnPeriodic(func() {
			if err := i.FetchNameServerAddr(); err != nil {
				common.LogDebugf("fetchNameServerAddr exception: %v", err)
			}
		}, 10_000, i.config.NamesrvRefreshIntervalMillis)
	}
	// Route refresh: first jump 10ms after start, then the poll interval.
	i.spawnPeriodic(func() {
		for _, topic := range i.topicsInUseSnapshot() {
			if _, err := i.UpdateTopicRouteInfoFromNameServer(topic, 5000, false); err != nil {
				common.LogDebugf("route refresh failed for %s: %v", topic, err)
			}
		}
	}, 10, i.config.PollNameServerIntervalMillis)
	// cleanOfflineBroker is bundled INTO the heartbeat task.
	i.spawnPeriodic(func() {
		i.cleanOfflineBroker()
		i.SendHeartbeatToAllBrokerWithLockV2(false, 5000)
	}, 1_000, i.config.HeartbeatIntervalMillis)
	i.spawnPeriodic(func() {
		i.PersistConsumerOffsets()
	}, 10_000, i.config.PersistConsumerOffsetIntervalMillis)
	i.spawnPeriodic(func() {
		i.AdjustThreadPools()
	}, 60_000, 60_000)
	// The five consumer-stats sets (scheduled per the classic protocol's stats
	// reporting, i.e. from instance construction; same cadences, tied
	// here to the instance's stop channel).
	i.consumerStats.schedule(i)
	return nil
}

// Shutdown applies the classic guard: while any producer/consumer is still
// registered the factory stays alive (the departing client must not rip out
// heartbeats, route refreshes and connections still in use).
func (i *Instance) Shutdown() {
	if tenants := i.tenantCount(); tenants > 0 {
		common.LogDebugf("client factory [%s] still has %d client(s) registered, skip shutdown", i.clientID, tenants)
		return
	}
	i.shutdownFactory()
}

// DetachFromRegistryIfLastTenant drops the clientId registration when this
// instance has no users left; a later start on the same clientId then builds
// a clean instance instead of reusing a half-dismantled one.
func (i *Instance) DetachFromRegistryIfLastTenant() {
	if i.tenantCount() == 0 {
		removeFromRegistry(i.clientID, i)
	}
}

// shutdownFactory is the RUNNING-state teardown, also used by a failed start
// (cleanup after start failure plus removal from the client factory).
func (i *Instance) shutdownFactory() {
	i.started.Store(false)
	i.stopOnce.Do(func() { close(i.stop) })
	i.remoting.Shutdown()
	removeFromRegistry(i.clientID, i)
}

func (i *Instance) shutdownDone() bool {
	select {
	case <-i.stop:
		return true
	default:
		return false
	}
}

// spawnPeriodic runs work at a fixed rate: jump n lands at
// initialDelay + (n-1)*period on one absolute timeline. Sleeping
// "initial, then period" each round would drift by the work's own duration
// (live-measured on the 30s route refresh: 30.19s instead of 10ms + 30s).
func (i *Instance) spawnPeriodic(work func(), initialDelayMillis, periodMillis int64) {
	period := time.Duration(periodMillis) * time.Millisecond
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		next := time.Now().Add(time.Duration(initialDelayMillis) * time.Millisecond)
		timer := time.NewTimer(time.Until(next))
		defer timer.Stop()
		for {
			select {
			case <-i.stop:
				return
			case <-timer.C:
			}
			next = next.Add(period)
			timer.Reset(time.Until(next))
			if !i.started.Load() {
				return
			}
			work()
		}
	}()
}

func (i *Instance) topicsInUseSnapshot() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]string, 0, len(i.topicsInUse))
	for t := range i.topicsInUse {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// AdjustThreadPools runs the elasticity sweep per consumer, swallowing
// failures (the classic sweep catches and ignores any exception).
func (i *Instance) AdjustThreadPools() {
	for _, c := range i.consumersSnapshot() {
		c.AdjustThreadPool()
	}
}

// PersistConsumerOffsets flushes every registered consumer's offsets; one
// consumer's failure does not stop the others.
func (i *Instance) PersistConsumerOffsets() {
	for _, c := range i.consumersSnapshot() {
		if err := c.PersistConsumerOffset(); err != nil {
			common.LogWarnf("persist consumer offset failed for group %s: %v", c.ConsumerGroup(), err)
		}
	}
}

// ---------------- nameserver address fetch ----------------

// FetchNameServerAddr GETs the address-server URL and applies the response
// only when it changed (the classic remoting protocol's address-server fetch).
// The body's first ';' field is the address list.
func (i *Instance) FetchNameServerAddr() error {
	wsAddr := i.config.NamesrvWsAddr
	if wsAddr == "" {
		return nil
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(wsAddr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return err
	}
	content := clearNewLine(string(body))
	var addrs []string
	for _, a := range strings.Split(content, ";") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	old := i.NameServerAddrs()
	if len(old) != len(addrs) {
		i.setNameServerAddrs(addrs)
		return nil
	}
	changed := false
	for idx := range addrs {
		if addrs[idx] != old[idx] {
			changed = true
			break
		}
	}
	if changed {
		i.setNameServerAddrs(addrs)
	}
	return nil
}

func clearNewLine(s string) string {
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}

// ---------------- route updates ----------------

// UpdateTopicRouteInfoFromNameServer fetches the topic's route from every
// nameserver and refreshes the local tables. An empty nameserver list is an
// immediate 10004 — with no address at all this must NOT surface as the
// per-topic no-route (10005).
func (i *Instance) UpdateTopicRouteInfoFromNameServer(topic string, timeoutMillis int64, isDefault bool) (bool, error) {
	addrs := i.NameServerAddrs()
	if len(addrs) == 0 {
		return false, common.ClientErrorCode(common.NoNameServerException, "name server address list is empty")
	}
	route, err := i.fetchTopicRouteFromNamesrv(topic, timeoutMillis, addrs)
	if err != nil {
		return false, err
	}
	// RocketMQ 5.x nameservers do not synthesize a default route for unknown
	// topics (they answer TOPIC_NOT_EXIST); a PRODUCER must fall back to the
	// default topic, as the classic remoting protocol prescribes, to build its
	// publish info.
	if route == nil && isDefault && topic != common.DefaultTopic {
		route, err = i.fetchTopicRouteFromNamesrv(common.DefaultTopic, timeoutMillis, addrs)
		if err != nil {
			return false, err
		}
		if route != nil {
			// The broker created the new topic with default_topic_queue_nums
			// queues while the default topic itself may be configured with
			// more — cap the counts so no illegal queueId gets selected.
			for _, qd := range route.QueueDatas {
				if qd.WriteQueueNums > common.DefaultTopicQueueNums {
					qd.WriteQueueNums = common.DefaultTopicQueueNums
				}
				if qd.ReadQueueNums > common.DefaultTopicQueueNums {
					qd.ReadQueueNums = common.DefaultTopicQueueNums
				}
			}
		}
	}
	if route == nil {
		return false, nil
	}
	i.mu.Lock()
	i.topicRouteTable[topic] = route
	if endpoints := topicRouteData2EndpointsForStaticTopic(topic, route); endpoints != nil {
		i.topicEndPointsTable[topic] = endpoints
	} else {
		delete(i.topicEndPointsTable, topic)
	}
	// The route's own brokerAddrs map is stored by shared reference, so the
	// classic "only overwrite when changed" still refreshes; overwriting
	// unconditionally here is the same end state.
	for _, bd := range route.BrokerDatas {
		addrs := make(map[int64]string, len(bd.BrokerAddrs))
		for _, pair := range bd.BrokerAddrs {
			addrs[pair.ID] = pair.Addr
		}
		i.brokerAddrTable[bd.BrokerName] = addrs
	}
	publish, ok := i.topicPublishInfoTable[topic]
	if !ok {
		publish = NewTopicPublishInfo()
		i.topicPublishInfoTable[topic] = publish
	}
	i.mu.Unlock()
	publish.UpdateFromRoute(route, topic)
	return true, nil
}

// fetchTopicRouteFromNamesrv tries the nameservers in order. A usable
// response that is NOT a route (non-SUCCESS, or SUCCESS without body) stops
// the search — the remaining nameservers would answer the same; only
// transport/decode errors move on to the next one. Afterwards the LAST such
// error is rethrown unless it is a broker-business error.
func (i *Instance) fetchTopicRouteFromNamesrv(topic string, timeoutMillis int64, addrs []string) (*TopicRouteData, error) {
	var lastErr error
	for _, nsAddr := range addrs {
		request := remoting.CreateRequestCommand(remoting.ReqGetRouteInfoByTopic, &remoting.GetRouteInfoRequestHeader{Topic: remoting.StrPtr(topic)})
		response, err := i.invokeSync(nsAddr, request, timeoutMillis)
		if err != nil {
			lastErr = err
			continue
		}
		if response.Code != remoting.RespSuccess {
			break
		}
		if len(response.Body) == 0 {
			break
		}
		route, err := DecodeTopicRouteData(response.Body)
		if err != nil {
			lastErr = err
			continue
		}
		return route, nil
	}
	if lastErr != nil {
		if e, ok := lastErr.(*common.Error); !ok || e.Kind != common.KindBroker {
			return nil, lastErr
		}
	}
	return nil, nil
}

// GetTopicPublishInfo returns a usable publish info: cache hit first, else one
// route fetch; still no queue -> "Can not find Message Queue for topic".
func (i *Instance) GetTopicPublishInfo(topic string, isDefault bool) (*TopicPublishInfo, error) {
	if info := i.PublishInfoOf(topic); info != nil && info.OK() {
		return info, nil
	}
	if _, err := i.UpdateTopicRouteInfoFromNameServer(topic, 5000, isDefault); err != nil {
		return nil, err
	}
	if info := i.PublishInfoOf(topic); info != nil && info.OK() {
		return info, nil
	}
	return nil, common.ClientError(fmt.Sprintf("Can not find Message Queue for topic: %s", topic))
}

// GetTopicRouteData returns the cached route, fetching once when missing
// (fetch errors are swallowed).
func (i *Instance) GetTopicRouteData(topic string) *TopicRouteData {
	if route := i.RouteOf(topic); route != nil {
		return route
	}
	if _, err := i.UpdateTopicRouteInfoFromNameServer(topic, 5000, false); err != nil {
		common.LogDebugf("get_topic_route_data refresh failed for %s: %v", topic, err)
	}
	return i.RouteOf(topic)
}

// GetTopicSubscribeInfo returns every READABLE queue of the topic for the
// consumer side (rebalance's topicSubscribeInfoTable). Empty list is fine —
// rebalancing on an empty subscription list only warns and keeps the existing
// assignment.
func (i *Instance) GetTopicSubscribeInfo(topic string) []common.MessageQueue {
	route := i.GetTopicRouteData(topic)
	if route == nil {
		return nil
	}
	return route.GetAllSubscribeMessageQueue(topic)
}

// RouteOf reads the cached route without any RPC.
func (i *Instance) RouteOf(topic string) *TopicRouteData {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.topicRouteTable[topic]
}

func (i *Instance) GetBrokerNameFromMessageQueue(mq common.MessageQueue) string {
	i.mu.Lock()
	endpoints := i.topicEndPointsTable[mq.Topic]
	i.mu.Unlock()
	if len(endpoints) > 0 {
		if real, ok := endpoints[mq]; ok {
			return real
		}
	}
	return mq.BrokerName
}

// EndPointsOf returns a copy of the static-topic endpoints entry for the
// topic (nil when the topic has no mapping, i.e. it is not a static topic).
// Diagnostics/live-tool support: this is the read-only accessor over
// topicEndPointsTable.
func (i *Instance) EndPointsOf(topic string) map[common.MessageQueue]string {
	i.mu.Lock()
	defer i.mu.Unlock()
	src := i.topicEndPointsTable[topic]
	if len(src) == 0 {
		return nil
	}
	out := make(map[common.MessageQueue]string, len(src))
	for mq, bname := range src {
		out[mq] = bname
	}
	return out
}

// PublishInfoOf reads the cached publish info without any RPC.
func (i *Instance) PublishInfoOf(topic string) *TopicPublishInfo {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.topicPublishInfoTable[topic]
}

// FindBrokerAddrInRoute picks an address for the broker inside one route:
// master preferred, else any.
func FindBrokerAddrInRoute(route *TopicRouteData, brokerName string) (string, bool) {
	for _, bd := range route.BrokerDatas {
		if bd.BrokerName == brokerName {
			return bd.SelectBrokerAddr()
		}
	}
	return "", false
}

// ---------------- address tools ----------------

// BrokerAddrForTopic resolves an address by topic (optional brokerName);
// admin-style APIs use it.
func (i *Instance) BrokerAddrForTopic(topic string) (string, error) {
	return i.AddrFor(topic, "")
}

// AddrFor resolves an address by topic, optionally pinned to a broker name.
// An empty brokerName degrades to "the first broker in the route".
func (i *Instance) AddrFor(topic, brokerName string) (string, error) {
	route := i.GetTopicRouteData(topic)
	if route == nil {
		return "", common.ClientError(fmt.Sprintf("No route info of this topic: %s", topic))
	}
	if brokerName != "" {
		addr, ok := FindBrokerAddrInRoute(route, brokerName)
		if !ok {
			return "", common.ClientError(fmt.Sprintf("Broker %s not found in route of topic %s", brokerName, topic))
		}
		return addr, nil
	}
	if len(route.BrokerDatas) == 0 {
		return "", common.ClientError(fmt.Sprintf("No broker in route of topic: %s", topic))
	}
	addr, ok := route.BrokerDatas[0].SelectBrokerAddr()
	if !ok {
		return "", common.ClientError(fmt.Sprintf("No available broker addr for topic: %s", topic))
	}
	return addr, nil
}

// BrokerAddr resolves a MessageQueue's broker address inside its own topic
// route.
func (i *Instance) BrokerAddr(mq common.MessageQueue) (string, error) {
	route := i.GetTopicRouteData(mq.Topic)
	if route == nil {
		return "", common.ClientError(fmt.Sprintf("No route info of this topic: %s", mq.Topic))
	}
	addr, ok := FindBrokerAddrInRoute(route, mq.BrokerName)
	if !ok {
		return "", common.ClientError(fmt.Sprintf("Broker %s not found in route of topic %s", mq.BrokerName, mq.Topic))
	}
	return addr, nil
}

// BrokerAddrOf searches ALL cached routes for the broker name — "any one of
// its addrs will do" (master preferred, slave
// fallback).
func (i *Instance) BrokerAddrOf(brokerName string) (string, bool) {
	i.mu.Lock()
	routes := make([]*TopicRouteData, 0, len(i.topicRouteTable))
	for _, r := range i.topicRouteTable {
		routes = append(routes, r)
	}
	i.mu.Unlock()
	for _, route := range routes {
		if addr, ok := FindBrokerAddrInRoute(route, brokerName); ok {
			return addr, true
		}
	}
	return "", false
}

// FindBrokerAddressInPublish returns ONLY the master (brokerId 0) address from
// the flat table — the classic remoting protocol's publish lookup. This is the
// send path:
// when the master is gone the honest answer is "not exist" — sending to a
// slave burns a whole retry round on SYSTEM_BUSY(2) with the wrong error
// type. Not found is a normal outcome (that is exactly a downed master); the
// caller decides what to report.
func (i *Instance) FindBrokerAddressInPublish(brokerName string) (string, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	addrs, ok := i.brokerAddrTable[brokerName]
	if !ok {
		return "", false
	}
	addr, ok := addrs[int64(common.MasterID)]
	return addr, ok
}

// PublishAddrFor is the master-only resolution shape shared by send /
// end-transaction / POP ack: table lookup -> refresh the topic route -> retry
// -> "The broker[X] not exist".
func (i *Instance) PublishAddrFor(brokerName, topic string) (string, error) {
	addr, ok := i.FindBrokerAddressInPublish(brokerName)
	if !ok {
		if _, err := i.UpdateTopicRouteInfoFromNameServer(topic, 5000, false); err != nil {
			return "", err
		}
		addr, ok = i.FindBrokerAddressInPublish(brokerName)
	}
	if !ok {
		return "", common.ClientError(fmt.Sprintf("The broker[%s] not exist", brokerName))
	}
	return addr, nil
}

// GetRouteOfAllBrokers collects one address per broker group across all
// cached routes (master preferred), deduplicated and sorted — the heartbeat
// fan-out shape for "any one will do".
func (i *Instance) GetRouteOfAllBrokers() []string {
	var addrs []string
	for _, route := range i.cachedRoutes() {
		for _, bd := range route.BrokerDatas {
			if addr, ok := bd.SelectBrokerAddr(); ok && !containsString(addrs, addr) {
				addrs = append(addrs, addr)
			}
		}
	}
	sort.Strings(addrs)
	return addrs
}

// GetAllBrokerAddrs collects EVERY address of every broker group (master AND
// slaves), deduplicated and sorted. Unregister(35) must reach each broker:
// ProducerManager/ConsumerManager state is per broker, and a slave that never
// gets the unregister only cleans up via the channel scan (~120s default).
func (i *Instance) GetAllBrokerAddrs() []string {
	var addrs []string
	for _, route := range i.cachedRoutes() {
		for _, bd := range route.BrokerDatas {
			for _, pair := range bd.BrokerAddrs {
				if pair.Addr != "" && !containsString(addrs, pair.Addr) {
					addrs = append(addrs, pair.Addr)
				}
			}
		}
	}
	sort.Strings(addrs)
	return addrs
}

func containsString(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func (i *Instance) cachedRoutes() []*TopicRouteData {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]*TopicRouteData, 0, len(i.topicRouteTable))
	for _, r := range i.topicRouteTable {
		out = append(out, r)
	}
	return out
}

// ConsumerOffsetAddr is the remote offset store's address discipline:
// master first; on a miss refresh the route,
// then retry RELAXED — offsets are HA-replicated, so the broker serves offset
// reads from slaves too.
func (i *Instance) ConsumerOffsetAddr(mq common.MessageQueue) (string, error) {
	addr, ok := i.FindBrokerAddressInPublish(mq.BrokerName)
	if !ok {
		if _, err := i.UpdateTopicRouteInfoFromNameServer(mq.Topic, 5000, false); err != nil {
			return "", err
		}
		addr, ok = i.BrokerAddrOf(mq.BrokerName)
	}
	if !ok {
		return "", common.ClientError(fmt.Sprintf("The broker[%s] not exist", mq.BrokerName))
	}
	return addr, nil
}

// cleanOfflineBroker drops brokerAddrTable entries no cached route still
// mentions, mirroring the classic heartbeat task which runs this sweep
// alongside the send.
func (i *Instance) cleanOfflineBroker() {
	i.mu.Lock()
	defer i.mu.Unlock()
	for brokerName, addrs := range i.brokerAddrTable {
		var live map[int64]string
		for _, route := range i.topicRouteTable {
			for _, bd := range route.BrokerDatas {
				if bd.BrokerName != brokerName {
					continue
				}
				if live == nil {
					live = map[int64]string{}
				}
				for _, pair := range bd.BrokerAddrs {
					live[pair.ID] = pair.Addr
				}
			}
		}
		if len(live) == 0 {
			common.LogWarnf("the broker[%s] offline, remove it", brokerName)
			delete(i.brokerAddrTable, brokerName)
			continue
		}
		for id := range addrs {
			if _, ok := live[id]; !ok {
				delete(addrs, id)
			}
		}
	}
}

// ---------------- heartbeat ----------------

// PrepareHeartbeatData assembles the heartbeat across all registered
// consumers (clientId is the instance's).
func (i *Instance) PrepareHeartbeatData() *remoting.HeartbeatData {
	hb := remoting.NewHeartbeatData(i.clientID)
	for _, c := range i.consumersSnapshot() {
		cd := remoting.NewConsumerData(c.ConsumerGroup(), c.ConsumeType(), c.MessageModel(), c.ConsumeFromWhere())
		cd.UnitMode = c.IsUnitMode()
		for _, sub := range c.Subscriptions() {
			cd.AddSubscriptionData(sub)
		}
		hb.AddConsumerData(cd)
	}
	return hb
}

// SendHeartbeat sends one heartbeat body to one broker.
//
// Every accepted beat also records the broker's protocol version, exactly as the
// reference does on its plain path: the version table is filled by whichever
// heartbeat form actually ran, so the V2 switch never decides whether a broker's
// version becomes known.
func (i *Instance) SendHeartbeat(addr string, hb *remoting.HeartbeatData, timeoutMillis int64) error {
	request := remoting.CreateRequestCommand(remoting.ReqHeartBeat, nil)
	request.SetBody(hb.Encode())
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if err := i.checkResponse(response); err != nil {
		return err
	}
	i.rememberBrokerVersion(addr, response.Version)
	return nil
}

// SendHeartbeatToAllBroker fans the heartbeat out to every known broker
// address INCLUDING SLAVES, returning the success count. The
// consumer-table-empty short-circuit means no send at all; with consumers
// registered slaves must receive heartbeats too (each broker keeps its own
// ConsumerManager — a slave without heartbeat state rejects pulls pointing at
// it with SUBSCRIPTION_NOT_EXIST). One broker's failure is debug-logged only.
func (i *Instance) SendHeartbeatToAllBroker(timeoutMillis int64) int {
	if len(i.consumerTableSnapshot()) == 0 {
		return 0
	}
	hb := i.PrepareHeartbeatData()
	ok := 0
	for _, addr := range i.GetAllBrokerAddrs() {
		if err := i.SendHeartbeat(addr, hb, timeoutMillis); err != nil {
			common.LogDebugf("heartbeat to %s failed: %v", addr, err)
			continue
		}
		ok++
	}
	return ok
}

// PrepareHeartbeatDataWithoutSubscriptions is the V2 sibling body: same
// consumers, `withoutSub` set and no subscription sets, which is what a V2
// broker uses to skip re-registering subscriptions it already has.
func (i *Instance) PrepareHeartbeatDataWithoutSubscriptions() *remoting.HeartbeatData {
	return i.PrepareHeartbeatData().CloneWithoutSubscriptions()
}

// SendHeartbeatV2 sends one V2 body (fingerprint filled, optionally without
// subscriptions) and reads the broker's two gating flags back off the response.
// The request code is still HEART_BEAT(34): what makes it V2 is the body plus
// the IS_SUPPORT_HEART_BEAT_V2 / IS_SUB_CHANGE answer.
func (i *Instance) SendHeartbeatV2(addr string, hb *remoting.HeartbeatData,
	timeoutMillis int64) (remoting.HeartbeatV2Result, error) {

	request := remoting.CreateRequestCommand(remoting.ReqHeartBeat, nil)
	request.SetBody(hb.Encode())
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return remoting.HeartbeatV2Result{}, err
	}
	if err := i.checkResponse(response); err != nil {
		return remoting.HeartbeatV2Result{}, err
	}
	return remoting.DecodeHeartbeatV2Result(response), nil
}

// SendHeartbeatToAllBrokerV2 is the fingerprint-driven beat. For each address it
// sends one of two bodies:
//
//   - the subscriptions WITHOUT the fingerprint-cached one, when this broker is
//     known to speak V2 and the fingerprint stored for it equals the current
//     one: nothing changed, so the subscription sets are dropped;
//   - the full body otherwise (an unknown broker, a broker that answered
//     "not supported", or a fingerprint the broker no longer agrees with).
//
// Both bodies carry the SAME fingerprint. The bookkeeping after a
// subscription-carrying answer is the part that keeps the protocol honest: a
// broker that supports V2 gets its fingerprint stored, unless it answered
// IS_SUB_CHANGE — that answer means "the group's fingerprint I hold differs",
// so the stored value is dropped and the next beat carries subscriptions again.
//
// isRebalance is the reference hook RebalancePushImpl uses: an assignment change
// invalidates every cached fingerprint, because the client's view of which
// subscriptions matter just changed and the broker must be told again.
//
// Returns the number of addresses that answered SUCCESS.
func (i *Instance) SendHeartbeatToAllBrokerV2(isRebalance bool, timeoutMillis int64) int {
	if len(i.consumerTableSnapshot()) == 0 {
		return 0
	}
	withSub := i.PrepareHeartbeatData()
	fingerprint := withSub.ComputeHeartbeatFingerprint()
	if fingerprint == 0 {
		// A broker dispatches on `fingerprint != 0` to take the V2 branch, so a
		// hash that lands on 0 would silently downgrade this beat to V1 while
		// this client still believes it is speaking V2. Rare as it is, the
		// answer is to send the plain body and keep the caches untouched.
		common.LogDebugf("heartbeat v2 fingerprint is 0 for [%s], fall back to the plain beat", i.clientID)
		return i.SendHeartbeatToAllBroker(timeoutMillis)
	}
	withSub.HeartbeatFingerprint = int64(fingerprint)
	withoutSub := withSub.CloneWithoutSubscriptions()

	if isRebalance {
		i.ResetBrokerHeartbeatFingerprints()
	}

	ok := 0
	for _, addr := range i.GetAllBrokerAddrs() {
		body := withSub
		if i.brokerSupportsHeartbeatV2(addr) && i.cachedHeartbeatFingerprint(addr) == fingerprint {
			body = withoutSub
		}
		result, err := i.SendHeartbeatV2(addr, body, timeoutMillis)
		if err != nil {
			common.LogDebugf("heartbeat v2 to %s failed: %v", addr, err)
			continue
		}
		ok++
		i.rememberBrokerVersion(addr, result.Version)
		if !result.SupportV2 {
			// An old broker: it understood the beat but not the fingerprint, so
			// the subscriptions must keep travelling on every beat.
			i.forgetHeartbeatV2Broker(addr)
			continue
		}
		i.markHeartbeatV2Broker(addr)
		if result.SubChange {
			i.dropHeartbeatFingerprint(addr)
			continue
		}
		i.storeHeartbeatFingerprint(addr, fingerprint)
	}
	return ok
}

// SendHeartbeatToAllBrokerWithLockV2 is the entry point the periodic task and a
// rebalance use: one beat at a time, a concurrent caller simply skips (the
// reference logs "lock heartBeat, but failed" rather than queueing, and a beat
// that misses one round is retried by the next period).
func (i *Instance) SendHeartbeatToAllBrokerWithLockV2(isRebalance bool, timeoutMillis int64) int {
	if !i.heartbeatMu.TryLock() {
		common.LogDebugf("sendHeartbeatToAllBrokerV2 lock heartBeat, but failed. [%s]", i.clientID)
		return 0
	}
	defer i.heartbeatMu.Unlock()
	if i.config.UseHeartbeatV2 {
		return i.SendHeartbeatToAllBrokerV2(isRebalance, timeoutMillis)
	}
	return i.SendHeartbeatToAllBroker(timeoutMillis)
}

// ResetBrokerHeartbeatFingerprints clears the per-address fingerprint cache (the
// reference's resetBrokerAddrHeartbeatFingerprintMap). Every broker then gets a
// subscription-carrying beat again.
func (i *Instance) ResetBrokerHeartbeatFingerprints() {
	i.heartbeatStateMu.Lock()
	defer i.heartbeatStateMu.Unlock()
	for addr := range i.heartbeatFingerprintTable {
		delete(i.heartbeatFingerprintTable, addr)
	}
}

// cachedHeartbeatFingerprint reads the fingerprint stored for one broker
// address; 0 means "nothing stored" (which is also the V1 fingerprint, so 0 can
// never be a stored V2 value — see ComputeHeartbeatFingerprint's normalisation).
func (i *Instance) cachedHeartbeatFingerprint(addr string) int32 {
	i.heartbeatStateMu.Lock()
	defer i.heartbeatStateMu.Unlock()
	return i.heartbeatFingerprintTable[addr]
}

func (i *Instance) storeHeartbeatFingerprint(addr string, fingerprint int32) {
	i.heartbeatStateMu.Lock()
	defer i.heartbeatStateMu.Unlock()
	i.heartbeatFingerprintTable[addr] = fingerprint
}

func (i *Instance) dropHeartbeatFingerprint(addr string) {
	i.heartbeatStateMu.Lock()
	defer i.heartbeatStateMu.Unlock()
	delete(i.heartbeatFingerprintTable, addr)
}

// brokerSupportsHeartbeatV2 reports the addresses this instance has already
// seen answer IS_SUPPORT_HEART_BEAT_V2.
func (i *Instance) brokerSupportsHeartbeatV2(addr string) bool {
	i.heartbeatStateMu.Lock()
	defer i.heartbeatStateMu.Unlock()
	_, ok := i.heartbeatV2Brokers[addr]
	return ok
}

func (i *Instance) markHeartbeatV2Broker(addr string) {
	i.heartbeatStateMu.Lock()
	defer i.heartbeatStateMu.Unlock()
	i.heartbeatV2Brokers[addr] = struct{}{}
}

func (i *Instance) forgetHeartbeatV2Broker(addr string) {
	i.heartbeatStateMu.Lock()
	defer i.heartbeatStateMu.Unlock()
	delete(i.heartbeatV2Brokers, addr)
	delete(i.heartbeatFingerprintTable, addr)
}

// rememberBrokerVersion records the broker protocol version a heartbeat response
// carried (the reference's brokerVersionTable).
func (i *Instance) rememberBrokerVersion(addr string, version int32) {
	if addr == "" {
		return
	}
	i.heartbeatStateMu.Lock()
	defer i.heartbeatStateMu.Unlock()
	i.brokerVersionTable[addr] = version
}

// BrokerVersion returns the last version one broker address reported through a
// heartbeat, and whether anything is known about it.
func (i *Instance) BrokerVersion(addr string) (int32, bool) {
	i.heartbeatStateMu.Lock()
	defer i.heartbeatStateMu.Unlock()
	v, ok := i.brokerVersionTable[addr]
	return v, ok
}

func (i *Instance) consumerTableSnapshot() []Consumer {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	out := make([]Consumer, 0, len(i.consumerTable))
	for _, c := range i.consumerTable {
		out = append(out, c)
	}
	return out
}

// ---------------- CHECK_CLIENT_CONFIG(46) ----------------

// findBrokerAddrByTopic reads the cached routes only and picks one broker
// (master preferred); no cache -> no address, the caller skips.
func (i *Instance) findBrokerAddrByTopic(topic string) (string, bool) {
	i.mu.Lock()
	route := i.topicRouteTable[topic]
	i.mu.Unlock()
	if route == nil {
		return "", false
	}
	if len(route.BrokerDatas) == 0 {
		return "", false
	}
	return route.BrokerDatas[pseudoRandomIndex(len(route.BrokerDatas))].SelectBrokerAddr()
}

// CheckClientInBroker validates every registered consumer's subscriptions at
// the broker (CHECK_CLIENT_CONFIG, the classic remoting protocol's
// pre-flight check). Only non-TAG expressions go on the
// wire: a broken SQL92 filter compiles to nothing on the broker and
// ExpressionMessageFilter then admits ALL messages — a silent subscribe-all.
// This call turns the broken expression into a startup-time failure.
func (i *Instance) CheckClientInBroker() error {
	for _, c := range i.consumersSnapshot() {
		subs := c.Subscriptions()
		if len(subs) == 0 {
			// The classic check returns (not continues) on a subscription-less consumer.
			return nil
		}
		if err := i.CheckSubscriptionsInBroker(c.ConsumerGroup(), subs); err != nil {
			return err
		}
	}
	return nil
}

// CheckSubscriptionsInBroker is the inner loop, exposed for callers that keep
// consumers outside the consumer table.
func (i *Instance) CheckSubscriptionsInBroker(group string, subs []*remoting.SubscriptionData) error {
	for _, sub := range subs {
		// Protocol tag-type check: null / "" / "TAG" all count as TAG.
		if sub.ExpressionType == "" || sub.ExpressionType == remoting.ExpressionTypeTag {
			continue
		}
		addr, ok := i.findBrokerAddrByTopic(sub.Topic)
		if !ok {
			continue
		}
		if err := i.checkClientConfig(addr, group, sub, DefaultMQClientAPITimeoutMillis); err != nil {
			if e, ok := err.(*common.Error); ok && e.Kind == common.KindClient {
				return err
			}
			// Transport-class failures (old brokers that do not know 46)
			// surface with the protocol's fixed wording; consumer.start() fails too.
			return common.ClientError(fmt.Sprintf(
				"Check client in broker error, maybe because you use %s to filter message, but server has not been upgraded to support!This error would not affect the launch of consumer, but may has impact on message receiving if you have use the new features which are not supported by server, please check the log!",
				sub.ExpressionType))
		}
	}
	return nil
}

// checkClientConfig sends one CHECK_CLIENT_CONFIG(46). The header is null and
// the body carries CheckClientRequestBody as JSON; a non-SUCCESS response
// raises a client error carrying the broker's response code (the response code
// itself is what the classic protocol's client error is built from).
func (i *Instance) checkClientConfig(addr, consumerGroup string, sub *remoting.SubscriptionData, timeoutMillis int64) error {
	request := remoting.CreateRequestCommand(remoting.ReqCheckClientConfig, nil)
	body := &remoting.CheckClientRequestBody{
		ClientID:         remoting.StrPtr(i.clientID),
		Group:            remoting.StrPtr(consumerGroup),
		SubscriptionData: sub,
	}
	request.SetBody(body.Encode())
	response, err := i.invokeSync(i.brokerVIPAddr(addr), request, timeoutMillis)
	if err != nil {
		return err
	}
	if response.Code != remoting.RespSuccess {
		return common.ClientErrorCode(response.Code, response.Remark)
	}
	return nil
}

// ---------------- UNREGISTER_CLIENT(35) ----------------

// UnregisterClient tells one broker the client is going away. Empty group
// slots stay OFF the wire entirely: the broker branches on group != null, and
// an empty string would be treated as a real group named "".
func (i *Instance) UnregisterClient(addr string, producerGroup, consumerGroup string, timeoutMillis int64) error {
	header := &remoting.UnregisterClientRequestHeader{ClientID: remoting.StrPtr(i.clientID)}
	if s := strings.TrimSpace(producerGroup); s != "" {
		header.ProducerGroup = remoting.StrPtr(producerGroup)
	}
	if s := strings.TrimSpace(consumerGroup); s != "" {
		header.ConsumerGroup = remoting.StrPtr(consumerGroup)
	}
	request := remoting.CreateRequestCommand(remoting.ReqUnregisterClient, header)
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return err
	}
	return i.checkResponse(response)
}

// UnregisterClientAllBrokers notifies every broker id (slaves included) —
// per-broker registration state otherwise survives until the channel scan.
func (i *Instance) UnregisterClientAllBrokers(producerGroup, consumerGroup string) {
	for _, addr := range i.GetAllBrokerAddrs() {
		if err := i.UnregisterClient(addr, producerGroup, consumerGroup, DefaultMQClientAPITimeoutMillis); err != nil {
			common.LogDebugf("unregister_client failed, addr=%s: %v", addr, err)
		}
	}
}

// ---------------- offset query / update ----------------

// QueryConsumerOffset reads one queue's committed offset. QUERY_NOT_FOUND(22)
// maps to found=false (no error); every other non-SUCCESS is a broker error.
// When addr is empty the address is derived (master, then slave fallback).
func (i *Instance) QueryConsumerOffset(consumerGroup string, mq common.MessageQueue, timeoutMillis int64, addr string, setZeroIfNotFound bool) (int64, bool, error) {
	if addr == "" {
		var err error
		if addr, err = i.ConsumerOffsetAddr(mq); err != nil {
			return 0, false, err
		}
	}
	header := &remoting.QueryConsumerOffsetRequestHeader{
		ConsumerGroup: remoting.StrPtr(consumerGroup),
		Topic:         remoting.StrPtr(mq.Topic),
		QueueID:       remoting.I32Ptr(mq.QueueID),
	}
	if setZeroIfNotFound {
		header.SetZeroIfNotFound = remoting.BoolPtr(true)
	}
	request := remoting.CreateRequestCommand(remoting.ReqQueryConsumerOffset, header)
	response, err := i.invokeSync(i.brokerVIPAddr(addr), request, timeoutMillis)
	if err != nil {
		return 0, false, err
	}
	if response.Code == remoting.RespQueryNotFound {
		return 0, false, nil
	}
	if err := i.checkResponse(response); err != nil {
		return 0, false, err
	}
	var respHeader remoting.QueryConsumerOffsetResponseHeader
	respHeader.FromExtFields(response.ExtFields())
	if respHeader.Offset == nil {
		return 0, true, nil
	}
	return *respHeader.Offset, true, nil
}

// UpdateConsumerOffset commits one queue's offset.
func (i *Instance) UpdateConsumerOffset(consumerGroup string, mq common.MessageQueue, commitOffset, timeoutMillis int64, addr string) error {
	if addr == "" {
		var err error
		if addr, err = i.BrokerAddr(mq); err != nil {
			return err
		}
	}
	header := &remoting.UpdateConsumerOffsetRequestHeader{
		ConsumerGroup: remoting.StrPtr(consumerGroup),
		Topic:         remoting.StrPtr(mq.Topic),
		QueueID:       remoting.I32Ptr(mq.QueueID),
		CommitOffset:  remoting.I64Ptr(commitOffset),
	}
	request := remoting.CreateRequestCommand(remoting.ReqUpdateConsumerOffset, header)
	response, err := i.invokeSync(i.brokerVIPAddr(addr), request, timeoutMillis)
	if err != nil {
		return err
	}
	return i.checkResponse(response)
}

// ---------------- low-level helpers ----------------

// invokeSync is address-transparent: it sends to exactly the address it is
// handed, which is what nameserver traffic and an already-rewritten address
// need. A broker-facing request in family of the VIP-served RPCs resolves its
// address and wraps it in brokerVIPAddr at the call site, one request at a
// time.
func (i *Instance) invokeSync(addr string, request *remoting.RemotingCommand, timeoutMillis int64) (*remoting.RemotingCommand, error) {
	return i.remoting.InvokeSync(addr, request, timeoutMillis)
}

// checkResponse maps any non-SUCCESS response to a broker error carrying the
// code and remark.
func (i *Instance) checkResponse(response *remoting.RemotingCommand) error {
	if response.Code != remoting.RespSuccess {
		return common.BrokerError(response.Code, response.Remark)
	}
	return nil
}
