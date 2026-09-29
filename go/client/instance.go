// Instance: the shared client factory behind every producer and consumer on
// one clientId (Java MQClientInstance / Python MQClientInstance).
//
// Responsibilities ported here:
//   - the topic route cache (route table + publish info + flat broker address
//     table) and its nameserver refresh;
//   - the address lookup family, each with its own master/slave discipline:
//     publish lookups are MASTER-ONLY (slaves reject writes with SYSTEM_BUSY),
//     offset reads may fall back to slaves, heartbeat/unregister fan out to
//     EVERY broker id;
//   - consumer heartbeats (34) — including slaves: each broker keeps its own
//     ConsumerManager, and a slave that never sees a heartbeat rejects pulls
//     with SUBSCRIPTION_NOT_EXIST;
//   - the broker->client handlers Java registers at factory construction
//     (40/220/221/326; 307/309 land with the push consumer, whose running-info
//     beans they need);
//   - the Java scheduled tasks with scheduleAtFixedRate anchoring (task n
//     fires at initialDelay + (n-1)*period, never initialDelay + period).
package client

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// DefaultMQClientAPITimeoutMillis is the instance-level API timeout (Rust
// MQ_CLIENT_API_TIMEOUT_MILLIS, Java MQClientAPIImpl's 3s default).
const DefaultMQClientAPITimeoutMillis = int64(3000)

// ClientInstanceConfig carries the instance-scoped knobs (the Java
// ClientConfig fields MQClientInstance actually reads).
type ClientInstanceConfig struct {
	ConnectTimeoutMillis int64
	InvokeTimeoutMillis  int64
	TLSEnable            bool
	// NamesrvWsAddr is the address-server URL; when set and no nameserver is
	// configured, start() fetches addresses from it (Java TopAddressing).
	NamesrvWsAddr string
	// PollNameServerIntervalMillis drives the route-refresh period (Java
	// getPollNameServerInterval, default 30s).
	PollNameServerIntervalMillis int64
	// NamesrvRefreshIntervalMillis drives the address-server re-fetch (Java
	// 2 minutes).
	NamesrvRefreshIntervalMillis int64
	// HeartbeatIntervalMillis (Java getHeartbeatBrokerInterval, default 30s).
	HeartbeatIntervalMillis int64
	// PersistConsumerOffsetIntervalMillis (Java default 5s).
	PersistConsumerOffsetIntervalMillis int64
}

// NewClientInstanceConfig returns the Java defaults.
func NewClientInstanceConfig() ClientInstanceConfig {
	return ClientInstanceConfig{
		ConnectTimeoutMillis:                3000,
		InvokeTimeoutMillis:                 3000,
		PollNameServerIntervalMillis:        30_000,
		NamesrvRefreshIntervalMillis:        120_000,
		HeartbeatIntervalMillis:             30_000,
		PersistConsumerOffsetIntervalMillis: 5_000,
	}
}

// Consumer is the instance's view of one registered consumer (Java
// MQConsumerInner / Rust RegisteredConsumer). Every method is driven by the
// instance: heartbeat assembly, offset persistence, broker pushes.
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
	// AdjustThreadPool is the periodic elasticity sweep (no-op in Java too).
	AdjustThreadPool()
	// ResetOffset handles RESET_CONSUMER_CLIENT_OFFSET(220); the instance
	// already runs it off the remoting read goroutine.
	ResetOffset(topic string, table remoting.MQOffsetTable)
	// GetConsumerStatus answers GET_CONSUMER_STATUS_FROM_CLIENT(221). A nil
	// topic means the request header carried none.
	GetConsumerStatus(topic *string) remoting.MQOffsetTable
	// PersistConsumerOffset is the periodic offset flush.
	PersistConsumerOffset() error
}

// ---------------------------------------------------------------- registry

var (
	registryMu sync.Mutex
	registry   = map[string]*Instance{}
)

// CreateOrGetInstance mirrors Java MQClientManager.getOrCreateMQClientInstance:
// an existing live instance with the same clientId is REUSED (the first
// caller's config wins).
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
	brokerAddrTable       map[string]map[int64]string
	topicsInUse           map[string]struct{}

	started atomic.Bool

	consumerTable      map[string]Consumer
	producerTable      map[string]struct{}
	consumerGroupTable map[string]struct{}
	tablesMu           sync.Mutex

	consumerIDsChanged atomic.Int64

	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

func newInstance(clientID string, nameServerAddrs []string, config ClientInstanceConfig) *Instance {
	inst := &Instance{
		clientID:              clientID,
		config:                config,
		nameServerAddrs:       append([]string(nil), nameServerAddrs...),
		topicRouteTable:       map[string]*TopicRouteData{},
		topicPublishInfoTable: map[string]*TopicPublishInfo{},
		brokerAddrTable:       map[string]map[int64]string{},
		topicsInUse:           map[string]struct{}{},
		consumerTable:         map[string]Consumer{},
		producerTable:         map[string]struct{}{},
		consumerGroupTable:    map[string]struct{}{},
		stop:                  make(chan struct{}),
	}
	inst.remoting = remoting.NewRemotingClientWithConfig(remoting.ClientConfig{
		ConnectTimeoutMillis:     config.ConnectTimeoutMillis,
		InvokeTimeoutMillis:      config.InvokeTimeoutMillis,
		TLSEnable:                config.TLSEnable,
		TLSTestMode:              true,
		EnableReconnectForGoAway: true,
	})
	inst.registerClientProcessors()
	return inst
}

// ClientID returns the instance's client id.
func (i *Instance) ClientID() string { return i.clientID }

// Remoting exposes the shared transport (producers register their RPC hooks
// on it; the offset store rides its invoke path).
func (i *Instance) Remoting() *remoting.RemotingClient { return i.remoting }

// Config exposes the instance-scoped configuration.
func (i *Instance) Config() ClientInstanceConfig { return i.config }

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

// RegisterConsumer installs a consumer under its group (no conflict check,
// same as the plain dict/HashMap assignment in Python/Java).
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

// UnregisterProducer must run BEFORE Shutdown (Java shutdown ordering), so
// the departing producer does not block the factory's teardown.
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

// tenantCount is the shutdown guard's three tables (Java shutdown reads
// producerTable / consumerTable / ... sizes).
func (i *Instance) tenantCount() int {
	i.tablesMu.Lock()
	defer i.tablesMu.Unlock()
	return len(i.consumerTable) + len(i.producerTable) + len(i.consumerGroupTable)
}

// RebalanceImmediately pokes every registered consumer (Java
// rebalanceImmediately wakes the shared rebalance service; without such a
// shared thread each consumer wakes its own loop).
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

// registerClientProcessors wires the instance-level processors (Python
// registers 326/220/221/307/309/40 at construction — Java's
// ClientRemotingProcessor). 307/309 arrive with the push consumer, which owns
// the running-info beans; before then the transport logs the unmatched codes.
func (i *Instance) registerClientProcessors() {
	i.remoting.RegisterProcessor(remoting.ReqNotifyConsumerIDsChanged, i.processNotifyConsumerIDsChanged)
	i.remoting.RegisterProcessor(remoting.ReqResetConsumerClientOffset, i.processResetOffset)
	i.remoting.RegisterProcessor(remoting.ReqGetConsumerStatusFromClient, i.processGetConsumerStatus)
	i.remoting.RegisterProcessor(remoting.ReqPushReplyMessageToClient, i.processReplyMessage)
}

// ackIfWanted answers the push. Java's processor returns null (no reply) for
// 40/220 and the broker sends both oneway; Respond itself drops replies for
// oneway pushes, so a hypothetical sync pusher still gets its SUCCESS.
func (i *Instance) ackIfWanted(sink *remoting.ResponseSink) {
	sink.Respond(remoting.CreateResponseCommand(remoting.RespSuccess, ""))
}

// processNotifyConsumerIdsChanged: one INFO line copied from Java
// (ClientRemotingProcessor#notifyConsumerIdsChanged), then rebalance
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

// processResetOffset (220): no reply (Java returns null); the actual reset
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

// processReplyMessage (326) routes request-reply answers to the waiting
// producer future table; the holder lands with the producer task. Until then
// the unmatched branch logs what Java logs on a miss and answers SUCCESS
// (both matched and unmatched paths answer SUCCESS in Java).
func (i *Instance) processReplyMessage(request *remoting.RemotingCommand, _ string, sink *remoting.ResponseSink) {
	var header remoting.ReplyMessageRequestHeader
	header.FromExtFields(request.ExtFields())
	bornHost := ""
	if header.BornHost != nil {
		bornHost = *header.BornHost
	}
	common.LogWarnf("receive reply message, but not matched any request, CorrelationId: %q, reply from host: %q", "", bornHost)
	sink.Respond(remoting.CreateResponseCommand(remoting.RespSuccess, ""))
}

// ---------------- lifecycle ----------------

// Start flips started and spawns the scheduled tasks. Multiple
// producers/consumers on one clientId all call it; only the first has effect
// (Java started.compareAndSet).
//
// When no nameserver is configured but the address-server URL is, the first
// fetch happens synchronously; a failed or empty fetch tears the instance
// down and reports Java's 10004 (NoNameServerException) rather than letting
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
		// scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min) — Java only
		// schedules this task when the address server is in play.
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
	// Java bundles cleanOfflineBroker INTO the heartbeat task.
	i.spawnPeriodic(func() {
		i.cleanOfflineBroker()
		i.SendHeartbeatToAllBroker(5000)
	}, 1_000, i.config.HeartbeatIntervalMillis)
	i.spawnPeriodic(func() {
		i.PersistConsumerOffsets()
	}, 10_000, i.config.PersistConsumerOffsetIntervalMillis)
	i.spawnPeriodic(func() {
		i.AdjustThreadPools()
	}, 60_000, 60_000)
	return nil
}

// Shutdown applies Java's guard: while any producer/consumer is still
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

// shutdownFactory is Java's RUNNING teardown, also used by a failed start
// (Java cleanupAfterStartFailure + removeClientFactory).
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
// failures (Java adjustThreadPool: catch Exception ignored).
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
// only when it changed (Java MQClientAPIImpl.fetchNameServerAddr). The body's
// first ';' field is the address list.
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
	// default topic the way the Java client does to build its publish info.
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
	// Java stores the route's own brokerAddrs map by shared reference, so its
	// "only overwrite when changed" still refreshes; overwriting unconditionally
	// here is the same end state.
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
// error is rethrown unless it is a broker-business error (Python's exact
// wording).
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
// (fetch errors are swallowed — Python's except Exception: pass).
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
// Java's rebalanceByTopic only warns and keeps the existing assignment.
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

// PublishInfoOf reads the cached publish info without any RPC.
func (i *Instance) PublishInfoOf(topic string) *TopicPublishInfo {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.topicPublishInfoTable[topic]
}

// FindBrokerAddrInRoute picks an address for the broker inside one route
// (Python find_broker_addr_in_route staticmethod): master preferred, else any.
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
// An empty brokerName degrades to "the first broker in the route" like
// Python's _addr_for.
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

// BrokerAddrOf searches ALL cached routes for the broker name (Python
// broker_addr_of) — "any one of its addrs will do" (master preferred, slave
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
// the flat table (Java findBrokerAddressInPublish). This is the send path:
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

// ConsumerOffsetAddr is RemoteBrokerOffsetStore's address discipline (Java
// fetchConsumeOffsetFromBroker): master first; on a miss refresh the route,
// then retry RELAXED — offsets are HA-replicated, Java reads them from slaves.
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
// mentions (Java MQClientInstance#cleanOfflineBroker, which schedules this
// inside the heartbeat task).
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
// consumers (Java prepareHeartbeatData; clientId is the instance's).
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
func (i *Instance) SendHeartbeat(addr string, hb *remoting.HeartbeatData, timeoutMillis int64) error {
	request := remoting.CreateRequestCommand(remoting.ReqHeartBeat, nil)
	request.SetBody(hb.Encode())
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return err
	}
	return i.checkResponse(response)
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
// the broker (Java checkClientInBroker). Only non-TAG expressions go on the
// wire: a broken SQL92 filter compiles to nothing on the broker and
// ExpressionMessageFilter then admits ALL messages — a silent subscribe-all.
// This call turns the broken expression into a startup-time failure.
func (i *Instance) CheckClientInBroker() error {
	for _, c := range i.consumersSnapshot() {
		subs := c.Subscriptions()
		if len(subs) == 0 {
			// Java returns (not continues) on a subscription-less consumer.
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
		// Java ExpressionType.isTagType: null / "" / "TAG" all count as TAG.
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
			// surface with Java's fixed wording; consumer.start() fails too.
			return common.ClientError(fmt.Sprintf(
				"Check client in broker error, maybe because you use %s to filter message, but server has not been upgraded to support!This error would not affect the launch of consumer, but may has impact on message receiving if you have use the new features which are not supported by server, please check the log!",
				sub.ExpressionType))
		}
	}
	return nil
}

// checkClientConfig sends one CHECK_CLIENT_CONFIG(46). The header is null and
// the body carries CheckClientRequestBody as JSON; a non-SUCCESS response
// raises a client error carrying the broker's response code (Java throws
// MQClientException from the response code itself).
func (i *Instance) checkClientConfig(addr, consumerGroup string, sub *remoting.SubscriptionData, timeoutMillis int64) error {
	request := remoting.CreateRequestCommand(remoting.ReqCheckClientConfig, nil)
	body := &remoting.CheckClientRequestBody{
		ClientID:         remoting.StrPtr(i.clientID),
		Group:            remoting.StrPtr(consumerGroup),
		SubscriptionData: sub,
	}
	request.SetBody(body.Encode())
	response, err := i.invokeSync(addr, request, timeoutMillis)
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
	response, err := i.invokeSync(addr, request, timeoutMillis)
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
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return err
	}
	return i.checkResponse(response)
}

// ---------------- low-level helpers ----------------

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
