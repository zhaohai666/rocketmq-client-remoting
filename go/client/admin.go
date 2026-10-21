// DefaultMQAdminExt: the management client of the classic remoting protocol.
//
// Coverage: topic CRUD + config, broker cluster info / runtime info / config,
// nameserver KV config, subscription-group management, consumer & producer
// connections, consume stats, offsets (including a real broker-side reset) and
// message query (key / uniqKey / msgId / consume-queue).
//
// Wire facts that were expensive to establish — do NOT "simplify" them:
//
//   - GET_BROKER_CONFIG(26)'s body is Properties TEXT (`k=v\n`), not
//     JSON. Parsing it as a KVTable fails on every real broker.
//   - UPDATE_AND_CREATE_SUBSCRIPTIONGROUP(200)'s body is a
//     SubscriptionGroupConfig JSON object.
//   - GET_TOPIC_CONFIG(351) sends `topic` + `lo`, and its body is a plain
//     TopicConfig JSON.
//   - GET_ALL_SUBSCRIPTIONGROUP_CONFIG(201) is PAGED:
//     groupSeq / maxGroupNum / dataVersion, accumulating until
//     groupSeq >= totalGroupNum-1. An older broker omits totalGroupNum, in
//     which case one round returns everything.
//   - ResetOffsetBody.offsetTable is Map<MessageQueue, Long>.
//   - The KV-config requests go to the NAMESERVER, and PUT/DELETE must be
//     BROADCAST to every nameserver.
//   - `fetchTopicsByCluster` sends `cluster` (not `clusterName`); with the
//     wrong key the nameserver NPEs internally, swallows it, and answers
//     SUCCESS with an empty list.
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

// DefaultAdminTimeoutMillis is the classic admin default timeout (5000 * 3).
const DefaultAdminTimeoutMillis = int64(5000 * 3)

// adminReadPermByDefault is the protocol's default topic permission
// (READ | WRITE).
const adminReadPermByDefault = int32(common.PermRead | common.PermWrite)

// DefaultMQAdminExt is the admin client.
type DefaultMQAdminExt struct {
	mu sync.Mutex

	NameServerAddrs []string
	InstanceName    string
	ClientID        string
	UnitName        string
	// EnableStreamRequestType is the classic enableStreamRequestType client knob.
	EnableStreamRequestType bool
	// VipChannelEnabled is the broker-facing VIP switch, off by default: with it
	// on, the broker RPCs the VIP channel serves go to `port - 2` of the route
	// address instead of the address itself. It is never applied to nameserver
	// traffic. Set it before Start (it is handed to the instance the admin runs
	// on) or through SetVipChannelEnabled, which flips both.
	VipChannelEnabled bool
	// PollNameServerIntervalMillis drives the route-refresh period; it is
	// passed through once, when the instance is built.
	PollNameServerIntervalMillis int64
	// RpcHook is the user hook (ACL).
	RpcHook remoting.RPCHook
	// KVNamespaceToDeleteList is cleaned up when a topic is deleted (the
	// classic kvNamespaceToDeleteList).
	KVNamespaceToDeleteList []string
	TimeoutMillis           int64
	// MaxPageSizeInGetMetadata is the page size sent as maxTopicNum /
	// maxGroupNum by the metadata paging loops (default 2000).
	MaxPageSizeInGetMetadata int

	mqClient *Instance
	started  bool
}

func NewDefaultMQAdminExt(rpcHook remoting.RPCHook) *DefaultMQAdminExt {
	return &DefaultMQAdminExt{
		InstanceName:                 "ADMIN",
		PollNameServerIntervalMillis: 30_000,
		RpcHook:                      rpcHook,
		TimeoutMillis:                DefaultAdminTimeoutMillis,
		MaxPageSizeInGetMetadata:     DefaultMaxPageSizeInGetMetadata,
	}
}

// SetNamesrvAddr sets the nameserver list from a `;`-separated string.
func (a *DefaultMQAdminExt) SetNamesrvAddr(addr string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.NameServerAddrs = nil
	for _, part := range strings.Split(addr, ";") {
		if p := strings.TrimSpace(part); p != "" {
			a.NameServerAddrs = append(a.NameServerAddrs, p)
		}
	}
}

// SetNameServerAddresses sets the nameserver list.
func (a *DefaultMQAdminExt) SetNameServerAddresses(addrs []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.NameServerAddrs = append([]string(nil), addrs...)
}

// SetInstanceName changes the instance name (and hence the clientId).
func (a *DefaultMQAdminExt) SetInstanceName(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.InstanceName = name
}

// SetUnitName affects the clientId suffix
// and the dynamic-addressing URL.
func (a *DefaultMQAdminExt) SetUnitName(unitName string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.UnitName = unitName
}

// UnitName returns the configured unit name.
func (a *DefaultMQAdminExt) UnitNameValue() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.UnitName
}

// SetEnableStreamRequestType marks this admin's requests as stream requests.
func (a *DefaultMQAdminExt) SetEnableStreamRequestType(enable bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.EnableStreamRequestType = enable
}

// SetVipChannelEnabled turns the VIP-port rewrite on for this admin. The instance
// is told as well, because the RPCs the admin delegates to it (offset read and
// write, max/min offset, consumer list, topic create/delete, system topic list)
// do the rewrite at their own invoke boundary.
func (a *DefaultMQAdminExt) SetVipChannelEnabled(enable bool) {
	a.mu.Lock()
	a.VipChannelEnabled = enable
	instance := a.mqClient
	a.mu.Unlock()
	if instance != nil {
		instance.SetVipChannelEnabled(enable)
	}
}

// GetNameServerAddr renders the nameserver list as a `;`-joined string.
func (a *DefaultMQAdminExt) GetNameServerAddr() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return strings.Join(a.NameServerAddrs, ";")
}

// GetNameServerAddressList returns the nameserver list.
func (a *DefaultMQAdminExt) GetNameServerAddressList() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.NameServerAddrs...)
}

// SetTimeoutMillis overrides the default admin timeout.
func (a *DefaultMQAdminExt) SetTimeoutMillis(timeoutMillis int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.TimeoutMillis = timeoutMillis
}

// SetMaxPageSizeInGetMetadata sets the page size used by the metadata paging
// loops; non-positive values restore the 2000 default. A running instance is
// told as well, so callers reading the knob there see the same value.
func (a *DefaultMQAdminExt) SetMaxPageSizeInGetMetadata(size int) {
	if size <= 0 {
		size = DefaultMaxPageSizeInGetMetadata
	}
	a.mu.Lock()
	a.MaxPageSizeInGetMetadata = size
	instance := a.mqClient
	a.mu.Unlock()
	if instance != nil {
		instance.SetMaxPageSizeInGetMetadata(size)
	}
}

// pageSizeInGetMetadata resolves the current metadata page size.
func (a *DefaultMQAdminExt) pageSizeInGetMetadata() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.MaxPageSizeInGetMetadata > 0 {
		return a.MaxPageSizeInGetMetadata
	}
	if a.mqClient != nil {
		return a.mqClient.MaxPageSizeInGetMetadata()
	}
	return DefaultMaxPageSizeInGetMetadata
}

// Start builds and starts the underlying instance.
func (a *DefaultMQAdminExt) Start() error {
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return nil
	}
	if len(a.NameServerAddrs) == 0 {
		a.mu.Unlock()
		return common.ClientError("name server address is not set")
	}
	a.InstanceName = common.ChangeInstanceNameToPID(a.InstanceName)
	if a.ClientID == "" {
		a.ClientID = common.ClientIDFor(a.InstanceName, a.UnitName, a.EnableStreamRequestType)
	}
	clientID := a.ClientID
	addrs := append([]string(nil), a.NameServerAddrs...)
	stream := a.EnableStreamRequestType
	hook := a.RpcHook
	pollInterval := a.PollNameServerIntervalMillis
	vipEnabled := a.VipChannelEnabled
	pageSize := a.MaxPageSizeInGetMetadata
	a.mu.Unlock()

	config := NewClientInstanceConfig()
	config.PollNameServerIntervalMillis = pollInterval
	config.VipChannelEnabled = vipEnabled
	config.MaxPageSizeInGetMetadata = pageSize
	instance := CreateOrGetInstance(clientID, addrs, config)
	instance.EnsureRPCHooks("", stream, hook)
	if err := instance.Start(); err != nil {
		return err
	}

	a.mu.Lock()
	a.mqClient = instance
	a.started = true
	a.mu.Unlock()
	return nil
}

// Shutdown stops the underlying instance.
func (a *DefaultMQAdminExt) Shutdown() {
	a.mu.Lock()
	if !a.started {
		a.mu.Unlock()
		return
	}
	a.started = false
	instance := a.mqClient
	a.mqClient = nil
	a.mu.Unlock()
	if instance != nil {
		instance.Shutdown()
		instance.DetachFromRegistryIfLastTenant()
	}
}

func (a *DefaultMQAdminExt) Instance() (*Instance, error) {
	return a.requireClient()
}

func (a *DefaultMQAdminExt) requireClient() (*Instance, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started || a.mqClient == nil {
		return nil, common.ClientError("admin not started, call Start() first")
	}
	return a.mqClient, nil
}

func (a *DefaultMQAdminExt) timeout(millis int64) int64 {
	if millis > 0 {
		return millis
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.TimeoutMillis > 0 {
		return a.TimeoutMillis
	}
	return DefaultAdminTimeoutMillis
}

func (a *DefaultMQAdminExt) vipChannel() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.mqClient != nil {
		// Once started, the instance owns the switch: the broker RPCs the admin
		// delegates to it read it there, so the admin's own invoke boundary has
		// to read the same value. One request, one shift.
		return a.mqClient.VipChannelEnabled()
	}
	return a.VipChannelEnabled
}

// brokerVIPAddr is the admin's invoke-boundary rewrite: it returns the VIP
// address of a resolved broker address when the switch is on, and the address
// itself otherwise. Requests aimed at a nameserver never go through it.
func (a *DefaultMQAdminExt) brokerVIPAddr(addr string) string {
	return common.BrokerVIPChannel(a.vipChannel(), addr)
}

// ---------------- low-level invoke helpers ----------------

// invokeBroker sends a request to one broker over the address the fast channel
// listener uses when it is enabled, and checks for SUCCESS.
func (a *DefaultMQAdminExt) invokeBroker(addr string, code int32, ext *common.StringMap,
	body []byte, timeoutMillis int64) (*remoting.RemotingCommand, error) {

	return a.invokeBrokerOn(a.brokerVIPAddr(addr), code, ext, body, timeoutMillis)
}

// invokeBrokerPlain is the same round trip on the address the nameserver
// published. Reading a broker's own configuration is the one admin RPC that
// never moves: it is served on the listening port only, so aiming it at the
// fast channel would talk to a port that is closed when the channel is off.
func (a *DefaultMQAdminExt) invokeBrokerPlain(addr string, code int32, ext *common.StringMap,
	body []byte, timeoutMillis int64) (*remoting.RemotingCommand, error) {

	return a.invokeBrokerOn(addr, code, ext, body, timeoutMillis)
}

func (a *DefaultMQAdminExt) invokeBrokerOn(target string, code int32, ext *common.StringMap,
	body []byte, timeoutMillis int64) (*remoting.RemotingCommand, error) {

	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	request := adminRequest(code, ext)
	if body != nil {
		request.SetBody(body)
	}
	response, err := instance.invokeSync(target, request, a.timeout(timeoutMillis))
	if err != nil {
		return nil, err
	}
	if err := instance.checkResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

func (a *DefaultMQAdminExt) invokeNameServerAll(code int32, ext *common.StringMap, body []byte, timeoutMillis int64) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	request := adminRequest(code, ext)
	if body != nil {
		request.SetBody(body)
	}
	var errResponse *remoting.RemotingCommand
	for _, nsAddr := range instance.NameServerAddrs() {
		response, err := instance.invokeSync(nsAddr, request, a.timeout(timeoutMillis))
		if err != nil {
			return err
		}
		if response.Code != remoting.RespSuccess {
			errResponse = response
		}
	}
	if errResponse != nil {
		remark := errResponse.Remark
		if remark == "" {
			remark = "put/delete kv config failed"
		}
		return common.ClientErrorCode(errResponse.Code, remark)
	}
	return nil
}

func (a *DefaultMQAdminExt) invokeNameServerOne(code int32, ext *common.StringMap, timeoutMillis int64) (*remoting.RemotingCommand, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	request := adminRequest(code, ext)
	var lastErr error
	for _, nsAddr := range instance.NameServerAddrs() {
		response, err := instance.invokeSync(nsAddr, request, a.timeout(timeoutMillis))
		if err != nil {
			lastErr = err
			continue
		}
		return response, nil
	}
	return nil, common.ClientError(fmt.Sprintf("all name servers unreachable: %v", lastErr))
}

func (a *DefaultMQAdminExt) invokeNameServerAddr(addr string, code int32, ext *common.StringMap, timeoutMillis int64) (*remoting.RemotingCommand, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	response, err := instance.invokeSync(addr, adminRequest(code, ext), a.timeout(timeoutMillis))
	if err != nil {
		return nil, err
	}
	if err := instance.checkResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

func (a *DefaultMQAdminExt) firstBrokerAddr() (string, error) {
	instance, err := a.requireClient()
	if err != nil {
		return "", err
	}
	return instance.FirstBrokerAddr()
}

// ---------------- Topic management ----------------

// CreateTopic creates a topic on every master of the default-topic route.
func (a *DefaultMQAdminExt) CreateTopic(key, newTopic string, queueNum int32, topicSysFlag int32) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	_ = key
	if queueNum <= 0 {
		queueNum = common.DefaultTopicQueueNums
	}
	return instance.CreateTopicInRoute(newTopic, queueNum, queueNum,
		adminReadPermByDefault, topicSysFlag, "", 5000)
}

// CreateAndUpdateTopicConfig pushes one TopicConfig to `addr`,
// keyed by the broker's reserved createTopicKey.
func (a *DefaultMQAdminExt) CreateAndUpdateTopicConfig(addr string, config *remoting.TopicConfig) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	return instance.CreateTopicInBroker(addr, common.DefaultTopic, config.TopicName,
		config.ReadQueueNums, config.WriteQueueNums, config.Perm, config.TopicSysFlag,
		remoting.TopicFilterTypeSingleTag, config.Order, "", 5000, AdminCreateTopicRetryTimes)
}

// CreateTopicInBroker creates a topic on one broker.
func (a *DefaultMQAdminExt) CreateTopicInBroker(brokerAddr, topic string,
	readQueueNums, writeQueueNums, perm int32) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	return instance.CreateTopicInBroker(brokerAddr, common.DefaultTopic, topic,
		readQueueNums, writeQueueNums, perm, 0,
		remoting.TopicFilterTypeSingleTag, false, "", 5000, AdminCreateTopicRetryTimes)
}

// DeleteTopicInBroker deletes a topic from one broker.
func (a *DefaultMQAdminExt) DeleteTopicInBroker(brokerAddr, topic string) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	return instance.DeleteTopicInBroker(brokerAddr, topic, 5000)
}

// DeleteTopicInNameServer deletes the topic route from the given nameservers
// (all of them when addrs is empty).
func (a *DefaultMQAdminExt) DeleteTopicInNameServer(addrs []string, topic string) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	targets := addrs
	if len(targets) == 0 {
		targets = instance.NameServerAddrs()
	}
	for _, nsAddr := range targets {
		if _, err := a.invokeNameServerAddr(nsAddr, remoting.ReqDeleteTopicInNameSrv,
			adminExt("topic", topic), 5000); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTopicInNameSrv deletes the topic route from the nameservers (first
// reachable wins).
func (a *DefaultMQAdminExt) DeleteTopicInNameSrv(topic string) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	return instance.DeleteTopicInNameSrv(topic, 5000)
}

func (a *DefaultMQAdminExt) DeleteTopic(topic string, clusterName string) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	addrs, err := instance.BrokerAddrsOfCluster(clusterName)
	if err != nil {
		return err
	}
	for _, broker := range addrs {
		if err := instance.DeleteTopicInBroker(broker, topic, 5000); err != nil {
			common.LogWarnf("delete topic %s in broker %s failed: %v", topic, broker, err)
		}
	}
	if err := instance.DeleteTopicInNameSrv(topic, 5000); err != nil {
		common.LogWarnf("delete topic %s in name server failed: %v", topic, err)
	}
	a.mu.Lock()
	namespaces := append([]string(nil), a.KVNamespaceToDeleteList...)
	a.mu.Unlock()
	for _, ns := range namespaces {
		if err := a.DeleteKVConfig(ns, topic); err != nil {
			common.LogWarnf("delete kv config %s/%s failed: %v", ns, topic, err)
		}
	}
	return nil
}

// FetchAllTopicList lists every topic the nameserver knows.
func (a *DefaultMQAdminExt) FetchAllTopicList() (*remoting.TopicList, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	return instance.GetAllTopicListFromNameServer(10_000)
}

// FetchTopicsByCluster sends GET_TOPICS_BY_CLUSTER(224) to a nameserver.
func (a *DefaultMQAdminExt) FetchTopicsByCluster(clusterName string) (map[string]bool, error) {
	response, err := a.invokeNameServerOne(remoting.ReqGetTopicsByCluster,
		adminExt("cluster", clusterName), 0)
	topics := map[string]bool{}
	if err != nil {
		return topics, err
	}
	if response.Code == remoting.RespSuccess && len(response.Body) > 0 {
		if obj, ok := mustDecodeObject(response.Body); ok {
			for _, item := range jsonArraySafe(obj["topicList"]) {
				if s, ok := item.(string); ok {
					topics[s] = true
				}
			}
		}
	}
	return topics, nil
}

// GetClusterList returns the cluster names whose brokers appear in the topic's
// route.
func (a *DefaultMQAdminExt) GetClusterList(topic string) (map[string]bool, error) {
	clusterInfo, err := a.FetchBrokerClusterInfo()
	if err != nil {
		return nil, err
	}
	route, err := a.ExamineTopicRoute(topic)
	if err != nil {
		return nil, err
	}
	brokerNames := map[string]bool{}
	for _, bd := range route.BrokerDatas {
		brokerNames[bd.BrokerName] = true
	}
	clusters := map[string]bool{}
	for clusterName, names := range clusterInfo.ClusterAddrTable {
		for _, n := range names {
			if brokerNames[n] {
				clusters[clusterName] = true
				break
			}
		}
	}
	return clusters, nil
}

func (a *DefaultMQAdminExt) GetTopicClusterList(topic string) (map[string]bool, error) {
	return a.GetClusterList(topic)
}

// FetchAllTopicRoute walks every topic in the nameserver list and returns the
// routes it can resolve.
func (a *DefaultMQAdminExt) FetchAllTopicRoute() ([]*TopicRouteData, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	topics, err := instance.GetAllTopicListFromNameServer(10_000)
	if err != nil {
		return nil, err
	}
	var result []*TopicRouteData
	for _, topic := range topics.TopicList {
		if route := instance.GetTopicRouteData(topic); route != nil {
			result = append(result, route)
		}
	}
	return result, nil
}

// ExamineTopicRoute resolves a topic's route, raising when it does not exist.
func (a *DefaultMQAdminExt) ExamineTopicRoute(topic string) (*TopicRouteData, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	route := instance.GetTopicRouteData(topic)
	if route == nil {
		return nil, common.ClientError(fmt.Sprintf("topic %s not exist", topic))
	}
	return route, nil
}

// ExamineTopicConfig sends GET_TOPIC_CONFIG(351) and decodes the TopicConfig
// JSON body.
func (a *DefaultMQAdminExt) ExamineTopicConfig(addr, topic string) (*remoting.TopicConfig, error) {
	response, err := a.invokeBroker(addr, remoting.ReqGetTopicConfig,
		adminExt("topic", topic, "lo", true), nil, 0)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.BrokerError(remoting.RespSystemError,
			fmt.Sprintf("empty topic config for %s", topic))
	}
	return remoting.DecodeTopicConfig(response.Body)
}

// GetAllTopicConfig sends GET_ALL_TOPIC_CONFIG(21), accumulating PAGES until
// topicSeq >= totalTopicNum-1. The first page carries an EMPTY dataVersion;
// later pages carry the JSON of the last version seen, and a changed version
// restarts paging from seq 0. A broker that does not send `totalTopicNum`
// (older versions) returns everything in one round, so the loop ends after the
// first page.
func (a *DefaultMQAdminExt) GetAllTopicConfig(brokerAddr string, timeoutMillis int64) (*remoting.TopicConfigSerializeWrapper, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	timeout := a.timeout(timeoutMillis)
	target := a.brokerVIPAddr(brokerAddr)
	pageSize := a.pageSizeInGetMetadata()
	var currentDataVersion map[string]any
	haveDataVersion := false
	topicSeq := 0
	table := map[string]*remoting.TopicConfig{}
	begin := time.Now()
	firstPage := true

	for {
		left := timeout - time.Since(begin).Milliseconds()
		if left < 0 {
			return nil, common.ClientError("invokeSync call timeout")
		}
		ext := adminExt("topicSeq", topicSeq, "maxTopicNum", pageSize)
		if firstPage {
			// The protocol requires the field on the first page, present and
			// empty; omitting it makes a paging broker treat the request as a
			// fresh enumeration of unknown version.
			ext.Put("dataVersion", "")
			firstPage = false
		} else {
			ext.Put("dataVersion", string(remoting.EncodeJSON(currentDataVersion)))
		}
		response, err := instance.invokeSync(target,
			adminRequest(remoting.ReqGetAllTopicConfig, ext), left)
		if err != nil {
			return nil, err
		}
		if response.Code != remoting.RespSuccess {
			return nil, common.BrokerError(response.Code, response.Remark)
		}
		wrapper, err := remoting.DecodeTopicConfigSerializeWrapper(response.Body)
		if err != nil {
			return nil, err
		}
		for k, v := range wrapper.TopicConfigTable {
			table[k] = v
		}

		totalText, hasTotal := response.ExtFields().Get("totalTopicNum")
		if !hasTotal {
			// Older broker: one round returns everything.
			currentDataVersion = wrapper.DataVersion
			haveDataVersion = true
			break
		}
		total, ok := adminParseInt64(totalText)
		if !ok {
			currentDataVersion = wrapper.DataVersion
			haveDataVersion = true
			break
		}
		newVersion := wrapper.DataVersion
		if haveDataVersion && !sameDataVersion(currentDataVersion, newVersion) {
			common.LogWarnf("topic config dataVersion changed, restart paging")
			currentDataVersion = newVersion
			topicSeq = 0
			table = map[string]*remoting.TopicConfig{}
			continue
		}
		currentDataVersion = newVersion
		haveDataVersion = true

		topicSeq += len(wrapper.TopicConfigTable)
		if topicSeq >= int(total)-1 {
			break
		}
	}

	result := remoting.NewTopicConfigSerializeWrapper()
	result.TopicConfigTable = table
	if haveDataVersion {
		result.DataVersion = currentDataVersion
	}
	return result, nil
}

// GetUserTopicConfig drops system topics and (unless specialTopic) the
// %RETRY% / %DLQ% topics.
func (a *DefaultMQAdminExt) GetUserTopicConfig(brokerAddr string, specialTopic bool, timeoutMillis int64) (*remoting.TopicConfigSerializeWrapper, error) {
	wrapper, err := a.GetAllTopicConfig(brokerAddr, timeoutMillis)
	if err != nil {
		return nil, err
	}
	sysList, err := a.GetSystemTopicListFromBroker(brokerAddr, timeoutMillis)
	if err != nil {
		return nil, err
	}
	sysTopics := map[string]bool{}
	for _, t := range sysList.TopicList {
		sysTopics[t] = true
	}
	kept := map[string]*remoting.TopicConfig{}
	for name, cfg := range wrapper.TopicConfigTable {
		if sysTopics[name] || common.IsSysTopic(name) {
			continue
		}
		if !specialTopic && (common.IsRetryTopic(name) || common.IsDLQTopic(name)) {
			continue
		}
		kept[name] = cfg
	}
	wrapper.TopicConfigTable = kept
	return wrapper, nil
}

// GetSystemTopicListFromBroker sends GET_SYSTEM_TOPIC_LIST_FROM_BROKER(305).
func (a *DefaultMQAdminExt) GetSystemTopicListFromBroker(brokerAddr string, timeoutMillis int64) (*remoting.TopicList, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	return instance.GetSystemTopicListFromBroker(brokerAddr, a.timeout(timeoutMillis))
}

// ExamineTopicStats merges GET_TOPIC_STATS_INFO(202) across the topic's
// brokers.
func (a *DefaultMQAdminExt) ExamineTopicStats(topic string) (*remoting.TopicStatsTable, error) {
	route, err := a.ExamineTopicRoute(topic)
	if err != nil {
		return nil, err
	}
	merged := remoting.NewTopicStatsTable()
	for _, bd := range route.BrokerDatas {
		addr, ok := bd.SelectBrokerAddr()
		if !ok {
			continue
		}
		part, err := a.ExamineTopicStatsByBroker(addr, topic)
		if err != nil {
			common.LogWarnf("getTopicStatsInfo error. topic=%s broker=%s: %v", topic, addr, err)
			continue
		}
		for q, off := range part.OffsetTable {
			merged.OffsetTable[q] = off
		}
		merged.TopicPutTps += part.TopicPutTps
	}
	if len(merged.OffsetTable) == 0 {
		return nil, common.ClientError("Not found the topic stats info")
	}
	return merged, nil
}

// ExamineTopicStatsByBroker sends GET_TOPIC_STATS_INFO(202) to one broker.
func (a *DefaultMQAdminExt) ExamineTopicStatsByBroker(brokerAddr, topic string) (*remoting.TopicStatsTable, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqGetTopicStatsInfo,
		adminExt("topic", topic), nil, 0)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeTopicStatsTable(response.Body)
}

// ---------------- Cluster / broker ----------------

// FetchBrokerClusterInfo sends GET_BROKER_CLUSTER_INFO(106).
func (a *DefaultMQAdminExt) FetchBrokerClusterInfo() (*remoting.ClusterInfo, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	return instance.GetBrokerClusterInfo(10_000)
}

func (a *DefaultMQAdminExt) ExamineBrokerClusterInfo() (*remoting.ClusterInfo, error) {
	return a.FetchBrokerClusterInfo()
}

// FetchBrokerRuntimeStats sends GET_BROKER_RUNTIME_INFO(28) and decodes the
// KVTable body.
func (a *DefaultMQAdminExt) FetchBrokerRuntimeStats(brokerAddr string, timeoutMillis int64) (*remoting.KVTable, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqGetBrokerRuntimeInfo, nil, nil, timeoutMillis)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeKVTable(response.Body)
}

func (a *DefaultMQAdminExt) GetBrokerRuntimeInfo(brokerAddr string, timeoutMillis int64) (*remoting.KVTable, error) {
	return a.FetchBrokerRuntimeStats(brokerAddr, timeoutMillis)
}

// GetBrokerConfig sends GET_BROKER_CONFIG(26). The body is properties TEXT,
// not JSON — parsing it as a KVTable fails on every real broker. This read is
// the one admin RPC that always uses the listening port.
func (a *DefaultMQAdminExt) GetBrokerConfig(brokerAddr string, timeoutMillis int64) (map[string]string, error) {
	response, err := a.invokeBrokerPlain(brokerAddr, remoting.ReqGetBrokerConfig, nil, nil, timeoutMillis)
	if err != nil {
		return nil, err
	}
	return String2Properties(string(response.Body)), nil
}

func (a *DefaultMQAdminExt) UpdateBrokerConfig(brokerAddr string, properties map[string]string, timeoutMillis int64) error {
	if perm, ok := properties["brokerPermission"]; ok && !common.PermIsValidStr(perm) {
		return common.ClientErrorCode(remoting.RespNoPermission,
			fmt.Sprintf("brokerPermission value: %s is invalid.", perm))
	}
	text := propertiesToString(properties)
	if text == "" {
		return nil
	}
	_, err := a.invokeBroker(brokerAddr, remoting.ReqUpdateBrokerConfig, nil, []byte(text), timeoutMillis)
	return err
}

// propertiesToString renders a properties map as `k=v\n` text. Keys are sorted
// so the output is deterministic; the broker reads the text back by name and
// does not care about order.
func propertiesToString(properties map[string]string) string {
	if len(properties) == 0 {
		return ""
	}
	keys := make([]string, 0, len(properties))
	for k := range properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(properties[k])
		sb.WriteByte('\n')
	}
	return sb.String()
}

// WipeWritePermOfBroker sends WIPE_WRITE_PERM_OF_BROKER(205) to one nameserver
// and returns the number of topics it affected.
func (a *DefaultMQAdminExt) WipeWritePermOfBroker(namesrvAddr, brokerName string) (int, error) {
	response, err := a.invokeNameServerAddr(namesrvAddr, remoting.ReqWipeWritePermOfBroker,
		adminExt("brokerName", brokerName), 0)
	if err != nil {
		return 0, err
	}
	return int(extInt64(response, "wipeTopicCount")), nil
}

// AddWritePermOfBroker sends ADD_WRITE_PERM_OF_BROKER(327) and returns the
// number of topics it affected.
func (a *DefaultMQAdminExt) AddWritePermOfBroker(namesrvAddr, brokerName string) (int, error) {
	response, err := a.invokeNameServerAddr(namesrvAddr, remoting.ReqAddWritePermOfBroker,
		adminExt("brokerName", brokerName), 0)
	if err != nil {
		return 0, err
	}
	return int(extInt64(response, "addTopicCount")), nil
}

// CleanUnusedTopic sends CLEAN_UNUSED_TOPIC(316) to every broker of the
// cluster. Returns false when any broker rejected it.
func (a *DefaultMQAdminExt) CleanUnusedTopic(clusterName, topic string) (bool, error) {
	instance, err := a.requireClient()
	if err != nil {
		return false, err
	}
	_ = topic
	addrs, err := instance.BrokerAddrsOfCluster(clusterName)
	if err != nil {
		return false, err
	}
	ok := true
	for _, addr := range addrs {
		if _, err := a.invokeBroker(addr, remoting.ReqCleanUnusedTopic, nil, nil, 0); err != nil {
			common.LogWarnf("cleanUnusedTopic on %s failed: %v", addr, err)
			ok = false
		}
	}
	return ok, nil
}

// ViewBrokerStatsData sends VIEW_BROKER_STATS_DATA(315) and returns the raw
// JSON object.
func (a *DefaultMQAdminExt) ViewBrokerStatsData(brokerAddr, statsName, statsKey string) (map[string]any, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqViewBrokerStatsData,
		adminExt("statsName", statsName, "statsKey", statsKey), nil, 0)
	if err != nil {
		return nil, err
	}
	obj, _ := mustDecodeObject(response.Body)
	return obj, nil
}

// ---------------- Nameserver KV config ----------------

// CreateAndUpdateKVConfig broadcasts PUT_KV_CONFIG(100) to every nameserver.
func (a *DefaultMQAdminExt) CreateAndUpdateKVConfig(namespace, key, value string) error {
	return a.invokeNameServerAll(remoting.ReqPutKVConfig,
		adminExt("namespace", namespace, "key", key, "value", value), nil, 0)
}

func (a *DefaultMQAdminExt) PutKVConfig(namespace, key, value string) error {
	return a.CreateAndUpdateKVConfig(namespace, key, value)
}

// GetKVConfig sends GET_KV_CONFIG(101) to one nameserver.
func (a *DefaultMQAdminExt) GetKVConfig(namespace, key string) (string, bool, error) {
	response, err := a.invokeNameServerOne(remoting.ReqGetKVConfig,
		adminExt("namespace", namespace, "key", key), 0)
	if err != nil {
		return "", false, err
	}
	if response.Code == remoting.RespSuccess {
		v, ok := response.ExtFields().Get("value")
		return v, ok, nil
	}
	return "", false, nil
}

// DeleteKVConfig broadcasts DELETE_KV_CONFIG(102) to every nameserver.
func (a *DefaultMQAdminExt) DeleteKVConfig(namespace, key string) error {
	return a.invokeNameServerAll(remoting.ReqDeleteKVConfig,
		adminExt("namespace", namespace, "key", key), nil, 0)
}

func (a *DefaultMQAdminExt) UpdateNameServerConfig(properties map[string]string, timeoutMillis int64) error {
	text := propertiesToString(properties)
	if text == "" {
		return nil
	}
	return a.invokeNameServerAll(remoting.ReqUpdateNameSrvConfig, nil, []byte(text), timeoutMillis)
}

func (a *DefaultMQAdminExt) GetNameServerConfig(nameServers []string, timeoutMillis int64) (map[string]map[string]string, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	targets := nameServers
	if len(targets) == 0 {
		targets = instance.NameServerAddrs()
	}
	if len(targets) == 0 {
		return nil, common.ClientError("no name server address available")
	}
	out := make(map[string]map[string]string, len(targets))
	for _, ns := range targets {
		response, err := a.invokeNameServerAddr(ns, remoting.ReqGetNameSrvConfig, nil, timeoutMillis)
		if err != nil {
			return nil, err
		}
		out[ns] = String2Properties(string(response.Body))
	}
	return out, nil
}

// GetKVListByNamespace sends GET_KVLIST_BY_NAMESPACE(219).
func (a *DefaultMQAdminExt) GetKVListByNamespace(namespace string) (*remoting.KVTable, error) {
	response, err := a.invokeNameServerOne(remoting.ReqGetKVListByNamespace,
		adminExt("namespace", namespace), 0)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeKVTable(response.Body)
}

// ---------------- Subscription groups ----------------

// CreateAndUpdateSubscriptionGroupConfig sends
// UPDATE_AND_CREATE_SUBSCRIPTIONGROUP(200); the body is a
// SubscriptionGroupConfig JSON object.
func (a *DefaultMQAdminExt) CreateAndUpdateSubscriptionGroupConfig(addr string, config *remoting.SubscriptionGroupConfig) error {
	_, err := a.invokeBroker(addr, remoting.ReqUpdateAndCreateSubscriptionGroup, nil, config.Encode(), 0)
	return err
}

// ExamineSubscriptionGroupConfig reads the group out of the full wrapper.
func (a *DefaultMQAdminExt) ExamineSubscriptionGroupConfig(addr, group string) (*remoting.SubscriptionGroupConfig, error) {
	wrapper, err := a.GetAllSubscriptionGroup(addr, 0)
	if err != nil {
		return nil, err
	}
	return wrapper.SubscriptionGroupTable[group], nil
}

// GetSubscriptionGroupConfig sends GET_SUBSCRIPTIONGROUP_CONFIG(352).
func (a *DefaultMQAdminExt) GetSubscriptionGroupConfig(addr, group string) (*remoting.SubscriptionGroupConfig, error) {
	response, err := a.invokeBroker(addr, remoting.ReqGetSubscriptionGroupConfig,
		adminExt("group", group), nil, 0)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, nil
	}
	return remoting.DecodeSubscriptionGroupConfig(response.Body)
}

// GetAllSubscriptionGroup sends GET_ALL_SUBSCRIPTIONGROUP_CONFIG(201),
// accumulating PAGES until groupSeq >= totalGroupNum-1.
//
// A broker that does not send `totalGroupNum` (older versions) returns
// everything in one round, so the loop ends after the first page.
func (a *DefaultMQAdminExt) GetAllSubscriptionGroup(brokerAddr string, timeoutMillis int64) (*remoting.SubscriptionGroupWrapper, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	timeout := a.timeout(timeoutMillis)
	// Every page goes to the same broker, so the address is resolved once — the
	// paging loop must not re-derive it.
	target := a.brokerVIPAddr(brokerAddr)
	pageSize := a.pageSizeInGetMetadata()
	var currentDataVersion map[string]any
	groupSeq := 0
	table := map[string]*remoting.SubscriptionGroupConfig{}
	forbidden := map[string]any{}
	begin := time.Now()
	firstPage := true

	for {
		left := timeout - time.Since(begin).Milliseconds()
		if left < 0 {
			return nil, common.ClientError("invokeSync call timeout")
		}
		ext := adminExt("groupSeq", groupSeq, "maxGroupNum", pageSize)
		if firstPage {
			// Present and empty on the first page, never omitted.
			ext.Put("dataVersion", "")
			firstPage = false
		} else {
			ext.Put("dataVersion", string(remoting.EncodeJSON(currentDataVersion)))
		}
		response, err := instance.invokeSync(target,
			adminRequest(remoting.ReqGetAllSubscriptionGroupConfig, ext), left)
		if err != nil {
			return nil, err
		}
		if response.Code != remoting.RespSuccess {
			return nil, common.BrokerError(response.Code, response.Remark)
		}
		wrapper, err := remoting.DecodeSubscriptionGroupWrapper(response.Body)
		if err != nil {
			return nil, err
		}
		for k, v := range wrapper.SubscriptionGroupTable {
			table[k] = v
		}
		for k, v := range wrapper.ForbiddenTable {
			forbidden[k] = v
		}
		newVersion := wrapper.DataVersion
		if currentDataVersion == nil {
			currentDataVersion = newVersion
		}
		groupSeq += len(wrapper.SubscriptionGroupTable)

		totalText, hasTotal := response.ExtFields().Get("totalGroupNum")
		if !hasTotal {
			// Older broker: one round returns everything.
			break
		}
		total, ok := adminParseInt64(totalText)
		if !ok {
			break
		}
		if !sameDataVersion(currentDataVersion, newVersion) {
			common.LogWarnf("subscription group dataVersion changed, restart paging")
			currentDataVersion = newVersion
			groupSeq = 0
			table = map[string]*remoting.SubscriptionGroupConfig{}
			forbidden = map[string]any{}
			continue
		}
		if groupSeq >= int(total)-1 {
			break
		}
	}

	result := remoting.NewSubscriptionGroupWrapper()
	result.SubscriptionGroupTable = table
	result.ForbiddenTable = forbidden
	if currentDataVersion != nil {
		result.DataVersion = currentDataVersion
	}
	return result, nil
}

// GetUserSubscriptionGroup drops system and predefined groups.
func (a *DefaultMQAdminExt) GetUserSubscriptionGroup(brokerAddr string, timeoutMillis int64) (*remoting.SubscriptionGroupWrapper, error) {
	wrapper, err := a.GetAllSubscriptionGroup(brokerAddr, timeoutMillis)
	if err != nil {
		return nil, err
	}
	kept := map[string]*remoting.SubscriptionGroupConfig{}
	for k, v := range wrapper.SubscriptionGroupTable {
		if common.IsSysConsumerGroup(k) || remoting.IsPredefinedGroup(k) {
			continue
		}
		kept[k] = v
	}
	wrapper.SubscriptionGroupTable = kept
	return wrapper, nil
}

// DeleteSubscriptionGroup sends DELETE_SUBSCRIPTIONGROUP(207).
func (a *DefaultMQAdminExt) DeleteSubscriptionGroup(addr, groupName string, removeOffset bool) error {
	_, err := a.invokeBroker(addr, remoting.ReqDeleteSubscriptionGroup,
		adminExt("groupName", groupName, "cleanOffset", removeOffset), nil, 0)
	return err
}

// ---------------- Consumer / producer connections ----------------

// ExamineConsumerConnectionInfo sends GET_CONSUMER_CONNECTION_LIST(203).
func (a *DefaultMQAdminExt) ExamineConsumerConnectionInfo(consumerGroup string, brokerAddr string) (*remoting.ConsumerConnection, error) {
	addr := brokerAddr
	if addr == "" {
		var err error
		if addr, err = a.firstBrokerAddr(); err != nil {
			return nil, err
		}
	}
	response, err := a.invokeBroker(addr, remoting.ReqGetConsumerConnectionList,
		adminExt("consumerGroup", consumerGroup), nil, 0)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.ClientError(fmt.Sprintf("consumer group %s not online", consumerGroup))
	}
	return remoting.DecodeConsumerConnection(response.Body)
}

func (a *DefaultMQAdminExt) ExamineConsumerConnection(consumerGroup string, brokerAddr string) (*remoting.ConsumerConnection, error) {
	return a.ExamineConsumerConnectionInfo(consumerGroup, brokerAddr)
}

// ExamineProducerConnectionInfo sends GET_PRODUCER_CONNECTION_LIST(204).
func (a *DefaultMQAdminExt) ExamineProducerConnectionInfo(producerGroup string, brokerAddr string) (*remoting.ProducerConnection, error) {
	addr := brokerAddr
	if addr == "" {
		var err error
		if addr, err = a.firstBrokerAddr(); err != nil {
			return nil, err
		}
	}
	response, err := a.invokeBroker(addr, remoting.ReqGetProducerConnectionList,
		adminExt("producerGroup", producerGroup), nil, 0)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeProducerConnection(response.Body)
}

// ExamineConsumerRunningInfo sends GET_CONSUMER_RUNNING_INFO(307) to a broker,
// which relays it to the named client.
func (a *DefaultMQAdminExt) ExamineConsumerRunningInfo(consumerGroup, clientID string, jstack bool, brokerAddr string) (*remoting.ConsumerRunningInfo, error) {
	addr := brokerAddr
	if addr == "" {
		var err error
		if addr, err = a.firstBrokerAddr(); err != nil {
			return nil, err
		}
	}
	response, err := a.invokeBroker(addr, remoting.ReqGetConsumerRunningInfo,
		adminExt("consumerGroup", consumerGroup, "clientId", clientID, "jstackEnable", jstack), nil, 0)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.ClientError(fmt.Sprintf("no running info for client %s", clientID))
	}
	return remoting.DecodeConsumerRunningInfo(response.Body)
}

func (a *DefaultMQAdminExt) GetConsumerRunningInfo(consumerGroup, clientID string, jstack bool, brokerAddr string) (*remoting.ConsumerRunningInfo, error) {
	return a.ExamineConsumerRunningInfo(consumerGroup, clientID, jstack, brokerAddr)
}

// GetConsumerListByGroup sends GET_CONSUMER_LIST_BY_GROUP(38).
func (a *DefaultMQAdminExt) GetConsumerListByGroup(consumerGroup string, brokerAddr string) ([]string, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	addr := brokerAddr
	if addr == "" {
		if addr, err = a.firstBrokerAddr(); err != nil {
			return nil, err
		}
	}
	return instance.GetConsumerListByGroup(addr, consumerGroup, a.timeout(0))
}

// ---------------- Consume stats ----------------

// ExamineConsumeStats sends GET_CONSUME_STATS(208) to one broker.
func (a *DefaultMQAdminExt) ExamineConsumeStats(brokerAddr, consumerGroup, topic string, topicList []string) (*remoting.ConsumeStats, error) {
	ext := adminExt("consumerGroup", consumerGroup)
	if topic != "" {
		ext.Put("topic", topic)
	}
	if len(topicList) > 0 {
		ext.Put("topicList", strings.Join(topicList, ";"))
	}
	response, err := a.invokeBroker(brokerAddr, remoting.ReqGetConsumeStats, ext, nil, 0)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeConsumeStats(response.Body)
}

// FetchConsumeStatsInBroker sends GET_BROKER_CONSUME_STATS(317).
func (a *DefaultMQAdminExt) FetchConsumeStatsInBroker(brokerAddr string, isOrder bool, timeoutMillis int64) (*remoting.ConsumeStatsList, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqGetBrokerConsumeStats,
		adminExt("isOrder", isOrder), nil, timeoutMillis)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeConsumeStatsList(response.Body)
}

// QueryTopicConsumeByWho sends QUERY_TOPIC_CONSUME_BY_WHO(300).
func (a *DefaultMQAdminExt) QueryTopicConsumeByWho(brokerAddr, topic string) (map[string]bool, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqQueryTopicConsumeByWho,
		adminExt("topic", topic), nil, 0)
	if err != nil {
		return nil, err
	}
	groups := map[string]bool{}
	if len(response.Body) == 0 {
		return groups, nil
	}
	if obj, ok := mustDecodeObject(response.Body); ok {
		for _, item := range jsonArraySafe(obj["groupList"]) {
			if s, ok := item.(string); ok {
				groups[s] = true
			}
		}
	}
	return groups, nil
}

func (a *DefaultMQAdminExt) ExamineConsumeStatsGroup(consumerGroup, topic string) (*remoting.ConsumeStats, error) {
	route, err := a.ExamineTopicRoute(common.GetRetryTopic(consumerGroup))
	if err != nil {
		return nil, err
	}
	result := remoting.NewConsumeStats()
	for _, bd := range route.BrokerDatas {
		addr, ok := bd.SelectBrokerAddr()
		if !ok {
			continue
		}
		part, err := a.ExamineConsumeStats(addr, consumerGroup, topic, nil)
		if err != nil {
			return nil, err
		}
		for q, w := range part.OffsetTable {
			result.OffsetTable[q] = w
		}
		result.ConsumeTps += part.ConsumeTps
	}
	if len(result.OffsetTable) == 0 {
		return nil, common.ClientError(fmt.Sprintf("no consume stats for group %s", consumerGroup))
	}
	return result, nil
}

// QueryTopicsByConsumerToBroker sends QUERY_TOPICS_BY_CONSUMER(343) to one
// broker.
//
// The broker answers out of its OFFSET table (keys of the form
// `topic@group`), so a group that has never committed an offset legitimately
// gets an empty list — that is expected, not a bug.
func (a *DefaultMQAdminExt) QueryTopicsByConsumerToBroker(brokerAddr, group string) (*remoting.TopicList, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqQueryTopicsByConsumer,
		adminExt("group", group), nil, 0)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeTopicList(response.Body)
}

func (a *DefaultMQAdminExt) QueryTopicsByConsumer(group string) (*remoting.TopicList, error) {
	route, err := a.ExamineTopicRoute(common.GetRetryTopic(group))
	if err != nil {
		return nil, err
	}
	result := remoting.NewTopicList()
	seen := map[string]bool{}
	for _, bd := range route.BrokerDatas {
		addr, ok := bd.SelectBrokerAddr()
		if !ok {
			continue
		}
		part, err := a.QueryTopicsByConsumerToBroker(addr, group)
		if err != nil {
			return nil, err
		}
		for _, topic := range part.TopicList {
			if !seen[topic] {
				seen[topic] = true
				result.TopicList = append(result.TopicList, topic)
			}
		}
	}
	return result, nil
}

// QuerySubscription sends QUERY_SUBSCRIPTION_BY_CONSUMER(345).
func (a *DefaultMQAdminExt) QuerySubscription(brokerAddr, group, topic string) (map[string]any, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqQuerySubscriptionByConsumer,
		adminExt("group", group, "topic", topic), nil, 0)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, nil
	}
	obj, _ := mustDecodeObject(response.Body)
	return obj, nil
}

// GetConsumeStatus sends INVOKE_BROKER_TO_GET_CONSUMER_STATUS(223).
func (a *DefaultMQAdminExt) GetConsumeStatus(brokerAddr, topic, group, clientAddr string) (map[string]map[string]any, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqInvokeBrokerToGetConsumerStatus,
		adminExt("topic", topic, "group", group, "clientAddr", clientAddr), nil, 0)
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]any{}
	if len(response.Body) == 0 {
		return out, nil
	}
	obj, _ := mustDecodeObject(response.Body)
	if table, ok := obj["consumerTable"].(map[string]any); ok {
		for k, v := range table {
			if row, ok := v.(map[string]any); ok {
				out[k] = row
			}
		}
	}
	return out, nil
}

// CloneGroupOffset sends CLONE_GROUP_OFFSET(314).
func (a *DefaultMQAdminExt) CloneGroupOffset(brokerAddr, srcGroup, destGroup, topic string, isOffline bool) error {
	_, err := a.invokeBroker(brokerAddr, remoting.ReqCloneGroupOffset, adminExt(
		"srcGroup", srcGroup,
		"destGroup", destGroup,
		"topic", topic,
		"offline", isOffline,
	), nil, 0)
	return err
}

// ---------------- consume time span ----------------

// QueryConsumeTimeSpanFromBroker sends QUERY_CONSUME_TIME_SPAN(303) to one
// broker and returns that broker's consumeTimeSpanSet.
//
// The header carries exactly `topic` + `group`. The body is a
// QueryConsumeTimeSpanBody — a JSON object holding the window per queue the
// broker owns for the topic; an empty body is a legitimate answer (a broker
// that owns no queue of the topic has no window to report) and yields no
// entries rather than an error.
func (a *DefaultMQAdminExt) QueryConsumeTimeSpanFromBroker(brokerAddr, topic, group string) ([]*remoting.QueueTimeSpan, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqQueryConsumeTimeSpan,
		adminExt("topic", topic, "group", group), nil, 0)
	if err != nil {
		return nil, err
	}
	body, err := remoting.DecodeQueryConsumeTimeSpanBody(response.Body)
	if err != nil {
		return nil, err
	}
	return body.ConsumeTimeSpanSet, nil
}

// QueryConsumeTimeSpan walks the topic route and concatenates every master's
// consume window for the group, in route order. A broker group without a
// master address is skipped, and the first broker error aborts the walk
// instead of returning a partial set for the brokers that did answer.
func (a *DefaultMQAdminExt) QueryConsumeTimeSpan(topic, group string) ([]*remoting.QueueTimeSpan, error) {
	route, err := a.ExamineTopicRoute(topic)
	if err != nil {
		return nil, err
	}
	var spans []*remoting.QueueTimeSpan
	for _, bd := range route.BrokerDatas {
		addr, ok := bd.SelectBrokerAddr()
		if !ok {
			continue
		}
		part, err := a.QueryConsumeTimeSpanFromBroker(addr, topic, group)
		if err != nil {
			return nil, err
		}
		spans = append(spans, part...)
	}
	return spans, nil
}
