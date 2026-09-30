// Shared admin response bodies (Java org.apache.rocketmq.remoting.protocol.body.*):
// KVTable, TopicList, ClusterInfo, Connection/ConsumerConnection/ProducerConnection,
// ConsumerRunningInfo and ConsumeStatsList.
//
// Two of these carry a fastjson2 inline-object key and therefore CANNOT be read
// by a strict JSON parser:
//   - ConsumerRunningInfo.mqTable / mqPopTable  (MessageQueue keys)
//
// Both reuse DecodeMapKey / messageQueueKeyJSON.
package remoting

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ---------------- KVTable ----------------

// KVTable mirrors body.KVTable (the `table` field holds the whole map).
type KVTable struct {
	Table map[string]string
}

// NewKVTable builds an empty KVTable.
func NewKVTable() *KVTable { return &KVTable{Table: map[string]string{}} }

func (k *KVTable) ToJSONValue() map[string]any {
	table := make(map[string]any, len(k.Table))
	for key, v := range k.Table {
		table[key] = v
	}
	return map[string]any{"table": table}
}

func (k *KVTable) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("KVTable: expected object")
	}
	k.Table = map[string]string{}
	if raw, ok := obj["table"].(map[string]any); ok {
		for key := range raw {
			k.Table[key] = jsonStringOr(raw, key, "")
		}
	}
	return nil
}

func (k *KVTable) Encode() []byte { return EncodeJSON(k.ToJSONValue()) }

// DecodeKVTable parses a KVTable body.
func DecodeKVTable(data []byte) (*KVTable, error) {
	k := NewKVTable()
	if len(data) == 0 {
		return k, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := k.FromJSONValue(value); err != nil {
		return nil, err
	}
	return k, nil
}

// ---------------- TopicList ----------------

// TopicList mirrors body.TopicList. `brokerAddr` is a String field with
// NON_NULL serialisation, so it disappears from the wire when empty.
type TopicList struct {
	TopicList  []string
	BrokerAddr string
	HasBroker  bool
}

// NewTopicList builds an empty TopicList.
func NewTopicList() *TopicList { return &TopicList{TopicList: []string{}} }

func (t *TopicList) ToJSONValue() map[string]any {
	d := map[string]any{"topicList": t.TopicList}
	if t.HasBroker {
		d["brokerAddr"] = t.BrokerAddr
	}
	return d
}

func (t *TopicList) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("TopicList: expected object")
	}
	// The topic list is a Set<String> on the Java side; keep declaration order
	// but drop duplicates so a merged multi-broker list matches Java's Set.
	seen := map[string]bool{}
	t.TopicList = []string{}
	for _, item := range jsonArray(obj, "topicList") {
		if s, ok := item.(string); ok && !seen[s] {
			seen[s] = true
			t.TopicList = append(t.TopicList, s)
		}
	}
	if addr, ok := obj["brokerAddr"].(string); ok {
		t.BrokerAddr = addr
		t.HasBroker = true
	}
	return nil
}

func (t *TopicList) Encode() []byte { return EncodeJSON(t.ToJSONValue()) }

// DecodeTopicList parses a TopicList body.
func DecodeTopicList(data []byte) (*TopicList, error) {
	t := NewTopicList()
	if len(data) == 0 {
		return t, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := t.FromJSONValue(value); err != nil {
		return nil, err
	}
	return t, nil
}

// ---------------- ClusterInfo ----------------

// BrokerAddrTableEntry is the per-broker row of ClusterInfo: the
// {brokerId: addr} map plus the cluster it belongs to.
type BrokerAddrTableEntry struct {
	Cluster            string
	BrokerName         string
	BrokerAddrs        map[int64]string
	EnableActingMaster bool
}

// ClusterInfo mirrors body.ClusterInfo.
//
// On the wire `brokerAddrTable[name]` is a BrokerData object and the real
// {brokerId: addr} map sits under its `brokerAddrs` field.
type ClusterInfo struct {
	BrokerAddrTable  map[string]*BrokerAddrTableEntry
	ClusterAddrTable map[string][]string
}

// NewClusterInfo builds an empty ClusterInfo.
func NewClusterInfo() *ClusterInfo {
	return &ClusterInfo{
		BrokerAddrTable:  map[string]*BrokerAddrTableEntry{},
		ClusterAddrTable: map[string][]string{},
	}
}

// BrokerAddrs flattens every broker address, de-duplicated, ordered by broker
// name then brokerId — the same order Java's DefaultMQAdminExt ends up with.
func (c *ClusterInfo) BrokerAddrs() []string {
	names := sortedKeys(c.BrokerAddrTable)
	var addrs []string
	seen := map[string]bool{}
	for _, name := range names {
		entry := c.BrokerAddrTable[name]
		for _, id := range sortedI64Keys(entry.BrokerAddrs) {
			addr := entry.BrokerAddrs[id]
			if addr != "" && !seen[addr] {
				seen[addr] = true
				addrs = append(addrs, addr)
			}
		}
	}
	return addrs
}

func (c *ClusterInfo) ToJSONValue() map[string]any {
	brokerTable := make(map[string]any, len(c.BrokerAddrTable))
	for name, entry := range c.BrokerAddrTable {
		addrs := make(map[string]any, len(entry.BrokerAddrs))
		for id, addr := range entry.BrokerAddrs {
			addrs[fmt.Sprintf("%d", id)] = addr
		}
		brokerTable[name] = map[string]any{
			"cluster":            entry.Cluster,
			"brokerName":         name,
			"brokerAddrs":        addrs,
			"enableActingMaster": entry.EnableActingMaster,
		}
	}
	clusterTable := make(map[string]any, len(c.ClusterAddrTable))
	for name, names := range c.ClusterAddrTable {
		clusterTable[name] = names
	}
	return map[string]any{
		"brokerAddrTable":  brokerTable,
		"clusterAddrTable": clusterTable,
	}
}

func (c *ClusterInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ClusterInfo: expected object")
	}
	c.BrokerAddrTable = map[string]*BrokerAddrTableEntry{}
	if raw, ok := obj["brokerAddrTable"].(map[string]any); ok {
		for name := range raw {
			row, ok := raw[name].(map[string]any)
			if !ok {
				continue
			}
			entry := &BrokerAddrTableEntry{
				BrokerName:  jsonStringOr(row, "brokerName", name),
				Cluster:     jsonStringOr(row, "cluster", ""),
				BrokerAddrs: map[int64]string{},
			}
			entry.EnableActingMaster = jsonBool(row, "enableActingMaster", false)
			if addrs, ok := row["brokerAddrs"].(map[string]any); ok {
				for id := range addrs {
					key, err := strconvParseInt(id)
					if err != nil {
						continue
					}
					entry.BrokerAddrs[key] = jsonStringOr(addrs, id, "")
				}
			}
			c.BrokerAddrTable[name] = entry
		}
	}
	c.ClusterAddrTable = map[string][]string{}
	if raw, ok := obj["clusterAddrTable"].(map[string]any); ok {
		for name := range raw {
			c.ClusterAddrTable[name] = jsonStringListOr(raw, name)
		}
	}
	return nil
}

func (c *ClusterInfo) Encode() []byte { return EncodeJSON(c.ToJSONValue()) }

// DecodeClusterInfo parses the GET_BROKER_CLUSTER_INFO(106) body.
func DecodeClusterInfo(data []byte) (*ClusterInfo, error) {
	c := NewClusterInfo()
	if len(data) == 0 {
		return c, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := c.FromJSONValue(value); err != nil {
		return nil, err
	}
	return c, nil
}

// ---------------- Connection / ConsumerConnection / ProducerConnection ----------------

// Connection mirrors body.Connection.
type Connection struct {
	ClientID   string
	ClientAddr string
	Language   string
	Version    int32
}

func (c *Connection) ToJSONValue() map[string]any {
	return map[string]any{
		"clientId":   c.ClientID,
		"clientAddr": c.ClientAddr,
		"language":   c.Language,
		"version":    c.Version,
	}
}

func (c *Connection) FromJSONValue(value any) error {
	c.ClientID = jsonStringOr(value, "clientId", "")
	c.ClientAddr = jsonStringOr(value, "clientAddr", "")
	c.Language = jsonStringOr(value, "language", "")
	c.Version = jsonI32(value, "version", 0)
	return nil
}

// ConsumerConnection mirrors body.ConsumerConnection.
type ConsumerConnection struct {
	ConnectionSet     []*Connection
	SubscriptionTable map[string]map[string]any
	ConsumeType       string
	MessageModel      string
	ConsumeFromWhere  string
}

// NewConsumerConnection builds an empty ConsumerConnection.
func NewConsumerConnection() *ConsumerConnection {
	return &ConsumerConnection{
		ConnectionSet:     []*Connection{},
		SubscriptionTable: map[string]map[string]any{},
	}
}

func (c *ConsumerConnection) ToJSONValue() map[string]any {
	conns := make([]any, 0, len(c.ConnectionSet))
	for _, conn := range c.ConnectionSet {
		conns = append(conns, conn.ToJSONValue())
	}
	subs := make(map[string]any, len(c.SubscriptionTable))
	for topic, sub := range c.SubscriptionTable {
		subs[topic] = sub
	}
	return map[string]any{
		"connectionSet":     conns,
		"subscriptionTable": subs,
		"consumeType":       c.ConsumeType,
		"messageModel":      c.MessageModel,
		"consumeFromWhere":  c.ConsumeFromWhere,
	}
}

func (c *ConsumerConnection) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ConsumerConnection: expected object")
	}
	c.ConnectionSet = []*Connection{}
	for _, item := range jsonArray(obj, "connectionSet") {
		conn := &Connection{}
		if err := conn.FromJSONValue(item); err != nil {
			return err
		}
		c.ConnectionSet = append(c.ConnectionSet, conn)
	}
	c.SubscriptionTable = map[string]map[string]any{}
	if raw, ok := obj["subscriptionTable"].(map[string]any); ok {
		for topic := range raw {
			if sub, ok := raw[topic].(map[string]any); ok {
				c.SubscriptionTable[topic] = sub
			}
		}
	}
	c.ConsumeType = jsonStringOr(obj, "consumeType", "")
	c.MessageModel = jsonStringOr(obj, "messageModel", "")
	c.ConsumeFromWhere = jsonStringOr(obj, "consumeFromWhere", "")
	return nil
}

func (c *ConsumerConnection) Encode() []byte { return EncodeJSON(c.ToJSONValue()) }

// DecodeConsumerConnection parses the GET_CONSUMER_CONNECTION_LIST(203) body.
func DecodeConsumerConnection(data []byte) (*ConsumerConnection, error) {
	c := NewConsumerConnection()
	if len(data) == 0 {
		return c, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := c.FromJSONValue(value); err != nil {
		return nil, err
	}
	return c, nil
}

// ProducerConnection mirrors body.ProducerConnection.
type ProducerConnection struct {
	ConnectionSet []*Connection
}

// NewProducerConnection builds an empty ProducerConnection.
func NewProducerConnection() *ProducerConnection {
	return &ProducerConnection{ConnectionSet: []*Connection{}}
}

func (p *ProducerConnection) ToJSONValue() map[string]any {
	conns := make([]any, 0, len(p.ConnectionSet))
	for _, conn := range p.ConnectionSet {
		conns = append(conns, conn.ToJSONValue())
	}
	return map[string]any{"connectionSet": conns}
}

func (p *ProducerConnection) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ProducerConnection: expected object")
	}
	p.ConnectionSet = []*Connection{}
	for _, item := range jsonArray(obj, "connectionSet") {
		conn := &Connection{}
		if err := conn.FromJSONValue(item); err != nil {
			return err
		}
		p.ConnectionSet = append(p.ConnectionSet, conn)
	}
	return nil
}

func (p *ProducerConnection) Encode() []byte { return EncodeJSON(p.ToJSONValue()) }

// DecodeProducerConnection parses the GET_PRODUCER_CONNECTION_LIST(204) body.
func DecodeProducerConnection(data []byte) (*ProducerConnection, error) {
	p := NewProducerConnection()
	if len(data) == 0 {
		return p, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := p.FromJSONValue(value); err != nil {
		return nil, err
	}
	return p, nil
}

// ---------------- ConsumerRunningInfo ----------------

// ConsumerRunningInfo property keys (Java ConsumerRunningInfo constants).
//
// Note PROP_CONSUME_ORDERLY: this is the ONE key where the Java constant NAME
// and its VALUE disagree. The field name carries the underscores and the value
// does not — `PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY"`. Verified by JVM
// reflection on 5.5.1 (`RemotingSerializable`-adjacent probe):
//
//	PROP_CONSUME_ORDERLY field name  = PROP_CONSUME_ORDERLY
//	PROP_CONSUME_ORDERLY field value = PROP_CONSUMEORDERLY
//
// The broker/admin look the property up BY VALUE, so shipping the underscored
// form makes the "is this consumer orderly" flag silently disappear from every
// 307 answer. Python / Rust / C++ / .NET / Node all use the underscore-free
// value; only Java's identifier keeps the underscores.
const (
	PropNameServerAddr         = "PROP_NAMESERVER_ADDR"
	PropThreadPoolCoreSize     = "PROP_THREADPOOL_CORE_SIZE"
	PropConsumeOrderly         = "PROP_CONSUMEORDERLY"
	PropConsumeType            = "PROP_CONSUME_TYPE"
	PropClientVersion          = "PROP_CLIENT_VERSION"
	PropConsumerStartTimestamp = "PROP_CONSUMER_START_TIMESTAMP"
)

// ConsumerRunningInfo mirrors body.ConsumerRunningInfo. It is used on both
// sides: the admin parses the broker's aggregate, and a client answers
// GET_CONSUMER_RUNNING_INFO(307) by encoding its own.
//
// mqTable / mqPopTable are MessageQueue-keyed and travel as inline object keys.
type ConsumerRunningInfo struct {
	Properties       map[string]string
	SubscriptionSet  []map[string]any
	MQTable          map[common.MessageQueue]map[string]any
	MQPopTable       map[common.MessageQueue]map[string]any
	StatusTable      map[string]map[string]any
	UserConsumerInfo map[string]string
	Jstack           string
	HasJstack        bool
}

// NewConsumerRunningInfo builds an empty ConsumerRunningInfo.
func NewConsumerRunningInfo() *ConsumerRunningInfo {
	return &ConsumerRunningInfo{
		Properties:       map[string]string{},
		SubscriptionSet:  []map[string]any{},
		MQTable:          map[common.MessageQueue]map[string]any{},
		MQPopTable:       map[common.MessageQueue]map[string]any{},
		StatusTable:      map[string]map[string]any{},
		UserConsumerInfo: map[string]string{},
	}
}

func (r *ConsumerRunningInfo) ToJSONValue() map[string]any {
	subs := make([]any, 0, len(r.SubscriptionSet))
	for _, sub := range r.SubscriptionSet {
		subs = append(subs, sub)
	}
	mqTable := newMQKeyedJSON(r.MQTable, func(info map[string]any) any { return info })
	mqPopTable := newMQKeyedJSON(r.MQPopTable, func(info map[string]any) any { return info })
	statusTable := make(map[string]any, len(r.StatusTable))
	for k, v := range r.StatusTable {
		statusTable[k] = v
	}
	props := make(map[string]any, len(r.Properties))
	for k, v := range r.Properties {
		props[k] = v
	}
	userInfo := make(map[string]any, len(r.UserConsumerInfo))
	for k, v := range r.UserConsumerInfo {
		userInfo[k] = v
	}
	d := map[string]any{
		"properties":       props,
		"subscriptionSet":  subs,
		"mqTable":          mqTable,
		"mqPopTable":       mqPopTable,
		"statusTable":      statusTable,
		"userConsumerInfo": userInfo,
	}
	// Java's `jstack` is a null String unless someone called setJstack(), and
	// fastjson2 drops null fields — so an un-set jstack is an ABSENT key, not
	// an empty string. The empty body therefore has exactly six keys, and
	// jstack is APPENDED last when present. Emitting "jstack":"" instead would
	// make a strict admin see a stack trace of zero length.
	if r.HasJstack {
		d["jstack"] = r.Jstack
	}
	return d
}

func (r *ConsumerRunningInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ConsumerRunningInfo: expected object")
	}
	r.Properties = jsonStringMapOr(obj, "properties")
	r.SubscriptionSet = []map[string]any{}
	for _, item := range jsonArray(obj, "subscriptionSet") {
		if sub, ok := item.(map[string]any); ok {
			r.SubscriptionSet = append(r.SubscriptionSet, sub)
		}
	}
	r.MQTable = decodeMQInfoMap(obj["mqTable"])
	r.MQPopTable = decodeMQInfoMap(obj["mqPopTable"])
	r.StatusTable = map[string]map[string]any{}
	if raw, ok := obj["statusTable"].(map[string]any); ok {
		for k := range raw {
			if v, ok := raw[k].(map[string]any); ok {
				r.StatusTable[k] = v
			}
		}
	}
	r.UserConsumerInfo = jsonStringMapOr(obj, "userConsumerInfo")
	if js, ok := obj["jstack"].(string); ok {
		r.Jstack = js
		r.HasJstack = true
	}
	return nil
}

// Encode renders the running info with fastjson2's inline-object mqTable keys.
// This MUST NOT go through EncodeJSON: encoding/json validates Marshaler output
// and rejects the unquoted key (see mqKeyedJSON).
func (r *ConsumerRunningInfo) Encode() []byte { return EncodeFastJSON(r.ToJSONValue()) }

// DecodeConsumerRunningInfo parses a ConsumerRunningInfo body.
func DecodeConsumerRunningInfo(data []byte) (*ConsumerRunningInfo, error) {
	r := NewConsumerRunningInfo()
	if len(data) == 0 {
		return r, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := r.FromJSONValue(value); err != nil {
		return nil, err
	}
	return r, nil
}

// decodeMQInfoMap reads a MessageQueue-keyed map of objects.
func decodeMQInfoMap(value any) map[common.MessageQueue]map[string]any {
	out := map[common.MessageQueue]map[string]any{}
	obj, ok := value.(map[string]any)
	if !ok {
		return out
	}
	for k := range obj {
		inner, ok := DecodeMapKey(k)
		if !ok {
			continue
		}
		m, ok := inner.(map[string]any)
		if !ok {
			continue
		}
		q := common.NewMessageQueue(
			jsonStringOr(m, "topic", ""),
			jsonStringOr(m, "brokerName", ""),
			jsonI32(m, "queueId", 0),
		)
		info, _ := obj[k].(map[string]any)
		out[q] = info
	}
	return out
}

// ---------------- ConsumeStatsList ----------------

// ConsumeStatsList mirrors body.ConsumeStatsList (GET_BROKER_CONSUME_STATS=317).
//
// The JSON key is the Java FIELD name `consumeStatsList` — a wrong key name
// silently parses to an empty list, which looks like "this broker has no
// backlog". `brokerAddr` is a String with NON_NULL serialisation; `totalDiff`
// and `totalInflightDiff` are primitive longs and are always present.
type ConsumeStatsList struct {
	StatsList         []*ConsumeStats
	BrokerAddr        string
	HasBroker         bool
	TotalDiff         int64
	TotalInflightDiff int64
}

// NewConsumeStatsList builds an empty ConsumeStatsList.
func NewConsumeStatsList() *ConsumeStatsList {
	return &ConsumeStatsList{StatsList: []*ConsumeStats{}}
}

func (l *ConsumeStatsList) ToJSONValue() map[string]any {
	stats := make([]any, 0, len(l.StatsList))
	for _, s := range l.StatsList {
		stats = append(stats, s.ToJSONValue())
	}
	d := map[string]any{"consumeStatsList": stats}
	if l.HasBroker {
		d["brokerAddr"] = l.BrokerAddr
	}
	d["totalDiff"] = l.TotalDiff
	d["totalInflightDiff"] = l.TotalInflightDiff
	return d
}

func (l *ConsumeStatsList) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ConsumeStatsList: expected object")
	}
	l.StatsList = []*ConsumeStats{}
	for _, item := range jsonArray(obj, "consumeStatsList") {
		s := NewConsumeStats()
		if err := s.FromJSONValue(item); err != nil {
			return err
		}
		l.StatsList = append(l.StatsList, s)
	}
	if addr, ok := obj["brokerAddr"].(string); ok {
		l.BrokerAddr = addr
		l.HasBroker = true
	}
	l.TotalDiff = jsonI64(obj, "totalDiff", 0)
	l.TotalInflightDiff = jsonI64(obj, "totalInflightDiff", 0)
	return nil
}

func (l *ConsumeStatsList) Encode() []byte { return EncodeJSON(l.ToJSONValue()) }

// DecodeConsumeStatsList parses the GET_BROKER_CONSUME_STATS(317) body.
func DecodeConsumeStatsList(data []byte) (*ConsumeStatsList, error) {
	l := NewConsumeStatsList()
	if len(data) == 0 {
		return l, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := l.FromJSONValue(value); err != nil {
		return nil, err
	}
	return l, nil
}

// ---------------- small sort/parse helpers ----------------

func sortedKeys(m map[string]*BrokerAddrTableEntry) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedI64Keys(m map[int64]string) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// strconvParseInt parses a brokerId key ("0", "-1", ...).
func strconvParseInt(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
