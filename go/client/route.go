// Package client holds the shared client instance (Java MQClientInstance):
// the topic route cache, publish-info bookkeeping, the heartbeat/persist
// scheduled tasks and the broker->client request handlers. Producers and
// consumers (packages to come) attach to one instance per clientId.
package client

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- QueueData

// QueueData is one queue partition of a topic route
// (org.apache.rocketmq.remoting.protocol.route.QueueData).
type QueueData struct {
	BrokerName     string
	ReadQueueNums  int32
	WriteQueueNums int32
	Perm           int32
	TopicSysFlag   int32
}

func (q *QueueData) ToJSONValue() map[string]any {
	return map[string]any{
		"brokerName":     q.BrokerName,
		"readQueueNums":  q.ReadQueueNums,
		"writeQueueNums": q.WriteQueueNums,
		"perm":           q.Perm,
		"topicSysFlag":   q.TopicSysFlag,
	}
}

func (q *QueueData) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("QueueData: expected object")
	}
	q.BrokerName = jsonString(obj, "brokerName", "")
	q.ReadQueueNums = jsonI32Field(obj, "readQueueNums", 0)
	q.WriteQueueNums = jsonI32Field(obj, "writeQueueNums", 0)
	q.Perm = jsonI32Field(obj, "perm", 0)
	q.TopicSysFlag = jsonI32Field(obj, "topicSysFlag", 0)
	return nil
}

// ---------------------------------------------------------------- BrokerData

// BrokerAddrPair is one brokerId -> address mapping. The pair list (not a
// map) preserves the wire order: "pick a random slave" iterates candidates in
// broker order, and the encoded bytes stay stable.
type BrokerAddrPair struct {
	ID   int64
	Addr string
}

// BrokerData is one broker group of a topic route
// (org.apache.rocketmq.remoting.protocol.route.BrokerData).
type BrokerData struct {
	Cluster            string
	BrokerName         string
	BrokerAddrs        []BrokerAddrPair
	ZoneName           string
	EnableActingMaster bool
}

// SelectBrokerAddr mirrors Java BrokerData.selectBrokerAddr: the master
// (brokerId 0) when present, otherwise a pseudo-random slave. An empty string
// address is returned as-is, matching Java (no validation).
func (b *BrokerData) SelectBrokerAddr() (string, bool) {
	for _, pair := range b.BrokerAddrs {
		if pair.ID == int64(common.MasterID) {
			return pair.Addr, true
		}
	}
	if len(b.BrokerAddrs) == 0 {
		return "", false
	}
	return b.BrokerAddrs[pseudoRandomIndex(len(b.BrokerAddrs))].Addr, true
}

// Equal matches Java BrokerData.equals: cluster / brokerName / brokerAddrs
// only. zoneName and enableActingMaster are excluded — route-change detection
// must not fire just because the broker moved zones.
func (b *BrokerData) Equal(other *BrokerData) bool {
	if b.Cluster != other.Cluster || b.BrokerName != other.BrokerName ||
		len(b.BrokerAddrs) != len(other.BrokerAddrs) {
		return false
	}
	for i := range b.BrokerAddrs {
		if b.BrokerAddrs[i] != other.BrokerAddrs[i] {
			return false
		}
	}
	return true
}

func (b *BrokerData) ToJSONValue() map[string]any {
	addrs := make(map[string]any, len(b.BrokerAddrs))
	for _, pair := range b.BrokerAddrs {
		addrs[strconv.FormatInt(pair.ID, 10)] = pair.Addr
	}
	return map[string]any{
		"cluster":            b.Cluster,
		"brokerName":         b.BrokerName,
		"brokerAddrs":        addrs,
		"zoneName":           b.ZoneName,
		"enableActingMaster": b.EnableActingMaster,
	}
}

func (b *BrokerData) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("BrokerData: expected object")
	}
	b.Cluster = jsonString(obj, "cluster", "")
	b.BrokerName = jsonString(obj, "brokerName", "")
	b.ZoneName = jsonString(obj, "zoneName", "")
	b.EnableActingMaster = jsonBoolField(obj, "enableActingMaster", false)
	b.BrokerAddrs = nil
	// fastjson2 writes bare numeric keys; the tolerant parser hands both the
	// quoted and bare forms over as key strings. Dirty keys are skipped so one
	// bad entry does not invalidate the whole route.
	if addrs, ok := obj["brokerAddrs"].(map[string]any); ok {
		for k, v := range addrs {
			id, err := strconv.ParseInt(trimSpace(k), 10, 64)
			if err != nil {
				continue
			}
			b.BrokerAddrs = append(b.BrokerAddrs, BrokerAddrPair{ID: id, Addr: jsonAnyString(v)})
		}
		// A JSON object has no order; decode into a canonical id order so
		// route-change detection does not fire on map-iteration luck.
		sort.Slice(b.BrokerAddrs, func(i, j int) bool { return b.BrokerAddrs[i].ID < b.BrokerAddrs[j].ID })
	}
	return nil
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

// pseudoRandomIndex returns a pseudo-random index in [0, bound) without
// pulling in math/rand: nanosecond clock xor a process-wide counter through
// xorshift64. Only "not the same one every time" matters here — load
// balancing does not need cryptographic quality.
// prngCounter drives pseudoRandomIndex; the xorshift64 state mixes in the
// nanosecond clock on every call.
var prngCounter atomic.Uint64

const prngStep = 0x5174C57B92B653AE

func pseudoRandomIndex(bound int) int {
	if bound <= 1 {
		return 0
	}
	x := prngCounter.Add(prngStep) ^ uint64(time.Now().UnixNano())
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	return int((x*0x2545F4914F6CDD1D)>>32) % bound
}

// ---------------------------------------------------------------- TopicRouteData

// FilterServerEntry is one brokerAddr -> filter-server list mapping, kept in
// wire order.
type FilterServerEntry struct {
	Addr    string
	Servers []string
}

// MappingEntry is one opaque topicQueueMappingByBroker entry; the protocol
// layer passes the mapping JSON through untouched.
type MappingEntry struct {
	Broker string
	Info   any
}

// TopicRouteData is the GET_ROUTE_INFO_BY_TOPIC(105) body and the client's
// whole route-table entry value.
//
// Two null semantics that must not be unified:
//   - orderTopicConf is ALWAYS written, null included (Java field order);
//   - topicQueueMappingByBroker: nil (key absent) is different from an empty
//     non-nil slice (key present as {}).
type TopicRouteData struct {
	OrderTopicConf            *string
	QueueDatas                []*QueueData
	BrokerDatas               []*BrokerData
	FilterServerTable         []FilterServerEntry
	TopicQueueMappingByBroker []MappingEntry
}

func (t *TopicRouteData) ToJSONValue() map[string]any {
	queueDatas := make([]any, 0, len(t.QueueDatas))
	for _, q := range t.QueueDatas {
		queueDatas = append(queueDatas, q.ToJSONValue())
	}
	brokerDatas := make([]any, 0, len(t.BrokerDatas))
	for _, b := range t.BrokerDatas {
		brokerDatas = append(brokerDatas, b.ToJSONValue())
	}
	filterServers := make(map[string]any, len(t.FilterServerTable))
	for _, e := range t.FilterServerTable {
		servers := make([]any, 0, len(e.Servers))
		for _, s := range e.Servers {
			servers = append(servers, s)
		}
		filterServers[e.Addr] = servers
	}
	var orderConf any
	if t.OrderTopicConf != nil {
		orderConf = *t.OrderTopicConf
	}
	out := map[string]any{
		"orderTopicConf":    orderConf,
		"queueDatas":        queueDatas,
		"brokerDatas":       brokerDatas,
		"filterServerTable": filterServers,
	}
	if t.TopicQueueMappingByBroker != nil {
		mapping := make(map[string]any, len(t.TopicQueueMappingByBroker))
		for _, e := range t.TopicQueueMappingByBroker {
			mapping[e.Broker] = e.Info
		}
		out["topicQueueMappingByBroker"] = mapping
	}
	return out
}

func (t *TopicRouteData) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("TopicRouteData: expected object")
	}
	t.OrderTopicConf = optStringField(obj, "orderTopicConf")
	t.QueueDatas = nil
	for _, item := range jsonArrayField(obj, "queueDatas") {
		q := &QueueData{}
		if err := q.FromJSONValue(item); err != nil {
			return err
		}
		t.QueueDatas = append(t.QueueDatas, q)
	}
	t.BrokerDatas = nil
	for _, item := range jsonArrayField(obj, "brokerDatas") {
		b := &BrokerData{}
		if err := b.FromJSONValue(item); err != nil {
			return err
		}
		t.BrokerDatas = append(t.BrokerDatas, b)
	}
	t.FilterServerTable = nil
	if servers, ok := obj["filterServerTable"].(map[string]any); ok {
		for addr, raw := range servers {
			entry := FilterServerEntry{Addr: addr}
			if arr, ok := raw.([]any); ok {
				for _, item := range arr {
					switch t := item.(type) {
					case string:
						entry.Servers = append(entry.Servers, t)
					default:
						entry.Servers = append(entry.Servers, jsonAnyString(item))
					}
				}
			}
			t.FilterServerTable = append(t.FilterServerTable, entry)
		}
	}
	// Absent and explicit null both read as nil; an explicit {} stays an
	// empty non-nil slice (the difference is visible on re-encode).
	switch raw := obj["topicQueueMappingByBroker"].(type) {
	case nil:
		t.TopicQueueMappingByBroker = nil
	case map[string]any:
		t.TopicQueueMappingByBroker = make([]MappingEntry, 0, len(raw))
		for broker, info := range raw {
			t.TopicQueueMappingByBroker = append(t.TopicQueueMappingByBroker, MappingEntry{Broker: broker, Info: info})
		}
	default:
		return decodeErrf("topicQueueMappingByBroker is not a json object")
	}
	return nil
}

func jsonAnyString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case remoting.JSONNumber:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	default:
		return ""
	}
}

// Encode produces the wire body (compact JSON).
func (t *TopicRouteData) Encode() []byte { return remoting.EncodeJSON(t.ToJSONValue()) }

// DecodeTopicRouteData parses a TopicRouteData body.
func DecodeTopicRouteData(data []byte) (*TopicRouteData, error) {
	value, err := remoting.DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	route := &TopicRouteData{}
	if err := route.FromJSONValue(value); err != nil {
		return nil, err
	}
	return route, nil
}

// Clone mirrors Java cloneTopicRouteData: queue/broker elements are copied by
// reference value (they are treated as immutable), the tables are deep.
func (t *TopicRouteData) Clone() *TopicRouteData {
	clone := &TopicRouteData{
		OrderTopicConf: t.OrderTopicConf,
		QueueDatas:     append([]*QueueData(nil), t.QueueDatas...),
		BrokerDatas:    append([]*BrokerData(nil), t.BrokerDatas...),
	}
	for _, e := range t.FilterServerTable {
		clone.FilterServerTable = append(clone.FilterServerTable, FilterServerEntry{
			Addr:    e.Addr,
			Servers: append([]string(nil), e.Servers...),
		})
	}
	if t.TopicQueueMappingByBroker != nil {
		clone.TopicQueueMappingByBroker = make([]MappingEntry, 0, len(t.TopicQueueMappingByBroker))
		for _, e := range t.TopicQueueMappingByBroker {
			clone.TopicQueueMappingByBroker = append(clone.TopicQueueMappingByBroker, MappingEntry{Broker: e.Broker, Info: e.Info})
		}
	}
	return clone
}

// GetAllMessageQueue assembles every writable queue, mirroring Java
// topicRouteData2TopicPublishInfo's assembly loop. Two skip conditions, verbatim
// from Java MQClientInstance: the broker is missing from the route, OR its
// brokerAddrs lack the master id. The second check is not redundancy — a slave
// registers itself with write permission enabled by default, and once the
// master drops the route keeps only brokerId=1 for that name. Picking such a
// queue sends the request to a slave, which rejects sends with SYSTEM_BUSY
// (retryable) — a whole retry round burned for nothing.
func (t *TopicRouteData) GetAllMessageQueue(topic string) []common.MessageQueue {
	var mqs []common.MessageQueue
	for _, qd := range t.QueueDatas {
		if !common.CheckPerm(qd.Perm, common.PermWrite) {
			continue
		}
		var bd *BrokerData
		for _, b := range t.BrokerDatas {
			if b.BrokerName == qd.BrokerName {
				bd = b
				break
			}
		}
		if bd == nil {
			continue
		}
		hasMaster := false
		for _, pair := range bd.BrokerAddrs {
			if pair.ID == int64(common.MasterID) {
				hasMaster = true
				break
			}
		}
		if !hasMaster {
			continue
		}
		for i := int32(0); i < qd.WriteQueueNums; i++ {
			mqs = append(mqs, common.NewMessageQueue(topic, qd.BrokerName, i))
		}
	}
	return mqs
}

// GetAllSubscribeMessageQueue assembles every READABLE queue (Java
// topicRouteData2TopicSubscribeInfo). Deliberately a separate path from
// GetAllMessageQueue: only the READ permission and readQueueNums count, no
// brokerDatas lookup and no master requirement. (1) perm=4 read-only topics
// are consumable in Java while their publish info is empty; (2) when the
// master is down the rebalance queue set must not change — pulls can go
// through slaves. The consumer side must always use this one.
func (t *TopicRouteData) GetAllSubscribeMessageQueue(topic string) []common.MessageQueue {
	var mqs []common.MessageQueue
	for _, qd := range t.QueueDatas {
		if !common.CheckPerm(qd.Perm, common.PermRead) {
			continue
		}
		for i := int32(0); i < qd.ReadQueueNums; i++ {
			mqs = append(mqs, common.NewMessageQueue(topic, qd.BrokerName, i))
		}
	}
	return mqs
}

// TopicRouteDataChanged is the short-circuit condition of
// updateTopicRouteInfoFromNameServer. Sorted comparison covers queueDatas by
// (brokerName, read, write, perm) — topicSysFlag excluded, so a broker-only
// sysFlag flip does not look like a route change — and brokerDatas by name,
// compared through the narrow Java-equals above.
func (t *TopicRouteData) TopicRouteDataChanged(old *TopicRouteData) bool {
	if old == nil {
		return true
	}
	sortedQueues := func(list []*QueueData) []*QueueData {
		out := append([]*QueueData(nil), list...)
		sort.SliceStable(out, func(i, j int) bool {
			a, b := out[i], out[j]
			if a.BrokerName != b.BrokerName {
				return a.BrokerName < b.BrokerName
			}
			if a.ReadQueueNums != b.ReadQueueNums {
				return a.ReadQueueNums < b.ReadQueueNums
			}
			if a.WriteQueueNums != b.WriteQueueNums {
				return a.WriteQueueNums < b.WriteQueueNums
			}
			return a.Perm < b.Perm
		})
		return out
	}
	sortedBrokers := func(list []*BrokerData) []*BrokerData {
		out := append([]*BrokerData(nil), list...)
		sort.SliceStable(out, func(i, j int) bool {
			return out[i].BrokerName < out[j].BrokerName
		})
		return out
	}
	newQ, oldQ := sortedQueues(t.QueueDatas), sortedQueues(old.QueueDatas)
	if len(newQ) != len(oldQ) {
		return true
	}
	for i := range newQ {
		if newQ[i].BrokerName != oldQ[i].BrokerName ||
			newQ[i].ReadQueueNums != oldQ[i].ReadQueueNums ||
			newQ[i].WriteQueueNums != oldQ[i].WriteQueueNums ||
			newQ[i].Perm != oldQ[i].Perm {
			return true
		}
	}
	newB, oldB := sortedBrokers(t.BrokerDatas), sortedBrokers(old.BrokerDatas)
	if len(newB) != len(oldB) {
		return true
	}
	for i := range newB {
		if !newB[i].Equal(oldB[i]) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- TopicPublishInfo

// QueueFilter decides whether a candidate queue is acceptable.
type QueueFilter func(*common.MessageQueue) bool

// TopicPublishInfo is the per-topic publish bookkeeping (Java
// TopicPublishInfo). SelectOneMessageQueue round-robins through a ring
// cursor; index may go negative or overflow, it is reduced modulo len first.
type TopicPublishInfo struct {
	mu             sync.Mutex
	orderTopic     bool
	msgQueueList   []common.MessageQueue
	topicRouteData *TopicRouteData
	index          int64
}

func NewTopicPublishInfo() *TopicPublishInfo { return &TopicPublishInfo{} }

// OK reports whether any queue is available.
func (p *TopicPublishInfo) OK() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.msgQueueList) > 0
}

// ResetIndex restarts the ring cursor.
func (p *TopicPublishInfo) ResetIndex() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.index = 0
}

// SelectOneMessageQueue picks the next queue (Java
// TopicPublishInfo.selectOneMessageQueue / Python
// select_one_message_queue(*filters)).
//
// No filters: unconditional round-robin, never returns ok=false. With
// filters: try at most n queues from the cursor (the cursor advances either
// way, Java same); only when all are filtered out does it return ok=false. An
// empty list is an error.
func (p *TopicPublishInfo) SelectOneMessageQueue(filters []QueueFilter) (common.MessageQueue, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.msgQueueList)
	if n == 0 {
		return common.MessageQueue{}, false, common.ClientError("no message queue for publish info")
	}
	if len(filters) == 0 {
		mq := p.msgQueueList[ringIndex(p.index, n)]
		p.index++
		return mq, true, nil
	}
	for i := 0; i < n; i++ {
		mq := p.msgQueueList[ringIndex(p.index, n)]
		p.index++
		accept := true
		for _, f := range filters {
			if !f(&mq) {
				accept = false
				break
			}
		}
		if accept {
			return mq, true, nil
		}
	}
	return common.MessageQueue{}, false, nil
}

func ringIndex(index int64, length int) int {
	r := index % int64(length)
	if r < 0 {
		r += int64(length)
	}
	return int(r)
}

// OrderTopic reports the orderTopic flag.
func (p *TopicPublishInfo) OrderTopic() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.orderTopic
}

// MsgQueueList snapshots the queue list.
func (p *TopicPublishInfo) MsgQueueList() []common.MessageQueue {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]common.MessageQueue(nil), p.msgQueueList...)
}

// TopicRouteData snapshots the backing route (may be nil).
func (p *TopicPublishInfo) TopicRouteData() *TopicRouteData {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.topicRouteData
}

// UpdateFromRoute rewrites the whole state from a freshly fetched route (the
// three-assignment tail of update_topic_route_info_from_name_server).
func (p *TopicPublishInfo) UpdateFromRoute(route *TopicRouteData, topic string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.orderTopic = route.OrderTopicConf != nil
	p.topicRouteData = route.Clone()
	p.msgQueueList = route.GetAllMessageQueue(topic)
}
