// Admin/管理侧 wire beans: TopicConfig, the subscription-group models and the
// response bodies of the DefaultMQAdminExt surface.
//
// Field names come from the Java 5.x probe (see the Python port's
// remoting/protocol/{body,admin_body,subscription}.py and common/topic_config.py,
// all of which were verified against a live 5.5.1 cluster). Do not "tidy" them:
//   - message-queue-keyed maps travel as fastjson2 INLINE OBJECT keys, which is
//     not legal JSON for a strict reader — ParseJSON/DecodeMapKey handle it;
//   - `GetBrokerConfig`'s body is java.util.Properties TEXT, not JSON;
//   - fastjson2 skips nulls, so every reader falls back to a default.
package remoting

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ---------------- extra tolerant readers (the int/string ones live in bodies.go) ----------------

// jsonFloatOr reads a float field; tps / rt values arrive as JSON numbers.
//
// Accepts native Go numerics as well as the wire shapes: FromJSONValue is fed
// both decoded JSON (JSONNumber / string) and, for in-process round-trips, the
// output of a sibling ToJSONValue (plain float64 / int). Without the native
// cases a value round-tripped through Go silently reads back as the default —
// every field looks present but zero.
func jsonFloatOr(value any, key string, def float64) float64 {
	obj, ok := value.(map[string]any)
	if !ok {
		return def
	}
	switch t := obj[key].(type) {
	case JSONNumber:
		if f, err := t.Float64(); err == nil {
			return f
		}
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(t), 64); err == nil {
			return f
		}
	case float64:
		return t
	case float32:
		return float64(t)
	case JavaDouble:
		return float64(t)
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint:
		return float64(t)
	case uint32:
		return float64(t)
	case uint64:
		return float64(t)
	}
	return def
}

// jsonStringMapOr reads a map<string,string>; a non-object yields an empty map
// (some brokers omit `attributes` entirely).
func jsonStringMapOr(value any, key string) map[string]string {
	out := map[string]string{}
	obj, ok := value.(map[string]any)
	if !ok {
		return out
	}
	raw, ok := obj[key].(map[string]any)
	if !ok {
		return out
	}
	for k := range raw {
		out[k] = jsonStringOr(raw, k, "")
	}
	return out
}

// jsonStringListOr reads an array of strings.
func jsonStringListOr(value any, key string) []string {
	out := []string{}
	for _, item := range jsonArray(value, key) {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ---------------- TopicConfig ----------------

// TopicConfig mirrors org.apache.rocketmq.common.TopicConfig.
//
// `new TopicConfig("t")` serialises as
//
//	{"attributes":{},"order":false,"perm":6,"readQueueNums":16,
//	 "topicFilterType":"SINGLE_TAG","topicName":"t","topicSysFlag":0,
//	 "writeQueueNums":16}
//
// `attributes` IS serialised — Java's getAttributes() carries no
// serialize=false, unlike the subscription beans where fastjson2 drops nulls.
const (
	DefaultReadQueueNums  int32 = 16
	DefaultWriteQueueNums int32 = 16
	// common.PermRead | common.PermWrite
	DefaultPerm int32 = 6

	TopicFilterTypeSingleTag = "SINGLE_TAG"
	TopicFilterTypeMultiTag  = "MULTI_TAG"
)

// TopicConfig is the broker-side topic definition.
type TopicConfig struct {
	TopicName       string
	ReadQueueNums   int32
	WriteQueueNums  int32
	Perm            int32
	TopicFilterType string
	TopicSysFlag    int32
	Order           bool
	Attributes      map[string]string
}

// NewTopicConfig builds a TopicConfig with Java's constructor defaults.
func NewTopicConfig(topicName string) *TopicConfig {
	return &TopicConfig{
		TopicName:       topicName,
		ReadQueueNums:   DefaultReadQueueNums,
		WriteQueueNums:  DefaultWriteQueueNums,
		Perm:            DefaultPerm,
		TopicFilterType: TopicFilterTypeSingleTag,
		Attributes:      map[string]string{},
	}
}

// ToJSONValue renders the Java field names. `attributes` is always present.
func (t *TopicConfig) ToJSONValue() map[string]any {
	attrs := make(map[string]any, len(t.Attributes))
	for k, v := range t.Attributes {
		attrs[k] = v
	}
	return map[string]any{
		"topicName":       t.TopicName,
		"readQueueNums":   t.ReadQueueNums,
		"writeQueueNums":  t.WriteQueueNums,
		"perm":            t.Perm,
		"topicFilterType": t.TopicFilterType,
		"topicSysFlag":    t.TopicSysFlag,
		"order":           t.Order,
		"attributes":      attrs,
	}
}

// FromJSONValue reads a TopicConfig, falling back to Java's defaults for every
// absent key.
func (t *TopicConfig) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		if value == nil {
			return decodeErrf("TopicConfig: expected a JSON object, got nil")
		}
		return decodeErrf("TopicConfig: expected a JSON object")
	}
	t.TopicName = jsonStringOr(obj, "topicName", "")
	t.ReadQueueNums = jsonI32(obj, "readQueueNums", DefaultReadQueueNums)
	t.WriteQueueNums = jsonI32(obj, "writeQueueNums", DefaultWriteQueueNums)
	t.Perm = jsonI32(obj, "perm", DefaultPerm)
	t.TopicFilterType = jsonStringOr(obj, "topicFilterType", TopicFilterTypeSingleTag)
	t.TopicSysFlag = jsonI32(obj, "topicSysFlag", 0)
	t.Order = jsonBool(obj, "order", false)
	t.Attributes = jsonStringMapOr(obj, "attributes")
	return nil
}

// Encode renders the JSON body the broker expects (Java RemotingSerializable).
func (t *TopicConfig) Encode() []byte { return EncodeJSON(t.ToJSONValue()) }

// DecodeTopicConfig parses a TopicConfig body.
func DecodeTopicConfig(data []byte) (*TopicConfig, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	cfg := &TopicConfig{}
	if err := cfg.FromJSONValue(value); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (t *TopicConfig) String() string {
	return fmt.Sprintf("TopicConfig[topicName=%s, readQueueNums=%d, writeQueueNums=%d, perm=%s]",
		t.TopicName, t.ReadQueueNums, t.WriteQueueNums, common.Perm2String(t.Perm))
}

// TopicConfigSerializeWrapper mirrors
// org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper.
//
// Probe output: {"dataVersion":{...},"topicConfigTable":{"Topic":{"attributes":{},...}}}
type TopicConfigSerializeWrapper struct {
	TopicConfigTable map[string]*TopicConfig
	DataVersion      map[string]any
}

// NewTopicConfigSerializeWrapper builds an empty wrapper.
func NewTopicConfigSerializeWrapper() *TopicConfigSerializeWrapper {
	return &TopicConfigSerializeWrapper{
		TopicConfigTable: map[string]*TopicConfig{},
		DataVersion:      map[string]any{},
	}
}

func (w *TopicConfigSerializeWrapper) ToJSONValue() map[string]any {
	table := make(map[string]any, len(w.TopicConfigTable))
	for k, v := range w.TopicConfigTable {
		table[k] = v.ToJSONValue()
	}
	return map[string]any{"dataVersion": w.DataVersion, "topicConfigTable": table}
}

func (w *TopicConfigSerializeWrapper) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("TopicConfigSerializeWrapper: expected object")
	}
	w.TopicConfigTable = map[string]*TopicConfig{}
	if raw, ok := obj["topicConfigTable"].(map[string]any); ok {
		for name := range raw {
			cfg := &TopicConfig{}
			if err := cfg.FromJSONValue(raw[name]); err != nil {
				return err
			}
			w.TopicConfigTable[name] = cfg
		}
	}
	w.DataVersion = map[string]any{}
	if dv, ok := obj["dataVersion"].(map[string]any); ok {
		w.DataVersion = dv
	}
	return nil
}

func (w *TopicConfigSerializeWrapper) Encode() []byte { return EncodeJSON(w.ToJSONValue()) }

// DecodeTopicConfigSerializeWrapper parses the GET_ALL_TOPIC_CONFIG(21) body.
func DecodeTopicConfigSerializeWrapper(data []byte) (*TopicConfigSerializeWrapper, error) {
	w := NewTopicConfigSerializeWrapper()
	if len(data) == 0 {
		return w, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := w.FromJSONValue(value); err != nil {
		return nil, err
	}
	return w, nil
}

// ---------------- TopicStatsTable / TopicOffset ----------------

// TopicOffset mirrors org.apache.rocketmq.remoting.protocol.admin.TopicOffset.
type TopicOffset struct {
	MinOffset           int64
	MaxOffset           int64
	LastUpdateTimestamp int64
}

func (o *TopicOffset) ToJSONValue() map[string]any {
	return map[string]any{
		"minOffset":           o.MinOffset,
		"maxOffset":           o.MaxOffset,
		"lastUpdateTimestamp": o.LastUpdateTimestamp,
	}
}

func (o *TopicOffset) FromJSONValue(value any) error {
	o.MinOffset = jsonI64(value, "minOffset", 0)
	o.MaxOffset = jsonI64(value, "maxOffset", 0)
	o.LastUpdateTimestamp = jsonI64(value, "lastUpdateTimestamp", 0)
	return nil
}

func (o *TopicOffset) String() string {
	return fmt.Sprintf("TopicOffset[min=%d, max=%d, ts=%d]", o.MinOffset, o.MaxOffset, o.LastUpdateTimestamp)
}

// TopicStatsTable mirrors admin.TopicStatsTable.
// Probe output: {"offsetTable":{<MessageQueue>:{...}},"topicPutTps":0.0}
//
// The offset table maps each queue to a TopicOffset OBJECT (min/max/lastUpdate),
// not to a bare offset — reading it as a plain offset map silently yields zeros.
type TopicStatsTable struct {
	OffsetTable map[common.MessageQueue]*TopicOffset
	TopicPutTps float64
}

// NewTopicStatsTable builds an empty table.
func NewTopicStatsTable() *TopicStatsTable {
	return &TopicStatsTable{OffsetTable: map[common.MessageQueue]*TopicOffset{}}
}

func (t *TopicStatsTable) ToJSONValue() map[string]any {
	table := newMQKeyedJSON(t.OffsetTable, func(o *TopicOffset) any { return o.ToJSONValue() })
	return map[string]any{"offsetTable": table, "topicPutTps": JavaDouble(t.TopicPutTps)}
}

func (t *TopicStatsTable) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("TopicStatsTable: expected object")
	}
	t.OffsetTable = map[common.MessageQueue]*TopicOffset{}
	if raw, ok := obj["offsetTable"].(map[string]any); ok {
		for k, v := range raw {
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
			off := &TopicOffset{}
			if err := off.FromJSONValue(v); err != nil {
				return err
			}
			t.OffsetTable[q] = off
		}
	}
	t.TopicPutTps = jsonFloatOr(obj, "topicPutTps", 0)
	return nil
}

func (t *TopicStatsTable) Encode() []byte { return EncodeFastJSON(t.ToJSONValue()) }

// DecodeTopicStatsTable parses the GET_TOPIC_STATS_INFO(202) body.
func DecodeTopicStatsTable(data []byte) (*TopicStatsTable, error) {
	t := NewTopicStatsTable()
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

// ---------------- ConsumeStats / OffsetWrapper ----------------

// OffsetWrapper mirrors admin.OffsetWrapper.
type OffsetWrapper struct {
	BrokerOffset   int64
	ConsumerOffset int64
	LastTimestamp  int64
	PullOffset     int64
}

// Lag mirrors Java OffsetWrapper#getLag: brokerOffset - consumerOffset.
func (o *OffsetWrapper) Lag() int64 { return o.BrokerOffset - o.ConsumerOffset }

func (o *OffsetWrapper) ToJSONValue() map[string]any {
	return map[string]any{
		"brokerOffset":   o.BrokerOffset,
		"consumerOffset": o.ConsumerOffset,
		"lastTimestamp":  o.LastTimestamp,
		"pullOffset":     o.PullOffset,
	}
}

func (o *OffsetWrapper) FromJSONValue(value any) error {
	o.BrokerOffset = jsonI64(value, "brokerOffset", 0)
	o.ConsumerOffset = jsonI64(value, "consumerOffset", 0)
	o.LastTimestamp = jsonI64(value, "lastTimestamp", 0)
	o.PullOffset = jsonI64(value, "pullOffset", 0)
	return nil
}

func (o *OffsetWrapper) String() string {
	return fmt.Sprintf("OffsetWrapper[broker=%d, consumer=%d, lag=%d]", o.BrokerOffset, o.ConsumerOffset, o.Lag())
}

// MQOffsetWrapperMap is a MessageQueue-keyed map of OffsetWrapper.
type MQOffsetWrapperMap map[common.MessageQueue]*OffsetWrapper

// ConsumeStats mirrors admin.ConsumeStats.
// Probe output: {"consumeTps":1.5,"offsetTable":{<MessageQueue>:<OffsetWrapper>}}
type ConsumeStats struct {
	OffsetTable MQOffsetWrapperMap
	ConsumeTps  float64
}

// NewConsumeStats builds an empty ConsumeStats.
func NewConsumeStats() *ConsumeStats {
	return &ConsumeStats{OffsetTable: MQOffsetWrapperMap{}}
}

// TotalLag sums the per-queue lag.
func (c *ConsumeStats) TotalLag() int64 {
	var total int64
	for _, w := range c.OffsetTable {
		total += w.Lag()
	}
	return total
}

func (c *ConsumeStats) ToJSONValue() map[string]any {
	table := newMQKeyedJSON(c.OffsetTable, func(w *OffsetWrapper) any { return w.ToJSONValue() })
	return map[string]any{"offsetTable": table, "consumeTps": JavaDouble(c.ConsumeTps)}
}

func (c *ConsumeStats) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ConsumeStats: expected object")
	}
	c.OffsetTable = MQOffsetWrapperMap{}
	raw, _ := obj["offsetTable"].(map[string]any)
	for k, v := range raw {
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
		w := &OffsetWrapper{}
		if err := w.FromJSONValue(v); err != nil {
			return err
		}
		c.OffsetTable[q] = w
	}
	c.ConsumeTps = jsonFloatOr(obj, "consumeTps", 0)
	return nil
}

func (c *ConsumeStats) Encode() []byte { return EncodeFastJSON(c.ToJSONValue()) }

// DecodeConsumeStats parses the GET_CONSUME_STATS(208) body.
func DecodeConsumeStats(data []byte) (*ConsumeStats, error) {
	c := NewConsumeStats()
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

// ConsumeStatus mirrors org.apache.rocketmq.remoting.protocol.body.ConsumeStatus
// (the per-group consume snapshot the admin tools render).
//
// The five rate fields are Java `double` and go through JavaDouble so the body
// reads `0.0` (not `0`) and a zero-span window reads `null` (not an empty
// body) — this struct feeds the 307 answer's statusTable, so the broker's
// console parses it directly.
type ConsumeStatus struct {
	PullRT            float64
	PullTPS           float64
	ConsumeRT         float64
	ConsumeOKTPS      float64
	ConsumeFailedTPS  float64
	ConsumeFailedMsgs int64
}

func (c *ConsumeStatus) ToJSONValue() map[string]any {
	return map[string]any{
		"pullRT":            JavaDouble(c.PullRT),
		"pullTPS":           JavaDouble(c.PullTPS),
		"consumeRT":         JavaDouble(c.ConsumeRT),
		"consumeOKTPS":      JavaDouble(c.ConsumeOKTPS),
		"consumeFailedTPS":  JavaDouble(c.ConsumeFailedTPS),
		"consumeFailedMsgs": c.ConsumeFailedMsgs,
	}
}

func (c *ConsumeStatus) FromJSONValue(value any) error {
	c.PullRT = jsonFloatOr(value, "pullRT", 0)
	c.PullTPS = jsonFloatOr(value, "pullTPS", 0)
	c.ConsumeRT = jsonFloatOr(value, "consumeRT", 0)
	c.ConsumeOKTPS = jsonFloatOr(value, "consumeOKTPS", 0)
	c.ConsumeFailedTPS = jsonFloatOr(value, "consumeFailedTPS", 0)
	c.ConsumeFailedMsgs = jsonI64(value, "consumeFailedMsgs", 0)
	return nil
}

// ---------------- QueryConsumeQueue ----------------

// ConsumeQueueData mirrors body.ConsumeQueueData.
//
// Probe/source fields: physicOffset, physicSize, tagsCode, extendDataJson,
// bitMap, eval, msg. Like Java, `extendDataJson` and `msg` are omitted from
// the JSON when nil.
type ConsumeQueueData struct {
	PhysicOffset   int64
	PhysicSize     int32
	TagsCode       int64
	ExtendDataJSON *string
	BitMap         *string
	Eval           bool
	Msg            *string
}

func (d *ConsumeQueueData) ToJSONValue() map[string]any {
	out := map[string]any{
		"physicOffset": d.PhysicOffset,
		"physicSize":   d.PhysicSize,
		"tagsCode":     d.TagsCode,
		"eval":         d.Eval,
		"bitMap":       d.BitMap,
	}
	if d.ExtendDataJSON != nil {
		out["extendDataJson"] = *d.ExtendDataJSON
	}
	if d.Msg != nil {
		out["msg"] = *d.Msg
	}
	return out
}

func (d *ConsumeQueueData) FromJSONValue(value any) error {
	d.PhysicOffset = jsonI64(value, "physicOffset", 0)
	d.PhysicSize = jsonI32(value, "physicSize", 0)
	d.TagsCode = jsonI64(value, "tagsCode", 0)
	d.Eval = jsonBool(value, "eval", false)
	if obj, ok := value.(map[string]any); ok {
		if s, ok := obj["extendDataJson"].(string); ok {
			d.ExtendDataJSON = &s
		}
		if s, ok := obj["bitMap"].(string); ok {
			d.BitMap = &s
		}
		if s, ok := obj["msg"].(string); ok {
			d.Msg = &s
		}
	}
	return nil
}

// QueryConsumeQueueResponseBody mirrors body.QueryConsumeQueueResponseBody.
//
// Probe output: {"filterData":"*","maxQueueIndex":88,"minQueueIndex":1,
// "subscriptionData":{...}} — `queueData` is absent when null.
type QueryConsumeQueueResponseBody struct {
	SubscriptionData map[string]any
	FilterData       *string
	QueueData        []*ConsumeQueueData
	MaxQueueIndex    int64
	MinQueueIndex    int64
}

// NewQueryConsumeQueueResponseBody builds an empty body.
func NewQueryConsumeQueueResponseBody() *QueryConsumeQueueResponseBody {
	return &QueryConsumeQueueResponseBody{}
}

func (b *QueryConsumeQueueResponseBody) ToJSONValue() map[string]any {
	out := map[string]any{
		"maxQueueIndex": b.MaxQueueIndex,
		"minQueueIndex": b.MinQueueIndex,
	}
	if b.SubscriptionData != nil {
		out["subscriptionData"] = b.SubscriptionData
	}
	if b.FilterData != nil {
		out["filterData"] = *b.FilterData
	}
	if b.QueueData != nil {
		rows := make([]any, 0, len(b.QueueData))
		for _, q := range b.QueueData {
			rows = append(rows, q.ToJSONValue())
		}
		out["queueData"] = rows
	}
	return out
}

func (b *QueryConsumeQueueResponseBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		if value == nil {
			return nil
		}
		return decodeErrf("QueryConsumeQueueResponseBody: expected object")
	}
	if sub, ok := obj["subscriptionData"].(map[string]any); ok {
		b.SubscriptionData = sub
	}
	if s, ok := obj["filterData"].(string); ok {
		b.FilterData = &s
	}
	if raw, ok := obj["queueData"].([]any); ok {
		b.QueueData = make([]*ConsumeQueueData, 0, len(raw))
		for _, item := range raw {
			d := &ConsumeQueueData{}
			if err := d.FromJSONValue(item); err != nil {
				return err
			}
			b.QueueData = append(b.QueueData, d)
		}
	}
	b.MaxQueueIndex = jsonI64(obj, "maxQueueIndex", 0)
	b.MinQueueIndex = jsonI64(obj, "minQueueIndex", 0)
	return nil
}

func (b *QueryConsumeQueueResponseBody) Encode() []byte { return EncodeJSON(b.ToJSONValue()) }

// DecodeQueryConsumeQueueResponseBody parses the QUERY_CONSUME_QUEUE(321) body.
func DecodeQueryConsumeQueueResponseBody(data []byte) (*QueryConsumeQueueResponseBody, error) {
	b := NewQueryConsumeQueueResponseBody()
	if len(data) == 0 {
		return b, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := b.FromJSONValue(value); err != nil {
		return nil, err
	}
	return b, nil
}
