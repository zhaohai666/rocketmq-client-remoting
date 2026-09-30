// Remoting body beans used by the client layer: the CHECK_CLIENT_CONFIG(46)
// request body and the MessageQueue-keyed offset maps of
// RESET_CONSUMER_CLIENT_OFFSET(220) / GET_CONSUMER_STATUS_FROM_CLIENT(221).
//
// MessageQueue-keyed maps travel as fastjson2 inline-object keys
// (`{"brokerName":"b","queueId":1,"topic":"Tt"}:9`). That byte stream is not
// valid JSON and the stdlib encoder cannot produce it, so bodies carrying one
// are serialised by EncodeFastJSON — see mqKeyedJSON below for why, and for the
// 5.5.1-jar evidence that the escaped alternative is rejected by fastjson2.
package remoting

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ---------------- small JSON field readers (tolerant) ----------------

func jsonStringOr(value any, key, def string) string {
	obj, ok := value.(map[string]any)
	if !ok {
		return def
	}
	switch t := obj[key].(type) {
	case string:
		return t
	case JSONNumber:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	default:
		return def
	}
}

func jsonI64(value any, key string, def int64) int64 {
	obj, ok := value.(map[string]any)
	if !ok {
		return def
	}
	if n, ok := numberAsI64(obj[key]); ok {
		return n
	}
	return def
}

// numberAsI64 reads an integer field. Besides the wire shapes (JSONNumber, a
// numeric string) it accepts native Go numerics, because FromJSONValue is used
// both for decoded JSON and for in-process round-trips of a sibling
// ToJSONValue. Missing the native cases makes such a round-trip silently read
// back the default — the field appears present but is zero.
func numberAsI64(v any) (int64, bool) {
	switch t := v.(type) {
	case JSONNumber:
		if n, err := t.Int64(); err == nil {
			return n, true
		}
		// A wire literal such as `3.0` still denotes an integer field.
		if f, err := t.Float64(); err == nil {
			return int64(f), true
		}
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64); err == nil {
			return n, true
		}
	case int:
		return int64(t), true
	case int8:
		return int64(t), true
	case int16:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case uint:
		return int64(t), true
	case uint8:
		return int64(t), true
	case uint16:
		return int64(t), true
	case uint32:
		return int64(t), true
	case uint64:
		return int64(t), true
	case float32:
		return int64(t), true
	case float64:
		return int64(t), true
	case JavaDouble:
		return int64(t), true
	}
	return 0, false
}

func jsonBool(value any, key string, def bool) bool {
	obj, ok := value.(map[string]any)
	if !ok {
		return def
	}
	switch t := obj[key].(type) {
	case bool:
		return t
	case string:
		return t == "true"
	case JSONNumber:
		return t.String() != "0"
	default:
		return def
	}
}

func jsonArray(value any, key string) []any {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	arr, _ := obj[key].([]any)
	return arr
}

// ---------------- CheckClientRequestBody (46) ----------------

// CheckClientRequestBody is the body of CHECK_CLIENT_CONFIG(46). The broker
// logs clientId/group and validates only subscriptionData's expressionType and
// subString. namespace exists in Java 5.5.1 but senders leave it empty.
type CheckClientRequestBody struct {
	ClientID         *string
	Group            *string
	SubscriptionData *SubscriptionData
	Namespace        *string
}

func (b *CheckClientRequestBody) ToJSONValue() map[string]any {
	out := map[string]any{
		"clientId": strOrNil(b.ClientID),
		"group":    strOrNil(b.Group),
	}
	if b.SubscriptionData != nil {
		out["subscriptionData"] = b.SubscriptionData.ToJSONValue()
	}
	if b.Namespace != nil {
		out["namespace"] = *b.Namespace
	}
	return out
}

func (b *CheckClientRequestBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("CheckClientRequestBody: expected object")
	}
	b.ClientID = optString(obj, "clientId")
	b.Group = optString(obj, "group")
	if sub, ok := obj["subscriptionData"].(map[string]any); ok {
		sd := &SubscriptionData{}
		if err := sd.FromJSONValue(sub); err != nil {
			return err
		}
		b.SubscriptionData = sd
	} else {
		b.SubscriptionData = nil
	}
	b.Namespace = optString(obj, "namespace")
	return nil
}

func strOrNil(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

func optString(obj map[string]any, key string) *string {
	v, ok := obj[key]
	if !ok || v == nil {
		return nil
	}
	switch t := v.(type) {
	case string:
		return &t
	case JSONNumber:
		s := t.String()
		return &s
	default:
		return nil
	}
}

// Encode produces the wire body (compact JSON).
func (b *CheckClientRequestBody) Encode() []byte { return EncodeJSON(b.ToJSONValue()) }

// DecodeCheckClientRequestBody parses a CheckClientRequestBody.
func DecodeCheckClientRequestBody(data []byte) (*CheckClientRequestBody, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	body := &CheckClientRequestBody{}
	if err := body.FromJSONValue(value); err != nil {
		return nil, err
	}
	return body, nil
}

// ---------------- MessageQueue-keyed offset maps ----------------

// MQOffsetTableEntry is one (MessageQueue -> offset) pair.
type MQOffsetTableEntry struct {
	Queue  common.MessageQueue
	Offset int64
}

// MQOffsetTable is the ordered MessageQueue -> offset table.
type MQOffsetTable []MQOffsetTableEntry

// messageQueueKeyJSON renders the fastjson2 inline-object key. encoding/json
// sorts map keys, which lands on Java's alphabetical brokerName/queueId/topic.
func messageQueueKeyJSON(q common.MessageQueue) string {
	return string(EncodeJSON(map[string]any{
		"brokerName": q.BrokerName,
		"queueId":    q.QueueID,
		"topic":      q.Topic,
	}))
}

// MessageQueueKeyJSON exposes the inline-object key form to the client layer
// (the local offset file uses the same MessageQueue-as-key shape).
func MessageQueueKeyJSON(q common.MessageQueue) string { return messageQueueKeyJSON(q) }

// ---------------- MessageQueue-keyed objects on the wire ----------------

// mqKeyedJSON is a JSON object whose keys are fastjson2 INLINE OBJECTS:
//
//	{"brokerName":"broker-a","queueId":3,"topic":"TopicA"}:{...}
//
// A plain map[string]any CANNOT express that. encoding/json escapes the quotes
// inside a map key, emitting
//
//	"{\"brokerName\":\"broker-a\",\"queueId\":3,\"topic\":\"TopicA\"}":{...}
//
// and fastjson2 rejects that spelling outright for a `Map<MessageQueue, X>`
// field — to it the key is a Java MessageQueue, not a JSON string. Both forms
// were fed to the 5.5.1 jars with the REAL Go bytes:
//
//	Go CRI -> Java ConsumerRunningInfo: REJECTED -> JSONException:
//	    read field 'ConsumerRunningInfo.setMqTable, offset 477'
//	Go TST -> Java TopicStatsTable:     REJECTED -> JSONException:
//	    illegal fieldName input26, offset 156
//
// So an escaped key makes every MessageQueue-keyed body unreadable to a Java
// admin or broker. The Go reader never noticed, because the local tolerant
// parser unescapes the key and DecodeMapKey re-parses it afterwards: Go could
// always read its own output, and only the Java direction was broken.
//
// Order: entries are sorted by (topic, brokerName, queueId) — Java's
// MessageQueue.compareTo. Only ConsumerRunningInfo.mqTable/mqPopTable are
// TreeMaps where Java's order is deterministic, but sorting everywhere keeps
// the output stable and byte-comparable with fastjson2's. (TopicStatsTable and
// ConsumeStats use ConcurrentHashMap, ResetOffsetBody/GetConsumerStatusBody
// HashMap — order is not part of their contract.)
type mqKeyedJSON []mqKeyedEntry

type mqKeyedEntry struct {
	key   string // rendered inline-object text, written UNQUOTED
	value any
}

// newMQKeyedJSON builds a sorted inline-object-keyed table.
func newMQKeyedJSON[V any](table map[common.MessageQueue]V, render func(V) any) mqKeyedJSON {
	queues := make([]common.MessageQueue, 0, len(table))
	for q := range table {
		queues = append(queues, q)
	}
	sort.Slice(queues, func(i, j int) bool { return compareMessageQueue(queues[i], queues[j]) < 0 })
	out := make(mqKeyedJSON, 0, len(queues))
	for _, q := range queues {
		out = append(out, mqKeyedEntry{key: messageQueueKeyJSON(q), value: render(table[q])})
	}
	return out
}

// compareMessageQueue mirrors Java MessageQueue.compareTo: topic, then
// brokerName, then queueId.
func compareMessageQueue(a, b common.MessageQueue) int {
	if a.Topic != b.Topic {
		if a.Topic < b.Topic {
			return -1
		}
		return 1
	}
	if a.BrokerName != b.BrokerName {
		if a.BrokerName < b.BrokerName {
			return -1
		}
		return 1
	}
	switch {
	case a.QueueID < b.QueueID:
		return -1
	case a.QueueID > b.QueueID:
		return 1
	}
	return 0
}

// MarshalJSON deliberately FAILS. An inline-object key is not valid JSON, so
// encoding/json can neither emit it nor pass it through — it validates the
// output of every Marshaler it calls (and without this method the slice would
// silently serialise as a JSON ARRAY, which is worse than an error). A body
// carrying one of these must be encoded with EncodeFastJSON.
func (mqKeyedJSON) MarshalJSON() ([]byte, error) {
	return nil, errMQKeyedNeedsFastJSON
}

var errMQKeyedNeedsFastJSON = errors.New(
	"remoting: a MessageQueue-keyed table cannot go through encoding/json " +
		"(fastjson2 writes the queue as an unquoted inline-object key); " +
		"encode this body with EncodeFastJSON instead of EncodeJSON")

// EncodeMQOffsetTable writes an ordered map with MessageQueue object keys.
func EncodeMQOffsetTable(table MQOffsetTable) mqKeyedJSON {
	out := make(mqKeyedJSON, 0, len(table))
	for _, e := range table {
		out = append(out, mqKeyedEntry{key: messageQueueKeyJSON(e.Queue), value: e.Offset})
	}
	return out
}

// DecodeMQOffsetTable reads a MessageQueue-keyed map. Keys that do not
// re-parse as MessageQueue objects are skipped (same tolerance as the route
// beans; Rust decode_message_queue_map behaves the same). A non-object value
// is an error.
func DecodeMQOffsetTable(value any) (MQOffsetTable, error) {
	var out MQOffsetTable
	obj, ok := value.(map[string]any)
	if !ok {
		if value == nil {
			return out, nil
		}
		return out, decodeErrf("messageQueue-keyed map is not an object")
	}
	for k, v := range obj {
		inner, ok := DecodeMapKey(k)
		if !ok {
			continue
		}
		m, ok := inner.(map[string]any)
		if !ok {
			continue
		}
		queue := common.NewMessageQueue(
			jsonStringOr(m, "topic", ""),
			jsonStringOr(m, "brokerName", ""),
			jsonI32(m, "queueId", 0),
		)
		offset, ok := numberAsI64(v)
		if !ok {
			return out, decodeErrf("offset for %v is not an int", k)
		}
		out = append(out, MQOffsetTableEntry{Queue: queue, Offset: offset})
	}
	return out, nil
}

// MQOffsetMap is the unordered MessageQueue -> offset map form, used by the
// TopicStatsTable offset table. Java declares both `HashMap` and `TreeMap`
// flavours of this shape; the map form is what the admin beans expose.
type MQOffsetMap map[common.MessageQueue]int64

// DecodeMQOffsetMap reads a MessageQueue-keyed map of integers. Keys that do
// not re-parse as MessageQueue objects are skipped, matching the tolerance of
// DecodeMQOffsetTable.
func DecodeMQOffsetMap(value any) MQOffsetMap {
	out := MQOffsetMap{}
	obj, ok := value.(map[string]any)
	if !ok {
		return out
	}
	for k, v := range obj {
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
		if n, ok := numberAsI64(v); ok {
			out[q] = n
		}
	}
	return out
}

// ---------------- ResetOffsetBody (220 push) ----------------

// ResetOffsetBody carries Map<MessageQueue, Long> — NOT a nested
// topic->queueId map.
type ResetOffsetBody struct {
	OffsetTable MQOffsetTable
}

func (b *ResetOffsetBody) ToJSONValue() map[string]any {
	return map[string]any{"offsetTable": EncodeMQOffsetTable(b.OffsetTable)}
}

func (b *ResetOffsetBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ResetOffsetBody: expected object")
	}
	table, err := DecodeMQOffsetTable(obj["offsetTable"])
	if err != nil {
		return err
	}
	b.OffsetTable = table
	return nil
}

func (b *ResetOffsetBody) Encode() []byte { return EncodeFastJSON(b.ToJSONValue()) }

// DecodeResetOffsetBody parses a ResetOffsetBody.
func DecodeResetOffsetBody(data []byte) (*ResetOffsetBody, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	body := &ResetOffsetBody{}
	if err := body.FromJSONValue(value); err != nil {
		return nil, err
	}
	return body, nil
}

// MessageQueueForC is org.apache.rocketmq.common.message.MessageQueueForC
// (element of the ResetOffsetBodyForC array). Field order matches Java's
// declaration.
type MessageQueueForC struct {
	Topic      string
	BrokerName string
	QueueID    int32
	Offset     int64
}

func (e *MessageQueueForC) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("MessageQueueForC: expected object")
	}
	e.Topic = jsonStringOr(obj, "topic", "")
	e.BrokerName = jsonStringOr(obj, "brokerName", "")
	e.QueueID = jsonI32(obj, "queueId", 0)
	e.Offset = jsonI64(obj, "offset", 0)
	return nil
}

// ResetOffsetBodyForC is the array-shaped 220 body: the broker only pushes it
// when the reset initiator ran with language=CPP (Broker2Client.resetOffset).
// Java management clients always send JAVA so the map shape suffices there;
// parsing the array keeps old-C++-admin interop from silently dropping resets.
type ResetOffsetBodyForC struct {
	OffsetTable []MessageQueueForC
}

func (b *ResetOffsetBodyForC) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ResetOffsetBodyForC: expected object")
	}
	b.OffsetTable = nil
	for _, item := range jsonArray(obj, "offsetTable") {
		if _, ok := item.(map[string]any); !ok {
			continue
		}
		e := &MessageQueueForC{}
		if err := e.FromJSONValue(item); err != nil {
			return err
		}
		b.OffsetTable = append(b.OffsetTable, *e)
	}
	return nil
}

// ParseResetOffsetTable decodes a 220 body: map shape first, then the C++
// array shape (mirrors Rust parse_reset_offset_table).
func ParseResetOffsetTable(data []byte) (MQOffsetTable, error) {
	body, mapErr := DecodeResetOffsetBody(data)
	if mapErr == nil {
		return body.OffsetTable, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, mapErr
	}
	forC := &ResetOffsetBodyForC{}
	if err := forC.FromJSONValue(value); err != nil {
		return nil, mapErr
	}
	out := make(MQOffsetTable, 0, len(forC.OffsetTable))
	for _, e := range forC.OffsetTable {
		out = append(out, MQOffsetTableEntry{
			Queue:  common.NewMessageQueue(e.Topic, e.BrokerName, e.QueueID),
			Offset: e.Offset,
		})
	}
	return out, nil
}

// ---------------- GetConsumerStatusBody (221 reply) ----------------

// GetConsumerStatusBody answers GET_CONSUMER_STATUS_FROM_CLIENT. Both maps are
// MessageQueue-keyed; consumerTable's outer key is the clientId string (a
// deprecated Java field kept for old-broker compatibility).
type GetConsumerStatusBody struct {
	MessageQueueTable MQOffsetTable
	OffsetTable       MQOffsetTable
	ConsumerTable     []ConsumerStatusEntry
}

// ConsumerStatusEntry is one clientId -> offset-table entry.
type ConsumerStatusEntry struct {
	ClientID          string
	MessageQueueTable MQOffsetTable
}

func (b *GetConsumerStatusBody) ToJSONValue() map[string]any {
	consumers := make(map[string]any, len(b.ConsumerTable))
	for _, e := range b.ConsumerTable {
		consumers[e.ClientID] = EncodeMQOffsetTable(e.MessageQueueTable)
	}
	return map[string]any{
		"messageQueueTable": EncodeMQOffsetTable(b.MessageQueueTable),
		"offsetTable":       EncodeMQOffsetTable(b.OffsetTable),
		"consumerTable":     consumers,
	}
}

func (b *GetConsumerStatusBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("GetConsumerStatusBody: expected object")
	}
	table, err := DecodeMQOffsetTable(obj["messageQueueTable"])
	if err != nil {
		return err
	}
	b.MessageQueueTable = table
	if raw, ok := obj["offsetTable"]; ok {
		table, err := DecodeMQOffsetTable(raw)
		if err != nil {
			return err
		}
		b.OffsetTable = table
	}
	if consumers, ok := obj["consumerTable"].(map[string]any); ok {
		b.ConsumerTable = nil
		for cid, raw := range consumers {
			table, err := DecodeMQOffsetTable(raw)
			if err != nil {
				return err
			}
			b.ConsumerTable = append(b.ConsumerTable, ConsumerStatusEntry{
				ClientID:          cid,
				MessageQueueTable: table,
			})
		}
	}
	return nil
}

func (b *GetConsumerStatusBody) Encode() []byte { return EncodeFastJSON(b.ToJSONValue()) }

// DecodeGetConsumerStatusBody parses a GetConsumerStatusBody.
func DecodeGetConsumerStatusBody(data []byte) (*GetConsumerStatusBody, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	body := &GetConsumerStatusBody{}
	if err := body.FromJSONValue(value); err != nil {
		return nil, err
	}
	return body, nil
}
