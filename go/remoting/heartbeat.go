// Heartbeat payload beans (org.apache.rocketmq.remoting.protocol.heartbeat.*).
//
// These are the JSON bodies behind HEART_BEAT(34): one HeartbeatData carrying
// the client id plus every registered consumer's subscription set. Field names
// must match Java byte for byte — the broker's ConsumerManager looks them up by
// reflection.
//
// Edge cases that must not "simplify":
//   - SUB_ALL ("*") keeps tagsSet/codeSet EMPTY. A non-empty tagsSet is the
//     switch for the client's second-stage tag filter; filling it with "*"
//     makes a subscribe-all consumer drop every tagged message itself.
//   - "a||b" keeps the raw spacing in subString while the tags are trimmed.
//   - "|||" splits into tags {"|"} (split only drops TRAILING empty parts);
//     "||" / "||||" are errors ("subString split error").
//   - ConsumerData carries the Java 5.x field set: no 4.x consumeTimestamp /
//     maxReconsumeTimes — extra keys trip fastjson2 on the broker.
package remoting

import (
	"sort"
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Enum values as on the wire (Java enums serialize by name).
const (
	ConsumeTypeConsumePassively = "CONSUME_PASSIVELY"
	ConsumeTypeConsumeActively  = "CONSUME_ACTIVELY"

	MessageModelBroadcasting  = "BROADCASTING"
	MessageModelClustering    = "CLUSTERING"
	MessageModelLiteSelective = "LITE_SELECTIVE"

	ConsumeFromWhereLastOffset  = "CONSUME_FROM_LAST_OFFSET"
	ConsumeFromWhereFirstOffset = "CONSUME_FROM_FIRST_OFFSET"
	ConsumeFromWhereTimestamp   = "CONSUME_FROM_TIMESTAMP"

	ExpressionTypeTag         = "TAG"
	ExpressionTypeSQL92       = "SQL92"
	ExpressionTypeClassFilter = "CLASS_FILTER"
)

// ---------------------------------------------------------------- SubscriptionData

// SubscriptionData is one entry of ConsumerData.subscriptionDataSet.
//
// TagsSet/CodeSet are kept sorted and deduplicated (Java writes set members in
// sorted order). CodeSet holds Java String.hashCode() per tag — the broker
// filters by tag hash before the client's second-stage filter runs.
type SubscriptionData struct {
	ClassFilterMode bool
	Topic           string
	SubString       string
	TagsSet         []string
	CodeSet         []int32
	SubVersion      int64
	ExpressionType  string
}

// NewSubscriptionData mirrors the Java default: subVersion = now millis,
// expressionType = TAG.
func NewSubscriptionData(topic, subString string) *SubscriptionData {
	return &SubscriptionData{
		Topic:          topic,
		SubString:      subString,
		SubVersion:     common.CurrentTimeMillis(),
		ExpressionType: ExpressionTypeTag,
	}
}

// SetTags stores the tag set and its hash set, sorted and deduplicated
// (equivalent to Python's sorted(set)).
func (s *SubscriptionData) SetTags(tags []string, codes []int32) {
	s.TagsSet = sortedDedupStrings(tags)
	s.CodeSet = sortedDedupI32(codes)
}

// Equal compares every field; the struct itself is not comparable in Go
// (slice members).
func (s *SubscriptionData) Equal(other *SubscriptionData) bool {
	if s == nil || other == nil {
		return s == other
	}
	return s.ClassFilterMode == other.ClassFilterMode &&
		s.Topic == other.Topic &&
		s.SubString == other.SubString &&
		strings.Join(s.TagsSet, "\x00") == strings.Join(other.TagsSet, "\x00") &&
		i32sEqual(s.CodeSet, other.CodeSet) &&
		s.SubVersion == other.SubVersion &&
		s.ExpressionType == other.ExpressionType
}

func sortedDedupStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return dedupStrings(out)
}

func dedupStrings(sorted []string) []string {
	out := sorted[:0]
	for i, v := range sorted {
		if i == 0 || v != sorted[i-1] {
			out = append(out, v)
		}
	}
	return out
}

func sortedDedupI32(in []int32) []int32 {
	out := append([]int32(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	dedup := out[:0]
	for i, v := range out {
		if i == 0 || v != out[i-1] {
			dedup = append(dedup, v)
		}
	}
	return dedup
}

func i32sEqual(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *SubscriptionData) ToJSONValue() map[string]any {
	tags := make([]any, 0, len(s.TagsSet))
	for _, t := range s.TagsSet {
		tags = append(tags, t)
	}
	codes := make([]any, 0, len(s.CodeSet))
	for _, c := range s.CodeSet {
		codes = append(codes, c)
	}
	return map[string]any{
		"classFilterMode": s.ClassFilterMode,
		"topic":           s.Topic,
		"subString":       s.SubString,
		"tagsSet":         tags,
		"codeSet":         codes,
		"subVersion":      s.SubVersion,
		"expressionType":  s.ExpressionType,
	}
}

func (s *SubscriptionData) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("SubscriptionData: expected object")
	}
	s.ClassFilterMode = jsonBool(obj, "classFilterMode", false)
	s.Topic = jsonStringOr(obj, "topic", "")
	s.SubString = jsonStringOr(obj, "subString", "")
	tags := make([]string, 0)
	for _, item := range jsonArray(obj, "tagsSet") {
		switch t := item.(type) {
		case string:
			tags = append(tags, t)
		case JSONNumber:
			tags = append(tags, t.String())
		case bool:
			tags = append(tags, strconv.FormatBool(t))
		default:
			return decodeErrf("SubscriptionData.tagsSet item %v is not a string", item)
		}
	}
	codes := make([]int32, 0)
	for _, item := range jsonArray(obj, "codeSet") {
		switch t := item.(type) {
		case JSONNumber:
			n, err := t.Int64()
			if err != nil {
				return decodeErrf("codeSet item %v is not an int", item)
			}
			codes = append(codes, int32(n))
		case string:
			n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 32)
			if err != nil {
				return decodeErrf("codeSet item %v is not an int", item)
			}
			codes = append(codes, int32(n))
		default:
			return decodeErrf("codeSet item %v is not an int", item)
		}
	}
	// Wire order is preserved on decode (Java reads the lists as-is);
	// sorting/dedup happens only at SetTags time.
	s.TagsSet = tags
	s.CodeSet = codes
	s.SubVersion = jsonI64(obj, "subVersion", 0)
	s.ExpressionType = jsonStringOr(obj, "expressionType", ExpressionTypeTag)
	return nil
}

// ---------------------------------------------------------------- FilterAPI

// FilterAPI builds SubscriptionData from a subscription expression
// (Java FilterAPI.buildSubscriptionData / Python common.subscription_data.FilterAPI).
type FilterAPI struct{}

// FilterAPISubAll is Java FilterAPI.SUB_ALL.
const FilterAPISubAll = "*"

// BuildSubscriptionData converts a subscription expression.
//
// An empty subString means "subscribe all" (callers pass "" for Java null):
// subString normalises to "*" with BOTH sets left empty — see the package
// comment for why those sets must stay empty.
//
// " TagA || TagB " keeps the raw spacing in subString and trims each tag.
// A whitespace-only expression goes through the split branch: tags end up
// empty but subString keeps the original spacing (Java StringUtils.isEmpty
// only treats null/"" as empty). "|||" yields tags {"|"}; "||"/"||||" error.
func (FilterAPI) BuildSubscriptionData(topic, subString string) (*SubscriptionData, error) {
	sub := NewSubscriptionData(topic, subString)
	if subString == "" || subString == FilterAPISubAll {
		return normaliseSubAll(sub), nil
	}
	parts := strings.Split(subString, "||")
	// Java String.split("\\|\\|") drops TRAILING empty parts only.
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	if len(parts) == 0 {
		return nil, common.ClientError("subString split error")
	}
	var tags []string
	var codes []int32
	for _, part := range parts {
		tag := strings.TrimSpace(part)
		if tag != "" {
			tags = append(tags, tag)
			codes = append(codes, common.JavaStringHash(tag))
		}
	}
	sub.SetTags(tags, codes)
	return sub, nil
}

func normaliseSubAll(sub *SubscriptionData) *SubscriptionData {
	sub.SubString = FilterAPISubAll
	sub.TagsSet = nil
	sub.CodeSet = nil
	return sub
}

// ---------------------------------------------------------------- ProducerData

// ProducerData carries only the group name.
type ProducerData struct {
	GroupName string
}

func NewProducerData(groupName string) *ProducerData { return &ProducerData{GroupName: groupName} }

func (p *ProducerData) ToJSONValue() map[string]any {
	return map[string]any{"groupName": p.GroupName}
}

func (p *ProducerData) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ProducerData: expected object")
	}
	p.GroupName = jsonStringOr(obj, "groupName", "")
	return nil
}

// ---------------------------------------------------------------- ConsumerData

// ConsumerData carries the Java 5.x field set. There is deliberately no
// consumeTimestamp / maxReconsumeTimes (4.x leftovers): the broker's fastjson2
// rejects unknown fields.
type ConsumerData struct {
	GroupName           string
	ConsumeType         string
	MessageModel        string
	ConsumeFromWhere    string
	SubscriptionDataSet []*SubscriptionData
	UnitMode            bool
}

// NewConsumerData fills the Java defaults (passive / clustering /
// CONSUME_FROM_LAST_OFFSET).
func NewConsumerData(groupName, consumeType, messageModel, consumeFromWhere string) *ConsumerData {
	return &ConsumerData{
		GroupName:           groupName,
		ConsumeType:         consumeType,
		MessageModel:        messageModel,
		ConsumeFromWhere:    consumeFromWhere,
		SubscriptionDataSet: nil,
	}
}

// AddSubscriptionData appends unless an equal entry is already present
// (Python models the set with a dict).
func (c *ConsumerData) AddSubscriptionData(data *SubscriptionData) {
	for _, existing := range c.SubscriptionDataSet {
		if existing.Equal(data) {
			return
		}
	}
	c.SubscriptionDataSet = append(c.SubscriptionDataSet, data)
}

func (c *ConsumerData) ToJSONValue() map[string]any {
	subs := make([]any, 0, len(c.SubscriptionDataSet))
	for _, s := range c.SubscriptionDataSet {
		subs = append(subs, s.ToJSONValue())
	}
	return map[string]any{
		"groupName":           c.GroupName,
		"consumeType":         c.ConsumeType,
		"messageModel":        c.MessageModel,
		"consumeFromWhere":    c.ConsumeFromWhere,
		"subscriptionDataSet": subs,
		"unitMode":            c.UnitMode,
	}
}

func (c *ConsumerData) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ConsumerData: expected object")
	}
	c.GroupName = jsonStringOr(obj, "groupName", "")
	c.ConsumeType = jsonStringOr(obj, "consumeType", ConsumeTypeConsumePassively)
	c.MessageModel = jsonStringOr(obj, "messageModel", MessageModelClustering)
	c.ConsumeFromWhere = jsonStringOr(obj, "consumeFromWhere", ConsumeFromWhereLastOffset)
	c.SubscriptionDataSet = nil
	for _, item := range jsonArray(obj, "subscriptionDataSet") {
		sub := &SubscriptionData{}
		if err := sub.FromJSONValue(item); err != nil {
			return err
		}
		c.SubscriptionDataSet = append(c.SubscriptionDataSet, sub)
	}
	c.UnitMode = jsonBool(obj, "unitMode", false)
	return nil
}

// ---------------------------------------------------------------- HeartbeatData

// HeartbeatData is the HEART_BEAT(34) body. heartbeatFingerprint is always 0
// and withoutSub always false (the fields exist for Java compatibility).
type HeartbeatData struct {
	ClientID             string
	ProducerDataSet      []*ProducerData
	ConsumerDataSet      []*ConsumerData
	HeartbeatFingerprint int64
	WithoutSub           bool
}

func NewHeartbeatData(clientID string) *HeartbeatData {
	return &HeartbeatData{ClientID: clientID}
}

// AddProducerData dedups by group name.
func (h *HeartbeatData) AddProducerData(data *ProducerData) {
	for _, existing := range h.ProducerDataSet {
		if existing.GroupName == data.GroupName {
			return
		}
	}
	h.ProducerDataSet = append(h.ProducerDataSet, data)
}

// AddConsumerData dedups by the full ConsumerData value.
func (h *HeartbeatData) AddConsumerData(data *ConsumerData) {
	for _, existing := range h.ConsumerDataSet {
		if consumerDataEqual(existing, data) {
			return
		}
	}
	h.ConsumerDataSet = append(h.ConsumerDataSet, data)
}

func consumerDataEqual(a, b *ConsumerData) bool {
	if a.GroupName != b.GroupName || a.ConsumeType != b.ConsumeType ||
		a.MessageModel != b.MessageModel || a.ConsumeFromWhere != b.ConsumeFromWhere ||
		a.UnitMode != b.UnitMode || len(a.SubscriptionDataSet) != len(b.SubscriptionDataSet) {
		return false
	}
	for i := range a.SubscriptionDataSet {
		if !a.SubscriptionDataSet[i].Equal(b.SubscriptionDataSet[i]) {
			return false
		}
	}
	return true
}

func (h *HeartbeatData) ToJSONValue() map[string]any {
	consumers := make([]any, 0, len(h.ConsumerDataSet))
	for _, c := range h.ConsumerDataSet {
		consumers = append(consumers, c.ToJSONValue())
	}
	producers := make([]any, 0, len(h.ProducerDataSet))
	for _, p := range h.ProducerDataSet {
		producers = append(producers, p.ToJSONValue())
	}
	return map[string]any{
		"clientID":             h.ClientID,
		"consumerDataSet":      consumers,
		"heartbeatFingerprint": h.HeartbeatFingerprint,
		"producerDataSet":      producers,
		"withoutSub":           h.WithoutSub,
	}
}

func (h *HeartbeatData) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("HeartbeatData: expected object")
	}
	h.ClientID = jsonStringOr(obj, "clientID", "")
	h.HeartbeatFingerprint = jsonI64(obj, "heartbeatFingerprint", 0)
	h.WithoutSub = jsonBool(obj, "withoutSub", false)
	h.ConsumerDataSet = nil
	for _, item := range jsonArray(obj, "consumerDataSet") {
		cd := &ConsumerData{}
		if err := cd.FromJSONValue(item); err != nil {
			return err
		}
		h.ConsumerDataSet = append(h.ConsumerDataSet, cd)
	}
	h.ProducerDataSet = nil
	for _, item := range jsonArray(obj, "producerDataSet") {
		pd := &ProducerData{}
		if err := pd.FromJSONValue(item); err != nil {
			return err
		}
		h.ProducerDataSet = append(h.ProducerDataSet, pd)
	}
	return nil
}

// Encode produces the wire body (compact JSON).
func (h *HeartbeatData) Encode() []byte { return EncodeJSON(h.ToJSONValue()) }

// DecodeHeartbeatData parses a HeartbeatData body.
func DecodeHeartbeatData(data []byte) (*HeartbeatData, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	hb := &HeartbeatData{}
	if err := hb.FromJSONValue(value); err != nil {
		return nil, err
	}
	return hb, nil
}
