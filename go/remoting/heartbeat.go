// Heartbeat payload beans.
//
// These are the JSON bodies behind HEART_BEAT(34): one HeartbeatData carrying
// the client id plus every registered consumer's subscription set. Field names
// must match byte for byte — the broker's ConsumerManager looks them up by
// reflection.
//
// Edge cases that must not "simplify":
//   - SUB_ALL ("*") keeps tagsSet/codeSet EMPTY. A non-empty tagsSet is the
//     switch for the client's second-stage tag filter; filling it with "*"
//     makes a subscribe-all consumer drop every tagged message itself.
//   - "a||b" keeps the raw spacing in subString while the tags are trimmed.
//   - "|||" splits into tags {"|"} (split only drops TRAILING empty parts);
//     "||" / "||||" are errors ("subString split error").
//   - ConsumerData carries the 5.x protocol field set: no 4.x consumeTimestamp
//     / maxReconsumeTimes — extra keys trip fastjson2 on the broker.
package remoting

import (
	"sort"
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Enum values as on the wire (the broker serializes enums by name).
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
// TagsSet/CodeSet are kept sorted and deduplicated (set members go on the
// wire in sorted order). CodeSet holds the 31-based string hash per tag — the
// broker filters by tag hash before the client's second-stage filter runs.
type SubscriptionData struct {
	ClassFilterMode bool
	Topic           string
	SubString       string
	TagsSet         []string
	CodeSet         []int32
	SubVersion      int64
	ExpressionType  string
}

// NewSubscriptionData applies the protocol defaults: subVersion = now millis,
// expressionType = TAG.
func NewSubscriptionData(topic, subString string) *SubscriptionData {
	return &SubscriptionData{
		Topic:          topic,
		SubString:      subString,
		SubVersion:     common.CurrentTimeMillis(),
		ExpressionType: ExpressionTypeTag,
	}
}

// SetTags stores the tag set and its hash set, sorted and deduplicated.
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
	// Wire order is preserved on decode;
	// sorting/dedup happens only at SetTags time.
	s.TagsSet = tags
	s.CodeSet = codes
	s.SubVersion = jsonI64(obj, "subVersion", 0)
	s.ExpressionType = jsonStringOr(obj, "expressionType", ExpressionTypeTag)
	return nil
}

// ---------------------------------------------------------------- FilterAPI

// FilterAPI builds SubscriptionData from a subscription expression.
type FilterAPI struct{}

// FilterAPISubAll is the subscribe-all marker "*".
const FilterAPISubAll = "*"

// BuildSubscriptionData converts a subscription expression.
//
// An empty subString means "subscribe all" (callers pass "" when no expression
// is given):
// subString normalises to "*" with BOTH sets left empty — see the package
// comment for why those sets must stay empty.
//
// " TagA || TagB " keeps the raw spacing in subString and trims each tag.
// A whitespace-only expression goes through the split branch: tags end up
// empty but subString keeps the original spacing (only null/"" count as
// empty). "|||" yields tags {"|"}; "||"/"||||" error.
func (FilterAPI) BuildSubscriptionData(topic, subString string) (*SubscriptionData, error) {
	sub := NewSubscriptionData(topic, subString)
	if subString == "" || subString == FilterAPISubAll {
		return normaliseSubAll(sub), nil
	}
	parts := strings.Split(subString, "||")
	// Splitting on "||" drops TRAILING empty parts only.
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
			codes = append(codes, common.StringHash31(tag))
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

// ConsumerData carries the 5.x protocol field set. There is deliberately no
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

// NewConsumerData builds a ConsumerData; the protocol defaults are
// passive / clustering / CONSUME_FROM_LAST_OFFSET.
func NewConsumerData(groupName, consumeType, messageModel, consumeFromWhere string) *ConsumerData {
	return &ConsumerData{
		GroupName:           groupName,
		ConsumeType:         consumeType,
		MessageModel:        messageModel,
		ConsumeFromWhere:    consumeFromWhere,
		SubscriptionDataSet: nil,
	}
}

// AddSubscriptionData appends unless an equal entry is already present.
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

// HeartbeatData is the HEART_BEAT(34) body.
//
// heartbeatFingerprint / withoutSub are the V2 pair: both stay at their zero
// value on a V1 beat (a broker that sees fingerprint 0 takes the plain
// registration path), and are filled by the V2 path — see
// ComputeHeartbeatFingerprint and CloneWithoutSubscriptions.
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

// ---------------------------------------------- heartbeat V2 (fingerprint)

// Ext-field names the broker sets on a HEART_BEAT(34) response that understands
// the V2 body. Both travel as `true` / `false` TEXT, so a reader that defaults
// to false is correct for a broker that never heard of V2.
const (
	// ExtIsSupportHeartBeatV2 is MixAll.IS_SUPPORT_HEART_BEAT_V2.
	ExtIsSupportHeartBeatV2 = "IS_SUPPORT_HEART_BEAT_V2"
	// ExtIsSubChange is MixAll.IS_SUB_CHANGE.
	ExtIsSubChange = "IS_SUB_CHANGE"
)

// HeartbeatV2Result is what a V2 heartbeat response amounts to: the broker
// version plus the two flags that drive the client's V2 gating.
type HeartbeatV2Result struct {
	// Version is the broker's protocol version, taken off the response header
	// exactly as the V1 path takes it.
	Version int32
	// SubChange tells the broker's view of this group's fingerprint changed, so
	// the client must drop its cached fingerprint and report subscriptions
	// again.
	SubChange bool
	// SupportV2 tells whether this broker understands the V2 body at all. A
	// false here means the client must keep sending full V1-style heartbeats to
	// this address.
	SupportV2 bool
}

// DecodeHeartbeatV2Result reads the V2 flags off a heartbeat response. A
// response without ext fields (an old broker, or a stub) reads as
// SupportV2=false, which is the safe answer: the caller then keeps sending the
// subscription-carrying body.
func DecodeHeartbeatV2Result(response *RemotingCommand) HeartbeatV2Result {
	if response == nil {
		return HeartbeatV2Result{}
	}
	out := HeartbeatV2Result{Version: response.Version}
	ext := response.ExtFields()
	if ext == nil {
		return out
	}
	if v, ok := ext.Get(ExtIsSupportHeartBeatV2); ok {
		out.SupportV2 = strings.EqualFold(v, "true")
	}
	if v, ok := ext.Get(ExtIsSubChange); ok {
		out.SubChange = strings.EqualFold(v, "true")
	}
	return out
}

// CloneWithoutSubscriptions builds the `withoutSub` sibling body: the same
// consumers/producers and the same client id, but with every subscription set
// emptied and `withoutSub` set, which is what lets a V2 broker skip the
// (unchanged) subscription registration work.
//
// Nothing shared with the receiver is mutated: the ConsumerData entries are new
// values, so the caller can keep using its own body afterwards.
func (h *HeartbeatData) CloneWithoutSubscriptions() *HeartbeatData {
	out := NewHeartbeatData(h.ClientID)
	out.HeartbeatFingerprint = h.HeartbeatFingerprint
	out.WithoutSub = true
	for _, c := range h.ConsumerDataSet {
		clone := *c
		clone.SubscriptionDataSet = nil
		out.ConsumerDataSet = append(out.ConsumerDataSet, &clone)
	}
	for _, p := range h.ProducerDataSet {
		clone := *p
		out.ProducerDataSet = append(out.ProducerDataSet, &clone)
	}
	return out
}

// ComputeHeartbeatFingerprint renders the fingerprint the V2 heartbeat path
// compares between two heartbeats of the same client.
//
// The reference model hashes a JSON rendering of the body with the
// subscription-version, client-id, without-sub and fingerprint fields themselves
// stripped — those three change per heartbeat or per client and would make every
// fingerprint differ. This port applies the same normalisation and additionally
// puts the sets in a defined order, because a Go map / a repeated registration
// gives no stable iteration order and a fingerprint that depends on it would
// report a subscription change on every single beat.
//
// Consequences worth knowing:
//   - the value is STABLE for one client while its subscriptions do not change,
//     and DIFFERENT as soon as they do (that is all the protocol asks of it: the
//     broker only ever compares a group's beats, and the client caches the value
//     per broker address);
//   - it is therefore not guaranteed to be the same NUMBER another language's
//     client computes for the same subscriptions. A broker that hosts both
//     answers IS_SUB_CHANGE on the alternating beat, which costs the redundant
//     subscription registration and nothing else — behaviour stays correct.
func (h *HeartbeatData) ComputeHeartbeatFingerprint() int32 {
	return javaStringHash(h.fingerprintCanonicalText())
}

// fingerprintCanonicalText is the normalized body used by
// ComputeHeartbeatFingerprint, exposed for tests and diagnostics.
func (h *HeartbeatData) fingerprintCanonicalText() string {
	consumers := make([]*ConsumerData, 0, len(h.ConsumerDataSet))
	for _, c := range h.ConsumerDataSet {
		clone := *c
		subs := make([]*SubscriptionData, 0, len(c.SubscriptionDataSet))
		for _, s := range c.SubscriptionDataSet {
			if s == nil {
				continue
			}
			sub := *s
			// subVersion is a wall-clock stamp: never part of the fingerprint.
			sub.SubVersion = 0
			sub.TagsSet = sortedDedupStrings(s.TagsSet)
			sub.CodeSet = sortedDedupI32(s.CodeSet)
			subs = append(subs, &sub)
		}
		sort.SliceStable(subs, func(i, j int) bool { return subs[i].less(subs[j]) })
		clone.SubscriptionDataSet = subs
		consumers = append(consumers, &clone)
	}
	sort.SliceStable(consumers, func(i, j int) bool { return consumers[i].less(consumers[j]) })

	producers := make([]*ProducerData, 0, len(h.ProducerDataSet))
	for _, p := range h.ProducerDataSet {
		if p == nil {
			continue
		}
		clone := *p
		producers = append(producers, &clone)
	}
	sort.SliceStable(producers, func(i, j int) bool { return producers[i].GroupName < producers[j].GroupName })

	body := &HeartbeatData{
		// The client id identifies the SENDER, not the subscription set: an
		// instance restart with a fresh suffix must not look like a change.
		ClientID:             "",
		ProducerDataSet:      producers,
		ConsumerDataSet:      consumers,
		HeartbeatFingerprint: 0,
		WithoutSub:           false,
	}
	return string(EncodeJSON(body.ToJSONValue()))
}

func (c *ConsumerData) less(other *ConsumerData) bool {
	if c.GroupName != other.GroupName {
		return c.GroupName < other.GroupName
	}
	if c.ConsumeType != other.ConsumeType {
		return c.ConsumeType < other.ConsumeType
	}
	if c.MessageModel != other.MessageModel {
		return c.MessageModel < other.MessageModel
	}
	if c.ConsumeFromWhere != other.ConsumeFromWhere {
		return c.ConsumeFromWhere < other.ConsumeFromWhere
	}
	if c.UnitMode != other.UnitMode {
		return c.UnitMode
	}
	return len(c.SubscriptionDataSet) < len(other.SubscriptionDataSet)
}

func (s *SubscriptionData) less(other *SubscriptionData) bool {
	if s.Topic != other.Topic {
		return s.Topic < other.Topic
	}
	if s.SubString != other.SubString {
		return s.SubString < other.SubString
	}
	if s.ExpressionType != other.ExpressionType {
		return s.ExpressionType < other.ExpressionType
	}
	return strings.Join(s.TagsSet, "\x00") < strings.Join(other.TagsSet, "\x00")
}

// javaStringHash is the protocol's 31-based string accumulation
// (common.StringHash31 — the same helper the tag codeSet and the consistent-hash
// queue ring use), applied to the canonical fingerprint text. Keeping one
// implementation is deliberate: the port already fixes the reference side's
// code-unit-versus-rune question once, and a second copy here would be free to
// drift from it.
func javaStringHash(text string) int32 { return common.StringHash31(text) }
