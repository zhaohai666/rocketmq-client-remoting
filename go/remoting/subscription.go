// Subscription-group models, mirroring
// org.apache.rocketmq.remoting.protocol.subscription.*:
// SubscriptionGroupConfig / GroupRetryPolicy / SimpleSubscriptionData and the
// SubscriptionGroupWrapper body.
//
// Field names and defaults come from the Java 5.x probe
// (`JSON.toJSONString(new SubscriptionGroupConfig())`):
//
//	{"attributes":{},"brokerId":0,"consumeBroadcastEnable":true,"consumeEnable":true,
//	 "consumeFromMinEnable":true,"consumeMessageOrderly":false,"consumeTimeoutMinute":15,
//	 "groupName":"MyGroup","groupRetryPolicy":{"type":"CUSTOMIZED"},"groupSysFlag":0,
//	 "notifyConsumerIdsChangedEnable":true,"retryMaxTimes":16,"retryQueueNums":1,
//	 "whichBrokerWhenConsumeSlowly":1}
//
// fastjson2 SKIPS null fields, so `subscriptionDataSet` disappears entirely
// when nil — every reader therefore falls back to a default.
package remoting

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// GroupRetryPolicyType mirrors Java GroupRetryPolicyType.
const (
	GroupRetryPolicyExponential = "EXPONENTIAL"
	GroupRetryPolicyCustomized  = "CUSTOMIZED"
)

// GroupRetryPolicy is the simplified form: `type` plus the two sub-policies
// kept as raw maps (Java leaves them null by default, so they are only
// serialised once explicitly set).
type GroupRetryPolicy struct {
	Type                   string
	ExponentialRetryPolicy map[string]any
	CustomizedRetryPolicy  map[string]any
}

// NewGroupRetryPolicy builds the Java default (type=CUSTOMIZED, both
// sub-policies null).
func NewGroupRetryPolicy() *GroupRetryPolicy {
	return &GroupRetryPolicy{Type: GroupRetryPolicyCustomized}
}

func (g *GroupRetryPolicy) ToJSONValue() map[string]any {
	d := map[string]any{"type": g.Type}
	if g.ExponentialRetryPolicy != nil {
		d["exponentialRetryPolicy"] = g.ExponentialRetryPolicy
	}
	if g.CustomizedRetryPolicy != nil {
		d["customizedRetryPolicy"] = g.CustomizedRetryPolicy
	}
	return d
}

func (g *GroupRetryPolicy) FromJSONValue(value any) error {
	g.Type = jsonStringOr(value, "type", GroupRetryPolicyCustomized)
	if obj, ok := value.(map[string]any); ok {
		if sub, ok := obj["exponentialRetryPolicy"].(map[string]any); ok {
			g.ExponentialRetryPolicy = sub
		}
		if sub, ok := obj["customizedRetryPolicy"].(map[string]any); ok {
			g.CustomizedRetryPolicy = sub
		}
	}
	return nil
}

// SimpleSubscriptionData mirrors subscription.SimpleSubscriptionData.
type SimpleSubscriptionData struct {
	Topic          string
	ExpressionType string
	Expression     string
	Version        int64
}

func (s *SimpleSubscriptionData) ToJSONValue() map[string]any {
	return map[string]any{
		"topic":          s.Topic,
		"expressionType": s.ExpressionType,
		"expression":     s.Expression,
		"version":        s.Version,
	}
}

func (s *SimpleSubscriptionData) FromJSONValue(value any) error {
	s.Topic = jsonStringOr(value, "topic", "")
	s.ExpressionType = jsonStringOr(value, "expressionType", "TAG")
	s.Expression = jsonStringOr(value, "expression", "*")
	s.Version = jsonI64(value, "version", 0)
	return nil
}

// SubscriptionGroupConfig mirrors subscription.SubscriptionGroupConfig.
type SubscriptionGroupConfig struct {
	GroupName                      string
	ConsumeEnable                  bool
	ConsumeFromMinEnable           bool
	ConsumeBroadcastEnable         bool
	ConsumeMessageOrderly          bool
	RetryQueueNums                 int32
	RetryMaxTimes                  int32
	GroupRetryPolicy               *GroupRetryPolicy
	BrokerID                       int64
	WhichBrokerWhenConsumeSlowly   int64
	NotifyConsumerIdsChangedEnable bool
	GroupSysFlag                   int32
	ConsumeTimeoutMinute           int32
	SubscriptionDataSet            []*SimpleSubscriptionData
	Attributes                     map[string]string
}

// NewSubscriptionGroupConfig builds a config with Java's defaults.
func NewSubscriptionGroupConfig(groupName string) *SubscriptionGroupConfig {
	return &SubscriptionGroupConfig{
		GroupName:                      groupName,
		ConsumeEnable:                  true,
		ConsumeFromMinEnable:           true,
		ConsumeBroadcastEnable:         true,
		ConsumeMessageOrderly:          false,
		RetryQueueNums:                 1,
		RetryMaxTimes:                  16,
		GroupRetryPolicy:               NewGroupRetryPolicy(),
		BrokerID:                       int64(common.MasterID),
		WhichBrokerWhenConsumeSlowly:   1,
		NotifyConsumerIdsChangedEnable: true,
		GroupSysFlag:                   0,
		ConsumeTimeoutMinute:           15,
		Attributes:                     map[string]string{},
	}
}

func (c *SubscriptionGroupConfig) ToJSONValue() map[string]any {
	d := map[string]any{
		"groupName":                      c.GroupName,
		"consumeEnable":                  c.ConsumeEnable,
		"consumeFromMinEnable":           c.ConsumeFromMinEnable,
		"consumeBroadcastEnable":         c.ConsumeBroadcastEnable,
		"consumeMessageOrderly":          c.ConsumeMessageOrderly,
		"retryQueueNums":                 c.RetryQueueNums,
		"retryMaxTimes":                  c.RetryMaxTimes,
		"brokerId":                       c.BrokerID,
		"whichBrokerWhenConsumeSlowly":   c.WhichBrokerWhenConsumeSlowly,
		"notifyConsumerIdsChangedEnable": c.NotifyConsumerIdsChangedEnable,
		"groupSysFlag":                   c.GroupSysFlag,
		"consumeTimeoutMinute":           c.ConsumeTimeoutMinute,
	}
	// fastjson2 drops nulls: only emit the key when there IS a retry policy.
	// Java's default constructor initialises it, but setGroupRetryPolicy(null)
	// is legal and a hand-built config can simply leave it nil — dereferencing
	// that used to panic the whole request (found by live_admin).
	if c.GroupRetryPolicy != nil {
		d["groupRetryPolicy"] = c.GroupRetryPolicy.ToJSONValue()
	}
	attrs := make(map[string]any, len(c.Attributes))
	for k, v := range c.Attributes {
		attrs[k] = v
	}
	d["attributes"] = attrs
	// fastjson2 drops nulls: only emit the key when there IS a subscription set.
	if c.SubscriptionDataSet != nil {
		subs := make([]any, 0, len(c.SubscriptionDataSet))
		for _, s := range c.SubscriptionDataSet {
			subs = append(subs, s.ToJSONValue())
		}
		d["subscriptionDataSet"] = subs
	}
	return d
}

func (c *SubscriptionGroupConfig) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		if value == nil {
			return decodeErrf("SubscriptionGroupConfig: expected object, got nil")
		}
		return decodeErrf("SubscriptionGroupConfig: expected object")
	}
	c.GroupName = jsonStringOr(obj, "groupName", "")
	c.ConsumeEnable = jsonBool(obj, "consumeEnable", true)
	c.ConsumeFromMinEnable = jsonBool(obj, "consumeFromMinEnable", true)
	c.ConsumeBroadcastEnable = jsonBool(obj, "consumeBroadcastEnable", true)
	c.ConsumeMessageOrderly = jsonBool(obj, "consumeMessageOrderly", false)
	c.RetryQueueNums = jsonI32(obj, "retryQueueNums", 1)
	c.RetryMaxTimes = jsonI32(obj, "retryMaxTimes", 16)
	c.GroupRetryPolicy = NewGroupRetryPolicy()
	if err := c.GroupRetryPolicy.FromJSONValue(obj["groupRetryPolicy"]); err != nil {
		return err
	}
	c.BrokerID = jsonI64(obj, "brokerId", int64(common.MasterID))
	c.WhichBrokerWhenConsumeSlowly = jsonI64(obj, "whichBrokerWhenConsumeSlowly", 1)
	c.NotifyConsumerIdsChangedEnable = jsonBool(obj, "notifyConsumerIdsChangedEnable", true)
	c.GroupSysFlag = jsonI32(obj, "groupSysFlag", 0)
	c.ConsumeTimeoutMinute = jsonI32(obj, "consumeTimeoutMinute", 15)
	if raw := jsonArray(obj, "subscriptionDataSet"); len(raw) > 0 {
		c.SubscriptionDataSet = make([]*SimpleSubscriptionData, 0, len(raw))
		for _, item := range raw {
			s := &SimpleSubscriptionData{}
			if err := s.FromJSONValue(item); err != nil {
				return err
			}
			c.SubscriptionDataSet = append(c.SubscriptionDataSet, s)
		}
	} else {
		c.SubscriptionDataSet = nil
	}
	c.Attributes = jsonStringMapOr(obj, "attributes")
	return nil
}

// Encode renders the JSON body UPDATE_AND_CREATE_SUBSCRIPTIONGROUP(200) wants.
func (c *SubscriptionGroupConfig) Encode() []byte { return EncodeJSON(c.ToJSONValue()) }

// DecodeSubscriptionGroupConfig parses a SubscriptionGroupConfig body.
func DecodeSubscriptionGroupConfig(data []byte) (*SubscriptionGroupConfig, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	cfg := &SubscriptionGroupConfig{}
	if err := cfg.FromJSONValue(value); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *SubscriptionGroupConfig) String() string {
	return fmt.Sprintf("SubscriptionGroupConfig[groupName=%s, retryQueueNums=%d, brokerId=%d]",
		c.GroupName, c.RetryQueueNums, c.BrokerID)
}

// SubscriptionGroupWrapper mirrors body.SubscriptionGroupWrapper.
// Probe output: {"dataVersion":{...},"forbiddenTable":{},"subscriptionGroupTable":{...}}
type SubscriptionGroupWrapper struct {
	SubscriptionGroupTable map[string]*SubscriptionGroupConfig
	ForbiddenTable         map[string]any
	DataVersion            map[string]any
}

// NewSubscriptionGroupWrapper builds an empty wrapper.
func NewSubscriptionGroupWrapper() *SubscriptionGroupWrapper {
	return &SubscriptionGroupWrapper{
		SubscriptionGroupTable: map[string]*SubscriptionGroupConfig{},
		ForbiddenTable:         map[string]any{},
		DataVersion:            map[string]any{},
	}
}

func (w *SubscriptionGroupWrapper) ToJSONValue() map[string]any {
	table := make(map[string]any, len(w.SubscriptionGroupTable))
	for k, v := range w.SubscriptionGroupTable {
		table[k] = v.ToJSONValue()
	}
	return map[string]any{
		"dataVersion":            w.DataVersion,
		"forbiddenTable":         w.ForbiddenTable,
		"subscriptionGroupTable": table,
	}
}

func (w *SubscriptionGroupWrapper) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		if value == nil {
			return nil
		}
		return decodeErrf("SubscriptionGroupWrapper: expected object")
	}
	w.SubscriptionGroupTable = map[string]*SubscriptionGroupConfig{}
	if raw, ok := obj["subscriptionGroupTable"].(map[string]any); ok {
		for name := range raw {
			cfg := &SubscriptionGroupConfig{}
			if err := cfg.FromJSONValue(raw[name]); err != nil {
				return err
			}
			w.SubscriptionGroupTable[name] = cfg
		}
	}
	w.ForbiddenTable = map[string]any{}
	if raw, ok := obj["forbiddenTable"].(map[string]any); ok {
		w.ForbiddenTable = raw
	}
	w.DataVersion = map[string]any{}
	if raw, ok := obj["dataVersion"].(map[string]any); ok {
		w.DataVersion = raw
	}
	return nil
}

func (w *SubscriptionGroupWrapper) Encode() []byte { return EncodeJSON(w.ToJSONValue()) }

// DecodeSubscriptionGroupWrapper parses a SubscriptionGroupWrapper body.
func DecodeSubscriptionGroupWrapper(data []byte) (*SubscriptionGroupWrapper, error) {
	w := NewSubscriptionGroupWrapper()
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

// PredefinedGroups mirrors Java MixAll.PREDEFINE_GROUP_SET.
//
// Keep this byte-for-byte with the Java/Python sets — getUserSubscriptionGroup
// filters with it, so an extra entry silently hides a real user group from the
// admin tools.
var PredefinedGroups = map[string]bool{
	"DEFAULT_CONSUMER":      true,
	"DEFAULT_PRODUCER":      true,
	"TOOLS_CONSUMER":        true,
	"SCHEDULE_CONSUMER":     true,
	"FILTERSRV_CONSUMER":    true,
	"__MONITOR_CONSUMER":    true,
	"CLIENT_INNER_PRODUCER": true,
	"SELF_TEST_P_GROUP":     true,
	"SELF_TEST_C_GROUP":     true,
	"CID_ONS-HTTP-PROXY":    true,
	"CID_ONSAPI_PERMISSION": true,
	"CID_ONSAPI_OWNER":      true,
	"CID_ONSAPI_PULL":       true,
	"CID_SYS_RMQ_TRANS":     true,
}

// IsPredefinedGroup mirrors Java MixAll.isPredefinedGroup.
func IsPredefinedGroup(group string) bool { return PredefinedGroups[group] }

// IsSysConsumerGroupAdmin is the admin-side view of "system group": the
// CID_RMQ_SYS_ prefix or a predefined group name.
func IsSysConsumerGroupAdmin(group string) bool {
	return common.IsSysConsumerGroup(group) || IsPredefinedGroup(group)
}
