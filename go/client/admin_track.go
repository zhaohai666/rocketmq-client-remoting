// Message track: "who consumed this message, and how far" — the
// DefaultMQAdminExtImpl.messageTrackDetail surface (Java
// tools.admin.api.TrackType / MessageTrack).
package client

import (
	"fmt"
	"sort"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// TrackType mirrors Java tools.admin.api.TrackType (the string value is the
// enum name, which is what travels on the wire).
type TrackType string

// TrackType values.
const (
	TrackConsumed            TrackType = "CONSUMED"
	TrackConsumedButFiltered TrackType = "CONSUMED_BUT_FILTERED"
	TrackPull                TrackType = "PULL"
	TrackNotConsumeYet       TrackType = "NOT_CONSUME_YET"
	TrackNotOnline           TrackType = "NOT_ONLINE"
	TrackConsumeBroadcasting TrackType = "CONSUME_BROADCASTING"
	TrackUnknown             TrackType = "UNKNOWN"
)

// MessageTrack is one consumer group's verdict for one message.
type MessageTrack struct {
	ConsumerGroup string
	TrackType     TrackType
	ExceptionDesc string
	HasException  bool
}

func (m *MessageTrack) ToJSONValue() map[string]any {
	d := map[string]any{
		"consumerGroup": m.ConsumerGroup,
		"trackType":     string(m.TrackType),
	}
	if m.HasException {
		d["exceptionDesc"] = m.ExceptionDesc
	}
	return d
}

func (m *MessageTrack) FromJSONValue(value any) error {
	m.ConsumerGroup = jsonStringOrAdmin(value, "consumerGroup")
	m.TrackType = TrackType(jsonStringOrAdmin(value, "trackType"))
	if m.TrackType == "" {
		m.TrackType = TrackUnknown
	}
	if obj, ok := value.(map[string]any); ok {
		if s, ok := obj["exceptionDesc"].(string); ok {
			m.ExceptionDesc = s
			m.HasException = true
		}
	}
	return nil
}

func (m *MessageTrack) Encode() []byte { return remoting.EncodeJSON(m.ToJSONValue()) }

// DecodeMessageTrack parses a MessageTrack body.
func DecodeMessageTrack(data []byte) (*MessageTrack, error) {
	value, err := remoting.ParseJSONBytes(data)
	if err != nil {
		return nil, err
	}
	m := &MessageTrack{}
	if err := m.FromJSONValue(value); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *MessageTrack) String() string {
	return fmt.Sprintf("MessageTrack [consumerGroup=%s, trackType=%s, exceptionDesc=%s]",
		m.ConsumerGroup, m.TrackType, m.ExceptionDesc)
}

func jsonStringOrAdmin(value any, key string) string {
	obj, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	if s, ok := obj[key].(string); ok {
		return s
	}
	return ""
}

// Consumed mirrors Java DefaultMQAdminExtImpl.consumed (:1533-1557): has this
// group's consumerOffset passed the message's queueOffset on that queue?
func (a *DefaultMQAdminExt) Consumed(msg *common.MessageExt, group string) (bool, error) {
	stats, err := a.ExamineConsumeStatsGroup(group, "")
	if err != nil {
		return false, err
	}
	clusterInfo, err := a.ExamineBrokerClusterInfo()
	if err != nil {
		return false, err
	}
	storeHost := msg.StoreHost
	for mq, wrapper := range stats.OffsetTable {
		if mq.Topic != msg.Topic || mq.QueueID != msg.QueueID {
			continue
		}
		entry := clusterInfo.BrokerAddrTable[mq.BrokerName]
		if entry == nil {
			continue
		}
		addr := entry.BrokerAddrs[int64(common.MasterID)]
		// Java normalises the master address to ip:port (convert2IpString)
		// before comparing; the addresses all four ports store are already in
		// the registered ip:port form, so compare directly.
		if addr != "" && storeHost != "" && addr == storeHost {
			if wrapper.ConsumerOffset > msg.QueueOffset {
				return true, nil
			}
		}
	}
	return false, nil
}

// MessageTrackDetail mirrors Java DefaultMQAdminExtImpl.messageTrackDetail
// (:1349-1427): ask which groups consume the topic, then classify each one.
func (a *DefaultMQAdminExt) MessageTrackDetail(msg *common.MessageExt) ([]*MessageTrack, error) {
	var result []*MessageTrack
	route, err := a.ExamineTopicRoute(msg.Topic)
	if err != nil {
		return result, err
	}
	brokerAddr := ""
	for _, bd := range route.BrokerDatas {
		if addr, ok := bd.SelectBrokerAddr(); ok {
			brokerAddr = addr
			break
		}
	}
	if brokerAddr == "" {
		return result, nil
	}
	groups, err := a.QueryTopicConsumeByWho(brokerAddr, msg.Topic)
	if err != nil {
		return result, err
	}
	// Java walks the broker's return order; the Go side gets a set here, so
	// sort for deterministic output.
	for _, group := range sortedStringSet(groups) {
		// Java's MessageTrack constructor defaults the type to UNKNOWN, so an
		// unclassified group must NOT come back as the empty string.
		mt := &MessageTrack{ConsumerGroup: group, TrackType: TrackUnknown}
		cc, err := a.ExamineConsumerConnectionInfo(group, "")
		if err != nil {
			if code, ok := responseCodeOf(err); ok && code == remoting.RespConsumerNotOnline {
				mt.TrackType = TrackNotOnline
			}
			mt.ExceptionDesc = fmt.Sprintf("CODE:%s DESC:%s", describeCode(err), err.Error())
			mt.HasException = true
			result = append(result, mt)
			continue
		}
		switch cc.ConsumeType {
		case "CONSUME_ACTIVELY":
			mt.TrackType = TrackPull
		case "CONSUME_PASSIVELY":
			ifConsumed, err := a.Consumed(msg, group)
			if err != nil {
				if code, ok := responseCodeOf(err); ok {
					switch code {
					case remoting.RespConsumerNotOnline:
						mt.TrackType = TrackNotOnline
						mt.ExceptionDesc = fmt.Sprintf("CODE:%d DESC:%s", code, err.Error())
						mt.HasException = true
					case remoting.RespNoBuyerId:
						// Java's BROADCAST_CONSUMPTION == NoBuyerId(204).
						mt.TrackType = TrackConsumeBroadcasting
					}
				}
				if !mt.HasException {
					mt.ExceptionDesc = err.Error()
					mt.HasException = true
				}
				result = append(result, mt)
				continue
			}
			if ifConsumed {
				mt.TrackType = TrackConsumed
				// Java looks the topic up in the subscription table: a
				// non-empty tagsSet that contains neither the message's tag
				// nor "*" means the subscription is narrower than the message,
				// i.e. this record was filtered out. A SQL92 subscription has
				// an empty tagsSet and therefore falls through to CONSUMED —
				// Java's semantics, kept faithfully.
				if sub := cc.SubscriptionTable[msg.Topic]; sub != nil {
					tags := stringSetOf(sub["tagsSet"])
					msgTag, _ := msg.GetTags()
					if len(tags) > 0 && !tags["*"] && !tags[msgTag] {
						mt.TrackType = TrackConsumedButFiltered
					}
				}
			} else {
				mt.TrackType = TrackNotConsumeYet
			}
		default:
			mt.TrackType = TrackUnknown
		}
		result = append(result, mt)
	}
	return result, nil
}

// describeCode renders an error's response code for the exceptionDesc text,
// or "null" when the error carries none (Java prints the Integer/null).
func describeCode(err error) string {
	if code, ok := responseCodeOf(err); ok {
		return fmt.Sprintf("%d", code)
	}
	return "null"
}

// sortedStringSet returns a set's members in sorted order.
func sortedStringSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// stringSetOf converts a decoded JSON array of strings into a set.
func stringSetOf(v any) map[string]bool {
	out := map[string]bool{}
	if arr, ok := v.([]any); ok {
		for _, item := range arr {
			if s, ok := item.(string); ok {
				out[s] = true
			}
		}
	}
	return out
}
