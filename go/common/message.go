package common

import (
	"fmt"
	"strconv"
	"strings"
)

// WaitStoreMsgOKOf mirrors Java Message#isWaitStoreMsgOK: absent means true,
// otherwise Boolean.parseBoolean — only a case-insensitive "true" is true
// ("1" / "" / "yes" are all false). Every writer of the WAIT property
// (MessageBatch.GenerateFromList in particular) must go through this rule.
func WaitStoreMsgOKOf(value string, present bool) bool {
	if !present {
		return true
	}
	return strings.EqualFold(value, "true")
}

// MessageQueue mirrors Java MessageQueue. Used as a map key throughout the
// rebalance/offset code, hence a comparable value type.
type MessageQueue struct {
	Topic      string
	BrokerName string
	QueueID    int32
}

func NewMessageQueue(topic, brokerName string, queueID int32) MessageQueue {
	return MessageQueue{Topic: topic, BrokerName: brokerName, QueueID: queueID}
}

// JavaString mirrors Java MessageQueue#toString. Deliberately NOT the same as
// String(): the consistent-hash strategy hashes exactly this string — one
// different space and the ring is no longer the one Java clients build.
func (q MessageQueue) JavaString() string {
	return fmt.Sprintf("MessageQueue [topic=%s, brokerName=%s, queueId=%d]", q.Topic, q.BrokerName, q.QueueID)
}

// HashCode mirrors Java MessageQueue#hashCode:
// ((31 + brokerHash) * 31 + queueId) * 31 + topicHash, 32-bit wrap.
func (q MessageQueue) HashCode() int32 {
	topicHash := JavaStringHash(q.Topic)
	brokerHash := JavaStringHash(q.BrokerName)
	r := int32(31)
	r += brokerHash
	r = r*31 + q.QueueID
	return r*31 + topicHash
}

// CompareTo orders by topic -> brokerName -> queueId (Java Comparable).
func (q MessageQueue) CompareTo(other MessageQueue) int {
	if c := strings.Compare(q.Topic, other.Topic); c != 0 {
		return c
	}
	if c := strings.Compare(q.BrokerName, other.BrokerName); c != 0 {
		return c
	}
	if q.QueueID < other.QueueID {
		return -1
	}
	if q.QueueID > other.QueueID {
		return 1
	}
	return 0
}

// String is the compact log form (rust Display). For the Java toString used by
// hashing, see JavaString.
func (q MessageQueue) String() string {
	return fmt.Sprintf("%s %s %d", q.Topic, q.BrokerName, q.QueueID)
}

// Message mirrors Java Message / Python Message.
type Message struct {
	Topic string
	Flag  int32
	// Body nil equals Java body == null; encoding treats it as empty. The zero
	// Message keeps nil so validators can tell "null body" from "zero length".
	Body          []byte
	Properties    *StringMap
	TransactionID string
}

// NewMessage: a nil body is normalized to empty (Python does the same).
func NewMessage(topic string, body []byte) *Message {
	if body == nil {
		body = []byte{}
	}
	return &Message{Topic: topic, Body: body}
}

// NewMessageWithTags: empty tags/keys are NOT written to properties.
func NewMessageWithTags(topic string, body []byte, tags, keys string, flag int32) *Message {
	msg := NewMessage(topic, body)
	msg.Flag = flag
	if tags != "" {
		msg.SetTags(tags)
	}
	if keys != "" {
		msg.SetKeys(keys)
	}
	return msg
}

func (m *Message) GetBody() []byte {
	if m.Body == nil {
		return []byte{}
	}
	return m.Body
}

func (m *Message) SetBody(body []byte) {
	if body == nil {
		body = []byte{}
	}
	m.Body = body
}

func (m *Message) ensureProps() *StringMap {
	if m.Properties == nil {
		m.Properties = NewStringMap()
	}
	return m.Properties
}

func (m *Message) GetProperty(name string) (string, bool) {
	return m.Properties.Get(name)
}

func (m *Message) PutProperty(name, value string) { m.ensureProps().Put(name, value) }

func (m *Message) RemoveProperty(name string) { m.Properties.Remove(name) }

func (m *Message) ClearProperty() { m.Properties = NewStringMap() }

func (m *Message) SetTags(tags string)     { m.PutProperty(PropertyTags, tags) }
func (m *Message) GetTags() (string, bool) { return m.GetProperty(PropertyTags) }

func (m *Message) SetKeys(keys string)     { m.PutProperty(PropertyKeys, keys) }
func (m *Message) GetKeys() (string, bool) { return m.GetProperty(PropertyKeys) }

func (m *Message) SetDelayTimeLevel(level int32) {
	m.PutProperty(PropertyDelayTimeLevel, strconv.FormatInt(int64(level), 10))
}

func (m *Message) GetDelayTimeLevel() (string, bool) { return m.GetProperty(PropertyDelayTimeLevel) }

// DelayTimeLevel: missing or illegal always 0 (not a delayed message).
func (m *Message) DelayTimeLevel() int32 {
	if raw, ok := m.GetDelayTimeLevel(); ok {
		if v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32); err == nil {
			return int32(v)
		}
	}
	return 0
}

// SetDelayTimeSec is Java Message#setDelayTimeSec (5.x timer wheel): the
// message is delivered `sec` seconds from now, expressed through the
// TIMER_DELAY_SEC property alias of DELAY.
func (m *Message) SetDelayTimeSec(sec int64) {
	m.PutProperty(PropertyTimerDelaySec, strconv.FormatInt(sec, 10))
}

func (m *Message) GetDelayTimeSec() (string, bool) { return m.GetProperty(PropertyTimerDelaySec) }

// SetDelayTimeMs is Java Message#setDelayTimeMs: delivered `ms` milliseconds
// from now (TIMER_DELAY_MS).
func (m *Message) SetDelayTimeMs(ms int64) {
	m.PutProperty(PropertyTimerDelayMs, strconv.FormatInt(ms, 10))
}

func (m *Message) GetDelayTimeMs() (string, bool) { return m.GetProperty(PropertyTimerDelayMs) }

// SetDeliverTimeMs is Java Message#setDeliverTimeMs: delivered AT the given
// wall-clock timestamp (TIMER_DELIVER_MS).
func (m *Message) SetDeliverTimeMs(ms int64) {
	m.PutProperty(PropertyTimerDeliverMs, strconv.FormatInt(ms, 10))
}

func (m *Message) GetDeliverTimeMs() (string, bool) { return m.GetProperty(PropertyTimerDeliverMs) }

func (m *Message) SetWaitStoreMsgOK(ok bool) {
	if ok {
		m.PutProperty(PropertyWaitStoreMsgOK, "true")
	} else {
		m.PutProperty(PropertyWaitStoreMsgOK, "false")
	}
}

func (m *Message) GetWaitStoreMsgOK() (string, bool) { return m.GetProperty(PropertyWaitStoreMsgOK) }

// IsWaitStoreMsgOK implements the Java parse rule; do NOT write
// `value == "true"` — Message.NewMessage never pre-writes WAIT, so "absent" is
// the common case, and treating it as false would send ordinary batches with
// WAIT=false (broker replies SEND_OK without waiting for the flush).
func (m *Message) IsWaitStoreMsgOK() bool {
	v, ok := m.GetWaitStoreMsgOK()
	return WaitStoreMsgOKOf(v, ok)
}

func (m *Message) SetUserProperty(name, value string) { m.PutProperty(name, value) }
func (m *Message) GetUserProperty(name string) (string, bool) {
	return m.GetProperty(name)
}

func (m *Message) SetProperties(properties *StringMap) { m.Properties = properties }

// Clone deep-copies the body and the property map.
func (m *Message) Clone() *Message {
	out := &Message{Topic: m.Topic, Flag: m.Flag, TransactionID: m.TransactionID}
	if m.Body != nil {
		out.Body = append([]byte(nil), m.Body...)
	}
	out.Properties = m.Properties.Clone()
	return out
}

func (m *Message) String() string {
	return fmt.Sprintf("Message(topic='%s', body=%d bytes)", m.Topic, len(m.GetBody()))
}

// MessageExt mirrors Java MessageExt (a pulled message). Embeds Message so
// topic/flag/body/properties accessors promote.
type MessageExt struct {
	Message

	QueueID                   int32
	StoreSize                 int32
	QueueOffset               int64
	SysFlag                   int32
	BornTimestamp             int64
	BornHost                  string
	BornHostPort              uint32
	StoreTimestamp            int64
	StoreHost                 string
	StoreHostPort             uint32
	MsgID                     string
	CommitLogOffset           int64
	BodyCRC                   uint32
	ReconsumeTimes            int32
	PreparedTransactionOffset int64
	BrokerName                string
	OffsetMsgID               string
	MsgType                   string
}

func NewMessageExt() *MessageExt {
	return &MessageExt{Message: *NewMessage("", nil)}
}

// ExtFromMessage derives an ext from a to-send message (the fields Java
// MessageExt inherits from Message).
func ExtFromMessage(msg *Message) *MessageExt {
	return &MessageExt{
		Message: Message{
			Topic:         msg.Topic,
			Flag:          msg.Flag,
			Body:          msg.Body,
			Properties:    msg.Properties.Clone(),
			TransactionID: msg.TransactionID,
		},
	}
}

// ToMessage strips back to a plain message (retry/forward paths only need the
// body and the properties).
func (e *MessageExt) ToMessage() *Message {
	return &Message{
		Topic:         e.Topic,
		Flag:          e.Flag,
		Body:          e.Body,
		Properties:    e.Properties.Clone(),
		TransactionID: e.TransactionID,
	}
}

func (e *MessageExt) GetProperty(name string) (string, bool) { return e.Message.GetProperty(name) }

// BornHostString: empty host yields "", port 0 yields the bare host.
func (e *MessageExt) BornHostString() string { return hostWithPort(e.BornHost, e.BornHostPort) }

func (e *MessageExt) StoreHostString() string { return hostWithPort(e.StoreHost, e.StoreHostPort) }

func hostWithPort(host string, port uint32) string {
	if host == "" {
		return ""
	}
	if port == 0 {
		return host
	}
	return fmt.Sprintf("%s:%d", host, port)
}

func (e *MessageExt) String() string {
	return fmt.Sprintf("MessageExt(topic='%s', msgId='%s', queueOffset=%d, body=%d bytes)",
		e.Topic, e.MsgID, e.QueueOffset, len(e.GetBody()))
}

// MessageBatch mirrors Java MessageBatch: no wire fields of its own — the body
// is the concatenation of per-message 6-segment light frames (EncodeMessage).
type MessageBatch struct {
	Message  *Message
	Messages []*Message
}

func (b *MessageBatch) Encode() []byte { return EncodeMessages(b.Messages) }

// GenerateFromList mirrors Java MessageBatch.generateFromList. Constraints:
// non-empty; same topic; same waitStoreMsgOK; no delayed messages; no retry
// topics. Java throws UnsupportedOperationException/IllegalArgumentException,
// Python ValueError — here all map to a client Error.
func GenerateFromList(messages []*Message) (*MessageBatch, error) {
	if len(messages) == 0 {
		return nil, ClientError("messages must not be null or empty")
	}
	first := messages[0]
	for _, message := range messages {
		if raw, ok := message.GetDelayTimeLevel(); ok {
			parsed, _ := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
			if raw != "" && parsed > 0 {
				return nil, ClientError("Delayed messages are not supported for batching")
			}
		}
		if strings.HasPrefix(message.Topic, RetryGroupTopicPrefix) {
			return nil, ClientError("Retry Group is not supported for batching")
		}
	}
	for _, message := range messages[1:] {
		if first.Topic != message.Topic {
			return nil, ClientError("The topic of the messages in one batch should be the same")
		}
		v1, ok1 := first.GetWaitStoreMsgOK()
		v2, ok2 := message.GetWaitStoreMsgOK()
		if v1 != v2 || ok1 != ok2 {
			return nil, ClientError("The waitStoreMsgOK of the messages in one batch should be the same")
		}
	}

	topic := first.Topic
	// Java generateFromList: batch.setWaitStoreMsgOK(first.isWaitStoreMsgOK()) —
	// absent means true. Comparing raw property values would flip ordinary
	// messages (WAIT never set) to WAIT=false and silently downgrade
	// persistence.
	waitStoreMsgOK := first.IsWaitStoreMsgOK()

	batch := &MessageBatch{Message: &Message{}, Messages: messages}
	batch.Message.Topic = topic
	batch.Message.SetWaitStoreMsgOK(waitStoreMsgOK)
	batch.Message.Body = batch.Encode()
	return batch, nil
}
