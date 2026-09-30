// POP-mode bodies: the request-mode switch (SET_MESSAGE_REQUEST_MODE = 401),
// the batched acknowledgement (BATCH_ACK_MESSAGE = 200151) and the broker's
// queue assignment reply.
//
// Two encoding facts drive this file:
//
//   - BatchAck carries explicit short JSON names via @JSONField (c/t/r/so/q/rq/
//     pt/it/b), so the wire body uses THOSE, not the Java property names. The
//     decoder must still accept the long alternate names — fastjson2 writes the
//     short ones and reads either.
//   - A Java BitSet serialises through BitSetSerializerDeserializer as
//     Base64(BitSet.toByteArray()). BitSet.toByteArray() is little-endian
//     within each byte and trims trailing zero bytes, so bit N lives in byte
//     N/8 at position N%8.
package remoting

import (
	"encoding/base64"
	"strconv"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// popFirstString / popFirstI64 read the first key that is present, so a decoder
// accepts both the short @JSONField name and the long alternateName.
func popFirstString(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if _, present := obj[key]; present {
			return jsonStringOr(obj, key, "")
		}
	}
	return ""
}

func popFirstI64(obj map[string]any, keys ...string) int64 {
	for _, key := range keys {
		if _, present := obj[key]; present {
			return jsonI64(obj, key, 0)
		}
	}
	return 0
}

// ---------------------------------------------------------------- request mode

// MessageRequestMode mirrors the Java enum (PULL / POP). The wire form is the
// enum NAME, not the ordinal.
const (
	MessageRequestModePull = "PULL"
	MessageRequestModePop  = "POP"
)

// SetMessageRequestModeRequestBody is the body of SET_MESSAGE_REQUEST_MODE
// (401). The broker has no header for this code — everything rides in the body.
type SetMessageRequestModeRequestBody struct {
	Topic            string
	ConsumerGroup    string
	Mode             string
	PopShareQueueNum int32
}

func (b *SetMessageRequestModeRequestBody) ToJSONValue() map[string]any {
	mode := b.Mode
	if mode == "" {
		// Java's field default is MessageRequestMode.PULL, so a body that never
		// set the mode asks the broker to switch the group back to pull.
		mode = MessageRequestModePull
	}
	return map[string]any{
		"topic":            b.Topic,
		"consumerGroup":    b.ConsumerGroup,
		"mode":             mode,
		"popShareQueueNum": b.PopShareQueueNum,
	}
}

func (b *SetMessageRequestModeRequestBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("SetMessageRequestModeRequestBody: expected object")
	}
	b.Topic = jsonStringOr(obj, "topic", "")
	b.ConsumerGroup = jsonStringOr(obj, "consumerGroup", "")
	b.Mode = jsonStringOr(obj, "mode", MessageRequestModePull)
	b.PopShareQueueNum = jsonI32(obj, "popShareQueueNum", 0)
	return nil
}

// Encode produces the wire body (compact JSON).
func (b *SetMessageRequestModeRequestBody) Encode() []byte { return EncodeJSON(b.ToJSONValue()) }

// DecodeSetMessageRequestModeRequestBody parses a 401 body.
func DecodeSetMessageRequestModeRequestBody(data []byte) (*SetMessageRequestModeRequestBody, error) {
	body := &SetMessageRequestModeRequestBody{Mode: MessageRequestModePull}
	if len(data) == 0 {
		return body, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := body.FromJSONValue(value); err != nil {
		return nil, err
	}
	return body, nil
}

// ---------------------------------------------------------------- batch ack

// BitSet is the POP acknowledge bitmap: bit N set means "message at
// ckQueueOffset+N is acknowledged". Mirrors java.util.BitSet closely enough for
// the wire: little-endian bit order inside each byte, trailing zero bytes
// dropped, Base64 over the raw bytes.
//
// Index arithmetic is the whole point — the batch-ack builder in
// MQClientAPIImpl sets bit (queueOffset - ckQueueOffset), so an off-by-one here
// acks the wrong message and leaves the intended one to be redelivered.
type BitSet struct {
	bytes []byte
}

// Set turns bit index on, growing the backing slice as needed.
func (b *BitSet) Set(index int) {
	if index < 0 {
		return
	}
	byteIndex := index / 8
	for len(b.bytes) <= byteIndex {
		b.bytes = append(b.bytes, 0)
	}
	b.bytes[byteIndex] |= 1 << uint(index%8)
}

// Get reports whether bit index is on. Out-of-range reads are false, like Java.
func (b *BitSet) Get(index int) bool {
	if index < 0 {
		return false
	}
	byteIndex := index / 8
	if byteIndex >= len(b.bytes) {
		return false
	}
	return b.bytes[byteIndex]&(1<<uint(index%8)) != 0
}

// Bytes is the raw representation (trailing zero bytes trimmed, as
// BitSet.toByteArray does).
func (b *BitSet) Bytes() []byte {
	end := len(b.bytes)
	for end > 0 && b.bytes[end-1] == 0 {
		end--
	}
	return b.bytes[:end]
}

// EncodeBase64 mirrors BitSetSerializerDeserializer.write.
func (b *BitSet) EncodeBase64() string {
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

// DecodeBase64 mirrors BitSetSerializerDeserializer.readObject; an empty string
// yields an empty (all-zero) set rather than an error.
func (b *BitSet) DecodeBase64(encoded string) error {
	if encoded == "" {
		b.bytes = nil
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return decodeErrf("BitSet: bad base64: %v", err)
	}
	b.bytes = raw
	return nil
}

// PopCount returns the number of set bits (used by the live verifiers to
// cross-check how many messages a batch ack covered).
func (b *BitSet) PopCount() int {
	n := 0
	for _, v := range b.Bytes() {
		for v != 0 {
			n += int(v & 1)
			v >>= 1
		}
	}
	return n
}

// BatchAck mirrors Java org.apache.rocketmq.remoting.protocol.body.BatchAck.
// One entry covers every acknowledged message that shares the same
// (retry, queueId, startOffset, popTime) group.
type BatchAck struct {
	ConsumerGroup string
	Topic         string
	Retry         string
	StartOffset   int64
	QueueID       int32
	ReviveQueueID int32
	PopTime       int64
	InvisibleTime int64
	BitSet        BitSet
}

func (a *BatchAck) ToJSONValue() map[string]any {
	return map[string]any{
		"c":  a.ConsumerGroup,
		"t":  a.Topic,
		"r":  a.Retry,
		"so": a.StartOffset,
		"q":  a.QueueID,
		"rq": a.ReviveQueueID,
		"pt": a.PopTime,
		"it": a.InvisibleTime,
		"b":  a.BitSet.EncodeBase64(),
	}
}

// FromJSONValue accepts both the short names fastjson2 writes and the long
// alternateNames it can read (jsontypeInfo-style tolerance is a pattern used by
// every decoder in this package).
func (a *BatchAck) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("BatchAck: expected object")
	}
	a.ConsumerGroup = popFirstString(obj, "c", "consumerGroup")
	a.Topic = popFirstString(obj, "t", "topic")
	a.Retry = popFirstString(obj, "r", "retry")
	a.StartOffset = popFirstI64(obj, "so", "startOffset")
	a.QueueID = int32(popFirstI64(obj, "q", "queueId"))
	a.ReviveQueueID = int32(popFirstI64(obj, "rq", "reviveQueueId"))
	a.PopTime = popFirstI64(obj, "pt", "popTime")
	a.InvisibleTime = popFirstI64(obj, "it", "invisibleTime")
	encoded := popFirstString(obj, "b", "bitSet")
	if encoded != "" {
		if err := a.BitSet.DecodeBase64(encoded); err != nil {
			return err
		}
	}
	return nil
}

// BatchAckMessageRequestBody is the body of BATCH_ACK_MESSAGE (200151).
type BatchAckMessageRequestBody struct {
	BrokerName string
	Acks       []*BatchAck
}

func (b *BatchAckMessageRequestBody) ToJSONValue() map[string]any {
	acks := make([]any, 0, len(b.Acks))
	for _, a := range b.Acks {
		acks = append(acks, a.ToJSONValue())
	}
	return map[string]any{
		"brokerName": b.BrokerName,
		"acks":       acks,
	}
}

func (b *BatchAckMessageRequestBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("BatchAckMessageRequestBody: expected object")
	}
	b.BrokerName = jsonStringOr(obj, "brokerName", "")
	b.Acks = nil
	for _, item := range jsonArray(obj, "acks") {
		ack := &BatchAck{}
		if err := ack.FromJSONValue(item); err != nil {
			return err
		}
		b.Acks = append(b.Acks, ack)
	}
	return nil
}

// Encode produces the wire body (compact JSON).
func (b *BatchAckMessageRequestBody) Encode() []byte { return EncodeJSON(b.ToJSONValue()) }

// DecodeBatchAckMessageRequestBody parses a 200151 body (the broker side reads
// it, and so do the live verifiers).
func DecodeBatchAckMessageRequestBody(data []byte) (*BatchAckMessageRequestBody, error) {
	body := &BatchAckMessageRequestBody{}
	if len(data) == 0 {
		return body, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := body.FromJSONValue(value); err != nil {
		return nil, err
	}
	return body, nil
}

// BuildBatchAckMessageRequestBody mirrors the group-and-bitmap step of
// MQClientAPIImpl.batchAckMessageAsync: checkpoints sharing
// (retry, queueId, ckQueueOffset, popTime) collapse into one BatchAck, and the
// bit index is (msgQueueOffset - ckQueueOffset).
//
// topic/consumerGroup are the caller's; every other value is read back out of
// the checkpoint, never guessed.
func BuildBatchAckMessageRequestBody(topic, consumerGroup string, extraInfos []string) (*BatchAckMessageRequestBody, error) {
	body := &BatchAckMessageRequestBody{}
	index := make(map[string]*BatchAck, len(extraInfos))
	for _, extraInfo := range extraInfos {
		parts := common.SplitExtraInfo(extraInfo)
		brokerName, err := common.GetBrokerName(parts)
		if err != nil {
			return nil, err
		}
		if body.BrokerName == "" {
			body.BrokerName = brokerName
		}
		retry, err := common.GetRetry(parts)
		if err != nil {
			return nil, err
		}
		queueID, err := common.GetQueueId(parts)
		if err != nil {
			return nil, err
		}
		ckOffset, err := common.GetCkQueueOffset(parts)
		if err != nil {
			return nil, err
		}
		popTime, err := common.GetPopTime(parts)
		if err != nil {
			return nil, err
		}
		msgOffset, err := common.GetQueueOffset(parts)
		if err != nil {
			return nil, err
		}
		reviveQid, err := common.GetReviveQid(parts)
		if err != nil {
			return nil, err
		}
		invisibleTime, err := common.GetInvisibleTime(parts)
		if err != nil {
			return nil, err
		}

		mergeKey := retry + "@" + strconv.Itoa(queueID) + "@" + strconv.FormatInt(ckOffset, 10) + "@" + strconv.FormatInt(popTime, 10)
		ack, ok := index[mergeKey]
		if !ok {
			ack = &BatchAck{
				ConsumerGroup: consumerGroup,
				Topic:         topic,
				Retry:         retry,
				StartOffset:   ckOffset,
				QueueID:       int32(queueID),
				ReviveQueueID: int32(reviveQid),
				PopTime:       popTime,
				InvisibleTime: invisibleTime,
			}
			index[mergeKey] = ack
			body.Acks = append(body.Acks, ack)
		}
		ack.BitSet.Set(int(msgOffset - ckOffset))
	}
	return body, nil
}

// ---------------------------------------------------------------- assignment

// MessageQueueAssignment mirrors Java MessageQueueAssignment: one queue plus the
// mode the broker wants it consumed in. POP rebalance reads `mode` to decide
// between the pop path and the pull path.
type MessageQueueAssignment struct {
	MessageQueue common.MessageQueue
	Mode         string
	Attachments  *common.StringMap
}

// ToJSONValue renders the fastjson2 object form used inside body arrays.
func (a *MessageQueueAssignment) ToJSONValue() map[string]any {
	out := map[string]any{
		"messageQueue": MessageQueueJSONValue(a.MessageQueue),
		"mode":         a.modeOrDefault(),
	}
	if a.Attachments != nil && a.Attachments.Len() > 0 {
		att := make(map[string]any, a.Attachments.Len())
		a.Attachments.Range(func(k, v string) { att[k] = v })
		out["attachments"] = att
	}
	return out
}

func (a *MessageQueueAssignment) modeOrDefault() string {
	if a.Mode == "" {
		return MessageRequestModePull
	}
	return a.Mode
}

// FromJSONValue parses one assignment. An absent mode means PULL, matching the
// Java field initialiser — this is the branch that decides whether a queue is
// popped or pulled, so the default must not be invented.
func (a *MessageQueueAssignment) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("MessageQueueAssignment: expected object")
	}
	if raw, ok := obj["messageQueue"]; ok {
		if mq, ok := DecodeMessageQueueJSONValue(raw); ok {
			a.MessageQueue = mq
		}
	}
	a.Mode = jsonStringOr(obj, "mode", MessageRequestModePull)
	if raw, ok := obj["attachments"].(map[string]any); ok && len(raw) > 0 {
		att := common.NewStringMap()
		for k, v := range raw {
			switch t := v.(type) {
			case string:
				att.Put(k, t)
			case JSONNumber:
				att.Put(k, t.String())
			case bool:
				att.Put(k, strconv.FormatBool(t))
			}
		}
		a.Attachments = att
	}
	return nil
}
