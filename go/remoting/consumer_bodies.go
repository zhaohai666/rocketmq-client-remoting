// Consumer-side RPC bodies that were not needed until the push consumer
// landed:
//
//   - LockBatchRequestBody / UnlockBatchRequestBody (LOCK_BATCH_MQ = 41 /
//     UNLOCK_BATCH_MQ = 42): the *body* carries the queue set and the group,
//     the header carries only group + clientId. Java sends both; the broker
//     reads both, so leaving either out silently locks nothing.
//   - LockBatchResponseBody (field lockOKMQSet): the queues the broker
//     actually granted. Only these may be pulled by an orderly consumer.
//   - GetConsumerListByGroupResponseBody (field consumerIdList): the rebalance
//     input — "who else is in my group".
//
// JSON field names are Java property names (fastjson2 deserialises by them);
// MessageQueue elements serialise with the same alphabetical
// brokerName/queueId/topic order the offset tables use.
package remoting

import (
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// MessageQueueJSONValue renders one MessageQueue as the fastjson2 object form.
func MessageQueueJSONValue(q common.MessageQueue) map[string]any {
	return map[string]any{
		"brokerName": q.BrokerName,
		"queueId":    q.QueueID,
		"topic":      q.Topic,
	}
}

// DecodeMessageQueueJSONValue parses one MessageQueue object. Tolerant on the
// numeric field like every other decoder in this package.
func DecodeMessageQueueJSONValue(value any) (common.MessageQueue, bool) {
	obj, ok := value.(map[string]any)
	if !ok {
		return common.MessageQueue{}, false
	}
	return common.NewMessageQueue(
		jsonStringOr(obj, "topic", ""),
		jsonStringOr(obj, "brokerName", ""),
		jsonI32(obj, "queueId", 0),
	), true
}

// ---------------------------------------------------------------- lock batch

// LockBatchRequestBody is the body of LOCK_BATCH_MQ(41).
type LockBatchRequestBody struct {
	ConsumerGroup *string
	ClientID      *string
	MQSet         []common.MessageQueue
}

func (b *LockBatchRequestBody) ToJSONValue() map[string]any {
	set := make([]any, 0, len(b.MQSet))
	for _, q := range b.MQSet {
		set = append(set, MessageQueueJSONValue(q))
	}
	return map[string]any{
		"consumerGroup": strOrNil(b.ConsumerGroup),
		"clientId":      strOrNil(b.ClientID),
		"mqSet":         set,
	}
}

func (b *LockBatchRequestBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("LockBatchRequestBody: expected object")
	}
	b.ConsumerGroup = optString(obj, "consumerGroup")
	b.ClientID = optString(obj, "clientId")
	b.MQSet = decodeMQSet(obj["mqSet"])
	return nil
}

// Encode produces the wire body (compact JSON).
func (b *LockBatchRequestBody) Encode() []byte { return EncodeJSON(b.ToJSONValue()) }

// DecodeLockBatchRequestBody parses a LockBatchRequestBody.
func DecodeLockBatchRequestBody(data []byte) (*LockBatchRequestBody, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	body := &LockBatchRequestBody{}
	if err := body.FromJSONValue(value); err != nil {
		return nil, err
	}
	return body, nil
}

// UnlockBatchRequestBody is the body of UNLOCK_BATCH_MQ(42). Same shape.
type UnlockBatchRequestBody struct {
	ConsumerGroup *string
	ClientID      *string
	MQSet         []common.MessageQueue
}

func (b *UnlockBatchRequestBody) ToJSONValue() map[string]any {
	inner := &LockBatchRequestBody{ConsumerGroup: b.ConsumerGroup, ClientID: b.ClientID, MQSet: b.MQSet}
	return inner.ToJSONValue()
}

func (b *UnlockBatchRequestBody) FromJSONValue(value any) error {
	inner := &LockBatchRequestBody{}
	if err := inner.FromJSONValue(value); err != nil {
		return err
	}
	b.ConsumerGroup, b.ClientID, b.MQSet = inner.ConsumerGroup, inner.ClientID, inner.MQSet
	return nil
}

// Encode produces the wire body (compact JSON).
func (b *UnlockBatchRequestBody) Encode() []byte { return EncodeJSON(b.ToJSONValue()) }

// LockBatchResponseBody is the reply of both 41 and 42 (Java reuses the bean);
// only lockOKMQSet matters to the client.
type LockBatchResponseBody struct {
	LockOKMQSet []common.MessageQueue
}

func (b *LockBatchResponseBody) ToJSONValue() map[string]any {
	set := make([]any, 0, len(b.LockOKMQSet))
	for _, q := range b.LockOKMQSet {
		set = append(set, MessageQueueJSONValue(q))
	}
	return map[string]any{"lockOKMQSet": set}
}

func (b *LockBatchResponseBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("LockBatchResponseBody: expected object")
	}
	b.LockOKMQSet = decodeMQSet(obj["lockOKMQSet"])
	return nil
}

// DecodeLockBatchResponseBody parses the 41/42 reply. An empty body is a valid
// "nothing was locked" answer, not an error.
func DecodeLockBatchResponseBody(data []byte) (*LockBatchResponseBody, error) {
	body := &LockBatchResponseBody{}
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

func decodeMQSet(raw any) []common.MessageQueue {
	arr, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]common.MessageQueue, 0, len(arr))
	for _, item := range arr {
		if q, ok := DecodeMessageQueueJSONValue(item); ok {
			out = append(out, q)
		}
	}
	return out
}

// ---------------------------------------------------------------- consumer list

// GetConsumerListByGroupResponseBody is the 38 reply. Java's real bean also has
// a consumerIdList; it is the only field the client reads.
type GetConsumerListByGroupResponseBody struct {
	ConsumerIDList []string
}

func (b *GetConsumerListByGroupResponseBody) ToJSONValue() map[string]any {
	ids := make([]any, 0, len(b.ConsumerIDList))
	for _, id := range b.ConsumerIDList {
		ids = append(ids, id)
	}
	return map[string]any{"consumerIdList": ids}
}

func (b *GetConsumerListByGroupResponseBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("GetConsumerListByGroupResponseBody: expected object")
	}
	b.ConsumerIDList = nil
	for _, item := range jsonArray(obj, "consumerIdList") {
		switch t := item.(type) {
		case string:
			b.ConsumerIDList = append(b.ConsumerIDList, t)
		case JSONNumber:
			b.ConsumerIDList = append(b.ConsumerIDList, t.String())
		}
	}
	return nil
}

// DecodeGetConsumerListByGroupResponseBody parses the 38 reply. An empty body
// is an empty list (Java answers SUCCESS with no body when the group has no
// online consumer; the client must not treat that as an error).
func DecodeGetConsumerListByGroupResponseBody(data []byte) (*GetConsumerListByGroupResponseBody, error) {
	body := &GetConsumerListByGroupResponseBody{}
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
