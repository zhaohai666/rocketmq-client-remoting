//   - assignments == null   -> "invalid result", the client SKIPS the update
//     and keeps its previous assignment. The Go decoder maps an empty body (or
//     a body without the key) to a nil slice for this.
//   - assignments == empty  -> "clear everything assigned to me before". The
//     Go decoder maps an explicit empty array to a non-nil empty slice.
package remoting

// QueryAssignmentRequestBody is the body of QUERY_ASSIGNMENT(400).
type QueryAssignmentRequestBody struct {
	Topic         string
	ConsumerGroup string
	ClientID      string
	// StrategyName is the client's allocate strategy name ("AVG", ...). The
	// broker only knows AVG / AVG_BY_CIRCLE and answers null for anything
	// else — the client then keeps its previous assignment.
	StrategyName string
	// MessageModel is the enum NAME ("CLUSTERING" / "BROADCASTING").
	MessageModel string
}

func (b *QueryAssignmentRequestBody) ToJSONValue() map[string]any {
	return map[string]any{
		"topic":         b.Topic,
		"consumerGroup": b.ConsumerGroup,
		"clientId":      b.ClientID,
		"strategyName":  b.StrategyName,
		"messageModel":  b.MessageModel,
	}
}

func (b *QueryAssignmentRequestBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("QueryAssignmentRequestBody: expected object")
	}
	b.Topic = jsonStringOr(obj, "topic", "")
	b.ConsumerGroup = jsonStringOr(obj, "consumerGroup", "")
	b.ClientID = jsonStringOr(obj, "clientId", "")
	b.StrategyName = jsonStringOr(obj, "strategyName", "")
	b.MessageModel = jsonStringOr(obj, "messageModel", MessageModelClustering)
	return nil
}

func (b *QueryAssignmentRequestBody) Encode() []byte { return EncodeJSON(b.ToJSONValue()) }

// DecodeQueryAssignmentRequestBody parses a 400 request body (mock/test side).
func DecodeQueryAssignmentRequestBody(data []byte) (*QueryAssignmentRequestBody, error) {
	body := &QueryAssignmentRequestBody{MessageModel: MessageModelClustering}
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

// QueryAssignmentResponseBody is the reply body of QUERY_ASSIGNMENT(400).
type QueryAssignmentResponseBody struct {
	MessageQueueAssignments []*MessageQueueAssignment
}

func (b *QueryAssignmentResponseBody) ToJSONValue() map[string]any {
	set := make([]any, 0, len(b.MessageQueueAssignments))
	for _, a := range b.MessageQueueAssignments {
		set = append(set, a.ToJSONValue())
	}
	return map[string]any{"messageQueueAssignments": set}
}

func (b *QueryAssignmentResponseBody) Encode() []byte { return EncodeJSON(b.ToJSONValue()) }

func DecodeQueryAssignmentResponseBody(data []byte) ([]*MessageQueueAssignment, error) {
	if len(data) == 0 {
		return nil, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, decodeErrf("QueryAssignmentResponseBody: expected object")
	}
	raw, present := obj["messageQueueAssignments"]
	if !present || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, decodeErrf("QueryAssignmentResponseBody: expected assignment array")
	}
	out := make([]*MessageQueueAssignment, 0, len(list))
	for _, item := range list {
		a := &MessageQueueAssignment{}
		if err := a.FromJSONValue(item); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}
