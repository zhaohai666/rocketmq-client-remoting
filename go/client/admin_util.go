// Small helpers shared by the DefaultMQAdminExt surface. They are kept in one
// place so the admin files read as pure wire choreography.
package client

import (
	"sort"
	"strconv"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// extInt64 reads an int64 out of a response's extFields.
func extInt64(response *remoting.RemotingCommand, key string) int64 {
	text, ok := response.ExtFields().Get(key)
	if !ok {
		return 0
	}
	v, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// adminParseInt64 parses a decimal string, reporting failure instead of
// throwing (Java would throw NumberFormatException; the ports agree on "not a
// number"). Distinct from process_queue's parseInt64, which returns an error.
func adminParseInt64(text string) (int64, bool) {
	v, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// mustDecodeObject decodes a JSON body that must be an object. A malformed or
// non-object body yields false rather than an error: several admin bodies
// (335/345/…) are unstructured and Java just hands them back as a JsonObject.
func mustDecodeObject(data []byte) (map[string]any, bool) {
	if len(data) == 0 {
		return map[string]any{}, false
	}
	value, err := remoting.ParseJSONBytes(data)
	if err != nil {
		return map[string]any{}, false
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return map[string]any{}, false
	}
	return obj, true
}

// jsonArraySafe type-asserts a decoded JSON array, tolerating a missing or
// wrongly typed field.
func jsonArraySafe(v any) []any {
	if arr, ok := v.([]any); ok {
		return arr
	}
	return nil
}

// sameDataVersion compares two decoded DataVersion objects by their rendered
// JSON (Java compares the objects, and both sides come from the same decode).
func sameDataVersion(a, b map[string]any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return string(remoting.EncodeJSON(a)) == string(remoting.EncodeJSON(b))
}

// sortByQueueOffset orders messages the way Java's MessageExt comparator does
// (ascending queueOffset), across brokers.
func sortByQueueOffset(messages []*common.MessageExt) {
	sort.SliceStable(messages, func(i, j int) bool {
		return messages[i].QueueOffset < messages[j].QueueOffset
	})
}
