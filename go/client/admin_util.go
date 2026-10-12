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

func adminParseInt64(text string) (int64, bool) {
	v, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

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

func sameDataVersion(a, b map[string]any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return string(remoting.EncodeJSON(a)) == string(remoting.EncodeJSON(b))
}

func sortByQueueOffset(messages []*common.MessageExt) {
	sort.SliceStable(messages, func(i, j int) bool {
		return messages[i].QueueOffset < messages[j].QueueOffset
	})
}
