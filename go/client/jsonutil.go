// Tolerant JSON field readers over the remoting package's fastjson2-compatible
// decoder output (map[string]any / []any / remoting.JSONNumber).
package client

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

func decodeErrf(format string, args ...any) error {
	return common.DecodeError(fmt.Sprintf(format, args...))
}

func jsonValueString(v any, def string) string {
	switch t := v.(type) {
	case string:
		return t
	case remoting.JSONNumber:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	default:
		return def
	}
}

func jsonString(obj map[string]any, key, def string) string {
	if v, ok := obj[key]; ok {
		return jsonValueString(v, def)
	}
	return def
}

func jsonI32Field(obj map[string]any, key string, def int32) int32 {
	if v, ok := obj[key]; ok {
		if n, ok := numberAsI32(v); ok {
			return n
		}
	}
	return def
}

func numberAsI32(v any) (int32, bool) {
	switch t := v.(type) {
	case remoting.JSONNumber:
		if n, err := t.Int64(); err == nil {
			return int32(n), true
		}
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 32); err == nil {
			return int32(n), true
		}
	}
	return 0, false
}

func jsonBoolField(obj map[string]any, key string, def bool) bool {
	switch t := obj[key].(type) {
	case bool:
		return t
	case string:
		return t == "true"
	case remoting.JSONNumber:
		return t.String() != "0"
	default:
		return def
	}
}

func jsonArrayField(obj map[string]any, key string) []any {
	arr, _ := obj[key].([]any)
	return arr
}

func optStringField(obj map[string]any, key string) *string {
	v, ok := obj[key]
	if !ok || v == nil {
		return nil
	}
	switch t := v.(type) {
	case string:
		return &t
	case remoting.JSONNumber:
		s := t.String()
		return &s
	default:
		return nil
	}
}
