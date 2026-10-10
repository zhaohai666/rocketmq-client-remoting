// A minimal fastjson2-compatible JSON writer for OUTBOUND bodies.
//
// Why this exists: fastjson2 writes a MessageQueue-keyed map with the queue as
// a raw INLINE OBJECT key —
//
//	{"mqTable":{{"brokerName":"b","queueId":3,"topic":"T"}:{...}}}
//
// — and that byte stream is not valid JSON. It therefore cannot be produced
// through encoding/json at all: the stdlib validates the output of every
// json.Marshaler it calls and rejects the unquoted `{` ("invalid character '{'
// looking for beginning of object key string"). Letting the stdlib escape the
// key instead is not an option either, because fastjson2 REJECTS the escaped
// spelling for a `Map<MessageQueue, X>` field (verified against the 5.5.1
// jars — see mqKeyedJSON in bodies.go for the transcript).
//
// So bodies whose payload contains an mqKeyedJSON are serialised by this
// writer. Everything else keeps going through EncodeJSON, which is why the
// change stays contained: only the five bodies that actually carry a
// MessageQueue-keyed table are affected.
//
// The writer deliberately mirrors encoding/json's decisions elsewhere so the
// output stays byte-identical to the previous encoder apart from the keys:
//   - object keys are sorted (Java's TreeMap order for the fields that care;
//     the stdlib sorted them too, so nothing else changes)
//   - strings use encodeJSONNoEscape, i.e. no HTML escaping of < > &
//   - float64 is rendered by FormatJavaDouble (Java's Double.toString layout)
//   - anything unrecognised falls back to encodeJSONNoEscape
package remoting

import (
	"bytes"
	"reflect"
	"sort"
	"strconv"
)

// EncodeFastJSON renders a body value with fastjson2's inline-object map keys.
// Returns nil on failure, matching EncodeJSON.
func EncodeFastJSON(v any) []byte {
	var buf bytes.Buffer
	if err := writeFastJSON(&buf, v); err != nil {
		return nil
	}
	return buf.Bytes()
}

func writeFastJSON(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case nil:
		buf.WriteString("null")
		return nil
	case bool:
		if t {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
		return nil
	case string:
		return writeFastJSONString(buf, t)
	case JavaDouble:
		buf.WriteString(FormatJavaDouble(float64(t)))
		return nil
	case float64:
		buf.WriteString(FormatJavaDouble(t))
		return nil
	case float32:
		buf.WriteString(FormatJavaDouble(float64(t)))
		return nil
	case JSONNumber:
		// A raw literal that came off the wire; re-emit it unchanged.
		buf.WriteString(string(t))
		return nil
	case int:
		buf.WriteString(strconv.FormatInt(int64(t), 10))
		return nil
	case int8:
		buf.WriteString(strconv.FormatInt(int64(t), 10))
		return nil
	case int16:
		buf.WriteString(strconv.FormatInt(int64(t), 10))
		return nil
	case int32:
		buf.WriteString(strconv.FormatInt(int64(t), 10))
		return nil
	case int64:
		buf.WriteString(strconv.FormatInt(t, 10))
		return nil
	case uint:
		buf.WriteString(strconv.FormatUint(uint64(t), 10))
		return nil
	case uint8:
		buf.WriteString(strconv.FormatUint(uint64(t), 10))
		return nil
	case uint16:
		buf.WriteString(strconv.FormatUint(uint64(t), 10))
		return nil
	case uint32:
		buf.WriteString(strconv.FormatUint(uint64(t), 10))
		return nil
	case uint64:
		buf.WriteString(strconv.FormatUint(t, 10))
		return nil
	case mqKeyedJSON:
		return writeMQKeyedJSON(buf, t)
	}

	rv := reflect.ValueOf(v)
	return writeFastJSONReflect(buf, rv)
}

// writeFastJSONReflect handles the container kinds by reflection, so slices of
// any element type ([]map[string]any, []any, …) work without enumeration.
func writeFastJSONReflect(buf *bytes.Buffer, rv reflect.Value) error {
	if !rv.IsValid() {
		buf.WriteString("null")
		return nil
	}
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			buf.WriteString("null")
			return nil
		}
		// Unwrap one level, then recurse through the concrete-type fast path.
		return writeFastJSON(buf, rv.Elem().Interface())
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			buf.WriteString("null")
			return nil
		}
		buf.WriteByte('[')
		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeFastJSONReflect(buf, rv.Index(i)); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
		return nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			// MessageQueue-keyed maps are converted to mqKeyedJSON by their
			// ToJSONValue; anything else with a non-string key has no
			// fastjson2 inline form here, so use the stdlib.
			return writeStdlibFallback(buf, rv.Interface())
		}
		keys := make([]string, 0, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			keys = append(keys, iter.Key().String())
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeFastJSONString(buf, k); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := writeFastJSONReflect(buf, rv.MapIndex(reflect.ValueOf(k))); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
		return nil
	}
	return writeStdlibFallback(buf, rv.Interface())
}

func writeStdlibFallback(buf *bytes.Buffer, v any) error {
	raw, err := encodeJSONNoEscape(v)
	if err != nil {
		return err
	}
	buf.Write(raw)
	return nil
}

func writeFastJSONString(buf *bytes.Buffer, s string) error {
	raw, err := encodeJSONNoEscape(s)
	if err != nil {
		return err
	}
	buf.Write(raw)
	return nil
}

func writeMQKeyedJSON(buf *bytes.Buffer, table mqKeyedJSON) error {
	buf.WriteByte('{')
	for i, e := range table {
		if i > 0 {
			buf.WriteByte(',')
		}
		// The key is written UNQUOTED: fastjson2 expects the queue as an object.
		buf.WriteString(e.key)
		buf.WriteByte(':')
		if err := writeFastJSON(buf, e.value); err != nil {
			return err
		}
	}
	buf.WriteByte('}')
	return nil
}
