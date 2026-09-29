// Wire serialization: JSON (RemotingSerializable) and the RocketMQ private
// binary header (RocketMQSerializable).
//
// The JSON side must tolerate fastjson2's non-standard output: bare numeric
// map keys, objects as map keys (offsetTable), NaN/Infinity, trailing commas.
package remoting

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// EncodeJSON writes compact JSON without HTML escaping (Java does not escape
// <, >, & either; the broker's fastjson2 accepts both).
func EncodeJSON(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil
	}
	// json.Encoder appends a newline; Java writes compact JSON without one.
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// DecodeJSON parses tolerant JSON bytes; empty input yields nil.
func DecodeJSON(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if !utf8.Valid(data) {
		return nil, decodeErrf("json header is not valid utf8")
	}
	return ParseJSON(string(data))
}

// extFieldsToJSON renders outbound extFields (all values are strings).
func extFieldsToJSON(ext *common.StringMap) map[string]any {
	out := make(map[string]any, ext.Len())
	ext.Range(func(k, v string) {
		out[k] = v
	})
	return out
}

// extFieldsFromJSONValue converts a decoded JSON object into extFields:
// strings stay, numbers keep their raw literal text, bools become
// "true"/"false", null entries are dropped (Java fastjson reflection skips
// them the same way).
func extFieldsFromJSONValue(v any) *common.StringMap {
	out := common.NewStringMap()
	obj, ok := v.(map[string]any)
	if !ok {
		return out
	}
	for k, val := range obj {
		switch t := val.(type) {
		case nil:
			continue
		case string:
			out.Put(k, t)
		case JSONNumber:
			out.Put(k, t.String())
		case bool:
			if t {
				out.Put(k, "true")
			} else {
				out.Put(k, "false")
			}
		default:
			if raw := EncodeJSON(val); raw != nil {
				out.Put(k, string(raw))
			}
		}
	}
	return out
}

// RocketMQHeader is the decoded RocketMQ binary header.
type RocketMQHeader struct {
	Code      int32
	Language  int32
	Version   int32
	Opaque    int32
	Flag      int32
	Remark    string
	ExtFields *common.StringMap
}

// CalTotalLen mirrors Java's calTotalLen for the ROCKETMQ header (header only,
// excluding the 8-byte frame prefix):
// code(2) language(1) version(2) opaque(4) flag(4) remarkLen(4) remark extLen(4) ext.
func CalTotalLen(remark []byte, ext []byte) int {
	return 2 + 1 + 2 + 4 + 4 + 4 + len(remark) + 4 + len(ext)
}

// MapSerialize encodes extFields as repeated (shortLenKey, intLenValue) strings.
func MapSerialize(ext *common.StringMap) []byte {
	if ext.IsEmpty() {
		return nil
	}
	w := common.NewWriter()
	ext.Range(func(k, v string) {
		key, val := k, v
		w.String(true, &key)
		w.String(false, &val)
	})
	return w.IntoInner()
}

// MapDeserialize decodes extFields written by MapSerialize, returning the map
// and the offset just past it.
func MapDeserialize(data []byte, offset, length int) (*common.StringMap, int, error) {
	out := common.NewStringMap()
	end := offset + length
	if offset < 0 || length < 0 || end > len(data) {
		return nil, 0, decodeErrf("extFields length %d overruns header buffer (%d bytes)", length, len(data))
	}
	pos := offset
	for pos < end {
		r := common.NewReaderAt(data, pos)
		k, err := r.String(true)
		if err != nil {
			return nil, 0, err
		}
		v, err := r.String(false)
		if err != nil {
			return nil, 0, err
		}
		pos = r.Pos()
		if k != nil && v != nil {
			out.Put(*k, *v)
		}
	}
	return out, pos, nil
}

// RocketMQProtocolEncode encodes the command header in the RocketMQ private
// binary format.
func RocketMQProtocolEncode(cmd *RemotingCommand) []byte {
	extBytes := MapSerialize(cmd.ExtFields())
	var remark []byte
	if cmd.Remark != "" {
		remark = []byte(cmd.Remark)
	}
	w := common.NewWriterCap(CalTotalLen(remark, extBytes))
	w.I16(cmd.Code)
	w.U8(byte(cmd.Language & 0xFF))
	w.I16(cmd.Version)
	w.I32(cmd.Opaque)
	w.I32(cmd.Flag)
	if len(remark) > 0 {
		w.I32(int32(len(remark)))
		w.Bytes(remark)
	} else {
		w.I32(0)
	}
	if len(extBytes) > 0 {
		w.I32(int32(len(extBytes)))
		w.Bytes(extBytes)
	} else {
		w.I32(0)
	}
	return w.IntoInner()
}

// RocketMQProtocolDecode decodes the RocketMQ private binary header.
func RocketMQProtocolDecode(header []byte) (*RocketMQHeader, error) {
	r := common.NewReader(header)
	code, err := r.I16()
	if err != nil {
		return nil, err
	}
	language, err := r.U8()
	if err != nil {
		return nil, err
	}
	version, err := r.I16()
	if err != nil {
		return nil, err
	}
	opaque, err := r.I32()
	if err != nil {
		return nil, err
	}
	flag, err := r.I32()
	if err != nil {
		return nil, err
	}
	remarkPtr, err := r.String(false)
	if err != nil {
		return nil, err
	}
	extLen, err := r.I32()
	if err != nil {
		return nil, err
	}
	remark := ""
	if remarkPtr != nil {
		remark = *remarkPtr
	}
	ext := common.NewStringMap()
	if extLen > 0 {
		m, _, err := MapDeserialize(header, r.Pos(), int(extLen))
		if err != nil {
			return nil, err
		}
		ext = m
	}
	return &RocketMQHeader{
		Code:      code,
		Language:  int32(language),
		Version:   version,
		Opaque:    opaque,
		Flag:      flag,
		Remark:    remark,
		ExtFields: ext,
	}, nil
}

// MarkProtocolType packs the header length into the low 24 bits and the
// serialize type into the high 8 bits (Java markProtocolType).
func MarkProtocolType(source int32, stype int32) int32 {
	return ((stype & 0xFF) << 24) | (source & 0x00FF_FFFF)
}

// ProtocolTypeOf extracts the serialize type from the packed header length.
func ProtocolTypeOf(source int32) int32 {
	return (source >> 24) & 0xFF
}

// HeaderLengthOf extracts the header length from the packed word.
func HeaderLengthOf(source int32) int32 {
	return source & 0x00FF_FFFF
}

// SerializeTypeFromEnv reads ROCKETMQ_SERIALIZE_TYPE / rocketmq.serialize.type;
// "ROCKETMQ" selects the binary header, anything else JSON (Java default).
func SerializeTypeFromEnv() int32 {
	raw := os.Getenv("ROCKETMQ_SERIALIZE_TYPE")
	if raw == "" {
		raw = os.Getenv("rocketmq.serialize.type")
	}
	if strings.EqualFold(strings.TrimSpace(raw), "ROCKETMQ") {
		return SerializeTypeRocketMQ
	}
	return SerializeTypeJSON
}
