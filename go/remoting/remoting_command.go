// RemotingCommand: the RocketMQ remote command frame.
//
// Wire format: totalLength(4) | headerLength(high 8 bits = serialize type, 4) |
// header | body
//   - JSON header: RemotingSerializable JSON encoding
//   - ROCKETMQ header: code(2) language(1) version(2) opaque(4) flag(4)
//     remark(int+utf8) extFields(int + key(short+utf8) value(int+utf8))
package remoting

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// CurrentVersion mirrors Java MQVersion.CURRENT_VERSION (= Version.V5_5_1).
//
// This value is not cosmetic: brokers gate admin-to-client forwarding on it —
// AdminBrokerProcessor#callConsumer requires >= V3_1_8_SNAPSHOT (ordinal 62),
// Broker2Client#getConsumeStatus requires >= V3_0_7_SNAPSHOT (ordinal 28).
// A default of 0 (= V3_0_0_SNAPSHOT) makes examineConsumerRunningInfo /
// getConsumeStatus fail with code=1 "too low to finish".
const CurrentVersion = int32(515)

// Env keys overriding the outbound protocol version (tests / debugging).
const (
	RemotingVersionKey   = "rocketmq.remoting.version"
	RemotingVersionKeyUC = "ROCKETMQ_REMOTING_VERSION"
)

var opaqueCounter atomic.Int32

// NextOpaque returns a monotonically increasing request id (Java requestId).
func NextOpaque() int32 {
	return opaqueCounter.Add(1) - 1
}

// CustomHeader mirrors Java CommandCustomHeader reflection: write non-nil
// fields into extFields, read them back. Concrete header types live in
// headers.go; decoding is `h := &remoting.XxxHeader{}; h.FromExtFields(...)`.
type CustomHeader interface {
	ToExtFields(out *common.StringMap)
	FromExtFields(ext *common.StringMap)
}

// RemotingCommand is one request or response frame.
type RemotingCommand struct {
	Code                    int32
	Language                int32
	Version                 int32
	Opaque                  int32
	Flag                    int32
	Remark                  string
	SerializeTypeCurrentRPC int32
	Body                    []byte

	extFields    *common.StringMap
	customHeader CustomHeader
}

// NewCommand builds a bare command (decode helper); outbound requests must go
// through CreateRequestCommand so the version header is set.
func NewCommand() *RemotingCommand {
	return &RemotingCommand{
		Code:                    0,
		Language:                LangGo,
		Version:                 0,
		Opaque:                  NextOpaque(),
		Flag:                    0,
		SerializeTypeCurrentRPC: SerializeTypeFromEnv(),
		extFields:               common.NewStringMap(),
	}
}

// CreateRequestCommand builds an outbound request.
func CreateRequestCommand(code int32, header CustomHeader) *RemotingCommand {
	cmd := NewCommand()
	cmd.Code = code
	cmd.customHeader = header
	cmd.setCmdVersion()
	return cmd
}

// CreateResponseCommand builds an outbound response (server side / replies).
func CreateResponseCommand(code int32, remark string) *RemotingCommand {
	cmd := NewCommand()
	cmd.Code = code
	cmd.Remark = remark
	cmd.MarkResponseType()
	cmd.setCmdVersion()
	return cmd

}

// BuildErrorResponse is the Java createResponseCommand(code, remark) shortcut.
func BuildErrorResponse(code int32, remark string) *RemotingCommand {
	return CreateResponseCommand(code, remark)
}

func (c *RemotingCommand) setCmdVersion() {
	raw := os.Getenv(RemotingVersionKey)
	if raw == "" {
		raw = os.Getenv(RemotingVersionKeyUC)
	}
	if v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32); err == nil {
		c.Version = int32(v)
		return
	}
	c.Version = CurrentVersion
}

// Clone deep-copies the frame fields. The custom header is shared (headers
// are treated as immutable once built); Body and extFields are copied so the
// clone can be mutated independently (GO_AWAY retry assigns a fresh opaque).
func (c *RemotingCommand) Clone() *RemotingCommand {
	clone := *c
	if c.Body != nil {
		clone.Body = append([]byte(nil), c.Body...)
	}
	clone.extFields = c.extFields.Clone()
	return &clone
}

// ---------------- flag bits ----------------

func (c *RemotingCommand) MarkResponseType()    { c.Flag |= 1 << FlagRPCType }
func (c *RemotingCommand) IsResponseType() bool { return c.Flag&(1<<FlagRPCType) != 0 }
func (c *RemotingCommand) MarkOnewayRPC()       { c.Flag |= 1 << FlagOnewayRPC }
func (c *RemotingCommand) IsOnewayRPC() bool    { return c.Flag&(1<<FlagOnewayRPC) != 0 }

// ---------------- field access ----------------

func (c *RemotingCommand) ExtFields() *common.StringMap { return c.extFields }

func (c *RemotingCommand) AddExtField(key, value string) { c.extFields.Put(key, value) }

func (c *RemotingCommand) GetExtField(key string) (string, bool) { return c.extFields.Get(key) }

func (c *RemotingCommand) SetBody(body []byte) { c.Body = body }

func (c *RemotingCommand) SetCustomHeader(header CustomHeader) { c.customHeader = header }

func (c *RemotingCommand) CustomHeader() CustomHeader { return c.customHeader }

// MakeCustomHeaderToNet merges the custom header's non-nil fields into
// extFields (Java reflection write).
func (c *RemotingCommand) MakeCustomHeaderToNet() {
	if c.customHeader == nil {
		return
	}
	out := common.NewStringMap()
	c.customHeader.ToExtFields(out)
	out.Range(func(k, v string) {
		c.extFields.Put(k, v)
	})
}

// ---------------- encoding ----------------

// HeaderEncode encodes only the header payload (no frame prefix), after
// flushing the custom header into extFields.
func (c *RemotingCommand) HeaderEncode() []byte {
	c.MakeCustomHeaderToNet()
	if c.SerializeTypeCurrentRPC == SerializeTypeRocketMQ {
		return RocketMQProtocolEncode(c)
	}
	return EncodeJSON(c.ToJSONValue())
}

// Encode writes the whole frame including the 4-byte totalLength prefix.
func (c *RemotingCommand) Encode() []byte {
	headerData := c.HeaderEncode()
	bodyLen := len(c.Body)
	totalLen := 4 + len(headerData) + bodyLen
	out := make([]byte, 0, 8+len(headerData)+bodyLen)
	out = binary.BigEndian.AppendUint32(out, uint32(totalLen))
	out = binary.BigEndian.AppendUint32(out, uint32(MarkProtocolType(int32(len(headerData)), c.SerializeTypeCurrentRPC)))
	out = append(out, headerData...)
	out = append(out, c.Body...)
	return out
}

// EncodeHeader writes the frame with a body placeholder of bodyLength bytes
// (Java encodeHeader(int bodyLength)); the caller writes the body separately.
func (c *RemotingCommand) EncodeHeader(bodyLength int) []byte {
	headerData := c.HeaderEncode()
	totalLen := 4 + len(headerData) + bodyLength
	out := make([]byte, 0, 8+len(headerData))
	out = binary.BigEndian.AppendUint32(out, uint32(totalLen))
	out = binary.BigEndian.AppendUint32(out, uint32(MarkProtocolType(int32(len(headerData)), c.SerializeTypeCurrentRPC)))
	out = append(out, headerData...)
	return out
}

// Decode parses a whole frame (data starts at totalLength).
func Decode(data []byte) (*RemotingCommand, error) {
	r := common.NewReader(data)
	totalLength, err := r.I32()
	if err != nil {
		return nil, err
	}
	if int64(totalLength) > int64(len(data)-4) {
		return nil, common.RemotingCommandError(fmt.Sprintf("decode error, bad total length: %d", totalLength))
	}
	oriHeaderLen, err := r.I32()
	if err != nil {
		return nil, err
	}
	headerLength := HeaderLengthOf(oriHeaderLen)
	if int64(headerLength) > int64(r.Remaining()) {
		return nil, common.RemotingCommandError(fmt.Sprintf("decode error, bad header length: %d", headerLength))
	}
	protocolType := ProtocolTypeOf(oriHeaderLen)
	headerData, err := r.Bytes(int(headerLength))
	if err != nil {
		return nil, err
	}
	rest := data[r.Pos():]
	var body []byte
	if len(rest) > 0 {
		body = rest
	}

	cmd := NewCommand()
	cmd.Code = 0
	cmd.Opaque = -1
	cmd.Remark = ""
	cmd.Flag = 0
	cmd.Version = 0
	cmd.Body = body
	cmd.extFields = common.NewStringMap()

	if protocolType == SerializeTypeRocketMQ {
		header, err := RocketMQProtocolDecode(headerData)
		if err != nil {
			return nil, err
		}
		cmd.Code = header.Code
		cmd.Language = header.Language
		cmd.Version = header.Version
		cmd.Opaque = header.Opaque
		cmd.Flag = header.Flag
		cmd.Remark = header.Remark
		cmd.extFields = header.ExtFields
	} else {
		value, err := DecodeJSON(headerData)
		if err != nil {
			return nil, err
		}
		cmd.Code = jsonI32(value, "code", 0)
		if lang, ok := value.(map[string]any)["language"]; ok {
			switch t := lang.(type) {
			case string:
				cmd.Language = LanguageCodeFromName(t)
			default:
				if n, ok := numberAsI32(lang); ok {
					cmd.Language = n
				} else {
					cmd.Language = LangGo
				}
			}
		} else {
			cmd.Language = LangGo
		}
		cmd.Version = jsonI32(value, "version", 0)
		cmd.Opaque = jsonI32(value, "opaque", -1)
		cmd.Flag = jsonI32(value, "flag", 0)
		if m, ok := value.(map[string]any); ok {
			if remark, ok := m["remark"].(string); ok {
				cmd.Remark = remark
			}
			if ext, ok := m["extFields"]; ok {
				cmd.extFields = extFieldsFromJSONValue(ext)
			}
		}
	}
	cmd.SerializeTypeCurrentRPC = protocolType
	return cmd, nil
}

// ToJSONValue renders the command as a JSON object for outbound JSON headers.
func (c *RemotingCommand) ToJSONValue() map[string]any {
	m := map[string]any{
		"code":     c.Code,
		"language": c.Language,
		"version":  c.Version,
		"opaque":   c.Opaque,
		"flag":     c.Flag,
	}
	if c.Remark != "" {
		m["remark"] = c.Remark
	}
	if !c.extFields.IsEmpty() {
		m["extFields"] = extFieldsToJSON(c.extFields)
	}
	return m
}

func numberAsI32(v any) (int32, bool) {
	switch t := v.(type) {
	case JSONNumber:
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

func jsonI32(value any, key string, def int32) int32 {
	m, ok := value.(map[string]any)
	if !ok {
		return def
	}
	if v, ok := m[key]; ok {
		if n, ok := numberAsI32(v); ok {
			return n
		}
	}
	return def
}

// String renders the command like Java's RemotingCommand.toString.
func (c *RemotingCommand) String() string {
	var pairs []string
	c.extFields.Range(func(k, v string) {
		pairs = append(pairs, k+"="+v)
	})
	return fmt.Sprintf(
		"RemotingCommand [code=%d, language=%d, version=%d, opaque=%d, flag(B)=%b, remark=%q, extFields=%s, serializeTypeCurrentRPC=%d]",
		c.Code, c.Language, c.Version, c.Opaque, c.Flag, c.Remark, strings.Join(pairs, ","), c.SerializeTypeCurrentRPC)
}
