package common

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

// 17-segment storage format (Java MessageDecoder#encode/decode), used for the
// broker's commitlog records and pull/get bodies:
//
//	 1 TOTALSIZE(4)          2 MAGICCODE(4, v1=-626843481 / v2=-626843477)
//	 3 BODYCRC(4)            4 QUEUEID(4)            5 FLAG(4)
//	 6 QUEUEOFFSET(8)        7 PHYSICALOFFSET(8)     8 SYSFLAG(4)
//	 9 BORNTIMESTAMP(8)     10 BORNHOST(8|20B)      11 STORETIMESTAMP(8)
//	12 STOREHOST(8|20B)     13 RECONSUMETIMES(4)    14 PREPAREDTRANSACTIONOFFSET(8)
//	15 BODY(4+len)          16 TOPIC(1B v1|2B v2 + len)
//	17 PROPERTIES(2+len, `k\x01v\x02` chain)
//
// 6-segment light format (Java MessageDecoder#encodeMessage/decodeMessage) is
// for batch messages only:
//
//	TOTALSIZE(4) | MAGICCODE(4, 0) | BODYCRC(4, 0) | FLAG(4) | BODY(4+len) | PROPERTIES(2+len)
const (
	// NameValueSeparator (Java MessageDecoder.NAME_VALUE_SEPARATOR).
	NameValueSeparator = '\x01'
	// PropertySeparator (Java MessageDecoder.PROPERTY_SEPARATOR).
	PropertySeparator = '\x02'

	MessageMagicCode   int32 = -626843481
	MessageMagicCodeV2 int32 = -626843477
	// BlankMagicCode marks a blank commitlog record — not a valid message frame.
	BlankMagicCode int32 = -875286124

	MessageMagicCodePosition      = 4
	MessageFlagPosition           = 16
	PhyPosPosition                = 4 + 4 + 4 + 4 + 4 + 8 // 28
	QueueOffsetPosition           = 4 + 4 + 4 + 4 + 4     // 20
	SysflagPosition               = 4 + 4 + 4 + 4 + 4 + 8 + 8
	MessageStoreTimestampPosition = 56

	// headerWithoutBodyLen: fixed head through the BODY length field, before
	// the two variable-length host addresses.
	headerWithoutBodyLen = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 4 + 8 // 68
)

// DecodeOptions carries the four switches of Java
// MessageDecoder#decode(bb, readBody, deCompressBody, isClient, checkCRC).
type DecodeOptions struct {
	// ReadBody=false skips the body bytes and leaves Body nil.
	ReadBody bool
	// DecompressBody decompresses when COMPRESSED_FLAG is set.
	DecompressBody bool
	// IsClient also copies MsgID into OffsetMsgID.
	IsClient bool
	// CheckCRC validates the body against BODYCRC.
	CheckCRC bool
}

func DefaultDecodeOptions() DecodeOptions {
	return DecodeOptions{ReadBody: true, DecompressBody: true, IsClient: true, CheckCRC: false}
}

func hostLength(sysFlag, v6Flag int32) int {
	if sysFlag&v6Flag != 0 {
		return 20
	}
	return 8
}

// MessageBytesSize is the encoded size of a message (Java's storeSize
// arithmetic) — avoids encoding just to measure.
func MessageBytesSize(topic string, bodyLen int, properties *StringMap, sysFlag int32) int {
	bornhostLength := hostLength(sysFlag, MessageSysFlagBornhostV6)
	storehostLength := hostLength(sysFlag, MessageSysFlagStorehostV6)
	return headerWithoutBodyLen + bornhostLength + storehostLength + 4 + bodyLen + 1 +
		len(topic) + MessageStringSize(MessageProperties2String(properties))
}

// MessageStringSize: UTF-8 bytes + the 2-byte length prefix.
func MessageStringSize(s string) int { return 2 + len(s) }

// IPAndPortToBytes: ip bytes + 4-byte big-endian port.
func IPAndPortToBytes(ip string, port uint32, v6 bool) ([]byte, error) {
	out, err := IPToBytes(ip, v6)
	if err != nil {
		return nil, err
	}
	return binary.BigEndian.AppendUint32(out, port), nil
}

// IPToBytes mirrors Python socket.inet_pton with the Java cross-family rules:
// a v4 address with v6=true becomes a v4-mapped v6; a v6 address with v6=false
// decompresses only when it is v4-mapped, otherwise it is an error.
func IPToBytes(ip string, v6 bool) ([]byte, error) {
	addr := net.ParseIP(ip)
	if addr == nil {
		return nil, DecodeError(fmt.Sprintf("illegal ip address: %s", ip))
	}
	if strings.Contains(ip, ":") { // textual IPv6
		if v6 {
			return append([]byte(nil), addr.To16()...), nil
		}
		if to4 := addr.To4(); to4 != nil {
			return append([]byte(nil), to4...), nil
		}
		return nil, DecodeError(fmt.Sprintf("ipv6 address %s needs BORNHOST_V6_FLAG / STOREHOSTADDRESS_V6_FLAG", ip))
	}
	// textual IPv4
	v4 := addr.To4()
	if v4 == nil {
		return nil, DecodeError(fmt.Sprintf("illegal ip address: %s", ip))
	}
	if !v6 {
		return append([]byte(nil), v4...), nil
	}
	out := make([]byte, 16)
	out[10], out[11] = 0xff, 0xff
	copy(out[12:], v4)
	return out, nil
}

// BytesToIPAndPort: 8 bytes = ipv4+port, 20 bytes = ipv6+port.
func BytesToIPAndPort(raw []byte) (string, uint32, error) {
	var ipLen int
	var v6 bool
	switch len(raw) {
	case 8:
		ipLen, v6 = 4, false
	case 20:
		ipLen, v6 = 16, true
	default:
		return "", 0, DecodeError(fmt.Sprintf("illegal host bytes length: %d", len(raw)))
	}
	ip, err := bytesToIP(raw[:ipLen], v6)
	if err != nil {
		return "", 0, err
	}
	port := binary.BigEndian.Uint32(raw[ipLen : ipLen+4])
	return ip, port, nil
}

func bytesToIP(b []byte, v6 bool) (string, error) {
	if v6 {
		if len(b) != 16 {
			return "", DecodeError("illegal ipv6 address length")
		}
		return net.IP(b).String(), nil
	}
	if len(b) != 4 {
		return "", DecodeError("illegal ipv4 address length")
	}
	return net.IPv4(b[0], b[1], b[2], b[3]).String(), nil
}

// CreateMessageID: addr(8|20B) + commitLogOffset(8B) -> UPPERCASE hex.
func CreateMessageID(addrBytes []byte, offset int64) string {
	raw := append([]byte(nil), addrBytes...)
	raw = binary.BigEndian.AppendUint64(raw, uint64(offset))
	return Bytes2String(raw)
}

// DecodeMessageID mirrors Java MessageDecoder.decodeMessageId:
// (ip, port, commitLogOffset).
func DecodeMessageID(msgID string) (string, uint32, int64, error) {
	raw := String2Bytes(msgID)
	if raw == nil {
		return "", 0, 0, DecodeError(fmt.Sprintf("illegal msgId: %s", msgID))
	}
	var ipLen int
	switch len(raw) {
	case 16:
		ipLen = 4
	case 28:
		ipLen = 16
	default:
		return "", 0, 0, DecodeError(fmt.Sprintf("illegal msgId length: %d", len(raw)))
	}
	ip, port, err := BytesToIPAndPort(raw[:ipLen+4])
	if err != nil {
		return "", 0, 0, err
	}
	offset := int64(binary.BigEndian.Uint64(raw[ipLen+4 : ipLen+12]))
	return ip, port, offset, nil
}

// MessageProperties2String mirrors Java messageProperties2String: entries
// joined as `k\x01v\x02` in insertion order.
func MessageProperties2String(properties *StringMap) string {
	var sb strings.Builder
	properties.Range(func(name, value string) {
		sb.WriteString(name)
		sb.WriteRune(NameValueSeparator)
		sb.WriteString(value)
		sb.WriteRune(PropertySeparator)
	})
	return sb.String()
}

// String2MessageProperties mirrors Java string2messageProperties. Parsing is
// CHARACTER-based and deliberately drops: fragments shorter than 3 chars, and
// fragments without a kv separator strictly inside — including EMPTY values
// (`k\x01\x02`). Copy this behavior, do not "fix" it.
func String2MessageProperties(propertiesStr string) *StringMap {
	result := NewStringMap()
	if propertiesStr == "" {
		return result
	}
	chars := []rune(propertiesStr)
	length := len(chars)
	index := 0
	for index < length {
		newIndex := findChar(chars, index, length, PropertySeparator)
		if newIndex < 0 {
			newIndex = length
		}
		if newIndex >= index+3 {
			if kvSep := findChar(chars, index, length, NameValueSeparator); kvSep >= 0 {
				if kvSep > index && kvSep < newIndex-1 {
					result.Put(string(chars[index:kvSep]), string(chars[kvSep+1:newIndex]))
				}
			}
		}
		index = newIndex + 1
	}
	return result
}

func findChar(chars []rune, from, length int, target rune) int {
	for i := from; i < length; i++ {
		if chars[i] == target {
			return i
		}
	}
	return -1
}

// EncodeMessageExt mirrors Java MessageDecoder#encode(MessageExt, needCompress).
// Java always writes a 1-byte topic length and the v1 magic; storeSize > 0
// pre-sizes the buffer and zero-pads the tail.
func EncodeMessageExt(ext *MessageExt, needCompress bool) ([]byte, error) {
	body := append([]byte(nil), ext.Body...)
	sysFlag := ext.SysFlag
	if needCompress && sysFlag&MessageSysFlagCompressed == MessageSysFlagCompressed {
		compressionType := GetCompressionType(sysFlag)
		var err error
		body, err = Compress(body, compressionType, DefaultCompressLevel)
		if err != nil {
			return nil, err
		}
	}
	bodyLength := len(body)

	topicBytes := []byte(ext.Topic)
	topicLen := len(topicBytes)
	if topicLen > 255 {
		// Java silently truncates to (byte) len; report instead.
		return nil, EncodeError(fmt.Sprintf("topic length %d exceeds 1-byte field, needs magic code v2", topicLen))
	}
	propertiesBytes := []byte(MessageProperties2String(ext.Properties))
	propertiesLength := len(propertiesBytes)

	bornhostLength := hostLength(sysFlag, MessageSysFlagBornhostV6)
	storehostLength := hostLength(sysFlag, MessageSysFlagStorehostV6)
	computedSize := headerWithoutBodyLen + bornhostLength + storehostLength + 4 + bodyLength + 1 + topicLen + 2 + propertiesLength
	storeSize := computedSize
	if ext.StoreSize > 0 && int(ext.StoreSize) > computedSize {
		storeSize = int(ext.StoreSize)
	}

	bornHost := ext.BornHost
	if bornHost == "" {
		bornHost = "127.0.0.1"
	}
	storeHost := ext.StoreHost
	if storeHost == "" {
		storeHost = "127.0.0.1"
	}
	bornAddr, err := IPAndPortToBytes(bornHost, ext.BornHostPort, bornhostLength == 20)
	if err != nil {
		return nil, err
	}
	storeAddr, err := IPAndPortToBytes(storeHost, ext.StoreHostPort, storehostLength == 20)
	if err != nil {
		return nil, err
	}

	w := NewWriterCap(storeSize)
	w.I32(int32(storeSize))              // 1 TOTALSIZE
	w.I32(MessageMagicCode)              // 2 MAGICCODE
	w.U32(ext.BodyCRC)                   // 3 BODYCRC
	w.I32(ext.QueueID)                   // 4 QUEUEID
	w.I32(ext.Flag)                      // 5 FLAG
	w.I64(ext.QueueOffset)               // 6 QUEUEOFFSET
	w.I64(ext.CommitLogOffset)           // 7 PHYSICALOFFSET
	w.I32(sysFlag)                       // 8 SYSFLAG
	w.I64(ext.BornTimestamp)             // 9 BORNTIMESTAMP
	w.Bytes(bornAddr)                    // 10 BORNHOST
	w.I64(ext.StoreTimestamp)            // 11 STORETIMESTAMP
	w.Bytes(storeAddr)                   // 12 STOREHOST
	w.I32(ext.ReconsumeTimes)            // 13 RECONSUMETIMES
	w.I64(ext.PreparedTransactionOffset) // 14
	w.I32(int32(bodyLength))             // 15 BODY
	w.Bytes(body)
	w.U8(byte(topicLen)) // 16 TOPIC
	w.Bytes(topicBytes)
	w.U16(uint32(propertiesLength)) // 17 PROPERTIES
	w.Bytes(propertiesBytes)
	if storeSize > w.Len() {
		w.Zeros(storeSize - w.Len())
	}
	return w.IntoInner(), nil
}

// DecodeMessage mirrors Java MessageDecoder#decode(ByteBuffer) defaults.
func DecodeMessage(raw []byte) (*MessageExt, error) {
	return DecodeMessageWith(raw, DefaultDecodeOptions())
}

// DecodeMessageWith mirrors Java MessageDecoder#decode with the four switches.
// Java wraps the whole body in try/catch and returns null on failure; here the
// same semantics is a DecodeError.
func DecodeMessageWith(raw []byte, options DecodeOptions) (*MessageExt, error) {
	reader := NewReader(raw)
	ext := NewMessageExt()

	storeSize, err := reader.I32()
	if err != nil {
		return nil, err
	}
	magicCode, err := reader.I32()
	if err != nil {
		return nil, err
	}
	// Java MessageVersion.valueOfMagicCode throws on unknown magic -> null.
	useV2 := false
	switch magicCode {
	case MessageMagicCode:
		useV2 = false
	case MessageMagicCodeV2:
		useV2 = true
	default:
		return nil, DecodeError(fmt.Sprintf("unknown magic code: %d", magicCode))
	}
	bodyCRC, err := reader.U32()
	if err != nil {
		return nil, err
	}
	queueID, err := reader.I32()
	if err != nil {
		return nil, err
	}
	flag, err := reader.I32()
	if err != nil {
		return nil, err
	}
	queueOffset, err := reader.I64()
	if err != nil {
		return nil, err
	}
	physicOffset, err := reader.I64()
	if err != nil {
		return nil, err
	}
	sysFlag, err := reader.I32()
	if err != nil {
		return nil, err
	}
	bornTimestamp, err := reader.I64()
	if err != nil {
		return nil, err
	}

	bornhostLength := hostLength(sysFlag, MessageSysFlagBornhostV6)
	bornAddr, err := reader.Bytes(bornhostLength)
	if err != nil {
		return nil, err
	}
	bornHost, bornPort, err := BytesToIPAndPort(bornAddr)
	if err != nil {
		return nil, err
	}
	storeTimestamp, err := reader.I64()
	if err != nil {
		return nil, err
	}
	storehostLength := hostLength(sysFlag, MessageSysFlagStorehostV6)
	storeAddr, err := reader.Bytes(storehostLength)
	if err != nil {
		return nil, err
	}
	storeHost, storePort, err := BytesToIPAndPort(storeAddr)
	if err != nil {
		return nil, err
	}
	reconsumeTimes, err := reader.I32()
	if err != nil {
		return nil, err
	}
	preparedTransactionOffset, err := reader.I64()
	if err != nil {
		return nil, err
	}

	ext.StoreSize = storeSize
	ext.BodyCRC = bodyCRC
	ext.QueueID = queueID
	ext.Flag = flag
	ext.QueueOffset = queueOffset
	ext.CommitLogOffset = physicOffset
	ext.SysFlag = sysFlag
	ext.BornTimestamp = bornTimestamp
	ext.BornHost = bornHost
	ext.BornHostPort = bornPort
	ext.StoreTimestamp = storeTimestamp
	ext.StoreHost = storeHost
	ext.StoreHostPort = storePort
	ext.ReconsumeTimes = reconsumeTimes
	ext.PreparedTransactionOffset = preparedTransactionOffset

	// 15 BODY
	bodyLen, err := reader.I32()
	if err != nil {
		return nil, err
	}
	if bodyLen > 0 {
		bodyBytes, err := reader.Bytes(int(bodyLen))
		if err != nil {
			return nil, err
		}
		if options.ReadBody {
			body := append([]byte(nil), bodyBytes...)
			if options.CheckCRC && Crc32(body) != bodyCRC {
				return nil, DecodeError("Msg crc is error")
			}
			if options.DecompressBody && sysFlag&MessageSysFlagCompressed != 0 {
				compressionType := GetCompressionType(sysFlag)
				body, err = Decompress(body, compressionType)
				if err != nil {
					return nil, err
				}
				sysFlag &^= MessageSysFlagCompressed
				ext.SysFlag = sysFlag
			}
			ext.Body = body
		} else {
			// Java/Python readBody=false: skip the bytes, body stays null.
			ext.Body = nil
		}
	} else {
		ext.Body = nil
	}

	// 16 TOPIC
	var topicLen int
	if useV2 {
		v, err := reader.U16()
		if err != nil {
			return nil, err
		}
		topicLen = int(v)
	} else {
		v, err := reader.U8()
		if err != nil {
			return nil, err
		}
		topicLen = int(v)
	}
	topicBytes, err := reader.Bytes(topicLen)
	if err != nil {
		return nil, err
	}
	ext.Topic = strings.ToValidUTF8(string(topicBytes), "\ufffd")

	// 17 PROPERTIES
	propertiesLength, err := reader.U16()
	if err != nil {
		return nil, err
	}
	if propertiesLength > 0 {
		propertiesBytes, err := reader.Bytes(int(propertiesLength))
		if err != nil {
			return nil, err
		}
		text := strings.ToValidUTF8(string(propertiesBytes), "\ufffd")
		ext.Properties = String2MessageProperties(text)
	}

	// msgId = storeHost(ip+port) + commitLogOffset
	msgAddr, err := IPAndPortToBytes(storeHost, storePort, storehostLength == 20)
	if err != nil {
		return nil, err
	}
	ext.MsgID = CreateMessageID(msgAddr, physicOffset)
	if options.IsClient {
		ext.OffsetMsgID = ext.MsgID
	}
	return ext, nil
}

// DecodeMessages mirrors Java MessageDecoder.decodes: split a pull-result
// message stream into a list, stopping at the first unparseable frame.
func DecodeMessages(raw []byte) []*MessageExt {
	return DecodeMessagesWith(raw, DefaultDecodeOptions())
}

// DecodeMessagesWith is the option-carrying variant of DecodeMessages.
func DecodeMessagesWith(raw []byte, options DecodeOptions) []*MessageExt {
	var result []*MessageExt
	pos := 0
	total := len(raw)
	for pos < total {
		if total-pos < 4 {
			break
		}
		storeSize := int32(binary.BigEndian.Uint32(raw[pos : pos+4]))
		if storeSize <= 0 || int(storeSize) > total-pos {
			break
		}
		end := pos + int(storeSize)
		msg, err := DecodeMessageWith(raw[pos:end], options)
		if err != nil {
			break
		}
		result = append(result, msg)
		pos = end
	}
	return result
}

// EncodeMessage mirrors Java MessageDecoder#encodeMessage(Message): the
// per-message light frame of a batch.
func EncodeMessage(message *Message) []byte {
	body := message.GetBody()
	propertiesBytes := []byte(MessageProperties2String(message.Properties))
	propertiesLength := len(propertiesBytes)
	storeSize := 4 + 4 + 4 + 4 + 4 + len(body) + 2 + propertiesLength

	w := NewWriterCap(storeSize)
	w.I32(int32(storeSize)) // 1 TOTALSIZE
	w.I32(0)                // 2 MAGICCODE (batch: fixed 0)
	w.I32(0)                // 3 BODYCRC
	w.I32(message.Flag)     // 4 FLAG
	w.I32(int32(len(body))) // 5 BODY
	w.Bytes(body)
	w.U16(uint32(propertiesLength)) // 6 PROPERTIES
	w.Bytes(propertiesBytes)
	return w.IntoInner()
}

// EncodeMessages mirrors Java MessageDecoder.encodeMessages(List<Message>).
func EncodeMessages(messages []*Message) []byte {
	out := NewWriter()
	for _, msg := range messages {
		out.Bytes(EncodeMessage(msg))
	}
	return out.IntoInner()
}

// DecodeBatchMessage mirrors Java MessageDecoder.decodeMessage(ByteBuffer):
// one batch unit -> Message.
func DecodeBatchMessage(raw []byte) (*Message, error) {
	reader := NewReader(raw)
	if _, err := reader.I32(); err != nil { // TOTALSIZE
		return nil, err
	}
	if _, err := reader.I32(); err != nil { // MAGICCODE
		return nil, err
	}
	if _, err := reader.I32(); err != nil { // BODYCRC
		return nil, err
	}
	flag, err := reader.I32()
	if err != nil {
		return nil, err
	}
	bodyLen, err := reader.I32()
	if err != nil {
		return nil, err
	}
	if bodyLen < 0 {
		bodyLen = 0
	}
	body, err := reader.Bytes(int(bodyLen))
	if err != nil {
		return nil, err
	}
	propertiesLen, err := reader.U16()
	if err != nil {
		return nil, err
	}
	propertiesBytes, err := reader.Bytes(int(propertiesLen))
	if err != nil {
		return nil, err
	}
	text := strings.ToValidUTF8(string(propertiesBytes), "\ufffd")
	msg := NewMessage("", body)
	msg.Flag = flag
	msg.Properties = String2MessageProperties(text)
	return msg, nil
}

// DecodeBatchMessages mirrors Java MessageDecoder.decodeMessages(ByteBuffer).
func DecodeBatchMessages(raw []byte) []*Message {
	var result []*Message
	pos := 0
	total := len(raw)
	for pos < total {
		if total-pos < 4 {
			break
		}
		storeSize := int32(binary.BigEndian.Uint32(raw[pos : pos+4]))
		if storeSize <= 0 || int(storeSize) > total-pos {
			break
		}
		end := pos + int(storeSize)
		msg, err := DecodeBatchMessage(raw[pos:end])
		if err != nil {
			break
		}
		result = append(result, msg)
		pos = end
	}
	return result
}

// CountInnerMsgNum mirrors Java MessageDecoder.countInnerMsgNum.
func CountInnerMsgNum(raw []byte) int32 {
	count := int32(0)
	pos := 0
	total := len(raw)
	for pos < total {
		count++
		if total-pos < 4 {
			break
		}
		size := int32(binary.BigEndian.Uint32(raw[pos : pos+4]))
		if size <= 0 || int(size) > total-pos {
			break
		}
		pos += int(size)
	}
	return count
}

// ParseTopicFilterBitmap decodes a binary string (Java BitsArray#toString,
// high bit first) into per-bit flags.
func ParseTopicFilterBitmap(bitmap string) ([]bool, error) {
	out := make([]bool, 0, len(bitmap))
	for _, ch := range bitmap {
		switch ch {
		case '0':
			out = append(out, false)
		case '1':
			out = append(out, true)
		default:
			return nil, DecodeError(fmt.Sprintf("illegal bitMap char %q at index %d", ch, len(out)))
		}
	}
	return out, nil
}
