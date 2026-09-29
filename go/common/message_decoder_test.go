package common

import (
	"encoding/binary"
	"strings"
	"testing"
)

// python3 生成的 139 字节 17 段帧（buildExt 的字段值见下），字节级真值。
const ext139Hex = "0000008BDAA320A7F3307B09000000030000000200000000000000580000000000000400" +
	"000000000000018BCFE568007F0000010000D4310000018BCFE5687B7F00000100002A9F" +
	"0000000100000000000000000000000E68656C6C6F20726F636B65746D7109546F706963" +
	"546573740019544147530154616741024B455953016B657931206B65793202"

// 同一消息去掉 body 后的 100 字节帧（bodyLen=0，属性为空）。
const emptyBodyHex = "00000064DAA320A700000000000000030000000200000000000000580000000000000400" +
	"000000000000018BCFE568007F0000010000D4310000018BCFE5687B7F00000100002A9F" +
	"0000000100000000000000000000000009546F706963546573740000"

// msgId = storeHost(ip+port) + commitLogOffset，storeHost=127.0.0.1:10911，offset=1024
const extMsgID = "7F00000100002A9F0000000000000400"

// storeHost=2001:db8::2:10911，offset=64 的 IPv6 msgId。
const extMsgIDV6 = "20010DB800000000000000000000000200002A9F0000000000000040"

func unhex(t *testing.T, hex string) []byte {
	t.Helper()
	raw := String2Bytes(hex)
	if raw == nil {
		t.Fatalf("bad fixture hex: %s", hex)
	}
	return raw
}

func hexUpper(b []byte) string { return Bytes2String(b) }

func buildExt() *MessageExt {
	ext := NewMessageExt()
	ext.Topic = "TopicTest"
	ext.Body = []byte("hello rocketmq")
	ext.Flag = 2
	ext.SysFlag = 0
	ext.BodyCRC = Crc32([]byte("hello rocketmq"))
	ext.QueueID = 3
	ext.QueueOffset = 88
	ext.CommitLogOffset = 1024
	ext.BornTimestamp = 1700000000000
	ext.StoreTimestamp = 1700000000123
	ext.BornHost = "127.0.0.1"
	ext.BornHostPort = 54321
	ext.StoreHost = "127.0.0.1"
	ext.StoreHostPort = 10911
	ext.ReconsumeTimes = 1
	ext.PutProperty(PropertyTags, "TagA")
	ext.PutProperty(PropertyKeys, "key1 key2")
	return ext
}

func TestEncodeMessageExtMatchesPythonFixture(t *testing.T) {
	raw, err := EncodeMessageExt(buildExt(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 139 {
		t.Fatalf("len %d, want 139", len(raw))
	}
	if got := hexUpper(raw); got != ext139Hex {
		t.Fatalf("got  %s\nwant %s", got, ext139Hex)
	}
}

func TestDecodeRoundTripAllFields(t *testing.T) {
	ext, err := DecodeMessage(unhex(t, ext139Hex))
	if err != nil {
		t.Fatal(err)
	}
	if ext.Topic != "TopicTest" || string(ext.GetBody()) != "hello rocketmq" {
		t.Fatalf("topic/body: %q %q", ext.Topic, ext.GetBody())
	}
	if ext.Flag != 2 || ext.QueueID != 3 || ext.QueueOffset != 88 {
		t.Fatalf("flag/queue: %d %d %d", ext.Flag, ext.QueueID, ext.QueueOffset)
	}
	if ext.CommitLogOffset != 1024 || ext.SysFlag != 0 {
		t.Fatalf("offset/sysflag: %d %d", ext.CommitLogOffset, ext.SysFlag)
	}
	if ext.BodyCRC != Crc32([]byte("hello rocketmq")) {
		t.Fatalf("body crc %d", ext.BodyCRC)
	}
	if ext.BornTimestamp != 1700000000000 || ext.StoreTimestamp != 1700000000123 {
		t.Fatalf("timestamps: %d %d", ext.BornTimestamp, ext.StoreTimestamp)
	}
	if ext.BornHost != "127.0.0.1" || ext.BornHostPort != 54321 {
		t.Fatalf("born host: %s:%d", ext.BornHost, ext.BornHostPort)
	}
	if ext.StoreHost != "127.0.0.1" || ext.StoreHostPort != 10911 {
		t.Fatalf("store host: %s:%d", ext.StoreHost, ext.StoreHostPort)
	}
	if ext.ReconsumeTimes != 1 {
		t.Fatalf("reconsume %d", ext.ReconsumeTimes)
	}
	if v, ok := ext.GetProperty(PropertyTags); !ok || v != "TagA" {
		t.Fatalf("tags %q", v)
	}
	if v, ok := ext.GetProperty(PropertyKeys); !ok || v != "key1 key2" {
		t.Fatalf("keys %q", v)
	}
	if ext.StoreSize != 139 {
		t.Fatalf("store size %d", ext.StoreSize)
	}
	if ext.MsgID != extMsgID {
		t.Fatalf("msgId %q, want %q", ext.MsgID, extMsgID)
	}
	// 默认 IsClient=true：OffsetMsgID 复制 MsgID
	if ext.OffsetMsgID != extMsgID {
		t.Fatalf("offsetMsgId %q", ext.OffsetMsgID)
	}
	if ext.BornHostString() != "127.0.0.1:54321" {
		t.Fatalf("born host string %q", ext.BornHostString())
	}
}

func TestFieldOffsets(t *testing.T) {
	raw, err := EncodeMessageExt(buildExt(), false)
	if err != nil {
		t.Fatal(err)
	}
	longAt := func(p int) int64 { return int64(binary.BigEndian.Uint64(raw[p : p+8])) }
	intAt := func(p int) int32 { return int32(binary.BigEndian.Uint32(raw[p : p+4])) }
	if longAt(QueueOffsetPosition) != 88 {
		t.Fatalf("queue offset at %d", QueueOffsetPosition)
	}
	if longAt(PhyPosPosition) != 1024 {
		t.Fatalf("phy offset at %d", PhyPosPosition)
	}
	if intAt(SysflagPosition) != 0 {
		t.Fatalf("sysflag at %d", SysflagPosition)
	}
	if longAt(MessageStoreTimestampPosition) != 1700000000123 {
		t.Fatalf("store ts at %d", MessageStoreTimestampPosition)
	}
	if intAt(MessageFlagPosition) != 2 {
		t.Fatalf("flag at %d", MessageFlagPosition)
	}
	if MessageMagicCodePosition != 4 {
		t.Fatalf("magic position %d", MessageMagicCodePosition)
	}
}

func TestStoreSizeRespected(t *testing.T) {
	ext := buildExt()
	ext.StoreSize = 1 // 小于计算值：按计算值分配
	raw, err := EncodeMessageExt(ext, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 139 {
		t.Fatalf("clamped len %d", len(raw))
	}
	declared := int32(binary.BigEndian.Uint32(raw[0:4]))
	if declared != 139 {
		t.Fatalf("declared %d", declared)
	}
	// 大于计算值：按 storeSize 分配，尾部补零（Java ByteBuffer.allocate 同语义）
	ext.StoreSize = 200
	raw, err = EncodeMessageExt(ext, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 200 {
		t.Fatalf("padded len %d", len(raw))
	}
	for _, b := range raw[139:] {
		if b != 0 {
			t.Fatal("tail must be zero padded")
		}
	}
}

func TestTopicTooLongIsAnEncodeError(t *testing.T) {
	ext := buildExt()
	ext.Topic = strings.Repeat("T", 256)
	// Java 静默截断成 (byte) 长度；Go 端口选择显式报错而不是产出坏帧
	_, err := EncodeMessageExt(ext, false)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected length error, got %v", err)
	}
}

func TestMsgIDFromStoreHostAndOffset(t *testing.T) {
	addr, err := IPAndPortToBytes("127.0.0.1", 10911, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := CreateMessageID(addr, 1024); got != extMsgID {
		t.Fatalf("got %q", got)
	}
}

func TestUnknownMagicCodeIsAnError(t *testing.T) {
	raw := unhex(t, ext139Hex)
	magic := BlankMagicCode
	binary.BigEndian.PutUint32(raw[4:8], uint32(magic))
	_, err := DecodeMessage(raw)
	if err == nil || !strings.Contains(err.Error(), "unknown magic code") {
		t.Fatalf("got %v", err)
	}
}

func TestTruncatedFrameIsAnError(t *testing.T) {
	raw := unhex(t, ext139Hex)
	if _, err := DecodeMessage(raw[:20]); err == nil {
		t.Fatal("truncated frame must fail")
	}
	if _, err := DecodeMessage(nil); err == nil {
		t.Fatal("empty frame must fail")
	}
}

func TestReadBodyFalseKeepsBodyNil(t *testing.T) {
	options := DefaultDecodeOptions()
	options.ReadBody = false
	ext, err := DecodeMessageWith(unhex(t, ext139Hex), options)
	if err != nil {
		t.Fatal(err)
	}
	if ext.Body != nil {
		t.Fatalf("body must stay nil, got %d bytes", len(ext.Body))
	}
	// topic 与属性照常解出
	if ext.Topic != "TopicTest" {
		t.Fatalf("topic %q", ext.Topic)
	}
	if v, ok := ext.GetProperty(PropertyTags); !ok || v != "TagA" {
		t.Fatalf("tags %q", v)
	}
}

func TestOffsetMsgIDOnlyForClients(t *testing.T) {
	options := DefaultDecodeOptions()
	options.IsClient = false
	ext, err := DecodeMessageWith(unhex(t, ext139Hex), options)
	if err != nil {
		t.Fatal(err)
	}
	if ext.MsgID != extMsgID {
		t.Fatalf("msgId %q", ext.MsgID)
	}
	if ext.OffsetMsgID != "" {
		t.Fatalf("offsetMsgId must stay empty for non-clients, got %q", ext.OffsetMsgID)
	}
}

func TestCheckCRCDetectsCorruption(t *testing.T) {
	raw, err := EncodeMessageExt(buildExt(), false)
	if err != nil {
		t.Fatal(err)
	}
	// 固定头 68 + 两个 IPv4 地址 16 = 84，body 再往后 4 字节长度前缀
	if headerWithoutBodyLen != 68 {
		t.Fatalf("header const %d", headerWithoutBodyLen)
	}
	raw[88] ^= 0xFF
	options := DefaultDecodeOptions()
	options.CheckCRC = true
	_, err = DecodeMessageWith(raw, options)
	if err == nil || !strings.Contains(err.Error(), "crc") {
		t.Fatalf("expected crc error, got %v", err)
	}
	// 不校验 CRC 时照样能解开（body 已是坏数据）
	if _, err := DecodeMessage(raw); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyBodyFrameDecodes(t *testing.T) {
	ext, err := DecodeMessage(unhex(t, emptyBodyHex))
	if err != nil {
		t.Fatal(err)
	}
	if ext.Body != nil {
		t.Fatalf("bodyLen=0 must decode to nil body, got %v", ext.Body)
	}
	if ext.Topic != "TopicTest" {
		t.Fatalf("topic %q", ext.Topic)
	}
	if ext.Properties.Len() != 0 {
		t.Fatalf("props %d", ext.Properties.Len())
	}
	if ext.StoreSize != 100 {
		t.Fatalf("store size %d", ext.StoreSize)
	}
	if ext.MsgID != extMsgID {
		t.Fatalf("msgId %q", ext.MsgID)
	}
	// 该帧也能由 encode 产出（body=nil 走空 body）
	ext2 := buildExt()
	ext2.Body = nil
	ext2.BodyCRC = 0
	ext2.Properties = NewStringMap()
	raw, err := EncodeMessageExt(ext2, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := hexUpper(raw); got != emptyBodyHex {
		t.Fatalf("got  %s\nwant %s", got, emptyBodyHex)
	}
}

func TestDecodeMessagesSplitsStream(t *testing.T) {
	frame := unhex(t, ext139Hex)
	two := append(append([]byte{}, frame...), frame...)
	msgs := DecodeMessages(two)
	if len(msgs) != 2 {
		t.Fatalf("decoded %d", len(msgs))
	}
	if string(msgs[1].GetBody()) != "hello rocketmq" || msgs[1].QueueID != 3 {
		t.Fatalf("second message wrong: %v", msgs[1])
	}
	// 尾部垃圾/截断：停在最后一条完整消息（Java decodes 的 break 同语义）
	if got := DecodeMessages(append(append([]byte{}, frame...), 'g', 'a', 'r', 'b')); len(got) != 1 {
		t.Fatalf("garbage tail decoded %d", len(got))
	}
	if got := DecodeMessages(append(append([]byte{}, frame...), 0x00)); len(got) != 1 {
		t.Fatalf("short tail decoded %d", len(got))
	}
	if got := DecodeMessages(nil); len(got) != 0 {
		t.Fatalf("nil decoded %d", len(got))
	}
	options := DefaultDecodeOptions()
	options.ReadBody = false
	for _, m := range DecodeMessagesWith(two, options) {
		if m.Body != nil {
			t.Fatal("ReadBody=false must leave bodies nil")
		}
	}
}

func buildV2Frame(t *testing.T, body []byte, topic, props string, sysFlag int32) []byte {
	t.Helper()
	storeSize := 68 + 16 + 4 + len(body) + 2 + len(topic) + 2 + len(props)
	born, err := IPAndPortToBytes("127.0.0.1", 54321, sysFlag&MessageSysFlagBornhostV6 != 0)
	if err != nil {
		t.Fatal(err)
	}
	store, err := IPAndPortToBytes("127.0.0.1", 10911, sysFlag&MessageSysFlagStorehostV6 != 0)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriterCap(storeSize)
	w.I32(int32(storeSize)).
		I32(MessageMagicCodeV2).
		U32(Crc32(body)).
		I32(0).    // queueId
		I32(0).    // flag
		I64(0).    // queueOffset
		I64(2048). // physicOffset
		I32(sysFlag).
		I64(1700000000000).
		Bytes(born).
		I64(1700000000123).
		Bytes(store).
		I32(0). // reconsumeTimes
		I64(0). // preparedTransactionOffset
		I32(int32(len(body))).
		Bytes(body).
		U16(uint32(len(topic))).
		Bytes([]byte(topic)).
		U16(uint32(len(props))).
		Bytes([]byte(props))
	if w.Len() != storeSize {
		t.Fatalf("frame len %d, want %d", w.Len(), storeSize)
	}
	return w.IntoInner()
}

func TestV2FrameDecodes(t *testing.T) {
	topic := strings.Repeat("T", 200) // v1 的 1 字节 topic 长度放不下，必须走 v2
	body := []byte("batch-body")
	props := "TAGS\x01TagB\x02"
	frame := buildV2Frame(t, body, topic, props, 0)
	if len(frame) != 312 {
		t.Fatalf("len %d, want 312", len(frame))
	}
	ext, err := DecodeMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	if ext.Topic != topic {
		t.Fatalf("topic len %d", len(ext.Topic))
	}
	if string(ext.GetBody()) != "batch-body" {
		t.Fatalf("body %q", ext.GetBody())
	}
	if ext.CommitLogOffset != 2048 {
		t.Fatalf("phy offset %d", ext.CommitLogOffset)
	}
	if ext.BornTimestamp != 1700000000000 || ext.StoreTimestamp != 1700000000123 {
		t.Fatal("timestamps lost")
	}
	if v, ok := ext.GetProperty(PropertyTags); !ok || v != "TagB" {
		t.Fatalf("tags %q", v)
	}
	want := CreateMessageID(mustAddr(t, "127.0.0.1", 10911, false), 2048)
	if ext.MsgID != want {
		t.Fatalf("msgId %q, want %q", ext.MsgID, want)
	}
}

func TestV2FrameDecompressesBody(t *testing.T) {
	payload := []byte("rocketmq-compressed-payload-payload-payload")
	compressed, err := ZlibCompress(payload, 5)
	if err != nil {
		t.Fatal(err)
	}
	frame := buildV2Frame(t, compressed, "TopicTest", "", MessageSysFlagCompressed|MessageSysFlagCompressionZlib)
	ext, err := DecodeMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	if string(ext.GetBody()) != string(payload) {
		t.Fatalf("body not decompressed: %d bytes", len(ext.GetBody()))
	}
	// 解压后 COMPRESSED 位清除，类型位保留
	if IsCompressed(ext.SysFlag) {
		t.Fatal("compressed flag must be cleared")
	}
	if GetCompressionType(ext.SysFlag) != ZlibType {
		t.Fatalf("type bits %d", GetCompressionType(ext.SysFlag))
	}
}

func mustAddr(t *testing.T, ip string, port uint32, v6 bool) []byte {
	t.Helper()
	raw, err := IPAndPortToBytes(ip, port, v6)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestIPv6HostsRoundTrip(t *testing.T) {
	ext := buildExt()
	ext.SysFlag = MessageSysFlagBornhostV6 | MessageSysFlagStorehostV6
	ext.BornHost = "2001:db8::1"
	ext.StoreHost = "2001:db8::2"
	raw, err := EncodeMessageExt(ext, false)
	if err != nil {
		t.Fatal(err)
	}
	// 68 + 20 + 20 + 4 + 14 + 1 + 9 + 2 + 25 = 163
	if len(raw) != 163 {
		t.Fatalf("len %d, want 163", len(raw))
	}
	got, err := DecodeMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.BornHost != "2001:db8::1" || got.StoreHost != "2001:db8::2" {
		t.Fatalf("hosts: %s / %s", got.BornHost, got.StoreHost)
	}
	// 该帧自己的 offset=1024：v6 msgId 与 v4 同构
	want := CreateMessageID(mustAddr(t, "2001:db8::2", 10911, true), 1024)
	if got.MsgID != want || !strings.HasSuffix(want, "0000000000000400") {
		t.Fatalf("msgId %q, want %q", got.MsgID, want)
	}
}

func TestIPHelpers(t *testing.T) {
	v4, err := IPToBytes("127.0.0.1", false)
	if err != nil || len(v4) != 4 || v4[0] != 127 {
		t.Fatalf("v4: %v %v", v4, err)
	}
	// v4 地址带 v6=true：v4-mapped
	mapped, err := IPToBytes("127.0.0.1", true)
	if err != nil || len(mapped) != 16 {
		t.Fatalf("mapped: %v %v", mapped, err)
	}
	if mapped[10] != 0xff || mapped[11] != 0xff || mapped[15] != 1 {
		t.Fatalf("mapped shape %x", mapped)
	}
	v6, err := IPToBytes("2001:db8::1", true)
	if err != nil || len(v6) != 16 {
		t.Fatalf("v6: %v %v", v6, err)
	}
	// 非v4-mapped 的 IPv6 不带 v6 标志必须报错（Java 跨族规则）
	if _, err := IPToBytes("2001:db8::1", false); err == nil {
		t.Fatal("ipv6 without the v6 flag must fail")
	}
	if _, err := IPToBytes("not-an-ip", false); err == nil {
		t.Fatal("garbage ip must fail")
	}
	addr := mustAddr(t, "127.0.0.1", 10911, false)
	if len(addr) != 8 {
		t.Fatalf("addr len %d", len(addr))
	}
	host, port, err := BytesToIPAndPort(addr)
	if err != nil || host != "127.0.0.1" || port != 10911 {
		t.Fatalf("back: %s:%d %v", host, port, err)
	}
	if _, _, err := BytesToIPAndPort([]byte{1, 2, 3}); err == nil {
		t.Fatal("bad host length must fail")
	}
}

func TestDecodeMessageID(t *testing.T) {
	host, port, offset, err := DecodeMessageID(extMsgID)
	if err != nil || host != "127.0.0.1" || port != 10911 || offset != 1024 {
		t.Fatalf("v4: %s:%d+%d %v", host, port, offset, err)
	}
	host, port, offset, err = DecodeMessageID(extMsgIDV6)
	if err != nil || host != "2001:db8::2" || port != 10911 || offset != 64 {
		t.Fatalf("v6: %s:%d+%d %v", host, port, offset, err)
	}
	if _, _, _, err := DecodeMessageID("XYZ"); err == nil {
		t.Fatal("bad hex must fail")
	}
	if _, _, _, err := DecodeMessageID("AB"); err == nil {
		t.Fatal("odd length must fail")
	}
	if _, _, _, err := DecodeMessageID("AABB"); err == nil {
		t.Fatal("wrong total length must fail")
	}
}

func TestMessageBytesSizeMatchesEncode(t *testing.T) {
	props := NewStringMap()
	props.Put(PropertyTags, "TagA")
	props.Put(PropertyKeys, "key1 key2")
	ext := buildExt()
	raw, err := EncodeMessageExt(ext, false)
	if err != nil {
		t.Fatal(err)
	}
	size := MessageBytesSize("TopicTest", len("hello rocketmq"), props, 0)
	if size != len(raw) || size != 139 {
		t.Fatalf("size %d, encoded %d", size, len(raw))
	}
	// v6 主机地址：每侧 20 字节，共 +24
	ext.SysFlag = MessageSysFlagBornhostV6 | MessageSysFlagStorehostV6
	raw, err = EncodeMessageExt(ext, false)
	if err != nil {
		t.Fatal(err)
	}
	size = MessageBytesSize("TopicTest", len("hello rocketmq"), props, MessageSysFlagBornhostV6|MessageSysFlagStorehostV6)
	if size != len(raw) || size != 163 {
		t.Fatalf("v6 size %d, encoded %d", size, len(raw))
	}
}

func TestPropertiesStringRoundTripAndDrops(t *testing.T) {
	props := NewStringMap()
	props.Put("TAGS", "TagA")
	props.Put("KEYS", "k1 k2 k3")
	props.Put("UNIQ_KEY", "0A0A0A0A")
	raw := MessageProperties2String(props)
	if got := strings.Count(raw, string(NameValueSeparator)); got != 3 {
		t.Fatalf("kv separators %d", got)
	}
	if got := strings.Count(raw, string(PropertySeparator)); got != 3 {
		t.Fatalf("entry separators %d", got)
	}
	back := String2MessageProperties(raw)
	if !back.Equal(props) {
		t.Fatalf("round trip lost data: %v", back.Keys())
	}
	// Java：长度 < 3、没有 kv 分隔符的片段、空值片段都会被丢掉 —— 锁住该行为
	if String2MessageProperties("").Len() != 0 {
		t.Fatal("empty string")
	}
	for _, garbage := range []string{"a\x02", "ab\x02", "no-separator\x02"} {
		if String2MessageProperties(garbage).Len() != 0 {
			t.Fatalf("%q must drop", garbage)
		}
	}
	one := String2MessageProperties("k\x01v\x02")
	if v, ok := one.Get("k"); !ok || v != "v" {
		t.Fatalf("k = %q %v", v, ok)
	}
	// WAIT 空值是「非法片段」，会被丢掉
	wait := NewStringMap()
	wait.Put(PropertyWaitStoreMsgOK, "")
	rawWait := MessageProperties2String(wait)
	if rawWait != "WAIT\x01\x02" {
		t.Fatalf("wait raw %q", rawWait)
	}
	if String2MessageProperties(rawWait).Len() != 0 {
		t.Fatal("empty WAIT value must be dropped")
	}
	// unicode 往返
	cjk := NewStringMap()
	cjk.Put("中文键", "中文值 带空格")
	if !String2MessageProperties(MessageProperties2String(cjk)).Equal(cjk) {
		t.Fatal("unicode round trip")
	}
	if MessageStringSize("") != 2 {
		t.Fatal("size includes the 2-byte length prefix")
	}
}

func TestParseTopicFilterBitmap(t *testing.T) {
	got, err := ParseTopicFilterBitmap("0110")
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{false, true, true, false}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("bit %d: %v", i, got[i])
		}
	}
	if _, err := ParseTopicFilterBitmap("0102"); err == nil {
		t.Fatal("illegal char must fail")
	}
	if _, err := ParseTopicFilterBitmap(""); err != nil || len(got) == 0 {
		t.Fatalf("empty bitmap: %v %v", got, err)
	}
}
