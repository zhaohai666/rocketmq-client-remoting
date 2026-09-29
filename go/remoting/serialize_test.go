package remoting

import (
	"bytes"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

func TestMapSerializeRoundTrip(t *testing.T) {
	ext := common.NewStringMap()
	ext.Put("topic", "TopicTest")
	ext.Put("queueId", "3")
	data := MapSerialize(ext)
	if data == nil {
		t.Fatal("expected non-nil bytes")
	}
	back, pos, err := MapDeserialize(data, 0, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if pos != len(data) {
		t.Fatalf("pos = %d", pos)
	}
	if !back.Equal(ext) {
		t.Fatalf("round trip mismatch: %#v", back)
	}
	if MapSerialize(common.NewStringMap()) != nil {
		t.Fatal("empty map must serialize to nil")
	}
}

func TestMapDeserializeOverrun(t *testing.T) {
	ext := common.NewStringMap()
	ext.Put("a", "b")
	data := MapSerialize(ext)
	if _, _, err := MapDeserialize(data, 0, len(data)+10); err == nil {
		t.Fatal("expected overrun error")
	}
	if _, _, err := MapDeserialize(data, -1, 1); err == nil {
		t.Fatal("expected negative offset error")
	}
}

func TestCalTotalLen(t *testing.T) {
	// 2+1+2+4+4+4 + 5(remark) + 4 + 7(ext)
	if got := CalTotalLen([]byte("hello"), []byte("topic=1")); got != 33 {
		t.Fatalf("CalTotalLen = %d, want 33", got)
	}
	if got := CalTotalLen(nil, nil); got != 21 {
		t.Fatalf("CalTotalLen(nil,nil) = %d, want 21", got)
	}
}

func TestMarkProtocolTypeRoundTrip(t *testing.T) {
	packed := MarkProtocolType(300, SerializeTypeRocketMQ)
	if ProtocolTypeOf(packed) != SerializeTypeRocketMQ {
		t.Fatal("protocol type lost")
	}
	if HeaderLengthOf(packed) != 300 {
		t.Fatal("header length lost")
	}
}

func TestRocketMQProtocolEncodeVector(t *testing.T) {
	cmd := NewCommand()
	cmd.Code = ReqGetRouteInfoByTopic
	cmd.Version = CurrentVersion
	cmd.Opaque = 7
	cmd.AddExtField("topic", "T")
	data := RocketMQProtocolEncode(cmd)

	// code(2)=105, language(1)=GO, version(2)=515, opaque(4)=7, flag(4)=0,
	// remarkLen(4)=0, extLen(4)=12, ext={topic:"T"}
	want := []byte{
		0x00, 0x69, // code 105
		0x09,       // language GO
		0x02, 0x03, // version 515
		0x00, 0x00, 0x00, 0x07, // opaque
		0x00, 0x00, 0x00, 0x00, // flag
		0x00, 0x00, 0x00, 0x00, // remark len
		0x00, 0x00, 0x00, 0x0C, // ext len 12
		0x00, 0x05, 't', 'o', 'p', 'i', 'c', // key
		0x00, 0x00, 0x00, 0x01, 'T', // value
	}
	if !bytes.Equal(data, want) {
		t.Fatalf("encode = %x\nwant    %x", data, want)
	}
	if len(data) != CalTotalLen(nil, data[21:]) {
		t.Fatal("CalTotalLen disagrees with encoded size")
	}

	header, err := RocketMQProtocolDecode(data)
	if err != nil {
		t.Fatal(err)
	}
	if header.Code != ReqGetRouteInfoByTopic || header.Language != LangGo ||
		header.Version != CurrentVersion || header.Opaque != 7 || header.Flag != 0 {
		t.Fatalf("header = %#v", header)
	}
	if v, ok := header.ExtFields.Get("topic"); !ok || v != "T" {
		t.Fatalf("extFields = %#v", header.ExtFields)
	}
}

func TestRocketMQProtocolEncodeWithRemark(t *testing.T) {
	cmd := NewCommand()
	cmd.Code = RespSystemError
	cmd.Opaque = 1
	cmd.Remark = "boom"
	data := RocketMQProtocolEncode(cmd)
	header, err := RocketMQProtocolDecode(data)
	if err != nil {
		t.Fatal(err)
	}
	if header.Remark != "boom" {
		t.Fatalf("remark = %q", header.Remark)
	}
}

func TestEncodeJSONNoHTMLEscape(t *testing.T) {
	out := EncodeJSON(map[string]any{"v": "<a>&b"})
	if bytes.Contains(out, []byte(`\u003c`)) || bytes.Contains(out, []byte(`\u0026`)) {
		t.Fatalf("HTML escaping leaked: %s", out)
	}
	if !bytes.Contains(out, []byte("<a>&b")) {
		t.Fatalf("raw text missing: %s", out)
	}
}

func TestExtFieldsFromJSONValue(t *testing.T) {
	v, err := ParseJSON(`{"s":"x","n":123,"big":18446744073709551615,"b":true,"nil":null,"arr":[1],"obj":{"k":1}}`)
	if err != nil {
		t.Fatal(err)
	}
	ext := extFieldsFromJSONValue(v)
	if got, _ := ext.Get("s"); got != "x" {
		t.Fatalf("s = %q", got)
	}
	if got, _ := ext.Get("n"); got != "123" {
		t.Fatalf("n = %q", got)
	}
	if got, _ := ext.Get("big"); got != "18446744073709551615" {
		t.Fatalf("big = %q", got)
	}
	if got, _ := ext.Get("b"); got != "true" {
		t.Fatalf("b = %q", got)
	}
	if ext.ContainsKey("nil") {
		t.Fatal("null entry must be dropped")
	}
	if !ext.ContainsKey("arr") || !ext.ContainsKey("obj") {
		t.Fatal("array/object entries should render as JSON text")
	}
	if got, _ := ext.Get("arr"); got != "[1]" {
		t.Fatalf("arr = %q", got)
	}
}

func TestSerializeTypeFromEnv(t *testing.T) {
	t.Setenv("ROCKETMQ_SERIALIZE_TYPE", "")
	t.Setenv("rocketmq.serialize.type", "")
	if got := SerializeTypeFromEnv(); got != SerializeTypeJSON {
		t.Fatalf("default = %d", got)
	}
	t.Setenv("ROCKETMQ_SERIALIZE_TYPE", "ROCKETMQ")
	if got := SerializeTypeFromEnv(); got != SerializeTypeRocketMQ {
		t.Fatalf("ROCKETMQ = %d", got)
	}
	t.Setenv("ROCKETMQ_SERIALIZE_TYPE", "")
	t.Setenv("rocketmq.serialize.type", "rocketmq")
	if got := SerializeTypeFromEnv(); got != SerializeTypeRocketMQ {
		t.Fatalf("lowercase rocketmq = %d", got)
	}
	t.Setenv("rocketmq.serialize.type", "JSON")
	if got := SerializeTypeFromEnv(); got != SerializeTypeJSON {
		t.Fatalf("JSON = %d", got)
	}
}
