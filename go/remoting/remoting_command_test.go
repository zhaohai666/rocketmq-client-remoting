package remoting

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

func TestOutgoingCommandsCarryTheProtocolVersion(t *testing.T) {
	if NewCommand().Version != 0 {
		t.Fatal("bare NewCommand is a decode helper and must keep version 0")
	}
	if got := CreateRequestCommand(ReqHeartBeat, nil).Version; got != CurrentVersion {
		t.Fatalf("request version = %d", got)
	}
	if got := CreateResponseCommand(RespSuccess, "").Version; got != CurrentVersion {
		t.Fatalf("response version = %d", got)
	}
}

func TestVersionEnvOverride(t *testing.T) {
	t.Setenv(RemotingVersionKey, "99")
	if got := CreateRequestCommand(ReqHeartBeat, nil).Version; got != 99 {
		t.Fatalf("version = %d", got)
	}
	t.Setenv(RemotingVersionKey, "")
	t.Setenv(RemotingVersionKeyUC, "62")
	if got := CreateRequestCommand(ReqHeartBeat, nil).Version; got != 62 {
		t.Fatalf("version = %d", got)
	}
	t.Setenv(RemotingVersionKeyUC, "junk")
	if got := CreateRequestCommand(ReqHeartBeat, nil).Version; got != CurrentVersion {
		t.Fatalf("junk must fall back to %d, got %d", CurrentVersion, got)
	}
}

func TestOpaqueMonotonic(t *testing.T) {
	a := NextOpaque()
	b := NextOpaque()
	if b != a+1 {
		t.Fatalf("opaque not monotonic: %d -> %d", a, b)
	}
}

func TestFlagBits(t *testing.T) {
	req := CreateRequestCommand(ReqHeartBeat, nil)
	if req.IsResponseType() || req.IsOnewayRPC() {
		t.Fatal("fresh request must not be response/oneway")
	}
	req.MarkOnewayRPC()
	if !req.IsOnewayRPC() {
		t.Fatal("oneway bit missing")
	}
	resp := CreateResponseCommand(RespSuccess, "")
	if !resp.IsResponseType() {
		t.Fatal("response bit missing")
	}
}

func TestMakeCustomHeaderToNet(t *testing.T) {
	header := &HeartbeatRequestHeader{ClientID: StrPtr("cid-1")}
	cmd := CreateRequestCommand(ReqHeartBeat, header)
	if _, ok := cmd.GetExtField("clientID"); ok {
		t.Fatal("extFields must be empty before MakeCustomHeaderToNet")
	}
	cmd.MakeCustomHeaderToNet()
	if v, ok := cmd.GetExtField("clientID"); !ok || v != "cid-1" {
		t.Fatalf("clientID = %q %v", v, ok)
	}
}

func TestJSONFrameRoundTrip(t *testing.T) {
	cmd := CreateRequestCommand(ReqGetRouteInfoByTopic, &GetRouteInfoRequestHeader{
		Topic: StrPtr("TopicTest"),
	})
	cmd.MakeCustomHeaderToNet()
	cmd.AddExtField("extra", "a<b>&c")
	cmd.SetBody([]byte("hello"))

	frame := cmd.Encode()
	back, err := Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if back.Code != ReqGetRouteInfoByTopic {
		t.Fatalf("code = %d", back.Code)
	}
	if back.Opaque != cmd.Opaque {
		t.Fatalf("opaque = %d", back.Opaque)
	}
	if back.IsResponseType() {
		t.Fatal("request must stay a request")
	}
	if v, ok := back.GetExtField("topic"); !ok || v != "TopicTest" {
		t.Fatalf("topic = %q %v", v, ok)
	}
	if v, ok := back.GetExtField("extra"); !ok || v != "a<b>&c" {
		t.Fatalf("HTML-escaped extField: %q", v)
	}
	if !bytes.Equal(back.Body, []byte("hello")) {
		t.Fatalf("body = %q", back.Body)
	}
	if back.SerializeTypeCurrentRPC != SerializeTypeJSON {
		t.Fatalf("serializeType = %d", back.SerializeTypeCurrentRPC)
	}
}

func TestJSONFrameResponseWithRemark(t *testing.T) {
	resp := CreateResponseCommand(RespSystemError, "boom")
	resp.AddExtField("k", "v")
	back, err := Decode(resp.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if !back.IsResponseType() || back.Remark != "boom" {
		t.Fatalf("back = %s", back)
	}
}

func TestRocketMQFrameRoundTrip(t *testing.T) {
	cmd := CreateRequestCommand(ReqPullMessage, &PullMessageRequestHeader{
		ConsumerGroup: StrPtr("G"),
		Topic:         StrPtr("T"),
		QueueID:       I32Ptr(3),
	})
	cmd.SerializeTypeCurrentRPC = SerializeTypeRocketMQ
	frame := cmd.Encode()

	// The packed header length must carry the ROCKETMQ type in the high bits.
	if got := ProtocolTypeOf(int32(frame[4])<<24 | int32(frame[5])<<16 | int32(frame[6])<<8 | int32(frame[7])); got != SerializeTypeRocketMQ {
		t.Fatalf("frame serialize type = %d", got)
	}

	back, err := Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if back.Code != ReqPullMessage || back.SerializeTypeCurrentRPC != SerializeTypeRocketMQ {
		t.Fatalf("back = %s", back)
	}
	if v, ok := back.GetExtField("consumerGroup"); !ok || v != "G" {
		t.Fatalf("consumerGroup = %q %v", v, ok)
	}
	if v, ok := back.GetExtField("queueId"); !ok || v != "3" {
		t.Fatalf("queueId = %q %v", v, ok)
	}
}

func TestEncodeHeaderMatchesEncodePrefix(t *testing.T) {
	cmd := CreateRequestCommand(ReqHeartBeat, &HeartbeatRequestHeader{ClientID: StrPtr("c")})
	body := []byte("0123456789")
	full := cmd.Encode()
	head := cmd.EncodeHeader(len(body))

	headerLen := HeaderLengthOf(beU32(head[4:8]))
	if headerLen != int32(len(head)-8) {
		t.Fatalf("header length word = %d, actual header bytes = %d", headerLen, len(head)-8)
	}
	if beU32(head[0:4]) != 4+headerLen+int32(len(body)) {
		t.Fatal("EncodeHeader totalLength must include the body placeholder")
	}
	if !bytes.Equal(full[8:], head[8:]) {
		t.Fatal("header bytes must match Encode")
	}
}

func beU32(b []byte) int32 {
	return int32(b[0])<<24 | int32(b[1])<<16 | int32(b[2])<<8 | int32(b[3])
}

func TestDecodeRejectsBadLengths(t *testing.T) {
	frame := CreateRequestCommand(ReqHeartBeat, nil).Encode()

	bad := append([]byte(nil), frame...)
	bad[0] = 0x7F // totalLength far beyond the buffer
	if _, err := Decode(bad); err == nil {
		t.Fatal("expected bad total length error")
	}

	bad2 := append([]byte(nil), frame...)
	bad2[5] = 0x7F // headerLength beyond remaining bytes
	if _, err := Decode(bad2); err == nil {
		t.Fatal("expected bad header length error")
	}
}

func jsonFrame(t *testing.T, headerJSON string) []byte {
	t.Helper()
	frame := make([]byte, 0, 8+len(headerJSON))
	frame = beAppendU32(frame, uint32(4+len(headerJSON)))
	frame = beAppendU32(frame, uint32(MarkProtocolType(int32(len(headerJSON)), SerializeTypeJSON)))
	return append(frame, headerJSON...)
}

func beAppendU32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func TestDecodeJSONLanguageVariants(t *testing.T) {
	cmd, err := Decode(jsonFrame(t, `{"code":105,"language":"JAVA","version":515,"opaque":9,"flag":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Language != LangJava {
		t.Fatalf("language = %d", cmd.Language)
	}
	if cmd.Opaque != 9 {
		t.Fatalf("opaque = %d", cmd.Opaque)
	}
	cmd2, err := Decode(jsonFrame(t, `{"code":105}`))
	if err != nil {
		t.Fatal(err)
	}
	if cmd2.Language != LangGo || cmd2.Opaque != -1 {
		t.Fatalf("defaults: language=%d opaque=%d", cmd2.Language, cmd2.Opaque)
	}
	// Numeric language code also decodes.
	cmd3, err := Decode(jsonFrame(t, `{"code":105,"language":9}`))
	if err != nil {
		t.Fatal(err)
	}
	if cmd3.Language != LangGo {
		t.Fatalf("numeric language = %d", cmd3.Language)
	}
}

func TestToJSONValueShape(t *testing.T) {
	cmd := CreateRequestCommand(ReqHeartBeat, nil)
	cmd.AddExtField("k", "v")
	m := cmd.ToJSONValue()
	if m["code"] != ReqHeartBeat || m["flag"] != int32(0) {
		t.Fatalf("m = %#v", m)
	}
	if _, ok := m["remark"]; ok {
		t.Fatal("empty remark must be omitted")
	}
	ext, ok := m["extFields"].(map[string]any)
	if !ok || ext["k"] != "v" {
		t.Fatalf("extFields = %#v", m["extFields"])
	}

	noExt := CreateRequestCommand(ReqHeartBeat, nil).ToJSONValue()
	if _, ok := noExt["extFields"]; ok {
		t.Fatal("empty extFields must be omitted")
	}
}

func TestCommandString(t *testing.T) {
	cmd := CreateRequestCommand(ReqHeartBeat, nil)
	cmd.AddExtField("a", "1")
	s := cmd.String()
	for _, want := range []string{"code=34", "opaque=", "a=1"} {
		if !strings.Contains(s, want) {
			t.Fatalf("String() missing %q: %s", want, s)
		}
	}
}

// CurrentVersionDesc must stay paired with CurrentVersion. There is no
// arithmetic relation to check (Java's Version ordinals are hand-maintained:
// V5_5_0=513, V5_5_1=515, V5_5_2=517), so both values are pinned literals —
// editing either one alone fails here, which is the whole point. A stale pair
// would put a version in the 307 answer that this client does not speak.
func TestCurrentVersionDescMatchesCurrentVersion(t *testing.T) {
	// The 5.5.1 jars: CURRENT_VERSION = Version.V5_5_1.ordinal() = 515 and
	// getVersionDesc(515) == "V5_5_1".
	if CurrentVersion != 515 {
		t.Errorf("CurrentVersion = %d, want 515", CurrentVersion)
	}
	if CurrentVersionDesc != "V5_5_1" {
		t.Errorf("CurrentVersionDesc = %q, want V5_5_1", CurrentVersionDesc)
	}
	// It must be a real version name: the console parses the digits out of
	// PROP_CLIENT_VERSION, so a placeholder reads as a much older (or newer)
	// client. `HIGHER_VERSION` is what Java returns for an out-of-range ordinal,
	// i.e. exactly the wrong answer to hardcode here.
	if !versionDescPattern.MatchString(CurrentVersionDesc) {
		t.Errorf("CurrentVersionDesc = %q, want a V<maj>_<min>_<patch> name", CurrentVersionDesc)
	}
	if CurrentVersionDesc == "HIGHER_VERSION" {
		t.Error("HIGHER_VERSION is getVersionDesc's out-of-range fallback, not a version")
	}
}

var versionDescPattern = regexp.MustCompile(`^V\d+_\d+_\d+$`)

func TestVersionDescPattern(t *testing.T) {
	for _, ok := range []string{"V5_5_1", "V5_5_0", "V5_9_9", "V3_0_0_SNAPSHOT"} {
		// NOTE: a _SNAPSHOT suffix is a distinct constant and must NOT match.
		if ok == "V3_0_0_SNAPSHOT" {
			if versionDescPattern.MatchString(ok) {
				t.Errorf("%q must not match — it is a different constant", ok)
			}
			continue
		}
		if !versionDescPattern.MatchString(ok) {
			t.Errorf("%q must match", ok)
		}
	}
	for _, bad := range []string{"", "V5", "V5_5", "5.5.1", "HIGHER_VERSION", "v5_5_1"} {
		if versionDescPattern.MatchString(bad) {
			t.Errorf("%q must not match", bad)
		}
	}
}
