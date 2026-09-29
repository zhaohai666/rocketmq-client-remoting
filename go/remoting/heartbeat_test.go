package remoting

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

func TestBuildSubscriptionDataSubAll(t *testing.T) {
	for _, sub := range []string{"", "*"} {
		sd, err := FilterAPI{}.BuildSubscriptionData("Tt", sub)
		if err != nil {
			t.Fatalf("%q: unexpected error %v", sub, err)
		}
		if sd.SubString != "*" {
			t.Errorf("%q: subString = %q, want *", sub, sd.SubString)
		}
		// 空订阅的 tagsSet/codeSet 必须留空：tagsSet 非空是客户端二次过滤的开关，
		// 塞 "*" 会把全量订阅自己过滤掉。
		if len(sd.TagsSet) != 0 || len(sd.CodeSet) != 0 {
			t.Errorf("%q: sets must stay empty, got %v/%v", sub, sd.TagsSet, sd.CodeSet)
		}
	}
}

func TestBuildSubscriptionDataTags(t *testing.T) {
	sd, err := FilterAPI{}.BuildSubscriptionData("Tt", " TagB || TagA || TagB ")
	if err != nil {
		t.Fatal(err)
	}
	// subString 保留原始空格，tag trim + 去重 + 升序。
	if sd.SubString != " TagB || TagA || TagB " {
		t.Errorf("subString must keep raw spacing, got %q", sd.SubString)
	}
	if strings.Join(sd.TagsSet, ",") != "TagA,TagB" {
		t.Errorf("tags = %v, want [TagA TagB]", sd.TagsSet)
	}
	if len(sd.CodeSet) != 2 || sd.CodeSet[0] != common.JavaStringHash("TagA") || sd.CodeSet[1] != common.JavaStringHash("TagB") {
		t.Errorf("codes = %v", sd.CodeSet)
	}
}

func TestBuildSubscriptionDataWhitespaceOnly(t *testing.T) {
	sd, err := FilterAPI{}.BuildSubscriptionData("Tt", "   ")
	if err != nil {
		t.Fatal(err)
	}
	// 纯空白走切分分支：两个集合空，但 subString 仍是原样（Java 只认 null/""）。
	if sd.SubString != "   " || len(sd.TagsSet) != 0 || len(sd.CodeSet) != 0 {
		t.Errorf("whitespace-only: got %q %v %v", sd.SubString, sd.TagsSet, sd.CodeSet)
	}
}

func TestBuildSubscriptionDataEdgeSplits(t *testing.T) {
	// "|||"：切分只丢末尾空串，中间的 "|" 留下。
	sd, err := FilterAPI{}.BuildSubscriptionData("Tt", "|||")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(sd.TagsSet, ",") != "|" || len(sd.CodeSet) != 1 || sd.CodeSet[0] != 124 {
		t.Errorf(`"|||" must give tags {"|"} codes {124}, got %v %v`, sd.TagsSet, sd.CodeSet)
	}
	for _, sub := range []string{"||", "||||"} {
		_, err := (FilterAPI{}).BuildSubscriptionData("Tt", sub)
		if err == nil || !strings.HasSuffix(err.Error(), "subString split error") {
			t.Errorf("%q must fail with subString split error, got %v", sub, err)
		}
	}
}

func TestSubscriptionDataJSONRoundTrip(t *testing.T) {
	sd := NewSubscriptionData("Tt", "TagA || TagB")
	sd.SetTags([]string{"TagB", "TagA", "TagA"}, []int32{common.JavaStringHash("TagB"), common.JavaStringHash("TagA")})
	enc := EncodeJSON(sd.ToJSONValue())
	back := &SubscriptionData{}
	if err := back.FromJSONValue(mustDecode(t, enc)); err != nil {
		t.Fatal(err)
	}
	if !back.Equal(sd) {
		t.Errorf("round trip mismatch: %+v vs %+v", back, sd)
	}
	if len(back.TagsSet) != 2 || len(back.CodeSet) != 2 {
		t.Errorf("dedup failed: %v %v", back.TagsSet, back.CodeSet)
	}
}

func TestSubscriptionDataDecodeTolerantItems(t *testing.T) {
	// tagsSet 里的数字/布尔按文本收；codeSet 的数字字符串也能读（Python int(v)）。
	text := `{"topic":"Tt","subString":"*","tagsSet":["TagA",7,true],"codeSet":["11",22],"subVersion":1234567890,"expressionType":"TAG"}`
	back := &SubscriptionData{}
	if err := back.FromJSONValue(mustDecode(t, []byte(text))); err != nil {
		t.Fatal(err)
	}
	if strings.Join(back.TagsSet, ",") != "TagA,7,true" {
		t.Errorf("tags = %v", back.TagsSet)
	}
	if len(back.CodeSet) != 2 || back.CodeSet[0] != 11 || back.CodeSet[1] != 22 {
		t.Errorf("codes = %v", back.CodeSet)
	}
}

func TestHeartbeatDataGoldenShape(t *testing.T) {
	hb := NewHeartbeatData("client@foo")
	cd := NewConsumerData("grp", ConsumeTypeConsumePassively, MessageModelClustering, ConsumeFromWhereLastOffset)
	sub, err := FilterAPI{}.BuildSubscriptionData("Tt", "TagA")
	if err != nil {
		t.Fatal(err)
	}
	cd.AddSubscriptionData(sub)
	hb.AddConsumerData(cd)
	hb.AddProducerData(NewProducerData("pid"))
	// 重复添加要被折叠。
	hb.AddConsumerData(cd)

	text := string(hb.Encode())
	for _, want := range []string{
		`"clientID":"client@foo"`,
		`"heartbeatFingerprint":0`,
		`"withoutSub":false`,
		`"consumeType":"CONSUME_PASSIVELY"`,
		`"messageModel":"CLUSTERING"`,
		`"consumeFromWhere":"CONSUME_FROM_LAST_OFFSET"`,
		`"unitMode":false`,
		`"groupName":"grp"`,
		`"groupName":"pid"`,
		`"expressionType":"TAG"`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("heartbeat body missing %s: %s", want, text)
		}
	}
	if strings.Count(text, `"groupName":"grp"`) != 1 {
		t.Error("duplicate ConsumerData must be folded")
	}

	back, err := DecodeHeartbeatData([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if back.ClientID != "client@foo" || len(back.ConsumerDataSet) != 1 || len(back.ProducerDataSet) != 1 {
		t.Errorf("decode mismatch: %+v", back)
	}
	if back.ConsumerDataSet[0].SubscriptionDataSet[0].Topic != "Tt" {
		t.Errorf("subscription lost: %+v", back.ConsumerDataSet[0])
	}
}

func mustDecode(t *testing.T, data []byte) any {
	t.Helper()
	v, err := DecodeJSON(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return v
}
