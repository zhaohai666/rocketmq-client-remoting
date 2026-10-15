package remoting

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

func TestMessageQueueKeyJSONShape(t *testing.T) {
	got := MessageQueueKeyJSON(common.NewMessageQueue("Tt", "broker-a", 1))
	want := `{"brokerName":"broker-a","queueId":1,"topic":"Tt"}`
	if got != want {
		t.Errorf("key = %s, want %s", got, want)
	}
}

func TestResetOffsetBodyRoundTrip(t *testing.T) {
	body := &ResetOffsetBody{OffsetTable: MQOffsetTable{
		{Queue: common.NewMessageQueue("Tt", "broker-a", 0), Offset: 7},
		{Queue: common.NewMessageQueue("Tt", "broker-a", 1), Offset: 9},
	}}
	enc := body.Encode()
	back, err := DecodeResetOffsetBody(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.OffsetTable) != 2 {
		t.Fatalf("entries = %d", len(back.OffsetTable))
	}
	byQ := map[int32]int64{}
	for _, e := range back.OffsetTable {
		byQ[e.Queue.QueueID] = e.Offset
	}
	if byQ[0] != 7 || byQ[1] != 9 {
		t.Errorf("offsets = %v", byQ)
	}
	table, err := ParseResetOffsetTable(enc)
	if err != nil || len(table) != 2 {
		t.Fatalf("ParseResetOffsetTable: %v %d", err, len(table))
	}
}

func TestParseResetOffsetTableArrayShape(t *testing.T) {
	// The array body sent by the CPP side: offsetTable is an array, each entry carrying its own offset.
	text := `{"offsetTable":[{"topic":"Tt","brokerName":"broker-a","queueId":2,"offset":31}]}`
	table, err := ParseResetOffsetTable([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if len(table) != 1 || table[0].Queue.QueueID != 2 || table[0].Offset != 31 {
		t.Errorf("table = %+v", table)
	}
	// The map parser only errors on an array — the fallback array parse exists for old CPP admin clusters.
	if _, err := DecodeResetOffsetBody([]byte(text)); err == nil {
		t.Error("map parser must reject the array shape")
	}
}

func TestParseResetOffsetTableSkipsDirtyKeys(t *testing.T) {
	text := `{"offsetTable":{"{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}":5,"garbage":9}}`
	table, err := ParseResetOffsetTable([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if len(table) != 1 || table[0].Offset != 5 {
		t.Errorf("table = %+v", table)
	}
}

func TestGetConsumerStatusBodyRoundTrip(t *testing.T) {
	body := &GetConsumerStatusBody{
		MessageQueueTable: MQOffsetTable{
			{Queue: common.NewMessageQueue("Tt", "broker-a", 0), Offset: 3},
		},
		ConsumerTable: []ConsumerStatusEntry{{
			ClientID: "cid",
			MessageQueueTable: MQOffsetTable{
				{Queue: common.NewMessageQueue("Tt", "broker-a", 1), Offset: 4},
			},
		}},
	}
	back, err := DecodeGetConsumerStatusBody(body.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if len(back.MessageQueueTable) != 1 || back.MessageQueueTable[0].Offset != 3 {
		t.Errorf("messageQueueTable = %+v", back.MessageQueueTable)
	}
	if len(back.ConsumerTable) != 1 || back.ConsumerTable[0].ClientID != "cid" ||
		len(back.ConsumerTable[0].MessageQueueTable) != 1 || back.ConsumerTable[0].MessageQueueTable[0].Offset != 4 {
		t.Errorf("consumerTable = %+v", back.ConsumerTable)
	}
}

func TestCheckClientRequestBodyShape(t *testing.T) {
	sub, _ := FilterAPI{}.BuildSubscriptionData("Tt", "TagA")
	body := &CheckClientRequestBody{
		ClientID:         StrPtr("cid"),
		Group:            StrPtr("grp"),
		SubscriptionData: sub,
	}
	text := string(body.Encode())
	for _, want := range []string{`"clientId":"cid"`, `"group":"grp"`, `"subscriptionData":{`, `"subString":"TagA"`} {
		if !strings.Contains(text, want) {
			t.Errorf("body missing %s: %s", want, text)
		}
	}
	// namespace is a reserved field but the sender leaves it blank: the key does not appear.
	if strings.Contains(text, "namespace") {
		t.Errorf("namespace must stay absent: %s", text)
	}
	// When clientId/group is nil it is written as null (the key is always present, matching the broker's reflection).
	empty := &CheckClientRequestBody{}
	text2 := string(empty.Encode())
	if !strings.Contains(text2, `"clientId":null`) || !strings.Contains(text2, `"group":null`) {
		t.Errorf("nil ids must be null: %s", text2)
	}
	back, err := DecodeCheckClientRequestBody([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if back.ClientID == nil || *back.ClientID != "cid" || back.SubscriptionData == nil ||
		back.SubscriptionData.Topic != "Tt" {
		t.Errorf("round trip mismatch: %+v", back)
	}
}
