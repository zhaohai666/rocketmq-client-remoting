package remoting

import (
	"reflect"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// headerCase pins the exact extFields key sequence of a fully-populated
// header; the order and spelling must match the Java header classes.
type headerCase struct {
	name string
	h    CustomHeader
	want []string
}

func headerCases() []headerCase {
	return []headerCase{
		{
			name: "SendMessageRequestHeader",
			h: &SendMessageRequestHeader{
				ProducerGroup:         StrPtr("pg"),
				Topic:                 StrPtr("t"),
				DefaultTopic:          StrPtr("TBW102"),
				DefaultTopicQueueNums: I32Ptr(8),
				QueueID:               I32Ptr(3),
				SysFlag:               I32Ptr(0),
				BornTimestamp:         I64Ptr(1789613086027),
				Flag:                  I32Ptr(0),
				Properties:            StrPtr("TAGS\x01TagA"),
				ReconsumeTimes:        I32Ptr(0),
				UnitMode:              BoolPtr(false),
				MaxReconsumeTimes:     I32Ptr(-1),
				Batch:                 BoolPtr(false),
			},
			want: []string{
				"producerGroup", "topic", "defaultTopic", "defaultTopicQueueNums",
				"queueId", "sysFlag", "bornTimestamp", "flag", "properties",
				"reconsumeTimes", "unitMode", "maxReconsumeTimes", "batch",
			},
		},
		{
			name: "SendMessageRequestHeaderV2",
			h: &SendMessageRequestHeaderV2{
				ProducerGroup:         StrPtr("pg"),
				Topic:                 StrPtr("t"),
				DefaultTopic:          StrPtr("TBW102"),
				DefaultTopicQueueNums: I32Ptr(8),
				QueueID:               I32Ptr(3),
				SysFlag:               I32Ptr(0),
				BornTimestamp:         I64Ptr(1789613086027),
				Flag:                  I32Ptr(0),
				Properties:            StrPtr("TAGS\x01TagA"),
				ReconsumeTimes:        I32Ptr(0),
				UnitMode:              BoolPtr(false),
				MaxReconsumeTimes:     I32Ptr(-1),
				Batch:                 BoolPtr(false),
				BrokerName:            StrPtr("broker-a"),
			},
			want: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"},
		},
		{
			name: "ReplyMessageRequestHeader",
			h: &ReplyMessageRequestHeader{
				ProducerGroup:         StrPtr("pg"),
				Topic:                 StrPtr("t"),
				DefaultTopic:          StrPtr("TBW102"),
				DefaultTopicQueueNums: I32Ptr(8),
				QueueID:               I32Ptr(3),
				SysFlag:               I32Ptr(0),
				BornTimestamp:         I64Ptr(1789613086027),
				Flag:                  I32Ptr(0),
				Properties:            StrPtr("TAGS\x01TagA"),
				ReconsumeTimes:        I32Ptr(0),
				UnitMode:              BoolPtr(false),
				BornHost:              StrPtr("127.0.0.1:1"),
				StoreHost:             StrPtr("127.0.0.1:2"),
				StoreTimestamp:        I64Ptr(1700000000123),
			},
			want: []string{
				"producerGroup", "topic", "defaultTopic", "defaultTopicQueueNums",
				"queueId", "sysFlag", "bornTimestamp", "flag", "properties",
				"reconsumeTimes", "unitMode", "bornHost", "storeHost", "storeTimestamp",
			},
		},
		{
			name: "SendMessageResponseHeader",
			h: &SendMessageResponseHeader{
				MsgID:         StrPtr("0A"),
				QueueID:       I32Ptr(1),
				QueueOffset:   I64Ptr(2),
				TransactionID: StrPtr("tid"),
				BatchUniqID:   StrPtr("uid"),
				RecallHandle:  StrPtr("handle"),
			},
			want: []string{"msgId", "queueId", "queueOffset", "transactionId", "batchUniqId", "recallHandle"},
		},
		{
			name: "PullMessageRequestHeader",
			h: &PullMessageRequestHeader{
				ConsumerGroup:        StrPtr("cg"),
				Topic:                StrPtr("t"),
				LiteTopic:            StrPtr("lt"),
				QueueID:              I32Ptr(2),
				QueueOffset:          I64Ptr(7),
				MaxMsgNums:           I32Ptr(32),
				SysFlag:              I32Ptr(0),
				CommitOffset:         I64Ptr(7),
				SuspendTimeoutMillis: I64Ptr(15000),
				Subscription:         StrPtr("*"),
				SubVersion:           I64Ptr(0),
				ExpressionType:       StrPtr("TAG"),
				MaxMsgBytes:          I32Ptr(1048576),
				RequestSource:        I32Ptr(RequestSourceSDK),
				ProxyFrowardClientID: StrPtr("px"),
			},
			want: []string{
				"consumerGroup", "topic", "liteTopic", "queueId", "queueOffset",
				"maxMsgNums", "sysFlag", "commitOffset", "suspendTimeoutMillis",
				"subscription", "subVersion", "expressionType", "maxMsgBytes",
				"requestSource", "proxyFrowardClientId",
			},
		},
		{
			name: "PullMessageResponseHeader",
			h: &PullMessageResponseHeader{
				NextBeginOffset:      I64Ptr(1),
				MinOffset:            I64Ptr(2),
				MaxOffset:            I64Ptr(3),
				SuggestWhichBrokerID: I32Ptr(0),
				TopicSysFlag:         I32Ptr(0),
				GroupSysFlag:         I32Ptr(0),
				ForbiddenType:        I32Ptr(1),
				OffsetDelta:          I64Ptr(4),
			},
			want: []string{
				"nextBeginOffset", "minOffset", "maxOffset", "suggestWhichBrokerId",
				"topicSysFlag", "groupSysFlag", "forbiddenType", "offsetDelta",
			},
		},
		{
			name: "EndTransactionRequestHeader",
			h: &EndTransactionRequestHeader{
				Topic:                StrPtr("t"),
				ProducerGroup:        StrPtr("pg"),
				TranStateTableOffset: I64Ptr(1),
				CommitLogOffset:      I64Ptr(2),
				CommitOrRollback:     I32Ptr(0),
				FromTransactionCheck: BoolPtr(false),
				MsgID:                StrPtr("m"),
				TransactionID:        StrPtr("tid"),
				Bname:                StrPtr("broker-a"),
			},
			want: []string{
				"topic", "producerGroup", "tranStateTableOffset", "commitLogOffset",
				"commitOrRollback", "fromTransactionCheck", "msgId", "transactionId", "bname",
			},
		},
		{
			name: "CheckTransactionStateRequestHeader",
			h: &CheckTransactionStateRequestHeader{
				Topic:                StrPtr("t"),
				TranStateTableOffset: I64Ptr(1),
				CommitLogOffset:      I64Ptr(2),
				MsgID:                StrPtr("m"),
				TransactionID:        StrPtr("tid"),
				OffsetMsgID:          StrPtr("om"),
				Bname:                StrPtr("broker-a"),
			},
			want: []string{
				"topic", "tranStateTableOffset", "commitLogOffset", "msgId",
				"transactionId", "offsetMsgId", "bname",
			},
		},
		{
			name: "ConsumerSendMsgBackRequestHeader",
			h: &ConsumerSendMsgBackRequestHeader{
				Offset:            I64Ptr(9),
				Group:             StrPtr("g"),
				DelayLevel:        I32Ptr(3),
				OriginMsgID:       StrPtr("om"),
				OriginTopic:       StrPtr("t"),
				UnitMode:          BoolPtr(false),
				MaxReconsumeTimes: I32Ptr(16),
			},
			want: []string{
				"offset", "group", "delayLevel", "originMsgId", "originTopic",
				"unitMode", "maxReconsumeTimes",
			},
		},
		{
			name: "QueryConsumerOffsetRequestHeader",
			h: &QueryConsumerOffsetRequestHeader{
				ConsumerGroup:     StrPtr("cg"),
				Topic:             StrPtr("t"),
				QueueID:           I32Ptr(1),
				SetZeroIfNotFound: BoolPtr(true),
			},
			want: []string{"consumerGroup", "topic", "queueId", "setZeroIfNotFound"},
		},
		{
			name: "GetRouteInfoRequestHeader",
			h: &GetRouteInfoRequestHeader{
				Topic:                  StrPtr("t"),
				AcceptStandardJSONOnly: BoolPtr(true),
			},
			want: []string{"topic", "acceptStandardJsonOnly"},
		},
		{
			name: "HeartbeatRequestHeader",
			h:    &HeartbeatRequestHeader{ClientID: StrPtr("cid")},
			want: []string{"clientID"},
		},
		{
			name: "UnregisterClientRequestHeader",
			h: &UnregisterClientRequestHeader{
				ClientID:      StrPtr("cid"),
				ProducerGroup: StrPtr("pg"),
				ConsumerGroup: StrPtr("cg"),
			},
			want: []string{"clientID", "producerGroup", "consumerGroup"},
		},
		{
			name: "RecallMessageRequestHeader",
			h: &RecallMessageRequestHeader{
				ProducerGroup: StrPtr("pg"),
				Topic:         StrPtr("t"),
				RecallHandle:  StrPtr("handle"),
				Bname:         StrPtr("broker-a"),
			},
			want: []string{"producerGroup", "topic", "recallHandle", "bname"},
		},
	}
}

func TestHeaderExtKeysExactlyMatchJava(t *testing.T) {
	for _, c := range headerCases() {
		ext := common.NewStringMap()
		c.h.ToExtFields(ext)
		if !reflect.DeepEqual(ext.Keys(), c.want) {
			t.Errorf("%s keys = %v\nwant           %v", c.name, ext.Keys(), c.want)
		}
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	for _, c := range headerCases() {
		ext := common.NewStringMap()
		c.h.ToExtFields(ext)
		back := reflect.New(reflect.TypeOf(c.h).Elem()).Interface().(CustomHeader)
		back.FromExtFields(ext)
		if !reflect.DeepEqual(back, c.h) {
			t.Errorf("%s round trip:\n got %#v\nwant %#v", c.name, back, c.h)
		}
	}
}

func TestOptionalFieldsAreSkippedWhenNil(t *testing.T) {
	ext := common.NewStringMap()
	(&SendMessageRequestHeader{Topic: StrPtr("t")}).ToExtFields(ext)
	if !reflect.DeepEqual(ext.Keys(), []string{"topic"}) {
		t.Fatalf("keys = %v", ext.Keys())
	}
}

func TestV2ShortKeysAndBrokerName(t *testing.T) {
	v1 := &SendMessageRequestHeader{
		ProducerGroup:     StrPtr("pg"),
		Topic:             StrPtr("TopicTest"),
		UnitMode:          BoolPtr(false),
		MaxReconsumeTimes: I32Ptr(-1),
	}
	ext1 := common.NewStringMap()
	v1.ToExtFields(ext1)
	if v, _ := ext1.Get("unitMode"); v != "false" {
		t.Fatalf("unitMode = %q", v)
	}
	if v, _ := ext1.Get("maxReconsumeTimes"); v != "-1" {
		t.Fatalf("maxReconsumeTimes = %q", v)
	}

	v2 := v1.CreateV2()
	v2.BrokerName = StrPtr("broker-a")
	ext2 := common.NewStringMap()
	v2.ToExtFields(ext2)
	if got := ext2.Keys(); !reflect.DeepEqual(got, []string{"a", "b", "k", "l", "n"}) {
		t.Fatalf("v2 keys = %v", got)
	}
	if v, _ := ext2.Get("a"); v != "pg" {
		t.Fatalf("a = %q", v)
	}
	if v, _ := ext2.Get("k"); v != "false" {
		t.Fatalf("k = %q", v)
	}
	if v, _ := ext2.Get("n"); v != "broker-a" {
		t.Fatalf("n = %q", v)
	}

	var back SendMessageRequestHeaderV2
	back.FromExtFields(ext2)
	if !reflect.DeepEqual(&back, v2) {
		t.Fatalf("v2 round trip: %#v vs %#v", &back, v2)
	}
	back1 := back.CreateV1()
	if !reflect.DeepEqual(back1, v1) {
		t.Fatalf("v1 conversion: %#v vs %#v", back1, v1)
	}
}

func TestFieldlessHeadersWriteNothing(t *testing.T) {
	ext := common.NewStringMap()
	(&GetConsumerListByGroupResponseHeader{}).ToExtFields(ext)
	if ext.Len() != 0 {
		t.Fatalf("fieldless header wrote %v", ext.Keys())
	}
}

func TestNumericParsingIsLenient(t *testing.T) {
	ext := common.NewStringMap()
	ext.Put("offset", "not-a-number")
	ext.Put("queueId", "1.5")
	var resp QueryConsumerOffsetResponseHeader
	resp.FromExtFields(ext)
	if resp.Offset != nil {
		t.Fatal("dirty offset must degrade to nil")
	}
	var req QueryConsumerOffsetRequestHeader
	req.FromExtFields(ext)
	if req.QueueID != nil || req.ConsumerGroup != nil {
		t.Fatal("dirty fields must degrade to nil")
	}

	big := common.NewStringMap()
	big.Put("queueId", "99999999999999")
	var req2 QueryConsumerOffsetRequestHeader
	req2.FromExtFields(big)
	if req2.QueueID != nil {
		t.Fatal("out-of-range int32 must degrade to nil")
	}
}

func TestSearchOffsetBoundaryType(t *testing.T) {
	upper := BoundaryUpper
	h := &SearchOffsetRequestHeader{Topic: StrPtr("t"), QueueID: I32Ptr(2), Timestamp: I64Ptr(1700000000000)}
	ext := common.NewStringMap()
	h.ToExtFields(ext)
	if !reflect.DeepEqual(ext.Keys(), []string{"topic", "queueId", "timestamp"}) {
		t.Fatalf("keys = %v", ext.Keys())
	}

	h.BoundaryType = &upper
	ext = common.NewStringMap()
	h.ToExtFields(ext)
	if v, _ := ext.Get("boundaryType"); v != "UPPER" {
		t.Fatalf("boundaryType = %q", v)
	}

	var back SearchOffsetRequestHeader
	back.FromExtFields(ext)
	if back.BoundaryType == nil || *back.BoundaryType != BoundaryUpper {
		t.Fatalf("boundaryType = %v", back.BoundaryType)
	}

	for _, text := range []string{"LOWER", "lower", "", "junk"} {
		e := common.NewStringMap()
		e.Put("boundaryType", text)
		var b SearchOffsetRequestHeader
		b.FromExtFields(e)
		if b.BoundaryType == nil || *b.BoundaryType != BoundaryLower {
			t.Fatalf("%q must decode as LOWER", text)
		}
	}
	var missing SearchOffsetRequestHeader
	missing.FromExtFields(common.NewStringMap())
	if missing.BoundaryType != nil {
		t.Fatal("missing boundaryType must stay nil")
	}
}

func TestBoolFieldsSerializeLowercase(t *testing.T) {
	ext := common.NewStringMap()
	(&QueryConsumerOffsetRequestHeader{SetZeroIfNotFound: BoolPtr(true)}).ToExtFields(ext)
	if v, _ := ext.Get("setZeroIfNotFound"); v != "true" {
		t.Fatalf("setZeroIfNotFound = %q", v)
	}
	(&QueryConsumerOffsetRequestHeader{SetZeroIfNotFound: BoolPtr(false)}).ToExtFields(ext)
	if v, _ := ext.Get("setZeroIfNotFound"); v != "false" {
		t.Fatalf("setZeroIfNotFound = %q", v)
	}
}

func TestInheritedBrokerNameIsBnameNotBrokerName(t *testing.T) {
	// RecallMessageRequestHeader was covered in the key table; assert the
	// EndTransaction/CheckTransaction payloads carry no "brokerName" key.
	ext := common.NewStringMap()
	(&EndTransactionRequestHeader{Bname: StrPtr("b"), Topic: StrPtr("t")}).ToExtFields(ext)
	if ext.ContainsKey("brokerName") {
		t.Fatal("EndTransactionRequestHeader must use bname, not brokerName")
	}
	ext2 := common.NewStringMap()
	(&CheckTransactionStateRequestHeader{Bname: StrPtr("b"), Topic: StrPtr("t")}).ToExtFields(ext2)
	if ext2.ContainsKey("brokerName") {
		t.Fatal("CheckTransactionStateRequestHeader must use bname, not brokerName")
	}
}
