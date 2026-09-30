package remoting

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Guards for the bodies a CLIENT emits: the per-queue snapshots inside
// ConsumerRunningInfo and the CONSUME_MESSAGE_DIRECTLY verdict. Every expected
// byte string below is the verbatim fastjson2 2.0.64 output from the 5.5.1
// jars (probe transcript quoted in client_info_bodies.go), so a rename or a
// stray always-present field fails here instead of on a live broker — where
// fastjson2 reflects by Java property name and silently DROPS the key.

// The constant NAME has the underscores and the VALUE does not. This is the
// single place in ConsumerRunningInfo where Java's identifier and its wire
// value disagree, and getting it wrong makes the orderly flag vanish from every
// 307 answer.
func TestConsumerRunningInfoPropertyKeySpellings(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"PROP_NAMESERVER_ADDR", PropNameServerAddr, "PROP_NAMESERVER_ADDR"},
		{"PROP_THREADPOOL_CORE_SIZE", PropThreadPoolCoreSize, "PROP_THREADPOOL_CORE_SIZE"},
		{"PROP_CONSUME_ORDERLY", PropConsumeOrderly, "PROP_CONSUMEORDERLY"},
		{"PROP_CONSUME_TYPE", PropConsumeType, "PROP_CONSUME_TYPE"},
		{"PROP_CLIENT_VERSION", PropClientVersion, "PROP_CLIENT_VERSION"},
		{"PROP_CONSUMER_START_TIMESTAMP", PropConsumerStartTimestamp, "PROP_CONSUMER_START_TIMESTAMP"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
	// Spell it out: the underscored spelling must NOT be what goes on the wire.
	if PropConsumeOrderly == "PROP_CONSUME_ORDERLY" {
		t.Error("PropConsumeOrderly regressed to the underscored form; Java's VALUE has no underscore")
	}
}

func TestProcessQueueInfoEmptyBodyMatchesJava(t *testing.T) {
	want := `{"cachedMsgCount":0,"cachedMsgMaxOffset":0,"cachedMsgMinOffset":0,` +
		`"cachedMsgSizeInMiB":0,"commitOffset":0,"droped":false,` +
		`"lastConsumeTimestamp":0,"lastLockTimestamp":0,"lastPullTimestamp":0,` +
		`"locked":false,"transactionMsgCount":0,"transactionMsgMaxOffset":0,` +
		`"transactionMsgMinOffset":0,"tryUnlockTimes":0}`
	if got := string((&ProcessQueueInfo{}).Encode()); got != want {
		t.Errorf("ProcessQueueInfo empty body\n got %s\nwant %s", got, want)
	}
}

func TestProcessQueueInfoFilledBodyMatchesJava(t *testing.T) {
	pqi := &ProcessQueueInfo{
		CommitOffset: 12345,

		CachedMsgMinOffset: 100,
		CachedMsgMaxOffset: 200,
		CachedMsgCount:     7,
		CachedMsgSizeInMiB: 3,

		TransactionMsgMinOffset: 300,
		TransactionMsgMaxOffset: 400,
		TransactionMsgCount:     2,

		Locked:            true,
		TryUnlockTimes:    9,
		LastLockTimestamp: 111,

		Droped:               true,
		LastPullTimestamp:    222,
		LastConsumeTimestamp: 333,
	}
	got := string(pqi.Encode())
	if !strings.Contains(got, `"cachedMsgSizeInMiB":3`) {
		t.Errorf("body = %s, want cachedMsgSizeInMiB 3 (a Java int, not a double)", got)
	}
	if !strings.Contains(got, `"cachedMsgSizeInMiB":3.0`) {
		// intentional: make sure it is NOT rendered as a JavaDouble
	} else {
		t.Errorf("body = %s, cachedMsgSizeInMiB must not carry a decimal point", got)
	}
	for _, kv := range []string{
		`"commitOffset":12345`, `"cachedMsgMinOffset":100`, `"cachedMsgMaxOffset":200`,
		`"cachedMsgCount":7`, `"transactionMsgMinOffset":300`, `"transactionMsgMaxOffset":400`,
		`"transactionMsgCount":2`, `"locked":true`, `"tryUnlockTimes":9`,
		`"lastLockTimestamp":111`, `"droped":true`, `"lastPullTimestamp":222`,
		`"lastConsumeTimestamp":333`,
	} {
		if !strings.Contains(got, kv) {
			t.Errorf("body = %s, missing %s", got, kv)
		}
	}
}

func TestProcessQueueInfoRoundTrips(t *testing.T) {
	in := &ProcessQueueInfo{
		CommitOffset: 42, CachedMsgMinOffset: 1, CachedMsgMaxOffset: 2,
		CachedMsgCount: 3, CachedMsgSizeInMiB: 4,
		TransactionMsgMinOffset: 5, TransactionMsgMaxOffset: 6, TransactionMsgCount: 7,
		Locked: true, TryUnlockTimes: 8, LastLockTimestamp: 9,
		Droped: true, LastPullTimestamp: 10, LastConsumeTimestamp: 11,
	}
	out := &ProcessQueueInfo{}
	if err := out.FromJSONValue(in.ToJSONValue()); err != nil {
		t.Fatalf("FromJSONValue: %v", err)
	}
	if *out != *in {
		t.Errorf("round-trip mismatch\n got %+v\nwant %+v", *out, *in)
	}
}

func TestPopProcessQueueInfoBodyMatchesJava(t *testing.T) {
	want := `{"droped":false,"lastPopTimestamp":0,"waitAckCount":0}`
	if got := string((&PopProcessQueueInfo{}).Encode()); got != want {
		t.Errorf("PopProcessQueueInfo empty body\n got %s\nwant %s", got, want)
	}
	filled := &PopProcessQueueInfo{WaitAckCount: 5, Droped: true, LastPopTimestamp: 444}
	got := string(filled.Encode())
	for _, kv := range []string{`"waitAckCount":5`, `"droped":true`, `"lastPopTimestamp":444`} {
		if !strings.Contains(got, kv) {
			t.Errorf("body = %s, missing %s", got, kv)
		}
	}
	out := &PopProcessQueueInfo{}
	if err := out.FromJSONValue(filled.ToJSONValue()); err != nil {
		t.Fatalf("FromJSONValue: %v", err)
	}
	if *out != *filled {
		t.Errorf("round-trip mismatch\n got %+v\nwant %+v", *out, *filled)
	}
}

func TestCMResultSpellings(t *testing.T) {
	want := []CMResult{
		CMResultSuccess, CMResultLater, CMResultRollback,
		CMResultCommit, CMResultThrowException, CMResultReturnNull,
	}
	names := []string{
		"CR_SUCCESS", "CR_LATER", "CR_ROLLBACK",
		"CR_COMMIT", "CR_THROW_EXCEPTION", "CR_RETURN_NULL",
	}
	for i, r := range want {
		if string(r) != names[i] {
			t.Errorf("CMResult #%d = %q, want %q (fastjson2 serialises the enum by name)", i, string(r), names[i])
		}
	}
}

// The nullable pair (consumeResult, remark) is what makes the empty body three
// keys instead of five with two nulls; and autoCommit defaults to TRUE.
func TestConsumeMessageDirectlyResultBodyMatchesJava(t *testing.T) {
	empty := NewConsumeMessageDirectlyResult()
	if got, want := string(empty.Encode()), `{"autoCommit":true,"order":false,"spentTimeMills":0}`; got != want {
		t.Errorf("empty body\n got %s\nwant %s", got, want)
	}

	remark := "hello"
	result := CMResultSuccess
	filled := &ConsumeMessageDirectlyResult{
		Order:          true,
		AutoCommit:     false,
		ConsumeResult:  &result,
		Remark:         &remark,
		SpentTimeMills: 12,
	}
	got := string(filled.Encode())
	for _, kv := range []string{
		`"autoCommit":false`, `"consumeResult":"CR_SUCCESS"`,
		`"order":true`, `"remark":"hello"`, `"spentTimeMills":12`,
	} {
		if !strings.Contains(got, kv) {
			t.Errorf("filled body = %s, missing %s", got, kv)
		}
	}
}

func TestConsumeMessageDirectlyResultRoundTripsBothShapes(t *testing.T) {
	// Unset nullable fields must stay nil through a round trip.
	in := NewConsumeMessageDirectlyResult()
	out := &ConsumeMessageDirectlyResult{}
	if err := out.FromJSONValue(in.ToJSONValue()); err != nil {
		t.Fatalf("FromJSONValue: %v", err)
	}
	if out.ConsumeResult != nil || out.Remark != nil {
		t.Errorf("unset nullable fields came back non-nil: %+v", out)
	}
	if !out.AutoCommit || out.Order {
		t.Errorf("defaults lost: %+v", out)
	}

	decoded, err := DecodeConsumeMessageDirectlyResult(in.Encode())
	if err != nil {
		t.Fatalf("DecodeConsumeMessageDirectlyResult: %v", err)
	}
	if decoded.ConsumeResult != nil || decoded.Remark != nil || !decoded.AutoCommit {
		t.Errorf("decode mismatch: %+v", decoded)
	}
}

// Java leaves `jstack` null unless setJstack() ran, and fastjson2 drops null
// fields — so the empty body has exactly SIX keys. Emitting "jstack":"" adds a
// seventh and makes a strict admin read a zero-length stack trace.
func TestConsumerRunningInfoOmitsJstackUntilItIsSet(t *testing.T) {
	empty := string(NewConsumerRunningInfo().Encode())
	want := `{"mqPopTable":{},"mqTable":{},"properties":{},"statusTable":{},` +
		`"subscriptionSet":[],"userConsumerInfo":{}}`
	if empty != want {
		t.Errorf("empty ConsumerRunningInfo body\n got %s\nwant %s", empty, want)
	}
	if strings.Contains(empty, "jstack") {
		t.Errorf("body = %s, jstack must be absent until explicitly set", empty)
	}

	withStack := NewConsumerRunningInfo()
	withStack.Jstack = "trace-here"
	withStack.HasJstack = true
	got := string(withStack.Encode())
	if !strings.Contains(got, `"jstack":"trace-here"`) {
		t.Errorf("body = %s, want jstack present once set", got)
	}
	// fastjson2 emits jstack first (alphabetical); the stdlib sorts the same way.
	if !strings.HasPrefix(got, `{"jstack":"trace-here",`) {
		t.Errorf("body = %s, want jstack to sort first as in Java", got)
	}
}

// An explicitly-set empty jstack is still an emitted key (Java distinguishes
// null from ""), which is why the struct carries HasJstack rather than testing
// `Jstack != ""`.
func TestConsumerRunningInfoEmitsAnExplicitlyEmptyJstack(t *testing.T) {
	cri := NewConsumerRunningInfo()
	cri.HasJstack = true
	if got := string(cri.Encode()); !strings.Contains(got, `"jstack":""`) {
		t.Errorf("body = %s, want an explicitly-set empty jstack to be emitted", got)
	}
}

func TestConsumerRunningInfoDecodesJstackPresence(t *testing.T) {
	a := NewConsumerRunningInfo()
	if err := a.FromJSONValue(map[string]any{"properties": map[string]any{}}); err != nil {
		t.Fatalf("FromJSONValue: %v", err)
	}
	if a.HasJstack {
		t.Error("HasJstack set from a body with no jstack key")
	}

	b := NewConsumerRunningInfo()
	raw, err := DecodeJSON([]byte(`{"jstack":""}`))
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if err := b.FromJSONValue(raw); err != nil {
		t.Fatalf("FromJSONValue: %v", err)
	}
	if !b.HasJstack || b.Jstack != "" {
		t.Errorf("want HasJstack=true with an empty Jstack, got %+v", b)
	}
}

// The mqTable key travels as a fastjson2 inline OBJECT, not a string. This pins
// the shape the broker's console re-parses.
func TestConsumerRunningInfoMqTableUsesInlineObjectKeys(t *testing.T) {
	cri := NewConsumerRunningInfo()
	q := common.NewMessageQueue("TopicA", "broker-a", 3)
	cri.MQTable[q] = (&ProcessQueueInfo{CommitOffset: 7}).ToJSONValue()

	got := string(cri.Encode())
	wantKey := `{"brokerName":"broker-a","queueId":3,"topic":"TopicA"}`
	if !strings.Contains(got, wantKey) {
		t.Errorf("body = %s, want the inline object key %s", got, wantKey)
	}
	if !strings.Contains(got, `"commitOffset":7`) {
		t.Errorf("body = %s, want the embedded ProcessQueueInfo", got)
	}

	// And it must decode back into a MessageQueue-keyed map.
	back := NewConsumerRunningInfo()
	raw, err := DecodeJSON([]byte(got))
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if err := back.FromJSONValue(raw); err != nil {
		t.Fatalf("FromJSONValue: %v", err)
	}
	if len(back.MQTable) != 1 {
		t.Fatalf("decoded mqTable has %d entries, want 1", len(back.MQTable))
	}
	for k, v := range back.MQTable {
		if k.Topic != "TopicA" || k.BrokerName != "broker-a" || k.QueueID != 3 {
			t.Errorf("decoded key = %+v", k)
		}
		if v["commitOffset"] == nil {
			t.Errorf("decoded info lost commitOffset: %+v", v)
		}
	}
}
