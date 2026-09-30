// 307 (GET_CONSUMER_RUNNING_INFO) and 309 (CONSUME_MESSAGE_DIRECTLY): the two
// broker-initiated requests an admin console uses to inspect a consumer. Each
// shape is asserted twice — once against the builder alone (deterministic, no
// network) and once through the real remoting path (registration + wire +
// decode) — because the two failure modes are different: a wrong builder is a
// wrong number in the console, a wrong wire form makes fastjson2 throw and the
// console shows "no data" with an exception behind it.
package client

import (
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- test doubles

// orderlyVerdictListener answers with a fixed status and records what the
// listener was handed (the 309 path hands it exactly one message).
type orderlyVerdictListener struct {
	status ConsumeOrderlyStatus
	// autoCommit, when set, flips the context like a binlog consumer does. The
	// 309 answer must report the value AFTER the listener ran.
	autoCommit *bool

	seenTopics []string
}

func (l *orderlyVerdictListener) ConsumeMessage(msgs []*common.MessageExt, ctx *ConsumeOrderlyContext) ConsumeOrderlyStatus {
	for _, m := range msgs {
		l.seenTopics = append(l.seenTopics, m.Topic)
	}
	if l.autoCommit != nil {
		ctx.AutoCommit = *l.autoCommit
	}
	return l.status
}

// concurrentVerdictListener is the concurrent twin.
type concurrentVerdictListener struct {
	status     ConsumeConcurrentlyStatus
	seenTopics []string
}

func (l *concurrentVerdictListener) ConsumeMessage(msgs []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
	for _, m := range msgs {
		l.seenTopics = append(l.seenTopics, m.Topic)
	}
	return l.status
}

// directProbeError is a named error type so the CR_THROW_EXCEPTION remark has a
// deterministic %T — Java's remark is `UtilAll.exceptionSimpleDesc(e)`, i.e.
// the class name followed by the message.
type directProbeError struct{}

func (directProbeError) Error() string { return "direct probe boom" }

type panickingConcurrentlyListener struct{}

func (panickingConcurrentlyListener) ConsumeMessage([]*common.MessageExt, *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
	panic(directProbeError{})
}

type panickingOrderlyListener struct{}

func (panickingOrderlyListener) ConsumeMessage([]*common.MessageExt, *ConsumeOrderlyContext) ConsumeOrderlyStatus {
	panic(directProbeError{})
}

// ---------------------------------------------------------------- test helpers

// connCount reports how many connections the client has opened to this mock
// server. pushSync needs one, and it appears asynchronously (the first
// heartbeat opens it).
func (s *mockServer) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// pushAndWait pushes a broker-initiated request and fails the test if the client
// never answers.
func pushAndWait(t *testing.T, s *mockServer, cmd *remoting.RemotingCommand) *remoting.RemotingCommand {
	t.Helper()
	resp, err := s.pushSync(cmd, 5*time.Second)
	if err != nil {
		t.Fatalf("push code=%d: %v", cmd.Code, err)
	}
	return resp
}

// pushRunningInfo asks the client for its running info of `group`.
func pushRunningInfo(t *testing.T, s *mockServer, group, clientID string, jstack bool) *remoting.RemotingCommand {
	t.Helper()
	return pushAndWait(t, s, remoting.CreateRequestCommand(remoting.ReqGetConsumerRunningInfo,
		&remoting.GetConsumerRunningInfoRequestHeader{
			ConsumerGroup: remoting.StrPtr(group),
			ClientID:      remoting.StrPtr(clientID),
			JstackEnable:  remoting.BoolPtr(jstack),
		}))
}

// jsonKeysOf returns the sorted top-level keys of a JSON object body.
func jsonKeysOf(t *testing.T, body []byte) []string {
	t.Helper()
	value, err := remoting.DecodeJSON(body)
	if err != nil {
		t.Fatalf("decode body %s: %v", body, err)
	}
	obj, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("body is not a JSON object: %s", body)
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func requireKeys(t *testing.T, body []byte, want ...string) {
	t.Helper()
	got := jsonKeysOf(t, body)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("top-level keys = %v, want %v (body: %s)", got, want, body)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("top-level keys = %v, want %v (body: %s)", got, want, body)
		}
	}
}

// ---------------------------------------------------------------- 307: builder

// An unstarted consumer is the deterministic case: no instance, no queues, no
// stats, no retry subscription. It pins the per-consumer property set and the
// empty-container spellings.
func TestConsumerRunningInfoUnstartedConsumerShape(t *testing.T) {
	const group = "GID_go_307_unstarted"
	const topic = "Go307Unstarted"

	c := MustNewDefaultMQPushConsumer(group)
	requireNoError(t, "subscribe", c.Subscribe(topic, "*"))

	info := c.ConsumerRunningInfo()

	// No instance layer: exactly the three per-consumer properties.
	if len(info.Properties) != 3 {
		t.Errorf("properties = %v, want exactly the 3 per-consumer keys", info.Properties)
	}
	c.mu.Lock()
	corePoolSize := c.corePoolSize
	consumeThreadMin := c.consumeThreadMin
	c.mu.Unlock()
	if corePoolSize <= 0 {
		corePoolSize = consumeThreadMin
	}
	if got, want := info.Properties[remoting.PropThreadPoolCoreSize], strconv.Itoa(corePoolSize); got != want {
		t.Errorf("PROP_THREADPOOL_CORE_SIZE = %q, want %q", got, want)
	}
	// The effective core size, never the raw field: Java asks the executor, and
	// "0 consume threads" is a state a started consumer cannot be in.
	if got := info.Properties[remoting.PropThreadPoolCoreSize]; got == "0" {
		t.Error("PROP_THREADPOOL_CORE_SIZE = 0; the effective core size must be reported")
	}
	if got := info.Properties[remoting.PropConsumeOrderly]; got != "false" {
		t.Errorf("PROP_CONSUMEORDERLY = %q, want %q", got, "false")
	}
	// Java's consumerStartTimestamp is a `long` that stays 0 until start(). Go's
	// time.Time zero value is not 0 epoch, so this is the regression guard.
	if got := info.Properties[remoting.PropConsumerStartTimestamp]; got != "0" {
		t.Errorf("PROP_CONSUMER_START_TIMESTAMP = %q, want \"0\" before Start", got)
	}
	for _, k := range []string{remoting.PropNameServerAddr, remoting.PropConsumeType, remoting.PropClientVersion} {
		if _, ok := info.Properties[k]; ok {
			t.Errorf("%s must come from the instance layer, not the consumer", k)
		}
	}

	// An unstarted clustering consumer has no %RETRY% subscription yet (Start
	// adds it), so statusTable is exactly the one subscribed topic.
	if len(info.StatusTable) != 1 {
		t.Fatalf("statusTable = %v, want exactly 1 key (%s)", info.StatusTable, topic)
	}
	status, ok := info.StatusTable[topic]
	if !ok {
		t.Fatalf("statusTable is missing %s: %v", topic, info.StatusTable)
	}
	assertConsumeStatusKeys(t, status)

	if len(info.MQTable) != 0 || len(info.MQPopTable) != 0 {
		t.Errorf("mqTable/mqPopTable must be empty before a rebalance: %v %v", info.MQTable, info.MQPopTable)
	}
	if len(info.SubscriptionSet) != 1 {
		t.Errorf("subscriptionSet = %v, want the one subscription", info.SubscriptionSet)
	}
	if info.HasJstack {
		t.Error("jstack must not be set unless someone asked for it")
	}
}

// The empty body is the case a console hits for a consumer with nothing
// assigned, and it is where fastjson2's drop-null rule bites: `jstack` is absent
// rather than null, and every empty table is `{}` — never `null`, never `[]`.
func TestConsumerRunningInfoEmptyBodyWireShape(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("GID_go_307_empty")

	body := c.ConsumerRunningInfo().Encode()
	raw := string(body)

	requireKeys(t, body, "mqPopTable", "mqTable", "properties", "statusTable", "subscriptionSet", "userConsumerInfo")

	for _, want := range []string{
		`"mqTable":{}`,
		`"mqPopTable":{}`,
		`"statusTable":{}`,
		`"userConsumerInfo":{}`,
		`"subscriptionSet":[]`,
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("body is missing %s: %s", want, raw)
		}
	}
	if strings.Contains(raw, "jstack") {
		t.Errorf("jstack must be DROPPED while unset (fastjson2 drops nulls): %s", raw)
	}
	// The per-consumer stamps travel even here.
	if !strings.Contains(raw, `"`+remoting.PropConsumeOrderly+`":"false"`) {
		t.Errorf("body is missing PROP_CONSUMEORDERLY: %s", raw)
	}
}

// A clustering consumer subscribes its retry topic at Start, so statusTable has
// one entry per subscription — including %RETRY%. Asserting on the business
// topic alone would pass even if the retry topic were dropped, which is exactly
// the entry an operator looks at when messages stop moving.
func TestConsumerRunningInfoStatusTableCoversTheRetrySubscription(t *testing.T) {
	topic := uniqueTopic("Go307Status", t)
	const group = "GID_go_307_status"

	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "s0", "s1")
	listener := newRecordingListener()
	c := f.newConsumer(t, group, listener, withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "both consumed", func() bool { return listener.receivedAll(2) })

	// The window arithmetic only produces a number across two samples, so the
	// snapshot has to be taken AFTER a manual sample — reading it first yields
	// all zeros, which is what a consumer that never sampled really reports.
	m := consumerStatsManagerOf(t, c)
	if m == nil {
		t.Fatal("the started consumer has no statistics manager")
	}
	m.samplingInSeconds()

	info := c.ConsumerRunningInfo()
	wantTopics := []string{topic, common.GetRetryTopic(group)}
	if len(info.StatusTable) != len(wantTopics) {
		t.Fatalf("statusTable = %v, want keys %v", info.StatusTable, wantTopics)
	}
	for _, want := range wantTopics {
		status, ok := info.StatusTable[want]
		if !ok {
			t.Fatalf("statusTable is missing %s: %v", want, info.StatusTable)
		}
		assertConsumeStatusKeys(t, status)
	}

	// The business topic's numbers must come from the stats manager's snapshot,
	// not from a zero literal: two acked messages leave consumeOKTPS > 0.
	want := m.consumeStatus(group, topic).ToJSONValue()
	got := info.StatusTable[topic]
	if !sameJSONValue(got, want) {
		t.Errorf("statusTable[%s] = %v, want the stats manager's snapshot %v", topic, got, want)
	}
	if asFloat(got["consumeOKTPS"]) <= 0 {
		t.Errorf("consumeOKTPS = %v, want > 0 after two acks", got["consumeOKTPS"])
	}
	// The retry topic was never consumed, so its status is the zero snapshot —
	// present, not missing.
	if retry := info.StatusTable[common.GetRetryTopic(group)]; asFloat(retry["consumeOKTPS"]) != 0 {
		t.Errorf("the retry topic cannot have consumed anything: %v", retry)
	}
}

// mqTable and mqPopTable are DISJOINT: a classic consumer fills only mqTable,
// a POP consumer only mqPopTable. Listing the same queue in both makes the
// console count one consumption path as two.
func TestConsumerRunningInfoClassicConsumerFillsOnlyMqTable(t *testing.T) {
	topic := uniqueTopic("Go307Classic", t)
	const group = "GID_go_307_classic"

	f := newClusterFixture(t, map[string]int{topic: 1})
	c := f.newConsumer(t, group, newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "queue assignment", func() bool { return consumerForQueue(c, topic, "b1", 0) != nil })

	info := c.ConsumerRunningInfo()
	if len(info.MQTable) != 1 {
		t.Fatalf("mqTable = %v, want exactly the assigned queue", info.MQTable)
	}
	if len(info.MQPopTable) != 0 {
		t.Errorf("mqPopTable = %v, want empty on a classic consumer", info.MQPopTable)
	}
	pqi, ok := info.MQTable[common.NewMessageQueue(topic, "b1", 0)]
	if !ok {
		t.Fatalf("mqTable key mismatch: %v", info.MQTable)
	}
	// processQueueInfo has 14 keys, all always present: every Java field is a
	// primitive, so fastjson2 has nothing to omit. The cached span is the
	// interesting part — Java only WRITES cachedMsgMin/MaxOffset and
	// cachedMsgCount when the buffer is non-empty, but the JSON key travels
	// either way, so an empty queue reports a zero range and not a missing one.
	if len(pqi) != 14 {
		t.Errorf("processQueueInfo has %d keys, want 14: %v", len(pqi), pqi)
	}
	for _, k := range []string{"cachedMsgMinOffset", "cachedMsgMaxOffset", "cachedMsgCount"} {
		v, ok := pqi[k]
		if !ok {
			t.Errorf("%s must be present (primitives are never omitted): %v", k, pqi)
			continue
		}
		if asFloat(v) != 0 {
			t.Errorf("%s = %v, want 0 while the message tree is empty", k, v)
		}
	}
	if _, ok := pqi["cachedMsgSizeInMiB"]; !ok {
		t.Error("cachedMsgSizeInMiB is unconditional in Java")
	}
	if _, ok := pqi["droped"]; !ok {
		t.Errorf("the field is spelled `droped` (one p), Java's typo: %v", pqi)
	}
	// A long field rendering matters: 14 keys means the int-typed
	// cachedMsgSizeInMiB must not have smuggled a JavaDouble in.
	if _, ok := pqi["cachedMsgSizeInMiB"].(int32); !ok {
		t.Errorf("cachedMsgSizeInMiB = %#v, want a plain int32 (Java `int`)", pqi["cachedMsgSizeInMiB"])
	}
}

func TestConsumerRunningInfoPopConsumerFillsOnlyMqPopTable(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func([]*common.MessageExt, *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	})
	startConsumer(t, c, "TopicA", "*")

	waitFor(t, "pop queue assignment", func() bool { return c.PopProcessQueueCount() > 0 })

	info := c.ConsumerRunningInfo()
	if len(info.MQPopTable) != 1 {
		t.Fatalf("mqPopTable = %v, want exactly the popped queue", info.MQPopTable)
	}
	if len(info.MQTable) != 0 {
		t.Errorf("mqTable = %v, want empty on a POP consumer "+
			"(the same queue in both tables double-counts it)", info.MQTable)
	}
	pop, ok := info.MQPopTable[common.NewMessageQueue("TopicA", "b1", 0)]
	if !ok {
		t.Fatalf("mqPopTable key mismatch: %v", info.MQPopTable)
	}
	// PopProcessQueueInfo has exactly 3 keys, all unconditional.
	if len(pop) != 3 {
		t.Fatalf("popProcessQueueInfo has %d keys, want 3: %v", len(pop), pop)
	}
	for _, k := range []string{"droped", "lastPopTimestamp", "waitAckCount"} {
		if _, ok := pop[k]; !ok {
			t.Errorf("popProcessQueueInfo is missing %s: %v", k, pop)
		}
	}
	_ = f
}

// A pull consumer answers properties + subscriptionSet only. Java's
// DefaultMQPullConsumerImpl has no process queue table, so a fabricated mqTable
// would claim a rebalance state the consumer does not track.
func TestConsumerRunningInfoPullConsumerHasNoTables(t *testing.T) {
	topic := uniqueTopic("Go307Pull", t)
	f := newClusterFixture(t, map[string]int{topic: 1})
	c := newPullConsumer(t, f, "GID_go_307_pull")
	requireNoError(t, "pull consumer start", c.Start())
	t.Cleanup(c.Shutdown)
	requireNoError(t, "subscribe", c.Subscribe(topic, "*"))

	info := c.ConsumerRunningInfo()
	if len(info.MQTable) != 0 || len(info.MQPopTable) != 0 {
		t.Errorf("a pull consumer must have no mqTable/mqPopTable: %v %v", info.MQTable, info.MQPopTable)
	}
	if len(info.StatusTable) != 0 {
		t.Errorf("a pull consumer has no statusTable in Java: %v", info.StatusTable)
	}
	if len(info.SubscriptionSet) != 1 {
		t.Errorf("subscriptionSet = %v, want the one subscription", info.SubscriptionSet)
	}
	if got := info.Properties[remoting.PropConsumerStartTimestamp]; got == "" || got == "0" {
		t.Errorf("PROP_CONSUMER_START_TIMESTAMP = %q, want a real stamp after Start", got)
	}
}

// assertConsumeStatusKeys pins the six ConsumeStatus fields.
func assertConsumeStatusKeys(t *testing.T, status map[string]any) {
	t.Helper()
	want := []string{"pullRT", "pullTPS", "consumeRT", "consumeOKTPS", "consumeFailedTPS", "consumeFailedMsgs"}
	if len(status) != len(want) {
		t.Fatalf("consumeStatus has %d keys, want %d: %v", len(status), len(want), status)
	}
	for _, k := range want {
		if _, ok := status[k]; !ok {
			t.Errorf("consumeStatus is missing %s: %v", k, status)
		}
	}
	// consumeFailedMsgs is a Java long, so it renders as 0 with no point; the
	// five rates are Java doubles and keep theirs.
	if _, ok := status["consumeFailedMsgs"].(int64); !ok {
		t.Errorf("consumeFailedMsgs = %#v, want a plain int64", status["consumeFailedMsgs"])
	}
}

// sameJSONValue compares two rendered JSON values structurally.
func sameJSONValue(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		if remoting.FormatJavaDouble(asFloat(va)) != remoting.FormatJavaDouble(asFloat(vb)) {
			return false
		}
	}
	return true
}

func asFloat(v any) float64 {
	switch n := v.(type) {
	case remoting.JavaDouble:
		return float64(n)
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	}
	return 0
}

// ---------------------------------------------------------------- 307: the wire

// The end-to-end 307: a real consumer registers itself, the broker pushes the
// request over the warm connection, and the answer must be byte-parseable by
// fastjson2 — which means the mqTable key is an UNQUOTED inline object. The
// escaped spelling (`"{\"brokerName\":...}"`) is silently accepted by this
// client's own tolerant parser and rejected by Java, so it is asserted on the
// raw bytes.
func TestGetConsumerRunningInfoPushWire(t *testing.T) {
	topic := uniqueTopic("Go307Wire", t)
	const group = "GID_go_307_wire"
	mq := common.NewMessageQueue(topic, "b1", 0)

	f := newClusterFixture(t, map[string]int{topic: 1})
	c := f.newConsumer(t, group, newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "queue assignment", func() bool { return consumerForQueue(c, topic, "b1", 0) != nil })
	waitFor(t, "warm connection to the broker", func() bool { return f.brokerSrv.connCount() > 0 })

	// ---- jstackEnable = false ----
	resp := pushRunningInfo(t, f.brokerSrv, group, f.clientID, false)
	if resp.Code != remoting.RespSuccess {
		t.Fatalf("307 answered code=%d remark=%q", resp.Code, resp.Remark)
	}
	raw := string(resp.Body)

	wantTableKey := `"mqTable":{{"brokerName":"b1","queueId":0,"topic":"` + topic + `"}:`
	if !strings.Contains(raw, wantTableKey) {
		t.Errorf("mqTable is not an inline-object-keyed table (fastjson2 needs %s): %s", wantTableKey, raw)
	}
	if strings.Contains(raw, `\"brokerName\"`) {
		t.Error("mqTable keys are quote-escaped; fastjson2 rejects that spelling")
	}
	if !strings.Contains(raw, `"mqPopTable":{}`) {
		t.Errorf("a classic consumer must answer an empty mqPopTable: %s", raw)
	}
	if strings.Contains(raw, "jstack") {
		t.Errorf("jstackEnable=false must NOT produce a jstack key: %s", raw)
	}

	info, err := remoting.DecodeConsumerRunningInfo(resp.Body)
	if err != nil {
		t.Fatalf("decode 307 body: %v", err)
	}
	if len(info.MQTable) != 1 || len(info.MQPopTable) != 0 {
		t.Fatalf("mqTable=%d mqPopTable=%d, want 1/0", len(info.MQTable), len(info.MQPopTable))
	}
	if pqi := info.MQTable[mq]; len(pqi) != 14 {
		t.Errorf("mqTable entry has %d keys, want 14: %v", len(pqi), pqi)
	}

	// The instance layer adds exactly three properties; with the three
	// per-consumer ones that is six, and no seventh may leak in.
	if len(info.Properties) != 6 {
		t.Errorf("properties = %v, want 6 (3 per-consumer + 3 instance)", info.Properties)
	}
	if got, want := info.Properties[remoting.PropNameServerAddr], f.nserver.addr+";"; got != want {
		t.Errorf("PROP_NAMESERVER_ADDR = %q, want %q (every address, then `;`)", got, want)
	}
	if got := info.Properties[remoting.PropConsumeType]; got != remoting.ConsumeTypeConsumePassively {
		t.Errorf("PROP_CONSUME_TYPE = %q, want %q", got, remoting.ConsumeTypeConsumePassively)
	}
	if got := info.Properties[remoting.PropClientVersion]; got != "V5_5_1" {
		t.Errorf("PROP_CLIENT_VERSION = %q, want V5_5_1", got)
	}
	for _, want := range []string{topic, common.GetRetryTopic(group)} {
		if _, ok := info.StatusTable[want]; !ok {
			t.Errorf("statusTable is missing %s: %v", want, info.StatusTable)
		}
	}

	// ---- jstackEnable = true ----
	resp2 := pushRunningInfo(t, f.brokerSrv, group, f.clientID, true)
	if resp2.Code != remoting.RespSuccess {
		t.Fatalf("307+jstack answered code=%d remark=%q", resp2.Code, resp2.Remark)
	}
	info2, err := remoting.DecodeConsumerRunningInfo(resp2.Body)
	if err != nil {
		t.Fatalf("decode 307+jstack body: %v", err)
	}
	if !info2.HasJstack || len(info2.Jstack) == 0 {
		t.Fatalf("jstackEnable=true must produce a jstack (HasJstack=%v len=%d)", info2.HasJstack, len(info2.Jstack))
	}
	// fastjson2 sorts keys, so an emitted jstack lands first.
	if !strings.HasPrefix(string(resp2.Body), `{"jstack":`) {
		t.Errorf("jstack must be the first (sorted) key: %.80s", resp2.Body)
	}
	requireKeys(t, resp2.Body, "jstack", "mqPopTable", "mqTable", "properties", "statusTable", "subscriptionSet", "userConsumerInfo")
}

// An unknown group is the error both 307 and 309 answer, and the remark is the
// one the console prints verbatim. Java builds it with the SAME text for both
// codes, so a divergence here shows up as a wrong console message, not an error.
func TestGetConsumerRunningInfoUnknownGroupIsSystemError(t *testing.T) {
	topic := uniqueTopic("Go307Unknown", t)
	f := newClusterFixture(t, map[string]int{topic: 1})
	c := f.newConsumer(t, "GID_go_307_known", newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")
	waitFor(t, "warm connection to the broker", func() bool { return f.brokerSrv.connCount() > 0 })

	resp := pushRunningInfo(t, f.brokerSrv, "GID_go_307_NOPE", "nowhere", false)
	if resp.Code != remoting.RespSystemError {
		t.Fatalf("unknown group answered code=%d, want SYSTEM_ERROR(%d)", resp.Code, remoting.RespSystemError)
	}
	wantRemark := "The Consumer Group <GID_go_307_NOPE> not exist in this consumer"
	if resp.Remark != wantRemark {
		t.Errorf("remark = %q, want %q", resp.Remark, wantRemark)
	}
	if len(resp.Body) != 0 {
		t.Errorf("an error answer must carry no body, got %s", resp.Body)
	}
}

// A group registered by a consumer that is NOT a push consumer has no running
// info; Java answers the same "not exist" error rather than an empty body, and a
// console that received `{}` would show a healthy-but-empty consumer.
func TestGetConsumerRunningInfoNonPushConsumerIsSystemError(t *testing.T) {
	topic := uniqueTopic("Go307Stub", t)
	f := newClusterFixture(t, map[string]int{topic: 1})
	inst := CreateOrGetInstance(uniqueClientID(t), []string{f.nserver.addr}, testConfig())
	t.Cleanup(inst.Shutdown)
	warmRoute(t, inst, topic)

	stub := &stubConsumer{group: "GID_go_307_stub"}
	inst.RegisterConsumer(stub.group, stub)

	info := inst.consumerRunningInfo(stub.group)
	if info != nil {
		t.Fatalf("a non-push consumer must yield nil running info, got %+v", info)
	}
}

// ---------------------------------------------------------------- 309: verdicts

// The concurrent verdicts. order=false and autoCommit=true are set up front and
// never touched on this path.
func TestConsumeMessageDirectlyConcurrentVerdicts(t *testing.T) {
	cases := []struct {
		name string
		st   ConsumeConcurrentlyStatus
		want remoting.CMResult
	}{
		{"success", ConsumeSuccess, remoting.CMResultSuccess},
		{"reconsume", ReconsumeLater, remoting.CMResultLater},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := MustNewDefaultMQPushConsumer("GID_go_309_concurrent")
			c.SetConcurrentlyListener(&concurrentVerdictListener{status: tc.st})

			got := c.consumeMessageDirectly(newProducedExt("Go309", 3, 11, "payload"), "b1")

			// order is FALSE on the concurrent path; the broker reads it when
			// printing the verdict.
			if got.Order {
				t.Error("order = true, want false on the concurrent path")
			}
			if !got.AutoCommit {
				t.Error("autoCommit = false, want true (Java never changes it here)")
			}
			if got.ConsumeResult == nil || *got.ConsumeResult != tc.want {
				t.Errorf("consumeResult = %v, want %v", got.ConsumeResult, tc.want)
			}
			if got.Remark != nil {
				t.Errorf("remark = %q, want nil on the happy path", *got.Remark)
			}
			if got.SpentTimeMills < 0 {
				t.Errorf("spentTimeMills = %d, want >= 0", got.SpentTimeMills)
			}
			// consumeResult is set here, so it travels; remark stays nil and is
			// dropped (fastjson2 drops nulls).
			requireKeys(t, got.Encode(), "autoCommit", "consumeResult", "order", "spentTimeMills")
		})
	}
}

// The null-verdict shape: a result nobody filled carries THREE keys, because
// fastjson2 drops the null enum and the null remark. The broker's console reads
// a missing consumeResult as "not consumed", so an empty result that serialised
// as `"consumeResult":null` would read as a fourth, meaningless state.
func TestConsumeMessageDirectlyResultNullVerdictShape(t *testing.T) {
	got := remoting.NewConsumeMessageDirectlyResult()
	body := got.Encode()
	if string(body) != `{"autoCommit":true,"order":false,"spentTimeMills":0}` {
		t.Errorf("empty result body = %s, want the 3-key Java default", body)
	}
	requireKeys(t, body, "autoCommit", "order", "spentTimeMills")
}

// The orderly verdicts. COMMIT and ROLLBACK are the two members the concurrent
// switch does not have — collapsing them into SUCCESS would silently commit a
// rollback, which is the whole point of the binlog consumer.
func TestConsumeMessageDirectlyOrderlyVerdicts(t *testing.T) {
	cases := []struct {
		name string
		st   ConsumeOrderlyStatus
		want remoting.CMResult
	}{
		{"commit", OrderlyCommit, remoting.CMResultCommit},
		{"rollback", OrderlyRollback, remoting.CMResultRollback},
		{"success", OrderlySuccess, remoting.CMResultSuccess},
		{"suspend", OrderlySuspendCurrentQueueAMoment, remoting.CMResultLater},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := MustNewDefaultMQPushConsumer("GID_go_309_orderly")
			c.SetOrderlyListener(&orderlyVerdictListener{status: tc.st})

			got := c.consumeMessageDirectly(newProducedExt("Go309", 3, 11, "payload"), "b1")

			if !got.Order {
				t.Error("order = false, want true on the orderly path")
			}
			if !got.AutoCommit {
				t.Error("autoCommit = false, want the context default (true)")
			}
			if got.ConsumeResult == nil || *got.ConsumeResult != tc.want {
				t.Errorf("consumeResult = %v, want %v", got.ConsumeResult, tc.want)
			}
			if got.Remark != nil {
				t.Errorf("remark = %q, want nil on the happy path", *got.Remark)
			}
		})
	}
}

// autoCommit is read from the context AFTER the listener ran. Reading the
// initial value (always true) would tell the broker to commit work a
// disabled-autoCommit consumer still owns.
func TestConsumeMessageDirectlyOrderlyAutoCommitIsReadAfterTheListener(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("GID_go_309_autocommit")
	listener := &orderlyVerdictListener{status: OrderlyCommit, autoCommit: remoting.BoolPtr(false)}
	c.SetOrderlyListener(listener)

	got := c.consumeMessageDirectly(newProducedExt("Go309", 0, 1, "payload"), "b1")

	if got.AutoCommit {
		t.Error("autoCommit = true; the listener turned it off, so the answer must report false")
	}
	if got.ConsumeResult == nil || *got.ConsumeResult != remoting.CMResultCommit {
		t.Errorf("consumeResult = %v, want CR_COMMIT", got.ConsumeResult)
	}
}

// A thrown exception becomes CR_THROW_EXCEPTION plus a remark; it must NOT be
// flattened into a retry status, which is what the normal consume path does
// (a panic there becomes RECONSUME_LATER).
func TestConsumeMessageDirectlyPanicBecomesThrowException(t *testing.T) {
	t.Run("concurrent", func(t *testing.T) {
		c := MustNewDefaultMQPushConsumer("GID_go_309_panic_c")
		c.SetConcurrentlyListener(panickingConcurrentlyListener{})

		got := c.consumeMessageDirectly(newProducedExt("Go309", 0, 1, "payload"), "b1")
		if got.ConsumeResult == nil || *got.ConsumeResult != remoting.CMResultThrowException {
			t.Fatalf("consumeResult = %v, want CR_THROW_EXCEPTION", got.ConsumeResult)
		}
		if got.Remark == nil || !strings.Contains(*got.Remark, "direct probe boom") {
			t.Fatalf("remark = %v, want the panic message", got.Remark)
		}
		if !strings.HasPrefix(*got.Remark, "client.directProbeError:") {
			t.Errorf("remark = %q, want the `type: message` shape", *got.Remark)
		}
		if !got.AutoCommit {
			t.Error("a concurrent failure still reports autoCommit = true")
		}
	})

	t.Run("orderly", func(t *testing.T) {
		c := MustNewDefaultMQPushConsumer("GID_go_309_panic_o")
		c.SetOrderlyListener(panickingOrderlyListener{})

		got := c.consumeMessageDirectly(newProducedExt("Go309", 0, 1, "payload"), "b1")
		if got.ConsumeResult == nil || *got.ConsumeResult != remoting.CMResultThrowException {
			t.Fatalf("consumeResult = %v, want CR_THROW_EXCEPTION", got.ConsumeResult)
		}
		if !got.Order {
			t.Error("order must stay true in the failure answer")
		}
	})
}

// A %RETRY% message reaches the listener under its ORIGINAL topic (Java
// resetRetryAndNamespace). Without it a caller's listener sees a topic it never
// subscribed to and typically drops the message.
func TestConsumeMessageDirectlyRestoresRetryTopicBeforeTheListener(t *testing.T) {
	const group = "GID_go_309_retry"
	c := MustNewDefaultMQPushConsumer(group)
	listener := &concurrentVerdictListener{status: ConsumeSuccess}
	c.SetConcurrentlyListener(listener)

	msg := newProducedExt(common.GetRetryTopic(group), 0, 9, "retried")
	msg.PutProperty(common.PropertyRetryTopic, "Go309Business")

	got := c.consumeMessageDirectly(msg, "b1")

	if len(listener.seenTopics) != 1 || listener.seenTopics[0] != "Go309Business" {
		t.Fatalf("listener saw topics %v, want [Go309Business]", listener.seenTopics)
	}
	if got.ConsumeResult == nil || *got.ConsumeResult != remoting.CMResultSuccess {
		t.Errorf("consumeResult = %v, want CR_SUCCESS", got.ConsumeResult)
	}
}

// ---------------------------------------------------------------- 309: the wire

func pushConsumeDirectly(t *testing.T, s *mockServer, group, brokerName string, body []byte) *remoting.RemotingCommand {
	t.Helper()
	cmd := remoting.CreateRequestCommand(remoting.ReqConsumeMessageDirectly,
		&remoting.ConsumeMessageDirectlyResultRequestHeader{
			ConsumerGroup: remoting.StrPtr(group),
			ClientID:      remoting.StrPtr("unused"),
			MsgID:         remoting.StrPtr("unused"),
			BrokerName:    remoting.StrPtr(brokerName),
		})
	cmd.SetBody(body)
	return pushAndWait(t, s, cmd)
}

func TestConsumeMessageDirectlyPushWire(t *testing.T) {
	topic := uniqueTopic("Go309Wire", t)
	const group = "GID_go_309_wire"

	f := newClusterFixture(t, map[string]int{topic: 1})
	listener := newRecordingListener()
	c := f.newConsumer(t, group, listener,
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")
	waitFor(t, "warm connection to the broker", func() bool { return f.brokerSrv.connCount() > 0 })

	// The broker sends ONE stored message, exactly as a commitlog record.
	msg := newProducedExt(topic, 0, 0, "direct payload")
	body, err := common.EncodeMessageExt(msg, false)
	if err != nil {
		t.Fatalf("encode stored message: %v", err)
	}

	resp := pushConsumeDirectly(t, f.brokerSrv, group, "b1", body)
	if resp.Code != remoting.RespSuccess {
		t.Fatalf("309 answered code=%d remark=%q", resp.Code, resp.Remark)
	}

	result, err := remoting.DecodeConsumeMessageDirectlyResult(resp.Body)
	if err != nil {
		t.Fatalf("decode 309 body %s: %v", resp.Body, err)
	}
	if result.ConsumeResult == nil || *result.ConsumeResult != remoting.CMResultSuccess {
		t.Errorf("consumeResult = %v, want CR_SUCCESS", result.ConsumeResult)
	}
	if !result.AutoCommit || result.Order {
		t.Errorf("autoCommit=%v order=%v, want true/false", result.AutoCommit, result.Order)
	}

	// The listener really ran with the broker's payload.
	if got := listener.bodies(); len(got) != 1 || got[0] != "direct payload" {
		t.Errorf("listener bodies = %v, want [\"direct payload\"]", got)
	}
}

// Both 309 error arms answer the same remark as 307. The second one is the
// subtle case: a pull/lite consumer group IS registered, but Java's
// MQClientInstance.consumeMessageDirectly returns null for anything that is not
// a DefaultMQPushConsumerImpl, so it must NOT get a silent success.
func TestConsumeMessageDirectlyErrorArms(t *testing.T) {
	topic := uniqueTopic("Go309Errs", t)
	const pushGroup = "GID_go_309_err_push"

	f := newClusterFixture(t, map[string]int{topic: 1})
	inst := CreateOrGetInstance(uniqueClientID(t), []string{f.nserver.addr}, testConfig())
	t.Cleanup(inst.Shutdown)
	warmRoute(t, inst, topic)

	// Register before the heartbeat so the connection carries this instance's
	// identity; the push itself needs the warm connection SendHeartbeat opens.
	inst.RegisterConsumer(pushGroup, &stubConsumer{group: pushGroup})
	if n := inst.SendHeartbeatToAllBroker(2000); n != 1 {
		t.Fatalf("heartbeat count = %d, want 1", n)
	}
	waitFor(t, "warm connection to the broker", func() bool { return f.brokerSrv.connCount() > 0 })

	msg := newProducedExt(topic, 0, 0, "payload")
	body, err := common.EncodeMessageExt(msg, false)
	if err != nil {
		t.Fatalf("encode stored message: %v", err)
	}

	pushGroupCase := func(name, group string, wantCode int32, wantRemark string) {
		t.Helper()
		resp := pushConsumeDirectly(t, f.brokerSrv, group, "b1", body)
		if resp.Code != wantCode {
			t.Errorf("%s: code = %d, want %d (remark %q)", name, resp.Code, wantCode, resp.Remark)
		}
		if resp.Remark != wantRemark {
			t.Errorf("%s: remark = %q, want %q", name, resp.Remark, wantRemark)
		}
	}

	t.Run("unknown group", func(t *testing.T) {
		pushGroupCase("unknown", "GID_go_309_absent", remoting.RespSystemError,
			"The Consumer Group <GID_go_309_absent> not exist in this consumer")
	})

	t.Run("registered but not a push consumer", func(t *testing.T) {
		pushGroupCase("stub", pushGroup, remoting.RespSystemError,
			"The Consumer Group <"+pushGroup+"> not exist in this consumer")
	})
}
