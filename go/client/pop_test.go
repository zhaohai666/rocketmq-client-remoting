// POP client tests against the in-process mock cluster from instance_test.go,
// extended with a POP_MESSAGE answer.
//
// What is worth asserting here (and what is not): the checkpoint the client
// REBUILDS from the broker's response tables and the exact fields of the ACK it
// sends back are the whole contract — a wrong segment or a wrong offset makes
// every ACK a silent no-op on a real cluster, with the only symptom being
// "messages keep coming back after popInvisibleTime". The listener side is
// already covered by the pull-path tests.
package client

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- mock broker

// popFixture is the canned POP answer. Everything is explicit so the test can
// drive each branch of processPopResponse without a real store behind it.
type popFixture struct {
	code            int32
	msgs            []*common.MessageExt
	popTime         int64
	invisibleTime   int64
	reviveQid       int32
	restNum         int64
	startOffsetInfo string
	msgOffsetInfo   string
	orderCountInfo  string
}

// popFixtureOf builds the two offset tables the way a broker would for one
// queue holding offsets [start, start+len).
func popFixtureOf(topic, brokerName string, queueID int32, start int64, count int, popTime, invisible int64) popFixture {
	offsets := make([]int64, 0, count)
	for i := 0; i < count; i++ {
		offsets = append(offsets, start+int64(i))
	}
	var starts, msgOffsets strings.Builder
	common.BuildStartOffsetInfo(&starts, topic, int(queueID), start)
	common.BuildMsgOffsetInfo(&msgOffsets, topic, int(queueID), offsets)
	return popFixture{
		code:            remoting.RespSuccess,
		popTime:         popTime,
		invisibleTime:   invisible,
		reviveQid:       3,
		startOffsetInfo: starts.String(),
		msgOffsetInfo:   msgOffsets.String(),
	}
}

// answerPop is the POP branch of the mock broker's request switch. It lives in
// this file so the consumer fixtures stay untouched.
func (b *consumerBroker) answerPop(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	header := &remoting.PopMessageRequestHeader{}
	header.FromExtFields(req.ExtFields())

	b.mu.Lock()
	b.popHeaders = append(b.popHeaders, header)
	b.popExt = append(b.popExt, req.ExtFields().Clone())
	b.popArrivals = append(b.popArrivals, common.CurrentTimeMillis())
	fixture := b.popFixture
	if b.popTail != nil {
		// A staged round wins while any is left; after that the tail answer keeps
		// the loop occupied without replaying a batch.
		if len(b.popScript) > 0 {
			round := b.popScript[0]
			b.popScript = b.popScript[1:]
			fixture = &round
		} else {
			fixture = b.popTail
		}
	}
	b.mu.Unlock()

	if fixture == nil {
		return remoting.CreateResponseCommand(remoting.RespPollingTimeout, "no fixture")
	}
	code := fixture.code
	if code == 0 {
		code = remoting.RespSuccess
	}
	resp := remoting.CreateResponseCommand(code, "")
	resp.SetCustomHeader(&remoting.PopMessageResponseHeader{
		PopTime:         remoting.I64Ptr(fixture.popTime),
		InvisibleTime:   remoting.I64Ptr(fixture.invisibleTime),
		ReviveQid:       remoting.I32Ptr(fixture.reviveQid),
		RestNum:         remoting.I64Ptr(fixture.restNum),
		StartOffsetInfo: remoting.StrPtr(fixture.startOffsetInfo),
		MsgOffsetInfo:   remoting.StrPtr(fixture.msgOffsetInfo),
		OrderCountInfo:  remoting.StrPtr(fixture.orderCountInfo),
	})
	if code != remoting.RespSuccess {
		return resp
	}
	var body []byte
	for _, msg := range fixture.msgs {
		encoded, err := common.EncodeMessageExt(msg, false)
		if err != nil {
			return remoting.CreateResponseCommand(remoting.RespSystemError, "encode: "+err.Error())
		}
		body = append(body, encoded...)
	}
	if len(body) > 0 {
		resp.SetBody(body)
	}
	return resp
}

func (b *consumerBroker) answerAck(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	header := &remoting.AckMessageRequestHeader{}
	header.FromExtFields(req.ExtFields())
	b.mu.Lock()
	b.acks = append(b.acks, header)
	b.mu.Unlock()
	return remoting.CreateResponseCommand(remoting.RespSuccess, "")
}

func (b *consumerBroker) answerBatchAck(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	body, err := remoting.DecodeBatchAckMessageRequestBody(req.Body)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.RespSystemError, err.Error())
	}
	b.mu.Lock()
	b.batchAcks = append(b.batchAcks, body)
	b.mu.Unlock()
	return remoting.CreateResponseCommand(remoting.RespSuccess, "")
}

func (b *consumerBroker) answerChangeInvisible(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	header := &remoting.ChangeInvisibleTimeRequestHeader{}
	header.FromExtFields(req.ExtFields())
	b.mu.Lock()
	b.changeInvisible = append(b.changeInvisible, header)
	fixture := b.popFixture
	b.mu.Unlock()
	popTime := int64(1)
	invisible := i64Or(header.InvisibleTime, 0)
	if fixture != nil {
		popTime = fixture.popTime
	}
	resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
	resp.SetCustomHeader(&remoting.ChangeInvisibleTimeResponseHeader{
		PopTime:       remoting.I64Ptr(popTime),
		InvisibleTime: remoting.I64Ptr(invisible),
		ReviveQid:     remoting.I32Ptr(3),
	})
	return resp
}

func (b *consumerBroker) answerSetMode(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	body, err := remoting.DecodeSetMessageRequestModeRequestBody(req.Body)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.RespSystemError, err.Error())
	}
	b.mu.Lock()
	b.requestModes = append(b.requestModes, body)
	b.mu.Unlock()
	return remoting.CreateResponseCommand(remoting.RespSuccess, "")
}

func (b *consumerBroker) setPopFixture(f popFixture) {
	b.mu.Lock()
	b.popFixture = &f
	b.mu.Unlock()
}

// setPopScript stages one canned answer per POP round; once the staged rounds are
// spent the broker keeps answering with tail. A test that asserts a per-round
// number (the pull TPS increment, the wait-ACK debt, the gap to the next round)
// needs the batch to be handed over exactly once, which a plain fixture cannot do:
// the loop would replay the same rows every round.
func (b *consumerBroker) setPopScript(tail popFixture, rounds ...popFixture) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.popScript = append([]popFixture(nil), rounds...)
	b.popTail = &tail
}

// popExtSnapshot is the raw extFields of every POP_MESSAGE as the broker received
// them, oldest first.
func (b *consumerBroker) popExtSnapshot() []*common.StringMap {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*common.StringMap(nil), b.popExt...)
}

// popArrivalsSnapshot is the arrival time of every POP_MESSAGE, oldest first.
func (b *consumerBroker) popArrivalsSnapshot() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int64(nil), b.popArrivals...)
}

// popCount is how many POP_MESSAGE requests reached the broker.
func (b *consumerBroker) popCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.popHeaders)
}

func (b *consumerBroker) popHeadersSnapshot() []*remoting.PopMessageRequestHeader {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*remoting.PopMessageRequestHeader(nil), b.popHeaders...)
}

func (b *consumerBroker) acksSnapshot() []*remoting.AckMessageRequestHeader {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*remoting.AckMessageRequestHeader(nil), b.acks...)
}

func (b *consumerBroker) changeInvisibleSnapshot() []*remoting.ChangeInvisibleTimeRequestHeader {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*remoting.ChangeInvisibleTimeRequestHeader(nil), b.changeInvisible...)
}

func (b *consumerBroker) requestModesSnapshot() []*remoting.SetMessageRequestModeRequestBody {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*remoting.SetMessageRequestModeRequestBody(nil), b.requestModes...)
}

// popMessage builds a broker-side message for the fixture.
func popMessage(topic string, queueID int32, queueOffset int64, body string) *common.MessageExt {
	ext := newProducedExt(topic, queueID, queueOffset, body)
	ext.BornTimestamp = common.CurrentTimeMillis()
	return ext
}

// ---------------------------------------------------------------- unit guards

// The checkpoint must end in the message's own queue offset (segment 7), not in
// the batch start (segment 0). The ACK then reads segment 7 back.
func TestPopCheckpointUsesPrMessageQueueOffset(t *testing.T) {
	popTime := common.CurrentTimeMillis()
	inst := newTestInstanceForPop(t)
	broker := inst.broker
	broker.setPopFixture(popFixtureOf("TopicA", "b1", 0, 10, 3, popTime, 60000))
	broker.mu.Lock()
	broker.popFixture.msgs = []*common.MessageExt{
		popMessage("TopicA", 0, 10, "m10"),
		popMessage("TopicA", 0, 11, "m11"),
		popMessage("TopicA", 0, 12, "m12"),
	}
	broker.mu.Unlock()

	result, err := popOnceForTest(inst, "TopicA", 0)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if result.PopStatus != PopFound || len(result.MsgFoundList) != 3 {
		t.Fatalf("pop result %v with %d msgs", result.PopStatus, len(result.MsgFoundList))
	}
	for i, want := range []string{
		"10 " + i64Text(popTime) + " 60000 3 0 b1 0 10",
		"10 " + i64Text(popTime) + " 60000 3 0 b1 0 11",
		"10 " + i64Text(popTime) + " 60000 3 0 b1 0 12",
	} {
		got := propertyOf(result.MsgFoundList[i], common.PropertyPopCk)
		if got != want {
			t.Fatalf("msg %d checkpoint\n got=%q\nwant=%q", i, got, want)
		}
	}
	// The topic handed to the listener is the requested one, and 1ST_POP_TIME is
	// stamped once.
	if result.MsgFoundList[0].Topic != "TopicA" {
		t.Fatalf("topic = %q", result.MsgFoundList[0].Topic)
	}
	if v := propertyOf(result.MsgFoundList[0], common.PropertyFirstPopTime); v != i64Text(popTime) {
		t.Fatalf("1ST_POP_TIME = %q", v)
	}
}

// The fallback path (broker sent no tables) still has to produce an 8-segment
// checkpoint ending in the message offset.
func TestPopCheckpointFallbackWhenNoOffsetTables(t *testing.T) {
	popTime := common.CurrentTimeMillis()
	inst := newTestInstanceForPop(t)
	broker := inst.broker
	broker.setPopFixture(popFixture{
		code:          remoting.RespSuccess,
		popTime:       popTime,
		invisibleTime: 60000,
		reviveQid:     7,
		msgs:          []*common.MessageExt{popMessage("TopicA", 1, 42, "m42")},
	})

	result, err := popOnceForTest(inst, "TopicA", 1)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	want := "42 " + i64Text(popTime) + " 60000 7 0 b1 1 42"
	if got := propertyOf(result.MsgFoundList[0], common.PropertyPopCk); got != want {
		t.Fatalf("fallback checkpoint\n got=%q\nwant=%q", got, want)
	}
}

// A retried message carries retry="1" and the client's ACK must go to the POP
// retry topic %RETRY%<group>_<topic> — not to the business topic, where the
// broker has no such checkpoint.
func TestPopRetryCheckpointResolvesToPopRetryTopic(t *testing.T) {
	popTime := common.CurrentTimeMillis()
	const group = "PopRetryGroup"
	retryTopic := common.BuildPopRetryTopicV1("TopicA", group)

	inst := newTestInstanceForPop(t)
	broker := inst.broker
	broker.setPopFixture(popFixtureOf(retryTopic, "b1", 0, 5, 1, popTime, 60000))
	broker.mu.Lock()
	broker.popFixture.msgs = []*common.MessageExt{popMessage(retryTopic, 0, 5, "retried")}
	broker.mu.Unlock()

	result, err := popOnceForTest(inst, "TopicA", 0)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	ck := propertyOf(result.MsgFoundList[0], common.PropertyPopCk)
	parts := common.SplitExtraInfo(ck)
	if got, _ := common.GetRetry(parts); got != "1" {
		t.Fatalf("retry marker = %q (checkpoint %q)", got, ck)
	}
	// The listener-facing topic is the REQUEST topic, so getRealTopic has to
	// rebuild the physical one from the marker.
	if result.MsgFoundList[0].Topic != "TopicA" {
		t.Fatalf("listener topic = %q", result.MsgFoundList[0].Topic)
	}
	if got := common.GetRealTopic(parts, "TopicA", group); got != retryTopic {
		t.Fatalf("ACK topic = %q, want %q", got, retryTopic)
	}
}

// On the DEFAULT retry path (brokerConfig.popResponseReturnActualRetryTopic =
// false) the broker stamps the checkpoint itself and rewrites msg.Topic to the
// business topic before the client ever sees it (PopMessageProcessor:849-853).
// The client must then keep the broker's checkpoint verbatim.
//
// The fixture is built to be the trap: the tables are keyed by the BUSINESS
// topic, so a client that blindly rebuilds would SUCCEED and silently change the
// marker from "1" to "0" — sending the ACK to the business topic, where the
// broker holds no checkpoint. Nothing would fail until the messages came back.
func TestPopKeepsBrokerCheckpointOnRewrittenRetryTopic(t *testing.T) {
	popTime := common.CurrentTimeMillis()
	const group = "PopRewrittenGroup"
	physical := common.BuildPopRetryTopicV1("TopicA", group)
	// What the broker built (from the PHYSICAL topic, hence marker "1") and
	// stamped with putIfAbsent.
	brokerCk := "5 " + i64Text(popTime) + " 60000 3 1 b1 0 5"

	inst := newTestInstanceForPop(t)
	broker := inst.broker
	broker.setPopFixture(popFixtureOf("TopicA", "b1", 0, 5, 1, popTime, 60000))
	msg := popMessage("TopicA", 0, 5, "retried")
	msg.PutProperty(common.PropertyPopCk, brokerCk)
	broker.mu.Lock()
	broker.popFixture.msgs = []*common.MessageExt{msg}
	broker.mu.Unlock()

	result, err := popOnceForTest(inst, "TopicA", 0)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if got := propertyOf(result.MsgFoundList[0], common.PropertyPopCk); got != brokerCk {
		t.Fatalf("checkpoint was rebuilt\n got=%q\nwant=%q (the broker's)", got, brokerCk)
	}
	// The surviving marker still resolves the ACK to the POP retry topic, which
	// is where the checkpoint actually lives.
	parts := common.SplitExtraInfo(brokerCk)
	if got := common.GetRealTopic(parts, "TopicA", group); got != physical {
		t.Fatalf("ACK topic = %q, want %q", got, physical)
	}
	if v := propertyOf(result.MsgFoundList[0], common.PropertyFirstPopTime); v != i64Text(popTime) {
		t.Fatalf("1ST_POP_TIME = %q", v)
	}
	if result.MsgFoundList[0].Topic != "TopicA" {
		t.Fatalf("listener topic = %q", result.MsgFoundList[0].Topic)
	}
}

// ---------------------------------------------------------------- full consumer

// popFixtureWithConsumer starts the mock cluster and a POP consumer on it.
func popFixtureWithConsumer(t *testing.T, listener any, opts ...consumerOpt) (*clusterFixture, *DefaultMQPushConsumer) {
	t.Helper()
	f := newClusterFixture(t, map[string]int{"TopicA": 1})
	all := append([]consumerOpt{func(c *DefaultMQPushConsumer) { c.SetPopMode(true) }}, opts...)
	c := f.newConsumer(t, "PopGroup", listener, all...)
	return f, c
}

// The client must acknowledge only what the listener reported as handled, and
// the ACK offset must be the checkpoint's segment 7.
func TestPopConsumerAcksWithCheckpointOffset(t *testing.T) {
	var mu sync.Mutex
	consumed := 0
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(msgs []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			mu.Lock()
			consumed += len(msgs)
			mu.Unlock()
			return ConsumeSuccess
		},
	})
	popTime := common.CurrentTimeMillis()
	f.broker.mu.Lock()
	f.broker.popFixture = &popFixture{
		code:          remoting.RespSuccess,
		popTime:       popTime,
		invisibleTime: 60000,
		reviveQid:     3,
		msgs: []*common.MessageExt{
			popMessage("TopicA", 0, 100, "a"),
			popMessage("TopicA", 0, 101, "b"),
		},
	}
	f.broker.mu.Unlock()

	startConsumer(t, c, "TopicA", "*")

	waitForBudget(t, 5*time.Second, func() bool { return len(f.broker.acksSnapshot()) >= 2 })
	acks := f.broker.acksSnapshot()
	offsets := map[int64]bool{}
	for _, ack := range acks {
		offsets[derefI64(ack.Offset)] = true
		if deref(ack.Topic) != "TopicA" {
			t.Fatalf("ack topic = %q", deref(ack.Topic))
		}
		if deref(ack.ConsumerGroup) != "PopGroup" {
			t.Fatalf("ack group = %q", deref(ack.ConsumerGroup))
		}
		if deref(ack.Bname) != "b1" {
			t.Fatalf("ack bname = %q", deref(ack.Bname))
		}
	}
	// 100 and 101 — the MESSAGE offsets, never the batch start twice.
	if !offsets[100] || !offsets[101] || len(offsets) != 2 {
		t.Fatalf("ack offsets = %v", offsets)
	}
	mu.Lock()
	got := consumed
	mu.Unlock()
	if got < 2 {
		t.Fatalf("consumed = %d", got)
	}
	// Every ACK must carry the full checkpoint, or the broker cannot match it.
	if ck := deref(f.broker.acksSnapshot()[0].ExtraInfo); len(common.SplitExtraInfo(ck)) != 8 {
		t.Fatalf("ack extraInfo = %q", ck)
	}
}

// A failed message is re-hidden by extending its invisibility window, using the
// SECONDS table sent as MILLISECONDS. Level comes from the reconsume count when
// the context did not choose one.
func TestPopConsumerFailureExtendsInvisibility(t *testing.T) {
	alwaysFail := func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
		return ReconsumeLater
	}
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{fn: alwaysFail})
	popTime := common.CurrentTimeMillis()
	msg := popMessage("TopicA", 0, 7, "poison")
	msg.ReconsumeTimes = 1
	f.broker.mu.Lock()
	f.broker.popFixture = &popFixture{
		code:          remoting.RespSuccess,
		popTime:       popTime,
		invisibleTime: 60000,
		reviveQid:     3,
		msgs:          []*common.MessageExt{msg},
	}
	f.broker.mu.Unlock()

	startConsumer(t, c, "TopicA", "*")

	waitForBudget(t, 5*time.Second, func() bool { return len(f.broker.changeInvisibleSnapshot()) >= 1 })
	chg := f.broker.changeInvisibleSnapshot()[0]
	// reconsumeTimes == 1 -> table[1] == 30 seconds -> 30000 ms.
	if got := derefI64(chg.InvisibleTime); got != 30000 {
		t.Fatalf("invisibleTime = %d, want 30000 (table[1]=30s in ms)", got)
	}
	if got := derefI64(chg.Offset); got != 7 {
		t.Fatalf("change-invisible offset = %d, want 7", got)
	}
	if deref(chg.Topic) != "TopicA" {
		t.Fatalf("change-invisible topic = %q", deref(chg.Topic))
	}
	// A failed message must NOT be acknowledged.
	if acks := f.broker.acksSnapshot(); len(acks) != 0 {
		t.Fatalf("failed message was acked: %v", acks)
	}
}

func TestPopConsumerCheckNeedAckOrDelayExtends(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ReconsumeLater
		},
	}, func(c *DefaultMQPushConsumer) { c.SetMaxReconsumeTimes(0) })

	popTime := common.CurrentTimeMillis()
	msg := popMessage("TopicA", 0, 9, "exhausted")
	// Born 45s ago: above table[2]=60? no — 45 >= 30 (table[1]) and < 60, so the
	// next notch is 2 (60s).
	msg.BornTimestamp = common.CurrentTimeMillis() - 45_000
	// Staged EXACTLY at the budget (0): the pre-delivery guard only drops a row
	// whose count is strictly above it, so this one must still reach the listener
	// and be settled by the post-listener path under test here.
	msg.ReconsumeTimes = 0
	f.broker.mu.Lock()
	f.broker.popFixture = &popFixture{
		code:          remoting.RespSuccess,
		popTime:       popTime,
		invisibleTime: 60000,
		reviveQid:     3,
		msgs:          []*common.MessageExt{msg},
	}
	f.broker.mu.Unlock()

	startConsumer(t, c, "TopicA", "*")

	waitForBudget(t, 5*time.Second, func() bool { return len(f.broker.changeInvisibleSnapshot()) >= 1 })
	got := derefI64(f.broker.changeInvisibleSnapshot()[0].InvisibleTime)
	if got != 60000 {
		t.Fatalf("invisibleTime = %d, want 60000 (next notch above 45s)", got)
	}
}

// Extra security: the 307 handler is exercised separately; here the point is
// that checkNeedAckOrDelay ACKs (gives up) once the message is older than twice
// the longest back-off instead of extending forever.
func TestPopConsumerCheckNeedAckOrDelayGivesUp(t *testing.T) {
	if popDelayLevel[len(popDelayLevel)-1] != 7200 {
		t.Fatalf("last delay level = %d", popDelayLevel[len(popDelayLevel)-1])
	}
	var delivered int32
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			atomic.AddInt32(&delivered, 1)
			return ReconsumeLater
		},
	}, func(c *DefaultMQPushConsumer) { c.SetMaxReconsumeTimes(0) })

	popTime := common.CurrentTimeMillis()
	msg := popMessage("TopicA", 0, 11, "ancient")
	// Older than 7200*1000*2 = 4h.
	msg.BornTimestamp = common.CurrentTimeMillis() - (7200*1000*2 + 60_000)
	// Exactly at the budget, so the pre-delivery guard passes it and the give-up
	// decision below is the one being exercised.
	msg.ReconsumeTimes = 0
	f.broker.mu.Lock()
	f.broker.popFixture = &popFixture{
		code:          remoting.RespSuccess,
		popTime:       popTime,
		invisibleTime: 60000,
		reviveQid:     3,
		msgs:          []*common.MessageExt{msg},
	}
	f.broker.mu.Unlock()

	startConsumer(t, c, "TopicA", "*")

	waitForBudget(t, 5*time.Second, func() bool { return len(f.broker.acksSnapshot()) >= 1 })
	if got := derefI64(f.broker.acksSnapshot()[0].Offset); got != 11 {
		t.Fatalf("ack offset = %d, want 11", got)
	}
	if len(f.broker.changeInvisibleSnapshot()) != 0 {
		t.Fatalf("should have given up, not extended: %v", f.broker.changeInvisibleSnapshot())
	}
	// The ACK above must be the listener's failure being settled, not the row
	// filtered away before delivery: it reached the listener first.
	if atomic.LoadInt32(&delivered) == 0 {
		t.Fatal("the row was acked without ever being delivered")
	}
}

// Start must push the broker-side mode switch; otherwise the consumer pops a
// broker that still serves the group with PULL requests.
func TestPopConsumerSendsSetMessageRequestMode(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	})
	f.broker.setPopFixture(popFixture{code: remoting.RespPollingTimeout})
	startConsumer(t, c, "TopicA", "*")

	waitForBudget(t, 5*time.Second, func() bool { return len(f.broker.requestModesSnapshot()) >= 1 })
	modes := f.broker.requestModesSnapshot()
	found := false
	for _, m := range modes {
		if m.Topic == "TopicA" && m.ConsumerGroup == "PopGroup" {
			found = true
			if m.Mode != remoting.MessageRequestModePop {
				t.Fatalf("mode = %q", m.Mode)
			}
		}
		// The auto-subscribed %RETRY% topic must NOT be switched on its own.
		if strings.HasPrefix(m.Topic, "%RETRY%") {
			t.Fatalf("request mode sent for retry topic %q", m.Topic)
		}
	}
	if !found {
		t.Fatalf("no SET_MESSAGE_REQUEST_MODE for TopicA: %v", modes)
	}
}

func TestPopRequestHeaderShape(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	})
	f.broker.setPopFixture(popFixture{code: remoting.RespPollingTimeout})
	startConsumer(t, c, "TopicA", "*")

	waitForBudget(t, 5*time.Second, func() bool { return len(f.broker.popHeadersSnapshot()) >= 1 })
	h := f.broker.popHeadersSnapshot()[0]
	if derefI64(h.InvisibleTime) != 60000 {
		t.Fatalf("invisibleTime = %d", derefI64(h.InvisibleTime))
	}
	if derefI32(h.MaxMsgNums) != 32 {
		t.Fatalf("maxMsgNums = %d", derefI32(h.MaxMsgNums))
	}
	if derefI64(h.PollTime) != 15000 {
		t.Fatalf("pollTime = %d", derefI64(h.PollTime))
	}
	if deref(h.ConsumerGroup) != "PopGroup" || deref(h.Topic) != "TopicA" {
		t.Fatalf("header %v", h)
	}
	if h.IsOrder() {
		t.Fatal("this port must not request orderly pop")
	}
	// bornTime must be a real current-millisecond stamp: the broker rejects a
	// stale bornTime with POLLING_TIMEOUT(210) without ever looking at the queue.
	born := derefI64(h.BornTime)
	now := common.CurrentTimeMillis()
	if born <= 0 || now-born > 60_000 {
		t.Fatalf("bornTime = %d (now=%d)", born, now)
	}
}

// A POP result out of its invisibility window must be dropped WITHOUT acking:
// the broker already revived the messages and someone else may own them.
func TestPopBatchOutOfWindowIsNotAcked(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	})
	// popTime far in the past, invisibleTime 5s -> already expired.
	f.broker.setPopFixture(popFixture{
		code:          remoting.RespSuccess,
		popTime:       common.CurrentTimeMillis() - 60_000,
		invisibleTime: 5000,
		reviveQid:     3,
		msgs:          []*common.MessageExt{popMessage("TopicA", 0, 3, "late")},
	})
	startConsumer(t, c, "TopicA", "*")

	// Give the loop time to pop and drop several times.
	time.Sleep(700 * time.Millisecond)
	if acks := f.broker.acksSnapshot(); len(acks) != 0 {
		t.Fatalf("expired batch was acked: %v", acks)
	}
	// Sampled live the debt is racy — a pop can sit between incFoundMsg and the
	// drop — so settle it: stop the consumer, let the in-flight consumes drain,
	// and require the counter to fall back to exactly zero. A batch that were
	// neither acked nor handed back would leave its debt parked here forever
	// (and the loop would have piled up one per popped batch long before this).
	c.Shutdown()
	waitForBudget(t, 2*time.Second, func() bool { return c.PopWaitAckCount() == 0 })
	if n := c.PopWaitAckCount(); n != 0 {
		t.Fatalf("ACK debt leaked: %d", n)
	}
}

// ---------------------------------------------------------------- pop queue

func TestPopProcessQueueSemantics(t *testing.T) {
	q := newPopProcessQueue()
	if q.WaitAckMsgCount() != 0 {
		t.Fatalf("fresh queue debt = %d", q.WaitAckMsgCount())
	}
	q.IncFoundMsg(3)
	if q.WaitAckMsgCount() != 3 {
		t.Fatalf("after incFoundMsg(3) = %d", q.WaitAckMsgCount())
	}
	if prev := q.Ack(); prev != 3 {
		t.Fatalf("ack() returned %d, want the pre-decrement 3", prev)
	}
	if q.WaitAckMsgCount() != 2 {
		t.Fatalf("after ack() = %d", q.WaitAckMsgCount())
	}
	q.DecFoundMsg(-2)
	if q.WaitAckMsgCount() != 0 {
		t.Fatalf("after decFoundMsg(-2) = %d", q.WaitAckMsgCount())
	}

	q.SetLastPopTimestamp(common.CurrentTimeMillis())
	if q.IsPullExpired() {
		t.Fatal("a just-touched queue must not be expired")
	}
	q.SetLastPopTimestamp(common.CurrentTimeMillis() - popPullMaxIdleTime - 1)
	if !q.IsPullExpired() {
		t.Fatal("an untouched queue past PULL_MAX_IDLE_TIME must be expired")
	}
}

// ---------------------------------------------------------------- gate in the loop

// popGateLoopFixture is a POP consumer whose pop loop is driven by the cache gate
// alone: the queue's debt stays over the threshold, so the loop never reaches a
// broker call and every round is exactly one gate judgment plus one wait.
func popGateLoopFixture(t *testing.T, group string, threshold, waitAck int) (*DefaultMQPushConsumer, common.MessageQueue, *popProcessQueue) {
	t.Helper()
	c := MustNewDefaultMQPushConsumer(group)
	requireNoError(t, "subscribe", c.Subscribe(fcTopic, "*"))
	c.SetPopMode(true)
	c.SetPopThresholdForQueue(threshold)
	mq := fcQueue(0)
	pq := newPopProcessQueue()
	pq.IncFoundMsg(waitAck)
	c.mu.Lock()
	c.popQueueTable[mq] = pq
	c.mu.Unlock()
	return c, mq, pq
}

// popWaitTrips polls the SHARED flow-control counter at 1 ms granularity until the pop
// loop has recorded want trips. The tight granularity matters: the interruptibility leg
// closes the stop channel right after a trip, so it has to know how far into the pause
// it is. Waiting on this counter is itself the observable of the change — a POP trip
// that did not reach it times out here.
func popWaitTrips(t *testing.T, c *DefaultMQPushConsumer, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.FlowControlTriggered() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the pop loop recorded %d flow-control trips in 3s, want %d", c.FlowControlTriggered(), want)
}

// The loop's pause is the cache-gate wait: two rounds take two of them, so the loop
// neither spins (lower bound) nor sits out one of the other POP back-offs (upper bound),
// keeps stamping the last-pop time BEFORE judging the gate (the stall criterion reads
// that stamp, so a pause that skipped the stamp would expire a merely throttled queue),
// leaves the debt alone, and returns as soon as the queue is stopped instead of running
// the wait out.
func TestPopFlowControlLoopPausesForTheCacheDelayAndIsStoppable(t *testing.T) {
	if popDelayWhenCacheFlowControl != 50 {
		t.Fatalf("POP cache backoff = %d ms, want 50 ms", popDelayWhenCacheFlowControl)
	}
	const waitAck = 4
	c, mq, pq := popGateLoopFixture(t, "PopGateLoopGroup", 3, waitAck)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.queuePopLoop(mq, stop)
	}()

	popWaitTrips(t, c, 1)
	begin := time.Now()
	popWaitTrips(t, c, 3)
	gap := time.Since(begin)
	if floor := time.Duration(2*(popDelayWhenCacheFlowControl-5)) * time.Millisecond; gap < floor {
		t.Errorf("two cache-gate rounds took %s, want at least %s: each round must wait popDelayWhenCacheFlowControl", gap, floor)
	}
	if ceil := time.Duration(2*popDelayWhenCacheFlowControl)*time.Millisecond + time.Second; gap > ceil {
		t.Errorf("two cache-gate rounds took %s, want at most %s: the cache gate must not borrow a longer back-off", gap, ceil)
	}

	stamped := pq.LastPopTimestamp()
	popWaitTrips(t, c, 4)
	if pq.LastPopTimestamp() <= stamped {
		t.Errorf("last-pop timestamp stayed %d across a tripped round: the loop must stamp before the gate", stamped)
	}

	// The debt is a pause, not a give-up: throttling hands no ACK back.
	if n := pq.WaitAckMsgCount(); n != waitAck {
		t.Errorf("waiting-ACK debt = %d during the pause, want the staged %d", n, waitAck)
	}

	// Interruptible: closing the queue's stop channel right after a trip cuts the
	// remaining wait short — a plain sleep would still hold the loop for almost the
	// whole 50 ms.
	popWaitTrips(t, c, 5)
	closeAt := time.Now()
	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the pop loop ignored its queue stop channel")
	}
	if waited := time.Since(closeAt); waited > time.Duration(popDelayWhenCacheFlowControl-5)*time.Millisecond {
		t.Errorf("the loop returned %s after the stop signal, want well inside the %d ms pause: the wait must be stop-interruptible",
			waited, popDelayWhenCacheFlowControl)
	}
}

func TestPopDelayLevelTable(t *testing.T) {
	if len(popDelayLevel) != 16 {
		t.Fatalf("table length = %d", len(popDelayLevel))
	}
	if popDelayLevel[0] != 10 || popDelayLevel[1] != 30 || popDelayLevel[15] != 7200 {
		t.Fatalf("table = %v", popDelayLevel)
	}
	if got := popDelayLevelSeconds(99); got != 7200 {
		t.Fatalf("clamped = %d", got)
	}
	if got := popDelayLevelSeconds(2); got != 60 {
		t.Fatalf("level 2 = %d", got)
	}
	// Next notch above the elapsed time.
	if got := popDelayLevelForElapsed(30_000); got != 2 {
		t.Fatalf("30s -> %d, want 2", got)
	}
	if got := popDelayLevelForElapsed(45_000); got != 2 {
		t.Fatalf("45s -> %d, want 2", got)
	}
	if got := popDelayLevelForElapsed(10_000); got != 1 {
		t.Fatalf("10s -> %d, want 1", got)
	}
	if got := popDelayLevelForElapsed(5_000); got != -1 {
		t.Fatalf("5s -> %d, want -1 (below the first delay level)", got)
	}
	if got := popDelayLevelSeconds(popDelayLevelForElapsed(5_000)); got != 10 {
		t.Fatalf("5s clamps to %d, want 10", got)
	}
	// At or above the longest notch the result is len(table), clamped to the
	// last entry by popDelayLevelSeconds.
	if got := popDelayLevelForElapsed(7200 * 1000); got != len(popDelayLevel) {
		t.Fatalf("7200s -> %d, want %d", got, len(popDelayLevel))
	}
	if got := popDelayLevelSeconds(popDelayLevelForElapsed(10_000 * 1000)); got != 7200 {
		t.Fatalf("clamped long delay = %d", got)
	}
}

// Orderly POP is refused at config time rather than silently losing ordering.
func TestPopRejectsOrderly(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("PopOrderly")
	c.SetPopMode(true)
	c.SetOrderlyListener(&countingOrderlyListener{})
	if err := c.checkConfigRanges(); err == nil {
		t.Fatal("pop + orderly must be rejected")
	}
}

func TestPopConfigBounds(t *testing.T) {
	cases := []struct {
		name  string
		apply func(*DefaultMQPushConsumer)
	}{
		{"invisible too small", func(c *DefaultMQPushConsumer) { c.SetPopInvisibleTime(4999) }},
		{"invisible too large", func(c *DefaultMQPushConsumer) { c.SetPopInvisibleTime(300001) }},
		{"batch zero", func(c *DefaultMQPushConsumer) { c.SetPopBatchNums(0) }},
		{"batch too large", func(c *DefaultMQPushConsumer) { c.SetPopBatchNums(33) }},
	}
	for _, tc := range cases {
		c := MustNewDefaultMQPushConsumer("PopBounds")
		c.SetPopMode(true)
		tc.apply(c)
		if err := c.checkConfigRanges(); err == nil {
			t.Fatalf("%s: expected a config error", tc.name)
		}
	}
	c := MustNewDefaultMQPushConsumer("PopBoundsOK")
	c.SetPopMode(true)
	if err := c.checkConfigRanges(); err != nil {
		t.Fatalf("defaults rejected: %v", err)
	}
}

// ---------------------------------------------------------------- helpers

type countingConcurrentlyListener struct {
	fn func([]*common.MessageExt, *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus
}

func (l *countingConcurrentlyListener) ConsumeMessage(msgs []*common.MessageExt,
	ctx *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
	return l.fn(msgs, ctx)
}

type countingOrderlyListener struct{}

func (l *countingOrderlyListener) ConsumeMessage(_ []*common.MessageExt,
	_ *ConsumeOrderlyContext) ConsumeOrderlyStatus {
	return OrderlySuccess
}

// popTestInstance is a minimal POP-capable Instance bound to one mock broker.
type popTestInstance struct {
	*Instance
	broker *consumerBroker
}

// newTestInstanceForPop wires an Instance straight at a mock broker (no
// nameserver), which is enough for popOnce's address resolution.
func newTestInstanceForPop(t *testing.T) *popTestInstance {
	t.Helper()
	broker := newConsumerBroker()
	srv := startMockServer(t, broker.handler())
	inst := CreateOrGetInstance("pop-unit-"+uniqueSuffix(t), nil, NewClientInstanceConfig())
	setBrokerAddrForTest(inst, "b1", srv.addr)
	t.Cleanup(func() {
		inst.Shutdown()
		srv.close()
	})
	return &popTestInstance{Instance: inst, broker: broker}
}

// setBrokerAddrForTest seeds the instance's broker address table directly, so
// the test needs no nameserver. It writes the MASTER id, which is the entry
// popAckAddress and FindBrokerAddressInSubscribe look for.
func setBrokerAddrForTest(inst *Instance, brokerName, addr string) {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	inst.brokerAddrTable[brokerName] = map[int64]string{int64(common.MasterID): addr}
}

// popOnceForTest is Instance.PopMessage with the header the consumer would send.
func popOnceForTest(inst *popTestInstance, topic string, queueID int32) (*PopResult, error) {
	header := &remoting.PopMessageRequestHeader{
		Bname:         remoting.StrPtr("b1"),
		ConsumerGroup: remoting.StrPtr("PopUnitGroup"),
		Topic:         remoting.StrPtr(topic),
		QueueID:       remoting.I32Ptr(queueID),
		MaxMsgNums:    remoting.I32Ptr(32),
		InvisibleTime: remoting.I64Ptr(60000),
		PollTime:      remoting.I64Ptr(15000),
		BornTime:      remoting.I64Ptr(common.CurrentTimeMillis()),
		InitMode:      remoting.I32Ptr(0),
		Order:         remoting.BoolPtr(false),
	}
	addr, _, ok := inst.FindBrokerAddressInSubscribe("b1", int64(common.MasterID), true)
	if !ok {
		return nil, fmt.Errorf("broker b1 not reachable")
	}
	return inst.PopMessage("b1", addr, header, "", 5000)
}

// waitForBudget polls cond until it holds or the budget runs out. (consumer_test
// .go already owns a waitFor with an explicit label; this one is the POP-side
// shorthand and keeps its call sites readable.)
func waitForBudget(t *testing.T, budget time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", budget)
}
