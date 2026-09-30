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
	fixture := b.popFixture
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

// Retry budget exhausted and the message younger than twice the longest delay:
// Java extends by the next notch above the elapsed time.
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
	msg.ReconsumeTimes = 5
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
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ReconsumeLater
		},
	}, func(c *DefaultMQPushConsumer) { c.SetMaxReconsumeTimes(0) })

	popTime := common.CurrentTimeMillis()
	msg := popMessage("TopicA", 0, 11, "ancient")
	// Older than 7200*1000*2 = 4h.
	msg.BornTimestamp = common.CurrentTimeMillis() - (7200*1000*2 + 60_000)
	msg.ReconsumeTimes = 3
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

// The POP request must carry the Java defaults and never suspend on a client
// offset: bornTime is "now", pollTime is 15s, invisibleTime is the configured
// 60s, and maxMsgNums is the configured batch size.
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
	// ack() returns the value BEFORE the decrement (Java AtomicInteger
	// .getAndDecrement), which the live verifiers read.
	if prev := q.Ack(); prev != 3 {
		t.Fatalf("ack() returned %d, want the pre-decrement 3", prev)
	}
	if q.WaitAckMsgCount() != 2 {
		t.Fatalf("after ack() = %d", q.WaitAckMsgCount())
	}
	// decFoundMsg ADDS — Java calls it with a negative count to hand debt back.
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

func TestPopDelayLevelTable(t *testing.T) {
	if len(popDelayLevel) != 16 {
		t.Fatalf("table length = %d", len(popDelayLevel))
	}
	if popDelayLevel[0] != 10 || popDelayLevel[1] != 30 || popDelayLevel[15] != 7200 {
		t.Fatalf("table = %v", popDelayLevel)
	}
	// Clamping, not erroring (Java: `delayLevel >= length ? last : table[level]`).
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
	// Below the shortest notch Java's loop falls off the end with -1 and then
	// indexes the table with -1 (AIOOBE). -1 is the faithful return; the clamp
	// lives in popDelayLevelSeconds / checkNeedAckOrDelay.
	if got := popDelayLevelForElapsed(5_000); got != -1 {
		t.Fatalf("5s -> %d, want -1 (Java's loop then AIOOBEs)", got)
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
	// The Java defaults must pass.
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
