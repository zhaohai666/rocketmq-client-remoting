// What a POP round is judged on: the rows this client kept, not the rows the
// broker handed over. The pre-delivery filter, the ACKs owed for the rows the
// filter removed, the pull stats, the wait-ACK debt and the re-schedule decision
// all read the surviving list, so a stage that is one row off shows up as a wrong
// counter, a missing ACK or a round that waited the wrong amount of time.
//
// The staged fixtures therefore put a row ON the retry-budget boundary (a batch of
// N+1 rows against a budget of N is the only staging that tells a strict comparison
// from an inclusive one), and the assertions read what the broker actually received
// or what the client actually recorded.
package client

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- stats reader

// popPullStats reads the raw PULL_TPS / PULL_RT counters this client recorded for
// one (group, topic) pair: the running sum and how many rounds added to it. The
// reported PullTPS is a rate over a sampled window, so a test that has to know what
// ONE round recorded reads the counters behind it.
func popPullStats(t *testing.T, c *DefaultMQPushConsumer, topic string) (tpsSum, tpsTimes, rtTimes int64) {
	t.Helper()
	if c.instance == nil {
		t.Fatal("consumer has no instance to read stats from")
	}
	key := statsKey(topic, c.consumerGroup)
	if it := c.instance.consumerStats.topicAndGroupPullTPS.getItem(key); it != nil {
		tpsSum, tpsTimes = it.value.Load(), it.times.Load()
	}
	if it := c.instance.consumerStats.topicAndGroupPullRT.getItem(key); it != nil {
		rtTimes = it.times.Load()
	}
	return tpsSum, tpsTimes, rtTimes
}

// ackOffsetSet counts the ACKs the broker received per queue offset.
func ackOffsetSet(acks []*remoting.AckMessageRequestHeader) map[int64]int {
	out := map[int64]int{}
	for _, a := range acks {
		out[derefI64(a.Offset)]++
	}
	return out
}

func wantAckOffsets(t *testing.T, got map[int64]int, want ...int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("acked offsets = %v, want exactly %v", got, want)
	}
	for _, off := range want {
		if got[off] != 1 {
			t.Fatalf("acked offsets = %v, want offset %d exactly once", got, off)
		}
	}
}

// ------------------------------------------------------ blocking POP listener

// popBlockingListener records every batch it is handed and then holds the consume
// thread there until the test releases it. While it is held, the only ACKs the
// broker can have received are the ones the POP round itself sent, which is what
// makes "the client acked the rows it filtered out" observable instead of merely
// consistent with a later total.
type popBlockingListener struct {
	release chan struct{}
	mu      sync.Mutex
	bodies  []string
	offsets []int64
	topics  []string
}

func newPopBlockingListener() *popBlockingListener {
	return &popBlockingListener{release: make(chan struct{})}
}

func (l *popBlockingListener) ConsumeMessage(msgs []*common.MessageExt,
	_ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
	l.mu.Lock()
	for _, msg := range msgs {
		l.bodies = append(l.bodies, string(msg.Body))
		l.offsets = append(l.offsets, msg.QueueOffset)
		l.topics = append(l.topics, msg.Topic)
	}
	l.mu.Unlock()
	select {
	case <-l.release:
	case <-time.After(10 * time.Second):
	}
	return ConsumeSuccess
}

// releaseNow lets the held batch finish; safe to call more than once.
func (l *popBlockingListener) releaseNow() {
	select {
	case <-l.release:
	default:
		close(l.release)
	}
}

func (l *popBlockingListener) seenBodies() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.bodies...)
}

func (l *popBlockingListener) batchCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.bodies)
}

func (l *popBlockingListener) waitDelivered(t *testing.T, want int) {
	t.Helper()
	waitForBudget(t, 5*time.Second, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return len(l.bodies) >= want
	})
}

// The default budget is the number the group's own retry limit answers to, so an
// unset knob must not read as "unlimited": with the knob left at its -1 default the
// row at 16 is delivered and the row at 17 is dropped and acked. An implementation
// that treats -1 as "no limit" delivers both and sends no ACK, so this goes red.
func TestPopBudgetDefaultsToSixteenWhenTheKnobIsUnset(t *testing.T) {
	listener := newPopBlockingListener()
	defer listener.releaseNow()
	f, c := popFixtureWithConsumer(t, listener)
	if c.maxReconsumeTimes != -1 {
		t.Fatalf("fixture knob = %d, want the -1 default this test is about", c.maxReconsumeTimes)
	}
	atDefault := popTaggedMessage("TopicA", 0, 16, "at16", "TagA", 16)
	overDefault := popTaggedMessage("TopicA", 0, 17, "at17", "TagA", 17)
	f.broker.setPopScript(noMoreMessages(), foundRound(atDefault, overDefault))
	startConsumer(t, c, "TopicA", "*")

	listener.waitDelivered(t, 1)
	if got := listener.seenBodies(); !equalStrings(got, []string{"at16"}) {
		t.Fatalf("listener saw %v, want only the row sitting exactly on the default budget", got)
	}
	// Held in the listener ⇒ un-acked, so the single ACK on the wire is the drop.
	wantAckOffsets(t, ackOffsetSet(f.broker.acksSnapshot()), 17)
	if n := c.PopWaitAckCount(); n != 1 {
		t.Fatalf("wait-ACK debt = %d, want the one delivered row", n)
	}
}

// The budget leg belongs to POP alone. A pull row is bounded by the offset, so a
// pre-dropped pull row would vanish with neither an ACK nor a send-back: the same
// over-budget row that POP acks away must reach the pull listener untouched. Hoisting
// the drop into the shared pre-delivery filter takes this leg red.
func TestPullPathDeliversRowsPastThePopBudget(t *testing.T) {
	const group = "GID_pull_keeps_over_budget"
	listener := newRecordingListener()
	c := MustNewDefaultMQPushConsumer(group)
	c.SetMaxReconsumeTimes(1)
	c.SetMessageListener(listener)
	mq := common.NewMessageQueue("TopicA", "b1", 0)
	// The queue was never registered, so every row here is on its first epoch.
	over := common.NewMessageExt()
	over.Topic = mq.Topic
	over.Body = []byte("over-budget")
	over.QueueOffset = 14
	over.ReconsumeTimes = 17

	c.consumeBatch(mq, []*common.MessageExt{over}, c.queueEpochOf(mq))

	if got := listener.bodies(); !equalStrings(got, []string{"over-budget"}) {
		t.Fatalf("pull listener saw %v, want the over-budget row delivered", got)
	}
}

// --------------------------------------------------------- filter hooks under test

// popFilterHook is a FilterMessageHook that either throws or removes the row whose
// body matches `drop`.
type popFilterHook struct {
	name   string
	drop   string
	calls  atomic.Int32
	throws bool
}

func (h *popFilterHook) HookName() string { return h.name }

func (h *popFilterHook) FilterMessage(ctx *FilterMessageContext) {
	h.calls.Add(1)
	if h.throws {
		panic("pop filter hook threw on purpose")
	}
	if h.drop == "" {
		return
	}
	out := make([]*common.MessageExt, 0, len(ctx.MsgList))
	for _, msg := range ctx.MsgList {
		if string(msg.Body) == h.drop {
			continue
		}
		out = append(out, msg)
	}
	ctx.MsgList = out
}

// popTaggedMessage builds a held row with a tag, a reconsume count and an offset
// the ACK can be traced back to.
func popTaggedMessage(topic string, queueID int32, offset int64, body, tag string, reconsume int32) *common.MessageExt {
	msg := popMessage(topic, queueID, offset, body)
	if tag != "" {
		msg.PutProperty(common.PropertyTags, tag)
	}
	msg.ReconsumeTimes = reconsume
	return msg
}

func foundRound(msgs ...*common.MessageExt) popFixture {
	return popFixture{
		code:          remoting.RespSuccess,
		popTime:       common.CurrentTimeMillis(),
		invisibleTime: 60000,
		reviveQid:     3,
		msgs:          msgs,
	}
}

// noMoreMessages is the tail answer: the loop stays occupied without ever handing
// over another batch, so a staged round really is the only round asserted on.
func noMoreMessages() popFixture {
	return popFixture{code: remoting.RespPollingTimeout}
}

// ---------------------------------------------------------------------- P1 initMode

// initMode is the field that decides where a group with no POP checkpoint yet
// starts in the queue, so it is asserted on the extFields the broker RECEIVED —
// a decoded Go value would also satisfy a request built from a constant.
func TestPopInitModeFollowsConsumeFromWhere(t *testing.T) {
	cases := []struct {
		name  string
		where string // "" = leave the consumer's own default
		want  string
	}{
		{"default policy asks for the tail", "", "1"},
		{"LAST_OFFSET asks for the tail", ConsumeFromWhereLastOffset, "1"},
		{"FIRST_OFFSET asks for the oldest", ConsumeFromWhereFirstOffset, "0"},
		{"only FIRST_OFFSET gets MIN", ConsumeFromWhereTimestamp, "1"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var opts []consumerOpt
			if tc.where != "" {
				opts = append(opts, withConsumeFromWhere(tc.where))
			}
			f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
				fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
					return ConsumeSuccess
				},
			}, opts...)
			f.broker.setPopFixture(noMoreMessages())
			startConsumer(t, c, "TopicA", "*")

			waitForBudget(t, 5*time.Second, func() bool { return f.broker.popCount() >= 1 })
			got, ok := f.broker.popExtSnapshot()[0].Get("initMode")
			if !ok {
				t.Fatal("POP_MESSAGE reached the broker with no initMode field")
			}
			if got != tc.want {
				t.Fatalf("initMode on the wire = %q, want %q", got, tc.want)
			}
		})
	}
}

// ------------------------------------------------------------- P2 pre-delivery filter

// Five rows staged against a budget of 1: the tag-mismatched row, the untagged row
// and the row strictly over the budget are dropped AND acked, the in-budget rows
// are the only ones delivered — and the one sitting exactly ON the budget is
// delivered, which is what tells the strict comparison from an inclusive one.
func TestPopPreDeliveryFilterDropsAndAcks(t *testing.T) {
	keep := popTaggedMessage("TopicA", 0, 10, "keep", "TagA", 0)
	badTag := popTaggedMessage("TopicA", 0, 11, "badTag", "TagB", 0)
	untagged := popTaggedMessage("TopicA", 0, 12, "untagged", "", 0)
	atBudget := popTaggedMessage("TopicA", 0, 13, "atBudget", "TagA", 1)
	overBudget := popTaggedMessage("TopicA", 0, 14, "overBudget", "TagA", 2)

	listener := newPopBlockingListener()
	defer listener.releaseNow()
	f, c := popFixtureWithConsumer(t, listener,
		func(c *DefaultMQPushConsumer) { c.SetMaxReconsumeTimes(1) },
		func(c *DefaultMQPushConsumer) { c.SetConsumeMessageBatchMaxSize(10) })
	f.broker.setPopScript(noMoreMessages(), foundRound(keep, badTag, untagged, atBudget, overBudget))
	startConsumer(t, c, "TopicA", "TagA")

	// The two surviving rows arrive as one batch, in the order the broker sent them.
	listener.waitDelivered(t, 2)
	if got := listener.seenBodies(); !equalStrings(got, []string{"keep", "atBudget"}) {
		t.Fatalf("listener saw %v, want exactly [keep atBudget]", got)
	}

	// While the batch is held in the listener nothing it contains has been acked,
	// so every ACK the broker holds is one the filter round owed.
	acked := ackOffsetSet(f.broker.acksSnapshot())
	wantAckOffsets(t, acked, 11, 12, 14)

	// The stats and the debt only ever saw the surviving rows: 2, never 5, and the
	// round is recorded once.
	if tpsSum, tpsTimes, rtTimes := popPullStats(t, c, "TopicA"); tpsSum != 2 || tpsTimes != 1 || rtTimes != 1 {
		t.Fatalf("PULL_TPS sum/times=%d/%d, PULL_RT times=%d; want 2/1/1", tpsSum, tpsTimes, rtTimes)
	}
	if n := c.PopWaitAckCount(); n != 2 {
		t.Fatalf("wait-ACK debt while held = %d, want the 2 delivered rows only", n)
	}

	listener.releaseNow()
	waitForBudget(t, 5*time.Second, func() bool { return c.PopWaitAckCount() == 0 })
	wantAckOffsets(t, ackOffsetSet(f.broker.acksSnapshot()), 10, 11, 12, 13, 14)
}

// A subscription with no tag expression must not filter anything — an empty tag
// set is "all", not "none". The control row here is the untagged one: the sibling
// test drops it, this one must deliver it.
func TestPopWildcardSubscriptionFiltersNothing(t *testing.T) {
	tagged := popTaggedMessage("TopicA", 0, 20, "tagged", "TagZ", 0)
	untagged := popTaggedMessage("TopicA", 0, 21, "untagged", "", 0)

	listener := newPopBlockingListener()
	defer listener.releaseNow()
	f, c := popFixtureWithConsumer(t, listener,
		func(c *DefaultMQPushConsumer) { c.SetConsumeMessageBatchMaxSize(10) })
	f.broker.setPopScript(noMoreMessages(), foundRound(tagged, untagged))
	startConsumer(t, c, "TopicA", "*")

	listener.waitDelivered(t, 2)
	if got := listener.seenBodies(); !equalStrings(got, []string{"tagged", "untagged"}) {
		t.Fatalf("listener saw %v, want both rows under a wildcard subscription", got)
	}
	if acks := f.broker.acksSnapshot(); len(acks) != 0 {
		t.Fatalf("wildcard round acked %d rows before delivery, want none", len(acks))
	}
	if tpsSum, _, _ := popPullStats(t, c, "TopicA"); tpsSum != 2 {
		t.Fatalf("PULL_TPS sum = %d, want 2", tpsSum)
	}
}

// The FilterMessageHook list runs over the rows the tag filter kept, over the POP
// path: a row a hook removed is acked and never delivered.
func TestPopFilterHookRemovalIsAcked(t *testing.T) {
	stay := popTaggedMessage("TopicA", 0, 30, "stay", "TagA", 0)
	goAway := popTaggedMessage("TopicA", 0, 31, "goAway", "TagA", 0)

	hook := &popFilterHook{name: "drop-goAway", drop: "goAway"}
	listener := newPopBlockingListener()
	defer listener.releaseNow()
	f, c := popFixtureWithConsumer(t, listener,
		func(c *DefaultMQPushConsumer) { c.SetConsumeMessageBatchMaxSize(10) })
	c.RegisterFilterMessageHook(hook)
	f.broker.setPopScript(noMoreMessages(), foundRound(stay, goAway))
	startConsumer(t, c, "TopicA", "TagA")

	listener.waitDelivered(t, 1)
	if got := listener.seenBodies(); !equalStrings(got, []string{"stay"}) {
		t.Fatalf("listener saw %v, want only [stay]", got)
	}
	if hook.calls.Load() == 0 {
		t.Fatal("the filter hook was never run on the POP path")
	}
	wantAckOffsets(t, ackOffsetSet(f.broker.acksSnapshot()), 31)
	if tpsSum, _, _ := popPullStats(t, c, "TopicA"); tpsSum != 1 {
		t.Fatalf("PULL_TPS sum = %d, want the 1 row that survived the hook", tpsSum)
	}
}

// A hook that throws costs the round its filter and nothing else: the batch still
// reaches the listener whole.
func TestPopFilterHookPanicKeepsTheBatch(t *testing.T) {
	first := popTaggedMessage("TopicA", 0, 40, "first", "TagA", 0)
	second := popTaggedMessage("TopicA", 0, 41, "second", "TagA", 0)

	hook := &popFilterHook{name: "throwing", throws: true}
	listener := newPopBlockingListener()
	defer listener.releaseNow()
	f, c := popFixtureWithConsumer(t, listener,
		func(c *DefaultMQPushConsumer) { c.SetConsumeMessageBatchMaxSize(10) })
	c.RegisterFilterMessageHook(hook)
	f.broker.setPopScript(noMoreMessages(), foundRound(first, second))
	startConsumer(t, c, "TopicA", "TagA")

	listener.waitDelivered(t, 2)
	if got := listener.seenBodies(); !equalStrings(got, []string{"first", "second"}) {
		t.Fatalf("listener saw %v, want the whole batch after a throwing hook", got)
	}
	if hook.calls.Load() == 0 {
		t.Fatal("the throwing hook was never reached")
	}
	// Nothing was filtered, so the round owes no ACK of its own.
	if acks := f.broker.acksSnapshot(); len(acks) != 0 {
		t.Fatalf("a throwing hook acked %d rows before delivery, want none", len(acks))
	}
}

// ------------------------------------------------------------------ P3 re-schedule

// The gap helper every re-schedule case shares: wait for `want` POP rounds and
// return the arrival times.
func popArrivalsWithin(t *testing.T, b *consumerBroker, want int, budget time.Duration) []int64 {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if got := b.popArrivalsSnapshot(); len(got) >= want {
			return got[:want]
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := b.popArrivalsSnapshot()
	t.Fatalf("only %d POP rounds arrived in %s, want %d", len(got), budget, want)
	return nil
}

// POLLING_NOT_FOUND means the broker already held the poll open, so the next round
// is not delayed: a run of rounds fits in far less time than even one back-off.
func TestPopPollingNotFoundRePopsImmediately(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	})
	f.broker.setPopFixture(popFixture{code: remoting.RespPollingTimeout})
	startConsumer(t, c, "TopicA", "*")

	// 15 rounds inside one second: any per-round wait, however small, makes this
	// red, and so does the POLLING_FULL back-off.
	arrivals := popArrivalsWithin(t, f.broker, 15, 10*time.Second)
	if span := arrivals[14] - arrivals[0]; span > 1000 {
		t.Fatalf("15 POLLING_NOT_FOUND rounds took %dms, want them back to back", span)
	}
	if acks := f.broker.acksSnapshot(); len(acks) != 0 {
		t.Fatalf("an empty round acked %d rows", len(acks))
	}
}

// POLLING_FULL is the broker asking to be approached later, so the round sleeps
// the single generic failed-round back-off the consumer captured at setup — not
// pullInterval, not an immediate re-pop, and not a hardcoded number. The knob is
// set to a value no fixed arm uses, so only the loop reading the knob reproduces
// this gap: a POLLING_FULL leg still wired to the old 3000 ms constant, or to the
// 1000 ms default, or to an immediate re-pop, each lands outside the window.
func TestPopPollingFullParksForTheConfiguredBackoff(t *testing.T) {
	const backoffMs = 400
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	}, func(c *DefaultMQPushConsumer) { c.SetPullFailedBackoffMillis(backoffMs) })
	// The POP path must read the same knob the pull path does.
	if got := c.PullFailedBackoffMillis(); got != backoffMs {
		t.Fatalf("configured back-off reads %d ms, want %d", got, backoffMs)
	}
	f.broker.setPopFixture(popFixture{code: remoting.RespPollingFull})
	startConsumer(t, c, "TopicA", "*")

	arrivals := popArrivalsWithin(t, f.broker, 2, 10*time.Second)
	gap := arrivals[1] - arrivals[0]
	// At least the configured wait ...
	if gap < backoffMs-100 {
		t.Fatalf("POLLING_FULL re-armed after %dms, want at least the %dms configured back-off",
			gap, backoffMs)
	}
	// ... and strictly under the 1000 ms default, so this cannot be the loop
	// ignoring the knob and falling back to any fixed arm.
	if gap > 900 {
		t.Fatalf("POLLING_FULL re-armed after %dms, want the %dms configured back-off and nothing"+
			" closer to the 1000ms default or the old 3000ms constant", gap, backoffMs)
	}
}

// A FOUND round the filter emptied is not a delivery: nothing is counted as a pull
// and the next pop is not delayed, even though the same round would have waited
// pullInterval had a row survived.
func TestPopFilteredEmptyFoundRePopsImmediatelyWithNoTPS(t *testing.T) {
	badTag := popTaggedMessage("TopicA", 0, 50, "badTag", "TagB", 0)
	untagged := popTaggedMessage("TopicA", 0, 51, "untagged", "", 0)

	var delivered int32
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(msgs []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			atomic.AddInt32(&delivered, int32(len(msgs)))
			return ConsumeSuccess
		},
	}, func(c *DefaultMQPushConsumer) { c.SetPullInterval(500) })
	// The same two rows come back every round (the fixture replays them), which is
	// exactly the shape this case needs: an unfiltered client would deliver them.
	f.broker.setPopFixture(foundRound(badTag, untagged))
	startConsumer(t, c, "TopicA", "TagA")

	// 12 rounds inside one second, while pullInterval alone would put 500 ms on
	// each of them.
	arrivals := popArrivalsWithin(t, f.broker, 12, 10*time.Second)
	if span := arrivals[11] - arrivals[0]; span > 1500 {
		t.Fatalf("12 filtered-empty FOUND rounds took %dms, want an immediate re-pop", span)
	}
	waitForBudget(t, 5*time.Second, func() bool { return len(f.broker.acksSnapshot()) >= 2 })
	if got := ackOffsetSet(f.broker.acksSnapshot()); len(got) != 2 || got[50] == 0 || got[51] == 0 {
		t.Fatalf("acked offsets = %v, want the two filtered rows 50 and 51", got)
	}
	// The round costs the client no pull throughput: the RT counter moves, the TPS
	// counter never does.
	if tpsSum, tpsTimes, rtTimes := popPullStats(t, c, "TopicA"); tpsSum != 0 || tpsTimes != 0 || rtTimes < 1 {
		t.Fatalf("PULL_TPS sum/times=%d/%d with PULL_RT times=%d, want 0/0/>=1", tpsSum, tpsTimes, rtTimes)
	}
	if n := c.PopWaitAckCount(); n != 0 {
		t.Fatalf("wait-ACK debt = %d, want 0: filtered rows never entered it", n)
	}
	if n := atomic.LoadInt32(&delivered); n != 0 {
		t.Fatalf("the listener was handed %d rows it should never have seen", n)
	}
}

// FOUND with rows left waits pullInterval, and only when pullInterval is positive:
// the two cases below differ in one knob and must differ in the gap they produce.
func TestPopFoundHonoursPullIntervalOnlyWhenPositive(t *testing.T) {
	cases := []struct {
		name       string
		intervalMs int64
		minGap     int64
		maxGap     int64
	}{
		{"zero interval re-pops at once", 0, 0, 1500},
		{"positive interval is waited out", 600, 500, 2900},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			one := popTaggedMessage("TopicA", 0, 60, "one", "TagA", 0)
			f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
				fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
					return ConsumeSuccess
				},
			}, func(c *DefaultMQPushConsumer) { c.SetPullInterval(tc.intervalMs) })
			// One FOUND round carrying a deliverable row, then nothing: the second
			// round is the one whose arrival time says which wait the loop used.
			f.broker.setPopScript(noMoreMessages(), foundRound(one))
			startConsumer(t, c, "TopicA", "TagA")

			arrivals := popArrivalsWithin(t, f.broker, 2, 10*time.Second)
			gap := arrivals[1] - arrivals[0]
			if gap < tc.minGap {
				t.Fatalf("round 2 arrived %dms after round 1, want at least %dms (pullInterval=%d)",
					gap, tc.minGap, tc.intervalMs)
			}
			if gap > tc.maxGap {
				t.Fatalf("round 2 arrived %dms after round 1, want under %dms (pullInterval=%d)",
					gap, tc.maxGap, tc.intervalMs)
			}
		})
	}
}

// ----------------------------------------------------------------- P4 retry topics

// The two POP retry-topic spellings are cut at their separator; exactly
// `%RETRY%<group>` (no separator), an ordinary topic and another group's retry
// topic are all left alone.
func TestPopResetRetryTopicNormalisation(t *testing.T) {
	const group = "NormGroup"
	cases := []struct {
		in   string
		want string
	}{
		{common.BuildPopRetryTopicV2("TopicA", group), "TopicA"},
		{common.BuildPopRetryTopicV1("TopicA", group), "TopicA"},
		{common.GetRetryTopic(group), common.GetRetryTopic(group)},
		{"TopicA", "TopicA"},
		// A retry topic this group never owns: cutting it would invent a topic.
		{common.BuildPopRetryTopicV2("TopicA", "OtherGroup"), common.BuildPopRetryTopicV2("TopicA", "OtherGroup")},
		// A business topic that itself contains the other separator: everything
		// after THIS row's separator is the topic.
		{common.BuildPopRetryTopicV2("Topic_A_Extra", group), "Topic_A_Extra"},
		{common.BuildPopRetryTopicV1("Topic_A_Extra", group), "Topic_A_Extra"},
		// A wrapper with nothing behind the separator names no business topic at
		// all: cutting it would leave the listener an empty topic.
		{common.RetryGroupTopicPrefix + group + "+", common.RetryGroupTopicPrefix + group + "+"},
		{common.RetryGroupTopicPrefix + group + "_", common.RetryGroupTopicPrefix + group + "_"},
	}
	c := MustNewDefaultMQPushConsumer(group)
	msgs := make([]*common.MessageExt, 0, len(cases))
	for _, tc := range cases {
		msg := common.NewMessageExt()
		msg.Topic = tc.in
		msgs = append(msgs, msg)
	}
	c.resetPopRetryTopic(msgs)
	for i, tc := range cases {
		if msgs[i].Topic != tc.want {
			t.Errorf("case %d: topic %q became %q, want %q", i, tc.in, msgs[i].Topic, tc.want)
		}
	}
}

// With a namespace the local prefix is peeled before the separator is cut and the
// result is peeled again, so the listener is handed the plain business topic.
func TestPopResetRetryTopicNormalisationStripsNamespace(t *testing.T) {
	const (
		group     = "NSGroup"
		namespace = "MQ_INST_ns"
	)
	c := MustNewDefaultMQPushConsumer(group)
	c.SetNamespace(namespace)

	namespaced := common.RetryGroupTopicPrefix + common.WrapNamespace(namespace, group) + "_TopicA"
	msg := common.NewMessageExt()
	msg.Topic = namespaced
	msgs := []*common.MessageExt{msg}
	c.resetPopRetryTopic(msgs)
	if msg.Topic != "TopicA" {
		t.Fatalf("topic %q became %q, want the business topic with no namespace", namespaced, msg.Topic)
	}
}

// End to end: a row popped from a `%RETRY%<group>+<topic>` queue is presented to
// the listener under the business topic it subscribed to, not under the retry name.
func TestPopRetryTopicIsNormalisedBeforeListener(t *testing.T) {
	const group = "GID_pop_retry_norm"
	retryTopic := common.BuildPopRetryTopicV2("TopicA", group)

	f := newClusterFixture(t, map[string]int{retryTopic: 1})
	listener := newRecordingListener()
	f.broker.setPopScript(noMoreMessages(),
		foundRound(popTaggedMessage(retryTopic, 0, 70, "retried", "TagA", 0)))
	c := f.newConsumer(t, group, listener,
		func(c *DefaultMQPushConsumer) { c.SetPopMode(true) },
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, retryTopic, "TagA")

	waitFor(t, "the retried row delivered", func() bool { return listener.receivedAll(1) })
	if got := listener.seenTopics(); got[0] != "TopicA" {
		t.Fatalf("listener saw topic %q, want %q normalised out of %q", got[0], "TopicA", retryTopic)
	}
	if got := listener.bodies(); !equalStrings(got, []string{"retried"}) {
		t.Fatalf("listener saw %v", got)
	}
}

// retryTopicOrderlyListener records the topic each row arrived under, so an
// orderly dispatch can be shown to keep the name its queue was locked under.
type retryTopicOrderlyListener struct {
	mu     sync.Mutex
	topics []string
}

func (l *retryTopicOrderlyListener) ConsumeMessage(msgs []*common.MessageExt,
	_ *ConsumeOrderlyContext) ConsumeOrderlyStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range msgs {
		l.topics = append(l.topics, m.Topic)
	}
	return OrderlySuccess
}

func (l *retryTopicOrderlyListener) seenTopics() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.topics...)
}

// The pull-mode concurrent entry is the other delivery entry the POP naming is
// cut at, and it runs ahead of the property-based restore: a wrapper this group
// owns becomes the business topic, while the classic %RETRY%<group> row still
// restores from its RETRY_TOPIC property and another group's wrapper is left
// alone.
func TestPullConcurrentEntryNormalisesRetryTopicBeforeListener(t *testing.T) {
	const group = "GID_pull_retry_norm"
	wrapped := common.NewMessageExt()
	wrapped.Topic = common.BuildPopRetryTopicV2("TopicA", group)
	wrapped.Body = []byte("popped-retry")
	classic := common.NewMessageExt()
	classic.Topic = common.GetRetryTopic(group)
	classic.Body = []byte("classic-retry")
	classic.PutProperty(common.PropertyRetryTopic, "TopicB")
	foreign := common.NewMessageExt()
	foreign.Topic = common.BuildPopRetryTopicV2("TopicA", "OtherGroup")
	foreign.Body = []byte("foreign")

	listener := newRecordingListener()
	c := MustNewDefaultMQPushConsumer(group)
	c.SetMessageListener(listener)
	mq := common.NewMessageQueue(wrapped.Topic, "b1", 0)
	c.consumeBatch(mq, []*common.MessageExt{wrapped, classic, foreign}, 0)

	if got := listener.seenTopics(); !equalStrings(got, []string{"TopicA", "TopicB", foreign.Topic}) {
		t.Fatalf("listener saw %v, want the wrapper cut and the property restore still applied", got)
	}
}

// Control: an orderly dispatch consumes the queue it locked under that queue's
// own name, so the wrapper stays -- the offset bookkeeping of a retry queue is
// keyed on it.
func TestPullOrderlyEntryKeepsThePopRetryTopicName(t *testing.T) {
	const group = "GID_pull_retry_orderly"
	orderly := &retryTopicOrderlyListener{}
	c := MustNewDefaultMQPushConsumer(group)
	c.SetMessageListener(orderly)

	name := common.BuildPopRetryTopicV1("TopicA", group)
	row := common.NewMessageExt()
	row.Topic = name
	row.Body = []byte("ordered-retry")
	c.consumeBatch(common.NewMessageQueue(name, "b1", 0), []*common.MessageExt{row}, 0)

	if got := orderly.seenTopics(); !equalStrings(got, []string{name}) {
		t.Fatalf("orderly listener saw %v, want the retry name it locked the queue under", got)
	}
}
