// The lite pull consumer's cache gates are the only thing that keeps an
// application that never polls from turning the local buffer into an unbounded
// queue, so this file pins them without a live cluster: the DIRECTION of all four
// comparisons (strictly greater, so a threshold reads as "how many may still be
// cached"), the UNITS (message BODY in whole MiB, offset span, message count), the
// aggregate budget's multiplier (a pending pull round charges a whole pullBatchSize,
// not the messages that round happened to bring back), the SCOPE of each budget
// (one queue alone, except the whole-buffer one), the 50 ms re-schedule that
// belongs to the refused queue only, the three trip counters with the shared WARN
// cadence, and the broker's own 215 answer getting the short 20 ms pause instead.
//
// Every positive assertion sits next to the control that goes red when the rule
// behind it is reverted; the paired leg is named in the comment above it.
package client

import (
	"strings"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const liteFCTopic = "GoLiteFlowControlTopic"

// liteFCOff switches a budget out of the way by making it enormous: the gates have
// no off value (0 means "trip on the first cached message"), so a test that wants
// one gate must raise the other three instead of leaving them at the default.
const liteFCOff = int64(1 << 40)

// liteFCParkExpiry is longer than the shortest pause this file asserts, so a test
// can tell "the window ran out" from "no window was ever scheduled".
const liteFCParkExpiry = 60 * time.Millisecond

func liteFCQueue(id int32) common.MessageQueue {
	return common.MessageQueue{Topic: liteFCTopic, BrokerName: "b1", QueueID: id}
}

// liteFCMsg builds one cached message with `body` bytes of body — what the size
// gate adds up — and `store` bytes of stored size, the width of the wire record.
// The two are set independently of each other so a stored-size implementation is
// tellable apart from the body-length one it replaced.
func liteFCMsg(queueID int32, offset int64, body, store int) *common.MessageExt {
	m := common.NewMessageExt()
	m.Topic = liteFCTopic
	m.BrokerName = "b1"
	m.QueueID = queueID
	m.QueueOffset = offset
	m.StoreSize = int32(store)
	m.SetBody([]byte(strings.Repeat("x", body)))
	return m
}

// liteFCMsgs builds n consecutive messages for one queue, offsets 0..n-1, each with
// a `body`-byte body carried by a minimal wire record. One call is one pull round.
func liteFCMsgs(queueID int32, n, body int) []*common.MessageExt {
	out := make([]*common.MessageExt, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, liteFCMsg(queueID, int64(i), body, 64))
	}
	return out
}

// liteFCConsumer is a not-started consumer with every budget off except the one
// under test: an earlier gate would mask the branch being asserted, because the
// whole-buffer budget is judged first, then count, then size, then span.
func liteFCConsumer(t *testing.T, group string) *DefaultLitePullConsumer {
	t.Helper()
	c := MustNewDefaultLitePullConsumer(group)
	c.SetPullThresholdForAll(liteFCOff)
	c.SetPullThresholdForQueue(liteFCOff)
	c.SetPullThresholdSizeForQueue(liteFCOff)
	c.SetConsumeMaxSpan(liteFCOff)
	return c
}

// liteFCStage fills the local buffer through the pull path's own entry point, so
// the per-queue backlog accounting moves exactly the way a fetched batch moves it.
// One call is one pull round.
func liteFCStage(c *DefaultLitePullConsumer, msgs ...*common.MessageExt) {
	c.enqueue(msgs)
}

// liteFCDrain hands the application up to n messages from the front of the buffer,
// the way Poll does, and returns how many left.
func liteFCDrain(c *DefaultLitePullConsumer, n int) int {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()
	return len(c.drainBufferLocked(n))
}

func liteFCTrips(c *DefaultLitePullConsumer) (all, cache, span int64) {
	return c.FlowControlAllTriggered(), c.FlowControlCacheTriggered(), c.FlowControlSpanTriggered()
}

// liteFCStaged wires a lite consumer to the fixture's mock broker owning the given
// queues and pauses them, so the background loop leaves them alone and a test can
// drive one queue's round by hand.
func liteFCStaged(t *testing.T, f *clusterFixture, group, topic string,
	queueIDs ...int32) (*DefaultLitePullConsumer, []common.MessageQueue) {
	t.Helper()
	c := newLiteConsumer(t, f, group)
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	mqs := make([]common.MessageQueue, 0, len(queueIDs))
	for _, id := range queueIDs {
		mqs = append(mqs, common.MessageQueue{Topic: topic, BrokerName: "b1", QueueID: id})
	}
	c.Assign(mqs)
	c.Pause(mqs)
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)
	return c, mqs
}

// liteFCSetBudgets sets the four knobs in one call for a fixture test.
func liteFCSetBudgets(c *DefaultLitePullConsumer, all, count, sizeMiB, span int64) {
	c.SetPullThresholdForAll(all)
	c.SetPullThresholdForQueue(count)
	c.SetPullThresholdSizeForQueue(sizeMiB)
	c.SetConsumeMaxSpan(span)
}

func liteFCPulls(f *clusterFixture, queueID int32) int {
	return len(f.broker.pullsFor(liteFCTopic, queueID))
}

// ---------------------------------------------------------------- the knobs

func TestLiteFlowControlKnobsDefaultToTheSharedBudgets(t *testing.T) {
	if DefaultLitePullThresholdForAll != 10000 || DefaultLitePullThresholdForQueue != 1000 ||
		DefaultLitePullThresholdSizeForQueue != 100 || DefaultLiteConsumeMaxSpan != 2000 {
		t.Errorf("default budgets = (%d, %d, %d MiB, span %d), want (10000, 1000, 100, 2000)",
			DefaultLitePullThresholdForAll, DefaultLitePullThresholdForQueue,
			DefaultLitePullThresholdSizeForQueue, DefaultLiteConsumeMaxSpan)
	}
	// The lite request size is its own default, and the aggregate gate multiplies
	// with it: 10, not the push consumer's 32.
	if DefaultLitePullBatchSize != 10 {
		t.Errorf("DefaultLitePullBatchSize = %d, want 10", DefaultLitePullBatchSize)
	}
	c := MustNewDefaultLitePullConsumer("GID_lite_fc_defaults")
	if got := c.PullBatchSize(); got != DefaultLitePullBatchSize {
		t.Errorf("a fresh consumer's PullBatchSize = %d, want %d", got, DefaultLitePullBatchSize)
	}
	if got := c.PullThresholdForAll(); got != DefaultLitePullThresholdForAll {
		t.Errorf("PullThresholdForAll = %d, want %d", got, DefaultLitePullThresholdForAll)
	}
	if got := c.PullThresholdForQueue(); got != DefaultLitePullThresholdForQueue {
		t.Errorf("PullThresholdForQueue = %d, want %d", got, DefaultLitePullThresholdForQueue)
	}
	if got := c.PullThresholdSizeForQueue(); got != DefaultLitePullThresholdSizeForQueue {
		t.Errorf("PullThresholdSizeForQueue = %d, want %d", got, DefaultLitePullThresholdSizeForQueue)
	}
	if got := c.ConsumeMaxSpan(); got != DefaultLiteConsumeMaxSpan {
		t.Errorf("ConsumeMaxSpan = %d, want %d", got, DefaultLiteConsumeMaxSpan)
	}

	c.SetPullThresholdForAll(7)
	c.SetPullThresholdForQueue(8)
	c.SetPullThresholdSizeForQueue(9)
	c.SetConsumeMaxSpan(10)
	c.SetPullBatchSize(4)
	if got := c.PullBatchSize(); got != 4 {
		t.Errorf("SetPullBatchSize/PullBatchSize round-tripped %d, want 4", got)
	}
	if got := c.PullThresholdForAll(); got != 7 {
		t.Errorf("SetPullThresholdForAll/PullThresholdForAll round-tripped %d, want 7", got)
	}
	if got := c.PullThresholdForQueue(); got != 8 {
		t.Errorf("SetPullThresholdForQueue/PullThresholdForQueue round-tripped %d, want 8", got)
	}
	if got := c.PullThresholdSizeForQueue(); got != 9 {
		t.Errorf("SetPullThresholdSizeForQueue/PullThresholdSizeForQueue round-tripped %d, want 9", got)
	}
	if got := c.ConsumeMaxSpan(); got != 10 {
		t.Errorf("SetConsumeMaxSpan/ConsumeMaxSpan round-tripped %d, want 10", got)
	}

	// Control: at the shipped budgets a small backlog pulls, and an empty queue
	// never reads as a wide span (a min/max over nothing would).
	d := MustNewDefaultLitePullConsumer("GID_lite_fc_default_open")
	liteFCStage(d, liteFCMsgs(0, 1, 100)...)
	if reason, _, _ := d.cacheGateReason(liteFCQueue(0)); reason != "" {
		t.Errorf("one 100-byte message trips %q against the default budgets", reason)
	}
	if reason, _, stats := d.cacheGateReason(liteFCQueue(1)); reason != "" || stats.span != 0 {
		t.Errorf("an empty queue trips %q with span %d, want no gate and span 0", reason, stats.span)
	}
	if all, cache, span := liteFCTrips(d); all != 0 || cache != 0 || span != 0 {
		t.Errorf("counters = (%d, %d, %d), want all zero while nothing is refused", all, cache, span)
	}
}

// The budgets are plain numbers: 0 means "trip on the first cached message", not
// "gate off" and not "clamp to one". Control leg for every gate: an EMPTY queue
// passes a 0 threshold, because all four compare strictly greater.
func TestLiteFlowControlZeroThresholdTripsOnTheFirstCachedMessage(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_zero")
	c.SetPullThresholdForQueue(0)
	if reason, _, stats := c.cacheGateReason(liteFCQueue(0)); reason != "" || stats.count != 0 {
		t.Errorf("an empty queue at count threshold 0 trips %q with count %d, want it to pull", reason, stats.count)
	}
	liteFCStage(c, liteFCMsg(0, 0, 16, 64))
	if reason, threshold, _ := c.cacheGateReason(liteFCQueue(0)); reason != liteGateCount || threshold != 0 {
		t.Errorf("one cached message at count threshold 0 = (%q, threshold %d), want (count, 0) with no clamping",
			reason, threshold)
	}

	size := liteFCConsumer(t, "GID_lite_fc_zero_size")
	size.SetPullThresholdSizeForQueue(0)
	liteFCStage(size, liteFCMsg(0, 0, 900*1024, 8))
	if reason, _, _ := size.cacheGateReason(liteFCQueue(0)); reason != "" {
		t.Errorf("900 KiB under a 0 MiB budget trips %q: a partial MiB must not count", reason)
	}
	liteFCStage(size, liteFCMsg(0, 1, mib, 8))
	if reason, _, _ := size.cacheGateReason(liteFCQueue(0)); reason != liteGateSize {
		t.Errorf("one whole MiB at threshold 0 = %q, want the size gate", reason)
	}

	sp := liteFCConsumer(t, "GID_lite_fc_zero_span")
	sp.SetConsumeMaxSpan(0)
	liteFCStage(sp, liteFCMsg(0, 5, 16, 64))
	if reason, _, _ := sp.cacheGateReason(liteFCQueue(0)); reason != "" {
		t.Errorf("a single cached message spans nothing but trips %q", reason)
	}
	liteFCStage(sp, liteFCMsg(0, 6, 16, 64))
	if reason, _, _ := sp.cacheGateReason(liteFCQueue(0)); reason != liteGateSpan {
		t.Errorf("a span of 1 at maxSpan 0 = %q, want the span gate", reason)
	}

	// The aggregate reads pending rounds, so at threshold 0 the FIRST round still
	// waiting for the application trips it — and an empty buffer does not.
	agg := liteFCConsumer(t, "GID_lite_fc_zero_all")
	agg.SetPullThresholdForAll(0)
	if reason, _, stats := agg.cacheGateReason(liteFCQueue(0)); reason != "" || stats.rounds != 0 || stats.reserved != 0 {
		t.Errorf("an empty buffer at the aggregate threshold 0 trips %q with %+v, want it to pull", reason, stats)
	}
	liteFCStage(agg, liteFCMsgs(0, 1, 16)...)
	if reason, _, stats := agg.cacheGateReason(liteFCQueue(1)); reason != liteGateAll ||
		stats.rounds != 1 || stats.reserved != int64(agg.PullBatchSize()) {
		t.Errorf("one pending round at the whole-buffer threshold 0 = (%q, %+v), want the aggregate gate"+
			" on 1 round x the batch size", reason, stats)
	}
}

// ---------------------------------------------------------------- the count gate

// Unit boundary: a queue sitting exactly on its threshold still pulls; one more
// message refuses the round and re-schedules THIS queue by 50 ms.
func TestLiteFlowControlCountGateIsStrictlyGreater(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_count")
	c.SetPullThresholdForQueue(3)

	liteFCStage(c, liteFCMsgs(0, 3, 16)...)
	if c.flowControlHit(liteFCQueue(0)) {
		t.Errorf("3 cached messages at threshold 3 must still pull: the gate is >, not >=")
	}
	if all, cache, span := liteFCTrips(c); all != 0 || cache != 0 || span != 0 {
		t.Errorf("a permitted round recorded trips (%d, %d, %d), want none", all, cache, span)
	}
	if got := c.PullPauseMillisOf(liteFCQueue(0)); got != 0 {
		t.Errorf("a queue allowed to pull has a pause window of %d ms, want none", got)
	}

	liteFCStage(c, liteFCMsg(0, 3, 16, 16))
	if !c.flowControlHit(liteFCQueue(0)) {
		t.Fatalf("4 cached messages at threshold 3 must refuse the round")
	}
	if got := c.PullPauseMillisOf(liteFCQueue(0)); got != 50 {
		t.Errorf("the refused round re-scheduled the queue for %d ms, want the shared cache pause of 50 ms", got)
	}
	if got := c.FlowControlCacheTriggered(); got != 1 {
		t.Errorf("FlowControlCacheTriggered = %d, want exactly 1 (one refusal is one tick)", got)
	}
	if all, _, span := liteFCTrips(c); all != 0 || span != 0 {
		t.Errorf("the count gate ticked the wrong counters: (%d, _, %d)", all, span)
	}
	// The pause is 50 ms because the port's cache pause is 50 ms — one shared value,
	// not a lite-only invention.
	if pullTimeDelayMillsWhenFlowControl != 50*time.Millisecond {
		t.Errorf("the shared cache pause is %s, and the lite gate must use it", pullTimeDelayMillsWhenFlowControl)
	}
}

// The same boundary where it matters: on the wire. With two messages cached the
// queue is exactly ON its threshold and the request still reaches the mock broker;
// with three the round must build no request at all — and move no cursor or commit.
func TestLitePullCacheGateAtTheCountThresholdStillSendsThePull(t *testing.T) {
	f := newClusterFixture(t, map[string]int{liteFCTopic: 1})
	c, mqs := liteFCStaged(t, f, "GID_lite_fc_wire", liteFCTopic, 0)
	mq := mqs[0]
	f.broker.add(liteFCTopic, 0, "w0", "w1", "w2", "w3", "w4")
	c.SetPullBatchSize(1)
	liteFCSetBudgets(c, liteFCOff, 2, liteFCOff, liteFCOff)

	// Two messages cached = exactly the threshold: the third request still goes out.
	requireLitePull(t, "round 1", c.pullOne(mq))
	requireLitePull(t, "round 2", c.pullOne(mq))
	if got := c.BufferedMessageCount(); got != 2 {
		t.Fatalf("buffered = %d after two rounds, want 2 (one message per round)", got)
	}
	if got := liteFCPulls(f, 0); got != 2 {
		t.Fatalf("the broker saw %d pulls for the queue at its threshold, want 2", got)
	}
	requireLitePull(t, "round 3 at exactly the threshold", c.pullOne(mq))
	if got := liteFCPulls(f, 0); got != 3 {
		t.Errorf("a queue exactly on its threshold sent %d requests in total, want 3", got)
	}

	// One more than the threshold: the round is refused before the request is built.
	before := c.PullCursorOf(mq)
	if c.pullOne(mq) {
		t.Errorf("a queue over its count budget reported progress")
	}
	if got := liteFCPulls(f, 0); got != 3 {
		t.Errorf("a refused round let %d requests reach the broker, want the count frozen at 3", got)
	}
	if got := c.PullPauseMillisOf(mq); got != int64(pullTimeDelayMillsWhenFlowControl/time.Millisecond) {
		t.Errorf("the refused round parked the queue for %d ms, want the cache pause %s",
			got, pullTimeDelayMillsWhenFlowControl)
	}
	if got := c.PullCursorOf(mq); got != before {
		t.Errorf("a refusal moved the pull cursor from %d to %d: nothing may be read ahead", before, got)
	}
	if got := c.BufferedMessageCount(); got != 3 {
		t.Errorf("a refusal left %d messages cached, want the 3 still in the buffer", got)
	}
	if n := f.broker.commitCount(); n != 0 {
		t.Errorf("a refusal committed %d offsets, want 0", n)
	}
	if got := c.FlowControlCacheTriggered(); got != 1 {
		t.Errorf("FlowControlCacheTriggered = %d, want 1", got)
	}
}

// ---------------------------------------------------------------- one queue, never the sum

// Gates 2 and 3 judge this queue alone. A hot queue over its count budget is
// refused while a cold sibling has nothing to refuse; the size gate must isolate
// the same way, reading only its own queue's bytes. Reverting a gate to a sum of
// the queues reddens the sibling legs, and letting the aggregate budget do the
// work reddens the reason checks.
func TestLiteFlowControlGatesJudgeOneQueueNotTheSum(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_per_queue")
	c.SetPullThresholdForQueue(2)
	liteFCStage(c, liteFCMsgs(0, 3, 16)...)

	if reason, _, _ := c.cacheGateReason(liteFCQueue(0)); reason != liteGateCount {
		t.Errorf("hot queue reason = %q, want the count gate", reason)
	}
	if reason, _, stats := c.cacheGateReason(liteFCQueue(1)); reason != "" || stats.count != 0 {
		t.Errorf("cold sibling refused by %q with count %d: a full sibling must not spread", reason, stats.count)
	}

	size := liteFCConsumer(t, "GID_lite_fc_per_queue_size")
	size.SetPullThresholdSizeForQueue(1)
	liteFCStage(size, liteFCMsg(0, 0, 2*mib, 8))
	liteFCStage(size, liteFCMsg(1, 0, 3*mib, 8))
	if reason, _, stats := size.cacheGateReason(liteFCQueue(0)); reason != liteGateSize || stats.bytes != 2*mib {
		t.Errorf("queue 0 = (%q, %d bytes), want the size gate on its own 2 MiB", reason, stats.bytes)
	}
	if reason, _, stats := size.cacheGateReason(liteFCQueue(1)); reason != liteGateSize || stats.bytes != 3*mib {
		t.Errorf("queue 1 = (%q, %d bytes), want the size gate on its own 3 MiB and no sibling bytes",
			reason, stats.bytes)
	}
}

// The wire shape of the same rule: the hot queue's requests stop, the sibling's
// keep arriving.
func TestLitePullCacheGateRefusesOneQueueAndPullsItsSibling(t *testing.T) {
	f := newClusterFixture(t, map[string]int{liteFCTopic: 2})
	c, mqs := liteFCStaged(t, f, "GID_lite_fc_sibling_wire", liteFCTopic, 0, 1)
	f.broker.add(liteFCTopic, 0, "h0", "h1", "h2")
	f.broker.add(liteFCTopic, 1, "s0")
	liteFCSetBudgets(c, liteFCOff, 2, liteFCOff, liteFCOff)

	// Three messages land on queue 0, over its own budget of 2; queue 1 stays cold.
	liteFCStage(c, liteFCMsgs(0, 3, 16)...)
	if c.pullOne(mqs[0]) {
		t.Errorf("the hot queue pulled while over its count budget")
	}
	if got := liteFCPulls(f, 0); got != 0 {
		t.Errorf("the hot queue sent %d requests, want 0", got)
	}
	if !c.pullOne(mqs[1]) {
		t.Fatalf("the cold sibling must still pull")
	}
	if got := liteFCPulls(f, 1); got == 0 {
		t.Errorf("the cold sibling sent no request: one refused queue stopped its sibling too")
	}
	if got := c.FlowControlCacheTriggered(); got != 1 {
		t.Errorf("trips = %d, want 1: only the refused queue records one", got)
	}
}

// ---------------------------------------------------------------- the size gate

// The size budget is spent on the message BODY the client keeps in memory, never on
// the width of the wire record: a large body inside a small store record must refuse
// the round, and the mirror case — a small body inside a large store record — must
// not. Body and store size are set independently here precisely so the two are not
// interchangeable.
func TestLiteFlowControlSizeGateCountsBodyLengthNotStoredSize(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_body_size")
	c.SetPullThresholdSizeForQueue(1)

	liteFCStage(c, liteFCMsg(0, 0, 4*mib, 64))
	if reason, _, stats := c.cacheGateReason(liteFCQueue(0)); reason != liteGateSize || stats.bytes != 4*mib {
		t.Errorf("a 4 MiB body carried by a 64-byte store record = (%q, %d bytes), want the size gate on 4 MiB",
			reason, stats.bytes)
	}

	// Mirror control: a 16-byte body inside a 4 MiB store record. A stored-size
	// implementation refuses this queue; the body-length one keeps it pulling.
	mirror := liteFCConsumer(t, "GID_lite_fc_store_size")
	mirror.SetPullThresholdSizeForQueue(1)
	liteFCStage(mirror, liteFCMsg(0, 0, 16, 4*mib))
	if reason, _, stats := mirror.cacheGateReason(liteFCQueue(0)); reason != "" || stats.bytes != 16 {
		t.Errorf("a 16-byte body inside a 4 MiB store record = (%q, %d bytes), want it to keep pulling",
			reason, stats.bytes)
	}
}

// Over the wire the budget must read the body of the messages that actually landed
// in the buffer. The mock broker pads a record to its store size, so a small body
// arrives as a large cached record — and must still leave the queue pulling. The
// mirror leg then fetches a genuinely large body, which does refuse the next round.
func TestLitePullSizeGateReadsTheBodyOfAFetchedBatch(t *testing.T) {
	f := newClusterFixture(t, map[string]int{liteFCTopic: 1})
	c, mqs := liteFCStaged(t, f, "GID_lite_fc_wire_size", liteFCTopic, 0)
	mq := mqs[0]
	fat := f.broker.add(liteFCTopic, 0, "big0", "thin1", strings.Repeat("y", 2*mib), "after")
	fat[0].StoreSize = int32(2 * mib)
	c.SetPullBatchSize(1)
	liteFCSetBudgets(c, liteFCOff, liteFCOff, 1, liteFCOff)

	// The first round brings back the padded record: 2 MiB on the wire, 4 bytes in
	// memory. The store width must not be the unit the gate reads.
	requireLitePull(t, "the round that fetches the fat record", c.pullOne(mq))
	if got := c.cacheStatsOf(mq).bytes; got != int64(len("big0")) {
		t.Fatalf("the queue's cached bytes = %d, want the body length, not the %d-byte record the wire carried",
			got, 2*mib)
	}
	// Control: the very next round still goes out. A stored-size implementation
	// refuses it here.
	requireLitePull(t, "the round after a fat record with a thin body", c.pullOne(mq))
	if got := c.FlowControlCacheTriggered(); got != 0 {
		t.Fatalf("FlowControlCacheTriggered = %d, want 0: the stored width of a record is not buffer pressure", got)
	}

	// Now a body that really is over the budget: the round that fetches it is
	// allowed, the one after is refused.
	blocked := liteFCPulls(f, 0)
	requireLitePull(t, "the round that fetches the 2 MiB body", c.pullOne(mq))
	if got := c.cacheStatsOf(mq).bytes; got != int64(2*mib)+int64(len("big0"))+int64(len("thin1")) {
		t.Fatalf("the queue's cached bytes = %d, want the 2 MiB body plus the 11 bytes already held", got)
	}
	if c.pullOne(mq) {
		t.Errorf("a queue holding over 2 MiB of body under a 1 MiB budget reported progress")
	}
	if got := liteFCPulls(f, 0); got != blocked+1 {
		t.Errorf("requests to the broker = %d, want the refused round to add nothing to the %d already sent",
			got, blocked+1)
	}
	if got := c.FlowControlCacheTriggered(); got != 1 {
		t.Errorf("FlowControlCacheTriggered = %d, want 1", got)
	}

	// Control: rewind past the large body so the buffer no longer holds it — the
	// gate is a reading of the backlog, not a latch.
	c.Seek(mq, 3)
	if got := c.cacheStatsOf(mq).bytes; got != 0 {
		t.Fatalf("after Seek(3) the queue still accounts %d bytes, want the pruned body gone", got)
	}
	requireLitePull(t, "the round after the large body was pruned", c.pullOne(mq))
}

// The size budget divides by whole MiB: under the threshold pulls, exactly at it
// pulls, and one byte over the threshold STILL pulls because the division is an
// integer one (a float ratio would refuse there). Only a full extra MiB refuses.
func TestLiteFlowControlSizeGateDividesWholeMiB(t *testing.T) {
	cases := []struct {
		label string
		bytes int
		want  string
	}{
		{"below", 900 * 1024, ""},
		{"exactly", mib, ""},
		{"overByAByte", mib + 1, ""},
		{"wholeExtraMiB", 2 * mib, liteGateSize},
	}
	for i, tc := range cases {
		q := liteFCConsumer(t, "GID_lite_fc_mib_division")
		q.SetPullThresholdSizeForQueue(1)
		liteFCStage(q, liteFCMsg(0, 0, tc.bytes, 8))
		reason, threshold, stats := q.cacheGateReason(liteFCQueue(0))
		if reason != tc.want {
			t.Errorf("case %d (%s): reason = %q, want %q (sizeMiB=%d, threshold=%d)",
				i, tc.label, reason, tc.want, stats.sizeMiB(), threshold)
		}
	}
	// An unseen queue has no backlog at all, so it cannot divide into a refusal.
	if got := liteFCConsumer(t, "GID_lite_fc_mib_none").cacheStatsOf(liteFCQueue(7)).sizeMiB(); got != 0 {
		t.Errorf("an unaccounted queue reports %d MiB of backlog", got)
	}
}

// ---------------------------------------------------------------- the span gate

// Span is this queue's highest cached offset minus its lowest, compared strictly
// greater than consumeMaxSpan, and measured over THIS queue's offsets only.
func TestLiteFlowControlSpanGateIsStrictlyGreaterAndPerQueue(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_span")
	c.SetConsumeMaxSpan(10)
	liteFCStage(c, liteFCMsg(0, 0, 16, 16), liteFCMsg(0, 10, 16, 16))
	if reason, _, stats := c.cacheGateReason(liteFCQueue(0)); reason != "" || stats.span != 10 {
		t.Errorf("a span of exactly 10 at maxSpan 10 = (%q, span %d), want it to pull", reason, stats.span)
	}
	liteFCStage(c, liteFCMsg(0, 11, 16, 16))
	if reason, threshold, stats := c.cacheGateReason(liteFCQueue(0)); reason != liteGateSpan ||
		threshold != 10 || stats.span != 11 || stats.minOffset != 0 || stats.maxOffset != 11 {
		t.Errorf("span 11 at maxSpan 10 = (%q, threshold %d, %+v), want the span gate", reason, threshold, stats)
	}

	// Out-of-order caching must still read max-min, not first-last.
	back := liteFCConsumer(t, "GID_lite_fc_span_backwards")
	back.SetConsumeMaxSpan(10)
	liteFCStage(back, liteFCMsg(0, 50, 16, 16), liteFCMsg(0, 5, 16, 16), liteFCMsg(0, 7, 16, 16))
	if reason, _, stats := back.cacheGateReason(liteFCQueue(0)); reason != liteGateSpan ||
		stats.minOffset != 5 || stats.maxOffset != 50 || stats.span != 45 || stats.count != 3 {
		t.Errorf("an out-of-order backlog = (%q, %+v), want span 45 over this queue's 3 messages", reason, stats)
	}

	// Control: a sibling's far-away offsets must not widen this queue's span.
	sib := liteFCConsumer(t, "GID_lite_fc_span_sibling")
	sib.SetConsumeMaxSpan(10)
	liteFCStage(sib, liteFCMsg(0, 0, 16, 16), liteFCMsg(0, 1, 16, 16), liteFCMsg(1, 5000, 16, 16))
	if reason, _, stats := sib.cacheGateReason(liteFCQueue(0)); reason != "" || stats.span != 1 {
		t.Errorf("queue 0 measured its sibling's offsets: (%q, span %d)", reason, stats.span)
	}
	if reason, _, stats := sib.cacheGateReason(liteFCQueue(1)); reason != "" || stats.count != 1 || stats.span != 0 {
		t.Errorf("the sibling with one far message = (%q, %+v), want no gate and span 0", reason, stats)
	}
}

// ---------------------------------------------------------------- the aggregate gate

// The whole-buffer budget is a separate budget, judged first, and it is stated in
// PULL CAPACITY: every pull round whose messages the application has not polled yet
// charges a whole pullBatchSize. Three small rounds therefore reserve three batches
// even though each came back with one message, and that is what refuses every queue
// here — while every queue is far inside its own count budget. Control: the same
// staging with one more message of budget refuses nothing anywhere.
func TestLiteFlowControlAggregateGateIsPendingRoundsTimesBatchSize(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_aggregate")
	c.SetPullBatchSize(4)
	c.SetPullThresholdForAll(11)
	c.SetPullThresholdForQueue(100)
	for id := int32(0); id < 3; id++ {
		liteFCStage(c, liteFCMsgs(id, 1, 16)...)
	}
	if got := c.BufferedMessageCount(); got != 3 {
		t.Fatalf("the buffer holds %d messages, want 3: the aggregate must NOT be reading them", got)
	}
	for id := int32(0); id < 3; id++ {
		reason, threshold, stats := c.cacheGateReason(liteFCQueue(id))
		if reason != liteGateAll {
			t.Errorf("queue %d of 3 tiny rounds refused by %q (its own count %d of budget 100):"+
				" want the whole-buffer gate", id, reason, stats.count)
			continue
		}
		if threshold != 11 || stats.batch != 4 || stats.rounds != 3 || stats.reserved != 12 || stats.count != 1 {
			t.Errorf("queue %d aggregate verdict reads (threshold %d, batch %d, rounds %d, reserved %d, own count %d),"+
				" want (11, 4, 3, 12, 1)", id, threshold, stats.batch, stats.rounds, stats.reserved, stats.count)
		}
	}
	for id := int32(0); id < 3; id++ {
		if !c.flowControlHit(liteFCQueue(id)) {
			t.Errorf("queue %d must be refused by the aggregate budget", id)
		}
	}
	if got := c.FlowControlAllTriggered(); got != 3 {
		t.Errorf("FlowControlAllTriggered = %d, want 3 (one tick per refused queue)", got)
	}
	if _, cache, span := liteFCTrips(c); cache != 0 || span != 0 {
		t.Errorf("the aggregate gate ticked the per-queue counters (cache %d, span %d)", cache, span)
	}

	// Exactly three rounds of four messages fit a budget of 12: reaching it is not
	// exceeding it, and the same staging does not refuse anywhere.
	ok := liteFCConsumer(t, "GID_lite_fc_aggregate_control")
	ok.SetPullBatchSize(4)
	ok.SetPullThresholdForAll(12)
	ok.SetPullThresholdForQueue(100)
	for id := int32(0); id < 3; id++ {
		liteFCStage(ok, liteFCMsgs(id, 4, 16)...)
	}
	for id := int32(0); id < 3; id++ {
		if reason, _, stats := ok.cacheGateReason(liteFCQueue(id)); reason != "" || stats.reserved != 12 {
			t.Errorf("12 units of reserved capacity at aggregate threshold 12: queue %d refused by %q (%+v)",
				id, reason, stats)
		}
	}
	if all, cache, span := liteFCTrips(ok); all != 0 || cache != 0 || span != 0 {
		t.Errorf("the allowed case ticked the counters (%d, %d, %d)", all, cache, span)
	}
}

// A round is budgeted a whole batch even when it came back with a single message,
// and the batch knob is the multiplier: the identical buffer refuses at one setting
// and pulls at another. Reverting the product to "count the cached messages" reddens
// the first leg; reading a stale batch size reddens the second.
func TestLiteFlowControlAggregateChargesAFullBatchPerPendingRound(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_aggregate_batch")
	c.SetPullBatchSize(10)
	c.SetPullThresholdForAll(9)
	c.SetPullThresholdForQueue(1000)
	liteFCStage(c, liteFCMsgs(0, 1, 16)...)
	if reason, _, stats := c.cacheGateReason(liteFCQueue(0)); reason != liteGateAll || stats.reserved != 10 {
		t.Errorf("one pending round of a batch of 10 = (%q, reserved %d), want the aggregate gate on 10",
			reason, stats.reserved)
	}

	// The same single cached message with the request size asking for one: the
	// reserved capacity follows the knob, so the round is allowed again.
	c.SetPullBatchSize(1)
	if reason, _, stats := c.cacheGateReason(liteFCQueue(0)); reason != "" || stats.reserved != 1 {
		t.Errorf("the same round at batch 1 = (%q, reserved %d), want no gate and 1 unit reserved",
			reason, stats.reserved)
	}

	// The round retires once its last message leaves the buffer: nothing waits and
	// nothing is reserved.
	c.SetPullBatchSize(10)
	if drained := len(c.tryDrain()); drained != 1 {
		t.Fatalf("tryDrain returned %d messages, want 1", drained)
	}
	if reason, _, stats := c.cacheGateReason(liteFCQueue(0)); reason != "" || stats.rounds != 0 || stats.reserved != 0 {
		t.Errorf("after the round was polled away the gate reads (%q, %+v), want nothing reserved", reason, stats)
	}
}

// Revoking a queue retires its pending rounds from the whole-buffer budget, exactly
// as the cached pull requests of that queue leave it: the messages it left behind
// stay pollable, but they no longer reserve capacity or refuse a round.
func TestLiteFlowControlRevokedQueueLeavesTheAggregateBudget(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_aggregate_revoke")
	c.SetPullBatchSize(10)
	c.SetPullThresholdForAll(9)
	c.SetPullThresholdForQueue(1000)
	orphan := liteFCQueue(0)
	liteFCStage(c, liteFCMsgs(0, 2, 16)...)
	if !c.flowControlHit(orphan) {
		t.Fatalf("one pending round at aggregate threshold 9 was not refused")
	}
	c.dropQueueCache(orphan)

	if reason, _, stats := c.cacheGateReason(liteFCQueue(1)); reason != "" || stats.rounds != 0 || stats.reserved != 0 {
		t.Errorf("a revoked queue's rounds left the sibling's aggregate reading (%q, %+v), want nothing reserved",
			reason, stats)
	}
	if stats := c.cacheStatsOf(orphan); stats.count != 0 {
		t.Errorf("the revoked queue's own accumulator survived with %d messages", stats.count)
	}
	if c.flowControlHit(orphan) {
		t.Errorf("the revoked queue was still refused: its stale rounds weigh on the whole-buffer budget")
	}
	if got := c.BufferedMessageCount(); got != 2 {
		t.Errorf("retiring the queue's rounds left %d messages in the buffer, want the 2 kept for the application", got)
	}
	if all, cache, span := liteFCTrips(c); all != 1 || cache != 0 || span != 0 {
		t.Errorf("the round after the revoke ticked (%d, %d, %d), want only the refusal before it", all, cache, span)
	}
}

// ---------------------------------------------------------------- counters and cadence

// The aggregate budget is stated in messages while the gate counts rounds, so the
// WARN line has to translate it back: how many rounds still fit is the threshold
// divided by the request size, rounded UP (a part-round still fits inside the
// budget). A floor division would under-report the budget the operator set.
func TestLiteFlowControlAggregateWarnReportsTheRoundsThatStillFit(t *testing.T) {
	rounds := liteCacheStats{batch: 10}
	for _, tc := range []struct {
		all  int64
		want int64
	}{
		{10000, 1000},
		{10001, 1001},
		{9, 1},
		{10, 1},
		{0, 0},
	} {
		if got := rounds.roundsFitInAll(tc.all); got != tc.want {
			t.Errorf("%d messages of aggregate budget at a batch of 10 reads as %d rounds, want %d",
				tc.all, got, tc.want)
		}
	}
	// A batch that never divides the budget is not a divisor the line may use.
	if got := (liteCacheStats{batch: 0}).roundsFitInAll(5); got != 5 {
		t.Errorf("with no request size the line reported %d rounds, want the budget itself", got)
	}
}

// One refusal per gate ticks exactly that gate's counter and no other, and the two
// "this queue holds too much" gates share one number.
func TestLiteFlowControlCountersSplitByGate(t *testing.T) {
	count := liteFCConsumer(t, "GID_lite_fc_counter_count")
	count.SetPullThresholdForQueue(0)
	liteFCStage(count, liteFCMsgs(0, 1, 16)...)
	if !count.flowControlHit(liteFCQueue(0)) {
		t.Fatalf("the count gate must refuse")
	}
	if all, cache, span := liteFCTrips(count); all != 0 || cache != 1 || span != 0 {
		t.Errorf("a count-gate refusal ticked (%d, %d, %d), want (0, 1, 0)", all, cache, span)
	}

	size := liteFCConsumer(t, "GID_lite_fc_counter_size")
	size.SetPullThresholdSizeForQueue(0)
	liteFCStage(size, liteFCMsg(0, 0, mib, 8))
	if !size.flowControlHit(liteFCQueue(0)) {
		t.Fatalf("the size gate must refuse")
	}
	if all, cache, span := liteFCTrips(size); all != 0 || cache != 1 || span != 0 {
		t.Errorf("a size-gate refusal ticked (%d, %d, %d), want (0, 1, 0): count and size share one counter",
			all, cache, span)
	}

	span := liteFCConsumer(t, "GID_lite_fc_counter_span")
	span.SetConsumeMaxSpan(0)
	liteFCStage(span, liteFCMsg(0, 0, 16, 16), liteFCMsg(0, 1, 16, 16))
	if !span.flowControlHit(liteFCQueue(0)) {
		t.Fatalf("the span gate must refuse")
	}
	if all, cache, sp := liteFCTrips(span); all != 0 || cache != 0 || sp != 1 {
		t.Errorf("a span-gate refusal ticked (%d, %d, %d), want (0, 0, 1)", all, cache, sp)
	}

	agg := liteFCConsumer(t, "GID_lite_fc_counter_all")
	agg.SetPullThresholdForAll(0)
	liteFCStage(agg, liteFCMsgs(0, 1, 16)...)
	if !agg.flowControlHit(liteFCQueue(0)) {
		t.Fatalf("the aggregate gate must refuse")
	}
	if all, cache, sp := liteFCTrips(agg); all != 1 || cache != 0 || sp != 0 {
		t.Errorf("an aggregate refusal ticked (%d, %d, %d), want (1, 0, 0)", all, cache, sp)
	}
}

// The WARN cadence has to be driven off the counter the getter exposes: a helper
// that warned correctly in isolation but ticked a different variable would pass
// the pure cadence test and still warn every round here.
func TestLiteFlowControlWarnCadenceUsesTheExposedCounter(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_cadence")
	c.SetPullThresholdForQueue(0)
	liteFCStage(c, liteFCMsgs(0, 1, 16)...)
	mq := liteFCQueue(0)

	if !c.flowControlHit(mq) {
		t.Fatalf("trip 1 must refuse the round")
	}
	if got := c.FlowControlCacheTriggered(); got != 1 || !isFlowControlWarnTrip(got) {
		t.Errorf("trip %d (getter says %d) must be the first WARN of the run", got, got)
	}
	for n := int64(2); n <= 1000; n++ {
		if !c.flowControlHit(mq) {
			t.Fatalf("trip %d must refuse the round", n)
		}
	}
	if got := c.FlowControlCacheTriggered(); got != 1000 || isFlowControlWarnTrip(got) {
		t.Errorf("after 1000 trips (getter says %d) the cadence must stay debug-only", got)
	}
	if !c.flowControlHit(mq) {
		t.Fatalf("trip 1001 must refuse the round")
	}
	if got := c.FlowControlCacheTriggered(); got != 1001 || !isFlowControlWarnTrip(got) {
		t.Errorf("after 1001 trips (getter says %d) the cadence must warn", got)
	}
	if all, _, span := liteFCTrips(c); all != 0 || span != 0 {
		t.Errorf("1001 count-gate trips leaked into the other counters (%d, _, %d)", all, span)
	}
}

// ---------------------------------------------------------------- accumulator lifetime

// A queue this consumer no longer owns loses its accumulator: a leftover count for
// a queue nobody holds must never refuse a pull, and its pause window goes with
// it. The messages it left in the buffer stay for the application.
func TestLiteFlowControlRevokedQueueLosesItsAccumulator(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_revoke")
	c.SetPullThresholdForQueue(2)
	mq := liteFCQueue(0)
	c.Assign([]common.MessageQueue{mq, liteFCQueue(1)})
	liteFCStage(c, liteFCMsgs(0, 3, 16)...)
	if !c.flowControlHit(mq) {
		t.Fatalf("the over-budget queue must be refused before it is revoked")
	}
	if got := c.PullPauseMillisOf(mq); got == 0 {
		t.Fatalf("the refusal left no pause window")
	}

	// Hand the assignment to a set that drops this queue: assign-mode shrink and
	// rebalance revoke share one retire path.
	c.Assign([]common.MessageQueue{liteFCQueue(1)})
	if c.flowControlHit(mq) {
		t.Errorf("a revoked queue still refused a pull: its stale accumulator survived the revoke")
	}
	if got := c.PullPauseMillisOf(mq); got != 0 {
		t.Errorf("the revoked queue kept its pause window of %d ms", got)
	}
	if got := c.BufferedMessageCount(); got != 3 {
		t.Errorf("revoking a queue left %d messages in the buffer, want all 3 kept for the application", got)
	}
	if got := c.FlowControlCacheTriggered(); got != 1 {
		t.Errorf("the allowed round after the revoke ticked the counter to %d, want it frozen at 1", got)
	}
}

// Seek rewinds a cursor and prunes the queue's cached messages, so the accumulator
// must follow the buffer — otherwise the gates would refuse on a backlog that is
// gone, or miss one that came back below the kept offsets. The pending rounds follow
// too: pruning takes the queue's OLDEST messages out of its OLDEST rounds, so a
// round only stops counting once its last message is gone.
func TestLitePullSeekKeepsTheBacklogAccountingHonest(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_seek")
	liteFCSetBudgets(c, liteFCOff, liteFCOff, liteFCOff, 4)
	mq := liteFCQueue(0)
	liteFCStage(c, liteFCMsgs(0, 6, 16)...)
	if reason, _, stats := c.cacheGateReason(mq); reason != liteGateSpan || stats.span != 5 {
		t.Fatalf("offsets 0..5 at maxSpan 4 read as (%q, span %d): the span gate should already judge them",
			reason, stats.span)
	}
	if got := c.cacheStatsOf(mq); got.rounds != 1 {
		t.Fatalf("one round of 6 cached messages reads %d pending rounds, want 1", got.rounds)
	}

	c.Seek(mq, 4)
	stats := c.cacheStatsOf(mq)
	if stats.count != 2 || stats.minOffset != 4 || stats.maxOffset != 5 || stats.span != 1 {
		t.Errorf("after Seek(4) the queue reads %+v, want the 2 messages spanning 4..5", stats)
	}
	// The 2 pruned messages came out of the round that brought all 6, and 4 of that
	// round are still pollable: the round is not retired yet.
	if stats.rounds != 1 {
		t.Errorf("after Seek(4) the queue reads %d pending rounds, want 1 (4 of that round still cached)", stats.rounds)
	}
	if got := c.BufferedMessageCount(); got != 2 {
		t.Errorf("the buffer holds %d messages, want the 2 the queue kept", got)
	}
	if reason, _, _ := c.cacheGateReason(mq); reason != "" {
		t.Errorf("the pruned backlog still refuses the round with %q", reason)
	}

	// Rewinding below everything keeps the accounting untouched, and a re-fetched
	// lower offset must extend the span rather than drive it negative.
	c.Seek(mq, 0)
	if got := c.cacheStatsOf(mq); got.count != 2 || got.span != 1 || got.rounds != 1 {
		t.Errorf("Seek back to 0 reads %+v, want the 2 cached messages and their round untouched", got)
	}
	liteFCStage(c, liteFCMsg(0, 0, 16, 64))
	if got := c.cacheStatsOf(mq); got.minOffset != 0 || got.maxOffset != 5 || got.span != 5 || got.count != 3 {
		t.Errorf("a re-fetched lower offset reads %+v, want 3 messages spanning 0..5", got)
	}
	if got := c.cacheStatsOf(mq); got.rounds != 2 {
		t.Errorf("the re-fetched message is a new round: pending rounds = %d, want 2", got.rounds)
	}
	if reason, _, stats := c.cacheGateReason(mq); reason != liteGateSpan {
		t.Errorf("the span grew back to %d at maxSpan 4 but the gate read %q, want the span gate",
			stats.span, reason)
	}
}

// Polling messages away must retire them from their queue's accumulator too, or
// the gates would keep refusing a backlog the application already took. A round
// retires only when its LAST message leaves, so a partially polled buffer still
// charges the aggregate.
func TestLitePullPollRetiresTheQueueAccounting(t *testing.T) {
	c := liteFCConsumer(t, "GID_lite_fc_drain")
	c.SetPullBatchSize(10)
	c.SetPullThresholdForQueue(3)
	liteFCStage(c, liteFCMsgs(0, 3, 16)...)
	if reason, _, _ := c.cacheGateReason(liteFCQueue(0)); reason != "" {
		t.Fatalf("3 messages at budget 3 = %q, want it to pull", reason)
	}
	liteFCStage(c, liteFCMsg(1, 0, 16, 64), liteFCMsg(0, 3, 16, 64))
	if reason, _, _ := c.cacheGateReason(liteFCQueue(0)); reason != liteGateCount {
		t.Errorf("4 messages for queue 0 = %q, want the count gate", reason)
	}
	if reason, _, _ := c.cacheGateReason(liteFCQueue(1)); reason != "" {
		t.Errorf("the sibling with 1 message refused by %q", reason)
	}

	// The first round of queue 0 is its 3 oldest buffered messages, then the
	// sibling's single message: polling those four retires two whole rounds and
	// leaves queue 0's second round — one message — still charged. `rounds` is the
	// count across every queue, so it is read as the buffer's total, not per queue.
	if drained := liteFCDrain(c, 4); drained != 4 {
		t.Fatalf("the partial poll took %d messages, want 4", drained)
	}
	bufferWide := c.cacheStatsOf(liteFCQueue(1))
	if bufferWide.rounds != 1 || bufferWide.reserved != 10 {
		t.Errorf("the buffer-wide rounds read %+v, want the one surviving round charging a whole batch of 10",
			bufferWide)
	}
	if stats := c.cacheStatsOf(liteFCQueue(0)); stats.count != 1 {
		t.Errorf("queue 0 after the partial poll holds %d messages, want its newest round still cached", stats.count)
	}
	if stats := c.cacheStatsOf(liteFCQueue(1)); stats.count != 0 || stats.bytes != 0 {
		t.Errorf("the sibling whose only message was polled reads %+v, want its own backlog retired", stats)
	}

	if drained := len(c.tryDrain()); drained != 1 {
		t.Fatalf("the final poll took %d messages, want the 1 left", drained)
	}
	for _, id := range []int32{0, 1} {
		if stats := c.cacheStatsOf(liteFCQueue(id)); stats.count != 0 || stats.bytes != 0 || stats.span != 0 ||
			stats.rounds != 0 {
			t.Errorf("queue %d still reads %+v after the buffer was polled dry", id, stats)
		}
	}
	if got := c.BufferedMessageCount(); got != 0 {
		t.Errorf("buffer length = %d after draining, want 0", got)
	}
	if reason, _, stats := c.cacheGateReason(liteFCQueue(0)); reason != "" || stats.reserved != 0 {
		t.Errorf("an emptied buffer still refuses with %q (reserved %d): its accounting drifted from the buffer",
			reason, stats.reserved)
	}
}

// ---------------------------------------------------------------- broker throttle 215

// The broker's own throttle is not an error on this path: a lite pull answered
// FLOW_CONTROL(215) re-schedules THIS queue 20 ms later. Any other failure keeps
// the lite path's existing handling — no per-queue pause, and the generic pull
// wait stays where it belongs. Both legs in one test, so a leaked mapping shows
// immediately.
func TestLitePullBrokerFlowControlParksTheQueueForTwentyMillis(t *testing.T) {
	f := newClusterFixture(t, map[string]int{liteFCTopic: 1})
	c, mqs := liteFCStaged(t, f, "GID_lite_fc_215", liteFCTopic, 0)
	mq := mqs[0]
	f.broker.add(liteFCTopic, 0, "t0")

	f.broker.pullCode = remoting.RespFlowControl
	if c.pullOne(mq) {
		t.Errorf("a 215 round must not report progress")
	}
	if got := c.PullPauseMillisOf(mq); got != int64(pullTimeDelayMillsWhenBrokerFlow/time.Millisecond) {
		t.Errorf("a 215 answer parked the queue for %d ms, want the broker throttle wait %s",
			got, pullTimeDelayMillsWhenBrokerFlow)
	}
	if got := liteFCPulls(f, 0); got != 1 {
		t.Errorf("requests to the broker = %d, want the 215 answer to come from one real request", got)
	}
	if got := c.BufferedMessageCount(); got != 0 {
		t.Errorf("a 215 round cached %d messages", got)
	}
	if all, cache, span := liteFCTrips(c); all != 0 || cache != 0 || span != 0 {
		t.Errorf("the broker's throttle ticked the local cache counters (%d, %d, %d)", all, cache, span)
	}

	// Control leg: the same forced-answer shape with a failure that is NOT 215
	// must leave no pause window behind (the 20 ms one is allowed to run out first).
	time.Sleep(liteFCParkExpiry)
	f.broker.pullCode = remoting.RespSystemBusy
	if c.pullOne(mq) {
		t.Errorf("a SYSTEM_BUSY round must not report progress either")
	}
	if got := c.PullPauseMillisOf(mq); got != 0 {
		t.Errorf("SYSTEM_BUSY parked the queue for %d ms, want the lite path's normal failure handling", got)
	}
	const genericWait = time.Duration(defaultFailedRoundDelayMillis) * time.Millisecond
	if got := pullRetryDelay(common.BrokerError(remoting.RespSystemBusy, "busy"), genericWait); got != genericWait {
		t.Errorf("a busy answer maps to %s, want the generic pull wait %s", got, genericWait)
	}
	if got := pullRetryDelay(common.BrokerError(remoting.RespFlowControl, "throttled"), genericWait); got != pullTimeDelayMillsWhenBrokerFlow {
		t.Errorf("the shared 215 recognition mapped it to %s, want %s", got, pullTimeDelayMillsWhenBrokerFlow)
	}
	f.broker.pullCode = 0
}

// The pause is a window, not a latch: once it runs out the queue pulls again —
// through the background loop, with nothing hand-driven.
func TestLitePullLoopSitsOutTheBrokerThrottleAndPullsAgain(t *testing.T) {
	f := newClusterFixture(t, map[string]int{liteFCTopic: 1})
	c := newLiteConsumer(t, f, "GID_lite_fc_215_loop")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	c.SetPullBatchSize(1)
	mq := common.MessageQueue{Topic: liteFCTopic, BrokerName: "b1", QueueID: 0}
	c.Assign([]common.MessageQueue{mq})
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	f.broker.pullCode = remoting.RespFlowControl
	waitForBudget(t, 5*time.Second, func() bool { return liteFCPulls(f, 0) >= 3 })
	f.broker.pullCode = 0
	f.broker.add(liteFCTopic, 0, "after215")
	got := pollAll(t, c, 1, 5*time.Second)
	if got[0] != "after215" {
		t.Errorf("delivered %q after the throttle ended, want the message the queue fetched next", got[0])
	}
}

// ---------------------------------------------------------------- the running loop

// The pause belongs to one queue: while the hot queue is refused every round its
// sibling must keep reaching the broker on the loop's own schedule. Asserted as
// request counts, so a loop that slept the 50 ms out inline (delaying everyone) or
// that never resumed reddens a leg.
func TestLitePullCacheGateParksOneQueueAndNotItsSibling(t *testing.T) {
	f := newClusterFixture(t, map[string]int{liteFCTopic: 2})
	c := newLiteConsumer(t, f, "GID_lite_fc_loop_park")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	c.SetPullBatchSize(1)
	hot := common.MessageQueue{Topic: liteFCTopic, BrokerName: "b1", QueueID: 0}
	cold := common.MessageQueue{Topic: liteFCTopic, BrokerName: "b1", QueueID: 1}
	c.Assign([]common.MessageQueue{hot, cold})
	liteFCSetBudgets(c, liteFCOff, 2, liteFCOff, liteFCOff)
	f.broker.add(liteFCTopic, 0, "p0", "p1", "p2", "p3", "p4", "p5")
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	// The hot queue fills past its budget of 2 and starts being refused; the cold
	// queue holds nothing and has no reason to stop.
	waitForBudget(t, 5*time.Second, func() bool {
		return c.FlowControlCacheTriggered() > 0 && c.BufferedMessageCount() >= 3
	})
	hotBefore := liteFCPulls(f, 0)
	coldBefore := liteFCPulls(f, 1)

	time.Sleep(400 * time.Millisecond)
	if grew := liteFCPulls(f, 1) - coldBefore; grew < 2 {
		t.Errorf("the cold sibling only got %d rounds away while its neighbour was parked:"+
			" a per-queue pause must not delay siblings", grew)
	}
	if grew := liteFCPulls(f, 0) - hotBefore; grew != 0 {
		t.Errorf("the parked queue reached the broker %d more times while its backlog stayed over budget", grew)
	}
	if got := c.FlowControlCacheTriggered(); got < 2 {
		t.Errorf("trips = %d after 400ms of a refused queue, want a refusal per round", got)
	}
	// Control: the refused round is the ONLY thing that stopped queue 0 — poll the
	// buffer away and it reaches the broker again inside the same loop.
	for c.BufferedMessageCount() > 0 {
		c.PollWithTimeout(0)
	}
	waitForBudget(t, 5*time.Second, func() bool { return liteFCPulls(f, 0) > hotBefore })
}

// The gates throttle a pull, they do not stop the consumer: once the application
// polls the buffer dry the very next round must pull again — hand-driven first, so
// no refusal can be latched in the accounting, then through the background loop.
func TestLitePullCacheGateRecoversOnceTheApplicationPolls(t *testing.T) {
	f := newClusterFixture(t, map[string]int{liteFCTopic: 1})
	c, mqs := liteFCStaged(t, f, "GID_lite_fc_recovery", liteFCTopic, 0)
	mq := mqs[0]
	f.broker.add(liteFCTopic, 0, "r0", "r1", "r2", "r3", "r4", "r5")
	c.SetPullBatchSize(1)
	liteFCSetBudgets(c, liteFCOff, 2, liteFCOff, liteFCOff)

	requireLitePull(t, "round 1", c.pullOne(mq))
	requireLitePull(t, "round 2", c.pullOne(mq))
	requireLitePull(t, "round 3 at exactly the threshold", c.pullOne(mq))
	blocked := liteFCPulls(f, 0)
	if c.pullOne(mq) {
		t.Fatalf("3 cached messages at budget 2 must be refused")
	}
	if got := liteFCPulls(f, 0); got != blocked {
		t.Fatalf("the refused round reached the broker (%d requests)", got)
	}

	for c.BufferedMessageCount() > 0 {
		if drained, err := c.PollWithTimeout(0); err != nil || drained == nil {
			t.Fatal("Poll found nothing although the buffer held messages")
		}
	}
	if stats := c.cacheStatsOf(mq); stats.count != 0 || stats.rounds != 0 || stats.reserved != 0 {
		t.Fatalf("after the drain the queue reads %+v, want an empty backlog and no reserved capacity", stats)
	}
	if got := c.PullPauseMillisOf(mq); got != 0 {
		t.Logf("the 50 ms window is still open (%d ms) - the round below must be hand-driven", got)
	}
	requireLitePull(t, "the round after the drain", c.pullOne(mq))
	if got := liteFCPulls(f, 0); got != blocked+1 {
		t.Errorf("requests to the broker = %d, want exactly one more after the drain", got)
	}

	// Same recovery through the loop: unpause it and let it drive itself, with
	// nothing hand-held — a parked queue must resume on its own deadline.
	c.Resume(mqs)
	after := liteFCPulls(f, 0)
	waitForBudget(t, 5*time.Second, func() bool { return liteFCPulls(f, 0) > after })
}

// A queue parked on a gate must still stop promptly: the pause is a deadline the
// loop wakes on rather than a wait the shutdown has to sit out, and after Shutdown
// no further request may reach the broker.
func TestLitePullShutdownEscapesAParkedQueue(t *testing.T) {
	f := newClusterFixture(t, map[string]int{liteFCTopic: 1})
	c := newLiteConsumer(t, f, "GID_lite_fc_shutdown")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	c.SetPullBatchSize(1)
	// A long idle interval and a queue refused every round: whenever the loop is
	// waiting, it is waiting on a parked queue's window.
	c.SetPullIntervalMillis(3000)
	mq := common.MessageQueue{Topic: liteFCTopic, BrokerName: "b1", QueueID: 0}
	c.Assign([]common.MessageQueue{mq})
	liteFCSetBudgets(c, liteFCOff, 1, liteFCOff, liteFCOff)
	f.broker.add(liteFCTopic, 0, "s0", "s1", "s2", "s3")
	requireNoError(t, "start", c.Start())

	waitForBudget(t, 5*time.Second, func() bool { return c.FlowControlCacheTriggered() > 0 })
	waitForBudget(t, time.Second, func() bool { return c.PullPauseMillisOf(mq) > 0 })

	done := make(chan struct{})
	go func() {
		c.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Shutdown did not return within 2s while a queue sat on a gate")
	}

	pulls := liteFCPulls(f, 0)
	time.Sleep(300 * time.Millisecond)
	if grew := liteFCPulls(f, 0) - pulls; grew != 0 {
		t.Errorf("the pull loop reached the broker %d times after Shutdown", grew)
	}
}

// ---------------------------------------------------------------- helpers

func requireLitePull(t *testing.T, what string, got bool) {
	t.Helper()
	if !got {
		t.Fatalf("%s: the round should have entered the buffer", what)
	}
}
