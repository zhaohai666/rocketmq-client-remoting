// The cache gates are the only thing that keeps a slow listener from turning into an
// unbounded buffer, so their arithmetic is pinned here without a broker: the hit DIRECTION
// (strictly greater, so a threshold reads as "how many may be cached"), the UNITS (MiB, not
// bytes; offset span, not count), the per-gate switches and the WARN cadence (once per 1000
// trips) are each asserted with a paired control, because every one of them fails in the
// silent direction when judged wrong.
//
// The POP queue's gate joins the same assertions at the bottom of the file: it judges the
// ACK debt of ONE queue and reports on the pull path's counter, so the two modes together
// give one throttling number per client.
package client

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

const fcTopic = "GoFlowControlUnitTopic"

const mib = 1024 * 1024

func fcQueue(id int32) common.MessageQueue {
	return common.NewMessageQueue(fcTopic, "b1", id)
}

// fcMsg builds one buffered message whose body is exactly bodyBytes long.
func fcMsg(offset int64, bodyBytes int) *common.MessageExt {
	m := common.NewMessageExt()
	m.Topic = fcTopic
	m.BrokerName = "b1"
	m.QueueID = 0
	m.QueueOffset = offset
	m.SetBody([]byte(strings.Repeat("x", bodyBytes)))
	return m
}

func fcMsgs(n, bodyBytes int) []*common.MessageExt {
	out := make([]*common.MessageExt, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fcMsg(int64(i), bodyBytes))
	}
	return out
}

// fcConsumer is a not-started consumer with every gate off except the one under test: an
// earlier gate would mask the branch being asserted.
func fcConsumer(t *testing.T, group string) *DefaultMQPushConsumer {
	t.Helper()
	c := MustNewDefaultMQPushConsumer(group)
	c.mu.Lock()
	c.pullThresholdForQueue = 1 << 30
	c.pullThresholdSizeForQueue = 0
	c.consumeConcurrentlyMaxSpan = 0
	c.pullThresholdForTopic = -1
	c.pullThresholdSizeForTopic = -1
	c.mu.Unlock()
	return c
}

// fcHit puts the messages in a fresh process queue (nothing dispatched, so the gate sees all
// of them) and asks the gate once.
func fcHit(c *DefaultMQPushConsumer, mq common.MessageQueue, msgs ...*common.MessageExt) bool {
	pq := newProcessQueue(false)
	pq.PutMessage(msgs)
	return c.flowControlHit(mq, pq)
}

func fcTriggered(c *DefaultMQPushConsumer) int64 {
	return c.FlowControlTriggered()
}

func TestFlowControlDefaultsLeaveSmallBuffersOpen(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("GID_go_fc_defaults")
	c.mu.Lock()
	count, size, span := c.pullThresholdForQueue, c.pullThresholdSizeForQueue, c.consumeConcurrentlyMaxSpan
	topicCount, topicSize := c.pullThresholdForTopic, c.pullThresholdSizeForTopic
	c.mu.Unlock()
	if count != 1000 || size != 100 || span != 2000 {
		t.Errorf("queue-level defaults = (%d, %d MiB, span %d), want (1000, 100, 2000)", count, size, span)
	}
	if topicCount != -1 || topicSize != -1 {
		t.Errorf("topic-level defaults = (%d, %d), want both -1 (off)", topicCount, topicSize)
	}
	if fcHit(c, fcQueue(0), fcMsgs(1, 100)...) {
		t.Errorf("one 100-byte message must not hit the default gates")
	}
	// An empty buffer (nothing pulled yet) must not hit either, and the span of an empty
	// buffer must not be computed as a negative number.
	if fcHit(c, fcQueue(0)) {
		t.Errorf("an empty buffer must not hit")
	}
	if got := fcTriggered(c); got != 0 {
		t.Errorf("flowControlTriggered = %d, want 0 while nothing hits", got)
	}
}

func TestFlowControlCountGateIsStrictlyGreater(t *testing.T) {
	c := fcConsumer(t, "GID_go_fc_count")
	c.SetPullThresholdForQueue(3)
	if fcHit(c, fcQueue(0), fcMsgs(2, 10)...) {
		t.Errorf("2 messages at threshold 3 must not hit")
	}
	// Reaching the threshold exactly still pulls. Reverting ">" to ">=" reddens this leg.
	if fcHit(c, fcQueue(0), fcMsgs(3, 10)...) {
		t.Errorf("3 messages at threshold 3 must not hit: the gate is >, not >=")
	}
	if !fcHit(c, fcQueue(0), fcMsgs(4, 10)...) {
		t.Errorf("4 messages at threshold 3 must hit")
	}
	if got := fcTriggered(c); got != 1 {
		t.Errorf("flowControlTriggered = %d, want exactly 1 (one judgment records one tick)", got)
	}
}

func TestFlowControlCountGateTreatsZeroAsOne(t *testing.T) {
	c := fcConsumer(t, "GID_go_fc_count_zero")
	c.SetPullThresholdForQueue(0)
	if fcHit(c, fcQueue(0), fcMsgs(1, 10)...) {
		t.Errorf("threshold 0 clamps to 1, so one cached message is not over it")
	}
	if !fcHit(c, fcQueue(0), fcMsgs(2, 10)...) {
		t.Errorf("threshold 0 must clamp to 1, i.e. hit on the 2nd message")
	}
}

func TestFlowControlSizeGateIsInMiBAndStrictlyGreater(t *testing.T) {
	c := fcConsumer(t, "GID_go_fc_size")
	c.SetPullThresholdSizeForQueue(1)
	if fcHit(c, fcQueue(0), fcMsgs(3, 300)...) {
		t.Errorf("900 bytes under a 1 MiB threshold must not hit")
	}
	// Exactly the threshold is NOT a hit (>, not >=)...
	if fcHit(c, fcQueue(0), fcMsg(0, mib)) {
		t.Errorf("exactly 1 MiB must not hit: the gate is >, not >=")
	}
	// ...but one byte over it is, which separates ">" from a sloppy "much greater".
	if !fcHit(c, fcQueue(0), fcMsg(0, mib+1)) {
		t.Errorf("1 MiB + 1 byte exceeds a 1 MiB threshold")
	}
	// 0 disables the gate outright; a huge buffer then means nothing.
	c.SetPullThresholdSizeForQueue(0)
	if fcHit(c, fcQueue(0), fcMsg(0, 4*mib)) {
		t.Errorf("threshold 0 must disable the byte gate")
	}
}

func TestFlowControlSpanGateIsStrictlyGreater(t *testing.T) {
	c := fcConsumer(t, "GID_go_fc_span")
	c.SetConsumeConcurrentlyMaxSpan(10)
	if !fcHit(c, fcQueue(0), fcMsg(0, 1), fcMsg(100, 1)) {
		t.Errorf("an offset span of 100 over maxSpan 10 must hit")
	}
	c.SetConsumeConcurrentlyMaxSpan(100)
	if fcHit(c, fcQueue(0), fcMsg(0, 1), fcMsg(100, 1)) {
		t.Errorf("a span of exactly 100 is not strictly greater than 100")
	}
	// Out-of-order buffering must still measure max-min, not first-last.
	c.SetConsumeConcurrentlyMaxSpan(10)
	if !fcHit(c, fcQueue(0), fcMsg(50, 1), fcMsg(5, 1), fcMsg(7, 1)) {
		t.Errorf("an out-of-order buffer must measure the span as max-min")
	}
	c.SetConsumeConcurrentlyMaxSpan(0)
	if fcHit(c, fcQueue(0), fcMsg(0, 1), fcMsg(1_000_000_000, 1)) {
		t.Errorf("maxSpan 0 must disable the span gate")
	}
}

func TestFlowControlGateOrderIsCountSizeSpan(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("GID_go_fc_order")
	c.mu.Lock()
	c.pullThresholdForQueue = 1
	c.pullThresholdSizeForQueue = 1
	c.consumeConcurrentlyMaxSpan = 1
	c.pullThresholdForTopic = -1
	c.pullThresholdSizeForTopic = -1
	c.mu.Unlock()
	// All three match at once: exactly one tick per judgment, not one per matching gate.
	if !fcHit(c, fcQueue(0), fcMsg(0, 2*mib), fcMsg(100, 2*mib)) {
		t.Fatalf("all three gates match, so the queue must pause")
	}
	if got := fcTriggered(c); got != 1 {
		t.Errorf("flowControlTriggered = %d, want 1 when several gates match at once", got)
	}
}

func TestFlowControlWarnCadenceIsTheFirstTripAndEvery1000thAfter(t *testing.T) {
	if flowControlWarnEvery != 1000 {
		t.Errorf("flowControlWarnEvery = %d, want 1000", flowControlWarnEvery)
	}
	for _, n := range []int64{1, 1001, 2001} {
		if !isFlowControlWarnTrip(n) {
			t.Errorf("trip %d must log a WARN", n)
		}
	}
	// Control: the trips between the boundaries stay on the debug channel only. A cadence
	// that degraded to "warn always" or "warn once and stop" is caught here, not by the
	// boundary set above.
	for _, n := range []int64{0, 2, 999, 1000, 1002, 2000} {
		if isFlowControlWarnTrip(n) {
			t.Errorf("trip %d must not log a WARN", n)
		}
	}
}

// The WARN cadence has to be driven off the same counter the diagnostic getter returns: a
// helper tested in isolation but wired to a different variable would pass the pure test above
// and still warn every round in production.
func TestFlowControlWarnUsesTheExposedTripCounter(t *testing.T) {
	c := fcConsumer(t, "GID_go_fc_warn_counter")
	c.SetPullThresholdForQueue(1)
	mq := fcQueue(0)
	over := fcMsgs(2, 10)
	for n := int64(1); n <= 1000; n++ {
		if !fcHit(c, mq, over...) {
			t.Fatalf("trip %d must pause the queue", n)
		}
	}
	if got := fcTriggered(c); got != 1000 || isFlowControlWarnTrip(got) {
		t.Errorf("after 1000 trips (getter says %d) the cadence must be silent", got)
	}
	if !fcHit(c, mq, over...) {
		t.Fatalf("trip 1001 must pause the queue")
	}
	if got := fcTriggered(c); got != 1001 || !isFlowControlWarnTrip(got) {
		t.Errorf("after 1001 trips (getter says %d) the cadence must warn", got)
	}
}

// fcStageQueues makes the consumer OWN len(counts) queues with live pull state and buffers
// that many messages in each one, exactly as a pull round would.
func fcStageQueues(c *DefaultMQPushConsumer, counts ...int) []*processQueue {
	out := make([]*processQueue, 0, len(counts))
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, n := range counts {
		msgs := make([]*common.MessageExt, 0, n)
		for j := 0; j < n; j++ {
			msgs = append(msgs, fcMsg(int64(j), 10))
		}
		pq := newProcessQueue(false)
		pq.PutMessage(msgs)
		c.processQueueTable[fcQueue(int32(i))] = pq
		out = append(out, pq)
	}
	return out
}

// fcPerQueueKnobs reads the two per-queue gates, i.e. what the pull-time gate uses.
func fcPerQueueKnobs(c *DefaultMQPushConsumer) (count, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pullThresholdForQueue, c.pullThresholdSizeForQueue
}

// Only three per-queue gates exist at pull time, and each names itself: an unpaused buffer
// names nothing, so a reason string can never come from a branch the test did not stage.
func TestFlowControlCacheGateReasonNamesOnlyThePerQueueGates(t *testing.T) {
	c := fcConsumer(t, "GID_go_fc_reason")
	c.SetPullThresholdForQueue(2)
	c.SetPullThresholdSizeForQueue(0)
	c.SetConsumeConcurrentlyMaxSpan(0)
	pq := newProcessQueue(false)
	if reason := c.cacheGateReason(pq); reason != "" {
		t.Errorf("empty buffer reason = %q, want no gate", reason)
	}
	if reason := c.cacheGateReason(fcStageQueue(c, 0, 3, 10)); reason != "count" {
		t.Errorf("over-buffered reason = %q, want the message-count gate", reason)
	}

	c.SetPullThresholdForQueue(1 << 30)
	c.SetPullThresholdSizeForQueue(1)
	if reason := c.cacheGateReason(fcStageQueue(c, 0, 1, mib+1)); reason != "size" {
		t.Errorf("over-bytes buffer reason = %q, want the byte gate", reason)
	}

	c.SetPullThresholdSizeForQueue(0)
	c.SetConsumeConcurrentlyMaxSpan(10)
	if reason := c.cacheGateReason(fcStageQueue(c, 0, 2, 1, fcMsg(0, 1), fcMsg(99, 1))); reason != "span" {
		t.Errorf("wide-offset buffer reason = %q, want the offset-span gate", reason)
	}
}

// fcStageQueue buffers into the i-th owned queue, replacing its content, so one gate can be
// tripped while the sibling queues stay empty.
func fcStageQueue(c *DefaultMQPushConsumer, index, count, bodyBytes int, override ...*common.MessageExt) *processQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	pq, ok := c.processQueueTable[fcQueue(int32(index))]
	if !ok {
		pq = newProcessQueue(false)
		c.processQueueTable[fcQueue(int32(index))] = pq
	}
	pq.Clear()
	msgs := override
	if len(msgs) == 0 {
		msgs = fcMsgs(count, bodyBytes)
	}
	pq.PutMessage(msgs)
	return pq
}

// The topic budget is split over the owned queues at rebalance and the pull-time gate then
// reads ONLY that queue's buffer. Reverting to a pull-time sum of the topic's queues reddens
// the first leg: the four buffers together reach the topic budget exactly.
func TestFlowControlGateNeverAddsUpTheQueuesOfATopic(t *testing.T) {
	c := fcConsumer(t, "GID_go_fc_no_topic_sum")
	c.SetPullThresholdForQueue(1 << 30)
	c.SetPullThresholdForTopic(1000)
	queues := fcStageQueues(c, 250, 250, 250, 250)

	c.divideTopicThresholds()
	if got, _ := fcPerQueueKnobs(c); got != 250 {
		t.Fatalf("per-queue count gate = %d, want the 1000-message topic budget split over 4 queues", got)
	}
	// 1000 messages sit in the topic cache: each queue is exactly at its own share, and a
	// gate that is strictly greater lets all four keep pulling.
	for i, pq := range queues {
		if reason := c.cacheGateReason(pq); reason != "" {
			t.Errorf("queue %d pauses with %q at its divided share; the gate must judge this queue alone", i, reason)
		}
	}

	// One message past the divided share pauses that queue, on the count gate: the byte and
	// span gates are off in this fixture, so no other branch can be the reason.
	full := fcStageQueue(c, 1, 251, 10)
	if reason := c.cacheGateReason(full); reason != "count" {
		t.Errorf("queue 1 at 251 messages under a 250 gate = %q, want the count gate", reason)
	}
	if !c.flowControlHit(fcQueue(1), full) {
		t.Errorf("a queue over its divided share must pause")
	}
	for _, i := range []int{0, 2, 3} {
		if reason := c.cacheGateReason(queues[i]); reason != "" {
			t.Errorf("queue %d stayed at its share but pauses with %q; a full sibling must not spread", i, reason)
		}
	}
}

// ---------------------------------------------------------------- the POP gate

// The POP queue carries no buffer, only the ACK debt, so its gate reads that debt —
// and the trip has to land on the same counter the pull gates use: one throttling
// number per client, not one per consumption mode. Each leg below sits next to the
// control that goes red when the sharing, the direction, or the cadence is reverted.

// popFCStage gives the consumer one POP queue owing the given number of ACKs, the
// way a pop round leaves it, and returns its state.
func popFCStage(c *DefaultMQPushConsumer, index int32, waitAck int) *popProcessQueue {
	pq := newPopProcessQueue()
	pq.IncFoundMsg(waitAck)
	c.mu.Lock()
	c.popQueueTable[fcQueue(index)] = pq
	c.mu.Unlock()
	return pq
}

// popFCHit asks the POP gate once, exactly as the pop loop does.
func popFCHit(c *DefaultMQPushConsumer, index int32, waitAck int) bool {
	return c.popFlowControlHit(fcQueue(index), popFCStage(c, index, waitAck))
}

// fcSeedTrips moves the shared counter to a chosen trip number, so the cadence can
// be pinned at its boundary without running a thousand real trips.
func fcSeedTrips(c *DefaultMQPushConsumer, times int64) {
	c.mu.Lock()
	c.flowControlTriggered = times
	c.mu.Unlock()
}

func TestPopFlowControlGateIsStrictlyGreater(t *testing.T) {
	c := fcConsumer(t, "GID_go_pop_fc_strict")
	c.SetPopThresholdForQueue(3)

	if popFCHit(c, 0, 2) {
		t.Errorf("2 waiting ACKs at threshold 3 must not trip")
	}
	// Reaching the threshold exactly still pops; a queue may hold that many.
	// Reverting ">" to ">=" reddens this leg.
	if popFCHit(c, 0, 3) {
		t.Errorf("3 waiting ACKs at threshold 3 must not trip: the gate is >, not >=")
	}
	if !popFCHit(c, 0, 4) {
		t.Errorf("4 waiting ACKs at threshold 3 must trip")
	}
	// Only the trip records a tick — the two non-trips above must have left the
	// counter alone, so a helper that incremented before judging is caught here.
	if got := fcTriggered(c); got != 1 {
		t.Errorf("flowControlTriggered = %d, want exactly 1 (one judgment records one tick)", got)
	}

	// Same arithmetic at the shipped default, which the knob starts at: 96 outstanding
	// ACKs is what the queue may hold, the 97th pauses it.
	if defaultPopThresholdForQueue != 96 {
		t.Errorf("default popThresholdForQueue = %d, want 96", defaultPopThresholdForQueue)
	}
	d := fcConsumer(t, "GID_go_pop_fc_default")
	if got := d.popThresholdForQueueValue(); got != defaultPopThresholdForQueue {
		t.Fatalf("a fresh consumer's popThresholdForQueue = %d, want the default %d", got, defaultPopThresholdForQueue)
	}
	if popFCHit(d, 0, 96) {
		t.Errorf("96 waiting ACKs at the default threshold of 96 must not trip")
	}
	if !popFCHit(d, 0, 97) {
		t.Errorf("97 waiting ACKs at the default threshold of 96 must trip")
	}
	if got := fcTriggered(d); got != 1 {
		t.Errorf("flowControlTriggered = %d, want 1: only the 97-ACK round ticked", got)
	}
}

// A nil queue state (the loop lost it between rounds) is not a trip: the gate has no
// debt to read, so it must not pause nor tick.
func TestPopFlowControlHitIgnoresAMissingQueueState(t *testing.T) {
	c := fcConsumer(t, "GID_go_pop_fc_nil")
	c.SetPopThresholdForQueue(0)
	if c.popFlowControlHit(fcQueue(0), nil) {
		t.Errorf("a queue with no state must not be reported as flow-controlled")
	}
	if got := fcTriggered(c); got != 0 {
		t.Errorf("flowControlTriggered = %d, want 0: a missing state records nothing", got)
	}
}

// One pull hit plus one POP hit on the SAME consumer read 2, in either order: this is
// the assertion that fails when the POP gate keeps a counter of its own or none.
func TestPopFlowControlHitSharesThePullPathCounter(t *testing.T) {
	c := fcConsumer(t, "GID_go_pop_fc_shared")
	c.SetPullThresholdForQueue(1)
	c.SetPopThresholdForQueue(1)

	if !c.flowControlHit(fcQueue(0), fcStageQueue(c, 0, 2, 10)) {
		t.Fatalf("2 buffered messages over a count gate of 1 must pause the pull loop")
	}
	if got := fcTriggered(c); got != 1 {
		t.Fatalf("flowControlTriggered = %d after the pull hit, want 1", got)
	}
	if !popFCHit(c, 0, 2) {
		t.Fatalf("2 waiting ACKs over a pop gate of 1 must pause the pop loop")
	}
	if got := fcTriggered(c); got != 2 {
		t.Errorf("flowControlTriggered = %d after one pull hit and one POP hit, want 2: both paths must report on one counter", got)
	}

	// The reverse order on a fresh consumer: the POP hit takes trip 1 and the pull
	// hit still lands on it, so neither path can own a private counter.
	d := fcConsumer(t, "GID_go_pop_fc_shared_rev")
	d.SetPullThresholdForQueue(1)
	d.SetPopThresholdForQueue(1)
	if !popFCHit(d, 0, 2) {
		t.Fatalf("2 waiting ACKs over a pop gate of 1 must pause the pop loop")
	}
	if got := fcTriggered(d); got != 1 {
		t.Fatalf("flowControlTriggered = %d after the POP hit, want 1", got)
	}
	if !d.flowControlHit(fcQueue(0), fcStageQueue(d, 0, 2, 10)) {
		t.Fatalf("2 buffered messages over a count gate of 1 must pause the pull loop")
	}
	if got := fcTriggered(d); got != 2 {
		t.Errorf("flowControlTriggered = %d after one POP hit and one pull hit, want 2", got)
	}
}

// The POP trips are numbered off the shared counter, so they inherit the WARN cadence.
// The counter is seeded at the boundary instead of running a thousand real trips.
func TestPopFlowControlTripJoinsTheWarnCadence(t *testing.T) {
	c := fcConsumer(t, "GID_go_pop_fc_cadence")
	c.SetPopThresholdForQueue(1)

	if !popFCHit(c, 0, 2) {
		t.Fatalf("the first POP hit must trip the gate")
	}
	if got := fcTriggered(c); got != 1 || !isFlowControlWarnTrip(got) {
		t.Errorf("the first POP trip (getter says %d) must log a WARN", got)
	}
	if !popFCHit(c, 0, 2) {
		t.Fatalf("the second POP hit must trip the gate")
	}
	if got := fcTriggered(c); got != 2 || isFlowControlWarnTrip(got) {
		t.Errorf("trip %d must stay on the debug channel only", got)
	}

	// The trip that lands one past the cadence period warns, and the one before the
	// boundary does not. A POP path that warned every round, or never, breaks one of
	// these two legs; one that did not touch the counter breaks both.
	fcSeedTrips(c, flowControlWarnEvery)
	if !popFCHit(c, 0, 2) {
		t.Fatalf("the hit past the seeded boundary must trip the gate")
	}
	if got := fcTriggered(c); got != flowControlWarnEvery+1 || !isFlowControlWarnTrip(got) {
		t.Errorf("trip %d (getter says %d) must log a WARN", flowControlWarnEvery+1, got)
	}

	fcSeedTrips(c, flowControlWarnEvery-1)
	if !popFCHit(c, 0, 2) {
		t.Fatalf("the hit just before the boundary must trip the gate")
	}
	if got := fcTriggered(c); got != flowControlWarnEvery || isFlowControlWarnTrip(got) {
		t.Errorf("trip %d (getter says %d) must stay silent", flowControlWarnEvery, got)
	}
}

// The gate judges THIS queue's own debt and never adds up the topic's POP queues: the
// empty sibling keeps popping although its sibling alone exceeds the threshold, and
// the over-threshold queue ticks exactly once. A summing gate reddens the sibling leg
// and the tick count.
func TestPopFlowControlGateJudgesOnlyThisQueuesDebt(t *testing.T) {
	c := fcConsumer(t, "GID_go_pop_fc_per_queue")
	c.SetPopThresholdForQueue(3)

	full := popFCStage(c, 0, 4)
	empty := popFCStage(c, 1, 0)
	atLimit := popFCStage(c, 2, 3)

	if !c.popFlowControlHit(fcQueue(0), full) {
		t.Errorf("a queue over its own threshold must pause")
	}
	if c.popFlowControlHit(fcQueue(1), empty) {
		t.Errorf("an empty queue pauses although its sibling is over the threshold: the gate must not add up the topic's queues")
	}
	if c.popFlowControlHit(fcQueue(2), atLimit) {
		t.Errorf("a queue exactly at the threshold pauses although a sibling owes more: neither the sum nor >= belongs here")
	}
	if got := fcTriggered(c); got != 1 {
		t.Errorf("flowControlTriggered = %d, want 1: only the over-threshold queue records a tick", got)
	}
}

// The POP threshold is a plain per-queue knob: the rebalance division that turns the
// two topic-level budgets into per-queue ones leaves it alone, so a hand-set POP
// threshold means the same number of outstanding ACKs whatever the queue count.
func TestPopThresholdIsNeverDividedAtRebalance(t *testing.T) {
	c := fcConsumer(t, "GID_go_pop_fc_no_divide")
	c.SetPopMode(true)
	c.SetPullThresholdForTopic(1000)
	c.SetPopThresholdForQueue(7)
	ttOwnPopQueues(c, 4)

	c.divideTopicThresholds()
	if got := c.popThresholdForQueueValue(); got != 7 {
		t.Errorf("popThresholdForQueue = %d, want the hand-set 7: the POP gate keeps its own per-queue knob", got)
	}
	if !popFCHit(c, 0, 8) {
		t.Errorf("8 waiting ACKs must trip the untouched threshold of 7")
	}
}
