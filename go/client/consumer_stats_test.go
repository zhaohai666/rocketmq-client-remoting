package client

import (
	"math"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ------------------------------------------------------- difference windows

// The snapshot is a DIFFERENCE over the retained call-snapshot list, not a
// per-period bucket: sum and times are differences between the LAST and the
// FIRST entry, tps divides by the timestamp span, and avgpt only exists when
// the times difference is positive. Hand-computed here so a change to the
// arithmetic cannot hide behind a self-consistent expectation.
func TestStatsSnapshotIsADifferenceWindow(t *testing.T) {
	list := []callSnapshot{
		{timestamp: 1_000, times: 0, value: 0},
		{timestamp: 11_000, times: 1, value: 100},
		{timestamp: 21_000, times: 3, value: 300},
	}
	snap := computeStatsData(list)
	if snap.Sum != 300 {
		t.Errorf("Sum = %d, want 300 (last.value - first.value, NOT the last sample alone)", snap.Sum)
	}
	if snap.Tps != 15.0 {
		t.Errorf("Tps = %v, want 15 (300 * 1000 / 20000ms span)", snap.Tps)
	}
	if snap.Times != 3 {
		t.Errorf("Times = %d, want 3 (a DIFFERENCE, not the raw counter)", snap.Times)
	}
	if snap.Avgpt != 100.0 {
		t.Errorf("Avgpt = %v, want 100 (300 / 3 calls)", snap.Avgpt)
	}

	// An empty list is Java's `csList.isEmpty()` branch: a zero snapshot, not an
	// error and not a division by zero.
	if got := computeStatsData(nil); got != (statsSnapshot{}) {
		t.Errorf("computeStatsData(nil) = %+v, want the zero snapshot", got)
	}

	// A times difference of 0 leaves avgpt at 0 even when the sum is positive:
	// Java guards the division rather than reporting Inf.
	sameTimes := computeStatsData([]callSnapshot{
		{timestamp: 1_000, times: 7, value: 0},
		{timestamp: 11_000, times: 7, value: 40},
	})
	if sameTimes.Avgpt != 0 {
		t.Errorf("Avgpt = %v, want 0 when the times difference is 0", sameTimes.Avgpt)
	}
	if sameTimes.Sum != 40 {
		t.Errorf("Sum = %d, want 40", sameTimes.Sum)
	}
}

// A one-element list divides by a zero span. Java does not guard it, so the
// correct answer here is NaN — a guard would be a silent divergence AND would
// hide the real bug (a caller that forgot to seed the list, whose snapshot
// would then read as a plausible 0 TPS).
func TestStatsSnapshotWithOnlyOneEntryIsNaNLikeJava(t *testing.T) {
	snap := computeStatsData([]callSnapshot{{timestamp: 1_000}})
	if snap.Sum != 0 || snap.Times != 0 || snap.Avgpt != 0 {
		t.Errorf("snapshot = %+v, want all zeros", snap)
	}
	if !math.IsNaN(snap.Tps) {
		t.Errorf("Tps = %v, want NaN (0 * 1000.0 / 0, unguarded as in Java)", snap.Tps)
	}
}

// The seed is what makes the FIRST snapshot meaningful: without it the first
// sample would be both first and last (span 0, NaN TPS). With it, the first
// snapshot already reports the whole total accumulated since the item was born,
// because the seed sits one period in the past with zeros.
func TestStatsSampleSeedsTheWindow(t *testing.T) {
	it := newStatsItem("TEST_SEED", "k", false)
	if got := it.statsDataInMinute(); got != (statsSnapshot{}) {
		t.Errorf("before any sample: %+v, want the zero snapshot", got)
	}

	it.add(10, 2)
	it.samplingInSeconds()

	if len(it.csListMinute) != 2 {
		t.Fatalf("csListMinute = %d entries, want 2 (seed + one sample)", len(it.csListMinute))
	}
	seed := it.csListMinute[0]
	if seed.value != 0 || seed.times != 0 {
		t.Errorf("seed = %+v, want a zero-value/zero-times snapshot", seed)
	}
	if got := it.csListMinute[1].timestamp - seed.timestamp; got != 10_000 {
		t.Errorf("sample is %d ms after the seed, want exactly 10000 (one 10s period)", got)
	}

	snap := it.statsDataInMinute()
	if snap.Sum != 10 || snap.Times != 2 {
		t.Errorf("snapshot = %+v, want Sum=10 Times=2 (the whole total, seen from the seed)", snap)
	}
	if snap.Tps != 1.0 {
		t.Errorf("Tps = %v, want 1 (10 events over the 10s span)", snap.Tps)
	}
	if snap.Avgpt != 5.0 {
		t.Errorf("Avgpt = %v, want 5 (10 / 2 calls)", snap.Avgpt)
	}
}

// Java's list capacities: 7 for the minute list, 7 for the hour list, 25 for
// the day list (StatsItem:163/176/189 — the day list is the only one that spans
// a full day at its 1h cadence).
func TestStatsCallSnapshotCapacities(t *testing.T) {
	it := newStatsItem("TEST_CAP", "k", false)
	for i := 0; i < 20; i++ {
		it.samplingInSeconds()
	}
	if got := len(it.csListMinute); got != statsMinuteListMax {
		t.Errorf("csListMinute = %d, want %d", got, statsMinuteListMax)
	}
	for i := 0; i < 20; i++ {
		it.samplingInMinutes()
	}
	if got := len(it.csListHour); got != statsHourListMax {
		t.Errorf("csListHour = %d, want %d", got, statsHourListMax)
	}
	for i := 0; i < 40; i++ {
		it.samplingInHour()
	}
	if got := len(it.csListDay); got != statsDayListMax {
		t.Errorf("csListDay = %d, want %d", got, statsDayListMax)
	}
}

// Trimming must FREE the oldest entry, not merely move the window start
// forward: a reslicing implementation (`list = list[1:]`) leaves the head
// unreachable-but-retained and makes every later append allocate, so the slice's
// capacity grows without bound in a process that runs for weeks.
//
// Asserted as "capacity stops growing", not "capacity == the max": the same
// in-place trim of a leaf instance legitimately keeps one slot of slack.
func TestStatsTrimBoundsTheBackingArray(t *testing.T) {
	it := newStatsItem("TEST_TRIM", "k", false)
	it.samplingInSeconds()
	seedTimestamp := it.csListMinute[0].timestamp
	for i := 0; i < 4*statsMinuteListMax; i++ {
		it.samplingInSeconds()
	}
	if it.csListMinute[0].timestamp == seedTimestamp {
		t.Fatalf("the seed is still the head after %d samples; the trim did not drop it",
			4*statsMinuteListMax)
	}
	settled := cap(it.csListMinute)
	for i := 0; i < 500; i++ {
		it.samplingInSeconds()
	}
	if got := cap(it.csListMinute); got > settled {
		t.Errorf("cap(csListMinute) grew %d -> %d over 500 more samples; the trim leaks the array",
			settled, got)
	}
	if got := len(it.csListMinute); got != statsMinuteListMax {
		t.Errorf("len = %d, want %d", got, statsMinuteListMax)
	}
}

// ------------------------------------------------------------- item sets

// Keys are `topic@group` — topic FIRST. The reverse order parses fine and joins
// against nothing.
func TestStatsKeyIsTopicAtGroup(t *testing.T) {
	if got := statsKey("TopicA", "GID_x"); got != "TopicA@GID_x" {
		t.Errorf("statsKey = %q, want %q", got, "TopicA@GID_x")
	}
}

// An unknown key answers the zero snapshot rather than a nil/absent marker, so
// the 307 answer reports zeros for a subscribed-but-never-consumed topic.
func TestStatsItemSetUnknownKeyIsAZeroSnapshot(t *testing.T) {
	set := newStatsItemSet("TEST_UNKNOWN")
	if got := set.statsDataInMinute("nope"); got != (statsSnapshot{}) {
		t.Errorf("minute = %+v, want zeros", got)
	}
	if got := set.statsDataInHour("nope"); got != (statsSnapshot{}) {
		t.Errorf("hour = %+v, want zeros", got)
	}
	if got := set.statsDataInDay("nope"); got != (statsSnapshot{}) {
		t.Errorf("day = %+v, want zeros", got)
	}
	if set.itemCount() != 0 {
		t.Errorf("a read must not create an item, got %d", set.itemCount())
	}
}

// Java's getAndCreateItem installs with putIfAbsent, so the FIRST caller's kind
// wins: an RT call followed by a plain one keeps the RT item (only the print
// format depends on it, but the API contract does too).
func TestStatsItemSetKeepsTheFirstItemsKind(t *testing.T) {
	set := newStatsItemSet("TEST_KIND")
	set.addRTValue("k", 1, 1)
	set.addValue("k", 1, 1)
	it := set.getItem("k")
	if it == nil {
		t.Fatal("item was not created")
	}
	if !it.rtItem {
		t.Error("the RT item was replaced by a plain one; Java keeps the first")
	}
	if !containsSubstring(it.statPrintDetail(it.statsDataInMinute()), "AVGRT") {
		t.Errorf("print detail = %q, want the RT variant", it.statPrintDetail(it.statsDataInMinute()))
	}
}

// RTStatsItem's ONLY difference is the print format (TIMES/AVGRT instead of
// SUM/TPS/AVGPT — for a response-time item the throughput numbers are noise).
func TestRTStatsItemPrintVariant(t *testing.T) {
	snap := statsSnapshot{Sum: 9, Tps: 0.9, Avgpt: 4.5, Times: 2}
	plain := newStatsItem("CONSUME_OK_TPS", "k", false)
	if got, want := plain.statPrintDetail(snap), "SUM: 9 TPS: 0.90 AVGPT: 4.50"; got != want {
		t.Errorf("plain detail = %q, want %q", got, want)
	}
	rt := newStatsItem("CONSUME_RT", "k", true)
	if got, want := rt.statPrintDetail(snap), "TIMES: 2 AVGRT: 4.50"; got != want {
		t.Errorf("rt detail = %q, want %q", got, want)
	}
}

// ------------------------------------------------------- consumeStatus

// Every field but consumeFailedMsgs comes from the MINUTE window; the failed
// message COUNT comes from the HOUR window. The seeds make the spans exactly
// 10s and 10min, so the TPS values are exact and there is no tolerance to hide
// a wrong denominator.
func TestConsumeStatusReadsMinuteWindowExceptFailedMsgs(t *testing.T) {
	const group, topic = "GID_stats_windows", "TopicStatsWindows"
	key := statsKey(topic, group)

	m := newConsumerStatsManager()
	m.incPullRT(group, topic, 7)
	m.incPullTPS(group, topic, 4)
	m.incConsumeRT(group, topic, 30)
	m.incConsumeOKTPS(group, topic, 9)
	m.incConsumeFailedTPS(group, topic, 2)
	m.incConsumeFailedTPS(group, topic, 3)

	// Nothing sampled yet: every window is empty, so the whole snapshot is zero.
	// This is what a 307 answer says for a subscribed topic that was never
	// pulled, and it must not look like data.
	if cs := m.consumeStatus(group, topic); *cs != (remoting.ConsumeStatus{}) {
		t.Errorf("unsampled snapshot = %+v, want all zeros", *cs)
	}

	m.samplingInSeconds()
	m.samplingInMinutes()

	cs := m.consumeStatus(group, topic)
	if cs.PullRT != 7 {
		t.Errorf("PullRT = %v, want 7 (the average RT: 7ms over 1 pull)", cs.PullRT)
	}
	if cs.PullTPS != 0.4 {
		t.Errorf("PullTPS = %v, want 0.4 (4 messages over the 10s minute window)", cs.PullTPS)
	}
	if cs.ConsumeRT != 30 {
		t.Errorf("ConsumeRT = %v, want 30", cs.ConsumeRT)
	}
	if cs.ConsumeOKTPS != 0.9 {
		t.Errorf("ConsumeOKTPS = %v, want 0.9 (9 messages over 10s)", cs.ConsumeOKTPS)
	}
	if cs.ConsumeFailedTPS != 0.5 {
		t.Errorf("ConsumeFailedTPS = %v, want 0.5 (5 messages over 10s, two calls)", cs.ConsumeFailedTPS)
	}
	if cs.ConsumeFailedMsgs != 5 {
		t.Errorf("ConsumeFailedMsgs = %d, want 5 (the HOUR window's SUM, a count not a rate)",
			cs.ConsumeFailedMsgs)
	}

	// Sanity: the hour window really does hold 5 over 600s, so the two windows
	// are distinguishable — the assertion above is not accidentally satisfied by
	// a minute value.
	if got := m.topicAndGroupConsumeFailedTPS.statsDataInHour(key).Tps; got != 5.0/600.0 {
		t.Errorf("hour Tps = %v, want %v", got, 5.0/600.0)
	}
}

// getConsumeRT is the ONE field with a minute -> hour fallback, and the gate is
// the minute SUM being zero — which happens naturally once the seed ages out of
// the minute window and nothing new was recorded, i.e. "no consume in ~60s".
// Without the fallback such a consumer would report 0 ms RT, which reads as
// "instantaneous" instead of "no data".
func TestConsumeRTFallsBackToTheHourWindowOnlyOnAZeroMinuteSum(t *testing.T) {
	const group, topic = "GID_stats_rt_fallback", "TopicStatsRtFallback"
	key := statsKey(topic, group)

	m := newConsumerStatsManager()
	m.incConsumeRT(group, topic, 200)
	m.samplingInMinutes() // only the hour window (fed by the 10-minute task)

	// The minute window was never sampled at all: empty, sum 0 -> fallback.
	if got := m.consumeStatus(group, topic).ConsumeRT; got != 200 {
		t.Fatalf("ConsumeRT = %v, want the hour window's 200 (the minute window is empty)", got)
	}

	// Seven second-samples push the seed out of the 7-entry minute window. With
	// no new RT recorded in between, the item still holds 200 but the window's
	// difference is 0 — the second, non-obvious way the gate fires.
	for i := 0; i < statsMinuteListMax; i++ {
		m.samplingInSeconds()
	}
	if got := m.topicAndGroupConsumeRT.statsDataInMinute(key).Sum; got != 0 {
		t.Fatalf("minute Sum = %d, want 0 once the seed has aged out", got)
	}
	if got := m.consumeStatus(group, topic).ConsumeRT; got != 200 {
		t.Errorf("ConsumeRT = %v, want the hour window's 200 (minute sum is 0)", got)
	}
}

// PullRT must NOT fall back, or a consumer that stopped pulling would keep
// reporting the stale hour average and look healthy. This is the discriminator
// between "the fallback exists" and "the fallback got applied everywhere".
func TestPullRTDoesNotFallBackToTheHourWindow(t *testing.T) {
	const group, topic = "GID_stats_pullrt", "TopicStatsPullRt"
	key := statsKey(topic, group)

	m := newConsumerStatsManager()
	m.incPullRT(group, topic, 40)
	m.samplingInMinutes() // hour window: 40

	if got := m.topicAndGroupPullRT.statsDataInHour(key).Sum; got != 40 {
		t.Fatalf("hour Sum = %d, want 40", got)
	}
	if got := m.consumeStatus(group, topic).PullRT; got != 0 {
		t.Errorf("PullRT = %v, want 0: the hour window holds 40 and must NOT be consulted", got)
	}

	// With minute-window data it reports that.
	m.samplingInSeconds()
	if got := m.consumeStatus(group, topic).PullRT; got != 40 {
		t.Errorf("PullRT = %v, want the minute window's 40", got)
	}
}

// The sharpest discriminator between the two windows: drive ONLY the 10-minute
// cadence, so one field has data and the other does not. ConsumeFailedMsgs then
// reports the hour COUNT (5) while ConsumeFailedTPS reports 0 because its minute
// window is empty. Swapping the two lines would make the dashboard's
// failed-message column show the last 10 seconds instead of the last hour.
func TestConsumeFailedMsgsComesFromTheHourWindowNotTheMinuteOne(t *testing.T) {
	const group, topic = "GID_stats_failedmsgs", "TopicStatsFailedMsgs"

	m := newConsumerStatsManager()
	m.incConsumeFailedTPS(group, topic, 2)
	m.incConsumeFailedTPS(group, topic, 3)

	m.samplingInMinutes() // the HOUR window only (fed by the 10-minute task)
	cs := m.consumeStatus(group, topic)
	if cs.ConsumeFailedMsgs != 5 {
		t.Errorf("ConsumeFailedMsgs = %d, want 5 (the hour window's sum, a count)", cs.ConsumeFailedMsgs)
	}
	if cs.ConsumeFailedTPS != 0 {
		t.Errorf("ConsumeFailedTPS = %v, want 0 (the minute window has no samples)", cs.ConsumeFailedTPS)
	}

	// Now with minute-window data the same 5 failures read as a RATE while the
	// count is unchanged — proving the two fields come from different windows.
	m.samplingInSeconds()
	cs = m.consumeStatus(group, topic)
	if cs.ConsumeFailedTPS != 0.5 {
		t.Errorf("ConsumeFailedTPS = %v, want 0.5 (5 failures over the 10s minute window)", cs.ConsumeFailedTPS)
	}
	if cs.ConsumeFailedMsgs != 5 {
		t.Errorf("ConsumeFailedMsgs = %d, want 5 (a count from the hour window, unaffected)", cs.ConsumeFailedMsgs)
	}
}

// The key really does separate topics and groups: recording under one group must
// not show up under another.
func TestConsumeStatusIsKeyedByTopicAndGroup(t *testing.T) {
	const topic = "TopicStatsKeyed"
	m := newConsumerStatsManager()
	m.incConsumeOKTPS("GID_a", topic, 10)
	m.samplingInSeconds()

	if got := m.consumeStatus("GID_a", topic).ConsumeOKTPS; got != 1.0 {
		t.Errorf("GID_a ConsumeOKTPS = %v, want 1 (10 over 10s)", got)
	}
	if got := m.consumeStatus("GID_b", topic).ConsumeOKTPS; got != 0 {
		t.Errorf("GID_b ConsumeOKTPS = %v, want 0", got)
	}
	if got := m.consumeStatus("GID_a", "OtherTopic").ConsumeOKTPS; got != 0 {
		t.Errorf("OtherTopic ConsumeOKTPS = %v, want 0", got)
	}
}

// ------------------------------------------------- OK/Failed accounting

// Java's `case CONSUME_SUCCESS` arm: [0, ackIndex] are OK, the rest failed, and
// the failed counter is bumped even when the count is zero (which adds a times
// tick but no value). Pinned on the raw sums so the arithmetic is visible.
func TestRecordConsumeSuccessTPSArithmetic(t *testing.T) {
	const group, topic = "GID_stats_ok", "TopicStatsOk"
	key := statsKey(topic, group)

	m := newConsumerStatsManager()
	c := &DefaultMQPushConsumer{consumerGroup: group, instance: &Instance{consumerStats: m}}

	c.recordConsumeSuccessTPS(topic, 1, 4) // ok 2, failed 2
	c.recordConsumeSuccessTPS(topic, 3, 4) // ok 4, failed 0 (still two ticks)

	m.samplingInSeconds()
	cs := m.consumeStatus(group, topic)
	if cs.ConsumeOKTPS != 0.6 {
		t.Errorf("ConsumeOKTPS = %v, want 0.6 (6 acked over 10s)", cs.ConsumeOKTPS)
	}
	if cs.ConsumeFailedTPS != 0.2 {
		t.Errorf("ConsumeFailedTPS = %v, want 0.2 (2 failed over 10s)", cs.ConsumeFailedTPS)
	}
	if got := m.topicAndGroupConsumeFailedTPS.statsDataInMinute(key).Times; got != 2 {
		t.Errorf("failed counter saw %d calls, want 2: the zero bump must still tick", got)
	}
}

// A listener that returns CONSUME_SUCCESS with a negative AckIndex makes `ok`
// negative. Java does not clamp that and neither does this port: clamping would
// change what the 307 answer reports versus the reference client. (The listener
// contract requires ackIndex >= 0, so only a bug reaches this.)
func TestRecordConsumeSuccessTPSReproducesNegativeOkWithoutClamping(t *testing.T) {
	const group, topic = "GID_stats_neg", "TopicStatsNeg"
	key := statsKey(topic, group)

	m := newConsumerStatsManager()
	c := &DefaultMQPushConsumer{consumerGroup: group, instance: &Instance{consumerStats: m}}
	c.recordConsumeSuccessTPS(topic, -5, 4)

	m.samplingInSeconds()
	if got := m.topicAndGroupConsumeOKTPS.statsDataInMinute(key).Sum; got != -4 {
		t.Errorf("OK sum = %d, want -4 (ackIndex+1 = -4, unclamped as in Java)", got)
	}
	if got := m.consumeStatus(group, topic).ConsumeFailedTPS; got != 0.8 {
		t.Errorf("ConsumeFailedTPS = %v, want 0.8 (4 - (-4) = 8 over 10s)", got)
	}
}

// A consumer without an instance (a bare unit test) must no-op instead of
// panicking on a nil manager.
func TestStatisticsAreANoOpWithoutAnInstance(t *testing.T) {
	c := &DefaultMQPushConsumer{consumerGroup: "GID_stats_bare"}
	c.incPullRT("t", 1)
	c.incPullTPS("t", 1)
	c.incConsumeRT("t", 1)
	c.incConsumeOKTPS("t", 1)
	c.incConsumeFailedTPS("t", 1)
	c.recordConsumeSuccessTPS("t", 1, 4)
}

// ------------------------------------------------------- clock anchors

// The print tasks are anchored to the NEXT clock boundary, not to the start
// instant, so "Stats In One Minute" lines land on the minute.
func TestStatsPrintAnchorsPointAtTheNextClockBoundary(t *testing.T) {
	if got := millisUntilNextMinute(); got <= 0 || got > 60_000 {
		t.Errorf("millisUntilNextMinute = %d, want (0, 60000]", got)
	}
	if got := millisUntilNextHour(); got <= 0 || got > 3_600_000 {
		t.Errorf("millisUntilNextHour = %d, want (0, 3600000]", got)
	}
	if got := millisUntilNextMorning(); got <= 0 || got > 86_400_000 {
		t.Errorf("millisUntilNextMorning = %d, want (0, 86400000]", got)
	}
}

// ------------------------------------------------------------- end to end

// A real consume through the push consumer must reach the shared manager: the
// pull counters from the FOUND answer, the RT/OK counters from the listener
// round trip, and nothing at all in the failed columns.
func TestConsumerRecordsStatisticsOnPullAndConsume(t *testing.T) {
	topic := uniqueTopic("GoConsumerStats", t)
	const group = "GID_go_stats"

	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "s0", "s1", "s2")
	listener := newRecordingListener()
	c := f.newConsumer(t, group, listener, withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "all three consumed", func() bool { return listener.receivedAll(3) })

	m := consumerStatsManagerOf(t, c)
	if m == nil {
		t.Fatal("the started consumer has no statistics manager")
	}
	key := statsKey(topic, group)

	// The OK counter is bumped AFTER the listener returns, so the listener's own
	// "saw all 3" is not enough.
	waitFor(t, "3 messages recorded as consumed", func() bool {
		it := m.topicAndGroupConsumeOKTPS.getItem(key)
		return it != nil && it.value.Load() >= 3
	})

	m.samplingInSeconds()
	cs := m.consumeStatus(group, topic)
	if cs.ConsumeOKTPS <= 0 {
		t.Errorf("ConsumeOKTPS = %v, want > 0", cs.ConsumeOKTPS)
	}
	if cs.ConsumeFailedTPS != 0 {
		t.Errorf("ConsumeFailedTPS = %v, want 0 — nothing was rejected", cs.ConsumeFailedTPS)
	}
	if cs.ConsumeFailedMsgs != 0 {
		t.Errorf("ConsumeFailedMsgs = %d, want 0", cs.ConsumeFailedMsgs)
	}
	if got := m.topicAndGroupPullTPS.statsDataInMinute(key).Tps; got <= 0 {
		t.Errorf("PullTPS = %v, want > 0 — the FOUND answer must be counted", got)
	}

	// The RT counters are ticked once per batch / per pull. Their VALUES may be
	// 0 (a loopback round trip is sub-millisecond), so the call counts are the
	// assertion — a value check here would be flaky, not stronger.
	if got := m.topicAndGroupConsumeRT.statsDataInMinute(key).Times; got < 1 {
		t.Errorf("CONSUME_RT saw %d calls, want >= 1", got)
	}
	if got := m.topicAndGroupPullRT.statsDataInMinute(key).Times; got < 1 {
		t.Errorf("PULL_RT saw %d calls, want >= 1", got)
	}
	// The successful batch still ticks the FAILED set (failed == 0); that tick
	// is what the hour-window consumeFailedMsgs sum is computed over.
	if got := m.topicAndGroupConsumeFailedTPS.statsDataInMinute(key).Times; got < 1 {
		t.Errorf("CONSUME_FAILED_TPS saw %d calls, want >= 1 (a successful batch still ticks it)", got)
	}

	// One topic, one group -> exactly one key per set. A second key would mean
	// the implicit %RETRY% subscription or the instance name leaked in.
	for _, set := range m.sets() {
		if got := set.itemCount(); got != 1 {
			t.Errorf("set %s holds %d keys, want 1 (%s)", set.statsName, got, key)
		}
	}
}

// ---------------------------------------------------------------- helpers

// containsSubstring avoids importing strings for one call in the guarded tests.
func containsSubstring(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// consumerStatsManagerOf reads the consumer's manager under its own lock, so a
// concurrent Shutdown cannot race the pointer read.
func consumerStatsManagerOf(t *testing.T, c *DefaultMQPushConsumer) *consumerStatsManager {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.instance == nil {
		return nil
	}
	return c.instance.consumerStats
}
