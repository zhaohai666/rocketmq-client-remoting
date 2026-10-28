// The PULL round's pacing: which wait, if any, sits between two rounds of
// queuePullLoop.
//
// consumer_pull.go puts pullInterval on ONE kind of round — the one that handed
// rows to the queue — and on no other. What counts as "handed rows" is the same
// list the PULL_TPS counter uses (pull_round_stats_test.go pins that list), so it
// is what survived the client's own tag pass and filter hooks, not what the broker
// put on the wire. The legs here are the two sides of that rule:
//
//   - a FOUND round that delivered waits pullInterval when it is positive, and
//     re-pulls at once when it is 0;
//   - a round that delivered nothing — a FOUND the tag pass emptied, NO_NEW_MSG,
//     NO_MATCHED_MSG — re-pulls at once, because the cadence paces rounds that
//     delivered;
//   - failures are a third case and are not re-proved here: an error round still
//     sleeps its own wait (consumer_test.go's backoff legs), which is neither
//     pullInterval nor an immediate retry.
//
// Every case measures the ARRIVAL times the mock broker stamped on its own
// PULL_MESSAGE requests, so it reads the gap the loop produced rather than the
// wall clock of the test. The rows stay replayable (the mock is told to answer the
// same nextBeginOffset every round) because a case about per-round waits needs the
// SAME round shape over and over, and a store whose cursor advanced past its rows
// would turn a delivered round into an idle one after the first pass.
package client

import (
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// pullArrivalsFor is the arrival time of every PULL_MESSAGE for one queue, oldest
// first. It is index-parallel to b.pulls, so it filters the same way pullsFor does.
func (b *consumerBroker) pullArrivalsFor(topic string, queueID int32) []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []int64
	for i, p := range b.pulls {
		if deref(p.Topic) == topic && derefI32(p.QueueID) == queueID {
			out = append(out, b.pullArrivals[i])
		}
	}
	return out
}

// pullArrivalsWithin blocks until `want` PULL rounds of one queue have arrived at
// the broker and returns those arrival times.
func pullArrivalsWithin(t *testing.T, b *consumerBroker, topic string, queueID int32,
	want int, budget time.Duration) []int64 {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if got := b.pullArrivalsFor(topic, queueID); len(got) >= want {
			return got[:want]
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := b.pullArrivalsFor(topic, queueID)
	t.Fatalf("only %d pull rounds arrived in %s, want %d", len(got), budget, want)
	return nil
}

// replayPullFrom pins the nextBeginOffset every PULL_MESSAGE answer carries, so
// the consumer keeps pulling at the same offset and the round under test repeats.
// A real long poll suspends at the broker; this mock answers at once, so the only
// per-round wait left is the one the loop itself adds.
func (b *consumerBroker) replayPullFrom(offset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextBegin = &offset
	b.pullReplayNext = true
}

// newPullPacingCluster is a PULL-mode consumer on one single-queue topic whose
// pullInterval is exactly the value under test.
func newPullPacingCluster(t *testing.T, group, topic string, listener any, intervalMs int64,
	opts ...consumerOpt) (*clusterFixture, *DefaultMQPushConsumer) {
	t.Helper()
	f := newClusterFixture(t, map[string]int{topic: 1})
	all := append([]consumerOpt{
		withConsumeFromWhere(ConsumeFromWhereFirstOffset),
		func(c *DefaultMQPushConsumer) { c.SetPullInterval(intervalMs) },
	}, opts...)
	c := f.newConsumer(t, group, listener, all...)
	return f, c
}

// assertNoRoundWait fails when `want` rounds did not fit inside maxSpanMS, i.e.
// when the loop demonstrably put a wait between them. `want` is chosen so that the
// pullInterval under test, if it were applied per round, would overshoot maxSpanMS
// on its own — the gap is therefore attributed to the loop, not to the machine.
func assertNoRoundWait(t *testing.T, arrivals []int64, want int, pullIntervalMs, maxSpanMs int64) {
	t.Helper()
	span := arrivals[want-1] - arrivals[0]
	if span > maxSpanMs {
		t.Fatalf("%d rounds spanned %dms, want them back to back: the loop slept something"+
			" per round although pullInterval=%d must not pace these rounds",
			want, span, pullIntervalMs)
	}
}

// waitPullRoundsCounted parks until the pull loop's own PULL_TPS counter has
// recorded `want` rounds. It has to exist because the loop hands the batch to the
// consume side BEFORE it ticks the counter, so the arrival of the last round can
// be observed while that round is still uncounted; comparing a count with an
// arrival timeline without this would race its own subject. The counter is read
// raw here — the sampling helper would add minute-window samples that are not
// what this is waiting on.
func waitPullRoundsCounted(t *testing.T, c *DefaultMQPushConsumer, topic string, want int64) {
	t.Helper()
	m := consumerStatsManagerOf(t, c)
	if m == nil {
		t.Fatal("the started consumer has no statistics manager to read from")
	}
	key := statsKey(topic, c.consumerGroup)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if it := m.topicAndGroupPullTPS.getItem(key); it != nil && it.times.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ------------------------------------------- FOUND that delivered rows: paced

// A delivered round waits pullInterval, and only when pullInterval is positive:
// the two cases differ in one knob and must differ in the gap they produce.
func TestPullFoundWithRowsWaitsPullIntervalOnlyWhenPositive(t *testing.T) {
	cases := []struct {
		name       string
		intervalMs int64
		rounds     int
		minSpanMs  int64
		maxSpanMs  int64
	}{
		// Six delivered rounds: five gaps, none shorter than the 50ms the knob
		// says, and the run still finishes long before any other wait could.
		{"positive interval is waited out", 50, 6, 5 * 45, 2500},
		// Thirty delivered rounds at pullInterval=0: at 50ms each they would need
		// 1450ms, so fitting inside 1000ms proves nothing was slept.
		{"zero interval re-pulls at once", 0, 30, 0, 1000},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			topic := uniqueTopic("PullPacingFound", t)
			const group = "GID_pull_pacing_found"

			listener := newRecordingListener()
			f, c := newPullPacingCluster(t, group, topic, listener, tc.intervalMs)
			stageRows(f.broker, topic, 0, taggedRow{"keep", "TagA"})
			f.broker.replayPullFrom(0)
			startConsumer(t, c, topic, "TagA")

			arrivals := pullArrivalsWithin(t, f.broker, topic, 0, tc.rounds, 20*time.Second)
			span := arrivals[tc.rounds-1] - arrivals[0]
			if tc.minSpanMs > 0 && span < tc.minSpanMs {
				t.Fatalf("%d delivered rounds spanned %dms, want at least %dms"+
					" (pullInterval=%d on every one of them)",
					tc.rounds, span, tc.minSpanMs, tc.intervalMs)
			}
			if span > tc.maxSpanMs {
				t.Fatalf("%d delivered rounds spanned %dms, want under %dms (pullInterval=%d)",
					tc.rounds, span, tc.maxSpanMs, tc.intervalMs)
			}
			// The rounds are provably delivered ones: every one of them handed the
			// same row to the queue, so PULL_TPS counted each of them. The counter is
			// the pull loop's own record, so it needs no dispatch round trip to land
			// — but it is ticked after the batch is handed over, so the count is
			// waited out rather than read at whatever instant the last arrival came.
			waitPullRoundsCounted(t, c, topic, int64(tc.rounds))
			stats := readPullRoundStats(t, c, topic)
			if stats.tpsTimes < int64(tc.rounds) {
				t.Fatalf("PULL_TPS ticked %d time(s) over %d rounds (%s):"+
					" the rounds measured here did not all deliver",
					stats.tpsTimes, tc.rounds, stats)
			}
		})
	}
}

// ----------------------------------- FOUND emptied by the tag pass: not paced

// The round the client's own filtering empties is not a delivery, so it waits
// nothing — even though the very same round would have slept pullInterval had a
// row survived. The knob is set to 500ms so this cannot be confused with "the
// machine is fast": eleven gaps of 500ms would need 5500ms.
func TestPullFoundEmptiedByTagPassWaitsNothing(t *testing.T) {
	const intervalMs = 500
	const rounds = 12
	const maxSpanMs = 1500

	topic := uniqueTopic("PullPacingEmptyFound", t)
	const group = "GID_pull_pacing_empty_found"

	listener := newRecordingListener()
	f, c := newPullPacingCluster(t, group, topic, listener, intervalMs)
	stageRows(f.broker, topic, 0,
		taggedRow{"wrong-tag-1", "TagB"},
		taggedRow{"wrong-tag-2", "TagC"},
		taggedRow{"untagged", ""})
	f.broker.replayPullFrom(0)
	startConsumer(t, c, topic, "TagA")

	arrivals := pullArrivalsWithin(t, f.broker, topic, 0, rounds, 20*time.Second)
	assertNoRoundWait(t, arrivals, rounds, intervalMs, maxSpanMs)

	// Both halves of the round's identity, read off the counters: it IS a FOUND
	// round (PULL_RT ticked) and it delivered nothing (PULL_TPS never ticked), so
	// the immediate re-pull below cannot have been an idle status answering instead.
	stats := readPullRoundStats(t, c, topic)
	if stats.rtTimes < rounds {
		t.Fatalf("PULL_RT ticked %d time(s) over %d rounds (%s): the broker's rows"+
			" did not reach the client as FOUND every round", stats.rtTimes, rounds, stats)
	}
	if stats.tpsSum != 0 || stats.tpsTimes != 0 {
		t.Fatalf("PULL_TPS counted %d row(s) over %d times (%s), want 0/0:"+
			" the tag pass drops every row of these rounds", stats.tpsSum, stats.tpsTimes, stats)
	}
	if n := listener.batchCount(); n != 0 {
		t.Fatalf("the listener saw %d batches it should never have been given", n)
	}
}

// -------------------------------------------------- idle statuses: not paced

// NO_NEW_MSG and NO_MATCHED_MSG are the two statuses a round can answer with when
// nothing was handed over, and both re-pull at once: the cadence paces rounds that
// delivered, so a further local wait on an idle round only delays the next fetch.
// Each case runs twelve rounds while pullInterval alone would put 500ms on them.
func TestPullIdleStatusesRePullImmediately(t *testing.T) {
	const intervalMs = 500
	const rounds = 12
	const maxSpanMs = 1500

	cases := []struct {
		name  string
		idle  func(f *clusterFixture)
		phase string
	}{
		{
			name:  "no new message",
			idle:  func(_ *clusterFixture) {},
			phase: "nothing is staged, so every answer is PULL_NOT_FOUND(19)",
		},
		{
			name: "no matched message",
			idle: func(f *clusterFixture) {
				// -1 keeps the answer forced for every round rather than a counted
				// run, so the whole phase is PULL_RETRY_IMMEDIATELY(20).
				f.broker.forcePullAnswers(remoting.RespPullRetryImmediately, -1)
			},
			phase: "the broker ran a full pass and matched nothing",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			topic := uniqueTopic("PullPacingIdle", t)
			const group = "GID_pull_pacing_idle"

			listener := newRecordingListener()
			f, c := newPullPacingCluster(t, group, topic, listener, intervalMs)
			// Rows exist behind the cursor for the NO_MATCHED case; for NO_NEW_MSG
			// the store simply has none. Either way no round of the measured run can
			// be a FOUND one, which is what makes the phase provably idle.
			tc.idle(f)
			startConsumer(t, c, topic, "TagA")

			arrivals := pullArrivalsWithin(t, f.broker, topic, 0, rounds, 20*time.Second)
			assertNoRoundWait(t, arrivals, rounds, intervalMs, maxSpanMs)

			stats := readPullRoundStats(t, c, topic)
			if stats.rtTimes != 0 || stats.tpsSum != 0 || stats.tpsTimes != 0 {
				t.Fatalf("an idle round counted something (%s); %s", stats, tc.phase)
			}
			if n := listener.batchCount(); n != 0 {
				t.Fatalf("the listener saw %d batches it should never have been given", n)
			}
		})
	}
}
