// The PULL round's statistics: what one round of queuePullLoop is worth in
// PULL_RT and in PULL_TPS.
//
// consumer_pull.go records both counters inside ONE `status == FOUND` arm
// (consumer_pull.go:133-138), and the TPS arm counts `result.MsgFoundList` as it
// stands AFTER `c.pullAPI.processPullResult(mq, result, sub)`
// (consumer_pull.go:97) — the decode, the second tag pass and the
// FilterMessageHook list have already run by then, so the list is what this
// client actually holds, not what the broker put on the wire. The legs here are
// the discriminating cases for that contract:
//
//   - a NON-FOUND round (NO_NEW_MSG, NO_MATCHED, a broker error answer) records
//     nothing at all — neither RT nor TPS. "RT on every round" would make an
//     idle queue look like a pulling one;
//   - a FOUND round the client's own tag pass emptied still records RT (the
//     round trip really happened) and ZERO TPS (no row was handed to anybody);
//   - a FOUND round of M rows of which the filter keeps N records TPS == N, and
//     the N it records is the N the listener is later handed.
//
// Everything runs against the in-process mock cluster of instance_test.go, whose
// broker already answers PULL_MESSAGE(11) out of an in-memory store
// (consumer_test.go answerPull) — no live broker, no real network.
package client

import (
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// pullStatsRoundInterval keeps the pull loop off the CPU while still letting
// several rounds land inside a test's wait. It also gives the assertions their
// happens-before: the loop records its stats BEFORE the next round's request is
// sent, so once round N+1 has arrived at the broker, round N's counters are set.
const pullStatsRoundInterval = 50 * time.Millisecond

// ------------------------------------------------------------------- fixtures

// newPullStatsCluster is a PULL-mode (never POP) consumer on one single-queue
// topic, started against the mock cluster. The queue is the only one the
// consumer can be assigned, so every number read back below belongs to this
// topic and this group alone.
func newPullStatsCluster(t *testing.T, group, topic string, listener any,
	opts ...consumerOpt) (*clusterFixture, *DefaultMQPushConsumer) {
	t.Helper()
	f := newClusterFixture(t, map[string]int{topic: 1})
	all := append([]consumerOpt{
		withConsumeFromWhere(ConsumeFromWhereFirstOffset),
		func(c *DefaultMQPushConsumer) { c.SetPullInterval(pullStatsRoundInterval.Milliseconds()) },
	}, opts...)
	c := f.newConsumer(t, group, listener, all...)
	return f, c
}

// taggedRow is one broker-side row: a body and the TAGS property it carries
// ("" = untagged, which the second tag pass drops just like a wrong tag).
type taggedRow struct {
	body string
	tag  string
}

// stageRows puts rows into the mock broker's store for one queue. The store's
// own answerPull then serves them from the requested offset on, exactly as a
// broker that had ignored the subscription expression would.
func stageRows(b *consumerBroker, topic string, queueID int32, rows ...taggedRow) []*common.MessageExt {
	bodies := make([]string, 0, len(rows))
	for _, r := range rows {
		bodies = append(bodies, r.body)
	}
	exts := b.add(topic, queueID, bodies...)
	for i, ext := range exts {
		if rows[i].tag != "" {
			ext.PutProperty(common.PropertyTags, rows[i].tag)
		}
	}
	return exts
}

// ---------------------------------------------------------------- stats reader

// pullRoundStats is what the pull loop recorded for one (group, topic) pair,
// read two ways: the raw counters behind the stats items, and the MINUTE-window
// difference the GET_CONSUMER_RUNNING_INFO(307) answer actually reports
// (consumeStatus reads PullTPS as a RATE over that window, so the sums — not the
// rate — are what a per-round assertion can be pinned on).
type pullRoundStats struct {
	tpsSum     int64
	tpsTimes   int64
	rtTimes    int64
	winTPS     int64
	winTPSTm   int64
	winRTTimes int64
}

func readPullRoundStats(t *testing.T, c *DefaultMQPushConsumer, topic string) pullRoundStats {
	t.Helper()
	m := consumerStatsManagerOf(t, c)
	if m == nil {
		t.Fatal("the started consumer has no statistics manager to read from")
	}
	key := statsKey(topic, c.consumerGroup)
	var out pullRoundStats
	if it := m.topicAndGroupPullTPS.getItem(key); it != nil {
		out.tpsSum, out.tpsTimes = it.value.Load(), it.times.Load()
	}
	if it := m.topicAndGroupPullRT.getItem(key); it != nil {
		out.rtTimes = it.times.Load()
	}
	// One second-cadence sample makes the minute window [seed(0,0), now(total)]:
	// its difference is then the whole total, so the window must say the same as
	// the counters. The RT item's VALUE (wall ms) is deliberately not asserted —
	// a loopback round trip is sub-millisecond, so only its call count is a
	// reliable observable.
	m.samplingInSeconds()
	tpsWin := m.topicAndGroupPullTPS.statsDataInMinute(key)
	out.winTPS, out.winTPSTm = tpsWin.Sum, tpsWin.Times
	out.winRTTimes = m.topicAndGroupPullRT.statsDataInMinute(key).Times
	return out
}

// String keeps a failure message on one line with both views side by side.
func (s pullRoundStats) String() string {
	return "PULL_TPS sum/times=" + i64Text(s.tpsSum) + "/" + i64Text(s.tpsTimes) +
		", minute-window TPS sum/times=" + i64Text(s.winTPS) + "/" + i64Text(s.winTPSTm) +
		", PULL_RT times=" + i64Text(s.rtTimes) +
		", minute-window RT times=" + i64Text(s.winRTTimes)
}

// waitForPullRounds blocks until the broker has SEEN `want` PULL_MESSAGE requests
// for one queue. Round N's stats are recorded before request N+1 leaves the loop,
// so this is the settle point for "round N has been counted".
func waitForPullRounds(t *testing.T, b *consumerBroker, topic string, queueID int32, want int) {
	t.Helper()
	waitForBudget(t, 15*time.Second, func() bool {
		return len(b.pullsFor(topic, queueID)) >= want
	})
}

// ------------------------------------------------------- leg A: NO_NEW_MSG

// An idle queue: every answer is PULL_NOT_FOUND(19), so no round is a FOUND one
// and the pair stays empty. Under "RT on every round" the RT item exists with
// one tick per idle round, so this goes red on the first asserted number.
func TestPullNoNewMsgRoundRecordsNoPullRTAndNoPullTPS(t *testing.T) {
	topic := uniqueTopic("PullStatsNoNewMsg", t)
	const group = "GID_pull_stats_no_new_msg"

	listener := newRecordingListener()
	f, c := newPullStatsCluster(t, group, topic, listener)
	// Nothing is staged: the store answers PULL_NOT_FOUND for offset 0.
	startConsumer(t, c, topic, "TagA")

	waitForPullRounds(t, f.broker, topic, 0, 3)
	// Nothing was ever staged for this queue, so every one of those rounds is
	// provably a PULL_NOT_FOUND one: a FOUND answer needs rows in the store.
	stats := readPullRoundStats(t, c, topic)
	if stats.rtTimes != 0 || stats.winRTTimes != 0 {
		t.Fatalf("PULL_RT ticked %d time(s) (%s): a NO_NEW_MSG round must record nothing",
			stats.rtTimes, stats)
	}
	if stats.tpsSum != 0 || stats.tpsTimes != 0 || stats.winTPS != 0 {
		t.Fatalf("an idle queue recorded throughput (%s), want nothing", stats)
	}
	if n := listener.batchCount(); n != 0 {
		t.Fatalf("the listener saw %d batches it should never have been given", n)
	}
}

// --------------------------------------------------- leg A2: broker error round

// A rejected pull is not a round trip worth reporting either: pullKernel surfaces
// the error, the loop sleeps its back-off and records nothing. The FOUND round
// that follows is then the ONLY tick — which is what tells "the error round added
// one" from "the FOUND round added one".
func TestPullBrokerErrorRoundRecordsNoPullStats(t *testing.T) {
	topic := uniqueTopic("PullStatsError", t)
	const group = "GID_pull_stats_error"

	listener := newRecordingListener()
	f, c := newPullStatsCluster(t, group, topic, listener)
	stageRows(f.broker, topic, 0, taggedRow{"only-row", "TagA"})
	// Round 1 is refused with a system error, round 2 onward comes from the store.
	f.broker.forcePullAnswers(remoting.RespSystemError, 1)
	startConsumer(t, c, topic, "TagA")

	// 1 = the error round, 2 = the FOUND round, 3 = the first idle round after it.
	waitForPullRounds(t, f.broker, topic, 0, 3)
	stats := readPullRoundStats(t, c, topic)
	if stats.rtTimes != 1 || stats.winRTTimes != 1 {
		t.Fatalf("PULL_RT ticked %d time(s) (%s), want exactly 1: the error round must record nothing",
			stats.rtTimes, stats)
	}
	if stats.tpsSum != 1 || stats.tpsTimes != 1 || stats.winTPS != 1 {
		t.Fatalf("PULL_TPS sum/times=%d/%d, want 1/1 (the minute window says %d/%d)",
			stats.tpsSum, stats.tpsTimes, stats.winTPS, stats.winTPSTm)
	}
	waitForBudget(t, 5*time.Second, func() bool { return listener.receivedAll(1) })
}

// -------------------------------------------------- leg A3: NO_MATCHED round

// PULL_RETRY_IMMEDIATELY(20) means "there were rows, none matched" — the client
// got nothing, so it counts nothing. Same staging as the error leg, one different
// forced code.
func TestPullNoMatchedRoundRecordsNoPullStats(t *testing.T) {
	topic := uniqueTopic("PullStatsNoMatched", t)
	const group = "GID_pull_stats_no_matched"

	listener := newRecordingListener()
	f, c := newPullStatsCluster(t, group, topic, listener)
	stageRows(f.broker, topic, 0, taggedRow{"row-a", "TagA"}, taggedRow{"row-b", "TagA"})
	f.broker.forcePullAnswers(remoting.RespPullRetryImmediately, 1)
	startConsumer(t, c, topic, "TagA")

	waitForPullRounds(t, f.broker, topic, 0, 3)
	stats := readPullRoundStats(t, c, topic)
	if stats.rtTimes != 1 || stats.winRTTimes != 1 {
		t.Fatalf("PULL_RT ticked %d time(s) (%s), want exactly 1: a NO_MATCHED round records nothing",
			stats.rtTimes, stats)
	}
	if stats.tpsSum != 2 || stats.tpsTimes != 1 || stats.winTPS != 2 {
		t.Fatalf("PULL_TPS sum/times=%d/%d, want 2/1 (the minute window says %d/%d)",
			stats.tpsSum, stats.tpsTimes, stats.winTPS, stats.winTPSTm)
	}
	waitForBudget(t, 5*time.Second, func() bool { return listener.receivedAll(2) })
}

// --------------------------------------------------------- leg B: emptied FOUND

// The trap this leg exists for: the broker hands over 2 rows, the subscription is
// TagA and both rows are tagged TagB, so the SECOND tag pass inside
// processPullResult drops every one of them. The round is still a FOUND round
// worth its RT, but its delivered-row count is zero — so PULL_RT ticks once and
// PULL_TPS is never touched. Read the TPS off the raw broker list instead and
// this goes red with sum=2, times=1.
func TestPullFoundRoundEmptiedByTheTagPassRecordsRTButNoTPS(t *testing.T) {
	topic := uniqueTopic("PullStatsEmptyFound", t)
	const group = "GID_pull_stats_empty_found"

	listener := newRecordingListener()
	f, c := newPullStatsCluster(t, group, topic, listener)
	stageRows(f.broker, topic, 0,
		taggedRow{"wrong-tag-1", "TagB"},
		taggedRow{"wrong-tag-2", "TagC"},
		taggedRow{"untagged", ""})
	startConsumer(t, c, topic, "TagA")

	// Round 1: FOUND with 3 rows, all dropped by the tag pass. Round 2 onward:
	// the offset moved past them, so the answers are NO_NEW_MSG.
	waitForPullRounds(t, f.broker, topic, 0, 2)
	stats := readPullRoundStats(t, c, topic)
	if stats.rtTimes != 1 || stats.winRTTimes != 1 {
		t.Fatalf("PULL_RT ticked %d time(s) (%s), want exactly the 1 FOUND round", stats.rtTimes, stats)
	}
	if stats.tpsSum != 0 || stats.tpsTimes != 0 || stats.winTPS != 0 || stats.winTPSTm != 0 {
		t.Fatalf("PULL_TPS counted the broker's raw rows (%s), want 0 for a round that delivered nothing",
			stats)
	}
	// The dropped rows never entered the buffer either — that is the same list.
	if pq := consumerForQueue(c, topic, "b1", 0); pq != nil && pq.MsgCount() != 0 {
		t.Fatalf("the queue buffer holds %d filtered rows", pq.MsgCount())
	}
	if n := listener.batchCount(); n != 0 {
		t.Fatalf("the listener saw %d batches, want none", n)
	}
}

// ------------------------------------------------------------ leg C: N of M kept

// A FOUND round of M rows whose filter keeps N records TPS == N. Two filter
// stages are covered separately, because the contract counts the list AFTER both:
//   - the second tag pass drops rows the broker sent anyway;
//   - the FilterMessageHook list drops rows the tag pass had kept.
//
// Both cases fail with sum=M if the recorder moves back to the raw list.
func TestPullTPSCountsTheRowsThatSurviveTheFilter(t *testing.T) {
	t.Run("second tag pass drops 2 of 5", func(t *testing.T) {
		topic := uniqueTopic("PullStatsTagPass", t)
		const group = "GID_pull_stats_tag_pass"

		listener := newRecordingListener()
		f, c := newPullStatsCluster(t, group, topic, listener)
		stageRows(f.broker, topic, 0,
			taggedRow{"keep-1", "TagA"},
			taggedRow{"drop-1", "TagB"},
			taggedRow{"keep-2", "TagA"},
			taggedRow{"drop-2", ""},
			taggedRow{"keep-3", "TagA"})
		startConsumer(t, c, topic, "TagA")

		waitForPullRounds(t, f.broker, topic, 0, 2)
		stats := readPullRoundStats(t, c, topic)
		// 3 kept, never the 5 the broker put in the body.
		if stats.tpsSum != 3 || stats.tpsTimes != 1 || stats.winTPS != 3 || stats.winTPSTm != 1 {
			t.Fatalf("PULL_TPS sum/times=%d/%d (minute window %d/%d), want 3/1 — the 3 rows the tag pass kept",
				stats.tpsSum, stats.tpsTimes, stats.winTPS, stats.winTPSTm)
		}
		if stats.rtTimes != 1 {
			t.Fatalf("PULL_RT ticked %d time(s) (%s), want the 1 FOUND round", stats.rtTimes, stats)
		}
		waitForBudget(t, 5*time.Second, func() bool { return listener.receivedAll(3) })
		if got := sortedBodies(listener.bodies()); !equalStrings(got,
			[]string{"keep-1", "keep-2", "keep-3"}) {
			t.Fatalf("the listener saw %v, want the 3 rows PULL_TPS counted", got)
		}
	})

	t.Run("filter hook drops 1 of 4", func(t *testing.T) {
		topic := uniqueTopic("PullStatsHook", t)
		const group = "GID_pull_stats_hook"

		listener := newRecordingListener()
		f, c := newPullStatsCluster(t, group, topic, listener)
		stageRows(f.broker, topic, 0,
			taggedRow{"stay-1", "TagA"},
			taggedRow{"go-away", "TagA"},
			taggedRow{"stay-2", "TagA"},
			taggedRow{"stay-3", "TagA"})
		// Registered before Start: the pull API takes the hook list at start-up.
		hook := &popFilterHook{name: "drop-go-away", drop: "go-away"}
		c.RegisterFilterMessageHook(hook)
		startConsumer(t, c, topic, "TagA")

		waitForPullRounds(t, f.broker, topic, 0, 2)
		stats := readPullRoundStats(t, c, topic)
		if stats.tpsSum != 3 || stats.tpsTimes != 1 || stats.winTPS != 3 {
			t.Fatalf("PULL_TPS sum/times=%d/%d (minute window %d), want 3/1 — the 3 rows past the hook",
				stats.tpsSum, stats.tpsTimes, stats.winTPS)
		}
		if hook.calls.Load() == 0 {
			t.Fatal("the filter hook never ran on the pull path, so this case proved nothing")
		}
		waitForBudget(t, 5*time.Second, func() bool { return listener.receivedAll(3) })
		for _, body := range listener.bodies() {
			if body == "go-away" {
				t.Fatalf("the hook dropped %q from the count but not from delivery", body)
			}
		}
	})
}
