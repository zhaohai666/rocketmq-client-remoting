// The FOUND pull round's offset sanity warning.
//
// A round of queuePullLoop remembers the cursor it asked to pull from
// (“prevRequestOffset“), advances the queue's cursor to the reply's
// “nextBeginOffset“ and hands its batch to the consume side. Only then does the
// client judge the round's own numbers, and it warns when either comparison holds:
//
//   - “nextBeginOffset < prevRequestOffset“, or
//   - “firstMsgOffset < prevRequestOffset“,
//
// where “firstMsgOffset“ is the queue offset of the FIRST row of the list the
// round produces for consumption — after this port's tag pass and filter hooks — and
// the largest int64 when that list is empty, so a round that handed no row over
// cannot trip the second comparison. One WARN line carries all three numbers.
//
// The warning is diagnostic only, so the pins here are two-sided: the decision
// itself (“pullOffsetsSanityWarning“ reads a round's status, its reply and its
// rows without a client), and one loop-level pair of legs that drives a warning
// round next to an identical consistent round and asserts the stored cursor, the
// next round's request point, the rows that reached the listener and the two pull
// counters come out the same on both.
//
// The loop-level legs run against the in-process mock cluster (consumer_test.go's
// answerPull), whose “replayPullFrom“ knob pins the “nextBeginOffset“ every PULL
// answer carries — that is how a round is made to answer below the offset the client
// asked for.
package client

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ------------------------------------------------------------------- the round

// sanityRow is one row as the pull path hands it over: only its queue offset matters
// to the check, so the rest of the ext keeps its zero values.
func sanityRow(offset int64) *common.MessageExt {
	m := common.NewMessageExt()
	m.QueueOffset = offset
	return m
}

// sanityFound is a FOUND round: the cursor its reply puts next, and the rows it
// hands to the consume side in the order the queue receives them.
func sanityFound(nextBeginOffset int64, rows ...int64) *PullResult {
	msgs := make([]*common.MessageExt, 0, len(rows))
	for _, off := range rows {
		msgs = append(msgs, sanityRow(off))
	}
	return &PullResult{
		PullStatus:      PullFound,
		NextBeginOffset: nextBeginOffset,
		MsgFoundList:    msgs,
	}
}

// sanityStatus is a round of one of the statuses that hands no rows over.
func sanityStatus(status PullStatus, nextBeginOffset int64) *PullResult {
	return &PullResult{PullStatus: status, NextBeginOffset: nextBeginOffset}
}

var sanityMQ = common.MessageQueue{Topic: "PullOffsetSanityTopic", BrokerName: "b1", QueueID: 0}

// ------------------------------------------- 1. the two comparisons, and nothing else

func TestPullOffsetsLookWrongIsExactlyTheTwoComparisons(t *testing.T) {
	cases := []struct {
		name              string
		nextBeginOffset   int64
		firstMsgOffset    int64
		prevRequestOffset int64
		want              bool
	}{
		{"first leg: the next cursor sits below the requested one", 9, 10, 10, true},
		{"second leg: the first row sits below the requested offset", 12, 9, 10, true},
		{"both legs at once", 9, 9, 10, true},
		{"CONTROL: cursor and row at the request point are not below it", 10, 10, 10, false},
		{"CONTROL: both above the request point", 12, 11, 10, false},
		{"CONTROL: the empty-list sentinel never fires the row leg", 12, math.MaxInt64, 10, false},
		{"the sentinel still leaves the cursor leg standing", 9, math.MaxInt64, 10, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := pullOffsetsLookWrong(tc.nextBeginOffset, tc.firstMsgOffset,
				tc.prevRequestOffset); got != tc.want {
				t.Fatalf("pullOffsetsLookWrong(next=%d, first=%d, prev=%d) = %v, want %v",
					tc.nextBeginOffset, tc.firstMsgOffset, tc.prevRequestOffset, got, tc.want)
			}
		})
	}
}

// The row number is the FIRST row of the round's own list, and an empty list carries
// the sentinel rather than a row that was never there.
func TestFirstPullMsgOffsetReadsTheFirstRowOrSentinel(t *testing.T) {
	if got := firstPullMsgOffset(nil); got != math.MaxInt64 {
		t.Fatalf("firstPullMsgOffset(nil) = %d, want the MaxInt64 sentinel", got)
	}
	if got := firstPullMsgOffset([]*common.MessageExt{}); got != math.MaxInt64 {
		t.Fatalf("an empty round reported %d, want the MaxInt64 sentinel", got)
	}
	// The list is the one the queue was handed, so its HEAD is what counts: the
	// second row's offset is never the number the warning reads.
	rows := []*common.MessageExt{sanityRow(7), sanityRow(3), sanityRow(9)}
	if got := firstPullMsgOffset(rows); got != 7 {
		t.Fatalf("firstPullMsgOffset = %d, want 7 (the first row of the handed-over list)", got)
	}
}

// ------------------------------------------------- 2. the warning line and its numbers

// A FOUND round whose next cursor sits below the offset it requested warns, and the
// single line carries all three numbers: which offset was asked for is what makes
// either comparison readable.
func TestPullOffsetsSanityWarningNamesAllThreeNumbers(t *testing.T) {
	warn := pullOffsetsSanityWarning(sanityFound(9, 10), sanityMQ, 10)
	if warn == "" {
		t.Fatal("a next cursor below the requested offset produced no warning")
	}
	for _, want := range []string{"requested at 10", "nextBeginOffset=9", "firstMsgOffset=10"} {
		if !strings.Contains(warn, want) {
			t.Errorf("the warning %q does not carry %q", warn, want)
		}
	}
	if strings.Contains(warn, "\n") {
		t.Errorf("the warning is not one line: %q", warn)
	}
}

// The second leg warns on its own, with the cursor looking perfectly consistent, and
// reads the consistent cursor against the row that is not.
func TestPullOffsetsSanityWarningFiresOnTheFirstRowLegAlone(t *testing.T) {
	warn := pullOffsetsSanityWarning(sanityFound(12, 9, 10), sanityMQ, 10)
	if warn == "" {
		t.Fatal("a first row below the requested offset produced no warning")
	}
	for _, want := range []string{"requested at 10", "nextBeginOffset=12", "firstMsgOffset=9"} {
		if !strings.Contains(warn, want) {
			t.Errorf("the warning %q does not carry %q", warn, want)
		}
	}
}

// --------------------------------------------- 3. the rounds and statuses that stay quiet

// A consistent FOUND round has nothing to say, and neither has an empty one: its row
// number is the sentinel, so only the cursor could have tripped the check.
func TestPullOffsetsSanityWarningStaysQuietForConsistentRounds(t *testing.T) {
	cases := []struct {
		name  string
		round *PullResult
		prev  int64
	}{
		{"a FOUND round whose cursor and rows both sit at or above its request", sanityFound(12, 10, 11), 10},
		{"CONTROL: exactly the requested offset is not below it", sanityFound(10, 10), 10},
		{"an empty FOUND round with a consistent cursor", sanityFound(10), 10},
		{"an empty FOUND round whose cursor equals the request point", sanityFound(12), 12},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if warn := pullOffsetsSanityWarning(tc.round, sanityMQ, tc.prev); warn != "" {
				t.Fatalf("a round with nothing to report warned: %q", warn)
			}
		})
	}
}

// Only a FOUND round is judged. The statuses that hand no rows over are checked with
// the SAME backwards numbers that warn above, so the quietness is the status rule and
// not the numbers happening to fit.
func TestPullOffsetsSanityWarningStaysQuietForNonFoundStatuses(t *testing.T) {
	cases := []struct {
		name   string
		status PullStatus
	}{
		{"NO_NEW_MSG", PullNoNewMsg},
		{"NO_MATCHED_MSG", PullNoMatchedMsg},
		{"OFFSET_ILLEGAL", PullOffsetIllegal},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if warn := pullOffsetsSanityWarning(
				sanityStatus(tc.status, 9), sanityMQ, 10); warn != "" {
				t.Fatalf("%s is not a FOUND round and must not warn, got %q", tc.status, warn)
			}
			// Same backwards cursor, this time with a row list attached: the status
			// still decides, because these statuses hand nothing over.
			round := sanityStatus(tc.status, 9)
			round.MsgFoundList = []*common.MessageExt{sanityRow(3)}
			if warn := pullOffsetsSanityWarning(round, sanityMQ, 10); warn != "" {
				t.Fatalf("%s warned although it is not a FOUND round: %q", tc.status, warn)
			}
		})
	}
}

// ------------------------------------------------- 4. the loop-level pair of legs

// pullRequestOffsetsFor reads back the offset each PULL round of one queue asked for,
// oldest first — the cursor as the loop actually carried it from round to round.
func pullRequestOffsetsFor(b *consumerBroker, topic string, queueID int32) []int64 {
	var out []int64
	for _, p := range b.pullsFor(topic, queueID) {
		out = append(out, derefI64(p.QueueOffset))
	}
	return out
}

// pullRequestOffsetsWithin blocks until `want` rounds of one queue have arrived at the
// broker and returns their request points.
func pullRequestOffsetsWithin(t *testing.T, b *consumerBroker, topic string, queueID int32,
	want int, budget time.Duration) []int64 {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if got := pullRequestOffsetsFor(b, topic, queueID); len(got) >= want {
			return got[:want]
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := pullRequestOffsetsFor(b, topic, queueID)
	t.Fatalf("only %d pull rounds arrived in %s, want %d", len(got), budget, want)
	return nil
}

// storedPullCursor is the queue's cursor as the loop left it, read under the
// consumer's own lock.
func storedPullCursor(t *testing.T, c *DefaultMQPushConsumer, mq common.MessageQueue) int64 {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	offset, ok := c.offsetTable[mq]
	if !ok {
		t.Fatal("the pull loop stored no cursor for this queue")
	}
	return offset
}

// sanityIntervalMs is the pullInterval both legs run with: long enough that a round
// which stopped waiting shows up in the arrival span, short enough that the pair does
// not slow the suite down.
const sanityIntervalMs = 40

// The pair measures this many rounds, each of which hands over the two staged rows.
const sanityRounds = 4
const sanityRowsPerRound = 2

// pullRoundShape is one leg's rounds in the terms the pair compares. The stored
// cursor is deliberately not one of them: that is the number the two legs answer
// differently on purpose, and each leg checks it against its own reply.
type pullRoundShape struct {
	// rowsPerRound is the loop's own record (PULL_TPS value over its count) of how
	// many rows one round handed to the consume side.
	rowsPerRound int64
	// paced says pullInterval sat between the rounds.
	paced bool
	// booked says both pull counters ticked once per round.
	booked bool
}

// TestPullOffsetSanityWarningLeavesTheRoundUnchanged drives a warning round next to
// the consistent round it replaced. Both legs ask the same offset and get the same
// rows back; only the reply's next cursor differs, and only one leg's warning fires.
// Every other outcome of the round is compared across the legs, so a check that grew
// teeth — clamping the cursor, dropping the batch, re-pacing the round, skipping the
// counters — reddens here.
func TestPullOffsetSanityWarningLeavesTheRoundUnchanged(t *testing.T) {
	legs := []struct {
		name string
		// pinned is the nextBeginOffset every PULL answer carries, judged against the
		// round that asks at 0 and gets the rows at 0 and 1 back: -1 sits below that
		// request point, 0 is exactly at it.
		pinned int64
		warns  bool
	}{
		{"warning leg: the reply cursor is below the requested one", -1, true},
		{"control leg: the reply cursor is the requested one", 0, false},
	}
	shapes := make([]pullRoundShape, len(legs))
	for i, tc := range legs {
		i, tc := i, tc
		t.Run(tc.name, func(t *testing.T) {
			// Which leg is which, decided by the function the loop itself calls with
			// this leg's numbers. The line it renders is pinned above; what matters
			// here is everything the round still does afterwards.
			warned := pullOffsetsSanityWarning(sanityFound(tc.pinned, 0, 1), sanityMQ, 0) != ""
			if warned != tc.warns {
				t.Fatalf("a reply cursor of %d against the requested 0 warned = %v, want %v",
					tc.pinned, warned, tc.warns)
			}
			shapes[i] = runPullOffsetSanityLeg(t, tc.pinned)
		})
	}
	if shapes[0] != shapes[1] {
		t.Fatalf("the warned round and the identical consistent round came out different"+
			" — the sanity check has teeth: warning leg %+v, control leg %+v",
			shapes[0], shapes[1])
	}
}

// runPullOffsetSanityLeg runs one queue's pull loop against the mock cluster with
// every PULL answer carrying `pinned` as its nextBeginOffset, and returns the shape
// of the rounds that followed.
func runPullOffsetSanityLeg(t *testing.T, pinned int64) pullRoundShape {
	t.Helper()
	topic := uniqueTopic("PullOffsetSanity", t)
	const group = "GID_pull_offset_sanity"

	listener := newRecordingListener()
	f, c := newPullPacingCluster(t, group, topic, listener, sanityIntervalMs)
	stageRows(f.broker, topic, 0,
		taggedRow{"keep-1", "TagA"}, taggedRow{"keep-2", "TagA"})
	f.broker.replayPullFrom(pinned)
	startConsumer(t, c, topic, "TagA")

	arrivals := pullArrivalsWithin(t, f.broker, topic, 0, sanityRounds, 20*time.Second)
	mq := common.NewMessageQueue(topic, "b1", 0)

	// The cursor is the reply's, and every round after the first asked from exactly
	// there: a warning neither rolls the cursor back nor holds the loop at the old one.
	if got := storedPullCursor(t, c, mq); got != pinned {
		t.Fatalf("stored cursor = %d, want the reply's %d", got, pinned)
	}
	requested := pullRequestOffsetsWithin(t, f.broker, topic, 0, sanityRounds, 20*time.Second)
	if requested[0] != 0 {
		t.Fatalf("round 1 requested offset %d, want the resolved start 0", requested[0])
	}
	for i, offset := range requested[1:] {
		if offset != pinned {
			t.Fatalf("round %d requested offset %d, want the cursor the reply carried (%d)",
				i+2, offset, pinned)
		}
	}

	// Both staged rows reached the consume side, and nothing the queue never staged
	// came with them.
	waitFor(t, "both staged rows consumed", func() bool { return listener.receivedAll(sanityRowsPerRound) })
	for _, body := range listener.bodies() {
		if body != "keep-1" && body != "keep-2" {
			t.Fatalf("the listener was handed %q, which this queue never staged", body)
		}
	}

	// A round that handed rows over is paced by pullInterval, warned or not.
	span := arrivals[sanityRounds-1] - arrivals[0]
	minSpan := int64(sanityRounds-1) * (sanityIntervalMs - 10)
	paced := span >= minSpan
	if !paced {
		t.Fatalf("%d rounds spanned %dms, want at least %dms: this leg stopped waiting"+
			" pullInterval=%d between its delivered rounds",
			sanityRounds, span, minSpan, sanityIntervalMs)
	}

	// Both counters book a whole round, so they are read with the loop stopped: a
	// round's row count and its tally land back to back, and a read that slipped
	// between the two would describe a round that never happened. The stop only
	// closes a channel, so the last round in flight gets its own interval plus a
	// margin to finish booking.
	c.Shutdown()
	time.Sleep(3 * sanityIntervalMs * time.Millisecond)

	// Both counters ticked once per round: RT for the round trip, TPS for the list
	// the round handed over.
	stats := readPullRoundStats(t, c, topic)
	booked := stats.rtTimes >= sanityRounds && stats.tpsTimes >= sanityRounds
	if !booked {
		t.Fatalf("the rounds did not all book their counters over %d rounds (%s)",
			sanityRounds, stats)
	}
	// And every one of those rounds handed its whole batch over: the loop counts the
	// list it passes to the consume side, so rows and rounds stay in exactly this
	// ratio whether or not the warning fired.
	if stats.tpsSum != sanityRowsPerRound*stats.tpsTimes {
		t.Fatalf("the loop booked %d rows over %d FOUND rounds, want exactly %d per round:"+
			" a round lost rows on its way to the consume side (%s)",
			stats.tpsSum, stats.tpsTimes, sanityRowsPerRound, stats)
	}
	return pullRoundShape{
		rowsPerRound: stats.tpsSum / stats.tpsTimes,
		paced:        paced,
		booked:       booked,
	}
}
