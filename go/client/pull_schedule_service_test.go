// MQPullConsumerScheduleService tests: queue-dimension task expansion
// (CLUSTERING share vs BROADCASTING everything), the serial per-queue round
// chain, the three delay channels (callback return / PullMessageLater /
// SetPullInterval) and their priority, cursor following including the
// OFFSET_ILLEGAL correction, queue-set add/remove on route changes, and the
// shutdown drain — all against the in-process mock cluster from
// consumer_test.go, never a real one.
package client

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- fixtures

// schedRound is one recorded callback invocation.
type schedRound struct {
	at     time.Time
	mq     common.MessageQueue
	result *PullResult // nil when the pull failed
	err    error       // ctx.PullError() of the round
}

// schedRecorder is the PullTaskCallback under test. The optional script runs
// per round and its return value is the callback's return value (the delay
// channel with the highest priority).
type schedRecorder struct {
	mu     sync.Mutex
	rounds []schedRound
	script func(round int, mq common.MessageQueue, result *PullResult, ctx *PullTaskContext) int64

	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (r *schedRecorder) callback(mq common.MessageQueue, result *PullResult, ctx *PullTaskContext) int64 {
	n := r.inFlight.Add(1)
	for {
		max := r.maxInFlight.Load()
		if n <= max || r.maxInFlight.CompareAndSwap(max, n) {
			break
		}
	}
	r.mu.Lock()
	round := len(r.rounds) + 1
	var err error
	if ctx != nil {
		err = ctx.PullError()
	}
	r.rounds = append(r.rounds, schedRound{at: time.Now(), mq: mq, result: result, err: err})
	script := r.script
	r.mu.Unlock()
	delay := int64(0)
	if script != nil {
		delay = script(round, mq, result, ctx)
	}
	r.inFlight.Add(-1)
	return delay
}

func (r *schedRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.rounds)
}

func (r *schedRecorder) snapshot() []schedRound {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]schedRound(nil), r.rounds...)
}

func (r *schedRecorder) peakInFlight() int32 { return r.maxInFlight.Load() }

// newSchedService wires a schedule service (owning its consumer) to the
// fixture's nameserver with a fast re-expansion tick. Shutdown is registered
// as the cleanup and also shuts the consumer down.
func newSchedService(t *testing.T, f *clusterFixture, group, instName string) (*MQPullConsumerScheduleService, *DefaultMQPullConsumer) {
	t.Helper()
	c := MustNewDefaultMQPullConsumer(group)
	c.SetNameServerAddresses([]string{f.nserver.addr})
	if instName == "" {
		instName = f.instName
	}
	c.SetInstanceName(instName)
	svc := newMQPullConsumerScheduleService(c, true)
	svc.SetRebalanceIntervalMillis(100)
	t.Cleanup(svc.Shutdown)
	return svc, c
}

// startSchedService registers the recorder for the topic and starts.
//
// The fixture pre-seeds cidAll with its own guessed clientID, but a started
// consumer's real clientId carries the @STREAM mode suffix — so cidAll is
// repointed at the live ClientID() after Start, or the CLUSTERING rebalance
// logs "not in cidAll" and expands nothing.
func startSchedService(t *testing.T, f *clusterFixture, svc *MQPullConsumerScheduleService, topic string, rec *schedRecorder) {
	t.Helper()
	requireNoError(t, "register callback", svc.RegisterPullTaskCallback(topic, rec.callback))
	requireNoError(t, "schedule start", svc.Start())
	f.broker.setClientIDs(svc.PullConsumer().ClientID())
}

// pullOffsetsFor lists the queueOffsets of every pull the broker saw for one
// queue, in arrival order.
func pullOffsetsFor(f *clusterFixture, topic string, queueID int32) []int64 {
	headers := f.broker.pullsFor(topic, queueID)
	out := make([]int64, 0, len(headers))
	for _, h := range headers {
		out = append(out, derefI64(h.QueueOffset))
	}
	return out
}

// ---------------------------------------------------------------- delay resolution (pure)

// TestPullTaskContextDelayResolution pins the three reschedule channels and
// their priority: an explicit return value beats PullMessageLater, which beats
// the standing interval; PullMessageLater is single-shot; negatives clamp to
// "immediately".
func TestPullTaskContextDelayResolution(t *testing.T) {
	ctx := &PullTaskContext{pullInterval: DefaultPullTaskNextDelayMillis}

	// Channel 3 alone: the interval is the default when nothing else spoke.
	if got := ctx.resolveNextDelay(0); got != DefaultPullTaskNextDelayMillis {
		t.Fatalf("default resolution = %d, want %d", got, DefaultPullTaskNextDelayMillis)
	}
	// A non-positive return value never becomes a delay.
	if got := ctx.resolveNextDelay(-10); got != DefaultPullTaskNextDelayMillis {
		t.Errorf("resolveNextDelay(-10) = %d, want the interval %d", got, DefaultPullTaskNextDelayMillis)
	}

	// Priority: return value > PullMessageLater > SetPullInterval.
	ctx.SetPullInterval(100)
	ctx.PullMessageLater(500)
	if got := ctx.resolveNextDelay(300); got != 300 {
		t.Errorf("a positive return = %d, want 300 (the return wins over both channels)", got)
	}
	// The winning return CONSUMED the pending later: the interval applies now.
	if got := ctx.resolveNextDelay(0); got != 100 {
		t.Errorf("after the winning return = %d, want the interval 100", got)
	}

	// PullMessageLater beats the interval, once.
	ctx.PullMessageLater(500)
	if got := ctx.resolveNextDelay(0); got != 500 {
		t.Errorf("PullMessageLater resolution = %d, want 500", got)
	}
	if got := ctx.resolveNextDelay(0); got != 100 {
		t.Errorf("the second resolution = %d, want the interval 100 (later is single-shot)", got)
	}

	// Negatives clamp to "immediately" on both channels.
	ctx.PullMessageLater(-5)
	if got := ctx.resolveNextDelay(0); got != 0 {
		t.Errorf("a negative later = %d, want 0", got)
	}
	ctx.SetPullInterval(-5)
	if got := ctx.resolveNextDelay(0); got != 0 {
		t.Errorf("a negative interval = %d, want 0", got)
	}
	if got := ctx.PullInterval(); got != -5 {
		t.Errorf("PullInterval() = %d, want the stored -5 (clamping happens on resolution)", got)
	}
}

// ---------------------------------------------------------------- expansion

// Two schedule services in one CLUSTERING group must expand to disjoint
// shares: 4 queues, 2 instances, 2 each, covering everything exactly once.
func TestPullScheduleClusteringExpandsToOwnedShare(t *testing.T) {
	const topic = "PullSchedShareTopic"
	const group = "GID_pull_sched_share"
	f := newClusterFixture(t, map[string]int{topic: 4})

	svcA, cA := newSchedService(t, f, group, "")
	svcB, cB := newSchedService(t, f, group, uniqueSuffix(t))
	startSchedService(t, f, svcA, topic, &schedRecorder{})
	startSchedService(t, f, svcB, topic, &schedRecorder{})
	f.broker.setClientIDs(cA.ClientID(), cB.ClientID())

	waitFor(t, "a 2/2 expansion", func() bool {
		return len(svcA.TaskQueues()) == 2 && len(svcB.TaskQueues()) == 2
	})

	seen := map[int32]int{}
	for _, mq := range append(append([]common.MessageQueue(nil), svcA.TaskQueues()...), svcB.TaskQueues()...) {
		if mq.Topic != topic || mq.BrokerName != "b1" {
			t.Fatalf("task for a foreign queue %s/%s", mq.Topic, mq.BrokerName)
		}
		seen[mq.QueueID]++
	}
	if len(seen) != 4 {
		t.Fatalf("the two shares cover %v, want all 4 queues", seen)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("queue %d expanded by %d services — duplicate consumption", id, count)
		}
	}
}

// BROADCASTING expands to EVERY queue without ever asking the broker for the
// group member list.
func TestPullScheduleBroadcastingExpandsToEveryQueue(t *testing.T) {
	const topic = "PullSchedBcastTopic"
	f := newClusterFixture(t, map[string]int{topic: 3})
	f.broker.setClientIDs("someone-else@other")

	svc, _ := newSchedService(t, f, "GID_pull_sched_bcast", "")
	svc.SetMessageModel(MessageModelBroadcasting)
	if got := svc.MessageModel(); got != MessageModelBroadcasting {
		t.Fatalf("MessageModel = %q, want %q", got, MessageModelBroadcasting)
	}
	startSchedService(t, f, svc, topic, &schedRecorder{})

	waitFor(t, "the full 3-queue expansion", func() bool { return len(svc.TaskQueues()) == 3 })
	if got := queueIDs(svc.TaskQueues()); !int32SliceEqual(got, []int32{0, 1, 2}) {
		t.Errorf("expansion = %v, want [0 1 2]", got)
	}
	if idx := f.broker.indexOfReq(remoting.ReqGetConsumerListByGroup); idx >= 0 {
		t.Errorf("BROADCASTING asked for the group member list (request %d); it must not", idx)
	}
}

// ---------------------------------------------------------------- round timing

// The first round runs immediately; the delay the callback RETURNS is the gap
// before the next one — a round must not fire before its delay expired.
func TestPullScheduleHonoursTheReturnedDelay(t *testing.T) {
	const topic = "PullSchedReturnDelayTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	rec := &schedRecorder{script: func(int, common.MessageQueue, *PullResult, *PullTaskContext) int64 {
		return 600
	}}
	svc, _ := newSchedService(t, f, "GID_pull_sched_returndelay", "")
	startSchedService(t, f, svc, topic, rec)

	start := time.Now()
	waitFor(t, "two rounds", func() bool { return rec.count() >= 2 })
	rounds := rec.snapshot()
	if first := rounds[0].at.Sub(start); first > 2*time.Second {
		t.Errorf("the first round took %s to arrive, want it scheduled immediately", first)
	}
	if gap := rounds[1].at.Sub(rounds[0].at); gap < 550*time.Millisecond {
		t.Errorf("round gap = %s, want >= the returned 600ms (minus timer slack)", gap)
	} else if gap > 2500*time.Millisecond {
		t.Errorf("round gap = %s, want ~600ms, not a stalled chain", gap)
	}
}

// The SetPullInterval and PullMessageLater channels reschedule the round too:
// the interval applies while it stands, the later is a single-shot override,
// and after it is consumed the per-round default takes over again.
func TestPullScheduleIntervalAndLaterChannels(t *testing.T) {
	const topic = "PullSchedChannelsTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	rec := &schedRecorder{script: func(round int, _ common.MessageQueue, _ *PullResult, ctx *PullTaskContext) int64 {
		switch round {
		case 1:
			ctx.SetPullInterval(100) // a short standing gap for round 2
		case 2:
			ctx.PullMessageLater(700) // a single-shot delay for round 3
		}
		return 0 // never the return-value channel here
	}}
	svc, _ := newSchedService(t, f, "GID_pull_sched_channels", "")
	startSchedService(t, f, svc, topic, rec)

	waitFor(t, "four rounds", func() bool { return rec.count() >= 4 })
	rounds := rec.snapshot()
	if gap := rounds[1].at.Sub(rounds[0].at); gap < 90*time.Millisecond || gap > 500*time.Millisecond {
		t.Errorf("SetPullInterval(100) gap = %s, want ~100ms", gap)
	}
	if gap := rounds[2].at.Sub(rounds[1].at); gap < 650*time.Millisecond {
		t.Errorf("PullMessageLater(700) gap = %s, want >= ~700ms", gap)
	}
	// Single-shot: round 4 falls back to the per-round default (200ms), NOT
	// another 700ms.
	if gap := rounds[3].at.Sub(rounds[2].at); gap < 150*time.Millisecond || gap > 550*time.Millisecond {
		t.Errorf("post-later gap = %s, want ~the 200ms default (later is single-shot)", gap)
	}
}

// One queue's rounds are SERIAL: even with a delay of ~0 and a slow callback,
// two rounds of the same queue must never be in flight at the same time.
func TestPullScheduleSameQueueNeverConcurrent(t *testing.T) {
	const topic = "PullSchedSerialTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	rec := &schedRecorder{script: func(int, common.MessageQueue, *PullResult, *PullTaskContext) int64 {
		time.Sleep(20 * time.Millisecond)
		return 1
	}}
	svc, _ := newSchedService(t, f, "GID_pull_sched_serial", "")
	startSchedService(t, f, svc, topic, rec)

	waitFor(t, "at least six rounds", func() bool { return rec.count() >= 6 })
	if peak := rec.peakInFlight(); peak != 1 {
		t.Errorf("peak in-flight rounds on one queue = %d, want exactly 1", peak)
	}
}

// ---------------------------------------------------------------- cursor

// The cursor starts at the broker's maxOffset (no committed offset), follows
// result.NextBeginOffset over FOUND batches, and every round parks the position
// into the offset store.
func TestPullScheduleCursorFollowsNextBeginOffset(t *testing.T) {
	const topic = "PullSchedCursorTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "m0", "m1", "m2")
	rec := &schedRecorder{}
	svc, c := newSchedService(t, f, "GID_pull_sched_cursor", "")
	startSchedService(t, f, svc, topic, rec)
	mq := common.NewMessageQueue(topic, "b1", 0)

	waitFor(t, "the first round", func() bool { return rec.count() >= 1 })
	first := rec.snapshot()[0]
	if first.err != nil {
		t.Fatalf("first round failed: %v", first.err)
	}
	if first.result.PullStatus != PullNoNewMsg {
		t.Errorf("first round status = %s, want NO_NEW_MSG (a fresh cursor starts at maxOffset)",
			first.result.PullStatus)
	}
	if offsets := pullOffsetsFor(f, topic, 0); len(offsets) == 0 || offsets[0] != 3 {
		t.Fatalf("first pull offset = %v, want it to start at maxOffset 3", offsets)
	}

	f.broker.add(topic, 0, "n0", "n1")
	waitFor(t, "the FOUND round", func() bool {
		for _, r := range rec.snapshot() {
			if r.result != nil && r.result.PullStatus == PullFound {
				return true
			}
		}
		return false
	})
	var found schedRound
	for _, r := range rec.snapshot() {
		if r.result != nil && r.result.PullStatus == PullFound {
			found = r
			break
		}
	}
	bodies := make([]string, 0, len(found.result.MsgFoundList))
	for _, msg := range found.result.MsgFoundList {
		bodies = append(bodies, string(msg.Body))
	}
	if !equalStrings(bodies, []string{"n0", "n1"}) {
		t.Errorf("FOUND round delivered %v, want [n0 n1] (m0..m2 are behind the start cursor)", bodies)
	}
	if found.result.NextBeginOffset != 5 {
		t.Errorf("nextBeginOffset = %d, want 5", found.result.NextBeginOffset)
	}
	// The FOUND round must have been requested AT the parked cursor (3), not
	// re-resolved from scratch.
	offsets := pullOffsetsFor(f, topic, 0)
	seenThree := false
	for _, o := range offsets {
		if o == 3 {
			seenThree = true
		}
	}
	if !seenThree {
		t.Errorf("no pull at the followed cursor 3 in %v", offsets)
	}
	waitFor(t, "the store to carry the cursor", func() bool {
		got, err := c.FetchConsumeOffset(mq, false)
		return err == nil && got == 5
	})
}

// An OFFSET_ILLEGAL answer carries the broker's corrected position and the very
// next round must pull AT it — this single follow is the whole correction.
func TestPullScheduleOffsetIllegalCorrectsCursor(t *testing.T) {
	const topic = "PullSchedIllegalTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	corrected := int64(42)
	f.broker.mu.Lock()
	f.broker.pullCode = remoting.RespPullOffsetMoved
	f.broker.nextBegin = &corrected
	f.broker.mu.Unlock()
	t.Cleanup(func() {
		f.broker.mu.Lock()
		f.broker.pullCode = 0
		f.broker.nextBegin = nil
		f.broker.mu.Unlock()
	})

	rec := &schedRecorder{}
	svc, c := newSchedService(t, f, "GID_pull_sched_illegal", "")
	startSchedService(t, f, svc, topic, rec)
	mq := common.NewMessageQueue(topic, "b1", 0)

	waitFor(t, "the OFFSET_ILLEGAL round", func() bool {
		rounds := rec.snapshot()
		return len(rounds) > 0 && rounds[0].result != nil &&
			rounds[0].result.PullStatus == PullOffsetIllegal
	})
	waitFor(t, "the corrected offset on the wire", func() bool {
		for _, o := range pullOffsetsFor(f, topic, 0) {
			if o == corrected {
				return true
			}
		}
		return false
	})
	waitFor(t, "the corrected offset in the store", func() bool {
		got, err := c.FetchConsumeOffset(mq, false)
		return err == nil && got == corrected
	})
}

// A FAILED pull hands the error to the callback through ctx.PullError() with a
// nil result — and the chain survives: rounds keep coming after the failure
// clears.
func TestPullSchedulePullErrorReachesCallbackAndChainSurvives(t *testing.T) {
	const topic = "PullSchedPullErrTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.mu.Lock()
	f.broker.pullCode = remoting.RespSystemError
	f.broker.mu.Unlock()

	rec := &schedRecorder{}
	svc, _ := newSchedService(t, f, "GID_pull_sched_pullerr", "")
	startSchedService(t, f, svc, topic, rec)

	waitFor(t, "the failed round", func() bool {
		rounds := rec.snapshot()
		return len(rounds) > 0 && rounds[0].err != nil && rounds[0].result == nil
	})
	f.broker.mu.Lock()
	f.broker.pullCode = 0
	f.broker.mu.Unlock()

	waitFor(t, "a healthy round after the failure", func() bool {
		for _, r := range rec.snapshot() {
			if r.err == nil && r.result != nil {
				return true
			}
		}
		return false
	})
}

// ---------------------------------------------------------------- queue set changes

// The expansion follows the route: a scale-out adds task chains, a scale-in
// revokes them, and a revoked queue stops being pulled.
func TestPullScheduleQueueSetGrowsAndShrinks(t *testing.T) {
	const topic = "PullSchedScaleTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	rec := &schedRecorder{}
	svc, _ := newSchedService(t, f, "GID_pull_sched_scale", "")
	startSchedService(t, f, svc, topic, rec)

	waitFor(t, "the 2-queue expansion", func() bool { return len(svc.TaskQueues()) == 2 })

	f.scaleTopic(t, topic, 4)
	waitFor(t, "the 4-queue expansion", func() bool { return len(svc.TaskQueues()) == 4 })
	if got := queueIDs(svc.TaskQueues()); !int32SliceEqual(got, []int32{0, 1, 2, 3}) {
		t.Fatalf("expansion = %v, want [0 1 2 3]", got)
	}

	f.scaleTopic(t, topic, 1)
	waitFor(t, "the 1-queue contraction", func() bool {
		got := svc.TaskQueues()
		return len(got) == 1 && got[0].QueueID == 0
	})

	// Let a possibly in-flight round of the revoked queue land, then prove the
	// revoked chain is really dead: no new pulls for queue 3.
	time.Sleep(300 * time.Millisecond)
	before := len(f.broker.pullsFor(topic, 3))
	time.Sleep(500 * time.Millisecond)
	if after := len(f.broker.pullsFor(topic, 3)); after != before {
		t.Errorf("revoked queue 3 kept pulling: %d -> %d", before, after)
	}
	// The surviving queue keeps its chain alive.
	aliveBefore := len(f.broker.pullsFor(topic, 0))
	time.Sleep(400 * time.Millisecond)
	if len(f.broker.pullsFor(topic, 0)) <= aliveBefore {
		t.Error("queue 0 stopped pulling after the contraction; the surviving chain must keep running")
	}
}

// Unregistering a callback revokes the topic's tasks right away instead of
// waiting for the next expansion tick.
func TestPullScheduleUnregisterRevokesTasks(t *testing.T) {
	const topic = "PullSchedUnregTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	rec := &schedRecorder{}
	svc, _ := newSchedService(t, f, "GID_pull_sched_unreg", "")
	startSchedService(t, f, svc, topic, rec)

	waitFor(t, "the expansion", func() bool { return len(svc.TaskQueues()) == 1 })
	if got := svc.CallbackTopics(); len(got) != 1 || got[0] != topic {
		t.Fatalf("CallbackTopics = %v, want [%s]", got, topic)
	}

	requireNoError(t, "unregister", svc.RegisterPullTaskCallback(topic, nil))
	waitFor(t, "the revocation", func() bool { return len(svc.TaskQueues()) == 0 })
	if got := svc.CallbackTopics(); len(got) != 0 {
		t.Errorf("CallbackTopics after unregister = %v, want empty", got)
	}
	// A revoked chain stays revoked: the periodic tick must not resurrect it.
	rounds := rec.count()
	time.Sleep(400 * time.Millisecond)
	if rec.count() != rounds {
		t.Errorf("rounds kept coming after the unregister: %d -> %d", rounds, rec.count())
	}
}

// ---------------------------------------------------------------- shutdown

// Shutdown must WAIT for a callback that is still running: the round it owns
// has to finish before the consumer underneath it goes away.
func TestPullScheduleShutdownDrainsRunningCallback(t *testing.T) {
	const topic = "PullSchedDrainTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	finished := make(chan struct{}, 1)
	rec := &schedRecorder{script: func(round int, _ common.MessageQueue, _ *PullResult, _ *PullTaskContext) int64 {
		if round == 1 {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			select {
			case finished <- struct{}{}:
			default:
			}
		}
		return 0
	}}
	svc, _ := newSchedService(t, f, "GID_pull_sched_drain", "")
	startSchedService(t, f, svc, topic, rec)

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the callback never ran")
	}

	done := make(chan struct{})
	go func() {
		svc.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Shutdown returned while a callback was still running")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after the callback finished")
	}
	select {
	case <-finished:
	default:
		t.Error("the drained callback never completed its body")
	}
	if svc.IsStarted() {
		t.Error("IsStarted = true after Shutdown")
	}
	if got := svc.TaskQueues(); len(got) != 0 {
		t.Errorf("tasks after Shutdown = %v, want none", got)
	}
	// No pulls may follow the drain.
	before := f.broker.pullCount()
	time.Sleep(300 * time.Millisecond)
	if after := f.broker.pullCount(); after != before {
		t.Errorf("pulls kept coming after Shutdown: %d -> %d", before, after)
	}
}

// A callback that never returns must not hang the process: the drain is
// bounded by the shared shutdown budget.
func TestPullScheduleShutdownDrainIsBounded(t *testing.T) {
	prev := shutdownDrainBudget
	shutdownDrainBudget = 100 * time.Millisecond
	t.Cleanup(func() { shutdownDrainBudget = prev })

	const topic = "PullSchedDrainBudgetTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	rec := &schedRecorder{script: func(round int, _ common.MessageQueue, _ *PullResult, _ *PullTaskContext) int64 {
		if round == 1 {
			<-hang
		}
		return 0
	}}
	svc, _ := newSchedService(t, f, "GID_pull_sched_drain_budget", "")
	startSchedService(t, f, svc, topic, rec)

	waitFor(t, "the hung round to start", func() bool { return rec.count() >= 1 })
	begin := time.Now()
	svc.Shutdown()
	if elapsed := time.Since(begin); elapsed > 3*time.Second {
		t.Fatalf("Shutdown took %s with a hung callback; want it bounded by the drain budget", elapsed)
	}
}

// A service that is started AGAIN after a Shutdown must expand and stop again:
// the second Shutdown has to be able to close the NEW stop channel, or the
// rebalance loop of the second life never exits.
func TestPullScheduleRestartAfterShutdown(t *testing.T) {
	const topic = "PullSchedRestartTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	rec := &schedRecorder{}
	svc, _ := newSchedService(t, f, "GID_pull_sched_restart", "")
	startSchedService(t, f, svc, topic, rec)

	waitFor(t, "the first-life expansion", func() bool { return len(svc.TaskQueues()) == 1 })
	waitFor(t, "the first-life rounds", func() bool { return rec.count() >= 1 })
	svc.Shutdown()
	if svc.IsStarted() {
		t.Fatal("IsStarted = true after Shutdown")
	}

	requireNoError(t, "restart", svc.Start())
	waitFor(t, "the second-life expansion", func() bool { return len(svc.TaskQueues()) == 1 })
	waitFor(t, "the second-life rounds", func() bool { return rec.count() >= 2 })

	// The regression this pins: without a fresh stop gate per Start the second
	// Shutdown blocks forever in loopWG.Wait.
	done := make(chan struct{})
	go func() {
		svc.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the second Shutdown hung — the restarted service could not stop")
	}
}

// ---------------------------------------------------------------- guards

// The start and register guards: no consumer means no start, an invalid topic
// is refused, and a nil consumer refuses registration.
func TestPullScheduleGuards(t *testing.T) {
	svc := newMQPullConsumerScheduleService(nil, false)
	if err := svc.Start(); err == nil {
		t.Error("Start without a consumer = nil error, want the refusal")
	}
	if got := svc.MessageModel(); got != "" {
		t.Errorf("MessageModel without a consumer = %q, want empty", got)
	}
	if err := svc.RegisterPullTaskCallback("", func(common.MessageQueue, *PullResult, *PullTaskContext) int64 {
		return 0
	}); err == nil {
		t.Error("RegisterPullTaskCallback with an empty topic = nil error, want the refusal")
	}

	// Configuration round-trips on a fresh service.
	svc2, _ := newSchedService(t, newClusterFixture(t, nil), "GID_pull_sched_guards", "")
	svc2.SetPullThreadNums(3)
	if got := svc2.PullThreadNums(); got != 3 {
		t.Errorf("PullThreadNums = %d, want 3", got)
	}
	svc2.SetRebalanceIntervalMillis(500)
	if got := svc2.RebalanceIntervalMillis(); got != 500 {
		t.Errorf("RebalanceIntervalMillis = %d, want 500", got)
	}
	svc2.SetPullBatchSize(7)
	if got := svc2.PullBatchSize(); got != 7 {
		t.Errorf("PullBatchSize = %d, want 7", got)
	}
	svc2.SetPullBatchSize(0) // ignored
	if got := svc2.PullBatchSize(); got != 7 {
		t.Errorf("PullBatchSize after a 0 set = %d, want it kept at 7", got)
	}
	if svc2.IsStarted() {
		t.Error("a fresh service must not report started")
	}
	// A Shutdown before any Start is a silent no-op.
	svc2.Shutdown()
}
