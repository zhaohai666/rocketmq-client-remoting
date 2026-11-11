// Pull-side rebalance (Java `DefaultMQPullConsumer.enableRebalance`).
//
// Java's pull consumer is not the "no rebalance" shape this port used to have:
// the shared RebalanceService thread calls mqClientFactory.doRebalance() every
// `rocketmq.client.rebalance.waitInterval` (20s), which walks the registered
// consumers and calls tryRebalance(). That method gates on `enableRebalance`
// (default true) and then runs rebalanceImpl.doRebalance(false) -> allocate ->
// messageQueueChanged(topic, mqAll, mqDivided).
//
// The assertions below pin the three things that are easy to get wrong:
//
//   - enableRebalance == false must still report BALANCED (tryRebalance returns
//     true at DefaultMQPullConsumerImpl:398-402) -- reporting "unbalanced" would
//     make the loop retry at minInterval forever and burn a round per second.
//   - the listener fires only when the allocation changed, and receives
//     (topic, mqAll, mqDivided) -- the same three arguments
//     RebalancePullImpl:44-53 passes.
//   - a failing listener must not escape: Java logs and swallows it
//     (RebalancePullImpl:49-51) so one bad callback cannot kill the loop.
package client

import (
	"strings"
	"sync"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// pullRebCids are the members the mock broker reports for the group. The
// consumers under test set their clientId explicitly so the sorted cid list and
// therefore the expected share is deterministic.
var pullRebCids = []string{"cid-A", "cid-B"}

// newStartedPullRebConsumer wires, subscribes and starts a pull consumer whose
// clientId and group membership are fixed by the test.
func newStartedPullRebConsumer(t *testing.T, f *clusterFixture, group, clientID, topic string,
	listener MessageQueueListener, opts ...func(*DefaultMQPullConsumer)) *DefaultMQPullConsumer {
	t.Helper()
	return newStartedPullRebConsumerWithCids(t, f, group, clientID, topic, pullRebCids, listener, opts...)
}

// newStartedPullRebConsumerWithCids is the explicit-membership form, for tests
// where this instance must be the only member of the group.
//
// The clientIds are suffixed with the test name: the instance registry is keyed
// by clientId, and two tests reusing a bare "cid-A" would otherwise share one
// instance (and its nameserver/broker) when a previous fixture has not torn
// down yet. Consumers created by two calls in the SAME test share the suffix,
// so they still see each other's membership.
func newStartedPullRebConsumerWithCids(t *testing.T, f *clusterFixture, group, clientID, topic string,
	cids []string, listener MessageQueueListener, opts ...func(*DefaultMQPullConsumer)) *DefaultMQPullConsumer {
	t.Helper()
	suffix := "-" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	clientID += suffix
	members := make([]string, len(cids))
	for i, cid := range cids {
		members[i] = cid + suffix
	}
	c := newPullConsumer(t, f, group)
	c.SetClientID(clientID)
	if listener != nil {
		c.SetMessageQueueListener(listener)
	}
	for _, opt := range opts {
		opt(c)
	}
	requireNoError(t, "subscribe", c.Subscribe(topic, "*"))
	f.broker.setClientIDs(members...)
	startPullConsumer(t, c)
	return c
}

func pullRebIDs(mqs []common.MessageQueue) []int32 {
	ids := make([]int32, len(mqs))
	for i, mq := range mqs {
		ids[i] = mq.QueueID
	}
	return ids
}

func pullRebEqualIDs(got []common.MessageQueue, want ...int32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i].QueueID != want[i] {
			return false
		}
	}
	return true
}

// pullRebRefreshRoute applies a nameserver route change to the running
// consumer's cache (the periodic route refresh lives in the instance).
func pullRebRefreshRoute(t *testing.T, c *DefaultMQPullConsumer, topic string) {
	t.Helper()
	if _, err := c.Instance().UpdateTopicRouteInfoFromNameServer(topic, 0, false); err != nil {
		t.Fatalf("route refresh for %s: %v", topic, err)
	}
}

// ---------------------------------------------------------------- recorders

type pullRebCall struct {
	topic   string
	all     []common.MessageQueue
	divided []common.MessageQueue
}

type pullRebRecorder struct {
	mu    sync.Mutex
	calls []pullRebCall
}

func (r *pullRebRecorder) MessageQueueChanged(topic string, mqAll, mqDivided []common.MessageQueue) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, pullRebCall{
		topic:   topic,
		all:     append([]common.MessageQueue(nil), mqAll...),
		divided: append([]common.MessageQueue(nil), mqDivided...),
	})
}

func (r *pullRebRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *pullRebRecorder) snapshot() []pullRebCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]pullRebCall(nil), r.calls...)
}

func (r *pullRebRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

// quiescePullRebalance stops the background loop and clears its product so a
// test can drive doRebalance directly and count exactly the rounds it starts.
// Start's connection setup can fire a channel-active wake, and that round would
// otherwise be mistaken for the test's first round.
func quiescePullRebalance(c *DefaultMQPullConsumer, rec *pullRebRecorder) {
	c.stopRebalanceLoop()
	c.mu.Lock()
	c.rebalanceTable = map[string][]common.MessageQueue{}
	c.mu.Unlock()
	if rec != nil {
		rec.reset()
	}
}

type pullRebBoom struct{}

func (pullRebBoom) MessageQueueChanged(string, []common.MessageQueue, []common.MessageQueue) {
	panic("listener exploded")
}

// pullRebCountingStrategy wraps the real strategy so a test can prove it was
// (not) consulted.
type pullRebCountingStrategy struct {
	inner AllocateMessageQueueStrategy
	mu    sync.Mutex
	calls int
}

func (s *pullRebCountingStrategy) Name() string { return s.inner.Name() }

func (s *pullRebCountingStrategy) Allocate(group, currentCID string,
	mqAll []common.MessageQueue, cidAll []string) []common.MessageQueue {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.inner.Allocate(group, currentCID, mqAll, cidAll)
}

func (s *pullRebCountingStrategy) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// ---------------------------------------------------------------- the switch

func TestPullRebalanceEnableDefaultsToJavaTrue(t *testing.T) {
	// Java DefaultMQPullConsumer.java:97 `private boolean enableRebalance = true`.
	c := MustNewDefaultMQPullConsumer("GID_pull_reb_switch")
	if !c.IsEnableRebalance() {
		t.Error("enableRebalance defaults to false, want true")
	}
}

func TestPullRebalanceEnableSetterRoundTrips(t *testing.T) {
	c := MustNewDefaultMQPullConsumer("GID_pull_reb_switch")
	c.SetEnableRebalance(false)
	if c.IsEnableRebalance() {
		t.Error("IsEnableRebalance = true after SetEnableRebalance(false)")
	}
	c.SetEnableRebalance(true)
	if !c.IsEnableRebalance() {
		t.Error("IsEnableRebalance = false after SetEnableRebalance(true)")
	}
}

func TestPullRebalanceWaitAndMinIntervalAreConfigurableAndClamped(t *testing.T) {
	c := MustNewDefaultMQPullConsumer("GID_pull_reb_switch")
	if got := c.RebalanceWaitMillis(); got != 20000 {
		t.Errorf("RebalanceWaitMillis = %d, want 20000", got)
	}
	if got := c.RebalanceMinIntervalMillis(); got != 1000 {
		t.Errorf("RebalanceMinIntervalMillis = %d, want 1000", got)
	}
	c.SetRebalanceWaitMillis(0)
	c.SetRebalanceMinIntervalMillis(-5)
	if got := c.RebalanceWaitMillis(); got != 1 {
		t.Errorf("RebalanceWaitMillis = %d, want 1 (a zero period would spin the loop)", got)
	}
	if got := c.RebalanceMinIntervalMillis(); got != 1 {
		t.Errorf("RebalanceMinIntervalMillis = %d, want 1", got)
	}
}

// ---------------------------------------------------------------- disabled

func TestPullRebalanceDisabledReportsBalancedAndNeverAllocates(t *testing.T) {
	const topic = "PullRebDisabledTopic"
	f := newClusterFixture(t, map[string]int{topic: 4})
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_disabled", "cid-A", topic, nil)
	quiescePullRebalance(c, nil)
	counting := &pullRebCountingStrategy{inner: c.allocateStrategy}
	c.SetAllocateMessageQueueStrategy(counting)
	c.SetEnableRebalance(false)

	// Java tryRebalance returns true when the switch is off (:398-402).
	if !c.doRebalance() {
		t.Error("doRebalance = false with rebalance off, want true (nothing to retry)")
	}
	if got := counting.count(); got != 0 {
		t.Errorf("strategy consulted %d times with rebalance off, want 0", got)
	}
	if table := c.GetRebalanceTable(); len(table) != 0 {
		t.Errorf("rebalance table = %v with rebalance off, want empty", table)
	}
}

func TestPullRebalanceDisabledKeepsFetchOnTheSpot(t *testing.T) {
	const topic = "PullRebDisabledFetchTopic"
	f := newClusterFixture(t, map[string]int{topic: 4})
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_disabled_fetch", "cid-A", topic, nil)
	quiescePullRebalance(c, nil)

	// A stale table must be ignored while the switch is off.
	c.mu.Lock()
	c.rebalanceTable[topic] = []common.MessageQueue{}
	c.mu.Unlock()
	c.SetEnableRebalance(false)

	got, err := c.FetchMessageQueuesInBalance(topic)
	requireNoError(t, "FetchMessageQueuesInBalance", err)
	if !pullRebEqualIDs(got, 0, 1) {
		t.Errorf("share = %v, want the on-the-spot averaging [0 1]", pullRebIDs(got))
	}
}

// ---------------------------------------------------------------- broadcasting

func TestPullRebalanceBroadcastingFirstRoundNotifiesAll(t *testing.T) {
	const topic = "PullRebBroadcastTopic"
	f := newClusterFixture(t, map[string]int{topic: 3})
	rec := &pullRebRecorder{}
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_broadcast", "cid-A", topic, rec,
		func(c *DefaultMQPullConsumer) { c.SetMessageModel(MessageModelBroadcasting) })
	quiescePullRebalance(c, rec)

	if c.doRebalance() {
		t.Error("first round = balanced, want false (the empty table changed)")
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("notifications = %d, want 1", len(calls))
	}
	if calls[0].topic != topic {
		t.Errorf("topic = %q, want %q", calls[0].topic, topic)
	}
	if !pullRebEqualIDs(calls[0].all, 0, 1, 2) || !pullRebEqualIDs(calls[0].divided, 0, 1, 2) {
		t.Errorf("mqAll/mqDivided = %v/%v, want [0 1 2]/[0 1 2]",
			pullRebIDs(calls[0].all), pullRebIDs(calls[0].divided))
	}
}

func TestPullRebalanceBroadcastingSecondRoundIsBalancedAndSilent(t *testing.T) {
	const topic = "PullRebBroadcastTopic"
	f := newClusterFixture(t, map[string]int{topic: 3})
	rec := &pullRebRecorder{}
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_broadcast", "cid-A", topic, rec,
		func(c *DefaultMQPullConsumer) { c.SetMessageModel(MessageModelBroadcasting) })
	quiescePullRebalance(c, rec)

	c.doRebalance()
	if !c.doRebalance() {
		t.Error("second round = unbalanced, want true (nothing changed)")
	}
	if got := rec.count(); got != 1 {
		t.Errorf("notifications = %d, want 1 (fire only on a real change)", got)
	}
}

// ---------------------------------------------------------------- clustering

func TestPullRebalanceClusteringTakesItsShare(t *testing.T) {
	const topic = "PullRebClusterTopic"
	f := newClusterFixture(t, map[string]int{topic: 4})
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_cluster", "cid-A", topic, nil)
	quiescePullRebalance(c, nil)

	c.doRebalance()
	got := c.GetRebalanceTable()[topic]
	if !pullRebEqualIDs(got, 0, 1) {
		t.Errorf("share = %v, want [0 1] (averagely: cid-A sorted first takes the first half)",
			pullRebIDs(got))
	}
}

func TestPullRebalanceClusteringTwoInstancesGetDisjointShares(t *testing.T) {
	const topic = "PullRebClusterTwoTopic"
	f := newClusterFixture(t, map[string]int{topic: 5})
	a := newStartedPullRebConsumer(t, f, "GID_pull_reb_two", "cid-A", topic, nil)
	b := newStartedPullRebConsumer(t, f, "GID_pull_reb_two", "cid-B", topic, nil)
	quiescePullRebalance(a, nil)
	quiescePullRebalance(b, nil)

	a.doRebalance()
	b.doRebalance()
	sa := a.GetRebalanceTable()[topic]
	sb := b.GetRebalanceTable()[topic]
	seen := map[int32]int{}
	for _, mq := range append(append([]common.MessageQueue(nil), sa...), sb...) {
		seen[mq.QueueID]++
	}
	for qid, n := range seen {
		if n != 1 {
			t.Errorf("queue %d owned %d times, want exactly 1 (%v / %v)", qid, n,
				pullRebIDs(sa), pullRebIDs(sb))
		}
	}
	if len(seen) != 5 {
		t.Errorf("together they own %d queues, want all 5 (%v / %v)", len(seen),
			pullRebIDs(sa), pullRebIDs(sb))
	}
}

func TestPullRebalanceNewQueueTriggersExactlyOneNotification(t *testing.T) {
	const topic = "PullRebGrowTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	rec := &pullRebRecorder{}
	// One member: this instance owns every queue, so the grown set is visible
	// in mqDivided as well as mqAll.
	c := newStartedPullRebConsumerWithCids(t, f, "GID_pull_reb_grow", "cid-A", topic,
		[]string{"cid-A"}, rec)
	quiescePullRebalance(c, rec)

	c.doRebalance()
	if got := rec.count(); got != 1 {
		t.Fatalf("notifications after the first round = %d, want 1", got)
	}

	f.scaleTopic(t, topic, 3)
	pullRebRefreshRoute(t, c, topic)
	if c.doRebalance() {
		t.Error("round after a queue was added = balanced, want false")
	}
	calls := rec.snapshot()
	if len(calls) != 2 {
		t.Fatalf("notifications = %d, want 2", len(calls))
	}
	last := calls[1]
	if !pullRebEqualIDs(last.all, 0, 1, 2) || !pullRebEqualIDs(last.divided, 0, 1, 2) {
		t.Errorf("mqAll/mqDivided = %v/%v, want [0 1 2]/[0 1 2]",
			pullRebIDs(last.all), pullRebIDs(last.divided))
	}
}

func TestPullRebalanceMissingConsumerIDListIsBalancedAndSilent(t *testing.T) {
	const topic = "PullRebNoCidTopic"
	f := newClusterFixture(t, map[string]int{topic: 3})
	rec := &pullRebRecorder{}
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_no_cid", "cid-A", topic, rec)
	quiescePullRebalance(c, rec)
	// The broker answers GET_CONSUMER_LIST_BY_GROUP with an empty list.
	f.broker.setClientIDs()

	// Java (:300-302) only warns; it does not notify and leaves balanced true.
	if !c.doRebalance() {
		t.Error("doRebalance = false without a consumer list, want true")
	}
	if got := rec.count(); got != 0 {
		t.Errorf("notifications = %d without a consumer list, want 0 (keep the old allocation)", got)
	}
	if table := c.GetRebalanceTable(); len(table) != 0 {
		t.Errorf("rebalance table = %v, want empty", table)
	}
}

func TestPullRebalanceVanishedTopicNotifiesEmptyAndStaysBalanced(t *testing.T) {
	const topic = "PullRebVanishTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	rec := &pullRebRecorder{}
	c := newStartedPullRebConsumerWithCids(t, f, "GID_pull_reb_vanish", "cid-A", topic,
		[]string{"cid-A"}, rec)
	quiescePullRebalance(c, rec)

	c.doRebalance()
	if got := rec.count(); got != 1 {
		t.Fatalf("notifications after the first round = %d, want 1", got)
	}

	f.scaleTopic(t, topic, 0)
	pullRebRefreshRoute(t, c, topic)
	// Java RebalanceImpl:276-294 leaves balanced at its initial true, so the
	// loop waits the full interval instead of hammering minInterval while the
	// topic is briefly gone.
	if !c.doRebalance() {
		t.Error("round with the route gone = unbalanced, want true (Java :276-294)")
	}
	table := c.GetRebalanceTable()
	if got, ok := table[topic]; !ok || len(got) != 0 {
		t.Errorf("table[%s] = %v (present=%v), want a present empty set", topic, pullRebIDs(got), ok)
	}
	calls := rec.snapshot()
	if len(calls) != 2 {
		t.Fatalf("notifications = %d, want 2", len(calls))
	}
	if len(calls[1].all) != 0 || len(calls[1].divided) != 0 {
		t.Errorf("mqAll/mqDivided = %v/%v, want two empty sets",
			pullRebIDs(calls[1].all), pullRebIDs(calls[1].divided))
	}
}

// ---------------------------------------------------------------- listener safety

func TestPullRebalanceThrowingListenerDoesNotEscape(t *testing.T) {
	const topic = "PullRebBoomTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	c := newStartedPullRebConsumerWithCids(t, f, "GID_pull_reb_boom", "cid-A", topic,
		[]string{"cid-A"}, pullRebBoom{})
	quiescePullRebalance(c, nil)

	// Java swallows the exception (RebalancePullImpl:49-51); the call must
	// simply return, and the table must still be updated.
	if c.doRebalance() {
		t.Error("first round = balanced, want false")
	}
	if got := c.GetRebalanceTable()[topic]; len(got) != 2 {
		t.Errorf("table = %v, want the 2 allocated queues", pullRebIDs(got))
	}
}

func TestPullRebalanceSameQueuesInAnotherOrderDoNotNotify(t *testing.T) {
	const topic = "PullRebOrderTopic"
	f := newClusterFixture(t, map[string]int{topic: 3})
	rec := &pullRebRecorder{}
	c := newStartedPullRebConsumerWithCids(t, f, "GID_pull_reb_order", "cid-A", topic,
		[]string{"cid-A"}, rec)
	quiescePullRebalance(c, rec)

	c.doRebalance()
	if got := rec.count(); got != 1 {
		t.Fatalf("notifications after the first round = %d, want 1", got)
	}

	// Same set, reversed order: not a change.
	reversed := []common.MessageQueue{
		common.NewMessageQueue(topic, "b1", 2),
		common.NewMessageQueue(topic, "b1", 1),
		common.NewMessageQueue(topic, "b1", 0),
	}
	c.mu.Lock()
	c.rebalanceTable[topic] = reversed
	c.mu.Unlock()

	if !c.doRebalance() {
		t.Error("same set in another order = unbalanced, want true")
	}
	if got := rec.count(); got != 1 {
		t.Errorf("notifications = %d, want 1", got)
	}
}

// ---------------------------------------------------------------- fetch uses the table

func TestPullRebalanceFetchPrefersTheTable(t *testing.T) {
	const topic = "PullRebTableWinsTopic"
	f := newClusterFixture(t, map[string]int{topic: 4})
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_table_wins", "cid-A", topic, nil)
	quiescePullRebalance(c, nil)

	c.mu.Lock()
	c.rebalanceTable[topic] = []common.MessageQueue{common.NewMessageQueue(topic, "b1", 3)}
	c.mu.Unlock()

	got, err := c.FetchMessageQueuesInBalance(topic)
	requireNoError(t, "FetchMessageQueuesInBalance", err)
	if !pullRebEqualIDs(got, 3) {
		t.Errorf("share = %v, want [3] (the background rebalance is authoritative)", pullRebIDs(got))
	}
}

func TestPullRebalanceFetchTableAnswerIsSortedAndIsolated(t *testing.T) {
	const topic = "PullRebTableCopyTopic"
	f := newClusterFixture(t, map[string]int{topic: 4})
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_table_copy", "cid-A", topic, nil)
	quiescePullRebalance(c, nil)

	c.mu.Lock()
	c.rebalanceTable[topic] = []common.MessageQueue{
		common.NewMessageQueue(topic, "broker-b", 1),
		common.NewMessageQueue(topic, "broker-a", 2),
	}
	c.mu.Unlock()

	got, err := c.FetchMessageQueuesInBalance(topic)
	requireNoError(t, "FetchMessageQueuesInBalance", err)
	if len(got) != 2 || got[0].BrokerName != "broker-a" || got[0].QueueID != 2 ||
		got[1].BrokerName != "broker-b" || got[1].QueueID != 1 {
		t.Errorf("share = %v, want [broker-a/2 broker-b/1]", got)
	}
	got[0] = common.NewMessageQueue("mutated", "x", 99)
	if stored := c.GetRebalanceTable()[topic]; len(stored) != 2 || stored[0].Topic != topic {
		t.Errorf("callers mutated the table through the returned slice: %v", stored)
	}
}

func TestPullRebalanceGetTableReturnsACopy(t *testing.T) {
	const topic = "PullRebTableSnapshotTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	c := newStartedPullRebConsumerWithCids(t, f, "GID_pull_reb_table_snapshot", "cid-A", topic,
		[]string{"cid-A"}, nil)
	quiescePullRebalance(c, nil)

	c.doRebalance()
	snapshot := c.GetRebalanceTable()
	if len(snapshot[topic]) != 2 {
		t.Fatalf("snapshot = %v, want the 2 allocated queues", pullRebIDs(snapshot[topic]))
	}
	snapshot[topic][0] = common.NewMessageQueue("mutated", "x", 99)
	snapshot[topic] = nil
	if stored := c.GetRebalanceTable()[topic]; len(stored) != 2 || stored[0].Topic != topic {
		t.Errorf("callers mutated the table through the snapshot: %v", stored)
	}
}

func TestPullRebalanceUnfilledTopicComputesOnTheSpot(t *testing.T) {
	const topic = "PullRebTableColdTopic"
	f := newClusterFixture(t, map[string]int{topic: 4})
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_table_cold", "cid-A", topic, nil)
	quiescePullRebalance(c, nil)

	if table := c.GetRebalanceTable(); len(table) != 0 {
		t.Fatalf("rebalance table = %v before any round, want empty", table)
	}
	got, err := c.FetchMessageQueuesInBalance(topic)
	requireNoError(t, "FetchMessageQueuesInBalance", err)
	if !pullRebEqualIDs(got, 0, 1) {
		t.Errorf("share = %v, want the on-the-spot averaging [0 1]", pullRebIDs(got))
	}
}

// ---------------------------------------------------------------- the loop

// TestPullRebalanceLoopFillsTheTable proves Start actually launches the
// background loop and Shutdown stops it. The period is shortened to a few
// milliseconds so the test does not sleep for the 20s default.
func TestPullRebalanceLoopFillsTheTable(t *testing.T) {
	const topic = "PullRebLoopTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	c := newStartedPullRebConsumerWithCids(t, f, "GID_pull_reb_loop", "cid-A", topic,
		[]string{"cid-A"}, nil,
		func(c *DefaultMQPullConsumer) {
			c.SetRebalanceWaitMillis(5)
			c.SetRebalanceMinIntervalMillis(5)
		})

	waitFor(t, "the background rebalance to fill the table", func() bool {
		return len(c.GetRebalanceTable()[topic]) == 2
	})

	c.Shutdown()
	if c.IsStarted() {
		t.Error("IsStarted = true after Shutdown")
	}
}

// emptyRebStrategy always hands out nothing. It pins Java's
// updateProcessQueueTableInRebalance semantics: only table additions/removals are a
// change, so "nothing -> nothing" is no change -- an empty opening share must stay
// silent and the round counts as balanced (otherwise the loop hammers minInterval).
type emptyRebStrategy struct{}

func (emptyRebStrategy) Allocate(consumerGroup, currentCID string,
	mqAll []common.MessageQueue, cidAll []string) []common.MessageQueue {
	return nil
}

func (emptyRebStrategy) Name() string { return "empty" }

func TestPullRebalanceFirstEmptyShareIsSilentAndBalanced(t *testing.T) {
	const topic = "PullRebEmptyFirstTopic"
	f := newClusterFixture(t, map[string]int{topic: 3})
	rec := &pullRebRecorder{}
	c := newStartedPullRebConsumer(t, f, "GID_pull_reb_empty", "cid-A", topic, rec,
		func(c *DefaultMQPullConsumer) {
			c.SetAllocateMessageQueueStrategy(emptyRebStrategy{})
		})
	quiescePullRebalance(c, rec)

	if !c.doRebalance() {
		t.Fatal("nothing -> nothing must count as a balanced round")
	}
	if got := rec.count(); got != 0 {
		t.Fatalf("listener calls after the empty opening round = %d, want 0", got)
	}
	if got := c.GetRebalanceTable()[topic]; len(got) != 0 {
		t.Fatalf("table share = %d queues, want empty", len(got))
	}

	// The share appearing later IS a change (the table grew from nothing).
	c.SetAllocateMessageQueueStrategy(AllocateMessageQueueAveragely{})
	if c.doRebalance() {
		t.Fatal("growing from nothing to a share is a change, must not be balanced")
	}
	if got := rec.count(); got != 1 {
		t.Fatalf("listener calls after the share appeared = %d, want 1", got)
	}
}
