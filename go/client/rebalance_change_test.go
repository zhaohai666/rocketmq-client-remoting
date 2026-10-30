// What a rebalance round owes the broker and the application depends on whether the
// assignment MOVED: the topics whose queue set changed re-share their subscription
// version, ONE immediate heartbeat round carries those versions to every broker, and
// last the application's queue listener hears about exactly the topics that moved.
// A round that re-pulled the same queues owes none of that, and a round that only
// self-heals a stalled pull loop (the queue stays assigned, so the set did not move)
// is not a change either.
//
// Every positive leg sits next to a control that goes red when the version is not
// re-shared, when the beat or the notification runs on every round, or when the
// restamp is applied to the whole subscription table instead of the moved topics.
package client

import (
	"sync"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// rcStaleVersion pins one topic's subscription version to a wall-clock value from the
// past. A re-shared version is then a comparison, not a race against millisecond
// resolution, and a round that must NOT touch it leaves the sentinel in place.
func rcStaleVersion(c *DefaultMQPushConsumer, topics ...string) int64 {
	const stale = int64(1_700_000_000_000)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, topic := range topics {
		if sub := c.subscription[topic]; sub != nil {
			// The table entry is swapped for a copy rather than edited in place, so
			// a pull loop already holding the old subscription never reads a value
			// that changed underneath it — the same rule restampSubscriptionVersions
			// follows.
			aged := *sub
			aged.SubVersion = stale
			c.subscription[topic] = &aged
		}
	}
	return stale
}

func rcSubVersion(c *DefaultMQPushConsumer, topic string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if sub := c.subscription[topic]; sub != nil {
		return sub.SubVersion
	}
	return -1
}

func rcHeartbeats(f *clusterFixture) int {
	return f.broker.countReq(remoting.ReqHeartBeat)
}

// rcQueueListener records every assignment-change notification as one entry, so a
// test can count notifications per topic instead of only the last one.
type rcQueueListener struct {
	mu    sync.Mutex
	calls []rcQueueCall
}

type rcQueueCall struct {
	topic        string
	all, divided int
}

func (l *rcQueueListener) MessageQueueChanged(topic string, mqAll, mqDivided []common.MessageQueue) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, rcQueueCall{topic: topic, all: len(mqAll), divided: len(mqDivided)})
}

func (l *rcQueueListener) snapshot() []rcQueueCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]rcQueueCall(nil), l.calls...)
}

// rcTopicsSince lists the topics of the notifications after the first n.
func rcTopicsSince(l *rcQueueListener, n int) []string {
	calls := l.snapshot()
	if n > len(calls) {
		n = len(calls)
	}
	var out []string
	for _, call := range calls[n:] {
		out = append(out, call.topic)
	}
	return out
}

// rcDropPullState removes one still-assigned queue's pull state and stops its loop,
// which is exactly the state a stalled loop leaves for the self-heal branch: the
// assignment did not move, only the local buffer is missing.
func rcDropPullState(c *DefaultMQPushConsumer, mq common.MessageQueue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if stop, ok := c.queueStop[mq]; ok {
		close(stop)
		delete(c.queueStop, mq)
	}
	delete(c.processQueueTable, mq)
}

func rcHasPullState(c *DefaultMQPushConsumer, mq common.MessageQueue) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.processQueueTable[mq]
	return ok
}

// rcPinSettledOnWire makes a pinned version safe to assert on the wire: a beat built
// before the pin can still be on its way to the broker, and "the newest beat" would
// then report the subscribe-time version. Ask for a beat and wait until the newest
// one carries the pinned value, which also means every older beat has landed.
func rcPinSettledOnWire(t *testing.T, c *DefaultMQPushConsumer, broker *consumerBroker,
	stale int64, topics ...string) {
	t.Helper()
	for _, topic := range topics {
		requireNoError(t, "beat after the pin", c.sendHeartbeatNow())
		waitFor(t, "the pinned version of "+topic+" on the wire", func() bool {
			versions := broker.beatSubVersions(topic)
			return len(versions) > 0 && versions[len(versions)-1] == stale
		})
	}
}

// A changed round re-shares the version and spends exactly one broker beat; the beat
// carries the fresh version. An unchanged round spends neither, so the pinned version
// is still there and the wire still shows it.
func TestRebalanceChangeRestampsVersionAndBeatsOnlyWhenQueuesMove(t *testing.T) {
	topic := uniqueTopic("GoRebalanceChangeBeat", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	c := f.newConsumer(t, "GID_go_rc_beat", newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")
	waitFor(t, "first pull", func() bool { return f.broker.pullCount() > 0 })

	// Control: the identical round must not restamp, must not notify and above all
	// must not beat — otherwise every 20 s round would re-send the subscriptions of
	// a consumer whose queues never moved.
	stale := rcStaleVersion(c, topic)
	rcPinSettledOnWire(t, c, f.broker, stale, topic)
	before := rcHeartbeats(f)
	requireNoError(t, "unchanged rebalance", c.doRebalance())
	if got := rcSubVersion(c, topic); got != stale {
		t.Errorf("subscription version = %d after an unchanged round, want the pinned %d", got, stale)
	}
	if got := rcHeartbeats(f) - before; got != 0 {
		t.Errorf("an unchanged round sent %d heartbeat(s), want 0", got)
	}
	// The pinned version is what the heartbeat builder reads, so a beat asked for
	// out of band still carries it: this is the control for the wire assertion below,
	// which needs the SAME beat to change.
	requireNoError(t, "explicit beat", c.sendHeartbeatNow())
	versions := f.broker.beatSubVersions(topic)
	if len(versions) == 0 {
		t.Fatalf("no heartbeat body was captured to check the version against")
	}
	if got := versions[len(versions)-1]; got != stale {
		t.Errorf("the beat carried version %d, want the pinned %d", got, stale)
	}

	// The move: a second queue joins, so this client picks it up.
	f.scaleTopic(t, topic, 2)
	ttRefreshRoute(t, c, topic)
	before = rcHeartbeats(f)
	requireNoError(t, "rebalance after the grow", c.doRebalance())

	grown := rcSubVersion(c, topic)
	if grown <= stale {
		t.Fatalf("subscription version = %d after the change, want a re-shared version above %d", grown, stale)
	}
	if got := rcHeartbeats(f) - before; got != 1 {
		t.Errorf("the changed round sent %d heartbeat(s), want exactly one round", got)
	}
	versions = f.broker.beatSubVersions(topic)
	if got := versions[len(versions)-1]; got != grown {
		t.Errorf("the newest beat carried version %d, want the re-shared %d", got, grown)
	}
}

// The version is re-shared per moved topic, not over the whole subscription table: a
// sibling whose queues did not move keeps its pinned version, and the beat that goes
// out for the moved topic carries the sibling's OLD version untouched.
func TestRebalanceChangeRestampsOnlyTheTopicThatMoved(t *testing.T) {
	moved := uniqueTopic("GoRebalanceChangeTopicA", t)
	still := uniqueTopic("GoRebalanceChangeTopicB", t)

	f := newClusterFixture(t, map[string]int{moved: 1, still: 1})
	c := f.newConsumer(t, "GID_go_rc_per_topic", newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	requireNoError(t, "subscribe moved", c.Subscribe(moved, "*"))
	requireNoError(t, "subscribe still", c.Subscribe(still, "*"))
	requireNoError(t, "consumer start", c.Start())
	t.Cleanup(c.Shutdown)
	waitFor(t, "pull of both topics", func() bool {
		return f.broker.pullsFor(moved, 0) != nil && f.broker.pullsFor(still, 0) != nil
	})

	stale := rcStaleVersion(c, moved, still)
	rcPinSettledOnWire(t, c, f.broker, stale, moved, still)
	f.scaleTopic(t, moved, 2)
	ttRefreshRoute(t, c, moved)
	requireNoError(t, "rebalance after the grow", c.doRebalance())

	if got := rcSubVersion(c, moved); got <= stale {
		t.Errorf("moved topic version = %d, want a re-shared version above %d", got, stale)
	}
	if got := rcSubVersion(c, still); got != stale {
		t.Errorf("untouched topic version = %d, want the pinned %d: a blanket restamp would re-share it", got, stale)
	}
	versions := f.broker.beatSubVersions(still)
	if len(versions) == 0 {
		t.Fatalf("no beat carried the untouched topic's subscriptions")
	}
	if got := versions[len(versions)-1]; got != stale {
		t.Errorf("the newest beat carried %d for the untouched topic, want the pinned %d", got, stale)
	}
}

// The application's queue listener is a change callback: it fires once per moved
// topic, with that topic's queues, and stays silent for a sibling that did not move
// and for a round that changed nothing.
func TestRebalanceChangeNotifiesTheListenerOnlyForMovedTopics(t *testing.T) {
	moved := uniqueTopic("GoRebalanceNotifyA", t)
	still := uniqueTopic("GoRebalanceNotifyB", t)

	f := newClusterFixture(t, map[string]int{moved: 1, still: 1})
	listener := &rcQueueListener{}
	c := f.newConsumer(t, "GID_go_rc_notify", newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	c.SetMessageQueueListener(listener)
	requireNoError(t, "subscribe moved", c.Subscribe(moved, "*"))
	requireNoError(t, "subscribe still", c.Subscribe(still, "*"))
	requireNoError(t, "consumer start", c.Start())
	t.Cleanup(c.Shutdown)
	waitFor(t, "pull of both topics", func() bool {
		return f.broker.pullsFor(moved, 0) != nil && f.broker.pullsFor(still, 0) != nil
	})

	// Start assigned both topics, so both moved once.
	startCalls := listener.snapshot()
	if len(startCalls) != 2 {
		t.Fatalf("start notified %d topics, want the two it just assigned: %+v", len(startCalls), startCalls)
	}

	// Control: a round with the same queues tells the listener nothing.
	n := len(listener.snapshot())
	requireNoError(t, "unchanged rebalance", c.doRebalance())
	if got := len(listener.snapshot()) - n; got != 0 {
		t.Errorf("an unchanged round notified %d topic(s), want 0", got)
	}

	f.scaleTopic(t, moved, 3)
	ttRefreshRoute(t, c, moved)
	n = len(listener.snapshot())
	requireNoError(t, "rebalance after the grow", c.doRebalance())

	topics := rcTopicsSince(listener, n)
	if len(topics) != 1 || topics[0] != moved {
		t.Fatalf("the change notified %v, want exactly the moved topic %q", topics, moved)
	}
	calls := listener.snapshot()
	var newest *rcQueueCall
	for i := range calls {
		if calls[i].topic == moved {
			newest = &calls[i]
		}
	}
	if newest == nil {
		t.Fatalf("no notification recorded for %s", moved)
	}
	if newest.divided != 3 || newest.all != 3 {
		t.Errorf("the change notification carried %d/%d queues, want 3 assigned of 3 in the route",
			newest.divided, newest.all)
	}
}

// A stalled loop is self-healed by the next round, but the queue was assigned all
// along: the set did not move, so the round rebuilds the buffer and stops there. No
// beat, no restamp, no listener call — a broker beat per repaired loop would put the
// subscriptions of an unchanged consumer on the wire every few seconds.
func TestRebalanceSelfHealRoundSpendsNoBrokerBeat(t *testing.T) {
	topic := uniqueTopic("GoRebalanceSelfHeal", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	listener := &rcQueueListener{}
	c := f.newConsumer(t, "GID_go_rc_selfheal", newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	c.SetMessageQueueListener(listener)
	startConsumer(t, c, topic, "*")
	waitFor(t, "first pull", func() bool { return f.broker.pullCount() > 0 })

	assigned := c.assignedQueues()
	if len(assigned) != 1 {
		t.Fatalf("assigned %d queues, want 1", len(assigned))
	}
	rcDropPullState(c, assigned[0])
	if rcHasPullState(c, assigned[0]) {
		t.Fatalf("the pull state of %v is still there, so the round below has nothing to heal", assigned[0])
	}

	stale := rcStaleVersion(c, topic)
	before := rcHeartbeats(f)
	n := len(listener.snapshot())
	requireNoError(t, "rebalance over the healed queue", c.doRebalance())

	if !rcHasPullState(c, assigned[0]) {
		t.Errorf("the stalled queue was not rebuilt, so the round never reached the self-heal branch")
	}
	if got := rcHeartbeats(f) - before; got != 0 {
		t.Errorf("a self-heal round sent %d heartbeat(s), want 0", got)
	}
	if got := rcSubVersion(c, topic); got != stale {
		t.Errorf("subscription version = %d, want the pinned %d: the queue set did not move", got, stale)
	}
	if got := len(listener.snapshot()) - n; got != 0 {
		t.Errorf("a self-heal round notified %d topic(s), want 0", got)
	}
}

// POP keeps its per-queue state in another table and syncs it in its own loop, but the
// debt is the same and is paid once: the beat and the listener call belong to the
// changed round, not to the POP sync.
func TestPopRebalanceChangeBeatsAndNotifiesOnce(t *testing.T) {
	topic := uniqueTopic("GoPopRebalanceChange", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	listener := &rcQueueListener{}
	c := f.newConsumer(t, "GID_go_rc_pop_change", &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	})
	c.SetMessageQueueListener(listener)
	c.SetPopMode(true)
	f.broker.setPopFixture(popFixture{code: remoting.RespPollingTimeout})
	startConsumer(t, c, topic, "*")
	waitFor(t, "first pop", func() bool { return len(f.broker.popHeadersSnapshot()) > 0 })

	stale := rcStaleVersion(c, topic)
	before := rcHeartbeats(f)
	n := len(listener.snapshot())
	requireNoError(t, "unchanged pop rebalance", c.doRebalance())
	if got := rcHeartbeats(f) - before; got != 0 {
		t.Errorf("an unchanged POP round sent %d heartbeat(s), want 0", got)
	}
	if got := len(listener.snapshot()) - n; got != 0 {
		t.Errorf("an unchanged POP round notified %d topic(s), want 0", got)
	}
	if got := rcSubVersion(c, topic); got != stale {
		t.Errorf("POP version = %d after an unchanged round, want the pinned %d", got, stale)
	}

	f.scaleTopic(t, topic, 2)
	ttRefreshRoute(t, c, topic)
	before = rcHeartbeats(f)
	n = len(listener.snapshot())
	requireNoError(t, "pop rebalance after the grow", c.doRebalance())

	if got := rcHeartbeats(f) - before; got != 1 {
		t.Errorf("the changed POP round sent %d heartbeat(s), want exactly one round", got)
	}
	if got := rcSubVersion(c, topic); got <= stale {
		t.Errorf("POP version = %d after the change, want a re-shared version above %d", got, stale)
	}
	if topics := rcTopicsSince(listener, n); len(topics) != 1 || topics[0] != topic {
		t.Errorf("the changed POP round notified %v, want exactly %q once", topics, topic)
	}
}
