// Tests for the LitePull topic-queue-change listener
// (Java DefaultLitePullConsumer:325 → DefaultLitePullConsumerImpl:1267 +
// fetchTopicMessageQueuesAndCompare:1230). The capability was missing from
// this port entirely, so these lock down the contract rather than incidental
// details:
//
//   - guards and overwrite semantics (Java :1268-1273);
//   - the snapshot-on-register rule (Java :1275-1277): registering while
//     RUNNING records the current set, so "what already exists" is not
//     reported as a change on the next round;
//   - change detection is SET equality (isSetEqual:1246), so scale-out and
//     scale-in both fire exactly once and a stable set never re-fires;
//   - the background loop really drives the comparison.
package client

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// queueChangeRecorder collects OnChanged callbacks.
type queueChangeRecorder struct {
	mu     sync.Mutex
	events []string // "<topic>:<queueID,queueID,...>"
}

func (r *queueChangeRecorder) OnChanged(topic string, mqs []common.MessageQueue) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]int32, 0, len(mqs))
	for _, mq := range mqs {
		ids = append(ids, mq.QueueID)
	}
	r.events = append(r.events, formatQueues(topic, ids))
}

func (r *queueChangeRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	copy(out, r.events)
	return out
}

func (r *queueChangeRecorder) waitUntil(want int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(r.snapshot()) >= want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return len(r.snapshot()) >= want
}

func formatQueues(topic string, ids []int32) string {
	s := topic + ":"
	for i, id := range ids {
		if i > 0 {
			s += ","
		}
		s += strconv.FormatInt(int64(id), 10)
	}
	return s
}

// startedLite wires a subscribe-mode lite consumer to the fixture and makes
// sure the route for its topic is cached.
func startedLite(t *testing.T, f *clusterFixture, group string) *DefaultLitePullConsumer {
	t.Helper()
	c := newLiteConsumer(t, f, group)
	if err := c.Subscribe("T", "*"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(c.Shutdown)
	return c
}

// refresh forces the instance to re-read the route (the mock nameserver serves
// the mutated body immediately).
func refresh(t *testing.T, c *DefaultLitePullConsumer, topic string) {
	t.Helper()
	if _, err := c.instance.UpdateTopicRouteInfoFromNameServer(topic, 5000, false); err != nil {
		t.Fatalf("route refresh for %s: %v", topic, err)
	}
}

func TestLiteQueueChangeListenerGuards(t *testing.T) {
	c := MustNewDefaultLitePullConsumer("GID_tqc_guard")
	if err := c.RegisterTopicMessageQueueChangeListener("", &queueChangeRecorder{}); err == nil {
		t.Fatal("blank topic must be rejected (Java throws MQClientException)")
	}
	if err := c.RegisterTopicMessageQueueChangeListener("T", nil); err == nil {
		t.Fatal("nil listener must be rejected")
	}
	if len(c.topicChangeListeners) != 0 {
		t.Fatalf("rejected registrations must not be stored: %v", c.topicChangeListeners)
	}
	// Default matches Java DefaultLitePullConsumer:160 (30s).
	if got := c.TopicMetadataCheckIntervalMillis(); got != DefaultLiteTopicMetadataCheckIntervalMillis {
		t.Fatalf("default check interval = %d, want %d", got, DefaultLiteTopicMetadataCheckIntervalMillis)
	}
	c.SetTopicMetadataCheckIntervalMillis(0)
	if got := c.TopicMetadataCheckIntervalMillis(); got != 1000 {
		t.Fatalf("interval floor = %d, want 1000", got)
	}
	c.SetTopicMetadataCheckIntervalMillis(2500)
	if got := c.TopicMetadataCheckIntervalMillis(); got != 2500 {
		t.Fatalf("interval = %d, want 2500", got)
	}
}

func TestLiteQueueChangeListenerDetectsScaleOutAndIn(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 2})
	c := startedLite(t, f, "GID_tqc_scale")

	rec := &queueChangeRecorder{}
	if err := c.RegisterTopicMessageQueueChangeListener("T", rec); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Registered while RUNNING ⇒ Java :1275-1277 snapshots the current set, so
	// a round with an unchanged route must stay silent.
	if n := c.FetchTopicMessageQueuesAndCompare(); n != 0 {
		t.Fatalf("first round fired %d listeners, want 0 (snapshot on register)", n)
	}

	f.scaleTopic(t, "T", 4) // scale out
	refresh(t, c, "T")
	if n := c.FetchTopicMessageQueuesAndCompare(); n != 1 {
		t.Fatalf("scale-out round fired %d listeners, want 1", n)
	}
	if n := c.FetchTopicMessageQueuesAndCompare(); n != 0 {
		t.Fatalf("unchanged set re-fired %d listeners, want 0", n)
	}

	f.scaleTopic(t, "T", 1) // scale in
	refresh(t, c, "T")
	if n := c.FetchTopicMessageQueuesAndCompare(); n != 1 {
		t.Fatalf("scale-in round fired %d listeners, want 1", n)
	}
	want := []string{formatQueues("T", []int32{0, 1, 2, 3}), formatQueues("T", []int32{0})}
	got := rec.snapshot()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestLiteQueueChangeListenerReRegisterOverwrites(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	c := startedLite(t, f, "GID_tqc_overwrite")

	first := &queueChangeRecorder{}
	second := &queueChangeRecorder{}
	if err := c.RegisterTopicMessageQueueChangeListener("T", first); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := c.RegisterTopicMessageQueueChangeListener("T", second); err != nil {
		t.Fatalf("register second: %v", err)
	}
	if len(c.topicChangeListeners) != 1 {
		t.Fatalf("listener map holds %d entries, want 1", len(c.topicChangeListeners))
	}
	f.scaleTopic(t, "T", 3)
	refresh(t, c, "T")
	c.FetchTopicMessageQueuesAndCompare()
	if len(first.snapshot()) != 0 {
		t.Fatalf("overwritten listener still fired: %v", first.snapshot())
	}
	if len(second.snapshot()) != 1 {
		t.Fatalf("current listener did not fire: %v", second.snapshot())
	}
}

func TestLiteQueueChangeListenerBeforeStartHasNoSnapshot(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 2})
	c := newLiteConsumer(t, f, "GID_tqc_pre")
	if err := c.Subscribe("T", "*"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	rec := &queueChangeRecorder{}
	if err := c.RegisterTopicMessageQueueChangeListener("T", rec); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, ok := c.messageQueuesForTopic["T"]; ok {
		t.Fatal("a pre-start registration must not snapshot (Java gates on RUNNING)")
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Shutdown()
	// Java behaves the same way: with no snapshot the first round reports the
	// set it sees, because "unknown" is not "equal".
	if n := c.FetchTopicMessageQueuesAndCompare(); n != 1 {
		t.Fatalf("first round after start fired %d, want 1", n)
	}
	if len(rec.snapshot()) != 1 {
		t.Fatalf("events = %v, want exactly one", rec.snapshot())
	}
}

func TestLiteQueueChangeListenerLoopRunsTheComparison(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	c := startedLite(t, f, "GID_tqc_loop")
	rec := &queueChangeRecorder{}
	if err := c.RegisterTopicMessageQueueChangeListener("T", rec); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Drive a second loop with the delays tests can afford; Start's own loop
	// keeps Java's 10s/30s timings and simply finds nothing changed.
	stop := make(chan struct{})
	go c.metadataLoop(stop, 50*time.Millisecond, 100*time.Millisecond)
	defer close(stop)

	f.scaleTopic(t, "T", 3)
	refresh(t, c, "T")
	if !rec.waitUntil(1, 3*time.Second) {
		t.Fatalf("metadata loop never compared: events=%v", rec.snapshot())
	}
}

func TestLiteQueueChangeSetEquality(t *testing.T) {
	mq := func(ids ...int32) []common.MessageQueue {
		out := make([]common.MessageQueue, 0, len(ids))
		for _, id := range ids {
			out = append(out, common.NewMessageQueue("T", "b1", id))
		}
		return out
	}
	if isQueueSetEqual(nil, false, mq()) {
		t.Fatal("no snapshot is Java's oldSet == null: never equal")
	}
	if isQueueSetEqual(mq(0, 1), true, mq(0)) {
		t.Fatal("different size must count as changed")
	}
	if !isQueueSetEqual(mq(0, 1), true, mq(1, 0)) {
		t.Fatal("same members in another order are equal")
	}
	if isQueueSetEqual(mq(0, 1), true, mq(0, 2)) {
		t.Fatal("same size, different members must count as changed")
	}
}
