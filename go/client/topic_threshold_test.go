// The two topic-level cache budgets are enforced by DIVISION, not by adding up the
// sibling queues: each enabled budget (-1 = off) is split over the queues this client
// owns, and the quotient becomes the per-queue gate. It happens on an assignment
// change, after the pull state has been rebuilt, and never at pull time.
//
// Each positive case sits next to a control that goes red when the division is
// reverted, moved, or fed the wrong number.
package client

import (
	"fmt"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ttOwnQueues gives the consumer n queues with live pull state, the way a rebalance
// round leaves them, and returns how many the state table now holds.
func ttOwnQueues(c *DefaultMQPushConsumer, n int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < n; i++ {
		c.processQueueTable[fcQueue(int32(i))] = newProcessQueue(false)
	}
	return len(c.processQueueTable)
}

// ttOwnPopQueues is the POP-mode variant: the live per-queue state sits in the POP
// table instead.
func ttOwnPopQueues(c *DefaultMQPushConsumer, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < n; i++ {
		c.popQueueTable[fcQueue(int32(i))] = newPopProcessQueue()
	}
}

// ttSwapRouteBody serves the same queue TOTAL over two brokers, so a consumer that owns
// every one of them keeps its queue COUNT while swapping queues: the first broker loses
// its highest queue, the second brings one in.
func ttSwapRouteBody(addr string, firstQueues, secondQueues int) string {
	return fmt.Sprintf(`{"orderTopicConf":null,`+
		`"queueDatas":[`+
		`{"brokerName":"b1","readQueueNums":%d,"writeQueueNums":%d,"perm":6,"topicSysFlag":0},`+
		`{"brokerName":"b2","readQueueNums":%d,"writeQueueNums":%d,"perm":6,"topicSysFlag":0}],`,
		firstQueues, firstQueues, secondQueues, secondQueues) +
		`"brokerDatas":[` +
		fmt.Sprintf(`{"cluster":"DefaultCluster","brokerName":"b1","brokerAddrs":{"0":"%s"},"zoneName":null,"enableActingMaster":false},`, addr) +
		fmt.Sprintf(`{"cluster":"DefaultCluster","brokerName":"b2","brokerAddrs":{"0":"%s"},"zoneName":null,"enableActingMaster":false}],`, addr) +
		`"filterServerTable":{}}`
}

// ttBrokerNames lists which brokers the consumer's live pull state sits on.
func ttBrokerNames(c *DefaultMQPushConsumer) map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for mq := range c.processQueueTable {
		out[mq.BrokerName]++
	}
	return out
}

// ttRefreshRoute forces the instance to re-read the topic route from the mock
// nameserver, which already serves the mutated body.
func ttRefreshRoute(t *testing.T, c *DefaultMQPushConsumer, topic string) {
	t.Helper()
	if ok, err := c.instance.UpdateTopicRouteInfoFromNameServer(topic, 2000, false); err != nil || !ok {
		t.Fatalf("route refresh for %s: ok=%v err=%v", topic, ok, err)
	}
}

// An enabled topic budget is divided by the owned queue count, and only the knob it
// belongs to moves. Reverting the division (or dropping the max(1, ..) floor) reddens
// one of these legs.
func TestTopicThresholdDivisionSplitsEachEnabledBudget(t *testing.T) {
	c := fcConsumer(t, "GID_go_tt_split")
	c.SetPullThresholdForQueue(999)
	c.SetPullThresholdSizeForQueue(50)
	c.SetPullThresholdForTopic(1000)
	c.SetPullThresholdSizeForTopic(-1)
	ttOwnQueues(c, 4)

	c.divideTopicThresholds()
	count, size := fcPerQueueKnobs(c)
	if count != 250 {
		t.Errorf("message-count gate = %d, want the 1000-message budget over 4 owned queues", count)
	}
	if size != 50 {
		t.Errorf("byte gate = %d, want the user value kept while the byte budget is off", size)
	}

	// The byte budget divides the same way, over the same queue count.
	c.SetPullThresholdSizeForTopic(100)
	c.divideTopicThresholds()
	count, size = fcPerQueueKnobs(c)
	if size != 25 {
		t.Errorf("byte gate = %d, want the 100 MiB budget over 4 owned queues", size)
	}
	if count != 250 {
		t.Errorf("message-count gate = %d, want the earlier quotient untouched", count)
	}
}

// Control: both budgets off means nothing is divided, so the user's per-queue values
// survive any queue count. An unconditional division would overwrite them here.
func TestTopicThresholdDivisionKeepsPerQueueKnobsWhenBudgetsOff(t *testing.T) {
	c := fcConsumer(t, "GID_go_tt_off")
	c.SetPullThresholdForQueue(4242)
	c.SetPullThresholdSizeForQueue(43)
	c.SetPullThresholdForTopic(-1)
	c.SetPullThresholdSizeForTopic(-1)
	ttOwnQueues(c, 4)

	c.divideTopicThresholds()
	if count, size := fcPerQueueKnobs(c); count != 4242 || size != 43 {
		t.Errorf("gates = (%d, %d), want (4242, 43) kept: an off budget must not divide", count, size)
	}
}

// A budget smaller than the queue count still leaves a usable gate: the quotient is
// floored at 1, never 0 (0 would mean "no message may ever be cached").
func TestTopicThresholdDivisionFloorsAtOneMessage(t *testing.T) {
	c := fcConsumer(t, "GID_go_tt_floor")
	c.SetPullThresholdForQueue(1000)
	c.SetPullThresholdSizeForQueue(100)
	c.SetPullThresholdForTopic(3)
	c.SetPullThresholdSizeForTopic(-1)
	ttOwnQueues(c, 4)

	c.divideTopicThresholds()
	if count, _ := fcPerQueueKnobs(c); count != 1 {
		t.Errorf("message-count gate = %d, want the 3/4 quotient floored at 1", count)
	}

	// Same for the byte budget.
	c.SetPullThresholdForTopic(-1)
	c.SetPullThresholdForQueue(1000)
	c.SetPullThresholdSizeForQueue(100)
	c.SetPullThresholdSizeForTopic(1)
	c.divideTopicThresholds()
	if _, size := fcPerQueueKnobs(c); size != 1 {
		t.Errorf("byte gate = %d, want the 1/4 quotient floored at 1", size)
	}
}

// With nothing owned there is nothing to divide by: both gates keep the user's values
// instead of collapsing to a quotient of a zero divisor.
func TestTopicThresholdDivisionSkipsWhenNoQueueIsOwned(t *testing.T) {
	c := fcConsumer(t, "GID_go_tt_no_queue")
	c.SetPullThresholdForQueue(1000)
	c.SetPullThresholdSizeForQueue(100)
	c.SetPullThresholdForTopic(1000)
	c.SetPullThresholdSizeForTopic(100)

	c.divideTopicThresholds()
	if count, size := fcPerQueueKnobs(c); count != 1000 || size != 100 {
		t.Errorf("gates = (%d, %d), want (1000, 100) with no owned queue", count, size)
	}
}

// The numerator is always the topic budget, so the same queue count re-derives the same
// gate, and a grown queue set is divided from the budget rather than from the gate the
// previous round wrote (1000/8 = 125, not 250/8 = 31).
func TestTopicThresholdDivisionRepeatsAndTracksTheQueueCount(t *testing.T) {
	c := fcConsumer(t, "GID_go_tt_repeat")
	c.SetPullThresholdForTopic(1000)
	c.SetPullThresholdSizeForTopic(100)
	ttOwnQueues(c, 4)

	c.divideTopicThresholds()
	first, firstSize := fcPerQueueKnobs(c)
	if first != 250 || firstSize != 25 {
		t.Fatalf("gates = (%d, %d), want (250, 25) over 4 queues", first, firstSize)
	}
	c.divideTopicThresholds()
	if count, size := fcPerQueueKnobs(c); count != 250 || size != 25 {
		t.Errorf("re-division = (%d, %d), want the same (250, 25): the quotient must not feed itself",
			count, size)
	}

	// Four more queues: the budget is re-split from the budget, not from 250.
	if got := ttOwnQueues(c, 8); got != 8 {
		t.Fatalf("pull state holds %d queues, want 8", got)
	}
	c.divideTopicThresholds()
	count, size := fcPerQueueKnobs(c)
	if count != 125 {
		t.Errorf("message-count gate = %d, want 1000/8 = 125", count)
	}
	if size != 12 {
		t.Errorf("byte gate = %d, want 100/8 = 12", size)
	}
}

// The divisor counts EVERY queue this client owns with live pull state, whatever topic
// it belongs to — not the queues of one topic.
func TestTopicThresholdDivisionCountsQueuesAcrossTopics(t *testing.T) {
	c := fcConsumer(t, "GID_go_tt_across_topics")
	c.SetPullThresholdForTopic(1000)
	c.mu.Lock()
	// Two queues of the fixture topic plus two of another one.
	for i := int32(0); i < 2; i++ {
		c.processQueueTable[fcQueue(i)] = newProcessQueue(false)
		c.processQueueTable[common.NewMessageQueue("GoFlowControlOtherTopic", "b1", i)] = newProcessQueue(false)
	}
	c.mu.Unlock()

	c.divideTopicThresholds()
	if count, _ := fcPerQueueKnobs(c); count != 250 {
		t.Errorf("message-count gate = %d, want 1000 over all 4 owned queues, not just this topic's 2", count)
	}
}

// POP keeps its per-queue state in its own table, and that is the table the divisor is
// read from: here the two tables disagree on purpose.
func TestTopicThresholdDivisionCountsThePopStateTable(t *testing.T) {
	c := fcConsumer(t, "GID_go_tt_pop_table")
	c.SetPopMode(true)
	c.SetPullThresholdForTopic(1000)
	ttOwnQueues(c, 8)
	ttOwnPopQueues(c, 4)

	c.divideTopicThresholds()
	if count, _ := fcPerQueueKnobs(c); count != 250 {
		t.Errorf("message-count gate = %d, want 1000 over the 4 queues of the live state table", count)
	}
}

// The division belongs to the rebalance round that CHANGED the assignment, and runs
// after the pull state was rebuilt: a round that changed nothing must leave a
// hand-configured gate alone, an equal-size swap must re-divide, and a shrink must
// divide by the count the round produced (500), not by the count it started from (250).
func TestTopicThresholdDivisionRunsOnAssignmentChangeOnly(t *testing.T) {
	topic := uniqueTopic("GoTopicThresholdRebalance", t)

	f := newClusterFixture(t, map[string]int{topic: 4})
	c := f.newConsumer(t, "GID_go_tt_rebalance", newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	c.SetPullThresholdForTopic(1000)
	startConsumer(t, c, topic, "*")

	if got := c.AssignedQueueCount(); got != 4 {
		t.Fatalf("assigned %d queues, want 4", got)
	}
	// Start's own round already split the budget over the four queues it owns.
	if count, _ := fcPerQueueKnobs(c); count != 250 {
		t.Fatalf("message-count gate = %d, want 1000 over the 4 queues this round owns", count)
	}

	// Control: the identical round must not re-run it, so the hand-set value survives.
	c.SetPullThresholdForQueue(777)
	requireNoError(t, "rebalance with the same queues", c.doRebalance())
	if count, _ := fcPerQueueKnobs(c); count != 777 {
		t.Errorf("message-count gate = %d, want the hand-set 777: an unchanged assignment must not divide",
			count)
	}

	// An equal-size swap: 3 queues on the first broker, 1 on the second, so the COUNT
	// is still 4 while the SET changed. A length comparison would call this unchanged.
	f.routeBodies.set(topic, ttSwapRouteBody(f.brokerSrv.addr, 3, 1))
	ttRefreshRoute(t, c, topic)
	requireNoError(t, "rebalance after the swap", c.doRebalance())

	names := ttBrokerNames(c)
	if names["b1"] != 3 || names["b2"] != 1 {
		t.Fatalf("live pull state sits on %v, want 3 queues of b1 and 1 of b2", names)
	}
	if count, _ := fcPerQueueKnobs(c); count != 250 {
		t.Errorf("message-count gate = %d, want the swap to re-divide 1000 over 4 queues", count)
	}

	// A shrink divides by the count THIS round produced. Doing the division before the
	// pull state was rebuilt would still see 4 and write 250.
	c.SetPullThresholdForQueue(777)
	f.scaleTopic(t, topic, 2)
	ttRefreshRoute(t, c, topic)
	requireNoError(t, "rebalance after the shrink", c.doRebalance())
	if got := ttBrokerNames(c); got["b1"] != 2 {
		t.Fatalf("live pull state holds %v, want the two remaining queues", got)
	}
	if count, _ := fcPerQueueKnobs(c); count != 500 {
		t.Errorf("message-count gate = %d, want 1000 over the 2 queues left after the shrink", count)
	}
}
