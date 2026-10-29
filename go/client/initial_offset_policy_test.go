// An initial-offset read that FAILS is not the same as "nothing committed": the queue
// keeps its assignment but gets no process queue and no pull loop for that round, and
// the next rebalance round retries the read. Falling back to the configured start
// position would silently skip the backlog (LAST_OFFSET would start at the tail), and
// a loop started without a cursor could only guess one.
package client

import (
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

func TestRebalanceSkipsAQueueWhoseOffsetReadFailed(t *testing.T) {
	topic := uniqueTopic("GoInitReadFail", t)
	const group = "GID_go_init_readfail"
	f := newClusterFixture(t, map[string]int{topic: 1})
	mq := common.MessageQueue{Topic: topic, BrokerName: "b1", QueueID: 0}

	// The store cannot answer at all -- not "no offset committed", which would fall
	// through to the policy.
	f.broker.failOffsetReads(remoting.RespSystemError)
	c := f.newConsumer(t, group, newRecordingListener())
	startConsumer(t, c, topic, "*")

	waitFor(t, "the queue assigned", func() bool { return assignedHolds(c, mq) })
	waitFor(t, "the committed-offset read attempted", func() bool { return f.broker.offsetReadsFor(topic) > 0 })

	// Nothing may pull it: watch a window in which a wrongly started loop would have
	// gone out.
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if rcHasPullState(c, mq) {
			t.Fatalf("a queue with no cursor got a pull loop for the round")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := f.broker.pullsFor(topic, 0); len(got) != 0 {
		t.Fatalf("%d PULL(s) went out for a queue the round could not place", len(got))
	}
	if _, maxQ := f.broker.boundaryQueries(topic); maxQ != 0 {
		t.Fatalf("a failed read must not fall through to the tail: %d GET_MAX_OFFSET", maxQ)
	}

	// The store recovers: the next round reads 7 and the loop starts exactly there.
	f.broker.failOffsetReads(0)
	f.broker.setCommitted(group, topic, 0, 7)
	requireNoError(t, "rebalance after the store recovers", c.doRebalance())
	waitFor(t, "the recovered queue pulls", func() bool { return len(f.broker.pullsFor(topic, 0)) > 0 })
	pulls := f.broker.pullsFor(topic, 0)
	if got := derefI64(pulls[0].QueueOffset); got != 7 {
		t.Errorf("the recovered loop started at %d, want the committed 7", got)
	}
}

// assignedHolds reports whether this round's assignment contains mq.
func assignedHolds(c *DefaultMQPushConsumer, mq common.MessageQueue) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, got := range c.assigned {
		if got == mq {
			return true
		}
	}
	return false
}
