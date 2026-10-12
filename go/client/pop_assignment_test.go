// What is worth asserting: the 400 request body's exact fields (the broker
// load-balances by them), the null-vs-empty reply semantics (keep current vs
// clear), and that the answer's queue set — not the local strategy — becomes
// the assignment the POP loops are built from. The default clientRebalance=true
// path must not send 400 at all.
package client

import (
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// assignmentFixture is the canned QUERY_ASSIGNMENT answer for one topic.
// code != 0 forces a broker error answer; a non-nil assignments slice is
// encoded as the reply's set (an EMPTY slice is the explicit "clear" answer —
// distinct from "no set at all", which is the fixture-absent default).
type assignmentFixture struct {
	code        int32
	assignments []*remoting.MessageQueueAssignment
}

// popAssignment builds one assignment the way a 5.x broker stamps them.
func popAssignment(topic, brokerName string, queueID int32, mode string) *remoting.MessageQueueAssignment {
	return &remoting.MessageQueueAssignment{
		MessageQueue: common.NewMessageQueue(topic, brokerName, queueID),
		Mode:         mode,
	}
}

// answerQueryAssignment is the QUERY_ASSIGNMENT branch of the mock broker's
// request switch. It lives in this file so the consumer fixtures stay
// untouched.
func (b *consumerBroker) answerQueryAssignment(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	body, err := remoting.DecodeQueryAssignmentRequestBody(req.Body)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.RespSystemError, err.Error())
	}
	b.mu.Lock()
	b.queryAssignments = append(b.queryAssignments, body)
	fixture := b.assignmentsByTopic[body.Topic]
	b.mu.Unlock()

	if fixture == nil {
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	}
	if fixture.code != 0 {
		return remoting.CreateResponseCommand(fixture.code, "fixture error")
	}
	resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
	if fixture.assignments != nil {
		resp.SetBody((&remoting.QueryAssignmentResponseBody{
			MessageQueueAssignments: fixture.assignments,
		}).Encode())
	}
	return resp
}

func (b *consumerBroker) setAssignmentFixture(topic string, fixture *assignmentFixture) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if fixture == nil {
		delete(b.assignmentsByTopic, topic)
		return
	}
	b.assignmentsByTopic[topic] = fixture
}

func (b *consumerBroker) queryAssignmentsSnapshot() []*remoting.QueryAssignmentRequestBody {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*remoting.QueryAssignmentRequestBody(nil), b.queryAssignments...)
}

// queryAssignmentsFor counts the captured 400s for one topic.
func (b *consumerBroker) queryAssignmentsFor(topic string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, body := range b.queryAssignments {
		if body.Topic == topic {
			n++
		}
	}
	return n
}

// assignedContains reports whether mq is in the consumer's current assignment.
func assignedContains(c *DefaultMQPushConsumer, mq common.MessageQueue) bool {
	for _, got := range c.AssignedQueues() {
		if got == mq {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- request body

// The 400 request body must carry exactly what the broker load-balances by:
// the requested topic, the group, THIS client's id, the strategy name and the
// message model.
func TestQueryAssignmentRequestBodyFields(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	}, func(c *DefaultMQPushConsumer) { c.SetClientRebalance(false) })
	f.broker.setAssignmentFixture("TopicA", &assignmentFixture{
		assignments: []*remoting.MessageQueueAssignment{
			popAssignment("TopicA", "b1", 0, remoting.MessageRequestModePop),
		},
	})
	// The auto-subscribed %RETRY% topic has no route here; answer nothing (the
	// "keep current" default) so it stays unassigned.
	startConsumer(t, c, "TopicA", "*")

	waitForBudget(t, 5*time.Second, func() bool { return f.broker.queryAssignmentsFor("TopicA") >= 1 })
	bodies := f.broker.queryAssignmentsSnapshot()
	var got *remoting.QueryAssignmentRequestBody
	for _, body := range bodies {
		if body.Topic == "TopicA" {
			got = body
		}
	}
	if got == nil {
		t.Fatalf("no QUERY_ASSIGNMENT for TopicA: %+v", bodies)
	}
	if got.ConsumerGroup != "PopGroup" {
		t.Fatalf("consumerGroup = %q", got.ConsumerGroup)
	}
	if got.ClientID != f.clientID {
		t.Fatalf("clientId = %q, want %q", got.ClientID, f.clientID)
	}
	if got.StrategyName != "AVG" {
		t.Fatalf("strategyName = %q (Java sends the client strategy's name)", got.StrategyName)
	}
	if got.MessageModel != MessageModelClustering {
		t.Fatalf("messageModel = %q", got.MessageModel)
	}
}

// The reply's queue set is what the POP loops are built from — including the
// queueID=-1 "pop every queue of this broker" form allocate4Pop produces when
// popShareQueueNum lets the whole group share.
func TestBrokerAssignmentBuildsPopQueues(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	}, func(c *DefaultMQPushConsumer) { c.SetClientRebalance(false) })
	f.broker.setAssignmentFixture("TopicA", &assignmentFixture{
		assignments: []*remoting.MessageQueueAssignment{
			popAssignment("TopicA", "b1", 0, remoting.MessageRequestModePop),
			popAssignment("TopicA", "b1", 1, remoting.MessageRequestModePop),
		},
	})
	f.broker.setPopFixture(popFixture{code: remoting.RespPollingTimeout})
	startConsumer(t, c, "TopicA", "*")

	// The assignment is computed synchronously inside Start.
	if !assignedContains(c, common.NewMessageQueue("TopicA", "b1", 0)) ||
		!assignedContains(c, common.NewMessageQueue("TopicA", "b1", 1)) {
		t.Fatalf("assigned = %v, want TopicA q0+q1 from the broker answer", c.AssignedQueues())
	}
	if c.PopProcessQueueCount() != 2 {
		t.Fatalf("pop process queues = %d, want one per assigned queue", c.PopProcessQueueCount())
	}
	// The pop loops must actually pop the broker-assigned queues.
	waitForBudget(t, 5*time.Second, func() bool {
		seen := map[int32]bool{}
		for _, h := range f.broker.popHeadersSnapshot() {
			seen[derefI32(h.QueueID)] = true
		}
		return seen[0] && seen[1]
	})
}

func TestBrokerAssignmentErrorKeepsCurrentAssignment(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	}, func(c *DefaultMQPushConsumer) { c.SetClientRebalance(false) })
	f.broker.setAssignmentFixture("TopicA", &assignmentFixture{
		assignments: []*remoting.MessageQueueAssignment{
			popAssignment("TopicA", "b1", 0, remoting.MessageRequestModePop),
		},
	})
	f.broker.setPopFixture(popFixture{code: remoting.RespPollingTimeout})
	startConsumer(t, c, "TopicA", "*")
	want := common.NewMessageQueue("TopicA", "b1", 0)
	if !assignedContains(c, want) {
		t.Fatalf("initial assignment missing %v: %v", want, c.AssignedQueues())
	}

	// Broker goes bad; force a rebalance round and let another one run after
	// it, so a cleared-then-rebuilt table would show.
	f.broker.setAssignmentFixture("TopicA", &assignmentFixture{code: remoting.RespSystemError})
	before := f.broker.queryAssignmentsFor("TopicA")
	c.RebalanceImmediately()
	waitForBudget(t, 5*time.Second, func() bool {
		return f.broker.queryAssignmentsFor("TopicA") > before
	})
	time.Sleep(300 * time.Millisecond)
	if !assignedContains(c, want) {
		t.Fatalf("assignment lost after broker error, want %v still assigned", want)
	}
}

// An explicit EMPTY set clears the assignment (the broker's javadoc: "empty
// set means the client should clear all load assigned to it before"), while a
// MISSING set (no key) keeps it — the two must stay distinguishable.
func TestBrokerAssignmentEmptySetClearsMissingSetKeeps(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	}, func(c *DefaultMQPushConsumer) { c.SetClientRebalance(false) })
	f.broker.setAssignmentFixture("TopicA", &assignmentFixture{
		assignments: []*remoting.MessageQueueAssignment{
			popAssignment("TopicA", "b1", 0, remoting.MessageRequestModePop),
		},
	})
	f.broker.setPopFixture(popFixture{code: remoting.RespPollingTimeout})
	startConsumer(t, c, "TopicA", "*")
	want := common.NewMessageQueue("TopicA", "b1", 0)
	if !assignedContains(c, want) {
		t.Fatalf("initial assignment missing %v: %v", want, c.AssignedQueues())
	}

	// Missing set (fixture removed -> default SUCCESS with no body): keep.
	before := f.broker.queryAssignmentsFor("TopicA")
	f.broker.setAssignmentFixture("TopicA", nil)
	c.RebalanceImmediately()
	waitForBudget(t, 5*time.Second, func() bool {
		return f.broker.queryAssignmentsFor("TopicA") > before
	})
	time.Sleep(300 * time.Millisecond)
	if !assignedContains(c, want) {
		t.Fatalf("missing set must keep the current assignment, got %v", c.AssignedQueues())
	}

	// Explicit empty set: clear everything.
	f.broker.setAssignmentFixture("TopicA", &assignmentFixture{
		assignments: []*remoting.MessageQueueAssignment{},
	})
	before = f.broker.queryAssignmentsFor("TopicA")
	c.RebalanceImmediately()
	waitForBudget(t, 5*time.Second, func() bool {
		return f.broker.queryAssignmentsFor("TopicA") > before
	})
	waitForBudget(t, 5*time.Second, func() bool {
		return len(c.AssignedQueues()) == 0 && c.PopProcessQueueCount() == 0
	})
}

// The default (clientRebalance=true) must not send QUERY_ASSIGNMENT at all:
// the queue set keeps coming from the local strategy.
func TestClientRebalanceDefaultKeepsLocalPath(t *testing.T) {
	f, c := popFixtureWithConsumer(t, &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	})
	f.broker.setPopFixture(popFixture{code: remoting.RespPollingTimeout})
	startConsumer(t, c, "TopicA", "*")

	waitForBudget(t, 5*time.Second, func() bool { return len(f.broker.popHeadersSnapshot()) >= 1 })
	if got := len(f.broker.queryAssignmentsSnapshot()); got != 0 {
		t.Fatalf("clientRebalance=true sent %d QUERY_ASSIGNMENT requests", got)
	}
	if len(c.AssignedQueues()) == 0 {
		t.Fatalf("local rebalance assigned nothing")
	}
	// The setter flips the path for later rounds only; the local strategy is
	// what built this assignment.
	if !c.IsClientRebalance() {
		t.Fatal("default clientRebalance must be true (Java DefaultMQPushConsumer)")
	}
}

// The broker path also serves a PULL-mode consumer: PULL-mode assignments
// build process queues and pulls, no POP loops.
func TestBrokerAssignmentBuildsPullQueues(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"TopicA": 2})
	c := f.newConsumer(t, "PullGroup", &countingConcurrentlyListener{
		fn: func(_ []*common.MessageExt, _ *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
			return ConsumeSuccess
		},
	}, func(c *DefaultMQPushConsumer) { c.SetClientRebalance(false) })
	f.broker.setAssignmentFixture("TopicA", &assignmentFixture{
		assignments: []*remoting.MessageQueueAssignment{
			popAssignment("TopicA", "b1", 1, remoting.MessageRequestModePull),
		},
	})
	startConsumer(t, c, "TopicA", "*")

	want := common.NewMessageQueue("TopicA", "b1", 1)
	if !assignedContains(c, want) {
		t.Fatalf("assigned = %v, want %v", c.AssignedQueues(), want)
	}
	if c.PopProcessQueueCount() != 0 {
		t.Fatalf("pull consumer grew pop queues: %d", c.PopProcessQueueCount())
	}
	// The pull loop pulls exactly the assigned queue.
	waitForBudget(t, 5*time.Second, func() bool {
		for _, h := range f.broker.pullRequests() {
			if deref(h.Topic) == "TopicA" && derefI32(h.QueueID) == 1 {
				return true
			}
		}
		return false
	})
}
