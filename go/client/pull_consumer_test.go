// DefaultMQPullConsumer tests. They run against the same mock broker/nameserver
// the push-consumer tests use (consumer_test.go), so the wire shape — the
// sysFlag, the extFields and the response header — is what is asserted, not the
// internal call graph.
package client

import (
	"strings"
	"sync"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- fixtures

// newPullConsumer wires a pull consumer to the fixture's nameserver. The
// instance name is set explicitly so ChangeInstanceNameToPID leaves it alone
// (only DEFAULT_INSTANCE_NAME is rewritten).
func newPullConsumer(t *testing.T, f *clusterFixture, group string) *DefaultMQPullConsumer {
	t.Helper()
	c := MustNewDefaultMQPullConsumer(group)
	c.SetNameServerAddresses([]string{f.nserver.addr})
	c.SetInstanceName(f.instName)
	return c
}

// startPullConsumer starts the consumer and registers the cleanup.
func startPullConsumer(t *testing.T, c *DefaultMQPullConsumer) {
	t.Helper()
	requireNoError(t, "pull consumer start", c.Start())
	t.Cleanup(c.Shutdown)
}

func pullTopicMQ(topic string) common.MessageQueue {
	return common.NewMessageQueue(topic, "b1", 0)
}

// ---------------------------------------------------------------- config

// TestPullConsumerStartValidatesConfig covers the four refusals in Java
// DefaultMQPullConsumerImpl.checkConfig:772-817. The interesting one is the
// long-poll invariant — a client that gives up before the broker is allowed to
// hold the request can only ever time out.
func TestPullConsumerStartValidatesConfig(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"PullCfgTopic": 1})

	t.Run("the default group is refused", func(t *testing.T) {
		c := newPullConsumer(t, f, common.DefaultConsumerGroup)
		err := c.Start()
		if err == nil || !strings.Contains(err.Error(), "consumerGroup can not equal") {
			t.Fatalf("Start = %v, want the default-group refusal", err)
		}
	})

	t.Run("a nil allocate strategy is refused", func(t *testing.T) {
		c := newPullConsumer(t, f, "GID_pull_cfg_strategy")
		c.SetAllocateMessageQueueStrategy(nil)
		err := c.Start()
		if err == nil || !strings.Contains(err.Error(), "allocateMessageQueueStrategy is null") {
			t.Fatalf("Start = %v, want the strategy refusal", err)
		}
	})

	t.Run("no nameserver is refused", func(t *testing.T) {
		c := MustNewDefaultMQPullConsumer("GID_pull_cfg_ns")
		c.SetInstanceName(f.instName)
		err := c.Start()
		if err == nil || !strings.Contains(err.Error(), "name server address is not set") {
			t.Fatalf("Start = %v, want the nameserver refusal", err)
		}
	})

	t.Run("suspend timeout below the broker suspend max is refused", func(t *testing.T) {
		c := newPullConsumer(t, f, "GID_pull_cfg_suspend")
		// 20s is the default broker budget; anything below it means the client
		// gives up before the broker is even allowed to answer.
		c.SetBrokerSuspendMaxTimeMillis(20000)
		c.SetConsumerTimeoutMillisWhenSuspend(15000)
		err := c.Start()
		if err == nil || !strings.Contains(err.Error(), "consumerTimeoutMillisWhenSuspend must greater") {
			t.Fatalf("Start = %v, want the long-poll refusal", err)
		}
	})

	t.Run("an exactly equal suspend timeout is accepted", func(t *testing.T) {
		// The Java guard is a strict `<` (DefaultMQPullConsumerImpl:811), so
		// equality is legal — the client gives up at the same instant the
		// broker's hold expires. Pinning it here keeps a future "cleanup" from
		// tightening the comparison into `<=`, which would break every config
		// that deliberately aligns the two clocks.
		c := newPullConsumer(t, f, "GID_pull_cfg_suspend_eq")
		c.SetBrokerSuspendMaxTimeMillis(20000)
		c.SetConsumerTimeoutMillisWhenSuspend(20000)
		requireNoError(t, "start with an equal suspend timeout", c.Start())
		c.Shutdown()
	})

	t.Run("the defaults start clean", func(t *testing.T) {
		c := newPullConsumer(t, f, "GID_pull_cfg_ok")
		requireNoError(t, "start", c.Start())
		defer c.Shutdown()
		if !c.IsStarted() {
			t.Fatal("IsStarted = false after a successful Start")
		}
		// Java DefaultMQPullConsumer enables the STREAM request type in every
		// constructor, so the clientId carries the @STREAM suffix.
		if !strings.Contains(c.ClientID(), "@STREAM") {
			t.Errorf("clientId = %q, want the @STREAM suffix", c.ClientID())
		}
	})
}

// TestPullConsumerSuspendFlagFollowsTheMethod is the regression guard for the
// most expensive trap on this path: `pull()` is a SHORT poll and must NOT set
// the suspend bit. With suspend on, the broker holds an empty queue for
// brokerSuspendMaxTimeMillis (20s) while the client gives up at
// consumerPullTimeoutMillis (10s) — a guaranteed timeout that only shows up
// against a real broker with an empty queue.
func TestPullConsumerSuspendFlagFollowsTheMethod(t *testing.T) {
	const topic = "PullSuspendTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "one", "two")
	c := newPullConsumer(t, f, "GID_pull_suspend")
	startPullConsumer(t, c)
	mq := pullTopicMQ(topic)

	if _, err := c.Pull(mq, "*", 0, 32); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	first := f.broker.firstPull()
	if first == nil {
		t.Fatal("no pull reached the broker")
	}
	if common.HasSuspendFlag(derefI32(first.SysFlag)) {
		t.Error("Pull() set the SUSPEND bit — the short poll would block until the broker's suspend budget")
	}
	if !common.HasSubscriptionFlag(derefI32(first.SysFlag)) {
		t.Error("Pull() must carry the SUBSCRIPTION bit")
	}
	if common.HasCommitOffsetFlag(derefI32(first.SysFlag)) {
		t.Error("Pull() must not commit offsets — the caller owns the cursor")
	}

	before := f.broker.pullCount()
	if _, err := c.PullBlockIfNotFound(mq, "*", 0, 32); err != nil {
		t.Fatalf("PullBlockIfNotFound: %v", err)
	}
	pulls := f.broker.pullRequests()
	if len(pulls) <= before {
		t.Fatal("the long poll never reached the broker")
	}
	last := pulls[len(pulls)-1]
	if !common.HasSuspendFlag(derefI32(last.SysFlag)) {
		t.Error("PullBlockIfNotFound() must set the SUSPEND bit")
	}
	// The long poll's budget switches to consumerTimeoutMillisWhenSuspend.
	if got := derefI64(last.SuspendTimeoutMillis); got != DefaultBrokerSuspendMaxTimeMillis {
		t.Errorf("suspendTimeoutMillis = %d, want %d", got, DefaultBrokerSuspendMaxTimeMillis)
	}
}

// TestPullConsumerReturnsMessagesFromTheGivenOffset: the offset in the request
// is what the caller passed, and the result's messages come back decoded with
// their queue/consumer metadata filled in.
func TestPullConsumerReturnsMessagesFromTheGivenOffset(t *testing.T) {
	const topic = "PullOffsetTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	f.broker.add(topic, 0, "m0", "m1", "m2")
	c := newPullConsumer(t, f, "GID_pull_offset")
	startPullConsumer(t, c)
	mq := pullTopicMQ(topic)

	res, err := c.Pull(mq, "*", 1, 32)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if res.PullStatus != PullFound {
		t.Fatalf("status = %s, want FOUND", res.PullStatus)
	}
	if len(res.MsgFoundList) != 2 {
		t.Fatalf("got %d messages, want 2 (offsets 1 and 2)", len(res.MsgFoundList))
	}
	if got := string(res.MsgFoundList[0].Body); got != "m1" {
		t.Errorf("first body = %q, want m1", got)
	}
	if res.NextBeginOffset != 3 {
		t.Errorf("nextBeginOffset = %d, want 3", res.NextBeginOffset)
	}
	// The queue metadata must be filled from the request, not from the wire:
	// a message with an empty BrokerName cannot be sent back later.
	for _, msg := range res.MsgFoundList {
		if msg.BrokerName != "b1" || msg.QueueID != 0 {
			t.Errorf("message queue metadata = %s/%d, want b1/0", msg.BrokerName, msg.QueueID)
		}
	}

	// A pull past the end answers NO_NEW_MSG rather than an error.
	empty, err := c.Pull(mq, "*", 99, 32)
	if err != nil {
		t.Fatalf("Pull past the end: %v", err)
	}
	if empty.PullStatus != PullNoNewMsg {
		t.Errorf("status = %s, want NO_NEW_MSG", empty.PullStatus)
	}
}

// TestPullConsumerAutoSubscribesAPulledTopic: pulling a topic the client never
// registered still has to register it, or the heartbeat never mentions it and
// the broker's consumer manager cannot reach the group.
func TestPullConsumerAutoSubscribesAPulledTopic(t *testing.T) {
	const topic = "PullAutoSubTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "auto")
	c := newPullConsumer(t, f, "GID_pull_autosub")
	startPullConsumer(t, c)

	if subs := c.Subscriptions(); len(subs) != 0 {
		t.Fatalf("subscriptions before the pull = %d, want 0", len(subs))
	}
	if _, err := c.Pull(pullTopicMQ(topic), "*", 0, 32); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	subs := c.Subscriptions()
	if len(subs) != 1 || subs[0].Topic != topic {
		t.Fatalf("subscriptions after the pull = %v, want just %s", subs, topic)
	}
	// SUB_ALL keeps the tag set empty (see FilterAPI).
	if len(subs[0].TagsSet) != 0 {
		t.Errorf("auto-subscription tagsSet = %v, want empty", subs[0].TagsSet)
	}
}

// TestPullConsumerTracksSuggestWhichBrokerID: every response rewrites the
// pull-from-which-node table, and an ABSENT field means master(0) — not "keep
// the previous value". Getting that wrong pins a queue to a dead slave.
func TestPullConsumerTracksSuggestWhichBrokerID(t *testing.T) {
	const topic = "PullSuggestTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "s0")
	c := newPullConsumer(t, f, "GID_pull_suggest")
	startPullConsumer(t, c)
	mq := pullTopicMQ(topic)

	f.broker.mu.Lock()
	f.broker.suggest = remoting.I32Ptr(1)
	f.broker.mu.Unlock()
	if _, err := c.Pull(mq, "*", 0, 32); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if id, ok := c.pullAPI.PullFromWhichNode(mq); !ok || id != 1 {
		t.Fatalf("pullFromWhichNode = %d (present=%v), want 1", id, ok)
	}

	// Now the broker stops suggesting and the table must fall back to master.
	f.broker.mu.Lock()
	f.broker.suggest = nil
	f.broker.mu.Unlock()
	if _, err := c.Pull(mq, "*", 0, 32); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if id, _ := c.pullAPI.PullFromWhichNode(mq); id != int64(common.MasterID) {
		t.Errorf("pullFromWhichNode = %d after an absent suggest, want master(0)", id)
	}
}

// TestPullConsumerOffsetRoundTrip: UpdateConsumeOffset is local, Persist sends
// it, and FetchConsumeOffset reads what the store holds. The pull path never
// commits by itself — that is the caller's job.
func TestPullConsumerOffsetRoundTrip(t *testing.T) {
	const topic = "PullOffsetStoreTopic"
	const group = "GID_pull_offsetstore"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "a", "b", "c")
	c := newPullConsumer(t, f, group)
	startPullConsumer(t, c)
	mq := pullTopicMQ(topic)

	if _, err := c.Pull(mq, "*", 0, 1); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if _, ok := f.broker.lastCommit(topic, 0); ok {
		t.Fatal("Pull() committed an offset — the caller owns the cursor")
	}

	requireNoError(t, "update offset", c.UpdateConsumeOffset(mq, 3))
	got, err := c.FetchConsumeOffset(mq, false)
	if err != nil {
		t.Fatalf("FetchConsumeOffset: %v", err)
	}
	if got != 3 {
		t.Fatalf("FetchConsumeOffset = %d, want 3", got)
	}
	// A backwards seek is allowed (Java updateOffset(..., increaseOnly=false)).
	requireNoError(t, "rewind", c.UpdateConsumeOffset(mq, 1))
	if got, _ := c.FetchConsumeOffset(mq, false); got != 1 {
		t.Errorf("after a rewind FetchConsumeOffset = %d, want 1", got)
	}

	requireNoError(t, "persist", c.PersistConsumerOffset())
	commit, ok := f.broker.lastCommit(topic, 0)
	if !ok {
		t.Fatal("PersistConsumerOffset sent nothing to the broker")
	}
	if commit.group != group || commit.offset != 1 {
		t.Errorf("committed %s/%d, want %s/1", commit.group, commit.offset, group)
	}

	// The direct-to-broker variant does not need the store.
	requireNoError(t, "to broker", c.UpdateConsumeOffsetToBroker(mq, 2))
	if commit, ok := f.broker.lastCommit(topic, 0); !ok || commit.offset != 2 {
		t.Errorf("UpdateConsumeOffsetToBroker committed %v, want 2", commit)
	}
}

// TestPullConsumerPersistsOnlyPulledQueues pins the port's stand-in for Java's
// process-queue table. RemoteBrokerOffsetStore.PersistAll DROPS the entries for
// queues left out, so the set handed to it must cover everything the consumer
// touched — and this test makes the two halves of that set differ on purpose:
// q0 is pulled AND has a local offset, q1 only has a local offset. A pulled-only
// set would drop q1 and silently lose the caller's progress; a local-only set
// would be fine here but would lose an offset a pull recorded for a queue the
// caller never updated.
func TestPullConsumerPersistsOnlyPulledQueues(t *testing.T) {
	const topic = "PullPersistSetTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	f.broker.add(topic, 0, "q0")
	f.broker.add(topic, 1, "q1")
	c := newPullConsumer(t, f, "GID_pull_persistset")
	startPullConsumer(t, c)

	q0 := common.NewMessageQueue(topic, "b1", 0)
	q1 := common.NewMessageQueue(topic, "b1", 1)
	// q0 enters via the pull path...
	if _, err := c.Pull(q0, "*", 0, 32); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	// ...and both enter via the offset path, q1 through that path alone.
	requireNoError(t, "update q0", c.UpdateConsumeOffset(q0, 1))
	requireNoError(t, "update q1", c.UpdateConsumeOffset(q1, 1))
	requireNoError(t, "persist", c.PersistConsumerOffset())

	commit0, ok0 := f.broker.lastCommit(topic, 0)
	if !ok0 {
		t.Fatal("queue 0's offset was not persisted")
	}
	if commit0.offset != 1 {
		t.Errorf("queue 0 committed %d, want 1", commit0.offset)
	}
	commit1, ok1 := f.broker.lastCommit(topic, 1)
	if !ok1 {
		t.Fatal("queue 1's offset was dropped — the persist set is a pulled-only set " +
			"and PersistAll removes what it is not handed")
	}
	if commit1.offset != 1 {
		t.Errorf("queue 1 committed %d, want 1", commit1.offset)
	}

	// PulledQueues stays the honest pull-side record: q1 was never pulled.
	pulled := c.PulledQueues()
	if len(pulled) != 1 || pulled[0].QueueID != 0 {
		t.Fatalf("PulledQueues = %v, want just queue 0", pulled)
	}
}

// TestPullConsumerSendMessageBackUsesThePullCeiling guards the value that is
// easy to get wrong by copying the push consumer: Java's DefaultMQPullConsumer
// defaults maxReconsumeTimes to 16 (:95), while the PUSH one defaults to -1.
// The broker takes the field at face value for any client version >= V3_4_9 and
// DLQs when reconsumeTimes(0) >= maxReconsumeTimes — so -1 would send every
// bounced message straight to %DLQ%.
func TestPullConsumerSendMessageBackUsesThePullCeiling(t *testing.T) {
	const topic = "PullSendBackTopic"
	const group = "GID_pull_sendback"
	f := newClusterFixture(t, map[string]int{topic: 1})
	msgs := f.broker.add(topic, 0, "bounce-me")
	c := newPullConsumer(t, f, group)
	startPullConsumer(t, c)

	// The address comes from the PUBLISH table, so the consumer has to have
	// looked the topic up first — the same constraint Java has.
	if _, err := c.Pull(pullTopicMQ(topic), "*", 0, 32); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	msg := msgs[0]
	msg.BrokerName = "b1"
	requireNoError(t, "send back", c.SendMessageBack(msg, 3, "b1"))

	backs := f.broker.sendBackSnapshot()
	if len(backs) != 1 {
		t.Fatalf("send-backs = %d, want 1", len(backs))
	}
	if got := derefI32(backs[0].MaxReconsumeTimes); got != DefaultPullConsumerMaxReconsumeTimes {
		t.Errorf("maxReconsumeTimes = %d, want %d (NOT the push consumer's -1)",
			got, DefaultPullConsumerMaxReconsumeTimes)
	}
	if got := derefI32(backs[0].DelayLevel); got != 3 {
		t.Errorf("delayLevel = %d, want 3", got)
	}
	if got := derefI64(backs[0].Offset); got != msg.CommitLogOffset {
		t.Errorf("offset = %d, want the commitLogOffset %d", got, msg.CommitLogOffset)
	}
	if got := deref(backs[0].Group); got != group {
		t.Errorf("group = %q, want %q", got, group)
	}

	// An explicit ceiling must be honoured.
	c.SetMaxReconsumeTimes(7)
	requireNoError(t, "send back again", c.SendMessageBack(msg, 1, "b1"))
	backs = f.broker.sendBackSnapshot()
	if got := derefI32(backs[len(backs)-1].MaxReconsumeTimes); got != 7 {
		t.Errorf("maxReconsumeTimes = %d after SetMaxReconsumeTimes(7)", got)
	}
}

// TestPullConsumerRejectsBadArguments covers pullSyncImpl:232-244. These are
// caller errors, reported before anything reaches the wire.
func TestPullConsumerRejectsBadArguments(t *testing.T) {
	const topic = "PullArgsTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	c := newPullConsumer(t, f, "GID_pull_args")
	startPullConsumer(t, c)
	mq := pullTopicMQ(topic)

	cases := []struct {
		name string
		call func() error
		want string
	}{
		{"negative offset", func() error { _, err := c.Pull(mq, "*", -1, 32); return err }, "offset < 0"},
		{"zero maxNums", func() error { _, err := c.Pull(mq, "*", 0, 0); return err }, "maxNums <= 0"},
		{"empty topic", func() error {
			_, err := c.Pull(common.MessageQueue{BrokerName: "b1"}, "*", 0, 32)
			return err
		}, "mq is null"},
		{"empty topic on the long poll", func() error {
			_, err := c.PullBlockIfNotFound(common.MessageQueue{BrokerName: "b1"}, "*", 0, 32)
			return err
		}, "mq is null"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
	if got := f.broker.pullCount(); got != 0 {
		t.Errorf("a rejected pull reached the broker %d times, want 0", got)
	}

	// A consumer that never started refuses too.
	idle := newPullConsumer(t, f, "GID_pull_idle")
	_, err := idle.Pull(mq, "*", 0, 32)
	if err == nil || !strings.Contains(err.Error(), "consumer not started") {
		t.Fatalf("Pull on a stopped consumer = %v, want the not-started refusal", err)
	}
}

// TestPullConsumerFilterHookRunsInsideThePull: the filter hook is applied by
// processPullResult, i.e. before the caller ever sees the messages — the same
// place the push path filters.
func TestPullConsumerFilterHookRunsInsideThePull(t *testing.T) {
	const topic = "PullFilterHookTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "keep", "drop")
	c := newPullConsumer(t, f, "GID_pull_filterhook")
	hook := &droppingFilterHook{dropBody: "drop"}
	c.RegisterFilterMessageHook(hook)
	startPullConsumer(t, c)

	if !c.HasFilterMessageHook() {
		t.Fatal("HasFilterMessageHook = false after registering one")
	}
	res, err := c.Pull(pullTopicMQ(topic), "*", 0, 32)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if hook.calls() == 0 {
		t.Fatal("the filter hook never ran")
	}
	if len(res.MsgFoundList) != 1 || string(res.MsgFoundList[0].Body) != "keep" {
		bodies := make([]string, 0, len(res.MsgFoundList))
		for _, m := range res.MsgFoundList {
			bodies = append(bodies, string(m.Body))
		}
		t.Fatalf("delivered %v, want just [keep]", bodies)
	}
	// The cursor still advances past the dropped message: the caller decides
	// the offset, and processPullResult is not allowed to rewind it.
	if res.NextBeginOffset != 2 {
		t.Errorf("nextBeginOffset = %d, want 2", res.NextBeginOffset)
	}
}

// droppingFilterHook removes one body from the delivery list.
type droppingFilterHook struct {
	dropBody string

	callCount int
}

func (h *droppingFilterHook) HookName() string { return "droppingFilterHook" }

func (h *droppingFilterHook) FilterMessage(ctx *FilterMessageContext) {
	h.callCount++
	kept := make([]*common.MessageExt, 0, len(ctx.MsgList))
	for _, msg := range ctx.MsgList {
		if string(msg.Body) == h.dropBody {
			continue
		}
		kept = append(kept, msg)
	}
	ctx.MsgList = kept
}

func (h *droppingFilterHook) calls() int { return h.callCount }

// TestPullConsumerConsumeHookPairRunsAroundThePull: the pull path has no consume
// service, so Java hangs the pair off the pull result (pullSyncImpl:270-283)
// with success already true.
func TestPullConsumerConsumeHookPairRunsAroundThePull(t *testing.T) {
	const topic = "PullConsumeHookTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "hooked")
	c := newPullConsumer(t, f, "GID_pull_consumehook")
	hook := &recordingConsumeHook{}
	c.RegisterConsumeMessageHook(hook)
	startPullConsumer(t, c)

	if _, err := c.Pull(pullTopicMQ(topic), "*", 0, 32); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	before, after := hook.counts()
	if before != 1 || after != 1 {
		t.Fatalf("hook pair ran %d/%d times, want 1/1", before, after)
	}
	ctx := hook.last()
	if ctx == nil {
		t.Fatal("the hook saw no context")
	}
	if ctx.ConsumerGroup != "GID_pull_consumehook" {
		t.Errorf("consumerGroup = %q", ctx.ConsumerGroup)
	}
	if !ctx.Success || ctx.Status != "CONSUME_SUCCESS" {
		t.Errorf("success/status = %v/%q, want true/CONSUME_SUCCESS", ctx.Success, ctx.Status)
	}
	if ctx.AccessChannel != AccessChannelLocal {
		t.Errorf("accessChannel = %q, want %q", ctx.AccessChannel, AccessChannelLocal)
	}
	if len(ctx.MsgList) != 1 {
		t.Errorf("hook msgList = %d messages, want 1", len(ctx.MsgList))
	}
}

// recordingConsumeHook counts the pair and keeps the last context.
type recordingConsumeHook struct {
	before  int
	after   int
	lastCtx *ConsumeMessageContext
}

func (h *recordingConsumeHook) HookName() string { return "recordingConsumeHook" }

func (h *recordingConsumeHook) ConsumeMessageBefore(ctx *ConsumeMessageContext) { h.before++ }

func (h *recordingConsumeHook) ConsumeMessageAfter(ctx *ConsumeMessageContext) {
	h.after++
	h.lastCtx = ctx
}

func (h *recordingConsumeHook) counts() (int, int) { return h.before, h.after }

func (h *recordingConsumeHook) last() *ConsumeMessageContext { return h.lastCtx }

// TestPullConsumerQueueLookups: subscribe info drives the queue set the caller
// iterates, publish info drives writes, and ParseSubscribeMessageQueues keeps
// only what this consumer subscribed to.
func TestPullConsumerQueueLookups(t *testing.T) {
	const topic = "PullQueuesTopic"
	f := newClusterFixture(t, map[string]int{topic: 3})
	c := newPullConsumer(t, f, "GID_pull_queues")
	startPullConsumer(t, c)

	mqs, err := c.FetchSubscribeMessageQueues(topic)
	if err != nil {
		t.Fatalf("FetchSubscribeMessageQueues: %v", err)
	}
	if len(mqs) != 3 {
		t.Fatalf("subscribe queues = %d, want 3", len(mqs))
	}
	for i, mq := range mqs {
		if mq.QueueID != int32(i) || mq.BrokerName != "b1" {
			t.Errorf("queue[%d] = %s/%d, want b1/%d", i, mq.BrokerName, mq.QueueID, i)
		}
	}

	pub, err := c.FetchPublishMessageQueues(topic)
	if err != nil {
		t.Fatalf("FetchPublishMessageQueues: %v", err)
	}
	if len(pub) != 3 {
		t.Errorf("publish queues = %d, want 3", len(pub))
	}

	// Nothing is subscribed yet, so the parse keeps nothing; after a manual
	// Subscribe it keeps that topic's queues only.
	if got := c.ParseSubscribeMessageQueues(mqs); len(got) != 0 {
		t.Errorf("ParseSubscribeMessageQueues before Subscribe = %d, want 0", len(got))
	}
	requireNoError(t, "subscribe", c.Subscribe(topic, "TagA"))
	kept := c.ParseSubscribeMessageQueues(append(mqs, common.NewMessageQueue("Other", "b1", 0)))
	if len(kept) != 3 {
		t.Errorf("ParseSubscribeMessageQueues = %d, want the 3 subscribed queues", len(kept))
	}
}

// TestPullConsumerCreateTopic pins the UPDATE_AND_CREATE_TOPIC(17) extFields.
//
// It is not a formality: the real broker parses `attributes` as `k=v;k=v` and
// rejects anything else with "kv string format wrong", so a positional-argument
// mix-up that sends the topic NAME there only shows up against a live cluster.
// The request is also the only place the read/write queue counts and the filter
// type are visible.
func TestPullConsumerCreateTopic(t *testing.T) {
	// TBW102 has to be routable: Java routes the create through the DEFAULT
	// topic's route to find the masters to push to.
	f := newClusterFixture(t, map[string]int{"PullCreateTopic": 1, common.DefaultTopic: 1})
	c := newPullConsumer(t, f, "GID_pull_create")
	startPullConsumer(t, c)

	requireNoError(t, "create topic", c.CreateTopic("TBW102", "PullCreatedTopic", 2, 0))
	reqs := f.broker.createTopicRequests()
	if len(reqs) == 0 {
		t.Fatal("CreateTopic sent no UPDATE_AND_CREATE_TOPIC")
	}
	ext := reqs[0]
	want := map[string]string{
		"topic":           "PullCreatedTopic",
		"defaultTopic":    "TBW102",
		"readQueueNums":   "2",
		"writeQueueNums":  "2",
		"perm":            "6",
		"topicFilterType": "SINGLE_TAG",
		"topicSysFlag":    "0",
		"order":           "false",
		"force":           "false",
		// Java AttributeParser.parseToString(empty map) == "", NOT the key
		// argument and NOT a JSON object.
		"attributes": "",
		// The pull consumer enables enableStreamRequestType in EVERY constructor,
		// so the hook writes ReqT on the admin path too (remoting/acl.go:237).
		"ReqT": "0",
	}
	for key, value := range want {
		got, ok := ext.Get(key)
		if !ok {
			t.Errorf("extFields is missing %q (Java writes every one of them)", key)
			continue
		}
		if got != value {
			t.Errorf("extFields[%q] = %q, want %q", key, got, value)
		}
	}
	if got := len(ext.Keys()); got != len(want) {
		t.Errorf("extFields has %d keys, want exactly %d: %v", got, len(want), ext.Keys())
	}
}

// TestPullConsumerOffsetAdminLookups: max/min/search/earliest go through the
// master-only admin path, not through the offset store.
func TestPullConsumerOffsetAdminLookups(t *testing.T) {
	const topic = "PullAdminOffsetTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "a", "b")
	c := newPullConsumer(t, f, "GID_pull_adminoffset")
	startPullConsumer(t, c)
	mq := pullTopicMQ(topic)

	max, err := c.MaxOffset(mq)
	if err != nil {
		t.Fatalf("MaxOffset: %v", err)
	}
	if max != 2 {
		t.Errorf("MaxOffset = %d, want 2", max)
	}
	min, err := c.MinOffset(mq)
	if err != nil {
		t.Fatalf("MinOffset: %v", err)
	}
	if min != 0 {
		t.Errorf("MinOffset = %d, want 0", min)
	}
	if _, err := c.SearchOffset(mq, common.CurrentTimeMillis()); err != nil {
		t.Fatalf("SearchOffset: %v", err)
	}
	if _, err := c.EarliestMsgStoreTime(mq); err != nil {
		t.Fatalf("EarliestMsgStoreTime: %v", err)
	}
	if len(f.broker.searchTimestamps()) != 1 {
		t.Errorf("search requests = %d, want 1", len(f.broker.searchTimestamps()))
	}
}

// TestPullConsumerResetOffsetAndStatus: RESET_CONSUMER_CLIENT_OFFSET(220) lands
// in the store and is persisted, and GET_CONSUMER_STATUS_FROM_CLIENT(221)
// reports what the store holds.
func TestPullConsumerResetOffsetAndStatus(t *testing.T) {
	const topic = "PullResetTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	f.broker.add(topic, 0, "r0")
	f.broker.add(topic, 1, "r1")
	c := newPullConsumer(t, f, "GID_pull_reset")
	startPullConsumer(t, c)

	q0 := common.NewMessageQueue(topic, "b1", 0)
	requireNoError(t, "update", c.UpdateConsumeOffset(q0, 1))
	c.ResetOffset(topic, remoting.MQOffsetTable{{Queue: q0, Offset: 0}})

	status := c.GetConsumerStatus(nil)
	if len(status) == 0 {
		t.Fatal("GetConsumerStatus returned nothing")
	}
	found := false
	for _, entry := range status {
		if entry.Queue == q0 {
			found = true
			if entry.Offset != 0 {
				t.Errorf("status offset = %d, want the reset value 0", entry.Offset)
			}
		}
	}
	if !found {
		t.Errorf("status has no entry for %v", q0)
	}
	if _, ok := f.broker.lastCommit(topic, 0); !ok {
		t.Error("ResetOffset did not persist to the broker")
	}

	// A topic filter that matches nothing yields nothing.
	if got := c.GetConsumerStatus(strPtrLocal("NoSuchTopic")); len(got) != 0 {
		t.Errorf("status for an unknown topic = %d entries, want 0", len(got))
	}
}

func strPtrLocal(s string) *string { return &s }

// TestPullConsumerShutdownUnregistersTheGroup: the broker drops the group
// immediately instead of keeping the clientId until the channel scan (~120s).
func TestPullConsumerShutdownUnregistersTheGroup(t *testing.T) {
	const topic = "PullShutdownTopic"
	f := newClusterFixture(t, map[string]int{topic: 1})
	c := newPullConsumer(t, f, "GID_pull_shutdown")
	startPullConsumer(t, c)

	// A pull first, so the group is known to the broker and there is an offset
	// to persist on the way out.
	f.broker.add(topic, 0, "bye")
	if _, err := c.Pull(pullTopicMQ(topic), "*", 0, 1); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	requireNoError(t, "update", c.UpdateConsumeOffset(pullTopicMQ(topic), 1))

	c.Shutdown()
	if c.IsStarted() {
		t.Error("IsStarted = true after Shutdown")
	}
	if f.broker.indexOfReq(remoting.ReqUnregisterClient) < 0 {
		t.Error("Shutdown did not unregister the client at the broker")
	}
	if _, ok := f.broker.lastCommit(topic, 0); !ok {
		t.Error("Shutdown did not persist the offsets")
	}

	// Shutdown is idempotent, and a second Start after it is allowed.
	c.Shutdown()
}

// TestPullConsumerRebalanceNotifiesTheQueueListener: this port has no pull-side
// assignment, so the listener is told mqAll == mqDivided — claiming a subset was
// "yours" would be a lie the caller might act on.
func TestPullConsumerRebalanceNotifiesTheQueueListener(t *testing.T) {
	const topic = "PullListenerTopic"
	f := newClusterFixture(t, map[string]int{topic: 2})
	c := newPullConsumer(t, f, "GID_pull_listener")
	listener := &recordingQueueListener{}
	c.SetMessageQueueListener(listener)
	requireNoError(t, "subscribe", c.Subscribe(topic, "*"))
	startPullConsumer(t, c)

	c.RebalanceImmediately()
	waitFor(t, "the queue listener to fire", func() bool { return listener.calls() > 0 })

	gotTopic, mqAll, mqDivided := listener.last()
	if gotTopic != topic {
		t.Errorf("topic = %q, want %q", gotTopic, topic)
	}
	if len(mqAll) != 2 || len(mqDivided) != 2 {
		t.Errorf("mqAll/mqDivided = %d/%d, want 2/2", len(mqAll), len(mqDivided))
	}
}

// recordingQueueListener records the last notification.
type recordingQueueListener struct {
	mu        sync.Mutex
	callCount int
	topic     string
	all       []common.MessageQueue
	divided   []common.MessageQueue
}

func (l *recordingQueueListener) MessageQueueChanged(topic string, mqAll, mqDivided []common.MessageQueue) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.callCount++
	l.topic = topic
	l.all = mqAll
	l.divided = mqDivided
}

func (l *recordingQueueListener) calls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.callCount
}

func (l *recordingQueueListener) last() (string, []common.MessageQueue, []common.MessageQueue) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.topic, l.all, l.divided
}

// TestPullConsumerConsumeTypeIsActive and the model/unit accessors: the
// heartbeat's ConsumerData must say CONSUME_ACTIVELY, or the console (and the
// broker's own logic in some paths) treats the group as a push consumer.
func TestPullConsumerConsumeTypeIsActive(t *testing.T) {
	c := MustNewDefaultMQPullConsumer("GID_pull_consume_type")
	if got := c.ConsumeType(); got != ConsumeTypeActively {
		t.Errorf("ConsumeType = %q, want %q", got, ConsumeTypeActively)
	}
	if got := c.MessageModel(); got != MessageModelClustering {
		t.Errorf("MessageModel = %q, want %q", got, MessageModelClustering)
	}
	if got := c.ConsumeFromWhere(); got != ConsumeFromWhereLastOffset {
		t.Errorf("ConsumeFromWhere = %q", got)
	}
	if c.IsUnitMode() {
		t.Error("IsUnitMode = true by default")
	}
	// An invalid group is refused by the constructor, not at Start.
	if _, err := NewDefaultMQPullConsumer(""); err == nil {
		t.Error("NewDefaultMQPullConsumer(\"\") = nil error")
	}
	// The default strategies/timings are Java's. ConsumerPullTimeoutMillis and
	// BrokerSuspendMaxTimeMillis are the pair covered by the long-poll
	// invariant, so assert them together with MaxReconsumeTimes (16, not -1).
	if got := c.ConsumerPullTimeoutMillis(); got != DefaultConsumerPullTimeoutMillis {
		t.Errorf("consumerPullTimeoutMillis = %d, want %d", got, DefaultConsumerPullTimeoutMillis)
	}
	if got := c.BrokerSuspendMaxTimeMillis(); got != DefaultBrokerSuspendMaxTimeMillis {
		t.Errorf("brokerSuspendMaxTimeMillis = %d, want %d", got, DefaultBrokerSuspendMaxTimeMillis)
	}
	if got := c.ConsumerTimeoutMillisWhenSuspend(); got != DefaultConsumerTimeoutMillisWhenSuspend {
		t.Errorf("consumerTimeoutMillisWhenSuspend = %d, want %d", got, DefaultConsumerTimeoutMillisWhenSuspend)
	}
	if got := c.MaxReconsumeTimes(); got != DefaultPullConsumerMaxReconsumeTimes {
		t.Errorf("maxReconsumeTimes = %d, want %d (NOT the push consumer's -1)", got, DefaultPullConsumerMaxReconsumeTimes)
	}
	if c.ConsumerTimeoutMillisWhenSuspend() < c.BrokerSuspendMaxTimeMillis() {
		t.Errorf("default timing violates the long-poll invariant: %d < %d",
			c.ConsumerTimeoutMillisWhenSuspend(), c.BrokerSuspendMaxTimeMillis())
	}
}
