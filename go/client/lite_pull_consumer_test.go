package client

import (
	"strings"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- fixtures

// newLiteConsumer wires a lite consumer to the fixture's nameserver under the
// instance name whose clientId the mock broker knows about.
func newLiteConsumer(t *testing.T, f *clusterFixture, group string) *DefaultLitePullConsumer {
	t.Helper()
	c := MustNewDefaultLitePullConsumer(group)
	c.SetNameServerAddresses([]string{f.nserver.addr})
	c.SetInstanceName(f.instName)
	return c
}

func liteClientID(f *clusterFixture) string {
	return common.ClientIDFor(f.instName, "", true)
}

// pollAll collects bodies through Poll until want arrived or the deadline
// passed.
func pollAll(t *testing.T, c *DefaultLitePullConsumer, want int, within time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(within)
	var bodies []string
	for len(bodies) < want && time.Now().Before(deadline) {
		messages, err := c.PollWithTimeout(200)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range messages {
			bodies = append(bodies, string(m.Body))
		}
	}
	if len(bodies) < want {
		t.Fatalf("poll collected %d bodies, want %d: %v", len(bodies), want, bodies)
	}
	return bodies
}

// preCommit writes a broker-side committed offset (as a previous run of the
// same group would have left behind).
func preCommit(f *clusterFixture, group, topic string, queueID int32, offset int64) {
	f.broker.mu.Lock()
	defer f.broker.mu.Unlock()
	f.broker.committed[commitKey(group, topic, queueID)] = offset
}

// ---------------------------------------------------------------- validation

func TestLitePullConsumerStartValidation(t *testing.T) {
	if _, err := NewDefaultLitePullConsumer(""); err == nil {
		t.Fatal("a blank consumerGroup must be rejected by the constructor")
	}

	c := MustNewDefaultLitePullConsumer(common.DefaultConsumerGroup)
	c.SetNameServerAddresses([]string{"127.0.0.1:1"})
	c.Subscribe("T", "*")
	if err := c.Start(); err == nil || !strings.Contains(err.Error(), "can not equal") {
		t.Fatalf("DEFAULT_CONSUMER group: err = %v", err)
	}
	if c.IsStarted() {
		t.Fatal("a rejected start must leave the consumer not started")
	}

	c2 := MustNewDefaultLitePullConsumer("GID_lite_validate")
	c2.Subscribe("T", "*")
	if err := c2.Start(); err == nil || !strings.Contains(err.Error(), "name server address is not set") {
		t.Fatalf("missing name server: err = %v", err)
	}

	c3 := MustNewDefaultLitePullConsumer("GID_lite_validate")
	c3.SetNameServerAddresses([]string{"127.0.0.1:1"})
	// No subscription is required at Start: subscribe() while RUNNING is legal.
	if err := c3.Start(); err != nil {
		t.Fatalf("start without a subscription: %v", err)
	}
	if err := c3.Start(); err == nil ||
		!strings.Contains(err.Error(), "The PullConsumer service state not OK, maybe started once, RUNNING") {
		t.Fatalf("second start: err = %v", err)
	}
	c3.Shutdown()
	if err := c3.Start(); err == nil ||
		!strings.Contains(err.Error(), "The PullConsumer service state not OK, maybe started once, SHUTDOWN_ALREADY") {
		t.Fatalf("start after shutdown: err = %v", err)
	}

	c4 := MustNewDefaultLitePullConsumer("GID_lite_validate")
	c4.SetNameServerAddresses([]string{"127.0.0.1:1"})
	c4.Subscribe("T", "*")
	c4.SetConsumeTimestamp("20260132") // month 32: a parse failure, not a valid date
	if err := c4.Start(); err == nil || !strings.Contains(err.Error(), "consumeTimestamp is invalid") {
		t.Fatalf("bad consumeTimestamp: err = %v", err)
	}

	c5 := MustNewDefaultLitePullConsumer("GID_lite_validate")
	c5.SetNameServerAddresses([]string{"127.0.0.1:1"})
	c5.SetConsumerTimeoutMillisWhenSuspend(c5.BrokerSuspendMaxTimeMillis() - 1)
	if err := c5.Start(); err == nil ||
		!strings.Contains(err.Error(), "consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis") {
		t.Fatalf("suspend budget below the broker's: err = %v", err)
	}
}

// A consumer may start with no subscription at all; subscribing while RUNNING
// must wire the route and begin delivery without a restart.
func TestLitePullConsumerSubscribeAfterStart(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	f.broker.setClientIDs(liteClientID(f))
	c := newLiteConsumer(t, f, "GID_lite_late_subscribe")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	requireNoError(t, "late subscribe", c.Subscribe("T", "*"))
	f.broker.add("T", 0, "late-0")
	got := pollAll(t, c, 1, 3*time.Second)
	if len(got) != 1 || got[0] != "late-0" {
		t.Fatalf("late subscription delivered %v, want [late-0]", got)
	}
}

// ---------------------------------------------------------------- assign mode

func TestLitePullConsumerAssignModeEndToEnd(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	c := newLiteConsumer(t, f, "GID_lite_assign")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	c.SetPullBatchSize(7)
	mq := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}
	c.Assign([]common.MessageQueue{mq})
	c.SetSubExpressionForAssign("T", "TagA || TagB")
	// Three messages match the expression; m3 carries TagC and must be dropped
	// by the client-side filter (the mock broker does no server-side filtering).
	batch := f.broker.add("T", 0, "m0", "m1", "m2", "m3")
	batch[0].PutProperty(common.PropertyTags, "TagA")
	batch[1].PutProperty(common.PropertyTags, "TagB")
	batch[2].PutProperty(common.PropertyTags, "TagA")
	batch[3].PutProperty(common.PropertyTags, "TagC")
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	msgs := pollAll(t, c, 3, 3*time.Second)
	if len(msgs) != 3 {
		t.Fatalf("delivered %d messages, want 3", len(msgs))
	}
	for i, want := range []string{"m0", "m1", "m2"} {
		if msgs[i] != want {
			t.Errorf("delivered[%d] = %q, want %q", i, msgs[i], want)
		}
	}

	// The wire: code 361, lite+subscription bits, no suspend / commitOffset,
	// the expression present, the batch size, SubVersion 0 for TAG.
	if f.broker.indexOfReq(remoting.ReqLitePullMessage) < 0 {
		t.Fatal("no LITE_PULL_MESSAGE(361) reached the broker")
	}
	pull := f.broker.pullRequests()[0]
	if deref(pull.Topic) != "T" || derefI32(pull.QueueID) != 0 {
		t.Errorf("pull target = %s@%d, want T@0", deref(pull.Topic), derefI32(pull.QueueID))
	}
	if derefI64(pull.QueueOffset) != 0 {
		t.Errorf("first pull offset = %d, want 0 (FIRST_OFFSET resolves without a min lookup)", derefI64(pull.QueueOffset))
	}
	if !common.HasLitePullFlag(derefI32(pull.SysFlag)) {
		t.Errorf("sysFlag %d misses the LITE_PULL_MESSAGE bit", derefI32(pull.SysFlag))
	}
	if !common.HasSubscriptionFlag(derefI32(pull.SysFlag)) {
		t.Errorf("sysFlag %d misses the SUBSCRIPTION bit", derefI32(pull.SysFlag))
	}
	if common.HasSuspendFlag(derefI32(pull.SysFlag)) {
		t.Error("a lite pull is a SHORT poll — the suspend bit must be off")
	}
	if common.HasCommitOffsetFlag(derefI32(pull.SysFlag)) {
		t.Error("lite commits come from the consume cursor, never inline — the commitOffset bit must be off")
	}
	if !f.broker.firstPullHas("subscription") {
		t.Error("the subscription bit is on, so the `subscription` extField must ride along")
	}
	if got := derefI32(pull.MaxMsgNums); got != 7 {
		t.Errorf("maxMsgNums = %d, want 7 (the configured pullBatchSize)", got)
	}
	if got := derefI64(pull.SubVersion); got != 0 {
		t.Errorf("subVersion = %d, want 0 for a TAG expression", got)
	}
	if got := deref(pull.ExpressionType); got != remoting.ExpressionTypeTag {
		t.Errorf("expressionType = %q, want TAG", got)
	}

	// Cursors: the pull cursor sits past every SCANNED offset (the filtered-out
	// m3 included); the consume cursor only covers what Poll DELIVERED.
	if got := c.PullCursorOf(mq); got != 4 {
		t.Errorf("pull cursor = %d, want 4 (nextBeginOffset after FOUND)", got)
	}
	if got := c.ConsumeCursorOf(mq); got != 3 {
		t.Errorf("consume cursor = %d, want 3 (m3 was filtered, never delivered)", got)
	}
	// autoCommit=false: nothing reached the broker's offset table.
	if n := f.broker.commitCount(); n != 0 {
		t.Errorf("autoCommit=false committed %d times, want 0", n)
	}
	// The subscription registered for the heartbeat too (assign mode needs the
	// broker-side filter exactly as much as subscribe mode).
	if len(c.Subscriptions()) != 1 || c.Subscriptions()[0].Topic != "T" {
		t.Errorf("Subscriptions() = %+v, want one entry for T", c.Subscriptions())
	}
}

// CursorDiscipline: pause the loop and drive pullOne by hand so every status
// transition is deterministic.
func TestLitePullConsumerCursorDiscipline(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	c := newLiteConsumer(t, f, "GID_lite_cursor")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	mq := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}
	c.Assign([]common.MessageQueue{mq})
	c.Pause([]common.MessageQueue{mq})
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	if got := c.PullCursorOf(mq); got != 0 {
		t.Fatalf("start resolved the pull cursor to %d, want 0 (FIRST_OFFSET)", got)
	}
	if got := c.ConsumeCursorOf(mq); got != -1 {
		t.Fatalf("consume cursor before any delivery = %d, want -1", got)
	}

	// NO_MATCHED_MSG: nothing to deliver, but the cursor MUST follow
	// nextBeginOffset — or the same unmatched window is rescanned forever.
	f.broker.pullCode = remoting.RespPullRetryImmediately
	f.broker.nextBegin = remoting.I64Ptr(10)
	if c.pullOne(mq) {
		t.Fatal("a NO_MATCHED_MSG round must not report progress")
	}
	if got := c.PullCursorOf(mq); got != 10 {
		t.Fatalf("pull cursor after NO_MATCHED_MSG = %d, want 10", got)
	}

	// OFFSET_ILLEGAL: nextBeginOffset carries the broker's corrected offset.
	f.broker.pullCode = remoting.RespPullOffsetMoved
	f.broker.nextBegin = remoting.I64Ptr(42)
	c.pullOne(mq)
	if got := c.PullCursorOf(mq); got != 42 {
		t.Fatalf("pull cursor after OFFSET_ILLEGAL = %d, want 42", got)
	}
	f.broker.pullCode = 0
	f.broker.nextBegin = nil

	// Seek back to 0 and pull the batch through both cursors.
	f.broker.add("T", 0, "m0", "m1", "m2")
	c.Seek(mq, 0)
	if got := c.PullCursorOf(mq); got != 0 || c.ConsumeCursorOf(mq) != 0 {
		t.Fatalf("after Seek: pull=%d consume=%d, want 0/0", c.PullCursorOf(mq), c.ConsumeCursorOf(mq))
	}
	if !c.pullOne(mq) {
		t.Fatal("the batch should have entered the buffer")
	}
	if got := c.PullCursorOf(mq); got != 3 {
		t.Fatalf("pull cursor after FOUND = %d, want 3", got)
	}
	// Seek(0) pinned the consume cursor at 0; pulling must NOT advance it.
	if got := c.ConsumeCursorOf(mq); got != 0 {
		t.Fatalf("consume cursor after buffering = %d, want 0 — only Poll delivery advances it", got)
	}
	if got := c.BufferedMessageCount(); got != 3 {
		t.Fatalf("buffered = %d, want 3", got)
	}
	pollAll(t, c, 3, 3*time.Second)
	if got := c.ConsumeCursorOf(mq); got != 3 {
		t.Fatalf("consume cursor after delivery = %d, want 3", got)
	}

	// NO_NEW_MSG keeps the cursor at nextBeginOffset (3 here — the mock's max).
	c.pullOne(mq)
	if got := c.PullCursorOf(mq); got != 3 {
		t.Fatalf("pull cursor after NO_NEW_MSG = %d, want 3", got)
	}
}

func TestLitePullConsumerSeekDropsBufferedMessages(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	c := newLiteConsumer(t, f, "GID_lite_seek")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	mq := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}
	c.Assign([]common.MessageQueue{mq})
	c.Pause([]common.MessageQueue{mq})
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	f.broker.add("T", 0, "m0", "m1", "m2")
	if !c.pullOne(mq) {
		t.Fatal("the batch should have entered the buffer")
	}
	if got := c.BufferedMessageCount(); got != 3 {
		t.Fatalf("buffered = %d, want 3", got)
	}

	c.Seek(mq, 2)
	if got := c.BufferedMessageCount(); got != 1 {
		t.Fatalf("buffered after Seek(2) = %d, want 1 (offsets 0 and 1 dropped)", got)
	}
	if got := c.PullCursorOf(mq); got != 2 || c.ConsumeCursorOf(mq) != 2 {
		t.Fatalf("cursors after Seek = %d/%d, want 2/2", c.PullCursorOf(mq), c.ConsumeCursorOf(mq))
	}
	msgs, pollErr := c.PollWithTimeout(2000)
	if pollErr != nil {
		t.Fatal(pollErr)
	}
	if len(msgs) != 1 || string(msgs[0].Body) != "m2" || msgs[0].QueueOffset != 2 {
		t.Fatalf("delivered %+v, want exactly m2@2", msgs)
	}
	if got := c.ConsumeCursorOf(mq); got != 3 {
		t.Fatalf("consume cursor = %d, want 3", got)
	}
	// The dropped window (0, 1) never comes back — the pull restarts AT the
	// seek point, so offset 2 is fetched once more, then the cursor moves past.
	if !c.pullOne(mq) {
		t.Fatal("the pull at the seek point should fetch offset 2 again")
	}
	if got := c.PullCursorOf(mq); got != 3 {
		t.Fatalf("pull cursor after the re-fetch = %d, want 3", got)
	}
	// ...and past it there is nothing new.
	if c.pullOne(mq) {
		t.Fatal("nothing new should be pullable past the seek point")
	}
	if got := c.PullCursorOf(mq); got != 3 {
		t.Fatalf("pull cursor = %d, want 3", got)
	}
}

// Seek only moves a cursor the consumer can actually honour. A queue it does not
// hold is refused before anything is written — in subscribe mode the queue may
// simply be waiting for a rebalance, in assign mode it never will be — and the
// offset is held to the queue's own bounds, so a typo cannot pin a cursor the
// pull path would reject round after round.
func TestLitePullConsumerSeekRefusesWhatItCannotHonour(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	mq := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}
	unheld := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 7}

	c := newLiteConsumer(t, f, "GID_lite_seek_guards")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	c.Assign([]common.MessageQueue{mq})
	c.Pause([]common.MessageQueue{mq})
	f.broker.add("T", 0, "a0", "a1", "a2")

	// Before Start the membership check passes (the queue is held) and the
	// running-state check refuses.
	if err := c.Seek(mq, 1); err == nil || !strings.Contains(err.Error(), "not running") {
		t.Errorf("Seek before Start error = %v, want the not-running refusal", err)
	}

	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	// Assign mode: a queue that was never assigned never will be.
	if err := c.Seek(unheld, 0); err == nil ||
		!strings.Contains(err.Error(), "The message queue is not in assigned list, message queue: ") ||
		strings.Contains(err.Error(), "may be rebalancing") {
		t.Errorf("Seek on an unheld queue in assign mode error = %v, want the plain membership refusal", err)
	}

	// The broker reports offsets 0..3; both ends are legal and both outside
	// points are refused with the range read back.
	for _, legal := range []int64{0, 3} {
		if err := c.Seek(mq, legal); err != nil {
			t.Errorf("Seek(%d) at the legal bound error = %v, want it accepted", legal, err)
		}
	}
	for _, illegal := range []int64{-1, 4} {
		err := c.Seek(mq, illegal)
		if err == nil || !strings.Contains(err.Error(), "Seek offset illegal") ||
			!strings.Contains(err.Error(), "min offset = 0, max offset = 3") {
			t.Errorf("Seek(%d) error = %v, want the range refusal read back", illegal, err)
		}
	}
	// A refused Seek must not have moved the cursor.
	if got := c.PullCursorOf(mq); got != 3 {
		t.Errorf("a refused Seek left the pull cursor at %d, want the last accepted 3", got)
	}

	// Subscribe mode names the same situation differently: there the queue may
	// merely be waiting for the rebalance that would hand it over.
	s := newLiteConsumer(t, f, "GID_lite_seek_guards_sub")
	requireNoError(t, "subscribe", s.Subscribe("T", "*"))
	requireNoError(t, "start", s.Start())
	t.Cleanup(s.Shutdown)
	if err := s.Seek(unheld, 0); err == nil ||
		!strings.Contains(err.Error(), "may be rebalancing") {
		t.Errorf("Seek on an unheld queue in subscribe mode error = %v, want the rebalancing refusal", err)
	}
}

// ResolveOrder: broker-committed offsets win (restart continuity), otherwise
// CONSUME_FROM_LAST_OFFSET starts at max offset — skipping everything the
// queue already holds.
func TestLitePullConsumerResolveOrder(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 2})
	group := "GID_lite_resolve"
	c := newLiteConsumer(t, f, group)
	c.SetAutoCommit(false)
	mq0 := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}
	mq1 := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 1}
	c.Assign([]common.MessageQueue{mq0, mq1})
	c.Pause([]common.MessageQueue{mq0, mq1})
	f.broker.add("T", 0, "a0", "a1", "a2", "a3")
	preCommit(f, group, "T", 1, 1)
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	if got := c.PullCursorOf(mq0); got != 4 {
		t.Errorf("q0 pull cursor = %d, want 4 (no commit on record → LAST_OFFSET → max)", got)
	}
	if got := c.PullCursorOf(mq1); got != 1 {
		t.Errorf("q1 pull cursor = %d, want 1 (the broker-committed offset)", got)
	}
}

// ---------------------------------------------------------------- commit paths

func TestLitePullConsumerCommitPaths(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 2})
	c := newLiteConsumer(t, f, "GID_lite_commit")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	mq0 := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}
	mq1 := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 1}
	c.Assign([]common.MessageQueue{mq0, mq1})
	c.Pause([]common.MessageQueue{mq0, mq1})
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	f.broker.add("T", 0, "a0", "a1", "a2")
	if !c.pullOne(mq0) {
		t.Fatal("the batch should have entered the buffer")
	}
	pollAll(t, c, 3, 3*time.Second)
	if got := c.ConsumeCursorOf(mq1); got != -1 {
		t.Fatalf("q1 was never delivered to: consume cursor = %d, want -1", got)
	}

	// commit(Map, persist=false): only the table moves, the wire is silent.
	if err := c.CommitOffsets(map[common.MessageQueue]int64{mq1: 7}, false); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}
	if got := c.PendingCommitOf(mq1); got != 7 {
		t.Fatalf("pending commit = %d, want 7", got)
	}
	if n := f.broker.commitCount(); n != 0 {
		t.Fatalf("persist=false sent %d commits, want 0", n)
	}

	// The empty-map guards: a warn-and-no-op, the accumulated 7 survives.
	if err := c.CommitOffsets(map[common.MessageQueue]int64{}, true); err != nil {
		t.Fatalf("empty CommitOffsets: %v", err)
	}
	if err := c.CommitQueues(nil, true); err != nil {
		t.Fatalf("empty CommitQueues: %v", err)
	}
	if got := c.PendingCommitOf(mq1); got != 7 {
		t.Fatalf("pending commit after no-op commits = %d, want 7", got)
	}

	if err := c.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	commit, ok := f.broker.lastCommit("T", 0)
	if !ok || commit.offset != 3 {
		t.Fatalf("q0 commit = %+v found=%v, want offset 3", commit, ok)
	}
	if _, ok := f.broker.lastCommit("T", 1); ok {
		t.Fatal("q1 has no consume cursor — it must never be committed")
	}
	if got := c.PendingCommitOf(mq1); got != -1 {
		t.Fatalf("q1's out-of-scope cell = %d, want -1 (persistAll swept it)", got)
	}

	// commit(Map, persist=true) sends the named queue and sweeps the others.
	if err := c.CommitOffsets(map[common.MessageQueue]int64{mq1: 9}, true); err != nil {
		t.Fatalf("CommitOffsets persist: %v", err)
	}
	commit, ok = f.broker.lastCommit("T", 1)
	if !ok || commit.offset != 9 {
		t.Fatalf("q1 commit = %+v found=%v, want offset 9", commit, ok)
	}
	if got := c.PendingCommitOf(mq0); got != -1 {
		t.Fatalf("q0's out-of-scope cell = %d, want -1 (swept)", got)
	}

	// commit(List, persist=true) commits from the current consume cursor.
	if err := c.CommitQueues([]common.MessageQueue{mq0}, true); err != nil {
		t.Fatalf("CommitQueues: %v", err)
	}
	commit, ok = f.broker.lastCommit("T", 0)
	if !ok || commit.offset != 3 {
		t.Fatalf("q0 commit = %+v found=%v, want offset 3", commit, ok)
	}

	foreign := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 9}
	if err := c.CommitOffsets(map[common.MessageQueue]int64{foreign: 5}, true); err != nil {
		t.Fatalf("CommitOffsets foreign: %v", err)
	}
	commit, ok = f.broker.lastCommit("T", 9)
	if !ok || commit.offset != 5 {
		t.Fatalf("foreign commit = %+v found=%v, want offset 5", commit, ok)
	}
}

func TestLitePullConsumerAssignDropsRevokedCursors(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 2})
	c := newLiteConsumer(t, f, "GID_lite_drop")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	mq0 := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}
	mq1 := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 1}
	c.Assign([]common.MessageQueue{mq0, mq1})
	// Paused so the background loop leaves the cursors Seek sets alone.
	c.Pause([]common.MessageQueue{mq0, mq1})
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)
	f.broker.add("T", 0, "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7")
	f.broker.add("T", 1, "b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7")
	requireNoError(t, "Seek(mq0)", c.Seek(mq0, 5))
	requireNoError(t, "Seek(mq1)", c.Seek(mq1, 7))

	c.Assign([]common.MessageQueue{mq0})
	if !c.IsAssignMode() {
		t.Fatal("Assign must select assign mode")
	}
	if got := c.PullCursorOf(mq0); got != 5 || c.ConsumeCursorOf(mq0) != 5 {
		t.Errorf("kept queue cursors = %d/%d, want 5/5", c.PullCursorOf(mq0), c.ConsumeCursorOf(mq0))
	}
	if got := c.PullCursorOf(mq1); got != -1 || c.ConsumeCursorOf(mq1) != -1 {
		t.Errorf("revoked queue cursors = %d/%d, want -1/-1", c.PullCursorOf(mq1), c.ConsumeCursorOf(mq1))
	}
	if got := len(c.Assignment()); got != 1 {
		t.Errorf("assignment holds %d queues, want 1", got)
	}
}

// ---------------------------------------------------------------- pause/resume

func TestLitePullConsumerPauseResumeGatesPulls(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	c := newLiteConsumer(t, f, "GID_lite_pause")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	mq := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}
	c.Assign([]common.MessageQueue{mq})
	c.Pause([]common.MessageQueue{mq})
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	f.broker.add("T", 0, "a0", "a1")
	time.Sleep(300 * time.Millisecond)
	if pulls := f.broker.pullsFor("T", 0); len(pulls) != 0 {
		t.Fatalf("a paused queue was pulled %d times, want 0", len(pulls))
	}

	c.Resume([]common.MessageQueue{mq})
	msgs := pollAll(t, c, 2, 3*time.Second)
	if len(msgs) != 2 {
		t.Fatalf("delivered %d messages after resume, want 2", len(msgs))
	}
	if pulls := f.broker.pullsFor("T", 0); len(pulls) == 0 {
		t.Fatal("the resumed queue should be pulled again")
	}
}

// ---------------------------------------------------------------- subscribe mode

func TestLitePullConsumerSubscribeModeRebalanceAndShutdown(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 2})
	// The lite consumer's clientId carries the @STREAM suffix; the mock broker
	// must report the group membership under THAT identity or the allocation
	// finds the consumer nowhere.
	f.broker.setClientIDs(liteClientID(f))
	c := newLiteConsumer(t, f, "GID_lite_sub")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	listener := &recordingQueueListener{}
	c.SetMessageQueueListener(listener)
	requireNoError(t, "subscribe", c.Subscribe("T", "*"))
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	if c.IsAssignMode() {
		t.Fatal("Subscribe must leave the consumer in subscribe mode")
	}
	waitFor(t, "the rebalance to hand out both queues", func() bool {
		return len(c.Assignment()) == 2
	})

	f.broker.add("T", 0, "a0", "a1")
	f.broker.add("T", 1, "b0", "b1", "b2")
	bodies := pollAll(t, c, 5, 5*time.Second)
	seen := map[string]bool{}
	for _, b := range bodies {
		seen[b] = true
	}
	for _, want := range []string{"a0", "a1", "b0", "b1", "b2"} {
		if !seen[want] {
			t.Errorf("body %q was never delivered (got %v)", want, bodies)
		}
	}

	// The rebalance must have notified the listener with the full queue set.
	if listener.calls() == 0 {
		t.Fatal("the message queue listener never fired")
	}
	gotTopic, mqAll, divided := listener.last()
	if gotTopic != "T" || len(mqAll) != 2 || len(divided) != 2 {
		t.Fatalf("listener last call = %q all=%d divided=%d, want T 2/2", gotTopic, len(mqAll), len(divided))
	}

	// The self-heartbeat went out during Start (before any pull could need it).
	if f.broker.indexOfReq(remoting.ReqHeartBeat) < 0 {
		t.Fatal("no HEART_BEAT(34) reached the broker")
	}

	// Explicit commit sends both consume cursors.
	if err := c.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	for _, want := range []struct {
		queueID int32
		offset  int64
	}{{0, 2}, {1, 3}} {
		commit, ok := f.broker.lastCommit("T", want.queueID)
		if !ok || commit.offset != want.offset {
			t.Errorf("q%d commit = %+v found=%v, want offset %d", want.queueID, commit, ok, want.offset)
		}
	}

	// Shutdown persists one last time, unregisters, and is idempotent.
	before := f.broker.commitCount()
	c.Shutdown()
	if got := f.broker.commitCount(); got < before+2 {
		t.Fatalf("shutdown committed %d times after %d, want at least 2 more", got, before)
	}
	if c.IsStarted() {
		t.Fatal("Shutdown must clear the started flag")
	}
	c.Shutdown()
	if got := f.broker.commitCount(); got != before+2 {
		t.Fatalf("a second Shutdown committed %d more times, want none", got-before-2)
	}
}

// ---------------------------------------------------------------- initial offset

// Each branch of the start-position contract gets its own offset from the mock
// broker, so the position the consumer ends up at says which rule placed it:
// tail=100 can only be GET_MAX_OFFSET, searched=200 can only be
// SEARCH_OFFSET_BY_TIMESTAMP, stored=7 can only be the committed offset, and a
// local 0 costs no round trip at all.
const (
	liteTailOffset     = 100
	liteSearchedOffset = 200
	liteStoredOffset   = 7
)

// litePausedOn wires one assign-mode queue and PAUSES it, so the background pull
// loop never touches the cursor: whatever PullCursorOf reports is the value
// Start's offset resolution produced, and the only request that can reach the
// broker is the pull the test drives by hand.
func litePausedOn(t *testing.T, f *clusterFixture, group, topic string,
	configure func(*DefaultLitePullConsumer)) (*DefaultLitePullConsumer, common.MessageQueue) {
	t.Helper()
	c := newLiteConsumer(t, f, group)
	mq := common.MessageQueue{Topic: topic, BrokerName: "b1", QueueID: 0}
	c.SetAutoCommit(false)
	c.Pause([]common.MessageQueue{mq})
	configure(c)
	c.Assign([]common.MessageQueue{mq})
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)
	return c, mq
}

func TestLitePullLastOffsetStartsAtMaxOffset(t *testing.T) {
	topic := uniqueTopic("GoLiteLast", t)
	group := "GID_lite_last"
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.mu.Lock()
	f.broker.maxOffset[mqKey{topic, 0}] = liteTailOffset
	f.broker.mu.Unlock()

	c, mq := litePausedOn(t, f, group, topic, func(c *DefaultLitePullConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereLastOffset)
	})
	if got := c.PullCursorOf(mq); got != liteTailOffset {
		t.Fatalf("start cursor = %d, want maxOffset %d", got, liteTailOffset)
	}
	c.pullOne(mq)
	if got := derefI64(f.broker.pullsFor(topic, 0)[0].QueueOffset); got != liteTailOffset {
		t.Errorf("queueOffset = %d, want maxOffset %d", got, liteTailOffset)
	}
	if minQ, maxQ := f.broker.boundaryQueries(topic); minQ != 0 || maxQ != 1 {
		t.Errorf("boundary RPCs for a LAST start = min %d/max %d, want 0/1", minQ, maxQ)
	}
	// A fresh group must be told "not found", never handed a 0: an offset read
	// that asks for the 0 answer would place this consumer at the head of the
	// queue and replay everything produced before it started.
	if got := f.broker.zeroIfNotFoundQueries(); len(got) != 0 {
		t.Errorf("the initial offset read asked the broker for a 0 answer for %v, want no such request", got)
	}
	// Control for the assertion above: the same read carrying the flag must be
	// visible on the wire, or the check would pass for a broker that never sees it.
	if _, _, err := c.instance.QueryConsumerOffset(group, mq, 2000, "", true); err != nil {
		t.Fatalf("control offset read: %v", err)
	}
	if got := f.broker.zeroIfNotFoundQueries(); len(got) != 1 || got[0] != topic {
		t.Errorf("the control read with the zero answer requested was captured as %v, want [%s]", got, topic)
	}
}

// The LAST sibling rule for a retry queue: everything already sitting there is
// awaiting redelivery, so the replay starts at 0 instead of the tail.
func TestLitePullRetryTopicLastOffsetStartsAtZero(t *testing.T) {
	group := "GID_lite_retry_last"
	retryTopic := common.GetRetryTopic(group)
	f := newClusterFixture(t, map[string]int{retryTopic: 1})
	f.broker.mu.Lock()
	f.broker.maxOffset[mqKey{retryTopic, 0}] = liteTailOffset
	f.broker.mu.Unlock()

	c, mq := litePausedOn(t, f, group, retryTopic, func(c *DefaultLitePullConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereLastOffset)
	})
	if got := c.PullCursorOf(mq); got != 0 {
		t.Fatalf("retry topic cursor = %d, want 0 (every retried message must be retried)", got)
	}
	c.pullOne(mq)
	if got := derefI64(f.broker.pullsFor(retryTopic, 0)[0].QueueOffset); got != 0 {
		t.Errorf("retry topic queueOffset = %d, want 0", got)
	}
	if minQ, maxQ := f.broker.boundaryQueries(retryTopic); minQ != 0 || maxQ != 0 {
		t.Errorf("the retry topic answered %d min and %d max offset queries; 0 is a local decision", minQ, maxQ)
	}
}

func TestLitePullFirstOffsetSkipsBoundaryQueries(t *testing.T) {
	topic := uniqueTopic("GoLiteFirst", t)
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.mu.Lock()
	// A queue whose head is not 0 either: FIRST must still answer 0 and let the
	// broker correct the cursor through PULL_OFFSET_MOVED.
	f.broker.maxOffset[mqKey{topic, 0}] = liteTailOffset
	f.broker.minOffset[mqKey{topic, 0}] = 40
	f.broker.mu.Unlock()
	f.broker.add(topic, 0, "m0")

	c, mq := litePausedOn(t, f, "GID_lite_first", topic, func(c *DefaultLitePullConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	})
	if got := c.PullCursorOf(mq); got != 0 {
		t.Fatalf("start cursor = %d, want 0", got)
	}
	c.pullOne(mq)
	if got := derefI64(f.broker.pullsFor(topic, 0)[0].QueueOffset); got != 0 {
		t.Errorf("queueOffset = %d, want 0", got)
	}
	if minQ, maxQ := f.broker.boundaryQueries(topic); minQ != 0 || maxQ != 0 {
		t.Errorf("CONSUME_FROM_FIRST_OFFSET issued %d min and %d max offset queries, want none: the extra "+
			"round trip only talks to the master, so a fresh consumer would get nothing while it is down", minQ, maxQ)
	}
}

func TestLitePullTimestampOffsetUsesSearchOffset(t *testing.T) {
	topic := uniqueTopic("GoLiteTimestamp", t)
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.mu.Lock()
	f.broker.maxOffset[mqKey{topic, 0}] = liteTailOffset
	f.broker.searchedOffset = liteSearchedOffset
	f.broker.mu.Unlock()

	c, mq := litePausedOn(t, f, "GID_lite_ts", topic, func(c *DefaultLitePullConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereTimestamp)
		c.SetConsumeTimestamp("20240102030405")
	})
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.Local).UnixMilli()
	got := f.broker.searchTimestamps()
	if len(got) == 0 {
		t.Fatal("no SEARCH_OFFSET_BY_TIMESTAMP(29) reached the broker")
	}
	if got[0] != want {
		t.Errorf("searchOffset timestamp = %d, want %d (20240102030405 read as local wall clock)", got[0], want)
	}
	if c.PullCursorOf(mq) != liteSearchedOffset {
		t.Errorf("start cursor = %d, want the searched %d, not the tail %d",
			c.PullCursorOf(mq), liteSearchedOffset, liteTailOffset)
	}
	c.pullOne(mq)
	if got := derefI64(f.broker.pullsFor(topic, 0)[0].QueueOffset); got != liteSearchedOffset {
		t.Errorf("queueOffset = %d, want the searched offset %d", got, liteSearchedOffset)
	}
	if minQ, maxQ := f.broker.boundaryQueries(topic); minQ != 0 || maxQ != 0 {
		t.Errorf("a TIMESTAMP start issued %d min and %d max offset queries, want none", minQ, maxQ)
	}
}

// A retry queue is created by the first redelivery, long after any configured
// instant, so searching there answers nothing: its replay starts at the tail.
func TestLitePullRetryTopicTimestampUsesMaxOffset(t *testing.T) {
	group := "GID_lite_retry_ts"
	retryTopic := common.GetRetryTopic(group)
	f := newClusterFixture(t, map[string]int{retryTopic: 1})
	f.broker.mu.Lock()
	f.broker.maxOffset[mqKey{retryTopic, 0}] = liteTailOffset
	f.broker.searchedOffset = liteSearchedOffset
	f.broker.mu.Unlock()

	c, mq := litePausedOn(t, f, group, retryTopic, func(c *DefaultLitePullConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereTimestamp)
		c.SetConsumeTimestamp("20240102030405")
	})
	if got := c.PullCursorOf(mq); got != liteTailOffset {
		t.Fatalf("retry topic cursor = %d, want maxOffset %d", got, liteTailOffset)
	}
	if got := f.broker.searchTimestamps(); len(got) != 0 {
		t.Errorf("SEARCH_OFFSET_BY_TIMESTAMP was called %d times for a retry topic; it must be answered with maxOffset",
			len(got))
	}
	c.pullOne(mq)
	if got := derefI64(f.broker.pullsFor(retryTopic, 0)[0].QueueOffset); got != liteTailOffset {
		t.Errorf("retry topic queueOffset = %d, want maxOffset %d", got, liteTailOffset)
	}
}

// A group that already committed resumes there: no policy branch and no boundary
// RPC may move it.
func TestLitePullStoredOffsetBeatsEveryPolicy(t *testing.T) {
	topic := uniqueTopic("GoLiteStored", t)
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.mu.Lock()
	f.broker.maxOffset[mqKey{topic, 0}] = liteTailOffset
	f.broker.searchedOffset = liteSearchedOffset
	f.broker.mu.Unlock()
	group := "GID_lite_stored"
	preCommit(f, group, topic, 0, liteStoredOffset)

	c, mq := litePausedOn(t, f, group, topic, func(c *DefaultLitePullConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereTimestamp)
		c.SetConsumeTimestamp("20240102030405")
	})
	if got := c.PullCursorOf(mq); got != liteStoredOffset {
		t.Fatalf("start cursor = %d, want the committed %d", got, liteStoredOffset)
	}
	c.pullOne(mq)
	if got := derefI64(f.broker.pullsFor(topic, 0)[0].QueueOffset); got != liteStoredOffset {
		t.Errorf("queueOffset = %d, want the committed offset %d", got, liteStoredOffset)
	}
	if minQ, maxQ := f.broker.boundaryQueries(topic); minQ != 0 || maxQ != 0 {
		t.Errorf("a group with a committed offset issued %d min and %d max offset queries, want none", minQ, maxQ)
	}
	if got := f.broker.searchTimestamps(); len(got) != 0 {
		t.Errorf("a group with a committed offset searched by timestamp %d times, want never", len(got))
	}
}

// The instant is a knob even when nobody sets it: TIMESTAMP with no value means
// "half an hour ago", so a consumer that boots after a producer does not lose the
// last window of traffic.
func TestLitePullDefaultTimestampIsThirtyMinutesBack(t *testing.T) {
	topic := uniqueTopic("GoLiteDefaultTS", t)
	f := newClusterFixture(t, map[string]int{topic: 1})

	// Paused, so the only offset work the consumer does is Start's resolution.
	litePausedOn(t, f, "GID_lite_default_ts", topic, func(c *DefaultLitePullConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereTimestamp)
	})
	got := f.broker.searchTimestamps()
	if len(got) == 0 {
		t.Fatal("a TIMESTAMP start with no configured instant never reached the broker")
	}
	want := time.Now().Add(-30 * time.Minute).UnixMilli()
	if diff := got[0] - want; diff < -5000 || diff > 5000 {
		t.Errorf("default instant = %d, want %d (+/-5s): the boot-time minus 30 minutes rule", got[0], want)
	}
	if got[0]%1000 != 0 {
		t.Errorf("default instant %d is not aligned to a second; yyyyMMddHHmmss has no sub-second part", got[0])
	}
}

// The auto-commit interval has a floor rather than a clamp: a setting below it is
// ignored outright, so whatever interval was already running keeps running instead of
// being pulled down to the floor or switched off.
func TestLiteAutoCommitIntervalRefusesSettingsBelowTheFloor(t *testing.T) {
	c := MustNewDefaultLitePullConsumer("GID_lite_autocommit_floor")
	if got := c.autoCommitIntervalMillis; got != DefaultLiteAutoCommitIntervalMillis {
		t.Fatalf("default interval = %d, want %d", got, DefaultLiteAutoCommitIntervalMillis)
	}
	for _, rejected := range []int64{999, 0, -1} {
		c.SetAutoCommitIntervalMillis(rejected)
		if got := c.autoCommitIntervalMillis; got != DefaultLiteAutoCommitIntervalMillis {
			t.Errorf("after setting %d the interval is %d, want the previous %d left alone", rejected, got, DefaultLiteAutoCommitIntervalMillis)
		}
	}
	c.SetAutoCommitIntervalMillis(MinLiteAutoCommitIntervalMillis)
	if got := c.autoCommitIntervalMillis; got != MinLiteAutoCommitIntervalMillis {
		t.Fatalf("the floor itself must be accepted, got %d", got)
	}
	c.SetAutoCommitIntervalMillis(2500)
	c.SetAutoCommitIntervalMillis(500)
	if got := c.autoCommitIntervalMillis; got != 2500 {
		t.Errorf("after rejecting 500 the interval is %d, want the accepted 2500", got)
	}
}

// The lite surface refuses to be used wrongly, and refuses out loud: an empty
// assignment would revoke every queue the consumer holds, a blank assign-mode
// expression would quietly mean "everything", and a poll or a committed-offset
// read on a consumer that was never started looks identical to "nothing to see
// here" -- which is how a missing Start() stays hidden forever.
func TestLiteSurfaceRefusesMisuse(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	c := newLiteConsumer(t, f, "GID_lite_guards")
	mq := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}

	if err := c.Assign(nil); err == nil || !strings.Contains(err.Error(), "can not be null or empty") {
		t.Errorf("Assign(nil) error = %v, want the empty-list refusal", err)
	}
	if err := c.Assign([]common.MessageQueue{}); err == nil {
		t.Error("Assign([]) must refuse: the empty list would revoke every held queue")
	}
	if c.IsAssignMode() {
		t.Error("a refused Assign must leave the mode alone")
	}
	if err := c.Subscribe("", "TagA"); err == nil || !strings.Contains(err.Error(), "Topic can not be null or empty") {
		t.Errorf("Subscribe(\"\") error = %v, want the empty-topic refusal", err)
	}
	if err := c.SetSubExpressionForAssign("T", "  "); err == nil ||
		!strings.Contains(err.Error(), "subExpression can not be null or empty") {
		t.Errorf("blank assign expression error = %v, want the blank-expression refusal", err)
	}
	if err := c.SetSubExpressionForAssign("  ", "TagA"); err == nil ||
		!strings.Contains(err.Error(), "Topic can not be null or empty") {
		t.Errorf("blank assign-expression topic error = %v, want the blank-topic refusal", err)
	}
	if _, err := c.Poll(); err == nil || !strings.Contains(err.Error(), "not running") {
		t.Errorf("Poll before Start error = %v, want the not-running refusal", err)
	}
	if _, err := c.PollWithTimeout(-1); err == nil || !strings.Contains(err.Error(), "not running") {
		t.Errorf("PollWithTimeout(-1) before Start error = %v, want the not-running refusal first", err)
	}
	if _, err := c.Committed(mq); err == nil || !strings.Contains(err.Error(), "not running") {
		t.Errorf("Committed before Start error = %v, want the not-running refusal", err)
	}
	// A table entry does not make a stopped consumer's offset real: the running
	// check comes first, so a caller cannot read a session that is not open.
	c.mu.Lock()
	c.offsetTable[mq] = 42
	c.mu.Unlock()
	if _, err := c.Committed(mq); err == nil || !strings.Contains(err.Error(), "not running") {
		t.Errorf("Committed on a stopped consumer holding an offset error = %v, want the not-running refusal", err)
	}
	c.mu.Lock()
	delete(c.offsetTable, mq)
	c.mu.Unlock()

	requireNoError(t, "subscribe", c.Subscribe("T", "*"))
	requireNoError(t, "start", c.Start())
	defer c.Shutdown()

	if _, err := c.PollWithTimeout(-1); err == nil || !strings.Contains(err.Error(), "Timeout must not be negative") {
		t.Errorf("PollWithTimeout(-1) error = %v, want the negative-timeout refusal", err)
	}
	if err := c.SetSubExpressionForAssign("T", "TagA"); err == nil ||
		!strings.Contains(err.Error(), "only can be called before start") {
		t.Errorf("assign expression after Start error = %v, want the pre-start refusal", err)
	}

	// A consumer subscribed once stays in subscribe mode: assigning a queue now
	// would leave the running halves describing two different consumers.
	if err := c.Assign([]common.MessageQueue{mq}); err == nil ||
		!strings.Contains(err.Error(), "Subscribe and assign are mutually exclusive") {
		t.Errorf("Assign on a subscribed consumer error = %v, want the mode-conflict refusal", err)
	}

	// The mirror image is just as fixed, and the assign-mode call itself is
	// accepted on a consumer that took that mode first.
	assigned := newLiteConsumer(t, f, "GID_lite_guards_assign")
	if err := assigned.Assign([]common.MessageQueue{mq}); err != nil {
		t.Fatalf("a non-empty Assign must be accepted, got %v", err)
	}
	if err := assigned.Subscribe("T", "*"); err == nil ||
		!strings.Contains(err.Error(), "Subscribe and assign are mutually exclusive") {
		t.Errorf("Subscribe on an assigned consumer error = %v, want the mode-conflict refusal", err)
	}
	if err := assigned.SetSubExpressionForAssign("T", "TagA"); err != nil {
		t.Errorf("an assign-mode consumer must accept its expression, got %v", err)
	}
}

// Unsubscribing a topic drops the queues this instance holds for it right away.
// In subscribe mode a rebalance would eventually hand them away; in assign mode
// nothing ever would, so the consumer would keep pulling a topic the caller has
// left. The sibling topic and its cursor stay untouched.
func TestLitePullConsumerUnsubscribeDropsTheTopicsQueues(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"A": 1, "B": 1})
	c := newLiteConsumer(t, f, "GID_lite_unsubscribe")
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	c.SetAutoCommit(false)
	mqA := common.MessageQueue{Topic: "A", BrokerName: "b1", QueueID: 0}
	mqB := common.MessageQueue{Topic: "B", BrokerName: "b1", QueueID: 0}
	requireNoError(t, "assign", c.Assign([]common.MessageQueue{mqA, mqB}))
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	if got := len(c.Assignment()); got != 2 {
		t.Fatalf("Assignment after Assign = %d queues, want 2", got)
	}
	if got := c.PullCursorOf(mqA); got != 0 {
		t.Fatalf("A pull cursor = %d, want 0", got)
	}

	c.Unsubscribe("A")

	if got := c.Assignment(); len(got) != 1 || got[0] != mqB {
		t.Errorf("Assignment after Unsubscribe(A) = %v, want only B", got)
	}
	if got := c.PullCursorOf(mqA); got != -1 {
		t.Errorf("A pull cursor after Unsubscribe = %d, want the queue to be gone", got)
	}
	if got := c.PullCursorOf(mqB); got != 0 {
		t.Errorf("B pull cursor = %d, want the sibling untouched at 0", got)
	}
	// A blank topic matches nothing and must not revoke anything.
	c.Unsubscribe("   ")
	if got := len(c.Assignment()); got != 1 {
		t.Errorf("a blank Unsubscribe left %d queues, want the held one untouched", got)
	}

	// Subscribe mode: the rebalance subscription goes with it, and the queues it
	// had been handed leave immediately instead of at the next rebalance.
	f.broker.setClientIDs(liteClientID(f))
	s := newLiteConsumer(t, f, "GID_lite_unsubscribe_sub")
	requireNoError(t, "subscribe A", s.Subscribe("A", "*"))
	requireNoError(t, "subscribe B", s.Subscribe("B", "*"))
	requireNoError(t, "start", s.Start())
	t.Cleanup(s.Shutdown)
	waitFor(t, "the rebalance to hand out both queues", func() bool {
		return len(s.Assignment()) == 2
	})
	s.Unsubscribe("A")
	if _, ok := s.SubscriptionExpressions()["A"]; ok {
		t.Error("Unsubscribe(A) left the rebalance subscription behind")
	}
	if _, ok := s.SubscriptionExpressions()["B"]; !ok {
		t.Error("Unsubscribe(A) dropped the sibling subscription too")
	}
	for _, mq := range s.Assignment() {
		if mq.Topic == "A" {
			t.Errorf("Unsubscribe(A) left %v assigned", mq)
		}
	}
}

// The self-heartbeat advertises rebalance subscriptions only. An assign-mode
// expression filters the queues this instance was handed; announcing it would
// make the broker treat the group as a subscriber of the whole topic.
func TestLitePullHeartbeatAdvertisesOnlyRebalanceSubscriptions(t *testing.T) {
	assigned := newClusterFixture(t, map[string]int{"T": 1})
	a := newLiteConsumer(t, assigned, "GID_lite_beat_assign")
	mq := common.MessageQueue{Topic: "T", BrokerName: "b1", QueueID: 0}
	requireNoError(t, "assign", a.Assign([]common.MessageQueue{mq}))
	requireNoError(t, "express", a.SetSubExpressionForAssign("T", "TagA"))
	requireNoError(t, "start", a.Start())
	t.Cleanup(a.Shutdown)
	waitFor(t, "the assign-mode start to beat", func() bool {
		return len(assigned.broker.beatSubVersions("T")) > 0
	})
	for _, v := range assigned.broker.beatSubVersions("T") {
		if v != -1 {
			t.Fatalf("an assign-mode beat announced a subscription for T (subVersion %d)", v)
		}
	}

	// Paired control: a rebalance subscription is advertised, so the assertion
	// above is about assign mode and not about a heartbeat that never carries any.
	subscribed := newClusterFixture(t, map[string]int{"T": 1})
	s := newLiteConsumer(t, subscribed, "GID_lite_beat_subscribe")
	requireNoError(t, "subscribe", s.Subscribe("T", "TagA"))
	requireNoError(t, "start", s.Start())
	t.Cleanup(s.Shutdown)
	waitFor(t, "the subscribe-mode beat to carry the subscription", func() bool {
		for _, v := range subscribed.broker.beatSubVersions("T") {
			if v >= 0 {
				return true
			}
		}
		return false
	})
}

// beatTopics maps topic -> subString for the newest captured HEARTBEAT; a topic
// the beat did not announce is absent.
func beatTopics(b *consumerBroker) map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]string{}
	if len(b.heartbeats) == 0 {
		return out
	}
	for _, cd := range b.heartbeats[len(b.heartbeats)-1].ConsumerDataSet {
		for _, sub := range cd.SubscriptionDataSet {
			out[sub.Topic] = sub.SubString
		}
	}
	return out
}

// The broker-suspend budget must reach the wire. A fixed constant passes the
// start-up validation and then ignores the knob for the life of the consumer.
func TestLitePullRequestCarriesTheConfiguredSuspendBudget(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	c := newLiteConsumer(t, f, "GID_lite_suspend_knob")
	c.SetBrokerSuspendMaxTimeMillis(8000)
	c.SetConsumerTimeoutMillisWhenSuspend(20000)
	c.SetConsumeFromWhere(ConsumeFromWhereFirstOffset)
	requireNoError(t, "assign", c.Assign([]common.MessageQueue{
		{Topic: "T", BrokerName: "b1", QueueID: 0},
	}))
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	waitFor(t, "the first lite pull", func() bool { return len(f.broker.pullsFor("T", 0)) > 0 })
	pull := f.broker.pullsFor("T", 0)[0]
	if got := derefI64(pull.SuspendTimeoutMillis); got != 8000 {
		t.Errorf("suspendTimeoutMillis = %d, want the configured 8000", got)
	}
	if common.HasSuspendFlag(derefI32(pull.SysFlag)) {
		t.Error("the suspend bit must stay off: a synchronous lite loop asks for a short poll")
	}
}

// An assign-mode consumer has no topic-wide subscription, so the built set is how
// it announces the filters its queues need; each build replaces the whole set.
func TestLitePullHeartbeatCarriesTheBuiltSubscriptionSet(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"A": 1, "B": 1})
	c := newLiteConsumer(t, f, "GID_lite_beat_built")
	requireNoError(t, "assign", c.Assign([]common.MessageQueue{
		{Topic: "A", BrokerName: "b1", QueueID: 0},
		{Topic: "B", BrokerName: "b1", QueueID: 0},
	}))
	requireNoError(t, "build", c.BuildSubscriptionsForHeartbeat(map[string]MessageSelector{
		"A": {Expression: "TagA"},
		"B": {Expression: "a > 1", Type: remoting.ExpressionTypeSQL92},
	}))
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	waitFor(t, "a beat announcing both built topics", func() bool {
		topics := beatTopics(f.broker)
		return topics["A"] != "" && topics["B"] != ""
	})
	if got := beatTopics(f.broker)["A"]; got != "TagA" {
		t.Errorf("A announced %q, want TagA", got)
	}
	if got := beatTopics(f.broker)["B"]; got != "a > 1" {
		t.Errorf("B announced %q, want the SQL92 expression", got)
	}
	if got := c.SubscriptionsForHeartbeat(); len(got) != 2 || got[0].Topic != "A" || got[1].Topic != "B" {
		t.Errorf("built set = %v, want A then B", got)
	}

	// A rebuild replaces the whole set: B is not announced any more, and A carries
	// its new expression.
	requireNoError(t, "rebuild", c.BuildSubscriptionsForHeartbeat(map[string]MessageSelector{
		"A": {Expression: "*"},
	}))
	c.sendHeartbeatToAllBrokers()
	waitFor(t, "the rebuilt beat dropping B", func() bool {
		topics := beatTopics(f.broker)
		_, hasB := topics["B"]
		return !hasB && topics["A"] == "*"
	})

	// A refused build must not damage the set that is already in place.
	if err := c.BuildSubscriptionsForHeartbeat(map[string]MessageSelector{
		"": {Expression: "TagA"},
	}); err == nil {
		t.Error("a build with an empty topic was accepted")
	}
	if err := c.BuildSubscriptionsForHeartbeat(map[string]MessageSelector{
		"A": {Type: remoting.ExpressionTypeSQL92},
	}); err == nil {
		t.Error("a SQL92 selector with no expression was accepted")
	}
	if got := c.SubscriptionsForHeartbeat(); len(got) != 1 || got[0].Topic != "A" {
		t.Errorf("a refused build changed the set: %v", got)
	}
}

// A topic the rebalance subscriptions cover keeps their expression: the built set
// only fills the topics they do not.
func TestLitePullBuiltSetYieldsToTheRebalanceSubscription(t *testing.T) {
	f := newClusterFixture(t, map[string]int{"T": 1})
	c := newLiteConsumer(t, f, "GID_lite_beat_both")
	requireNoError(t, "subscribe", c.Subscribe("T", "TagA"))
	requireNoError(t, "build", c.BuildSubscriptionsForHeartbeat(map[string]MessageSelector{
		"T": {Expression: "TagZ"},
	}))
	requireNoError(t, "start", c.Start())
	t.Cleanup(c.Shutdown)

	waitFor(t, "a beat announcing T", func() bool { return beatTopics(f.broker)["T"] != "" })
	if got := beatTopics(f.broker)["T"]; got != "TagA" {
		t.Errorf("T announced %q, want the rebalance expression TagA", got)
	}
	if got := len(c.SubscriptionsForHeartbeat()); got != 1 {
		t.Errorf("built set holds %d entries, want the one the application added", got)
	}
}
