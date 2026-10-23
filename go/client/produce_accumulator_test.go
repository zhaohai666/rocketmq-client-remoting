package client

import (
	"strconv"
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// commaResult builds a broker-style batch SendResult whose MsgId/OffsetMsgId are
// comma-joined per-message ids.
func commaResult(msgs []*common.Message, baseOffset int64) *SendResult {
	ids := make([]string, len(msgs))
	for i := range msgs {
		ids[i] = "id" + strconv.Itoa(i)
	}
	return &SendResult{
		SendStatus:  SendOK,
		MsgID:       strings.Join(ids, ","),
		OffsetMsgID: strings.Join(ids, ","),
		QueueOffset: baseOffset,
	}
}

func TestAggregateKeyMergeRules(t *testing.T) {
	base := newAggregateKeyOfMessage(common.NewMessage("T", []byte("x")))
	same := newAggregateKeyOfMessage(common.NewMessage("T", []byte("y")))
	if base != same {
		t.Fatalf("same topic/tag/wait must merge: %v vs %v", base, same)
	}

	tagged := common.NewMessage("T", []byte("x"))
	tagged.SetTags("A")
	if newAggregateKeyOfMessage(tagged) == base {
		t.Fatal("present tag must differ from absent tag")
	}

	emptyTag := common.NewMessage("T", []byte("x"))
	emptyTag.SetTags("")
	ke := newAggregateKeyOfMessage(emptyTag)
	if ke == base {
		t.Fatal("empty tag (present) must differ from absent tag")
	}
	if ke == newAggregateKeyOfMessage(tagged) {
		t.Fatal("empty tag must differ from tag A")
	}

	pinned := newAggregateKeyOfMessageWithMq(common.NewMessage("T", []byte("x")),
		common.NewMessageQueue("T", "broker", 0))
	if pinned == base {
		t.Fatal("pinned must differ from unpinned")
	}
}

func TestCanBatchRules(t *testing.T) {
	if !canBatch(common.NewMessage("T", []byte("x"))) {
		t.Fatal("plain message must be batchable")
	}
	delayed := common.NewMessage("T", []byte("x"))
	delayed.SetDelayTimeLevel(3)
	if canBatch(delayed) {
		t.Fatal("delayed message must not be batchable")
	}
	if canBatch(common.NewMessage("%RETRY%g", []byte("x"))) {
		t.Fatal("retry-topic message must not be batchable")
	}
	// A timer-scheduled member is the case that used to slip through: it joins a
	// group, then the flush is refused by the batch builder instead of the message
	// simply going out as a single send.
	timer := map[string]func(*common.Message){
		common.PropertyTimerDelaySec:  func(m *common.Message) { m.SetDelayTimeSec(30) },
		common.PropertyTimerDelayMs:   func(m *common.Message) { m.SetDelayTimeMs(30000) },
		common.PropertyTimerDeliverMs: func(m *common.Message) { m.SetDeliverTimeMs(1700000000000) },
	}
	for name, apply := range timer {
		msg := common.NewMessage("T", []byte("x"))
		apply(msg)
		if canBatch(msg) {
			t.Fatalf("%s message must not be batchable", name)
		}
		// Paired control: the same name at 0 asks for nothing, so it still batches.
		zero := common.NewMessage("T", []byte("x"))
		zero.PutProperty(name, "0")
		if !canBatch(zero) {
			t.Fatalf("%s=0 must stay batchable", name)
		}
	}
	// A message already stamped with a producer group is a transaction half
	// message; its group is what the broker locates the connection by.
	grouped := common.NewMessage("T", []byte("x"))
	grouped.PutProperty(common.PropertyProducerGroup, "G")
	if canBatch(grouped) {
		t.Fatal("message carrying a producer group must not be batchable")
	}
}

func TestTryAddMessageByteGate(t *testing.T) {
	acc := NewProduceAccumulator("t")
	if err := acc.TotalBatchMaxBytes(4); err != nil {
		t.Fatal(err)
	}
	if !acc.TryAddMessage(common.NewMessage("T", []byte("aaa"))) {
		t.Fatal("3 bytes under gate of 4 must be admitted")
	}
	if !acc.TryAddMessage(common.NewMessage("T", []byte("bbb"))) {
		t.Fatal("cur=3 < 4 must still admit")
	}
	if acc.TryAddMessage(common.NewMessage("T", []byte("ccc"))) {
		t.Fatal("cur=6 >= 4 must be rejected")
	}
	if got := acc.CurrentlyHoldSize(); got != 6 {
		t.Fatalf("currentlyHoldSize = %d, want 6", got)
	}
	// An empty body is admitted (consumes no allowance) while the gate is open.
	fresh := NewProduceAccumulator("t")
	if err := fresh.TotalBatchMaxBytes(4); err != nil {
		t.Fatal(err)
	}
	if !fresh.TryAddMessage(common.NewMessage("T", nil)) {
		t.Fatal("empty body must be admitted when the gate is open")
	}
	if fresh.CurrentlyHoldSize() != 0 {
		t.Fatalf("empty body must not consume allowance, got %d", fresh.CurrentlyHoldSize())
	}
}

func TestSplitBatchResultsCommaAndNoComma(t *testing.T) {
	// Comma: split into per-message results, offset incremented.
	msgs := []*common.Message{
		common.NewMessage("T", []byte("a")),
		common.NewMessage("T", []byte("b")),
		common.NewMessage("T", []byte("c")),
	}
	out, err := splitBatchResults(commaResult(msgs, 100), 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 {
		t.Fatalf("want 3 results, got %d", len(out))
	}
	for i, want := range []string{"id0", "id1", "id2"} {
		if out[i].MsgID != want {
			t.Fatalf("result[%d].MsgID = %q, want %q", i, out[i].MsgID, want)
		}
		if out[i].QueueOffset != 100+int64(i) {
			t.Fatalf("result[%d].QueueOffset = %d, want %d", i, out[i].QueueOffset, 100+int64(i))
		}
	}

	// No comma: every index shares the SAME instance.
	single := &SendResult{SendStatus: SendOK, MsgID: "only", OffsetMsgID: "off", QueueOffset: 9}
	shared, err := splitBatchResults(single, 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if shared[i] != single {
			t.Fatalf("no-comma result[%d] must share the same instance", i)
		}
	}

	// Count mismatch is an illegal result.
	bad := &SendResult{MsgID: "a,b", OffsetMsgID: "x,y"}
	if _, err := splitBatchResults(bad, 3); err == nil {
		t.Fatal("count mismatch must error")
	}
}

func TestProduceAccumulatorSyncSingleFlush(t *testing.T) {
	p := &DefaultMQProducer{}
	acc := NewProduceAccumulator("t")
	// holdSize 1 => a 2-byte message is immediately ready.
	if err := acc.BatchMaxBytes(1); err != nil {
		t.Fatal(err)
	}
	var got []*common.Message
	acc.SendBatchFunc = func(msgs []*common.Message, _ *common.MessageQueue) (*SendResult, error) {
		got = append([]*common.Message(nil), msgs...)
		return &SendResult{SendStatus: SendOK, MsgID: "only-one", OffsetMsgID: "off", QueueOffset: 5}, nil
	}
	r, err := acc.Send(common.NewMessage("T", []byte("aa")), p)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil || r.MsgID != "only-one" {
		t.Fatalf("unexpected result: %v", r)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 message in batch, got %d", len(got))
	}
	if acc.CurrentlyHoldSize() != 0 {
		t.Fatalf("global gate not released: %d", acc.CurrentlyHoldSize())
	}
}

func TestProduceAccumulatorAsyncSplitAndDeliver(t *testing.T) {
	p := &DefaultMQProducer{}
	acc := NewProduceAccumulator("t")
	// Large thresholds so nothing flushes until we drive it.
	if err := acc.BatchMaxBytes(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := acc.BatchMaxDelayMs(30000); err != nil {
		t.Fatal(err)
	}
	var got []*common.Message
	acc.SendBatchFunc = func(msgs []*common.Message, _ *common.MessageQueue) (*SendResult, error) {
		got = append([]*common.Message(nil), msgs...)
		return commaResult(msgs, 7), nil
	}

	key := newAggregateKeyOfMessage(common.NewMessage("T", []byte("x")))
	batch := acc.getOrCreateAsyncBatch(key, p)

	var results []*SendResult
	var errs []error
	mk := func() SendCallback {
		return SendCallbackFunc{
			SuccessFn: func(r *SendResult) { results = append(results, r) },
			ExceptFn:  func(e error) { errs = append(errs, e) },
		}
	}
	// Account each message through the byte gate first, exactly as the real
	// SendAsync entry point does, so the global gate balances to zero on flush.
	for _, body := range []string{"aa", "bb", "cc"} {
		m := common.NewMessage("T", []byte(body))
		if !acc.TryAddMessage(m) {
			t.Fatal("byte gate must admit small messages")
		}
		if !batch.AddAsync(m, mk()) {
			t.Fatal("add")
		}
	}
	batch.flushAsync()

	if len(got) != 3 {
		t.Fatalf("want 3 messages flushed as one batch, got %d", len(got))
	}
	if len(errs) != 0 {
		t.Fatalf("unexpected callback errors: %v", errs)
	}
	if len(results) != 3 {
		t.Fatalf("want 3 per-message results, got %d", len(results))
	}
	for i, want := range []string{"id0", "id1", "id2"} {
		if results[i].MsgID != want {
			t.Fatalf("result[%d].MsgID = %q, want %q", i, results[i].MsgID, want)
		}
		if results[i].QueueOffset != 7+int64(i) {
			t.Fatalf("result[%d].QueueOffset = %d, want %d", i, results[i].QueueOffset, 7+int64(i))
		}
	}
	if acc.CurrentlyHoldSize() != 0 {
		t.Fatalf("global gate not released after async flush: %d", acc.CurrentlyHoldSize())
	}
}

func TestGuardRemovesEmptySyncBatch(t *testing.T) {
	acc := NewProduceAccumulator("t")
	p := &DefaultMQProducer{}
	key := newAggregateKeyOfMessage(common.NewMessage("T", []byte("x")))
	acc.PutEmptySyncBatch(key, p)
	if acc.SyncBatchCount() != 1 {
		t.Fatalf("want 1 batch, got %d", acc.SyncBatchCount())
	}
	acc.RunGuardOnce()
	if acc.SyncBatchCount() != 0 {
		t.Fatalf("guard must remove the empty batch, got %d", acc.SyncBatchCount())
	}
}
