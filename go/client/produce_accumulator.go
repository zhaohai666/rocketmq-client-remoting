// When a producer enables autoBatch, Send(Message) no longer goes out one by one:
// messages are accumulated by an AggregateKey (topic + mq + waitStoreMsgOK + tag)
// into a MessageAccumulation, and only flushed as ONE MessageBatch when this
// batch's holdSize (bytes) or holdMs (time) threshold is reached, or when a guard
// goroutine wakes waiting senders. The broker returns a comma-separated list of
// per-message MsgIds which is split back into one SendResult per message — the
// caller sees exactly what a direct send would have shown.
package client

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

const (
	DefaultTotalHoldSize int64 = 32 * 1024 * 1024
	DefaultHoldSize      int64 = 32 * 1024
	DefaultHoldMs        int64 = 10
)

type AggregateKey struct {
	Topic          string
	Mq             common.MessageQueue
	Pinned         bool
	WaitStoreMsgOK bool
	Tag            string
	TagNull        bool
}

func newAggregateKeyOfMessage(msg *common.Message) AggregateKey {
	tag, hasTag := msg.GetTags()
	return AggregateKey{
		Topic:          msg.Topic,
		Pinned:         false,
		WaitStoreMsgOK: msg.IsWaitStoreMsgOK(),
		Tag:            tag,
		TagNull:        !hasTag,
	}
}

func newAggregateKeyOfMessageWithMq(msg *common.Message, mq common.MessageQueue) AggregateKey {
	tag, hasTag := msg.GetTags()
	return AggregateKey{
		Topic:          msg.Topic,
		Mq:             mq,
		Pinned:         true,
		WaitStoreMsgOK: msg.IsWaitStoreMsgOK(),
		Tag:            tag,
		TagNull:        !hasTag,
	}
}

// String is a debug form; not part of the wire contract.
func (k AggregateKey) String() string {
	tag := k.Tag
	if k.TagNull {
		tag = "<null>"
	}
	mq := "<unpinned>"
	if k.Pinned {
		mq = k.Mq.String()
	}
	return "AggregateKey(topic=" + k.Topic + ", mq=" + mq +
		", waitStoreMsgOK=" + boolText(k.WaitStoreMsgOK) + ", tag=" + tag + ")"
}

func boolText(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

type MessageAccumulation struct {
	aggregateKey AggregateKey
	producer     *DefaultMQProducer
	owner        *ProduceAccumulator

	// mu guards every field below; cond is its condition variable.
	mu           sync.Mutex
	cond         *sync.Cond
	closed       bool
	messages     []*common.Message
	callbacks    []SendCallback
	keys         map[string]struct{} // sync Add collects; async does not
	count        int
	messagesSize int
	sendResults  []*SendResult
	sendErr      error
	createTime   int64
}

func newMessageAccumulation(key AggregateKey, producer *DefaultMQProducer, owner *ProduceAccumulator) *MessageAccumulation {
	b := &MessageAccumulation{
		aggregateKey: key,
		producer:     producer,
		owner:        owner,
		keys:         make(map[string]struct{}),
		createTime:   time.Now().UnixMilli(),
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// readyToSend is per-batch: this batch's byte count OR this batch's age, not the
// global allowance.
func (b *MessageAccumulation) readyToSend() bool {
	return b.messagesSize > int(b.owner.holdSize.Load()) ||
		time.Now().UnixMilli() >= b.createTime+b.owner.holdMs.Load()
}

func (b *MessageAccumulation) pinnedMq() *common.MessageQueue {
	if b.aggregateKey.Pinned {
		mq := b.aggregateKey.Mq
		return &mq
	}
	return nil
}

// IsClosed reports whether the batch has already been sent/closed.
func (b *MessageAccumulation) IsClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// MessagesSize exposes the accumulated byte count (test/inspection seam).
func (b *MessageAccumulation) MessagesSize() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.messagesSize
}

// Count exposes the number of messages accumulated so far.
func (b *MessageAccumulation) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

// SendResults exposes the per-index results (test seam).
func (b *MessageAccumulation) SendResults() []*SendResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*SendResult(nil), b.sendResults...)
}

// Add accumulates a message synchronously and blocks (under mu) until the batch
// is flushed. It returns this message's index, or -1 if the batch was already
// closed by another goroutine (caller must retry with a fresh batch).
func (b *MessageAccumulation) Add(msg *common.Message) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return -1
	}
	ret := b.count
	b.count++
	b.messages = append(b.messages, msg)
	b.messagesSize += len(msg.GetBody())
	if ks, ok := msg.GetKeys(); ok {
		splitKeysInto(b.keys, ks)
	}
	for !b.closed {
		if b.readyToSend() {
			b.flushLocked()
			break
		}
		b.cond.Wait()
	}
	return ret
}

func (b *MessageAccumulation) AddAsync(msg *common.Message, cb SendCallback) bool {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return false
	}
	b.count++
	b.messages = append(b.messages, msg)
	b.callbacks = append(b.callbacks, cb)
	b.messagesSize += len(msg.GetBody())
	ready := b.readyToSend()
	b.mu.Unlock()

	if ready {
		b.flushAsync()
	}
	return true
}

// wakeup signals every waiting Add goroutine to re-check readyToSend.
func (b *MessageAccumulation) wakeup() {
	b.mu.Lock()
	if !b.closed {
		b.cond.Broadcast()
	}
	b.mu.Unlock()
}

func (b *MessageAccumulation) flushLocked() {
	if b.closed {
		return
	}
	b.closed = true

	msgs := b.messages
	size := b.messagesSize
	cnt := b.count
	pinned := b.pinnedMq()

	res, err := b.doSend(msgs, pinned)
	if err != nil {
		b.sendErr = err
	} else if res == nil {
		b.sendErr = common.ClientError("sync batch send returned null")
	} else if results, serr := splitBatchResults(res, cnt); serr != nil {
		b.sendErr = serr
	} else {
		b.sendResults = results
	}
	b.owner.releaseHold(size)
	b.cond.Broadcast()
}

// flushAsync sends the batch and delivers each accumulated callback its own
// per-message result. It runs the (blocking) batch send OUTSIDE b.mu so the
// guard goroutine and concurrent AddAsync callers are not stalled on the mutex;
// the closed flag still guarantees exactly one flush per batch.
func (b *MessageAccumulation) flushAsync() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	msgs := b.messages
	cbs := b.callbacks
	size := b.messagesSize
	cnt := b.count
	pinned := b.pinnedMq()
	b.mu.Unlock()

	res, err := b.doSend(msgs, pinned)
	if err == nil && res == nil {
		err = common.ClientError("async batch send returned null")
	}
	var results []*SendResult
	if err == nil {
		var serr error
		results, serr = splitBatchResults(res, cnt)
		if serr != nil {
			err = serr
		}
	}

	if err != nil {
		for _, cb := range cbs {
			cb.OnException(err)
		}
	} else {
		for i := 0; i < len(cbs) && i < len(results); i++ {
			cbs[i].OnSuccess(results[i])
		}
	}
	b.owner.releaseHold(size)
}

// doSend performs the actual batch send, honouring the test seam.
func (b *MessageAccumulation) doSend(msgs []*common.Message, pinned *common.MessageQueue) (*SendResult, error) {
	if b.owner != nil && b.owner.SendBatchFunc != nil {
		return b.owner.SendBatchFunc(msgs, pinned)
	}
	return b.producer.sendAccumulatorBatch(msgs, b.aggregateKey.Tag, !b.aggregateKey.TagNull,
		b.keys, pinned, b.producer.sendMsgTimeout)
}

// splitBatchResults turns the broker's batch SendResult into one per message.
// A comma-bearing MsgId is split (counts must match); otherwise every index
// shares the same SendResult instance (old broker / single reply).
func splitBatchResults(sendResult *SendResult, count int) ([]*SendResult, error) {
	results := make([]*SendResult, count)
	msgID := sendResult.MsgID
	if strings.Contains(msgID, ",") {
		msgIds := strings.Split(msgID, ",")
		offsetMsgIds := strings.Split(sendResult.OffsetMsgID, ",")
		if len(msgIds) != count || len(offsetMsgIds) != count {
			return nil, common.ClientError("sendResult is illegal")
		}
		for i := 0; i < count; i++ {
			results[i] = &SendResult{
				SendStatus:    sendResult.SendStatus,
				MsgID:         msgIds[i],
				MessageQueue:  sendResult.MessageQueue,
				QueueOffset:   sendResult.QueueOffset + int64(i),
				TransactionID: sendResult.TransactionID,
				OffsetMsgID:   offsetMsgIds[i],
				RegionID:      sendResult.RegionID,
				TraceOn:       sendResult.TraceOn,
				RecallHandle:  sendResult.RecallHandle,
			}
		}
		return results, nil
	}
	for i := 0; i < count; i++ {
		results[i] = sendResult
	}
	return results, nil
}

func splitKeysInto(target map[string]struct{}, keys string) {
	parts := strings.Split(keys, common.KeySeparator)
	end := len(parts)
	for end > 0 && parts[end-1] == "" {
		end--
	}
	for i := 0; i < end; i++ {
		target[parts[i]] = struct{}{}
	}
}

func canBatch(msg *common.Message) bool {
	if msg.DelayTimeLevel() > 0 {
		return false
	}
	if strings.HasPrefix(msg.Topic, common.RetryGroupTopicPrefix) {
		return false
	}
	return true
}

// ProduceAccumulator accumulates auto-batched sends, keyed by AggregateKey, and
// runs a guard goroutine that flushes time-expired batches and cleans up empty
// ones. One instance is owned per producer (created in Start, stopped in
// Shutdown).
type ProduceAccumulator struct {
	instanceName string

	// Global byte gate.
	currentlyHoldSize atomic.Int64
	holdMs            atomic.Int64
	holdSize          atomic.Int64
	totalHoldSize     atomic.Int64

	syncMu       sync.Mutex
	syncBatches  map[AggregateKey]*MessageAccumulation
	asyncMu      sync.Mutex
	asyncBatches map[AggregateKey]*MessageAccumulation

	// Test seam: when non-nil it replaces the real batch send, so unit tests can
	// verify accumulation/split with no broker.
	SendBatchFunc func(messages []*common.Message, mq *common.MessageQueue) (*SendResult, error)

	guardMu sync.Mutex
	started bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func NewProduceAccumulator(instanceName string) *ProduceAccumulator {
	a := &ProduceAccumulator{
		instanceName: instanceName,
		syncBatches:  make(map[AggregateKey]*MessageAccumulation),
		asyncBatches: make(map[AggregateKey]*MessageAccumulation),
	}
	a.holdMs.Store(DefaultHoldMs)
	a.holdSize.Store(DefaultHoldSize)
	a.totalHoldSize.Store(DefaultTotalHoldSize)
	return a
}

func (a *ProduceAccumulator) GetBatchMaxDelayMs() int64 { return a.holdMs.Load() }

func (a *ProduceAccumulator) BatchMaxDelayMs(holdMs int64) error {
	if holdMs <= 0 || holdMs > 30*1000 {
		return common.ClientError("batchMaxDelayMs expect between 1ms and 30s, but get " + itoa64(holdMs) + "!")
	}
	a.holdMs.Store(holdMs)
	return nil
}

func (a *ProduceAccumulator) GetBatchMaxBytes() int64 { return a.holdSize.Load() }

func (a *ProduceAccumulator) BatchMaxBytes(holdSize int64) error {
	if holdSize <= 0 || holdSize > 2*1024*1024 {
		return common.ClientError("batchMaxBytes expect between 1B and 2MB, but get " + itoa64(holdSize) + "!")
	}
	a.holdSize.Store(holdSize)
	return nil
}

func (a *ProduceAccumulator) GetTotalBatchMaxBytes() int64 { return a.holdSize.Load() }

func (a *ProduceAccumulator) TotalBatchMaxBytes(totalHoldSize int64) error {
	if totalHoldSize <= 0 {
		return common.ClientError("totalBatchMaxBytes must bigger then 0, but get " + itoa64(totalHoldSize) + "!")
	}
	a.totalHoldSize.Store(totalHoldSize)
	return nil
}

func (a *ProduceAccumulator) CurrentlyHoldSize() int64 { return a.currentlyHoldSize.Load() }

// ---- global byte gate ----

// TryAddMessage is the global byte gate: admits while currentlyHoldSize <
// totalHoldSize and accounts the body length. An empty body is admitted but
// consumes no allowance.
func (a *ProduceAccumulator) TryAddMessage(msg *common.Message) bool {
	for {
		cur := a.currentlyHoldSize.Load()
		if cur >= a.totalHoldSize.Load() {
			return false
		}
		n := int64(len(msg.GetBody()))
		if n <= 0 {
			return true
		}
		if a.currentlyHoldSize.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

// ReleaseHold returns bytes to the global gate once a batch actually sent.
func (a *ProduceAccumulator) releaseHold(size int) {
	a.currentlyHoldSize.Add(int64(-size))
}

// ---- table operations ----

func (a *ProduceAccumulator) getOrCreateSyncBatch(key AggregateKey, producer *DefaultMQProducer) *MessageAccumulation {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()
	if b, ok := a.syncBatches[key]; ok {
		return b
	}
	b := newMessageAccumulation(key, producer, a)
	a.syncBatches[key] = b
	return b
}

func (a *ProduceAccumulator) getOrCreateAsyncBatch(key AggregateKey, producer *DefaultMQProducer) *MessageAccumulation {
	a.asyncMu.Lock()
	defer a.asyncMu.Unlock()
	if b, ok := a.asyncBatches[key]; ok {
		return b
	}
	b := newMessageAccumulation(key, producer, a)
	a.asyncBatches[key] = b
	return b
}

func (a *ProduceAccumulator) removeSyncBatch(key AggregateKey, batch *MessageAccumulation) {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()
	if b, ok := a.syncBatches[key]; ok && b == batch {
		delete(a.syncBatches, key)
	}
}

func (a *ProduceAccumulator) removeAsyncBatch(key AggregateKey, batch *MessageAccumulation) {
	a.asyncMu.Lock()
	defer a.asyncMu.Unlock()
	if b, ok := a.asyncBatches[key]; ok && b == batch {
		delete(a.asyncBatches, key)
	}
}

func (a *ProduceAccumulator) SyncBatchCount() int {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()
	return len(a.syncBatches)
}

func (a *ProduceAccumulator) AsyncBatchCount() int {
	a.asyncMu.Lock()
	defer a.asyncMu.Unlock()
	return len(a.asyncBatches)
}

func (a *ProduceAccumulator) SyncBatchesSnapshot() []*MessageAccumulation {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()
	out := make([]*MessageAccumulation, 0, len(a.syncBatches))
	for _, b := range a.syncBatches {
		out = append(out, b)
	}
	return out
}

func (a *ProduceAccumulator) AsyncBatchesSnapshot() []*MessageAccumulation {
	a.asyncMu.Lock()
	defer a.asyncMu.Unlock()
	out := make([]*MessageAccumulation, 0, len(a.asyncBatches))
	for _, b := range a.asyncBatches {
		out = append(out, b)
	}
	return out
}

// PutEmptySyncBatch inserts a fresh (empty) sync batch for a key — used by tests
// to exercise the guard's empty-batch cleanup without racing a real Add.
func (a *ProduceAccumulator) PutEmptySyncBatch(key AggregateKey, producer *DefaultMQProducer) *MessageAccumulation {
	return a.getOrCreateSyncBatch(key, producer)
}

// RunGuardOnce runs one guard round synchronously (test seam).
func (a *ProduceAccumulator) RunGuardOnce() {
	a.doGuardRound()
}

// ---- send entry points ----

// Send accumulates a message synchronously and returns this message's result.
func (a *ProduceAccumulator) Send(msg *common.Message, producer *DefaultMQProducer) (*SendResult, error) {
	return a.sendSync(newAggregateKeyOfMessage(msg), msg, nil, producer)
}

// SendWithMq accumulates a message synchronously to a pinned queue.
func (a *ProduceAccumulator) SendWithMq(msg *common.Message, mq common.MessageQueue, producer *DefaultMQProducer) (*SendResult, error) {
	return a.sendSync(newAggregateKeyOfMessageWithMq(msg, mq), msg, &mq, producer)
}

func (a *ProduceAccumulator) sendSync(key AggregateKey, msg *common.Message, mq *common.MessageQueue, producer *DefaultMQProducer) (*SendResult, error) {
	// Byte gate first; a full gate sends directly without accounting.
	if !a.TryAddMessage(msg) {
		return producer.sendDirectWithMq(msg, mq, producer.sendMsgTimeout)
	}
	if !canBatch(msg) {
		return producer.sendDirectWithMq(msg, mq, producer.sendMsgTimeout)
	}
	for {
		batch := a.getOrCreateSyncBatch(key, producer)
		idx := batch.Add(msg)
		if idx == -1 {
			a.removeSyncBatch(key, batch)
			continue
		}
		batch.mu.Lock()
		sendErr := batch.sendErr
		var result *SendResult
		if idx >= 0 && idx < len(batch.sendResults) {
			result = batch.sendResults[idx]
		}
		batch.mu.Unlock()
		if sendErr != nil {
			return nil, sendErr
		}
		return result, nil
	}
}

// SendAsync accumulates a message asynchronously; the callback fires exactly
// once via the accumulator.
func (a *ProduceAccumulator) SendAsync(msg *common.Message, callback SendCallback, producer *DefaultMQProducer) {
	a.sendAsync(newAggregateKeyOfMessage(msg), msg, nil, callback, producer)
}

// SendAsyncWithMq accumulates a message asynchronously to a pinned queue.
func (a *ProduceAccumulator) SendAsyncWithMq(msg *common.Message, mq common.MessageQueue, callback SendCallback, producer *DefaultMQProducer) {
	a.sendAsync(newAggregateKeyOfMessageWithMq(msg, mq), msg, &mq, callback, producer)
}

func (a *ProduceAccumulator) sendAsync(key AggregateKey, msg *common.Message, mq *common.MessageQueue, callback SendCallback, producer *DefaultMQProducer) {
	if !a.TryAddMessage(msg) {
		producer.sendAsyncDirect(msg, mq, callback, producer.sendMsgTimeout)
		return
	}
	if !canBatch(msg) {
		producer.sendAsyncDirect(msg, mq, callback, producer.sendMsgTimeout)
		return
	}
	for {
		batch := a.getOrCreateAsyncBatch(key, producer)
		if batch.AddAsync(msg, callback) {
			return
		}
		a.removeAsyncBatch(key, batch)
	}
}

// ---- guard goroutine ----

// Start launches the guard goroutine. Idempotent.
func (a *ProduceAccumulator) Start() {
	a.guardMu.Lock()
	if a.started {
		a.guardMu.Unlock()
		return
	}
	a.stopCh = make(chan struct{})
	a.started = true
	a.guardMu.Unlock()

	a.wg.Add(1)
	go a.guardLoop()
}

// Shutdown stops the guard goroutine. Idempotent.
func (a *ProduceAccumulator) Shutdown() {
	a.guardMu.Lock()
	if !a.started {
		a.guardMu.Unlock()
		return
	}
	close(a.stopCh)
	a.started = false
	a.guardMu.Unlock()
	a.wg.Wait()
}

func (a *ProduceAccumulator) guardLoop() {
	defer a.wg.Done()
	for {
		select {
		case <-a.stopCh:
			return
		default:
		}
		a.doGuardRound()
	}
}

func (a *ProduceAccumulator) doGuardRound() {
	sleepMs := int64(1)
	if h := a.holdMs.Load() / 2; h > sleepMs {
		sleepMs = h
	}
	// Sync: wake waiters so they re-check readyToSend; drop truly-empty batches.
	for _, v := range a.SyncBatchesSnapshot() {
		v.mu.Lock()
		if v.closed {
			v.mu.Unlock()
			continue
		}
		if v.count == 0 {
			v.closed = true
			v.mu.Unlock()
			a.removeSyncBatch(v.aggregateKey, v)
			continue
		}
		v.cond.Broadcast()
		v.mu.Unlock()
	}
	// Async: flush ready batches, then drop empty ones.
	for _, v := range a.AsyncBatchesSnapshot() {
		v.mu.Lock()
		closed := v.closed
		ready := !closed && v.readyToSend()
		empty := !closed && v.count == 0
		v.mu.Unlock()
		if ready {
			v.flushAsync()
		} else if empty {
			v.mu.Lock()
			if !v.closed {
				v.closed = true
			}
			v.mu.Unlock()
			a.removeAsyncBatch(v.aggregateKey, v)
		}
	}
	time.Sleep(time.Duration(sleepMs) * time.Millisecond)
}

// itoa64 is a small local helper to avoid importing strconv just for error text.
func itoa64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
