// AsyncTraceDispatcher — the client-side trace producer
// (Java org.apache.rocketmq.client.trace.AsyncTraceDispatcher, Python
// trace_dispatcher.py, Rust trace_dispatcher.rs).
//
// Hooks append TraceContexts to an in-memory queue; a worker thread flushes
// them either when batchNum records have piled up or 5s after the last flush,
// and the encoded text goes to the trace topic through a DEDICATED internal
// producer (group "_INNER_TRACE_PRODUCER-<host group>-<PRODUCE|CONSUME>-<N>").
//
// Two anti-recursion guards are mandatory — with only one of them, trace
// messages trace themselves and the traffic multiplies forever:
//
//  1. the internal producer's own enableTrace is FALSE, so it never installs a
//     trace hook;
//  2. the hooks skip any message whose topic starts with the trace topic name.
//
// Shutdown flushes and WAITS for the last batches: a short-lived client that
// stops one tick after its last consume must still get that record out (the
// Rust port hit exactly this — the final batch was still queued when the
// internal producer was torn down).
package client

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// TraceDispatcherType is Java TraceDispatcher.Type ("PRODUCE"/"CONSUME"). It
// ends up in the internal producer's group name, so the broker sees which side
// of the client a trace batch came from.
type TraceDispatcherType string

const (
	TraceDispatcherProduce TraceDispatcherType = "PRODUCE"
	TraceDispatcherConsume TraceDispatcherType = "CONSUME"
)

const (
	// traceQueueCapacity is Java's ArrayBlockingQueue(2048). A full queue
	// DISCARDS (counted) rather than blocking the business thread.
	traceQueueCapacity = 2048
	// traceMaxBatchNum caps the configured batch size — Java's comment is
	// explicit that 20 is the ceiling.
	traceMaxBatchNum = 20
	// traceMaxMsgSize is the 128K payload cap per trace message.
	traceMaxMsgSize = 128000
	// traceFlushIntervalMillis mirrors FLUSH_TRACE_INTERVAL.
	traceFlushIntervalMillis = 5000
	// traceWorkerIdleSleep keeps an idle worker from spinning (Java sleeps 5ms).
	traceWorkerIdleSleep = 5 * time.Millisecond
	// traceProducerSendTimeoutMillis is the internal producer's send timeout.
	traceProducerSendTimeoutMillis = 5000
	// traceWorkerJoinBudget bounds the wait for the worker to notice `stopped`.
	traceWorkerJoinBudget = 5 * time.Second
	// traceSendDrainBudget bounds the wait for in-flight trace sends during
	// Shutdown. Each send is itself capped by the 5s producer timeout, so this
	// only fires when something is badly wedged.
	traceSendDrainBudget = 30 * time.Second
	// defaultTraceMsgBatchNum is Java's DefaultMQProducer/DefaultMQPushConsumer
	// traceMsgBatchNum default.
	defaultTraceMsgBatchNum = 10
)

// traceProducerGroupCounter mints the per-dispatcher suffix of the internal
// producer's group name (Java's AtomicInteger, Python's itertools.count).
var traceProducerGroupCounter atomic.Int64

// AsyncTraceDispatcher see the package comment above.
type AsyncTraceDispatcher struct {
	batchNum    int
	maxMsgSize  int
	group       string
	dispatcher  TraceDispatcherType
	traceTopic  string
	traceQueue  chan *TraceContext
	traceProd   *DefaultMQProducer
	hostProd    *DefaultMQProducer
	hostCons    *DefaultMQPushConsumer
	accessChan  string
	discardCnt  atomic.Int64
	lastFlushMs atomic.Int64
	sendSeq     atomic.Int64
	sendWG      sync.WaitGroup

	mu        sync.Mutex
	started   bool
	stopped   bool
	workerEnd chan struct{}
}

// NewAsyncTraceDispatcher builds the dispatcher and its internal producer. The
// producer is NOT started here — Start does that with the name server address.
//
// rpcHook is the HOST client's hook (ACL signing, namespace fields); the
// internal producer needs its own copy or every trace send is rejected on a
// cluster that authenticates (Java passes the host hook the same way).
func NewAsyncTraceDispatcher(group string, dispatcherType TraceDispatcherType,
	batchNum int, traceTopic string, rpcHook remoting.RPCHook) *AsyncTraceDispatcher {

	if batchNum > traceMaxBatchNum {
		batchNum = traceMaxBatchNum
	}
	if batchNum <= 0 {
		batchNum = 1
	}
	topic := traceTopic
	if topic == "" {
		topic = common.TraceTopic
	}
	d := &AsyncTraceDispatcher{
		batchNum:   batchNum,
		maxMsgSize: traceMaxMsgSize,
		group:      group,
		dispatcher: dispatcherType,
		traceTopic: topic,
		traceQueue: make(chan *TraceContext, traceQueueCapacity),
		accessChan: AccessChannelLocal,
		workerEnd:  make(chan struct{}),
	}
	p, err := NewDefaultMQProducer(d.genGroupNameForTrace())
	if err != nil {
		// The generated group name is always valid, so this cannot fail; a nil
		// producer would panic on the first send instead of at construction.
		panic(err)
	}
	p.SetSendMsgTimeout(traceProducerSendTimeoutMillis)
	p.SetMaxMessageSize(d.maxMsgSize)
	if rpcHook != nil {
		p.SetRpcHook(rpcHook)
	}
	// Anti-recursion guard 1.
	p.SetEnableTrace(false)
	d.traceProd = p
	d.lastFlushMs.Store(common.CurrentTimeMillis())
	return d
}

// genGroupNameForTrace is Java
// AsyncTraceDispatcher#getGroupNameForTrace: the trace producer's group must be
// distinguishable in the broker's producer table from the host group.
func (d *AsyncTraceDispatcher) genGroupNameForTrace() string {
	return strings.Join([]string{TraceGroupNamePrefix, d.group, string(d.dispatcher),
		i64Text(traceProducerGroupCounter.Add(1))}, "-")
}

// TraceTopicName is Java getTraceTopicName.
func (d *AsyncTraceDispatcher) TraceTopicName() string { return d.traceTopic }

// SetHostProducer points the dispatcher at the producer that owns it; the
// EndTransaction hook reads the host clientId off it.
func (d *AsyncTraceDispatcher) SetHostProducer(p *DefaultMQProducer) { d.hostProd = p }

// SetHostConsumer points the dispatcher at the owning consumer.
func (d *AsyncTraceDispatcher) SetHostConsumer(c *DefaultMQPushConsumer) { d.hostCons = c }

// clientID is Python's _client_id: the EndTransaction record's clientHost is
// the HOST client's id, not the internal producer's (Java
// EndTransactionTraceHookImpl uses the host context's client id).
func (d *AsyncTraceDispatcher) clientID() string {
	if d.hostProd != nil {
		return d.hostProd.ClientID()
	}
	if d.hostCons != nil {
		return d.hostCons.ClientID()
	}
	return ""
}

// SetMaxMsgSize overrides the 128K chunk cap (tests and unusual topics).
func (d *AsyncTraceDispatcher) SetMaxMsgSize(n int) { d.maxMsgSize = n }

// Start brings up the internal producer and the flush worker. Errors are
// returned rather than swallowed; the callers log them — a broken trace stack
// must never fail a client start.
func (d *AsyncTraceDispatcher) Start(nameSrvAddr, accessChannel string) error {
	d.mu.Lock()
	if d.started || d.stopped {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	if accessChannel != "" {
		d.accessChan = accessChannel
	}
	d.traceProd.SetNameServerAddr(nameSrvAddr)
	d.traceProd.SetInstanceName(TraceInstanceName + "_" + nameSrvAddr)
	if err := d.traceProd.Start(); err != nil {
		return err
	}

	d.mu.Lock()
	d.started = true
	d.stopped = false
	d.workerEnd = make(chan struct{})
	d.mu.Unlock()
	go d.workerLoop()
	return nil
}

// Append enqueues one context without blocking; a full queue discards it and
// bumps the counter (Java logs and drops — trace data is best-effort).
func (d *AsyncTraceDispatcher) Append(ctx *TraceContext) bool {
	if ctx == nil {
		return false
	}
	select {
	case d.traceQueue <- ctx:
		return true
	default:
		n := d.discardCnt.Add(1)
		common.LogInfof("trace buffer full%d, context is %v", n, ctx)
		return false
	}
}

// DiscardCount is how many contexts were dropped on a full queue.
func (d *AsyncTraceDispatcher) DiscardCount() int64 { return d.discardCnt.Load() }

func (d *AsyncTraceDispatcher) isStopped() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stopped
}

func (d *AsyncTraceDispatcher) workerLoop() {
	defer close(d.workerEnd)
	for !d.isStopped() {
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogErrorf("trace flushTraceContext error: %v", r)
				}
			}()
			d.flushTraceContext(false)
		}()
	}
}

// flushTraceContext is Java flushTraceContext: flush when forced, when the
// batch is full, or when 5s have passed since the last send. Otherwise it
// sleeps 5ms to keep the worker from spinning.
func (d *AsyncTraceDispatcher) flushTraceContext(force bool) {
	if len(d.traceQueue) > 0 {
		now := common.CurrentTimeMillis()
		if force || len(d.traceQueue) >= d.batchNum ||
			now-d.lastFlushMs.Load() > traceFlushIntervalMillis {
			batch := make([]*TraceContext, 0, d.batchNum)
		drain:
			for i := 0; i < d.batchNum; i++ {
				select {
				case ctx := <-d.traceQueue:
					batch = append(batch, ctx)
				default:
					break drain
				}
			}
			d.asyncSendTraceMessage(batch)
			return
		}
	}
	time.Sleep(traceWorkerIdleSleep)
}

func (d *AsyncTraceDispatcher) asyncSendTraceMessage(contexts []*TraceContext) {
	if len(contexts) == 0 {
		return
	}
	d.lastFlushMs.Store(common.CurrentTimeMillis())
	d.sendWG.Add(1)
	go func() {
		defer d.sendWG.Done()
		defer func() {
			if r := recover(); r != nil {
				common.LogErrorf("send trace data panicked: %v", r)
			}
		}()
		d.sendTraceData(contexts)
	}()
}

// Flush drains the queue and waits (bounded) for the sends to land. Java's
// flush() only forces the flush; waiting here is what makes Shutdown able to
// guarantee the last batch is out before the internal producer stops.
func (d *AsyncTraceDispatcher) Flush() {
	for len(d.traceQueue) > 0 {
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogErrorf("flushTraceContext error: %v", r)
				}
			}()
			d.flushTraceContext(true)
		}()
	}
	d.waitSends(traceSendDrainBudget)
}

func (d *AsyncTraceDispatcher) waitSends(budget time.Duration) {
	done := make(chan struct{})
	go func() {
		d.sendWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(budget):
		common.LogWarnf("trace send drain timed out after %s, group=%s", budget, d.group)
	}
}

// Shutdown stops the worker, flushes what is queued (and waits for it), then
// stops the internal producer. Safe to call more than once.
func (d *AsyncTraceDispatcher) Shutdown() {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	started := d.started
	workerEnd := d.workerEnd
	d.mu.Unlock()

	if started {
		// The worker only sleeps and spawns sends, so it exits promptly; the
		// budget is a safety net, not an expected path.
		select {
		case <-workerEnd:
		case <-time.After(traceWorkerJoinBudget):
			common.LogWarnf("trace dispatcher worker did not exit within %s, group=%s",
				traceWorkerJoinBudget, d.group)
		}
	}
	d.Flush()
	d.traceProd.Shutdown()
}

// ------------------------------------------------------------------ sending

type traceGroupKey struct{ topic, traceTopic string }

// sendTraceData is Java sendTraceData: contexts are grouped by
// (business topic, trace topic) — the business topic is the FIRST bean's — and
// each group is encoded and chunked separately.
func (d *AsyncTraceDispatcher) sendTraceData(contexts []*TraceContext) {
	beanMap := map[traceGroupKey][]*TraceTransferBean{}
	var order []traceGroupKey
	for _, ctx := range contexts {
		if ctx == nil {
			continue
		}
		accessChannel := ctx.AccessChannel
		if accessChannel == "" {
			accessChannel = d.accessChan
		}
		region := ctx.RegionID
		// Java skips a context with no region or no beans; client-side hooks
		// always fill the region on the Pub path, so this only drops broken
		// payloads.
		if region == "" || len(ctx.TraceBeans) == 0 {
			continue
		}
		traceTopic := d.traceTopic
		if accessChannel == AccessChannelCloud {
			traceTopic = TraceTopicPrefix + region
		}
		key := traceGroupKey{topic: ctx.TraceBeans[0].Topic, traceTopic: traceTopic}
		tb := EncodeTraceContext(ctx)
		if tb == nil {
			continue
		}
		if _, ok := beanMap[key]; !ok {
			order = append(order, key)
		}
		beanMap[key] = append(beanMap[key], tb)
	}
	for _, key := range order {
		d.flushData(beanMap[key], key.traceTopic)
	}
}

// flushData chunks one group's records at maxMsgSize (Java flushData).
func (d *AsyncTraceDispatcher) flushData(beans []*TraceTransferBean, traceTopic string) {
	if len(beans) == 0 {
		return
	}
	var buffer strings.Builder
	keySet := map[string]struct{}{}
	count := 0
	for _, bean := range beans {
		for k := range bean.TransKey {
			keySet[k] = struct{}{}
		}
		buffer.WriteString(bean.TransData)
		count++
		if buffer.Len() >= d.maxMsgSize {
			d.sendTraceDataByMQ(keySet, buffer.String(), traceTopic)
			buffer.Reset()
			keySet = map[string]struct{}{}
			count = 0
		}
	}
	if count > 0 {
		d.sendTraceDataByMQ(keySet, buffer.String(), traceTopic)
	}
}

// sendTraceDataByMQ sends one trace message. Two ways in, exactly as Java:
// plain send by default, or a selector restricted to the brokers that actually
// host the trace topic (only meaningful once a route is known — a unit test
// without a cluster takes the plain path).
func (d *AsyncTraceDispatcher) sendTraceDataByMQ(keySet map[string]struct{}, data, traceTopic string) {
	msg := common.NewMessage(traceTopic, []byte(data))
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		// The empty key comes from splitting a KEYS property that ends in a
		// space — Java's sorted(...).filter(non-empty) drops it the same way.
		if k != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	msg.SetKeys(strings.Join(keys, common.KeySeparator))

	brokerSet := d.tryGetMessageQueueBrokerSet(traceTopic)
	var err error
	if len(brokerSet) == 0 {
		_, err = d.traceProd.SendWithTimeout(msg, traceProducerSendTimeoutMillis)
	} else {
		_, err = d.traceProd.SendBySelectorWithTimeout(msg, traceBrokerSetSelector{d: d},
			brokerSet, traceProducerSendTimeoutMillis)
	}
	if err != nil {
		common.LogErrorf("send trace data failed, the traceData is %s: %v", data, err)
	}
}

// tryGetMessageQueueBrokerSet is Java tryGetMessageQueueBrokerSet: the broker
// names the trace topic is published on. No route yet (or no cluster) is an
// empty set, which the caller reads as "plain send".
func (d *AsyncTraceDispatcher) tryGetMessageQueueBrokerSet(topic string) map[string]struct{} {
	out := map[string]struct{}{}
	publish, err := d.traceProd.topicPublishInfo(topic)
	if err != nil || publish == nil {
		return out
	}
	for _, q := range publish.MsgQueueList() {
		out[q.BrokerName] = struct{}{}
	}
	return out
}

// traceBrokerSetSelector is Python's _BrokerSetSelector: round-robin over the
// queues whose broker is in the set, falling back to every queue when the
// filter leaves nothing (Java would throw IndexOutOfBounds there; the ports
// treat it as "no cross-cluster filtering needed").
type traceBrokerSetSelector struct{ d *AsyncTraceDispatcher }

func (s traceBrokerSetSelector) Select(mqs []common.MessageQueue, msg *common.Message,
	arg any) (common.MessageQueue, error) {

	brokerSet, _ := arg.(map[string]struct{})
	filtered := make([]common.MessageQueue, 0, len(mqs))
	for _, q := range mqs {
		if _, ok := brokerSet[q.BrokerName]; ok {
			filtered = append(filtered, q)
		}
	}
	if len(filtered) == 0 {
		filtered = mqs
	}
	if len(filtered) == 0 {
		return common.MessageQueue{}, common.ClientError("no message queue for trace topic")
	}
	pos := int(s.d.sendSeq.Add(1)-1) % len(filtered)
	return filtered[pos], nil
}
