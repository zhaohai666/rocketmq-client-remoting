// MQPullConsumerScheduleService — the scheduled pull consumer (Java
// org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService, with the
// queue-serial dispatch discipline of impl.consumer.PullMessageService /
// PullRequest).
//
// It sits above DefaultMQPullConsumer and takes the manual loop out of the
// caller's hands: register ONE callback per topic, and the service fans it
// out to every MessageQueue this instance should own, pulls each queue on a
// schedule and hands the result to the callback, which answers with the delay
// until the next round. Per-queue it is exactly the Java PullTaskImpl chain
// (schedule → run → re-schedule with context.getPullNextDelayTimeMillis()),
// but pulled forward by the service rather than by the callback: the callback
// never calls Pull itself, so the cursor, the OFFSET_ILLEGAL correction and
// the offset bookkeeping stay in ONE place instead of in every user callback.
//
// The rules that make it behave:
//
//  1. Queue expansion follows the assignment, not the whole topic. The task
//     set for a topic is computed with FetchMessageQueuesInBalance (the same
//     on-demand rebalanceByTopic formula the pull consumer exposes): BROAD
//     -CASTING takes every queue without consulting the group, CLUSTERING
//     takes this clientId's slice — one task per owned queue, never two
//     instances on one queue (example/simple/PullConsumer.java:62's point).
//  2. One queue runs one round at a time. Java gets this from scheduling the
//     SAME PullTaskImpl again only after its run() returned; here a queue's
//     re-schedule likewise happens only after its round completed, and a
//     per-queue mutex additionally serialises the handover window where an
//     old cancelled task is still mid-round while an expansion already
//     created its replacement (putTask's cancel-then-add, same race Java
//     has — here it is closed).
//  3. Three reschedule channels, in priority order (documented on
//     resolveNextDelay): the callback's return value, the one-shot
//     ctx.PullMessageLater (PullMessageService#executePullRequestLater's
//     shape), and the per-round ctx.SetPullInterval (Java
//     PullTaskContext.setPullNextDelayTimeMillis, default 200ms).
//  4. The cursor follows nextBeginOffset after EVERY round whatever the
//     status (DefaultLitePullConsumer's rule, and the OFFSET_ILLEGAL
//     correction rides in that same field) and is parked into the offset
//     store each round, so a queue revoked and re-assigned later resumes
//     from the broker-committed position instead of restarting blind.
//
// Lifecycle: Start creates the dispatch scheduler (a Go goroutine per queue
// plus a pullThreadNums-wide execution gate, Java's ScheduledThreadPoolExecutor
// of 20), installs the queue listener on the consumer (Java start():90 —
// any listener the caller set is replaced, same as Java), starts the consumer
// and runs the first expansion; a 20s rebalance tick (Java's
// RebalanceService period) re-expands every registered topic, and the broker's
// NOTIFY_CONSUMER_IDS_CHANGED(40) push triggers one immediately through the
// installed listener. Shutdown stops accepting rounds, cancels the queue
// tasks, DRAINS the rounds already running (bounded by shutdownDrainBudget)
// and only then shuts the consumer down — which persists the offsets,
// unregisters the group and returns the instance reference (an instance is
// only torn down when its last tenant leaves, see DetachFromRegistryIfLast
// Tenant). A caller-supplied consumer is reused as-is; when the service built
// its own it owns its whole lifecycle.
package client

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Schedule-service defaults (Java MQPullConsumerScheduleService /
// PullTaskContext 5.x).
const (
	// DefaultPullScheduleThreadNums is MQPullConsumerScheduleService.pullThread
	// Nums:46 — the pool that runs PullTaskImpls.
	DefaultPullScheduleThreadNums = 20
	// DefaultPullTaskNextDelayMillis is PullTaskContext.pullNextDelayTimeMillis
	// :24 — the gap between two rounds when the callback does not choose.
	DefaultPullTaskNextDelayMillis = int64(200)
	// DefaultPullScheduleRebalanceIntervalMillis is the queue-set re-expansion
	// period (Java RebalanceService's 20s; the pull consumer has no rebalance
	// loop of its own, so the service runs the one that drives putTask).
	DefaultPullScheduleRebalanceIntervalMillis = int64(20000)
	// pullTaskPanicDelayMillis is the reschedule delay after a callback that
	// threw (Java PullTaskImpl.run:183-184: setPullNextDelayTimeMillis(1000)).
	// A var so a test can watch the retry without waiting a second.
	pullTaskPanicDelayMillis = int64(1000)
)

// PullTaskCallback is the Java PullTaskCallback flipped to its Go shape: the
// service has already pulled by the time it runs, and the callback answers
// with the delay until THIS queue's next round. Returning <= 0 defers to the
// context channels (PullMessageLater / SetPullInterval). When the pull failed
// result is nil and context.PullError() carries the cause — the queue's
// cursor is untouched in that case, the next round retries the same offset.
type PullTaskCallback func(mq common.MessageQueue, result *PullResult, context *PullTaskContext) int64

// PullTaskContext is Java's PullTaskContext (pullConsumer + the next-round
// delay) plus the PullMessageService-style one-shot channel and the round's
// pull error. Safe for use from the callback goroutine.
type PullTaskContext struct {
	mu           sync.Mutex
	consumer     *DefaultMQPullConsumer
	pullInterval int64
	pullLater    *int64
	pullError    error
}

// PullConsumer exposes the consumer driving this round (Java
// PullTaskContext#getPullConsumer — here read-only: pulls go through the
// service, the context hands the consumer to callers that need offsets or
// admin lookups inside the callback).
func (x *PullTaskContext) PullConsumer() *DefaultMQPullConsumer { return x.consumer }

// SetPullInterval is Java setPullNextDelayTimeMillis: the standing gap between
// this queue's rounds. Durable — it applies to every later round until changed.
func (x *PullTaskContext) SetPullInterval(millis int64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.pullInterval = millis
}

// PullInterval reads back the standing gap.
func (x *PullTaskContext) PullInterval() int64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.pullInterval
}

// PullMessageLater is the executePullRequestLater channel: schedule THIS
// queue's next round millis later, ONCE (the standing interval keeps
// applying afterwards). A round may overwrite it before it is consumed.
func (x *PullTaskContext) PullMessageLater(millis int64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	m := millis
	x.pullLater = &m
}

// PullError is the pull failure of this round (nil when the pull succeeded).
func (x *PullTaskContext) PullError() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.pullError
}

// resolveNextDelay folds the three reschedule channels into the delay until
// the next round. Priority: (1) a positive value the callback returned,
// (2) the one-shot PullMessageLater (consumed here), (3) the standing
// SetPullInterval (default 200ms, Java's context initialiser). A
// PullMessageLater that was overwritten by a newer round before being
// consumed is simply the newer value — the three channels are one queue's
// state, never shared.
func (x *PullTaskContext) resolveNextDelay(returned int64) int64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	if returned > 0 {
		x.pullLater = nil
		return returned
	}
	if x.pullLater != nil {
		v := *x.pullLater
		x.pullLater = nil
		if v < 0 {
			return 0
		}
		return v
	}
	if x.pullInterval < 0 {
		return 0
	}
	return x.pullInterval
}

// MQPullConsumerScheduleService schedules one PullTaskCallback per topic and
// one queue round per owned queue.
type MQPullConsumerScheduleService struct {
	mu sync.Mutex

	started  bool
	stopped  atomic.Bool
	stop     chan struct{}
	stopOnce sync.Once
	loopWG   sync.WaitGroup // the rebalance loop
	runWG    sync.WaitGroup // rounds currently running (the drain target)

	consumer     *DefaultMQPullConsumer
	ownsConsumer bool

	pullThreadNums          int
	rebalanceIntervalMillis int64
	pullBatchSize           int32

	callbacks  map[string]PullTaskCallback
	tasks      map[common.MessageQueue]*pullQueueTask
	queueLocks map[common.MessageQueue]*sync.Mutex
	sem        chan struct{}
	// rebalanceNow is the 40-push / registration kick: a capacity-1 coalescing
	// channel, so a burst of notifications runs one extra expansion, not one
	// per push.
	rebalanceNow chan struct{}
}

// pullQueueTask is Java's PullTaskImpl: one queue's scheduling chain. The
// chain runs on its own goroutine; cancellation is a closed channel plus the
// cancelled flag Java checks after doPullTask (:187).
type pullQueueTask struct {
	mq         common.MessageQueue
	service    *MQPullConsumerScheduleService
	cancel     chan struct{}
	cancelOnce sync.Once
	cancelled  atomic.Bool
	// cursor is the queue's pull position; -1 means "not resolved yet" and
	// 	the next round reads it from the offset store / broker.
	cursor atomic.Int64
}

// NewMQPullConsumerScheduleService mirrors Java's constructor: the group is
// mandatory, and the internal pull consumer is CLUSTERING (MQPullConsumer
// ScheduleService:53).
func NewMQPullConsumerScheduleService(consumerGroup string) (*MQPullConsumerScheduleService, error) {
	c, err := NewDefaultMQPullConsumer(consumerGroup)
	if err != nil {
		return nil, err
	}
	c.SetMessageModel(MessageModelClustering)
	return newMQPullConsumerScheduleService(c, true), nil
}

// MustNewMQPullConsumerScheduleService panics instead of returning an error.
func MustNewMQPullConsumerScheduleService(consumerGroup string) *MQPullConsumerScheduleService {
	s, err := NewMQPullConsumerScheduleService(consumerGroup)
	if err != nil {
		panic(err)
	}
	return s
}

func newMQPullConsumerScheduleService(c *DefaultMQPullConsumer, owns bool) *MQPullConsumerScheduleService {
	return &MQPullConsumerScheduleService{
		consumer:                c,
		ownsConsumer:            owns,
		pullThreadNums:          DefaultPullScheduleThreadNums,
		rebalanceIntervalMillis: DefaultPullScheduleRebalanceIntervalMillis,
		pullBatchSize:           DefaultPullMaxMsgNums,
		callbacks:               map[string]PullTaskCallback{},
		tasks:                   map[common.MessageQueue]*pullQueueTask{},
		queueLocks:              map[common.MessageQueue]*sync.Mutex{},
		rebalanceNow:            make(chan struct{}, 1),
	}
}

// ---------------------------------------------------------------- configuration

// PullConsumer exposes the underlying pull consumer (Java
// getDefaultMQPullConsumer — configure nameservers, the model, the timeouts
// and the allocate strategy through it before Start).
func (s *MQPullConsumerScheduleService) PullConsumer() *DefaultMQPullConsumer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumer
}

// SetPullConsumer injects a caller-owned pull consumer (Java
// setDefaultMQPullConsumer). It must run before Start; the service starts and
// shuts down the injected consumer exactly as Java's start()/shutdown() do,
// so a consumer the caller started itself is merely reused (Start and
// Shutdown are idempotent). After Start it is refused with a warning: hot
// swapping would leave the old consumer's instance reference unreturned.
func (s *MQPullConsumerScheduleService) SetPullConsumer(c *DefaultMQPullConsumer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		common.LogWarnf("SetPullConsumer ignored: the schedule service for group %s is already started",
			s.groupLocked())
		return
	}
	if c == nil {
		return
	}
	s.consumer = c
	s.ownsConsumer = false
}

func (s *MQPullConsumerScheduleService) groupLocked() string {
	if s.consumer == nil {
		return ""
	}
	return s.consumer.ConsumerGroup()
}

// SetPullThreadNums is Java setPullThreadNums:46. Read when Start builds the
// execution gate; later changes need a restart. A non-positive value falls
// back to the default rather than creating a gate that admits nothing.
func (s *MQPullConsumerScheduleService) SetPullThreadNums(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pullThreadNums = n
}

func (s *MQPullConsumerScheduleService) PullThreadNums() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pullThreadNums
}

// SetRebalanceIntervalMillis tunes the queue-set re-expansion period. Like
// pullThreadNums it is read at Start.
func (s *MQPullConsumerScheduleService) SetRebalanceIntervalMillis(millis int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebalanceIntervalMillis = millis
}

func (s *MQPullConsumerScheduleService) RebalanceIntervalMillis() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rebalanceIntervalMillis
}

// SetPullBatchSize is the maxNums of every scheduled Pull (Java's examples
// pass 32 by hand; DefaultPullMaxMsgNums is the shared default).
func (s *MQPullConsumerScheduleService) SetPullBatchSize(n int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n > 0 {
		s.pullBatchSize = n
	}
}

func (s *MQPullConsumerScheduleService) PullBatchSize() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pullBatchSize
}

// MessageModel forwards to the consumer (Java getMessageModel /
// setMessageModel; putTask's mqAll-vs-mqDivided choice depends on it).
func (s *MQPullConsumerScheduleService) SetMessageModel(model string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consumer != nil {
		s.consumer.SetMessageModel(model)
	}
}

func (s *MQPullConsumerScheduleService) MessageModel() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consumer == nil {
		return ""
	}
	return s.consumer.MessageModel()
}

// RegisterPullTaskCallback is Java registerPullTaskCallback:98 — one callback
// per topic. The topic is also subscribed (Java's
// registerMessageQueueListener(topic, null) half: without the subscription
// the heartbeat never carries it and the pull auto-registration does the
// work late). A nil callback unregisters the topic and revokes its tasks.
// Usable before AND after Start; after Start the queue expansion for the
// topic runs immediately.
func (s *MQPullConsumerScheduleService) RegisterPullTaskCallback(topic string, callback PullTaskCallback) error {
	if err := common.CheckTopic(topic); err != nil {
		return err
	}
	s.mu.Lock()
	consumer := s.consumer
	started := s.started
	if callback == nil {
		delete(s.callbacks, topic)
	} else {
		s.callbacks[topic] = callback
	}
	s.mu.Unlock()
	if consumer == nil {
		return common.ClientError("the pull consumer is not set")
	}
	if err := consumer.Subscribe(topic, "*"); err != nil {
		return err
	}
	if callback == nil {
		// Unregister: revoke the topic's tasks right now, do not wait for the
		// next expansion tick (an expansion over a deleted callback yields an
		// empty set, which is exactly putTask's revoke-all path — keep the two
		// consistent by doing it directly).
		s.putTask(topic, nil)
		return nil
	}
	if started {
		s.requestRebalance()
	}
	return nil
}

// CallbackTopics snapshots the registered topics (Java getCallbackTable's key
// set), sorted for stable callers.
func (s *MQPullConsumerScheduleService) CallbackTopics() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.callbacks))
	for topic := range s.callbacks {
		out = append(out, topic)
	}
	sort.Strings(out)
	return out
}

// TaskQueues snapshots the queues with a live task chain — the observable
// shape of "the expansion owns exactly this instance's share".
func (s *MQPullConsumerScheduleService) TaskQueues() []common.MessageQueue {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]common.MessageQueue, 0, len(s.tasks))
	for mq := range s.tasks {
		out = append(out, mq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CompareTo(out[j]) < 0 })
	return out
}

// IsStarted reports whether Start succeeded and Shutdown has not run.
func (s *MQPullConsumerScheduleService) IsStarted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// ---------------------------------------------------------------- lifecycle

// Start mirrors Java start():83-96: build the scheduler, install the queue
// listener (replacing any listener on the consumer — Java :90 does the same),
// start the consumer and take the first expansion. The consumer's Start is
// what puts the group on the instance registry; the service's Shutdown hands
// that reference back.
func (s *MQPullConsumerScheduleService) Start() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	consumer := s.consumer
	if consumer == nil {
		s.mu.Unlock()
		return common.ClientError("the pull consumer is not set")
	}
	if s.pullThreadNums <= 0 {
		s.pullThreadNums = DefaultPullScheduleThreadNums
	}
	if s.rebalanceIntervalMillis <= 0 {
		s.rebalanceIntervalMillis = DefaultPullScheduleRebalanceIntervalMillis
	}
	s.stop = make(chan struct{})
	s.sem = make(chan struct{}, s.pullThreadNums)
	s.stopped.Store(false)
	s.started = true
	// Java setMessageQueueListener(messageQueueListener) — the broker's 40
	// push lands on the pull consumer, RebalanceImmediately notifies the
	// listener, and the listener kicks an expansion of the affected topic.
	consumer.SetMessageQueueListener(&pullScheduleQueueListener{service: s})
	group := consumer.ConsumerGroup()
	s.mu.Unlock()

	if err := consumer.Start(); err != nil {
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return err
	}
	common.LogInfof("MQPullConsumerScheduleService start OK, %s %v", group, s.CallbackTopics())
	s.loopWG.Add(1)
	go s.rebalanceLoop()
	return nil
}

// pullScheduleQueueListener is Java's MessageQueueListenerImpl: a queue-set
// change for a registered topic kicks an expansion (the expansion itself
// applies the BROADCASTING/CLUSTERING choice, same switch Java's listener
// branches on).
type pullScheduleQueueListener struct {
	service *MQPullConsumerScheduleService
}

func (l *pullScheduleQueueListener) MessageQueueChanged(topic string, mqAll, mqDivided []common.MessageQueue) {
	l.service.mu.Lock()
	_, registered := l.service.callbacks[topic]
	l.service.mu.Unlock()
	if !registered {
		return
	}
	l.service.requestRebalance()
}

func (s *MQPullConsumerScheduleService) requestRebalance() {
	select {
	case s.rebalanceNow <- struct{}{}:
	default:
	}
}

// rebalanceLoop is the pull-side stand-in for the rebalance service this port
// deliberately does not run inside the consumer (see pull_consumer.go's file
// header): one tick per interval, plus the coalesced immediate kicks. The
// FIRST pass runs immediately — Java schedules a new task at delay 0 in
// putTask, and an expansion that waited 20s would leave every queue idle for
// a whole interval after start.
func (s *MQPullConsumerScheduleService) rebalanceLoop() {
	defer s.loopWG.Done()
	s.doRebalance()
	interval := time.Duration(s.RebalanceIntervalMillis()) * time.Millisecond
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-s.rebalanceNow:
			s.doRebalance()
		case <-timer.C:
			s.doRebalance()
		}
		timer.Reset(interval)
	}
}

// doRebalance expands every registered topic into queue tasks. The route is
// refreshed first (this port's pull consumer reads subscribe info from the
// instance cache; without a refresh the expansion would only ever see the
// route that Start fetched) and the share is computed by the consumer's own
// FetchMessageQueuesInBalance — ONE formula with the push consumer's
// doRebalance, so an expansion and a co-instance's rebalance can never
// disagree about who owns a queue.
func (s *MQPullConsumerScheduleService) doRebalance() {
	if s.stopped.Load() {
		return
	}
	s.mu.Lock()
	consumer := s.consumer
	topics := make([]string, 0, len(s.callbacks))
	for topic := range s.callbacks {
		topics = append(topics, topic)
	}
	s.mu.Unlock()
	if consumer == nil {
		return
	}
	sort.Strings(topics)
	instance := consumer.Instance()
	for _, topic := range topics {
		if s.stopped.Load() {
			return
		}
		if instance != nil {
			if _, err := instance.UpdateTopicRouteInfoFromNameServer(topic, 5000, false); err != nil {
				common.LogDebugf("pull schedule: route refresh failed for %s: %v", topic, err)
			}
		}
		queues, err := consumer.FetchMessageQueuesInBalance(topic)
		if err != nil {
			common.LogWarnf("pull schedule: balance view failed for %s: %v", topic, err)
			continue
		}
		s.putTask(topic, queues)
	}
}

// putTask is Java putTask:61-81: revoke this topic's queues that are no
// longer ours (cancel + drop), then start a task chain for every queue that
// has none — an existing chain keeps its cursor and simply continues.
func (s *MQPullConsumerScheduleService) putTask(topic string, queues []common.MessageQueue) {
	sorted := append([]common.MessageQueue(nil), queues...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].CompareTo(sorted[j]) < 0 })
	set := make(map[common.MessageQueue]struct{}, len(sorted))
	for _, mq := range sorted {
		set[mq] = struct{}{}
	}

	s.mu.Lock()
	for mq, task := range s.tasks {
		if mq.Topic != topic {
			continue
		}
		if _, ok := set[mq]; !ok {
			task.cancelTask()
			delete(s.tasks, mq)
		}
	}
	for _, mq := range sorted {
		if _, ok := s.tasks[mq]; ok {
			continue
		}
		task := &pullQueueTask{mq: mq, service: s, cancel: make(chan struct{})}
		task.cursor.Store(-1)
		s.tasks[mq] = task
		go task.run()
	}
	s.mu.Unlock()
}

// cancelTask is Java setCancelled(true) + the table removal, plus a closed
// channel so a task sleeping between rounds notices immediately.
func (t *pullQueueTask) cancelTask() {
	t.cancelled.Store(true)
	t.cancelOnce.Do(func() { close(t.cancel) })
}

// run is the PullTaskImpl chain: wait out the delay, run one round, take the
// next delay from the channels, repeat until cancelled or the service stops.
// One goroutine per queue is what makes the queue serial "for free" — the
// next round is only ever scheduled after the current one returned, exactly
// like Java re-scheduling the same Runnable from the end of run().
func (t *pullQueueTask) run() {
	s := t.service
	delay := int64(0) // Java :77 schedules a new task with delay 0
	for {
		if !t.waitDelay(delay) {
			return
		}
		if t.cancelled.Load() {
			common.LogWarnf("The Pull Task is cancelled, %v", t.mq)
			return
		}
		// The pullThreadNums gate (Java's fixed-size ScheduledThreadPool):
		// more queues than workers makes the excess wait, never fan out
		// unboundedly.
		select {
		case s.sem <- struct{}{}:
		case <-s.stop:
			return
		case <-t.cancel:
			return
		}
		l := s.lockFor(t.mq)
		l.Lock()
		next := int64(DefaultPullTaskNextDelayMillis)
		// Add before the stopped check: a round that STARTED must be counted
		// in the drain, or Shutdown could return while it is mid-Pull and the
		// consumer underneath it goes away.
		s.runWG.Add(1)
		if s.stopped.Load() {
			s.runWG.Done()
			l.Unlock()
			<-s.sem
			return
		}
		next = s.runRound(t)
		s.runWG.Done()
		l.Unlock()
		<-s.sem
		if t.cancelled.Load() {
			// Java :191 — the revoke landed while doPullTask ran; its result
			// is dropped and the chain dies here.
			common.LogWarnf("The Pull Task is cancelled after doPullTask, %v", t.mq)
			return
		}
		if s.stopped.Load() {
			return
		}
		delay = next
	}
}

func (t *pullQueueTask) waitDelay(millis int64) bool {
	if millis < 0 {
		millis = 0
	}
	timer := time.NewTimer(time.Duration(millis) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-t.service.stop:
		return false
	case <-t.cancel:
		return false
	}
}

// lockFor returns the queue's handover mutex. Entries live for the service's
// lifetime: a queue that goes away and comes back must still share ONE lock
// with a cancelled task that is finishing its last round, and the queue
// universe is bounded by the route tables, so there is nothing to garbage
// collect.
func (s *MQPullConsumerScheduleService) lockFor(mq common.MessageQueue) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.queueLocks[mq]
	if !ok {
		l = &sync.Mutex{}
		s.queueLocks[mq] = l
	}
	return l
}

// runRound is one queue's Pull + callback + delay resolution.
func (s *MQPullConsumerScheduleService) runRound(t *pullQueueTask) int64 {
	s.mu.Lock()
	consumer := s.consumer
	s.mu.Unlock()
	ctx := &PullTaskContext{consumer: consumer, pullInterval: DefaultPullTaskNextDelayMillis}

	result, err := s.pullForTask(t)
	ctx.mu.Lock()
	ctx.pullError = err
	ctx.mu.Unlock()

	s.mu.Lock()
	callback := s.callbacks[t.mq.Topic]
	s.mu.Unlock()
	if callback == nil {
		common.LogWarnf("Pull Task Callback not exist , %s", t.mq.Topic)
		return DefaultPullTaskNextDelayMillis
	}
	returned := s.invokeCallback(callback, t, result, ctx)
	return ctx.resolveNextDelay(returned)
}

// invokeCallback runs the user callback once. Java wraps doPullTask in
// catch (Throwable) and reschedules at 1000ms — a panicking callback must
// not kill the queue's chain nor the goroutine running it.
func (s *MQPullConsumerScheduleService) invokeCallback(callback PullTaskCallback, t *pullQueueTask,
	result *PullResult, ctx *PullTaskContext) (returned int64) {

	panicked := false
	defer func() {
		if panicked {
			returned = pullTaskPanicDelayMillis
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			common.LogErrorf("doPullTask Exception, %v: %v", t.mq, r)
		}
	}()
	returned = callback(t.mq, result, ctx)
	return returned
}

// pullForTask pulls the queue at the task's cursor and advances the cursor
// over the answer. Two invariants:
//   - the cursor follows result.NextBeginOffset for EVERY status (the
//     lite consumer's rule: NO_NEW_MSG parks at maxOffset, NO_MATCHED_MSG
//     steps over the filtered window instead of rescanning it forever, and
//     OFFSET_ILLEGAL carries the broker's corrected position — this single
//     line is the whole correction);
//   - the cursor is parked into the offset store each round, so the periodic
//     persist/Shutdown flush carries it and a re-expansion of the queue
//     resumes from the committed position instead of restarting at max.
//
// A failed pull touches neither.
func (s *MQPullConsumerScheduleService) pullForTask(t *pullQueueTask) (*PullResult, error) {
	s.mu.Lock()
	consumer := s.consumer
	batch := s.pullBatchSize
	s.mu.Unlock()
	if consumer == nil {
		return nil, common.ClientError("the pull consumer is not set")
	}

	offset := t.cursor.Load()
	if offset < 0 {
		resolved, err := s.resolveStartOffset(t.mq)
		if err != nil {
			return nil, err
		}
		t.cursor.Store(resolved)
		offset = resolved
	}
	result, err := consumer.Pull(t.mq, "*", offset, batch)
	if err != nil {
		return nil, err
	}
	t.cursor.Store(result.NextBeginOffset)
	if err := consumer.UpdateConsumeOffset(t.mq, result.NextBeginOffset); err != nil {
		// The store is bookkeeping only (the cursor lives on the task too);
		// a refused update (e.g. a concurrent Shutdown) must not stop rounds.
		common.LogDebugf("pull schedule: park offset failed for %v: %v", t.mq, err)
	}
	return result, nil
}

// resolveStartOffset picks a task's first position (the schedule service's
// stand-in for Java's RebalancePullImpl + OffsetStore combination, which a
// caller-driven pull consumer resolves by hand):
//   - a committed offset (this port: the store, MEMORY_FIRST then the broker)
//     wins — that is what makes a revoked-and-regranted queue resume;
//   - a %RETRY% topic with nothing committed starts at 0, the push consumer's
//     rule: retried messages must not be skipped by a max-offset tail read;
//   - anything else tails the queue (CONSUME_FROM_LAST_OFFSET semantics,
//     GET_MAX_OFFSET straight from the queue's own route).
func (s *MQPullConsumerScheduleService) resolveStartOffset(mq common.MessageQueue) (int64, error) {
	if committed, err := s.consumer.FetchConsumeOffset(mq, false); err == nil && committed >= 0 {
		return committed, nil
	}
	if common.IsRetryTopic(mq.Topic) {
		return 0, nil
	}
	return s.consumer.MaxOffset(mq)
}

// Shutdown mirrors Java shutdown():103-111 in the order Go needs: stop
// accepting new work, cancel every task chain, WAIT for the rounds that are
// already running (their Pulls ride on the consumer's instance — tearing the
// instance down first would fail them mid-flight and lose the last offsets),
// then shut the consumer down. The consumer's Shutdown is also what returns
// the instance reference: with an injected consumer the caller still holds
// their own tenancy; with a self-built one the instance detaches once this
// was its last tenant.
func (s *MQPullConsumerScheduleService) Shutdown() {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	s.started = false
	s.mu.Unlock()

	s.stopped.Store(true)
	s.stopOnce.Do(func() {
		if s.stop != nil {
			close(s.stop)
		}
	})
	// Wake the sleeping chains, then drain the running rounds within the
	// shared shutdown budget (a callback that never returns must not hang
	// the process — same bound the push consumer's drain uses).
	s.mu.Lock()
	for mq, task := range s.tasks {
		task.cancelTask()
		delete(s.tasks, mq)
	}
	s.mu.Unlock()
	s.loopWG.Wait()
	if !waitGroupBudget(&s.runWG, shutdownDrainBudget) {
		common.LogWarnf("pull schedule shutdown drain timed out for group %s — "+
			"callbacks still running are left to finish on their own",
			func() string {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.groupLocked()
			}())
	}

	s.mu.Lock()
	consumer := s.consumer
	s.mu.Unlock()
	if consumer != nil {
		consumer.Shutdown()
	}
}

// waitGroupBudget waits for a WaitGroup with a deadline and reports whether
// it drained in time. (sync.WaitGroup has no Go() that can hang without
// joining, so the waiter runs on a helper goroutine; a group that timed out
// keeps draining on its own, the WaitGroup protocol is intact.)
func waitGroupBudget(wg *sync.WaitGroup, budget time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
