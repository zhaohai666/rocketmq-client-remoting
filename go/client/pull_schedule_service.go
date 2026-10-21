// The rules that make it behave:
package client

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

const (
	// DefaultPullScheduleThreadNums is MQPullConsumerScheduleService.pullThread
	// Nums:46 — the pool that runs PullTaskImpls.
	DefaultPullScheduleThreadNums = 20
	// DefaultPullTaskNextDelayMillis is PullTaskContext.pullNextDelayTimeMillis
	// :24 — the gap between two rounds when the callback does not choose.
	DefaultPullTaskNextDelayMillis             = int64(200)
	DefaultPullScheduleRebalanceIntervalMillis = int64(20000)
	pullTaskPanicDelayMillis                   = int64(1000)
)

type PullTaskCallback func(mq common.MessageQueue, result *PullResult, context *PullTaskContext) int64

type PullTaskContext struct {
	mu           sync.Mutex
	consumer     *DefaultMQPullConsumer
	pullInterval int64
	pullLater    *int64
	pullError    error
}

func (x *PullTaskContext) PullConsumer() *DefaultMQPullConsumer { return x.consumer }

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
	stopOnce *sync.Once     // replaced per Start, so a restarted service can stop again
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

type pullQueueTask struct {
	mq         common.MessageQueue
	service    *MQPullConsumerScheduleService
	cancel     chan struct{}
	cancelOnce sync.Once
	cancelled  atomic.Bool
	// stop and sem are THIS task's life: putTask captures them under s.mu, and
	// Start replaces the service's copies. A task that outlives Shutdown (parked
	// in its delay, or draining past the bounded wait) therefore keeps watching
	// the channel Shutdown closed, so it exits — and it never reads the fields a
	// later Start writes.
	stop chan struct{}
	sem  chan struct{}
	// cursor is the queue's pull position; -1 means "not resolved yet" and
	// 	the next round reads it from the offset store / broker.
	cursor atomic.Int64
}

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
		stopOnce:                new(sync.Once),
		rebalanceNow:            make(chan struct{}, 1),
	}
}

// ---------------------------------------------------------------- configuration

func (s *MQPullConsumerScheduleService) PullConsumer() *DefaultMQPullConsumer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumer
}

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
	// A fresh Once per Start: Shutdown consumes it, and a service that is
	// started AGAIN after a Shutdown must be able to close its NEW stop
	// channel — with a spent Once the second Shutdown would never wake the
	// second rebalance loop and loopWG.Wait would hang forever.
	s.stopOnce = new(sync.Once)
	s.sem = make(chan struct{}, s.pullThreadNums)
	s.stopped.Store(false)
	s.started = true
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
			if _, err := instance.UpdateTopicRouteInfoFromNameServer(topic, 0, false); err != nil {
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
		task := &pullQueueTask{mq: mq, service: s, cancel: make(chan struct{}),
			stop: s.stop, sem: s.sem}
		task.cursor.Store(-1)
		s.tasks[mq] = task
		go task.run()
	}
	s.mu.Unlock()
}

func (t *pullQueueTask) cancelTask() {
	t.cancelled.Store(true)
	t.cancelOnce.Do(func() { close(t.cancel) })
}

func (t *pullQueueTask) run() {
	s := t.service
	delay := int64(0)
	for {
		if !t.waitDelay(delay) {
			return
		}
		if t.cancelled.Load() {
			common.LogWarnf("The Pull Task is cancelled, %v", t.mq)
			return
		}
		select {
		case t.sem <- struct{}{}:
		case <-t.stop:
			return
		case <-t.cancel:
			return
		}
		l := s.lockFor(t.mq)
		l.Lock()
		next := int64(DefaultPullTaskNextDelayMillis)
		// The Add must sit under s.mu, fused with the stopped check: Shutdown
		// stores stopped, then takes s.mu for the cancel sweep, then waits on
		// runWG. An Add that acquires s.mu after the sweep sees stopped and
		// bails without touching the WaitGroup; an Add before the sweep
		// happens-before the Wait. An Add racing a Wait on a zero counter is
		// sync.WaitGroup misuse and can panic the whole process — and a round
		// that STARTED is still counted in the drain, so Shutdown can never
		// return while it is mid-Pull and the consumer underneath goes away.
		s.mu.Lock()
		if s.stopped.Load() {
			s.mu.Unlock()
			l.Unlock()
			<-t.sem
			return
		}
		s.runWG.Add(1)
		s.mu.Unlock()
		next = s.runRound(t)
		s.runWG.Done()
		l.Unlock()
		<-t.sem
		if t.cancelled.Load() {
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
	case <-t.stop:
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

func (s *MQPullConsumerScheduleService) resolveStartOffset(mq common.MessageQueue) (int64, error) {
	if committed, err := s.consumer.FetchConsumeOffset(mq, false); err == nil && committed >= 0 {
		return committed, nil
	}
	if common.IsRetryTopic(mq.Topic) {
		return 0, nil
	}
	return s.consumer.MaxOffset(mq)
}

func (s *MQPullConsumerScheduleService) Shutdown() {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	s.started = false
	stopOnce := s.stopOnce
	s.mu.Unlock()

	s.stopped.Store(true)
	if stopOnce != nil {
		stopOnce.Do(func() {
			s.mu.Lock()
			stop := s.stop
			s.mu.Unlock()
			if stop != nil {
				close(stop)
			}
		})
	}
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
