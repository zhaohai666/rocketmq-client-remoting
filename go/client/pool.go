// Bounded worker pools shared by the async sender, the callback executor and
// the transaction check executor.
package client

import (
	"sync"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// errPoolRejected is Java RejectedExecutionException. Every RocketMQ client
// pool is a ThreadPoolExecutor with a BOUNDED LinkedBlockingQueue, so a busy
// client rejects instead of growing without limit — and the three callers each
// react differently (see their doc comments): the async sender either runs the
// task inline or fails the send with "executor rejected", while the callback
// executor falls back to running in the calling goroutine.
var errPoolRejected = common.ClientError("executor rejected")

// boundedPool is the ThreadPoolExecutor shape the RocketMQ client pools rely
// on: a fixed worker count plus a bounded queue, with rejection once the queue
// is full. Go needs no core/max distinction — the Java pools all pass
// core == max, which is exactly "N workers, extra tasks wait".
//
// Unlike ThreadPoolExecutor this pool does NOT block on submit: Submit is the
// non-blocking `offer()` branch, so a full queue is reported immediately. That
// is what makes it usable as a back-pressure signal.
type boundedPool struct {
	name string

	mu     sync.Mutex
	closed bool
	tasks  chan func()

	wg sync.WaitGroup
}

// newBoundedPool starts `workers` goroutines draining a queue of `capacity`.
// Both must be positive; a non-positive capacity is clamped to 1 so the pool
// can never be accidentally unbounded.
func newBoundedPool(name string, workers, capacity int) *boundedPool {
	if workers < 1 {
		workers = 1
	}
	if capacity < 1 {
		capacity = 1
	}
	p := &boundedPool{
		name:  name,
		tasks: make(chan func(), capacity),
	}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *boundedPool) worker() {
	defer p.wg.Done()
	for task := range p.tasks {
		p.run(task)
	}
}

// run isolates one task. A panicking task must NOT take the worker down:
// ThreadPoolExecutor swallows the Throwable and keeps the worker alive, so a
// crashing callback cannot silently shrink the pool to zero workers.
func (p *boundedPool) run(task func()) {
	defer func() {
		if r := recover(); r != nil {
			common.LogErrorf("%s: task panicked: %v", p.name, r)
		}
	}()
	task()
}

// Submit enqueues without blocking, returning errPoolRejected when the queue is
// full or the pool is already shut down.
func (p *boundedPool) Submit(task func()) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errPoolRejected
	}
	select {
	case p.tasks <- task:
		return nil
	default:
		return errPoolRejected
	}
}

// SubmitOrRunInline enqueues, running the task in the CALLER's goroutine when
// the queue is full. Java's async-send path does exactly this once back
// pressure is enabled (DefaultMQProducerImpl:675-681): the permits were already
// taken, so the send must run somewhere or the capacity leaks until timeout.
func (p *boundedPool) SubmitOrRunInline(task func()) {
	if err := p.Submit(task); err != nil {
		task()
	}
}

// QueueCapacity reports the bounded queue size (Java getQueue().remainingCapacity()
// + size, i.e. the fixed capacity). Used by tests and by callers that want to
// size their own buffers.
func (p *boundedPool) QueueCapacity() int { return cap(p.tasks) }

// Shutdown stops accepting new tasks and lets the already-queued ones finish,
// exactly like ExecutorService.shutdown() (as opposed to shutdownNow).
func (p *boundedPool) Shutdown() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.tasks)
	p.mu.Unlock()
	p.wg.Wait()
}
