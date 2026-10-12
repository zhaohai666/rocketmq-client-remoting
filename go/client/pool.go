// Bounded worker pools shared by the async sender, the callback executor and
// the transaction check executor.
package client

import (
	"sync"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

var errPoolRejected = common.ClientError("executor rejected")

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

func (p *boundedPool) SubmitOrRunInline(task func()) {
	if err := p.Submit(task); err != nil {
		task()
	}
}

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
