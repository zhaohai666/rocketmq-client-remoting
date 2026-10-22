package client

import (
	"sync"
	"time"
)

//   - resizing and acquire/release are mutually exclusive inside the object;
//   - resizing never strands a waiter on a stale object: it carries on waiting
//     with the new total.
type fairSemaphore struct {
	mu      sync.Mutex
	total   int
	free    int
	waiters []*semaphoreWaiter
}

// semaphoreWaiter is one queued request. ready is buffered so a grant can be
// handed over without the granting goroutine ever blocking.
type semaphoreWaiter struct {
	permits int
	ready   chan struct{}
	granted bool
}

func newFairSemaphore(permits int) *fairSemaphore {
	return &fairSemaphore{total: permits, free: permits}
}

// ⚠ EVERY exit — taking the permits, giving up, and handing back a grant that
// only arrived after the caller's budget — must wake the queue again.
// Under fairness only the head may proceed, so when the head changes the
// request behind it can go from "not my turn" to "my turn" without the permit
// COUNT changing for it: with the head asking for 5 out of 6 free, the head
// takes 5 and leaves 1, which is exactly enough for whatever was second in
// line — but release() has long since finished, so nobody else would wake it.
// Miss that wake-up and the request sleeps until its own timeout: on a real
// cluster that is a hard 5-second stall, not a lost message.
func (s *fairSemaphore) tryAcquire(permits int, timeoutMillis int64) bool {
	s.mu.Lock()
	if len(s.waiters) == 0 && s.free >= permits {
		s.free -= permits
		s.mu.Unlock()
		return true
	}
	w := &semaphoreWaiter{permits: permits, ready: make(chan struct{}, 1)}
	s.waiters = append(s.waiters, w)
	// Invariant: after any notifyLocked the head is NOT satisfiable, so this is
	// a no-op. It is kept because it is cheap and it makes "a satisfiable
	// request never sleeps" true by construction rather than by argument.
	s.notifyLocked()
	if w.granted {
		s.mu.Unlock()
		return true
	}
	s.mu.Unlock()

	if timeoutMillis < 0 {
		timeoutMillis = 0
	}
	timer := time.NewTimer(time.Duration(timeoutMillis) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-w.ready:
	case <-timer.C:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !w.granted {
		s.removeWaiterLocked(w)
		s.notifyLocked() // the queue lost a member; the next request may now fit
		return false
	}
	// A grant is paid for out of the free count the moment it lands in w.ready's
	// buffer, which can be long before this goroutine gets to read it. Stop()
	// reports false only once the budget has run out, so a wake-up that is read
	// after the caller's own deadline is an expired caller, not a granted one:
	// give the permits back and let the queue pay someone still inside their
	// budget with them. Keeping them would report a success the caller then has
	// to unwind — and the async sender, which trusts this answer, would send past
	// its own timeout.
	if !timer.Stop() {
		s.free += w.permits
		s.notifyLocked() // the permits are back and this waiter is gone: re-decide
		return false
	}
	return true
}

func (s *fairSemaphore) release(permits int) {
	if permits <= 0 {
		return
	}
	s.mu.Lock()
	s.free += permits
	s.notifyLocked()
	s.mu.Unlock()
}

func (s *fairSemaphore) availablePermits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.free
}

func (s *fairSemaphore) setTotalPermits(total int) {
	s.mu.Lock()
	s.free += total - s.total
	s.total = total
	s.notifyLocked()
	s.mu.Unlock()
}

func (s *fairSemaphore) totalPermits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

func (s *fairSemaphore) waitingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiters)
}

// notifyLocked hands permits to as many queued requests as possible, head
// first. Callers must hold s.mu.
func (s *fairSemaphore) notifyLocked() {
	for len(s.waiters) > 0 {
		head := s.waiters[0]
		if s.free < head.permits {
			return
		}
		s.waiters = s.waiters[1:]
		s.free -= head.permits
		head.granted = true
		head.ready <- struct{}{}
	}
}

// removeWaiterLocked drops one waiter that gave up, wherever it sits in the
// queue (a request in the middle gives up just as often as the head does).
func (s *fairSemaphore) removeWaiterLocked(target *semaphoreWaiter) {
	for i, w := range s.waiters {
		if w == target {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}
