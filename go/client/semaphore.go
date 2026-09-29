// Fair back-pressure semaphore for the async send path (Java
// DefaultMQProducerImpl:122-153, `new Semaphore(permits, true)`).
package client

import (
	"sync"
	"time"
)

// fairSemaphore is Java's `new Semaphore(permits, true)` — a FAIR counting
// semaphore. Fairness is the whole point of the async back-pressure: without it
// a producer that keeps submitting can overtake older waiters indefinitely,
// which is exactly why Java passes `true` explicitly. So only the HEAD of the
// queue may be granted; a later request is not allowed to cut in even when
// enough permits happen to be free (Java's fair `tryAcquire(permits, …)` also
// only inspects the head for a multi-permit request).
//
// It also does something Java does not offer directly: setTotalPermits shifts
// the total on the SAME object. Java resizes at runtime by replacing the whole
// Semaphore (`new Semaphore(num - acquired), DefaultMQProducer:1383-1391`),
// leaning on its ReadWriteCASLock so that no thread is blocked on the old
// object at the instant of the swap — otherwise those waiters would never be
// woken by the new object and would just sit out their own timeout. Keeping all
// state behind one mutex removes the need for that read-write lock:
//
//   - resizing and acquire/release are mutually exclusive inside the object;
//   - resizing never strands a waiter on a stale object: it carries on waiting
//     with the new total.
//
// The observable outcome matches Java — in-flight permits are preserved and the
// free permits become newTotal - inFlight, which is precisely the sum Java's own
// DefaultMQProducerTest:593-595 asserts.
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

// tryAcquire is Java `tryAcquire(permits, timeout, MILLIS)`: it reports failure
// instead of raising (Java only throws when the thread is interrupted).
//
// ⚠ BOTH exits — taking the permits and giving up — must wake the queue again.
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
		return true
	case <-timer.C:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if w.granted {
		// Raced with a grant: the permits are already ours. Reporting failure
		// here would leak them — every other bookkeeping path trusts that a
		// granted waiter consumes what it was given.
		return true
	}
	s.removeWaiterLocked(w)
	s.notifyLocked() // the queue lost a member; the next request may now fit
	return false
}

// release is Java `release(permits)`. It may exceed the total (Java does not
// validate either), so a few extra releases inside a shrink window cannot lose
// count.
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

// setTotalPermits shifts the total to `total`, preserving the in-flight permits.
// The free count may go NEGATIVE — Java's `new Semaphore(negative)` accepts that
// too, and returning the outstanding permits pulls it back positive.
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

// waitingCount is Java Semaphore#getQueueLength. Fairness can only be argued
// from "who is queued where", so without this the tests could do nothing but
// guess the order with sleeps.
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
