# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time

from client.backpressure import (MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE,
                                          FairSemaphore)


def _wait_threads(threads, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    for t in threads:
        remaining = max(0.0, deadline - time.monotonic())
        t.join(remaining)


def test_try_acquire_times_out_without_raising():
    pass
    sem = FairSemaphore(1)
    assert sem.try_acquire(1, 0) is True
    began = time.monotonic()
    assert sem.try_acquire(1, 120) is False
    elapsed_ms = int((time.monotonic() - began) * 1000)
    assert 100 <= elapsed_ms < 1000, "a timeout should not return early, nor wait forever"
    # The timed-out party has left the queue and will not permanently block those behind it on a vanished request
    sem.release(1)
    assert sem.try_acquire(1, 0) is True


def test_non_positive_timeout_never_waits():
    sem = FairSemaphore(0)
    assert sem.try_acquire(1, 0) is False
    assert sem.try_acquire(1, -5000) is False


def test_only_the_queue_head_is_granted():
    """The whole point of fair mode: a request queued behind others must **not** jump the queue, even if permits are now sufficient for it."""
    sem = FairSemaphore(2)
    assert sem.try_acquire(2, 0) is True          # drained
    acquired: list = []
    big = threading.Thread(target=lambda: acquired.append(
        ("big", sem.try_acquire(2, 5000))))
    big.start()
    _wait_until(lambda: len(sem._queue) == 1)
    # Now 0 free, and the head is a big request wanting 2. Returning 1 is only enough for the small request — it must wait.
    small = threading.Thread(target=lambda: acquired.append(
        ("small", sem.try_acquire(1, 300))))
    small.start()
    _wait_until(lambda: len(sem._queue) == 2)
    sem.release(1)
    small.join(5.0)
    assert ("small", False) in acquired, "a later arrival jumped the queue before the head was satisfied"
    sem.release(1)                               # top up to the 2 the head wants
    big.join(5.0)
    assert ("big", True) in acquired


def test_release_wakes_the_head_in_order():
    sem = FairSemaphore(1)
    assert sem.try_acquire(1, 0) is True
    order: list = []
    lock = threading.Lock()

    def _waiter(tag: str) -> None:
        got = sem.try_acquire(1, 5000)
        with lock:
            order.append(tag if got else "%s-lost" % tag)

    threads = [threading.Thread(target=_waiter, args=("a",)),
               threading.Thread(target=_waiter, args=("b",))]
    threads[0].start()
    _wait_until(lambda: len(sem._queue) == 1)
    threads[1].start()
    _wait_until(lambda: len(sem._queue) == 2)
    sem.release(1)
    sem.release(1)
    _wait_threads(threads)
    # Queue order is wake-up order (the observable promise of a fair semaphore)
    assert order == ["a", "b"]


def test_set_total_permits_keeps_outstanding_work():
    pass
    sem = FairSemaphore(10)
    assert sem.try_acquire(4, 0) is True          # 4 in flight
    sem.set_total_permits(15)
    assert sem.available_permits() == 11          # 15 - 4
    assert sem.total_permits() == 15
    sem.release(4)
    assert sem.available_permits() == 15


def test_shrinking_below_outstanding_gives_negative_free():
    pass
    sem = FairSemaphore(10)
    assert sem.try_acquire(6, 0) is True
    sem.set_total_permits(2)
    assert sem.available_permits() == -4
    sem.release(6)
    assert sem.available_permits() == 2
    assert sem.try_acquire(2, 0) is True


def test_shrink_wakes_someone_blocked_on_the_old_capacity():
    pass
    sem = FairSemaphore(1)
    assert sem.try_acquire(1, 0) is True
    got = []
    waiter = threading.Thread(target=lambda: got.append(sem.try_acquire(1, 5000)))
    waiter.start()
    _wait_until(lambda: len(sem._queue) == 1)
    sem.set_total_permits(5)                     # expand
    sem.release(1)                               # return the in-flight share
    waiter.join(5.0)
    assert got == [True]


def test_a_granted_head_still_feeds_the_waiter_behind_it():
    """After the head takes the permits, the remaining free capacity must eventually be enough for the person behind it.

    What is locked in here is that "one release only lets one through" does not happen: 6 free, the head wants 5, the second wants 1,
    both should get their permits, and neither should wait until timeout. (``try_acquire`` calls ``notify_all`` once more after
    acquiring, for a different, trickier arrangement — the second person queues up only *after* that notification, so nobody calls out to it again,
    and it can only rely on this extra shout when the head leaves. That window cannot be reproduced reliably in this test,
    so the assertions here only cover the "both woken" half; the regression guard for the other half is the timeout-exit case below.)
    """
    sem = FairSemaphore(6)
    assert sem.try_acquire(5, 0) is True           # 5 in flight, 1 free
    outcome: list = []
    lock = threading.Lock()
    began = time.monotonic()

    def _waiter(tag: str, permits: int, budget: int) -> None:
        got = sem.try_acquire(permits, budget)
        with lock:
            outcome.append((tag, got, int((time.monotonic() - began) * 1000)))

    head = threading.Thread(target=_waiter, args=("head", 5, 3000))
    head.start()
    _wait_until(lambda: len(sem._queue) == 1)
    # 1 free, enough for the second person — but in fair mode it must wait for the head to leave first
    second = threading.Thread(target=_waiter, args=("second", 1, 3000))
    second.start()
    _wait_until(lambda: len(sem._queue) == 2)
    sem.release(5)                                 # free 1 → 6: the head now has enough
    _wait_threads([head, second])
    got = {tag: (ok, ms) for tag, ok, ms in outcome}
    assert got["head"][0] is True
    assert got["second"][0] is True, "after the head took the permits, the second was left behind (lost wakeup)"
    # Getting the permit should not burn the whole budget: sleeping until timeout means it was never woken
    assert got["second"][1] < 2000, "second waited %dms" % got["second"][1]
    assert sem.available_permits() == 0            # 6 - 5 - 1


def test_a_timed_out_head_wakes_the_waiter_behind_it():
    """A head that **times out** is also a head change: when it wants more permits than the total capacity, the person behind it could have passed.

    This is the regression guard for lost wakeups (without that extra ``notify_all`` on exit, second would sleep the full 3000ms):
    the real-world symptom is an async send burning its whole budget for nothing, then a ``semaphoreAsyncNum timeout`` callback,
    even though the permits were freed long ago.
    """
    sem = FairSemaphore(3)
    assert sem.try_acquire(3, 0) is True           # drained
    outcome: list = []
    lock = threading.Lock()
    began = time.monotonic()

    def _waiter(tag: str, permits: int, budget: int) -> None:
        got = sem.try_acquire(permits, budget)
        with lock:
            outcome.append((tag, got, int((time.monotonic() - began) * 1000)))

    # The head wants 4 > total 3: it can never acquire, only burn its full 200ms budget
    head = threading.Thread(target=_waiter, args=("head", 4, 200))
    head.start()
    _wait_until(lambda: len(sem._queue) == 1)
    second = threading.Thread(target=_waiter, args=("second", 1, 3000))
    second.start()
    _wait_until(lambda: len(sem._queue) == 2)
    sem.release(3)                                 # free is enough for second, but the head is the greedy one
    _wait_threads([head, second], timeout=5.0)
    got = {tag: (ok, ms) for tag, ok, ms in outcome}
    assert got["head"][0] is False, "asking for more than the total should never succeed"
    assert got["second"][0] is True, "not woken after the head exited (lost wakeup)"
    assert got["second"][1] < 2000, "second waited %dms" % got["second"][1]


def test_release_beyond_total_is_allowed():
    pass
    sem = FairSemaphore(1)
    sem.release(3)
    assert sem.available_permits() == 4


def test_zero_permits_acquire_is_satisfied_even_when_empty():
    pass
    sem = FairSemaphore(0)
    assert sem.try_acquire(0, 0) is True


def test_backpressure_floors_are_the_documented_minima():
    assert MIN_ASYNC_SEND_NUM == 10
    assert MIN_ASYNC_SEND_SIZE == 1024 * 1024


def _wait_until(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("timed out waiting for condition")
        time.sleep(0.005)
