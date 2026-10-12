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
    assert 100 <= elapsed_ms < 1000, "超时不该提前返回，也不该无限等"
    # 超时的人已经出队，不会把后面的人永久挡在一个已经消失的请求上
    sem.release(1)
    assert sem.try_acquire(1, 0) is True


def test_non_positive_timeout_never_waits():
    sem = FairSemaphore(0)
    assert sem.try_acquire(1, 0) is False
    assert sem.try_acquire(1, -5000) is False


def test_only_the_queue_head_is_granted():
    """公平模式的全部意义：排在别人后面的请求**不许**插队，哪怕许可现在够它。"""
    sem = FairSemaphore(2)
    assert sem.try_acquire(2, 0) is True          # 掏空
    acquired: list = []
    big = threading.Thread(target=lambda: acquired.append(
        ("big", sem.try_acquire(2, 5000))))
    big.start()
    _wait_until(lambda: len(sem._queue) == 1)
    # 现在空闲 0、队首是一个要 2 个的大请求。还 1 个只够小请求 —— 它必须等。
    small = threading.Thread(target=lambda: acquired.append(
        ("small", sem.try_acquire(1, 300))))
    small.start()
    _wait_until(lambda: len(sem._queue) == 2)
    sem.release(1)
    small.join(5.0)
    assert ("small", False) in acquired, "队首还没满足时后来者插队了"
    sem.release(1)                               # 补齐队首要的 2 个
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
    # 队列顺序就是醒来顺序（公平信号量的可观察承诺）
    assert order == ["a", "b"]


def test_set_total_permits_keeps_outstanding_work():
    pass
    sem = FairSemaphore(10)
    assert sem.try_acquire(4, 0) is True          # 在途 4
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
    sem.set_total_permits(5)                     # 扩容
    sem.release(1)                               # 在途的那份还回去
    waiter.join(5.0)
    assert got == [True]


def test_a_granted_head_still_feeds_the_waiter_behind_it():
    """队首拿走许可之后，剩下的空闲如果够后面的人，他必须最终拿到。

    这里锁的是「一次 release 只放一个」这件事不会发生：空闲 6、队首要 5、第二个人要 1，
    两个人都该拿到，且都不该等到超时。（``try_acquire`` 拿到许可后还会再 ``notify_all``
    一次，为的是另一种更刁的排布 —— 第二个人是在那次通知**之后**才排上队的，就没人再喊过他，
    只能靠队首离场时补这一嗓子。那个窗口在本用例里没法稳定复现，所以这里的断言只覆盖
    「都被叫醒」这一半，另一半的回归守卫是下面那个超时退出的用例。）
    """
    sem = FairSemaphore(6)
    assert sem.try_acquire(5, 0) is True           # 在途 5，空闲 1
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
    # 空闲 1 个，够第二个人 —— 但公平模式下它必须等队首先走
    second = threading.Thread(target=_waiter, args=("second", 1, 3000))
    second.start()
    _wait_until(lambda: len(sem._queue) == 2)
    sem.release(5)                                 # 空闲 1 → 6：队首这就够了
    _wait_threads([head, second])
    got = {tag: (ok, ms) for tag, ok, ms in outcome}
    assert got["head"][0] is True
    assert got["second"][0] is True, "队首拿走许可后，第二个人被丢下了（丢唤醒）"
    # 拿到许可不该花掉整个预算：睡到超时说明根本没被叫醒
    assert got["second"][1] < 2000, "second waited %dms" % got["second"][1]
    assert sem.available_permits() == 0            # 6 - 5 - 1


def test_a_timed_out_head_wakes_the_waiter_behind_it():
    """队首**超时退出**也是一次队首换人：它要的许可数超过总容量时，后面的人本来能过。

    这条是丢唤醒的回归守卫（少了离场时的那次 ``notify_all``，second 会一路睡满 3000ms）：
    真机上对应的表现是异步发送白等满预算，再回调一个 ``semaphoreAsyncNum timeout``，
    而许可其实早就空出来了。
    """
    sem = FairSemaphore(3)
    assert sem.try_acquire(3, 0) is True           # 掏空
    outcome: list = []
    lock = threading.Lock()
    began = time.monotonic()

    def _waiter(tag: str, permits: int, budget: int) -> None:
        got = sem.try_acquire(permits, budget)
        with lock:
            outcome.append((tag, got, int((time.monotonic() - began) * 1000)))

    # 队首要 4 个 > 总量 3：它永远拿不到，只能等满自己的 200ms 预算
    head = threading.Thread(target=_waiter, args=("head", 4, 200))
    head.start()
    _wait_until(lambda: len(sem._queue) == 1)
    second = threading.Thread(target=_waiter, args=("second", 1, 3000))
    second.start()
    _wait_until(lambda: len(sem._queue) == 2)
    sem.release(3)                                 # 空闲够 second，但队首是那个贪心的
    _wait_threads([head, second], timeout=5.0)
    got = {tag: (ok, ms) for tag, ok, ms in outcome}
    assert got["head"][0] is False, "要得比总量还多，本该拿不到"
    assert got["second"][0] is True, "队首退出后没被叫醒（丢唤醒）"
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
            raise AssertionError("等待条件超时")
        time.sleep(0.005)
