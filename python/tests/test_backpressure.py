# -*- coding: utf-8 -*-
"""``rocketmq.client.backpressure`` 的单测 —— 只测那个公平计数信号量本身。

对端是 Java ``new Semaphore(permits, true)``：异步发送背压整套语义都压在它身上，
所以这里盯的是**公平**（只有队首能拿）与**运行时改容量**（在途份数原样保留）两件事，
而不是「能不能拿到许可」这种换谁都一样的部分。

生产者那一侧怎么用它（拿不到就回调、还几次、队满就地跑）在
``tests/test_producer_async.py`` 的「异步发送背压」一节。
"""
from __future__ import annotations

import threading
import time

from rocketmq.client.backpressure import (MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE,
                                          FairSemaphore)


def _wait_threads(threads, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    for t in threads:
        remaining = max(0.0, deadline - time.monotonic())
        t.join(remaining)


def test_try_acquire_times_out_without_raising():
    """Java ``tryAcquire(permits, timeout, MILLIS)`` 超时返回 false，只有 interrupt 才抛。"""
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
    """改总量 = 「空闲 = 新总量 - 在途」，与 Java ``new Semaphore(num - acquired)`` 同解。"""
    sem = FairSemaphore(10)
    assert sem.try_acquire(4, 0) is True          # 在途 4
    sem.set_total_permits(15)
    assert sem.available_permits() == 11          # 15 - 4
    assert sem.total_permits() == 15
    sem.release(4)
    assert sem.available_permits() == 15


def test_shrinking_below_outstanding_gives_negative_free():
    """Java 的 ``new Semaphore(负数)`` 是合法的，归还许可会把它拉回正数 —— 这里同样接受。"""
    sem = FairSemaphore(10)
    assert sem.try_acquire(6, 0) is True
    sem.set_total_permits(2)
    assert sem.available_permits() == -4
    sem.release(6)
    assert sem.available_permits() == 2
    assert sem.try_acquire(2, 0) is True


def test_shrink_wakes_someone_blocked_on_the_old_capacity():
    """这是本实现换掉 Java「整体换一个 Semaphore 对象」写法的理由：
    调大容量时，正堵在等待队列里的人会被叫醒；Java 那些等待者挂在旧对象上，
    只能等到自己的超时（Java 靠写锁排他避开了同一时刻的换对象，但被丢下的人照旧白等）。
    """
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


def test_release_beyond_total_is_allowed():
    """Java ``release()`` 不校验是否超过总量（信号量可以被"无中生有"地放大）。"""
    sem = FairSemaphore(1)
    sem.release(3)
    assert sem.available_permits() == 4


def test_zero_permits_acquire_is_satisfied_even_when_empty():
    """Java ``tryAcquire(0, …)`` 恒真；批量消息为空时我们按 1 算，所以这里只锁住基元语义。"""
    sem = FairSemaphore(0)
    assert sem.try_acquire(0, 0) is True


def test_floors_match_java():
    assert MIN_ASYNC_SEND_NUM == 10
    assert MIN_ASYNC_SEND_SIZE == 1024 * 1024


def _wait_until(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("等待条件超时")
        time.sleep(0.005)
