# -*- coding: utf-8 -*-
"""异步发送背压（对应 Java ``DefaultMQProducer:169/175/181`` 的三个配置、
``DefaultMQProducerImpl:122-153`` 的两个公平信号量、``:635-682`` 的
``executeAsyncMessageSend`` 闸门和 ``:577-633`` 的 ``BackpressureSendCallBack``）。

Java 的开关默认是**关**的。开了之后，异步发送在把任务投进 ``AsyncSenderExecutor``
**之前**（也就是在调用方线程上）先按两个维度限流：

* ``semaphoreAsyncSendNum`` —— 在途**条数**，``backPressureForAsyncSendNum`` 默认 1024，
  地板 10；
* ``semaphoreAsyncSendSize`` —— 在途**字节数**，``backPressureForAsyncSendSize`` 默认 100M，
  地板 1M，一笔消息扣掉 ``body.length`` 个许可（body 为空按 1 算）。

两个许可都用**整个剩余预算**去等，等不到就直接回调
``RemotingTooMuchRequestException("send message tryAcquire semaphoreAsyncNum timeout")``
（第二个是 ``...semaphoreAsyncSize timeout``），一次请求都不会发出去。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque

# Java DefaultMQProducerImpl:141-153 的两个地板值
MIN_ASYNC_SEND_NUM = 10
MIN_ASYNC_SEND_SIZE = 1024 * 1024


class _Pending:
    """一个还在排队的许可申请（只为让队首能被稳定识别，不代表已拿到许可）。"""

    __slots__ = ("permits",)

    def __init__(self, permits: int) -> None:
        self.permits = permits


class FairSemaphore:
    """对应 Java ``new Semaphore(permits, true)``：**公平**的计数信号量。

    公平是这套背压的全部意义所在 —— 非公平的话一个持续涌入的生产者能让早到的请求
    无限插队，Java 正是为了不插队才显式传 ``true``。所以这里只有**队首**能拿许可，
    后面的请求即使空闲许可够它也不许插队（Java 公平模式下 ``tryAcquire(permits,…)``
    对多条待批请求同样只看队首）。

    另外支持 Java 没有直接提供的一件事：``set_total_permits`` 在**同一个对象**上平移总量。
    Java 的运行时改容量（``DefaultMQProducer:1383-1391`` → ``setSemaphoreAsyncSendNum``）是
    ``new Semaphore(num - acquired)`` **换掉整个对象**，靠 ``ReadWriteCASLock`` 的写锁保证
    换的瞬间没有线程正阻塞在旧对象上（否则那些等待者永远不会被新对象叫醒，只能等到自己
    超时）。本实现把所有状态放在同一把锁里改，于是：

    * 不需要那层自旋读写锁 —— 改容量与拿/还许可在对象内部天然互斥；
    * 改容量不会把等待者丢在旧对象上，改完它们会带着新容量继续等。

    可观察结果与 Java 一致：在途份数原样保留、空闲许可 = 新总量 - 在途份数
    （Java 的原测试断言的正是这个和，见 ``DefaultMQProducerTest:593-595``）。
    """

    def __init__(self, permits: int) -> None:
        self._cond = threading.Condition()
        self._total = permits
        self._free = permits
        self._queue: Deque[_Pending] = deque()

    def try_acquire(self, permits: int, timeout_millis: int) -> bool:
        """对应 Java ``tryAcquire(permits, timeout, MILLIS)``：拿不到就返回 ``False``，
        不抛异常（Java 也只有被 interrupt 才抛）。

        ⚠ 拿到许可和**放弃排队**这两个出口都必须再叫醒一次：公平模式下只有队首能拿，
        队首一换人，后面的申请就可能从「轮不到我」变成「该我了」，而它的 ``permits`` 数量
        未必被前一个人的动作影响（队首要 5 个、空闲 6 个时，队首拿走 5 个后剩下 1 个，
        正好够排在第二的那 1 个 —— 但 ``release`` 早就跑完了，没人为它叫醒）。
        少叫醒这一次，那个人就会一直睡到自己的超时：真机上是 5 秒死等，不是丢一条消息。
        """
        deadline = time.monotonic() + max(timeout_millis, 0) / 1000.0
        request = _Pending(permits)
        with self._cond:
            self._queue.append(request)
            while True:
                if self._queue[0] is request and self._free >= permits:
                    self._queue.popleft()
                    self._free -= permits
                    self._cond.notify_all()  # 队首换人，下一个人可能就够了
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # 超时：把自己从队列里摘掉，别挡后面的人
                    try:
                        self._queue.remove(request)
                    except ValueError:  # pragma: no cover —— 只有被 grant 后才会不在队里
                        pass
                    self._cond.notify_all()  # 同上：挡路的人走了
                    return False
                self._cond.wait(remaining)

    def release(self, permits: int) -> None:
        """对应 Java ``release(permits)``：**可以超过总量**（Java 同样不做校验），
        所以一次改小容量的窗口里多还几次不会丢计数。"""
        if permits <= 0:
            return
        with self._cond:
            self._free += permits
            self._cond.notify_all()

    def available_permits(self) -> int:
        with self._cond:
            return self._free

    def set_total_permits(self, total: int) -> None:
        """把总量平移到 ``total``，在途份数原样保留（可能算出负的空闲许可 ——
        Java ``new Semaphore(负数)`` 同样接受，归还许可会把它拉回正数）。"""
        with self._cond:
            self._free += total - self._total
            self._total = total
            self._cond.notify_all()

    def total_permits(self) -> int:
        with self._cond:
            return self._total
