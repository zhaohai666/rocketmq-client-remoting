# -*- coding: utf-8 -*-
"""消费执行器：Java ``ThreadPoolExecutor`` 的最小等价物。

为什么不用标准库的 ``concurrent.futures.ThreadPoolExecutor``：它**只有 max_workers
一个上限**，没有 corePoolSize 的概念，而 Java 的线程弹性语义恰恰全部挂在 core 上：

* ``new ThreadPoolExecutor(min, max, 60s, new LinkedBlockingQueue<>())``
  —— 队列**无界**，于是「线程数超过 core」这条路径永远不会走（无界队列下
  ``poolSize < corePoolSize`` 才新建线程，否则入队），**真实并发度 == corePoolSize**；
* ``AbstractConsumeMessageService.updateCorePoolSize(n)`` → ``setCorePoolSize(n)``
  —— 运行时改的就是这个并发度，``getCorePoolSize()`` 能读回来。

所以要让 ``update_core_pool_size`` / ``get_core_pool_size`` 有真实语义（而不是把配置
存下来只为打印），必须自己实现 core/max 两档。对齐点：

1. 任务到来时，仅当 ``workers < core`` 才新建线程，否则入队（Java 无界队列行为）。
2. ``> core`` 的线程空闲超过 keep_alive 后退出；``<= core`` 的线程**永不**退出
   （Java ``allowCoreThreadTimeOut`` 默认 false）。
3. ``set_core_pool_size(n)``：core 变大且队列非空时补足 worker（Java
   ``setCorePoolSize`` 里的 ``addWorker(null, false)`` 循环）。
4. 任务抛异常**不杀线程**（Java ``ThreadPoolExecutor`` 会补一个新 worker；
   这里直接在循环内捕获并记日志，效果等价且没有补线程的竞态）。
"""

from __future__ import annotations

import collections
import threading
from typing import Any, Callable, Deque, List, Optional, Tuple

from ..logging import get_logger

logger = get_logger(__name__)


class ConsumeExecutor:
    """core/max 两档线程池（对应 Java ``ThreadPoolExecutor`` + ``LinkedBlockingQueue``）。

    默认参数即 Java ``AbstractConsumeMessageService`` 的构造参数：
    ``core=consumeThreadMin``、``max=consumeThreadMax``、``keepAlive=60s``。
    """

    def __init__(self, core_pool_size: int, maximum_pool_size: int,
                 keep_alive_seconds: float = 60.0,
                 thread_name_prefix: str = "rmq-consume") -> None:
        core = max(0, int(core_pool_size))
        self._core = core
        self._max = max(core, int(maximum_pool_size))
        self._keep_alive = float(keep_alive_seconds)
        self._prefix = str(thread_name_prefix)
        self._queue: Deque[Tuple[Callable[..., Any], tuple, dict]] = collections.deque()
        self._lock = threading.Lock()
        self._work_available = threading.Condition(self._lock)
        self._workers = 0          # 当前存活 worker 数（Java poolSize）
        self._idle = 0             # 其中处于等待状态的数量
        self._threads: List[threading.Thread] = []
        self._seq = 0
        self._shutdown = False
        self._handler_exceptions = 0

    # ---------------- 对外 API ----------------

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """投递任务（Java ``execute``）。线程池已关闭时抛 ``RuntimeError``。"""
        with self._lock:
            if self._shutdown:
                raise RuntimeError("ConsumeExecutor has been shut down")
            self._queue.append((fn, args, kwargs))
            # Java 无界队列语义：只有 poolSize < corePoolSize 才新建线程
            if self._workers < self._core:
                self._spawn_locked()
            elif self._workers == 0:
                # Java ThreadPoolExecutor.execute 的兜底分支：入队成功后若
                # workerCount == 0（core=0 的配置）仍要补一个线程，否则任务永远没人跑。
                self._spawn_locked()
            self._work_available.notify()

    def set_core_pool_size(self, n: int) -> None:
        """对应 Java ``ThreadPoolExecutor.setCorePoolSize``。

        Java 的实现（JDK 8+）::

            int delta = corePoolSize - this.corePoolSize;
            this.corePoolSize = corePoolSize;
            if (workerCountOf(ctl.get()) > corePoolSize) interruptIdleWorkers();
            else if (delta > 0) {
                int k = Math.min(delta, workQueue.size());
                while (k-- > 0 && addWorker(null, true)) {
                    if (workQueue.isEmpty()) break;
                }
            }

        即：core 变大时按 ``min(delta, 队列长度)`` **补齐**线程（不是"补到跟队列一样多"），
        core 变小时只打断空闲线程（这里交给 worker 自己的 keep_alive 退出逻辑处理）。
        """
        n = int(n)
        if n < 0:
            raise ValueError("core pool size must be >= 0")
        with self._lock:
            delta = n - self._core
            self._core = n
            if n > self._max:
                # Java 允许 core > max（会把 max 抬到 core）；这里显式对齐
                self._max = n
            if delta > 0 and not self._shutdown:
                k = min(delta, len(self._queue))
                while k > 0 and self._workers < self._max:
                    self._spawn_locked()
                    k -= 1
                    if not self._queue:
                        break

    def get_core_pool_size(self) -> int:
        with self._lock:
            return self._core

    def get_max_pool_size(self) -> int:
        with self._lock:
            return self._max

    def worker_count(self) -> int:
        """当前存活 worker 数（Java ``getPoolSize``）。仅供观测/单测。"""
        with self._lock:
            return self._workers

    def queued_count(self) -> int:
        """队列中待执行任务数（Java ``getQueue().size()``）。仅供观测/单测。"""
        with self._lock:
            return len(self._queue)

    def handler_exception_count(self) -> int:
        """被吞掉的任务异常计数（仅供观测/单测）。"""
        return self._handler_exceptions

    def shutdown(self, wait: bool = False) -> None:
        """对应 Java ``shutdown()``：停止接收新任务，把手上的队列跑完。

        Java 的 ``shutdown`` **不中断**已提交任务；consumer 停止时走的是
        ``shutdownGracefully``（先 shutdown，超时后 shutdownNow）。这里
        ``wait=True`` 即"优雅等待"，不等价于 ``shutdownNow``（不中断在跑的任务）。
        """
        with self._lock:
            if self._shutdown:
                threads = list(self._threads)
            else:
                self._shutdown = True
                self._work_available.notify_all()
                threads = list(self._threads)
        if wait:
            for t in threads:
                t.join()

    # ---------------- 内部 ----------------

    def _spawn_locked(self) -> None:
        """调用方必须已持有 ``self._lock``。"""
        self._workers += 1
        name = "%s-%d" % (self._prefix, self._seq)
        self._seq += 1
        t = threading.Thread(target=self._run, name=name, daemon=True)
        self._threads.append(t)
        t.start()

    def _run(self) -> None:
        while True:
            task: Optional[Tuple[Callable[..., Any], tuple, dict]] = None
            with self._lock:
                while not self._queue and not self._shutdown:
                    self._idle += 1
                    try:
                        self._work_available.wait(timeout=self._keep_alive)
                    finally:
                        self._idle -= 1
                    if self._queue or self._shutdown:
                        break
                    # 空闲超时：只有超编线程（> core）才退出
                    if self._workers > self._core:
                        self._workers -= 1
                        self._retire_locked()
                        return
                if self._shutdown and not self._queue:
                    self._workers -= 1
                    self._retire_locked()
                    return
                task = self._queue.popleft()
            if task is None:
                continue
            fn, args, kwargs = task
            try:
                fn(*args, **kwargs)
            except BaseException:  # noqa: BLE001 —— 任务异常不能杀 worker（Java 同理）
                self._handler_exceptions += 1
                logger.exception("consume executor task raised, worker kept alive")

    def _retire_locked(self) -> None:
        """调用方必须已持有 ``self._lock``：把退出线程从名册里摘掉。"""
        cur = threading.current_thread()
        for i, t in enumerate(self._threads):
            if t is cur:
                del self._threads[i]
                break
