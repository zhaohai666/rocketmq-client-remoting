# -*- coding: utf-8 -*-

from __future__ import annotations

import collections
import threading
from typing import Any, Callable, Deque, List, Optional, Tuple

from rocketmq_logging import get_logger

logger = get_logger(__name__)


class RejectedExecutionError(RuntimeError):
    pass


class ConsumeExecutor:
    pass

    def __init__(self, core_pool_size: int, maximum_pool_size: int,
                 keep_alive_seconds: float = 60.0,
                 thread_name_prefix: str = "rmq-consume",
                 max_queue_size: int = 0,
                 thread_name_sep: str = "-",
                 thread_index_from: int = 0) -> None:
        core = max(0, int(core_pool_size))
        self._core = core
        self._max = max(core, int(maximum_pool_size))
        self._keep_alive = float(keep_alive_seconds)
        self._prefix = str(thread_name_prefix)
        self._name_sep = str(thread_name_sep)
        self._queue: Deque[Tuple[Callable[..., Any], tuple, dict]] = collections.deque()
        self._max_queue_size = max(0, int(max_queue_size))
        self._lock = threading.Lock()
        self._work_available = threading.Condition(self._lock)
        self._workers = 0
        self._idle = 0             # number of them in a waiting state
        self._threads: List[threading.Thread] = []
        self._seq = int(thread_index_from)
        self._shutdown = False
        self._handler_exceptions = 0

    # ---------------- public API ----------------

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        pass
        with self._lock:
            if self._shutdown:
                raise RejectedExecutionError("ConsumeExecutor has been shut down")
            queue_full = bool(self._max_queue_size) and len(self._queue) >= self._max_queue_size
            can_grow = self._workers < self._max
            if queue_full and not can_grow:
                raise RejectedExecutionError(
                    "ConsumeExecutor queue is full (%d)" % self._max_queue_size)
            self._queue.append((fn, args, kwargs))
            if self._workers < self._core or (queue_full and can_grow) or self._workers == 0:
                self._spawn_locked()
            self._work_available.notify()

    def set_core_pool_size(self, n: int) -> None:
        pass
        n = int(n)
        if n < 0:
            raise ValueError("core pool size must be >= 0")
        with self._lock:
            delta = n - self._core
            self._core = n
            if n > self._max:
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
        pass
        with self._lock:
            return self._workers

    def queued_count(self) -> int:
        pass
        with self._lock:
            return len(self._queue)

    def handler_exception_count(self) -> int:
        """Count of task exceptions that were swallowed (for observation/unit tests only)."""
        return self._handler_exceptions

    def shutdown(self, wait: bool = False, timeout: Optional[float] = None) -> None:
        """Stop accepting tasks; workers still finish the tasks already queued.

        ``wait``: join the worker threads before returning. ``timeout`` (seconds) bounds
        that join; ``None`` (the default) keeps the join unbounded. Idempotent: a second
        call after shutdown still collects and joins the live worker roster.
        """
        pass
        with self._lock:
            if self._shutdown:
                threads = list(self._threads)
            else:
                self._shutdown = True
                self._work_available.notify_all()
                threads = list(self._threads)
        if wait:
            for t in threads:
                t.join(timeout=timeout)

    # ---------------- internal ----------------

    def _spawn_locked(self) -> None:
        """Caller must already hold ``self._lock``."""
        self._workers += 1
        name = "%s%s%d" % (self._prefix, self._name_sep, self._seq)
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
                    # idle timeout: only over-quota threads (> core) exit
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
            except BaseException:
                self._handler_exceptions += 1
                logger.exception("consume executor task raised, worker kept alive")

    def _retire_locked(self) -> None:
        """Caller must already hold ``self._lock``: remove the exited thread from the roster."""
        cur = threading.current_thread()
        for i, t in enumerate(self._threads):
            if t is cur:
                del self._threads[i]
                break
