# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque

MIN_ASYNC_SEND_NUM = 10
MIN_ASYNC_SEND_SIZE = 1024 * 1024


class _Pending:
    """A permit request still waiting in the queue (only so the queue head can be stably identified; does not mean the permit is acquired)."""

    __slots__ = ("permits",)

    def __init__(self, permits: int) -> None:
        self.permits = permits


class FairSemaphore:
    pass

    def __init__(self, permits: int) -> None:
        self._cond = threading.Condition()
        self._total = permits
        self._free = permits
        self._queue: Deque[_Pending] = deque()

    def try_acquire(self, permits: int, timeout_millis: int) -> bool:
        pass
        deadline = time.monotonic() + max(timeout_millis, 0) / 1000.0
        request = _Pending(permits)
        with self._cond:
            self._queue.append(request)
            while True:
                if self._queue[0] is request and self._free >= permits:
                    self._queue.popleft()
                    self._free -= permits
                    self._cond.notify_all()  # head changed; the next person may now be satisfied
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # timeout: remove self from the queue, don't block those behind
                    try:
                        self._queue.remove(request)
                    except ValueError:  # pragma: no cover -- only absent from the queue after being granted
                        pass
                    self._cond.notify_all()  # same as above: the blocker is gone
                    return False
                self._cond.wait(remaining)

    def release(self, permits: int) -> None:
        pass
        if permits <= 0:
            return
        with self._cond:
            self._free += permits
            self._cond.notify_all()

    def available_permits(self) -> int:
        with self._cond:
            return self._free

    def set_total_permits(self, total: int) -> None:
        pass
        with self._cond:
            self._free += total - self._total
            self._total = total
            self._cond.notify_all()

    def total_permits(self) -> int:
        with self._cond:
            return self._total
