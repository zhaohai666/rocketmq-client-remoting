# -*- coding: utf-8 -*-
"""客户端基础指标（对应 Java 的 RT/计数统计：send/consume 耗时与成功失败数）。

本地只做最轻量的线程安全计数器与累加器，不引入导出格式；提供 getter 供外部采集。
与 Java 的 ``MQClientAPIImpl`` / ``DefaultMQPushConsumer`` 内部的统计口径一致：
发送维度统计 sendRT、sendCount、sendFailureCount；消费维度统计 consumeRT、
consumeCount、consumeFailureCount。
"""
from __future__ import annotations

import threading
import time
from typing import Optional


class ClientMetrics:
    """线程安全的发送/消费基本指标计数器。"""

    def __init__(self):
        self._lock = threading.Lock()
        # 发送
        self._send_count = 0
        self._send_failure_count = 0
        self._send_rt_sum = 0.0
        self._send_rt_max = 0.0
        self._send_rt_min = 0.0
        self._send_started = False
        # 消费
        self._consume_count = 0
        self._consume_failure_count = 0
        self._consume_rt_sum = 0.0
        self._consume_rt_max = 0.0
        self._consume_rt_min = 0.0
        self._consume_started = False

    # ---------------- 发送 ----------------
    def record_send_start(self) -> float:
        return time.time() * 1000.0

    def record_send_success(self, start_ms: float) -> None:
        rt = time.time() * 1000.0 - start_ms
        with self._lock:
            self._send_count += 1
            self._send_rt_sum += rt
            if not self._send_started or rt > self._send_rt_max:
                self._send_rt_max = rt
            if not self._send_started or rt < self._send_rt_min:
                self._send_rt_min = rt
            self._send_started = True

    def record_send_failure(self, start_ms: float) -> None:
        rt = time.time() * 1000.0 - start_ms
        with self._lock:
            self._send_failure_count += 1
            self._send_rt_sum += rt
            if not self._send_started or rt > self._send_rt_max:
                self._send_rt_max = rt
            if not self._send_started or rt < self._send_rt_min:
                self._send_rt_min = rt
            self._send_started = True

    # ---------------- 消费 ----------------
    def record_consume_start(self) -> float:
        return time.time() * 1000.0

    def record_consume_success(self, start_ms: float) -> None:
        rt = time.time() * 1000.0 - start_ms
        with self._lock:
            self._consume_count += 1
            self._consume_rt_sum += rt
            if not self._consume_started or rt > self._consume_rt_max:
                self._consume_rt_max = rt
            if not self._consume_started or rt < self._consume_rt_min:
                self._consume_rt_min = rt
            self._consume_started = True

    def record_consume_failure(self, start_ms: float) -> None:
        rt = time.time() * 1000.0 - start_ms
        with self._lock:
            self._consume_failure_count += 1
            self._consume_rt_sum += rt
            if not self._consume_started or rt > self._consume_rt_max:
                self._consume_rt_max = rt
            if not self._consume_started or rt < self._consume_rt_min:
                self._consume_rt_min = rt
            self._consume_started = True

    # ---------------- 快照 ----------------
    def snapshot(self) -> dict:
        with self._lock:
            return {
                "sendCount": self._send_count,
                "sendFailureCount": self._send_failure_count,
                "sendRTSum": round(self._send_rt_sum, 3),
                "sendRTMax": round(self._send_rt_max, 3),
                "sendRTMin": round(self._send_rt_min, 3),
                "sendRTAvg": round(self._send_rt_sum / self._send_count, 3) if self._send_count else 0.0,
                "consumeCount": self._consume_count,
                "consumeFailureCount": self._consume_failure_count,
                "consumeRTSum": round(self._consume_rt_sum, 3),
                "consumeRTMax": round(self._consume_rt_max, 3),
                "consumeRTMin": round(self._consume_rt_min, 3),
                "consumeRTAvg": round(self._consume_rt_sum / self._consume_count, 3) if self._consume_count else 0.0,
            }

    def __repr__(self):
        return "ClientMetrics%s" % self.snapshot()


__all__ = ["ClientMetrics"]
