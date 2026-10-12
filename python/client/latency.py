# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import random
import threading
import time
from typing import Callable, Dict, Optional

from common.message import MessageQueue

logger = logging.getLogger(__name__)


class FaultItem:
    pass

    def __init__(self, name: str):
        self.name = name
        self.current_latency: float = 0.0
        self.start_timestamp: float = 0.0
        self.check_stamp: float = 0.0
        self.reachable_flag: bool = True

    def update_not_available_duration(self, not_available_duration: float) -> None:
        if not_available_duration > 0 and time.time() * 1000.0 + not_available_duration > self.start_timestamp:
            self.start_timestamp = time.time() * 1000.0 + not_available_duration

    def is_available(self) -> bool:
        return time.time() * 1000.0 >= self.start_timestamp

    def is_reachable(self) -> bool:
        return self.reachable_flag

    def __repr__(self):
        return "FaultItem{name=%s, latency=%.0f, startTs=%.0f, reachable=%s}" % (
            self.name, self.current_latency, self.start_timestamp, self.reachable_flag)


class LatencyFaultToleranceImpl:
    pass

    def __init__(self, resolver: Optional[Callable[[str], Optional[str]]] = None,
                 service_detector: Optional[Callable[[str, float], bool]] = None,
                 detect_timeout_millis: float = 200,
                 detect_interval_millis: float = 2000):
        self._fault_item_table: Dict[str, FaultItem] = {}
        self._lock = threading.RLock()
        self.resolver = resolver
        self.service_detector = service_detector
        self.detect_timeout_millis = detect_timeout_millis
        self.detect_interval_millis = detect_interval_millis
        self._start_detector_enable = False
        self._detector_thread: Optional[threading.Thread] = None
        self._detector_stop = threading.Event()

    def update_fault_item(self, name: str, current_latency: float,
                          not_available_duration: float, reachable: bool) -> None:
        with self._lock:
            item = self._fault_item_table.get(name)
            if item is None:
                item = FaultItem(name)
                item.current_latency = current_latency
                item.update_not_available_duration(not_available_duration)
                item.reachable_flag = reachable
                self._fault_item_table[name] = item
                return
            item.current_latency = current_latency
            item.update_not_available_duration(not_available_duration)
            item.reachable_flag = reachable

    def is_available(self, name: str) -> bool:
        with self._lock:
            item = self._fault_item_table.get(name)
        if item is not None:
            return item.is_available()
        return True

    def is_reachable(self, name: str) -> bool:
        with self._lock:
            item = self._fault_item_table.get(name)
        if item is not None:
            return item.is_reachable()
        return True

    def remove(self, name: str) -> None:
        with self._lock:
            self._fault_item_table.pop(name, None)

    def pick_one_at_least(self) -> Optional[str]:
        pass
        with self._lock:
            items = list(self._fault_item_table.values())
        random.shuffle(items)
        for item in items:
            if item.reachable_flag:
                return item.name
        return None

    def get_fault_item(self, name: str) -> Optional[FaultItem]:
        with self._lock:
            return self._fault_item_table.get(name)

    def is_start_detector_enable(self) -> bool:
        return self._start_detector_enable

    def set_start_detector_enable(self, enable: bool) -> None:
        self._start_detector_enable = enable

    def set_detect_timeout(self, detect_timeout_millis: float) -> None:
        self.detect_timeout_millis = detect_timeout_millis

    def set_detect_interval(self, detect_interval_millis: float) -> None:
        self.detect_interval_millis = detect_interval_millis

    def detect_by_one_round(self) -> None:
        pass
        now_ms = time.time() * 1000.0
        with self._lock:
            items = list(self._fault_item_table.values())
        for broker_item in items:
            if now_ms - broker_item.check_stamp < 0:
                continue
            broker_item.check_stamp = time.time() * 1000.0 + self.detect_interval_millis
            resolver = self.resolver
            if resolver is None:
                continue
            broker_addr = resolver(broker_item.name)
            if broker_addr is None:
                self.remove(broker_item.name)
                continue
            detector = self.service_detector
            if detector is None:
                continue
            try:
                service_ok = bool(detector(broker_addr, self.detect_timeout_millis))
            except Exception:
                service_ok = False
            if service_ok and not broker_item.reachable_flag:
                logger.info("%s is reachable now, then it can be used.", broker_item.name)
                broker_item.reachable_flag = True

    def start_detector(self, initial_delay: float = 3.0, period: float = 3.0) -> None:
        pass
        with self._lock:
            if self._detector_thread is not None and self._detector_thread.is_alive():
                return
            self._detector_stop.clear()

            def _run() -> None:
                if self._detector_stop.wait(initial_delay):
                    return
                while not self._detector_stop.wait(period):
                    try:
                        if self._start_detector_enable:
                            self.detect_by_one_round()
                    except Exception:
                        logger.warning("Unexpected exception raised while detecting service reachability",
                                       exc_info=True)

            self._detector_thread = threading.Thread(
                target=_run, name="LatencyFaultToleranceScheduledThread", daemon=True)
            self._detector_thread.start()

    def shutdown(self) -> None:
        self._detector_stop.set()


class MQFaultStrategy:
    pass

    LATENCY_MAX = [50, 100, 550, 1800, 3000, 5000, 15000]
    NOT_AVAILABLE_DURATION = [0, 0, 2000, 5000, 6000, 10000, 30000]

    def __init__(self, send_latency_fault_enable: bool = False,
                 resolver: Optional[Callable[[str], Optional[str]]] = None,
                 service_detector: Optional[Callable[[str, float], bool]] = None):
        self._send_latency_fault_enable = send_latency_fault_enable
        self._start_detector_enable = False
        self._latency_fault_tolerance = LatencyFaultToleranceImpl(resolver, service_detector)
        self.latency_max = list(self.LATENCY_MAX)
        self.not_available_duration = list(self.NOT_AVAILABLE_DURATION)

    # ---- 配置 ----
    def is_send_latency_fault_enable(self) -> bool:
        return self._send_latency_fault_enable

    def set_send_latency_fault_enable(self, enable: bool) -> None:
        self._send_latency_fault_enable = enable

    def is_start_detector_enable(self) -> bool:
        pass
        return self._start_detector_enable

    def set_start_detector_enable(self, enable: bool) -> None:
        pass
        self._start_detector_enable = enable
        self._latency_fault_tolerance.set_start_detector_enable(enable)

    def start_detector(self) -> None:
        self._latency_fault_tolerance.start_detector()

    def shutdown(self) -> None:
        self._latency_fault_tolerance.shutdown()

    @property
    def detect_timeout_millis(self) -> float:
        return self._latency_fault_tolerance.detect_timeout_millis

    @property
    def detect_interval_millis(self) -> float:
        return self._latency_fault_tolerance.detect_interval_millis

    # ---- 队列选择 ----
    def _available_filter(self, mq: MessageQueue) -> bool:
        return self._latency_fault_tolerance.is_available(mq.get_broker_name())

    def _reachable_filter(self, mq: MessageQueue) -> bool:
        return self._latency_fault_tolerance.is_reachable(mq.get_broker_name())

    def select_one_message_queue(self, tp_info, last_broker_name: Optional[str],
                                 reset_index: bool = False) -> MessageQueue:
        broker_filter: Callable[[MessageQueue], bool] = (
            lambda mq: last_broker_name is None or mq.get_broker_name() != last_broker_name)
        if self._send_latency_fault_enable:
            if reset_index:
                tp_info.reset_index()
            mq = tp_info.select_one_message_queue(self._available_filter, broker_filter)
            if mq is not None:
                return mq
            mq = tp_info.select_one_message_queue(self._reachable_filter, broker_filter)
            if mq is not None:
                return mq
            return tp_info.select_one_message_queue()
        mq = tp_info.select_one_message_queue(broker_filter)
        if mq is not None:
            return mq
        return tp_info.select_one_message_queue()

    # ---- 故障记录 ----
    def update_fault_item(self, broker_name: str, current_latency: float,
                          isolation: bool, reachable: bool) -> None:
        if not self._send_latency_fault_enable:
            return
        latency = 10000 if isolation else current_latency
        duration = self._compute_not_available_duration(latency)
        self._latency_fault_tolerance.update_fault_item(broker_name, current_latency, duration, reachable)

    def _compute_not_available_duration(self, current_latency: float) -> float:
        for i in range(len(self.latency_max) - 1, -1, -1):
            if current_latency >= self.latency_max[i]:
                return self.not_available_duration[i]
        return 0

    @property
    def latency_fault_tolerance(self) -> LatencyFaultToleranceImpl:
        return self._latency_fault_tolerance


__all__ = ["FaultItem", "LatencyFaultToleranceImpl", "MQFaultStrategy"]
