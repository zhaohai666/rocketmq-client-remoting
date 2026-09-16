# -*- coding: utf-8 -*-
"""发送延迟故障容错（对应 org.apache.rocketmq.client.latency.*）。

实现 MQFaultStrategy + LatencyFaultToleranceImpl（带 FaultItem）：追踪每个 broker 的
发送延迟，延迟过高或发生异常时**隔离**一段时间（不分配给新消息），默认关闭。

与 Java 关键点逐条对齐：
  * latencyMax / notAvailableDuration 两套阈值表；
  * updateFaultItem 的 notAvailableDuration 取 ``computeNotAvailableDuration``，
    隔离（异常）场景固定按 10000ms 算档位；
  * FaultItem.isAvailable() = now >= startTimestamp（隔离期未过则不可用）；
  * LatencyFaultToleranceImpl.isAvailable/isReachable 在没有记录时返回 true，
    即"从未出过问题的 broker 默认可用/可达"。

默认 sendLatencyFaultEnable=false，与 Java 一致；开启后才会记录与生效。
"""
from __future__ import annotations

import threading
import time
from typing import Callable, Dict, Optional

from ..common.message import MessageQueue


class FaultItem:
    """单个 broker 的故障项（对应 Java LatencyFaultToleranceImpl.FaultItem）。"""

    def __init__(self, name: str):
        self.name = name
        self.current_latency: float = 0.0
        self.start_timestamp: float = 0.0
        self.check_stamp: float = 0.0
        self.reachable_flag: bool = True

    def update_not_available_duration(self, not_available_duration: float) -> None:
        # Java：only when now + dur > startTimestamp 才更新（保持最长隔离期）
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
    """对应 Java client.latency.LatencyFaultToleranceImpl（简化为纯内存版，无探测线程）。

    省略 Java 的"后台可达性探测线程"（startDetector），因为探测依赖真实 broker 连接；
    本地保留 reachableFlag 语义：updateFaultItem(..., reachable) 时写 reachableFlag。
    """

    def __init__(self):
        self._fault_item_table: Dict[str, FaultItem] = {}
        self._lock = threading.RLock()

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

    def get_fault_item(self, name: str) -> Optional[FaultItem]:
        with self._lock:
            return self._fault_item_table.get(name)


class MQFaultStrategy:
    """对应 Java client.latency.MQFaultStrategy。

    仅当 ``send_latency_fault_enable`` 为 True 时，发送选队列阶段会：
      1) 优先选 available（隔离期已过）的 broker；
      2) 否则选 reachable 的 broker；
      3) 否则退化为普通轮询。
    发送结果/异常会回调 ``update_fault_item`` 写延迟与隔离信息。
    """

    LATENCY_MAX = [50, 100, 550, 1800, 3000, 5000, 15000]
    NOT_AVAILABLE_DURATION = [0, 0, 2000, 5000, 6000, 10000, 30000]

    def __init__(self, send_latency_fault_enable: bool = False):
        self._send_latency_fault_enable = send_latency_fault_enable
        self._latency_fault_tolerance = LatencyFaultToleranceImpl()
        self.latency_max = list(self.LATENCY_MAX)
        self.not_available_duration = list(self.NOT_AVAILABLE_DURATION)

    # ---- 配置 ----
    def is_send_latency_fault_enable(self) -> bool:
        return self._send_latency_fault_enable

    def set_send_latency_fault_enable(self, enable: bool) -> None:
        self._send_latency_fault_enable = enable

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
