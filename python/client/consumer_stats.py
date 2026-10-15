# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque, Dict, Optional, Tuple

from rocketmq_logging import get_logger
from remoting.protocol.body import ConsumeStatus

logger = get_logger(__name__)

SAMPLING_INTERVAL_SECONDS = 10.0
HOUR_SAMPLING_INTERVAL_SECONDS = 600.0
MINUTE_LIST_MAX = 60
HOUR_LIST_MAX = 60


class StatsSnapshot:
    pass

    __slots__ = ("sum", "tps", "avgpt", "times")

    def __init__(self) -> None:
        self.sum: int = 0
        self.tps: float = 0.0
        self.avgpt: float = 0.0
        self.times: int = 0

    def __repr__(self) -> str:  # pragma: no cover - for debugging
        return "StatsSnapshot(sum=%d, tps=%.2f, avgpt=%.2f, times=%d)" % (
            self.sum, self.tps, self.avgpt, self.times)


def compute_stats_data(cs_list) -> StatsSnapshot:
    pass
    ss = StatsSnapshot()
    if not cs_list:
        return ss
    first = cs_list[0]
    last = cs_list[-1]
    ss.sum = int(last[1] - first[1])
    span_ms = last[0] - first[0]
    if span_ms > 0:
        ss.tps = (ss.sum * 1000.0) / span_ms
    times_diff = int(last[2] - first[2])
    ss.times = times_diff
    if times_diff > 0:
        ss.avgpt = (ss.sum * 1.0) / times_diff
    return ss


class StatsItem:
    """Single-item statistics: cumulative value/times + minute/hour two-level sampling chains."""

    def __init__(self, stats_name: str, stats_key: str) -> None:
        self.stats_name = stats_name
        self.stats_key = stats_key
        self._lock = threading.Lock()
        self._value = 0
        self._times = 0
        # element = (timestamp_ms, cumulative value, cumulative times)
        self._minute: Deque[Tuple[int, int, int]] = deque()
        self._hour: Deque[Tuple[int, int, int]] = deque()

    def add_value(self, inc_value: int, inc_times: int) -> None:
        with self._lock:
            self._value += int(inc_value)
            self._times += int(inc_times)

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    @property
    def times(self) -> int:
        with self._lock:
            return self._times

    def _sample_locked(self) -> Tuple[int, int, int]:
        return (int(time.time() * 1000), self._value, self._times)

    def sample(self) -> None:
        """Append a minute-level sampling point (called by the sampling thread every 10s)."""
        with self._lock:
            self._minute.append(self._sample_locked())
            while len(self._minute) > MINUTE_LIST_MAX:
                self._minute.popleft()

    def sample_hour(self) -> None:
        """Append an hour-level sampling point (called by the sampling thread every 10 minutes)."""
        with self._lock:
            self._hour.append(self._sample_locked())
            while len(self._hour) > HOUR_LIST_MAX:
                self._hour.popleft()

    def get_stats_data_in_minute(self) -> StatsSnapshot:
        with self._lock:
            return compute_stats_data(self._minute)

    def get_stats_data_in_hour(self) -> StatsSnapshot:
        with self._lock:
            return compute_stats_data(self._hour)


class StatsItemSet:
    pass

    def __init__(self, stats_name: str) -> None:
        self.stats_name = stats_name
        self._items: Dict[str, StatsItem] = {}
        self._lock = threading.Lock()

    def get_and_create(self, key: str) -> StatsItem:
        with self._lock:
            item = self._items.get(key)
            if item is None:
                item = StatsItem(self.stats_name, key)
                self._items[key] = item
            return item

    def find(self, key: str) -> Optional[StatsItem]:
        with self._lock:
            return self._items.get(key)

    def add_value(self, key: str, inc_value: int, inc_times: int) -> None:
        self.get_and_create(key).add_value(inc_value, inc_times)

    def keys(self) -> list:
        with self._lock:
            return list(self._items.keys())

    def sample_all(self) -> None:
        for key in self.keys():
            self.find(key).sample()

    def sample_hour_all(self) -> None:
        for key in self.keys():
            self.find(key).sample_hour()


class ConsumerStatsManager:
    pass

    def __init__(self) -> None:
        self.topic_and_group_pull_rt = StatsItemSet("PULL_RT")
        self.topic_and_group_pull_tps = StatsItemSet("PULL_TPS")
        self.topic_and_group_consume_rt = StatsItemSet("CONSUME_RT")
        self.topic_and_group_consume_ok_tps = StatsItemSet("CONSUME_OK_TPS")
        self.topic_and_group_consume_failed_tps = StatsItemSet("CONSUME_FAILED_TPS")
        self._sets = (
            self.topic_and_group_pull_rt,
            self.topic_and_group_pull_tps,
            self.topic_and_group_consume_rt,
            self.topic_and_group_consume_ok_tps,
            self.topic_and_group_consume_failed_tps,
        )
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ---------------- lifecycle ----------------
    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True,
                                        name="rmq-consumer-stats-sampler")
        self._thread.start()

    def shutdown(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def _sample_loop(self) -> None:
        rounds = 0
        while not self._stop.wait(SAMPLING_INTERVAL_SECONDS):
            rounds += 1
            for s in self._sets:
                s.sample_all()
            if rounds % 60 == 0:    # 60 * 10s = 10 minutes
                for s in self._sets:
                    s.sample_hour_all()

    @staticmethod
    def _key(topic: str, group: str) -> str:
        return "%s@%s" % (topic, group)

    def inc_pull_rt(self, group: str, topic: str, rt: int) -> None:
        self.topic_and_group_pull_rt.add_value(self._key(topic, group), int(rt), 1)

    def inc_pull_tps(self, group: str, topic: str, msgs: int) -> None:
        self.topic_and_group_pull_tps.add_value(self._key(topic, group), int(msgs), 1)

    def inc_consume_rt(self, group: str, topic: str, rt: int) -> None:
        self.topic_and_group_consume_rt.add_value(self._key(topic, group), int(rt), 1)

    def inc_consume_ok_tps(self, group: str, topic: str, msgs: int) -> None:
        self.topic_and_group_consume_ok_tps.add_value(self._key(topic, group), int(msgs), 1)

    def inc_consume_failed_tps(self, group: str, topic: str, msgs: int) -> None:
        self.topic_and_group_consume_failed_tps.add_value(self._key(topic, group), int(msgs), 1)

    # ---------------- query ----------------
    def consume_status(self, group: str, topic: str) -> ConsumeStatus:
        pass
        cs = ConsumeStatus()
        key = self._key(topic, group)
        ss = self.topic_and_group_pull_rt.find(key)
        if ss is not None:
            cs.pull_rt = ss.get_stats_data_in_minute().avgpt
        ss = self.topic_and_group_pull_tps.find(key)
        if ss is not None:
            cs.pull_tps = ss.get_stats_data_in_minute().tps
        ss = self.topic_and_group_consume_rt.find(key)
        if ss is not None:
            cs.consume_rt = ss.get_stats_data_in_minute().avgpt
        ss = self.topic_and_group_consume_ok_tps.find(key)
        if ss is not None:
            cs.consume_ok_tps = ss.get_stats_data_in_minute().tps
        ss = self.topic_and_group_consume_failed_tps.find(key)
        if ss is not None:
            cs.consume_failed_tps = ss.get_stats_data_in_minute().tps
            cs.consume_failed_msgs = ss.get_stats_data_in_hour().sum
        return cs
