# -*- coding: utf-8 -*-
"""消费侧统计（ConsumerStatsManager）单测 —— 离线，不依赖采样线程与集群。

Java 语义锚点（common/stats/StatsItem.java + client/stat/ConsumerStatsManager.java，5.5.1）：
* StatsItem 持有**累计** value/times；快照 = 累计差分（不是每分钟一个桶）；
  tps = sum*1000/(last.ts-first.ts)（**每秒**）；avgpt = sum/timesDiff（RT 项 = 平均耗时）；
* TPS 计数：addValue(key, msgs, 1) → tps = **调用次数**/秒，sum = 消息数；
* RT 计数：addRTValue(key, rt, 1) → avgpt = 平均毫秒；
* consumeStatus 全取 minute 快照；consumeFailedMsgs 取 failed 的 **hour** 窗口 sum。
"""
from __future__ import annotations

import time

from rocketmq.client.consumer_stats import (ConsumerStatsManager, StatsItem, StatsItemSet,
                                            compute_stats_data)
from rocketmq.remoting.protocol.body import ConsumeStatus

GROUP = "GID_StatsUnit"
TOPIC = "StatsTopic"


class TestComputeStatsData:
    def test_empty_list_gives_zero_snapshot(self):
        ss = compute_stats_data([])
        assert ss.sum == 0 and ss.tps == 0.0 and ss.avgpt == 0.0 and ss.times == 0

    def test_java_formula(self):
        # 两个累计点：10 秒内 value 增 30、times 增 3
        snaps = [(0, 100, 10), (10_000, 130, 13)]
        ss = compute_stats_data(snaps)
        assert ss.sum == 30
        assert abs(ss.tps - 3.0) < 1e-6          # 30 msgs / 10s = 3/s
        assert abs(ss.avgpt - 10.0) < 1e-6       # 30 / 3 = 10 per call
        assert ss.times == 3

    def test_single_sample_has_zero_span(self):
        # 只有一个点：span=0 → tps=0（Java 中除零防护来自 span>0 判断）
        ss = compute_stats_data([(1_000, 5, 1)])
        assert ss.sum == 0 and ss.tps == 0.0


class TestStatsItem:
    def test_cumulative_values(self):
        it = StatsItem("PULL_TPS", "T@G")
        it.add_value(5, 1)
        it.add_value(7, 1)
        assert it.value == 12
        assert it.times == 2

    def test_minute_snapshot_diffs_samples(self):
        it = StatsItem("PULL_TPS", "T@G")
        it.add_value(10, 1)
        it.sample()                    # 第一个累计点
        it.add_value(15, 1)
        it.sample()                    # 第二个累计点
        ss = it.get_stats_data_in_minute()
        assert ss.sum == 15
        assert ss.times == 1
        assert ss.tps >= 0

    def test_hour_window_separate_from_minute(self):
        # 累计差分：两点 (ts,100,2) → (ts,400,4)：sum=400-100=300、timesDiff=2
        it = StatsItem("PULL_RT", "T@G")
        it.add_value(100, 2)
        it.sample_hour()
        it.add_value(300, 2)
        it.sample_hour()
        ss = it.get_stats_data_in_hour()
        assert ss.sum == 300 and ss.times == 2
        assert abs(ss.avgpt - 150.0) < 1e-6

    def test_minute_list_capped(self):
        it = StatsItem("PULL_TPS", "T@G")
        for _ in range(80):
            it.sample()
        # 60 个点封顶（MINUTE_LIST_MAX）
        assert len(it._minute) == 60


class TestStatsItemSet:
    def test_get_and_create_is_idempotent(self):
        s = StatsItemSet("X")
        a = s.get_and_create("k")
        b = s.get_and_create("k")
        assert a is b

    def test_add_value_creates_and_accumulates(self):
        s = StatsItemSet("X")
        s.add_value("T@G", 3, 1)
        s.add_value("T@G", 4, 1)
        it = s.find("T@G")
        assert it is not None and it.value == 7 and it.times == 2
        assert s.find("other") is None


class TestConsumerStatsManager:
    def test_inc_semantics_match_java(self):
        # TPS 类：value=消息数、times=调用次数；RT 类：value=耗时、times=调用次数
        m = ConsumerStatsManager()
        m.inc_pull_tps(GROUP, TOPIC, 10)
        m.inc_pull_tps(GROUP, TOPIC, 20)
        item = m.topic_and_group_pull_tps.find("StatsTopic@GID_StatsUnit")
        assert item.value == 30 and item.times == 2

        m.inc_pull_rt(GROUP, TOPIC, 25)
        rt = m.topic_and_group_pull_rt.find("StatsTopic@GID_StatsUnit")
        assert rt.value == 25 and rt.times == 1

    def test_key_is_topic_at_group(self):
        m = ConsumerStatsManager()
        m.inc_consume_ok_tps("myGroup", "myTopic", 1)
        assert m.topic_and_group_consume_ok_tps.find("myTopic@myGroup") is not None

    def test_consume_status_all_zeros_when_untouched(self):
        m = ConsumerStatsManager()
        cs = m.consume_status(GROUP, TOPIC)
        assert isinstance(cs, ConsumeStatus)
        assert cs.pull_rt == 0 and cs.pull_tps == 0 and cs.consume_rt == 0
        assert cs.consume_ok_tps == 0 and cs.consume_failed_tps == 0
        assert cs.consume_failed_msgs == 0

    def test_consume_status_maps_rt_to_avgpt_and_tps_to_tps(self):
        # 直接喂累计快照点（间隔 1s），绕开真实采样线程，窗口内增量即窗口数据：
        #   pull_rt：value 增 200 / times 增 4 → avgpt=50ms
        #   consume_rt：value 增 40 / times 增 4 → avgpt=10ms
        #   pull_tps：value（消息数）增 20 → tps=20/s（Java 的 TPS 是**消息**每秒）
        #   consume_ok_tps：value 增 8 → tps=8/s
        m = ConsumerStatsManager()
        now = int(time.time() * 1000)

        def feed(s: StatsItemSet) -> None:
            it = s.find("StatsTopic@GID_StatsUnit")
            assert it is not None
            it._minute.clear()
            it._minute.append((now - 1000, 0, 0))
            it._minute.append((now, it.value, it.times))

        m.inc_pull_rt(GROUP, TOPIC, 50)
        m.inc_pull_rt(GROUP, TOPIC, 50)
        m.inc_pull_rt(GROUP, TOPIC, 50)
        m.inc_pull_rt(GROUP, TOPIC, 50)         # value=200 times=4
        for _ in range(4):
            m.inc_consume_rt(GROUP, TOPIC, 10)  # value=40 times=4
            m.inc_pull_tps(GROUP, TOPIC, 5)     # value=20 times=4
            m.inc_consume_ok_tps(GROUP, TOPIC, 2)  # value=8 times=4
        feed(m.topic_and_group_pull_rt)
        feed(m.topic_and_group_consume_rt)
        feed(m.topic_and_group_pull_tps)
        feed(m.topic_and_group_consume_ok_tps)
        cs = m.consume_status(GROUP, TOPIC)
        assert abs(cs.pull_rt - 50.0) < 1e-6
        assert abs(cs.consume_rt - 10.0) < 1e-6
        assert abs(cs.pull_tps - 20.0) < 1e-6
        assert abs(cs.consume_ok_tps - 8.0) < 1e-6

    def test_consume_failed_msgs_uses_hour_sum(self):
        m = ConsumerStatsManager()
        m.inc_consume_failed_tps(GROUP, TOPIC, 7)
        it = m.topic_and_group_consume_failed_tps.find("StatsTopic@GID_StatsUnit")
        now = int(time.time() * 1000)
        it._hour.append((now - 1000, 0, 0))
        it._hour.append((now, 7, 1))
        cs = m.consume_status(GROUP, TOPIC)
        assert cs.consume_failed_msgs == 7

    def test_start_shutdown_sampler(self):
        m = ConsumerStatsManager()
        m.start()
        try:
            assert m._thread is not None and m._thread.is_alive()
        finally:
            m.shutdown()
        assert m._thread is None
