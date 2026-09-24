# -*- coding: utf-8 -*-
"""路由刷新 / 位点落盘周期的可配置性（离线，无集群）。

Java 锚点（5.5.1 逐条核对）：

  * ``ClientConfig.java:58`` ``private int pollNameServerInterval = 1000 * 30;``（30s）、
    ``:62`` ``heartbeatBrokerInterval = 1000 * 30``、``:66``
    ``persistConsumerOffsetInterval = 1000 * 5;``（5s）；getter/setter 在 ``:311-333``。
    ``DefaultMQProducer`` / ``DefaultMQPushConsumer`` / ``DefaultMQPullConsumer`` /
    ``DefaultLitePullConsumer`` / ``DefaultMQAdminExt`` 都 ``extends ClientConfig``，
    因此这几个开关在 Java 里对每个客户端都可见。
  * ``MQClientInstance.startScheduledTask``：
      - ``:400-407`` ``updateTopicRouteInfoFromNameServer``：
        ``scheduleAtFixedRate(..., 10, clientConfig.getPollNameServerInterval(), MS)``
        —— initialDelay 10ms，周期默认 30s。
      - ``:414-424`` ``persistAllConsumerOffset``：
        ``scheduleAtFixedRate(..., 1000 * 10, clientConfig.getPersistConsumerOffsetInterval(), MS)``
        —— initialDelay 10s，周期默认 5s。
  * 两条任务都走 ``scheduledExecutorService.scheduleAtFixedRate``：**启动时把周期排定**，
    运行期再改字段不影响已排定的节奏；**首个任务在 initialDelay 之后立刻执行**，不是
    initialDelay + 一个周期之后。本端口的后台循环同样如此（这一条被真机验证脚本
    ``verify_interval_live.py`` 的 I1/I2 钉住：见 2026-09-24 那轮实测）。
"""
from __future__ import annotations

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import (DefaultLitePullConsumer, DefaultMQPullConsumer,
                                      DefaultMQPushConsumer)
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer

POLL_DEFAULT = 30000
PERSIST_DEFAULT = 5000


class _RecordingStop:
    """假的 ``threading.Event``：记下每次 ``wait()`` 请求的超时，按剧本返回。

    ``results`` 给出前几次 wait 的返回值（``True`` = 立刻退出循环）；剧本用尽后
    一律返回 ``True``，保证循环一定收敛、测试不会挂着。
    """

    def __init__(self, results=()):
        self.results = list(results)
        self.timeouts = []

    def wait(self, timeout=None):
        self.timeouts.append(timeout)
        if self.results:
            return bool(self.results.pop(0))
        return True


def _drop(inst: MQClientInstance) -> None:
    MQClientInstance.INSTANCE_MAP.pop(inst.client_id, None)


# ---------------------------------------------------------------- 默认值（对齐 ClientConfig）
def test_facade_defaults_match_java_client_config():
    producer = DefaultMQProducer("PG_interval_defaults")
    assert producer.poll_name_server_interval == POLL_DEFAULT
    assert producer.heartbeat_interval_millis == POLL_DEFAULT

    push = DefaultMQPushConsumer("CG_interval_defaults")
    assert push.poll_name_server_interval == POLL_DEFAULT
    assert push.persist_consumer_offset_interval == PERSIST_DEFAULT
    assert push.heartbeat_interval_millis == POLL_DEFAULT

    assert DefaultMQPullConsumer("CG_pull_defaults").poll_name_server_interval == POLL_DEFAULT
    assert DefaultLitePullConsumer("CG_lite_defaults").poll_name_server_interval == POLL_DEFAULT
    assert DefaultMQAdminExt().poll_name_server_interval == POLL_DEFAULT


def test_client_instance_defaults_match_java_client_config():
    inst = MQClientInstance("interval_defaults", ["127.0.0.1:9876"])
    try:
        assert inst.poll_name_server_interval == POLL_DEFAULT
    finally:
        _drop(inst)


# ---------------------------------------------------------------- 路由刷新循环
def test_route_refresh_loop_uses_initial_delay_then_configured_period():
    """Java initialDelay = 10ms，随后按 pollNameServerInterval 固定周期。

    首轮刷新发生在 initialDelay 之后（``scheduleAtFixedRate`` 语义），不是
    ``initialDelay + 一个周期`` 之后 —— 真机验证脚本 I1 就是按"1s 周期组 ≤6s 看到
    新建 topic 的路由"钉住这一点的。
    """
    inst = MQClientInstance("interval_route", ["127.0.0.1:9876"])
    try:
        inst.poll_name_server_interval = 1500
        inst._started = True
        stop = _RecordingStop([False, False])
        inst._route_refresh_stop = stop  # type: ignore[assignment]
        inst._route_refresh_loop()
        assert stop.timeouts[0] == 0.01          # Java initialDelay = 10ms
        assert stop.timeouts[1:] == [1.5, 1.5]   # 配置值换算成秒
    finally:
        _drop(inst)


def test_route_refresh_loop_refreshes_every_topic_in_use():
    """循环体逐个刷新在用 topic（对应 Java updateTopicRouteInfoFromNameServer()）。"""
    inst = MQClientInstance("interval_route_body", ["127.0.0.1:9876"])
    try:
        calls = []
        inst.update_topic_route_info_from_name_server = (  # type: ignore[method-assign]
            lambda topic, *a, **k: calls.append(topic) or True)
        inst.register_topic_in_use("T_IntervalA")
        inst.register_topic_in_use("T_IntervalB")
        inst._started = True
        # initialDelay（10ms）之后立刻跑首轮循环体，之后才按周期等待。
        stop = _RecordingStop([False])
        inst._route_refresh_stop = stop  # type: ignore[assignment]
        inst._route_refresh_loop()
        assert stop.timeouts == [0.01, POLL_DEFAULT / 1000.0]
        assert sorted(calls) == ["T_IntervalA", "T_IntervalB"]
    finally:
        _drop(inst)


def test_route_refresh_loop_stops_on_the_wait_before_the_first_tick():
    """start 后立刻 shutdown（stop 已置位）时首轮就退出，不再拉路由。"""
    inst = MQClientInstance("interval_route_stop", ["127.0.0.1:9876"])
    try:
        calls = []
        inst.update_topic_route_info_from_name_server = (  # type: ignore[method-assign]
            lambda topic, *a, **k: calls.append(topic) or True)
        inst.register_topic_in_use("T_IntervalStop")
        inst._started = True
        stop = _RecordingStop([True])
        inst._route_refresh_stop = stop  # type: ignore[assignment]
        inst._route_refresh_loop()
        assert stop.timeouts == [0.01]
        assert calls == []
    finally:
        _drop(inst)


# ---------------------------------------------------------------- 另外两条定时任务
def test_namesrv_refresh_loop_first_fetch_is_after_ten_seconds():
    """Java：动态取址 initialDelay 10s、周期 2 分钟（首拉在 10s，不是 130s）。"""
    inst = MQClientInstance("interval_namesrv", ["127.0.0.1:9876"])
    try:
        calls = []
        inst.fetch_name_server_addr = lambda: calls.append(True)  # type: ignore[method-assign]
        inst._started = True
        stop = _RecordingStop([False])
        inst._namesrv_refresh_stop = stop  # type: ignore[assignment]
        inst._namesrv_refresh_loop()
        assert stop.timeouts == [10.0, 120.0]
        assert len(calls) == 1
    finally:
        _drop(inst)


def test_adjust_thread_pool_loop_first_run_is_after_one_minute():
    """Java：adjustThreadPool initialDelay 1 分钟、周期 1 分钟。"""
    inst = MQClientInstance("interval_adjust", ["127.0.0.1:9876"])
    try:
        calls = []
        inst.adjust_thread_pool = lambda: calls.append(True)  # type: ignore[method-assign]
        inst._started = True
        stop = _RecordingStop([False])
        inst._adjust_pool_stop = stop  # type: ignore[assignment]
        inst._adjust_thread_pool_loop()
        assert stop.timeouts == [60.0, 60.0]
        assert len(calls) == 1
    finally:
        _drop(inst)


# ---------------------------------------------------------------- 位点落盘循环
def test_offset_persist_loop_uses_java_initial_delay_and_default_period():
    """Java：initialDelay 10s、周期 5s；首笔落盘就在 10s（不是 15s）。"""
    consumer = DefaultMQPushConsumer("CG_interval_persist_default")
    persisted = []
    consumer._persist_offsets_once = lambda: persisted.append(True)  # type: ignore[method-assign]
    stop = _RecordingStop([False])
    consumer._stop = stop  # type: ignore[assignment]
    consumer._offset_persist_loop()
    assert stop.timeouts == [10.0, PERSIST_DEFAULT / 1000.0]
    assert len(persisted) == 1


def test_offset_persist_loop_honours_the_configured_period():
    consumer = DefaultMQPushConsumer("CG_interval_persist_fast")
    consumer.persist_consumer_offset_interval = 700
    persisted = []
    consumer._persist_offsets_once = lambda: persisted.append(True)  # type: ignore[method-assign]
    stop = _RecordingStop([False, False])
    consumer._stop = stop  # type: ignore[assignment]
    consumer._offset_persist_loop()
    assert stop.timeouts[0] == 10.0
    assert stop.timeouts[1:] == [0.7, 0.7]
    assert len(persisted) == 2


def test_offset_persist_loop_exits_when_stopped_before_first_tick():
    consumer = DefaultMQPushConsumer("CG_interval_persist_stop")
    persisted = []
    consumer._persist_offsets_once = lambda: persisted.append(True)  # type: ignore[method-assign]
    stop = _RecordingStop([True])
    consumer._stop = stop  # type: ignore[assignment]
    consumer._offset_persist_loop()
    assert stop.timeouts == [10.0]
    assert persisted == []


# ---------------------------------------------------------------- 透传
def test_lite_pull_forwards_the_interval_to_the_client_instance():
    lite = DefaultLitePullConsumer("CG_interval_lite")
    lite.client_id = "interval_lite_client"
    lite.poll_name_server_interval = 2500
    inst = lite._create_client()
    try:
        assert inst.poll_name_server_interval == 2500
    finally:
        _drop(inst)


def test_admin_forwards_the_interval_to_the_client_instance(monkeypatch):
    """admin.start() 里的构造点也要把可配置周期带上（其余 facade 走同一条路径）。"""
    captured = {}

    class _FakeClient:
        def __init__(self, client_id, name_server_addrs, **kwargs):
            captured["client_id"] = client_id
            captured["kwargs"] = kwargs
            self.name_server_addrs = list(name_server_addrs)
            self.client_id = client_id

        def start(self):
            captured["started"] = True

    monkeypatch.setattr("rocketmq.client.admin.MQClientInstance", _FakeClient)
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr("127.0.0.1:9876")
    admin.poll_name_server_interval = 1200
    admin.start()
    assert captured["kwargs"]["poll_name_server_interval"] == 1200
    assert captured.get("started") is True
