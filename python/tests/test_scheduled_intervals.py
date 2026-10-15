# -*- coding: utf-8 -*-
from __future__ import annotations

from client.admin import DefaultMQAdminExt
from client.consumer import (DefaultLitePullConsumer, DefaultMQPullConsumer,
                                      DefaultMQPushConsumer)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer

POLL_DEFAULT = 30000
PERSIST_DEFAULT = 5000


class _RecordingStop:
    """A fake ``threading.Event``: records the timeout of each ``wait()`` request and returns per the script.

    ``results`` gives the return values of the first few waits (``True`` = exit the loop
    immediately); once the script is exhausted it always returns ``True``, guaranteeing the
    loop converges and the test does not hang.
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


# ---------------------------------------------------------------- Defaults (aligned with ClientConfig)
def test_facade_defaults_match_the_client_config():
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


def test_client_instance_defaults_match_the_client_config():
    inst = MQClientInstance("interval_defaults", ["127.0.0.1:9876"])
    try:
        assert inst.poll_name_server_interval == POLL_DEFAULT
    finally:
        _drop(inst)


# ---------------------------------------------------------------- Route refresh loop
def test_route_refresh_loop_uses_initial_delay_then_configured_period():
    pass
    inst = MQClientInstance("interval_route", ["127.0.0.1:9876"])
    try:
        inst.poll_name_server_interval = 1500
        inst._started = True
        stop = _RecordingStop([False, False])
        inst._route_refresh_stop = stop  # type: ignore[assignment]
        inst._route_refresh_loop()
        assert stop.timeouts[0] == 0.01
        assert stop.timeouts[1:] == [1.5, 1.5]   # the configured value converted to seconds
    finally:
        _drop(inst)


def test_route_refresh_loop_refreshes_every_topic_in_use():
    pass
    inst = MQClientInstance("interval_route_body", ["127.0.0.1:9876"])
    try:
        calls = []
        inst.update_topic_route_info_from_name_server = (  # type: ignore[method-assign]
            lambda topic, *a, **k: calls.append(topic) or True)
        inst.register_topic_in_use("T_IntervalA")
        inst.register_topic_in_use("T_IntervalB")
        inst._started = True
        # after initialDelay (10ms) the first loop body runs immediately, only then does it wait per period.
        stop = _RecordingStop([False])
        inst._route_refresh_stop = stop  # type: ignore[assignment]
        inst._route_refresh_loop()
        assert stop.timeouts == [0.01, POLL_DEFAULT / 1000.0]
        assert sorted(calls) == ["T_IntervalA", "T_IntervalB"]
    finally:
        _drop(inst)


def test_route_refresh_loop_stops_on_the_wait_before_the_first_tick():
    """When shutdown happens right after start (stop already set), the first round exits without pulling routes."""
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


# ---------------------------------------------------------------- The other two scheduled tasks
def test_namesrv_refresh_loop_first_fetch_is_after_ten_seconds():
    pass
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
    pass
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


# ---------------------------------------------------------------- Offset persist loop
def test_offset_persist_loop_uses_the_default_initial_delay_and_period():
    pass
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


# ---------------------------------------------------------------- Pass-through
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
    """The construction point inside admin.start() must also carry the configurable period (the other facades take the same path)."""
    captured = {}

    class _FakeClient:
        def __init__(self, client_id, name_server_addrs, **kwargs):
            captured["client_id"] = client_id
            captured["kwargs"] = kwargs
            self.name_server_addrs = list(name_server_addrs)
            self.client_id = client_id

        def start(self):
            captured["started"] = True

    monkeypatch.setattr("client.admin.MQClientInstance", _FakeClient)
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr("127.0.0.1:9876")
    admin.poll_name_server_interval = 1200
    admin.start()
    assert captured["kwargs"]["poll_name_server_interval"] == 1200
    assert captured.get("started") is True
