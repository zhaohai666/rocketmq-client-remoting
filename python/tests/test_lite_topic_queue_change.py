# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time

import pytest

from client.consumer import (DefaultLitePullConsumer, TopicMessageQueueChangeListener)
from client.exception import MQClientException
from common.message import MessageQueue


class FakeInstance:
    """只需要订阅视图（get_topic_subscribe_info）的最小 MQClientInstance 替身。"""

    def __init__(self):
        self.queues = {}
        self.calls = []
        self.route_refreshes = []
        self.raise_for = set()

    def get_topic_subscribe_info(self, topic):
        self.calls.append(topic)
        if topic in self.raise_for:
            raise RuntimeError("route query failed")
        return list(self.queues.get(topic, []))

    def update_topic_route_info_from_name_server(self, topic, timeout_millis=5000,
                                                 is_default=False):
        self.route_refreshes.append(topic)
        return True

    def set(self, topic, broker_queues):
        self.queues[topic] = [MessageQueue(topic, b, q) for (b, q) in broker_queues]


class Recorder(TopicMessageQueueChangeListener):
    def __init__(self):
        self.events = []

    def on_changed(self, topic, message_queues):
        self.events.append((topic, sorted(mq.queue_id for mq in message_queues)))


def _until(timeout_s: float, cond) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    return bool(cond())


def _consumer(namespace=""):
    c = DefaultLitePullConsumer("GID_tqc", namespace=namespace)
    c.set_namesrv_addr("127.0.0.1:9876")
    fake = FakeInstance()
    c._mq_client = fake
    return c, fake


def test_null_topic_or_listener_is_rejected():
    c, _ = _consumer()
    with pytest.raises(MQClientException):
        c.register_topic_message_queue_change_listener("", Recorder())
    with pytest.raises(MQClientException):
        c.register_topic_message_queue_change_listener("T", None)
    assert c._topic_change_listeners == {}


def test_re_register_overwrites_listener():
    c, fake = _consumer()
    first, second = Recorder(), Recorder()
    c.register_topic_message_queue_change_listener("T", first)
    c.register_topic_message_queue_change_listener("T", second)
    assert len(c._topic_change_listeners) == 1
    fake.set("T", [("broker-a", 0)])
    assert c.fetch_topic_message_queues_and_compare() == 1
    assert second.events == [("T", [0])]
    assert first.events == []


def test_snapshot_only_taken_when_started():
    c, fake = _consumer()
    fake.set("T", [("broker-a", 0), ("broker-a", 1)])
    c.register_topic_message_queue_change_listener("T", Recorder())
    assert "T" not in c._message_queues_for_topic
    c._started = True
    rec = Recorder()
    c.register_topic_message_queue_change_listener("T", rec)
    assert c._message_queues_for_topic["T"] == set(fake.queues["T"])
    assert rec.events == []          # 快照当下不算变化
    assert c.fetch_topic_message_queues_and_compare() == 0


def test_is_set_equal_branches():
    mq = [MessageQueue("T", "broker-a", 0), MessageQueue("T", "broker-a", 1)]
    f = DefaultLitePullConsumer._is_set_equal
    assert f(None, None) is True
    assert f(set(mq), None) is False
    assert f(None, set(mq)) is False
    assert f(set(mq), set(mq[:1])) is False
    assert f(set(mq), set(reversed(mq))) is True


def test_callback_fires_only_on_change():
    c, fake = _consumer()
    fake.set("T", [("broker-a", 0)])
    rec = Recorder()
    c.register_topic_message_queue_change_listener("T", rec)
    c._started = True                 # 让注册的快照分支生效过一遍
    assert rec.events == []

    fake.set("T", [("broker-a", 0), ("broker-a", 1)])   # 扩容
    assert c.fetch_topic_message_queues_and_compare() == 1
    assert rec.events == [("T", [0, 1])]
    assert c.fetch_topic_message_queues_and_compare() == 0   # 未变不再回调
    assert rec.events == [("T", [0, 1])]

    fake.set("T", [("broker-a", 0)])                      # 缩容
    assert c.fetch_topic_message_queues_and_compare() == 1
    assert rec.events == [("T", [0, 1]), ("T", [0])]
    # 快照跟着新集合走，否则缩容会反复回调
    assert c._message_queues_for_topic["T"] == set(fake.queues["T"])


def test_one_topic_failure_does_not_stop_the_round():
    c, fake = _consumer()
    fake.set("OK", [("broker-a", 0)])
    fake.raise_for.add("BAD")
    ok_rec, bad_rec = Recorder(), Recorder()
    c.register_topic_message_queue_change_listener("BAD", bad_rec)
    c.register_topic_message_queue_change_listener("OK", ok_rec)
    assert c.fetch_topic_message_queues_and_compare() == 1
    assert ok_rec.events == [("OK", [0])]
    assert bad_rec.events == []       # 抛异常的那个 topic 不回调


def test_listener_key_is_namespaced_and_reported_back():
    c, fake = _consumer(namespace="ns1")
    fake.set("ns1%T", [("broker-a", 3)])
    rec = Recorder()
    c.register_topic_message_queue_change_listener("T", rec)
    assert list(c._topic_change_listeners) == ["ns1%T"]
    assert c.fetch_topic_message_queues_and_compare() == 1
    assert rec.events == [("ns1%T", [3])]
    assert fake.calls and fake.calls[0] == "ns1%T"


def test_listener_refreshes_the_route_from_the_nameserver_every_round():
    pass
    c, fake = _consumer(namespace="ns1")
    fake.set("ns1%T", [("broker-a", 0)])
    c._started = True
    rec = Recorder()
    c.register_topic_message_queue_change_listener("T", rec)
    assert fake.route_refreshes == ["ns1%T"], fake.route_refreshes
    assert fake.calls == ["ns1%T"], fake.calls

    fake.set("ns1%T", [("broker-a", 0), ("broker-a", 1)])
    assert c.fetch_topic_message_queues_and_compare() == 1
    assert fake.route_refreshes == ["ns1%T", "ns1%T"], fake.route_refreshes
    assert rec.events == [("ns1%T", [0, 1])]


def test_check_interval_floor_and_default():
    c, _ = _consumer()
    assert c.topic_metadata_check_interval_millis == 30 * 1000
    c.set_topic_metadata_check_interval_millis(0)
    assert c.topic_metadata_check_interval_millis == 1000
    c.set_topic_metadata_check_interval_millis(5000)
    assert c.get_topic_metadata_check_interval_millis() == 5000


def test_metadata_loop_periodically_compares():
    pass
    c, fake = _consumer()
    fake.set("T", [("broker-a", 0)])
    rec = Recorder()
    c.register_topic_message_queue_change_listener("T", rec)
    c._running = True
    c.topic_metadata_check_interval_millis = 1000
    t = threading.Thread(target=c._metadata_loop, kwargs={"first_delay_millis": 200},
                         daemon=True)
    t.start()
    try:
        assert _until(2.5, lambda: bool(rec.events)), "后台循环没有在比对队列集合"
        assert rec.events[-1] == ("T", [0])
        fake.set("T", [("broker-a", 0), ("broker-a", 1)])
        assert _until(2.5, lambda: rec.events == [("T", [0]), ("T", [0, 1])]), \
            "扩容后的第二轮比对没触发回调: %s" % rec.events
    finally:
        c._running = False
        t.join(timeout=2.0)
    assert not t.is_alive()


def test_empty_queue_set_is_not_found_not_a_scale_in():
    """空队列集 ≡ "查不到"：取队列抛错、比对跳过，快照不被清空。

    nameserver 抖动或 topic 暂时没路由时，返回空列表会被读成"缩到 0 队列"，
    于是白报一次假缩容回调，还把快照抹成空集（下一轮又"扩回"一次）。
    """
    c, fake = _consumer()
    fake.set("T", [("broker-a", 0), ("broker-a", 1)])
    c._started = True
    rec = Recorder()
    c.register_topic_message_queue_change_listener("T", rec)
    assert c.fetch_topic_message_queues_and_compare() == 0

    fake.queues["T"] = []
    with pytest.raises(MQClientException) as ei:
        c.fetch_message_queues("T")
    assert "Namesrv return empty" in str(ei.value)

    assert c.fetch_topic_message_queues_and_compare() == 0
    assert rec.events == []
    assert c._message_queues_for_topic["T"] == {
        MessageQueue("T", "broker-a", 0), MessageQueue("T", "broker-a", 1)}

    # 路由回来且和快照一致 ⇒ 仍然不算变化
    fake.set("T", [("broker-a", 0), ("broker-a", 1)])
    assert c.fetch_topic_message_queues_and_compare() == 0
    assert rec.events == []
