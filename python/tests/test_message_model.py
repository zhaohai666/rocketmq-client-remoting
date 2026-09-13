# -*- coding: utf-8 -*-
"""消息模型测试（对应 org.apache.rocketmq.common.message 包）。"""
from __future__ import annotations

import pytest

from rocketmq.common.message import Message, MessageBatch, MessageExt, MessageQueue
from rocketmq.common.message_const import MessageConst


class TestMessageQueue:
    """MessageQueue.hashCode 必须与 Java 逐步一致（否则 consumer rebalance 会算错）。"""

    @staticmethod
    def _java_hash(s: str) -> int:
        h = 0
        for ch in s:
            h = (31 * h + ord(ch)) & 0xFFFFFFFF
        return h - 0x100000000 if h >= 0x80000000 else h

    @staticmethod
    def _reference_hash(topic: str, broker_name: str, queue_id: int) -> int:
        """按 Java hashCode 实现逐位推导：((1*31+bh)*31+qid)*31+th。"""
        r = 1
        r = (31 * r + TestMessageQueue._java_hash(broker_name)) & 0xFFFFFFFF
        r = (31 * r + queue_id) & 0xFFFFFFFF
        r = (31 * r + TestMessageQueue._java_hash(topic)) & 0xFFFFFFFF
        return r - 0x100000000 if r >= 0x80000000 else r

    @pytest.mark.parametrize(
        "topic,broker,queue_id,expected",
        [
            ("TopicTest", "BrokerA", 0, -1229861880),
            ("TopicTest", "BrokerA", 3, -1229861787),
            ("", "", 0, 29791),
            ("%RETRY%GroupA", "BrokerB", 1, 566955243),
        ],
    )
    def test_hashcode_matches_java(self, topic, broker, queue_id, expected):
        mq = MessageQueue(topic, broker, queue_id)
        assert mq.hashcode() == expected
        assert mq.hashcode() == self._reference_hash(topic, broker, queue_id)

    def test_equal_and_hash(self):
        a = MessageQueue("TopicTest", "BrokerA", 1)
        b = MessageQueue("TopicTest", "BrokerA", 1)
        c = MessageQueue("TopicTest", "BrokerA", 2)
        assert a == b
        assert hash(a) == hash(b)
        assert a != c
        assert a != "not-a-queue"

    def test_compare_to(self):
        base = MessageQueue("TopicTest", "BrokerA", 1)
        assert base.compare_to(MessageQueue("TopicTest", "BrokerA", 1)) == 0
        assert base.compare_to(MessageQueue("TopicTest", "BrokerA", 2)) < 0
        assert base.compare_to(MessageQueue("TopicTest", "BrokerZ", 0)) < 0
        assert base.compare_to(MessageQueue("AAATopic", "BrokerA", 1)) > 0

    def test_getters_and_setters(self):
        mq = MessageQueue()
        mq.set_topic("T")
        mq.set_broker_name("B")
        mq.set_queue_id(7)
        assert (mq.get_topic(), mq.get_broker_name(), mq.get_queue_id()) == ("T", "B", 7)
        assert mq.get_queue_id_str() == "7"


class TestMessage:
    def test_defaults(self):
        msg = Message(topic="TopicTest", body=b"hello")
        assert msg.get_topic() == "TopicTest"
        assert msg.get_body() == b"hello"
        assert msg.get_flag() == 0
        assert msg.get_properties() == {}
        assert msg.get_transaction_id() is None

    def test_tags_keys_go_to_properties(self):
        msg = Message(topic="T", body=b"x", tags="TagA", keys="k1 k2")
        assert msg.get_tags() == "TagA"
        assert msg.get_keys() == "k1 k2"
        assert msg.properties[MessageConst.PROPERTY_TAGS] == "TagA"
        assert msg.properties[MessageConst.PROPERTY_KEYS] == "k1 k2"

    def test_empty_tags_not_written(self):
        msg = Message(topic="T", body=b"x", tags="", keys="")
        assert msg.get_properties() == {}

    def test_delay_and_wait_flag(self):
        msg = Message(topic="T", body=b"x")
        msg.set_delay_time_level(3)
        assert msg.get_delay_time_level() == "3"
        msg.set_wait_store_msg_ok(False)
        assert msg.get_wait_store_msg_ok() == "false"
        msg.set_wait_store_msg_ok(True)
        assert msg.get_wait_store_msg_ok() == "true"

    def test_user_property(self):
        msg = Message(topic="T", body=b"x")
        msg.set_user_property("a", "1")
        msg.put_property("b", "2")
        assert msg.get_user_property("a") == "1"
        assert msg.get_property("b") == "2"
        msg.remove_property("a")
        assert msg.get_property("a") is None
        msg.clear_property()
        assert msg.get_properties() == {}

    def test_body_default_is_empty_bytes(self):
        assert Message(topic="T").get_body() == b""


class TestMessageExt:
    def test_defaults(self):
        ext = MessageExt()
        assert ext.queue_id == 0
        assert ext.store_size == 0
        assert ext.queue_offset == 0
        assert ext.sys_flag == 0
        assert ext.reconsume_times == 0
        assert ext.msg_id is None
        assert ext.offset_msg_id is None

    def test_accessors_roundtrip(self):
        ext = MessageExt()
        ext.set_queue_id(2)
        ext.set_store_size(100)
        ext.set_queue_offset(9)
        ext.set_sys_flag(1)
        ext.set_born_timestamp(1700000000000)
        ext.set_store_timestamp(1700000000001)
        ext.set_born_host("127.0.0.1")
        ext.born_host_port = 10000
        ext.set_store_host("127.0.0.1")
        ext.store_host_port = 10911
        ext.set_msg_id("0A0A0A0A0000XXXX")
        ext.set_commit_log_offset(512)
        ext.set_body_crc(12345)
        ext.set_reconsume_times(2)
        ext.set_prepared_transaction_offset(7)
        ext.set_msg_type("NORMAL")

        assert (ext.get_queue_id(), ext.get_store_size(), ext.get_queue_offset()) == (2, 100, 9)
        assert ext.get_body_crc() == 12345
        assert ext.get_commit_log_offset() == 512
        assert ext.get_reconsume_times() == 2
        assert ext.get_prepared_transaction_offset() == 7
        assert ext.get_msg_type() == "NORMAL"
        assert ext.get_born_host_string() == "127.0.0.1:10000"
        assert ext.get_store_host_string() == "127.0.0.1:10911"

    def test_host_string_without_port(self):
        ext = MessageExt()
        ext.set_born_host("10.0.0.1")
        assert ext.get_born_host_string() == "10.0.0.1"


class TestMessageBatch:
    def test_generate_from_list(self):
        batch = MessageBatch.generate_from_list([
            Message(topic="T", body=b"a"),
            Message(topic="T", body=b"b"),
        ])
        assert batch.get_topic() == "T"
        assert len(batch) == 2
        assert [m.get_body() for m in batch] == [b"a", b"b"]
        assert isinstance(batch.get_body(), bytes) and len(batch.get_body()) > 0

    def test_empty_list_rejected(self):
        with pytest.raises(ValueError):
            MessageBatch.generate_from_list([])

    def test_mixed_topic_rejected(self):
        with pytest.raises(ValueError):
            MessageBatch.generate_from_list([
                Message(topic="T1", body=b"a"),
                Message(topic="T2", body=b"b"),
            ])

    def test_delay_message_rejected(self):
        msg = Message(topic="T", body=b"a")
        msg.set_delay_time_level(2)
        with pytest.raises(ValueError):
            MessageBatch.generate_from_list([msg])

    def test_retry_topic_rejected(self):
        with pytest.raises(ValueError):
            MessageBatch.generate_from_list([Message(topic="%RETRY%GroupA", body=b"a")])

    def test_wait_store_msg_ok_mismatch_rejected(self):
        m1 = Message(topic="T", body=b"a")
        m2 = Message(topic="T", body=b"b")
        m1.set_wait_store_msg_ok(True)
        m2.set_wait_store_msg_ok(False)
        with pytest.raises(ValueError):
            MessageBatch.generate_from_list([m1, m2])
