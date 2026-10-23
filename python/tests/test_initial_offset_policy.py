# -*- coding: utf-8 -*-
"""Where a newly assigned queue starts pulling.

Two rules hold the whole surface together, and each carries a paired control:
  * the start offset depends ONLY on the consume-from-where policy -- never on
    the subscription type (a SQL92 subscription that starts at the queue tail
    silently drops the backlog produced before the consumer came up);
  * broadcasting and clustering differ only in where the stored offset is read
    from, so a CONSUME_FROM_TIMESTAMP or %RETRY% start is the same in both
    modes.
"""
from __future__ import annotations

import time

from client.consumer import DefaultLitePullConsumer, DefaultMQPushConsumer
from client.exception import MQClientException
from common.message import MessageQueue
from common.subscription_data import ExpressionType, SubscriptionData
from remoting.protocol.heartbeat import ConsumeFromWhere, MessageModel
from remoting.protocol.namespace_util import NamespaceUtil

GROUP = "GID_InitOffsetUnitTest"
TOPIC = "InitOffsetUnitTestTopic"
BROKER = "broker-a"
MAX_OFFSET = 5000
SEARCH_OFFSET = 3000
RETRY_TOPIC = "%%RETRY%%%s" % GROUP

# A fixed instant years in the past: an implementation that resolves the start
# from "boot time minus 30 minutes" cannot produce it by accident.
CONFIGURED_TS = "20200101000000"


class FakeClient:
    """Records the offset lookups; ``broker_offsets`` is the committed table."""

    def __init__(self):
        self.broker_offsets = {}
        self.max_calls = []
        self.search_calls = []

    def query_consumer_offset(self, group, mq, set_zero_if_not_found=False):
        return self.broker_offsets.get(mq)

    def get_max_offset(self, mq, timeout=None, addr=None):
        self.max_calls.append(mq)
        return MAX_OFFSET

    def search_offset_by_timestamp(self, mq, timestamp, timeout=None, addr=None):
        self.search_calls.append((mq, timestamp))
        return SEARCH_OFFSET


def push_consumer(model=MessageModel.CLUSTERING, where=ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
                  sql92=False):
    c = DefaultMQPushConsumer(GROUP)
    c._mq_client = FakeClient()
    c.message_model = model
    c.consume_from_where = where
    sub = SubscriptionData(topic=TOPIC, sub_string="a > 0" if sql92 else "*")
    if sql92:
        sub.expression_type = ExpressionType.SQL92
    c.subscription_data[TOPIC] = sub
    return c


def mq(topic=TOPIC):
    return MessageQueue(topic, BROKER, 0)


def client_of(c):
    return c._mq_client


# ------------------------------------------------- the policy decides, not the subscription


class TestSubscriptionIrrelevant:
    def test_sql92_first_offset_still_starts_at_zero(self):
        c = push_consumer(where=ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, sql92=True)
        assert c._resolve_initial_offset(client_of(c), mq()) == 0
        assert client_of(c).max_calls == []

    def test_tag_first_offset_starts_at_zero(self):
        # control for the case above: the same policy with a tag subscription
        c = push_consumer(where=ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        assert c._resolve_initial_offset(client_of(c), mq()) == 0

    def test_sql92_last_offset_goes_to_the_tail(self):
        # and the converse: SQL92 must not be pushed to 0 either
        c = push_consumer(sql92=True)
        assert c._resolve_initial_offset(client_of(c), mq()) == MAX_OFFSET


class TestStoredOffsetWins:
    def test_committed_offset_beats_every_policy(self):
        c = push_consumer(where=ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        client_of(c).broker_offsets[mq()] = 42
        assert c._resolve_initial_offset(client_of(c), mq()) == 42
        assert client_of(c).max_calls == []
        assert client_of(c).search_calls == []

    def test_broadcast_local_offset_beats_every_policy(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        c = push_consumer(model=MessageModel.BROADCASTING,
                          where=ConsumeFromWhere.CONSUME_FROM_TIMESTAMP)
        c.consume_timestamp = CONFIGURED_TS
        c._consume_offsets[c._mq_key(mq())] = 77
        c._mq_map[c._mq_key(mq())] = mq()
        c._save_local_offsets()
        assert c._resolve_initial_offset(client_of(c), mq()) == 77
        assert client_of(c).search_calls == []

    def test_broadcast_business_queue_goes_to_the_tail(self):
        # control: the tail lookup is still there for broadcasting under LAST_OFFSET
        c = push_consumer(model=MessageModel.BROADCASTING)
        assert c._resolve_initial_offset(client_of(c), mq()) == MAX_OFFSET
        assert client_of(c).max_calls == [mq()]


# ------------------------------------------------- CONSUME_FROM_TIMESTAMP


class TestTimestampStart:
    def test_business_queue_searches_at_the_configured_instant(self):
        c = push_consumer(where=ConsumeFromWhere.CONSUME_FROM_TIMESTAMP)
        c.consume_timestamp = CONFIGURED_TS
        assert c._resolve_initial_offset(client_of(c), mq()) == SEARCH_OFFSET
        assert client_of(c).search_calls == [(mq(), c._consume_timestamp_millis())]
        # the configured instant really is years back, so "boot time minus 30
        # minutes" cannot satisfy the assertion above
        assert c._consume_timestamp_millis() < int(time.time() * 1000) - 365 * 86400000

    def test_retry_queue_with_timestamp_policy_starts_at_the_tail(self):
        # a retry queue has no meaningful "time of the oldest retry"; searching one
        # backwards can land before messages that were never produced
        c = push_consumer(where=ConsumeFromWhere.CONSUME_FROM_TIMESTAMP)
        c.consume_timestamp = CONFIGURED_TS
        assert c._resolve_initial_offset(client_of(c), mq(RETRY_TOPIC)) == MAX_OFFSET
        assert client_of(c).search_calls == []

    def test_broadcasting_timestamp_searches_just_like_clustering(self):
        # the regression this pins: broadcasting used to fall straight to the tail
        c = push_consumer(model=MessageModel.BROADCASTING,
                          where=ConsumeFromWhere.CONSUME_FROM_TIMESTAMP)
        c.consume_timestamp = CONFIGURED_TS
        assert c._resolve_initial_offset(client_of(c), mq()) == SEARCH_OFFSET
        assert client_of(c).search_calls == [(mq(), c._consume_timestamp_millis())]
        assert client_of(c).max_calls == []

    def test_broadcasting_first_offset_starts_at_zero(self):
        c = push_consumer(model=MessageModel.BROADCASTING,
                          where=ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        assert c._resolve_initial_offset(client_of(c), mq()) == 0
        assert client_of(c).max_calls == []

    def test_bad_timestamp_is_a_hard_failure(self):
        c = push_consumer(where=ConsumeFromWhere.CONSUME_FROM_TIMESTAMP)
        c.consume_timestamp = "2020-01-01 00:00:00"
        try:
            c._resolve_initial_offset(client_of(c), mq())
        except MQClientException as e:
            assert "consumeTimestamp is invalid" in str(e)
            assert "2020-01-01 00:00:00" in str(e)
        else:
            raise AssertionError("a malformed consumeTimestamp must not fall back")


# ------------------------------------------------- %RETRY% under the tail policy


class TestRetryTopic:
    def test_last_offset_retry_queue_starts_at_zero(self):
        c = push_consumer()
        assert c._resolve_initial_offset(client_of(c), mq(RETRY_TOPIC)) == 0
        assert client_of(c).max_calls == []

    def test_last_offset_business_queue_is_the_control(self):
        c = push_consumer()
        assert c._resolve_initial_offset(client_of(c), mq()) == MAX_OFFSET
        assert client_of(c).max_calls == [mq()]

    def test_broadcasting_retry_queue_also_starts_at_zero(self):
        c = push_consumer(model=MessageModel.BROADCASTING)
        assert c._resolve_initial_offset(client_of(c), mq(RETRY_TOPIC)) == 0
        assert client_of(c).max_calls == []

    def test_the_helper_recognises_the_retry_name(self):
        # guards the literal above: every "%RETRY%<group>" topic reads as a retry queue
        assert NamespaceUtil.is_retry_topic(RETRY_TOPIC)


# ------------------------------------------------- lite pull consumer


class TestLitePull:
    def _lite(self, where=ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET):
        c = DefaultLitePullConsumer(GROUP)
        c._mq_client = FakeClient()
        c.consume_from_where = where
        c.consume_timestamp = CONFIGURED_TS
        return c

    def test_retry_queue_starts_at_zero(self):
        c = self._lite()
        assert c._resolve_initial_offset(mq(RETRY_TOPIC)) == 0
        assert c._mq_client.max_calls == []

    def test_business_queue_starts_at_the_tail(self):
        # control: the same consumer shape, business topic, still queries the tail
        c = self._lite()
        assert c._resolve_initial_offset(mq()) == MAX_OFFSET
        assert c._mq_client.max_calls == [mq()]

    def test_retry_queue_with_timestamp_policy_starts_at_the_tail(self):
        c = self._lite(where=ConsumeFromWhere.CONSUME_FROM_TIMESTAMP)
        assert c._resolve_initial_offset(mq(RETRY_TOPIC)) == MAX_OFFSET
        assert c._mq_client.search_calls == []

    def test_business_queue_with_timestamp_policy_searches(self):
        c = self._lite(where=ConsumeFromWhere.CONSUME_FROM_TIMESTAMP)
        assert c._resolve_initial_offset(mq()) == SEARCH_OFFSET
        assert c._mq_client.max_calls == []
