# -*- coding: utf-8 -*-
"""Re-registration when a connection to a broker comes back.

A broker drops a consumer group together with the last channel of that group, so a channel
that was taken down by a sync timeout (the transport does that on purpose) leaves the group
unknown to the broker: every pull is answered SUBSCRIPTION_NOT_EXIST until the next periodic
heartbeat round. Measured on a 5.5.1 broker with a 30 s heartbeat and 3 s pull timeouts: the
group stayed unknown for 27 s, and the delayed message was only delivered by the rebalance
that followed the heartbeat.

The classic client closes that window by sending a heartbeat as soon as a channel becomes
active and waking rebalance. These cases pin that reaction, and the controls that keep it off
the nameserver and honour the switch.
"""
from __future__ import annotations

import time

from client.consumer import DefaultMQPushConsumer
from client.mq_client import MQClientInstance

NS = ["127.0.0.1:9876"]
CLIENT_ID = "python-channel-active@1"
GROUP = "GID_ChannelActive"
BROKER = "127.0.0.1:10911"
BROKER_SLAVE = "127.0.0.1:10921"


class _RecordingConsumer:
    def __init__(self, fail=False):
        self.heartbeats: list = []
        self.rebalances = 0
        self.fail = fail

    def heartbeat_to_broker(self, addr):
        if self.fail:
            raise RuntimeError("heartbeat failed")
        self.heartbeats.append(addr)
        return 1

    def rebalance_immediately(self):
        self.rebalances += 1


def _instance() -> MQClientInstance:
    mqc = MQClientInstance(CLIENT_ID, list(NS))
    mqc.broker_addr_table = {"broker-a": {0: BROKER, 1: BROKER_SLAVE}}
    return mqc


def _wait_until(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def _fire(mqc: MQClientInstance, addr: str) -> None:
    """Deliver the transport's channel-active event the way a new connection would."""
    mqc.remoting_client._fire_channel_active(addr)


def test_channel_active_to_a_known_broker_heartbeats_that_broker_and_rebalances():
    mqc = _instance()
    consumer = _RecordingConsumer()
    mqc._consumer_table[GROUP] = consumer
    try:
        _fire(mqc, BROKER)
        assert _wait_until(lambda: consumer.heartbeats == [BROKER]), \
            "the group has to be registered with the broker the new connection leads to"
        assert consumer.rebalances >= 1, "and rebalance has to follow the registration"
    finally:
        mqc.shutdown()


def test_channel_active_to_a_non_broker_address_sends_nothing():
    """A nameserver connection is not a broker and has no consumer registry to restore."""
    mqc = _instance()
    consumer = _RecordingConsumer()
    mqc._consumer_table[GROUP] = consumer
    try:
        _fire(mqc, "127.0.0.1:9876")
        time.sleep(0.2)
        assert consumer.heartbeats == []
        assert consumer.rebalances == 0
    finally:
        mqc.shutdown()


def test_the_switch_turns_the_reaction_off():
    mqc = _instance()
    mqc.enable_heartbeat_channel_event_listener = False
    consumer = _RecordingConsumer()
    mqc._consumer_table[GROUP] = consumer
    try:
        _fire(mqc, BROKER)
        time.sleep(0.2)
        assert consumer.heartbeats == [], \
            "with the switch off the group waits for the periodic heartbeat round"
        assert consumer.rebalances == 0
    finally:
        mqc.shutdown()


def test_one_failing_group_does_not_skip_rebalance():
    mqc = _instance()
    broken = _RecordingConsumer(fail=True)
    healthy = _RecordingConsumer()
    mqc._consumer_table["GID_Broken"] = broken
    mqc._consumer_table[GROUP] = healthy
    try:
        _fire(mqc, BROKER)
        assert _wait_until(lambda: healthy.heartbeats == [BROKER]), \
            "every registered group gets its own round"
        assert _wait_until(lambda: healthy.rebalances >= 1), \
            "a failing heartbeat must not swallow the rebalance"
    finally:
        mqc.shutdown()


def test_push_consumer_heartbeat_to_broker_sends_one_round_to_that_address():
    consumer = DefaultMQPushConsumer(GROUP)
    mqc = MQClientInstance(CLIENT_ID + "-facade", list(NS))
    sent = []
    mqc.send_heartbeat = lambda addr, hb, timeout: sent.append((addr, hb))
    consumer._mq_client = mqc
    consumer._started = True
    consumer._build_heartbeat = lambda: "heartbeat-body"
    try:
        assert consumer.heartbeat_to_broker(BROKER) == 1
        assert sent == [(BROKER, "heartbeat-body")], \
            "one round, to the address the connection was re-established on"
    finally:
        mqc.shutdown()
