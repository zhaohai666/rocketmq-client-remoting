# -*- coding: utf-8 -*-
"""Heartbeat V2: the fingerprint-gated registration path, driven through a fake transport.

What is pinned here is the decision table in ``client/heartbeat_v2.py`` plus the two body
fields it depends on, seen as the broker sees them: the decoded ``HEART_BEAT``(34) body of
every round. Assertions therefore go through the wire (request code + body), not through the
coordinator's own bookkeeping, because a wrong body is what a real broker reacts to.

Every positive statement is paired with a control that fails when the mechanism is removed:
a broker that never answers the two extFields must keep receiving the full body, a
subscription change must break out of the light body, and a rejected round must leave the
stream unremembered.
"""
from __future__ import annotations

import pytest

from client.consumer import DefaultMQPushConsumer
from client.exception import MQBrokerException
from client.mq_client import MQClientInstance
from common.message import MessageQueue
from common.mix_all import MixAll
from common.subscription_data import SubscriptionData
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.heartbeat import (ConsumeType, ConsumerData, HeartbeatData,
                                         HeartbeatV2Result, MessageModel, ProducerData)
from remoting.protocol.remoting_command import RemotingCommand

NS = ["127.0.0.1:9876"]
CLIENT_ID = "python-v2-unit@1"
BROKER_A = "127.0.0.1:10911"
BROKER_B = "127.0.0.1:10921"
GROUP = "GID_V2Coord"
PRODUCER_GROUP = "PID_V2Coord"
TOPIC = "V2CoordTopic"
OTHER_TOPIC = "V2CoordTopicTwo"
BROKER_VERSION = 401


# ---------------------------------------------------------------- fake transport


class _BrokerBehaviour:
    def __init__(self, support_v2=True, sub_change=False, fail_with=None):
        self.support_v2 = support_v2
        self.sub_change = sub_change
        self.fail_with = fail_with


class FakeTransport:
    """A per-address fake broker: answers the two V2 extFields and keeps every body it saw."""

    def __init__(self):
        self.brokers = {}
        self.rounds = []

    def add(self, addr, support_v2=True, sub_change=False, fail_with=None):
        self.brokers[addr] = _BrokerBehaviour(support_v2, sub_change, fail_with)
        return self

    def install(self, mqc) -> "FakeTransport":
        mqc.remoting_client.invoke_sync = self
        return self

    def __call__(self, addr, request, timeout_millis=None):
        assert request.code == RequestCode.HEART_BEAT, \
            "there is no HEART_BEAT_V2 request code; V2 rides the plain heartbeat frame"
        behaviour = self.brokers[addr]
        self.rounds.append((addr, HeartbeatData.decode(request.body)))
        if behaviour.fail_with is not None:
            raise MQBrokerException(behaviour.fail_with, "fake broker rejected the heartbeat")
        response = RemotingCommand()
        response.code = ResponseCode.SUCCESS
        response.version = BROKER_VERSION
        ext = {}
        if behaviour.support_v2:
            ext[MixAll.IS_SUPPORT_HEART_BEAT_V2] = "true"
        if behaviour.sub_change:
            ext[MixAll.IS_SUB_CHANGE] = "true"
        response.ext_fields = ext
        return response

    def bodies(self, addr) -> list:
        return [hb for seen_addr, hb in self.rounds if seen_addr == addr]

    def last(self, addr) -> HeartbeatData:
        bodies = self.bodies(addr)
        assert bodies, "no heartbeat reached %s" % addr
        return bodies[-1]


def _sub(topic: str, sub_version: int = 0) -> SubscriptionData:
    s = SubscriptionData(topic=topic, sub_string="*")
    s.tags_set.add("*")
    s.sub_version = sub_version
    return s


def _heartbeat(topics=(TOPIC,), group: str = GROUP,
               client_id: str = CLIENT_ID) -> HeartbeatData:
    hb = HeartbeatData(client_id)
    cd = ConsumerData(group, ConsumeType.CONSUME_PASSIVELY)
    for topic in topics:
        cd.subscription_data_set.add(_sub(topic))
    hb.consumer_data_set.add(cd)
    return hb


def _producer_heartbeat(group: str = PRODUCER_GROUP,
                        client_id: str = CLIENT_ID) -> HeartbeatData:
    hb = HeartbeatData(client_id)
    hb.producer_data_set.add(ProducerData(group))
    return hb


def _topics(hb: HeartbeatData) -> set:
    seen = set()
    for cd in hb.consumer_data_set:
        for s in cd.subscription_data_set:
            seen.add(s.topic)
    return seen


def _groups(hb: HeartbeatData) -> set:
    return {cd.group_name for cd in hb.consumer_data_set}


def _instance(transport: FakeTransport, client_id: str = CLIENT_ID) -> MQClientInstance:
    mqc = MQClientInstance(client_id, list(NS))
    transport.install(mqc)
    return mqc


# -------------------------------------------------- first round: the full body


def test_first_round_sends_the_full_registration_and_stamps_a_fingerprint():
    hb = _heartbeat()
    expected = hb.compute_fingerprint()
    assert expected != 0, "a zero fingerprint makes the broker take the V1 path forever"

    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    version = mqc.send_heartbeat_to_broker_v2(BROKER_A, hb, GROUP)

    assert version == BROKER_VERSION, "the version still comes from the response header"
    sent = transport.last(BROKER_A)
    assert sent.without_sub is False
    assert _topics(sent) == {TOPIC}, "the first round must carry the subscription set"
    assert sent.heartbeat_fingerprint == expected
    assert hb.heartbeat_fingerprint == expected, \
        "the round stamps the fingerprint on the caller's body, not on a private copy"

    state = mqc._heartbeat_v2.state(GROUP)
    assert BROKER_A in state.support_v2_broker_addrs
    assert state.broker_addr_heartbeat_fingerprint_table[BROKER_A] == expected


def test_broker_without_the_v2_ext_fields_only_ever_gets_the_full_body():
    """Control for the case above: a pre-V2 broker sends neither flag and must never be
    recorded as a V2 broker, so it keeps the full subscription set in every round."""
    transport = FakeTransport().add(BROKER_A, support_v2=False)
    mqc = _instance(transport)

    for _ in range(3):
        mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)

    assert len(transport.bodies(BROKER_A)) == 3
    for sent in transport.bodies(BROKER_A):
        assert sent.without_sub is False
        assert _topics(sent) == {TOPIC}
        assert sent.heartbeat_fingerprint != 0, \
            "the fingerprint is stamped regardless: the broker, not the client, decides the path"

    state = mqc._heartbeat_v2.state(GROUP)
    assert state.support_v2_broker_addrs == set()
    assert state.broker_addr_heartbeat_fingerprint_table == {}


# ---------------------------------------------- later rounds: the light body


def test_unchanged_subscription_sends_the_without_sub_body_with_the_same_fingerprint():
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    full = _heartbeat()
    mqc.send_heartbeat_to_broker_v2(BROKER_A, full, GROUP)
    fingerprint = full.heartbeat_fingerprint
    # subVersion is a wall-clock stamp: a new round carries a fresh one and must still match.
    second = _heartbeat()
    for cd in second.consumer_data_set:
        for s in cd.subscription_data_set:
            s.sub_version = 987654321

    mqc.send_heartbeat_to_broker_v2(BROKER_A, second, GROUP)

    light = transport.last(BROKER_A)
    assert light.without_sub is True
    assert _topics(light) == set(), "the light body re-registers the group without subscriptions"
    assert _groups(light) == {GROUP}, "the group itself still has to be in the body"
    assert light.client_id == CLIENT_ID
    assert light.heartbeat_fingerprint == fingerprint, \
        "the broker compares the fingerprint it stored, so the light copy carries the same value"
    # The light body cannot compute its own fingerprint: its subscription set is empty.
    assert light.compute_fingerprint() != fingerprint


def test_changed_subscription_breaks_out_of_the_light_body():
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    first = _heartbeat()
    mqc.send_heartbeat_to_broker_v2(BROKER_A, first, GROUP)
    old_fingerprint = first.heartbeat_fingerprint

    widened = _heartbeat(topics=(TOPIC, OTHER_TOPIC))
    mqc.send_heartbeat_to_broker_v2(BROKER_A, widened, GROUP)

    sent = transport.last(BROKER_A)
    assert sent.without_sub is False, "a subscription change must be re-delivered in full"
    assert _topics(sent) == {TOPIC, OTHER_TOPIC}
    new_fingerprint = widened.heartbeat_fingerprint
    assert new_fingerprint != old_fingerprint
    assert mqc._heartbeat_v2.state(GROUP).broker_addr_heartbeat_fingerprint_table[BROKER_A] == \
        new_fingerprint

    # And the round after the change is light again.
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(topics=(TOPIC, OTHER_TOPIC)), GROUP)
    assert transport.last(BROKER_A).without_sub is True


def test_producer_stream_loses_only_its_own_subscriptions_in_the_light_body():
    """A producer heartbeat carries ProducerData and no consumer data, so its light body has to
    keep the producer group -- otherwise the broker forgets the transactional/producer registration."""
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _producer_heartbeat(), PRODUCER_GROUP)
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _producer_heartbeat(), PRODUCER_GROUP)

    full, light = transport.bodies(BROKER_A)
    assert full.without_sub is False
    assert {p.group_name for p in full.producer_data_set} == {PRODUCER_GROUP}
    assert light.without_sub is True
    assert {p.group_name for p in light.producer_data_set} == {PRODUCER_GROUP}
    assert light.consumer_data_set == set()
    assert light.heartbeat_fingerprint == full.heartbeat_fingerprint


# ----------------------------------------------- IS_SUB_CHANGE eviction


def test_sub_change_on_the_light_round_forces_the_next_full_redelivery():
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)
    hb = _heartbeat()

    mqc.send_heartbeat_to_broker_v2(BROKER_A, hb, GROUP)
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.last(BROKER_A).without_sub is True

    # The broker's per-group table moved under our feet (another instance of the same group
    # registered different subscriptions): it answers IS_SUB_CHANGE on the light round.
    transport.brokers[BROKER_A].sub_change = True
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.last(BROKER_A).without_sub is True, \
        "the round that is told about the change is still the light one; the answer only takes effect next round"
    assert mqc._heartbeat_v2.state(GROUP).broker_addr_heartbeat_fingerprint_table == {}, \
        "the remembered fingerprint is dropped so the next round re-delivers"
    assert BROKER_A in mqc._heartbeat_v2.state(GROUP).support_v2_broker_addrs, \
        "the broker still speaks V2 -- only the remembered fingerprint is stale"

    transport.brokers[BROKER_A].sub_change = False
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.last(BROKER_A).without_sub is False, \
        "the round after IS_SUB_CHANGE must carry the subscription set again"


def test_sub_change_on_the_full_round_records_the_broker_but_not_the_fingerprint():
    transport = FakeTransport().add(BROKER_A, support_v2=True, sub_change=True)
    mqc = _instance(transport)

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)

    state = mqc._heartbeat_v2.state(GROUP)
    assert BROKER_A in state.support_v2_broker_addrs
    assert state.broker_addr_heartbeat_fingerprint_table == {}

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.last(BROKER_A).without_sub is False

    # Control: the same two rounds without IS_SUB_CHANGE do reach the light body.
    plain = FakeTransport().add(BROKER_A, support_v2=True)
    plain_instance = _instance(plain)
    plain_instance.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    plain_instance.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert plain.last(BROKER_A).without_sub is True


# -------------------------------------------------- one fingerprint per round


def test_two_brokers_of_one_round_share_the_same_fingerprint():
    """The facade builds one body per round and hands it to every broker, so all brokers of that
    round must see the same value -- a per-broker recomputation would make the broker tables
    disagree about which instance holds which registration."""
    transport = FakeTransport().add(BROKER_A, support_v2=True).add(BROKER_B, support_v2=True)
    mqc = _instance(transport)
    hb = _heartbeat()

    mqc.send_heartbeat_to_broker_v2(BROKER_A, hb, GROUP)
    mqc.send_heartbeat_to_broker_v2(BROKER_B, hb, GROUP)

    assert transport.last(BROKER_A).heartbeat_fingerprint == \
        transport.last(BROKER_B).heartbeat_fingerprint
    table = mqc._heartbeat_v2.state(GROUP).broker_addr_heartbeat_fingerprint_table
    assert table[BROKER_A] == table[BROKER_B] == hb.heartbeat_fingerprint


# ---------------------------------------------------- owner keying of the state


def test_two_streams_on_one_broker_do_not_evict_each_other():
    """One broker address receives the producer's and the consumer's heartbeat as two independent
    streams. Keying the tables by address alone would make them overwrite one another's
    fingerprint every round and silently degrade both to the full body forever."""
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _producer_heartbeat(), PRODUCER_GROUP)
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    producer_round, consumer_round = transport.bodies(BROKER_A)
    assert producer_round.heartbeat_fingerprint != consumer_round.heartbeat_fingerprint

    # Second round of each stream: both must land on the light body.
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _producer_heartbeat(), PRODUCER_GROUP)
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.bodies(BROKER_A)[2].without_sub is True, \
        "the consumer round evicted the producer's fingerprint"
    assert transport.bodies(BROKER_A)[3].without_sub is True, \
        "the producer round evicted the consumer's fingerprint"

    assert mqc._heartbeat_v2.owners() == {PRODUCER_GROUP, GROUP}
    assert transport.bodies(BROKER_A)[3].heartbeat_fingerprint == consumer_round.heartbeat_fingerprint


def test_empty_owner_is_its_own_stream():
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), "")
    assert transport.bodies(BROKER_A)[1].without_sub is False, \
        "the default owner must not reuse the group's remembered fingerprint"

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), "")
    assert transport.bodies(BROKER_A)[2].without_sub is True
    assert mqc._heartbeat_v2.owners() == {GROUP, ""}


# ----------------------------------------------------- fingerprint reset


def test_reset_asks_for_a_redelivery_and_keeps_the_v2_knowledge():
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.last(BROKER_A).without_sub is True

    mqc.reset_heartbeat_v2_fingerprint_table(GROUP)

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.last(BROKER_A).without_sub is False, \
        "an allocation change means the queues moved: the broker needs the subscriptions again"
    state = mqc._heartbeat_v2.state(GROUP)
    assert BROKER_A in state.support_v2_broker_addrs, \
        "the reset drops this round's memory, not the broker's capability"


def test_reset_without_owner_clears_every_stream_but_no_broker_capability():
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    for _ in range(2):
        mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
        mqc.send_heartbeat_to_broker_v2(BROKER_A, _producer_heartbeat(), PRODUCER_GROUP)
    assert transport.bodies(BROKER_A)[-1].without_sub is True

    mqc.reset_heartbeat_v2_fingerprint_table()

    for owner in (GROUP, PRODUCER_GROUP):
        state = mqc._heartbeat_v2.state(owner)
        assert state.broker_addr_heartbeat_fingerprint_table == {}, \
            "the reset without an owner must reach every stream"
        assert BROKER_A in state.support_v2_broker_addrs

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _producer_heartbeat(), PRODUCER_GROUP)
    assert transport.bodies(BROKER_A)[-2].without_sub is False
    assert transport.bodies(BROKER_A)[-1].without_sub is False


def test_reset_of_one_owner_leaves_the_other_streams_alone():
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    for _ in range(2):
        mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
        mqc.send_heartbeat_to_broker_v2(BROKER_A, _producer_heartbeat(), PRODUCER_GROUP)

    mqc.reset_heartbeat_v2_fingerprint_table(GROUP)

    mqc.send_heartbeat_to_broker_v2(BROKER_A, _producer_heartbeat(), PRODUCER_GROUP)
    assert transport.last(BROKER_A).without_sub is True, \
        "resetting the consumer stream must not disturb the producer's fingerprint"
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.last(BROKER_A).without_sub is False


# ------------------------------------------------------ failed rounds


def test_rejected_round_propagates_and_leaves_the_stream_unremembered():
    transport = FakeTransport().add(BROKER_A, support_v2=True,
                                    fail_with=ResponseCode.SYSTEM_ERROR)
    mqc = _instance(transport)

    with pytest.raises(MQBrokerException):
        mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)

    state = mqc._heartbeat_v2.state(GROUP)
    assert state.support_v2_broker_addrs == set()
    assert state.broker_addr_heartbeat_fingerprint_table == {}, \
        "remembering a broker that rejected the heartbeat would keep a fingerprint it never stored"

    transport.brokers[BROKER_A].fail_with = None
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.bodies(BROKER_A)[-1].without_sub is False


def test_state_written_before_the_failure_is_not_rolled_back_but_only_on_support():
    """A broker that answers the flags and then fails the *next* round must still be treated as
    a V2 broker: the capability is a property of the broker, so the remembered fingerprint is
    what has to keep the round honest -- and it is refreshed from the body, not from the answer."""
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)

    hb = _heartbeat()
    mqc.send_heartbeat_to_broker_v2(BROKER_A, hb, GROUP)
    fingerprint = hb.heartbeat_fingerprint

    transport.brokers[BROKER_A].fail_with = ResponseCode.SYSTEM_ERROR
    with pytest.raises(MQBrokerException):
        mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)

    state = mqc._heartbeat_v2.state(GROUP)
    assert BROKER_A in state.support_v2_broker_addrs
    assert state.broker_addr_heartbeat_fingerprint_table[BROKER_A] == fingerprint

    transport.brokers[BROKER_A].fail_with = None
    mqc.send_heartbeat_to_broker_v2(BROKER_A, _heartbeat(), GROUP)
    assert transport.last(BROKER_A).without_sub is True


# ------------------------------------------------- response extField parsing


@pytest.mark.parametrize("raw,expected", [
    ("true", True), ("TRUE", True), (" True ", True),
    ("false", False), ("1", False), ("", False), (None, False),
])
def test_support_flag_is_parsed_as_case_insensitive_true_text(raw, expected):
    ext = {} if raw is None else {MixAll.IS_SUPPORT_HEART_BEAT_V2: raw}
    assert HeartbeatV2Result.from_response(1, ext).support_v2 is expected


@pytest.mark.parametrize("raw,expected", [
    ("true", True), ("TRUE", True), (" True ", True),
    ("false", False), ("yes", False), ("", False), (None, False),
])
def test_sub_change_flag_is_parsed_as_case_insensitive_true_text(raw, expected):
    ext = {} if raw is None else {MixAll.IS_SUB_CHANGE: raw}
    assert HeartbeatV2Result.from_response(1, ext).sub_change is expected


def test_missing_ext_fields_block_reads_as_a_pre_v2_broker():
    result = HeartbeatV2Result.from_response(0, None)
    assert result.support_v2 is False and result.sub_change is False
    # A broker that answers only one flag is still a V2 broker without a changed subscription
    # (and the reverse), which is what keeps the two branches independent.
    only_support = HeartbeatV2Result.from_response(
        7, {MixAll.IS_SUPPORT_HEART_BEAT_V2: "true"})
    assert (only_support.support_v2, only_support.sub_change, only_support.version) == (True, False, 7)


# --------------------------------------------- body field round trip


def test_both_v2_fields_survive_the_wire():
    """The broker reads the two flags back out of the body, so an encoder/decoder that drops
    them would silently turn every V2 round into a V1 registration."""
    hb = _heartbeat()
    hb.heartbeat_fingerprint = 123456789
    hb.without_sub = True

    decoded = HeartbeatData.decode(hb.encode())
    assert decoded.heartbeat_fingerprint == 123456789
    assert decoded.without_sub is True

    # Control: a V1 body keeps the two defaults, so it stays byte-compatible with a client
    # that does not opt in.
    plain = HeartbeatData.decode(HeartbeatData(CLIENT_ID).encode())
    assert plain.heartbeat_fingerprint == 0
    assert plain.without_sub is False


def test_fingerprint_ignores_the_per_process_and_per_round_fields():
    base = _heartbeat(client_id="python-v2-unit@1")
    with_stamp = _heartbeat(client_id="python-v2-unit@2")
    for cd in with_stamp.consumer_data_set:
        for s in cd.subscription_data_set:
            s.sub_version = 987654321
    assert base.compute_fingerprint() == with_stamp.compute_fingerprint(), \
        "clientId and subVersion differ per process / per round: hashing them would report a change every heartbeat"

    changed = _heartbeat(topics=(TOPIC, OTHER_TOPIC))
    assert changed.compute_fingerprint() != base.compute_fingerprint()

    other_group = _heartbeat(group="GID_V2Other")
    assert other_group.compute_fingerprint() != base.compute_fingerprint()


# ------------------------------------------------ facade opt-in wiring


def _started_push_consumer(v2: bool) -> tuple:
    consumer = DefaultMQPushConsumer(GROUP)
    consumer.name_server_addrs = list(NS)
    consumer.set_use_heartbeat_v2(v2)
    transport = FakeTransport().add(BROKER_A, support_v2=True)
    mqc = _instance(transport)
    mqc.get_all_broker_addrs = lambda: [BROKER_A]
    consumer._started = True
    consumer._mq_client = mqc
    consumer.subscription_data = {TOPIC: _sub(TOPIC)}
    return consumer, transport, mqc


def test_push_consumer_defaults_to_the_v1_heartbeat():
    consumer, transport, mqc = _started_push_consumer(False)

    assert consumer.use_heartbeat_v2 is False
    assert consumer._send_heartbeat_to_all_broker() == 1

    sent = transport.last(BROKER_A)
    assert sent.heartbeat_fingerprint == 0, \
        "the opt-in default must keep the broker on the V1 registration path"
    assert sent.without_sub is False
    assert mqc._heartbeat_v2.owners() == set()


def test_push_consumer_opted_in_uses_the_fingerprint_path_and_resends_the_light_body():
    consumer, transport, mqc = _started_push_consumer(True)

    assert consumer._send_heartbeat_to_all_broker() == 1
    assert transport.last(BROKER_A).heartbeat_fingerprint != 0
    assert transport.last(BROKER_A).without_sub is False

    assert consumer._send_heartbeat_to_all_broker() == 1
    assert transport.last(BROKER_A).without_sub is True
    assert mqc._heartbeat_v2.owners() == {GROUP}


def test_assignment_change_resets_only_this_group_and_only_when_opted_in():
    """The rebalance path is what makes a fingerprint reset happen in production: after an
    assignment change the next round has to re-deliver the subscriptions. With V2 off there is
    no fingerprint table to reset, so the call must not even be made."""
    for v2, expected_resets in ((True, 1), (False, 0)):
        consumer, _transport, mqc = _started_push_consumer(v2)
        resets = []
        mqc.reset_heartbeat_v2_fingerprint_table = \
            lambda owner=None: resets.append(owner)
        consumer._rebalance_pull_threads = lambda: None
        # The transport here answers heartbeats only; resolve the queue's start explicitly so the
        # rebalance keeps the queue instead of skipping it on an unanswerable offset read.
        consumer._resolve_initial_offset = lambda client, mq: 0
        mqc.get_topic_subscribe_info = lambda topic: [
            MessageQueue(topic=topic, broker_name="broker-a", queue_id=0)]
        consumer.message_model = MessageModel.BROADCASTING

        consumer._do_rebalance()
        # Same allocation again: no change, so no reset.
        consumer._do_rebalance()

        assert resets == ([GROUP] if expected_resets else []), \
            "an unchanged allocation must not reset, and a change must reset this group only"


def test_assignment_change_reshares_the_subscription_version_and_notifies_at_once():
    """A rebalance that moved queues re-shares what this client subscribes to: every subscription gets
    a fresh wall-clock version -- the pull request carries it and the broker reads a higher one as new
    subscription information -- and one heartbeat goes out in that same round instead of waiting for
    the periodic one. An unchanged allocation must do neither."""
    consumer, transport, mqc = _started_push_consumer(False)
    consumer._rebalance_pull_threads = lambda: None
    consumer.message_model = MessageModel.BROADCASTING
    queues = [MessageQueue(topic=TOPIC, broker_name="broker-a", queue_id=0)]
    mqc.get_topic_subscribe_info = lambda topic: list(queues)
    consumer.subscription_data[TOPIC].sub_version = 1234
    for mq in queues:
        consumer._offset_table[consumer._mq_key(mq)] = 0

    consumer._do_rebalance()
    assert transport.bodies(BROKER_A), "an assignment change must notify the broker without waiting"
    assert consumer.subscription_data[TOPIC].sub_version > 1234
    sent = next(iter(transport.last(BROKER_A).consumer_data_set)).subscription_data_set
    assert [s.sub_version for s in sent] == [consumer.subscription_data[TOPIC].sub_version], \
        "the heartbeat must carry the version the next pull will send"

    rounds = len(transport.rounds)
    settled = consumer.subscription_data[TOPIC].sub_version
    consumer._do_rebalance()
    assert len(transport.rounds) == rounds, "an unchanged allocation must not notify again"
    assert consumer.subscription_data[TOPIC].sub_version == settled, \
        "the version moves with the assignment, not with every rebalance round"