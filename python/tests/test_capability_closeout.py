# -*- coding: utf-8 -*-
"""The five capability gaps closed against the Java 5.x reference client.

Each block pins one Java facade to this port, and each pairs the new accessor with
the behavior it is supposed to steer, so a later refactor that leaves the setter
in place but drops its wiring fails here:

  * ConsumeFromWhere -- all six values of the Java enum exist and serialize onto
    the heartbeat verbatim. The three "deprecated" ones are NOT dead: Java folds
    them into the CONSUME_FROM_LAST_OFFSET branch of the offset-init switch, so a
    heartbeat carrying them must round-trip byte for byte.
  * LitePull connectBrokerByUser / defaultBrokerId -- the only way to read from a
    slave. Off must keep the historic "no broker id on the request" shape.
  * Admin adminExtGroup -- default "admin_ext_group" plus the Java accessor pair.
"""
from __future__ import annotations

import json

from client.admin import DEFAULT_ADMIN_EXT_GROUP, DefaultMQAdminExt
from client.consumer import DefaultLitePullConsumer
from common.message import MessageQueue
from common.mix_all import MixAll
from remoting.protocol.heartbeat import ConsumeFromWhere

GROUP = "GID_CapabilityCloseout"
TOPIC = "CapabilityCloseoutTopic"
BROKER = "broker-a"


# ---------------------------------------------------------------- ConsumeFromWhere
# The authoritative list is Java common/.../consumer/ConsumeFromWhere.java; the
# three tail entries share CONSUME_FROM_LAST_OFFSET's branch in
# RebalancePushImpl#computePullFromWhereWithException (lines 171-173).
JAVA_CONSUME_FROM_WHERE = [
    "CONSUME_FROM_LAST_OFFSET",
    "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
    "CONSUME_FROM_MIN_OFFSET",
    "CONSUME_FROM_MAX_OFFSET",
    "CONSUME_FROM_FIRST_OFFSET",
    "CONSUME_FROM_TIMESTAMP",
]


def test_consume_from_where_has_all_six_java_values() -> None:
    for value in JAVA_CONSUME_FROM_WHERE:
        assert getattr(ConsumeFromWhere, value) == value


def test_consume_from_where_survives_the_heartbeat_json() -> None:
    # The wire form is the plain string, so every value has to survive the
    # heartbeat's JSON round-trip without being normalized to LAST_OFFSET.
    for value in JAVA_CONSUME_FROM_WHERE:
        c = DefaultLitePullConsumer(GROUP)
        c.set_consume_from_where(value)
        beat = c._build_heartbeat()
        decoded = json.loads(beat.encode())
        reported = decoded["consumerDataSet"][0]["consumeFromWhere"]
        assert reported == value, (value, reported)


# ---------------------------------------------------------------- LitePull broker pin


def test_lite_pull_broker_pin_defaults_match_java() -> None:
    c = DefaultLitePullConsumer(GROUP)
    assert c.is_connect_broker_by_user() is False
    assert c.get_default_broker_id() == MixAll.MASTER_ID


def test_lite_pull_broker_pin_off_keeps_the_historic_request_shape() -> None:
    # Off means "no broker id on the request" -- MQClientInstance.pull_message then
    # resolves the address from the route, which is what every lite round did
    # before this switch existed. Sending MASTER_ID instead would switch the
    # address lookup to the broker-id path and change the error for a
    # route-missing broker.
    c = DefaultLitePullConsumer(GROUP)
    mq = MessageQueue(TOPIC, BROKER, 0)
    assert c._recalculate_pull_from_which_node(mq) is None


def test_lite_pull_broker_pin_on_reports_the_pinned_id() -> None:
    c = DefaultLitePullConsumer(GROUP)
    mq = MessageQueue(TOPIC, BROKER, 0)
    c.set_connect_broker_by_user(True)
    c.set_default_broker_id(1)
    assert c.is_connect_broker_by_user() is True
    assert c.get_default_broker_id() == 1
    assert c._recalculate_pull_from_which_node(mq) == 1
    # Flipping back restores the unpinned shape rather than latching to 0/1.
    c.set_connect_broker_by_user(False)
    assert c._recalculate_pull_from_which_node(mq) is None


def test_lite_pull_broker_pin_reaches_the_pull_request() -> None:
    # The pin is worthless if _pull_one drops it on the floor: capture what the
    # client instance is asked for.
    c = DefaultLitePullConsumer(GROUP)
    mq = MessageQueue(TOPIC, BROKER, 0)
    seen = []

    class _Client:
        def pull_message(self, *args, **kwargs):
            seen.append(kwargs.get("broker_id"))
            raise RuntimeError("stop here")

    c._mq_client = _Client()
    # Preset the cursor so the round does not have to resolve an initial offset first.
    c._next_offset[mq] = 0
    assert c._pull_one(mq) is False
    assert seen == [None], seen
    seen.clear()
    c.set_connect_broker_by_user(True)
    c.set_default_broker_id(3)
    assert c._pull_one(mq) is False
    assert seen == [3], seen


# ---------------------------------------------------------------- Admin group


def test_admin_ext_group_matches_java() -> None:
    assert DEFAULT_ADMIN_EXT_GROUP == "admin_ext_group"
    admin = DefaultMQAdminExt()
    assert admin.get_admin_ext_group() == "admin_ext_group"
    admin.set_admin_ext_group("custom-admin-group")
    assert admin.get_admin_ext_group() == "custom-admin-group"


def test_admin_ext_group_can_be_set_at_construction() -> None:
    admin = DefaultMQAdminExt(admin_ext_group="ctor-admin-group")
    assert admin.get_admin_ext_group() == "ctor-admin-group"
