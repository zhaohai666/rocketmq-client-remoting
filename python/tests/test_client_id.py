# -*- coding: utf-8 -*-
from __future__ import annotations

import re

import pytest

from client import DefaultMQProducer
from client.admin import DefaultMQAdminExt
from client.consumer import (DefaultLitePullConsumer, DefaultMQPullConsumer,
                                     DefaultMQPushConsumer)
from common.mix_all import MixAll
from remoting.exception import RemotingConnectException
from remoting.protocol.heartbeat import MessageModel

# <IP>@<pid>#<nanoseconds>
CLIENT_ID_PATTERN = re.compile(r"^[^@\s]+@%d#\d+$" % MixAll.pid())
STREAM_CLIENT_ID_PATTERN = re.compile(r"^[^@\s]+@%d#\d+@STREAM$" % MixAll.pid())


def _split(client_id: str):
    ip, _, instance = client_id.partition("@")
    assert instance, "clientId is missing the IP@instanceName separator: %s" % client_id
    return ip, instance


class TestMixAllClientId:
    pass

    def test_build_mq_client_id_is_ip_first(self):
        assert MixAll.build_mq_client_id("10.0.0.1", "inst") == "10.0.0.1@inst"

    def test_unit_name_suffix(self):
        assert MixAll.build_mq_client_id("10.0.0.1", "inst", "unit-a") == "10.0.0.1@inst@unit-a"
        assert MixAll.build_mq_client_id("10.0.0.1", "inst", "   ") == "10.0.0.1@inst"
        assert MixAll.build_mq_client_id("10.0.0.1", "inst", None) == "10.0.0.1@inst"

    def test_change_instance_name_to_pid_only_touches_the_default(self):
        assert MixAll.change_instance_name_to_pid("inst") == "inst"
        rewritten = MixAll.change_instance_name_to_pid(MixAll.DEFAULT_INSTANCE_NAME)
        assert rewritten.startswith("%d#" % MixAll.pid()), rewritten
        assert MixAll.change_instance_name_to_pid(rewritten) == rewritten

    def test_cached_ip_str_is_stable(self):
        assert MixAll.cached_ip_str() == MixAll.cached_ip_str()
        assert MixAll.cached_ip_str()

    def test_client_id_for_prefixes_with_the_local_ip(self):
        ip, instance = _split(MixAll.client_id_for("inst"))
        assert ip == MixAll.cached_ip_str()
        assert instance == "inst"


class TestProducerClientId:
    def test_start_stamps_the_documented_client_id_format(self):
        p = DefaultMQProducer("PID_clientid_shape")
        p.set_namesrv_addr("127.0.0.1:1")
        p.start()
        try:
            assert CLIENT_ID_PATTERN.match(p.client_id), p.client_id
            ip, instance = _split(p.client_id)
            assert ip == MixAll.cached_ip_str()
            # instanceName is written back in place: a restart must not produce a different clientId
            assert p.instance_name == instance
            restarted = p.client_id
        finally:
            p.shutdown()
        p.start()
        try:
            assert p.client_id == restarted
        finally:
            p.shutdown()

    def test_two_producers_in_one_process_do_not_share_a_client_id(self):
        """The old scheme (second-level timestamp) necessarily collides within the same second, and a collision means sharing an INSTANCE_MAP key."""
        a = DefaultMQProducer("PID_clientid_a")
        b = DefaultMQProducer("PID_clientid_b")
        for c in (a, b):
            c.set_namesrv_addr("127.0.0.1:1")
            c.start()
        try:
            assert a.client_id != b.client_id, (a.client_id, b.client_id)
        finally:
            a.shutdown()
            b.shutdown()

    def test_explicit_instance_name_is_kept_verbatim(self):
        p = DefaultMQProducer("PID_clientid_named")
        p.set_instance_name("clientid-parity-fixed")
        p.set_namesrv_addr("127.0.0.1:1")
        p.start()
        try:
            assert p.client_id == "%s@clientid-parity-fixed" % MixAll.cached_ip_str()
            assert p.instance_name == "clientid-parity-fixed"
        finally:
            p.shutdown()

    def test_explicit_client_id_wins(self):
        p = DefaultMQProducer("PID_clientid_explicit")
        p.client_id = "cid-set-by-caller"
        p.set_namesrv_addr("127.0.0.1:1")
        p.start()
        try:
            assert p.client_id == "cid-set-by-caller"
        finally:
            p.shutdown()


class TestConsumerClientId:
    pass

    def test_push_clustering_rewrites_and_broadcast_does_not(self):
        clustering = DefaultMQPushConsumer("CID_clientid_clustering")
        clustering.set_namesrv_addr("127.0.0.1:1")
        clustering.subscribe("T", "TagA")
        clustering.set_message_listener(lambda msgs: None)
        with pytest.raises(RemotingConnectException):
            clustering.start()
        try:
            assert CLIENT_ID_PATTERN.match(clustering.client_id), clustering.client_id
        finally:
            clustering.shutdown()

        broadcast = DefaultMQPushConsumer("CID_clientid_broadcast")
        broadcast.set_namesrv_addr("127.0.0.1:1")
        broadcast.set_message_model(MessageModel.BROADCASTING)
        broadcast.subscribe("T", "TagA")
        broadcast.set_message_listener(lambda msgs: None)
        with pytest.raises(RemotingConnectException):
            broadcast.start()
        try:
            assert broadcast.instance_name == MixAll.DEFAULT_INSTANCE_NAME
            assert broadcast.client_id == "%s@DEFAULT" % MixAll.cached_ip_str()
        finally:
            broadcast.shutdown()

    def test_pull_and_lite_follow_the_same_rule(self):
        pass
        pull = DefaultMQPullConsumer("CID_clientid_pull")
        pull.set_namesrv_addr("127.0.0.1:1")
        pull.start()
        try:
            assert STREAM_CLIENT_ID_PATTERN.match(pull.client_id), pull.client_id
        finally:
            pull.shutdown()

        lite = DefaultLitePullConsumer("CID_clientid_lite")
        lite.set_namesrv_addr("127.0.0.1:1")
        lite.subscribe("T", "*")
        lite.start()
        try:
            assert STREAM_CLIENT_ID_PATTERN.match(lite.client_id), lite.client_id
        finally:
            lite.shutdown()

        lite_broadcast = DefaultLitePullConsumer("CID_clientid_lite_broadcast")
        lite_broadcast.set_namesrv_addr("127.0.0.1:1")
        lite_broadcast.set_message_model(MessageModel.BROADCASTING)
        lite_broadcast.subscribe("T", "*")
        lite_broadcast.start()
        try:
            assert lite_broadcast.client_id == "%s@DEFAULT@STREAM" % MixAll.cached_ip_str()
        finally:
            lite_broadcast.shutdown()

    def test_stream_suffix_can_be_turned_off_and_on(self):
        pass
        off = DefaultMQPullConsumer("CID_clientid_stream_off")
        off.set_instance_name("stream-switch")
        off.set_enable_stream_request_type(False)
        off.set_namesrv_addr("127.0.0.1:1")
        off.start()
        try:
            assert off.client_id == "%s@stream-switch" % MixAll.cached_ip_str()
        finally:
            off.shutdown()

        on = DefaultMQPushConsumer("CID_clientid_stream_on")
        on.set_instance_name("stream-switch")
        on.set_enable_stream_request_type(True)
        on.set_namesrv_addr("127.0.0.1:1")
        on.subscribe("T", "TagA")
        on.set_message_listener(lambda msgs: None)
        with pytest.raises(RemotingConnectException):
            on.start()
        try:
            assert on.client_id == "%s@stream-switch@STREAM" % MixAll.cached_ip_str()
        finally:
            on.shutdown()


class TestUnitNameClientId:
    pass

    def test_pure_string_part(self):
        assert MixAll.build_mq_client_id("10.0.0.1", "inst", "unit-a") == "10.0.0.1@inst@unit-a"
        assert MixAll.build_mq_client_id("10.0.0.1", "inst", "   ") == "10.0.0.1@inst"
        assert MixAll.build_mq_client_id("10.0.0.1", "inst", None) == "10.0.0.1@inst"
        # The two suffix segments have a fixed order: unitName first, then STREAM
        assert (MixAll.build_mq_client_id("10.0.0.1", "inst", "unit-a", True)
                == "10.0.0.1@inst@unit-a@STREAM")

    def test_producer_carries_the_unit_name(self):
        p = DefaultMQProducer("PID_clientid_unit")
        p.set_instance_name("unit-producer")
        p.set_unit_name("unitA")
        p.set_namesrv_addr("127.0.0.1:1")
        p.start()
        try:
            assert p.client_id == "%s@unit-producer@unitA" % MixAll.cached_ip_str()
            # unitName does not participate in changeInstanceNameToPID: instanceName should be kept as-is
            assert p.instance_name == "unit-producer"
            assert p.get_unit_name() == "unitA"
        finally:
            p.shutdown()

    def test_blank_unit_name_is_ignored(self):
        p = DefaultMQProducer("PID_clientid_blank_unit")
        p.set_instance_name("blank-unit")
        p.set_unit_name("  ")
        p.set_namesrv_addr("127.0.0.1:1")
        p.start()
        try:
            assert p.client_id == "%s@blank-unit" % MixAll.cached_ip_str()
        finally:
            p.shutdown()

    def test_two_units_get_two_client_ids(self):
        """Two clients in the same process with the same instanceName but different unitName must not share a clientId —
        otherwise the latter would overwrite the former in INSTANCE_MAP, and heartbeats and rebalance would cross-talk."""
        a = DefaultMQProducer("PID_clientid_unit_a")
        b = DefaultMQProducer("PID_clientid_unit_b")
        for c in (a, b):
            c.set_instance_name("same-instance")
            c.set_namesrv_addr("127.0.0.1:1")
        a.set_unit_name("unitA")
        b.set_unit_name("unitB")
        a.start()
        b.start()
        try:
            assert a.client_id != b.client_id, (a.client_id, b.client_id)
        finally:
            a.shutdown()
            b.shutdown()

    def test_consumer_and_admin_also_take_a_unit_name(self):
        lite = DefaultLitePullConsumer("CID_clientid_unit_lite")
        lite.set_instance_name("unit-lite")
        lite.set_unit_name("unitA")
        lite.set_namesrv_addr("127.0.0.1:1")
        lite.subscribe("T", "*")
        lite.start()
        try:
            assert lite.client_id == "%s@unit-lite@unitA@STREAM" % MixAll.cached_ip_str()
        finally:
            lite.shutdown()

        admin = DefaultMQAdminExt()
        admin.set_unit_name("unitA")
        admin.set_namesrv_addr("127.0.0.1:1")
        admin.start()
        try:
            assert admin.client_id == "%s@ADMIN@unitA" % MixAll.cached_ip_str()
        finally:
            admin.shutdown()



class TestAdminClientId:
    def test_admin_keeps_its_own_instance_name(self):
        pass
        admin = DefaultMQAdminExt()
        admin.set_namesrv_addr("127.0.0.1:1")
        admin.start()
        try:
            ip, instance = _split(admin.client_id)
            assert ip == MixAll.cached_ip_str()
            assert instance == "ADMIN"
        finally:
            admin.shutdown()

    def test_admin_rewrites_a_default_instance_name(self):
        admin = DefaultMQAdminExt()
        admin.set_instance_name(MixAll.DEFAULT_INSTANCE_NAME)
        admin.set_namesrv_addr("127.0.0.1:1")
        admin.start()
        try:
            assert CLIENT_ID_PATTERN.match(admin.client_id), admin.client_id
        finally:
            admin.shutdown()


@pytest.mark.parametrize("factory", [
    lambda: DefaultMQProducer("PID_clientid_dead_group"),
])
def test_failed_start_does_not_leave_a_stamped_client_id_behind(factory):
    """When start() fails before address validation, the rename must not have happened yet: both the rename and clientId assembly happen after validation."""
    c = factory()
    with pytest.raises(Exception):
        c.start()
    assert c.client_id is None
    assert c.instance_name == MixAll.DEFAULT_INSTANCE_NAME
