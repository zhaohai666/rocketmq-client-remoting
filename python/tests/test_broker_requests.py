# -*- coding: utf-8 -*-
from __future__ import annotations

import time
import threading

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import (ConsumeConcurrentlyContext,
                                             ConsumeConcurrentlyStatus,
                                             ConsumeOrderlyStatus,
                                             MessageListener, MessageListenerOrderly)
from client.mq_client import MQClientInstance
from common.message import MessageExt, MessageQueue
from common.message_decoder import decode_message, encode_message_ext
from remoting.protocol.body import (CMResult, ConsumeMessageDirectlyResult,
                                             ConsumerRunningInfo, GetConsumerStatusBody,
                                             MessageQueueForC, ProcessQueueInfo,
                                             ResetOffsetBody, ResetOffsetBodyForC)
from remoting.protocol.codes import (RequestCode, ResponseCode)

GET_CONSUMER_RUNNING_INFO = RequestCode.GET_CONSUMER_RUNNING_INFO
GET_CONSUMER_STATUS_FROM_CLIENT = RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT
RESET_CONSUMER_CLIENT_OFFSET = RequestCode.RESET_CONSUMER_CLIENT_OFFSET
CONSUME_MESSAGE_DIRECTLY = RequestCode.CONSUME_MESSAGE_DIRECTLY
NOTIFY_CONSUMER_IDS_CHANGED = RequestCode.NOTIFY_CONSUMER_IDS_CHANGED
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.headers import (ConsumeMessageDirectlyResultRequestHeader,
                                                GetConsumerRunningInfoRequestHeader,
                                                GetConsumerStatusRequestHeader,
                                                ResetOffsetRequestHeader)
from common.subscription_data import SubscriptionData
from remoting.protocol.heartbeat import HeartbeatData

GROUP = "GID_BrokerReqUnit"
TOPIC = "BrokerReqTopic"
BROKER = "broker-a"


def mq(topic: str = TOPIC, queue_id: int = 0) -> MessageQueue:
    return MessageQueue(topic=topic, broker_name=BROKER, queue_id=queue_id)


def sub(topic: str = TOPIC) -> SubscriptionData:
    s = SubscriptionData(topic=topic, sub_string="*")
    s.tags_set.add("*")
    return s


def consumer_with_state() -> DefaultMQPushConsumer:
    """Build a consumer with assigned queues and a consumed offset (without calling start, to avoid networking)."""
    c = DefaultMQPushConsumer(GROUP)
    c.name_server_addrs = ["127.0.0.1:9876"]
    c._start_time = time.time()                       # normally set inside start(); filled in here
    c.namespace = None
    c.subscription_data = {TOPIC: sub()}
    q = mq()
    key = "%s@%s@%d" % (q.topic, q.broker_name, q.queue_id)
    c._mq_map = {key: q}
    c._consume_offsets = {key: 42}
    c._pending = {key: []}
    c._pop_queues = {}
    return c


def listener_returning(status: ConsumeConcurrentlyStatus):
    class L(MessageListener):
        def consume_message(self, msgs, context):
            return status
    return L()


# ---------------------------------------------------------------- body round trip


class TestBodyRoundTrip:
    def test_reset_offset_body(self):
        b = ResetOffsetBody()
        b.offset_table = {mq(): 7}
        d = ResetOffsetBody.decode(b.encode())
        assert len(d.offset_table) == 1
        m = next(iter(d.offset_table))
        assert m.topic == TOPIC and m.queue_id == 0 and d.offset_table[m] == 7

    def test_reset_offset_body_for_c_round_trip(self):
        pass
        b = ResetOffsetBodyForC()
        b.offset_table = [MessageQueueForC(topic=TOPIC, broker_name=BROKER,
                                           queue_id=1, offset=9)]
        d = ResetOffsetBodyForC.decode(b.encode())
        assert [(e.topic, e.broker_name, e.queue_id, e.offset) for e in d.offset_table] \
            == [(TOPIC, BROKER, 1, 9)]

    def test_get_consumer_status_body(self):
        b = GetConsumerStatusBody()
        b.message_queue_table = {mq(): 11}
        d = GetConsumerStatusBody.decode(b.encode())
        m = next(iter(d.message_queue_table))
        assert d.message_queue_table[m] == 11

    def test_consumer_running_info_body(self):
        info = ConsumerRunningInfo()
        info.properties = {ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP: "123"}
        info.subscription_set = [sub().to_dict()]
        pqi = ProcessQueueInfo()
        pqi.commit_offset = 99
        info.mq_table = {mq(): pqi.to_dict()}
        d = ConsumerRunningInfo.decode(info.encode())
        assert d.properties.get(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP) == "123"
        assert len(d.mq_table) == 1
        m = next(iter(d.mq_table))
        assert m.topic == TOPIC and d.mq_table[m]["commitOffset"] == 99

    def test_consume_message_directly_result_body(self):
        r = ConsumeMessageDirectlyResult()
        r.order = False
        r.auto_commit = True
        r.consume_result = CMResult.CR_SUCCESS
        r.remark = "ok"
        r.spent_time_mills = 5
        d = ConsumeMessageDirectlyResult.decode(r.encode())
        assert d.consume_result == CMResult.CR_SUCCESS
        assert d.spent_time_mills == 5 and d.auto_commit is True


# ---------------------------------------------------------------- consumer methods


class TestConsumerHandlers:
    def test_get_consumer_status(self):
        c = consumer_with_state()
        out = c.get_consumer_status(TOPIC)
        assert len(out) == 1
        m = next(iter(out))
        assert out[m] == 42

    def test_get_consumer_status_other_topic_empty(self):
        c = consumer_with_state()
        assert c.get_consumer_status("OtherTopic") == {}

    def test_consumer_running_info(self):
        c = consumer_with_state()
        info = c.consumer_running_info()
        assert info.properties[ConsumerRunningInfo.PROP_NAMESERVER_ADDR] == "127.0.0.1:9876;"
        assert info.properties[ConsumerRunningInfo.PROP_CONSUME_ORDERLY] == "false"
        assert len(info.mq_table) == 1
        assert info.subscription_set  # non-empty

    def test_consume_message_directly_success(self):
        c = consumer_with_state()
        c.set_message_listener(listener_returning(ConsumeConcurrentlyStatus.CONSUME_SUCCESS))
        m = MessageExt(topic=TOPIC, body=b"hi")
        r = c.consume_message_directly(m, BROKER)
        assert r.consume_result == CMResult.CR_SUCCESS
        assert r.order is False and r.auto_commit is True

    def test_consume_message_directly_later(self):
        c = consumer_with_state()
        c.set_message_listener(listener_returning(ConsumeConcurrentlyStatus.RECONSUME_LATER))
        r = c.consume_message_directly(MessageExt(topic=TOPIC, body=b"x"), BROKER)
        assert r.consume_result == CMResult.CR_LATER

    def test_consume_message_directly_none(self):
        class L(MessageListener):
            def consume_message(self, msgs, context):
                return None
        c = consumer_with_state()
        c.set_message_listener(L())
        r = c.consume_message_directly(MessageExt(topic=TOPIC, body=b"x"), BROKER)
        assert r.consume_result == CMResult.CR_RETURN_NULL

    def test_consume_message_directly_throws(self):
        class L(MessageListener):
            def consume_message(self, msgs, context):
                raise RuntimeError("boom")
        c = consumer_with_state()
        c.set_message_listener(L())
        r = c.consume_message_directly(MessageExt(topic=TOPIC, body=b"x"), BROKER)
        assert r.consume_result == CMResult.CR_THROW_EXCEPTION
        assert "boom" in (r.remark or "")

    def test_consume_message_directly_orderly_maps_commit_rollback(self):
        pass
        for status, expect in (
            (ConsumeOrderlyStatus.SUCCESS, CMResult.CR_SUCCESS),
            (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT, CMResult.CR_LATER),
            (ConsumeOrderlyStatus.COMMIT, CMResult.CR_COMMIT),
            (ConsumeOrderlyStatus.ROLLBACK, CMResult.CR_ROLLBACK),
        ):
            class L(MessageListenerOrderly):
                def consume_message(self, msgs, context):
                    context.auto_commit = False
                    return status
            c = consumer_with_state()
            c.set_message_listener(L())
            r = c.consume_message_directly(MessageExt(topic=TOPIC, body=b"x"), BROKER)
            assert r.consume_result == expect, status
            assert r.order is True, status
            assert r.auto_commit is False, status

    def test_consume_message_directly_orderly_none_is_return_null(self):
        pass
        c = consumer_with_state()
        c.set_message_listener(None)
        r = c.consume_message_directly(MessageExt(topic=TOPIC, body=b"x"), BROKER)
        assert r.consume_result == CMResult.CR_RETURN_NULL

    def test_reset_offset_writes_position(self):
        pass
        c = consumer_with_state()
        events = []
        key = "%s@%s@%d" % (TOPIC, BROKER, 0)
        assert c._queue_epoch.get(key, 0) == 0
        c._do_rebalance = lambda: events.append("rebalance")  # type: ignore[method-assign]
        c._on_queues_revoked = lambda qs: events.append(("revoked", qs))  # type: ignore[method-assign]
        c.reset_offset(TOPIC, {mq(): 100})
        assert key not in c._consume_offsets
        assert key not in c._mq_map
        assert c._queue_epoch[key] == 1
        assert ("revoked", [(mq(), 100)]) in events   # persist carries the new offset
        assert "rebalance" in events


# ---------------------------------------------------------------- MQClientInstance dispatch


def fake_cmd(code: int, ext: dict, body: bytes = None) -> RemotingCommand:
    cmd = RemotingCommand(code=code, body=body)
    cmd.ext_fields = {k: str(v) if isinstance(v, int) else v for k, v in ext.items()}
    return cmd


class TestMqClientDispatch:
    def _client_with_consumer(self, c: DefaultMQPushConsumer) -> MQClientInstance:
        mqc = MQClientInstance("client@id", ["127.0.0.1:9876"])
        mqc.register_consumer(c.consumer_group, c)
        return mqc

    def test_221_dispatch(self):
        c = consumer_with_state()
        mqc = self._client_with_consumer(c)
        cmd = fake_cmd(GET_CONSUMER_STATUS_FROM_CLIENT,
                       {"topic": TOPIC, "group": GROUP, "clientAddr": "1.2.3.4"})
        resp = mqc._process_get_consumer_status(cmd, "127.0.0.1:10911")
        assert resp is not None and resp.code == ResponseCode.SUCCESS
        body = GetConsumerStatusBody.decode(resp.body)
        assert len(body.message_queue_table) == 1

    def test_307_dispatch(self):
        c = consumer_with_state()
        mqc = self._client_with_consumer(c)
        cmd = fake_cmd(GET_CONSUMER_RUNNING_INFO,
                       {"consumerGroup": GROUP, "clientId": "client@id"})
        resp = mqc._process_get_consumer_running_info(cmd, "127.0.0.1:10911")
        assert resp is not None and resp.code == ResponseCode.SUCCESS
        info = ConsumerRunningInfo.decode(resp.body)
        assert len(info.mq_table) == 1

    def test_309_dispatch(self):
        c = consumer_with_state()
        c.set_message_listener(listener_returning(ConsumeConcurrentlyStatus.CONSUME_SUCCESS))
        mqc = self._client_with_consumer(c)
        msg = MessageExt(topic=TOPIC, body=b"direct")
        msg.queue_id = 0
        raw = encode_message_ext(msg, need_compress=False)
        cmd = fake_cmd(CONSUME_MESSAGE_DIRECTLY,
                       {"consumerGroup": GROUP, "clientId": "client@id",
                        "brokerName": BROKER}, body=raw)
        resp = mqc._process_consume_message_directly(cmd, "127.0.0.1:10911")
        assert resp is not None and resp.code == ResponseCode.SUCCESS
        r = ConsumeMessageDirectlyResult.decode(resp.body)
        assert r.consume_result == CMResult.CR_SUCCESS

    def test_220_dispatch_is_oneway_and_async(self):
        """220 is oneway (returns None), and the reset logic is dispatched to a background thread without blocking the read thread."""
        recorded = {"called": False, "event": threading.Event()}
        c = consumer_with_state()

        def reset(topic, offset_table):
            recorded["called"] = True
            recorded["event"].set()

        c.reset_offset = reset  # type: ignore[method-assign]
        mqc = self._client_with_consumer(c)
        body = ResetOffsetBody()
        body.offset_table = {mq(): 5}
        cmd = fake_cmd(RESET_CONSUMER_CLIENT_OFFSET,
                       {"topic": TOPIC, "group": GROUP, "timestamp": 0, "isForce": True},
                       body=body.encode())
        resp = mqc._process_reset_offset(cmd, "127.0.0.1:10911")
        assert resp is None, "220 is oneway and must return None"
        # The background thread should have (or be about to) call reset_offset
        recorded["event"].wait(timeout=2.0)
        assert recorded["called"] is True

    def test_220_array_body_for_c_is_not_dropped(self):
        pass
        seen = {}
        done = threading.Event()
        c = consumer_with_state()

        def reset(topic, offset_table):
            seen["topic"] = topic
            seen["table"] = dict(offset_table)
            done.set()

        c.reset_offset = reset  # type: ignore[method-assign]
        mqc = self._client_with_consumer(c)
        raw = b'{"offsetTable":[{"topic":"' + TOPIC.encode() + b'","brokerName":"' \
            + BROKER.encode() + b'","queueId":1,"offset":9}]}'
        cmd = fake_cmd(RESET_CONSUMER_CLIENT_OFFSET,
                       {"topic": TOPIC, "group": GROUP, "timestamp": 0, "isForce": True},
                       body=raw)
        assert mqc._process_reset_offset(cmd, "127.0.0.1:10911") is None
        assert done.wait(timeout=2.0), "the array-shaped 220 was silently dropped"
        assert seen["topic"] == TOPIC
        assert seen["table"] == {mq(queue_id=1): 9}

    def test_220_map_body_is_not_double_counted(self):
        """Negative control: the map shape is still parsed as-is (must not be rewritten/doubled by the fallback path)."""
        seen = {}
        done = threading.Event()
        c = consumer_with_state()

        def reset(topic, offset_table):
            seen["table"] = dict(offset_table)
            done.set()

        c.reset_offset = reset  # type: ignore[method-assign]
        mqc = self._client_with_consumer(c)
        body = ResetOffsetBody()
        body.offset_table = {mq(queue_id=2): 4}
        cmd = fake_cmd(RESET_CONSUMER_CLIENT_OFFSET,
                       {"topic": TOPIC, "group": GROUP}, body=body.encode())
        assert mqc._process_reset_offset(cmd, "127.0.0.1:10911") is None
        assert done.wait(timeout=2.0)
        assert seen["table"] == {mq(queue_id=2): 4}

    def test_220_garbage_body_is_still_an_empty_noop(self):
        pass
        seen = {}
        done = threading.Event()
        c = consumer_with_state()

        def reset(topic, offset_table):
            seen["table"] = dict(offset_table)
            done.set()

        c.reset_offset = reset  # type: ignore[method-assign]
        mqc = self._client_with_consumer(c)
        cmd = fake_cmd(RESET_CONSUMER_CLIENT_OFFSET,
                       {"topic": TOPIC, "group": GROUP}, body=b'{"unrelated":1}')
        assert mqc._process_reset_offset(cmd, "127.0.0.1:10911") is None
        assert done.wait(timeout=2.0)
        assert seen["table"] == {}

    def test_220_undecodable_body_never_reaches_reset_offset(self):
        pass
        called = []
        c = consumer_with_state()
        c.reset_offset = lambda topic, table: called.append((topic, table))  # type: ignore[method-assign]
        mqc = self._client_with_consumer(c)
        cmd = fake_cmd(RESET_CONSUMER_CLIENT_OFFSET,
                       {"topic": TOPIC, "group": GROUP}, body=b'not json at all')
        assert mqc._process_reset_offset(cmd, "127.0.0.1:10911") is None
        assert called == []

    def test_40_is_instance_level_and_wakes_every_consumer(self):
        pass
        c1 = consumer_with_state()
        c2 = consumer_with_state()
        mqc = MQClientInstance("client@40", ["127.0.0.1:9876"])
        mqc.register_consumer(GROUP, c1)
        mqc.register_consumer("GID_Other", c2)
        # The processor must be the instance's own, not some consumer's method
        assert mqc.remoting_client._processors[NOTIFY_CONSUMER_IDS_CHANGED] \
            == mqc._process_notify_consumer_ids_changed

        cmd = fake_cmd(NOTIFY_CONSUMER_IDS_CHANGED, {"consumerGroup": GROUP})
        resp = mqc._process_notify_consumer_ids_changed(cmd, "127.0.0.1:10911")
        assert resp is None, "40 is a broker oneway notification and must not reply"
        assert mqc.consumer_ids_changed_count == 1
        assert c1._rebalance_now.is_set() and c2._rebalance_now.is_set(), \
            "the whole group is woken together (rebalanceImmediately does not read consumerGroup)"

    def test_40_with_no_group_or_consumer_is_silent(self):
        mqc = MQClientInstance("client@40-empty", ["127.0.0.1:9876"])
        cmd = fake_cmd(NOTIFY_CONSUMER_IDS_CHANGED, {})
        assert mqc._process_notify_consumer_ids_changed(cmd, "127.0.0.1:10911") is None
        assert mqc.consumer_ids_changed_count == 1

        class NoRebalance:
            """An unregistered object must not be woken, and must not make the notification handler throw."""

        mqc.register_consumer(GROUP, None)
        mqc._consumer_table["GID_None"] = NoRebalance()
        mqc.rebalance_immediately()
        assert mqc.consumer_ids_changed_count == 1

    def test_dispatch_unknown_group_returns_error(self):
        mqc = MQClientInstance("client@id", ["127.0.0.1:9876"])
        cmd = fake_cmd(GET_CONSUMER_STATUS_FROM_CLIENT,
                       {"topic": TOPIC, "group": "nobody", "clientAddr": "x"})
        resp = mqc._process_get_consumer_status(cmd, "127.0.0.1:10911")
        assert resp is not None and resp.code == ResponseCode.SYSTEM_ERROR


# ------------------------------------------------------- VIP channel address rewrite

VIP_BROKER = "127.0.0.1:10911"
VIP_PORT = "127.0.0.1:10909"


def _recording_client(vip: bool) -> tuple:
    """An instance whose transport only records which address each request actually hit."""
    mqc = MQClientInstance("client@vip", ["127.0.0.1:9876"], vip_channel_enabled=vip)
    seen = []

    def fake_invoke(addr, request, timeout_millis=None):
        seen.append((addr, request.code))
        r = RemotingCommand()
        r.code = ResponseCode.SUCCESS
        return r

    mqc.remoting_client.invoke_sync = fake_invoke
    return mqc, seen


def test_topic_admin_rpc_uses_the_vip_port():
    mqc, seen = _recording_client(True)
    mqc.create_topic_in_broker(VIP_BROKER, "DefaultTopic", TOPIC)
    assert seen[-1] == (VIP_PORT, RequestCode.UPDATE_AND_CREATE_TOPIC)
    mqc.delete_topic_in_broker(VIP_BROKER, TOPIC)
    assert seen[-1] == (VIP_PORT, RequestCode.DELETE_TOPIC_IN_BROKER)

    # Switch off: the same RPCs go to the port the route table published
    plain, plain_seen = _recording_client(False)
    plain.create_topic_in_broker(VIP_BROKER, "DefaultTopic", TOPIC)
    assert plain_seen[-1] == (VIP_BROKER, RequestCode.UPDATE_AND_CREATE_TOPIC)
    plain.delete_topic_in_broker(VIP_BROKER, TOPIC)
    assert plain_seen[-1] == (VIP_BROKER, RequestCode.DELETE_TOPIC_IN_BROKER)


def test_heartbeat_and_nameserver_rpc_never_use_the_vip_port():
    mqc, seen = _recording_client(True)
    mqc.send_heartbeat(VIP_BROKER, HeartbeatData())
    assert seen[-1] == (VIP_BROKER, RequestCode.HEART_BEAT), \
        "a heartbeat rewritten to port - 2 lands on a channel that has no subscription registration"

    mqc.send_heartbeat_v2(VIP_BROKER, HeartbeatData())
    assert seen[-1][0] == VIP_BROKER, "the V2 body rides the same unrewritten heartbeat frame"

    mqc.unregister_client(VIP_BROKER, "client@vip", GROUP, "")
    assert seen[-1] == (VIP_BROKER, RequestCode.UNREGISTER_CLIENT)
