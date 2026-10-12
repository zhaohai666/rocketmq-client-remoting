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
    """构造一个已分配队列、已消费到某位点的消费者（不调 start，避免联网）。"""
    c = DefaultMQPushConsumer(GROUP)
    c.name_server_addrs = ["127.0.0.1:9876"]
    c._start_time = time.time()                       # start() 里才会设，这里补上
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


# ---------------------------------------------------------------- body 往返


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


# ---------------------------------------------------------------- 消费者方法


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
        assert info.subscription_set  # 非空

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
        assert ("revoked", [(mq(), 100)]) in events   # persist 带上的是新位点
        assert "rebalance" in events


# ---------------------------------------------------------------- MQClientInstance 分派


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
        """220 是 oneway（返回 None），且重置逻辑丢到后台线程，不阻塞读线程。"""
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
        assert resp is None, "220 是 oneway，必须返回 None"
        # 后台线程应已（或即将）调用到 reset_offset
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
        assert done.wait(timeout=2.0), "数组形状的 220 被静默丢弃了"
        assert seen["topic"] == TOPIC
        assert seen["table"] == {mq(queue_id=1): 9}

    def test_220_map_body_is_not_double_counted(self):
        """负向对照：map 形状仍按原样解析（不能被兜底路径改写/翻倍）。"""
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
        # 处理器必须是实例自己那份，而不是某个消费者的方法
        assert mqc.remoting_client._processors[NOTIFY_CONSUMER_IDS_CHANGED] \
            == mqc._process_notify_consumer_ids_changed

        cmd = fake_cmd(NOTIFY_CONSUMER_IDS_CHANGED, {"consumerGroup": GROUP})
        resp = mqc._process_notify_consumer_ids_changed(cmd, "127.0.0.1:10911")
        assert resp is None, "40 是 broker 的 oneway 通知，Java 返回 null ⇒ 不回包"
        assert mqc.consumer_ids_changed_count == 1
        assert c1._rebalance_now.is_set() and c2._rebalance_now.is_set(), \
            "整组一起唤醒（Java rebalanceImmediately 不读 consumerGroup）"

    def test_40_with_no_group_or_consumer_is_silent(self):
        mqc = MQClientInstance("client@40-empty", ["127.0.0.1:9876"])
        cmd = fake_cmd(NOTIFY_CONSUMER_IDS_CHANGED, {})
        assert mqc._process_notify_consumer_ids_changed(cmd, "127.0.0.1:10911") is None
        assert mqc.consumer_ids_changed_count == 1

        class NoRebalance:
            """没注册过的对象不该被叫醒，也不能让通知处理抛出去。"""

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
