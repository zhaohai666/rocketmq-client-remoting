# -*- coding: utf-8 -*-
"""SEND_MESSAGE_V2(310) 头里那三个"来自路由 / producer 配置"的字段。

Java 锚点（5.5.1 逐条核对）：

  * ``SendMessageRequestHeaderV2``:69 ``@CFNullable private String n; // brokerName``，
    ``encode()``:128 按 ``writeIfNotNull(out, "n", n)`` 写字母键，
    ``createSendMessageRequestHeaderV1/V2``:86/:105 在 V1/V2 之间搬 ``brokerName``。
    值来自 ``DefaultMQProducerImpl.sendKernelImpl``:1007 ``requestHeader.setBrokerName(brokerName)``，
    也就是**路由选中的那台 broker 的名字**（不是地址）。
  * 同类 :996-997 ``setDefaultTopic(producer.getCreateTopicKey())`` /
    ``setDefaultTopicQueueNums(producer.getDefaultTopicQueueNums())`` —— 这两个值由
    **生产者配置**给出，字母键分别是 ``c`` / ``d``。写死成 ``TBW102``/4 的话
    ``set_create_topic_key`` 与 ``set_default_topic_queue_nums`` 就成了假 setter：
    broker 侧 ``TopicQueueMappingManager``/``registerTopicInBroker`` 用 ``c``+``d``
    在自动建 topic 时决定队列数，配置根本传不过去。
  * ``n`` 的 broker 侧消费点：经典 broker 的 ``SendMessageProcessor`` 不读它（寻址靠
    连接本身），它主要是 **V1/V2 之间的报文对齐**与控制台/轨迹侧的排障字段 —— 所以
    这一项按"报文一致性"来测，不假装它有业务行为。
"""
from __future__ import annotations

import threading
from typing import List, Optional

from rocketmq.client.mq_client import MQClientInstance, TopicPublishInfo
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendResult, SendStatus
from rocketmq.common.message import Message, MessageBatch, MessageQueue
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.codes import RequestCode

MQ = MessageQueue("T_Header", "broker-a", 3)
ADDR = "127.0.0.1:10911"


def _new_instance(prefix: str) -> MQClientInstance:
    """只构造、不 start() 的实例：不建连、不发心跳，纯离线断言。"""
    return MQClientInstance("%s_%d" % (prefix, threading.get_ident()),
                            ["127.0.0.1:9876"])


def _drop(inst: MQClientInstance) -> None:
    MQClientInstance.INSTANCE_MAP.pop(inst.client_id, None)


# ---------------------------------------------------------------- 报文形状
def test_send_header_carries_broker_name_as_the_single_letter_n():
    """Java sendKernelImpl:1007 → V2 的 ``n``；值取自路由选中的 mq。"""
    inst = _new_instance("hdr_broker")
    try:
        req = inst._build_send_request("PG", Message("T_Header", b"body"), MQ, 3000, 0)
        assert req.custom_header.broker_name == "broker-a"
        req.make_custom_header_to_net()
        assert req.ext_fields["n"] == "broker-a"
        assert req.code == RequestCode.SEND_MESSAGE_V2
    finally:
        _drop(inst)


def test_broker_name_is_omitted_when_the_queue_has_none():
    """``n`` 是 @CFNullable：没有 broker 名就不写字段，不能发空串。

    Java 的 ``writeIfNotNull`` 只跳过 null；我们把空串也归一成 None，
    否则报文里会出现 ``n=""`` 这种 Java 侧解不出来的形状。
    """
    inst = _new_instance("hdr_no_broker")
    try:
        req = inst._build_send_request("PG", Message("T_Header", b"body"),
                                       MessageQueue("T_Header", "", 0), 3000, 0)
        assert req.custom_header.broker_name is None
        req.make_custom_header_to_net()
        assert "n" not in req.ext_fields
    finally:
        _drop(inst)


def test_default_topic_and_queue_nums_follow_the_caller():
    """c/d 来自生产者配置（Java :996-997），不传才落回 TBW102/4。"""
    inst = _new_instance("hdr_cd")
    msg = Message("T_Header", b"body")
    try:
        on = inst._build_send_request("PG", msg, MQ, 3000, 0, False,
                                      "CREATED_BY_ME", 8)
        on.make_custom_header_to_net()
        assert on.ext_fields["c"] == "CREATED_BY_ME"
        assert on.ext_fields["d"] == "8"

        off = inst._build_send_request("PG", msg, MQ, 3000, 0)
        off.make_custom_header_to_net()
        assert off.ext_fields["c"] == MixAll.DEFAULT_TOPIC == "TBW102"
        # 0 是合法配置值，不能被 `or` 当成"没传"
        zero = inst._build_send_request("PG", msg, MQ, 3000, 0, False, None, 0)
        zero.make_custom_header_to_net()
        assert zero.ext_fields["d"] == "0"
    finally:
        _drop(inst)


def test_batch_and_reply_paths_keep_the_same_three_fields():
    """批量走 320、应答走 325，但 n/c/d 是同一份 header，不能漏。"""
    inst = _new_instance("hdr_batch")
    batch = MessageBatch.generate_from_list([Message("T_Header", b"a"),
                                             Message("T_Header", b"b")])
    batch.set_body(batch.encode())
    try:
        req = inst._build_send_request("PG", batch, MQ, 3000, 0, False, "MY_KEY", 12)
        assert req.code == RequestCode.SEND_BATCH_MESSAGE
        req.make_custom_header_to_net()
        assert (req.ext_fields["n"], req.ext_fields["c"], req.ext_fields["d"]) == (
            "broker-a", "MY_KEY", "12")
        assert req.ext_fields["m"] == "true"
    finally:
        _drop(inst)


# ---------------------------------------------------------------- facade 接通
class _Recorder:
    """MQClientInstance 替身：把三条发送入口收到的头参数原样记下来。

    签名故意写全而不是 ``**kwargs``：facade 少传/错传某个字段时要立刻 TypeError，
    而不是被 ``**`` 悄悄吞掉。
    """

    def __init__(self):
        self.publish = TopicPublishInfo()
        self.publish.msg_queue_list = [MQ]
        self.sync: List[dict] = []
        self.oneway: List[dict] = []
        self.built: List[dict] = []

    def register_topic_in_use(self, topic):
        pass

    def get_topic_publish_info(self, topic, is_default=False):
        return self.publish

    def broker_addr_of(self, broker_name):
        return ADDR

    def get_topic_route_data(self, topic):
        return None

    def send_message(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                     default_topic=None, default_topic_queue_nums=None):
        self.sync.append({"unit_mode": unit_mode, "default_topic": default_topic,
                          "default_topic_queue_nums": default_topic_queue_nums,
                          "mq": mq})
        return SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq)

    def send_message_oneway(self, group, msg, mq, addr, timeout, sys_flag,
                            unit_mode=False, default_topic=None,
                            default_topic_queue_nums=None):
        self.oneway.append({"unit_mode": unit_mode, "default_topic": default_topic,
                            "default_topic_queue_nums": default_topic_queue_nums,
                            "mq": mq})

    def build_send_request(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                           default_topic=None, default_topic_queue_nums=None):
        self.built.append({"unit_mode": unit_mode, "default_topic": default_topic,
                           "default_topic_queue_nums": default_topic_queue_nums,
                           "mq": mq})
        from rocketmq.remoting.protocol.remoting_command import RemotingCommand
        return RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2, None)

    def send_message_async(self, addr, request, msg, mq, timeout, on_complete):
        on_complete(SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq), None)

    def shutdown(self):
        pass


def _producer(create_topic_key: Optional[str] = None,
              default_topic_queue_nums: Optional[int] = None) -> DefaultMQProducer:
    p = DefaultMQProducer("GID_header_test")
    p.set_namesrv_addr("127.0.0.1:9876")
    if create_topic_key is not None:
        p.set_create_topic_key(create_topic_key)
    if default_topic_queue_nums is not None:
        p.set_default_topic_queue_nums(default_topic_queue_nums)
    # start() 会建真实客户端，这里只补齐异步链需要的三样：两个池 + started 标记
    p._create_async_executors()
    p._mq_client = _Recorder()
    p._started = True
    return p


class _Latch:
    """SendCallback 替身：把回调落成一次 Event。"""

    def __init__(self, done: threading.Event):
        self._done = done
        self.result = None
        self.error: Optional[BaseException] = None

    def on_success(self, send_result: SendResult) -> None:
        self.result = send_result
        self._done.set()

    def on_exception(self, e: BaseException) -> None:
        self.error = e
        self._done.set()


def test_sync_send_forwards_producer_config():
    """set_create_topic_key / set_default_topic_queue_nums 必须真的上线（旧代码是假 setter）。"""
    p = _producer("MY_KEY", 16)
    p.send(Message("T_Header", b"x"))
    seen = p._mq_client.sync[-1]
    assert seen["default_topic"] == "MY_KEY"
    assert seen["default_topic_queue_nums"] == 16


def test_defaults_are_java_defaults_when_setters_never_run():
    p = _producer()
    p.send(Message("T_Header", b"x"))
    seen = p._mq_client.sync[-1]
    assert seen["default_topic"] == MixAll.DEFAULT_TOPIC
    assert seen["default_topic_queue_nums"] == MixAll.DEFAULT_TOPIC_QUEUE_NUMS
    assert seen["unit_mode"] is False


class _FirstQueue:
    """MessageQueueSelector 替身（producer.send_by_selector 走 .select 方法）。"""

    def select(self, mqs, msg, arg):
        return mqs[0]


def test_every_send_entry_forwards_the_same_config():
    """定点 / 轮询 / selector / oneway / 批量 / 异步都走 _send_header_args，漏一个就是一致性 bug。"""
    p = _producer("K2", 7)
    p.set_back_pressure_for_async_send_num(1000)
    p.set_back_pressure_for_async_send_size(1000 * 1024 * 1024)
    client = p._mq_client
    try:
        p.send(Message("T_Header", b"x"), mq=MQ)
        p.send(Message("T_Header", b"x"))
        p.send_by_selector(Message("T_Header", b"x"), _FirstQueue(), None)
        p.send_oneway(Message("T_Header", b"x"))
        p.send([Message("T_Header", b"a"), Message("T_Header", b"b")])
        done = threading.Event()
        p.send_async(Message("T_Header", b"x"), _Latch(done), 3000)
        assert done.wait(5.0), "async send never came back"
    finally:
        p.shutdown()

    records = client.sync + client.oneway + client.built
    # 2 次同步 + 1 次 selector + 1 次批量 = 4；oneway 1；异步 1
    assert (len(client.sync), len(client.oneway), len(client.built)) == (4, 1, 1), (
        len(client.sync), len(client.oneway), len(client.built))
    for rec in records:
        assert rec["default_topic"] == "K2", rec
        assert rec["default_topic_queue_nums"] == 7, rec
        assert rec["unit_mode"] is False, rec
        # brokerName 不在参数里：它跟着 mq 走，由 _build_send_request 现取
        assert rec["mq"].broker_name == "broker-a", rec
