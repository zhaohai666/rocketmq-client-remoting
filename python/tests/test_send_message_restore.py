# -*- coding: utf-8 -*-
"""发送返回后**调用方的 Message 必须还原**（Java ``sendKernelImpl`` 的 ``finally``）的单测。

对齐基准是 Java 5.5.1 ``DefaultMQProducerImpl#sendKernelImpl``：

  * ``:930`` ``byte[] prevBody = msg.getBody();`` 记下原始 body；
  * ``:1095-1096`` ``finally { msg.setBody(prevBody); msg.setTopic(
    NamespaceUtil.withoutNamespace(msg.getTopic(), ns)); }`` —— 成功、抛异常、超时，
    三条路都要还原成发送前的样子；
  * ``:1028-1038`` 异步分支更早一步：压缩过就先克隆出带压缩 body 的副本、**立刻**还原
    调用方那份（rocketmq-externals#66），拼过命名空间又没克隆过就再克隆一次、然后剥掉
    调用方 topic 上的命名空间；
  * 批量路径例外：``batch()`` 给**每条子消息**拼命名空间后不还原（Java 也如此），
    子消息 body 不参与压缩，所以没有损坏问题。

不还原的后果是**静默的数据损坏**：同一个 Message 再发一次时 body 已是上一轮的压缩流，
长度掉到阈值以下、``tryToCompressMessage`` 这次返回 false、压缩标志也不置位，broker 照存、
消费者照收，业务侧解出来的是 zlib 裸流 —— 发送、存储、消费全程零报错。

C++ / .NET 是靠「发之前拷贝一份」达到同一效果的（``producer.cpp`` 里 ``Message out = msg``、
dotnet ``CloneMessage``），Rust 见 ``src/client/producer.rs`` 的同名测试。
"""
from __future__ import annotations

import threading
import time
import zlib
from typing import List, Optional

import pytest

from rocketmq.client.exception import MQClientException
from rocketmq.client.mq_client import TopicPublishInfo
from rocketmq.client.producer import (DefaultMQProducer, LocalTransactionState,
                                      TransactionListener)
from rocketmq.client.send_result import SendResult, SendStatus
from rocketmq.common.message import Message, MessageQueue
from rocketmq.common.message_client_id_setter import get_uniq_id
from rocketmq.common.message_decoder import _compress
from rocketmq.common.sysflag import MessageSysFlag
from rocketmq.remoting.exception import RemotingSendRequestException
from rocketmq.remoting.protocol.codes import RequestCode
from rocketmq.remoting.protocol.headers import EndTransactionRequestHeader
from rocketmq.remoting.protocol.remoting_command import RemotingCommand

ADDR = "127.0.0.1:10911"
NS = "ns1"
ORIGINAL = b"A" * 8192


def _offset_msg_id(offset: int) -> str:
    """broker 的 offsetMsgId：4 字节 IP + 4 字节端口 + 8 字节偏移（MessageDecoder 格式）。"""
    return "7f000001" + "00002a9f" + "%016x" % offset


class _FakeRemoting:
    """只收 END_TRANSACTION 这类 oneway 请求。"""

    def __init__(self):
        self.oneway: List[tuple] = []   # (addr, cmd)

    def invoke_oneway(self, addr, cmd) -> None:
        self.oneway.append((addr, cmd))


class _FakeClient:
    """MQClientInstance 替身，缝在发送链真正读消息对象的那几个接缝上。

    记录的 ``topics``/``bodies``/``sys_flags`` 就是「上线时 broker 看到的东西」——
    还原必须在这些值被取走**之后**发生。
    """

    def __init__(self):
        self.publish = TopicPublishInfo()
        self.publish.msg_queue_list = [MessageQueue("TopicTest", "broker-a", 0)]
        # MQClientInstance 真有这个字段（动态取址时它才是权威来源）
        self.name_server_addrs = ["127.0.0.1:9876"]
        self.topics: List[str] = []
        self.bodies: List[bytes] = []
        self.sys_flags: List[int] = []
        self.remoting_client = _FakeRemoting()

    # --- 发送链会碰到的 MQClientInstance 接口 ---
    def register_topic_in_use(self, topic):
        pass

    def get_topic_publish_info(self, topic, is_default=False):
        return self.publish

    def broker_addr_of(self, broker_name):
        return ADDR

    def _record(self, msg, sys_flag):
        self.topics.append(msg.topic)
        self.bodies.append(msg.body)
        self.sys_flags.append(sys_flag)

    def send_message(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                     default_topic=None, default_topic_queue_nums=None):
        self._record(msg, sys_flag)
        # offset_msg_id 要能过 decode_message_id（endTransaction 用它取 commitLogOffset）
        return SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq,
                          transaction_id="t-1", offset_msg_id=_offset_msg_id(0))

    def send_message_oneway(self, group, msg, mq, addr, timeout, sys_flag, unit_mode=False,
                            default_topic=None, default_topic_queue_nums=None):
        self._record(msg, sys_flag)

    def build_send_request(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                           default_topic=None, default_topic_queue_nums=None):
        return RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2, None)

    def send_message_async(self, addr, request, msg, mq, timeout, on_complete):
        self._record(msg, 0)
        on_complete(SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq), None)

    def shutdown(self):
        pass


class _Callback:
    def __init__(self):
        self.results: List[SendResult] = []
        self.errors: List[BaseException] = []
        self.done = threading.Event()

    def on_success(self, send_result: SendResult) -> None:
        self.results.append(send_result)
        self.done.set()

    def on_exception(self, e: BaseException) -> None:
        self.errors.append(e)
        self.done.set()

    def wait(self, timeout: float = 5.0) -> None:
        assert self.done.wait(timeout), "异步回调没有触发"


class _Selector:
    """MessageQueueSelector 替身：选第一个队列，并记下选队列时看到的 body。"""

    def __init__(self):
        self.seen_body: Optional[bytes] = None

    def select(self, queues, msg, arg):
        self.seen_body = msg.body
        return queues[0]


class _Listener(TransactionListener):
    """记录本地事务看到的 body（Java 里这时调用方的 Message 已被 finally 还原）。"""

    def __init__(self):
        self.body: Optional[bytes] = None
        self.topic: Optional[str] = None
        self.args: List = []

    def execute_local_transaction(self, msg, arg):
        self.body = msg.body
        self.topic = msg.topic
        self.args.append(arg)
        return LocalTransactionState.COMMIT_MESSAGE

    def check_local_transaction(self, msg_ext):
        return LocalTransactionState.COMMIT_MESSAGE


def _producer(namespace: Optional[str] = None) -> DefaultMQProducer:
    p = DefaultMQProducer("GID_restore_test", namespace=namespace or "")
    p.set_namesrv_addr("127.0.0.1:9876")
    p.set_retry_times_when_send_failed(0)
    p.retry_times_when_send_async_failed = 0
    # start() 会建真实客户端，这里只补齐发送链需要的：started 标记 + 两个池
    p._create_async_executors()
    p._mq_client = _FakeClient()
    p._started = True
    return p


def _compression_flag(p: DefaultMQProducer) -> int:
    """本生产者压缩时应置的 sysFlag 位（COMPRESSED_FLAG + 压缩类型）。"""
    return MessageSysFlag.set_compression_type(MessageSysFlag.COMPRESSED_FLAG, p.compress_type)


def _wait_restored(msg: Message, body: bytes = ORIGINAL, timeout: float = 2.0) -> None:
    """等异步链的还原跑完（同步链的还原在调用返回前必然发生，不需要它）。

    还原跑在 ``AsyncSenderExecutor`` 工作线程上（Java 的 finally 也是这个线程），
    回调则在 ``NettyClientPublicExecutor`` 上：两边谁先谁后没有保证。Java 因为
    「克隆带压缩 body 的副本去发」可以**在派发前**就还原调用方那份；本实现把调用方
    对象直接发上线，只能等派发之后还原 —— 所以这里轮询等它出现，而不是断言某一
    瞬间的状态。
    """
    deadline = time.monotonic() + timeout
    while msg.body != body and time.monotonic() < deadline:
        time.sleep(0.005)
    assert msg.body == body, "发送结束后调用方的 body 没有还原"


# ---------------------------------------------------------------- 同步
def test_sync_send_restores_the_callers_body_and_recompresses_next_time():
    """两次发送同一个 Message：每次上线的是「原文压缩后」的同一串字节。

    少了还原，第二次上线的就是第一次那段 zlib 流（长度已低于阈值、压缩标志也不会置位），
    消费端解出来是乱码。
    """
    p = _producer()
    client = p._mq_client
    msg = Message("TopicTest", ORIGINAL)

    p.send(msg)
    p.send(msg)

    assert len(client.bodies) == 2
    assert client.bodies[0] == client.bodies[1]
    assert client.bodies[0] != ORIGINAL
    assert zlib.decompress(client.bodies[0]) == ORIGINAL
    assert client.sys_flags[0] == _compression_flag(p)
    assert client.sys_flags[1] == _compression_flag(p)
    assert msg.body == ORIGINAL


def test_sync_send_restores_the_namespaced_topic():
    p = _producer(NS)
    client = p._mq_client
    msg = Message("TopicTest", b"x")

    p.send(msg)

    assert client.topics[0] == "%s%%TopicTest" % NS
    assert msg.topic == "TopicTest"


# ---------------------------------------------------------------- 批量
def test_batch_send_keeps_sub_message_topic_namespaced_like_java():
    """批量是例外：Java ``batch()`` 拼完命名空间**不**还原，子消息 body 也不压缩。

    这里把差别钉住，免得日后有人"顺手"把批量也一起还原了 —— 那会与 Java 分道扬镳。
    """
    p = _producer(NS)
    client = p._mq_client
    m1 = Message("TopicTest", b"a")
    m2 = Message("TopicTest", b"b")

    p.send([m1, m2])

    assert m1.topic == "%s%%TopicTest" % NS
    assert m2.topic == "%s%%TopicTest" % NS
    assert m1.body == b"a" and m2.body == b"b"
    # 批量永不压缩，sysFlag 里没有 COMPRESSED 位
    assert client.sys_flags[0] == 0
    assert get_uniq_id(m1) and get_uniq_id(m2)


# ---------------------------------------------------------------- 异步
def test_async_send_restores_the_callers_body():
    p = _producer()
    client = p._mq_client
    cb = _Callback()
    msg = Message("TopicTest", ORIGINAL)

    p.send_async(msg, cb, 3000)
    cb.wait()

    assert len(cb.results) == 1 and not cb.errors
    assert client.bodies[0] != ORIGINAL
    assert zlib.decompress(client.bodies[0]) == ORIGINAL
    _wait_restored(msg)


def test_async_send_restores_the_namespaced_topic():
    p = _producer(NS)
    client = p._mq_client
    cb = _Callback()
    msg = Message("TopicTest", b"x")

    p.send_async(msg, cb, 3000)
    cb.wait()

    assert client.topics[0] == "%s%%TopicTest" % NS
    deadline = time.monotonic() + 2.0
    while msg.topic != "TopicTest" and time.monotonic() < deadline:
        time.sleep(0.005)
    assert msg.topic == "TopicTest"


def test_async_send_restores_the_body_even_when_the_send_fails():
    """失败路径同样要还原：Java 的还原在 finally 里，与成败无关。"""
    p = _producer()
    client = p._mq_client
    cb = _Callback()

    def _boom(addr, request, msg, mq, timeout, on_complete):
        raise RemotingSendRequestException(ADDR, "connection refused")

    client.send_message_async = _boom
    msg = Message("TopicTest", ORIGINAL)

    p.send_async(msg, cb, 3000)
    cb.wait()

    assert len(cb.errors) == 1 and not cb.results
    _wait_restored(msg)


# ---------------------------------------------------------------- 单向 / selector
def test_oneway_send_restores_the_callers_message():
    p = _producer(NS)
    client = p._mq_client
    msg = Message("TopicTest", ORIGINAL)

    p.send_oneway(msg)

    assert client.topics[0] == "%s%%TopicTest" % NS
    assert msg.body == ORIGINAL
    assert msg.topic == "TopicTest"


def test_selector_send_restores_the_callers_message():
    p = _producer(NS)
    client = p._mq_client
    sel = _Selector()
    msg = Message("TopicTest", ORIGINAL)

    p.send_by_selector(msg, sel, None)

    # 选择器在压缩之前跑（Java sendSelectImpl 的顺序），看到的是原始 body
    assert sel.seen_body == ORIGINAL
    assert client.topics[0] == "%s%%TopicTest" % NS
    assert msg.body == ORIGINAL
    assert msg.topic == "TopicTest"


# ---------------------------------------------------------------- 事务
def test_transaction_send_restores_the_message_and_keeps_the_wire_topic():
    """半消息发完即还原：本地事务看到原始 body，endTransaction 报**已剥命名空间**的 topic。

    最后这点容易想反：Java 5.5.1 ``endTransaction:1543`` 是 ``requestHeader.setTopic(
    msg.getTopic())`` —— 用的正是**调用方**那条 msg，而它的 topic 已经被 sendKernelImpl
    的 finally（:1096）剥掉了命名空间。``queueWithNamespace`` 只用来定位 brokerName。
    """
    p = _producer(NS)
    client = p._mq_client
    listener = _Listener()
    msg = Message("TopicTest", ORIGINAL)

    result = p.send_message_in_transaction(msg, listener, "arg-1")

    assert result.local_transaction_state == LocalTransactionState.COMMIT_MESSAGE
    assert listener.args == ["arg-1"]
    # 本地事务看到的是还原后的消息：原始 body + 已剥命名空间的 topic
    assert listener.body == ORIGINAL
    assert listener.topic == "TopicTest"
    # 半消息上线时仍是压缩 body + 带命名空间的 topic
    assert client.bodies[0] != ORIGINAL
    assert zlib.decompress(client.bodies[0]) == ORIGINAL
    assert client.topics[0] == "%s%%TopicTest" % NS

    assert client.remoting_client.oneway, "没有发出 END_TRANSACTION"
    _addr, cmd = client.remoting_client.oneway[-1]
    # ext_fields 是编码时才铺开的（make_custom_header_to_net），这里走同一条路，断言的是
    # **真正上线**的键值，而不是内存里的 header 对象
    cmd.make_custom_header_to_net()
    header = EndTransactionRequestHeader()
    header.from_ext_fields(cmd.ext_fields)
    assert header.topic == "TopicTest"
    assert msg.body == ORIGINAL
    assert msg.topic == "TopicTest"


def test_transaction_without_listener_still_leaves_the_message_untouched():
    p = _producer()
    msg = Message("TopicTest", ORIGINAL)

    with pytest.raises(MQClientException):
        p.send_message_in_transaction(msg, None)

    assert msg.body == ORIGINAL


# ---------------------------------------------------------------- 判据本身
def test_compress_helper_is_deterministic_so_the_two_sends_are_comparable():
    """上面断言用「两次上线的字节相同」判定压缩跑了两次，先钉住这个前提。"""
    once = _compress(ORIGINAL, MessageSysFlag.ZLIB_TYPE, 5)
    assert once == _compress(ORIGINAL, MessageSysFlag.ZLIB_TYPE, 5)
    # 负向对照：少了还原，第二次上线的就是压了两遍的流 —— 判据能发现它
    twice = _compress(once, MessageSysFlag.ZLIB_TYPE, 5)
    assert twice != once
    assert zlib.decompress(once) == ORIGINAL
    assert zlib.decompress(twice) != ORIGINAL
