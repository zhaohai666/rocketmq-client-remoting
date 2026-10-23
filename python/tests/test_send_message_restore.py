# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time
import zlib
from typing import List, Optional

import pytest

from client.exception import MQClientException
from client.mq_client import TopicPublishInfo
from client.producer import (DefaultMQProducer, LocalTransactionState,
                                      TransactionListener)
from client.send_result import SendResult, SendStatus
from common.message import Message, MessageQueue
from common.message_client_id_setter import get_uniq_id
from common.message_decoder import _compress
from common.sysflag import MessageSysFlag
from remoting.exception import RemotingSendRequestException
from remoting.protocol.codes import RequestCode
from remoting.protocol.headers import EndTransactionRequestHeader
from remoting.protocol.remoting_command import RemotingCommand

ADDR = "127.0.0.1:10911"
NS = "ns1"
ORIGINAL = b"A" * 8192


def _offset_msg_id(offset: int) -> str:
    """The broker's offsetMsgId: 4-byte IP + 4-byte port + 8-byte offset (MessageDecoder format)."""
    return "7f000001" + "00002a9f" + "%016x" % offset


class _FakeRemoting:
    """Only receives oneway requests such as END_TRANSACTION."""

    def __init__(self):
        self.oneway: List[tuple] = []   # (addr, cmd)

    def invoke_oneway(self, addr, cmd) -> None:
        self.oneway.append((addr, cmd))


class _FakeClient:
    """An MQClientInstance stand-in, sewn onto the few seams where the send chain really reads the message object.

    The recorded ``topics``/``bodies``/``sys_flags`` are exactly "what the broker sees on the wire" --
    restoration must happen **after** these values have been taken.
    """

    def __init__(self):
        self.publish = TopicPublishInfo()
        self.publish.msg_queue_list = [MessageQueue("TopicTest", "broker-a", 0)]
        # MQClientInstance really has this field (with dynamic addressing it is the authoritative source)
        self.name_server_addrs = ["127.0.0.1:9876"]
        self.topics: List[str] = []
        self.bodies: List[bytes] = []
        self.sys_flags: List[int] = []
        self.remoting_client = _FakeRemoting()

    # --- the MQClientInstance interface the send chain will hit ---
    def register_topic_in_use(self, topic):
        pass

    def get_topic_publish_info(self, topic, is_default=False):
        return self.publish

    def find_broker_address_in_publish(self, broker_name):
        return ADDR

    def publish_addr_for(self, broker_name, topic):
        return self.find_broker_address_in_publish(broker_name)

    def _record(self, msg, sys_flag):
        self.topics.append(msg.topic)
        self.bodies.append(msg.body)
        self.sys_flags.append(sys_flag)

    def send_message(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                     default_topic=None, default_topic_queue_nums=None):
        self._record(msg, sys_flag)
        # offset_msg_id must pass decode_message_id (endTransaction uses it to get commitLogOffset)
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
        assert self.done.wait(timeout), "the async callback did not fire"


class _Selector:
    """A MessageQueueSelector stand-in: picks the first queue and records the body seen when picking."""

    def __init__(self):
        self.seen_body: Optional[bytes] = None

    def select(self, queues, msg, arg):
        self.seen_body = msg.body
        return queues[0]


class _Listener(TransactionListener):
    pass

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
    # start() would build a real client; here only what the send chain needs is filled in: the started flag + two pools
    p._create_async_executors()
    p._mq_client = _FakeClient()
    p._started = True
    return p


def _compression_flag(p: DefaultMQProducer) -> int:
    """The sysFlag bit this producer should set when compressing (COMPRESSED_FLAG + compression type)."""
    return MessageSysFlag.set_compression_type(MessageSysFlag.COMPRESSED_FLAG, p.compress_type)


def _wait_restored(msg: Message, body: bytes = ORIGINAL, timeout: float = 2.0) -> None:
    pass
    deadline = time.monotonic() + timeout
    while msg.body != body and time.monotonic() < deadline:
        time.sleep(0.005)
    assert msg.body == body, "the caller's body was not restored after the send finished"


# ---------------------------------------------------------------- Sync
def test_sync_send_restores_the_callers_body_and_recompresses_next_time():
    """Sending the same Message twice: what goes on the wire each time is the same bytes of "compressed original".

    Without restoration, the second send would put the first send's zlib stream on the wire (its length is already below the threshold, and the compression flag would not be set either),
    so the consumer side would decode garbage.
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


# ---------------------------------------------------------------- Batch
def test_batch_send_keeps_sub_message_topic_namespaced():
    pass
    p = _producer(NS)
    client = p._mq_client
    m1 = Message("TopicTest", b"a")
    m2 = Message("TopicTest", b"b")

    p.send([m1, m2])

    assert m1.topic == "%s%%TopicTest" % NS
    assert m2.topic == "%s%%TopicTest" % NS
    assert m1.body == b"a" and m2.body == b"b"
    # batches are never compressed, so sysFlag has no COMPRESSED bit
    assert client.sys_flags[0] == 0
    assert get_uniq_id(m1) and get_uniq_id(m2)


# ---------------------------------------------------------------- Async
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
    pass
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


# ---------------------------------------------------------------- Oneway / selector
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

    assert sel.seen_body == ORIGINAL
    assert client.topics[0] == "%s%%TopicTest" % NS
    assert msg.body == ORIGINAL
    assert msg.topic == "TopicTest"


# ---------------------------------------------------------------- Transactions
def test_transaction_send_restores_the_message_and_keeps_the_wire_topic():
    pass
    p = _producer(NS)
    client = p._mq_client
    listener = _Listener()
    msg = Message("TopicTest", ORIGINAL)

    result = p.send_message_in_transaction(msg, listener, "arg-1")

    assert result.local_transaction_state == LocalTransactionState.COMMIT_MESSAGE
    assert listener.args == ["arg-1"]
    # the local transaction sees the restored message: original body + topic with the namespace stripped
    assert listener.body == ORIGINAL
    assert listener.topic == "TopicTest"
    # when the half message goes on the wire it is still the compressed body + namespaced topic
    assert client.bodies[0] != ORIGINAL
    assert zlib.decompress(client.bodies[0]) == ORIGINAL
    assert client.topics[0] == "%s%%TopicTest" % NS

    assert client.remoting_client.oneway, "no END_TRANSACTION was sent"
    _addr, cmd = client.remoting_client.oneway[-1]
    # ext_fields is only laid out at encoding time (make_custom_header_to_net); the same path is taken here, so what is asserted is the
    # **really-on-the-wire** key-value pairs, not the in-memory header object
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


def test_transaction_refuses_a_message_holding_any_schedule_property():
    # A half message is committed exactly as it stands, so a schedule asked for here
    # could never be honoured. The key alone decides: "3", a "0" and an empty value
    # are all a stamp the transaction path cannot carry.
    for value in ("3", "0", ""):
        for name in ("DELAY", "TIMER_DELAY_SEC", "TIMER_DELAY_MS", "TIMER_DELIVER_MS"):
            p = _producer()
            msg = Message("TopicTest", b"x")
            msg.put_property(name, value)

            with pytest.raises(MQClientException) as info:
                p.send_message_in_transaction(msg, _Listener())

            assert "do not support delayed delivery" in str(info.value), (name, value)
            assert p._mq_client.topics == [], "the refused half message must not reach the wire"


def test_transaction_sends_a_message_holding_an_unrelated_property():
    # Only the four schedule spellings refuse a transactional send. "DELAY_TIME"
    # looks like one and is not one; a property that merely resembles a delay
    # request must still commit, or the guard would reject messages asking for
    # nothing at all.
    for name in ("DELAY_TIME", "SOME_USER_PROPERTY"):
        p = _producer()
        msg = Message("TopicTest", b"x")
        msg.put_property(name, "3")

        result = p.send_message_in_transaction(msg, _Listener())

        assert result.local_transaction_state == LocalTransactionState.COMMIT_MESSAGE, name


# ---------------------------------------------------------------- The criterion itself
def test_compress_helper_is_deterministic_so_the_two_sends_are_comparable():
    """The assertions above use "the bytes on the wire are identical twice" to judge that compression ran twice; pin this premise down first."""
    once = _compress(ORIGINAL, MessageSysFlag.ZLIB_TYPE, 5)
    assert once == _compress(ORIGINAL, MessageSysFlag.ZLIB_TYPE, 5)
    # negative control: without restoration the second send would put a twice-compressed stream on the wire -- the criterion can catch it
    twice = _compress(once, MessageSysFlag.ZLIB_TYPE, 5)
    assert twice != once
    assert zlib.decompress(once) == ORIGINAL
    assert zlib.decompress(twice) != ORIGINAL
