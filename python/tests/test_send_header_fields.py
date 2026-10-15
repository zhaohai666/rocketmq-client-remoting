# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
from typing import List, Optional

from client.mq_client import MQClientInstance, TopicPublishInfo
from client.producer import DefaultMQProducer
from client.send_result import SendResult, SendStatus
from common.message import Message, MessageBatch, MessageQueue
from common.mix_all import MixAll
from remoting.protocol.codes import RequestCode

MQ = MessageQueue("T_Header", "broker-a", 3)
ADDR = "127.0.0.1:10911"


def _new_instance(prefix: str) -> MQClientInstance:
    """An instance that is only constructed, not start()ed: no connection, no heartbeat, pure offline assertions."""
    return MQClientInstance("%s_%d" % (prefix, threading.get_ident()),
                            ["127.0.0.1:9876"])


def _drop(inst: MQClientInstance) -> None:
    MQClientInstance.INSTANCE_MAP.pop(inst.client_id, None)


# ---------------------------------------------------------------- Request shape
def test_send_header_carries_broker_name_as_the_single_letter_n():
    pass
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
    pass
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
    pass
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
        # 0 is a legal config value and must not be treated as "not passed" by `or`
        zero = inst._build_send_request("PG", msg, MQ, 3000, 0, False, None, 0)
        zero.make_custom_header_to_net()
        assert zero.ext_fields["d"] == "0"
    finally:
        _drop(inst)


def test_batch_and_reply_paths_keep_the_same_three_fields():
    """Batch goes via 320 and replies via 325, but n/c/d are the same header and must not be missed."""
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


# ---------------------------------------------------------------- Facade wiring
class _Recorder:
    """An MQClientInstance stand-in: records the header parameters received by the three send entries as-is.

    The signature is deliberately written out in full rather than ``**kwargs``: when a facade omits/mis-passes a field it must TypeError immediately,
    rather than being silently swallowed by ``**``.
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

    def find_broker_address_in_publish(self, broker_name):
        return ADDR

    def publish_addr_for(self, broker_name, topic):
        return self.find_broker_address_in_publish(broker_name)

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
        from remoting.protocol.remoting_command import RemotingCommand
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
    # start() would build a real client; here only the three things the async chain needs are filled in: two pools + the started flag
    p._create_async_executors()
    p._mq_client = _Recorder()
    p._started = True
    return p


class _Latch:
    """A SendCallback stand-in: turns the callback into a single Event."""

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
    """set_create_topic_key / set_default_topic_queue_nums must really work (the old code had fake setters)."""
    p = _producer("MY_KEY", 16)
    p.send(Message("T_Header", b"x"))
    seen = p._mq_client.sync[-1]
    assert seen["default_topic"] == "MY_KEY"
    assert seen["default_topic_queue_nums"] == 16


def test_defaults_are_the_protocol_defaults_when_setters_never_run():
    p = _producer()
    p.send(Message("T_Header", b"x"))
    seen = p._mq_client.sync[-1]
    assert seen["default_topic"] == MixAll.DEFAULT_TOPIC
    assert seen["default_topic_queue_nums"] == MixAll.DEFAULT_TOPIC_QUEUE_NUMS
    assert seen["unit_mode"] is False


class _FirstQueue:
    """A MessageQueueSelector stand-in (producer.send_by_selector goes through the .select method)."""

    def select(self, mqs, msg, arg):
        return mqs[0]


def test_every_send_entry_forwards_the_same_config():
    """Fixed-queue / round-robin / selector / oneway / batch / async all go through _send_header_args; missing one is a consistency bug."""
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
    # 2 sync + 1 selector + 1 batch = 4; oneway 1; async 1
    assert (len(client.sync), len(client.oneway), len(client.built)) == (4, 1, 1), (
        len(client.sync), len(client.oneway), len(client.built))
    for rec in records:
        assert rec["default_topic"] == "K2", rec
        assert rec["default_topic_queue_nums"] == 7, rec
        assert rec["unit_mode"] is False, rec
        # brokerName is not among the parameters: it travels with mq and is fetched on the spot by _build_send_request
        assert rec["mq"].broker_name == "broker-a", rec
