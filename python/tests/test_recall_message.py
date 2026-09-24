# -*- coding: utf-8 -*-
"""定时消息撤回（RECALL_MESSAGE 370）离线单测，不需要集群。

对齐基准是 Java 5.5.1 的 ``RecallMessageHandle`` / ``RecallMessageRequestHeader`` /
``DefaultMQProducerImpl#recallMessage``。盯的是三类错得很安静的东西：
  * 句柄编解码：分隔符是**空格**、版本段是 ``v1``、编码是 base64url **带 ``=`` 填充**；
    Java 解码器不吃无填充串，这里两种都吃（见模块 docstring），所以两种都要测。
  * 报文键名：``RecallMessageRequestHeader`` 在 Java 里继承 ``RpcRequestHeader``，
    brokerName 的**反射名是 bname**，写成 ``brokerName`` 会被 broker 静默丢掉。
  * 客户端校验顺序：状态 → checkTopic → 禁 retry/DLQ → 解句柄 → 定位 broker，
    前三步都不该打网络。
"""
from __future__ import annotations

import time
from typing import List, Optional

import pytest

from rocketmq.client.exception import MQBrokerException, MQClientException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.mq_client import TopicPublishInfo
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common import recall_message_handle
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode
from rocketmq.remoting.protocol.headers import (RecallMessageRequestHeader,
                                                RecallMessageResponseHeader,
                                                SendMessageResponseHeader)
from rocketmq.remoting.protocol.remoting_command import RemotingCommand

TOPIC = "TopicRecallUnit"
BROKER = "broker-a"
UNIQ_KEY = "0123456789ABCDEF0123456789abcdef"
ADDR = "127.0.0.1:10911"


# ---------------- 句柄编解码 ----------------
def test_build_handle_matches_java_vector():
    """向量是拿 Java 的 Base64.getUrlEncoder() 实算出来的，改动即失配。"""
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    assert handle == ("djEgVG9waWNSZWNhbGxVbml0IGJyb2tlci1hIDE3MDAwMDAwMDAwMDAg"
                      "MDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlhYmNkZWY=")  # noqa: E501


def test_build_handle_keeps_java_padding():
    """Java 用带填充的 getUrlEncoder；无填充会让 broker 侧某些实现解不出来。"""
    handle = recall_message_handle.build_handle("a", "b", "1", "c")
    assert len(handle) % 4 == 0


def test_round_trip_and_padded_and_unpadded_both_decode():
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    assert recall_message_handle.decode_handle(handle) == recall_message_handle.HandleV1(
        TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    assert recall_message_handle.decode_handle(handle.rstrip("=")) == \
        recall_message_handle.decode_handle(handle)


def test_extra_segments_are_ignored_like_java_split():
    raw = " ".join(["v1", TOPIC, BROKER, "1700000000000", UNIQ_KEY, "whatever"])
    import base64
    handle = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")
    got = recall_message_handle.decode_handle(handle)
    assert (got.topic, got.broker_name, got.timestamp_str, got.message_id) == \
        (TOPIC, BROKER, "1700000000000", UNIQ_KEY)


def _b64(text: str) -> str:
    import base64
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii")


@pytest.mark.parametrize("bad", [
    "",                                             # Java: StringUtils.isEmpty
    "not-a-handle",                                 # 解出来不是 v1 开头的 5 段
    "!!!!",                                         # 非法 base64 字符 -> DecoderException
    _b64("v2 TopicA broker-a 1 " + UNIQ_KEY),       # 版本不对
    _b64("v1 TopicA broker-a 1"),                   # 段数不足
    "__4gYmFkIHV0Zjg=",                             # 解出来不是合法 utf-8
])
def test_bad_handles_fail_with_the_java_message(bad):
    with pytest.raises(MQClientException) as exc:
        recall_message_handle.decode_handle(bad)
    assert str(exc.value) == "recall handle is invalid"


# ---------------- 报文头 ----------------
def test_recall_request_headers_use_java_keys():
    header = RecallMessageRequestHeader()
    header.producer_group = "PG"
    header.topic = TOPIC
    header.recall_handle = "h"
    header.bname = BROKER
    assert header.to_ext_fields() == {
        "producerGroup": "PG", "topic": TOPIC, "recallHandle": "h", "bname": BROKER,
    }
    other = RecallMessageRequestHeader()
    other.from_ext_fields(header.to_ext_fields())
    assert other.bname == BROKER


def test_recall_response_header_and_send_response_recall_handle():
    resp = RecallMessageResponseHeader()
    resp.from_ext_fields({"msgId": UNIQ_KEY})
    assert resp.msg_id == UNIQ_KEY
    assert resp.to_ext_fields() == {"msgId": UNIQ_KEY}

    sent = SendMessageResponseHeader()
    sent.from_ext_fields({"msgId": "0" * 32, "recallHandle": "djEg"})
    assert sent.recall_handle == "djEg"
    assert sent.to_ext_fields()["recallHandle"] == "djEg"
    # 普通消息没有这个字段：既不编出来，也不误读成空串。
    assert "recallHandle" not in SendMessageResponseHeader().to_ext_fields()
    plain = SendMessageResponseHeader()
    plain.from_ext_fields({"msgId": "0" * 32})
    assert plain.recall_handle is None


# ---------------- MQClientInstance.recall_message ----------------
class _StubClient(MQClientInstance):
    """跳过 __init__（不建 socket），只留 _invoke_sync 缝，验证 370 的响应处理。"""

    def __init__(self, response: RemotingCommand):
        self.calls: List[tuple] = []
        self.response = response

    def _invoke_sync(self, addr, request, timeout_millis=None):
        self.calls.append((addr, request, timeout_millis))
        return self.response


def _recall_response(code: int, ext: Optional[dict] = None, remark: str = "") -> RemotingCommand:
    cmd = RemotingCommand.create_response_command(code, remark, None)
    cmd.ext_fields = ext or {}
    return cmd


def _request_header() -> RecallMessageRequestHeader:
    header = RecallMessageRequestHeader()
    header.producer_group = "PG"
    header.topic = TOPIC
    header.recall_handle = "djEg"
    header.bname = BROKER
    return header


def test_recall_message_rpc_returns_msg_id_and_uses_code_370():
    client = _StubClient(_recall_response(ResponseCode.SUCCESS, {"msgId": UNIQ_KEY}))
    assert client.recall_message(ADDR, _request_header(), 3000) == UNIQ_KEY
    addr, request, timeout = client.calls[0]
    assert (addr, timeout) == (ADDR, 3000)
    assert request.code == RequestCode.RECALL_MESSAGE
    request.make_custom_header_to_net()          # 真正落盘到 ext_fields 的那一步
    assert request.ext_fields == {"producerGroup": "PG", "topic": TOPIC,
                                  "recallHandle": "djEg", "bname": BROKER}


def test_recall_message_surfaces_broker_code_and_remark():
    client = _StubClient(_recall_response(ResponseCode.ILLEGAL_OPERATION,
                                          remark="recall failed, timestamp invalid"))
    with pytest.raises(MQBrokerException) as exc:
        client.recall_message(ADDR, _request_header(), 3000)
    assert exc.value.response_code == ResponseCode.ILLEGAL_OPERATION
    assert "timestamp invalid" in exc.value.error_message


def test_recall_message_rejects_success_without_msg_id():
    client = _StubClient(_recall_response(ResponseCode.SUCCESS, {}))
    with pytest.raises(MQBrokerException):
        client.recall_message(ADDR, _request_header(), 3000)


# ---------------- 生产者校验顺序 ----------------
def _producer() -> DefaultMQProducer:
    p = DefaultMQProducer("PG")
    p.name_server_addrs = [ADDR]
    return p


def test_recall_before_start_fails_locally():
    with pytest.raises(MQClientException):
        _producer().recall_message(TOPIC, "djEg")


class _NoIoClient:
    """任何网络入口都被记下来：本地校验必须在打网络**之前**跑完。"""

    def __init__(self):
        self.touched: List[str] = []
        # False = 模拟「路由拿不到」；True = 预热成功，让流程继续往下走。
        self.no_publish_error = False
        # MQClientInstance 上的真字段：拿不到路由时 validateNameServerSetting 靠它
        # 分辨"没配 name server"(10004) 和"这个 topic 没路由"(10005)，这里给个地址。
        self.name_server_addrs: List[str] = [ADDR]

    def register_topic_in_use(self, topic):
        self.touched.append("register")

    def get_topic_publish_info(self, topic, is_default=False):
        self.touched.append("publish")
        if not self.no_publish_error:
            raise MQClientException("No route info of this topic: %s" % topic)
        return TopicPublishInfo()

    def get_topic_route_data(self, topic):
        self.touched.append("route")
        return None

    def broker_addr_of(self, broker_name):
        self.touched.append("addr")
        return None

    def recall_message(self, addr, header, timeout):
        self.touched.append("rpc")
        return UNIQ_KEY


def _started(client) -> DefaultMQProducer:
    p = _producer()
    p._started = True
    p._mq_client = client
    return p


def test_retry_and_dlq_topics_are_refused_before_any_io():
    for topic in ("%RETRY%PG", "%DLQ%PG"):
        client = _NoIoClient()
        with pytest.raises(MQClientException) as exc:
            _started(client).recall_message(topic, "djEg")
        assert str(exc.value) == "topic is not supported"
        assert client.touched == []


def test_illegal_topic_names_are_refused_before_any_io():
    client = _NoIoClient()
    with pytest.raises(MQClientException):
        _started(client).recall_message("bad topic!", "djEg")
    assert client.touched == []


def test_corrupt_handle_fails_before_any_io():
    client = _NoIoClient()
    began = time.monotonic()
    with pytest.raises(MQClientException) as exc:
        _started(client).recall_message(TOPIC, "not-a-handle")
    assert str(exc.value) == "recall handle is invalid"
    assert client.touched == []
    assert (time.monotonic() - began) < 0.2


def test_missing_broker_address_uses_java_message():
    client = _NoIoClient()
    client.no_publish_error = True          # 路由预热成功，才轮到定位 broker 地址
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    with pytest.raises(MQClientException) as exc:
        _started(client).recall_message(TOPIC, handle)
    assert str(exc.value) == "The broker service address not found"
    # 路由预热先于定位地址，且一次网络都没打。
    assert "publish" in client.touched
    assert "rpc" not in client.touched


def test_publish_info_failure_propagates_like_java():
    """DefaultMQProducerImpl:1586 的 tryToFindTopicPublishInfo 没有 try/catch：
    路由拿不到就直接失败，不会继续往后拼请求。"""
    client = _NoIoClient()
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    with pytest.raises(MQClientException) as exc:
        _started(client).recall_message(TOPIC, handle)
    assert "No route info" in str(exc.value)
    assert "addr" not in client.touched and "rpc" not in client.touched


def test_recall_goes_out_to_the_broker_named_in_the_handle():
    client = _NoIoClient()
    client.no_publish_error = True
    client.broker_addr_of = lambda broker_name: ADDR      # type: ignore[assignment]
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    assert _started(client).recall_message(TOPIC, handle) == UNIQ_KEY
    assert client.touched[-1] == "rpc"
