# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from typing import List, Optional

import pytest

from client.exception import MQBrokerException, MQClientException
from client.mq_client import MQClientInstance
from client.mq_client import TopicPublishInfo
from client.producer import DefaultMQProducer
from common import recall_message_handle
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.headers import (RecallMessageRequestHeader,
                                                RecallMessageResponseHeader,
                                                SendMessageResponseHeader)
from remoting.protocol.remoting_command import RemotingCommand

TOPIC = "TopicRecallUnit"
BROKER = "broker-a"
UNIQ_KEY = "0123456789ABCDEF0123456789abcdef"
ADDR = "127.0.0.1:10911"


# ---------------- Handle encoding/decoding ----------------
def test_build_handle_matches_the_wire_vector():
    pass
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    assert handle == ("djEgVG9waWNSZWNhbGxVbml0IGJyb2tlci1hIDE3MDAwMDAwMDAwMDAg"
                      "MDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlhYmNkZWY=")  # noqa: E501


def test_build_handle_keeps_the_field_padding():
    pass
    handle = recall_message_handle.build_handle("a", "b", "1", "c")
    assert len(handle) % 4 == 0


def test_round_trip_and_padded_and_unpadded_both_decode():
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    assert recall_message_handle.decode_handle(handle) == recall_message_handle.HandleV1(
        TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    assert recall_message_handle.decode_handle(handle.rstrip("=")) == \
        recall_message_handle.decode_handle(handle)


def test_extra_segments_are_ignored_by_the_split_rule():
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
    "",
    "not-a-handle",                                 # decoded result is not 5 segments starting with v1
    "!!!!",                                         # illegal base64 chars -> DecoderException
    _b64("v2 TopicA broker-a 1 " + UNIQ_KEY),       # wrong version
    _b64("v1 TopicA broker-a 1"),                   # not enough segments
    "__4gYmFkIHV0Zjg=",                             # decoded result is not valid utf-8
])
def test_bad_handles_fail_with_the_protocol_message(bad):
    with pytest.raises(MQClientException) as exc:
        recall_message_handle.decode_handle(bad)
    assert str(exc.value) == "recall handle is invalid"


# ---------------- Request/response headers ----------------
def test_recall_request_headers_use_the_wire_keys():
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
    # a normal message has no such field: it is neither encoded nor misread as an empty string.
    assert "recallHandle" not in SendMessageResponseHeader().to_ext_fields()
    plain = SendMessageResponseHeader()
    plain.from_ext_fields({"msgId": "0" * 32})
    assert plain.recall_handle is None


# ---------------- MQClientInstance.recall_message ----------------
class _StubClient(MQClientInstance):
    """Skip __init__ (no socket built), leaving only the _invoke_sync seam, to verify the response handling of 370."""

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
    request.make_custom_header_to_net()          # the step that actually lands the header in ext_fields
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


# ---------------- Producer validation order ----------------
def _producer() -> DefaultMQProducer:
    p = DefaultMQProducer("PG")
    p.name_server_addrs = [ADDR]
    return p


def test_recall_before_start_fails_locally():
    with pytest.raises(MQClientException):
        _producer().recall_message(TOPIC, "djEg")


class _NoIoClient:
    """Every network entry is recorded: local validation must run completely **before** the network is touched."""

    def __init__(self):
        self.touched: List[str] = []
        # False = simulate "route unavailable"; True = warm-up succeeded, let the flow continue
        self.no_publish_error = False
        # a real field on MQClientInstance: when the route cannot be fetched, validateNameServerSetting uses it
        # to distinguish "no name server configured" (10004) from "this topic has no route" (10005); give it an address here.
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

    def find_broker_address_in_publish(self, broker_name):
        self.touched.append("addr")
        return None

    def find_broker_addr_by_topic(self, topic):
        self.touched.append("addr-by-topic")
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


def test_missing_broker_address_uses_the_protocol_message():
    client = _NoIoClient()
    client.no_publish_error = True          # route warm-up succeeded, only now does it get to locating the broker address
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    with pytest.raises(MQClientException) as exc:
        _started(client).recall_message(TOPIC, handle)
    assert str(exc.value) == "The broker service address not found"
    # route warm-up precedes address location, and not a single network call was made.
    assert "publish" in client.touched
    assert "rpc" not in client.touched


def test_publish_info_failure_propagates_unchanged():
    """tryToFindTopicPublishInfo at DefaultMQProducerImpl:1586 has no try/catch:
    if the route cannot be fetched it fails directly, without continuing to assemble the request."""
    client = _NoIoClient()
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    with pytest.raises(MQClientException) as exc:
        _started(client).recall_message(TOPIC, handle)
    assert "No route info" in str(exc.value)
    assert "addr" not in client.touched and "rpc" not in client.touched


def test_recall_goes_out_to_the_broker_named_in_the_handle():
    client = _NoIoClient()
    client.no_publish_error = True
    client.find_broker_address_in_publish = lambda broker_name: ADDR      # type: ignore[assignment]
    handle = recall_message_handle.build_handle(TOPIC, BROKER, "1700000000000", UNIQ_KEY)
    assert _started(client).recall_message(TOPIC, handle) == UNIQ_KEY
    assert client.touched[-1] == "rpc"
