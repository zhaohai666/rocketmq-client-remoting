# -*- coding: utf-8 -*-
"""RemotingCommand 帧格式测试（对应 org.apache.rocketmq.remoting.protocol.RemotingCommand）。

线格式（Java encode）：
    totalLength(4) | headerLength(4, 高 8 位为序列化类型) | headerData | bodyData
其中 totalLength = 4 + len(headerData) + len(bodyData)。
"""
from __future__ import annotations

import json
import struct

import pytest

from rocketmq.remoting.protocol.codes import LanguageCode, RequestCode, ResponseCode, SerializeType
from rocketmq.remoting.protocol.headers import PullMessageRequestHeader, SendMessageRequestHeaderV2
from rocketmq.remoting.protocol.remoting_command import RemotingCommand


def make_request(code=RequestCode.SEND_MESSAGE_V2, opaque=9, **kwargs):
    cmd = RemotingCommand.create_request_command(code, kwargs.get("header"))
    cmd.opaque = opaque
    cmd.version = 0
    cmd.remark = kwargs.get("remark")
    if kwargs.get("ext"):
        cmd.ext_fields.update(kwargs["ext"])
    cmd.body = kwargs.get("body")
    return cmd


class TestFrameLayout:
    def test_encode_layout_is_4_then_marked_header_then_body(self):
        cmd = make_request(ext={"topic": "TopicTest"}, body=b"payload")
        data = cmd.encode()

        (total_len,) = struct.unpack_from(">i", data, 0)
        (ori_header_len,) = struct.unpack_from(">i", data, 4)
        header_len = RemotingCommand.get_header_length(ori_header_len)
        assert total_len == len(data) - 4
        assert total_len == 4 + header_len + len(b"payload")
        assert RemotingCommand.get_protocol_type(ori_header_len) == SerializeType.JSON

    def test_header_is_json_with_expected_keys(self):
        cmd = make_request(remark="hi", ext={"topic": "T"})
        data = cmd.encode()
        (header_len,) = struct.unpack_from(">i", data, 4)
        header = json.loads(data[8:8 + RemotingCommand.get_header_length(header_len)].decode("utf-8"))
        assert header["code"] == RequestCode.SEND_MESSAGE_V2
        assert header["opaque"] == 9
        assert header["remark"] == "hi"
        assert header["extFields"] == {"topic": "T"}
        assert header["language"] == LanguageCode.PYTHON
        assert "body" not in header, "Java side body 用 @JSONField(serialize=false) 排除"

    def test_null_remark_is_omitted(self):
        cmd = make_request()
        data = cmd.encode()
        (header_len,) = struct.unpack_from(">i", data, 4)
        header = json.loads(data[8:8 + RemotingCommand.get_header_length(header_len)].decode("utf-8"))
        assert "remark" not in header

    def test_mark_protocol_type_roundtrip(self):
        assert RemotingCommand.mark_protocol_type(128, SerializeType.JSON) == 128
        marked = RemotingCommand.mark_protocol_type(128, SerializeType.ROCKETMQ)
        assert RemotingCommand.get_header_length(marked) == 128
        assert RemotingCommand.get_protocol_type(marked) == SerializeType.ROCKETMQ

    def test_encode_header_excludes_body_but_counts_length(self):
        cmd = make_request(ext={"topic": "T"})
        only_header = cmd.encode_header(64)
        (total_len,) = struct.unpack_from(">i", only_header, 0)
        assert total_len == len(only_header) - 4 + 64
        assert len(only_header) == 8 + (total_len - 4 - 64)


class TestRoundTrip:
    @pytest.mark.parametrize("serialize_type", [SerializeType.JSON, SerializeType.ROCKETMQ])
    def test_roundtrip_with_body(self, serialize_type):
        cmd = make_request(remark="中文 remark", ext={"topic": "TopicTest", "queueId": "3"},
                           body=b"\x01\x02binary")
        cmd.serialize_type_current_rpc = serialize_type
        got = RemotingCommand.decode(cmd.encode())
        assert got.code == cmd.code
        assert got.opaque == cmd.opaque
        assert got.remark == cmd.remark
        assert got.flag == cmd.flag
        assert got.ext_fields == cmd.ext_fields
        assert got.body == b"\x01\x02binary"
        assert got.serialize_type_current_rpc == serialize_type

    def test_json_roundtrip_without_body(self):
        cmd = make_request()
        got = RemotingCommand.decode(cmd.encode())
        assert got.body is None

    def test_rockettmq_roundtrip_without_ext_fields(self):
        cmd = make_request(remark="no-ext")
        cmd.serialize_type_current_rpc = SerializeType.ROCKETMQ
        got = RemotingCommand.decode(cmd.encode())
        assert got.remark == "no-ext"
        assert got.ext_fields == {}

    def test_ext_field_roundtrip_preserves_strings(self):
        """extFields 在网络上只能是字符串（Java Map<String, String>）。"""
        cmd = make_request(ext={"queueId": "3", "batch": "true"})
        cmd.serialize_type_current_rpc = SerializeType.ROCKETMQ
        got = RemotingCommand.decode(cmd.encode())
        assert got.ext_fields == {"queueId": "3", "batch": "true"}

    def test_bad_header_length_raises(self):
        data = bytearray(make_request().encode())
        struct.pack_into(">i", data, 4, 1 << 20)
        from rocketmq.remoting.exception import RemotingCommandException
        with pytest.raises(RemotingCommandException):
            RemotingCommand.decode(bytes(data))


class TestFlags:
    def test_request_defaults(self):
        cmd = RemotingCommand(code=1, opaque=1)
        assert cmd.is_response_type() is False
        assert cmd.is_oneway_rpc() is False

    def test_response_flag(self):
        cmd = RemotingCommand.create_response_command(code=ResponseCode.SUCCESS, remark="ok")
        assert cmd.is_response_type() is True
        assert RemotingCommand.decode(cmd.encode()).is_response_type() is True

    def test_oneway_flag(self):
        cmd = make_request()
        cmd.mark_oneway_rpc()
        assert cmd.is_oneway_rpc() is True
        got = RemotingCommand.decode(cmd.encode())
        assert got.is_oneway_rpc() is True
        assert got.get_type() == "REQUEST_COMMAND"

    def test_response_type_detected_from_decoded_frame(self):
        cmd = RemotingCommand.create_response_command_with_header(
            RequestCode.SEND_MESSAGE_V2, SendMessageRequestHeaderV2())
        got = RemotingCommand.decode(cmd.encode())
        assert got.get_type() == "RESPONSE_COMMAND"


class TestCustomHeader:
    def _v2_header(self):
        h = SendMessageRequestHeaderV2()
        h.producer_group = "PG_1"
        h.topic = "TopicTest"
        h.default_topic = "TBW102"
        h.default_topic_queue_nums = 4
        h.queue_id = 1
        h.sys_flag = 0
        h.born_timestamp = 1700000000000
        h.flag = 0
        h.properties = "TAGS\x01TagA\x02"
        h.reconsume_times = 0
        h.unit_mode = False
        h.max_reconsume_times = 0
        h.batch = False
        h.broker_name = "broker-a"
        return h

    @pytest.mark.parametrize("serialize_type", [SerializeType.JSON, SerializeType.ROCKETMQ])
    def test_v2_short_field_names(self, serialize_type):
        """V2 使用 a..m 短字段名，必须与 Java SendMessageRequestHeaderV2 一致。"""
        cmd = RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2, self._v2_header())
        cmd.serialize_type_current_rpc = serialize_type
        got = RemotingCommand.decode(cmd.encode())
        assert sorted(got.ext_fields) == list("abcdefghijklmn")
        h = got.decode_command_custom_header(SendMessageRequestHeaderV2)
        assert h.producer_group == "PG_1"
        assert h.topic == "TopicTest"
        assert h.default_topic == "TBW102"
        assert h.default_topic_queue_nums == 4
        assert h.queue_id == 1
        assert h.born_timestamp == 1700000000000
        assert h.reconsume_times == 0
        assert h.unit_mode is False
        assert h.batch is False
        assert h.properties == "TAGS\x01TagA\x02"
        assert h.broker_name == "broker-a"

    def test_pull_request_long_field_names(self):
        h = PullMessageRequestHeader()
        h.consumer_group = "CG"
        h.topic = "TopicTest"
        h.queue_id = 2
        h.queue_offset = 100
        h.max_msg_nums = 32
        h.sys_flag = 6
        h.commit_offset = 99
        h.suspend_timeout_millis = 15000
        h.subscription = "TagA"
        h.sub_version = 1700000000000
        h.expression_type = "TAG"
        cmd = RemotingCommand.create_request_command(RequestCode.PULL_MESSAGE, h)
        got = RemotingCommand.decode(cmd.encode())
        h2 = got.decode_command_custom_header(PullMessageRequestHeader)
        assert h2.consumer_group == "CG"
        assert h2.queue_id == 2
        assert h2.queue_offset == 100
        assert h2.max_msg_nums == 32
        assert h2.subscription == "TagA"
        assert h2.expression_type == "TAG"

    def test_opaque_is_assigned(self):
        cmd = RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        assert cmd.opaque >= 0
        assert RemotingCommand.decode(cmd.encode()).opaque == cmd.opaque


def test_remoting_command_version_default():
    cmd = RemotingCommand.create_request_command(RequestCode.GET_ROUTEINFO_BY_TOPIC)
    assert cmd.version == 0
