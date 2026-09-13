# -*- coding: utf-8 -*-
"""序列化层测试（RemotingSerializable / RocketMQSerializable）。"""
from __future__ import annotations

import json
import struct

from rocketmq.remoting.protocol.serialize import RemotingSerializable, RocketMQSerializable


class TestRemotingSerializable:
    def test_encode_dict(self):
        raw = RemotingSerializable.encode({"a": 1, "b": "中文"})
        assert json.loads(raw.decode("utf-8")) == {"a": 1, "b": "中文"}

    def test_encode_none_and_bytes(self):
        assert RemotingSerializable.encode(None) == b""
        assert RemotingSerializable.encode(b"abc") == b"abc"

    def test_decode(self):
        data = json.dumps({"x": [1, 2]}).encode("utf-8")
        assert RemotingSerializable.decode(data) == {"x": [1, 2]}

    def test_decode_empty(self):
        assert RemotingSerializable.decode(None) is None
        assert RemotingSerializable.decode(b"") is None


class TestRocketMQSerializable:
    @staticmethod
    def total_len(remark, ext_bytes):
        remark_len = len(remark.encode("utf-8")) if remark else 0
        ext_len = len(ext_bytes) if ext_bytes else 0
        return 2 + 1 + 2 + 4 + 4 + 4 + remark_len + 4 + ext_len

    def test_map_serialize_uses_short_key_int_value(self):
        raw = RocketMQSerializable.map_serialize({"k": "v"})
        # key: short len(2) + 1 字节；value: int len(4) + 1 字节
        assert raw[:2] == struct.pack(">H", 1)
        assert raw[2:3] == b"k"
        assert raw[3:7] == struct.pack(">i", 1)
        assert raw[7:] == b"v"

    def test_map_roundtrip(self):
        data = {"queueId": "3", "subscription": "TagA", "中文": "值"}
        raw = RocketMQSerializable.map_serialize(data)
        got, offset = RocketMQSerializable.map_deserialize(raw, 0, len(raw))
        assert got == data
        assert offset == len(raw)

    def test_empty_map(self):
        assert RocketMQSerializable.map_serialize({}) is None
        assert RocketMQSerializable.map_serialize(None) is None

    def test_encode_header_layout(self):
        class Cmd:
            code = 11
            language = 3
            version = 0
            opaque = 42
            flag = 1
            remark = "hello"
            ext_fields = {"topic": "TopicTest"}

        raw = RocketMQSerializable.rocket_mq_protocol_encode(Cmd())
        offset = 0
        (code,) = struct.unpack_from(">h", raw, offset); offset += 2
        (language,) = struct.unpack_from(">B", raw, offset); offset += 1
        (version,) = struct.unpack_from(">h", raw, offset); offset += 2
        (opaque,) = struct.unpack_from(">i", raw, offset); offset += 4
        (flag,) = struct.unpack_from(">i", raw, offset); offset += 4
        remark, offset = RocketMQSerializable.read_str(raw, offset, False)
        (ext_len,) = struct.unpack_from(">i", raw, offset); offset += 4

        assert (code, language, version, opaque, flag) == (11, 3, 0, 42, 1)
        assert remark == "hello"
        assert ext_len == len(RocketMQSerializable.map_serialize({"topic": "TopicTest"}))
        assert len(raw) == self.total_len("hello", RocketMQSerializable.map_serialize({"topic": "TopicTest"}))

    def test_encode_decode_roundtrip(self):
        class Cmd:
            code = 105
            language = 3
            version = 7
            opaque = 12345
            flag = 0
            remark = ""
            ext_fields = {"topic": "TopicTest", "中文键": "中文值"}

        raw = RocketMQSerializable.rocket_mq_protocol_encode(Cmd())
        got = RocketMQSerializable.rocket_mq_protocol_decode(raw)
        assert got["code"] == 105
        assert got["language"] == 3
        assert got["version"] == 7
        assert got["opaque"] == 12345
        assert got["flag"] == 0
        assert got["remark"] is None, "Java readStr 长度 0 返回 null"
        assert got["extFields"] == {"topic": "TopicTest", "中文键": "中文值"}

    def test_write_str_short_and_int_length(self):
        buf = bytearray()
        RocketMQSerializable.write_str(buf, True, "ab")
        assert buf[:2] == struct.pack(">H", 2)
        buf2 = bytearray()
        RocketMQSerializable.write_str(buf2, False, "ab")
        assert buf2[:4] == struct.pack(">i", 2)
        buf3 = bytearray()
        RocketMQSerializable.write_str(buf3, False, None)
        assert buf3 == struct.pack(">i", 0)
