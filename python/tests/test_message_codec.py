# -*- coding: utf-8 -*-
"""消息编解码测试（对应 org.apache.rocketmq.common.message.MessageDecoder）。

覆盖两条互不相同的编码路径：
  * 17 段存储格式：encode_message_ext / decode_message / decode_messages
  * 6 段批量轻量格式：MessageBatch.encode / encode_messages / decode_batch_messages
"""
from __future__ import annotations

import struct

import pytest

from rocketmq.common.message import Message, MessageBatch, MessageExt
from rocketmq.common.message_decoder import (
    BLANK_MAGIC_CODE, MESSAGE_MAGIC_CODE, MESSAGE_MAGIC_CODE_V2,
    bytes_to_ip_and_port, bytes2string, count_inner_msg_num, crc32,
    create_message_id, decode_batch_message, decode_batch_messages,
    decode_message, decode_message_id, decode_messages,
    encode_message, encode_message_ext, encode_messages,
    ip_and_port_to_bytes, message_properties_2_string, string_2_message_properties)
from rocketmq.common.sysflag import MessageSysFlag


def build_ext(**overrides) -> MessageExt:
    """构造一条字段全量赋值的 MessageExt。"""
    ext = MessageExt()
    ext.set_topic(overrides.get("topic", "TopicTest"))
    ext.set_body(overrides.get("body", b"hello rocketmq"))
    ext.set_flag(overrides.get("flag", 2))
    ext.set_sys_flag(overrides.get("sys_flag", 0))
    ext.set_body_crc(crc32(ext.get_body()))
    ext.set_queue_id(overrides.get("queue_id", 3))
    ext.set_queue_offset(overrides.get("queue_offset", 88))
    ext.set_commit_log_offset(overrides.get("commit_log_offset", 1024))
    ext.set_born_timestamp(1700000000000)
    ext.set_store_timestamp(1700000000123)
    ext.set_born_host("127.0.0.1")
    ext.born_host_port = 54321
    ext.set_store_host("127.0.0.1")
    ext.store_host_port = 10911
    ext.set_reconsume_times(1)
    ext.set_prepared_transaction_offset(0)
    ext.set_properties({"TAGS": "TagA", "KEYS": "key1 key2"})
    return ext


# ----------------------------------------------------------- 属性串


class TestPropertiesCodec:
    def test_roundtrip(self):
        props = {"TAGS": "TagA", "KEYS": "k1 k2 k3", "UNIQ_KEY": "0A0A0A0A"}
        raw = message_properties_2_string(props)
        assert raw.count(chr(1)) == 3
        assert raw.count(chr(2)) == 3
        assert string_2_message_properties(raw) == props

    def test_none_map(self):
        assert message_properties_2_string(None) == ""
        assert string_2_message_properties(None) == {}

    def test_empty_string(self):
        assert string_2_message_properties("") == {}

    def test_none_value_skipped(self):
        assert message_properties_2_string({"a": None}) == ""

    def test_unicode_roundtrip(self):
        props = {"中文键": "中文值 带空格"}
        assert string_2_message_properties(message_properties_2_string(props)) == props

    def test_garbage_segments_ignored(self):
        """Java：长度 < 3 或无 kv 分隔符的片段会被跳过。"""
        assert string_2_message_properties("a\x02") == {}
        assert string_2_message_properties("ab\x02") == {}
        assert string_2_message_properties("no-separator\x02") == {}
        assert string_2_message_properties("k\x01v\x02") == {"k": "v"}

    def test_empty_value_is_dropped(self):
        """与 Java 一致：newIndex-index>=3 且 kvSep < newIndex-1 才计入，
        空 value ``k\\x01\\x02`` 会被判定为非法片段而丢失，这里锁住该行为。"""
        raw = message_properties_2_string({"WAIT": ""})
        assert raw == "WAIT\x01\x02"
        assert string_2_message_properties(raw) == {}


# ------------------------------------------------------- 17 段编解码


class TestMessageExtCodec:
    def test_full_roundtrip(self):
        ext = build_ext()
        raw = encode_message_ext(ext)
        got = decode_message(raw)
        assert got is not None
        assert got.get_topic() == "TopicTest"
        assert got.get_body() == b"hello rocketmq"
        assert got.get_flag() == 2
        assert got.get_queue_id() == 3
        assert got.get_queue_offset() == 88
        assert got.get_commit_log_offset() == 1024
        assert got.get_body_crc() == crc32(b"hello rocketmq")
        assert got.get_born_timestamp() == 1700000000000
        assert got.get_store_timestamp() == 1700000000123
        assert got.get_born_host() == "127.0.0.1" and got.born_host_port == 54321
        assert got.get_store_host() == "127.0.0.1" and got.store_host_port == 10911
        assert got.get_reconsume_times() == 1
        assert got.get_properties() == {"TAGS": "TagA", "KEYS": "key1 key2"}

    def test_totalsize_is_authoritative(self):
        """TOTALSIZE 必须等于实际帧长，否则 broker/puller 无法切分消息边界。"""
        ext = build_ext()
        raw = encode_message_ext(ext)
        (store_size,) = struct.unpack_from(">i", raw, 0)
        (magic,) = struct.unpack_from(">i", raw, 4)
        assert store_size == len(raw)
        assert magic == MESSAGE_MAGIC_CODE

    def test_field_offsets(self):
        ext = build_ext()
        raw = encode_message_ext(ext)
        assert struct.unpack_from(">q", raw, 4 + 4 + 4 + 4 + 4)[0] == 88          # QUEUEOFFSET@20
        assert struct.unpack_from(">q", raw, 28)[0] == 1024                        # PHYSICALOFFSET@28
        assert struct.unpack_from(">i", raw, 36)[0] == 0                           # SYSFLAG@36
        assert struct.unpack_from(">q", raw, 56)[0] == 1700000000123               # STORETIMESTAMP@56

    def test_store_size_respects_existing_value(self):
        ext = build_ext()
        ext.set_store_size(1)  # < 真实长度时应被 max 兜底到可用长度
        raw = encode_message_ext(ext)
        assert struct.unpack_from(">i", raw, 0)[0] == len(raw)

    def test_msg_id_from_store_host_and_offset(self):
        ext = build_ext()
        raw = encode_message_ext(ext)
        got = decode_message(raw)
        expected = create_message_id(ip_and_port_to_bytes("127.0.0.1", 10911), 1024)
        assert got.get_msg_id() == expected
        assert got.get_offset_msg_id() == expected

    def test_unknown_magic_code_returns_none(self):
        raw = bytearray(encode_message_ext(build_ext()))
        raw[4:8] = struct.pack(">i", BLANK_MAGIC_CODE)
        assert decode_message(bytes(raw)) is None

    def test_truncated_frame_returns_none(self):
        raw = encode_message_ext(build_ext())
        assert decode_message(raw[:20]) is None

    def test_read_body_false_keeps_body_none(self):
        raw = encode_message_ext(build_ext())
        got = decode_message(raw, read_body=False)
        assert got is not None
        assert got.get_body() is None
        assert got.get_topic() == "TopicTest"

    def test_check_crc_detects_corruption(self):
        ext = build_ext()
        raw = bytearray(encode_message_ext(ext))
        # body 紧跟在 TOTALSIZE..PREPAREDTXOFFSET(共 84 字节) + 4 字节长度之后
        body_start = 84 + 4
        raw[body_start] ^= 0xFF
        got = decode_message(bytes(raw), check_crc=True)
        assert got is None

    def test_decodes_multiple_messages(self):
        stream = encode_message_ext(build_ext(body=b"one")) + encode_message_ext(build_ext(body=b"two"))
        msgs = decode_messages(stream)
        assert [m.get_body() for m in msgs] == [b"one", b"two"]

    def test_decodes_stops_on_garbage(self):
        stream = encode_message_ext(build_ext(body=b"one")) + b"\x00\x01"
        assert [m.get_body() for m in decode_messages(stream)] == [b"one"]

    def test_count_inner_msg_num(self):
        stream = b"".join(encode_message_ext(build_ext(body=b"m%d" % i)) for i in range(3))
        assert count_inner_msg_num(stream) == 3

    def test_ipv6_hosts(self):
        ext = build_ext()
        sys_flag = MessageSysFlag.BORNHOST_V6_FLAG | MessageSysFlag.STOREHOSTADDRESS_V6_FLAG
        ext.set_sys_flag(sys_flag)
        ext.set_born_host("2001:db8::1")
        ext.set_store_host("2001:db8::2")
        ext.set_commit_log_offset(64)
        raw = encode_message_ext(ext)
        got = decode_message(raw)
        assert got is not None
        assert got.get_born_host() == "2001:db8::1"
        assert got.get_store_host() == "2001:db8::2"
        assert got.get_msg_id() == create_message_id(
            ip_and_port_to_bytes("2001:db8::2", 10911, v6=True), 64)

    def test_ip_port_bytes(self):
        raw = ip_and_port_to_bytes("10.0.0.1", 9876)
        assert len(raw) == 8
        assert bytes_to_ip_and_port(raw) == ("10.0.0.1", 9876)
        raw6 = ip_and_port_to_bytes("2001:db8::1", 9876, v6=True)
        assert len(raw6) == 20
        assert bytes_to_ip_and_port(raw6) == ("2001:db8::1", 9876)


# ------------------------------------------------- msgId / 十六进制


class TestMessageId:
    def test_create_and_decode_roundtrip_v4(self):
        msg_id = create_message_id(ip_and_port_to_bytes("10.0.0.1", 10911), 2048)
        assert len(msg_id) == 32
        assert msg_id == msg_id.upper(), "Java UtilAll.bytes2string 产出大写十六进制"
        assert decode_message_id(msg_id) == ("10.0.0.1", 10911, 2048)

    def test_create_and_decode_roundtrip_v6(self):
        msg_id = create_message_id(ip_and_port_to_bytes("2001:db8::1", 10911, v6=True), 4096)
        assert len(msg_id) == 56
        assert decode_message_id(msg_id) == ("2001:db8::1", 10911, 4096)

    def test_bytes2string_matches_java_hex(self):
        assert bytes2string(b"\x00\x0f\xab\xff") == "000FABFF"


# --------------------------------------------------------- V2 魔数


def build_v2_frame(topic: str, body: bytes, properties_bytes: bytes) -> bytes:
    """手工构造 V2 帧（topic 长度 2 字节），模拟 broker 写入超长 topic 的场景。"""
    topic_bytes = topic.encode("utf-8")
    store_size = (4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8
                  + 4 + len(body) + 2 + len(topic_bytes) + 2 + len(properties_bytes))
    buf = bytearray()
    buf += struct.pack(">i", store_size)
    buf += struct.pack(">i", MESSAGE_MAGIC_CODE_V2)
    buf += struct.pack(">I", crc32(body))
    buf += struct.pack(">i", 0)
    buf += struct.pack(">i", 0)
    buf += struct.pack(">q", 0)
    buf += struct.pack(">q", 2048)
    buf += struct.pack(">i", 0)
    buf += struct.pack(">q", 1700000000000)
    buf += ip_and_port_to_bytes("127.0.0.1", 54321)
    buf += struct.pack(">q", 1700000000123)
    buf += ip_and_port_to_bytes("127.0.0.1", 10911)
    buf += struct.pack(">i", 0)
    buf += struct.pack(">q", 0)
    buf += struct.pack(">i", len(body)) + body
    buf += struct.pack(">H", len(topic_bytes)) + topic_bytes
    buf += struct.pack(">H", len(properties_bytes)) + properties_bytes
    return bytes(buf)


class TestV2MagicCode:
    def test_long_topic_decode(self):
        topic = "TopicTestLong" + "x" * 130          # > 127，必须走 V2
        raw = build_v2_frame(topic, b"batch-body", b"TAGS\x01TagB\x02")
        msgs = decode_messages(raw)
        assert len(msgs) == 1
        assert msgs[0].get_topic() == topic
        assert msgs[0].get_body() == b"batch-body"
        assert msgs[0].get_properties().get("TAGS") == "TagB"

    def test_v2_msg_id(self):
        topic = "L" * 200
        raw = build_v2_frame(topic, b"x", b"")
        got = decode_message(raw)
        assert got is not None
        assert got.get_msg_id() == create_message_id(ip_and_port_to_bytes("127.0.0.1", 10911), 2048)


# --------------------------------------------- 6 段批量消息编码路径


class TestBatchMessageCodec:
    def test_encode_one_message_layout(self):
        msg = Message(topic="T", body=b"body", tags="TagA")
        msg.set_flag(7)
        raw = encode_message(msg)
        assert struct.unpack_from(">i", raw, 0)[0] == len(raw)
        assert struct.unpack_from(">i", raw, 4)[0] == 0        # MAGICCODE 固定 0
        assert struct.unpack_from(">i", raw, 8)[0] == 0        # BODYCRC 固定 0
        assert struct.unpack_from(">i", raw, 12)[0] == 7       # FLAG
        assert struct.unpack_from(">i", raw, 16)[0] == 4       # BODY LEN

    def test_roundtrip_single(self):
        msg = Message(topic="T", body=b"body", tags="TagA", keys="k1")
        got = decode_batch_message(encode_message(msg))
        assert got.get_body() == b"body"
        assert got.get_flag() == msg.get_flag()
        assert got.get_properties() == msg.get_properties()

    def test_roundtrip_multiple(self):
        msgs = [Message(topic="T", body=b"m1"), Message(topic="T", body=b"m2", tags="TagA")]
        raw = encode_messages(msgs)
        got = decode_batch_messages(raw)
        assert [m.get_body() for m in got] == [b"m1", b"m2"]
        assert got[1].get_properties().get("TAGS") == "TagA"

    def test_message_batch_body_is_encoded_messages(self):
        batch = MessageBatch.generate_from_list([Message(topic="T", body=b"m1"),
                                                 Message(topic="T", body=b"m2")])
        assert batch.get_body() == encode_messages(batch.messages)
        assert decode_batch_messages(batch.get_body())[0].get_body() == b"m1"

    def test_count_inner_messages(self):
        raw = encode_messages([Message(topic="T", body=b"m%d" % i) for i in range(4)])
        assert count_inner_msg_num(raw) == 4


# ------------------------------------------------------------ crc32


def test_crc32_is_standard():
    assert crc32(b"") == 0
    assert crc32(b"a") == 0xE8B7BE43
    assert crc32(b"abc") == 0x352441C2


@pytest.mark.parametrize("compress_type", [MessageSysFlag.ZLIB_TYPE])
def test_compressed_message_roundtrip(compress_type):
    """带 COMPRESSED_FLAG 的消息解码时需解压（zlib 可用）。"""
    body = b"compress-me" * 50
    import zlib as _zlib

    sys_flag = MessageSysFlag.COMPRESSED_FLAG | MessageSysFlag.set_compression_type(0, compress_type)
    topic_bytes = b"TopicTest"
    props_bytes = b""
    payload = _zlib.compress(body, 5)
    store_size = (4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8
                  + 4 + len(payload) + 1 + len(topic_bytes) + 2 + len(props_bytes))
    buf = bytearray()
    buf += struct.pack(">i", store_size)
    buf += struct.pack(">i", MESSAGE_MAGIC_CODE)
    buf += struct.pack(">I", crc32(body))
    buf += struct.pack(">i", 0)
    buf += struct.pack(">i", 0)
    buf += struct.pack(">q", 0)
    buf += struct.pack(">q", 1)
    buf += struct.pack(">i", sys_flag)
    buf += struct.pack(">q", 1700000000000)
    buf += ip_and_port_to_bytes("127.0.0.1", 1)
    buf += struct.pack(">q", 1700000000123)
    buf += ip_and_port_to_bytes("127.0.0.1", 10911)
    buf += struct.pack(">i", 0)
    buf += struct.pack(">q", 0)
    buf += struct.pack(">i", len(payload)) + payload
    buf += struct.pack(">B", len(topic_bytes)) + topic_bytes
    buf += struct.pack(">H", len(props_bytes)) + props_bytes

    got = decode_message(bytes(buf))
    assert got is not None
    assert got.get_body() == body
    # Java: sysFlag &= ~COMPRESSED_FLAG，仅清标志位，压缩类型位（bit8~10）保留
    assert got.get_sys_flag() == MessageSysFlag.clear_compressed_flag(sys_flag)
    assert got.get_sys_flag() == MessageSysFlag.set_compression_type(0, compress_type)
