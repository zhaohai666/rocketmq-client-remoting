# -*- coding: utf-8 -*-
"""协议序列化：JSON（RemotingSerializable）与 RocketMQ 私有二进制（RocketMQSerializable）。

对应 org.apache.rocketmq.remoting.protocol.RemotingSerializable / RocketMQSerializable。
"""
from __future__ import annotations

import json
import struct
from typing import Any, Dict, List, Optional


class RemotingSerializable:
    @staticmethod
    def encode(obj: Any) -> bytes:
        if obj is None:
            return b""
        if isinstance(obj, (bytes, bytearray)):
            return bytes(obj)
        return json.dumps(obj, default=_json_default, ensure_ascii=False).encode("utf-8")

    @staticmethod
    def to_json(obj: Any, pretty_format: bool = False) -> str:
        if pretty_format:
            return json.dumps(obj, default=_json_default, ensure_ascii=False, indent=2)
        return json.dumps(obj, default=_json_default, ensure_ascii=False)

    @staticmethod
    def decode(data: Optional[bytes], expect_type=None):
        if data is None or len(data) == 0:
            return None
        text = data.decode("utf-8")
        return RemotingSerializable.from_json(text, expect_type)

    @staticmethod
    def from_json(json_str: str, expect_type=None):
        obj = json.loads(json_str)
        if expect_type is not None and not isinstance(obj, expect_type):
            try:
                return expect_type.from_dict(obj)
            except Exception:
                return obj
        return obj

    @staticmethod
    def decode_json(data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


def _json_default(obj: Any):
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", "replace")
    return str(obj)


class RocketMQSerializable:
    """RocketMQ 4.x 私有二进制协议：code(2) + language(1) + version(2) + opaque(4) + flag(4) + remark(int+utf8) + extFields(int + key(short+utf8)/value(int+utf8))。"""

    @staticmethod
    def write_decimal_long(buf: bytearray, value: int) -> None:
        buf += struct.pack(">i", 0)
        start = len(buf)
        value = int(value)
        if value == 0:
            buf += b"0"
        else:
            neg = value < 0
            if neg:
                buf += b"-"
                if value == -9223372036854775808:
                    buf += b"9223372036854775808"
                    buf[start - 4:start] = struct.pack(">i", len(buf) - start)
                    return
                value = -value
            buf += str(value).encode("utf-8")
        buf[start - 4:start] = struct.pack(">i", len(buf) - start)

    @staticmethod
    def write_decimal_int(buf: bytearray, value: int) -> None:
        RocketMQSerializable.write_decimal_long(buf, value)

    @staticmethod
    def write_str(buf: bytearray, use_short_length: bool, s: Optional[str]) -> None:
        bs = b"" if s is None else s.encode("utf-8")
        n = len(bs)
        if use_short_length:
            buf += struct.pack(">H", n)
        else:
            buf += struct.pack(">i", n)
        buf += bs

    @staticmethod
    def read_str(buf, offset: int, use_short_length: bool):
        if use_short_length:
            (n,) = struct.unpack_from(">H", buf, offset)
            offset += 2
        else:
            (n,) = struct.unpack_from(">i", buf, offset)
            offset += 4
        if n == 0:
            return None, offset
        s = bytes(buf[offset:offset + n]).decode("utf-8")
        return s, offset + n

    @staticmethod
    def map_serialize(map_data: Optional[Dict[str, str]]) -> Optional[bytes]:
        if not map_data:
            return None
        buf = bytearray()
        for k, v in map_data.items():
            if k is None or v is None:
                continue
            RocketMQSerializable.write_str(buf, True, k)
            RocketMQSerializable.write_str(buf, False, v)
        return bytes(buf)

    @staticmethod
    def cal_total_len(remark: Optional[str], ext: Optional[bytes]) -> int:
        remark_len = 0 if not remark else len(remark.encode("utf-8"))
        ext_len = 0 if not ext else len(ext)
        return 2 + 1 + 2 + 4 + 4 + 4 + remark_len + 4 + ext_len

    @staticmethod
    def rocket_mq_protocol_encode(cmd) -> bytes:
        remark = cmd.remark
        remark_bytes = None
        if remark:
            remark_bytes = remark.encode("utf-8")
        ext_fields_bytes = RocketMQSerializable.map_serialize(cmd.ext_fields)
        total_len = RocketMQSerializable.cal_total_len(remark, ext_fields_bytes)
        buf = bytearray()
        buf += struct.pack(">h", cmd.code & 0xFFFF)
        buf += struct.pack(">B", cmd.language & 0xFF)
        buf += struct.pack(">h", cmd.version & 0xFFFF)
        buf += struct.pack(">i", cmd.opaque)
        buf += struct.pack(">i", cmd.flag)
        if remark_bytes is not None:
            buf += struct.pack(">i", len(remark_bytes))
            buf += remark_bytes
        else:
            buf += struct.pack(">i", 0)
        if ext_fields_bytes is not None:
            buf += struct.pack(">i", len(ext_fields_bytes))
            buf += ext_fields_bytes
        else:
            buf += struct.pack(">i", 0)
        assert len(buf) == total_len, "rocketmq protocol encode length mismatch"
        return bytes(buf)

    @staticmethod
    def map_deserialize(buf, offset: int, length: int):
        map_data: Dict[str, str] = {}
        end = offset + length
        while offset < end:
            k, offset = RocketMQSerializable.read_str(buf, offset, True)
            v, offset = RocketMQSerializable.read_str(buf, offset, False)
            if k is not None and v is not None:
                map_data[k] = v
        return map_data, offset

    @staticmethod
    def rocket_mq_protocol_decode(header_bytes: bytes) -> dict:
        offset = 0
        (code,) = struct.unpack_from(">h", header_bytes, offset)
        offset += 2
        (language,) = struct.unpack_from(">B", header_bytes, offset)
        offset += 1
        (version,) = struct.unpack_from(">h", header_bytes, offset)
        offset += 2
        (opaque,) = struct.unpack_from(">i", header_bytes, offset)
        offset += 4
        (flag,) = struct.unpack_from(">i", header_bytes, offset)
        offset += 4
        remark, offset = RocketMQSerializable.read_str(header_bytes, offset, False)
        (ext_len,) = struct.unpack_from(">i", header_bytes, offset)
        offset += 4
        ext_fields = None
        if ext_len > 0:
            ext_fields, offset = RocketMQSerializable.map_deserialize(header_bytes, offset, ext_len)
        return {
            "code": code & 0xFFFF,
            "language": language,
            "version": version & 0xFFFF,
            "opaque": opaque,
            "flag": flag,
            "remark": remark,
            "extFields": ext_fields,
        }