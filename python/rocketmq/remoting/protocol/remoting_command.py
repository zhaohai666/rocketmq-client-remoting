# -*- coding: utf-8 -*-
"""RemotingCommand：RocketMQ 远程命令（对应 org.apache.rocketmq.remoting.protocol.RemotingCommand）。

线格式：totalLength(4) | headerLength(带序列化类型高8位, 4) | header | body
header（JSON）：RemotingSerializable JSON 编码
header（ROCKETMQ）：code(2) language(1) version(2) opaque(4) flag(4) remark(int+utf8) extFields(int + key(short+utf8) value(int+utf8))
"""
from __future__ import annotations

import itertools
import json
import os
import threading
import struct
from typing import Any, Dict, List, Optional, Type

from .codes import LanguageCode, RemotingCommandType, SerializeType
from .serialize import RemotingSerializable, RocketMQSerializable

SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type"
SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE"
REMOTING_VERSION_KEY = "rocketmq.remoting.version"

RPC_TYPE = 0
RPC_ONEWAY = 1

_serialize_type_config_in_this_server = SerializeType.JSON


def _load_serialize_type_config():
    global _serialize_type_config_in_this_server
    v = os.environ.get(SERIALIZE_TYPE_ENV) or os.environ.get(SERIALIZE_TYPE_PROPERTY)
    if v:
        v = v.strip().upper()
        if v == "ROCKETMQ":
            _serialize_type_config_in_this_server = SerializeType.ROCKETMQ
        else:
            _serialize_type_config_in_this_server = SerializeType.JSON


_load_serialize_type_config()

_request_id = itertools.count()


class RemotingCommand:
    def __init__(self, code: int = 0, custom_header=None, remark: Optional[str] = None,
                 opaque: Optional[int] = None, flag: int = 0, body: Optional[bytes] = None):
        self.code = code
        self.language = LanguageCode.PYTHON
        self.version = 0
        self.opaque = next(_request_id) if opaque is None else opaque
        self.flag = flag
        self.remark = remark
        self.ext_fields: Dict[str, str] = {}
        self.custom_header = custom_header
        self.body = body
        self.serialize_type_current_rpc = _serialize_type_config_in_this_server
        self.cached_header = None

    # ---------------- 工厂方法 ----------------
    @staticmethod
    def create_request_command(code: int, custom_header=None) -> "RemotingCommand":
        cmd = RemotingCommand(code=code, custom_header=custom_header)
        _set_cmd_version(cmd)
        return cmd

    @staticmethod
    def create_response_command_with_header(code: int, custom_header=None) -> "RemotingCommand":
        cmd = RemotingCommand(code=code, custom_header=custom_header)
        cmd.mark_response_type()
        _set_cmd_version(cmd)
        return cmd

    @staticmethod
    def create_response_command(code: int = 0, remark: str = "not set any response code",
                                class_header: Optional[Type] = None) -> Optional["RemotingCommand"]:
        """与 Java createResponseCommand(int code, String remark, Class classHeader) 对齐。"""
        cmd = RemotingCommand(code=code, remark=remark)
        cmd.mark_response_type()
        _set_cmd_version(cmd)
        if class_header is not None:
            try:
                cmd.custom_header = class_header()
            except Exception:
                return None
        return cmd

    @staticmethod
    def build_error_response(code: int, remark: str) -> Optional["RemotingCommand"]:
        return RemotingCommand.create_response_command(code, remark, None)

    @staticmethod
    def get_protocol_type(source: int) -> int:
        """返回序列化类型数值（与 Java byte getProtocolType 对齐），供与 SerializeType 常量比较。"""
        return (source >> 24) & 0xFF

    @staticmethod
    def get_header_length(length: int) -> int:
        return length & 0xFFFFFF

    @staticmethod
    def mark_protocol_type(source: int, stype: int) -> int:
        return ((stype & 0xFF) << 24) | (source & 0x00FFFFFF)

    # ---------------- 编解码 ----------------
    def mark_response_type(self) -> None:
        self.flag |= 1 << RPC_TYPE

    def is_response_type(self) -> bool:
        return (self.flag & (1 << RPC_TYPE)) == (1 << RPC_TYPE)

    def mark_oneway_rpc(self) -> None:
        self.flag |= 1 << RPC_ONEWAY

    def is_oneway_rpc(self) -> bool:
        return (self.flag & (1 << RPC_ONEWAY)) == (1 << RPC_ONEWAY)

    def get_type(self) -> str:
        return RemotingCommandType.RESPONSE_COMMAND if self.is_response_type() else RemotingCommandType.REQUEST_COMMAND

    def make_custom_header_to_net(self) -> None:
        """把 custom_header 的非 None 字段写入 ext_fields（对应 Java 反射行为）。"""
        if self.custom_header is not None:
            for name, value in self.custom_header.to_ext_fields().items():
                if value is not None:
                    self.ext_fields[name] = str(value)

    def header_encode(self) -> bytes:
        self.make_custom_header_to_net()
        if self.serialize_type_current_rpc == SerializeType.ROCKETMQ:
            return RocketMQSerializable.rocket_mq_protocol_encode(self)
        return RemotingSerializable.encode(self._to_dict())

    def encode(self) -> bytes:
        length = 4
        header_data = self.header_encode()
        length += len(header_data)
        body = self.body
        if body is not None:
            length += len(body)
        out = bytearray()
        out += struct.pack(">i", length)
        out += struct.pack(">i", RemotingCommand.mark_protocol_type(len(header_data), self.serialize_type_current_rpc))
        out += header_data
        if body is not None:
            out += body
        return bytes(out)

    def encode_header(self, body_length: int = 0) -> bytes:
        length = 4
        header_data = self.header_encode()
        length += len(header_data) + body_length
        out = bytearray()
        out += struct.pack(">i", length)
        out += struct.pack(">i", RemotingCommand.mark_protocol_type(len(header_data), self.serialize_type_current_rpc))
        out += header_data
        return bytes(out)

    @staticmethod
    def decode(data: bytes) -> "RemotingCommand":
        offset = 0
        (total_length,) = struct.unpack_from(">i", data, offset)
        offset += 4
        if total_length > len(data) - 4:
            from ..exception import RemotingCommandException
            raise RemotingCommandException("decode error, bad total length: %d" % total_length)
        (ori_header_len,) = struct.unpack_from(">i", data, offset)
        offset += 4
        header_length = RemotingCommand.get_header_length(ori_header_len)
        if header_length > len(data) - offset:
            from ..exception import RemotingCommandException
            raise RemotingCommandException("decode error, bad header length: %d" % header_length)
        protocol_type = RemotingCommand.get_protocol_type(ori_header_len)
        header_data = data[offset:offset + header_length]
        offset += header_length
        if protocol_type == SerializeType.ROCKETMQ:
            fields = RocketMQSerializable.rocket_mq_protocol_decode(header_data)
            cmd = RemotingCommand(code=fields["code"], remark=fields["remark"], flag=fields["flag"])
            cmd.language = fields["language"]
            cmd.version = fields["version"]
            cmd.opaque = fields["opaque"]
            cmd.ext_fields = fields["extFields"] or {}
        else:
            obj = json.loads(header_data.decode("utf-8"))
            cmd = RemotingCommand(code=int(obj.get("code", 0)), remark=obj.get("remark"), flag=int(obj.get("flag", 0)))
            # 5.x NameServer 把 language 序列化为枚举名字符串（如 "JAVA"），4.x 用 int；
            # 这里兼容两种形态，统一成 int 码。
            _lang = obj.get("language", LanguageCode.PYTHON)
            if isinstance(_lang, str):
                _lang = LanguageCode.name_to_code(_lang)
            cmd.language = int(_lang)
            cmd.version = int(obj.get("version", 0))
            cmd.opaque = int(obj.get("opaque", -1))
            cmd.ext_fields = obj.get("extFields") or {}
        cmd.serialize_type_current_rpc = protocol_type
        body_length = len(data) - offset
        cmd.body = data[offset:] if body_length > 0 else None
        return cmd

    def decode_command_custom_header(self, header_cls: Type, use_fast_encode: bool = True):
        """把 ext_fields 映射到 header 对象（对应 Java decodeCommandCustomHeader）。

        优先走 header 自带的 from_ext_fields（短字段名等特殊映射），
        否则按属性名逐字段赋值并做基础类型转换。
        """
        h = header_cls()
        if hasattr(h, "from_ext_fields"):
            h.from_ext_fields(self.ext_fields)
        else:
            for name, value in self.ext_fields.items():
                if hasattr(h, name):
                    setattr(h, name, _convert_value(getattr(header_cls, name, None), value))
        self.cached_header = h
        return h

    def __jsonfields_skip__(self, name: str) -> bool:
        return name in ("custom_header", "cached_header", "serialize_type_current_rpc", "body", "ext_fields")

    def _to_dict(self) -> dict:
        d: dict = {
            "code": self.code,
            "language": self.language,
            "version": self.version,
            "opaque": self.opaque,
            "flag": self.flag,
        }
        if self.remark is not None:
            d["remark"] = self.remark
        if self.ext_fields:
            d["extFields"] = self.ext_fields
        return d

    def add_ext_field(self, key: str, value: str) -> None:
        self.ext_fields[key] = value

    def get_ext_field(self, key: str) -> Optional[str]:
        return self.ext_fields.get(key)

    def __repr__(self):
        return "RemotingCommand [code=%s, language=%s, version=%s, opaque=%s, flag(B)=%s, remark=%s, extFields=%s, serializeTypeCurrentRPC=%s]" % (
            self.code, self.language, self.version, self.opaque, bin(self.flag),
            self.remark, self.ext_fields, self.serialize_type_current_rpc)


def _set_cmd_version(cmd: RemotingCommand) -> None:
    v = os.environ.get(REMOTING_VERSION_KEY)
    if v:
        try:
            cmd.version = int(v)
            return
        except ValueError:
            pass
    cmd.version = 0


def _convert_value(field_type, value: str):
    """按字段类型转换 ext_fields 字符串值。"""
    if field_type in (int, "int"):
        return int(value)
    if field_type in (float, "float"):
        return float(value)
    if field_type in (bool, "bool"):
        return value.lower() in ("true", "1")
    return value