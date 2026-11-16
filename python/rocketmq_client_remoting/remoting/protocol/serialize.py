# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
import struct
from typing import Any, Dict, List, Optional

# FastJSON (the default JSON implementation of RocketMQ 5.x) writes the numeric keys
# of a Map without quotes, e.g. {"brokerAddrs":{0:"127.0.0.1:10911"}}, which is not
# standard JSON. Compatibility is handled here: a numeric key that directly follows
# "{" or "," is matched and the quotes are added back.
_ESCAPES = {'"': '"', '\\': '\\', '/': '/', 'b': '\b', 'f': '\f',
            'n': '\n', 'r': '\r', 't': '\t'}
_WS = ' \t\r\n'
_NUMBER_RE = re.compile(r'-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?')


class FastJsonDecodeError(ValueError):
    pass


# The output of FastJSON2 (the default JSON implementation of RocketMQ 5.x) is **not**
# strict JSON, so json.loads alone cannot parse it:
#  1) numeric Map keys are unquoted: {"brokerAddrs":{0:"127.0.0.1:10911"}}
#  2) object Map keys are inlined directly as JSON objects (that is **invalid JSON**),
#     and the admin side inevitably runs into it:
#     TopicStatsTable / ConsumeStats use MessageQueue as the offsetTable key ->
#       {"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{...}}}
#  3) special floats: NaN / Infinity / -Infinity
# Therefore a small parser that tolerates "non-string keys" is needed, rather than
# patching in the quotes afterwards.
_UNQUOTED_NUM_KEY_RE = re.compile(r'([{,]\s*)(-?\d+(?:\.\d+)?)\s*(:)')


class _FastJsonParser:
    """A JSON parser that tolerates "an object/array used as a map key" (a
    fastjson2-compatible subset)."""

    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.size = len(text)

    # ---------------- infrastructure ----------------
    def _skip_ws(self) -> None:
        while self.pos < self.size and self.text[self.pos] in _WS:
            self.pos += 1

    def _peek(self) -> str:
        if self.pos >= self.size:
            raise FastJsonDecodeError("unexpected end of input at offset %d" % self.pos)
        return self.text[self.pos]

    # ---------------- each type ----------------
    def parse(self):
        value = self._value()
        self._skip_ws()
        if self.pos != self.size:
            raise FastJsonDecodeError("trailing data at offset %d" % self.pos)
        return value

    def _value(self):
        self._skip_ws()
        c = self._peek()
        if c == '{':
            return self._object()
        if c == '[':
            return self._array()
        if c == '"':
            return self._string()
        return self._literal()

    def _object(self) -> Dict[str, Any]:
        self.pos += 1  # consume '{'
        result: Dict[str, Any] = {}
        self._skip_ws()
        if self._peek() == '}':
            self.pos += 1
            return result
        while True:
            key = self._key()
            self._skip_ws()
            if self._peek() != ':':
                raise FastJsonDecodeError("expected ':' at offset %d" % self.pos)
            self.pos += 1
            result[key] = self._value()
            self._skip_ws()
            c = self._peek()
            if c == ',':
                self.pos += 1
                self._skip_ws()
                if self._peek() == '}':  # tolerate a trailing comma
                    self.pos += 1
                    return result
                continue
            if c == '}':
                self.pos += 1
                return result
            raise FastJsonDecodeError("expected ',' or '}' at offset %d" % self.pos)

    def _array(self) -> List[Any]:
        self.pos += 1  # consume '['
        items: List[Any] = []
        self._skip_ws()
        if self._peek() == ']':
            self.pos += 1
            return items
        while True:
            items.append(self._value())
            self._skip_ws()
            c = self._peek()
            if c == ',':
                self.pos += 1
                self._skip_ws()
                if self._peek() == ']':
                    self.pos += 1
                    return items
                continue
            if c == ']':
                self.pos += 1
                return items
            raise FastJsonDecodeError("expected ',' or ']' at offset %d" % self.pos)

    def _key(self):
        """The key: a string / an inline object / an inline array / a bare literal (number, true, false, null)."""
        self._skip_ws()
        c = self._peek()
        if c == '"':
            return self._string()
        if c in '{[':
            # fastjson2 writes a non-string map key (such as MessageQueue) as-is in
            # JSON. The raw text is preserved so that callers can parse it again with
            # decode_message_queue_key().
            start = self.pos
            self._value()
            return self.text[start:self.pos]
        start = self.pos
        while self.pos < self.size and self.text[self.pos] not in ':' + _WS:
            self.pos += 1
        return self.text[start:self.pos]

    def _string(self) -> str:
        self.pos += 1  # consume the opening quote
        out: List[str] = []
        while True:
            if self.pos >= self.size:
                raise FastJsonDecodeError("unterminated string")
            c = self.text[self.pos]
            if c == '"':
                self.pos += 1
                return "".join(out)
            if c == '\\':
                self.pos += 1
                if self.pos >= self.size:
                    raise FastJsonDecodeError("unterminated escape")
                e = self.text[self.pos]
                if e == 'u':
                    digits = self.text[self.pos + 1:self.pos + 5]
                    if len(digits) != 4:
                        raise FastJsonDecodeError("bad \\u escape")
                    out.append(chr(int(digits, 16)))
                    self.pos += 5
                    continue
                out.append(_ESCAPES.get(e, e))
                self.pos += 1
                continue
            out.append(c)
            self.pos += 1

    def _literal(self):
        rest = self.text[self.pos:]
        for token, value in (("true", True), ("false", False), ("null", None),
                             ("NaN", float("nan")), ("Infinity", float("inf")),
                             ("-Infinity", float("-inf"))):
            if rest.startswith(token):
                tail = rest[len(token):len(token) + 1]
                # must not mistake things like "nullish" for null
                if tail == "" or tail in ',}]' + _WS:
                    self.pos += len(token)
                    return value
        m = _NUMBER_RE.match(rest)
        if m is None:
            raise FastJsonDecodeError("unexpected token at offset %d: %r"
                                      % (self.pos, rest[:20]))
        self.pos += m.end()
        raw = m.group(0)
        if '.' in raw or 'e' in raw or 'E' in raw:
            return float(raw)
        return int(raw)


def fastjson_loads(text: str) -> Any:
    """Parse fastjson2 output (including non-string map keys). Raises FastJsonDecodeError on failure."""
    return _FastJsonParser(text).parse()


def decode_message_queue_key(key: str) -> Optional[Dict[str, Any]]:
    """Turn a MessageQueue inline object key written by fastjson2 back into a dict.

    Returning None means the key is not an inline object (an ordinary string key, for example).
    """
    text = key.strip()
    if not text.startswith('{'):
        return None
    try:
        return fastjson_loads(text)
    except FastJsonDecodeError:
        return None


def _tolerant_json_loads(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    try:
        return fastjson_loads(text)
    except FastJsonDecodeError:
        # Fallback: the legacy path (only patch in quotes for numeric keys). If that
        # still fails, let the exception bubble up instead of silently returning bad data.
        fixed = _UNQUOTED_NUM_KEY_RE.sub(
            lambda m: '%s"%s"%s' % (m.group(1), m.group(2), m.group(3)), text)
        return json.loads(fixed)


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
        return _tolerant_json_loads(data.decode("utf-8"))


def _json_default(obj: Any):
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", "replace")
    return str(obj)


class RocketMQSerializable:
    """The RocketMQ 4.x private binary protocol: code(2) + language(1) + version(2) + opaque(4) + flag(4) + remark(int+utf8) + extFields(int + key(short+utf8)/value(int+utf8))."""

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