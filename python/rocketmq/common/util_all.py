# -*- coding: utf-8 -*-
"""通用工具函数（对应 org.apache.rocketmq.common.UtilAll）。"""
from __future__ import annotations

import datetime
import socket
import struct
import threading
import time
import zlib
from typing import Optional


class UtilAll:
    # Java 相同的日期格式化
    YYYY_MM_DD_HH_MM_SS = "%Y-%m-%d %H:%M:%S"
    YYYY_MM_DD_HH_MM_SS_SSS = "%Y-%m-%d %H:%M:%S.%f"

    @staticmethod
    def current_time_millis() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def current_time_seconds() -> int:
        return int(time.time())

    @staticmethod
    def offset_2_filename(offset: int) -> str:
        return "%020d" % offset

    @staticmethod
    def compute_elapse_time_millis(last_time: int) -> int:
        return int(time.time() * 1000) - last_time

    @staticmethod
    def time_to_human_string(ts: int, pattern: str = YYYY_MM_DD_HH_MM_SS) -> str:
        if ts <= 0:
            return "-"
        dt = datetime.datetime.fromtimestamp(ts / 1000.0)
        return dt.strftime(pattern)

    @staticmethod
    def is_blank(s: Optional[str]) -> bool:
        return s is None or s.strip() == ""

    @staticmethod
    def is_not_blank(s: Optional[str]) -> bool:
        return not UtilAll.is_blank(s)

    @staticmethod
    def get_pid() -> int:
        import os
        return os.getpid()

    @staticmethod
    def is_ipv4(addr: str) -> bool:
        try:
            socket.inet_pton(socket.AF_INET, addr)
            return True
        except OSError:
            return False

    @staticmethod
    def string_2_unicode_shift(index: str, offset: int) -> str:
        # 对应 Java：取字符串与整数的异或混淆（保持接口兼容）
        if index is None:
            return index
        if offset < 0:
            return index
        return index

    @staticmethod
    def crc32(data: bytes) -> int:
        return zlib.crc32(data) & 0xFFFFFFFF

    HEX_ARRAY = "0123456789ABCDEF"

    @staticmethod
    def bytes_2_string(bs: bytes) -> str:
        """Java UtilAll.bytes2string：逐字节转大写十六进制。"""
        return "".join(UtilAll.HEX_ARRAY[(b >> 4) & 0x0F] + UtilAll.HEX_ARRAY[b & 0x0F] for b in bs)

    @staticmethod
    def string_2_bytes(hex_string: str) -> Optional[bytes]:
        """Java UtilAll.string2bytes：十六进制字符串 -> 字节（非 UTF-8 编码）。"""
        if hex_string is None or hex_string == "":
            return None
        hex_string = hex_string.upper()
        length = len(hex_string) // 2
        out = bytearray(length)
        for i in range(length):
            pos = i * 2
            out[i] = (UtilAll.char_to_byte(hex_string[pos]) << 4) | UtilAll.char_to_byte(hex_string[pos + 1])
        return bytes(out)

    @staticmethod
    def char_to_byte(c: str) -> int:
        return UtilAll.HEX_ARRAY.index(c)

    @staticmethod
    def empty_bytes() -> bytes:
        return b""

    @staticmethod
    def next_millis() -> int:
        return UtilAll.current_time_millis()

    @staticmethod
    def local_ip() -> str:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            try:
                return socket.gethostbyname(socket.gethostname())
            except Exception:
                return "127.0.0.1"
        finally:
            s.close()


class InnerIdGenerator:
    """消息唯一 ID 生成器（对应用户端 MessageClientIDSetter）。"""

    _lock = threading.Lock()
    _counter = 0

    @staticmethod
    def create_uniq_id() -> str:
        """生成 32 位十六进制唯一 ID：IP(4|16B) + PID(2B) + 类加载hash(4B) + 当月毫秒(4B) + 自增(2B)。"""
        import os
        from .mix_all import MixAll
        pid = os.getpid()
        ip = MixAll.get_ip_str()
        with InnerIdGenerator._lock:
            InnerIdGenerator._counter += 1
            counter = InnerIdGenerator._counter & 0xFFFF
        result = bytearray()
        if UtilAll.is_ipv4(ip):
            result += socket.inet_pton(socket.AF_INET, ip)
        else:
            result += socket.inet_pton(socket.AF_INET6, ip)
        result += struct.pack(">H", pid & 0xFFFF)
        result += struct.pack(">I", abs(hash("RocketMQClient")) & 0xFFFFFFFF)
        now = datetime.datetime.now()
        month_ms = ((now.hour * 60 + now.minute) * 60 + now.second) * 1000 + now.microsecond // 1000
        result += struct.pack(">I", month_ms)
        result += struct.pack(">H", counter)
        return UtilAll.bytes_2_string(bytes(result))