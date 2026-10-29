# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime
import socket
import struct
import threading
import time
import zlib
from typing import Optional


def utf16_int32_hash(s: str) -> int:
    pass
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - 0x100000000 if h >= 0x80000000 else h


class UtilAll:
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
        if index is None:
            return index
        if offset < 0:
            return index
        return index

    @staticmethod
    def crc32(data: bytes) -> int:
        """The BODYCRC value a stored message carries: CRC-32 with the sign bit dropped.

        Kept identical to the decoder's helper of the same name; a whole 32-bit CRC is not
        what the wire field holds.
        """
        return zlib.crc32(data) & 0x7FFFFFFF

    HEX_ARRAY = "0123456789ABCDEF"

    @staticmethod
    def bytes_2_string(bs: bytes) -> str:
        pass
        return "".join(UtilAll.HEX_ARRAY[(b >> 4) & 0x0F] + UtilAll.HEX_ARRAY[b & 0x0F] for b in bs)

    @staticmethod
    def string_2_bytes(hex_string: str) -> Optional[bytes]:
        pass
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
    """Unique message ID generator (corresponds to the client-side MessageClientIDSetter)."""

    _lock = threading.Lock()
    _counter = 0

    @staticmethod
    def create_uniq_id() -> str:
        """Generate a 32-character hex unique ID: IP(4|16B) + PID(2B) + class-loader hash(4B) + ms within current month(4B) + auto-increment(2B)."""
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
        # Milliseconds elapsed since the first day of the current month at 00:00:00.
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_ms = (now - month_start) // datetime.timedelta(milliseconds=1)
        result += struct.pack(">I", month_ms)
        result += struct.pack(">H", counter)
        return UtilAll.bytes_2_string(bytes(result))