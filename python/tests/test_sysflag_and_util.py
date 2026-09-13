# -*- coding: utf-8 -*-
"""系统标志位与通用工具测试（sysflag / UtilAll / MixAll）。"""
from __future__ import annotations

import pytest

from rocketmq.common.sysflag import MessageSysFlag, PermName, PullSysFlag
from rocketmq.common.util_all import UtilAll


class TestMessageSysFlag:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("COMPRESSED_FLAG", 0x1),
            ("MULTI_TAGS_FLAG", 0x2),
            ("TRANSACTION_NOT_TYPE", 0),
            ("TRANSACTION_PREPARED_TYPE", 0x4),
            ("TRANSACTION_COMMIT_TYPE", 0x8),
            ("TRANSACTION_ROLLBACK_TYPE", 0xC),
            ("BORNHOST_V6_FLAG", 0x10),
            ("STOREHOSTADDRESS_V6_FLAG", 0x20),
            ("NEED_UNWRAP_FLAG", 0x40),
            ("INNER_BATCH_FLAG", 0x80),
            ("COMPRESSION_LZ4_TYPE", 0x1 << 8),
            ("COMPRESSION_ZSTD_TYPE", 0x2 << 8),
            ("COMPRESSION_ZLIB_TYPE", 0x3 << 8),
            ("COMPRESSION_TYPE_COMPARATOR", 0x7 << 8),
        ],
    )
    def test_bit_positions(self, name, expected):
        assert getattr(MessageSysFlag, name) == expected

    def test_compression_type_roundtrip(self):
        for ctype in (MessageSysFlag.LZ4_TYPE, MessageSysFlag.ZSTD_TYPE, MessageSysFlag.ZLIB_TYPE):
            flag = MessageSysFlag.set_compression_type(0, ctype)
            assert MessageSysFlag.get_compression_type(flag) == ctype

    def test_get_compression_type_masks_other_bits(self):
        flag = MessageSysFlag.set_compression_type(MessageSysFlag.COMPRESSED_FLAG, MessageSysFlag.ZLIB_TYPE)
        assert MessageSysFlag.get_compression_type(flag) == MessageSysFlag.ZLIB_TYPE
        assert MessageSysFlag.is_compressed(flag) is True

    def test_clear_compressed_flag_keeps_compression_type(self):
        flag = MessageSysFlag.set_compression_type(MessageSysFlag.COMPRESSED_FLAG, MessageSysFlag.ZLIB_TYPE)
        cleared = MessageSysFlag.clear_compressed_flag(flag)
        assert MessageSysFlag.is_compressed(cleared) is False
        assert MessageSysFlag.get_compression_type(cleared) == MessageSysFlag.ZLIB_TYPE

    def test_transaction_value(self):
        assert MessageSysFlag.get_transaction_value(MessageSysFlag.TRANSACTION_PREPARED_TYPE) == 0x4
        assert MessageSysFlag.get_transaction_value(MessageSysFlag.TRANSACTION_COMMIT_TYPE) == 0x8
        assert MessageSysFlag.get_transaction_value(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE) == 0xC
        assert MessageSysFlag.reset_transaction_value(0xFF, 0) == 0xF3

    def test_check(self):
        assert MessageSysFlag.check(MessageSysFlag.COMPRESSED_FLAG, MessageSysFlag.COMPRESSED_FLAG)
        assert not MessageSysFlag.check(0, MessageSysFlag.COMPRESSED_FLAG)


class TestPullSysFlag:
    def test_build(self):
        assert PullSysFlag.build_sys_flag(True, False, False, False) == 0x1
        assert PullSysFlag.build_sys_flag(False, True, False, False) == 0x2
        assert PullSysFlag.build_sys_flag(False, False, True, False) == 0x4
        assert PullSysFlag.build_sys_flag(False, False, False, True) == 0x8
        assert PullSysFlag.build_sys_flag(True, True, True, True) == 0xF
        assert PullSysFlag.build_sys_flag(True, True, True, True, True) == 0x1F

    def test_has_and_clear(self):
        flag = PullSysFlag.build_sys_flag(True, True, True, True, True)
        assert PullSysFlag.has_commit_offset_flag(flag)
        assert PullSysFlag.has_suspend_flag(flag)
        assert PullSysFlag.has_subscription_flag(flag)
        assert PullSysFlag.has_class_filter_flag(flag)
        assert PullSysFlag.has_lite_pull_flag(flag)
        assert not PullSysFlag.has_commit_offset_flag(PullSysFlag.clear_commit_offset_flag(flag))
        assert not PullSysFlag.has_suspend_flag(PullSysFlag.clear_suspend_flag(flag))
        assert PullSysFlag.build_sys_flag_with_subscription(0) == 0x4


class TestPermName:
    @pytest.mark.parametrize("perm,expected", [(7, "RWX"), (6, "RW-"), (4, "R--"), (0, "---")])
    def test_perm_to_string(self, perm, expected):
        assert PermName.perm_to_string(perm) == expected
        assert PermName.perm2string(perm) == expected

    def test_check_perm(self):
        assert PermName.check_perm(PermName.PERM_READ | PermName.PERM_WRITE, PermName.PERM_READ)
        assert not PermName.check_perm(PermName.PERM_READ, PermName.PERM_WRITE)


class TestUtilAll:
    def test_bytes_2_string_is_upper_hex(self):
        assert UtilAll.bytes_2_string(b"\x0a\xff") == "0AFF"
        assert UtilAll.bytes_2_string(b"") == ""

    def test_string_2_bytes_roundtrip(self):
        assert UtilAll.string_2_bytes("0AFF") == b"\x0a\xff"
        assert UtilAll.string_2_bytes("0aff") == b"\x0a\xff", "Java 内部先 toUpperCase"
        assert UtilAll.string_2_bytes("") is None
        assert UtilAll.string_2_bytes(None) is None

    def test_crc32(self):
        assert UtilAll.crc32(b"") == 0
        assert UtilAll.crc32(b"a") == 0xE8B7BE43

    def test_blank(self):
        assert UtilAll.is_blank(None)
        assert UtilAll.is_blank("   ")
        assert not UtilAll.is_blank("x")
        assert UtilAll.is_not_blank("x")

    def test_offset_2_filename(self):
        assert UtilAll.offset_2_filename(1024) == "00000000000000001024"

    def test_current_time_millis(self):
        assert UtilAll.current_time_millis() > 1_600_000_000_000

    def test_uniq_id_is_32_upper_hex_chars(self):
        uniq = UtilAll.InnerIdGenerator.create_uniq_id() if hasattr(UtilAll, "InnerIdGenerator") else None
        from rocketmq.common.util_all import InnerIdGenerator
        uniq = InnerIdGenerator.create_uniq_id()
        assert len(uniq) == 32
        assert uniq == uniq.upper()
