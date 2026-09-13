# -*- coding: utf-8 -*-
"""消息系统标志位常量（对应 org.apache.rocketmq.common.sysflag.*）。"""


class MessageSysFlag:
    """对应 org.apache.rocketmq.common.sysflag.MessageSysFlag。

    位含义（低 -> 高）：
        bit0  COMPRESSED
        bit1  MULTI_TAGS
        bit2  TRANSACTION_PREPARED
        bit3  TRANSACTION_COMMIT(0x2<<2) / TRANSACTION_ROLLBACK(0x3<<2) 共用掩码
        bit4  BORNHOST_V6
        bit5  STOREHOSTADDRESS_V6
        bit6  NEED_UNWRAP
        bit7  INNER_BATCH
        bit8~10 COMPRESSION_TYPE（7 种取值，掩码 0x7<<8）
    """

    COMPRESSED_FLAG = 0x1
    MULTI_TAGS_FLAG = 0x1 << 1
    TRANSACTION_NOT_TYPE = 0
    TRANSACTION_PREPARED_TYPE = 0x1 << 2
    TRANSACTION_COMMIT_TYPE = 0x2 << 2
    TRANSACTION_ROLLBACK_TYPE = 0x3 << 2
    BORNHOST_V6_FLAG = 0x1 << 4
    STOREHOSTADDRESS_V6_FLAG = 0x1 << 5
    # 批量消息解包标记（避免与其它位冲突）
    NEED_UNWRAP_FLAG = 0x1 << 6
    # 内部批量标记
    INNER_BATCH_FLAG = 0x1 << 7

    COMPRESSION_LZ4_TYPE = 0x1 << 8
    COMPRESSION_ZSTD_TYPE = 0x2 << 8
    COMPRESSION_ZLIB_TYPE = 0x3 << 8
    COMPRESSION_TYPE_COMPARATOR = 0x7 << 8
    COMPRESSION_TYPE_SHIFT = 8

    # 兼容历史命名（供 compression_type 数值 -> flag 位）
    LZ4_TYPE = 1
    ZSTD_TYPE = 2
    ZLIB_TYPE = 3
    SNAPPY_TYPE = 4

    @staticmethod
    def get_compression_type(sys_flag: int) -> int:
        """Java: (flag & COMPRESSION_TYPE_COMPARATOR) >> 8。"""
        return (sys_flag & MessageSysFlag.COMPRESSION_TYPE_COMPARATOR) >> MessageSysFlag.COMPRESSION_TYPE_SHIFT

    @staticmethod
    def set_compression_type(sys_flag: int, compression_type: int) -> int:
        return (sys_flag & ~MessageSysFlag.COMPRESSION_TYPE_COMPARATOR) | (
            (compression_type << MessageSysFlag.COMPRESSION_TYPE_SHIFT) & MessageSysFlag.COMPRESSION_TYPE_COMPARATOR)

    @staticmethod
    def is_compressed(sys_flag: int) -> bool:
        return (sys_flag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG

    @staticmethod
    def clear_compressed_flag(sys_flag: int) -> int:
        return sys_flag & ~MessageSysFlag.COMPRESSED_FLAG

    @staticmethod
    def get_transaction_value(flag: int) -> int:
        """Java: flag & TRANSACTION_ROLLBACK_TYPE。"""
        return flag & MessageSysFlag.TRANSACTION_ROLLBACK_TYPE

    @staticmethod
    def reset_transaction_value(flag: int, transaction_type: int) -> int:
        return (flag & ~MessageSysFlag.TRANSACTION_ROLLBACK_TYPE) | transaction_type

    @staticmethod
    def check(flag: int, expected_flag: int) -> bool:
        return (flag & expected_flag) != 0


class PullSysFlag:
    FLAG_COMMIT_OFFSET = 0x1
    FLAG_SUSPEND = 0x1 << 1
    FLAG_SUBSCRIPTION = 0x1 << 2
    FLAG_CLASS_FILTER = 0x1 << 3
    FLAG_LITE_PULL_MESSAGE = 0x1 << 4
    FLAG_PROXY_BLOCK = 0x1 << 5
    FLAG_EXT_BROKER_GROUP = 0x1 << 6
    FLAG_INNER_SQL = 0x1 << 7
    FLAG_MULTI_TAG = 0x1 << 8
    FLAG_START_OFFSET = 0x1 << 9

    @staticmethod
    def build_sys_flag(commit_offset: bool, suspend: bool, subscription: bool,
                       class_filter: bool, lite_pull: bool = False) -> int:
        flag = 0
        if commit_offset:
            flag |= PullSysFlag.FLAG_COMMIT_OFFSET
        if suspend:
            flag |= PullSysFlag.FLAG_SUSPEND
        if subscription:
            flag |= PullSysFlag.FLAG_SUBSCRIPTION
        if class_filter:
            flag |= PullSysFlag.FLAG_CLASS_FILTER
        if lite_pull:
            flag |= PullSysFlag.FLAG_LITE_PULL_MESSAGE
        return flag

    @staticmethod
    def clear_commit_offset_flag(sys_flag: int) -> int:
        return sys_flag & ~PullSysFlag.FLAG_COMMIT_OFFSET

    @staticmethod
    def has_commit_offset_flag(sys_flag: int) -> bool:
        return (sys_flag & PullSysFlag.FLAG_COMMIT_OFFSET) == PullSysFlag.FLAG_COMMIT_OFFSET

    @staticmethod
    def has_suspend_flag(sys_flag: int) -> bool:
        return (sys_flag & PullSysFlag.FLAG_SUSPEND) == PullSysFlag.FLAG_SUSPEND

    @staticmethod
    def clear_suspend_flag(sys_flag: int) -> int:
        return sys_flag & ~PullSysFlag.FLAG_SUSPEND

    @staticmethod
    def has_subscription_flag(sys_flag: int) -> bool:
        return (sys_flag & PullSysFlag.FLAG_SUBSCRIPTION) == PullSysFlag.FLAG_SUBSCRIPTION

    @staticmethod
    def build_sys_flag_with_subscription(sys_flag: int) -> int:
        return sys_flag | PullSysFlag.FLAG_SUBSCRIPTION

    @staticmethod
    def has_class_filter_flag(sys_flag: int) -> bool:
        return (sys_flag & PullSysFlag.FLAG_CLASS_FILTER) == PullSysFlag.FLAG_CLASS_FILTER

    @staticmethod
    def has_lite_pull_flag(sys_flag: int) -> bool:
        return (sys_flag & PullSysFlag.FLAG_LITE_PULL_MESSAGE) == PullSysFlag.FLAG_LITE_PULL_MESSAGE


class PermName:
    PERM_PRIORITY = 0x1 << 3
    PERM_READ = 0x4
    PERM_WRITE = 0x2
    PERM_INHERIT = 0x1
    PERM_OWNER = 0x1 << 4

    @staticmethod
    def perm_to_string(perm: int) -> str:
        sb = []
        if PermName.PERM_READ == (perm & PermName.PERM_READ):
            sb.append("R")
        else:
            sb.append("-")
        if PermName.PERM_WRITE == (perm & PermName.PERM_WRITE):
            sb.append("W")
        else:
            sb.append("-")
        if PermName.PERM_INHERIT == (perm & PermName.PERM_INHERIT):
            sb.append("X")
        else:
            sb.append("-")
        return "".join(sb)

    @staticmethod
    def perm2string(perm: int) -> str:
        return PermName.perm_to_string(perm)

    @staticmethod
    def check_perm(perm: int, wanted_perm: int) -> bool:
        return (perm & wanted_perm) == wanted_perm


class SubscriptionMode:
    GROUP = 0
    BROADCASTING = 1