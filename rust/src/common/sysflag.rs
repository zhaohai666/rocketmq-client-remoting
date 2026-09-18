//! 消息系统标志位（对应 Java `org.apache.rocketmq.common.sysflag.*`，
//! 参考实现 `python/rocketmq/common/sysflag.py`）。
//!
//! `sysFlag` 是一个 32 位整数，随消息一起落盘（17 段格式的第 8 项）并在
//! pull / send 请求头里透传，所以位含义必须与 Java 逐位一致。

/// 对应 Java `MessageSysFlag`。
///
/// 位含义（低 -> 高）：
/// ```text
/// bit0   COMPRESSED
/// bit1   MULTI_TAGS
/// bit2   TRANSACTION_PREPARED
/// bit3   TRANSACTION_COMMIT(0x2<<2) / TRANSACTION_ROLLBACK(0x3<<2) 共用掩码
/// bit4   BORNHOST_V6
/// bit5   STOREHOSTADDRESS_V6
/// bit6   NEED_UNWRAP
/// bit7   INNER_BATCH
/// bit8~10 COMPRESSION_TYPE（掩码 0x7<<8）
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageSysFlag;

impl MessageSysFlag {
    pub const COMPRESSED_FLAG: i32 = 0x1;
    pub const MULTI_TAGS_FLAG: i32 = 0x1 << 1;
    pub const TRANSACTION_NOT_TYPE: i32 = 0;
    pub const TRANSACTION_PREPARED_TYPE: i32 = 0x1 << 2;
    pub const TRANSACTION_COMMIT_TYPE: i32 = 0x2 << 2;
    pub const TRANSACTION_ROLLBACK_TYPE: i32 = 0x3 << 2;
    pub const BORNHOST_V6_FLAG: i32 = 0x1 << 4;
    pub const STOREHOSTADDRESS_V6_FLAG: i32 = 0x1 << 5;
    /// 批量消息解包标记（避免与其它位冲突）。
    pub const NEED_UNWRAP_FLAG: i32 = 0x1 << 6;
    /// 内部批量标记。
    pub const INNER_BATCH_FLAG: i32 = 0x1 << 7;

    pub const COMPRESSION_LZ4_TYPE: i32 = 0x1 << 8;
    pub const COMPRESSION_ZSTD_TYPE: i32 = 0x2 << 8;
    pub const COMPRESSION_ZLIB_TYPE: i32 = 0x3 << 8;
    pub const COMPRESSION_TYPE_COMPARATOR: i32 = 0x7 << 8;
    pub const COMPRESSION_TYPE_SHIFT: i32 = 8;

    /// 兼容历史命名：压缩类型**数值**（未左移），对应 Python 的同名常量。
    pub const LZ4_TYPE: i32 = 1;
    pub const ZSTD_TYPE: i32 = 2;
    pub const ZLIB_TYPE: i32 = 3;
    /// Java 5.x 里没有 SNAPPY 的 Compressor 实现，落到 `CompressorFactory` 会抛异常。
    pub const SNAPPY_TYPE: i32 = 4;

    /// Java: `(flag & COMPRESSION_TYPE_COMPARATOR) >> 8`。
    pub fn get_compression_type(sys_flag: i32) -> i32 {
        (sys_flag & Self::COMPRESSION_TYPE_COMPARATOR) >> Self::COMPRESSION_TYPE_SHIFT
    }

    /// 覆盖写压缩类型位（其余位保持不变）。
    pub fn set_compression_type(sys_flag: i32, compression_type: i32) -> i32 {
        (sys_flag & !Self::COMPRESSION_TYPE_COMPARATOR)
            | ((compression_type << Self::COMPRESSION_TYPE_SHIFT) & Self::COMPRESSION_TYPE_COMPARATOR)
    }

    pub fn is_compressed(sys_flag: i32) -> bool {
        (sys_flag & Self::COMPRESSED_FLAG) == Self::COMPRESSED_FLAG
    }

    /// 只清 COMPRESSED_FLAG：解压后调用，压缩类型位**保留**（Java 同语义）。
    pub fn clear_compressed_flag(sys_flag: i32) -> i32 {
        sys_flag & !Self::COMPRESSED_FLAG
    }

    /// Java: `flag & TRANSACTION_ROLLBACK_TYPE`（低 2 位即事务类型）。
    pub fn get_transaction_value(flag: i32) -> i32 {
        flag & Self::TRANSACTION_ROLLBACK_TYPE
    }

    pub fn reset_transaction_value(flag: i32, transaction_type: i32) -> i32 {
        (flag & !Self::TRANSACTION_ROLLBACK_TYPE) | transaction_type
    }

    pub fn check(flag: i32, expected_flag: i32) -> bool {
        (flag & expected_flag) != 0
    }
}

/// 对应 Java `CommitLogFlag`（broker 写 commitlog 时的标志，客户端只用来读
/// `MessageExt.sysFlag` 的派生信息）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitLogFlag;

impl CommitLogFlag {
    pub const COMMIT: i32 = 0x1 << 0;
    pub const FLUSH: i32 = 0x1 << 1;
    pub const ROLLBACK: i32 = 0x1 << 2;
}

/// 对应 Java `ConsumeInitMode`（`QueryConsumerOffsetResponseHeader.mode`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeInitMode;

impl ConsumeInitMode {
    /// 从最小位点开始消费（新订阅组首次上线）。
    pub const MIN: i32 = 0;
    /// 从最大位点开始消费。
    pub const MAX: i32 = 1;
}

/// 对应 Java `PullSysFlag`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullSysFlag;

impl PullSysFlag {
    pub const FLAG_COMMIT_OFFSET: i32 = 0x1;
    pub const FLAG_SUSPEND: i32 = 0x1 << 1;
    pub const FLAG_SUBSCRIPTION: i32 = 0x1 << 2;
    pub const FLAG_CLASS_FILTER: i32 = 0x1 << 3;
    pub const FLAG_LITE_PULL_MESSAGE: i32 = 0x1 << 4;
    pub const FLAG_PROXY_BLOCK: i32 = 0x1 << 5;
    pub const FLAG_EXT_BROKER_GROUP: i32 = 0x1 << 6;
    pub const FLAG_INNER_SQL: i32 = 0x1 << 7;
    pub const FLAG_MULTI_TAG: i32 = 0x1 << 8;
    pub const FLAG_START_OFFSET: i32 = 0x1 << 9;
    /// ⚠ Python 参考实现与本仓库 Java 快照都**没有**这一位（上游 5.3+ 才引入）。
    /// 这里按下一个空闲位实现，接 SQL 过滤哈希计算前必须与 broker 对拍。
    pub const FLAG_SUPPORT_FILTER_AND_COMPUTE_HASHING: i32 = 0x1 << 10;

    /// 对应 Java `buildSysFlag(commitOffset, suspend, subscription, classFilter)`。
    pub fn build_sys_flag(
        commit_offset: bool,
        suspend: bool,
        subscription: bool,
        class_filter: bool,
        lite_pull: bool,
    ) -> i32 {
        let mut flag = 0;
        if commit_offset {
            flag |= Self::FLAG_COMMIT_OFFSET;
        }
        if suspend {
            flag |= Self::FLAG_SUSPEND;
        }
        if subscription {
            flag |= Self::FLAG_SUBSCRIPTION;
        }
        if class_filter {
            flag |= Self::FLAG_CLASS_FILTER;
        }
        if lite_pull {
            flag |= Self::FLAG_LITE_PULL_MESSAGE;
        }
        flag
    }

    /// Java 的四参重载（不含 litePull）。
    pub fn build_sys_flag_basic(
        commit_offset: bool,
        suspend: bool,
        subscription: bool,
        class_filter: bool,
    ) -> i32 {
        Self::build_sys_flag(commit_offset, suspend, subscription, class_filter, false)
    }

    /// 在已有 sysFlag 上按需补 `supportsFilterAndComputeHashing` 位。
    pub fn build_sys_flag_with_hashing(sys_flag: i32, support: bool) -> i32 {
        if support {
            sys_flag | Self::FLAG_SUPPORT_FILTER_AND_COMPUTE_HASHING
        } else {
            sys_flag
        }
    }

    pub fn has_support_filter_and_compute_hashing_flag(sys_flag: i32) -> bool {
        (sys_flag & Self::FLAG_SUPPORT_FILTER_AND_COMPUTE_HASHING)
            == Self::FLAG_SUPPORT_FILTER_AND_COMPUTE_HASHING
    }

    pub fn clear_commit_offset_flag(sys_flag: i32) -> i32 {
        sys_flag & !Self::FLAG_COMMIT_OFFSET
    }

    pub fn has_commit_offset_flag(sys_flag: i32) -> bool {
        (sys_flag & Self::FLAG_COMMIT_OFFSET) == Self::FLAG_COMMIT_OFFSET
    }

    pub fn has_suspend_flag(sys_flag: i32) -> bool {
        (sys_flag & Self::FLAG_SUSPEND) == Self::FLAG_SUSPEND
    }

    pub fn clear_suspend_flag(sys_flag: i32) -> i32 {
        sys_flag & !Self::FLAG_SUSPEND
    }

    pub fn has_subscription_flag(sys_flag: i32) -> bool {
        (sys_flag & Self::FLAG_SUBSCRIPTION) == Self::FLAG_SUBSCRIPTION
    }

    pub fn build_sys_flag_with_subscription(sys_flag: i32) -> i32 {
        sys_flag | Self::FLAG_SUBSCRIPTION
    }

    pub fn has_class_filter_flag(sys_flag: i32) -> bool {
        (sys_flag & Self::FLAG_CLASS_FILTER) == Self::FLAG_CLASS_FILTER
    }

    pub fn has_lite_pull_flag(sys_flag: i32) -> bool {
        (sys_flag & Self::FLAG_LITE_PULL_MESSAGE) == Self::FLAG_LITE_PULL_MESSAGE
    }

    pub fn has_start_offset_flag(sys_flag: i32) -> bool {
        (sys_flag & Self::FLAG_START_OFFSET) == Self::FLAG_START_OFFSET
    }
}

/// 对应 Java `PermName`（Python 里也放在 `sysflag.py`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermName;

impl PermName {
    pub const PERM_PRIORITY: i32 = 0x1 << 3;
    pub const PERM_READ: i32 = 0x4;
    pub const PERM_WRITE: i32 = 0x2;
    pub const PERM_INHERIT: i32 = 0x1;
    pub const PERM_OWNER: i32 = 0x1 << 4;

    /// Java `PermName.isValid`：`perm >= 0 && perm < PERM_PRIORITY`。
    pub fn is_valid(perm: i32) -> bool {
        (0..Self::PERM_PRIORITY).contains(&perm)
    }

    /// Java `PermName.isValid(String)`：非数字在 Java 里抛 NumberFormatException，
    /// 这里等价于 `false`（Python `admin._perm_is_valid` 也是这个口径）。
    pub fn is_valid_str(perm: &str) -> bool {
        match perm.trim().parse::<i32>() {
            Ok(v) => Self::is_valid(v),
            Err(_) => false,
        }
    }

    /// 对应 Java `PermName.perm2string`：`R/W/X` 三字符。
    pub fn perm_to_string(perm: i32) -> String {
        let mut sb = String::with_capacity(3);
        sb.push(if Self::PERM_READ == (perm & Self::PERM_READ) { 'R' } else { '-' });
        sb.push(if Self::PERM_WRITE == (perm & Self::PERM_WRITE) { 'W' } else { '-' });
        sb.push(if Self::PERM_INHERIT == (perm & Self::PERM_INHERIT) { 'X' } else { '-' });
        sb
    }

    /// Java 原名。
    pub fn perm2string(perm: i32) -> String {
        Self::perm_to_string(perm)
    }

    pub fn check_perm(perm: i32, wanted_perm: i32) -> bool {
        (perm & wanted_perm) == wanted_perm
    }
}

/// 对应 Python `SubscriptionMode`（Java 侧是 `ConsumeMode` 的子集）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionMode;

impl SubscriptionMode {
    pub const GROUP: i32 = 0;
    pub const BROADCASTING: i32 = 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_positions_match_java() {
        assert_eq!(MessageSysFlag::COMPRESSED_FLAG, 0x1);
        assert_eq!(MessageSysFlag::MULTI_TAGS_FLAG, 0x2);
        assert_eq!(MessageSysFlag::TRANSACTION_NOT_TYPE, 0);
        assert_eq!(MessageSysFlag::TRANSACTION_PREPARED_TYPE, 0x4);
        assert_eq!(MessageSysFlag::TRANSACTION_COMMIT_TYPE, 0x8);
        assert_eq!(MessageSysFlag::TRANSACTION_ROLLBACK_TYPE, 0xC);
        assert_eq!(MessageSysFlag::BORNHOST_V6_FLAG, 0x10);
        assert_eq!(MessageSysFlag::STOREHOSTADDRESS_V6_FLAG, 0x20);
        assert_eq!(MessageSysFlag::NEED_UNWRAP_FLAG, 0x40);
        assert_eq!(MessageSysFlag::INNER_BATCH_FLAG, 0x80);
        assert_eq!(MessageSysFlag::COMPRESSION_LZ4_TYPE, 0x1 << 8);
        assert_eq!(MessageSysFlag::COMPRESSION_ZSTD_TYPE, 0x2 << 8);
        assert_eq!(MessageSysFlag::COMPRESSION_ZLIB_TYPE, 0x3 << 8);
        assert_eq!(MessageSysFlag::COMPRESSION_TYPE_COMPARATOR, 0x7 << 8);
        assert_eq!(
            (
                CommitLogFlag::COMMIT,
                CommitLogFlag::FLUSH,
                CommitLogFlag::ROLLBACK,
                ConsumeInitMode::MIN,
                ConsumeInitMode::MAX
            ),
            (0x1, 0x2, 0x4, 0, 1)
        );
    }

    #[test]
    fn compression_type_roundtrip() {
        for ctype in [MessageSysFlag::LZ4_TYPE, MessageSysFlag::ZSTD_TYPE, MessageSysFlag::ZLIB_TYPE]
        {
            let flag = MessageSysFlag::set_compression_type(0, ctype);
            assert_eq!(MessageSysFlag::get_compression_type(flag), ctype);
        }
    }

    #[test]
    fn get_compression_type_masks_other_bits() {
        let flag =
            MessageSysFlag::set_compression_type(MessageSysFlag::COMPRESSED_FLAG, MessageSysFlag::ZLIB_TYPE);
        assert_eq!(MessageSysFlag::get_compression_type(flag), MessageSysFlag::ZLIB_TYPE);
        assert!(MessageSysFlag::is_compressed(flag));
    }

    #[test]
    fn clear_compressed_flag_keeps_compression_type() {
        let flag =
            MessageSysFlag::set_compression_type(MessageSysFlag::COMPRESSED_FLAG, MessageSysFlag::ZLIB_TYPE);
        let cleared = MessageSysFlag::clear_compressed_flag(flag);
        assert!(!MessageSysFlag::is_compressed(cleared));
        assert_eq!(MessageSysFlag::get_compression_type(cleared), MessageSysFlag::ZLIB_TYPE);
    }

    #[test]
    fn transaction_value() {
        assert_eq!(
            MessageSysFlag::get_transaction_value(MessageSysFlag::TRANSACTION_PREPARED_TYPE),
            0x4
        );
        assert_eq!(MessageSysFlag::get_transaction_value(MessageSysFlag::TRANSACTION_COMMIT_TYPE), 0x8);
        assert_eq!(MessageSysFlag::get_transaction_value(MessageSysFlag::TRANSACTION_ROLLBACK_TYPE), 0xC);
        assert_eq!(MessageSysFlag::reset_transaction_value(0xFF, 0), 0xF3);
    }

    #[test]
    fn check_helper() {
        assert!(MessageSysFlag::check(
            MessageSysFlag::COMPRESSED_FLAG,
            MessageSysFlag::COMPRESSED_FLAG
        ));
        assert!(!MessageSysFlag::check(0, MessageSysFlag::COMPRESSED_FLAG));
    }

    #[test]
    fn pull_sys_flag_build() {
        assert_eq!(PullSysFlag::build_sys_flag_basic(true, false, false, false), 0x1);
        assert_eq!(PullSysFlag::build_sys_flag_basic(false, true, false, false), 0x2);
        assert_eq!(PullSysFlag::build_sys_flag_basic(false, false, true, false), 0x4);
        assert_eq!(PullSysFlag::build_sys_flag_basic(false, false, false, true), 0x8);
        assert_eq!(PullSysFlag::build_sys_flag(true, true, true, true, false), 0xF);
        assert_eq!(PullSysFlag::build_sys_flag(true, true, true, true, true), 0x1F);
    }

    #[test]
    fn pull_sys_flag_has_and_clear() {
        let flag = PullSysFlag::build_sys_flag(true, true, true, true, true);
        assert!(PullSysFlag::has_commit_offset_flag(flag));
        assert!(PullSysFlag::has_suspend_flag(flag));
        assert!(PullSysFlag::has_subscription_flag(flag));
        assert!(PullSysFlag::has_class_filter_flag(flag));
        assert!(PullSysFlag::has_lite_pull_flag(flag));
        assert!(!PullSysFlag::has_commit_offset_flag(PullSysFlag::clear_commit_offset_flag(flag)));
        assert!(!PullSysFlag::has_suspend_flag(PullSysFlag::clear_suspend_flag(flag)));
        assert_eq!(PullSysFlag::build_sys_flag_with_subscription(0), 0x4);
    }

    #[test]
    fn pull_sys_flag_hashing_bit() {
        assert!(!PullSysFlag::has_support_filter_and_compute_hashing_flag(0x1F));
        let with = PullSysFlag::build_sys_flag_with_hashing(0x1F, true);
        assert_eq!(with, 0x1F | (0x1 << 10));
        assert!(PullSysFlag::has_support_filter_and_compute_hashing_flag(with));
        assert_eq!(PullSysFlag::build_sys_flag_with_hashing(with, false), with);
    }

    #[test]
    fn perm_name() {
        for (perm, expected) in [(7, "RWX"), (6, "RW-"), (4, "R--"), (0, "---")] {
            assert_eq!(PermName::perm_to_string(perm), expected);
            assert_eq!(PermName::perm2string(perm), expected);
        }
        assert!(PermName::check_perm(PermName::PERM_READ | PermName::PERM_WRITE, PermName::PERM_READ));
        assert!(!PermName::check_perm(PermName::PERM_READ, PermName::PERM_WRITE));
        assert!(PermName::is_valid(0));
        assert!(PermName::is_valid(7));
        assert!(!PermName::is_valid(8));
        assert!(!PermName::is_valid(-1));
        assert!(PermName::is_valid_str("6"));
        assert!(!PermName::is_valid_str("RW"));
        assert_eq!(
            (
                PermName::PERM_PRIORITY,
                PermName::PERM_OWNER,
                SubscriptionMode::GROUP,
                SubscriptionMode::BROADCASTING
            ),
            (8, 16, 0, 1)
        );
    }
}
