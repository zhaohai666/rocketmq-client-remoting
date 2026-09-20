//! topic / group 名字的合法性判定（对应 `org.apache.rocketmq.common.topic.TopicValidator`，
//! 口径以 `python/rocketmq/common/topic_validator.py` 为准）。
//!
//! Java 的字符表白名单是 `^[%|a-zA-Z0-9_-]+$`，实现方式是一张 128 长的
//! `VALID_CHAR_BIT_MAP`：**码点 >= 128 一律非法**。这里逐字节照抄，因为 broker 侧
//! （`TopicConfigValidator`）用的是同一张表，客户端放行而 broker 拒绝只会把错误推迟到
//! 建 topic 那一刻，而名字是**发送路径**上就该定的东西。
//!
//! ⚠ 与 Java 的差别（有意为之，Python 端口同口径）：`TopicValidator.validateTopic` /
//! `validateGroup` 那对返回 `ValidateResult` 的管理端入口未移植 —— 客户端只用抛异常式
//! 的 [`crate::client::validators`]。

use std::collections::HashSet;

/// topic 名长度上限（Java `TOPIC_MAX_LENGTH`）。
pub const TOPIC_MAX_LENGTH: i32 = 127;
/// group 名要参与拼 `%RETRY%group_topic` / `%DLQ%group_topic`，所以比 topic 更短。
pub const GROUP_MAX_LENGTH: i32 = 120;
/// 仅 Java `validateTopic` 用到（`checkTopic` 不查这一档），保留常量口径。
pub const RETRY_OR_DLQ_TOPIC_MAX_LENGTH: i32 = 255;

/// Java 报错文案里引用的正则原文（判定本身走查表，不用正则，与 Java 一致）。
pub const VALID_CHAR_PATTERN: &str = "^[%|a-zA-Z0-9_-]+$";

pub const AUTO_CREATE_TOPIC_KEY_TOPIC: &str = "TBW102";
pub const RMQ_SYS_SCHEDULE_TOPIC: &str = "SCHEDULE_TOPIC_XXXX";
pub const RMQ_SYS_BENCHMARK_TOPIC: &str = "BenchmarkTest";
pub const RMQ_SYS_TRANS_HALF_TOPIC: &str = "RMQ_SYS_TRANS_HALF_TOPIC";
pub const RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC";
pub const RMQ_SYS_TRACE_TOPIC: &str = "RMQ_SYS_TRACE_TOPIC";
pub const RMQ_SYS_TRANS_OP_HALF_TOPIC: &str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
pub const RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC";
pub const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: &str = "TRANS_CHECK_MAX_TIME_TOPIC";
pub const RMQ_SYS_SELF_TEST_TOPIC: &str = "SELF_TEST_TOPIC";
pub const RMQ_SYS_OFFSET_MOVED_EVENT: &str = "OFFSET_MOVED_EVENT";
pub const RMQ_SYS_ROCKSDB_OFFSET_TOPIC: &str = "CHECKPOINT_TOPIC";

pub const SYSTEM_TOPIC_PREFIX: &str = "rmq_sys_";

/// 客户端**不能直接发**的 topic：这几个是 broker 内部状态流水（半消息、延迟、轨迹校验…），
/// 用户发进去会污染 broker 的事务/延迟/校验逻辑。
/// ⚠ `%RETRY%` 前缀不在名单里：`sendMessageBack` 就是往 `%RETRY%group` 写的，
/// 禁掉会打断重投链路（`%DLQ%` 同理，只是本集合按 Java 口径不含它）。
pub fn not_allowed_send_topic_set() -> &'static HashSet<&'static str> {
    static SET: std::sync::OnceLock<HashSet<&'static str>> = std::sync::OnceLock::new();
    SET.get_or_init(|| {
        [
            RMQ_SYS_SCHEDULE_TOPIC,
            RMQ_SYS_TRANS_HALF_TOPIC,
            RMQ_SYS_TRANS_OP_HALF_TOPIC,
            RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
            RMQ_SYS_SELF_TEST_TOPIC,
            RMQ_SYS_OFFSET_MOVED_EVENT,
            RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
            RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
        ]
        .into_iter()
        .collect()
    })
}

/// 系统 topic 名单（Java `getSystemTopicSet`）；判定另看 [`SYSTEM_TOPIC_PREFIX`]。
pub fn system_topic_set() -> &'static HashSet<&'static str> {
    static SET: std::sync::OnceLock<HashSet<&'static str>> = std::sync::OnceLock::new();
    SET.get_or_init(|| {
        [
            AUTO_CREATE_TOPIC_KEY_TOPIC,
            RMQ_SYS_SCHEDULE_TOPIC,
            RMQ_SYS_BENCHMARK_TOPIC,
            RMQ_SYS_TRANS_HALF_TOPIC,
            RMQ_SYS_TRACE_TOPIC,
            RMQ_SYS_TRANS_OP_HALF_TOPIC,
            RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
            RMQ_SYS_SELF_TEST_TOPIC,
            RMQ_SYS_OFFSET_MOVED_EVENT,
            RMQ_SYS_ROCKSDB_OFFSET_TOPIC,
            RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
            RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
        ]
        .into_iter()
        .collect()
    })
}

/// 对应 Java 位表：只有 ASCII 数字、大小写字母与 `% - _ |` 置位。
fn allowed_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'%' | b'-' | b'_' | b'|')
}

/// 对应 `TopicValidator.isTopicOrGroupIllegal`：**空串返回 false**（空/纯空白由
/// `UtilAll::is_blank` 那一步管，与 Java 的顺序一致）。
///
/// Rust 的 `String` 是 UTF-8 字节串，Java 按 `char`（UTF-16 码元）判定：
/// 非 ASCII 字符的 UTF-8 首字节必然 >= 0x80，因此在真实输入上与 Java 等价。
pub fn is_topic_or_group_illegal(name: &str) -> bool {
    name.as_bytes().iter().any(|&b| b >= 0x80 || !allowed_char(b))
}

pub fn is_system_topic(topic: &str) -> bool {
    system_topic_set().contains(topic) || topic.starts_with(SYSTEM_TOPIC_PREFIX)
}

pub fn is_not_allowed_send_topic(topic: &str) -> bool {
    not_allowed_send_topic_set().contains(topic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn charset_matches_java_whitelist() {
        for legal in [
            "order-topic",
            "order_topic",
            "CID_ONSAPI_PULL",
            "%RETRY%myGroup",
            "%DLQ%myGroup",
            "Topic|With|Pipe",
            "TOPIC_with_9_digits",
            "CID_ONS-HTTP-PROXY",
        ] {
            assert!(!is_topic_or_group_illegal(legal), "{legal} 应当合法");
        }
        for illegal in [
            "topic.name",
            "topic/name",
            "topic\\name",
            "topic:broker",
            "topic@host",
            "topic 1",
            "中文topic",
            "tab\there",
            "line\nbreak",
        ] {
            assert!(is_topic_or_group_illegal(illegal), "{illegal} 应当非法");
        }
    }

    #[test]
    fn empty_is_legal_and_code_point_boundary_is_byte_exact() {
        // 空由 is_blank 管；这里只查字符表
        assert!(!is_topic_or_group_illegal(""));
        // DEL(0x7f) 在位表范围内但未置位 => 非法；0x80 起越界 => 非法
        assert!(is_topic_or_group_illegal("\u{7f}"));
        assert!(is_topic_or_group_illegal("\u{80}"));
        assert!(is_topic_or_group_illegal("topic\u{e9}"));
        // '~'(0x7e) 紧挨着边界但不在白名单
        assert!(is_topic_or_group_illegal("topic~"));
    }

    #[test]
    fn system_and_forbidden_sets_match_java() {
        assert_eq!(system_topic_set().len(), 12);
        assert_eq!(not_allowed_send_topic_set().len(), 8);
        for t in [
            "TBW102",
            "SCHEDULE_TOPIC_XXXX",
            "BenchmarkTest",
            "RMQ_SYS_TRACE_TOPIC",
            "CHECKPOINT_TOPIC",
            "OFFSET_MOVED_EVENT",
        ] {
            assert!(is_system_topic(t), "{t} 是系统 topic");
        }
        // 前缀命中即算系统 topic；大小写敏感的 RMQ_SYS_ 前缀不走这条规则
        assert!(is_system_topic("rmq_sys_anything"));
        assert!(!is_system_topic("RMQ_SYS_TRACE_TOPIC_X"));
        assert!(!is_system_topic("MyBusinessTopic"));

        // 8 个禁发名单；TBW102 是系统 topic 但**允许**发（建路由要用它）
        for t in [
            "SCHEDULE_TOPIC_XXXX",
            "RMQ_SYS_TRANS_HALF_TOPIC",
            "RMQ_SYS_TRANS_OP_HALF_TOPIC",
            "TRANS_CHECK_MAX_TIME_TOPIC",
            "SELF_TEST_TOPIC",
            "OFFSET_MOVED_EVENT",
            "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC",
            "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC",
        ] {
            assert!(is_not_allowed_send_topic(t), "{t} 禁止直发");
        }
        assert!(!is_not_allowed_send_topic("TBW102"));
        // 重投链路的目标 topic 绝不能被禁
        assert!(!is_not_allowed_send_topic("%RETRY%myGroup"));
    }
}
