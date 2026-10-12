
use std::collections::HashSet;

pub const TOPIC_MAX_LENGTH: i32 = 127;
/// group 名要参与拼 `%RETRY%group_topic` / `%DLQ%group_topic`，所以比 topic 更短。
pub const GROUP_MAX_LENGTH: i32 = 120;
pub const RETRY_OR_DLQ_TOPIC_MAX_LENGTH: i32 = 255;

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

fn allowed_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'%' | b'-' | b'_' | b'|')
}

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
    fn charset_matches_server_whitelist() {
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
    fn system_and_forbidden_sets_match_server() {
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
