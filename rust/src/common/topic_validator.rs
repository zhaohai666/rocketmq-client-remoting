
use std::collections::HashSet;

pub const TOPIC_MAX_LENGTH: i32 = 127;
/// Group names are used to build `%RETRY%group_topic` / `%DLQ%group_topic`, so they are shorter than topics.
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
            assert!(!is_topic_or_group_illegal(legal), "{legal} should be legal");
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
            assert!(is_topic_or_group_illegal(illegal), "{illegal} should be illegal");
        }
    }

    #[test]
    fn empty_is_legal_and_code_point_boundary_is_byte_exact() {
        // Empty is handled by is_blank; here only the character set is checked
        assert!(!is_topic_or_group_illegal(""));
        // DEL(0x7f) is within the bit-table range but unset => illegal; from 0x80 on it is out of range => illegal
        assert!(is_topic_or_group_illegal("\u{7f}"));
        assert!(is_topic_or_group_illegal("\u{80}"));
        assert!(is_topic_or_group_illegal("topic\u{e9}"));
        // '~'(0x7e) sits right at the boundary but is not in the whitelist
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
            assert!(is_system_topic(t), "{t} is a system topic");
        }
        // A prefix hit counts as a system topic; the case-sensitive RMQ_SYS_ prefix does not go through this rule
        assert!(is_system_topic("rmq_sys_anything"));
        assert!(!is_system_topic("RMQ_SYS_TRACE_TOPIC_X"));
        assert!(!is_system_topic("MyBusinessTopic"));

        // The 8-entry forbidden-send list; TBW102 is a system topic but sending to it is **allowed** (route creation needs it)
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
            assert!(is_not_allowed_send_topic(t), "{t} must not be sent to directly");
        }
        assert!(!is_not_allowed_send_topic("TBW102"));
        // Target topics of the redelivery path must never be forbidden
        assert!(!is_not_allowed_send_topic("%RETRY%myGroup"));
    }
}
