//! 消息属性键常量（对应 Java `org.apache.rocketmq.common.message.MessageConst`，
//! 参考实现 `python/rocketmq/common/message_const.py`）。
//!
//! 这些字符串就是 broker 与客户端之间的**协议字面量**（写进 17 段消息的第 17 项
//! 属性区，也被 broker 侧过滤/轨迹/事务逻辑按名字读取），所以取值一个字符都不能改；
//! 常量名沿用 Python（去掉 `PROPERTY_` 前缀会丢语义，故保留）。

/// `MessageConst.PROPERTY_KEYS`
pub const PROPERTY_KEYS: &str = "KEYS";
pub const PROPERTY_TAGS: &str = "TAGS";
pub const PROPERTY_WAIT_STORE_MSG_OK: &str = "WAIT";
pub const PROPERTY_DELAY_TIME_LEVEL: &str = "DELAY";
pub const PROPERTY_RETRY_TOPIC: &str = "RETRY_TOPIC";
pub const PROPERTY_REAL_TOPIC: &str = "REAL_TOPIC";
pub const PROPERTY_REAL_QUEUE_ID: &str = "REAL_QID";
pub const PROPERTY_TRANSACTION_PREPARED: &str = "TRAN_MSG";
pub const PROPERTY_PRODUCER_GROUP: &str = "PGROUP";
pub const PROPERTY_MIN_OFFSET: &str = "MIN_OFFSET";
pub const PROPERTY_MAX_OFFSET: &str = "MAX_OFFSET";
pub const PROPERTY_BUYER_ID: &str = "BUYER_ID";
pub const PROPERTY_ORIGIN_MESSAGE_ID: &str = "ORIGIN_MESSAGE_ID";
pub const PROPERTY_TRANSFER_FLAG: &str = "TRANSFER_FLAG";
pub const PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS: &str = "CHECK_IMMUNITY_TIME_IN_SECONDS";
pub const PROPERTY_RECONSUME_TIME: &str = "RECONSUME_TIME";
pub const PROPERTY_MSG_REGION: &str = "MSG_REGION";
/// 消息轨迹开关：broker 在 SEND 响应头里带回，消费侧从消息属性读同一个 key。
pub const PROPERTY_TRACE_SWITCH: &str = "TRACE_ON";
/// 客户端唯一 ID（`MessageClientIDSetter` 写入）。
pub const PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX: &str = "UNIQ_KEY";
pub const PROPERTY_MAX_RECONSUME_TIMES: &str = "MAX_RECONSUME_TIMES";
pub const PROPERTY_CONSUME_START_TIMESTAMP: &str = "CONSUME_START_TIME";
pub const PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET: &str = "TRAN_PREPARED_QUEUE_OFFSET";
pub const PROPERTY_TRANSACTION_CHECK_TIMES: &str = "TRANSACTION_CHECK_TIMES";
pub const PROPERTY_CHECKED_TOPIC: &str = "CHECKED_TOPIC";
pub const PROPERTY_BORN_HOST: &str = "BORN_HOST";
pub const PROPERTY_BORN_TIMESTAMP: &str = "BORN_TIMESTAMP";
pub const PROPERTY_STORE_HOST: &str = "STORE_HOST";
pub const PROPERTY_STORE_TIMESTAMP: &str = "STORE_TIMESTAMP";
pub const PROPERTY_MSG_ID: &str = "MSG_ID";
pub const PROPERTY_WAIT_STORE_MSG_OK_PROP: &str = "WAIT_STORE_MSG_OK";
pub const PROPERTY_INSTANCE_ID: &str = "INSTANCE_ID";
pub const PROPERTY_CLUSTER: &str = "CLUSTER";
pub const PROPERTY_MESSAGE_TYPE: &str = "MSG_TYPE";
/// Request-Reply（5.x）：请求消息带 CORRELATION_ID / REPLY_TO_CLIENT / TTL。
pub const PROPERTY_CORRELATION_ID: &str = "CORRELATION_ID";
pub const PROPERTY_MESSAGE_REPLY_TO_CLIENT: &str = "REPLY_TO_CLIENT";
pub const PROPERTY_MESSAGE_TTL: &str = "TTL";
pub const PROPERTY_REPLY_MESSAGE_ARRIVE_TIME: &str = "REPLY_MESSAGE_ARRIVE_TIME";
pub const PROPERTY_PUSH_REPLY_TIME: &str = "PUSH_REPLY_TIME";
pub const PROPERTY_INNER_MULTI_DISPATCH: &str = "INNER_MULTI_DISPATCH";
pub const PROPERTY_INNER_MULTI_QUEUE_OFFSET: &str = "INNER_MULTI_QUEUE_OFFSET";
pub const PROPERTY_POP_CK: &str = "POP_CK";
pub const PROPERTY_POP_CK_OFFSET: &str = "POP_CK_OFFSET";
pub const PROPERTY_POP_TIME: &str = "POP_TIME";
/// Java 侧写的是 `1ST_POP_TIME`（`PROPERTY_FIRST_POP_TIME`），客户端仅在缺失时补。
pub const PROPERTY_FIRST_POP_TIME: &str = "1ST_POP_TIME";
pub const PROPERTY_INVISIBLE_TIME: &str = "INVISIBLE_TIME";
pub const PROPERTY_DELAY_TIME: &str = "DELAY_TIME";
pub const PROPERTY_START_TIME: &str = "START_TIME";
pub const PROPERTY_END_TIME: &str = "END_TIME";
pub const PROPERTY_EXPIRE_TIME: &str = "EXPIRE_TIME";
pub const PROPERTY_LAST_CONSUME_TIMESTAMP: &str = "LAST_CONSUME_TIME";
pub const PROPERTY_SELF_CONSUME_ENABLE: &str = "SELF_CONSUME";
pub const PROPERTY_RECONSUME_GROUP: &str = "RECONSUME_GROUP";
pub const PROPERTY_RECONSUME_TOPIC: &str = "RECONSUME_TOPIC";
pub const PROPERTY_KEYS_CONST: &str = "KEYS";
pub const PROPERTY_ORIGIN_QUEUE_ID: &str = "ORIGIN_QID";
pub const PROPERTY_ORIGIN_TOPIC: &str = "ORIGIN_TOPIC";

/// Java 4.x 的 `MessageConst.STRING_ALL_PROPERTY`（5.x 已删，Python 里降级成了
/// `STRING_HASH_SET = 1` 这个占位整数）。这里按 Python 的声明顺序给出全量属性键，
/// 供「区分系统属性与用户属性」的场景使用（如轨迹、控制台展示）。
pub const STRING_ALL_PROPERTY: &[&str] = &[
    PROPERTY_KEYS,
    PROPERTY_TAGS,
    PROPERTY_WAIT_STORE_MSG_OK,
    PROPERTY_DELAY_TIME_LEVEL,
    PROPERTY_RETRY_TOPIC,
    PROPERTY_REAL_TOPIC,
    PROPERTY_REAL_QUEUE_ID,
    PROPERTY_TRANSACTION_PREPARED,
    PROPERTY_PRODUCER_GROUP,
    PROPERTY_MIN_OFFSET,
    PROPERTY_MAX_OFFSET,
    PROPERTY_BUYER_ID,
    PROPERTY_ORIGIN_MESSAGE_ID,
    PROPERTY_TRANSFER_FLAG,
    PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
    PROPERTY_RECONSUME_TIME,
    PROPERTY_MSG_REGION,
    PROPERTY_TRACE_SWITCH,
    PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
    PROPERTY_MAX_RECONSUME_TIMES,
    PROPERTY_CONSUME_START_TIMESTAMP,
    PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
    PROPERTY_TRANSACTION_CHECK_TIMES,
    PROPERTY_CHECKED_TOPIC,
    PROPERTY_BORN_HOST,
    PROPERTY_BORN_TIMESTAMP,
    PROPERTY_STORE_HOST,
    PROPERTY_STORE_TIMESTAMP,
    PROPERTY_MSG_ID,
    PROPERTY_INSTANCE_ID,
    PROPERTY_CLUSTER,
    PROPERTY_MESSAGE_TYPE,
    PROPERTY_CORRELATION_ID,
    PROPERTY_MESSAGE_REPLY_TO_CLIENT,
    PROPERTY_MESSAGE_TTL,
    PROPERTY_INNER_MULTI_DISPATCH,
    PROPERTY_INNER_MULTI_QUEUE_OFFSET,
    PROPERTY_POP_CK,
    PROPERTY_POP_CK_OFFSET,
    PROPERTY_POP_TIME,
    PROPERTY_FIRST_POP_TIME,
    PROPERTY_INVISIBLE_TIME,
];

/// Python 遗留占位（Java 原本是 `Set<String> STRING_HASH_SET`），仅为常量表完整。
pub const STRING_HASH_SET: i32 = 1;

pub const KEY_SEPARATOR: &str = " ";
pub const KEY_SEPARATOR_CHAR: &str = " ";
pub const CHARACTER_MAX_LENGTH: i32 = 255;
pub const MESSAGE_ID_PREFIX: &str = "MSGID-";

/// 索引查询类型（`MessageConst.INDEX_KEY_TYPE` 等）：broker 的
/// `QueryMessageRequestHeader.indexType` 取这三个值，为空时按 `"K"` 处理。
pub const INDEX_KEY_TYPE: &str = "K";
pub const INDEX_UNIQUE_TYPE: &str = "U";
pub const INDEX_TAG_TYPE: &str = "T";

/// 对应 Java `MessageConst#messageIdPrefix`（Python 里是 staticmethod）。
pub fn message_id_prefix() -> &'static str {
    MESSAGE_ID_PREFIX
}

/// 属性名是否属于系统属性（`STRING_ALL_PROPERTY`）。
pub fn is_system_property(name: &str) -> bool {
    STRING_ALL_PROPERTY.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_literals_match_wire_names() {
        // 这些是协议字面量，改动必然破坏与 broker 的兼容性
        assert_eq!(PROPERTY_KEYS, "KEYS");
        assert_eq!(PROPERTY_TAGS, "TAGS");
        assert_eq!(PROPERTY_WAIT_STORE_MSG_OK, "WAIT");
        assert_eq!(PROPERTY_DELAY_TIME_LEVEL, "DELAY");
        assert_eq!(PROPERTY_TRANSACTION_PREPARED, "TRAN_MSG");
        assert_eq!(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "UNIQ_KEY");
        assert_eq!(PROPERTY_CONSUME_START_TIMESTAMP, "CONSUME_START_TIME");
        assert_eq!(PROPERTY_REAL_QUEUE_ID, "REAL_QID");
        assert_eq!(PROPERTY_MSG_REGION, "MSG_REGION");
        assert_eq!(PROPERTY_TRACE_SWITCH, "TRACE_ON");
        assert_eq!(PROPERTY_FIRST_POP_TIME, "1ST_POP_TIME");
    }

    #[test]
    fn index_types_and_misc() {
        assert_eq!((INDEX_KEY_TYPE, INDEX_UNIQUE_TYPE, INDEX_TAG_TYPE), ("K", "U", "T"));
        assert_eq!(message_id_prefix(), MESSAGE_ID_PREFIX);
        assert_eq!(CHARACTER_MAX_LENGTH, 255);
        assert_eq!(KEY_SEPARATOR, " ");
        assert_eq!(STRING_HASH_SET, 1);
    }

    #[test]
    fn string_all_property_has_no_duplicates() {
        let mut sorted = STRING_ALL_PROPERTY.to_vec();
        sorted.sort_unstable();
        let before = sorted.len();
        sorted.dedup();
        assert_eq!(before, sorted.len(), "STRING_ALL_PROPERTY 里有重复键");
        for key in STRING_ALL_PROPERTY {
            assert!(is_system_property(key));
        }
        assert!(!is_system_property("userProp"));
    }
}
