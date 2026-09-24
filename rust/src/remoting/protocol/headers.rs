//! 全部请求/响应头（对应 `org.apache.rocketmq.remoting.protocol.header.*`）。
//!
//! 移植 `python/rocketmq/remoting/protocol/headers.py` 的 79 个
//! `CommandCustomHeader` 实现：每个结构体都提供
//!
//! * `to_ext_fields` —— 只写**非 `None`** 字段，与 Java
//!   `RemotingCommand.makeCustomHeaderToNet` 的反射过滤一致；`bool` 一律写成
//!   Java `Boolean.toString` 的小写 `true`/`false`（Python 的 `str(False)` 是
//!   `"False"`，直接落报文会和 Java 客户端不一致）。
//! * `from_ext_fields` —— 把 extFields 还原回字段，缺键回 `None`/默认值。
//!
//! ## 长名 vs 短名
//!
//! 只有 `SendMessageRequestHeaderV2` 用单字母短键（`a`…`n`），配
//! `request_code::SEND_MESSAGE_V2 = 310` 使用；其余 header 全部用 Java 的
//! camelCase 长名。两套拼写在下面各自的结构体里逐字段固定，改一个字节 broker 就读不到。
//!
//! ## 与 Python 的两处刻意的差异
//!
//! 1. Python 的 `PopMessageRequestHeader` / `AckMessageRequestHeader` /
//!    `ChangeInvisibleTimeRequestHeader` 没有 `from_ext_fields`（只用发送方向），
//!    这里补齐以便回环测试与代理侧解析。
//! 2. Python 的 `CreateTopicRequestHeader.from_ext_fields` 漏了 `attributes` 与
//!    `force`，这里同样补齐。
//! 3. Python 用 `bool(v)` 解析 `enableActingMaster` / `changed` /
//!    `acceptStandardJsonOnly`，于是 `"false"` 会被判成 `True`；这里按 Java
//!    `Boolean.parseBoolean` 的语义解析。

use std::any::Any;
use std::fmt;

use super::ext_fields::{CustomHeader, ExtFields};

/// Java `Boolean.toString`：broker 侧用 `Boolean.parseBoolean`，大小写不敏感，
/// 但抓包对拍要求小写。
pub fn bool_text(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

/// 读字符串扩展字段（对应 Python `ext.get(k)`）。
pub fn ext_str(ext: &ExtFields, key: &str) -> Option<String> {
    ext.get(key).map(|s| s.to_string())
}

/// 读 `int` 扩展字段；缺失或不是合法数字返回 `None`（Python 的 `int()` 会抛，
/// 这里按「脏数据不 panic」的要求降级）。
pub fn ext_i32(ext: &ExtFields, key: &str) -> Option<i32> {
    ext.get(key).and_then(|v| v.trim().parse::<i32>().ok())
}

/// 读 `long` 扩展字段。
pub fn ext_i64(ext: &ExtFields, key: &str) -> Option<i64> {
    ext.get(key).and_then(|v| v.trim().parse::<i64>().ok())
}

/// 读 `boolean` 扩展字段，对应 Python `_b`（`true` / `1` 为真）。
pub fn ext_bool(ext: &ExtFields, key: &str) -> Option<bool> {
    ext.get(key).map(|v| {
        let t = v.trim().to_ascii_lowercase();
        t == "true" || t == "1"
    })
}

/// 字段种类 -> Rust 类型。
///
/// `s` String，`i` Integer/int，`l` Long/long，`b` Boolean（可空），
/// `B` Java 原始 `boolean`（恒有值，默认 false），`I` Java 有默认值的 `int`，
/// `E` 枚举（入网写 `Enum.toString()`，如 `BoundaryType`）。
macro_rules! field_ty {
    (s) => { Option<String> };
    (i) => { Option<i32> };
    (l) => { Option<i64> };
    (b) => { Option<bool> };
    (B) => { bool };
    (I) => { i32 };
    (E) => { Option<crate::common::boundary_type::BoundaryType> };
}

/// 定义一个 `CommandCustomHeader`：字段声明顺序 == extFields 写入顺序。
macro_rules! header_struct {
    (
        $(#[$meta:meta])*
        $name:ident { $( $field:ident : $kind:ident => $key:literal , )* }
    ) => {
        #[derive(Debug, Clone, Default, PartialEq, Eq)]
        $(#[$meta])*
        pub struct $name {
            $(pub $field: field_ty!($kind),)*
        }

        impl CustomHeader for $name {
            fn to_ext_fields(&self, out: &mut ExtFields) {
                $(header_struct!(@put $kind, out, $key, self.$field);)*
                // 无字段 header（Java 里就是空类）会留下未使用形参，这里显式消费掉。
                let _ = &out;
            }

            fn from_ext_fields(&mut self, ext: &ExtFields) {
                $(self.$field = header_struct!(@get $kind, ext, $key);)*
                let _ = &ext;
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn boxed_clone(&self) -> Box<dyn CustomHeader> {
                Box::new(self.clone())
            }
        }
    };
    (@put s, $out:expr, $key:expr, $v:expr) => { $out.insert_opt($key, $v.clone()); };
    (@put i, $out:expr, $key:expr, $v:expr) => { $out.insert_opt($key, $v.map(|x| x.to_string())); };
    (@put l, $out:expr, $key:expr, $v:expr) => { $out.insert_opt($key, $v.map(|x| x.to_string())); };
    (@put b, $out:expr, $key:expr, $v:expr) => { $out.insert_opt($key, $v.map(bool_text)); };
    (@put B, $out:expr, $key:expr, $v:expr) => { $out.insert($key, bool_text($v)); };
    (@put I, $out:expr, $key:expr, $v:expr) => { $out.insert($key, $v.to_string()); };
    // Java makeCustomHeaderToNet 写 value.toString()：枚举的入网文本是枚举名大写。
    (@put E, $out:expr, $key:expr, $v:expr) => { $out.insert_opt($key, $v.map(|x| x.name().to_string())); };
    (@get s, $ext:expr, $key:expr) => { ext_str($ext, $key) };
    // 缺键回 None（读取端自行回落 LOWER）；有键则走 Java BoundaryType.getType 的宽松解析。
    (@get E, $ext:expr, $key:expr) => {
        ext_str($ext, $key).map(|s| crate::common::boundary_type::BoundaryType::get_type(&s))
    };
    (@get i, $ext:expr, $key:expr) => { ext_i32($ext, $key) };
    (@get l, $ext:expr, $key:expr) => { ext_i64($ext, $key) };
    (@get b, $ext:expr, $key:expr) => { ext_bool($ext, $key) };
    (@get B, $ext:expr, $key:expr) => { ext_bool($ext, $key).unwrap_or(false) };
    (@get I, $ext:expr, $key:expr) => { ext_i32($ext, $key).unwrap_or(0) };
}

/// 让 `Box<dyn CustomHeader>` 可打印。
///
/// 只输出 `TypeId`：header 里含 topic / group / clientId 等业务标识，日志里不该出现全文；
/// 需要具体内容时调用方自己 `to_ext_fields` 后打印。
impl fmt::Debug for dyn CustomHeader + '_ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CustomHeader({:?})", self.as_any().type_id())
    }
}


//   GetMaxOffsetRequestHeader: java-only fields ["committed"]
//   SearchOffsetRequestHeader: java-only fields ["liteTopic"]
//   QueryMessageResponseHeader: java-only fields ["indexLastUpdatePhyoffset"]
//   ViewMessageRequestHeader: java-only fields ["topic"]
//   ConsumeMessageDirectlyResultRequestHeader: java-only fields ["topic", "topicSysFlag", "groupSysFlag"]
//   ResetOffsetRequestHeader: java-only fields ["queueId", "offset"]
//   EndTransactionRequestHeader: python-only keys ["bname"]
//   CheckTransactionStateRequestHeader: python-only keys ["bname"]
//   CheckTransactionStateResponseHeader: python-only keys ["groupName", "transactionState", "offset"]
//   CheckTransactionStateResponseHeader: java-only fields ["producerGroup", "tranStateTableOffset", "commitLogOffset", "commitOrRollback"]
//   GetAllTopicConfigRequestHeader: java-only fields ["topicSeq", "dataVersion", "maxTopicNum"]
//   GetAllTopicConfigResponseHeader: python-only keys ["dataVersion"]
//   GetAllTopicConfigResponseHeader: java-only fields ["totalTopicNum"]
//   GetConsumeStatsRequestHeader: java-only fields ["TOPIC_NAME_SEPARATOR", "topicList"]
header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader`
    SendMessageRequestHeader {
        producer_group: s => "producerGroup",
        topic: s => "topic",
        default_topic: s => "defaultTopic",
        default_topic_queue_nums: i => "defaultTopicQueueNums",
        queue_id: i => "queueId",
        sys_flag: i => "sysFlag",
        born_timestamp: l => "bornTimestamp",
        flag: i => "flag",
        properties: s => "properties",
        reconsume_times: i => "reconsumeTimes",
        unit_mode: b => "unitMode",
        max_reconsume_times: i => "maxReconsumeTimes",
        batch: b => "batch",

    }
}

header_struct! {
/// 短字段名编码（producerGroup->a ...），与 Java V2 严格一致。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2`
    SendMessageRequestHeaderV2 {
        producer_group: s => "a",
        topic: s => "b",
        default_topic: s => "c",
        default_topic_queue_nums: i => "d",
        queue_id: i => "e",
        sys_flag: i => "f",
        born_timestamp: l => "g",
        flag: i => "h",
        properties: s => "i",
        reconsume_times: i => "j",
        unit_mode: b => "k",
        max_reconsume_times: i => "l",
        batch: b => "m",
        broker_name: s => "n",

    }
}

header_struct! {
/// broker → 请求方 的 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 请求头。
///
/// 对应 Java ``org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader``
/// （字段与 ``SendMessageRequestHeader`` 高度重合，但多了 bornHost/storeHost/storeTimestamp，
/// broker 的 ``ReplyMessageProcessor#pushReplyMessage`` 就是用它拼出来的）。
/// 请求方收到后据此 + body 还原出真正的应答 MessageExt。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader`
    ReplyMessageRequestHeader {
        producer_group: s => "producerGroup",
        topic: s => "topic",
        default_topic: s => "defaultTopic",
        default_topic_queue_nums: i => "defaultTopicQueueNums",
        queue_id: i => "queueId",
        sys_flag: i => "sysFlag",
        born_timestamp: l => "bornTimestamp",
        flag: i => "flag",
        properties: s => "properties",
        reconsume_times: i => "reconsumeTimes",
        unit_mode: b => "unitMode",
        born_host: s => "bornHost",
        store_host: s => "storeHost",
        store_timestamp: l => "storeTimestamp",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader`
    SendMessageResponseHeader {
        msg_id: s => "msgId",
        queue_id: i => "queueId",
        queue_offset: l => "queueOffset",
        transaction_id: s => "transactionId",
        batch_uniq_id: s => "batchUniqId",
        // 只给定时/延迟消息（Java `SendMessageProcessor#attachRecallHandle`），
        // 普通消息解出来是 `None`。见 `crate::common::recall_message_handle`。
        recall_handle: s => "recallHandle",

    }
}

header_struct! {
/// 定时/延迟消息撤回请求（`RECALL_MESSAGE` 370）。
///
/// 对应 Java ``org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader``。
/// ⚠ `broker_name` 走的是继承来的 `RpcRequestHeader.bname`，反射名是 ``bname`` 而不是
/// ``brokerName``，写错 broker 侧静默丢字段。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader`
    RecallMessageRequestHeader {
        producer_group: s => "producerGroup",
        topic: s => "topic",
        recall_handle: s => "recallHandle",
        bname: s => "bname",

    }
}

header_struct! {
/// `recallMessage` 的响应：撤回动作本身是一条写进定时队列的删除消息，
/// Java 回填它的 uniqId（等于被撤回消息的 UNIQ_KEY），调用方拿它确认撤回生效。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader`
    RecallMessageResponseHeader {
        msg_id: s => "msgId",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader`
    PullMessageRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",
        lite_topic: s => "liteTopic",
        queue_id: i => "queueId",
        queue_offset: l => "queueOffset",
        max_msg_nums: i => "maxMsgNums",
        sys_flag: i => "sysFlag",
        commit_offset: l => "commitOffset",
        suspend_timeout_millis: l => "suspendTimeoutMillis",
        subscription: s => "subscription",
        sub_version: l => "subVersion",
        expression_type: s => "expressionType",
        max_msg_bytes: i => "maxMsgBytes",
        request_source: i => "requestSource",
        proxy_froward_client_id: s => "proxyFrowardClientId",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader`
    PullMessageResponseHeader {
        next_begin_offset: l => "nextBeginOffset",
        min_offset: l => "minOffset",
        max_offset: l => "maxOffset",
        suggest_which_broker_id: l => "suggestWhichBrokerId",
        topic_sys_flag: i => "topicSysFlag",
        group_sys_flag: i => "groupSysFlag",
        forbidden_type: i => "forbiddenType",
        offset_delta: l => "offsetDelta",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader`
    QueryConsumerOffsetRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",
        queue_id: i => "queueId",
        set_zero_if_not_found: b => "setZeroIfNotFound",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader`
    QueryConsumerOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader`
    UpdateConsumerOffsetRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",
        queue_id: i => "queueId",
        commit_offset: l => "commitOffset",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetResponseHeader`
    UpdateConsumerOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader`
    GetMaxOffsetRequestHeader {
        topic: s => "topic",
        queue_id: i => "queueId",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader`
    GetMaxOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader`
    GetMinOffsetRequestHeader {
        topic: s => "topic",
        queue_id: i => "queueId",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader`
    GetMinOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader`
    SearchOffsetRequestHeader {
        topic: s => "topic",
        queue_id: i => "queueId",
        timestamp: l => "timestamp",
        // Java 该字段 @CFNullable：None 时整键不写（只有已废弃的 5 参
        // MQClientAPIImpl#searchOffset 会这样发）。
        boundary_type: E => "boundaryType",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader`
    SearchOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeRequestHeader`
    GetEarliestMsgStoretimeRequestHeader {
        topic: s => "topic",
        queue_id: i => "queueId",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader`
    GetEarliestMsgStoretimeResponseHeader {
        timestamp: l => "timestamp",

    }
}

header_struct! {
/// 对应 org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader。
///
/// ``index_type`` 取 MessageConst.INDEX_KEY_TYPE("K") / INDEX_UNIQUE_TYPE("U") /
/// INDEX_TAG_TYPE("T")；broker 侧为空时默认按 "K"（普通 key 索引）查。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader`
    QueryMessageRequestHeader {
        topic: s => "topic",
        key: s => "key",
        max_num: i => "maxNum",
        begin_timestamp: l => "beginTimestamp",
        end_timestamp: l => "endTimestamp",
        index_type: s => "indexType",
        last_key: s => "lastKey",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.QueryMessageResponseHeader`
    QueryMessageResponseHeader {
        index_last_update_timestamp: l => "indexLastUpdateTimestamp",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader`
    ViewMessageRequestHeader {
        offset: l => "offset",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.ViewMessageResponseHeader`
    ViewMessageResponseHeader {

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.HeartbeatRequestHeader`
    HeartbeatRequestHeader {
        client_id: s => "clientID",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader`
    UnregisterClientRequestHeader {
        client_id: s => "clientID",
        producer_group: s => "producerGroup",
        consumer_group: s => "consumerGroup",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader`
    ConsumerSendMsgBackRequestHeader {
        offset: l => "offset",
        group: s => "group",
        delay_level: i => "delayLevel",
        origin_msg_id: s => "originMsgId",
        origin_topic: s => "originTopic",
        unit_mode: b => "unitMode",
        max_reconsume_times: i => "maxReconsumeTimes",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader`
    GetConsumerListByGroupRequestHeader {
        consumer_group: s => "consumerGroup",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseHeader`
    GetConsumerListByGroupResponseHeader {

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.NotifyConsumerIdsChangedRequestHeader`
    NotifyConsumerIdsChangedRequestHeader {
        consumer_group: s => "consumerGroup",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader`
    GetConsumerConnectionListRequestHeader {
        consumer_group: s => "consumerGroup",

    }
}

header_struct! {
/// GET_CONSUMER_STATUS_FROM_CLIENT(221) 的请求头。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetConsumerStatusRequestHeader`
    GetConsumerStatusRequestHeader {
        topic: s => "topic",
        group: s => "group",
        client_addr: s => "clientAddr",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader`
    GetConsumerRunningInfoRequestHeader {
        consumer_group: s => "consumerGroup",
        client_id: s => "clientId",
        jstack_enabled: b => "jstackEnable",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader`
    ConsumeMessageDirectlyResultRequestHeader {
        consumer_group: s => "consumerGroup",
        client_id: s => "clientId",
        msg_id: s => "msgId",
        broker_name: s => "brokerName",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader`
    ResetOffsetRequestHeader {
        topic: s => "topic",
        group: s => "group",
        timestamp: l => "timestamp",
        is_force: b => "isForce",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.LockBatchMqRequestHeader`
    LockBatchMqRequestHeader {
        consumer_group: s => "consumerGroup",
        client_id: s => "clientId",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.UnlockBatchMqRequestHeader`
    UnlockBatchMqRequestHeader {
        consumer_group: s => "consumerGroup",
        client_id: s => "clientId",

    }
}

header_struct! {
/// 对应 org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader。
///
/// ⚠ 继承 RpcRequestHeader 的 brokerName 字段在 Java 里**反射名是 ``bname``**
/// （setter 为 setBrokerName，但字段声明名是 bname）。写错键 broker 会静默丢字段。
/// 字段名严格与 Java 一致：topic / producerGroup / tranStateTableOffset /
/// commitLogOffset / commitOrRollback / fromTransactionCheck / msgId /
/// transactionId / bname。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader`
    EndTransactionRequestHeader {
        topic: s => "topic",
        producer_group: s => "producerGroup",
        tran_state_table_offset: l => "tranStateTableOffset",
        commit_log_offset: l => "commitLogOffset",
        commit_or_rollback: i => "commitOrRollback",
        from_transaction_check: b => "fromTransactionCheck",
        msg_id: s => "msgId",
        transaction_id: s => "transactionId",
        bname: s => "bname",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.EndTransactionResponseHeader`
    EndTransactionResponseHeader {
        msg_id: s => "msgId",
        transaction_id: s => "transactionId",

    }
}

header_struct! {
/// 对应 org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader。
///
/// ⚠ 同样继承 RpcRequestHeader：brokerName 反射名是 ``bname``。字段名严格与 Java
/// 一致：topic / tranStateTableOffset / commitLogOffset / msgId / transactionId /
/// offsetMsgId / bname。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader`
    CheckTransactionStateRequestHeader {
        topic: s => "topic",
        tran_state_table_offset: l => "tranStateTableOffset",
        commit_log_offset: l => "commitLogOffset",
        msg_id: s => "msgId",
        transaction_id: s => "transactionId",
        offset_msg_id: s => "offsetMsgId",
        bname: s => "bname",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateResponseHeader`
    CheckTransactionStateResponseHeader {
        group_name: s => "groupName",
        transaction_state: i => "transactionState",
        offset: l => "offset",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigRequestHeader`
    GetAllTopicConfigRequestHeader {

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader`
    GetAllTopicConfigResponseHeader {
        data_version: s => "dataVersion",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader`
    GetTopicConfigRequestHeader {
        topic: s => "topic",

    }
}

header_struct! {
/// 对应 org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader。
///
/// ⚠ broker 的 checkFields() 会把 ``topicFilterType`` 解析成枚举，**为空直接抛
/// RemotingCommandException("topicFilterType = [null] value invalid")**，
/// 所以哪怕只想建普通 topic，也必须显式下发 topicFilterType。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader`
    CreateTopicRequestHeader {
        topic: s => "topic",
        default_topic: s => "defaultTopic",
        read_queue_nums: i => "readQueueNums",
        write_queue_nums: i => "writeQueueNums",
        perm: i => "perm",
        topic_filter_type: s => "topicFilterType",
        topic_sys_flag: i => "topicSysFlag",
        order: b => "order",
        attributes: s => "attributes",
        force: b => "force",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader`
    DeleteTopicRequestHeader {
        topic: s => "topic",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader`
    GetTopicStatsInfoRequestHeader {
        topic: s => "topic",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader`
    GetConsumeStatsRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",

    }
}

header_struct! {
/// extFields: MISSING-IN-JAVA
    GetAllSubscriptionGroupConfigRequestHeader {

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader`
    GetSubscriptionGroupConfigRequestHeader {
        group: s => "group",

    }
}

header_struct! {
/// extFields: MISSING-IN-JAVA
    InterviewGetConsumerStatusRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader`
    GetTopicsByClusterRequestHeader {
        cluster: s => "cluster",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetBrokerConfigResponseHeader`
    GetBrokerConfigResponseHeader {
        version: s => "version",

    }
}

header_struct! {
/// extFields: MISSING-IN-JAVA
    GetTopicConfigResponseHeader {

    }
}

header_struct! {
/// extFields: MISSING-IN-JAVA
    GetSubscriptionGroupResponseHeader {

    }
}

header_struct! {
/// extFields: MISSING-IN-JAVA
    GetTopicListResponseHeader {
        broker_addr: s => "brokerAddr",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.RegisterBrokerRequestHeader`
    RegisterBrokerRequestHeader {
        broker_name: s => "brokerName",
        broker_addr: s => "brokerAddr",
        cluster_name: s => "clusterName",
        ha_server_addr: s => "haServerAddr",
        broker_id: i => "brokerId",
        heartbeat_timeout_millis: i => "heartbeatTimeoutMillis",
        enable_acting_master: b => "enableActingMaster",
        compressed: B => "compressed",
        body_crc32: I => "bodyCrc32",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.RegisterBrokerResponseHeader`
    RegisterBrokerResponseHeader {
        ha_server_addr: s => "haServerAddr",
        master_addr: s => "masterAddr",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader`
    UnRegisterBrokerRequestHeader {
        broker_name: s => "brokerName",
        broker_addr: s => "brokerAddr",
        cluster_name: s => "clusterName",
        broker_id: i => "brokerId",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader`
    GetRouteInfoRequestHeader {
        topic: s => "topic",
        accept_standard_json_only: b => "acceptStandardJsonOnly",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.PutKVConfigRequestHeader`
    PutKVConfigRequestHeader {
        namespace: s => "namespace",
        key: s => "key",
        value: s => "value",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigRequestHeader`
    GetKVConfigRequestHeader {
        namespace: s => "namespace",
        key: s => "key",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader`
    GetKVConfigResponseHeader {
        value: s => "value",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader`
    DeleteKVConfigRequestHeader {
        namespace: s => "namespace",
        key: s => "key",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVListByNamespaceRequestHeader`
    GetKVListByNamespaceRequestHeader {
        namespace: s => "namespace",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.RegisterTopicRequestHeader`
    RegisterTopicRequestHeader {
        topic: s => "topic",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.RegisterOrderTopicRequestHeader`
    RegisterOrderTopicRequestHeader {
        topic: s => "topic",
        order_topic_string: s => "orderTopicString",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader`
    DeleteTopicFromNamesrvRequestHeader {
        topic: s => "topic",
        cluster_name: s => "clusterName",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.GetBrokerMemberGroupRequestHeader`
    GetBrokerMemberGroupRequestHeader {
        cluster_name: s => "clusterName",
        broker_name: s => "brokerName",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader`
    WipeWritePermOfBrokerRequestHeader {
        broker_name: s => "brokerName",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader`
    WipeWritePermOfBrokerResponseHeader {
        wipe_topic_count: i => "wipeTopicCount",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader`
    AddWritePermOfBrokerRequestHeader {
        broker_name: s => "brokerName",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader`
    AddWritePermOfBrokerResponseHeader {
        add_topic_count: i => "addTopicCount",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.BrokerHeartbeatRequestHeader`
    BrokerHeartbeatRequestHeader {
        cluster_name: s => "clusterName",
        broker_addr: s => "brokerAddr",
        broker_name: s => "brokerName",
        broker_id: i => "brokerId",
        epoch: i => "epoch",
        max_offset: i => "maxOffset",
        confirm_offset: i => "confirmOffset",
        heartbeat_timeout_mills: i => "heartbeatTimeoutMills",
        election_priority: i => "electionPriority",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.QueryDataVersionRequestHeader`
    QueryDataVersionRequestHeader {
        broker_name: s => "brokerName",
        broker_addr: s => "brokerAddr",
        cluster_name: s => "clusterName",
        broker_id: i => "brokerId",

    }
}

header_struct! {
/// extFields: `org.apache.rocketmq.remoting.protocol.header.namesrv.QueryDataVersionResponseHeader`
    QueryDataVersionResponseHeader {
        changed: b => "changed",

    }
}

header_struct! {
/// Java ``PopMessageRequestHeader``（RequestCode.POP_MESSAGE = 200050）。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader`
    PopMessageRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",
        queue_id: i => "queueId",
        max_msg_nums: i => "maxMsgNums",
        invisible_time: l => "invisibleTime",
        poll_time: l => "pollTime",
        born_time: l => "bornTime",
        init_mode: i => "initMode",
        exp_type: s => "expType",
        exp: s => "exp",
        order: B => "order",
        attempt_id: s => "attemptId",

    }
}

header_struct! {
/// Java ``PopMessageResponseHeader``。
///
/// ``start_offset_info`` / ``msg_offset_info`` / ``order_count_info`` 是编码过的
/// 字符串（空格做字段分隔、分号做队列分隔），用 ``extra_info`` 模块解析。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader`
    PopMessageResponseHeader {
        pop_time: l => "popTime",
        invisible_time: l => "invisibleTime",
        revive_qid: i => "reviveQid",
        rest_num: l => "restNum",
        start_offset_info: s => "startOffsetInfo",
        msg_offset_info: s => "msgOffsetInfo",
        order_count_info: s => "orderCountInfo",

    }
}

header_struct! {
/// Java ``AckMessageRequestHeader``（RequestCode.ACK_MESSAGE = 200051）。
///
/// ``offset`` 是 **consumeQueue offset**（即 CK 串第 8 段 / msgQueueOffset），
/// 不是 commitlog offset —— 传错会被 broker 回 NO_MESSAGE。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader`
    AckMessageRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",
        queue_id: i => "queueId",
        extra_info: s => "extraInfo",
        offset: l => "offset",
        lite_topic: s => "liteTopic",

    }
}

header_struct! {
/// Java ``ChangeInvisibleTimeRequestHeader``（CHANGE_MESSAGE_INVISIBLETIME = 200053）。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader`
    ChangeInvisibleTimeRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",
        queue_id: i => "queueId",
        extra_info: s => "extraInfo",
        offset: l => "offset",
        invisible_time: l => "invisibleTime",
        lite_topic: s => "liteTopic",
        suspend: B => "suspend",

    }
}

header_struct! {
/// Java ``ChangeInvisibleTimeResponseHeader``。
///
/// 返回的是**新的** ``invisible_time`` / ``pop_time`` / ``revive_qid``；
/// 客户端要用它们重建 extraInfo 供后续 ACK 使用。
///
/// extFields: `org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader`
    ChangeInvisibleTimeResponseHeader {
        pop_time: l => "popTime",
        invisible_time: l => "invisibleTime",
        revive_qid: i => "reviveQid",

    }
}

/// 每个 header 的 extFields 键名快照，供 `test_java_alignment` 与 Java 源码对拍。
///
/// 与 `org.apache.rocketmq.remoting.protocol.header.*` 的字段名逐字对齐；
/// `SendMessageRequestHeaderV2` 是唯一的短名例外（Java 侧字段名本身就是 `a`…`n`）。
#[cfg(test)]
const JAVA_HEADER_FIELDS: &[(&str, &[&str])] = &[
    ("SendMessageRequestHeader", &["producerGroup", "topic", "defaultTopic", "defaultTopicQueueNums", "queueId", "sysFlag", "bornTimestamp", "flag", "properties", "reconsumeTimes", "unitMode", "maxReconsumeTimes", "batch", ]),
    ("SendMessageRequestHeaderV2", &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", ]),
    ("ReplyMessageRequestHeader", &["producerGroup", "topic", "defaultTopic", "defaultTopicQueueNums", "queueId", "sysFlag", "bornTimestamp", "flag", "properties", "reconsumeTimes", "unitMode", "bornHost", "storeHost", "storeTimestamp", ]),
    ("SendMessageResponseHeader", &["msgId", "queueId", "queueOffset", "transactionId", "batchUniqId", "recallHandle", ]),
    ("RecallMessageRequestHeader", &["producerGroup", "topic", "recallHandle", "bname", ]),
    ("RecallMessageResponseHeader", &["msgId", ]),
    ("PullMessageRequestHeader", &["consumerGroup", "topic", "liteTopic", "queueId", "queueOffset", "maxMsgNums", "sysFlag", "commitOffset", "suspendTimeoutMillis", "subscription", "subVersion", "expressionType", "maxMsgBytes", "requestSource", "proxyFrowardClientId", ]),
    ("PullMessageResponseHeader", &["nextBeginOffset", "minOffset", "maxOffset", "suggestWhichBrokerId", "topicSysFlag", "groupSysFlag", "forbiddenType", "offsetDelta", ]),
    ("QueryConsumerOffsetRequestHeader", &["consumerGroup", "topic", "queueId", "setZeroIfNotFound", ]),
    ("QueryConsumerOffsetResponseHeader", &["offset", ]),
    ("UpdateConsumerOffsetRequestHeader", &["consumerGroup", "topic", "queueId", "commitOffset", ]),
    ("UpdateConsumerOffsetResponseHeader", &["offset", ]),
    ("GetMaxOffsetRequestHeader", &["topic", "queueId", ]),
    ("GetMaxOffsetResponseHeader", &["offset", ]),
    ("GetMinOffsetRequestHeader", &["topic", "queueId", ]),
    ("GetMinOffsetResponseHeader", &["offset", ]),
    ("SearchOffsetRequestHeader", &["topic", "queueId", "timestamp", "boundaryType", ]),
    ("SearchOffsetResponseHeader", &["offset", ]),
    ("GetEarliestMsgStoretimeRequestHeader", &["topic", "queueId", ]),
    ("GetEarliestMsgStoretimeResponseHeader", &["timestamp", ]),
    ("QueryMessageRequestHeader", &["topic", "key", "maxNum", "beginTimestamp", "endTimestamp", "indexType", "lastKey", ]),
    ("QueryMessageResponseHeader", &["indexLastUpdateTimestamp", ]),
    ("ViewMessageRequestHeader", &["offset", ]),
    ("ViewMessageResponseHeader", &[]),
    ("HeartbeatRequestHeader", &["clientID", ]),
    ("UnregisterClientRequestHeader", &["clientID", "producerGroup", "consumerGroup", ]),
    ("ConsumerSendMsgBackRequestHeader", &["offset", "group", "delayLevel", "originMsgId", "originTopic", "unitMode", "maxReconsumeTimes", ]),
    ("GetConsumerListByGroupRequestHeader", &["consumerGroup", ]),
    ("GetConsumerListByGroupResponseHeader", &[]),
    ("NotifyConsumerIdsChangedRequestHeader", &["consumerGroup", ]),
    ("GetConsumerConnectionListRequestHeader", &["consumerGroup", ]),
    ("GetConsumerStatusRequestHeader", &["topic", "group", "clientAddr", ]),
    ("GetConsumerRunningInfoRequestHeader", &["consumerGroup", "clientId", "jstackEnable", ]),
    ("ConsumeMessageDirectlyResultRequestHeader", &["consumerGroup", "clientId", "msgId", "brokerName", ]),
    ("ResetOffsetRequestHeader", &["topic", "group", "timestamp", "isForce", ]),
    ("LockBatchMqRequestHeader", &["consumerGroup", "clientId", ]),
    ("UnlockBatchMqRequestHeader", &["consumerGroup", "clientId", ]),
    ("EndTransactionRequestHeader", &["topic", "producerGroup", "tranStateTableOffset", "commitLogOffset", "commitOrRollback", "fromTransactionCheck", "msgId", "transactionId", "bname", ]),
    ("EndTransactionResponseHeader", &["msgId", "transactionId", ]),
    ("CheckTransactionStateRequestHeader", &["topic", "tranStateTableOffset", "commitLogOffset", "msgId", "transactionId", "offsetMsgId", "bname", ]),
    ("CheckTransactionStateResponseHeader", &["groupName", "transactionState", "offset", ]),
    ("GetAllTopicConfigRequestHeader", &[]),
    ("GetAllTopicConfigResponseHeader", &["dataVersion", ]),
    ("GetTopicConfigRequestHeader", &["topic", ]),
    ("CreateTopicRequestHeader", &["topic", "defaultTopic", "readQueueNums", "writeQueueNums", "perm", "topicFilterType", "topicSysFlag", "order", "attributes", "force", ]),
    ("DeleteTopicRequestHeader", &["topic", ]),
    ("GetTopicStatsInfoRequestHeader", &["topic", ]),
    ("GetConsumeStatsRequestHeader", &["consumerGroup", "topic", ]),
    ("GetAllSubscriptionGroupConfigRequestHeader", &[]),
    ("GetSubscriptionGroupConfigRequestHeader", &["group", ]),
    ("InterviewGetConsumerStatusRequestHeader", &["consumerGroup", "topic", ]),
    ("GetTopicsByClusterRequestHeader", &["cluster", ]),
    ("GetBrokerConfigResponseHeader", &["version", ]),
    ("GetTopicConfigResponseHeader", &[]),
    ("GetSubscriptionGroupResponseHeader", &[]),
    ("GetTopicListResponseHeader", &["brokerAddr", ]),
    ("RegisterBrokerRequestHeader", &["brokerName", "brokerAddr", "clusterName", "haServerAddr", "brokerId", "heartbeatTimeoutMillis", "enableActingMaster", "compressed", "bodyCrc32", ]),
    ("RegisterBrokerResponseHeader", &["haServerAddr", "masterAddr", ]),
    ("UnRegisterBrokerRequestHeader", &["brokerName", "brokerAddr", "clusterName", "brokerId", ]),
    ("GetRouteInfoRequestHeader", &["topic", "acceptStandardJsonOnly", ]),
    ("PutKVConfigRequestHeader", &["namespace", "key", "value", ]),
    ("GetKVConfigRequestHeader", &["namespace", "key", ]),
    ("GetKVConfigResponseHeader", &["value", ]),
    ("DeleteKVConfigRequestHeader", &["namespace", "key", ]),
    ("GetKVListByNamespaceRequestHeader", &["namespace", ]),
    ("RegisterTopicRequestHeader", &["topic", ]),
    ("RegisterOrderTopicRequestHeader", &["topic", "orderTopicString", ]),
    ("DeleteTopicFromNamesrvRequestHeader", &["topic", "clusterName", ]),
    ("GetBrokerMemberGroupRequestHeader", &["clusterName", "brokerName", ]),
    ("WipeWritePermOfBrokerRequestHeader", &["brokerName", ]),
    ("WipeWritePermOfBrokerResponseHeader", &["wipeTopicCount", ]),
    ("AddWritePermOfBrokerRequestHeader", &["brokerName", ]),
    ("AddWritePermOfBrokerResponseHeader", &["addTopicCount", ]),
    ("BrokerHeartbeatRequestHeader", &["clusterName", "brokerAddr", "brokerName", "brokerId", "epoch", "maxOffset", "confirmOffset", "heartbeatTimeoutMills", "electionPriority", ]),
    ("QueryDataVersionRequestHeader", &["brokerName", "brokerAddr", "clusterName", "brokerId", ]),
    ("QueryDataVersionResponseHeader", &["changed", ]),
    ("PopMessageRequestHeader", &["consumerGroup", "topic", "queueId", "maxMsgNums", "invisibleTime", "pollTime", "bornTime", "initMode", "expType", "exp", "order", "attemptId", ]),
    ("PopMessageResponseHeader", &["popTime", "invisibleTime", "reviveQid", "restNum", "startOffsetInfo", "msgOffsetInfo", "orderCountInfo", ]),
    ("AckMessageRequestHeader", &["consumerGroup", "topic", "queueId", "extraInfo", "offset", "liteTopic", ]),
    ("ChangeInvisibleTimeRequestHeader", &["consumerGroup", "topic", "queueId", "extraInfo", "offset", "invisibleTime", "liteTopic", "suspend", ]),
    ("ChangeInvisibleTimeResponseHeader", &["popTime", "invisibleTime", "reviveQid", ]),
];




// ============================== 测试 ==============================

#[cfg(test)]
mod tests {
    use super::*;
    use super::JAVA_HEADER_FIELDS;
    use crate::common::boundary_type::BoundaryType;
    use crate::remoting::protocol::codes::request_code;
    use crate::remoting::protocol::ext_fields::ExtFields;

    const TOPIC: &str = "PopUnitTestTopic";
    const GROUP: &str = "PopUnitTestGroup";
    const BROKER: &str = "broker-a";

    fn keys(ext: &ExtFields) -> Vec<&str> {
        ext.iter().map(|(k, _)| k.as_str()).collect()
    }

    /// 逐字节守卫：POP 请求头的 12 个长字段名与顺序（`python/tests/test_pop.py`、
    /// `cpp/tests/test_pop.cpp` 同一组断言）。
    #[test]
    fn pop_message_request_header_ext_keys_exactly_match_java() {
        let h = PopMessageRequestHeader {
            consumer_group: Some(GROUP.into()),
            topic: Some(TOPIC.into()),
            queue_id: Some(-1),
            max_msg_nums: Some(32),
            invisible_time: Some(60000),
            poll_time: Some(0),
            // bornTime 填 0 会让 broker 直接回 POLLING_TIMEOUT(210)，必须非 0
            born_time: Some(1789613086027),
            init_mode: Some(0),
            exp_type: Some("TAG".into()),
            exp: Some("*".into()),
            attempt_id: Some("attempt-1".into()),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(
            keys(&ext),
            vec![
                "consumerGroup",
                "topic",
                "queueId",
                "maxMsgNums",
                "invisibleTime",
                "pollTime",
                "bornTime",
                "initMode",
                "expType",
                "exp",
                "order",
                "attemptId",
            ]
        );
        assert_eq!(ext.get("consumerGroup"), Some(GROUP));
        assert_eq!(ext.get("queueId"), Some("-1"));
        assert_eq!(ext.get("bornTime"), Some("1789613086027"));
    }

    /// Java 字段是 `Boolean order = Boolean.FALSE`（非 null），`encodeHeader` 不会跳过它。
    #[test]
    fn pop_message_order_is_always_serialized_as_lowercase() {
        let h = PopMessageRequestHeader {
            topic: Some(TOPIC.into()),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(ext.get("order"), Some("false"));
        // 未设置的 Option 字段不能出现在报文里。
        assert!(!ext.contains_key("consumerGroup"));
        assert!(!ext.contains_key("exp"));
        assert!(!ext.contains_key("attemptId"));
        assert_eq!(ext.len(), 2);
    }

    #[test]
    fn pop_message_response_header_decodes_strings_and_tolerates_missing() {
        let mut ext = ExtFields::new();
        for (k, v) in [
            ("popTime", "1789613086027"),
            ("invisibleTime", "60000"),
            ("reviveQid", "0"),
            ("restNum", "0"),
            ("startOffsetInfo", "0 0 0;0 3 0;0 2 0"),
            ("msgOffsetInfo", "0 0 0;0 3 0;0 2 0"),
        ] {
            ext.insert(k, v);
        }
        let mut h = PopMessageResponseHeader::default();
        h.from_ext_fields(&ext);
        assert_eq!(h.pop_time, Some(1789613086027));
        assert_eq!(h.invisible_time, Some(60000));
        assert_eq!(h.revive_qid, Some(0));
        assert_eq!(h.rest_num, Some(0));
        assert_eq!(h.start_offset_info.as_deref(), Some("0 0 0;0 3 0;0 2 0"));
        assert_eq!(h.msg_offset_info.as_deref(), Some("0 0 0;0 3 0;0 2 0"));
        assert_eq!(h.order_count_info, None);

        let empty = ExtFields::new();
        let mut blank = PopMessageResponseHeader::default();
        blank.from_ext_fields(&empty);
        assert_eq!(blank, PopMessageResponseHeader::default());
        assert!(blank.pop_time.is_none());
        assert!(blank.start_offset_info.is_none());
    }

    #[test]
    fn ack_request_header_emits_exactly_five_keys() {
        let h = AckMessageRequestHeader {
            consumer_group: Some(GROUP.into()),
            topic: Some(TOPIC.into()),
            queue_id: Some(3),
            extra_info: Some("ck".into()),
            offset: Some(7),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(
            keys(&ext),
            vec!["consumerGroup", "topic", "queueId", "extraInfo", "offset"]
        );
        assert_eq!(ext.get("extraInfo"), Some("ck"));
        assert_eq!(ext.get("offset"), Some("7"));
        assert_eq!(ext.get("queueId"), Some("3"));
        assert!(!ext.contains_key("liteTopic"));
    }

    #[test]
    fn change_invisible_time_request_header_keys_and_suspend() {
        let h = ChangeInvisibleTimeRequestHeader {
            consumer_group: Some(GROUP.into()),
            topic: Some(TOPIC.into()),
            queue_id: Some(3),
            extra_info: Some("ck".into()),
            offset: Some(7),
            invisible_time: Some(20000),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(
            keys(&ext),
            vec![
                "consumerGroup",
                "topic",
                "queueId",
                "extraInfo",
                "offset",
                "invisibleTime",
                "suspend",
            ]
        );
        // Java 是 primitive boolean，所以总是出现，且是小写
        assert_eq!(ext.get("suspend"), Some("false"));
    }

    #[test]
    fn change_invisible_time_response_header_parse() {
        let mut ext = ExtFields::new();
        ext.insert("popTime", "111");
        ext.insert("invisibleTime", "222");
        ext.insert("reviveQid", "3");
        let mut h = ChangeInvisibleTimeResponseHeader::default();
        h.from_ext_fields(&ext);
        assert_eq!((h.pop_time, h.invisible_time, h.revive_qid), (Some(111), Some(222), Some(3)));
    }

    /// `SEND_MESSAGE_V2 = 310` 依赖单字母短键；长短两套拼写都必须与 Java 一致。
    #[test]
    fn send_message_v2_uses_short_keys() {
        assert_eq!(request_code::SEND_MESSAGE_V2, 310);
        let v1 = SendMessageRequestHeader {
            producer_group: Some("pg".into()),
            topic: Some(TOPIC.into()),
            default_topic: Some("TBW102".into()),
            default_topic_queue_nums: Some(8),
            queue_id: Some(3),
            sys_flag: Some(0),
            born_timestamp: Some(1789613086027),
            flag: Some(0),
            properties: Some("TAGS\x01TagA".into()),
            reconsume_times: Some(0),
            unit_mode: Some(false),
            max_reconsume_times: Some(-1),
            batch: Some(false),
        };
        let mut ext1 = ExtFields::new();
        v1.to_ext_fields(&mut ext1);
        assert_eq!(
            keys(&ext1),
            vec![
                "producerGroup",
                "topic",
                "defaultTopic",
                "defaultTopicQueueNums",
                "queueId",
                "sysFlag",
                "bornTimestamp",
                "flag",
                "properties",
                "reconsumeTimes",
                "unitMode",
                "maxReconsumeTimes",
                "batch",
            ]
        );
        assert_eq!(ext1.get("unitMode"), Some("false"));
        assert_eq!(ext1.get("maxReconsumeTimes"), Some("-1"));

        let v2 = SendMessageRequestHeaderV2 {
            producer_group: Some("pg".into()),
            topic: Some(TOPIC.into()),
            default_topic: Some("TBW102".into()),
            default_topic_queue_nums: Some(8),
            queue_id: Some(3),
            sys_flag: Some(0),
            born_timestamp: Some(1789613086027),
            flag: Some(0),
            properties: Some("TAGS\x01TagA".into()),
            reconsume_times: Some(0),
            unit_mode: Some(false),
            max_reconsume_times: Some(-1),
            batch: Some(false),
            broker_name: Some(BROKER.into()),
        };
        let mut ext2 = ExtFields::new();
        v2.to_ext_fields(&mut ext2);
        assert_eq!(
            keys(&ext2),
            vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"]
        );
        assert_eq!(ext2.get("a"), Some("pg"));
        assert_eq!(ext2.get("g"), Some("1789613086027"));
        assert_eq!(ext2.get("k"), Some("false"));
        assert_eq!(ext2.get("n"), Some(BROKER));

        let mut back = SendMessageRequestHeaderV2::default();
        back.from_ext_fields(&ext2);
        assert_eq!(back, v2);
        let mut back1 = SendMessageRequestHeader::default();
        back1.from_ext_fields(&ext1);
        assert_eq!(back1, v1);
    }

    /// broker 的 `checkFields()` 把 `topicFilterType` 解析成枚举，为空直接抛异常；
    /// 同时守 `attributes` / `force` 两个后加的字段（`cpp/tests/test_admin.cpp` 同组断言）。
    #[test]
    fn create_topic_request_header_guards() {
        let h = CreateTopicRequestHeader {
            topic: Some("attr-topic".into()),
            default_topic: Some("TBW102".into()),
            read_queue_nums: Some(16),
            write_queue_nums: Some(16),
            perm: Some(6),
            topic_filter_type: Some("SINGLE_TAG".into()),
            topic_sys_flag: Some(0),
            order: Some(false),
            attributes: Some("".into()),
            force: Some(false),
        };
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(ext.get("topicFilterType"), Some("SINGLE_TAG"));
        assert_eq!(ext.get("attributes"), Some(""));
        assert_eq!(ext.get("force"), Some("false"));
        assert_eq!(ext.get("order"), Some("false"));
        assert_eq!(ext.len(), 10);

        // 只给 topic 时，可选字段必须整体缺席（而不是写出空串）。
        let minimal = CreateTopicRequestHeader {
            topic: Some("t".into()),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        minimal.to_ext_fields(&mut ext);
        assert_eq!(keys(&ext), vec!["topic"]);

        // Python 的 from_ext_fields 漏了 attributes/force，这里补齐：必须能还原。
        let full = CreateTopicRequestHeader {
            topic: Some("attr-topic".into()),
            attributes: Some("+deleteWhen\x1d04".into()),
            force: Some(true),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        full.to_ext_fields(&mut ext);
        let mut back = CreateTopicRequestHeader::default();
        back.from_ext_fields(&ext);
        assert_eq!(back, full);
    }

    /// `RECALL_MESSAGE`(370) 的三条字段名守卫。
    ///
    /// ⚠ `bname` 是从 `RpcRequestHeader` 继承下来的，反射名不是 `brokerName`；
    /// 写成后者 broker 收不到值，而 Java 客户端会照写 `bname`。
    #[test]
    fn recall_message_headers_use_java_keys() {
        let h = RecallMessageRequestHeader {
            producer_group: Some(GROUP.into()),
            topic: Some(TOPIC.into()),
            recall_handle: Some("djEgVG9waWNBIGJyb2tlci1h".into()),
            bname: Some("broker-a".into()),
        };
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(
            keys(&ext),
            vec!["producerGroup", "topic", "recallHandle", "bname"]
        );
        let mut back = RecallMessageRequestHeader::default();
        back.from_ext_fields(&ext);
        assert_eq!(back, h);

        let mut ext = ExtFields::new();
        ext.insert("msgId", "0123456789ABCDEF0123456789abcdef");
        let mut resp = RecallMessageResponseHeader::default();
        resp.from_ext_fields(&ext);
        assert_eq!(
            resp.msg_id.as_deref(),
            Some("0123456789ABCDEF0123456789abcdef")
        );

        // 定时消息的 recallHandle 由 broker 下发，普通消息不带这个键。
        let mut ext = ExtFields::new();
        ext.insert("msgId", "offset-msg-id");
        ext.insert("queueId", "3");
        ext.insert("queueOffset", "77");
        ext.insert("recallHandle", "djEgVG9waWNBIGJyb2tlci1h");
        let mut send_resp = SendMessageResponseHeader::default();
        send_resp.from_ext_fields(&ext);
        assert_eq!(
            send_resp.recall_handle.as_deref(),
            Some("djEgVG9waWNBIGJyb2tlci1h")
        );
        assert_eq!(send_resp.batch_uniq_id, None);
    }

    /// `QueryMessageRequestHeader`（`cpp/tests/test_admin.cpp` 的往返守卫）。
    #[test]
    fn query_message_request_header_round_trip() {
        let h = QueryMessageRequestHeader {
            topic: Some(TOPIC.into()),
            key: Some("order-1".into()),
            max_num: Some(32),
            begin_timestamp: Some(0),
            end_timestamp: Some(1789613086027),
            index_type: Some("U".into()),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(ext.get("indexType"), Some("U"));
        assert!(!ext.contains_key("lastKey"));
        assert_eq!(
            keys(&ext),
            vec!["topic", "key", "maxNum", "beginTimestamp", "endTimestamp", "indexType"]
        );
        let mut back = QueryMessageRequestHeader::default();
        back.from_ext_fields(&ext);
        assert_eq!(back, h);
    }

    /// `SearchOffsetRequestHeader.boundaryType`（Java `DefaultMQAdminExt`:133/:137）。
    ///
    /// 入网文本是 `Enum.toString()` 的大写枚举名（不是 `getName()` 的小写名）；
    /// 未设置时整键不写（@CFNullable）；回解走 Java `BoundaryType.getType` 的宽松语义。
    #[test]
    fn search_offset_request_header_carries_the_java_boundary_type() {
        let mut h = SearchOffsetRequestHeader {
            topic: Some(TOPIC.into()),
            queue_id: Some(2),
            timestamp: Some(1700000000000),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(keys(&ext), vec!["topic", "queueId", "timestamp"]);

        h.boundary_type = Some(BoundaryType::Lower);
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(ext.get("boundaryType"), Some("LOWER"));
        h.boundary_type = Some(BoundaryType::Upper);
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(ext.get("boundaryType"), Some("UPPER"));

        let mut back = SearchOffsetRequestHeader::default();
        back.from_ext_fields(&ext);
        assert_eq!(back.boundary_type, Some(BoundaryType::Upper));
        // 未知值 / 小写名一律 LOWER（Java getType），缺键回 None
        for text in ["LOWER", "lower", "", "junk"] {
            let mut e = ExtFields::new();
            e.insert("boundaryType", text);
            let mut b = SearchOffsetRequestHeader::default();
            b.from_ext_fields(&e);
            assert_eq!(b.boundary_type, Some(BoundaryType::Lower), "{text}");
        }
        let mut b = SearchOffsetRequestHeader::default();
        b.from_ext_fields(&ExtFields::new());
        assert_eq!(b.boundary_type, None);
    }

    /// Java 的字段名是 `clientID`（ID 全大写），不是 `clientId`。
    #[test]
    fn client_id_key_spelling_is_capitalized_id() {
        let mut ext = ExtFields::new();
        HeartbeatRequestHeader {
            client_id: Some("cid".into()),
        }
        .to_ext_fields(&mut ext);
        assert_eq!(keys(&ext), vec!["clientID"]);

        let mut ext = ExtFields::new();
        UnregisterClientRequestHeader {
            client_id: Some("cid".into()),
            producer_group: Some("pg".into()),
            consumer_group: Some("cg".into()),
        }
        .to_ext_fields(&mut ext);
        assert_eq!(keys(&ext), vec!["clientID", "producerGroup", "consumerGroup"]);
    }

    /// `RegisterBrokerRequestHeader` 的两个非空默认值（`compressed=false`、`bodyCrc32=0`）
    /// 即使调用方不填也要落报文，与 Java 反射一致。
    #[test]
    fn register_broker_defaults_always_emitted() {
        let mut ext = ExtFields::new();
        RegisterBrokerRequestHeader::default().to_ext_fields(&mut ext);
        assert_eq!(keys(&ext), vec!["compressed", "bodyCrc32"]);
        assert_eq!(ext.get("compressed"), Some("false"));
        assert_eq!(ext.get("bodyCrc32"), Some("0"));

        let mut src = ExtFields::new();
        src.insert("brokerName", BROKER);
        src.insert("brokerId", "0");
        src.insert("heartbeatTimeoutMillis", "30000");
        src.insert("enableActingMaster", "false");
        src.insert("compressed", "true");
        src.insert("bodyCrc32", "123");
        let mut h = RegisterBrokerRequestHeader::default();
        h.from_ext_fields(&src);
        assert_eq!(h.broker_name.as_deref(), Some(BROKER));
        assert_eq!(h.broker_id, Some(0));
        assert_eq!(h.heartbeat_timeout_millis, Some(30000));
        // Python 用 bool(v) 解析，"false" 会被判真；这里按 Java parseBoolean。
        assert_eq!(h.enable_acting_master, Some(false));
        assert!(h.compressed);
        assert_eq!(h.body_crc32, 123);
    }

    /// 无字段 header（Java 里就是空类）不能写出任何 extFields。
    #[test]
    fn fieldless_headers_write_nothing() {
        for ext in [
            ViewMessageResponseHeader::default().to_ext(),
            GetAllTopicConfigRequestHeader::default().to_ext(),
            GetConsumerListByGroupResponseHeader::default().to_ext(),
            GetSubscriptionGroupResponseHeader::default().to_ext(),
            GetTopicConfigResponseHeader::default().to_ext(),
            GetAllSubscriptionGroupConfigRequestHeader::default().to_ext(),
        ] {
            assert!(ext.is_empty(), "expected no extFields, got {ext:?}");
        }
    }

    /// 数值字段解析必须是十进制整数字符串；脏数据降级为 `None` 而不是 panic。
    #[test]
    fn numeric_parsing_is_lenient_never_panics() {
        let mut ext = ExtFields::new();
        ext.insert("offset", "not-a-number");
        ext.insert("queueId", "1.5");
        let mut h = QueryConsumerOffsetResponseHeader::default();
        h.from_ext_fields(&ext);
        assert!(h.offset.is_none());
        let mut h2 = QueryConsumerOffsetRequestHeader::default();
        h2.from_ext_fields(&ext);
        assert!(h2.queue_id.is_none());
        assert!(h2.consumer_group.is_none());
        // 越界数字同样降级
        let mut big = ExtFields::new();
        big.insert("queueId", "99999999999999");
        let mut h3 = QueryConsumerOffsetRequestHeader::default();
        h3.from_ext_fields(&big);
        assert_eq!(h3.queue_id, None);
    }

    /// `RemotingCommand` 编码链路：`makeCustomHeaderToNet` 之后 extFields 才有内容。
    #[test]
    fn header_lands_in_remoting_command_ext_fields() {
        use crate::remoting::protocol::RemotingCommand;
        let mut cmd = RemotingCommand::create_request_command(
            request_code::GET_ROUTEINFO_BY_TOPIC,
            Some(Box::new(GetRouteInfoRequestHeader {
                topic: Some(TOPIC.into()),
                accept_standard_json_only: Some(true),
            })),
        );
        assert_eq!(cmd.get_ext_field("topic"), None);
        cmd.make_custom_header_to_net();
        assert_eq!(cmd.get_ext_field("topic"), Some(TOPIC));
        assert_eq!(cmd.get_ext_field("acceptStandardJsonOnly"), Some("true"));
        let back: GetRouteInfoRequestHeader = cmd.decode_command_custom_header().unwrap();
        assert_eq!(back.topic.as_deref(), Some(TOPIC));
        assert_eq!(back.accept_standard_json_only, Some(true));
    }

    /// 每个 header 的 extFields 键名必须仍然是 Java `protocol.header.*` 里的字段名。
    ///
    /// 只有设置 `ROCKETMQ_JAVA_SRC` 时才跑（指向 RocketMQ Java 仓库根目录），否则静默跳过。
    #[test]
    fn test_java_alignment() {
        let root = match std::env::var("ROCKETMQ_JAVA_SRC") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                eprintln!("ROCKETMQ_JAVA_SRC 未设置，跳过 java 字段名对拍");
                return;
            }
        };
        let root = std::path::PathBuf::from(root);
        assert!(root.is_dir(), "ROCKETMQ_JAVA_SRC 不是目录: {root:?}");
        for (class, fields) in JAVA_HEADER_FIELDS {
            let declared = java_declared_fields(&root, class, 0);
            if declared.is_empty() {
                // Python 专有 header（Java 无对应类），不校验。
                continue;
            }
            for field in *fields {
                assert!(
                    declared.iter().any(|d| d == field),
                    "java {class} 已无字段 {field}（当前字段: {declared:?}）"
                );
            }
        }
    }

    /// 递归找 `<name>.java`，收集本类与父类（最多 3 层）声明的字段名。
    fn java_declared_fields(root: &std::path::Path, class: &str, depth: usize) -> Vec<String> {
        let mut out = Vec::new();
        let Some(path) = java_source_file(root, class) else {
            return out;
        };
        let Ok(text) = std::fs::read_to_string(&path) else {
            return out;
        };
        for line in text.lines() {
            let line = line.trim();
            let Some(rest) = ["private ", "protected "]
                .iter()
                .find_map(|p| line.strip_prefix(p))
                .map(|r| r.trim_end())
            else {
                continue;
            };
            let rest = rest.strip_suffix(';').unwrap_or(rest);
            if rest.contains('(') || rest.contains(" static ") || rest.contains(" new ") {
                continue;
            }
            let decl = rest.split('=').next().unwrap_or("").trim();
            let Some((_ty, name)) = decl.rsplit_once(char::is_whitespace) else {
                continue;
            };
            let name = name.trim();
            if !name.is_empty()
                && name.chars().all(|c| c.is_alphanumeric() || c == '_')
                && !name.starts_with(char::is_numeric)
            {
                out.push(name.to_string());
            }
        }
        if depth < 3 {
            if let Some(start) = text.find("class ").and_then(|i| text[i..].find("extends ").map(|j| i + j + 8)) {
                let tail = text[start..].trim_start();
                let mut end = tail.len();
                for (i, c) in tail.char_indices() {
                    if !matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '.') {
                        end = i;
                        break;
                    }
                }
                let super_name = tail[..end].rsplit('.').next().unwrap_or("");
                if !super_name.is_empty() {
                    let mut sup = java_declared_fields(root, super_name, depth + 1);
                    out.append(&mut sup);
                }
            }
        }
        out
    }

    fn java_source_file(root: &std::path::Path, class: &str) -> Option<std::path::PathBuf> {
        let target = format!("{class}.java");
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if path.file_name().map(|n| n.to_string_lossy().into_owned()) == Some(target.clone())
                        && path.to_string_lossy().contains("/src/main/java/")
                    {
                        return Some(path);
                    }
                } else if path.is_dir() {
                    let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
                    if matches!(name.as_deref(), Some("target") | Some(".git") | Some("build")) {
                        continue;
                    }
                    stack.push(path);
                }
            }
        }
        None
    }

    trait ToExt {
        fn to_ext(&self) -> ExtFields;
    }
    impl<T: CustomHeader> ToExt for T {
        fn to_ext(&self) -> ExtFields {
            ExtFields::from_header(self)
        }
    }
}
