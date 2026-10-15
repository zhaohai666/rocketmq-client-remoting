//! This module holds the protocol's 79
//! `CommandCustomHeader` implementations: each struct provides
//!
//!
//! ## Long names vs short names
//!
//!
//! ## Two deliberate trade-offs

use std::any::Any;
use std::fmt;

use super::ext_fields::{CustomHeader, ExtFields};

pub fn bool_text(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

/// Reads a string extension field (returns `None` if missing).
pub fn ext_str(ext: &ExtFields, key: &str) -> Option<String> {
    ext.get(key).map(|s| s.to_string())
}

/// Reads an `int` extension field; returns `None` if missing or not a valid number
/// (degrades per the "dirty data must not panic" requirement).
pub fn ext_i32(ext: &ExtFields, key: &str) -> Option<i32> {
    ext.get(key).and_then(|v| v.trim().parse::<i32>().ok())
}

/// Reads a `long` extension field.
pub fn ext_i64(ext: &ExtFields, key: &str) -> Option<i64> {
    ext.get(key).and_then(|v| v.trim().parse::<i64>().ok())
}

/// Reads a `boolean` extension field (`true` / `1` count as true).
pub fn ext_bool(ext: &ExtFields, key: &str) -> Option<bool> {
    ext.get(key).map(|v| {
        let t = v.trim().to_ascii_lowercase();
        t == "true" || t == "1"
    })
}

/// Field kind -> Rust type.
macro_rules! field_ty {
    (s) => { Option<String> };
    (i) => { Option<i32> };
    (l) => { Option<i64> };
    (b) => { Option<bool> };
    (B) => { bool };
    (I) => { i32 };
    (E) => { Option<crate::common::boundary_type::BoundaryType> };
}

/// Defines a `CommandCustomHeader`: field declaration order == extFields write order.
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
    (@put E, $out:expr, $key:expr, $v:expr) => { $out.insert_opt($key, $v.map(|x| x.name().to_string())); };
    (@get s, $ext:expr, $key:expr) => { ext_str($ext, $key) };
    (@get E, $ext:expr, $key:expr) => {
        ext_str($ext, $key).map(|s| crate::common::boundary_type::BoundaryType::get_type(&s))
    };
    (@get i, $ext:expr, $key:expr) => { ext_i32($ext, $key) };
    (@get l, $ext:expr, $key:expr) => { ext_i64($ext, $key) };
    (@get b, $ext:expr, $key:expr) => { ext_bool($ext, $key) };
    (@get B, $ext:expr, $key:expr) => { ext_bool($ext, $key).unwrap_or(false) };
    (@get I, $ext:expr, $key:expr) => { ext_i32($ext, $key).unwrap_or(0) };
}

/// Makes `Box<dyn CustomHeader>` printable.
///
/// Prints only the `TypeId`: headers carry business identifiers such as topic / group / clientId, and the full contents must not leak into logs;
/// callers that need the concrete contents should call `to_ext_fields` themselves and print that.
impl fmt::Debug for dyn CustomHeader + '_ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CustomHeader({:?})", self.as_any().type_id())
    }
}


//   GetMaxOffsetRequestHeader: server-side-only fields ["committed"]
//   SearchOffsetRequestHeader: server-side-only fields ["liteTopic"]
//   QueryMessageResponseHeader: server-side-only fields ["indexLastUpdatePhyoffset"]
//   ViewMessageRequestHeader: server-side-only fields ["topic"]
//   ConsumeMessageDirectlyResultRequestHeader: server-side-only fields ["topic", "topicSysFlag", "groupSysFlag"]
//   ResetOffsetRequestHeader: server-side-only fields ["queueId", "offset"]
//   EndTransactionRequestHeader: extra client-reported keys ["bname"]
//   CheckTransactionStateRequestHeader: extra client-reported keys ["bname"]
//   CheckTransactionStateResponseHeader: extra client-reported keys ["groupName", "transactionState", "offset"]
//   CheckTransactionStateResponseHeader: server-side-only fields ["producerGroup", "tranStateTableOffset", "commitLogOffset", "commitOrRollback"]
//   GetAllTopicConfigRequestHeader: server-side-only fields ["topicSeq", "dataVersion", "maxTopicNum"]
//   GetAllTopicConfigResponseHeader: extra client-reported keys ["dataVersion"]
//   GetAllTopicConfigResponseHeader: server-side-only fields ["totalTopicNum"]
//   GetConsumeStatsRequestHeader: server-side-only fields ["TOPIC_NAME_SEPARATOR", "topicList"]
header_struct! {
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
/// The broker → requester PUSH_REPLY_MESSAGE_TO_CLIENT(326) request header.
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
    SendMessageResponseHeader {
        msg_id: s => "msgId",
        queue_id: i => "queueId",
        queue_offset: l => "queueOffset",
        transaction_id: s => "transactionId",
        batch_uniq_id: s => "batchUniqId",
        recall_handle: s => "recallHandle",

    }
}

header_struct! {
/// Timed/delayed-message recall request (`RECALL_MESSAGE` 370).
    RecallMessageRequestHeader {
        producer_group: s => "producerGroup",
        topic: s => "topic",
        recall_handle: s => "recallHandle",
        bname: s => "bname",

    }
}

header_struct! {
    RecallMessageResponseHeader {
        msg_id: s => "msgId",

    }
}

header_struct! {
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
    QueryConsumerOffsetRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",
        queue_id: i => "queueId",
        set_zero_if_not_found: b => "setZeroIfNotFound",

    }
}

header_struct! {
    QueryConsumerOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
    UpdateConsumerOffsetRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",
        queue_id: i => "queueId",
        commit_offset: l => "commitOffset",

    }
}

header_struct! {
    UpdateConsumerOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
    GetMaxOffsetRequestHeader {
        topic: s => "topic",
        queue_id: i => "queueId",

    }
}

header_struct! {
    GetMaxOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
    GetMinOffsetRequestHeader {
        topic: s => "topic",
        queue_id: i => "queueId",

    }
}

header_struct! {
    GetMinOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
    SearchOffsetRequestHeader {
        topic: s => "topic",
        queue_id: i => "queueId",
        timestamp: l => "timestamp",
        boundary_type: E => "boundaryType",

    }
}

header_struct! {
    SearchOffsetResponseHeader {
        offset: l => "offset",

    }
}

header_struct! {
    GetEarliestMsgStoretimeRequestHeader {
        topic: s => "topic",
        queue_id: i => "queueId",

    }
}

header_struct! {
    GetEarliestMsgStoretimeResponseHeader {
        timestamp: l => "timestamp",

    }
}

header_struct! {
/// ``index_type`` takes the key-index / unique-index / tag-index kinds "K" / "U" /
/// "T"; when it arrives empty the broker queries by "K" (ordinary key index) by default.
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
    QueryMessageResponseHeader {
        index_last_update_timestamp: l => "indexLastUpdateTimestamp",

    }
}

header_struct! {
    ViewMessageRequestHeader {
        offset: l => "offset",

    }
}

header_struct! {
    ViewMessageResponseHeader {

    }
}

header_struct! {
    HeartbeatRequestHeader {
        client_id: s => "clientID",

    }
}

header_struct! {
    UnregisterClientRequestHeader {
        client_id: s => "clientID",
        producer_group: s => "producerGroup",
        consumer_group: s => "consumerGroup",

    }
}

header_struct! {
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
    GetConsumerListByGroupRequestHeader {
        consumer_group: s => "consumerGroup",

    }
}

header_struct! {
    GetConsumerListByGroupResponseHeader {

    }
}

header_struct! {
    NotifyConsumerIdsChangedRequestHeader {
        consumer_group: s => "consumerGroup",

    }
}

header_struct! {
    GetConsumerConnectionListRequestHeader {
        consumer_group: s => "consumerGroup",

    }
}

header_struct! {
/// Request header of GET_CONSUMER_STATUS_FROM_CLIENT(221).
    GetConsumerStatusRequestHeader {
        topic: s => "topic",
        group: s => "group",
        client_addr: s => "clientAddr",

    }
}

header_struct! {
    GetConsumerRunningInfoRequestHeader {
        consumer_group: s => "consumerGroup",
        client_id: s => "clientId",
        jstack_enabled: b => "jstackEnable",

    }
}

header_struct! {
    ConsumeMessageDirectlyResultRequestHeader {
        consumer_group: s => "consumerGroup",
        client_id: s => "clientId",
        msg_id: s => "msgId",
        broker_name: s => "brokerName",

    }
}

header_struct! {
    ResetOffsetRequestHeader {
        topic: s => "topic",
        group: s => "group",
        timestamp: l => "timestamp",
        is_force: b => "isForce",

    }
}

header_struct! {
    LockBatchMqRequestHeader {
        consumer_group: s => "consumerGroup",
        client_id: s => "clientId",

    }
}

header_struct! {
    UnlockBatchMqRequestHeader {
        consumer_group: s => "consumerGroup",
        client_id: s => "clientId",

    }
}

header_struct! {
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
    EndTransactionResponseHeader {
        msg_id: s => "msgId",
        transaction_id: s => "transactionId",

    }
}

header_struct! {
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
    CheckTransactionStateResponseHeader {
        group_name: s => "groupName",
        transaction_state: i => "transactionState",
        offset: l => "offset",

    }
}

header_struct! {
    GetAllTopicConfigRequestHeader {

    }
}

header_struct! {
    GetAllTopicConfigResponseHeader {
        data_version: s => "dataVersion",

    }
}

header_struct! {
    GetTopicConfigRequestHeader {
        topic: s => "topic",

    }
}

header_struct! {
/// ⚠ the broker's checkFields() parses ``topicFilterType`` into an enum and **throws directly when it is
/// empty: RemotingCommandException("topicFilterType = [null] value invalid")**,
/// so even when only creating an ordinary topic, topicFilterType must be sent explicitly.
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
    DeleteTopicRequestHeader {
        topic: s => "topic",

    }
}

header_struct! {
    GetTopicStatsInfoRequestHeader {
        topic: s => "topic",

    }
}

header_struct! {
    GetConsumeStatsRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",

    }
}

header_struct! {
/// extFields: MISSING-IN-SERVER
    GetAllSubscriptionGroupConfigRequestHeader {

    }
}

header_struct! {
    GetSubscriptionGroupConfigRequestHeader {
        group: s => "group",

    }
}

header_struct! {
/// extFields: MISSING-IN-SERVER
    InterviewGetConsumerStatusRequestHeader {
        consumer_group: s => "consumerGroup",
        topic: s => "topic",

    }
}

header_struct! {
    GetTopicsByClusterRequestHeader {
        cluster: s => "cluster",

    }
}

header_struct! {
    GetBrokerConfigResponseHeader {
        version: s => "version",

    }
}

header_struct! {
/// extFields: MISSING-IN-SERVER
    GetTopicConfigResponseHeader {

    }
}

header_struct! {
/// extFields: MISSING-IN-SERVER
    GetSubscriptionGroupResponseHeader {

    }
}

header_struct! {
/// extFields: MISSING-IN-SERVER
    GetTopicListResponseHeader {
        broker_addr: s => "brokerAddr",

    }
}

header_struct! {
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
    RegisterBrokerResponseHeader {
        ha_server_addr: s => "haServerAddr",
        master_addr: s => "masterAddr",

    }
}

header_struct! {
    UnRegisterBrokerRequestHeader {
        broker_name: s => "brokerName",
        broker_addr: s => "brokerAddr",
        cluster_name: s => "clusterName",
        broker_id: i => "brokerId",

    }
}

header_struct! {
    GetRouteInfoRequestHeader {
        topic: s => "topic",
        accept_standard_json_only: b => "acceptStandardJsonOnly",

    }
}

header_struct! {
    PutKVConfigRequestHeader {
        namespace: s => "namespace",
        key: s => "key",
        value: s => "value",

    }
}

header_struct! {
    GetKVConfigRequestHeader {
        namespace: s => "namespace",
        key: s => "key",

    }
}

header_struct! {
    GetKVConfigResponseHeader {
        value: s => "value",

    }
}

header_struct! {
    DeleteKVConfigRequestHeader {
        namespace: s => "namespace",
        key: s => "key",

    }
}

header_struct! {
    GetKVListByNamespaceRequestHeader {
        namespace: s => "namespace",

    }
}

header_struct! {
    RegisterTopicRequestHeader {
        topic: s => "topic",

    }
}

header_struct! {
    RegisterOrderTopicRequestHeader {
        topic: s => "topic",
        order_topic_string: s => "orderTopicString",

    }
}

header_struct! {
    DeleteTopicFromNamesrvRequestHeader {
        topic: s => "topic",
        cluster_name: s => "clusterName",

    }
}

header_struct! {
    GetBrokerMemberGroupRequestHeader {
        cluster_name: s => "clusterName",
        broker_name: s => "brokerName",

    }
}

header_struct! {
    WipeWritePermOfBrokerRequestHeader {
        broker_name: s => "brokerName",

    }
}

header_struct! {
    WipeWritePermOfBrokerResponseHeader {
        wipe_topic_count: i => "wipeTopicCount",

    }
}

header_struct! {
    AddWritePermOfBrokerRequestHeader {
        broker_name: s => "brokerName",

    }
}

header_struct! {
    AddWritePermOfBrokerResponseHeader {
        add_topic_count: i => "addTopicCount",

    }
}

header_struct! {
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
    QueryDataVersionRequestHeader {
        broker_name: s => "brokerName",
        broker_addr: s => "brokerAddr",
        cluster_name: s => "clusterName",
        broker_id: i => "brokerId",

    }
}

header_struct! {
    QueryDataVersionResponseHeader {
        changed: b => "changed",

    }
}

header_struct! {
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
/// ``start_offset_info`` / ``msg_offset_info`` / ``order_count_info`` are encoded
/// strings (spaces separate fields, semicolons separate queues), parsed by the ``extra_info`` module.
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
/// ``offset`` is the **consumeQueue offset** (i.e. the 8th segment of the CK string / msgQueueOffset),
/// not the commitlog offset -- passing the wrong one makes the broker answer NO_MESSAGE.
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
/// Returns the **new** ``invisible_time`` / ``pop_time`` / ``revive_qid``;
/// the client must use them to rebuild extraInfo for subsequent ACKs.
    ChangeInvisibleTimeResponseHeader {
        pop_time: l => "popTime",
        invisible_time: l => "invisibleTime",
        revive_qid: i => "reviveQid",

    }
}

#[cfg(test)]
const PROTOCOL_HEADER_FIELDS: &[(&str, &[&str])] = &[
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




// ============================== tests ==============================

#[cfg(test)]
mod tests {
    use super::*;
    use super::PROTOCOL_HEADER_FIELDS;
    use crate::common::boundary_type::BoundaryType;
    use crate::remoting::protocol::codes::request_code;
    use crate::remoting::protocol::ext_fields::ExtFields;

    const TOPIC: &str = "PopUnitTestTopic";
    const GROUP: &str = "PopUnitTestGroup";
    const BROKER: &str = "broker-a";

    fn keys(ext: &ExtFields) -> Vec<&str> {
        ext.iter().map(|(k, _)| k.as_str()).collect()
    }

    /// Byte-for-byte guard: the POP request header's 12 long field names and their order are pinned to the protocol definition.
    #[test]
    fn pop_message_request_header_ext_keys_exactly_match_protocol() {
        let h = PopMessageRequestHeader {
            consumer_group: Some(GROUP.into()),
            topic: Some(TOPIC.into()),
            queue_id: Some(-1),
            max_msg_nums: Some(32),
            invisible_time: Some(60000),
            poll_time: Some(0),
            // bornTime set to 0 makes the broker answer POLLING_TIMEOUT(210) directly; it must be non-zero
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

    #[test]
    fn pop_message_order_is_always_serialized_as_lowercase() {
        let h = PopMessageRequestHeader {
            topic: Some(TOPIC.into()),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        h.to_ext_fields(&mut ext);
        assert_eq!(ext.get("order"), Some("false"));
        // Unset Option fields must not appear on the wire.
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

    /// The broker's `checkFields()` parses `topicFilterType` into an enum and throws directly when it is empty;
    /// also guards the two late-added fields `attributes` / `force`.
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

        // With only topic given, the optional fields must be absent entirely (not written as empty strings).
        let minimal = CreateTopicRequestHeader {
            topic: Some("t".into()),
            ..Default::default()
        };
        let mut ext = ExtFields::new();
        minimal.to_ext_fields(&mut ext);
        assert_eq!(keys(&ext), vec!["topic"]);

        // attributes / force are late-added fields; the reading side must be able to restore them.
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

    /// The three field-name guards of `RECALL_MESSAGE`(370).
    #[test]
    fn recall_message_headers_use_wire_keys() {
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

        // A timed message's recallHandle is handed down by the broker; ordinary messages do not carry this key.
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

    /// Field-by-field round-trip guard for `QueryMessageRequestHeader`.
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

    #[test]
    fn search_offset_request_header_carries_the_protocol_boundary_type() {
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
        assert_eq!(h.enable_acting_master, Some(false));
        assert!(h.compressed);
        assert_eq!(h.body_crc32, 123);
    }

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

    /// Numeric field parsing requires a decimal integer string; dirty data degrades to `None` instead of panicking.
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
        // out-of-range numbers degrade the same way
        let mut big = ExtFields::new();
        big.insert("queueId", "99999999999999");
        let mut h3 = QueryConsumerOffsetRequestHeader::default();
        h3.from_ext_fields(&big);
        assert_eq!(h3.queue_id, None);
    }

    /// The `RemotingCommand` encode path: extFields only get content after `makeCustomHeaderToNet`.
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

    #[test]
    fn test_header_field_names_match_protocol() {
        let root = match std::env::var("ROCKETMQ_SERVER_SRC") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                eprintln!("ROCKETMQ_SERVER_SRC not set, skipping the server-side field-name cross-check");
                return;
            }
        };
        let root = std::path::PathBuf::from(root);
        assert!(root.is_dir(), "ROCKETMQ_SERVER_SRC is not a directory: {root:?}");
        for (class, fields) in PROTOCOL_HEADER_FIELDS {
            let declared = java_declared_fields(&root, class, 0);
            if declared.is_empty() {
                continue;
            }
            for field in *fields {
                assert!(
                    declared.iter().any(|d| d == field),
                    "server {class} no longer has field {field} (current fields: {declared:?})"
                );
            }
        }
    }

    /// Recursively finds `<name>.java` and collects the field names declared by the class and its parents (up to 3 levels).
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
