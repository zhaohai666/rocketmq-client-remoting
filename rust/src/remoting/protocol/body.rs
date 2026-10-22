//! Model classes for every request/response body in the body layer of wire messages:
//! `KVTable` / `TopicList` / `LockBatchRequestBody` / `LockBatchResponseBody` /
//! `UnlockBatchRequestBody` / `GetConsumerListByGroupResponseBody` / `ClusterInfo` /
//! `ConsumerRunningInfo` / `Connection` / `ConsumerConnection` / `ProducerConnection` /
//! `QueryConsumeTimeSpanBody` / `ConsumeStatus` / `ConsumeStatsList` /
//! `ResetOffsetBody` / `GetConsumerStatusBody` / `ProcessQueueInfo` / `CMResult` /
//! `ConsumeMessageDirectlyResult` / `BatchAck` / `BatchAckMessageRequestBody`.
//!
//! ## Three different spellings of "the same MessageQueue"

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use serde_json::{Map, Value};

use super::admin_body::{
    expect_object, jarray, jboolean, jdouble, jentries, jfield, jint, jlong, jraw_map, json_object,
    jstring, MessageQueueKey,
};
use super::ext_fields::StringMap;
use super::extra_info;
use super::heartbeat::SubscriptionData;
use super::serialize::RemotingSerializable;
use crate::error::{Error, Result};

// Also re-export the MessageQueue key helpers from `admin_body` here, so upper layers can pick them up nearby.
pub use super::admin_body::{
    decode_message_queue_map, encode_message_queue_map, message_queue_key, parse_message_queue_key,
};

// ---------------------------------------------------------------- internal small helpers

/// String-array field: missing / null falls back to an empty vec.
///
/// Elements use `jstring`'s lenient conversion: a broker sometimes writes strings as numbers.
fn string_list(value: &Value, key: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for item in jarray(value, key)? {
        out.push(match item {
            Value::String(s) => s.clone(),
            Value::Null => String::new(),
            other => other.to_string(),
        });
    }
    Ok(out)
}

/// Object arrays passed through verbatim (`subscriptionSet` / `consumeStatsList` / `consumeTimeSpanSet`).
///
/// No shape validation -- even `null` elements are kept exactly as they are.
fn raw_object_list(value: &Value, key: &str) -> Result<Vec<Value>> {
    Ok(jarray(value, key)?.into_iter().cloned().collect())
}

/// Object maps passed through verbatim, keeping wire order (`statusTable` / `subscriptionTable`).
fn raw_object_map(value: &Value, key: &str) -> Result<Vec<(String, Value)>> {
    Ok(jentries(value, key)?
        .into_iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect())
}

/// Write a `Vec<(String, Value)>` back into a JSON object (`Map` has `preserve_order`, so output order is insertion order).
fn object_of(entries: &[(String, Value)]) -> Value {
    let mut map = Map::new();
    for (k, v) in entries {
        map.insert(k.clone(), v.clone());
    }
    Value::Object(map)
}

/// Value parser for `decode_message_queue_map`: passes values through unchanged (see the docs for value types).
fn clone_value(value: &Value) -> Result<Value> {
    Ok(value.clone())
}

fn string_array(items: &[String]) -> Value {
    Value::Array(items.iter().map(|s| Value::String(s.clone())).collect())
}

/// `None` becomes `null` (keys are written unconditionally, which is different from "key absent").
fn optional_string(value: &Option<String>) -> Value {
    match value {
        Some(s) => Value::String(s.clone()),
        None => Value::Null,
    }
}

/// The same lenient conversion as [`jlong`] but applied to a bare `Value` (map values have no key name to pass).
fn long_of(value: &Value) -> i64 {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .or_else(|| n.as_u64().map(|v| v as i64))
            .unwrap_or(0),
        Value::String(s) => s.trim().parse::<i64>().unwrap_or(0),
        Value::Bool(b) => i64::from(*b),
        _ => 0,
    }
}

/// Value conversion for `encode_message_queue_map`: the offset table.
fn long_value(value: &i64) -> Value {
    Value::from(*value)
}

/// Lenient read of a text field (map-value context, same as [`jstring`] but no key name needed).
fn text_of(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Read a MessageQueue array field (`mqSet` / `lockOKMQSet`).
///
/// Non-object elements error out directly: a lenient fallback would turn dirty data
/// into "a queue with an empty topic" and silently drop locks, which is worse than erroring.
fn message_queue_list(value: &Value, key: &str) -> Result<Vec<MessageQueueKey>> {
    let mut out = Vec::new();
    for item in jarray(value, key)? {
        match MessageQueueKey::from_value(item) {
            Some(mq) => out.push(mq),
            None => {
                return Err(Error::Decode(format!(
                    "{key}[{item}] is not a messageQueue object"
                )))
            }
        }
    }
    Ok(out)
}

fn message_queue_array(list: &[MessageQueueKey]) -> Value {
    Value::Array(list.iter().map(message_queue_value).collect())
}

/// Shape of `MessageQueue` when used as a **value** (array element / nested object),
/// key order `topic,brokerName,queueId`.
pub fn message_queue_value(mq: &MessageQueueKey) -> Value {
    json_object(vec![
        ("topic", Value::String(mq.topic.clone())),
        ("brokerName", Value::String(mq.broker_name.clone())),
        ("queueId", Value::from(mq.queue_id)),
    ])
}

// ---------------------------------------------------------------- KVTable

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KVTable {
    /// cluster → the KV string given by the broker; order is the broker's return order, hence [`StringMap`].
    pub table: StringMap,
}

impl KVTable {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![("table", self.table.to_json())])
    }

    pub fn from_json_value(value: &Value) -> Result<KVTable> {
        expect_object(value, "KVTable")?;
        Ok(KVTable {
            // Missing key / null / non-object all fall back to an empty table
            table: StringMap::from_json(&jraw_map(value, "table")),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<KVTable> {
        KVTable::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- TopicList

/// When `brokerAddr` is `None` the **entire key is absent**; an empty string must still be written.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicList {
    pub topic_list: Vec<String>,
    pub broker_addr: Option<String>,
}

impl TopicList {
    pub fn new() -> TopicList {
        TopicList::default()
    }

    /// Return a copy of the topic list.
    pub fn get_topic_list(&self) -> Vec<String> {
        self.topic_list.clone()
    }

    pub fn to_json_value(&self) -> Value {
        let mut map = Map::new();
        map.insert("topicList".to_string(), string_array(&self.topic_list));
        if let Some(addr) = &self.broker_addr {
            map.insert("brokerAddr".to_string(), Value::String(addr.clone()));
        }
        Value::Object(map)
    }

    pub fn from_json_value(value: &Value) -> Result<TopicList> {
        expect_object(value, "TopicList")?;
        Ok(TopicList {
            topic_list: string_list(value, "topicList")?,
            broker_addr: jstring(value, "brokerAddr"),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<TopicList> {
        TopicList::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- batch lock / unlock

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockBatchRequestBody {
    pub consumer_group: Option<String>,
    pub client_id: Option<String>,
    pub mq_set: Vec<MessageQueueKey>,
}

impl LockBatchRequestBody {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("consumerGroup", optional_string(&self.consumer_group)),
            ("clientId", optional_string(&self.client_id)),
            ("mqSet", message_queue_array(&self.mq_set)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<LockBatchRequestBody> {
        expect_object(value, "LockBatchRequestBody")?;
        Ok(LockBatchRequestBody {
            consumer_group: jstring(value, "consumerGroup"),
            client_id: jstring(value, "clientId"),
            mq_set: message_queue_list(value, "mqSet")?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<LockBatchRequestBody> {
        LockBatchRequestBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnlockBatchRequestBody {
    pub consumer_group: Option<String>,
    pub client_id: Option<String>,
    pub mq_set: Vec<MessageQueueKey>,
}

impl UnlockBatchRequestBody {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("consumerGroup", optional_string(&self.consumer_group)),
            ("clientId", optional_string(&self.client_id)),
            ("mqSet", message_queue_array(&self.mq_set)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<UnlockBatchRequestBody> {
        expect_object(value, "UnlockBatchRequestBody")?;
        Ok(UnlockBatchRequestBody {
            consumer_group: jstring(value, "consumerGroup"),
            client_id: jstring(value, "clientId"),
            mq_set: message_queue_list(value, "mqSet")?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<UnlockBatchRequestBody> {
        UnlockBatchRequestBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockBatchResponseBody {
    pub lock_ok_mq_set: Vec<MessageQueueKey>,
}

impl LockBatchResponseBody {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![(
            "lockOKMQSet",
            message_queue_array(&self.lock_ok_mq_set),
        )])
    }

    pub fn from_json_value(value: &Value) -> Result<LockBatchResponseBody> {
        expect_object(value, "LockBatchResponseBody")?;
        Ok(LockBatchResponseBody {
            lock_ok_mq_set: message_queue_list(value, "lockOKMQSet")?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<LockBatchResponseBody> {
        LockBatchResponseBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetConsumerListByGroupResponseBody {
    pub consumer_id_list: Vec<String>,
}

impl GetConsumerListByGroupResponseBody {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![(
            "consumerIdList",
            string_array(&self.consumer_id_list),
        )])
    }

    pub fn from_json_value(value: &Value) -> Result<GetConsumerListByGroupResponseBody> {
        expect_object(value, "GetConsumerListByGroupResponseBody")?;
        Ok(GetConsumerListByGroupResponseBody {
            consumer_id_list: string_list(value, "consumerIdList")?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<GetConsumerListByGroupResponseBody> {
        GetConsumerListByGroupResponseBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- CHECK_CLIENT_CONFIG

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckClientRequestBody {
    pub client_id: Option<String>,
    pub group: Option<String>,
    pub subscription_data: Option<SubscriptionData>,
    pub namespace: Option<String>,
}

impl CheckClientRequestBody {
    pub fn to_json_value(&self) -> Value {
        let mut entries: Vec<(&'static str, Value)> = vec![
            ("clientId", optional_string(&self.client_id)),
            ("group", optional_string(&self.group)),
        ];
        if let Some(sd) = &self.subscription_data {
            entries.push(("subscriptionData", sd.to_json_value()));
        }
        if let Some(ns) = &self.namespace {
            entries.push(("namespace", Value::String(ns.clone())));
        }
        json_object(entries)
    }

    pub fn from_json_value(value: &Value) -> Result<CheckClientRequestBody> {
        expect_object(value, "CheckClientRequestBody")?;
        Ok(CheckClientRequestBody {
            client_id: jstring(value, "clientId"),
            group: jstring(value, "group"),
            subscription_data: match value.get("subscriptionData") {
                Some(v) if v.is_object() => Some(SubscriptionData::from_json_value(v)?),
                _ => None,
            },
            namespace: jstring(value, "namespace"),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<CheckClientRequestBody> {
        CheckClientRequestBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- ClusterInfo

/// Only two tables are kept internally: `{brokerName: {brokerId: addr}}` and `{cluster: [brokerName]}`;
/// `to_json_value` re-wraps the former into BrokerData shape, where `cluster:""` and
/// `enableActingMaster:false` are **constant defaults**,
/// so the real cluster name / acting-master flag never round-trips through this type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterInfo {
    pub broker_addr_table: Vec<(String, Vec<(i64, String)>)>,
    pub cluster_addr_table: Vec<(String, Vec<String>)>,
}

impl ClusterInfo {
    /// All broker addresses: deduplicated, ascending by brokerName then brokerId, skipping empty strings.
    pub fn get_broker_addrs(&self) -> Vec<String> {
        let mut ordered: Vec<&(String, Vec<(i64, String)>)> =
            self.broker_addr_table.iter().collect();
        ordered.sort_by(|a, b| a.0.cmp(&b.0));
        let mut addrs: Vec<String> = Vec::new();
        for (_, ids) in ordered {
            let mut sorted = ids.clone();
            sorted.sort_by_key(|(id, _)| *id);
            for (_, addr) in sorted {
                if !addr.is_empty() && !addrs.contains(&addr) {
                    addrs.push(addr);
                }
            }
        }
        addrs
    }

    pub fn to_json_value(&self) -> Value {
        let mut table = Map::new();
        for (name, addrs) in &self.broker_addr_table {
            let mut addr_map = Map::new();
            for (id, addr) in addrs {
                // fastjson2 writes Long keys as bare numbers; this crate and route.rs both
                // write quoted decimal strings; both readings are tolerated, output always uses the quoted form.
                addr_map.insert(id.to_string(), Value::String(addr.clone()));
            }
            table.insert(
                name.clone(),
                json_object(vec![
                    ("cluster", Value::String(String::new())),
                    ("brokerName", Value::String(name.clone())),
                    ("brokerAddrs", Value::Object(addr_map)),
                    ("enableActingMaster", Value::Bool(false)),
                ]),
            );
        }
        let mut clusters = Map::new();
        for (cluster, names) in &self.cluster_addr_table {
            clusters.insert(cluster.clone(), string_array(names));
        }
        json_object(vec![
            ("brokerAddrTable", Value::Object(table)),
            ("clusterAddrTable", Value::Object(clusters)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<ClusterInfo> {
        expect_object(value, "ClusterInfo")?;
        let mut broker_addr_table = Vec::new();
        for (name, broker) in jentries(value, "brokerAddrTable")? {
            if !broker.is_object() {
                return Err(Error::Decode(format!(
                    "brokerAddrTable[{name}] is not a json object"
                )));
            }
            // For real brokers, brokerAddrTable[name] is a BrokerData; the address table sits in its brokerAddrs
            let mut addrs = Vec::new();
            for (id, addr) in jentries(broker, "brokerAddrs")? {
                let id = id.trim().parse::<i64>().map_err(|_| {
                    Error::Decode(format!(
                        "brokerAddrTable[{name}] brokerId {id:?} is not a long"
                    ))
                })?;
                addrs.push((id, text_of(addr)));
            }
            broker_addr_table.push((name.clone(), addrs));
        }
        let mut cluster_addr_table = Vec::new();
        for (cluster, names) in jentries(value, "clusterAddrTable")? {
            match names.as_array() {
                None => {
                    return Err(Error::Decode(format!(
                        "clusterAddrTable[{cluster}] is not a json array"
                    )))
                }
                Some(items) => {
                    cluster_addr_table.push((cluster.clone(), items.iter().map(text_of).collect()))
                }
            }
        }
        Ok(ClusterInfo {
            broker_addr_table,
            cluster_addr_table,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ClusterInfo> {
        ClusterInfo::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- ConsumerRunningInfo

/// Needed on both sides: the admin side parses running info aggregated from brokers, while the client side
/// encodes its own running info when answering `GET_CONSUMER_RUNNING_INFO(307)`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConsumerRunningInfo {
    pub properties: StringMap,
    pub subscription_set: Vec<Value>,
    pub mq_table: Vec<(MessageQueueKey, Value)>,
    pub mq_pop_table: Vec<(MessageQueueKey, Value)>,
    pub status_table: Vec<(String, Value)>,
    pub user_consumer_info: StringMap,
    pub jstack: Option<String>,
}

impl ConsumerRunningInfo {
    pub const PROP_NAMESERVER_ADDR: &'static str = "PROP_NAMESERVER_ADDR";
    pub const PROP_THREADPOOL_CORE_SIZE: &'static str = "PROP_THREADPOOL_CORE_SIZE";
    pub const PROP_CONSUME_ORDERLY: &'static str = "PROP_CONSUMEORDERLY";
    pub const PROP_CONSUME_TYPE: &'static str = "PROP_CONSUME_TYPE";
    pub const PROP_CLIENT_VERSION: &'static str = "PROP_CLIENT_VERSION";
    pub const PROP_CONSUMER_START_TIMESTAMP: &'static str = "PROP_CONSUMER_START_TIMESTAMP";

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("properties", self.properties.to_json()),
            (
                "subscriptionSet",
                Value::Array(self.subscription_set.clone()),
            ),
            (
                "mqTable",
                encode_message_queue_map(&self.mq_table, |v| v.clone()),
            ),
            (
                "mqPopTable",
                encode_message_queue_map(&self.mq_pop_table, |v| v.clone()),
            ),
            ("statusTable", object_of(&self.status_table)),
            ("userConsumerInfo", self.user_consumer_info.to_json()),
            // The jstack key is written unconditionally: None becomes null instead of disappearing
            ("jstack", optional_string(&self.jstack)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<ConsumerRunningInfo> {
        expect_object(value, "ConsumerRunningInfo")?;
        Ok(ConsumerRunningInfo {
            properties: StringMap::from_json(&jraw_map(value, "properties")),
            subscription_set: raw_object_list(value, "subscriptionSet")?,
            mq_table: decode_message_queue_map(jfield(value, "mqTable"), clone_value)?,
            mq_pop_table: decode_message_queue_map(jfield(value, "mqPopTable"), clone_value)?,
            status_table: raw_object_map(value, "statusTable")?,
            user_consumer_info: StringMap::from_json(&jraw_map(value, "userConsumerInfo")),
            jstack: jstring(value, "jstack"),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ConsumerRunningInfo> {
        ConsumerRunningInfo::from_json_value(&RemotingSerializable::decode(data)?)
    }

    /// Decode `statusTable[topic]` in place into a [`ConsumeStatus`], sparing callers a manual conversion.
    pub fn consume_status(&self, topic: &str) -> Result<Option<ConsumeStatus>> {
        match self.status_table.iter().find(|(k, _)| k == topic) {
            None => Ok(None),
            Some((_, v)) => Ok(Some(ConsumeStatus::from_json_value(v)?)),
        }
    }
}

// ---------------------------------------------------------------- Connection / connection classes

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Connection {
    pub client_id: Option<String>,
    pub client_addr: Option<String>,
    pub language: Option<String>,
    pub version: Option<i32>,
}

impl Connection {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("clientId", optional_string(&self.client_id)),
            ("clientAddr", optional_string(&self.client_addr)),
            ("language", optional_string(&self.language)),
            (
                "version",
                match self.version {
                    Some(v) => Value::from(v),
                    None => Value::Null,
                },
            ),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<Connection> {
        expect_object(value, "Connection")?;
        Ok(Connection {
            client_id: jstring(value, "clientId"),
            client_addr: jstring(value, "clientAddr"),
            language: jstring(value, "language"),
            // Missing key / null -> None, kept apart from "the broker really gave 0"
            version: jfield(value, "version").map(|v| long_of(v) as i32),
        })
    }
}

/// How `connectionSet` is read (shared by ConsumerConnection / ProducerConnection).
fn connections(value: &Value) -> Result<Vec<Connection>> {
    let mut out = Vec::new();
    for item in jarray(value, "connectionSet")? {
        out.push(Connection::from_json_value(item)?);
    }
    Ok(out)
}

fn connection_array(list: &[Connection]) -> Value {
    Value::Array(list.iter().map(|c| c.to_json_value()).collect())
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConsumerConnection {
    pub connection_set: Vec<Connection>,
    /// topic → JSON of a `SubscriptionData` (type see `heartbeat::SubscriptionData`).
    pub subscription_table: Vec<(String, Value)>,
    pub consume_type: Option<String>,
    pub message_model: Option<String>,
    pub consume_from_where: Option<String>,
}

impl ConsumerConnection {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("connectionSet", connection_array(&self.connection_set)),
            ("subscriptionTable", object_of(&self.subscription_table)),
            ("consumeType", optional_string(&self.consume_type)),
            ("messageModel", optional_string(&self.message_model)),
            (
                "consumeFromWhere",
                optional_string(&self.consume_from_where),
            ),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<ConsumerConnection> {
        expect_object(value, "ConsumerConnection")?;
        Ok(ConsumerConnection {
            connection_set: connections(value)?,
            subscription_table: raw_object_map(value, "subscriptionTable")?,
            consume_type: jstring(value, "consumeType"),
            message_model: jstring(value, "messageModel"),
            consume_from_where: jstring(value, "consumeFromWhere"),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ConsumerConnection> {
        ConsumerConnection::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProducerConnection {
    pub connection_set: Vec<Connection>,
}

impl ProducerConnection {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![(
            "connectionSet",
            connection_array(&self.connection_set),
        )])
    }

    pub fn from_json_value(value: &Value) -> Result<ProducerConnection> {
        expect_object(value, "ProducerConnection")?;
        Ok(ProducerConnection {
            connection_set: connections(value)?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ProducerConnection> {
        ProducerConnection::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryConsumeTimeSpanBody {
    pub consume_time_span_set: Vec<Value>,
}

impl QueryConsumeTimeSpanBody {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![(
            "consumeTimeSpanSet",
            Value::Array(self.consume_time_span_set.clone()),
        )])
    }

    pub fn from_json_value(value: &Value) -> Result<QueryConsumeTimeSpanBody> {
        expect_object(value, "QueryConsumeTimeSpanBody")?;
        Ok(QueryConsumeTimeSpanBody {
            consume_time_span_set: raw_object_list(value, "consumeTimeSpanSet")?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<QueryConsumeTimeSpanBody> {
        QueryConsumeTimeSpanBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- ConsumeStatus

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConsumeStatus {
    pub pull_rt: f64,
    pub pull_tps: f64,
    pub consume_rt: f64,
    pub consume_ok_tps: f64,
    pub consume_failed_tps: f64,
    pub consume_failed_msgs: i64,
}

impl ConsumeStatus {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("pullRT", Value::from(self.pull_rt)),
            ("pullTPS", Value::from(self.pull_tps)),
            ("consumeRT", Value::from(self.consume_rt)),
            ("consumeOKTPS", Value::from(self.consume_ok_tps)),
            ("consumeFailedTPS", Value::from(self.consume_failed_tps)),
            ("consumeFailedMsgs", Value::from(self.consume_failed_msgs)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<ConsumeStatus> {
        expect_object(value, "ConsumeStatus")?;
        Ok(ConsumeStatus {
            pull_rt: jdouble(value, "pullRT", 0.0),
            pull_tps: jdouble(value, "pullTPS", 0.0),
            consume_rt: jdouble(value, "consumeRT", 0.0),
            consume_ok_tps: jdouble(value, "consumeOKTPS", 0.0),
            consume_failed_tps: jdouble(value, "consumeFailedTPS", 0.0),
            consume_failed_msgs: jlong(value, "consumeFailedMsgs", 0),
        })
    }
}

// ---------------------------------------------------------------- ConsumeStatsList

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConsumeStatsList {
    pub stats_list: Vec<Value>,
    pub broker_addr: Option<String>,
    pub total_diff: i64,
    pub total_inflight_diff: i64,
}

impl ConsumeStatsList {
    pub fn new() -> ConsumeStatsList {
        ConsumeStatsList::default()
    }

    pub fn to_json_value(&self) -> Value {
        let mut map = Map::new();
        map.insert(
            "consumeStatsList".to_string(),
            Value::Array(self.stats_list.clone()),
        );
        if let Some(addr) = &self.broker_addr {
            map.insert("brokerAddr".to_string(), Value::String(addr.clone()));
        }
        map.insert("totalDiff".to_string(), Value::from(self.total_diff));
        map.insert(
            "totalInflightDiff".to_string(),
            Value::from(self.total_inflight_diff),
        );
        Value::Object(map)
    }

    pub fn from_json_value(value: &Value) -> Result<ConsumeStatsList> {
        expect_object(value, "ConsumeStatsList")?;
        Ok(ConsumeStatsList {
            stats_list: raw_object_list(value, "consumeStatsList")?,
            broker_addr: jstring(value, "brokerAddr"),
            total_diff: jlong(value, "totalDiff", 0),
            total_inflight_diff: jlong(value, "totalInflightDiff", 0),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ConsumeStatsList> {
        ConsumeStatsList::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- offset reset / status reporting

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResetOffsetBody {
    pub offset_table: Vec<(MessageQueueKey, i64)>,
}

impl ResetOffsetBody {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![(
            "offsetTable",
            encode_message_queue_map(&self.offset_table, long_value),
        )])
    }

    pub fn from_json_value(value: &Value) -> Result<ResetOffsetBody> {
        expect_object(value, "ResetOffsetBody")?;
        Ok(ResetOffsetBody {
            offset_table: decode_message_queue_map(jfield(value, "offsetTable"), |v| {
                Ok(long_of(v))
            })?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ResetOffsetBody> {
        ResetOffsetBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- ResetOffsetBodyForC

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageQueueForC {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub offset: i64,
}

impl MessageQueueForC {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("topic", Value::String(self.topic.clone())),
            ("brokerName", Value::String(self.broker_name.clone())),
            ("queueId", Value::from(self.queue_id)),
            ("offset", Value::from(self.offset)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<MessageQueueForC> {
        expect_object(value, "MessageQueueForC")?;
        Ok(MessageQueueForC {
            topic: jstring(value, "topic").unwrap_or_default(),
            broker_name: jstring(value, "brokerName").unwrap_or_default(),
            queue_id: jint(value, "queueId", 0),
            offset: jlong(value, "offset", 0),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResetOffsetBodyForC {
    pub offset_table: Vec<MessageQueueForC>,
}

impl ResetOffsetBodyForC {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![(
            "offsetTable",
            Value::Array(
                self.offset_table
                    .iter()
                    .map(MessageQueueForC::to_json_value)
                    .collect(),
            ),
        )])
    }

    pub fn from_json_value(value: &Value) -> Result<ResetOffsetBodyForC> {
        expect_object(value, "ResetOffsetBodyForC")?;
        let mut offset_table = Vec::new();
        for item in jarray(value, "offsetTable")? {
            if item.is_object() {
                offset_table.push(MessageQueueForC::from_json_value(item)?);
            }
        }
        Ok(ResetOffsetBodyForC { offset_table })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ResetOffsetBodyForC> {
        ResetOffsetBodyForC::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetConsumerStatusBody {
    pub message_queue_table: Vec<(MessageQueueKey, i64)>,
    pub consumer_table: Vec<(String, Vec<(MessageQueueKey, i64)>)>,
}

impl GetConsumerStatusBody {
    pub fn to_json_value(&self) -> Value {
        let mut consumer_table = Map::new();
        for (cid, table) in &self.consumer_table {
            consumer_table.insert(cid.clone(), encode_message_queue_map(table, long_value));
        }
        json_object(vec![
            (
                "messageQueueTable",
                encode_message_queue_map(&self.message_queue_table, long_value),
            ),
            ("consumerTable", Value::Object(consumer_table)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<GetConsumerStatusBody> {
        expect_object(value, "GetConsumerStatusBody")?;
        let mut consumer_table = Vec::new();
        for (cid, table) in jentries(value, "consumerTable")? {
            consumer_table.push((
                cid.clone(),
                decode_message_queue_map(Some(table), |v| Ok(long_of(v)))?,
            ));
        }
        Ok(GetConsumerStatusBody {
            message_queue_table: decode_message_queue_map(
                jfield(value, "messageQueueTable"),
                |v| Ok(long_of(v)),
            )?,
            consumer_table,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<GetConsumerStatusBody> {
        GetConsumerStatusBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- ProcessQueueInfo

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessQueueInfo {
    pub commit_offset: i64,
    pub cached_msg_min_offset: i64,
    pub cached_msg_max_offset: i64,
    pub cached_msg_count: i32,
    pub cached_msg_size_in_mib: i32,
    pub transaction_msg_min_offset: i64,
    pub transaction_msg_max_offset: i64,
    pub transaction_msg_count: i32,
    pub locked: bool,
    pub try_unlock_times: i64,
    pub last_lock_timestamp: i64,
    pub droped: bool,
    pub last_pull_timestamp: i64,
    pub last_consume_timestamp: i64,
}

impl ProcessQueueInfo {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("commitOffset", Value::from(self.commit_offset)),
            (
                "cachedMsgMinOffset",
                Value::from(self.cached_msg_min_offset),
            ),
            (
                "cachedMsgMaxOffset",
                Value::from(self.cached_msg_max_offset),
            ),
            ("cachedMsgCount", Value::from(self.cached_msg_count)),
            (
                "cachedMsgSizeInMiB",
                Value::from(self.cached_msg_size_in_mib),
            ),
            (
                "transactionMsgMinOffset",
                Value::from(self.transaction_msg_min_offset),
            ),
            (
                "transactionMsgMaxOffset",
                Value::from(self.transaction_msg_max_offset),
            ),
            (
                "transactionMsgCount",
                Value::from(self.transaction_msg_count),
            ),
            ("locked", Value::from(self.locked)),
            ("tryUnlockTimes", Value::from(self.try_unlock_times)),
            ("lastLockTimestamp", Value::from(self.last_lock_timestamp)),
            ("droped", Value::from(self.droped)),
            ("lastPullTimestamp", Value::from(self.last_pull_timestamp)),
            (
                "lastConsumeTimestamp",
                Value::from(self.last_consume_timestamp),
            ),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<ProcessQueueInfo> {
        expect_object(value, "ProcessQueueInfo")?;
        Ok(ProcessQueueInfo {
            commit_offset: jlong(value, "commitOffset", 0),
            cached_msg_min_offset: jlong(value, "cachedMsgMinOffset", 0),
            cached_msg_max_offset: jlong(value, "cachedMsgMaxOffset", 0),
            cached_msg_count: jint(value, "cachedMsgCount", 0),
            cached_msg_size_in_mib: jint(value, "cachedMsgSizeInMiB", 0),
            transaction_msg_min_offset: jlong(value, "transactionMsgMinOffset", 0),
            transaction_msg_max_offset: jlong(value, "transactionMsgMaxOffset", 0),
            transaction_msg_count: jint(value, "transactionMsgCount", 0),
            locked: jboolean(value, "locked", false),
            try_unlock_times: jlong(value, "tryUnlockTimes", 0),
            last_lock_timestamp: jlong(value, "lastLockTimestamp", 0),
            droped: jboolean(value, "droped", false),
            last_pull_timestamp: jlong(value, "lastPullTimestamp", 0),
            last_consume_timestamp: jlong(value, "lastConsumeTimestamp", 0),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ProcessQueueInfo> {
        ProcessQueueInfo::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- 309 consume a single message directly

pub struct CMResult;

impl CMResult {
    pub const CR_SUCCESS: &'static str = "CR_SUCCESS";
    pub const CR_LATER: &'static str = "CR_LATER";
    pub const CR_ROLLBACK: &'static str = "CR_ROLLBACK";
    pub const CR_COMMIT: &'static str = "CR_COMMIT";
    pub const CR_THROW_EXCEPTION: &'static str = "CR_THROW_EXCEPTION";
    pub const CR_RETURN_NULL: &'static str = "CR_RETURN_NULL";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeMessageDirectlyResult {
    pub order: bool,
    pub auto_commit: bool,
    /// The enum name from [`CMResult`].
    pub consume_result: Option<String>,
    pub remark: Option<String>,
    pub spent_time_mills: i64,
}

impl Default for ConsumeMessageDirectlyResult {
    fn default() -> Self {
        ConsumeMessageDirectlyResult {
            order: false,
            auto_commit: true,
            consume_result: None,
            remark: None,
            spent_time_mills: 0,
        }
    }
}

impl ConsumeMessageDirectlyResult {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("order", Value::from(self.order)),
            ("autoCommit", Value::from(self.auto_commit)),
            ("consumeResult", optional_string(&self.consume_result)),
            ("remark", optional_string(&self.remark)),
            ("spentTimeMills", Value::from(self.spent_time_mills)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<ConsumeMessageDirectlyResult> {
        expect_object(value, "ConsumeMessageDirectlyResult")?;
        Ok(ConsumeMessageDirectlyResult {
            order: jboolean(value, "order", false),
            // autoCommit defaults to true (turned off only when explicitly false)
            auto_commit: jboolean(value, "autoCommit", true),
            consume_result: jstring(value, "consumeResult"),
            remark: jstring(value, "remark"),
            spent_time_mills: jlong(value, "spentTimeMills", 0),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ConsumeMessageDirectlyResult> {
        ConsumeMessageDirectlyResult::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- 200151 batch acknowledge of POP messages

/// The offset set of one batch-ack entry, as the indices of the bits that are set, ascending.
///
/// Index `n` means "the message at `startOffset + n` of this queue is acknowledged". The wire form is
/// standard base64 of the little-endian byte packing (bit `n` sits at bit `n % 8` of byte `n / 8`), with
/// trailing zero bytes dropped -- so the empty set encodes as an empty string.
pub fn ack_bit_set_to_base64(bits: &[u32]) -> String {
    let highest = match bits.iter().max() {
        None => return String::new(),
        Some(v) => *v,
    };
    let mut bytes = vec![0_u8; (highest as usize / 8) + 1];
    for bit in bits {
        bytes[*bit as usize / 8] |= 1_u8 << (*bit % 8);
    }
    BASE64_STANDARD.encode(bytes)
}

/// Inverse of [`ack_bit_set_to_base64`]; missing / null / empty text reads back as the empty set.
pub fn ack_bit_set_from_base64(value: &Value, key: &str) -> Result<Vec<u32>> {
    let text = match jstring(value, key) {
        None => return Ok(Vec::new()),
        Some(s) => s,
    };
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let bytes = BASE64_STANDARD
        .decode(&text)
        .map_err(|e| Error::Decode(format!("{key} is not a base64 bit set: {e}")))?;
    let mut bits = Vec::new();
    for (index, byte) in bytes.iter().enumerate() {
        for position in 0..8 {
            if byte & (1_u8 << position) != 0 {
                bits.push((index * 8 + position) as u32);
            }
        }
    }
    Ok(bits)
}

/// One batch-ack entry: every acknowledged offset of a single `(retry, queueId, startOffset, popTime)` group.
///
/// All nine fields go on the wire under their compact spellings (`c`, `t`, `r`, `so`, `q`, `rq`, `pt`, `it`,
/// `b`); reading accepts the long names too, because a broker-side tool may have written those.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchAck {
    pub consumer_group: String,
    pub topic: String,
    /// The retry flag text ("0" / "1" / "2"), kept as a string exactly as the extra info carries it.
    pub retry: String,
    /// The ck-queue offset shared by the group; bit index 0 of [`Self::bit_set`] is this offset.
    pub start_offset: i64,
    pub queue_id: i32,
    pub revive_queue_id: i32,
    pub pop_time: i64,
    pub invisible_time: i64,
    /// Set bit indices, ascending.
    pub bit_set: Vec<u32>,
}

impl BatchAck {
    /// Marks one offset relative to [`Self::start_offset`]; a negative or duplicate index is rejected
    /// rather than silently dropped, because a wrong offset would ack somebody else's message.
    pub fn set_offset_bit(&mut self, queue_offset: i64) -> Result<()> {
        let bit = queue_offset
            .checked_sub(self.start_offset)
            .ok_or_else(|| Error::Encode("offset delta overflows".to_string()))?;
        if bit < 0 {
            return Err(Error::Encode(format!(
                "batch ack offset {queue_offset} is below the group start offset {}",
                self.start_offset
            )));
        }
        let bit = bit as u32;
        if !self.bit_set.contains(&bit) {
            self.bit_set.push(bit);
            self.bit_set.sort_unstable();
        }
        Ok(())
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("c", Value::String(self.consumer_group.clone())),
            ("t", Value::String(self.topic.clone())),
            ("r", Value::String(self.retry.clone())),
            ("so", Value::from(self.start_offset)),
            ("q", Value::from(self.queue_id)),
            ("rq", Value::from(self.revive_queue_id)),
            ("pt", Value::from(self.pop_time)),
            ("it", Value::from(self.invisible_time)),
            ("b", Value::String(ack_bit_set_to_base64(&self.bit_set))),
        ])
    }

    /// `compact` / `long` pair lookup: the compact spelling wins when both are present.
    fn text(value: &Value, compact: &str, long: &str) -> String {
        jstring(value, compact)
            .or_else(|| jstring(value, long))
            .unwrap_or_default()
    }

    fn number(value: &Value, compact: &str, long: &str) -> i64 {
        jlong(value, compact, jlong(value, long, 0))
    }

    pub fn from_json_value(value: &Value) -> Result<BatchAck> {
        expect_object(value, "BatchAck")?;
        Ok(BatchAck {
            consumer_group: Self::text(value, "c", "consumerGroup"),
            topic: Self::text(value, "t", "topic"),
            retry: Self::text(value, "r", "retry"),
            start_offset: Self::number(value, "so", "startOffset"),
            queue_id: Self::number(value, "q", "queueId") as i32,
            revive_queue_id: Self::number(value, "rq", "reviveQueueId") as i32,
            pop_time: Self::number(value, "pt", "popTime"),
            invisible_time: Self::number(value, "it", "invisibleTime"),
            // One lookup only: a corrupt compact value must not be masked by an absent long-name twin.
            bit_set: ack_bit_set_from_base64(
                value,
                if jfield(value, "b").is_some() {
                    "b"
                } else {
                    "bitSet"
                },
            )?,
        })
    }
}

/// Body of BATCH_ACK_MESSAGE: the broker to talk to plus one entry per acknowledged offset group.
///
/// Built from a list of extra-info strings: the grouping key is `retry@queueId@ckQueueOffset@popTime`, and
/// the broker name is the one of the **first** string -- a later string may name another broker, but one
/// request can only ever address a single one, so its name is not consulted again.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchAckMessageRequestBody {
    pub broker_name: Option<String>,
    pub acks: Vec<BatchAck>,
}

impl BatchAckMessageRequestBody {
    /// Groups `extra_info_list` into entries; see [`BatchAck`] for the field meanings.
    ///
    /// The first extra info of a group fixes that group's fields; a later extra info of the same group only
    /// contributes its bit index. An empty list yields an empty body (no broker name, no acks).
    pub fn from_extra_info_list(
        consumer_group: &str,
        topic: &str,
        extra_info_list: &[&str],
    ) -> Result<BatchAckMessageRequestBody> {
        let mut broker_name: Option<String> = None;
        // Insertion order is kept: the wire array follows the order the groups first appear in.
        let mut groups: Vec<(String, usize)> = Vec::new();
        let mut acks: Vec<BatchAck> = Vec::new();
        for info in extra_info_list {
            let segments = extra_info::split(info)?;
            if broker_name.is_none() {
                broker_name = Some(extra_info::get_broker_name(&segments)?);
            }
            let retry = extra_info::get_retry(&segments)?;
            let queue_id = extra_info::get_queue_id(&segments)?;
            let start_offset = extra_info::get_ck_queue_offset(&segments)?;
            let pop_time = extra_info::get_pop_time(&segments)?;
            let queue_offset = extra_info::get_queue_offset(&segments)?;
            let key = format!("{retry}@{queue_id}@{start_offset}@{pop_time}");
            let index = match groups.iter().position(|(k, _)| *k == key) {
                Some(index) => groups[index].1,
                None => {
                    acks.push(BatchAck {
                        consumer_group: consumer_group.to_string(),
                        topic: topic.to_string(),
                        retry,
                        start_offset,
                        queue_id,
                        revive_queue_id: extra_info::get_revive_qid(&segments)?,
                        pop_time,
                        invisible_time: extra_info::get_invisible_time(&segments)?,
                        bit_set: Vec::new(),
                    });
                    groups.push((key, acks.len() - 1));
                    acks.len() - 1
                }
            };
            acks[index].set_offset_bit(queue_offset)?;
        }
        Ok(BatchAckMessageRequestBody { broker_name, acks })
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("brokerName", optional_string(&self.broker_name)),
            (
                "acks",
                Value::Array(self.acks.iter().map(|a| a.to_json_value()).collect()),
            ),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<BatchAckMessageRequestBody> {
        expect_object(value, "BatchAckMessageRequestBody")?;
        let mut acks = Vec::new();
        for item in jarray(value, "acks")? {
            acks.push(BatchAck::from_json_value(item)?);
        }
        Ok(BatchAckMessageRequestBody {
            broker_name: jstring(value, "brokerName"),
            acks,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<BatchAckMessageRequestBody> {
        BatchAckMessageRequestBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Turn compact JSON text into a `Value` (used to build fixtures for "pass-through" fields).
    fn py(text: &str) -> Value {
        RemotingSerializable::decode(text.as_bytes()).unwrap()
    }

    fn json_of(value: &Value) -> String {
        RemotingSerializable::to_json_string(value)
    }

    #[test]
    fn kv_table_golden_json_and_round_trip() {
        let mut table = StringMap::new();
        table.insert("DefaultCluster", "127.0.0.1:10911;127.0.0.1:10912");
        table.insert("broker-a", "127.0.0.1:10911");
        let kv = KVTable { table };
        // Wire-recorded message: {"table": {"DefaultCluster": "...", "broker-a": "..."}}
        assert_eq!(
            json_of(&kv.to_json_value()),
            r#"{"table":{"DefaultCluster":"127.0.0.1:10911;127.0.0.1:10912","broker-a":"127.0.0.1:10911"}}"#
        );
        assert_eq!(KVTable::decode(&kv.encode()).unwrap(), kv);
        // Missing key / null table -> empty map
        assert_eq!(
            KVTable::decode(br#"{"table":null}"#).unwrap().table.len(),
            0
        );
        assert_eq!(KVTable::decode(b"{}").unwrap(), KVTable::default());
    }

    #[test]
    fn topic_list_drops_broker_addr_key_when_none() {
        let mut tl = TopicList::new();
        tl.topic_list = vec![
            "BodyGoldenTopic".into(),
            "AnotherTopic".into(),
            "TBW102".into(),
        ];
        tl.broker_addr = Some("127.0.0.1:10911".into());
        assert_eq!(
            json_of(&tl.to_json_value()),
            r#"{"topicList":["BodyGoldenTopic","AnotherTopic","TBW102"],"brokerAddr":"127.0.0.1:10911"}"#
        );
        assert_eq!(TopicList::decode(&tl.encode()).unwrap(), tl);
        assert_eq!(tl.get_topic_list(), tl.topic_list);

        let bare = TopicList {
            topic_list: vec!["BodyGoldenTopic".into()],
            broker_addr: None,
        };
        assert_eq!(
            json_of(&bare.to_json_value()),
            r#"{"topicList":["BodyGoldenTopic"]}"#
        );
        assert!(bare.to_json_value().get("brokerAddr").is_none());
        assert_eq!(TopicList::decode(&bare.encode()).unwrap(), bare);
        // An empty-string address is "a value"; the key must stay
        let empty_addr = TopicList {
            topic_list: Vec::new(),
            broker_addr: Some(String::new()),
        };
        assert_eq!(
            json_of(&empty_addr.to_json_value()),
            r#"{"topicList":[],"brokerAddr":""}"#
        );
    }

    #[test]
    fn lock_and_unlock_batch_bodies_use_wire_field_order() {
        let body = LockBatchRequestBody {
            consumer_group: Some("GID_BodyGolden".into()),
            client_id: Some("client-1@127.0.0.1".into()),
            mq_set: vec![
                MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0),
                MessageQueueKey::new("BodyGoldenTopic", "broker-a", 1),
            ],
        };
        assert_eq!(
            json_of(&body.to_json_value()),
            concat!(
                r#"{"consumerGroup":"GID_BodyGolden","clientId":"client-1@127.0.0.1","mqSet":"#,
                r#"[{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":0},"#,
                r#"{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":1}]}"#
            )
        );
        assert_eq!(LockBatchRequestBody::decode(&body.encode()).unwrap(), body);
        // Wire-recorded message: null fields are written out too, the three keys are always present
        assert_eq!(
            json_of(&LockBatchRequestBody::default().to_json_value()),
            r#"{"consumerGroup":null,"clientId":null,"mqSet":[]}"#
        );

        let unlock = UnlockBatchRequestBody {
            consumer_group: Some("GID_BodyGolden".into()),
            client_id: Some("client-1@127.0.0.1".into()),
            mq_set: vec![MessageQueueKey::new("BodyGoldenTopic", "broker-a", 7)],
        };
        assert_eq!(
            json_of(&unlock.to_json_value()),
            concat!(
                r#"{"consumerGroup":"GID_BodyGolden","clientId":"client-1@127.0.0.1","mqSet":"#,
                r#"[{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":7}]}"#
            )
        );
        assert_eq!(
            UnlockBatchRequestBody::decode(&unlock.encode()).unwrap(),
            unlock
        );
    }

    #[test]
    fn lock_batch_response_body_accepts_string_queue_id() {
        // brokers write queueId as a numeric string; reading must tolerate it
        let text = r#"{"lockOKMQSet":[{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":"2"}]}"#;
        let body = LockBatchResponseBody::decode(text.as_bytes()).unwrap();
        assert_eq!(
            body.lock_ok_mq_set,
            vec![MessageQueueKey::new("BodyGoldenTopic", "broker-a", 2)]
        );
        assert_eq!(
            json_of(&body.to_json_value()),
            r#"{"lockOKMQSet":[{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":2}]}"#
        );
        assert_eq!(
            LockBatchResponseBody::decode(b"{}").unwrap(),
            LockBatchResponseBody::default()
        );
        assert!(matches!(
            LockBatchResponseBody::decode(br#"{"lockOKMQSet":["broker-a"]}"#),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn consumer_id_list_and_time_span_and_stats_list_golden() {
        let body = GetConsumerListByGroupResponseBody {
            consumer_id_list: vec!["client-1@127.0.0.1".into(), "client-2@127.0.0.2".into()],
        };
        assert_eq!(
            json_of(&body.to_json_value()),
            r#"{"consumerIdList":["client-1@127.0.0.1","client-2@127.0.0.2"]}"#
        );
        assert_eq!(
            GetConsumerListByGroupResponseBody::decode(&body.encode()).unwrap(),
            body
        );

        let span = QueryConsumeTimeSpanBody {
            consume_time_span_set: vec![py(
                r#"{"topic":"BodyGoldenTopic","consumerGroup":"GID_BodyGolden","consumeTimeStamp":1700000001000,"latency":23}"#,
            )],
        };
        assert_eq!(
            json_of(&span.to_json_value()),
            concat!(
                r#"{"consumeTimeSpanSet":[{"topic":"BodyGoldenTopic","#,
                r#""consumerGroup":"GID_BodyGolden","consumeTimeStamp":1700000001000,"latency":23}]}"#
            )
        );
        assert_eq!(
            QueryConsumeTimeSpanBody::decode(&span.encode()).unwrap(),
            span
        );

        let stats = ConsumeStatsList {
            stats_list: vec![py(r#"{"offsetTable":{},"consumeTps":1.5}"#)],
            broker_addr: Some("127.0.0.1:10911".into()),
            total_diff: 7,
            total_inflight_diff: 2,
        };
        assert_eq!(
            json_of(&stats.to_json_value()),
            concat!(
                r#"{"consumeStatsList":[{"offsetTable":{},"consumeTps":1.5}],"#,
                r#""brokerAddr":"127.0.0.1:10911","totalDiff":7,"totalInflightDiff":2}"#
            )
        );
        assert_eq!(ConsumeStatsList::decode(&stats.encode()).unwrap(), stats);
        assert_eq!(
            json_of(&ConsumeStatsList::new().to_json_value()),
            r#"{"consumeStatsList":[],"totalDiff":0,"totalInflightDiff":0}"#
        );
    }

    #[test]
    fn cluster_info_rewraps_broker_addr_table() {
        let cluster = ClusterInfo {
            broker_addr_table: vec![
                (
                    "broker-b".into(),
                    vec![(1, "127.0.0.1:10921".into()), (0, "127.0.0.1:10920".into())],
                ),
                (
                    "broker-a".into(),
                    vec![(0, "127.0.0.1:10911".into()), (1, "127.0.0.1:10912".into())],
                ),
            ],
            cluster_addr_table: vec![(
                "DefaultCluster".into(),
                vec!["broker-a".into(), "broker-b".into()],
            )],
        };
        // Messages are wrapped into BrokerData shape in insertion order, cluster / enableActingMaster are always the defaults
        assert_eq!(
            json_of(&cluster.to_json_value()),
            r#"{"brokerAddrTable":{"broker-b":{"cluster":"","brokerName":"broker-b","brokerAddrs":{"1":"127.0.0.1:10921","0":"127.0.0.1:10920"},"enableActingMaster":false},"broker-a":{"cluster":"","brokerName":"broker-a","brokerAddrs":{"0":"127.0.0.1:10911","1":"127.0.0.1:10912"},"enableActingMaster":false}},"clusterAddrTable":{"DefaultCluster":["broker-a","broker-b"]}}"#
        );
        // getBrokerAddrs sorted, deduplicated output (order pinned to the wire form)
        assert_eq!(
            cluster.get_broker_addrs(),
            vec![
                "127.0.0.1:10911",
                "127.0.0.1:10912",
                "127.0.0.1:10920",
                "127.0.0.1:10921"
            ]
        );
        assert_eq!(
            ClusterInfo::default().get_broker_addrs(),
            Vec::<String>::new()
        );

        // Real nameserver message: bare numeric keys + duplicate addresses, sorted by name / id after reading back
        let wire = r#"{"brokerAddrTable":{"broker-a":{"cluster":"DefaultCluster","brokerName":"broker-a","brokerAddrs":{1:"127.0.0.1:10911",0:"127.0.0.1:10911"},"enableActingMaster":true}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}"#;
        let back = ClusterInfo::decode(wire.as_bytes()).unwrap();
        assert_eq!(back.broker_addr_table.len(), 1);
        assert_eq!(back.broker_addr_table[0].0, "broker-a");
        assert_eq!(
            back.broker_addr_table[0].1,
            vec![
                (1, "127.0.0.1:10911".to_string()),
                (0, "127.0.0.1:10911".to_string())
            ]
        );
        assert_eq!(back.cluster_addr_table[0].1, vec!["broker-a".to_string()]);
        assert_eq!(back.get_broker_addrs(), vec!["127.0.0.1:10911".to_string()]);

        assert!(matches!(
            ClusterInfo::decode(
                br#"{"brokerAddrTable":{"broker-a":{"brokerAddrs":{"x":"1.2.3.4:10911"}}}}"#
            ),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            ClusterInfo::decode(br#"{"brokerAddrTable":{"broker-a":"1.2.3.4:10911"}}"#),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            ClusterInfo::decode(br#"{"clusterAddrTable":{"c1":"broker-a"}}"#),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn consumer_running_info_message_queue_inline_keys() {
        let mut info = ConsumerRunningInfo::default();
        info.properties
            .insert(ConsumerRunningInfo::PROP_NAMESERVER_ADDR, "127.0.0.1:9876;");
        info.properties
            .insert(ConsumerRunningInfo::PROP_CONSUME_ORDERLY, "false");
        info.properties.insert(
            ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP,
            "1700000000000",
        );
        info.subscription_set = vec![py(
            r#"{"classFilterMode":false,"topic":"BodyGoldenTopic","subString":"*","tagsSet":["TagA"],"codeSet":[],"subVersion":1789712527657,"expressionType":"TAG"}"#,
        )];
        info.mq_table = vec![(
            MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0),
            ProcessQueueInfo {
                commit_offset: 99,
                cached_msg_count: 3,
                ..Default::default()
            }
            .to_json_value(),
        )];
        info.mq_pop_table = vec![(
            MessageQueueKey::new("BodyGoldenTopic", "broker-a", 1),
            py(r#"{"commitOffset":5}"#),
        )];
        info.status_table = vec![(
            "BodyGoldenTopic".into(),
            ConsumeStatus::default().to_json_value(),
        )];
        info.user_consumer_info.insert("key", "value");

        // Inline object keys: written out in fastjson2 form, read back as the same MessageQueueKey
        let json = info.to_json_value();
        let key = json["mqTable"]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone();
        assert_eq!(
            key,
            r#"{"brokerName":"broker-a","queueId":0,"topic":"BodyGoldenTopic"}"#
        );
        assert_eq!(
            parse_message_queue_key(&key),
            Some(MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0))
        );
        let back = ConsumerRunningInfo::decode(&info.encode()).unwrap();
        assert_eq!(back, info);
        assert_eq!(
            back.properties
                .get(ConsumerRunningInfo::PROP_CONSUME_ORDERLY),
            Some("false")
        );
        assert_eq!(
            back.mq_table[0].0,
            MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0)
        );
        assert_eq!(back.mq_table[0].1["commitOffset"], 99);
        assert_eq!(back.mq_pop_table[0].0.queue_id, 1);
        assert_eq!(
            back.consume_status("BodyGoldenTopic").unwrap().unwrap(),
            ConsumeStatus::default()
        );
        assert!(back.consume_status("Missing").unwrap().is_none());
        assert!(back.jstack.is_none());
        // When jstack has a value it lands on the last key as well
        let with_stack = ConsumerRunningInfo {
            jstack: Some("thread dump".into()),
            ..Default::default()
        };
        assert!(json_of(&with_stack.to_json_value()).ends_with(r#","jstack":"thread dump"}"#));

        // Wire message of an empty object: all 7 keys present, jstack lands as null
        assert_eq!(
            json_of(&ConsumerRunningInfo::default().to_json_value()),
            concat!(
                r#"{"properties":{},"subscriptionSet":[],"mqTable":{},"mqPopTable":{},"#,
                r#""statusTable":{},"userConsumerInfo":{},"jstack":null}"#
            )
        );
    }

    #[test]
    fn consumer_running_info_accepts_server_inline_object_keys() {
        // fastjson2 real-machine output (invalid JSON); this crate's lenient parser must read it
        let wire = concat!(
            r#"{"properties":{"PROP_CONSUME_TYPE":"CONSUME_PASSIVELY"},"subscriptionSet":[],"#,
            r#""mqTable":{{"brokerName":"broker-a","queueId":0,"topic":"BodyGoldenTopic"}:"#,
            r#"{"commitOffset":42,"cachedMsgCount":1,"droped":false}},"#,
            r#""statusTable":{"BodyGoldenTopic":{"pullRT":1.5,"consumeFailedMsgs":2}}}"#
        );
        let info = ConsumerRunningInfo::decode(wire.as_bytes()).unwrap();
        assert_eq!(info.mq_table.len(), 1);
        assert_eq!(
            info.mq_table[0].0,
            MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0)
        );
        assert_eq!(info.mq_table[0].1["commitOffset"], 42);
        assert_eq!(
            ProcessQueueInfo::from_json_value(&info.mq_table[0].1)
                .unwrap()
                .commit_offset,
            42
        );
        let status = info.consume_status("BodyGoldenTopic").unwrap().unwrap();
        assert_eq!(status.pull_rt, 1.5);
        assert_eq!(status.consume_failed_msgs, 2);
        // After re-encoding, the key text is still the same inline object
        let again = info.to_json_value();
        assert!(again["mqTable"]
            .as_object()
            .unwrap()
            .keys()
            .any(|k| *k == message_queue_key(&info.mq_table[0].0)));
    }

    #[test]
    fn connection_and_consumer_connection_golden() {
        let conn = Connection {
            client_id: Some("client-1@127.0.0.1".into()),
            client_addr: Some("/127.0.0.1:52100".into()),
            language: Some("JAVA".into()),
            version: Some(40),
        };
        let cc = ConsumerConnection {
            connection_set: vec![conn.clone()],
            subscription_table: vec![(
                "BodyGoldenTopic".into(),
                py(
                    r#"{"classFilterMode":false,"topic":"BodyGoldenTopic","subString":"*","tagsSet":["TagA"],"codeSet":[],"subVersion":1789712527657,"expressionType":"TAG"}"#,
                ),
            )],
            consume_type: Some("CONSUME_PASSIVELY".into()),
            message_model: Some("CLUSTERING".into()),
            consume_from_where: Some("CONSUME_FROM_LAST_OFFSET".into()),
        };
        assert_eq!(
            json_of(&cc.to_json_value()),
            r#"{"connectionSet":[{"clientId":"client-1@127.0.0.1","clientAddr":"/127.0.0.1:52100","language":"JAVA","version":40}],"subscriptionTable":{"BodyGoldenTopic":{"classFilterMode":false,"topic":"BodyGoldenTopic","subString":"*","tagsSet":["TagA"],"codeSet":[],"subVersion":1789712527657,"expressionType":"TAG"}},"consumeType":"CONSUME_PASSIVELY","messageModel":"CLUSTERING","consumeFromWhere":"CONSUME_FROM_LAST_OFFSET"}"#
        );
        assert_eq!(ConsumerConnection::decode(&cc.encode()).unwrap(), cc);
        // Empty connection: the three enum keys remain with null values (keys written unconditionally)
        assert_eq!(
            json_of(&ConsumerConnection::default().to_json_value()),
            r#"{"connectionSet":[],"subscriptionTable":{},"consumeType":null,"messageModel":null,"consumeFromWhere":null}"#
        );
        let pc = ProducerConnection {
            connection_set: vec![conn.clone()],
        };
        assert_eq!(
            json_of(&pc.to_json_value()),
            concat!(
                r#"{"connectionSet":[{"clientId":"client-1@127.0.0.1","#,
                r#""clientAddr":"/127.0.0.1:52100","language":"JAVA","version":40}]}"#
            )
        );
        assert_eq!(ProducerConnection::decode(&pc.encode()).unwrap(), pc);
        // Missing version -> None (distinct from the broker really giving 0)
        let bare = Connection::from_json_value(&py(r#"{"clientId":"c"}"#)).unwrap();
        assert_eq!(bare.client_id.as_deref(), Some("c"));
        assert_eq!(bare.version, None);
        assert_eq!(
            json_of(&bare.to_json_value()),
            r#"{"clientId":"c","clientAddr":null,"language":null,"version":null}"#
        );
        assert_eq!(
            Connection::from_json_value(&py(r#"{"version":"40"}"#))
                .unwrap()
                .version,
            Some(40)
        );
    }

    #[test]
    fn reset_offset_body_round_trips_inline_keys_and_server_wire() {
        let body = ResetOffsetBody {
            offset_table: vec![
                (MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0), 10),
                (MessageQueueKey::new("BodyGoldenTopic", "broker-a", 1), 20),
            ],
        };
        // Keys encode() writes are quoted escaped strings (equivalent to fastjson2's bare inline form)
        assert_eq!(
            json_of(&body.to_json_value()),
            r#"{"offsetTable":{"{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"BodyGoldenTopic\"}":10,"{\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"BodyGoldenTopic\"}":20}}"#
        );
        assert_eq!(ResetOffsetBody::decode(&body.encode()).unwrap(), body);
        let wire = concat!(
            r#"{"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"BodyGoldenTopic"}:10,"#,
            r#"{"brokerName":"broker-a","queueId":1,"topic":"BodyGoldenTopic"}:"20"}}"#
        );
        assert_eq!(ResetOffsetBody::decode(wire.as_bytes()).unwrap(), body);
        assert_eq!(
            GetConsumerStatusBody::default().to_json_value(),
            py(r#"{"messageQueueTable":{},"consumerTable":{}}"#)
        );
    }

    #[test]
    fn reset_offset_body_for_c_array_round_trips_and_server_wire() {
        let body = ResetOffsetBodyForC {
            offset_table: vec![
                MessageQueueForC {
                    topic: "BodyGoldenTopic".to_string(),
                    broker_name: "broker-a".to_string(),
                    queue_id: 0,
                    offset: 10,
                },
                MessageQueueForC {
                    topic: "BodyGoldenTopic".to_string(),
                    broker_name: "broker-a".to_string(),
                    queue_id: 1,
                    offset: 20,
                },
            ],
        };
        assert_eq!(
            json_of(&body.to_json_value()),
            r#"{"offsetTable":[{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":0,"offset":10},{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":1,"offset":20}]}"#
        );
        assert_eq!(ResetOffsetBodyForC::decode(&body.encode()).unwrap(), body);
        let wire = concat!(
            r#"{"offsetTable":[{"offset":10,"brokerName":"broker-a","queueId":0,"topic":"BodyGoldenTopic"},"#,
            r#"{"topic":"BodyGoldenTopic","queueId":1,"brokerName":"broker-a","offset":20}]}"#
        );
        assert_eq!(ResetOffsetBodyForC::decode(wire.as_bytes()).unwrap(), body);
        // The two shapes never accept each other: feeding a map body to ForC only errors (and vice versa, see
        // malformed_bodies_return_decode_errors)
        assert!(matches!(
            ResetOffsetBodyForC::decode(br#"{"offsetTable":{{"brokerName":"b","queueId":0,"topic":"t"}:1}}"#),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn get_consumer_status_body_nested_maps() {
        let body = GetConsumerStatusBody {
            message_queue_table: vec![
                (MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0), 11),
                (MessageQueueKey::new("BodyGoldenTopic", "broker-a", 1), 12),
            ],
            consumer_table: vec![(
                "client-1@127.0.0.1".into(),
                vec![(MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0), 42)],
            )],
        };
        let text = json_of(&body.to_json_value());
        assert_eq!(
            text,
            r#"{"messageQueueTable":{"{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"BodyGoldenTopic\"}":11,"{\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"BodyGoldenTopic\"}":12},"consumerTable":{"client-1@127.0.0.1":{"{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"BodyGoldenTopic\"}":42}}}"#
        );
        assert_eq!(
            GetConsumerStatusBody::decode(text.as_bytes()).unwrap(),
            body
        );
        let wire = concat!(
            r#"{"messageQueueTable":{{"brokerName":"b","queueId":0,"topic":"T"}:7},"#,
            r#""consumerTable":{"c1":{{"brokerName":"b","queueId":1,"topic":"T"}:8}}}"#
        );
        let back = GetConsumerStatusBody::decode(wire.as_bytes()).unwrap();
        assert_eq!(back.message_queue_table[0].1, 7);
        assert_eq!(back.consumer_table[0].0, "c1");
        assert_eq!(back.consumer_table[0].1[0].0.queue_id, 1);
        assert_eq!(back.consumer_table[0].1[0].1, 8);
    }

    #[test]
    fn process_queue_info_golden_key_order_and_droped_spelling() {
        let info = ProcessQueueInfo {
            commit_offset: 1000,
            cached_msg_min_offset: 1001,
            cached_msg_max_offset: 1009,
            cached_msg_count: 9,
            cached_msg_size_in_mib: 4,
            transaction_msg_min_offset: 5,
            transaction_msg_max_offset: 7,
            transaction_msg_count: 2,
            locked: true,
            try_unlock_times: 1,
            last_lock_timestamp: 1700000000123,
            droped: true,
            last_pull_timestamp: 1700000000456,
            last_consume_timestamp: 1700000000789,
        };
        assert_eq!(
            json_of(&info.to_json_value()),
            concat!(
                r#"{"commitOffset":1000,"cachedMsgMinOffset":1001,"cachedMsgMaxOffset":1009,"#,
                r#""cachedMsgCount":9,"cachedMsgSizeInMiB":4,"transactionMsgMinOffset":5,"#,
                r#""transactionMsgMaxOffset":7,"transactionMsgCount":2,"locked":true,"#,
                r#""tryUnlockTimes":1,"lastLockTimestamp":1700000000123,"droped":true,"#,
                r#""lastPullTimestamp":1700000000456,"lastConsumeTimestamp":1700000000789}"#
            )
        );
        assert_eq!(ProcessQueueInfo::decode(&info.encode()).unwrap(), info);
        // Zero-value message (straight from serialization): the 14 keys are always present
        assert_eq!(
            json_of(&ProcessQueueInfo::default().to_json_value()),
            concat!(
                r#"{"commitOffset":0,"cachedMsgMinOffset":0,"cachedMsgMaxOffset":0,"#,
                r#""cachedMsgCount":0,"cachedMsgSizeInMiB":0,"transactionMsgMinOffset":0,"#,
                r#""transactionMsgMaxOffset":0,"transactionMsgCount":0,"locked":false,"#,
                r#""tryUnlockTimes":0,"lastLockTimestamp":0,"droped":false,"#,
                r#""lastPullTimestamp":0,"lastConsumeTimestamp":0}"#
            )
        );
        // Missing keys / dirty values fall back to defaults, never panic (same rule as jlong / jboolean)
        let partial = ProcessQueueInfo::decode(br#"{"commitOffset":"8","locked":"true"}"#).unwrap();
        assert_eq!(partial.commit_offset, 8);
        assert!(partial.locked);
        assert!(!partial.droped);
        assert_eq!(
            ProcessQueueInfo::decode(br#"{"commitOffset":{}}"#)
                .unwrap()
                .commit_offset,
            0
        );
    }

    #[test]
    fn consume_status_floats_and_cm_result_names() {
        let cs = ConsumeStatus {
            pull_rt: 12.5,
            pull_tps: 100.0,
            consume_rt: 3.25,
            consume_ok_tps: 99.5,
            consume_failed_tps: 0.5,
            consume_failed_msgs: 7,
        };
        // Wire-recorded message: {"pullRT": 12.5, "pullTPS": 100.0, "consumeRT": 3.25,
        //                  "consumeOKTPS": 99.5, "consumeFailedTPS": 0.5, "consumeFailedMsgs": 7}
        assert_eq!(
            json_of(&cs.to_json_value()),
            r#"{"pullRT":12.5,"pullTPS":100.0,"consumeRT":3.25,"consumeOKTPS":99.5,"consumeFailedTPS":0.5,"consumeFailedMsgs":7}"#
        );
        assert_eq!(
            ConsumeStatus::from_json_value(&cs.to_json_value()).unwrap(),
            cs
        );
        // Zero values of float fields are written as Double `0.0` per fastjson2, not integer `0`
        assert_eq!(
            json_of(&ConsumeStatus::default().to_json_value()),
            r#"{"pullRT":0.0,"pullTPS":0.0,"consumeRT":0.0,"consumeOKTPS":0.0,"consumeFailedTPS":0.0,"consumeFailedMsgs":0}"#
        );
        assert_eq!(
            ConsumeStatus::from_json_value(&py(
                r#"{"pullRT":0,"pullTPS":0,"consumeFailedMsgs":0}"#
            ))
            .unwrap()
            .pull_rt,
            0.0
        );
        assert_eq!(CMResult::CR_SUCCESS, "CR_SUCCESS");
        assert_eq!(CMResult::CR_THROW_EXCEPTION, "CR_THROW_EXCEPTION");
        assert_eq!(CMResult::CR_RETURN_NULL, "CR_RETURN_NULL");
    }

    #[test]
    fn consume_message_directly_result_defaults_auto_commit_true() {
        let r = ConsumeMessageDirectlyResult {
            order: true,
            auto_commit: true,
            consume_result: Some(CMResult::CR_SUCCESS.to_string()),
            remark: Some("ok".into()),
            spent_time_mills: 5,
        };
        assert_eq!(
            json_of(&r.to_json_value()),
            r#"{"order":true,"autoCommit":true,"consumeResult":"CR_SUCCESS","remark":"ok","spentTimeMills":5}"#
        );
        assert_eq!(
            ConsumeMessageDirectlyResult::decode(&r.encode()).unwrap(),
            r
        );
        // Wire message of an empty result: autoCommit starts as true
        assert_eq!(
            json_of(&ConsumeMessageDirectlyResult::default().to_json_value()),
            r#"{"order":false,"autoCommit":true,"consumeResult":null,"remark":null,"spentTimeMills":0}"#
        );
        let later = ConsumeMessageDirectlyResult::decode(
            br#"{"consumeResult":"CR_LATER","autoCommit":false}"#,
        )
        .unwrap();
        assert_eq!(later.consume_result.as_deref(), Some(CMResult::CR_LATER));
        assert!(!later.auto_commit);
        assert!(!later.order);
    }

    #[test]
    fn message_queue_value_text_is_server_declaration_order() {
        let mq = MessageQueueKey::new("BodyGoldenTopic", "broker-a", 3);
        assert_eq!(
            json_of(&message_queue_value(&mq)),
            r#"{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":3}"#
        );
        // Deliberately different from the alphabetical order of inline object keys
        assert_eq!(
            message_queue_key(&mq),
            r#"{"brokerName":"broker-a","queueId":3,"topic":"BodyGoldenTopic"}"#
        );
    }

    #[test]
    fn malformed_bodies_return_decode_errors() {
        assert!(matches!(TopicList::decode(b""), Err(Error::Decode(_))));
        assert!(matches!(
            TopicList::decode(br#"{"topicList":5}"#),
            Err(Error::Decode(_))
        ));
        assert!(matches!(KVTable::decode(b"[1,2]"), Err(Error::Decode(_))));
        assert!(matches!(
            ResetOffsetBody::decode(br#"{"offsetTable":[1]}"#),
            Err(Error::Decode(_))
        ));
        assert_eq!(
            ResetOffsetBodyForC::decode(br#"{"offsetTable":[1,{"topic":"t"}]}"#)
                .unwrap()
                .offset_table,
            vec![MessageQueueForC {
                topic: "t".to_string(),
                broker_name: String::new(),
                queue_id: 0,
                offset: 0,
            }]
        );
        assert!(matches!(
            ResetOffsetBodyForC::decode(br#"{"offsetTable":{"a":1}}"#),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            ConsumerConnection::decode(br#"{"connectionSet":[1]}"#),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            ConsumeStatsList::decode(br#"{"consumeStatsList":{}}"#),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            ConsumerRunningInfo::decode(br#"{"mqTable":[1]}"#),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            ConsumerRunningInfo::decode(br#"{"statusTable":5}"#),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            ConsumeStatus::from_json_value(&Value::Bool(true)),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            QueryConsumeTimeSpanBody::decode(br#"{"consumeTimeSpanSet":1}"#),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            GetConsumerListByGroupResponseBody::decode(br#"{"consumerIdList":"a"}"#),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            GetConsumerStatusBody::decode(br#"{"consumerTable":{"c1":[1]}}"#),
            Err(Error::Decode(_))
        ));
    }

    // ---------------- BATCH_ACK_MESSAGE body ----------------

    /// Extra infos shaped exactly as the POP path writes them: `ck popTime invisible reviveQid retry broker queueId msgOffset`.
    const CK_QUEUE3_BASE: &str = "100 1700000000000 60000 0 0 broker-a 3 100";
    /// Same group as `CK_QUEUE3_BASE`, two offsets further into the queue.
    const CK_QUEUE3_PLUS2: &str = "100 1700000000000 60000 0 0 broker-a 3 102";
    /// A second group: same popTime, different queue id.
    const CK_QUEUE4: &str = "7 1700000000000 60000 3 0 broker-a 4 7";
    /// A retry-topic ticket (retry flag 1) of queue 3.
    const CK_QUEUE3_RETRY: &str = "100 1700000000000 60000 0 1 broker-a 3 101";

    fn group_of(infos: &[&str]) -> BatchAckMessageRequestBody {
        BatchAckMessageRequestBody::from_extra_info_list("GID_Batch", "BatchTopic", infos)
            .expect("grouping of a well-formed extra info list")
    }

    #[test]
    fn batch_ack_body_encodes_compact_keys_in_wire_order() {
        let body = group_of(&[CK_QUEUE3_BASE]);
        assert_eq!(
            json_of(&body.to_json_value()),
            r#"{"brokerName":"broker-a","acks":[{"c":"GID_Batch","t":"BatchTopic","r":"0","so":100,"q":3,"rq":0,"pt":1700000000000,"it":60000,"b":"AQ=="}]}"#
        );
        // The long spellings must not leak into the request this side sends.
        let text = json_of(&body.to_json_value());
        for long in [
            "\"consumerGroup\"",
            "\"queueId\"",
            "\"startOffset\"",
            "\"bitSet\"",
        ] {
            assert!(!text.contains(long), "{text} leaked {long}");
        }
    }

    #[test]
    fn batch_ack_body_reads_both_key_spellings() {
        let compact = BatchAckMessageRequestBody::decode(&group_of(&[CK_QUEUE3_BASE]).encode())
            .expect("compact body decodes");
        let long = BatchAckMessageRequestBody::from_json_value(
            &py(r#"{"brokerName":"broker-a","acks":[{"consumerGroup":"GID_Batch","topic":"BatchTopic","retry":"0","startOffset":100,"queueId":3,"reviveQueueId":0,"popTime":1700000000000,"invisibleTime":60000,"bitSet":"AQ=="}]}"#),
        )
        .expect("long-name body decodes");
        assert_eq!(compact, long);
        // A missing / null bit set reads back as the empty set, not as a decode failure.
        let no_bits = BatchAck::from_json_value(&py(
            r#"{"c":"G","t":"T","r":"0","so":1,"q":1,"rq":0,"pt":2,"it":3,"b":null}"#,
        ))
        .unwrap();
        assert!(no_bits.bit_set.is_empty());
        assert!(BatchAck::from_json_value(&py(r#"{"c":"G"}"#))
            .unwrap()
            .bit_set
            .is_empty());
    }

    #[test]
    fn batch_ack_groups_two_tickets_of_one_key_into_a_single_entry() {
        // Two messages of the same pop group: one entry carrying both bits.
        let body = group_of(&[CK_QUEUE3_BASE, CK_QUEUE3_PLUS2]);
        assert_eq!(body.acks.len(), 1, "one merge key means one entry");
        assert_eq!(body.acks[0].bit_set, vec![0, 2]);
        // The bit set is the base64 of the little-endian byte packing: bits 0 and 2 -> 0b101.
        assert_eq!(ack_bit_set_to_base64(&body.acks[0].bit_set), "BQ==");
        assert_eq!(
            ack_bit_set_from_base64(&py(r#"{"b":"BQ=="}"#), "b").unwrap(),
            vec![0, 2]
        );
    }

    #[test]
    fn batch_ack_keeps_distinct_merge_keys_apart() {
        // Control for the grouping rule: same popTime but a different queue id stays a separate entry.
        let body = group_of(&[CK_QUEUE3_BASE, CK_QUEUE4]);
        assert_eq!(body.acks.len(), 2, "two merge keys mean two entries");
        assert_eq!(body.acks[0].queue_id, 3);
        assert_eq!(body.acks[0].start_offset, 100);
        assert_eq!(body.acks[0].bit_set, vec![0]);
        assert_eq!(body.acks[1].queue_id, 4);
        assert_eq!(body.acks[1].start_offset, 7);
        assert_eq!(body.acks[1].revive_queue_id, 3);
        assert_eq!(body.acks[1].bit_set, vec![0]);
        // Entries follow the order the groups first appear in: queue 3 first, then queue 4.
        let acks = body.to_json_value()["acks"].as_array().unwrap().clone();
        assert_eq!(acks[0]["q"], Value::from(3));
        assert_eq!(acks[1]["q"], Value::from(4));
    }

    #[test]
    fn batch_ack_merge_key_covers_every_group_component() {
        // Each variant differs from CK_QUEUE3_BASE in exactly ONE component of the merge key, so a key
        // that ignored that component would silently merge two different pop groups.
        let only_retry = group_of(&[CK_QUEUE3_BASE, CK_QUEUE3_RETRY]);
        assert_eq!(only_retry.acks.len(), 2, "the retry flag belongs to the key");
        assert_eq!(only_retry.acks[0].retry, "0");
        assert_eq!(only_retry.acks[1].retry, "1");

        let only_queue = group_of(&[CK_QUEUE3_BASE, "100 1700000000000 60000 0 0 broker-a 4 100"]);
        assert_eq!(only_queue.acks.len(), 2, "the queue id belongs to the key");
        assert_eq!(only_queue.acks[0].queue_id, 3);
        assert_eq!(only_queue.acks[1].queue_id, 4);

        let only_start = group_of(&[CK_QUEUE3_BASE, "99 1700000000000 60000 0 0 broker-a 3 100"]);
        assert_eq!(only_start.acks.len(), 2, "the ck queue offset belongs to the key");
        assert_eq!(only_start.acks[1].start_offset, 99);
        assert_eq!(only_start.acks[1].bit_set, vec![1]);

        let only_pop = group_of(&[CK_QUEUE3_BASE, "100 1999000000000 60000 0 0 broker-a 3 100"]);
        assert_eq!(only_pop.acks.len(), 2, "the pop time belongs to the key");
        assert_eq!(only_pop.acks[1].pop_time, 1999000000000);

        // Control: a ticket matching all four components merges instead of opening a group.
        let merged = group_of(&[CK_QUEUE3_BASE, CK_QUEUE3_PLUS2]);
        assert_eq!(merged.acks.len(), 1);
        assert_eq!(merged.acks[0].bit_set, vec![0, 2]);
    }

    #[test]
    fn batch_ack_bit_index_is_the_offset_relative_to_the_group_start_offset() {
        // ckQueueOffset 100, message offset 130 -> bit 30, i.e. byte 3 with the low bit set.
        let body = group_of(&["100 1700000000000 60000 0 0 broker-a 3 130"]);
        assert_eq!(body.acks[0].bit_set, vec![30]);
        assert_eq!(ack_bit_set_to_base64(&body.acks[0].bit_set), "AAAAQA==");
        // Bit 64 opens a ninth byte: trailing zero bytes are kept only up to the highest set bit.
        assert_eq!(ack_bit_set_to_base64(&[64]), "AAAAAAAAAAAB");
        assert_eq!(
            ack_bit_set_from_base64(&py(r#"{"b":"AAAAAAAAAAAB"}"#), "b").unwrap(),
            vec![64]
        );
        // Repeated tickets of one group must not inflate the entry.
        let twice = group_of(&[CK_QUEUE3_BASE, CK_QUEUE3_BASE, CK_QUEUE3_PLUS2]);
        assert_eq!(twice.acks.len(), 1);
        assert_eq!(twice.acks[0].bit_set, vec![0, 2]);
    }

    #[test]
    fn batch_ack_bit_set_of_a_blank_group_encodes_as_empty_text() {
        let ack = BatchAck::default();
        assert_eq!(ack_bit_set_to_base64(&ack.bit_set), "");
        assert!(ack.to_json_value()["b"].is_string());
        // Round-trip of the empty set: empty text is the empty set, not a decode failure.
        let back = BatchAck::from_json_value(&ack.to_json_value()).unwrap();
        assert_eq!(back, ack);
        assert!(matches!(
            ack_bit_set_from_base64(&py(r#"{"b":"not base64 !!"}"#), "b"),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn batch_ack_broker_name_comes_from_the_first_ticket_only() {
        let body = group_of(&["100 1700000000000 60000 0 0 broker-a 3 100", CK_QUEUE4]);
        assert_eq!(body.broker_name.as_deref(), Some("broker-a"));
        // An empty list acks nothing and names no broker.
        let empty = group_of(&[]);
        assert_eq!(empty.broker_name, None);
        assert!(empty.acks.is_empty());
        assert_eq!(
            json_of(&empty.to_json_value()),
            r#"{"brokerName":null,"acks":[]}"#
        );
    }

    #[test]
    fn batch_ack_rejects_tickets_the_pop_layer_cannot_have_written() {
        // A 7-segment ticket has no message offset, so no bit can be computed.
        let err = BatchAckMessageRequestBody::from_extra_info_list(
            "GID_Batch",
            "BatchTopic",
            &["100 1700000000000 60000 0 0 broker-a 3"],
        )
        .expect_err("a ticket without the message offset cannot be grouped");
        assert!(err.to_string().contains("getQueueOffset"), "{err}");

        // A message offset below the group's start offset would address a negative bit.
        let err = group_err(&["100 1700000000000 60000 0 0 broker-a 3 99"]);
        assert!(
            err.to_string()
                .contains("batch ack offset 99 is below the group start offset 100"),
            "{err}"
        );
    }

    fn group_err(infos: &[&str]) -> Error {
        BatchAckMessageRequestBody::from_extra_info_list("GID_Batch", "BatchTopic", infos)
            .expect_err("grouping is expected to fail")
    }

    #[test]
    fn batch_ack_round_trips_through_the_wire_form() {
        let body = group_of(&[CK_QUEUE3_BASE, CK_QUEUE3_PLUS2, CK_QUEUE4, CK_QUEUE3_RETRY]);
        let back = BatchAckMessageRequestBody::decode(&body.encode()).unwrap();
        assert_eq!(back, body);
        assert_eq!(back.acks.len(), 3);
        // Missing or non-array acks, and a non-object item, are hard failures.
        assert!(BatchAckMessageRequestBody::decode(br#"{"acks":null}"#)
            .unwrap()
            .acks
            .is_empty());
        assert!(BatchAckMessageRequestBody::decode(br#"{"acks":"nope"}"#).is_err());
        assert!(BatchAckMessageRequestBody::decode(br#"{"acks":["nope"]}"#).is_err());
        assert!(BatchAckMessageRequestBody::decode(b"[1]").is_err());
    }
}
