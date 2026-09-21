//! 公共响应体（对应 `org.apache.rocketmq.remoting.protocol.body.*` 常用部分）。
//!
//! 移植 `python/rocketmq/remoting/protocol/body.py`（609 行）里的全部类：
//! `KVTable` / `TopicList` / `LockBatchRequestBody` / `LockBatchResponseBody` /
//! `UnlockBatchRequestBody` / `GetConsumerListByGroupResponseBody` / `ClusterInfo` /
//! `ConsumerRunningInfo` / `Connection` / `ConsumerConnection` / `ProducerConnection` /
//! `QueryConsumeTimeSpanBody` / `ConsumeStatus` / `ConsumeStatsList` /
//! `ResetOffsetBody` / `GetConsumerStatusBody` / `ProcessQueueInfo` / `CMResult` /
//! `ConsumeMessageDirectlyResult`。
//!
//! ## 三种「同一个 MessageQueue」的不同写法
//!
//! 1. **map 键**（`offsetTable` / `mqTable` / `messageQueueTable`）：fastjson2 把对象
//!    键内联成 JSON，产出的是**非法 JSON**。这里全部复用 `admin_body` 的
//!    [`message_queue_key`] / [`parse_message_queue_key`] /
//!    [`decode_message_queue_map`] / [`encode_message_queue_map`]，键序固定为字母序
//!    `brokerName,queueId,topic`。
//! 2. **数组元素**（`mqSet` / `lockOKMQSet`）：走普通对象值，键序是 Java 的
//!    **字段声明序** `topic,brokerName,queueId`（`MessageQueue.java:25-27`、
//!    `mq_client.py:1140`），所以另用 [`message_queue_value`]，不能复用键文本。
//! 3. **普通字符串键的 map**（`statusTable` / `subscriptionTable` / `consumerTable`
//!    外层 / `brokerAddrTable`）：用 `jentries` 保序读写。
//!
//! ## 与 Python 的两处已知差异（都朝 Java 靠拢）
//!
//! - 浮点零值：Python 的默认值是 int `0`，`json.dumps` 写出 `0`；这里写 `0.0`，与
//!   fastjson2 的 `Double` 输出一致（见 `ConsumeStatus` 测试）。
//! - `Connection` / `ConsumeStatus` 在 Java 里不是 `RemotingSerializable`
//!   （`body.py` 也没给它们 `encode`），故这两个类型只有 `to_json_value` /
//!   `from_json_value`，不导出 body 编解码。

use serde_json::{Map, Value};

use super::admin_body::{
    expect_object, jarray, jboolean, jdouble, jentries, jfield, jint, jlong, jraw_map, json_object,
    jstring, MessageQueueKey,
};
use super::ext_fields::StringMap;
use super::heartbeat::SubscriptionData;
use super::serialize::RemotingSerializable;
use crate::error::{Error, Result};

// `body.py:9` 同样从 `admin_body` 把 MessageQueue 键工具再导出一份，供上层就近取用。
pub use super::admin_body::{
    decode_message_queue_map, encode_message_queue_map, message_queue_key, parse_message_queue_key,
};

// ---------------------------------------------------------------- 内部小工具

/// 字符串数组字段（对应 `list(d.get(k) or [])`）；缺失 / null 回空表。
///
/// 元素按 `jstring` 的口径宽松转换：broker 偶尔把字符串写在数字里。
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

/// 原样透传的对象数组（`subscriptionSet` / `consumeStatsList` / `consumeTimeSpanSet`）。
///
/// 不做形状校验，与 Python 的 `list(d.get(k) or [])` 一致 —— 连 `null` 元素也照留。
fn raw_object_list(value: &Value, key: &str) -> Result<Vec<Value>> {
    Ok(jarray(value, key)?.into_iter().cloned().collect())
}

/// 原样透传的对象 map，保持报文顺序（`statusTable` / `subscriptionTable`）。
fn raw_object_map(value: &Value, key: &str) -> Result<Vec<(String, Value)>> {
    Ok(jentries(value, key)?
        .into_iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect())
}

/// 把 `Vec<(String, Value)>` 写回 JSON 对象（`Map` 开了 `preserve_order`，顺序即插入序）。
fn object_of(entries: &[(String, Value)]) -> Value {
    let mut map = Map::new();
    for (k, v) in entries {
        map.insert(k.clone(), v.clone());
    }
    Value::Object(map)
}

/// `decode_message_queue_map` 的 value 解析：原样透传（值类型见文档）。
fn clone_value(value: &Value) -> Result<Value> {
    Ok(value.clone())
}

fn string_array(items: &[String]) -> Value {
    Value::Array(items.iter().map(|s| Value::String(s.clone())).collect())
}

/// `None` 落成 `null`（Python 的 `to_dict` 无条件放键，与「键不存在」是两回事）。
fn optional_string(value: &Option<String>) -> Value {
    match value {
        Some(s) => Value::String(s.clone()),
        None => Value::Null,
    }
}

/// 等价于 [`jlong`] 的宽松转换，但作用在裸 `Value` 上（map 的 value 没有键名可传）。
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

/// `encode_message_queue_map` 的 value 转换：位点表。
fn long_value(value: &i64) -> Value {
    Value::from(*value)
}

/// 文本字段的宽松读取（map value 场景，同 [`jstring`] 但无需键名）。
fn text_of(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// MessageQueue 数组字段的读取（`mqSet` / `lockOKMQSet`）。
///
/// 非对象元素直接报错：Python 靠 `d.get("topic")` 兜底，但那会把脏数据变成
/// 一个「topic 为空的队列」，静默丢锁，比报错更糟。
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

/// MessageQueue 数组字段的写出：键序 = Java 字段声明序。
fn message_queue_array(list: &[MessageQueueKey]) -> Value {
    Value::Array(list.iter().map(message_queue_value).collect())
}

/// `MessageQueue` 作为**值**（数组元素 / 嵌套对象）时的形态，
/// 键序 `topic,brokerName,queueId`（`MessageQueue.java:25-27`）。
pub fn message_queue_value(mq: &MessageQueueKey) -> Value {
    json_object(vec![
        ("topic", Value::String(mq.topic.clone())),
        ("brokerName", Value::String(mq.broker_name.clone())),
        ("queueId", Value::from(mq.queue_id)),
    ])
}

// ---------------------------------------------------------------- KVTable

/// 对应 `org.apache.rocketmq.remoting.protocol.body.KVTable`。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KVTable {
    /// cluster → broker 给的 KV 串；顺序即 broker 返回顺序，故用 [`StringMap`]。
    pub table: StringMap,
}

impl KVTable {
    pub fn to_json_value(&self) -> Value {
        json_object(vec![("table", self.table.to_json())])
    }

    pub fn from_json_value(value: &Value) -> Result<KVTable> {
        expect_object(value, "KVTable")?;
        Ok(KVTable {
            // Python: `d.get("table") or {}` —— 缺键 / null / 非对象都回空表
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.TopicList`。
///
/// `brokerAddr` 为 `None` 时**整个键不出现**（`body.py:41-45`），空串则要照写。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicList {
    pub topic_list: Vec<String>,
    pub broker_addr: Option<String>,
}

impl TopicList {
    pub fn new() -> TopicList {
        TopicList::default()
    }

    /// 对应 Python `get_topic_list()`。
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

// ---------------------------------------------------------------- 批量锁 / 解锁

/// 对应 `org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody`（235）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockBatchRequestBody {
    pub consumer_group: Option<String>,
    pub client_id: Option<String>,
    /// Java `Set<MessageQueue> mqSet`，写成对象数组。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody`（236）。
///
/// 与 [`LockBatchRequestBody`] 字段同形，但 Java 是两个类、两个请求码，报文分开保留。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody`。
///
/// 键名照抄 Java 的 `lockOKMQSet`（`OK`、`MQ` 都大写）。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.GetConsumerListByGroupResponseBody`。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody`（46）。
///
/// 只被 `CHECK_CLIENT_CONFIG(46)` 用到：broker 拿 clientId/group 记日志，真正被校验的只有
/// `subscriptionData` 的 expressionType 与 subString（Java
/// `ClientManageProcessor#checkClientConfig`）。`namespace` 字段 Java 5.5.1 里有但发送端
/// 不填，这里同样保留字段而不写值。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ClusterInfo`
/// （nameserver `GET_BROKER_CLUSTER_INFO(100)`）。
///
/// 内部只保留 `{brokerName: {brokerId: addr}}` 与 `{cluster: [brokerName]}` 两张表，
/// `to_json_value` 再把前者包成 BrokerData 形状，其中 `cluster:""` 与
/// `enableActingMaster:false` 是**恒定默认值**（`body.py:179-190`），
/// 所以真实集群名 / acting-master 开关不会随本类型往返。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterInfo {
    pub broker_addr_table: Vec<(String, Vec<(i64, String)>)>,
    pub cluster_addr_table: Vec<(String, Vec<String>)>,
}

impl ClusterInfo {
    /// 所有 broker 地址：按 brokerName、brokerId 升序去重、跳过空串（`body.py:167-177`）。
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
                // fastjson2 的 Long 键写成裸数字，但 Python 与 route.rs 都用
                // 带引号的十进制字符串；两种读法都容忍，这里与 Python 报文一致。
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
            // 真实 broker 的 brokerAddrTable[name] 是 BrokerData，地址表在它的 brokerAddrs 里
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo`（307 应答）。
///
/// 两端都用得到：admin 侧解析 broker 汇总的运行信息，客户端侧在应答
/// `GET_CONSUMER_RUNNING_INFO(307)` 时编码自己的运行信息。
///
/// ⚠ `mq_table` / `mq_pop_table` 的键是 `MessageQueue`（Java 用 `TreeMap`），
/// fastjson2 会把它内联成 JSON 对象，必须走 [`message_queue_key`] +
/// [`decode_message_queue_map`]，不能当普通字符串键。
///
/// value 侧原样透传：`mqTable` 是 [`ProcessQueueInfo`]、`mqPopTable` 是 Java
/// `PopProcessQueueInfo`（Python 与本文件都没有该类）、`statusTable` 是
/// [`ConsumeStatus`]，由上层用各自的 `from_json_value` 解读。
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
    /// 注意 Java 常量**值**没有下划线：`PROP_CONSUMEORDERLY`（`ConsumerRunningInfo.java:33`）。
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
            // Python 无条件放键，所以 jstack=None 落成 null 而非消失
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

    /// `statusTable[topic]` 解成 [`ConsumeStatus`]（Python 让上层手工转换）。
    pub fn consume_status(&self, topic: &str) -> Result<Option<ConsumeStatus>> {
        match self.status_table.iter().find(|(k, _)| k == topic) {
            None => Ok(None),
            Some((_, v)) => Ok(Some(ConsumeStatus::from_json_value(v)?)),
        }
    }
}

// ---------------------------------------------------------------- Connection / 连接类

/// 对应 `org.apache.rocketmq.remoting.protocol.body.Connection`。
///
/// `version` 是 Java 版本枚举的**序数**（int），不是版本字符串。
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
            // 缺键 / null → None，与「broker 真给了 0」区分开
            version: jfield(value, "version").map(|v| long_of(v) as i32),
        })
    }
}

/// `connectionSet` 的读法（ConsumerConnection / ProducerConnection 共用）。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ConsumerConnection`。
///
/// `consumeType` / `messageModel` / `consumeFromWhere` 在 Java 是枚举，broker
/// 也可能回 null（老版本 / 广播组），所以三个键**恒出现**、值可为 null。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConsumerConnection {
    pub connection_set: Vec<Connection>,
    /// topic → `SubscriptionData` 的 JSON（类型见 `heartbeat::SubscriptionData`）。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ProducerConnection`。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody`。
///
/// 元素是 Java `ConsumeTimeSpan{topic, consumerGroup, consumeTimeStamp, latency}`；
/// `body.py:353-371` 不解读，故原样透传。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ConsumeStatus`
/// （`ConsumerRunningInfo.statusTable` 的 value；Java 不继承 `RemotingSerializable`）。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList`。
///
/// ⚠ JSON 键是 Java 字段名 `consumeStatsList`，**不是** `statsList`：早期移植
/// 猜错了键名，于是真机响应永远解析出空列表，看着像「这个 broker 没有积压」。
///
/// `stats_list` 元素是 `List<Map<groupName, List<ConsumeStats>>>` 的 JSON；
/// `broker_addr` 为 None 时整键不出现（Java `RemotingSerializable` 用
/// NON_NULL 序列化 String），`totalDiff` / `totalInflightDiff` 是 Java 的
/// `long` 原语字段，恒出现在 JSON 里。
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

// ---------------------------------------------------------------- 位点重置 / 状态上报

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody`（220 下发）。
///
/// ⚠ Java 字段是 `Map<MessageQueue, Long> offsetTable`，**不是**
/// topic→queueId→offset 的嵌套 map（`body.py:431-437` 记录了早期实现的这个错误）；
/// 键走 fastjson2 内联对象。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody`（42）。
///
/// 两个 map 的**内层**键都是 `MessageQueue`（fastjson2 内联对象）；外层
/// `consumerTable` 的键是 clientId 字符串（Java 已废弃该字段，仍保留以兼容老 broker）。
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

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo`。
///
/// 键序 = Java 字段声明序（`ProcessQueueInfo.java:25-40`）；注意 Java 的拼写
/// `droped`（不是 `dropped`），改成正确拼写 broker 就读不到了。
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
    /// Java 字段名就是 `droped`（拼写如此）。
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

// ---------------------------------------------------------------- 309 单条消息试消费

/// 对应 `org.apache.rocketmq.remoting.protocol.body.CMResult`（Java 枚举名原文）。
pub struct CMResult;

impl CMResult {
    pub const CR_SUCCESS: &'static str = "CR_SUCCESS";
    pub const CR_LATER: &'static str = "CR_LATER";
    pub const CR_ROLLBACK: &'static str = "CR_ROLLBACK";
    pub const CR_COMMIT: &'static str = "CR_COMMIT";
    pub const CR_THROW_EXCEPTION: &'static str = "CR_THROW_EXCEPTION";
    pub const CR_RETURN_NULL: &'static str = "CR_RETURN_NULL";
}

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult`。
///
/// 309 的应答 body，字段全是标量 —— 这批 body 里唯一不涉及 MessageQueue 键的。
/// `autoCommit` 的 Java 初值是 **true**，所以手写 `Default` 而不是 derive。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeMessageDirectlyResult {
    pub order: bool,
    pub auto_commit: bool,
    /// [`CMResult`] 里的枚举名。
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
            // body.py:598 `bool(d.get("autoCommit", True))`
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 把参考实现打印出的紧凑 JSON 变成 `Value`（用于构造「原样透传」字段的夹具）。
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
        // 参考实现：{"table": {"DefaultCluster": "...", "broker-a": "..."}}
        assert_eq!(
            json_of(&kv.to_json_value()),
            r#"{"table":{"DefaultCluster":"127.0.0.1:10911;127.0.0.1:10912","broker-a":"127.0.0.1:10911"}}"#
        );
        assert_eq!(KVTable::decode(&kv.encode()).unwrap(), kv);
        // 缺键 / null 表 → 空 map（Python `d.get("table") or {}`）
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
        // 空串地址是「有值」，键必须留着
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
    fn lock_and_unlock_batch_bodies_use_java_field_order() {
        let body = LockBatchRequestBody {
            consumer_group: Some("GID_BodyGolden".into()),
            client_id: Some("client-1@127.0.0.1".into()),
            mq_set: vec![
                MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0),
                MessageQueueKey::new("BodyGoldenTopic", "broker-a", 1),
            ],
        };
        // 值对象键序 = Java 声明序 topic,brokerName,queueId（不是内联键的字母序）
        assert_eq!(
            json_of(&body.to_json_value()),
            concat!(
                r#"{"consumerGroup":"GID_BodyGolden","clientId":"client-1@127.0.0.1","mqSet":"#,
                r#"[{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":0},"#,
                r#"{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":1}]}"#
            )
        );
        assert_eq!(LockBatchRequestBody::decode(&body.encode()).unwrap(), body);
        // 参考实现的 null 字段：三个键恒在
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
        // mq_client.py:1149 `int(d.get("queueId") or 0)` —— broker 会给字符串数字
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
        // brokerAddr 为 None 时整键消失（Java String 字段走 NON_NULL），
        // 两个 long 字段则恒在
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
        // 参考实现按插入顺序包成 BrokerData 形状，cluster / enableActingMaster 恒为默认值
        assert_eq!(
            json_of(&cluster.to_json_value()),
            r#"{"brokerAddrTable":{"broker-b":{"cluster":"","brokerName":"broker-b","brokerAddrs":{"1":"127.0.0.1:10921","0":"127.0.0.1:10920"},"enableActingMaster":false},"broker-a":{"cluster":"","brokerName":"broker-a","brokerAddrs":{"0":"127.0.0.1:10911","1":"127.0.0.1:10912"},"enableActingMaster":false}},"clusterAddrTable":{"DefaultCluster":["broker-a","broker-b"]}}"#
        );
        // getBrokerAddrs 的排序去重输出（与参考实现一致）
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

        // nameserver 真实报文：裸数字键 + 重复地址，读回后按名 / 号排序
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

        // 内联对象键：写出即 fastjson2 形态，读回还是同一个 MessageQueueKey
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
        // jstack 有值时同样落在最后一个键上
        let with_stack = ConsumerRunningInfo {
            jstack: Some("thread dump".into()),
            ..Default::default()
        };
        assert!(json_of(&with_stack.to_json_value()).ends_with(r#","jstack":"thread dump"}"#));

        // 参考实现的空对象：7 个键全在，jstack 落成 null
        assert_eq!(
            json_of(&ConsumerRunningInfo::default().to_json_value()),
            concat!(
                r#"{"properties":{},"subscriptionSet":[],"mqTable":{},"mqPopTable":{},"#,
                r#""statusTable":{},"userConsumerInfo":{},"jstack":null}"#
            )
        );
    }

    #[test]
    fn consumer_running_info_accepts_java_inline_object_keys() {
        // fastjson2 真机输出（非法 JSON），Python 的 _FastJsonParser 与 Rust 都能读
        let java = concat!(
            r#"{"properties":{"PROP_CONSUME_TYPE":"CONSUME_PASSIVELY"},"subscriptionSet":[],"#,
            r#""mqTable":{{"brokerName":"broker-a","queueId":0,"topic":"BodyGoldenTopic"}:"#,
            r#"{"commitOffset":42,"cachedMsgCount":1,"droped":false}},"#,
            r#""statusTable":{"BodyGoldenTopic":{"pullRT":1.5,"consumeFailedMsgs":2}}}"#
        );
        let info = ConsumerRunningInfo::decode(java.as_bytes()).unwrap();
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
        // 重新编码后键文本仍是同一份内联对象
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
        // 空连接：三个枚举键仍在、值为 null（Python 无条件放键）
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
        // version 缺失 → None（区别于 broker 真给了 0）
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
    fn reset_offset_body_round_trips_inline_keys_and_java_wire() {
        let body = ResetOffsetBody {
            offset_table: vec![
                (MessageQueueKey::new("BodyGoldenTopic", "broker-a", 0), 10),
                (MessageQueueKey::new("BodyGoldenTopic", "broker-a", 1), 20),
            ],
        };
        // 参考实现 encode()：键是带引号的转义字符串（与 fastjson2 裸内联同义）
        assert_eq!(
            json_of(&body.to_json_value()),
            r#"{"offsetTable":{"{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"BodyGoldenTopic\"}":10,"{\"brokerName\":\"broker-a\",\"queueId\":1,\"topic\":\"BodyGoldenTopic\"}":20}}"#
        );
        assert_eq!(ResetOffsetBody::decode(&body.encode()).unwrap(), body);
        // Java 真机报文（裸内联对象键；位点写成字符串也认，对齐 Python 的 int(v)）
        let java = concat!(
            r#"{"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"BodyGoldenTopic"}:10,"#,
            r#"{"brokerName":"broker-a","queueId":1,"topic":"BodyGoldenTopic"}:"20"}}"#
        );
        assert_eq!(ResetOffsetBody::decode(java.as_bytes()).unwrap(), body);
        assert_eq!(
            GetConsumerStatusBody::default().to_json_value(),
            py(r#"{"messageQueueTable":{},"consumerTable":{}}"#)
        );
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
        let java = concat!(
            r#"{"messageQueueTable":{{"brokerName":"b","queueId":0,"topic":"T"}:7},"#,
            r#""consumerTable":{"c1":{{"brokerName":"b","queueId":1,"topic":"T"}:8}}}"#
        );
        let back = GetConsumerStatusBody::decode(java.as_bytes()).unwrap();
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
        // 零值报文（参考实现 `to_dict()` 直出）：14 个键恒在
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
        // 缺键 / 脏值回默认值，绝不 panic（对齐 jlong / jboolean 的口径）
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
        // 参考实现：{"pullRT": 12.5, "pullTPS": 100.0, "consumeRT": 3.25,
        //            "consumeOKTPS": 99.5, "consumeFailedTPS": 0.5, "consumeFailedMsgs": 7}
        assert_eq!(
            json_of(&cs.to_json_value()),
            r#"{"pullRT":12.5,"pullTPS":100.0,"consumeRT":3.25,"consumeOKTPS":99.5,"consumeFailedTPS":0.5,"consumeFailedMsgs":7}"#
        );
        assert_eq!(
            ConsumeStatus::from_json_value(&cs.to_json_value()).unwrap(),
            cs
        );
        // 参考实现零值写 `0`（Python int），这里按 fastjson2 的 Double 写 `0.0`
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
        // 参考实现 `ConsumeMessageDirectlyResult()`：autoCommit 初值 true
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
    fn message_queue_value_text_is_java_declaration_order() {
        let mq = MessageQueueKey::new("BodyGoldenTopic", "broker-a", 3);
        assert_eq!(
            json_of(&message_queue_value(&mq)),
            r#"{"topic":"BodyGoldenTopic","brokerName":"broker-a","queueId":3}"#
        );
        // 与内联对象键的字母序刻意不同
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
}
