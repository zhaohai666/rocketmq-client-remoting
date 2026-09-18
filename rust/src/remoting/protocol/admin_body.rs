//! 管理端响应体（对应 `org.apache.rocketmq.remoting.protocol.admin.*` 与 `body.*` 中的管理类）。
//!
//! 移植 `python/rocketmq/remoting/protocol/admin_body.py`：
//! `TopicStatsTable` / `TopicOffset` / `ConsumeStats` / `OffsetWrapper` /
//! `TopicConfigSerializeWrapper` / `ConsumeQueueData` / `QueryConsumeQueueResponseBody`。
//!
//! ## 关键坑：MessageQueue 做 map 键
//!
//! Java 侧 `Map<MessageQueue, TopicOffset>`，fastjson2 会把键**内联成 JSON 对象**，
//! 产出的是**非法 JSON**：
//!
//! ```text
//! {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:{...}}}
//! ```
//!
//! 因此这里不能把键当普通字符串：写出时用 [`message_queue_key`]（键按字母序，与
//! fastjson2 一致），读回时用 [`parse_message_queue_key`] /
//! [`decode_message_queue_map`]，并依赖 `serialize::fastjson` 的容忍解析。
//!
//! ## 为什么这里有 `MessageQueueKey` 而不是 `crate::common::MessageQueue`
//!
//! 协议层必须能脱离 common 层独立编译（common 层由另一条流水线并行移植），
//! 所以在本层声明一个最小可比较的键类型；上层可以直接 `From` 转换。
//!
//! 本模块同时导出 `j*` 系列 JSON 取值工具，供 `route` / `body` / `heartbeat` /
//! `subscription` 复用（对齐 Python 里 `body.py` 从 `admin_body.py` 借键工具的写法）。

use serde_json::{Map, Value};

use super::serialize::{fastjson, RemotingSerializable};
use crate::error::{Error, Result};

// ---------------------------------------------------------------- 通用 JSON 取值工具
//
// 语义与 Python 侧 `d.get(key, default)` + `int()/float()/bool()` 宽松转换一致：
// 缺键回默认值，数字写成字符串也能读，脏数据回默认值而不是 panic。

/// 取原始字段（不存在或显式 `null` 都返回 `None`，对应 Python 的 `d.get(k)`）。
pub fn jfield<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key).filter(|v| !v.is_null())
}

/// 字符串字段：缺失 / null 返回 `None`。
pub fn jstring(value: &Value, key: &str) -> Option<String> {
    match jfield(value, key)? {
        Value::String(s) => Some(s.clone()),
        // Java 偶尔把字符串写在数字里（例如 `version: 400`），按 fastjson 转成文本。
        other => Some(other.to_string()),
    }
}

/// 字符串字段，带默认值（对应 `d.get(k, "")`）。
pub fn jstring_or(value: &Value, key: &str, default: &str) -> String {
    jstring(value, key).unwrap_or_else(|| default.to_string())
}

/// `int` 字段：接受数字、数字字符串、布尔（fastjson2 的历史输出）。
pub fn jint(value: &Value, key: &str, default: i32) -> i32 {
    jlong(value, key, default as i64) as i32
}

/// `long` 字段。
pub fn jlong(value: &Value, key: &str, default: i64) -> i64 {
    match jfield(value, key) {
        Some(Value::Number(n)) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .or_else(|| n.as_u64().map(|v| v as i64))
            .unwrap_or(default),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok().unwrap_or(default),
        Some(Value::Bool(true)) => 1,
        Some(Value::Bool(false)) => 0,
        _ => default,
    }
}

/// `double` / TPS 字段。
pub fn jdouble(value: &Value, key: &str, default: f64) -> f64 {
    match jfield(value, key) {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(default),
        Some(Value::String(s)) => s.trim().parse::<f64>().ok().unwrap_or(default),
        _ => default,
    }
}

/// 布尔字段（对应 `d.get(k, default)`；只认真正的 `true` / `"true"`）。
pub fn jboolean(value: &Value, key: &str, default: bool) -> bool {
    match jfield(value, key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => {
            let t = s.trim().to_ascii_lowercase();
            if t == "true" || t == "1" {
                true
            } else if t == "false" || t == "0" {
                false
            } else {
                default
            }
        }
        Some(Value::Number(n)) => n.as_f64().map(|f| f != 0.0).unwrap_or(default),
        _ => default,
    }
}

/// 对象字段的 (key, value) 列表，保持 JSON 里的顺序；缺失或 null 回空表。
///
/// 非对象（且非 null）说明报文形状不对，返回 `Err(Decode)`。
pub fn jentries<'a>(value: &'a Value, key: &str) -> Result<Vec<(&'a String, &'a Value)>> {
    match jfield(value, key) {
        None => Ok(Vec::new()),
        Some(Value::Object(map)) => Ok(map.iter().collect()),
        Some(_) => Err(Error::Decode(format!("field {key} is not a json object"))),
    }
}

/// 数组字段的元素列表；缺失或 null 回空表。
pub fn jarray<'a>(value: &'a Value, key: &str) -> Result<Vec<&'a Value>> {
    match jfield(value, key) {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => Ok(items.iter().collect()),
        Some(_) => Err(Error::Decode(format!("field {key} is not a json array"))),
    }
}

/// 顶层必须是对象；`null` / 非对象都算脏数据。
pub fn expect_object(value: &Value, what: &str) -> Result<()> {
    if value.is_object() {
        Ok(())
    } else {
        Err(Error::Decode(format!(
            "{what} body is not a json object: {}",
            compact(value)
        )))
    }
}

/// 原样透传的 map 字段（缺失时给 `{}`，与 Python 的 `dict(d.get(k) or {})` 一致）。
pub fn jraw_map(value: &Value, key: &str) -> Value {
    match jfield(value, key) {
        Some(Value::Object(map)) => Value::Object(map.clone()),
        _ => Value::Object(Map::new()),
    }
}

/// 错误信息里截断打印脏数据，避免把整个 broker 响应塞进日志。
fn compact(value: &Value) -> String {
    let text = value.to_string();
    if text.len() > 120 {
        format!("{}...", &text[..120])
    } else {
        text
    }
}

/// 按插入顺序构造 JSON 对象（`Map` 开了 `preserve_order`）。
pub fn json_object(pairs: Vec<(&'static str, Value)>) -> Value {
    let mut map = Map::new();
    for (k, v) in pairs {
        map.insert(k.to_string(), v);
    }
    Value::Object(map)
}

fn number_or_null(v: i64) -> Value {
    Value::from(v)
}

// ---------------------------------------------------------------- MessageQueue 键

/// `MessageQueue` 在本层的最小形态：只用作 map 键与路由产物。
///
/// 字段顺序刻意按 Java `MessageQueue` 的语义字段；fastjson2 的**文本**键顺序由
/// [`message_queue_key`] 固定为字母序（`brokerName` / `queueId` / `topic`）。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageQueueKey {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
}

impl MessageQueueKey {
    pub fn new(topic: impl Into<String>, broker_name: impl Into<String>, queue_id: i32) -> Self {
        MessageQueueKey {
            topic: topic.into(),
            broker_name: broker_name.into(),
            queue_id,
        }
    }

    /// 内联对象键文本（等价于 [`message_queue_key`]）。
    pub fn key_string(&self) -> String {
        message_queue_key(self)
    }

    /// 从 fastjson2 内联对象 `Value` 还原；不是对象返回 `None`。
    pub fn from_value(value: &Value) -> Option<MessageQueueKey> {
        if !value.is_object() {
            return None;
        }
        Some(MessageQueueKey {
            topic: jstring(value, "topic").unwrap_or_default(),
            broker_name: jstring(value, "brokerName").unwrap_or_default(),
            queue_id: jint(value, "queueId", 0),
        })
    }
}

/// 把 `MessageQueue` 序列化成 fastjson2 风格的内联对象键（键按字母序）。
///
/// 与 Python 唯一的差别：topic / brokerName 里的引号与控制字符会被转义
/// （Python 用 `%s` 直插，遇到特殊字符会写出更不合法的 JSON；Java fastjson2 是转义的）。
pub fn message_queue_key(mq: &MessageQueueKey) -> String {
    format!(
        "{{\"brokerName\":{},\"queueId\":{},\"topic\":{}}}",
        json_text(&mq.broker_name),
        mq.queue_id,
        json_text(&mq.topic)
    )
}

fn json_text(s: &str) -> String {
    Value::String(s.to_string()).to_string()
}

/// 把 fastjson2 写出的 MessageQueue 内联对象键还原成 [`MessageQueueKey`]。
///
/// 返回 `None` 表示这不是内联对象键（例如 `"0"`、`"G1"` 这类普通字符串键）。
pub fn parse_message_queue_key(key: &str) -> Option<MessageQueueKey> {
    let value = fastjson::decode_map_key(key)?;
    MessageQueueKey::from_value(&value)
}

/// 通用：解析以 MessageQueue 为键的 map（fastjson2 非字符串键），保持报文顺序。
///
/// 非内联对象键按 Python 语义直接跳过。
pub fn decode_message_queue_map<T, F>(raw: Option<&Value>, parse: F) -> Result<Vec<(MessageQueueKey, T)>>
where
    F: Fn(&Value) -> Result<T>,
{
    let mut out = Vec::new();
    match raw {
        None | Some(Value::Null) => return Ok(out),
        Some(Value::Object(_)) => {}
        Some(_) => return Err(Error::Decode("messageQueue-keyed map is not an object".into())),
    }
    if let Some(map) = raw.and_then(Value::as_object) {
        for (k, v) in map {
            if let Some(mq) = parse_message_queue_key(k) {
                out.push((mq, parse(v)?));
            }
        }
    }
    Ok(out)
}

/// 把 MessageQueue 键的表写回 fastjson2 内联对象键形式。
pub fn encode_message_queue_map<T, F>(table: &[(MessageQueueKey, T)], value_of: F) -> Value
where
    F: Fn(&T) -> Value,
{
    let mut map = Map::new();
    for (mq, v) in table {
        map.insert(message_queue_key(mq), value_of(v));
    }
    Value::Object(map)
}

// ---------------------------------------------------------------- TopicOffset

/// 对应 `org.apache.rocketmq.remoting.protocol.admin.TopicOffset`。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicOffset {
    pub min_offset: i64,
    pub max_offset: i64,
    pub last_update_timestamp: i64,
}

impl TopicOffset {
    pub fn new(min_offset: i64, max_offset: i64, last_update_timestamp: i64) -> TopicOffset {
        TopicOffset {
            min_offset,
            max_offset,
            last_update_timestamp,
        }
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("minOffset", number_or_null(self.min_offset)),
            ("maxOffset", number_or_null(self.max_offset)),
            (
                "lastUpdateTimestamp",
                number_or_null(self.last_update_timestamp),
            ),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<TopicOffset> {
        expect_object(value, "TopicOffset")?;
        Ok(TopicOffset {
            min_offset: jlong(value, "minOffset", 0),
            max_offset: jlong(value, "maxOffset", 0),
            last_update_timestamp: jlong(value, "lastUpdateTimestamp", 0),
        })
    }
}

// ---------------------------------------------------------------- TopicStatsTable

/// 对应 `org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable`。
///
/// Java 探针输出：`{"offsetTable":{<MessageQueue>:{...}},"topicPutTps":0.0}`
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicStatsTable {
    pub offset_table: Vec<(MessageQueueKey, TopicOffset)>,
    pub topic_put_tps: f64,
}

impl TopicStatsTable {
    pub fn new() -> TopicStatsTable {
        TopicStatsTable {
            offset_table: Vec::new(),
            topic_put_tps: 0.0,
        }
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            (
                "offsetTable",
                encode_message_queue_map(&self.offset_table, |v| v.to_json_value()),
            ),
            ("topicPutTps", Value::from(self.topic_put_tps)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<TopicStatsTable> {
        expect_object(value, "TopicStatsTable")?;
        Ok(TopicStatsTable {
            offset_table: decode_message_queue_map(jfield(value, "offsetTable"), |v| {
                TopicOffset::from_json_value(v)
            })?,
            topic_put_tps: jdouble(value, "topicPutTps", 0.0),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<TopicStatsTable> {
        TopicStatsTable::from_json_value(&RemotingSerializable::decode(data)?)
    }

    /// 所有队列 `maxOffset` 之和（admin 工具常用）。
    pub fn total_max_offset(&self) -> i64 {
        self.offset_table.iter().map(|(_, o)| o.max_offset).sum()
    }
}

// ---------------------------------------------------------------- OffsetWrapper

/// 对应 `org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper`。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OffsetWrapper {
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub last_timestamp: i64,
    pub pull_offset: i64,
}

impl OffsetWrapper {
    pub fn new(broker_offset: i64, consumer_offset: i64, last_timestamp: i64, pull_offset: i64) -> OffsetWrapper {
        OffsetWrapper {
            broker_offset,
            consumer_offset,
            last_timestamp,
            pull_offset,
        }
    }

    /// 与 Java `OffsetWrapper.getLag()` 一致：`brokerOffset - consumerOffset`。
    pub fn lag(&self) -> i64 {
        self.broker_offset - self.consumer_offset
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("brokerOffset", number_or_null(self.broker_offset)),
            ("consumerOffset", number_or_null(self.consumer_offset)),
            ("lastTimestamp", number_or_null(self.last_timestamp)),
            ("pullOffset", number_or_null(self.pull_offset)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<OffsetWrapper> {
        expect_object(value, "OffsetWrapper")?;
        Ok(OffsetWrapper {
            broker_offset: jlong(value, "brokerOffset", 0),
            consumer_offset: jlong(value, "consumerOffset", 0),
            last_timestamp: jlong(value, "lastTimestamp", 0),
            pull_offset: jlong(value, "pullOffset", 0),
        })
    }
}

// ---------------------------------------------------------------- ConsumeStats

/// 对应 `org.apache.rocketmq.remoting.protocol.admin.ConsumeStats`。
///
/// Java 探针输出：`{"consumeTps":1.5,"offsetTable":{<MessageQueue>:<OffsetWrapper>}}`
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConsumeStats {
    pub offset_table: Vec<(MessageQueueKey, OffsetWrapper)>,
    pub consume_tps: f64,
}

impl ConsumeStats {
    pub fn new() -> ConsumeStats {
        ConsumeStats {
            offset_table: Vec::new(),
            consume_tps: 0.0,
        }
    }

    pub fn total_lag(&self) -> i64 {
        self.offset_table.iter().map(|(_, o)| o.lag()).sum()
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            (
                "offsetTable",
                encode_message_queue_map(&self.offset_table, |v| v.to_json_value()),
            ),
            ("consumeTps", Value::from(self.consume_tps)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<ConsumeStats> {
        expect_object(value, "ConsumeStats")?;
        Ok(ConsumeStats {
            offset_table: decode_message_queue_map(jfield(value, "offsetTable"), |v| {
                OffsetWrapper::from_json_value(v)
            })?,
            consume_tps: jdouble(value, "consumeTps", 0.0),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ConsumeStats> {
        ConsumeStats::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- TopicConfigSerializeWrapper

/// 对应 `org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper`。
///
/// `topicConfigTable` 的 value 是 common 层的 `TopicConfig`；协议层不引用 common 层，
/// 所以这里原样保存 JSON，由上层自己 `TopicConfig::from_json_value`。
/// `dataVersion` 同理（Java `DataVersion{counter,timestamp}`）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicConfigSerializeWrapper {
    pub topic_config_table: Vec<(String, Value)>,
    pub data_version: Value,
}

impl TopicConfigSerializeWrapper {
    pub fn new() -> TopicConfigSerializeWrapper {
        TopicConfigSerializeWrapper {
            topic_config_table: Vec::new(),
            data_version: Value::Object(Map::new()),
        }
    }

    pub fn to_json_value(&self) -> Value {
        let mut table = Map::new();
        for (topic, cfg) in &self.topic_config_table {
            table.insert(topic.clone(), cfg.clone());
        }
        json_object(vec![
            ("dataVersion", self.data_version.clone()),
            ("topicConfigTable", Value::Object(table)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<TopicConfigSerializeWrapper> {
        expect_object(value, "TopicConfigSerializeWrapper")?;
        let mut table = Vec::new();
        for (k, v) in jentries(value, "topicConfigTable")? {
            if !v.is_object() {
                return Err(Error::Decode(format!("topicConfigTable[{k}] is not an object")));
            }
            table.push((k.clone(), v.clone()));
        }
        Ok(TopicConfigSerializeWrapper {
            topic_config_table: table,
            data_version: jraw_map(value, "dataVersion"),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<TopicConfigSerializeWrapper> {
        TopicConfigSerializeWrapper::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- QueryConsumeQueue

/// 对应 `org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData`。
///
/// 字段：`physicOffset, physicSize, tagsCode, extendDataJson, bitMap, eval, msg`。
/// 与 Java 一致：`extendDataJson` / `msg` 为 null 时**不出现**在报文里，
/// 其余字段（含 `bitMap: null`）恒定写出。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConsumeQueueData {
    pub physic_offset: i64,
    pub physic_size: i32,
    pub tags_code: i64,
    pub extend_data_json: Option<String>,
    pub bit_map: Option<String>,
    /// Java 字段名 `eval`（`isEval`）。
    pub eval: bool,
    pub msg: Option<String>,
}

impl ConsumeQueueData {
    pub fn to_json_value(&self) -> Value {
        let mut map = Map::new();
        map.insert("physicOffset".to_string(), Value::from(self.physic_offset));
        map.insert("physicSize".to_string(), Value::from(self.physic_size));
        map.insert("tagsCode".to_string(), Value::from(self.tags_code));
        map.insert("eval".to_string(), Value::from(self.eval));
        map.insert(
            "bitMap".to_string(),
            match &self.bit_map {
                Some(s) => Value::String(s.clone()),
                None => Value::Null,
            },
        );
        if let Some(v) = &self.extend_data_json {
            map.insert("extendDataJson".to_string(), Value::String(v.clone()));
        }
        if let Some(v) = &self.msg {
            map.insert("msg".to_string(), Value::String(v.clone()));
        }
        Value::Object(map)
    }

    pub fn from_json_value(value: &Value) -> Result<ConsumeQueueData> {
        expect_object(value, "ConsumeQueueData")?;
        Ok(ConsumeQueueData {
            physic_offset: jlong(value, "physicOffset", 0),
            physic_size: jint(value, "physicSize", 0),
            tags_code: jlong(value, "tagsCode", 0),
            extend_data_json: jstring(value, "extendDataJson"),
            bit_map: jstring(value, "bitMap"),
            eval: jboolean(value, "eval", false),
            msg: jstring(value, "msg"),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<ConsumeQueueData> {
        ConsumeQueueData::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

/// 对应 `org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody`。
///
/// Java 探针输出：`{"filterData":"*","maxQueueIndex":88,"minQueueIndex":1,"subscriptionData":{...}}`
/// （`queueData` 为 null 时整个键不出现。）
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryConsumeQueueResponseBody {
    pub subscription_data: Option<Value>,
    pub filter_data: Option<String>,
    pub queue_data: Option<Vec<ConsumeQueueData>>,
    pub max_queue_index: i32,
    pub min_queue_index: i32,
}

impl QueryConsumeQueueResponseBody {
    pub fn to_json_value(&self) -> Value {
        let mut map = Map::new();
        map.insert("maxQueueIndex".to_string(), Value::from(self.max_queue_index));
        map.insert("minQueueIndex".to_string(), Value::from(self.min_queue_index));
        if let Some(v) = &self.subscription_data {
            map.insert("subscriptionData".to_string(), v.clone());
        }
        if let Some(v) = &self.filter_data {
            map.insert("filterData".to_string(), Value::String(v.clone()));
        }
        if let Some(list) = &self.queue_data {
            map.insert(
                "queueData".to_string(),
                Value::Array(list.iter().map(|q| q.to_json_value()).collect()),
            );
        }
        Value::Object(map)
    }

    pub fn from_json_value(value: &Value) -> Result<QueryConsumeQueueResponseBody> {
        expect_object(value, "QueryConsumeQueueResponseBody")?;
        let queue_data = match jfield(value, "queueData") {
            None => None,
            Some(arr) => {
                let items = arr
                    .as_array()
                    .ok_or_else(|| Error::Decode("queueData is not a json array".into()))?;
                let mut list = Vec::with_capacity(items.len());
                for item in items {
                    list.push(ConsumeQueueData::from_json_value(item)?);
                }
                // Python: `[..] if raw else None` —— 空数组等价于 None。
                Some(list)
            }
        };
        let queue_data = match queue_data {
            Some(list) if list.is_empty() => None,
            other => other,
        };
        Ok(QueryConsumeQueueResponseBody {
            subscription_data: jfield(value, "subscriptionData").cloned(),
            filter_data: jstring(value, "filterData"),
            queue_data,
            max_queue_index: jint(value, "maxQueueIndex", 0),
            min_queue_index: jint(value, "minQueueIndex", 0),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<QueryConsumeQueueResponseBody> {
        QueryConsumeQueueResponseBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java fastjson2 真实输出（内联对象键 —— 非法 JSON），来自
    /// `python/tests/test_admin_models.py::JAVA_TOPIC_STATS`。
    pub const JAVA_TOPIC_STATS: &str = concat!(
        r#"{"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:"#,
        r#"{"lastUpdateTimestamp":1700000000000,"maxOffset":500,"minOffset":0}},"#,
        r#""topicPutTps":0.0}"#
    );

    /// 来自 `python/tests/test_admin_models.py::JAVA_CONSUME_STATS`。
    pub const JAVA_CONSUME_STATS: &str = concat!(
        r#"{"consumeTps":1.5,"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:"#,
        r#"{"brokerOffset":100,"consumerOffset":90,"lastTimestamp":1700000000000,"pullOffset":0}}}"#
    );

    #[test]
    fn message_queue_key_matches_java_text() {
        let mq = MessageQueueKey::new("MyTopic", "broker-a", 3);
        assert_eq!(
            message_queue_key(&mq),
            r#"{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}"#
        );
        assert_eq!(mq.key_string(), message_queue_key(&mq));
    }

    #[test]
    fn plain_string_map_keys_are_not_message_queues() {
        // Python: parseMessageQueueKey("0"|"G1") -> false
        assert!(parse_message_queue_key("0").is_none());
        assert!(parse_message_queue_key("G1").is_none());
        assert!(parse_message_queue_key("").is_none());
        let mq = parse_message_queue_key(r#"{"brokerName":"b","queueId":1,"topic":"T"}"#).unwrap();
        assert_eq!(mq, MessageQueueKey::new("T", "b", 1));
    }

    #[test]
    fn topic_stats_table_decodes_java_inline_object_keys() {
        let table = TopicStatsTable::decode(JAVA_TOPIC_STATS.as_bytes()).unwrap();
        assert_eq!(table.offset_table.len(), 1);
        assert_eq!(table.offset_table.len(), 1);
        let (mq, offset) = &table.offset_table[0];
        assert_eq!((mq.topic.as_str(), mq.broker_name.as_str(), mq.queue_id), ("MyTopic", "broker-a", 3));
        assert_eq!(offset.min_offset, 0);
        assert_eq!(offset.max_offset, 500);
        assert_eq!(offset.last_update_timestamp, 1700000000000);
        assert_eq!(table.topic_put_tps, 0.0);
        assert_eq!(table.total_max_offset(), 500);
        // 重新编码后键文本仍然逐字节一致（broker 侧按文本比对位点表）。
        let again = TopicStatsTable::from_json_value(&table.to_json_value()).unwrap();
        assert_eq!(again.offset_table[0].0, *mq);
    }

    #[test]
    fn consume_stats_lag_and_total_lag() {
        let stats = ConsumeStats::decode(JAVA_CONSUME_STATS.as_bytes()).unwrap();
        assert_eq!(stats.consume_tps, 1.5);
        assert_eq!(stats.offset_table.len(), 1);
        let ow = &stats.offset_table[0].1;
        assert_eq!((ow.broker_offset, ow.consumer_offset, ow.pull_offset), (100, 90, 0));
        assert_eq!(ow.last_timestamp, 1700000000000);
        assert_eq!(ow.lag(), 10);
        assert_eq!(stats.total_lag(), 10);
    }

    #[test]
    fn two_entry_offset_table_keeps_report_order() {
        // python/tests/test_admin_models.py 的两队列夹具：totalMaxOffset 10
        let text = concat!(
            r#"{"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"T"}:"#,
            r#"{"minOffset":0,"maxOffset":8,"lastUpdateTimestamp":1789374000000},"#,
            r#"{"brokerName":"broker-a","queueId":1,"topic":"T"}:"#,
            r#"{"minOffset":0,"maxOffset":2,"lastUpdateTimestamp":1789374000001}}"#,
            r#","topicPutTps":0.0}"#
        );
        let table = TopicStatsTable::decode(text.as_bytes()).unwrap();
        assert_eq!(table.total_max_offset(), 10);
        assert_eq!(table.offset_table[0].0.queue_id, 0);
        assert_eq!(table.offset_table[0].1.max_offset, 8);
        assert_eq!(table.offset_table[1].1.last_update_timestamp, 1789374000001);
    }

    #[test]
    fn consume_stats_fixture_matches_python() {
        let text = concat!(
            r#"{"consumeTps":0.0,"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"T"}:"#,
            r#"{"brokerOffset":8,"consumerOffset":5,"lastTimestamp":1,"pullOffset":7}}}"#
        );
        let stats = ConsumeStats::decode(text.as_bytes()).unwrap();
        assert_eq!(stats.offset_table[0].1.lag(), 3);
        assert_eq!(stats.total_lag(), 3);
    }

    #[test]
    fn reset_style_long_value_map_decodes() {
        // admin_body 的通用键解码也被 body.rs 的 ResetOffsetBody 复用
        let value: Value = fastjson::from_str(
            r#"{"offsetTable":{{"brokerName":"b","queueId":0,"topic":"T"}:10,{"brokerName":"b","queueId":1,"topic":"T"}:20}}"#,
        )
        .unwrap();
        let table = decode_message_queue_map(jfield(&value, "offsetTable"), |v| {
            v.as_i64()
                .ok_or_else(|| Error::Decode("offset is not a long".into()))
        })
        .unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(table[0].1, 10);
        assert_eq!(table[1].1, 20);
    }

    #[test]
    fn topic_config_serialize_wrapper_passes_values_through() {
        let text =
            r#"{"dataVersion":{"counter":0,"timestamp":1},"topicConfigTable":{"t":{"perm":6}}}"#;
        let w = TopicConfigSerializeWrapper::decode(text.as_bytes()).unwrap();
        assert_eq!(w.data_version["counter"], 0);
        assert_eq!(w.topic_config_table.len(), 1);
        assert_eq!(w.topic_config_table[0].0, "t");
        assert_eq!(w.topic_config_table[0].1["perm"], 6);
        assert_eq!(w.to_json_value()["topicConfigTable"]["t"]["perm"], 6);
    }

    #[test]
    fn consume_queue_data_omits_null_optionals() {
        // 探针：{"bitMap":null,"eval":false,"physicOffset":0,"physicSize":100,"tagsCode":0}
        let d = ConsumeQueueData {
            physic_offset: 0,
            physic_size: 100,
            tags_code: 0,
            ..Default::default()
        };
        let json = d.to_json_value();
        assert_eq!(
            RemotingSerializable::to_json_string(&json),
            r#"{"physicOffset":0,"physicSize":100,"tagsCode":0,"eval":false,"bitMap":null}"#
        );
        assert!(json.get("extendDataJson").is_none());
        assert!(json.get("msg").is_none());
        let back = ConsumeQueueData::from_json_value(&json).unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn query_consume_queue_response_body_fixtures() {
        let text = concat!(
            r#"{"filterData":"*","maxQueueIndex":88,"minQueueIndex":1,"subscriptionData":"#,
            r#"{"classFilterMode":false,"expressionType":"TAG","subString":"*","topic":"t","version":1}}"#
        );
        let body = QueryConsumeQueueResponseBody::decode(text.as_bytes()).unwrap();
        assert_eq!(body.max_queue_index, 88);
        assert_eq!(body.min_queue_index, 1);
        assert_eq!(body.filter_data.as_deref(), Some("*"));
        assert!(body.queue_data.is_none());
        assert_eq!(body.subscription_data.as_ref().unwrap()["subString"], "*");

        let text2 = concat!(
            r#"{"maxQueueIndex":2,"minQueueIndex":0,"queueData":[{"bitMap":null,"eval":false,"#,
            r#""physicOffset":0,"physicSize":100,"tagsCode":0}]}"#
        );
        let body2 = QueryConsumeQueueResponseBody::decode(text2.as_bytes()).unwrap();
        assert_eq!(body2.queue_data.as_ref().unwrap().len(), 1);
        assert_eq!(body2.queue_data.as_ref().unwrap()[0].physic_size, 100);
        assert!(body2.subscription_data.is_none());
        assert!(body2.filter_data.is_none());
    }

    #[test]
    fn malformed_bodies_return_decode_errors() {
        assert!(matches!(
            TopicStatsTable::decode(b""),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            TopicStatsTable::decode(b"not-json"),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            TopicStatsTable::decode(b"[1,2]"),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            ConsumeStats::decode(b"{\"offsetTable\":[1]}"),
            Err(Error::Decode(_))
        ));
    }
}
