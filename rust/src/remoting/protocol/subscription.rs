//! 订阅组模型（对应 `org.apache.rocketmq.remoting.protocol.subscription` 包）。
//!
//! 移植 `python/rocketmq/remoting/protocol/subscription.py`：
//! [`SubscriptionGroupConfig`] / [`GroupRetryPolicy`] / [`SimpleSubscriptionData`] /
//! [`SubscriptionGroupWrapper`]。admin（`examineSubscriptionGroupConfig` 等）与
//! broker 侧返回体都走这里。
//!
//! ## 报文细节
//!
//! * Java 字段名 `type` 在 Rust 是关键字，字段用 `retry_policy_type`，**上线键仍是 `type`**。
//! * fastjson2 跳过 null：`subscriptionDataSet` 为 `None` 时整个键不出现（探针实测），
//!   `groupRetryPolicy` 的两个子策略同理，所以这里用 `Option` 而不是空容器。
//! * 顶层键顺序按 Java 探针输出写出；本仓库 Python 的 `json.dumps` 带空格、
//!   Java fastjson 不带，Rust 统一走紧凑形式（[`RemotingSerializable::encode`]）。
//! * `attributes` 用 [`StringMap`] 保持插入顺序，避免签名/比对时抖动。

use serde_json::{Map, Value};

use super::admin_body::{
    expect_object, jboolean, jfield, jint, jlong, jstring_or, json_object,
};
use super::serialize::RemotingSerializable;
use crate::error::{Error, Result};
use crate::remoting::protocol::ext_fields::StringMap;

/// `MixAll.MASTER_ID`。
pub const MASTER_ID: i32 = 0;

/// 对应 Java `GroupRetryPolicyType`（枚举名直接作为 JSON 字符串写出）。
pub mod group_retry_policy_type {
    pub const EXPONENTIAL: &str = "EXPONENTIAL";
    pub const CUSTOMIZED: &str = "CUSTOMIZED";
}

/// 对应 Java `SimpleSubscriptionData`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleSubscriptionData {
    pub topic: String,
    pub expression_type: String,
    pub expression: String,
    pub version: i64,
}

impl Default for SimpleSubscriptionData {
    fn default() -> Self {
        SimpleSubscriptionData {
            topic: String::new(),
            expression_type: "TAG".to_string(),
            expression: "*".to_string(),
            version: 0,
        }
    }
}

impl SimpleSubscriptionData {
    pub fn new(topic: &str, expression_type: &str, expression: &str) -> SimpleSubscriptionData {
        SimpleSubscriptionData {
            topic: topic.to_string(),
            expression_type: expression_type.to_string(),
            expression: expression.to_string(),
            version: 0,
        }
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("topic", Value::String(self.topic.clone())),
            ("expressionType", Value::String(self.expression_type.clone())),
            ("expression", Value::String(self.expression.clone())),
            ("version", Value::from(self.version)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<SimpleSubscriptionData> {
        expect_object(value, "SimpleSubscriptionData")?;
        Ok(SimpleSubscriptionData {
            topic: jstring_or(value, "topic", ""),
            expression_type: jstring_or(value, "expressionType", "TAG"),
            expression: jstring_or(value, "expression", "*"),
            version: jlong(value, "version", 0),
        })
    }
}

/// 对应 Java `GroupRetryPolicy`。
///
/// 两个子策略保留 broker 原样 JSON：Java 侧类型是 `ExponentialRetryPolicy` /
/// `CustomizedRetryPolicy`，字段较多且客户端只透传给 admin，解析成结构化字段
/// 反而会在 broker 加字段时丢信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRetryPolicy {
    /// Java 字段名 `type`。
    pub retry_policy_type: String,
    pub exponential_retry_policy: Option<Value>,
    pub customized_retry_policy: Option<Value>,
}

impl Default for GroupRetryPolicy {
    fn default() -> Self {
        GroupRetryPolicy {
            retry_policy_type: group_retry_policy_type::CUSTOMIZED.to_string(),
            exponential_retry_policy: None,
            customized_retry_policy: None,
        }
    }
}

impl GroupRetryPolicy {
    pub fn to_json_value(&self) -> Value {
        let mut pairs = vec![("type", Value::String(self.retry_policy_type.clone()))];
        if let Some(v) = &self.exponential_retry_policy {
            pairs.push(("exponentialRetryPolicy", v.clone()));
        }
        if let Some(v) = &self.customized_retry_policy {
            pairs.push(("customizedRetryPolicy", v.clone()));
        }
        json_object(pairs)
    }

    pub fn from_json_value(value: &Value) -> Result<GroupRetryPolicy> {
        // broker 可能整体不下发 groupRetryPolicy，Java 此时用默认实例。
        if value.is_null() {
            return Ok(GroupRetryPolicy::default());
        }
        expect_object(value, "GroupRetryPolicy")?;
        Ok(GroupRetryPolicy {
            retry_policy_type: jstring_or(value, "type", group_retry_policy_type::CUSTOMIZED),
            exponential_retry_policy: jfield(value, "exponentialRetryPolicy").cloned(),
            customized_retry_policy: jfield(value, "customizedRetryPolicy").cloned(),
        })
    }
}

/// 对应 Java `SubscriptionGroupConfig`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionGroupConfig {
    pub group_name: String,
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    pub retry_queue_nums: i32,
    pub retry_max_times: i32,
    pub group_retry_policy: GroupRetryPolicy,
    pub broker_id: i32,
    pub which_broker_when_consume_slowly: i32,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
    pub consume_timeout_minute: i32,
    /// Java 默认 `null`：不出现该键，而不是空数组。
    pub subscription_data_set: Option<Vec<SimpleSubscriptionData>>,
    pub attributes: StringMap,
}

impl Default for SubscriptionGroupConfig {
    fn default() -> Self {
        SubscriptionGroupConfig {
            group_name: String::new(),
            consume_enable: true,
            consume_from_min_enable: true,
            consume_broadcast_enable: true,
            consume_message_orderly: false,
            retry_queue_nums: 1,
            retry_max_times: 16,
            group_retry_policy: GroupRetryPolicy::default(),
            broker_id: MASTER_ID,
            which_broker_when_consume_slowly: 1,
            notify_consumer_ids_changed_enable: true,
            group_sys_flag: 0,
            consume_timeout_minute: 15,
            subscription_data_set: None,
            attributes: StringMap::new(),
        }
    }
}

impl SubscriptionGroupConfig {
    pub fn new(group_name: &str) -> SubscriptionGroupConfig {
        SubscriptionGroupConfig {
            group_name: group_name.to_string(),
            ..SubscriptionGroupConfig::default()
        }
    }

    pub fn to_json_value(&self) -> Value {
        let mut pairs = vec![
            ("groupName", Value::String(self.group_name.clone())),
            ("consumeEnable", Value::from(self.consume_enable)),
            ("consumeFromMinEnable", Value::from(self.consume_from_min_enable)),
            ("consumeBroadcastEnable", Value::from(self.consume_broadcast_enable)),
            ("consumeMessageOrderly", Value::from(self.consume_message_orderly)),
            ("retryQueueNums", Value::from(self.retry_queue_nums)),
            ("retryMaxTimes", Value::from(self.retry_max_times)),
            ("groupRetryPolicy", self.group_retry_policy.to_json_value()),
            ("brokerId", Value::from(self.broker_id)),
            (
                "whichBrokerWhenConsumeSlowly",
                Value::from(self.which_broker_when_consume_slowly),
            ),
            (
                "notifyConsumerIdsChangedEnable",
                Value::from(self.notify_consumer_ids_changed_enable),
            ),
            ("groupSysFlag", Value::from(self.group_sys_flag)),
            ("consumeTimeoutMinute", Value::from(self.consume_timeout_minute)),
            ("attributes", self.attributes.to_json()),
        ];
        if let Some(set) = &self.subscription_data_set {
            pairs.push((
                "subscriptionDataSet",
                Value::Array(set.iter().map(|s| s.to_json_value()).collect()),
            ));
        }
        json_object(pairs)
    }

    pub fn from_json_value(value: &Value) -> Result<SubscriptionGroupConfig> {
        expect_object(value, "SubscriptionGroupConfig")?;
        let subscription_data_set = match jfield(value, "subscriptionDataSet") {
            None => None,
            Some(Value::Null) => None,
            Some(Value::Array(items)) => {
                let mut set = Vec::with_capacity(items.len());
                for item in items {
                    set.push(SimpleSubscriptionData::from_json_value(item)?);
                }
                Some(set)
            }
            Some(other) => {
                return Err(Error::Decode(format!(
                    "SubscriptionGroupConfig.subscriptionDataSet is not an array: {other}"
                )))
            }
        };
        Ok(SubscriptionGroupConfig {
            group_name: jstring_or(value, "groupName", ""),
            consume_enable: jboolean(value, "consumeEnable", true),
            consume_from_min_enable: jboolean(value, "consumeFromMinEnable", true),
            consume_broadcast_enable: jboolean(value, "consumeBroadcastEnable", true),
            consume_message_orderly: jboolean(value, "consumeMessageOrderly", false),
            retry_queue_nums: jint(value, "retryQueueNums", 1),
            retry_max_times: jint(value, "retryMaxTimes", 16),
            group_retry_policy: GroupRetryPolicy::from_json_value(
                jfield(value, "groupRetryPolicy").unwrap_or(&Value::Null),
            )?,
            broker_id: jint(value, "brokerId", MASTER_ID),
            which_broker_when_consume_slowly: jint(value, "whichBrokerWhenConsumeSlowly", 1),
            notify_consumer_ids_changed_enable: jboolean(
                value,
                "notifyConsumerIdsChangedEnable",
                true,
            ),
            group_sys_flag: jint(value, "groupSysFlag", 0),
            consume_timeout_minute: jint(value, "consumeTimeoutMinute", 15),
            subscription_data_set,
            attributes: match jfield(value, "attributes") {
                Some(v) => StringMap::from_json(v),
                None => StringMap::new(),
            },
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<SubscriptionGroupConfig> {
        SubscriptionGroupConfig::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

/// 对应 Java `SubscriptionGroupWrapper`（实际类在 `protocol.body` 包，
/// Python 与本文件一并移植，因为二者总是成对出现）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SubscriptionGroupWrapper {
    /// 保序：`getAllSubscriptionGroup` 的返回表按 broker 顺序透传。
    pub subscription_group_table: Vec<(String, SubscriptionGroupConfig)>,
    /// broker 侧为 `ConcurrentHashMap<String, ?>`，客户端只原样带出。
    pub forbidden_table: Value,
    /// 对应 Java `DataVersion`，同样只原样带出（admin 侧仅做展示）。
    pub data_version: Value,
}

impl SubscriptionGroupWrapper {
    pub fn new() -> SubscriptionGroupWrapper {
        SubscriptionGroupWrapper {
            forbidden_table: Value::Object(Map::new()),
            data_version: Value::Object(Map::new()),
            ..Default::default()
        }
    }

    pub fn get(&self, group_name: &str) -> Option<&SubscriptionGroupConfig> {
        self.subscription_group_table
            .iter()
            .find(|(k, _)| k == group_name)
            .map(|(_, v)| v)
    }

    pub fn insert(&mut self, config: SubscriptionGroupConfig) {
        let name = config.group_name.clone();
        match self
            .subscription_group_table
            .iter_mut()
            .find(|(k, _)| *k == name)
        {
            Some(slot) => slot.1 = config,
            None => self.subscription_group_table.push((name, config)),
        }
    }

    pub fn to_json_value(&self) -> Value {
        let mut table = Map::new();
        for (name, config) in &self.subscription_group_table {
            table.insert(name.clone(), config.to_json_value());
        }
        json_object(vec![
            ("dataVersion", self.data_version.clone()),
            ("forbiddenTable", self.forbidden_table.clone()),
            ("subscriptionGroupTable", Value::Object(table)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<SubscriptionGroupWrapper> {
        expect_object(value, "SubscriptionGroupWrapper")?;
        let mut wrapper = SubscriptionGroupWrapper::new();
        if let Some(Value::Object(entries)) = jfield(value, "subscriptionGroupTable") {
            for (name, item) in entries {
                wrapper
                    .subscription_group_table
                    .push((name.clone(), SubscriptionGroupConfig::from_json_value(item)?));
            }
        }
        if let Some(v) = jfield(value, "forbiddenTable") {
            if !v.is_null() {
                wrapper.forbidden_table = v.clone();
            }
        }
        if let Some(v) = jfield(value, "dataVersion") {
            if !v.is_null() {
                wrapper.data_version = v.clone();
            }
        }
        Ok(wrapper)
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<SubscriptionGroupWrapper> {
        SubscriptionGroupWrapper::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

/// 对应 Java `TopicQueueCountTable`（`admin.topicQueueCount` 用的极简体）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicQueueCountTable {
    pub table: Vec<(String, i64)>,
}

impl TopicQueueCountTable {
    pub fn to_json_value(&self) -> Value {
        let mut map = Map::new();
        for (topic, count) in &self.table {
            map.insert(topic.clone(), Value::from(*count));
        }
        Value::Object(map)
    }

    pub fn from_json_value(value: &Value) -> Result<TopicQueueCountTable> {
        expect_object(value, "TopicQueueCountTable")?;
        let mut table = Vec::new();
        if let Value::Object(map) = value {
            for (topic, count) in map {
                let count = count
                    .as_i64()
                    .ok_or_else(|| Error::Decode(format!("topicQueueCount[{topic}] is not a number")))?;
                table.push((topic.clone(), count));
            }
        }
        Ok(TopicQueueCountTable { table })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<TopicQueueCountTable> {
        TopicQueueCountTable::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Python `SubscriptionGroupConfig('MyGroup').encode()` 的紧凑形式。
    const GOLDEN_CONFIG: &str = r#"{"groupName":"MyGroup","consumeEnable":true,"consumeFromMinEnable":true,"consumeBroadcastEnable":true,"consumeMessageOrderly":false,"retryQueueNums":1,"retryMaxTimes":16,"groupRetryPolicy":{"type":"CUSTOMIZED"},"brokerId":0,"whichBrokerWhenConsumeSlowly":1,"notifyConsumerIdsChangedEnable":true,"groupSysFlag":0,"consumeTimeoutMinute":15,"attributes":{}}"#;

    #[test]
    fn default_config_matches_java_probe() {
        let encoded = String::from_utf8(SubscriptionGroupConfig::new("MyGroup").encode()).unwrap();
        assert_eq!(encoded, GOLDEN_CONFIG);
    }

    #[test]
    fn null_subscription_data_set_keeps_key_absent() {
        let cfg = SubscriptionGroupConfig::new("G");
        assert!(jfield(&cfg.to_json_value(), "subscriptionDataSet").is_none());

        let mut with_set = SubscriptionGroupConfig::new("MyGroup");
        let mut sub = SimpleSubscriptionData::new("TopicTest", "TAG", "*||TagA");
        sub.version = 1234;
        with_set.subscription_data_set = Some(vec![sub]);
        let value = with_set.to_json_value();
        let text = serde_json::to_string(&value).unwrap();
        assert!(
            text.ends_with(r#","subscriptionDataSet":[{"topic":"TopicTest","expressionType":"TAG","expression":"*||TagA","version":1234}]}"#),
            "subscriptionDataSet 必须排在 attributes 之后，got {text}"
        );
        let back = SubscriptionGroupConfig::from_json_value(&value).unwrap();
        assert_eq!(back, with_set);
    }

    #[test]
    fn decode_fills_java_defaults_for_absent_keys() {
        let cfg = SubscriptionGroupConfig::decode(br#"{"groupName":"G"}"#).unwrap();
        assert_eq!(cfg.group_name, "G");
        assert!(cfg.consume_enable);
        assert!(cfg.consume_from_min_enable);
        assert!(cfg.consume_broadcast_enable);
        assert!(!cfg.consume_message_orderly);
        assert_eq!(cfg.retry_queue_nums, 1);
        assert_eq!(cfg.retry_max_times, 16);
        assert_eq!(cfg.group_retry_policy.retry_policy_type, "CUSTOMIZED");
        assert_eq!(cfg.broker_id, MASTER_ID);
        assert_eq!(cfg.which_broker_when_consume_slowly, 1);
        assert!(cfg.notify_consumer_ids_changed_enable);
        assert_eq!(cfg.group_sys_flag, 0);
        assert_eq!(cfg.consume_timeout_minute, 15);
        assert!(cfg.subscription_data_set.is_none());
        assert!(cfg.attributes.is_empty());
    }

    #[test]
    fn retry_policy_sub_policies_only_when_present() {
        let mut policy = GroupRetryPolicy::default();
        assert_eq!(
            serde_json::to_string(&policy.to_json_value()).unwrap(),
            r#"{"type":"CUSTOMIZED"}"#
        );
        policy.retry_policy_type = group_retry_policy_type::EXPONENTIAL.to_string();
        policy.exponential_retry_policy = Some(serde_json::json!({"maxDeliveryAttempts":5}));
        let value = policy.to_json_value();
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#"{"type":"EXPONENTIAL","exponentialRetryPolicy":{"maxDeliveryAttempts":5}}"#
        );
        assert_eq!(GroupRetryPolicy::from_json_value(&value).unwrap(), policy);
        // broker 整体省略该字段时回到默认实例（Java 的字段初始化行为）。
        assert_eq!(
            GroupRetryPolicy::from_json_value(&Value::Null).unwrap(),
            GroupRetryPolicy::default()
        );
    }

    #[test]
    fn attributes_keep_insertion_order() {
        let mut cfg = SubscriptionGroupConfig::new("G");
        cfg.attributes.insert("+cgt", "10");
        cfg.attributes.insert("-delete", "true");
        let text = serde_json::to_string(&cfg.to_json_value()).unwrap();
        assert!(
            text.contains(r#""attributes":{"+cgt":"10","-delete":"true"}"#),
            "got {text}"
        );
        let back = SubscriptionGroupConfig::decode(&cfg.encode()).unwrap();
        assert_eq!(back, cfg);
    }

    #[test]
    fn wrapper_round_trip_keeps_table_order() {
        let mut wrapper = SubscriptionGroupWrapper::new();
        wrapper.insert(SubscriptionGroupConfig::new("GroupB"));
        let mut a = SubscriptionGroupConfig::new("GroupA");
        a.retry_max_times = 3;
        wrapper.insert(a.clone());
        wrapper.data_version = serde_json::json!({"stateVersion":1,"timestamp":1700000000000_i64,"counter":0});

        let encoded = wrapper.encode();
        let text = String::from_utf8(encoded.clone()).unwrap();
        assert!(
            text.starts_with(r#"{"dataVersion":{"stateVersion":1,"timestamp":1700000000000,"counter":0},"forbiddenTable":{},"subscriptionGroupTable":{"GroupB":"#),
            "顶层与表内键序必须稳定，got {text}"
        );
        let back = SubscriptionGroupWrapper::decode(&encoded).unwrap();
        assert_eq!(back.subscription_group_table.len(), 2);
        assert_eq!(back.get("GroupA").unwrap().retry_max_times, 3);
        assert_eq!(back, wrapper);

        // insert 覆盖同名项且不改变位置
        let mut updated = SubscriptionGroupConfig::new("GroupA");
        updated.retry_max_times = 9;
        wrapper.insert(updated);
        assert_eq!(wrapper.subscription_group_table[1].0, "GroupA");
        assert_eq!(wrapper.get("GroupA").unwrap().retry_max_times, 9);
    }

    #[test]
    fn wrapper_tolerates_missing_and_null_sections() {
        let w = SubscriptionGroupWrapper::decode(b"{}").unwrap();
        assert!(w.subscription_group_table.is_empty());
        assert_eq!(w.forbidden_table, Value::Object(Map::new()));
        let w = SubscriptionGroupWrapper::decode(
            br#"{"dataVersion":null,"forbiddenTable":null,"subscriptionGroupTable":null}"#,
        )
        .unwrap();
        assert!(w.subscription_group_table.is_empty());
        assert!(
            SubscriptionGroupWrapper::decode(br#"{"subscriptionGroupTable":"x"}"#).is_ok()
        );
        assert!(matches!(
            SubscriptionGroupConfig::decode(b"[1,2]"),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            SubscriptionGroupConfig::decode(br#"{"subscriptionDataSet":"nope"}"#),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn topic_queue_count_table_round_trip() {
        let t = TopicQueueCountTable {
            table: vec![("TopicA".to_string(), 8), ("TopicB".to_string(), 0)],
        };
        let text = String::from_utf8(t.encode()).unwrap();
        assert_eq!(text, r#"{"TopicA":8,"TopicB":0}"#);
        assert_eq!(TopicQueueCountTable::decode(text.as_bytes()).unwrap(), t);
        assert!(matches!(
            TopicQueueCountTable::decode(br#"{"TopicA":"many"}"#),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn simple_subscription_data_defaults() {
        let d = SimpleSubscriptionData::from_json_value(&Value::Object(Map::new())).unwrap();
        assert_eq!(d.expression_type, "TAG");
        assert_eq!(d.expression, "*");
        assert_eq!(d.topic, "");
        assert_eq!(d.version, 0);
    }
}
