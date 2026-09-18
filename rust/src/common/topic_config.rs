//! `TopicConfig`（对应 `org.apache.rocketmq.common.TopicConfig`，
//! Python `rocketmq/common/topic_config.py`）。
//!
//! 字段与默认值以 Java 5.x 为准（探针实测，勿凭记忆改）：
//! `new TopicConfig("t")` → readQueueNums=16, writeQueueNums=16, perm=6,
//! topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}。
//! `attributes` **会被序列化**（Java 的 `getAttributes()` 没有 `serialize=false`）。
//!
//! ## 键顺序：与 Python 一致，而不是与 Java 一致
//!
//! [`TopicConfig::to_json_value`] 按 Python `to_dict()` 的声明顺序写
//! （topicName → … → attributes）。Java `JSON.toJSONString` 走的是 POJO 字段
//! **字母序**（`attributes, order, perm, readQueueNums, topicFilterType, topicName,
//! topicSysFlag, writeQueueNums`），所以同一条配置的两端报文文本不同。broker 按 key
//! 名解析，顺序不影响语义；本仓库以 Python 为参照实现，故测试里的期望文本是 Python 顺序，
//! [`TOPIC_CONFIG_ATTRIBUTES_FIRST`] 只是留档的 Java 真值。
//!
//! ## 为什么协议层不引它
//!
//! `remoting::protocol::admin_body::TopicConfigSerializeWrapper` 里
//! `topicConfigTable` 存的是原始 [`Value`]（协议层不依赖 common 层），
//! 上层读到后再用 [`TopicConfig::from_json_value`] 转成强类型。

use std::fmt;

use serde_json::Value;

use crate::common::sysflag::PermName;
use crate::error::Result;
use crate::remoting::protocol::admin_body::{
    expect_object, jboolean, jfield, jint, json_object, jstring_or,
};
use crate::remoting::protocol::ext_fields::StringMap;
use crate::remoting::protocol::serialize::RemotingSerializable;

/// Java `TopicConfig.defaultReadQueueNums`。
pub const DEFAULT_READ_QUEUE_NUMS: i32 = 16;
/// Java `TopicConfig.defaultWriteQueueNums`。
pub const DEFAULT_WRITE_QUEUE_NUMS: i32 = 16;
/// `PermName.PERM_READ | PermName.PERM_WRITE`。
pub const DEFAULT_PERM: i32 = 6;

/// Java `TopicConfig.TopicFilterType` 枚举名；线上就是这两个字符串，
/// 所以与 Python 一样用 `&'static str` 而不是 Rust 枚举
/// （`CreateTopicRequestHeader.topicFilterType` 直接收字符串）。
pub struct TopicFilterType;

impl TopicFilterType {
    pub const SINGLE_TAG: &'static str = "SINGLE_TAG";
    pub const MULTI_TAG: &'static str = "MULTI_TAG";
}

/// 对应 Java `TopicConfig`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfig {
    pub topic_name: String,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub topic_filter_type: String,
    pub topic_sys_flag: i32,
    pub order: bool,
    /// 保序容器：`+key` / `-key` 的插入顺序会体现在报文文本里。
    pub attributes: StringMap,
}

impl Default for TopicConfig {
    /// Java 无参构造：topicName 为空串，其余取类里的默认值。
    fn default() -> TopicConfig {
        TopicConfig {
            topic_name: String::new(),
            read_queue_nums: DEFAULT_READ_QUEUE_NUMS,
            write_queue_nums: DEFAULT_WRITE_QUEUE_NUMS,
            perm: DEFAULT_PERM,
            topic_filter_type: TopicFilterType::SINGLE_TAG.to_string(),
            topic_sys_flag: 0,
            order: false,
            attributes: StringMap::new(),
        }
    }
}

impl TopicConfig {
    /// 对应 Java `new TopicConfig(topicName)` / Python `TopicConfig("t")`：
    /// 只有 topic 名可变，其余保持 Java 默认。
    pub fn new(topic_name: &str) -> TopicConfig {
        TopicConfig {
            topic_name: topic_name.to_string(),
            ..Default::default()
        }
    }

    /// 对应 Python `encode()`：`json.dumps(to_dict())`。
    pub fn encode(&self) -> String {
        self.to_json_value().to_string()
    }

    /// 对应 Python `to_dict()`，键顺序与 Python 字面量一致。
    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("topicName", Value::String(self.topic_name.clone())),
            ("readQueueNums", Value::from(self.read_queue_nums)),
            ("writeQueueNums", Value::from(self.write_queue_nums)),
            ("perm", Value::from(self.perm)),
            (
                "topicFilterType",
                Value::String(self.topic_filter_type.clone()),
            ),
            ("topicSysFlag", Value::from(self.topic_sys_flag)),
            ("order", Value::from(self.order)),
            ("attributes", self.attributes.to_json()),
        ])
    }

    /// 对应 Python `from_dict()`：缺键回 Java 默认值，脏数据不 panic。
    pub fn from_json_value(value: &Value) -> Result<TopicConfig> {
        expect_object(value, "TopicConfig")?;
        let mut attributes = StringMap::new();
        if let Some(obj) = jfield(value, "attributes") {
            attributes = StringMap::from_json(obj);
        }
        Ok(TopicConfig {
            topic_name: jstring_or(value, "topicName", ""),
            read_queue_nums: jint(value, "readQueueNums", DEFAULT_READ_QUEUE_NUMS),
            write_queue_nums: jint(value, "writeQueueNums", DEFAULT_WRITE_QUEUE_NUMS),
            perm: jint(value, "perm", DEFAULT_PERM),
            topic_filter_type: jstring_or(value, "topicFilterType", TopicFilterType::SINGLE_TAG),
            topic_sys_flag: jint(value, "topicSysFlag", 0),
            order: jboolean(value, "order", false),
            attributes,
        })
    }

    /// 对应 Python `decode(json_str)`。Python 用严格 `json.loads`；这里走
    /// [`RemotingSerializable::decode`]（严格失败再退 fastjson2 容忍解析），
    /// 这样 broker/Java 写出的内联对象键也能读。
    pub fn decode(text: &str) -> Result<TopicConfig> {
        let value = RemotingSerializable::decode(text.as_bytes())?;
        TopicConfig::from_json_value(&value)
    }

    /// `attributes` 里某个 key 的原始文本（`+cgt` 这类扩展属性）。
    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key)
    }

    pub fn set_attribute(&mut self, key: &str, value: &str) {
        self.attributes.insert(key, value);
    }

    /// Java 侧只认 topicName 非空；admin 用它挡掉「查不到配置」的空响应。
    pub fn has_topic_name(&self) -> bool {
        !self.topic_name.is_empty()
    }
}

/// 对应 Python `__repr__`（`perm` 走 `PermName.perm2string`）。
impl fmt::Display for TopicConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TopicConfig[topicName={}, readQueueNums={}, writeQueueNums={}, perm={}]",
            self.topic_name,
            self.read_queue_nums,
            self.write_queue_nums,
            PermName::perm_to_string(self.perm)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    /// Java 探针输出：`JSON.toJSONString(new TopicConfig("attr-topic"))`（字母序）。
    /// 本模块**不产出**这段文本，留作与 Java 对拍时的参照。
    const TOPIC_CONFIG_ATTRIBUTES_FIRST: &str = concat!(
        r#"{"attributes":{"+fileReservedTime":"72","+deleteWhen":"04"},"order":false,"perm":6,"#,
        r#""readQueueNums":16,"topicFilterType":"SINGLE_TAG","topicName":"attr-topic","#,
        r#""topicSysFlag":0,"writeQueueNums":16}"#,
    );

    #[test]
    fn java_defaults_match_probe() {
        // Python test_topic_config_java_defaults
        let cfg = TopicConfig::new("t");
        assert_eq!(cfg.read_queue_nums, 16);
        assert_eq!(cfg.write_queue_nums, 16);
        assert_eq!(cfg.perm, DEFAULT_PERM);
        assert_eq!(cfg.topic_filter_type, TopicFilterType::SINGLE_TAG);
        assert_eq!(cfg.topic_sys_flag, 0);
        assert!(!cfg.order);
        assert!(cfg.attributes.is_empty());
    }

    #[test]
    fn encode_uses_python_key_order() {
        let mut cfg = TopicConfig::new("t");
        cfg.attributes.insert("+cgt", "10");
        cfg.attributes.insert("-delete", "true");
        assert_eq!(
            cfg.encode(),
            r#"{"topicName":"t","readQueueNums":16,"writeQueueNums":16,"perm":6,"topicFilterType":"SINGLE_TAG","topicSysFlag":0,"order":false,"attributes":{"+cgt":"10","-delete":"true"}}"#
        );
    }

    #[test]
    fn decode_java_output_keeps_attributes() {
        // Java 文本能读回来（键顺序无关），attributes 逐项保留
        let cfg = TopicConfig::decode(TOPIC_CONFIG_ATTRIBUTES_FIRST).unwrap();
        assert_eq!(cfg.topic_name, "attr-topic");
        assert_eq!(cfg.attribute("+deleteWhen"), Some("04"));
        assert_eq!(cfg.attribute("+fileReservedTime"), Some("72"));
        assert_eq!(cfg.read_queue_nums, 16);
        assert_eq!(cfg.perm, 6);
        // 读回后再写出的是 Python 顺序
        assert!(cfg.encode().starts_with(r#"{"topicName":"attr-topic","#));
    }

    #[test]
    fn decode_tolerates_missing_and_null_fields() {
        let cfg = TopicConfig::decode(r#"{"topicName":"only"}"#).unwrap();
        assert_eq!(cfg.read_queue_nums, DEFAULT_READ_QUEUE_NUMS);
        assert_eq!(cfg.perm, DEFAULT_PERM);
        assert!(cfg.attributes.is_empty());

        let cfg = TopicConfig::decode(
            r#"{"topicName":"t","attributes":null,"order":null,"readQueueNums":"8"}"#,
        )
        .unwrap();
        assert_eq!(cfg.read_queue_nums, 8, "数字写成字符串也要能读");
        assert!(!cfg.order, "null 回默认值");
        assert!(cfg.attributes.is_empty());
    }

    #[test]
    fn decode_rejects_non_object() {
        assert!(TopicConfig::decode("[1,2]").is_err());
        assert!(matches!(
            TopicConfig::decode("null"),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn display_uses_perm_string() {
        assert_eq!(
            TopicConfig::new("t").to_string(),
            "TopicConfig[topicName=t, readQueueNums=16, writeQueueNums=16, perm=RW-]"
        );
        let mut cfg = TopicConfig::new("ro");
        cfg.perm = PermName::PERM_READ;
        assert_eq!(
            cfg.to_string(),
            "TopicConfig[topicName=ro, readQueueNums=16, writeQueueNums=16, perm=R--]"
        );
    }

    #[test]
    fn round_trip_keeps_every_field() {
        let mut cfg = TopicConfig::new("rt");
        cfg.read_queue_nums = 3;
        cfg.write_queue_nums = 2;
        cfg.perm = 7;
        cfg.topic_filter_type = TopicFilterType::MULTI_TAG.to_string();
        cfg.topic_sys_flag = 1;
        cfg.order = true;
        cfg.set_attribute("+k", "v");
        let text = cfg.encode();
        let back = TopicConfig::decode(&text).unwrap();
        assert_eq!(back, cfg);
    }
}
