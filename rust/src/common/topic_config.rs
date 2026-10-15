//! ## Why the protocol layer does not depend on this type
//!
//! In `remoting::protocol::admin_body::TopicConfigSerializeWrapper`,
//! `topicConfigTable` holds raw [`Value`]s (the protocol layer does not depend on the common layer);
//! after the upper layer reads them, it converts them into the strongly typed form with [`TopicConfig::from_json_value`].

use std::fmt;

use serde_json::Value;

use crate::common::sysflag::PermName;
use crate::error::Result;
use crate::remoting::protocol::admin_body::{
    expect_object, jboolean, jfield, jint, json_object, jstring_or,
};
use crate::remoting::protocol::ext_fields::StringMap;
use crate::remoting::protocol::serialize::RemotingSerializable;

pub const DEFAULT_READ_QUEUE_NUMS: i32 = 16;
pub const DEFAULT_WRITE_QUEUE_NUMS: i32 = 16;
/// `PermName.PERM_READ | PermName.PERM_WRITE`.
pub const DEFAULT_PERM: i32 = 6;

pub struct TopicFilterType;

impl TopicFilterType {
    pub const SINGLE_TAG: &'static str = "SINGLE_TAG";
    pub const MULTI_TAG: &'static str = "MULTI_TAG";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfig {
    pub topic_name: String,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub topic_filter_type: String,
    pub topic_sys_flag: i32,
    pub order: bool,
    /// Order-preserving container: the insertion order of `+key` / `-key` entries is reflected in the wire text.
    pub attributes: StringMap,
}

impl Default for TopicConfig {
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
    pub fn new(topic_name: &str) -> TopicConfig {
        TopicConfig {
            topic_name: topic_name.to_string(),
            ..Default::default()
        }
    }

    /// Writes the compact JSON string (identical to the serialization of `to_json_value()`).
    pub fn encode(&self) -> String {
        self.to_json_value().to_string()
    }

    /// Key order is pinned to the recorded wire JSON literal.
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

    pub fn decode(text: &str) -> Result<TopicConfig> {
        let value = RemotingSerializable::decode(text.as_bytes())?;
        TopicConfig::from_json_value(&value)
    }

    /// Raw text of a key inside `attributes` (extended attributes such as `+cgt`).
    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key)
    }

    pub fn set_attribute(&mut self, key: &str, value: &str) {
        self.attributes.insert(key, value);
    }

    pub fn has_topic_name(&self) -> bool {
        !self.topic_name.is_empty()
    }
}

/// Text form for diagnostics (`perm` goes through `PermName::perm_to_string`).
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

    const TOPIC_CONFIG_ATTRIBUTES_FIRST: &str = concat!(
        r#"{"attributes":{"+fileReservedTime":"72","+deleteWhen":"04"},"order":false,"perm":6,"#,
        r#""readQueueNums":16,"topicFilterType":"SINGLE_TAG","topicName":"attr-topic","#,
        r#""topicSysFlag":0,"writeQueueNums":16}"#,
    );

    #[test]
    fn defaults_match_server_probe() {
        // Defaults follow the recorded broker-side ground truth
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
    fn encode_uses_recorded_key_order() {
        let mut cfg = TopicConfig::new("t");
        cfg.attributes.insert("+cgt", "10");
        cfg.attributes.insert("-delete", "true");
        assert_eq!(
            cfg.encode(),
            r#"{"topicName":"t","readQueueNums":16,"writeQueueNums":16,"perm":6,"topicFilterType":"SINGLE_TAG","topicSysFlag":0,"order":false,"attributes":{"+cgt":"10","-delete":"true"}}"#
        );
    }

    #[test]
    fn decode_server_output_keeps_attributes() {
        let cfg = TopicConfig::decode(TOPIC_CONFIG_ATTRIBUTES_FIRST).unwrap();
        assert_eq!(cfg.topic_name, "attr-topic");
        assert_eq!(cfg.attribute("+deleteWhen"), Some("04"));
        assert_eq!(cfg.attribute("+fileReservedTime"), Some("72"));
        assert_eq!(cfg.read_queue_nums, 16);
        assert_eq!(cfg.perm, 6);
        // Encoding after reading back still yields the same key order
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
        assert_eq!(cfg.read_queue_nums, 8, "numbers written as strings must still parse");
        assert!(!cfg.order, "null falls back to the default");
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
