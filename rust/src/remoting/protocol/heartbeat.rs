//! Heartbeat payload models: `ProducerData` / `ConsumerData` / `HeartbeatData`, plus
//! the [`SubscriptionData`] needed by `ConsumerData.subscriptionDataSet`.
//!
//! ## Why this layer carries its own `SubscriptionData`
//!
//!
//! ## Payload details

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use super::admin_body::{expect_object, jarray, jboolean, jlong, json_object, jstring_or};
use super::serialize::RemotingSerializable;
use crate::error::{Error, Result};

/// `System.currentTimeMillis()`: the source of the `subVersion` default.
pub fn now_millis() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
    }
}

// ---------------------------------------------------------------- enum constants

pub struct ConsumeType;

impl ConsumeType {
    /// Active consumption (pull client)
    pub const CONSUME_ACTIVELY: &'static str = "CONSUME_ACTIVELY";
    /// Passive consumption (push client)
    pub const CONSUME_PASSIVELY: &'static str = "CONSUME_PASSIVELY";
    pub const CONSUME_POP: &'static str = "CONSUME_POP";
}

/// `MessageModel`
pub struct MessageModel;

impl MessageModel {
    pub const BROADCASTING: &'static str = "BROADCASTING";
    pub const CLUSTERING: &'static str = "CLUSTERING";
    pub const LITE_SELECTIVE: &'static str = "LITE_SELECTIVE";
}

/// `ConsumeFromWhere`
pub struct ConsumeFromWhere;

impl ConsumeFromWhere {
    pub const CONSUME_FROM_LAST_OFFSET: &'static str = "CONSUME_FROM_LAST_OFFSET";
    pub const CONSUME_FROM_FIRST_OFFSET: &'static str = "CONSUME_FROM_FIRST_OFFSET";
    pub const CONSUME_FROM_TIMESTAMP: &'static str = "CONSUME_FROM_TIMESTAMP";
}

/// `ExpressionType`
pub struct ExpressionType;

impl ExpressionType {
    pub const TAG: &'static str = "TAG";
    pub const SQL92: &'static str = "SQL92";
    pub const CLASS_FILTER: &'static str = "CLASS_FILTER";

    /// `ExpressionType.isTagType`: null, empty and `TAG` all count as the tag type. Anything
    /// else (SQL92, class filter) is broker-side filtered and must keep `tagsSet` empty.
    pub fn is_tag_type(type_: &str) -> bool {
        type_.is_empty() || type_ == Self::TAG
    }
}

// ---------------------------------------------------------------- SubscriptionData

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionData {
    pub class_filter_mode: bool,
    pub topic: String,
    pub sub_string: String,
    pub tags_set: Vec<String>,
    pub code_set: Vec<i32>,
    pub sub_version: i64,
    pub expression_type: String,
}

impl Default for SubscriptionData {
    fn default() -> Self {
        // subVersion takes the current millisecond timestamp.
        SubscriptionData {
            class_filter_mode: false,
            topic: String::new(),
            sub_string: String::new(),
            tags_set: Vec::new(),
            code_set: Vec::new(),
            sub_version: now_millis(),
            expression_type: ExpressionType::TAG.to_string(),
        }
    }
}

impl SubscriptionData {
    pub fn new(topic: impl Into<String>, sub_string: impl Into<String>) -> SubscriptionData {
        SubscriptionData {
            topic: topic.into(),
            sub_string: sub_string.into(),
            ..SubscriptionData::default()
        }
    }

    /// Sets the tag set and the matching hash set in one go: **dedup + ascending** (the order goes into the payload).
    pub fn set_tags(&mut self, tags: Vec<String>, codes: Vec<i32>) {
        let mut tags: Vec<String> = tags;
        tags.sort();
        tags.dedup();
        let mut codes: Vec<i32> = codes;
        codes.sort();
        codes.dedup();
        self.tags_set = tags;
        self.code_set = codes;
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("classFilterMode", Value::from(self.class_filter_mode)),
            ("topic", Value::String(self.topic.clone())),
            ("subString", Value::String(self.sub_string.clone())),
            (
                "tagsSet",
                Value::Array(self.tags_set.iter().cloned().map(Value::String).collect()),
            ),
            (
                "codeSet",
                Value::Array(self.code_set.iter().map(|c| Value::from(*c)).collect()),
            ),
            ("subVersion", Value::from(self.sub_version)),
            ("expressionType", Value::String(self.expression_type.clone())),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<SubscriptionData> {
        expect_object(value, "SubscriptionData")?;
        let mut tags_set: Vec<String> = Vec::new();
        for item in jarray(value, "tagsSet")? {
            let tag = match item {
                Value::String(s) => s.clone(),
                v if v.is_number() || v.is_boolean() => v.to_string(),
                v => {
                    return Err(Error::Decode(format!(
                        "SubscriptionData.tagsSet item {:?} is not a string",
                        v
                    )))
                }
            };
            tags_set.push(tag);
        }
        let mut code_set: Vec<i32> = Vec::new();
        for item in jarray(value, "codeSet")? {
            // The broker's codeSet contains both numbers and numeric strings; read both.
            let code = match item {
                Value::Number(n) => n
                    .as_i64()
                    .or_else(|| n.as_f64().map(|f| f as i64))
                    .or_else(|| n.as_u64().map(|v| v as i64))
                    .map(|v| v as i32),
                Value::String(s) => s.trim().parse::<i32>().ok(),
                _ => None,
            }
            .ok_or_else(|| Error::Decode(format!("codeSet item {:?} is not an int", item)))?;
            code_set.push(code);
        }
        tags_set.sort();
        tags_set.dedup();
        code_set.sort();
        code_set.dedup();
        Ok(SubscriptionData {
            // classFilterMode defaults to false (only a real true / nonzero counts)
            class_filter_mode: jboolean(value, "classFilterMode", false),
            topic: jstring_or(value, "topic", ""),
            sub_string: jstring_or(value, "subString", ""),
            tags_set,
            code_set,
            sub_version: jlong(value, "subVersion", 0),
            expression_type: jstring_or(value, "expressionType", ExpressionType::TAG),
        })
    }
}

// ---------------------------------------------------------------- FilterAPI

pub struct FilterAPI;

impl FilterAPI {
    pub const SUB_ALL: &'static str = "*";

    /// Builds subscription data from a subscription expression.
    pub fn build_subscription_data(
        topic: &str,
        sub_string: Option<&str>,
    ) -> Result<SubscriptionData> {
        let mut sub = SubscriptionData::new(topic, sub_string.unwrap_or(""));
        let raw = match sub_string {
            None => return Ok(normalise_sub_all(sub)),
            Some(s) => s,
        };
        if raw.is_empty() || raw == Self::SUB_ALL {
            return Ok(normalise_sub_all(sub));
        }
        let mut parts: Vec<&str> = raw.split("||").collect();
        while parts.last() == Some(&"") {
            parts.pop();
        }
        if parts.is_empty() {
            return Err(Error::client("subString split error"));
        }
        let mut tags: Vec<String> = Vec::new();
        let mut codes: Vec<i32> = Vec::new();
        for part in parts {
            let tag = part.trim();
            if !tag.is_empty() {
                tags.push(tag.to_string());
                codes.push(crate::common::util_all::java_string_hash(tag));
            }
        }
        // set_tags takes care of dedup + ascending order.
        sub.set_tags(tags, codes);
        Ok(sub)
    }

    /// `FilterAPI.build(topic, subString, type)`: the entry a `MessageSelector` goes through.
    ///
    /// Exactly `TAG` or an absent type compiles the tag expression into `tagsSet`/`codeSet`; any
    /// other type (SQL92 in practice) forwards the raw expression and leaves **both collections
    /// empty**, which is also what switches off the client-side re-filter when a pull result is
    /// post-processed (that one only runs when `tagsSet` is non-empty).
    /// An empty expression for a non-tag type is rejected.
    pub fn build(topic: &str, sub_string: &str, expression_type: Option<&str>) -> Result<SubscriptionData> {
        let type_ = match expression_type {
            None => return Self::build_subscription_data(topic, Some(sub_string)),
            Some(t) => t,
        };
        if type_ == ExpressionType::TAG {
            let mut sub = Self::build_subscription_data(topic, Some(sub_string))?;
            sub.expression_type = type_.to_string();
            return Ok(sub);
        }
        if sub_string.is_empty() {
            return Err(Error::client(format!("Expression can't be null! {type_}")));
        }
        Ok(SubscriptionData {
            topic: topic.to_string(),
            sub_string: sub_string.to_string(),
            expression_type: type_.to_string(),
            ..SubscriptionData::default()
        })
    }
}

fn normalise_sub_all(mut sub: SubscriptionData) -> SubscriptionData {
    sub.sub_string = FilterAPI::SUB_ALL.to_string();
    sub.tags_set.clear();
    sub.code_set.clear();
    sub
}

// ---------------------------------------------------------------- ProducerData

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProducerData {
    pub group_name: String,
}

impl ProducerData {
    pub fn new(group_name: impl Into<String>) -> ProducerData {
        ProducerData {
            group_name: group_name.into(),
        }
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![("groupName", Value::String(self.group_name.clone()))])
    }

    pub fn from_json_value(value: &Value) -> Result<ProducerData> {
        expect_object(value, "ProducerData")?;
        Ok(ProducerData {
            group_name: jstring_or(value, "groupName", ""),
        })
    }
}

// ---------------------------------------------------------------- ConsumerData

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerData {
    pub group_name: String,
    pub consume_type: String,
    pub message_model: String,
    pub consume_from_where: String,
    pub subscription_data_set: Vec<SubscriptionData>,
    pub unit_mode: bool,
}

impl Default for ConsumerData {
    fn default() -> Self {
        ConsumerData {
            group_name: String::new(),
            consume_type: ConsumeType::CONSUME_PASSIVELY.to_string(),
            message_model: MessageModel::CLUSTERING.to_string(),
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string(),
            subscription_data_set: Vec::new(),
            unit_mode: false,
        }
    }
}

impl ConsumerData {
    pub fn new(
        group_name: impl Into<String>,
        consume_type: impl Into<String>,
        message_model: impl Into<String>,
        consume_from_where: impl Into<String>,
    ) -> ConsumerData {
        ConsumerData {
            group_name: group_name.into(),
            consume_type: consume_type.into(),
            message_model: message_model.into(),
            consume_from_where: consume_from_where.into(),
            subscription_data_set: Vec::new(),
            unit_mode: false,
        }
    }

    /// Adds a subscription (set semantics: duplicates are folded away).
    pub fn add_subscription_data(&mut self, data: SubscriptionData) {
        if !self.subscription_data_set.contains(&data) {
            self.subscription_data_set.push(data);
        }
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("groupName", Value::String(self.group_name.clone())),
            ("consumeType", Value::String(self.consume_type.clone())),
            ("messageModel", Value::String(self.message_model.clone())),
            (
                "consumeFromWhere",
                Value::String(self.consume_from_where.clone()),
            ),
            (
                "subscriptionDataSet",
                Value::Array(
                    self.subscription_data_set
                        .iter()
                        .map(|s| s.to_json_value())
                        .collect(),
                ),
            ),
            ("unitMode", Value::from(self.unit_mode)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<ConsumerData> {
        expect_object(value, "ConsumerData")?;
        let mut subscription_data_set = Vec::new();
        for item in jarray(value, "subscriptionDataSet")? {
            subscription_data_set.push(SubscriptionData::from_json_value(item)?);
        }
        Ok(ConsumerData {
            group_name: jstring_or(value, "groupName", ""),
            consume_type: jstring_or(value, "consumeType", ConsumeType::CONSUME_PASSIVELY),
            message_model: jstring_or(value, "messageModel", MessageModel::CLUSTERING),
            consume_from_where: jstring_or(
                value,
                "consumeFromWhere",
                ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
            ),
            subscription_data_set,
            unit_mode: jboolean(value, "unitMode", false),
        })
    }
}

// ---------------------------------------------------------------- HeartbeatData

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatData {
    pub client_id: String,
    pub producer_data_set: Vec<ProducerData>,
    pub consumer_data_set: Vec<ConsumerData>,
    /// Heartbeat V2 fingerprint (`HeartbeatData.heartbeatFingerprint`). The V1 path keeps the
    /// 0 default, which is what makes the broker take its legacy registration branch; the V2
    /// path sets it from [`compute_heartbeat_fingerprint`](Self::compute_heartbeat_fingerprint).
    pub heartbeat_fingerprint: i32,
    /// Heartbeat V2 "no subscriptions in this body" marker
    /// (`HeartbeatData.isWithoutSub`). See [`without_sub_copy`](Self::without_sub_copy).
    pub without_sub: bool,
}

impl Default for HeartbeatData {
    fn default() -> Self {
        HeartbeatData::new("")
    }
}

/// `HeartbeatV2Result`: what one V2 heartbeat round answers.
///
/// A broker that predates heartbeat V2 answers `HEART_BEAT` with plain `SUCCESS` and **no**
/// extFields, which decodes to `support_v2 = false` / `sub_change = false`: the client then
/// keeps sending full V1-style bodies (fingerprint 0) to that address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatV2Result {
    /// `version`: the broker's protocol version, taken from the response header the same way
    /// the V1 path takes it (`sendHeartbeat` returns it into `brokerVersionTable`).
    pub version: i32,
    /// `isSubChange` (`MixAll.IS_SUB_CHANGE`): the fingerprint the broker holds for one of the
    /// groups differs from the one just sent, so the client must drop its cached fingerprint and
    /// send the subscriptions again on the next beat.
    pub sub_change: bool,
    /// `isSupportV2` (`MixAll.IS_SUPPORT_HEART_BEAT_V2`): the broker understood the V2 body, so
    /// later beats to this address may use the subscription-free body.
    pub support_v2: bool,
}

impl HeartbeatV2Result {
    pub fn new(version: i32, sub_change: bool, support_v2: bool) -> HeartbeatV2Result {
        HeartbeatV2Result {
            version,
            sub_change,
            support_v2,
        }
    }

    /// Parses the two boolean extFields of a `HEART_BEAT` response. Absent, empty or non-"true"
    /// values all read as false: these flags are lenient strings on the wire, and
    /// anything the broker did not echo as "true" is not set.
    pub fn from_response(response: &crate::remoting::protocol::remoting_command::RemotingCommand) -> HeartbeatV2Result {
        HeartbeatV2Result::new(
            response.version,
            is_ext_true(response, crate::common::mix_all::MixAll::IS_SUB_CHANGE),
            is_ext_true(
                response,
                crate::common::mix_all::MixAll::IS_SUPPORT_HEART_BEAT_V2,
            ),
        )
    }
}

fn is_ext_true(
    response: &crate::remoting::protocol::remoting_command::RemotingCommand,
    key: &str,
) -> bool {
    response
        .get_ext_field(key)
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

impl HeartbeatData {
    pub fn new(client_id: impl Into<String>) -> HeartbeatData {
        HeartbeatData {
            client_id: client_id.into(),
            producer_data_set: Vec::new(),
            consumer_data_set: Vec::new(),
            heartbeat_fingerprint: 0,
            without_sub: false,
        }
    }

    /// [`Self::compute_heartbeat_fingerprint`]: a fingerprint over the **subscription
    /// content** of this beat, deliberately blind to everything that churns between beats:
    /// every `subVersion` (a wall-clock timestamp), `clientID`, `withoutSub` and the fingerprint
    /// field itself. Two beats with the same group/subscription set therefore hash equal, and
    /// the broker's per-group table can answer "did this client change its subscriptions?".
    ///
    /// The value is the 31-multiplier integer string hash of the canonical JSON, i.e. the same hash family
    /// `codeSet` uses; it only has to be stable **within this process**, since the broker just
    /// compares consecutive fingerprints from the same client id.
    pub fn compute_heartbeat_fingerprint(&self) -> i32 {
        let mut copy = self.clone();
        for consumer in &mut copy.consumer_data_set {
            for sub in &mut consumer.subscription_data_set {
                sub.sub_version = 0;
            }
        }
        copy.without_sub = false;
        copy.heartbeat_fingerprint = 0;
        copy.client_id = String::new();
        crate::common::util_all::java_string_hash(&RemotingSerializable::to_json_string(
            &copy.to_json_value(),
        ))
    }

    /// `prepareHeartbeatData(true)` equivalent: the same registration without the subscription
    /// sets, flagged `withoutSub`, carrying the fingerprint computed from the **full** body.
    /// The broker keeps the subscriptions it already holds for this channel as long as the
    /// fingerprint matches, which is what makes the cheap beat cheap.
    pub fn without_sub_copy(&self) -> HeartbeatData {
        let mut copy = self.clone();
        for consumer in &mut copy.consumer_data_set {
            consumer.subscription_data_set.clear();
        }
        copy.without_sub = true;
        copy
    }

    /// Set semantics: duplicate groups / duplicate consumers are folded.
    pub fn add_producer_data(&mut self, data: ProducerData) {
        if !self.producer_data_set.contains(&data) {
            self.producer_data_set.push(data);
        }
    }

    pub fn add_consumer_data(&mut self, data: ConsumerData) {
        if !self.consumer_data_set.contains(&data) {
            self.consumer_data_set.push(data);
        }
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("clientID", Value::String(self.client_id.clone())),
            (
                "consumerDataSet",
                Value::Array(
                    self.consumer_data_set
                        .iter()
                        .map(|c| c.to_json_value())
                        .collect(),
                ),
            ),
            ("heartbeatFingerprint", Value::from(i64::from(self.heartbeat_fingerprint))),
            (
                "producerDataSet",
                Value::Array(
                    self.producer_data_set
                        .iter()
                        .map(|p| p.to_json_value())
                        .collect(),
                ),
            ),
            ("withoutSub", Value::from(self.without_sub)),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<HeartbeatData> {
        expect_object(value, "HeartbeatData")?;
        let mut producer_data_set = Vec::new();
        for item in jarray(value, "producerDataSet")? {
            producer_data_set.push(ProducerData::from_json_value(item)?);
        }
        let mut consumer_data_set = Vec::new();
        for item in jarray(value, "consumerDataSet")? {
            consumer_data_set.push(ConsumerData::from_json_value(item)?);
        }
        let without_sub = jboolean(value, "withoutSub", jboolean(value, "isWithoutSub", false));
        Ok(HeartbeatData {
            client_id: jstring_or(value, "clientID", ""),
            producer_data_set,
            consumer_data_set,
            heartbeat_fingerprint: jlong(value, "heartbeatFingerprint", 0) as i32,
            without_sub,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<HeartbeatData> {
        HeartbeatData::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

/// Reads out the scalar fields other than `subscriptionDataSet`, for upper-layer logging/debugging.
pub fn consumer_data_summary(data: &ConsumerData) -> String {
    format!(
        "ConsumerData [groupName={}, consumeType={}, messageModel={}, consumeFromWhere={}]",
        data.group_name, data.consume_type, data.message_model, data.consume_from_where
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_data_json_uses_wire_field_names_and_drops_filter_class_source() {
        let mut sd = SubscriptionData::new("TopicProbe", "TagA||TagB");
        sd.set_tags(
            vec!["TagB".into(), "TagA".into(), "TagA".into()],
            vec![2598920, 2598919, 2598919],
        );
        sd.class_filter_mode = false;
        sd.sub_version = 1700000000000;
        let text = RemotingSerializable::to_json_string(&sd.to_json_value());
        assert!(text.contains(r#""classFilterMode":false"#));
        assert!(text.contains(r#""subString":"TagA||TagB"#));
        assert!(text.contains(r#""subVersion":1700000000000"#), "subVersion must be an integer");
        assert!(text.contains(r#""expressionType":"TAG""#));
        assert!(text.contains(r#""topic":"TopicProbe""#));
        assert!(text.contains(r#""tagsSet":["TagA","TagB"]"#));
        assert!(text.contains(r#""codeSet":[2598919,2598920]"#));
        assert!(!text.contains("filterClassSource"));
        assert!(!text.contains("class_filter_mode"), "no snake_case keys allowed");
        assert_eq!(SubscriptionData::from_json_value(&sd.to_json_value()).unwrap(), sd);

        let mut other = sd.clone();
        other.sub_version = 1700000000001;
        assert_ne!(other, sd);
    }

    #[test]
    fn subscription_data_sub_version_defaults_to_now() {
        let fresh = SubscriptionData::default();
        assert!(fresh.sub_version > 1_600_000_000_000, "default should be on the order of current milliseconds");
        assert_eq!(fresh.expression_type, "TAG");
        assert!(fresh.tags_set.is_empty() && fresh.code_set.is_empty());
    }

    /// Byte-for-byte guard of the heartbeat JSON: key order and fields are pinned to the payload.
    #[test]
    fn heartbeat_exact_json_matches_fastjson2() {
        let mut hb = HeartbeatData::new("cid");
        hb.add_producer_data(ProducerData::new("pg"));
        assert_eq!(
            RemotingSerializable::to_json_string(&hb.to_json_value()),
            r#"{"clientID":"cid","consumerDataSet":[],"heartbeatFingerprint":0,"producerDataSet":[{"groupName":"pg"}],"withoutSub":false}"#
        );
        // Stable: encoding the same object twice is byte-for-byte identical (ordered sets, no randomness).
        assert_eq!(hb.encode(), hb.encode());
    }

    #[test]
    fn consumer_data_json_field_guards() {
        let mut cd = ConsumerData::new(
            "cg_probe",
            ConsumeType::CONSUME_PASSIVELY,
            MessageModel::CLUSTERING,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
        );
        let mut sd = SubscriptionData::new("TopicProbe", "TagA||TagB");
        sd.set_tags(vec!["TagA".into(), "TagB".into()], vec![2598919, 2598920]);
        sd.sub_version = 1700000000000;
        cd.add_subscription_data(sd.clone());
        cd.add_subscription_data(sd);
        assert_eq!(cd.subscription_data_set.len(), 1, "duplicate subscription folded (set semantics)");
        let text = RemotingSerializable::to_json_string(&cd.to_json_value());
        assert!(text.contains(r#""groupName":"cg_probe""#));
        assert!(text.contains(r#""consumeType":"CONSUME_PASSIVELY""#));
        assert!(text.contains(r#""messageModel":"CLUSTERING""#));
        assert!(text.contains(r#""consumeFromWhere":"CONSUME_FROM_LAST_OFFSET""#));
        assert!(text.contains(r#""unitMode":false"#));
        assert!(text.contains(r#""subscriptionDataSet":["#));
        assert!(!text.contains("consumeTimestamp"), "server 5.x has no such field");
        assert!(!text.contains("maxReconsumeTimes"), "server 5.x has no such field");
        assert_eq!(ConsumerData::from_json_value(&cd.to_json_value()).unwrap(), cd);
        assert_eq!(
            consumer_data_summary(&cd),
            "ConsumerData [groupName=cg_probe, consumeType=CONSUME_PASSIVELY, messageModel=CLUSTERING, consumeFromWhere=CONSUME_FROM_LAST_OFFSET]"
        );
    }

    #[test]
    fn heartbeat_round_trip_keeps_nested_data() {
        let mut cd = ConsumerData::new(
            "cg_probe",
            ConsumeType::CONSUME_PASSIVELY,
            MessageModel::CLUSTERING,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
        );
        let mut sd = SubscriptionData::new("TopicProbe", "TagA||TagB");
        sd.set_tags(vec!["TagA".into(), "TagB".into()], vec![2598919, 2598920]);
        sd.sub_version = 1700000000000;
        cd.add_subscription_data(sd.clone());

        let mut full = HeartbeatData::new("10.0.0.1@12345");
        full.add_producer_data(ProducerData::new("pg_probe"));
        full.add_consumer_data(cd.clone());
        let text = RemotingSerializable::to_json_string(&full.to_json_value());
        assert!(text.contains(r#""clientID":"10.0.0.1@12345""#));
        assert!(text.contains(r#""heartbeatFingerprint":0"#));
        assert!(text.contains(r#""withoutSub":false"#));
        assert!(!text.contains("isWithoutSub"), "writes withoutSub, not isWithoutSub");

        let decoded = HeartbeatData::decode(&full.encode()).unwrap();
        assert_eq!(decoded.client_id, full.client_id);
        assert_eq!(decoded.producer_data_set, vec![ProducerData::new("pg_probe")]);
        assert_eq!(decoded.consumer_data_set.len(), 1);
        assert_eq!(decoded.consumer_data_set[0].group_name, "cg_probe");
        assert_eq!(decoded.consumer_data_set[0].subscription_data_set, vec![sd]);
        assert_eq!(decoded.consumer_data_set[0], cd);
        assert_eq!(decoded.heartbeat_fingerprint, 0);
        assert!(!decoded.without_sub);
    }

    #[test]
    fn heartbeat_reads_is_without_sub_alias() {
        let text = r#"{"clientID":"c","isWithoutSub":true,"heartbeatFingerprint":7,"consumerDataSet":[],"producerDataSet":[]}"#;
        let hb = HeartbeatData::decode(text.as_bytes()).unwrap();
        assert!(hb.without_sub);
        assert_eq!(hb.heartbeat_fingerprint, 7);
        // Encoding now writes what the object holds: the V2 fields survive a round trip, so a
        // fingerprint the V2 path set is actually put on the wire.
        let back = RemotingSerializable::to_json_string(&hb.to_json_value());
        assert!(back.contains(r#""heartbeatFingerprint":7"#));
        assert!(back.contains(r#""withoutSub":true"#));
        assert_eq!(HeartbeatData::decode(back.as_bytes()).unwrap(), hb);
    }

    /// The fingerprint must ignore everything that churns between two beats of the same
    /// subscription set: `subVersion` (wall clock), `clientID`, `withoutSub` and the field itself.
    #[test]
    fn heartbeat_fingerprint_ignores_sub_version_client_id_and_its_own_fields() {
        let mut base = HeartbeatData::new("10.0.0.1@1");
        let mut cd = ConsumerData::new("cg", ConsumeType::CONSUME_PASSIVELY, MessageModel::CLUSTERING, ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
        cd.add_subscription_data(SubscriptionData {
            sub_version: 1,
            ..SubscriptionData::new("TopicA", "TagA")
        });
        base.add_consumer_data(cd);
        base.add_producer_data(ProducerData::new("pg"));

        let first = base.compute_heartbeat_fingerprint();
        assert_ne!(first, 0, "a real subscription set must not hash to the V1 marker");

        // Same subscriptions, different subVersion / clientId / fingerprint / withoutSub.
        let mut moved = base.clone();
        moved.client_id = "10.0.0.9@999".to_string();
        moved.heartbeat_fingerprint = first;
        moved.without_sub = true;
        moved.consumer_data_set[0].subscription_data_set[0].sub_version = 9_999_999;
        assert_eq!(moved.compute_heartbeat_fingerprint(), first);

        // A different subscription expression is a different fingerprint.
        let mut changed = base.clone();
        changed.consumer_data_set[0].subscription_data_set[0].sub_string = "TagB".to_string();
        assert_ne!(changed.compute_heartbeat_fingerprint(), first);

        // An extra producer group is visible too (the broker re-registers producers on every beat).
        let mut extra = base.clone();
        extra.add_producer_data(ProducerData::new("pg2"));
        assert_ne!(extra.compute_heartbeat_fingerprint(), first);

        // Stable across calls on the same value (no clock, no randomness).
        assert_eq!(base.compute_heartbeat_fingerprint(), first);
    }

    #[test]
    fn without_sub_copy_keeps_registration_and_marks_the_body() {
        let mut hb = HeartbeatData::new("cid");
        let mut cd = ConsumerData::new("cg", ConsumeType::CONSUME_PASSIVELY, MessageModel::CLUSTERING, ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
        cd.add_subscription_data(SubscriptionData::new("TopicA", "*"));
        cd.add_subscription_data(SubscriptionData::new("TopicB", "TagB"));
        hb.add_consumer_data(cd);
        hb.add_producer_data(ProducerData::new("pg"));

        let fp = hb.compute_heartbeat_fingerprint();
        let light = {
            let mut light = hb.without_sub_copy();
            light.heartbeat_fingerprint = fp;
            light
        };
        assert!(light.without_sub);
        assert_eq!(light.heartbeat_fingerprint, fp);
        assert!(light.consumer_data_set[0].subscription_data_set.is_empty());
        assert_eq!(light.client_id, "cid");
        assert_eq!(light.producer_data_set.len(), 1);
        // The heavy body is untouched.
        assert_eq!(hb.consumer_data_set[0].subscription_data_set.len(), 2);
        assert!(!hb.without_sub);
        // The cheap beat is only valid because it CARRIES the full body's fingerprint in the field:
        // recomputing over a subscription-stripped body deliberately differs (the subscriptions are
        // part of what the fingerprint identifies), so a sender that recomputed it would tell the
        // broker "subscriptions changed" every single round and the optimization would buy nothing.
        assert_eq!(light.heartbeat_fingerprint, hb.compute_heartbeat_fingerprint());
        assert_ne!(light.compute_heartbeat_fingerprint(), hb.compute_heartbeat_fingerprint());
    }

    #[test]
    fn heartbeat_v2_result_reads_ext_fields_leniently() {
        use crate::remoting::protocol::remoting_command::RemotingCommand;
        let mut response = RemotingCommand::create_response(0, None);
        response.version = 403;
        assert_eq!(
            HeartbeatV2Result::from_response(&response),
            HeartbeatV2Result::new(403, false, false),
            "a pre-V2 broker answers SUCCESS with no extFields at all"
        );

        response.ext_fields_mut().insert(
            crate::common::mix_all::MixAll::IS_SUPPORT_HEART_BEAT_V2,
            "true",
        );
        response.ext_fields_mut().insert(
            crate::common::mix_all::MixAll::IS_SUB_CHANGE,
            "TRUE",
        );
        assert_eq!(
            HeartbeatV2Result::from_response(&response),
            HeartbeatV2Result::new(403, true, true)
        );

        response.ext_fields_mut().insert(
            crate::common::mix_all::MixAll::IS_SUB_CHANGE,
            "",
        );
        assert!(
            !HeartbeatV2Result::from_response(&response).sub_change,
            "empty is not true"
        );
    }

    #[test]
    fn filter_api_build_routes_by_expression_type() {
        // TAG: compiled into tagsSet/codeSet like buildSubscriptionData.
        let tag = FilterAPI::build("T", "TagA||TagB", Some(ExpressionType::TAG)).unwrap();
        assert_eq!(tag.tags_set, vec!["TagA".to_string(), "TagB".to_string()]);
        assert_eq!(tag.expression_type, ExpressionType::TAG);

        // SQL92: raw expression, both collections stay empty (this is what turns the
        // client-side tag re-filter off).
        let sql = FilterAPI::build("T", "a > 10", Some(ExpressionType::SQL92)).unwrap();
        assert_eq!(sql.sub_string, "a > 10");
        assert_eq!(sql.expression_type, ExpressionType::SQL92);
        assert!(sql.tags_set.is_empty() && sql.code_set.is_empty());
        assert!(!sql.class_filter_mode);

        // No type at all: same as the two-argument form.
        let none = FilterAPI::build("T", "TagA", None).unwrap();
        assert_eq!(none.tags_set, vec!["TagA".to_string()]);
        assert_eq!(none.expression_type, ExpressionType::TAG);

        // An empty expression for a non-tag type is rejected with a client error.
        let err = FilterAPI::build("T", "", Some(ExpressionType::SQL92)).unwrap_err();
        assert!(
            matches!(&err, crate::error::Error::Client { message, .. } if message == "Expression can't be null! SQL92"),
            "{err}"
        );
    }

    #[test]
    fn expression_type_is_tag_type_only_for_empty_and_tag() {
        assert!(ExpressionType::is_tag_type(""));
        assert!(ExpressionType::is_tag_type(ExpressionType::TAG));
        assert!(!ExpressionType::is_tag_type(ExpressionType::SQL92));
        assert!(!ExpressionType::is_tag_type(ExpressionType::CLASS_FILTER));
    }

    #[test]
    fn missing_and_tolerant_pieces() {
        let hb = HeartbeatData::decode(br#"{"clientID":"c","producerDataSet":null,"subscriptionDataSet":[{"topic":"T"}]}"#).unwrap();
        assert_eq!(hb, HeartbeatData::new("c"));
        // Booleans in numeric form and numbers in string form both parse.
        let cd = ConsumerData::from_json_value(
            &serde_json::json!({"groupName":"g","unitMode":1,"subscriptionDataSet":[{"subVersion":"5","codeSet":["7"],"tagsSet":["A"],"classFilterMode":"no"}]}),
        )
        .unwrap();
        assert!(cd.unit_mode, "numeric 1 counts as true (lenient boolean semantics)");
        assert_eq!(cd.subscription_data_set[0].sub_version, 5);
        assert_eq!(cd.subscription_data_set[0].code_set, vec![7]);
        assert!(!cd.subscription_data_set[0].class_filter_mode, "\"no\" is not true");
    }

    #[test]
    fn garbage_heartbeat_is_a_decode_error() {
        for raw in [
            b"".as_slice(),
            b"not-json".as_slice(),
            b"[]".as_slice(),
            br#"{"clientID":"c","producerDataSet":{"pg":{"groupName":"pg"}}}"#.as_slice(),
            br#"{"clientID":"c","consumerDataSet":{}}"#.as_slice(),
            br#"{"clientID":"c","consumerDataSet":[{"subscriptionDataSet":["x"]}]}"#.as_slice(),
            br#"{"clientID":"c","consumerDataSet":[{"subscriptionDataSet":[{"codeSet":["x"]}]}]}"#
                .as_slice(),
        ] {
            assert!(
                matches!(HeartbeatData::decode(raw), Err(Error::Decode(_))),
                "input {raw:?} must be rejected"
            );
        }
    }

    /// The golden vectors come from **real behavior recorded on the wire**, not hand-derived from rules.
    #[test]
    fn filter_api_matches_the_golden_reference_vector_by_vector() {
        /// (case name, subscription expression, expected subString, expected tagsSet, expected codeSet)
        type Case = (&'static str, Option<&'static str>, &'static str, &'static [&'static str], &'static [i32]);
        let cases: &[Case] = &[
            ("none", None, "*", &[], &[]),
            ("empty", Some(""), "*", &[], &[]),
            ("sub-all", Some("*"), "*", &[], &[]),
            ("blank-kept", Some("   "), "   ", &[], &[]),
            ("one-tag", Some("TagA"), "TagA", &["TagA"], &[2598919]),
            (
                "two-tags",
                Some("TagA||TagB"),
                "TagA||TagB",
                &["TagA", "TagB"],
                &[2598919, 2598920],
            ),
            (
                "spaces-around-tags",
                Some(" TagA || TagB "),
                " TagA || TagB ",
                &["TagA", "TagB"],
                &[2598919, 2598920],
            ),
            ("only-separators", Some("|||"), "|||", &["|"], &[124]),
            ("trailing-empty-dropped", Some("a||||"), "a||||", &["a"], &[97]),
            ("leading-empty-dropped", Some("||TagA"), "||TagA", &["TagA"], &[2598919]),
            ("duplicates-collapse", Some("TagA||TagA"), "TagA||TagA", &["TagA"], &[2598919]),
        ];
        for (what, expression, sub_string, tags, codes) in cases {
            let sub = FilterAPI::build_subscription_data("T", *expression)
                .unwrap_or_else(|e| panic!("{what}: {e}"));
            assert_eq!(sub.topic, "T", "{what}");
            assert_eq!(&sub.sub_string, sub_string, "{what} subString");
            assert_eq!(&sub.tags_set, tags, "{what} tagsSet");
            assert_eq!(&sub.code_set, codes, "{what} codeSet");
            // Building subscription data does not rewrite expressionType / classFilterMode.
            assert_eq!(sub.expression_type, ExpressionType::TAG, "{what}");
            assert!(!sub.class_filter_mode, "{what}");
        }
    }

    #[test]
    fn filter_api_rejects_an_all_empty_split() {
        for raw in ["||", "||||"] {
            assert!(
                matches!(
                    FilterAPI::build_subscription_data("T", Some(raw)),
                    Err(Error::Client { message, .. }) if message == "subString split error"
                ),
                "input {raw:?} must raise subString split error"
            );
        }
    }
}
