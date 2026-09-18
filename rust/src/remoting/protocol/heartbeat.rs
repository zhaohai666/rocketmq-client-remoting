//! 心跳数据（对应 `org.apache.rocketmq.remoting.protocol.heartbeat.*`）。
//!
//! 移植 `python/rocketmq/remoting/protocol/heartbeat.py`：`ProducerData` /
//! `ConsumerData` / `HeartbeatData`，以及 `ConsumerData.subscriptionDataSet` 需要的
//! [`SubscriptionData`]。
//!
//! ## 为什么本层自带 `SubscriptionData`
//!
//! Python 把它放在 `rocketmq.common.subscription_data`，`heartbeat.py` 再从那里
//! import。Rust 里心跳报文与它是同一套编解码，因此类型声明在本层，字段名 / 顺序 /
//! 缺省值与 Python 的 `to_dict()` 逐一对齐；`filterClassSource` 在 Java 上是
//! `@JSONField(serialize = false)`，**故意不出现**在本结构里。
//! [`FilterAPI`] 计算 `codeSet` 要用 `common::util_all::java_string_hash` ——
//! 协议层用到 `common` 的工具函数在本 crate 是常态（`route.rs`、`extra_info.rs`、
//! `serialize.rs` 同样如此）。
//!
//! ## 报文细节
//!
//! * `clientID` 的 `ID` 全大写（Java 字段名如此），不是 `clientId`。
//! * `heartbeatFingerprint` 恒写 0、`withoutSub` 恒写 `false`：broker 见到 0 才走
//!   V1 全量注册路径（用完整 `subscriptionDataSet` 注册），最稳妥；非 0 会进
//!   heartBeatV2 增量优化。因此编码**不**回写这两个值，只在解码时读出来供诊断。
//! * 解码兼容 Java 的另一路拼写 `isWithoutSub`（fastjson2 读 `isXxx` 布尔属性时
//!   认这个键）。
//! * 顶层键按字母序写出，与 Java fastjson2 的实际输出一致（对象键顺序对 broker
//!   无语义，但便于与 cpp/Java 的黄金报文逐字节比对）。

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use super::admin_body::{expect_object, jarray, jboolean, jlong, json_object, jstring_or};
use super::serialize::RemotingSerializable;
use crate::error::{Error, Result};

/// `System.currentTimeMillis()`：`subVersion` 的默认值来源。
pub fn now_millis() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
    }
}

// ---------------------------------------------------------------- 枚举常量

/// `ConsumeType`：值必须是 Java 枚举名原文。
pub struct ConsumeType;

impl ConsumeType {
    /// 主动消费（pull 客户端）
    pub const CONSUME_ACTIVELY: &'static str = "CONSUME_ACTIVELY";
    /// 被动消费（push 客户端）
    pub const CONSUME_PASSIVELY: &'static str = "CONSUME_PASSIVELY";
    /// Java 5.x POP 消费。Python 的 `heartbeat.py` 里没有这一项，此处按 Java 补齐。
    pub const CONSUME_POP: &'static str = "CONSUME_POP";
}

/// `MessageModel`
pub struct MessageModel;

impl MessageModel {
    pub const BROADCASTING: &'static str = "BROADCASTING";
    pub const CLUSTERING: &'static str = "CLUSTERING";
    /// Java 5.x 轻量选择性模型；Python 侧同样缺席，按 Java 补齐。
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
}

// ---------------------------------------------------------------- SubscriptionData

/// 对应 Java `SubscriptionData`（心跳里 `subscriptionDataSet` 的元素）。
///
/// `tags_set` / `code_set` 在 Python 里是 `set`，`to_dict()` 写出前 `sorted()`；
/// Rust 直接用**已排序去重**的 `Vec` 表达同一语义（见 [`SubscriptionData::set_tags`]）。
/// `code_set` 是 Java `String.hashCode()`（broker 侧按 tag 哈希过滤的依据），
/// 由调用方用 `common::util_all::java_string_hash` 算好后填入。
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
        // Python: sub_version = int(time.time() * 1000)，即当前毫秒时间戳。
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

    /// 一次性设置 tag 集合与对应哈希集合：去重 + 升序，等价 Python 的 `sorted(set)`。
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
            // Python: `int(v)` —— 数字与数字字符串都能读。
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
            // Python: bool(sd.get("classFilterMode", False))
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

/// 订阅表达式 → [`SubscriptionData`]（对应 Java `FilterAPI.buildSubscriptionData`，
/// Python `common/subscription_data.FilterAPI`）。
pub struct FilterAPI;

impl FilterAPI {
    /// Java `FilterAPI.SUB_ALL`。
    pub const SUB_ALL: &'static str = "*";

    /// 按订阅表达式构造订阅数据。
    ///
    /// Java 行为（Python 侧用探针实测过，这里是同一批向量）：
    /// * `None` / `""` / `"*"` ⇒ `subString` 归一成 `"*"`，**`tagsSet` 与 `codeSet` 都留空**。
    ///   留空不是省事：`tagsSet` 非空是客户端二次 tag 过滤的开关
    ///   （`PullAPIWrapper.processPullResult` 的 `!tagsSet.isEmpty()`），塞了 `"*"`
    ///   会把订阅全量时所有带 tag 的消息自己过滤掉；`codeSet` 则是 broker 侧
    ///   按 tag 哈希过滤的依据。
    /// * `" TagA || TagB "` ⇒ `subString` **原样保留空格**，标签各自 trim。
    /// * `"   "`（纯空白）⇒ 走切分分支，标签 trim 后为空 ⇒ 两个集合都空，
    ///   但 `subString` 仍是那三个空格（Java `StringUtils.isEmpty` 只认 null/`""`）。
    /// * `"|||"` ⇒ `tagsSet={"|"}`、`codeSet={124}`（切分只丢**末尾**空串）。
    /// * `"||"` / `"||||"` ⇒ 报错 `subString split error`（Java 的切分结果长度为 0）。
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
        // Java String.split("\\|\\|")：丢掉**末尾**空串，中间的留着。
        let mut parts: Vec<&str> = raw.split("||").collect();
        while parts.last() == Some(&"") {
            parts.pop();
        }
        if parts.is_empty() {
            // Java: throw new Exception("subString split error")
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
        // set_tags 负责去重 + 升序（等价 Python 的 sorted(set)）。
        sub.set_tags(tags, codes);
        Ok(sub)
    }
}

/// Java 的 SUB_ALL 归一：`subString = "*"`，两个集合保持为空。
fn normalise_sub_all(mut sub: SubscriptionData) -> SubscriptionData {
    sub.sub_string = FilterAPI::SUB_ALL.to_string();
    sub.tags_set.clear();
    sub.code_set.clear();
    sub
}

// ---------------------------------------------------------------- ProducerData

/// 对应 Java `ProducerData`：只有一个 `groupName`。
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

/// 对应 Java 5.x `ConsumerData`。
///
/// 字段集合严格对齐 Java 5.x：**没有** 4.x 的 `consumeTimestamp` /
/// `maxReconsumeTimes`，多写一个就会被 broker 侧 fastjson2 判为未知字段。
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

    /// 加入订阅（Python 用 `set`，重复元素会被折叠；这里同样去重）。
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

/// 对应 Java `HeartbeatData`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatData {
    pub client_id: String,
    pub producer_data_set: Vec<ProducerData>,
    pub consumer_data_set: Vec<ConsumerData>,
    /// 只读：编码时恒写 0（见模块头的说明）。
    pub heartbeat_fingerprint: i64,
    /// 只读：编码时恒写 `false`。
    pub without_sub: bool,
}

impl Default for HeartbeatData {
    fn default() -> Self {
        HeartbeatData::new("")
    }
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

    /// Python 用 `set`，重复 group / 重复 consumer 会被折叠。
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
            ("heartbeatFingerprint", Value::from(0_i64)),
            (
                "producerDataSet",
                Value::Array(
                    self.producer_data_set
                        .iter()
                        .map(|p| p.to_json_value())
                        .collect(),
                ),
            ),
            ("withoutSub", Value::from(false)),
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
        // isWithoutSub / withoutSub 两种拼写都认（Java 字段名是 isWithoutSub，
        // fastjson2 读 `isXxx` 布尔属性时两种键都会落到同一个字段）。
        let without_sub = jboolean(value, "withoutSub", jboolean(value, "isWithoutSub", false));
        Ok(HeartbeatData {
            client_id: jstring_or(value, "clientID", ""),
            producer_data_set,
            consumer_data_set,
            heartbeat_fingerprint: jlong(value, "heartbeatFingerprint", 0),
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

/// 把 `subscriptionDataSet` 之外的标量字段读出来，供上层日志/调试用。
/// （与 Python `ConsumerData.__repr__` 等价的诊断辅助。）
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
    fn subscription_data_json_uses_java_names_and_drops_filter_class_source() {
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
        assert!(text.contains(r#""subVersion":1700000000000"#), "subVersion 必须是整数");
        assert!(text.contains(r#""expressionType":"TAG""#));
        assert!(text.contains(r#""topic":"TopicProbe""#));
        assert!(text.contains(r#""tagsSet":["TagA","TagB"]"#));
        assert!(text.contains(r#""codeSet":[2598919,2598920]"#));
        assert!(!text.contains("filterClassSource"));
        assert!(!text.contains("class_filter_mode"), "不得出现 snake_case 键");
        assert_eq!(SubscriptionData::from_json_value(&sd.to_json_value()).unwrap(), sd);

        // Java equals 含 subVersion（Python 的 __eq__ 不含，这里跟 Java）。
        let mut other = sd.clone();
        other.sub_version = 1700000000001;
        assert_ne!(other, sd);
    }

    #[test]
    fn subscription_data_sub_version_defaults_to_now() {
        let fresh = SubscriptionData::default();
        assert!(fresh.sub_version > 1_600_000_000_000, "默认应为当前毫秒量级");
        assert_eq!(fresh.expression_type, "TAG");
        assert!(fresh.tags_set.is_empty() && fresh.code_set.is_empty());
    }

    /// cpp/tests/test_route_heartbeat.cpp 的黄金报文，逐字节。
    #[test]
    fn heartbeat_exact_json_matches_fastjson2() {
        let mut hb = HeartbeatData::new("cid");
        hb.add_producer_data(ProducerData::new("pg"));
        assert_eq!(
            RemotingSerializable::to_json_string(&hb.to_json_value()),
            r#"{"clientID":"cid","consumerDataSet":[],"heartbeatFingerprint":0,"producerDataSet":[{"groupName":"pg"}],"withoutSub":false}"#
        );
        // 稳定：同一对象两次编码逐字节一致（集合有序，无随机成分）。
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
        assert_eq!(cd.subscription_data_set.len(), 1, "重复订阅被折叠（set 语义）");
        let text = RemotingSerializable::to_json_string(&cd.to_json_value());
        assert!(text.contains(r#""groupName":"cg_probe""#));
        assert!(text.contains(r#""consumeType":"CONSUME_PASSIVELY""#));
        assert!(text.contains(r#""messageModel":"CLUSTERING""#));
        assert!(text.contains(r#""consumeFromWhere":"CONSUME_FROM_LAST_OFFSET""#));
        assert!(text.contains(r#""unitMode":false"#));
        assert!(text.contains(r#""subscriptionDataSet":["#));
        assert!(!text.contains("consumeTimestamp"), "Java 5.x 无此字段");
        assert!(!text.contains("maxReconsumeTimes"), "Java 5.x 无此字段");
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
        assert!(!text.contains("isWithoutSub"), "写出用 withoutSub，不是 isWithoutSub");

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
        // 编码回到 V1 路径的固定值。
        let back = RemotingSerializable::to_json_string(&hb.to_json_value());
        assert!(back.contains(r#""heartbeatFingerprint":0"#));
        assert!(back.contains(r#""withoutSub":false"#));
    }

    #[test]
    fn missing_and_tolerant_pieces() {
        let hb = HeartbeatData::decode(br#"{"clientID":"c","producerDataSet":null,"subscriptionDataSet":[{"topic":"T"}]}"#).unwrap();
        assert_eq!(hb, HeartbeatData::new("c"));
        // 数字形式的布尔、字符串形式的数字都能读。
        let cd = ConsumerData::from_json_value(
            &serde_json::json!({"groupName":"g","unitMode":1,"subscriptionDataSet":[{"subVersion":"5","codeSet":["7"],"tagsSet":["A"],"classFilterMode":"no"}]}),
        )
        .unwrap();
        assert!(cd.unit_mode, "数字 1 视为 true（Java 宽松语义）");
        assert_eq!(cd.subscription_data_set[0].sub_version, 5);
        assert_eq!(cd.subscription_data_set[0].code_set, vec![7]);
        assert!(!cd.subscription_data_set[0].class_filter_mode, "\"no\" 不是 true");
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

    /// 黄金向量取自**跑起来的 Python 参考实现**
    /// （`python/rocketmq/common/subscription_data.FilterAPI`），不是手推的。
    #[test]
    fn filter_api_matches_the_python_reference_vector_by_vector() {
        /// (用例名, 订阅表达式, 期望 subString, 期望 tagsSet, 期望 codeSet)
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
            // Python 的 build_subscription_data 从不改 expressionType / classFilterMode。
            assert_eq!(sub.expression_type, ExpressionType::TAG, "{what}");
            assert!(!sub.class_filter_mode, "{what}");
        }
    }

    #[test]
    fn filter_api_rejects_an_all_empty_split_like_java() {
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
