//! - `assignments == null`（body 为空 / 缺键 / 显式 null）→ 「无效结果」，客户端
//!   **跳过本轮更新、保留上一次分配**；[`QueryAssignmentResponseBody::decode`] 回 `None`。
//! - `assignments == 空数组` → 「清除我之前拿到的全部负载」；decode 回 `Some(vec![])`。
//! - 报文本身不合法 → `Err`（严出：不猜）。

use serde_json::Value;

use super::admin_body::{expect_object, json_object, jstring, jstring_or, MessageQueueKey};
use super::body::message_queue_value;
use super::heartbeat::MessageModel;
use super::serialize::RemotingSerializable;
use crate::error::{Error, Result};

pub const MESSAGE_REQUEST_MODE_POP: &str = "POP";
pub const MESSAGE_REQUEST_MODE_PULL: &str = "PULL";

// ---------------------------------------------------------------- 请求体

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryAssignmentRequestBody {
    pub topic: String,
    pub consumer_group: String,
    pub client_id: String,
    /// 本端的分配策略名（"AVG" / "AVG_CIRCLE" / …）。broker 只认识
    /// `AllocateMessageQueueAveragely`（AVG）与 `AVG_BY_CIRCLE`，其余回 null
    /// （= 无效结果，客户端保留上一次分配）。
    pub strategy_name: String,
    /// 枚举名："CLUSTERING" / "BROADCASTING"。
    pub message_model: String,
}

impl QueryAssignmentRequestBody {
    pub fn new(
        topic: &str,
        consumer_group: &str,
        client_id: &str,
        strategy_name: &str,
        message_model: &str,
    ) -> Self {
        QueryAssignmentRequestBody {
            topic: topic.to_string(),
            consumer_group: consumer_group.to_string(),
            client_id: client_id.to_string(),
            strategy_name: strategy_name.to_string(),
            message_model: message_model.to_string(),
        }
    }

    pub fn to_json_value(&self) -> Value {
        json_object(vec![
            ("topic", Value::String(self.topic.clone())),
            ("consumerGroup", Value::String(self.consumer_group.clone())),
            ("clientId", Value::String(self.client_id.clone())),
            ("strategyName", Value::String(self.strategy_name.clone())),
            ("messageModel", Value::String(self.message_model.clone())),
        ])
    }

    pub fn from_json_value(value: &Value) -> Result<QueryAssignmentRequestBody> {
        expect_object(value, "QueryAssignmentRequestBody")?;
        Ok(QueryAssignmentRequestBody {
            topic: jstring(value, "topic").unwrap_or_default(),
            consumer_group: jstring(value, "consumerGroup").unwrap_or_default(),
            client_id: jstring(value, "clientId").unwrap_or_default(),
            strategy_name: jstring(value, "strategyName").unwrap_or_default(),
            message_model: jstring_or(value, "messageModel", MessageModel::CLUSTERING),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        RemotingSerializable::encode(&self.to_json_value())
    }

    pub fn decode(data: &[u8]) -> Result<QueryAssignmentRequestBody> {
        QueryAssignmentRequestBody::from_json_value(&RemotingSerializable::decode(data)?)
    }
}

// ---------------------------------------------------------------- 单个分配项

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageQueueAssignment {
    pub message_queue: Option<MessageQueueKey>,
    pub mode: String,
}

impl MessageQueueAssignment {
    pub fn new(message_queue: MessageQueueKey, mode: &str) -> Self {
        MessageQueueAssignment {
            message_queue: Some(message_queue),
            mode: mode.to_string(),
        }
    }

    pub fn to_json_value(&self) -> Value {
        let mut pairs = Vec::with_capacity(2);
        match &self.message_queue {
            Some(mq) => pairs.push(("messageQueue", message_queue_value(mq))),
            None => pairs.push(("messageQueue", Value::Null)),
        }
        pairs.push((
            "mode",
            Value::String(if self.mode.is_empty() {
                MESSAGE_REQUEST_MODE_PULL.to_string()
            } else {
                self.mode.clone()
            }),
        ));
        json_object(pairs)
    }

    pub fn from_json_value(value: &Value) -> Result<MessageQueueAssignment> {
        expect_object(value, "MessageQueueAssignment")?;
        Ok(MessageQueueAssignment {
            message_queue: MessageQueueKey::from_value(&jraw_map_null(value, "messageQueue")),
            mode: jstring(value, "mode")
                .unwrap_or_else(|| MESSAGE_REQUEST_MODE_PULL.to_string()),
        })
    }
}

/// `jraw_map` 的姊妹：字段是对象则克隆，否则（缺失 / null / 脏类型）回 `Value::Null`
/// —— 正好喂给 `MessageQueueKey::from_value`（非对象回 None）。
fn jraw_map_null(value: &Value, key: &str) -> Value {
    match value.get(key) {
        Some(v) if v.is_object() => v.clone(),
        _ => Value::Null,
    }
}

// ---------------------------------------------------------------- 响应体

pub struct QueryAssignmentResponseBody;

impl QueryAssignmentResponseBody {
    pub fn encode(assignments: Option<&[MessageQueueAssignment]>) -> Vec<u8> {
        let list = match assignments {
            Some(list) => list,
            None => {
                return RemotingSerializable::encode(&json_object(Vec::new()));
            }
        };
        let items: Vec<Value> = list.iter().map(|a| a.to_json_value()).collect();
        RemotingSerializable::encode(&json_object(vec![(
            "messageQueueAssignments",
            Value::Array(items),
        )]))
    }

    /// 解析 400 响应 body。返回值的三态即语义：
    pub fn decode(data: &[u8]) -> Result<Option<Vec<MessageQueueAssignment>>> {
        if data.is_empty() {
            return Ok(None);
        }
        let value = RemotingSerializable::decode(data)?;
        if value.is_null() {
            return Ok(None);
        }
        expect_object(&value, "QueryAssignmentResponseBody")?;
        match value.get("messageQueueAssignments") {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Array(items)) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(MessageQueueAssignment::from_json_value(item)?);
                }
                Ok(Some(out))
            }
            Some(_) => Err(Error::Decode(
                "messageQueueAssignments is not a json array".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mq(topic: &str, broker: &str, id: i32) -> MessageQueueKey {
        MessageQueueKey::new(topic, broker, id)
    }

    #[test]
    fn request_body_encodes_java_property_names() {
        let body = QueryAssignmentRequestBody::new(
            "T1",
            "GID_g",
            "127.0.0.1@inst",
            "AVG",
            "CLUSTERING",
        );
        let text = String::from_utf8(body.encode()).unwrap();
        for key in [
            "\"topic\":\"T1\"",
            "\"consumerGroup\":\"GID_g\"",
            "\"clientId\":\"127.0.0.1@inst\"",
            "\"strategyName\":\"AVG\"",
            "\"messageModel\":\"CLUSTERING\"",
        ] {
            assert!(text.contains(key), "{text} 缺 {key}");
        }
    }

    #[test]
    fn request_body_round_trip_is_tolerant() {
        let back = QueryAssignmentRequestBody::decode(b"{}").unwrap();
        assert_eq!(back.topic, "");
        assert_eq!(back.message_model, MessageModel::CLUSTERING);

        let full = QueryAssignmentRequestBody::new("T", "G", "C", "AVG", "BROADCASTING");
        let back = QueryAssignmentRequestBody::decode(&full.encode()).unwrap();
        assert_eq!(back, full);

        // 脏类型：字符串字段写成对象 → 回默认值而不是 panic（fastjson2 的 to_string 容错）
        let v: Value = serde_json::from_str(
            "{\"topic\":{\"a\":1},\"consumerGroup\":\"G\",\"clientId\":\"C\",\"strategyName\":\"AVG\",\"messageModel\":\"CLUSTERING\"}",
        )
        .unwrap();
        let back = QueryAssignmentRequestBody::from_json_value(&v).unwrap();
        assert_eq!(back.consumer_group, "G");
    }

    #[test]
    fn response_body_keeps_null_vs_empty_distinction() {
        assert_eq!(QueryAssignmentResponseBody::decode(b"").unwrap(), None);
        assert_eq!(QueryAssignmentResponseBody::decode(b"null").unwrap(), None);
        assert_eq!(QueryAssignmentResponseBody::decode(b"{}").unwrap(), None);
        assert_eq!(
            QueryAssignmentResponseBody::decode(
                b"{\"messageQueueAssignments\":null}"
            )
            .unwrap(),
            None
        );
        // 显式空数组：Some(空) —— 「清除之前的全部分配」
        let got =
            QueryAssignmentResponseBody::decode(b"{\"messageQueueAssignments\":[]}").unwrap();
        assert_eq!(got.map(|v| v.len()), Some(0));
    }

    #[test]
    fn response_body_decodes_assignment_items() {
        let data = br#"{"messageQueueAssignments":[
            {"messageQueue":{"topic":"T1","brokerName":"broker-a","queueId":3},"mode":"POP"},
            {"messageQueue":{"topic":"T1","brokerName":"broker-b","queueId":0}},
            {"mode":"POP"}
        ]}"#;
        let got = QueryAssignmentResponseBody::decode(data)
            .unwrap()
            .expect("有集");
        assert_eq!(got.len(), 3);
        assert_eq!(
            got[0].message_queue.as_ref(),
            Some(&mq("T1", "broker-a", 3))
        );
        assert_eq!(got[0].mode, MESSAGE_REQUEST_MODE_POP);
        assert_eq!(got[1].mode, MESSAGE_REQUEST_MODE_PULL);
        assert_eq!(got[2].message_queue, None);
    }

    #[test]
    fn response_body_rejects_malformed_payloads() {
        // 顶层不是对象 / 集不是数组 → 严出报错
        assert!(QueryAssignmentResponseBody::decode(b"[1,2]").is_err());
        assert!(
            QueryAssignmentResponseBody::decode(b"{\"messageQueueAssignments\":\"nope\"}").is_err()
        );
        assert!(QueryAssignmentResponseBody::decode(
            b"{\"messageQueueAssignments\":[\"nope\"]}"
        )
        .is_err());
    }

    #[test]
    fn response_encode_round_trips_through_decode() {
        let items = vec![
            MessageQueueAssignment::new(mq("T", "broker-a", 0), MESSAGE_REQUEST_MODE_POP),
            MessageQueueAssignment::new(mq("T", "broker-a", 1), MESSAGE_REQUEST_MODE_PULL),
        ];
        let got = QueryAssignmentResponseBody::decode(&QueryAssignmentResponseBody::encode(Some(
            &items,
        )))
        .unwrap()
        .expect("有集");
        assert_eq!(got, items);

        // None 编码不带键 → 解码回 None
        let none_body = QueryAssignmentResponseBody::encode(None);
        assert!(QueryAssignmentResponseBody::decode(&none_body)
            .unwrap()
            .is_none());
    }
}
