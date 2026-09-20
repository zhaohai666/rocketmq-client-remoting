//! 消息模型（对应 Java `org.apache.rocketmq.common.message.{MessageQueue,Message,
//! MessageExt,MessageBatch}`，参考实现 `python/rocketmq/common/message.py`）。
//!
//! 属性容器一律用 [`StringMap`]（保序）：17 段格式第 17 项是按插入顺序拼出来的
//! `k\x01v\x02` 字节串，换成 `HashMap` 会直接破坏逐字节对拍。

use std::cmp::Ordering;
use std::fmt;

use crate::common::message_const::{
    PROPERTY_DELAY_TIME_LEVEL, PROPERTY_KEYS, PROPERTY_TAGS, PROPERTY_WAIT_STORE_MSG_OK,
};
use crate::common::mix_all::MixAll;
use crate::error::{Error, Result};
use crate::remoting::protocol::ext_fields::StringMap;

/// 对应 Java `MessageQueue`（Python 里也在 `common/message.py`）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct MessageQueue {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
}

impl MessageQueue {
    pub fn new(topic: &str, broker_name: &str, queue_id: i32) -> MessageQueue {
        MessageQueue { topic: topic.to_string(), broker_name: broker_name.to_string(), queue_id }
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: &str) {
        self.topic = topic.to_string();
    }

    pub fn get_broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn set_broker_name(&mut self, broker_name: &str) {
        self.broker_name = broker_name.to_string();
    }

    pub fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    pub fn get_queue_id_str(&self) -> String {
        self.queue_id.to_string()
    }

    /// 对应 Java `MessageQueue#toString`。
    ///
    /// ⚠ 刻意**不**与本模块的 [`fmt::Display`] 合并：Java 的 `toString` 是
    /// `MessageQueue [topic=.., brokerName=.., queueId=..]`，而一致性哈希策略要哈希的
    /// 正是这个字符串（差一个空格就与 Java 客户端不是同一个环），`Display` 那种
    /// 面向日志的紧凑写法不能顶替它。
    pub fn to_java_string(&self) -> String {
        format!(
            "MessageQueue [topic={}, brokerName={}, queueId={}]",
            self.topic, self.broker_name, self.queue_id
        )
    }

    /// Java `MessageQueue#hashCode`：`((31 + brokerHash) * 31 + queueId) * 31 + topicHash`，
    /// 32 位有符号回绕。消费者 rebalance / 去重按它分桶，必须逐位一致。
    pub fn hashcode(&self) -> i32 {
        let topic_hash = crate::common::util_all::java_string_hash(&self.topic);
        let broker_hash = crate::common::util_all::java_string_hash(&self.broker_name);
        let mut r: i32 = 31;
        r = r.wrapping_add(broker_hash);
        r = r.wrapping_mul(31).wrapping_add(self.queue_id);
        r.wrapping_mul(31).wrapping_add(topic_hash)
    }

    /// 对应 Java `Comparable#compareTo`：topic -> brokerName -> queueId 字典序。
    pub fn compare_to(&self, other: &MessageQueue) -> Ordering {
        match self.topic.cmp(&other.topic) {
            Ordering::Equal => {}
            r => return r,
        }
        match self.broker_name.cmp(&other.broker_name) {
            Ordering::Equal => {}
            r => return r,
        }
        self.queue_id.cmp(&other.queue_id)
    }
}

impl Ord for MessageQueue {
    fn cmp(&self, other: &MessageQueue) -> Ordering {
        self.compare_to(other)
    }
}

impl PartialOrd for MessageQueue {
    fn partial_cmp(&self, other: &MessageQueue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for MessageQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.topic, self.broker_name, self.queue_id)
    }
}

/// 对应 Java `Message` / Python `Message`。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message {
    pub topic: String,
    pub flag: i32,
    /// `None` 等价 Java 的 `body == null`；`encode` 时按空串处理。
    pub body: Option<Vec<u8>>,
    pub properties: StringMap,
    pub transaction_id: Option<String>,
}

impl Message {
    /// 对应 Python `Message(topic, body)`：`body=None` 时置空串（Python 亦如此）。
    pub fn new(topic: &str, body: Option<&[u8]>) -> Message {
        Message {
            topic: topic.to_string(),
            flag: 0,
            body: Some(body.map(|b| b.to_vec()).unwrap_or_default()),
            properties: StringMap::new(),
            transaction_id: None,
        }
    }

    /// 对应 Python `Message(topic, body, tags, keys, flag)`：**空 tags / keys 不写入属性**
    /// （Python 的 `if tags is not None and tags`）。
    pub fn with_tags_and_keys(
        topic: &str,
        body: Option<&[u8]>,
        tags: Option<&str>,
        keys: Option<&str>,
        flag: i32,
    ) -> Message {
        let mut msg = Message {
            topic: topic.to_string(),
            flag,
            body: Some(body.map(|b| b.to_vec()).unwrap_or_default()),
            properties: StringMap::new(),
            transaction_id: None,
        };
        if let Some(t) = tags {
            if !t.is_empty() {
                msg.properties.insert(PROPERTY_TAGS, t);
            }
        }
        if let Some(k) = keys {
            if !k.is_empty() {
                msg.properties.insert(PROPERTY_KEYS, k);
            }
        }
        msg
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: &str) {
        self.topic = topic.to_string();
    }

    /// 无正文时返回空切片（Python 的 `get_body()` 同理，构造期已兜底为 `b""`）。
    pub fn get_body(&self) -> &[u8] {
        self.body.as_deref().unwrap_or(&[])
    }

    pub fn set_body(&mut self, body: Option<&[u8]>) {
        self.body = Some(body.map(|b| b.to_vec()).unwrap_or_default());
    }

    pub fn get_flag(&self) -> i32 {
        self.flag
    }

    pub fn set_flag(&mut self, flag: i32) {
        self.flag = flag;
    }

    pub fn get_properties(&self) -> &StringMap {
        &self.properties
    }

    pub fn set_properties(&mut self, properties: StringMap) {
        self.properties = properties;
    }

    pub fn get_transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub fn set_transaction_id(&mut self, transaction_id: Option<&str>) {
        self.transaction_id = transaction_id.map(|s| s.to_string());
    }

    pub fn set_tags(&mut self, tags: &str) {
        self.properties.insert(PROPERTY_TAGS, tags);
    }

    pub fn get_tags(&self) -> Option<&str> {
        self.properties.get(PROPERTY_TAGS)
    }

    pub fn set_keys(&mut self, keys: &str) {
        self.properties.insert(PROPERTY_KEYS, keys);
    }

    pub fn get_keys(&self) -> Option<&str> {
        self.properties.get(PROPERTY_KEYS)
    }

    pub fn set_delay_time_level(&mut self, level: i32) {
        self.properties.insert(PROPERTY_DELAY_TIME_LEVEL, level.to_string());
    }

    /// Python 返回原始字符串（可能是 `None`）。
    pub fn get_delay_time_level(&self) -> Option<&str> {
        self.properties.get(PROPERTY_DELAY_TIME_LEVEL)
    }

    /// Java `Message#getDelayTimeLevel()`：缺失或非法一律 0（表示非延时消息）。
    pub fn delay_time_level(&self) -> i32 {
        match self.get_delay_time_level() {
            Some(v) => v.trim().parse::<i32>().unwrap_or(0),
            None => 0,
        }
    }

    pub fn set_wait_store_msg_ok(&mut self, ok: bool) {
        self.properties.insert(PROPERTY_WAIT_STORE_MSG_OK, if ok { "true" } else { "false" });
    }

    pub fn get_wait_store_msg_ok(&self) -> Option<&str> {
        self.properties.get(PROPERTY_WAIT_STORE_MSG_OK)
    }

    pub fn set_user_property(&mut self, name: &str, value: &str) {
        self.properties.insert(name, value);
    }

    pub fn get_user_property(&self, name: &str) -> Option<&str> {
        self.properties.get(name)
    }

    pub fn put_property(&mut self, name: &str, value: &str) {
        self.properties.insert(name, value);
    }

    pub fn remove_property(&mut self, name: &str) {
        self.properties.remove(name);
    }

    pub fn get_property(&self, name: &str) -> Option<&str> {
        self.properties.get(name)
    }

    pub fn clear_property(&mut self) {
        self.properties = StringMap::new();
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Message(topic='{}', body={} bytes)", self.topic, self.get_body().len())
    }
}

/// 对应 Java `MessageExt` / Python `MessageExt`（拉取到的消息）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MessageExt {
    pub topic: String,
    pub flag: i32,
    /// 17 段格式里 `bodyLen <= 0` 或 `readBody=false` 时为 `None`（Python 同语义）。
    pub body: Option<Vec<u8>>,
    pub properties: StringMap,
    pub transaction_id: Option<String>,

    pub queue_id: i32,
    pub store_size: i32,
    pub queue_offset: i64,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub born_host: Option<String>,
    pub born_host_port: u32,
    pub store_timestamp: i64,
    pub store_host: Option<String>,
    pub store_host_port: u32,
    pub msg_id: Option<String>,
    pub commit_log_offset: i64,
    pub body_crc: u32,
    pub reconsume_times: i32,
    pub prepared_transaction_offset: i64,
    pub broker_name: Option<String>,
    pub offset_msg_id: Option<String>,
    pub msg_type: Option<String>,
}

impl MessageExt {
    /// 空消息（对应 Python `MessageExt()`）。
    pub fn new() -> MessageExt {
        MessageExt {
            topic: String::new(),
            body: Some(Vec::new()),
            ..Default::default()
        }
    }

    /// 由待发消息派生（对应 Java `MessageExt` 继承 `Message` 的那部分字段）。
    pub fn from_message(msg: &Message) -> MessageExt {
        MessageExt {
            topic: msg.topic.clone(),
            flag: msg.flag,
            body: msg.body.clone(),
            properties: msg.properties.clone(),
            transaction_id: msg.transaction_id.clone(),
            ..Default::default()
        }
    }

    /// 剥回普通消息（上层「重投 / 转发」场景只要正文和属性）。
    pub fn to_message(&self) -> Message {
        Message {
            topic: self.topic.clone(),
            flag: self.flag,
            body: self.body.clone(),
            properties: self.properties.clone(),
            transaction_id: self.transaction_id.clone(),
        }
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: &str) {
        self.topic = topic.to_string();
    }

    pub fn get_body(&self) -> &[u8] {
        self.body.as_deref().unwrap_or(&[])
    }

    pub fn set_body(&mut self, body: Option<&[u8]>) {
        self.body = body.map(|b| b.to_vec());
    }

    pub fn get_flag(&self) -> i32 {
        self.flag
    }

    pub fn set_flag(&mut self, flag: i32) {
        self.flag = flag;
    }

    pub fn get_properties(&self) -> &StringMap {
        &self.properties
    }

    pub fn set_properties(&mut self, properties: StringMap) {
        self.properties = properties;
    }

    pub fn get_property(&self, name: &str) -> Option<&str> {
        self.properties.get(name)
    }

    pub fn put_property(&mut self, name: &str, value: &str) {
        self.properties.insert(name, value);
    }

    pub fn set_tags(&mut self, tags: &str) {
        self.properties.insert(PROPERTY_TAGS, tags);
    }

    pub fn get_tags(&self) -> Option<&str> {
        self.properties.get(PROPERTY_TAGS)
    }

    pub fn set_keys(&mut self, keys: &str) {
        self.properties.insert(PROPERTY_KEYS, keys);
    }

    pub fn get_keys(&self) -> Option<&str> {
        self.properties.get(PROPERTY_KEYS)
    }

    pub fn set_delay_time_level(&mut self, level: i32) {
        self.properties.insert(PROPERTY_DELAY_TIME_LEVEL, level.to_string());
    }

    pub fn get_delay_time_level(&self) -> Option<&str> {
        self.properties.get(PROPERTY_DELAY_TIME_LEVEL)
    }

    /// Java `MessageExt#getDelayTimeLevel()`：缺失/非法一律 0。
    pub fn delay_time_level(&self) -> i32 {
        match self.get_delay_time_level() {
            Some(v) => v.trim().parse::<i32>().unwrap_or(0),
            None => 0,
        }
    }

    pub fn set_wait_store_msg_ok(&mut self, ok: bool) {
        self.properties.insert(PROPERTY_WAIT_STORE_MSG_OK, if ok { "true" } else { "false" });
    }

    pub fn get_wait_store_msg_ok(&self) -> Option<&str> {
        self.properties.get(PROPERTY_WAIT_STORE_MSG_OK)
    }

    pub fn remove_property(&mut self, name: &str) {
        self.properties.remove(name);
    }

    pub fn get_user_property(&self, name: &str) -> Option<&str> {
        self.properties.get(name)
    }

    pub fn get_transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub fn set_transaction_id(&mut self, transaction_id: Option<&str>) {
        self.transaction_id = transaction_id.map(|s| s.to_string());
    }

    pub fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    pub fn get_store_size(&self) -> i32 {
        self.store_size
    }

    pub fn set_store_size(&mut self, size: i32) {
        self.store_size = size;
    }

    pub fn get_queue_offset(&self) -> i64 {
        self.queue_offset
    }

    pub fn set_queue_offset(&mut self, queue_offset: i64) {
        self.queue_offset = queue_offset;
    }

    pub fn get_sys_flag(&self) -> i32 {
        self.sys_flag
    }

    pub fn set_sys_flag(&mut self, sys_flag: i32) {
        self.sys_flag = sys_flag;
    }

    pub fn get_born_timestamp(&self) -> i64 {
        self.born_timestamp
    }

    pub fn set_born_timestamp(&mut self, born_timestamp: i64) {
        self.born_timestamp = born_timestamp;
    }

    pub fn get_born_host(&self) -> Option<&str> {
        self.born_host.as_deref()
    }

    pub fn set_born_host(&mut self, born_host: Option<&str>) {
        self.born_host = born_host.map(|s| s.to_string());
    }

    pub fn get_store_timestamp(&self) -> i64 {
        self.store_timestamp
    }

    pub fn set_store_timestamp(&mut self, ts: i64) {
        self.store_timestamp = ts;
    }

    pub fn get_store_host(&self) -> Option<&str> {
        self.store_host.as_deref()
    }

    pub fn set_store_host(&mut self, store_host: Option<&str>) {
        self.store_host = store_host.map(|s| s.to_string());
    }

    pub fn get_msg_id(&self) -> Option<&str> {
        self.msg_id.as_deref()
    }

    pub fn set_msg_id(&mut self, msg_id: Option<&str>) {
        self.msg_id = msg_id.map(|s| s.to_string());
    }

    pub fn get_commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }

    pub fn set_commit_log_offset(&mut self, offset: i64) {
        self.commit_log_offset = offset;
    }

    pub fn get_body_crc(&self) -> u32 {
        self.body_crc
    }

    pub fn set_body_crc(&mut self, crc: u32) {
        self.body_crc = crc;
    }

    pub fn get_reconsume_times(&self) -> i32 {
        self.reconsume_times
    }

    pub fn set_reconsume_times(&mut self, n: i32) {
        self.reconsume_times = n;
    }

    pub fn get_prepared_transaction_offset(&self) -> i64 {
        self.prepared_transaction_offset
    }

    pub fn set_prepared_transaction_offset(&mut self, offset: i64) {
        self.prepared_transaction_offset = offset;
    }

    pub fn set_broker_name(&mut self, broker_name: Option<&str>) {
        self.broker_name = broker_name.map(|s| s.to_string());
    }

    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn get_offset_msg_id(&self) -> Option<&str> {
        self.offset_msg_id.as_deref()
    }

    pub fn set_offset_msg_id(&mut self, msg_id: Option<&str>) {
        self.offset_msg_id = msg_id.map(|s| s.to_string());
    }

    pub fn get_msg_type(&self) -> Option<&str> {
        self.msg_type.as_deref()
    }

    pub fn set_msg_type(&mut self, msg_type: Option<&str>) {
        self.msg_type = msg_type.map(|s| s.to_string());
    }

    /// 对应 Python `get_born_host_string`：端口为 0 时只返回 IP。
    pub fn get_born_host_string(&self) -> Option<String> {
        host_with_port(self.born_host.as_deref(), self.born_host_port)
    }

    /// 对应 Python `get_store_host_string`。
    pub fn get_store_host_string(&self) -> Option<String> {
        host_with_port(self.store_host.as_deref(), self.store_host_port)
    }
}

fn host_with_port(host: Option<&str>, port: u32) -> Option<String> {
    match host {
        Some(h) if !h.is_empty() && port != 0 => Some(format!("{h}:{port}")),
        Some(h) => Some(h.to_string()),
        None => None,
    }
}

impl fmt::Display for MessageExt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageExt(topic='{}', msgId='{}', queueOffset={}, body={} bytes)",
            self.topic,
            self.msg_id.as_deref().unwrap_or(""),
            self.queue_offset,
            self.get_body().len()
        )
    }
}

/// 对应 Java `MessageBatch` / Python `MessageBatch`。
///
/// 自己没有序列化字段：正文由 [`encode`](Self::encode) 生成，即每条 6 段轻量格式
/// （见 [`crate::common::message_decoder`]）的拼接结果。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MessageBatch {
    /// 批量消息作为「一条消息」发送时的外层字段（topic / WAIT / body）。
    pub message: Message,
    pub messages: Vec<Message>,
}

impl MessageBatch {
    pub fn new(messages: Vec<Message>) -> MessageBatch {
        MessageBatch { message: Message::default(), messages }
    }

    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn topic(&self) -> &str {
        &self.message.topic
    }

    pub fn body(&self) -> &[u8] {
        self.message.get_body()
    }

    pub fn properties(&self) -> &StringMap {
        &self.message.properties
    }

    pub fn get_property(&self, name: &str) -> Option<&str> {
        self.message.get_property(name)
    }

    /// 对应 Java `MessageBatch#encode`。
    pub fn encode(&self) -> Vec<u8> {
        crate::common::message_decoder::encode_messages(&self.messages)
    }

    /// 对应 Java `MessageBatch.generateFromList`。
    ///
    /// 约束：非空；同一 topic；同一 waitStoreMsgOK；不允许延时消息；不允许重试 topic。
    /// Java 抛 `UnsupportedOperationException` / `IllegalArgumentException`，
    /// Python 统一成 `ValueError`，这里统一映射为 [`Error::Client`]。
    pub fn generate_from_list(messages: Vec<Message>) -> Result<MessageBatch> {
        if messages.is_empty() {
            return Err(Error::client("messages must not be null or empty"));
        }
        let first = &messages[0];
        for message in &messages {
            let delay_level = message.get_delay_time_level().unwrap_or("");
            let parsed = delay_level.trim().parse::<i64>().unwrap_or(0);
            if !delay_level.is_empty() && parsed > 0 {
                return Err(Error::client("Delayed messages are not supported for batching"));
            }
            if message.get_topic().starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX) {
                return Err(Error::client("Retry Group is not supported for batching"));
            }
        }
        for message in messages.iter().skip(1) {
            if first.get_topic() != message.get_topic() {
                return Err(Error::client("The topic of the messages in one batch should be the same"));
            }
            if first.get_wait_store_msg_ok() != message.get_wait_store_msg_ok() {
                return Err(Error::client(
                    "The waitStoreMsgOK of the messages in one batch should be the same",
                ));
            }
        }

        let topic = first.get_topic().to_string();
        let wait_store_msg_ok = first.get_wait_store_msg_ok() == Some("true");

        let mut batch = MessageBatch { message: Message::default(), messages };
        batch.message.set_topic(&topic);
        batch.message.set_wait_store_msg_ok(wait_store_msg_ok);
        let body = batch.encode();
        batch.message.set_body(Some(&body));
        Ok(batch)
    }
}

impl IntoIterator for MessageBatch {
    type Item = Message;
    type IntoIter = std::vec::IntoIter<Message>;

    fn into_iter(self) -> std::vec::IntoIter<Message> {
        self.messages.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message_decoder::encode_messages;

    #[test]
    fn message_queue_hashcode_matches_java() {
        // 与 Python tests/test_message_model.py 的字面量一致（rebalance 依赖它）
        let cases = [
            ("TopicTest", "BrokerA", 0, -1229861880i32),
            ("TopicTest", "BrokerA", 3, -1229861787),
            ("", "", 0, 29791),
            ("%RETRY%GroupA", "BrokerB", 1, 566955243),
            ("A", "B", 0, 93282), // cpp/tests/test_codec.cpp 同一条
        ];
        for (topic, broker, qid, expected) in cases {
            assert_eq!(MessageQueue::new(topic, broker, qid).hashcode(), expected);
        }
    }

    #[test]
    fn message_queue_equality_order_and_accessors() {
        let a = MessageQueue::new("TopicTest", "BrokerA", 1);
        let b = MessageQueue::new("TopicTest", "BrokerA", 1);
        let c = MessageQueue::new("TopicTest", "BrokerA", 2);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.hashcode(), b.hashcode());
        assert_eq!(a.compare_to(&b), Ordering::Equal);
        assert_eq!(a.compare_to(&c), Ordering::Less);
        assert_eq!(a.compare_to(&MessageQueue::new("TopicTest", "BrokerZ", 0)), Ordering::Less);
        assert_eq!(a.compare_to(&MessageQueue::new("AAATopic", "BrokerA", 1)), Ordering::Greater);
        assert!(c > a);

        let mut mq = MessageQueue::default();
        mq.set_topic("T");
        mq.set_broker_name("B");
        mq.set_queue_id(7);
        assert_eq!((mq.get_topic(), mq.get_broker_name(), mq.get_queue_id()), ("T", "B", 7));
        assert_eq!(mq.get_queue_id_str(), "7");
        assert_eq!(mq.to_string(), "T B 7");
    }

    #[test]
    fn message_defaults() {
        let msg = Message::new("TopicTest", Some(b"hello"));
        assert_eq!(msg.get_topic(), "TopicTest");
        assert_eq!(msg.get_body(), b"hello");
        assert_eq!(msg.get_flag(), 0);
        assert!(msg.get_properties().is_empty());
        assert_eq!(msg.get_transaction_id(), None);
        assert_eq!(Message::new("T", None).get_body(), b"");
    }

    #[test]
    fn tags_and_keys_go_to_properties_in_declaration_order() {
        let msg =
            Message::with_tags_and_keys("T", Some(b"x"), Some("TagA"), Some("k1 k2"), 0);
        assert_eq!(msg.get_tags(), Some("TagA"));
        assert_eq!(msg.get_keys(), Some("k1 k2"));
        assert_eq!(
            msg.properties.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec![PROPERTY_TAGS, PROPERTY_KEYS]
        );
    }

    #[test]
    fn empty_tags_are_not_written() {
        let msg = Message::with_tags_and_keys("T", Some(b"x"), Some(""), Some(""), 0);
        assert!(msg.get_properties().is_empty());
    }

    #[test]
    fn delay_and_wait_helpers() {
        let mut msg = Message::new("T", Some(b"x"));
        msg.set_delay_time_level(3);
        assert_eq!(msg.get_delay_time_level(), Some("3"));
        assert_eq!(msg.delay_time_level(), 3);
        let mut none = Message::new("T", Some(b"x"));
        assert_eq!(none.delay_time_level(), 0);
        none.put_property(PROPERTY_DELAY_TIME_LEVEL, "bogus");
        assert_eq!(none.delay_time_level(), 0);

        let mut m2 = Message::new("T", Some(b"x"));
        m2.set_wait_store_msg_ok(false);
        assert_eq!(m2.get_wait_store_msg_ok(), Some("false"));
        m2.set_wait_store_msg_ok(true);
        assert_eq!(m2.get_wait_store_msg_ok(), Some("true"));
    }

    #[test]
    fn user_properties() {
        let mut msg = Message::new("T", Some(b"x"));
        msg.set_user_property("a", "1");
        msg.put_property("b", "2");
        assert_eq!(msg.get_user_property("a"), Some("1"));
        assert_eq!(msg.get_property("b"), Some("2"));
        msg.remove_property("a");
        assert_eq!(msg.get_property("a"), None);
        msg.clear_property();
        assert!(msg.get_properties().is_empty());
    }

    #[test]
    fn message_ext_defaults() {
        let ext = MessageExt::new();
        assert_eq!(ext.queue_id, 0);
        assert_eq!(ext.store_size, 0);
        assert_eq!(ext.queue_offset, 0);
        assert_eq!(ext.sys_flag, 0);
        assert_eq!(ext.reconsume_times, 0);
        assert_eq!(ext.msg_id, None);
        assert_eq!(ext.offset_msg_id, None);
        assert_eq!(ext.body_crc, 0);
        assert_eq!(ext.get_body(), b"");
    }

    #[test]
    fn message_ext_accessors_round_trip() {
        let mut ext = MessageExt::new();
        ext.set_queue_id(2);
        ext.set_store_size(100);
        ext.set_queue_offset(9);
        ext.set_sys_flag(1);
        ext.set_born_timestamp(1700000000000);
        ext.set_store_timestamp(1700000000001);
        ext.set_born_host(Some("127.0.0.1"));
        ext.born_host_port = 10000;
        ext.set_store_host(Some("127.0.0.1"));
        ext.store_host_port = 10911;
        ext.set_msg_id(Some("0A0A0A0A0000XXXX"));
        ext.set_commit_log_offset(512);
        ext.set_body_crc(12345);
        ext.set_reconsume_times(2);
        ext.set_prepared_transaction_offset(7);
        ext.set_msg_type(Some("NORMAL"));

        assert_eq!((ext.get_queue_id(), ext.get_store_size(), ext.get_queue_offset()), (2, 100, 9));
        assert_eq!(ext.get_body_crc(), 12345);
        assert_eq!(ext.get_commit_log_offset(), 512);
        assert_eq!(ext.get_reconsume_times(), 2);
        assert_eq!(ext.get_prepared_transaction_offset(), 7);
        assert_eq!(ext.get_msg_type(), Some("NORMAL"));
        assert_eq!(ext.get_born_host_string().as_deref(), Some("127.0.0.1:10000"));
        assert_eq!(ext.get_store_host_string().as_deref(), Some("127.0.0.1:10911"));
    }

    #[test]
    fn host_string_without_port() {
        let mut ext = MessageExt::new();
        ext.set_born_host(Some("10.0.0.1"));
        assert_eq!(ext.get_born_host_string().as_deref(), Some("10.0.0.1"));
        assert_eq!(MessageExt::new().get_store_host_string(), None);
    }

    #[test]
    fn message_ext_and_message_convert() {
        let mut msg = Message::with_tags_and_keys("T", Some(b"b"), Some("TagA"), None, 5);
        msg.set_transaction_id(Some("tx-1"));
        let ext = MessageExt::from_message(&msg);
        assert_eq!(ext.get_tags(), Some("TagA"));
        assert_eq!(ext.get_flag(), 5);
        assert_eq!(ext.get_transaction_id(), Some("tx-1"));
        let back = ext.to_message();
        assert_eq!(back, msg);
    }

    #[test]
    fn batch_generate_from_list() {
        let batch = MessageBatch::generate_from_list(vec![
            Message::new("T", Some(b"a")),
            Message::new("T", Some(b"b")),
        ])
        .unwrap();
        assert_eq!(batch.topic(), "T");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.messages()[0].get_body(), b"a");
        assert!(!batch.body().is_empty());
        assert_eq!(batch.body(), encode_messages(batch.messages()).as_slice());
        assert_eq!(batch, MessageBatch::generate_from_list(vec![
            Message::new("T", Some(b"a")),
            Message::new("T", Some(b"b")),
        ]).unwrap());
    }

    #[test]
    fn batch_rejects_illegal_lists() {
        assert!(MessageBatch::generate_from_list(vec![]).is_err());
        assert!(MessageBatch::generate_from_list(vec![
            Message::new("T1", Some(b"a")),
            Message::new("T2", Some(b"b")),
        ])
        .is_err());
        let mut delayed = Message::new("T", Some(b"a"));
        delayed.set_delay_time_level(2);
        assert!(MessageBatch::generate_from_list(vec![delayed]).is_err());
        assert!(
            MessageBatch::generate_from_list(vec![Message::new("%RETRY%GroupA", Some(b"a"))])
                .is_err()
        );
        let mut w1 = Message::new("T", Some(b"a"));
        w1.set_wait_store_msg_ok(true);
        let mut w2 = Message::new("T", Some(b"b"));
        w2.set_wait_store_msg_ok(false);
        assert!(MessageBatch::generate_from_list(vec![w1, w2]).is_err());
        // 错误消息与 Python 保持一致（上层按字符串匹配日志）
        let err = MessageBatch::generate_from_list(vec![]).unwrap_err().to_string();
        assert!(err.contains("messages must not be null or empty"), "{err}");
    }

    #[test]
    fn batch_wait_store_msg_ok_is_propagated() {
        let mut a = Message::new("T", Some(b"a"));
        a.set_wait_store_msg_ok(true);
        let mut b = Message::new("T", Some(b"b"));
        b.set_wait_store_msg_ok(true);
        let batch = MessageBatch::generate_from_list(vec![a, b]).unwrap();
        assert_eq!(batch.get_property(PROPERTY_WAIT_STORE_MSG_OK), Some("true"));
    }

    #[test]
    fn display_helpers() {
        assert_eq!(Message::new("T", Some(b"abc")).to_string(), "Message(topic='T', body=3 bytes)");
        let mut ext = MessageExt::new();
        ext.set_topic("T");
        ext.set_body(Some(b"xy"));
        ext.set_msg_id(Some("ID"));
        assert_eq!(
            ext.to_string(),
            "MessageExt(topic='T', msgId='ID', queueOffset=0, body=2 bytes)"
        );
    }
}
