//! 客户端消息唯一 ID（对应 `org.apache.rocketmq.common.message.MessageClientIDSetter`，
//! 逐条对齐 `python/rocketmq/common/message_client_id_setter.py`）。
//!
//! 用途：
//! * [`create_uniq_id`] 生成 32 位十六进制唯一 ID（IP + PID + 类哈希 + 当日毫秒 + 自增），
//!   实现在 [`crate::common::util_all::create_uniq_id`]（Python 也把它放在 `util_all.InnerIdGenerator`）；
//! * [`set_uniq_id`] 发送前把 `UNIQ_KEY` 写到消息属性上（Java 在
//!   `DefaultMQProducerImpl.sendKernelImpl` 里对**非批量**消息调用）；
//! * [`get_uniq_id`] 取 `UNIQ_KEY` —— `SendResult.msgId`、消息轨迹的 msgId、事务消息的
//!   transactionId 都用它。
//!
//! ⚠ 没有它的话 `SendResult.msgId` 只能退化成 broker 的 offsetMsgId（含 commitlog 偏移），
//! 与 Java 的语义不同，且轨迹里的 msgId 与消费侧对不上。

use crate::common::message::{Message, MessageBatch, MessageExt};
use crate::common::message_const::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX;
use crate::common::util_all;

/// 生成唯一 ID（对应 Java `MessageClientIDSetter.createUniqID`）。
pub fn create_uniq_id() -> String {
    util_all::create_uniq_id()
}

/// `UNIQ_KEY` 缺失时才写入（对应 Java `MessageClientIDSetter.setUniqID`）。
///
/// 已有值时**不覆盖**：重试发送同一条消息时 msgId 必须保持稳定。
pub fn set_uniq_id(msg: &mut Message) {
    if msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).is_none() {
        msg.put_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, &create_uniq_id());
    }
}

/// 取 `UNIQ_KEY`（对应 Java `MessageClientIDSetter.getUniqID`）。
pub fn get_uniq_id(msg: &Message) -> Option<String> {
    msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).map(str::to_string)
}

/// 消费侧取 `UNIQ_KEY`（Python 的 duck typing 让同一个函数也吃 `MessageExt`）。
pub fn get_uniq_id_of_ext(msg: &MessageExt) -> Option<String> {
    msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).map(str::to_string)
}

/// 批量消息的 `UNIQ_KEY`：逐条 ID 用 `,` 拼接（对应 Java `getUniqID(MessageBatch)`）。
///
/// 注意 Python 的 `MessageBatch.generate_from_list` **不会**给子消息写 UNIQ_KEY
/// （Java 会），所以直接构造的批量消息这里通常返回 None，
/// `SendResult.msgId` 随即回落成 broker 的 offsetMsgId —— 与 Python 行为一致。
pub fn get_uniq_id_from_batch(batch: &MessageBatch) -> Option<String> {
    let ids: Vec<String> = batch
        .messages()
        .iter()
        .filter_map(get_uniq_id)
        .filter(|s| !s.is_empty())
        .collect();
    if ids.is_empty() {
        return None;
    }
    Some(ids.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message_const::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX as UNIQ;

    #[test]
    fn set_uniq_id_is_idempotent() {
        let mut msg = Message::new("TopicTest", Some(b"body"));
        set_uniq_id(&mut msg);
        let first = get_uniq_id(&msg).expect("第一次必须写入");
        set_uniq_id(&mut msg);
        assert_eq!(get_uniq_id(&msg).unwrap(), first, "已有值不得覆盖");
        // 与 Python 一致：32 个大写 hex（IPv4 环境）
        assert_eq!(first.len(), 32, "got {first}");
        assert_eq!(first, first.to_ascii_uppercase());
    }

    #[test]
    fn batch_uniq_id_joins_with_comma() {
        let one = {
            let mut m = Message::new("T", Some(b"1"));
            set_uniq_id(&mut m);
            m
        };
        let mut plain = Message::new("T", Some(b"2"));
        plain.put_property(UNIQ, "ID2");
        let batch = MessageBatch::new(vec![one.clone(), plain]);
        let joined = get_uniq_id_from_batch(&batch).unwrap();
        assert_eq!(joined, format!("{},ID2", get_uniq_id(&one).unwrap()));

        // 全都没有 UNIQ_KEY → None（Python 的 generate_from_list 就是这个形态）
        let empty = MessageBatch::new(vec![Message::new("T", Some(b"a"))]);
        assert_eq!(get_uniq_id_from_batch(&empty), None);
        assert_eq!(get_uniq_id_from_batch(&MessageBatch::default()), None);
    }

    #[test]
    fn ext_and_message_share_the_property() {
        let mut msg = Message::new("T", Some(b"b"));
        msg.put_property(UNIQ, "ABC");
        let ext = MessageExt::from_message(&msg);
        assert_eq!(get_uniq_id_of_ext(&ext).as_deref(), Some("ABC"));
    }
}
