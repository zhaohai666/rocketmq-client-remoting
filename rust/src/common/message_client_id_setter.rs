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

use crate::common::message::{Message, MessageExt};
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
    fn ext_and_message_share_the_property() {
        let mut msg = Message::new("T", Some(b"b"));
        msg.put_property(UNIQ, "ABC");
        let ext = MessageExt::from_message(&msg);
        assert_eq!(get_uniq_id_of_ext(&ext).as_deref(), Some("ABC"));
    }
}
