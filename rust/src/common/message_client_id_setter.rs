
use crate::common::message::{Message, MessageExt};
use crate::common::message_const::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX;
use crate::common::util_all;

pub fn create_uniq_id() -> String {
    util_all::create_uniq_id()
}

/// 已有值时**不覆盖**：重试发送同一条消息时 msgId 必须保持稳定。
pub fn set_uniq_id(msg: &mut Message) {
    if msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).is_none() {
        msg.put_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, &create_uniq_id());
    }
}

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
