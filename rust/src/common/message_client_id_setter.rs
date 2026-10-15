
use crate::common::message::{Message, MessageExt};
use crate::common::message_const::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX;
use crate::common::util_all;

pub fn create_uniq_id() -> String {
    util_all::create_uniq_id()
}

/// Does **not** overwrite an existing value: the msgId must stay stable when the same message is resent on retry.
pub fn set_uniq_id(msg: &mut Message) {
    if msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).is_none() {
        msg.put_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, &create_uniq_id());
    }
}

pub fn get_uniq_id(msg: &Message) -> Option<String> {
    msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).map(str::to_string)
}

/// Reads `UNIQ_KEY` on the consumer side (the same property also exists on `MessageExt`, so both share one key name).
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
        let first = get_uniq_id(&msg).expect("first call must write the value");
        set_uniq_id(&mut msg);
        assert_eq!(get_uniq_id(&msg).unwrap(), first, "an existing value must not be overwritten");
        // Protocol literal: 32 uppercase hex chars (IPv4 environment)
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
