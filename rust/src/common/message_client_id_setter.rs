use crate::common::message::{Message, MessageExt};
use crate::common::message_const::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX;
use crate::common::util_all;

pub fn create_uniq_id() -> String {
    util_all::create_uniq_id()
}

/// Does **not** overwrite an existing value: the msgId must stay stable when the same message is resent on retry.
pub fn set_uniq_id(msg: &mut Message) {
    if msg
        .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .is_none()
    {
        msg.put_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, &create_uniq_id());
    }
}

pub fn get_uniq_id(msg: &Message) -> Option<String> {
    msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .map(str::to_string)
}

/// Reads `UNIQ_KEY` on the consumer side (the same property also exists on `MessageExt`, so both share one key name).
pub fn get_uniq_id_of_ext(msg: &MessageExt) -> Option<String> {
    msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .map(str::to_string)
}

/// Java `MessageClientExt#getMsgId()` parity on a consumed message: a
/// non-empty `UNIQ_KEY` property wins, otherwise the broker offset id kept in
/// `msg_id` is the fallback. Client-side decoding yields a `MessageClientExt`,
/// so every consumer-side call site that in Java reads `msg.getMsgId()` must go
/// through this helper instead of reading `msg_id` (the offset id) directly.
pub fn client_msg_id(msg: &MessageExt) -> String {
    if let Some(uniq) = msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) {
        if !uniq.is_empty() {
            return uniq.to_string();
        }
    }
    msg.msg_id.clone().unwrap_or_default()
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
        assert_eq!(
            get_uniq_id(&msg).unwrap(),
            first,
            "an existing value must not be overwritten"
        );
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

    const OFFSET_ID: &str = "0A0B0C0D0000000000000000000EA7";
    const UNIQ_ID: &str = "AC1E0A0B2C3D4E5F60718293A4B5C6D7";

    fn consumed(with_uniq: bool) -> MessageExt {
        let mut msg = MessageExt::new();
        msg.msg_id = Some(OFFSET_ID.to_string());
        if with_uniq {
            msg.put_property(UNIQ, UNIQ_ID);
        }
        msg
    }

    #[test]
    fn client_msg_id_prefers_uniq_key_and_falls_back_to_offset_id() {
        assert_eq!(client_msg_id(&consumed(true)), UNIQ_ID);
        assert_eq!(client_msg_id(&consumed(false)), OFFSET_ID);

        let mut empty = consumed(true);
        empty.put_property(UNIQ, "");
        assert_eq!(
            client_msg_id(&empty),
            OFFSET_ID,
            "empty UNIQ_KEY falls back"
        );

        let mut no_id = consumed(false);
        no_id.msg_id = None;
        assert_eq!(client_msg_id(&no_id), "", "no id at all yields empty");
    }
}
