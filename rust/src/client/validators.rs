//! **Why reject on the client side**: for an illegal topic/group name the broker would refuse too, but only
//! after the request actually goes out, yielding `TOPIC_NOT_EXIST` / `ILLEGAL_TOPIC` -- and `TOPIC_NOT_EXIST` is in the
//! send-retry **retryable code** set -- so every doomed message would burn through the retry count and the timeout budget
//! once, only to report the same cause at the end. Local validation makes such inputs fail on the first line of `send()`.

use crate::common::message::Message;
use crate::common::message_const::PROPERTY_INNER_MULTI_DISPATCH;
use crate::common::topic_validator::{
    GROUP_MAX_LENGTH, TOPIC_MAX_LENGTH, VALID_CHAR_PATTERN,
    is_not_allowed_send_topic as in_forbidden_send_set, is_system_topic as in_system_topic_set,
    is_topic_or_group_illegal,
};
use crate::common::util_all::is_blank;
use crate::error::{Error, Result};
use crate::remoting::protocol::codes::response_code::MESSAGE_ILLEGAL;

pub const CHARACTER_MAX_LENGTH: i32 = 255;

pub const FILE_SEPARATOR: char = std::path::MAIN_SEPARATOR;

pub fn check_group(group: &str) -> Result<()> {
    if is_blank(Some(group)) {
        return Err(Error::client("the specified group is blank"));
    }
    if group.chars().count() > GROUP_MAX_LENGTH as usize {
        return Err(Error::client(format!(
            "the specified group[{group}] is longer than group max length: {GROUP_MAX_LENGTH}."
        )));
    }
    if is_topic_or_group_illegal(group) {
        return Err(Error::client(format!(
            "the specified group[{group}] contains illegal characters, allowing only {VALID_CHAR_PATTERN}"
        )));
    }
    Ok(())
}

/// Check order: blank -> length (127) -> character set.
pub fn check_topic(topic: &str) -> Result<()> {
    if is_blank(Some(topic)) {
        return Err(Error::client("The specified topic is blank"));
    }
    if topic.chars().count() > TOPIC_MAX_LENGTH as usize {
        return Err(Error::client(format!(
            "The specified topic is longer than topic max length {TOPIC_MAX_LENGTH}."
        )));
    }
    if is_topic_or_group_illegal(topic) {
        return Err(Error::client(format!(
            "The specified topic[{topic}] contains illegal characters, allowing only {VALID_CHAR_PATTERN}"
        )));
    }
    Ok(())
}

/// Creating a topic whose name collides with a broker-internal resource would silently tamper with system bookkeeping.
pub fn is_system_topic(topic: &str) -> Result<()> {
    if in_system_topic_set(topic) {
        return Err(Error::client(format!(
            "The topic[{topic}] is conflict with system topic."
        )));
    }
    Ok(())
}

/// Rejects topics that are forbidden send targets.
pub fn is_not_allowed_send_topic(topic: &str) -> Result<()> {
    if in_forbidden_send_set(topic) {
        return Err(Error::client(format!(
            "Sending message to topic[{topic}] is forbidden."
        )));
    }
    Ok(())
}

pub fn check_message(msg: &Message, max_message_size: i32) -> Result<()> {
    check_topic(&msg.topic)?;
    is_not_allowed_send_topic(&msg.topic)?;

    let body_illegal = |m: String| Error::client_with_code(MESSAGE_ILLEGAL, m);
    match msg.body.as_deref() {
        None => return Err(body_illegal("the message body is null".to_string())),
        Some([]) => return Err(body_illegal("the message body length is zero".to_string())),
        Some(body) => {
            let len = body.len();
            if len > max_message_size as usize {
                return Err(body_illegal(format!(
                    "the message body size over max value, MAX: {max_message_size}"
                )));
            }
        }
    }

    // A multi-queue dispatch (LMQ) path containing the filesystem separator would let the broker assemble an out-of-bounds path when creating queues
    if let Some(lmq_path) = msg.get_property(PROPERTY_INNER_MULTI_DISPATCH) {
        if !lmq_path.is_empty() && lmq_path.contains(FILE_SEPARATOR) {
            return Err(body_illegal(format!(
                "INNER_MULTI_DISPATCH {lmq_path} can not contains {FILE_SEPARATOR} character"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(topic: &str, body: Option<&[u8]>) -> Message {
        Message {
            topic: topic.to_string(),
            body: body.map(|b| b.to_vec()),
            ..Default::default()
        }
    }

    fn repeated(c: char, n: usize) -> String {
        std::iter::repeat_n(c, n).collect()
    }

    /// Extracts (message, response_code) from `Error::Client`; any other variant fails the test.
    fn client_err(e: Error) -> (String, Option<i32>) {
        match e {
            Error::Client { response_code, message } => (message, response_code),
            other => panic!("expected Error::Client, got {other:?}"),
        }
    }

    #[test]
    fn check_group_rules_and_texts_match_broker_contract() {
        for blank in ["", "   ", "\t "] {
            let (m, code) = client_err(check_group(blank).unwrap_err());
            assert_eq!(m, "the specified group is blank");
            assert_eq!(code, None);
        }
        check_group(&repeated('g', 120)).expect("120 should pass");
        let too_long = repeated('g', 121);
        let (m, code) = client_err(check_group(&too_long).unwrap_err());
        assert_eq!(
            m,
            format!("the specified group[{too_long}] is longer than group max length: 120.")
        );
        assert_eq!(code, None);
        let (m, _) = client_err(check_group("CID 001").unwrap_err());
        assert_eq!(
            m,
            format!("the specified group[CID 001] contains illegal characters, allowing only {VALID_CHAR_PATTERN}")
        );
        check_group("CID_ok-1|2%3").expect("legal group name");
    }

    #[test]
    fn check_topic_rules_and_texts_match_broker_contract() {
        let (m, _) = client_err(check_topic("").unwrap_err());
        assert_eq!(m, "The specified topic is blank");
        check_topic(&repeated('a', 127)).expect("127 should pass");
        let (m, _) = client_err(check_topic(&repeated('a', 128)).unwrap_err());
        assert_eq!(m, "The specified topic is longer than topic max length 127.");
        let (m, _) = client_err(check_topic("bad.topic").unwrap_err());
        assert_eq!(
            m,
            format!("The specified topic[bad.topic] contains illegal characters, allowing only {VALID_CHAR_PATTERN}")
        );
    }

    #[test]
    fn system_and_forbidden_topics_throw_with_client_side_code() {
        let (m, code) = client_err(is_system_topic("rmq_sys_watermark").unwrap_err());
        assert_eq!(m, "The topic[rmq_sys_watermark] is conflict with system topic.");
        assert_eq!(code, None);
        is_system_topic("MyTopic").expect("not a system topic");

        let (m, code) = client_err(is_not_allowed_send_topic("SCHEDULE_TOPIC_XXXX").unwrap_err());
        assert_eq!(
            m,
            "Sending message to topic[SCHEDULE_TOPIC_XXXX] is forbidden."
        );
        assert_eq!(code, None);
    }

    #[test]
    fn check_message_body_branches_carry_code_13() {
        let (m, code) = client_err(check_message(&msg("T1", None), 4096).unwrap_err());
        assert_eq!(m, "the message body is null");
        assert_eq!(code, Some(MESSAGE_ILLEGAL));

        let (m, code) = client_err(check_message(&msg("T1", Some(&[])), 4096).unwrap_err());
        assert_eq!(m, "the message body length is zero");
        assert_eq!(code, Some(MESSAGE_ILLEGAL));

        let (m, code) = client_err(check_message(&msg("T1", Some(&[7u8; 4097])), 4096).unwrap_err());
        assert_eq!(m, "the message body size over max value, MAX: 4096");
        assert_eq!(code, Some(MESSAGE_ILLEGAL));

        check_message(&msg("T1", Some(&[7u8; 4096])), 4096).expect("equal to the threshold should pass");
    }

    #[test]
    fn check_message_validates_topic_before_body() {
        let (m, code) = client_err(check_message(&msg("bad.topic", Some(&[])), 4096).unwrap_err());
        assert!(m.starts_with("The specified topic[bad.topic]"), "{m}");
        assert_eq!(code, None);
    }

    #[test]
    fn check_message_allows_retry_topic_and_rejects_lmq_separator() {
        // %RETRY%group is a normal sendMessageBack target and must never be forbidden
        check_message(&msg("%RETRY%myGroup", Some(&[1])), 4096).expect("%RETRY% should pass");
        check_message(&msg("%DLQ%myGroup", Some(&[1])), 4096).expect("%DLQ% should pass");

        let mut m = msg("T1", Some(&[1]));
        m.put_property(PROPERTY_INNER_MULTI_DISPATCH, &format!("queueA{FILE_SEPARATOR}extra"));
        let (text, code) = client_err(check_message(&m, 4096).unwrap_err());
        assert_eq!(
            text,
            format!(
                "INNER_MULTI_DISPATCH queueA{FILE_SEPARATOR}extra can not contains \
                 {FILE_SEPARATOR} character"
            )
        );
        assert_eq!(code, Some(MESSAGE_ILLEGAL));

        // Normal LMQ path (comma-separated multiple queues) passes
        let mut ok = msg("T1", Some(&[1]));
        ok.put_property(PROPERTY_INNER_MULTI_DISPATCH, "queueA,queueB");
        check_message(&ok, 4096).expect("comma-separated LMQ path should pass");
    }
}
