//! The handle is issued by the **broker** in the `recallHandle` field of the SEND response (only for timer messages),
//! and its content is the base64url encoding of
//! `v1 <topic> <brokerName> <deliverMs> <uniqKey>` (space-separated).
//! When the client calls `recallMessage(370)` it must both send the handle back verbatim and read `brokerName` from it
//! to decide which broker the request goes to -- without the handle it can only fall back to topic routing and hope.

use base64::Engine;

use crate::error::{Error, Result};

const SEPARATOR: &str = " ";
const VERSION_1: &str = "v1";

const INVALID_HANDLE: &str = "recall handle is invalid";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandleV1 {
    pub topic: String,
    pub broker_name: String,
    pub timestamp_str: String,
    pub message_id: String,
}

pub fn build_handle(topic: &str, broker_name: &str, timestamp_str: &str, message_id: &str) -> String {
    let raw = [VERSION_1, topic, broker_name, timestamp_str, message_id].join(SEPARATOR);
    base64::engine::general_purpose::URL_SAFE.encode(raw.as_bytes())
}

pub fn decode_handle(handle: &str) -> Result<HandleV1> {
    if handle.is_empty() {
        return Err(Error::client(INVALID_HANDLE));
    }
    let stripped = handle.trim_end_matches('=');
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(stripped)
        .map_err(|_| Error::client(INVALID_HANDLE))?;
    let raw = String::from_utf8(decoded).map_err(|_| Error::client(INVALID_HANDLE))?;
    let items: Vec<&str> = raw.split(SEPARATOR).collect();
    if items.first().copied() != Some(VERSION_1) || items.len() < 5 {
        return Err(Error::client(INVALID_HANDLE));
    }
    Ok(HandleV1 {
        topic: items[1].to_string(),
        broker_name: items[2].to_string(),
        timestamp_str: items[3].to_string(),
        message_id: items[4].to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNIQ_KEY: &str = "0123456789ABCDEF0123456789abcdef";

    fn client_message(err: Error) -> String {
        match err {
            Error::Client { message, .. } => message,
            other => panic!("expected a client error, got {other}"),
        }
    }

    #[test]
    fn handle_round_trips() {
        let handle = build_handle("TopicA", "broker-a", "1700000000000", UNIQ_KEY);
        let decoded = decode_handle(&handle).expect("valid handle");
        assert_eq!(
            HandleV1 {
                topic: "TopicA".to_string(),
                broker_name: "broker-a".to_string(),
                timestamp_str: "1700000000000".to_string(),
                message_id: UNIQ_KEY.to_string(),
            },
            decoded
        );
    }

    /// base64url of the payload `"v1 TopicA broker-a 1700000000000 <UNIQ_KEY>"`,
    /// computed with the URL-safe Base64 encoder variant (with padding).
    #[test]
    fn encoding_matches_the_wire_vector() {
        assert_eq!(
            "djEgVG9waWNBIGJyb2tlci1hIDE3MDAwMDAwMDAwMDAg\
             MDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlhYmNkZWY=",
            build_handle("TopicA", "broker-a", "1700000000000", UNIQ_KEY)
        );
    }

    #[test]
    fn padded_and_unpadded_handles_both_decode() {
        let padded = build_handle("TopicA", "broker-a", "1700000000000", UNIQ_KEY);
        let unpadded = padded.trim_end_matches('=').to_string();
        assert_ne!(padded, unpadded, "the URL-safe encoder does add padding");
        assert_eq!(
            decode_handle(&unpadded).expect("unpadded"),
            decode_handle(&padded).expect("padded")
        );
    }

    #[test]
    fn bad_handles_are_rejected_with_the_canonical_message() {
        let v2 = base64::engine::general_purpose::URL_SAFE.encode(b"v2 a b c d");
        for bad in ["", "not base64 !!", "!!!!", &v2] {
            let err = decode_handle(bad).expect_err("must not decode");
            assert_eq!(INVALID_HANDLE, client_message(err), "{bad:?}");
        }
    }

    #[test]
    fn fewer_than_five_segments_is_invalid() {
        let short = base64::engine::general_purpose::URL_SAFE.encode(b"v1 TopicA broker-a");
        assert_eq!(
            INVALID_HANDLE,
            client_message(decode_handle(&short).unwrap_err())
        );
    }

    #[test]
    fn extra_segments_are_ignored_by_trailing_empty_split() {
        let raw = format!("v1 TopicA broker-a 1700000000000 {UNIQ_KEY} trailing");
        let encoded = base64::engine::general_purpose::URL_SAFE.encode(raw);
        assert_eq!(UNIQ_KEY, decode_handle(&encoded).expect("valid").message_id);
    }
}
