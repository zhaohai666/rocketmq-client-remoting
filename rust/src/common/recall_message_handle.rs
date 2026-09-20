//! 定时/延迟消息的 recall handle（对应 `org.apache.rocketmq.common.producer.RecallMessageHandle`）。
//!
//! handle 由 **broker** 在 SEND 响应的 `recallHandle` 字段里下发（只给定时消息，见
//! `SendMessageProcessor#attachRecallHandle`），内容是 base64url 编码的
//! `v1 <topic> <brokerName> <deliverMs> <uniqKey>`（空格分隔）。
//! 客户端 `recallMessage(370)` 时既要原样把 handle 发回去，又要从里面读 `brokerName`
//! 来决定请求打到哪台 broker —— 拿不到 handle 就只能靠 topic 路由碰运气。

use base64::Engine;

use crate::error::{Error, Result};

/// Java `RecallMessageHandle` 里的两个私有常量。
const SEPARATOR: &str = " ";
const VERSION_1: &str = "v1";

/// Java `DecoderException("recall handle is invalid")` 的文案，broker 也原样回传。
const INVALID_HANDLE: &str = "recall handle is invalid";

/// Java `RecallMessageHandle.HandleV1`。
///
/// `timestamp_str` 保持字符串：Java 侧就是 `String`，broker 按
/// `NumberUtils.toLong(..., -1)` 宽松解析，坏值该由 broker 判定，客户端不该抢跑。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandleV1 {
    pub topic: String,
    pub broker_name: String,
    pub timestamp_str: String,
    pub message_id: String,
}

/// 对应 Java `HandleV1.buildHandle`（Java 只有 broker 调用；客户端带上它才可能在单测里
/// 造出真 handle，同时也是这个格式的唯一权威定义）。
///
/// 编码照 Java `Base64.getUrlEncoder()` —— **含 `=` 填充**。
/// 不做参数校验，与 Java 的 `// no param check` 一致。
pub fn build_handle(topic: &str, broker_name: &str, timestamp_str: &str, message_id: &str) -> String {
    let raw = [VERSION_1, topic, broker_name, timestamp_str, message_id].join(SEPARATOR);
    base64::engine::general_purpose::URL_SAFE.encode(raw.as_bytes())
}

/// 对应 Java `RecallMessageHandle.decodeHandle`：空串、非法 base64、版本不是 `v1`、
/// 段数不足 5 段都报 `recall handle is invalid`。
///
/// ⚠ Java 用 `Base64.getUrlDecoder()`，它既不要求也不拒绝 `=` 填充；Rust 的
/// `URL_SAFE_NO_PAD` 遇到填充字符会直接报错，所以先剥掉尾部 `=` 再解 —— 这样
/// Java broker 下发的带填充 handle 和其他客户端产出的无填充 handle 都能吃下。
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

    /// 取出 `Error::Client` 里的原始文案（Java 的异常 message 要逐字对得上）。
    fn client_message(err: Error) -> String {
        match err {
            Error::Client { message, .. } => message,
            other => panic!("期望客户端错误，实际 {other}"),
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

    /// 载荷 `"v1 TopicA broker-a 1700000000000 <UNIQ_KEY>"` 的 base64url，
    /// 用 `java.util.Base64.getUrlEncoder()` 口径（含填充）算出。
    #[test]
    fn encoding_matches_the_java_vector() {
        assert_eq!(
            "djEgVG9waWNBIGJyb2tlci1hIDE3MDAwMDAwMDAwMDAg\
             MDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlhYmNkZWY=",
            build_handle("TopicA", "broker-a", "1700000000000", UNIQ_KEY)
        );
    }

    #[test]
    fn java_padded_and_unpadded_handles_both_decode() {
        let padded = build_handle("TopicA", "broker-a", "1700000000000", UNIQ_KEY);
        let unpadded = padded.trim_end_matches('=').to_string();
        assert_ne!(padded, unpadded, "Java 的编码器确实会补填充");
        assert_eq!(
            decode_handle(&unpadded).expect("unpadded"),
            decode_handle(&padded).expect("padded")
        );
    }

    #[test]
    fn bad_handles_are_rejected_with_the_java_message() {
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

    /// Java `split(" ")` 只丢尾部空段，中间空段算一段 —— 载荷里多一段仍然合法，
    /// 前 5 段之外的内容照 Java 被忽略。
    #[test]
    fn extra_segments_are_ignored_like_java_split() {
        let raw = format!("v1 TopicA broker-a 1700000000000 {UNIQ_KEY} trailing");
        let encoded = base64::engine::general_purpose::URL_SAFE.encode(raw);
        assert_eq!(UNIQ_KEY, decode_handle(&encoded).expect("valid").message_id);
    }
}
