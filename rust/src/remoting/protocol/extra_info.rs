//! Encoding/decoding of the POP-mode extraInfo (colloquially the CK string).
//!
//!
//! The CK string is the core ticket of POP mode: the broker does **not** write the `POP_CK`
//! property on ordinary-topic messages in POP responses (only the retry-topic re-encode path
//! writes it), so the client must reconstruct the string itself from the response headers'
//! `startOffsetInfo` / `msgOffsetInfo` and send it back as the `extraInfo` of ACK / CHANGE_MESSAGE_INVISIBLETIME. The format is 7~8 segments, **space**-separated:
//!
//! ```text
//! ckQueueOffset popTime invisibleTime reviveQid retryFlag brokerName queueId [msgQueueOffset]
//! ```
//!
//! The separator is a space, not a comma -- getting it wrong makes the broker fail parsing **silently**.
//! `retryFlag` is not the topic text itself but the `"0"/"1"/"2"` computed by [`retry_of_topic`].

use crate::common::mix_all::MixAll;
use crate::error::{Error, Result};

pub const KEY_SEPARATOR: &str = " ";
/// Separator between queues (`startOffsetInfo` / `msgOffsetInfo` / `orderCountInfo`).
pub const QUEUE_SEPARATOR: &str = ";";
/// Separator between multiple offsets of the same queue inside `msgOffsetInfo`.
pub const OFFSET_SEPARATOR: &str = ",";

/// `retryFlag`: ordinary topic.
pub const NORMAL_TOPIC: &str = "0";
/// `retryFlag`: V1 retry topic (`%RETRY%<cid>_<topic>`).
pub const RETRY_TOPIC: &str = "1";
/// `retryFlag`: V2 retry topic (`%RETRY%<cid>+<topic>`).
pub const RETRY_TOPIC_V2: &str = "2";
pub const QUEUE_OFFSET: &str = "qo";

pub const POP_ORDER_REVIVE_QUEUE: i32 = 999;

const POP_RETRY_SEPARATOR_V1: &str = "_";
/// topic/cid joiner of the V2 retry topic.
const POP_RETRY_SEPARATOR_V2: &str = "+";

// ---------------------------------------------------------------- retry topic naming

pub fn build_pop_retry_topic_v1(topic: &str, cid: &str) -> String {
    format!(
        "{}{}{}{}",
        MixAll::RETRY_GROUP_TOPIC_PREFIX,
        cid,
        POP_RETRY_SEPARATOR_V1,
        topic
    )
}

pub fn build_pop_retry_topic_v2(topic: &str, cid: &str) -> String {
    format!(
        "{}{}{}{}",
        MixAll::RETRY_GROUP_TOPIC_PREFIX,
        cid,
        POP_RETRY_SEPARATOR_V2,
        topic
    )
}

pub fn build_pop_retry_topic(topic: &str, cid: &str, enable_retry_v2: bool) -> String {
    if enable_retry_v2 {
        build_pop_retry_topic_v2(topic, cid)
    } else {
        build_pop_retry_topic_v1(topic, cid)
    }
}

/// Checking for `+` alone is not enough: an ordinary topic name may also contain `+`, which would be misjudged as retry V2.
pub fn is_pop_retry_topic_v2(retry_topic: Option<&str>) -> bool {
    match retry_topic {
        None => false,
        Some(t) => {
            !t.is_empty()
                && t.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX)
                && t.contains(POP_RETRY_SEPARATOR_V2)
        }
    }
}

/// The business topic a POP retry name hides, or the name itself when it hides none.
///
/// A retried POP row is stored under `%RETRY%<group>_<topic>`, or under the newer
/// `%RETRY%<group>+<topic>`: everything after the separator is the topic the subscriber
/// actually named. `+` is tried first because it is the unambiguous one -- a business topic
/// may itself contain `_`, and cutting at the first `_` would hand back half a name.
///
/// Two names are deliberately left alone: `%RETRY%<group>` with nothing after the separator,
/// since an empty tail is not a topic name, and any name built on a different group, since
/// this consumer has no business rewriting another group's retry queue.
pub fn parse_pop_normal_topic(topic: &str, cid: &str) -> String {
    if topic.is_empty()
        || cid.is_empty()
        || !topic.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX)
    {
        return topic.to_string();
    }
    let head = format!("{}{}", MixAll::RETRY_GROUP_TOPIC_PREFIX, cid);
    for separator in [POP_RETRY_SEPARATOR_V2, POP_RETRY_SEPARATOR_V1] {
        let prefix = format!("{head}{separator}");
        if let Some(rest) = topic.strip_prefix(prefix.as_str()) {
            if !rest.is_empty() {
                return rest.to_string();
            }
        }
    }
    topic.to_string()
}

/// Order matters: test V2 (contains `+`) first, then the `%RETRY%` prefix (V1); otherwise an ordinary topic
/// would always be `"0"`.
pub fn retry_of_topic(topic: &str) -> &'static str {
    if is_pop_retry_topic_v2(Some(topic)) {
        RETRY_TOPIC_V2
    } else if topic.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX) {
        RETRY_TOPIC
    } else {
        NORMAL_TOPIC
    }
}

/// An unknown flag errors out instead of falling back to `topic`: falling back would ack a retry message as an ordinary one.
pub fn get_real_topic(topic: &str, cid: &str, retry: &str) -> Result<String> {
    if retry == NORMAL_TOPIC {
        Ok(topic.to_string())
    } else if retry == RETRY_TOPIC {
        Ok(build_pop_retry_topic_v1(topic, cid))
    } else if retry == RETRY_TOPIC_V2 {
        Ok(build_pop_retry_topic_v2(topic, cid))
    } else {
        Err(Error::Decode("getRetry fail, format is wrong".into()))
    }
}

// ---------------------------------------------------------------- CK string splitting and getters

fn split_drop_trailing(value: &str, sep: &str) -> Vec<String> {
    let mut parts: Vec<String> = value.split(sep).map(str::to_string).collect();
    while parts.last().map(|p| p.is_empty()).unwrap_or(false) {
        parts.pop();
    }
    parts
}

pub fn split(extra_info: &str) -> Result<Vec<String>> {
    Ok(split_drop_trailing(extra_info, KEY_SEPARATOR))
}

fn require(segments: &[String], need: usize, what: &str) -> Result<()> {
    if segments.len() < need {
        return Err(Error::Decode(format!(
            "{what} fail, extraInfoStrs length {}",
            segments.len()
        )));
    }
    Ok(())
}

/// Takes one segment and parses it as a long; a non-numeric value is an error, not a silent default.
fn segment_as(segments: &[String], index: usize, need: usize, what: &str) -> Result<i64> {
    require(segments, need, what)?;
    let raw = &segments[index];
    raw.trim().parse::<i64>().map_err(|_| {
        Error::Decode(format!(
            "{what} fail, extraInfoStrs[{index}]={raw:?} is not a number"
        ))
    })
}

/// `segments[0]`: the ckQueueOffset of this message within its queue.
pub fn get_ck_queue_offset(segments: &[String]) -> Result<i64> {
    segment_as(segments, 0, 1, "getCkQueueOffset")
}

/// `segments[1]`: POP time (milliseconds).
pub fn get_pop_time(segments: &[String]) -> Result<i64> {
    segment_as(segments, 1, 2, "getPopTime")
}

/// `segments[2]`: invisible time (milliseconds).
pub fn get_invisible_time(segments: &[String]) -> Result<i64> {
    segment_as(segments, 2, 3, "getInvisibleTime")
}

/// `segments[3]`: revive queue id; [`POP_ORDER_REVIVE_QUEUE`] means ordered consumption.
pub fn get_revive_qid(segments: &[String]) -> Result<i32> {
    Ok(segment_as(segments, 3, 4, "getReviveQid")? as i32)
}

/// `segments[4]`: the `retryFlag` text (`"0"/"1"/"2"`).
pub fn get_retry(segments: &[String]) -> Result<String> {
    require(segments, 5, "getRetry")?;
    Ok(segments[4].clone())
}

/// `segments[5]`: broker name.
pub fn get_broker_name(segments: &[String]) -> Result<String> {
    require(segments, 6, "getBrokerName")?;
    Ok(segments[5].clone())
}

/// `segments[6]`: queue id.
pub fn get_queue_id(segments: &[String]) -> Result<i32> {
    Ok(segment_as(segments, 6, 7, "getQueueId")? as i32)
}

/// `segments[7]`: the message offset within the queue (only present in 8-segment strings).
pub fn get_queue_offset(segments: &[String]) -> Result<i64> {
    segment_as(segments, 7, 8, "getQueueOffset")
}

pub fn is_order(segments: &[String]) -> Result<bool> {
    Ok(get_revive_qid(segments)? == POP_ORDER_REVIVE_QUEUE)
}

// ---------------------------------------------------------------- CK string assembly

#[allow(clippy::too_many_arguments)]
pub fn build_extra_info(
    ck_queue_offset: i64,
    pop_time: i64,
    invisible_time: i64,
    revive_qid: i32,
    topic: &str,
    broker_name: &str,
    queue_id: i32,
    msg_queue_offset: Option<i64>,
) -> String {
    let mut parts = vec![
        ck_queue_offset.to_string(),
        pop_time.to_string(),
        invisible_time.to_string(),
        revive_qid.to_string(),
        retry_of_topic(topic).to_string(),
        broker_name.to_string(),
        queue_id.to_string(),
    ];
    if let Some(offset) = msg_queue_offset {
        parts.push(offset.to_string());
    }
    parts.join(KEY_SEPARATOR)
}

/// Prepends `;` when `out` is non-empty, so callers can simply append queue after queue in order.
pub fn build_start_offset_info(out: &mut String, topic: &str, queue_id: i32, start_offset: i64) {
    append_queue_segment(
        out,
        &[
            retry_of_topic(topic),
            &queue_id.to_string(),
            &start_offset.to_string(),
        ],
    );
}

pub fn build_queue_id_order_count_info(
    out: &mut String,
    topic: &str,
    queue_id: i32,
    order_count: i32,
) {
    append_queue_segment(
        out,
        &[
            retry_of_topic(topic),
            &queue_id.to_string(),
            &order_count.to_string(),
        ],
    );
}

pub fn build_queue_offset_order_count_info(
    out: &mut String,
    topic: &str,
    queue_id: i64,
    queue_offset: i64,
    order_count: i32,
) {
    append_queue_segment(
        out,
        &[
            retry_of_topic(topic),
            &get_queue_offset_key_value_key(queue_id, queue_offset),
            &order_count.to_string(),
        ],
    );
}

pub fn build_msg_offset_info(out: &mut String, topic: &str, queue_id: i32, msg_offsets: &[i64]) {
    let joined = msg_offsets
        .iter()
        .map(|o| o.to_string())
        .collect::<Vec<String>>()
        .join(OFFSET_SEPARATOR);
    append_queue_segment(
        out,
        &[retry_of_topic(topic), &queue_id.to_string(), &joined],
    );
}

/// Appends one three-segment group: segments joined by spaces, groups joined by `;`.
fn append_queue_segment(out: &mut String, segments: &[&str]) {
    if !out.is_empty() {
        out.push_str(QUEUE_SEPARATOR);
    }
    out.push_str(&segments.join(KEY_SEPARATOR));
}

// ---------------------------------------------------------------- response header info table parsing

/// Common shape of the response header map: `"<retryFlag> <queueKey> <value>"`, multiple entries joined by `;`.
fn split_queue_segments(value: &str) -> Vec<String> {
    if value.contains(QUEUE_SEPARATOR) {
        split_drop_trailing(value, QUEUE_SEPARATOR)
    } else {
        vec![value.to_string()]
    }
}

/// Parses a single group and returns `(key, third)`; the key is `"<retryFlag>@<queueKey>"`.
///
/// Generic `T`: the three `parse*Info` functions have different value types (long / long list / int count),
/// and only `out` is used here for duplicate-key checks, so the concrete `T` does not matter.
fn one_segment<T>(
    one: &str,
    raw: &str,
    what: &str,
    out: &[(String, T)],
) -> Result<(String, String)> {
    let parts: Vec<&str> = one.split(KEY_SEPARATOR).collect();
    if parts.len() != 3 {
        return Err(Error::Decode(format!("parse {what} error, {raw}")));
    }
    let key = format!("{}@{}", parts[0], parts[1]);
    if out.iter().any(|(k, _)| k == &key) {
        return Err(Error::Decode(format!(
            "parse {what} error, duplicate, {raw}"
        )));
    }
    Ok((key, parts[2].to_string()))
}

/// Parses `startOffsetInfo`, shaped like `"0 3 0;0 2 1"`.
pub fn parse_start_offset_info(
    start_offset_info: Option<&str>,
) -> Result<Option<Vec<(String, i64)>>> {
    let raw = match start_offset_info {
        None | Some("") => return Ok(None),
        Some(v) => v,
    };
    let mut out: Vec<(String, i64)> = Vec::new();
    for one in split_queue_segments(raw) {
        let (key, third) = one_segment(&one, raw, "startOffsetInfo", &out)?;
        let value = third.trim().parse::<i64>().map_err(|_| {
            Error::Decode(format!(
                "parse startOffsetInfo error, {third:?} is not a long"
            ))
        })?;
        out.push((key, value));
    }
    Ok(Some(out))
}

/// `msgOffsetInfo` parse result: `("<retryFlag>@<queueKey>", offsets)` in insertion order.
pub type MsgOffsetTable = Vec<(String, Vec<i64>)>;

/// Parses `msgOffsetInfo`, shaped like `"0 3 0,1,2;1 2 5"`.
///
/// Same key as [`parse_start_offset_info`]; the value is the list of offsets of the messages popped from that queue this time.
pub fn parse_msg_offset_info(msg_offset_info: Option<&str>) -> Result<Option<MsgOffsetTable>> {
    let raw = match msg_offset_info {
        None | Some("") => return Ok(None),
        Some(v) => v,
    };
    let mut out: MsgOffsetTable = Vec::new();
    for one in split_queue_segments(raw) {
        let (key, third) = one_segment(&one, raw, "msgOffsetInfo", &out)?;
        let mut offsets = Vec::new();
        for item in split_drop_trailing(&third, OFFSET_SEPARATOR) {
            let value = item.trim().parse::<i64>().map_err(|_| {
                Error::Decode(format!("parse msgOffsetInfo error, {item:?} is not a long"))
            })?;
            offsets.push(value);
        }
        out.push((key, offsets));
    }
    Ok(Some(out))
}

/// Parses `orderCountInfo` (used by ordered consumption); the third segment is the count (int).
pub fn parse_order_count_info(
    order_count_info: Option<&str>,
) -> Result<Option<Vec<(String, i32)>>> {
    let raw = match order_count_info {
        None | Some("") => return Ok(None),
        Some(v) => v,
    };
    let mut out: Vec<(String, i32)> = Vec::new();
    for one in split_queue_segments(raw) {
        let (key, third) = one_segment(&one, raw, "orderCountInfo", &out)?;
        let value = third.trim().parse::<i32>().map_err(|_| {
            Error::Decode(format!(
                "parse orderCountInfo error, {third:?} is not an int"
            ))
        })?;
        out.push((key, value));
    }
    Ok(Some(out))
}

// ---------------------------------------------------------------- lookup keys

pub fn get_start_offset_info_map_key(topic: &str, key: i64) -> String {
    format!("{}@{}", retry_of_topic(topic), key)
}

pub fn get_queue_offset_key_value_key(queue_id: i64, queue_offset: i64) -> String {
    format!("{QUEUE_OFFSET}{queue_id}%{queue_offset}")
}

pub fn get_queue_offset_map_key(topic: &str, queue_id: i64, queue_offset: i64) -> String {
    format!(
        "{}@{}",
        retry_of_topic(topic),
        get_queue_offset_key_value_key(queue_id, queue_offset)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wire-record ground truth of the extra-info string (space-separated fields).
    const CK_NORMAL: &str = "12345 1700000000000 60000 0 0 broker-a 3";
    const CK_WITH_OFFSET: &str = "12345 1700000000000 60000 0 0 broker-a 3 67890";
    const CK_RETRY_V1: &str = "1 2 3 4 1 broker-a 0";
    const CK_RETRY_V2: &str = "1 2 3 4 2 broker-a 0";
    const CK_ORDER: &str = "7 8 9 999 0 broker-a 1 10";

    const TOPIC: &str = "BodyGoldenTopic";
    const GROUP: &str = "GID_BodyGolden";

    fn segments(text: &str) -> Vec<String> {
        split(text).unwrap()
    }

    #[test]
    fn build_extra_info_matches_reference_output() {
        assert_eq!(
            build_extra_info(12345, 1700000000000, 60000, 0, TOPIC, "broker-a", 3, None),
            CK_NORMAL
        );
        assert_eq!(
            build_extra_info(
                12345,
                1700000000000,
                60000,
                0,
                TOPIC,
                "broker-a",
                3,
                Some(67890)
            ),
            CK_WITH_OFFSET
        );
        // the 5th segment is the retryFlag, not the topic
        let v1 = build_pop_retry_topic_v1(TOPIC, GROUP);
        let v2 = build_pop_retry_topic_v2(TOPIC, GROUP);
        assert_eq!(v1, "%RETRY%GID_BodyGolden_BodyGoldenTopic");
        assert_eq!(v2, "%RETRY%GID_BodyGolden+BodyGoldenTopic");
        assert_eq!(build_pop_retry_topic(TOPIC, GROUP, false), v1);
        assert_eq!(build_pop_retry_topic(TOPIC, GROUP, true), v2);
        assert_eq!(
            build_extra_info(1, 2, 3, 4, &v1, "broker-a", 0, None),
            CK_RETRY_V1
        );
        assert_eq!(
            build_extra_info(1, 2, 3, 4, &v2, "broker-a", 0, None),
            CK_RETRY_V2
        );
        assert_eq!(
            build_extra_info(
                7,
                8,
                9,
                POP_ORDER_REVIVE_QUEUE,
                TOPIC,
                "broker-a",
                1,
                Some(10)
            ),
            CK_ORDER
        );
        // the segment-count difference between 7 and 8 is exactly the presence of msgQueueOffset
        assert_eq!(split(CK_NORMAL).unwrap().len(), 7);
        assert_eq!(split(CK_WITH_OFFSET).unwrap().len(), 8);
    }

    #[test]
    fn parse_pop_normal_topic_cuts_both_wire_spellings() {
        assert_eq!(parse_pop_normal_topic("%RETRY%GID_Topic", "GID"), "Topic");
        assert_eq!(parse_pop_normal_topic("%RETRY%GID+Topic", "GID"), "Topic");
        // The '+' right after the group is the separator, so a topic carrying an
        // underscore is read whole instead of being cut at its first '_'.
        assert_eq!(
            parse_pop_normal_topic("%RETRY%GID+A_B", "GID"),
            "A_B"
        );
        // Nothing after the separator is not a topic name, so the retry name stays.
        assert_eq!(parse_pop_normal_topic("%RETRY%GID_", "GID"), "%RETRY%GID_");
        assert_eq!(parse_pop_normal_topic("%RETRY%GID+", "GID"), "%RETRY%GID+");
        // The bare group retry topic and another group's retry topic are both left alone.
        assert_eq!(parse_pop_normal_topic("%RETRY%GID", "GID"), "%RETRY%GID");
        assert_eq!(
            parse_pop_normal_topic("%RETRY%Other+Topic", "GID"),
            "%RETRY%Other+Topic"
        );
        assert_eq!(parse_pop_normal_topic("Topic", "GID"), "Topic");
        assert_eq!(parse_pop_normal_topic("", "GID"), "");
        assert_eq!(parse_pop_normal_topic("%RETRY%GID_Topic", ""), "%RETRY%GID_Topic");
    }

    #[test]
    fn getters_read_every_position_and_validate_length() {
        let segs = segments(CK_WITH_OFFSET);
        assert_eq!(get_ck_queue_offset(&segs).unwrap(), 12345);
        assert_eq!(get_pop_time(&segs).unwrap(), 1700000000000);
        assert_eq!(get_invisible_time(&segs).unwrap(), 60000);
        assert_eq!(get_revive_qid(&segs).unwrap(), 0);
        assert_eq!(get_retry(&segs).unwrap(), "0");
        assert_eq!(get_broker_name(&segs).unwrap(), "broker-a");
        assert_eq!(get_queue_id(&segs).unwrap(), 3);
        assert_eq!(get_queue_offset(&segs).unwrap(), 67890);
        assert!(!is_order(&segs).unwrap());
        assert!(is_order(&segments(CK_ORDER)).unwrap());
        assert_eq!(
            get_real_topic(TOPIC, GROUP, &get_retry(&segments(CK_RETRY_V2)).unwrap()).unwrap(),
            build_pop_retry_topic_v2(TOPIC, GROUP)
        );

        let short = segments(CK_NORMAL);
        let err = get_queue_offset(&short).unwrap_err().to_string();
        assert!(
            err.contains("getQueueOffset fail, extraInfoStrs length 7"),
            "{err}"
        );
        // with fewer than 5 segments even retry cannot be read
        assert!(get_retry(&["1".to_string()])
            .unwrap_err()
            .to_string()
            .contains("getRetry fail, extraInfoStrs length 1"));
        // a non-numeric segment does not panic; it reports Decode
        let junk = segments("abc 2 3 4 0 broker-a 1 5");
        assert!(matches!(get_ck_queue_offset(&junk), Err(Error::Decode(_))));
    }

    #[test]
    fn split_drops_trailing_empty_segments() {
        assert_eq!(
            split("12345 1700000000000 ").unwrap(),
            vec!["12345".to_string(), "1700000000000".to_string()]
        );
        assert_eq!(
            split("1  2").unwrap(),
            vec!["1".to_string(), "".to_string(), "2".to_string()]
        );
        assert_eq!(split("").unwrap(), Vec::<String>::new());
        assert_eq!(split("0").unwrap(), vec!["0".to_string()]);
    }

    #[test]
    fn parse_start_offset_info_matches_reference() {
        let out = parse_start_offset_info(Some("0 3 0;0 2 1"))
            .unwrap()
            .unwrap();
        // key order preserved as in the wire payload: {"0@3": 0, "0@2": 1}
        assert_eq!(
            out,
            vec![("0@3".to_string(), 0i64), ("0@2".to_string(), 1i64)]
        );
        assert_eq!(
            parse_start_offset_info(Some("1 5 12")).unwrap().unwrap(),
            vec![("1@5".to_string(), 12i64)]
        );
        // retryFlag 2 alone (V2 retry) still assembles the lookup key
        assert_eq!(
            get_start_offset_info_map_key(&build_pop_retry_topic_v2(TOPIC, GROUP), 3),
            "2@3"
        );
        assert_eq!(get_start_offset_info_map_key(TOPIC, 3), "0@3");
        assert!(parse_start_offset_info(None).unwrap().is_none());
        assert!(parse_start_offset_info(Some("")).unwrap().is_none());
        // wrong segment count / duplicate key -> Decode
        assert!(matches!(
            parse_start_offset_info(Some("0 3")),
            Err(Error::Decode(_))
        ));
        let err = parse_start_offset_info(Some("0 3 0;0 3 1"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("parse startOffsetInfo error, duplicate"),
            "{err}"
        );
        assert!(matches!(
            parse_start_offset_info(Some("0 3 x")),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn parse_msg_offset_info_splits_comma_list() {
        let out = parse_msg_offset_info(Some("0 3 0,1,2;1 2 5"))
            .unwrap()
            .unwrap();
        // order preserved exactly as in the wire payload: {"0@3": [0,1,2], "1@2": [5]}
        assert_eq!(
            out,
            vec![
                ("0@3".to_string(), vec![0i64, 1, 2]),
                ("1@2".to_string(), vec![5i64])
            ]
        );
        assert_eq!(
            parse_msg_offset_info(Some("0 3 9")).unwrap().unwrap(),
            vec![("0@3".to_string(), vec![9i64])]
        );
        assert!(parse_msg_offset_info(None).unwrap().is_none());
        assert!(parse_msg_offset_info(Some("")).unwrap().is_none());
        assert_eq!(
            parse_msg_offset_info(Some("0 3 1,2,")).unwrap().unwrap(),
            vec![("0@3".to_string(), vec![1i64, 2])]
        );
        assert!(matches!(
            parse_msg_offset_info(Some("0 3 1,boom")),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            parse_msg_offset_info(Some("0 3 1 x")),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn parse_order_count_info_matches_reference() {
        let out = parse_order_count_info(Some("0 3 1;0 2 2"))
            .unwrap()
            .unwrap();
        assert_eq!(
            out,
            vec![("0@3".to_string(), 1i32), ("0@2".to_string(), 2i32)]
        );
        assert!(parse_order_count_info(Some("")).unwrap().is_none());
        assert!(matches!(
            parse_order_count_info(Some("0 3 1;0 3 2")),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn queue_offset_keys_match_reference() {
        assert_eq!(get_queue_offset_key_value_key(3, 67890), "qo3%67890");
        assert_eq!(get_queue_offset_map_key(TOPIC, 3, 67890), "0@qo3%67890");
        assert_eq!(
            get_queue_offset_map_key(&build_pop_retry_topic_v1(TOPIC, GROUP), 1, 2),
            "1@qo1%2"
        );
        assert_eq!(QUEUE_OFFSET, "qo");
    }

    #[test]
    fn offset_info_builders_append_into_one_string() {
        let mut start = String::new();
        build_start_offset_info(&mut start, TOPIC, 3, 0);
        build_start_offset_info(&mut start, &build_pop_retry_topic_v1(TOPIC, GROUP), 2, 1);
        assert_eq!(start, "0 3 0;1 2 1");
        assert_eq!(
            parse_start_offset_info(Some(&start)).unwrap().unwrap(),
            vec![("0@3".to_string(), 0i64), ("1@2".to_string(), 1i64)]
        );

        let mut msg = String::new();
        build_msg_offset_info(&mut msg, TOPIC, 3, &[0, 1, 2]);
        build_msg_offset_info(&mut msg, &build_pop_retry_topic_v2(TOPIC, GROUP), 2, &[5]);
        assert_eq!(msg, "0 3 0,1,2;2 2 5");
        assert_eq!(
            parse_msg_offset_info(Some(&msg)).unwrap().unwrap(),
            vec![
                ("0@3".to_string(), vec![0i64, 1, 2]),
                ("2@2".to_string(), vec![5i64])
            ]
        );

        let mut order = String::new();
        build_queue_id_order_count_info(&mut order, TOPIC, 3, 1);
        build_queue_id_order_count_info(&mut order, TOPIC, 2, 2);
        assert_eq!(order, "0 3 1;0 2 2");
        assert_eq!(
            parse_order_count_info(Some(&order)).unwrap().unwrap(),
            vec![("0@3".to_string(), 1i32), ("0@2".to_string(), 2i32)]
        );

        let mut by_offset = String::new();
        build_queue_offset_order_count_info(&mut by_offset, TOPIC, 3, 67890, 4);
        assert_eq!(by_offset, "0 qo3%67890 4");
        // matches the parse_* keys exactly; this is exactly the path POP ordered consumption uses to find the count back
        assert_eq!(get_queue_offset_map_key(TOPIC, 3, 67890), "0@qo3%67890");
    }

    #[test]
    fn round_trip_recovers_the_full_ack_ticket() {
        // reconstruct the CK string from the POP response headers, then read every segment back (the inputs of ACK / change-invisible-time)
        let start = parse_start_offset_info(Some("0 3 100")).unwrap().unwrap();
        let offsets = parse_msg_offset_info(Some("0 3 100,101")).unwrap().unwrap();
        let (queue_key, start_offset) = &start[0];
        assert_eq!(queue_key, &get_start_offset_info_map_key(TOPIC, 3));
        let (_, msg_offsets) = &offsets[0];

        let ck = build_extra_info(
            msg_offsets[0],
            1700000000000,
            60000,
            0,
            TOPIC,
            "broker-a",
            3,
            Some(*start_offset),
        );
        assert_eq!(ck, "100 1700000000000 60000 0 0 broker-a 3 100");
        let segs = segments(&ck);
        assert_eq!(get_ck_queue_offset(&segs).unwrap(), 100);
        assert_eq!(get_queue_offset(&segs).unwrap(), 100);
        assert_eq!(get_broker_name(&segs).unwrap(), "broker-a");
        assert_eq!(get_queue_id(&segs).unwrap(), 3);
        assert!(!is_order(&segs).unwrap());
    }

    #[test]
    fn retry_flag_shapes_and_real_topic() {
        assert_eq!(retry_of_topic(TOPIC), NORMAL_TOPIC);
        assert_eq!(
            retry_of_topic(&build_pop_retry_topic_v1(TOPIC, GROUP)),
            RETRY_TOPIC
        );
        assert_eq!(
            retry_of_topic(&build_pop_retry_topic_v2(TOPIC, GROUP)),
            RETRY_TOPIC_V2
        );
        // an ordinary topic name containing `+` does not count as V2 (the %RETRY% prefix must be present too)
        assert_eq!(retry_of_topic("Topic+With+Plus"), NORMAL_TOPIC);
        assert!(!is_pop_retry_topic_v2(None));
        assert!(!is_pop_retry_topic_v2(Some("")));
        assert!(!is_pop_retry_topic_v2(Some(TOPIC)));
        assert!(is_pop_retry_topic_v2(Some(&build_pop_retry_topic_v2(
            TOPIC, GROUP
        ))));

        assert_eq!(get_real_topic(TOPIC, GROUP, NORMAL_TOPIC).unwrap(), TOPIC);
        assert_eq!(
            get_real_topic(TOPIC, GROUP, RETRY_TOPIC).unwrap(),
            "%RETRY%GID_BodyGolden_BodyGoldenTopic"
        );
        assert_eq!(
            get_real_topic(TOPIC, GROUP, RETRY_TOPIC_V2).unwrap(),
            "%RETRY%GID_BodyGolden+BodyGoldenTopic"
        );
        let err = get_real_topic(TOPIC, GROUP, "3").unwrap_err().to_string();
        assert!(err.contains("getRetry fail, format is wrong"), "{err}");
    }

    #[test]
    fn separators_match_protocol_constants() {
        assert_eq!(KEY_SEPARATOR, " ");
        assert_eq!(QUEUE_SEPARATOR, ";");
        assert_eq!(OFFSET_SEPARATOR, ",");
        assert_eq!(POP_ORDER_REVIVE_QUEUE, 999);
        assert_eq!(MixAll::RETRY_GROUP_TOPIC_PREFIX, "%RETRY%");
    }
}
