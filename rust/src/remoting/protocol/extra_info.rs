//! POP 模式的 extraInfo（俗称 CK 串）编解码。
//!
//! 逐条移植 `python/rocketmq/remoting/protocol/extra_info.py`（257 行），即 Java
//! `org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil`。
//!
//! CK 串是 POP 模式的核心凭据：broker 在 POP 响应里**不**给普通 topic 的消息写
//! `POP_CK` 属性（只在 retry topic 重编码路径写），客户端必须自己用响应头的
//! `startOffsetInfo` / `msgOffsetInfo` 反构出这个串，再作为 ACK /
//! CHANGE_MESSAGE_INVISIBLETIME 的 `extraInfo` 回传。格式是 7~8 段、**空格**分隔：
//!
//! ```text
//! ckQueueOffset popTime invisibleTime reviveQid retryFlag brokerName queueId [msgQueueOffset]
//! ```
//!
//! 分隔符是空格而不是逗号 —— 用错会让 broker **静默**解析失败。
//! `retryFlag` 不是 topic 原文，而是 [`retry_of_topic`] 算出的 `"0"/"1"/"2"`。

use crate::common::mix_all::MixAll;
use crate::error::{Error, Result};

/// 段内分隔符；对应 Java `MessageConst.KEY_SEPARATOR`（也是
/// `common::message_const::KEY_SEPARATOR`），必须是单个空格。
pub const KEY_SEPARATOR: &str = " ";
/// 队列之间的分隔符（`startOffsetInfo` / `msgOffsetInfo` / `orderCountInfo`）。
pub const QUEUE_SEPARATOR: &str = ";";
/// `msgOffsetInfo` 里同一队列多条 offset 的分隔符。
pub const OFFSET_SEPARATOR: &str = ",";

/// `retryFlag`：普通 topic。
pub const NORMAL_TOPIC: &str = "0";
/// `retryFlag`：V1 retry topic（`%RETRY%<cid>_<topic>`）。
pub const RETRY_TOPIC: &str = "1";
/// `retryFlag`：V2 retry topic（`%RETRY%<cid>+<topic>`）。
pub const RETRY_TOPIC_V2: &str = "2";
/// `queueOffset` 键的前缀（Java `ExtraInfoUtil.QUEUE_OFFSET`）。
pub const QUEUE_OFFSET: &str = "qo";

/// 顺序消费用的固定 revive 队列号，对应 Java `KeyBuilder.POP_ORDER_REVIVE_QUEUE`。
pub const POP_ORDER_REVIVE_QUEUE: i32 = 999;

/// V1 retry topic 的 topic/cid 连接符（Java `KeyBuilder.POP_RETRY_SEPARATOR`）。
const POP_RETRY_SEPARATOR_V1: &str = "_";
/// V2 retry topic 的 topic/cid 连接符。
const POP_RETRY_SEPARATOR_V2: &str = "+";

// ---------------------------------------------------------------- retry topic 命名

/// `%RETRY%<cid>_<topic>`（Java `KeyBuilder.buildPopRetryTopicV1`）。
pub fn build_pop_retry_topic_v1(topic: &str, cid: &str) -> String {
    format!(
        "{}{}{}{}",
        MixAll::RETRY_GROUP_TOPIC_PREFIX,
        cid,
        POP_RETRY_SEPARATOR_V1,
        topic
    )
}

/// `%RETRY%<cid>+<topic>`（Java `KeyBuilder.buildPopRetryTopicV2`）。
pub fn build_pop_retry_topic_v2(topic: &str, cid: &str) -> String {
    format!(
        "{}{}{}{}",
        MixAll::RETRY_GROUP_TOPIC_PREFIX,
        cid,
        POP_RETRY_SEPARATOR_V2,
        topic
    )
}

/// Java `buildPopRetryTopic`：`enableRetryTopicV2` 关（默认）走 V1。
pub fn build_pop_retry_topic(topic: &str, cid: &str, enable_retry_v2: bool) -> String {
    if enable_retry_v2 {
        build_pop_retry_topic_v2(topic, cid)
    } else {
        build_pop_retry_topic_v1(topic, cid)
    }
}

/// Java `KeyBuilder.isPopRetryTopicV2`：`%RETRY%` 前缀**且**含 `+`。
///
/// 只看含 `+` 不够：普通 topic 名里也可能有 `+`，那样会被误判成 retry V2。
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

/// 由 topic 形状判定 `retryFlag`（Java `ExtraInfoUtil.getRetry(topic)`）。
///
/// 顺序很重要：先判 V2（含 `+`），再判 `%RETRY%` 前缀（V1），否则普通 topic
/// 一律是 `"0"`。
pub fn retry_of_topic(topic: &str) -> &'static str {
    if is_pop_retry_topic_v2(Some(topic)) {
        RETRY_TOPIC_V2
    } else if topic.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX) {
        RETRY_TOPIC
    } else {
        NORMAL_TOPIC
    }
}

/// 由 `retryFlag` 还原真实 topic（Java `ExtraInfoUtil.getRealTopic`）。
///
/// 未知 flag 报错而不是回退到 `topic`：回退会把 retry 消息当普通消息确认掉。
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

// ---------------------------------------------------------------- CK 串切分与取值

/// 模拟 Java `String.split`：**丢弃末尾空串**（Python `str.split(sep)` 会保留）。
///
/// 这个差异是真实的：Java `"a b ".split(" ")` 得 `["a","b"]`，
/// Python 得 `["a","b",""]`；段数校验依赖它（`extra_info.py:64-73`）。
fn split_drop_trailing(value: &str, sep: &str) -> Vec<String> {
    let mut parts: Vec<String> = value.split(sep).map(str::to_string).collect();
    while parts.last().map(|p| p.is_empty()).unwrap_or(false) {
        parts.pop();
    }
    parts
}

/// 按空格切分 CK 串（Java `ExtraInfoUtil.split`）。
pub fn split(extra_info: &str) -> Result<Vec<String>> {
    Ok(split_drop_trailing(extra_info, KEY_SEPARATOR))
}

/// 段数校验（Java 的 `IllegalArgumentException` 文案逐字照抄）。
fn require(segments: &[String], need: usize, what: &str) -> Result<()> {
    if segments.len() < need {
        return Err(Error::Decode(format!(
            "{what} fail, extraInfoStrs length {}",
            segments.len()
        )));
    }
    Ok(())
}

/// 取某一段并按 long 解析（`Long.valueOf` / `Integer.parseInt` 的失败口径）。
fn segment_as(segments: &[String], index: usize, need: usize, what: &str) -> Result<i64> {
    require(segments, need, what)?;
    let raw = &segments[index];
    raw.trim().parse::<i64>().map_err(|_| {
        Error::Decode(format!(
            "{what} fail, extraInfoStrs[{index}]={raw:?} is not a number"
        ))
    })
}

/// `segments[0]`：本条消息在队列里的 ckQueueOffset。
pub fn get_ck_queue_offset(segments: &[String]) -> Result<i64> {
    segment_as(segments, 0, 1, "getCkQueueOffset")
}

/// `segments[1]`：POP 时刻（毫秒）。
pub fn get_pop_time(segments: &[String]) -> Result<i64> {
    segment_as(segments, 1, 2, "getPopTime")
}

/// `segments[2]`：不可见时间（毫秒）。
pub fn get_invisible_time(segments: &[String]) -> Result<i64> {
    segment_as(segments, 2, 3, "getInvisibleTime")
}

/// `segments[3]`：revive 队列号；[`POP_ORDER_REVIVE_QUEUE`] 表示顺序消费。
pub fn get_revive_qid(segments: &[String]) -> Result<i32> {
    Ok(segment_as(segments, 3, 4, "getReviveQid")? as i32)
}

/// `segments[4]`：`retryFlag` 原文（`"0"/"1"/"2"`）。
pub fn get_retry(segments: &[String]) -> Result<String> {
    require(segments, 5, "getRetry")?;
    Ok(segments[4].clone())
}

/// `segments[5]`：broker 名。
pub fn get_broker_name(segments: &[String]) -> Result<String> {
    require(segments, 6, "getBrokerName")?;
    Ok(segments[5].clone())
}

/// `segments[6]`：队列号。
pub fn get_queue_id(segments: &[String]) -> Result<i32> {
    Ok(segment_as(segments, 6, 7, "getQueueId")? as i32)
}

/// `segments[7]`：消息在队列里的 offset（只有 8 段串存在）。
pub fn get_queue_offset(segments: &[String]) -> Result<i64> {
    segment_as(segments, 7, 8, "getQueueOffset")
}

/// reviveQid 是 999 表示顺序消费（Java `ExtraInfoUtil.isOrder`）。
pub fn is_order(segments: &[String]) -> Result<bool> {
    Ok(get_revive_qid(segments)? == POP_ORDER_REVIVE_QUEUE)
}

// ---------------------------------------------------------------- CK 串拼装

/// 拼 CK 串。传 `msg_queue_offset` 得 8 段，不传得 7 段
/// （Java 的两个 `buildExtraInfo` 重载；ACK 场景用 8 段版本）。
///
/// 第 5 段写的是 [`retry_of_topic`] 的结果，**不是** topic 原文。
// 参数表与 Java `buildExtraInfo(ckQueueOffset, popTime, invisibleTime, reviveQid,
// topic, brokerName, queueId, msgQueueOffset)` 一一对应，合并参数会破坏逐段对照。
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

/// 往 `startOffsetInfo` 里追加一个队列（Java `buildStartOffsetInfo`）。
///
/// `out` 非空时先补 `;`，所以调用方按队列顺序连续 append 即可。
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

/// 往 `orderCountInfo` 里追加「队列 → 计数」（Java `buildQueueIdOrderCountInfo`）。
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

/// 往 `orderCountInfo` 里追加「queueOffset → 计数」（Java
/// `buildQueueOffsetOrderCountInfo`，键换成 [`get_queue_offset_key_value_key`]）。
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

/// 往 `msgOffsetInfo` 里追加一个队列的若干 offset（Java `buildMsgOffsetInfo`）。
///
/// 第三段内部用 `,` 连接；空列表会留下一个只有两段的坏串，与 Java 一致。
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

/// 三段一组地追加：段内空格分隔、组间 `;` 分隔。
fn append_queue_segment(out: &mut String, segments: &[&str]) {
    if !out.is_empty() {
        out.push_str(QUEUE_SEPARATOR);
    }
    out.push_str(&segments.join(KEY_SEPARATOR));
}

// ---------------------------------------------------------------- 响应头信息表解析

/// 响应头 map 的通用形状：`"<retryFlag> <queueKey> <value>"`，多条用 `;` 连接。
///
/// 组内用**不丢尾空串**的 split（`extra_info.py:177`：Python 直接 `one.split(" ")`），
/// 所以 `"0 3 0 "` 这种带尾空格的串会被判成 4 段而报错 —— Java 反而会容忍，
/// 这里以参考实现为准。
fn split_queue_segments(value: &str) -> Vec<String> {
    if value.contains(QUEUE_SEPARATOR) {
        split_drop_trailing(value, QUEUE_SEPARATOR)
    } else {
        vec![value.to_string()]
    }
}

/// 解析单组并返回 `(key, third)`；key 是 `"<retryFlag>@<queueKey>"`。
///
/// 泛型 `T`：三个 `parse*Info` 的 value 类型不同（long / long 列表 / int 计数），
/// 这里只用 `out` 做重复键检查，所以不关心 `T` 具体是什么。
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

/// 解析 `startOffsetInfo`，形如 `"0 3 0;0 2 1"`。
///
/// key 是 `"<retryFlag>@<queueId>"`，value 是该队列本次弹出的起始 offset。
/// 空 / 缺失回 `None`（Java 直接 `return null`，调用方按 null 判）。
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

/// `msgOffsetInfo` 解析结果：按插入顺序的 `("<retryFlag>@<queueKey>", offsets)`。
pub type MsgOffsetTable = Vec<(String, Vec<i64>)>;

/// 解析 `msgOffsetInfo`，形如 `"0 3 0,1,2;1 2 5"`。
///
/// key 同 [`parse_start_offset_info`]，value 是该队列本次弹出的各条消息 offset 列表。
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

/// 解析 `orderCountInfo`（顺序消费用），第三段是计数（int）。
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

// ---------------------------------------------------------------- 查表键

/// `startOffsetInfo` 表的查询键（Java `getStartOffsetInfoMapKey`）。
pub fn get_start_offset_info_map_key(topic: &str, key: i64) -> String {
    format!("{}@{}", retry_of_topic(topic), key)
}

/// `queueOffset` 形态的键值：`"qo<queueId>%<queueOffset>"`（Java `getQueueOffsetKeyValueKey`）。
pub fn get_queue_offset_key_value_key(queue_id: i64, queue_offset: i64) -> String {
    format!("{QUEUE_OFFSET}{queue_id}%{queue_offset}")
}

/// `orderCountInfo`（按 queueOffset 计数时）的查询键（Java `getQueueOffsetMapKey`）。
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

    /// 参考实现 `build_extra_info` 的真实输出（`extra_info.py:142-160`）。
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
        // 第 5 段是 retryFlag，不是 topic
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
        // 7 段与 8 段的段数差就是 msgQueueOffset 的有无
        assert_eq!(split(CK_NORMAL).unwrap().len(), 7);
        assert_eq!(split(CK_WITH_OFFSET).unwrap().len(), 8);
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

        // 只有 7 段时取第 8 段：Java 文案 "getQueueOffset fail, extraInfoStrs length 7"
        let short = segments(CK_NORMAL);
        let err = get_queue_offset(&short).unwrap_err().to_string();
        assert!(
            err.contains("getQueueOffset fail, extraInfoStrs length 7"),
            "{err}"
        );
        // 少于 5 段时连 retry 也取不到
        assert!(get_retry(&["1".to_string()])
            .unwrap_err()
            .to_string()
            .contains("getRetry fail, extraInfoStrs length 1"));
        // 非数字段不 panic，报 Decode
        let junk = segments("abc 2 3 4 0 broker-a 1 5");
        assert!(matches!(get_ck_queue_offset(&junk), Err(Error::Decode(_))));
    }

    #[test]
    fn split_drops_trailing_empty_segments_like_java() {
        // Java `"a b ".split(" ")` -> ["a","b"]；Python split 会多一个空串
        assert_eq!(
            split("12345 1700000000000 ").unwrap(),
            vec!["12345".to_string(), "1700000000000".to_string()]
        );
        // 中间的空段保留（会让段数校验失败，与 Java 一致）
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
        // 参考实现：{"0@3": 0, "0@2": 1}（插入顺序）
        assert_eq!(
            out,
            vec![("0@3".to_string(), 0i64), ("0@2".to_string(), 1i64)]
        );
        assert_eq!(
            parse_start_offset_info(Some("1 5 12")).unwrap().unwrap(),
            vec![("1@5".to_string(), 12i64)]
        );
        // 只有 retryFlag 2（V2 retry）也照样能拼出查询键
        assert_eq!(
            get_start_offset_info_map_key(&build_pop_retry_topic_v2(TOPIC, GROUP), 3),
            "2@3"
        );
        assert_eq!(get_start_offset_info_map_key(TOPIC, 3), "0@3");
        // 空 / None -> None（Java return null）
        assert!(parse_start_offset_info(None).unwrap().is_none());
        assert!(parse_start_offset_info(Some("")).unwrap().is_none());
        // 段数不对 / 重复键 -> Decode
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
        // 参考实现：{"0@3": [0,1,2], "1@2": [5]}
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
        // 尾随逗号按 Java 口径丢掉
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
    fn java_style_builders_append_into_one_string() {
        // 对应 Java 的 StringBuilder 版本：startOffsetInfo / msgOffsetInfo / orderCountInfo
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
        // 与 parse_* 的键完全对得上，这正是 POP 顺序消费找回计数的路径
        assert_eq!(get_queue_offset_map_key(TOPIC, 3, 67890), "0@qo3%67890");
    }

    #[test]
    fn round_trip_recovers_the_full_ack_ticket() {
        // 从 POP 响应头反构 CK 串，再把每一段读回来（ACK / 改不可见时间的入参）
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
        // 普通 topic 名里带 `+` 不算 V2（必须同时有 %RETRY% 前缀）
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
    fn separators_match_java_constants() {
        assert_eq!(KEY_SEPARATOR, " ");
        assert_eq!(QUEUE_SEPARATOR, ";");
        assert_eq!(OFFSET_SEPARATOR, ",");
        assert_eq!(POP_ORDER_REVIVE_QUEUE, 999);
        assert_eq!(MixAll::RETRY_GROUP_TOPIC_PREFIX, "%RETRY%");
    }
}
