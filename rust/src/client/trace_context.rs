//! W3C Trace Context（traceparent）透传（OpenTracing/OTel 场景的消息级上下文），
//! 逐条对齐 `python/rocketmq/client/trace_context.py`。
//!
//! 格式（W3C Trace Context，<https://www.w3.org/TR/trace-context/>）：
//!
//! ```text
//! traceparent: 00-<trace-id 32hex>-<parent-id 16hex>-<flags 2hex>
//! ```
//!
//! * trace-id / parent-id 不能全 0；version `00` 时 flags 为任意两位 hex；
//! * 生产侧：消息没有 `traceparent` 属性时注入一个根 span 上下文（opt-in，
//!   `enable_trace_context = true` 或环境变量 `ROCKETMQ_TRACE_CONTEXT_ENABLE`）——
//!   Java 客户端把这类注入交给外部链路追踪的 `SendMessageHook`（SkyWalking/OTel），
//!   本实现内建等价能力，键名沿用 W3C 的小写 `traceparent`；已有值**不覆盖**
//!   （调用方传播的上下文优先）；
//! * 消费侧：从 `MessageExt.properties` 里取出，供业务做父子 span 关联。
//!
//! 与本仓库 Python / dotnet 端一致的**有意差异**：Python 用 `secrets.token_hex`
//! （CSPRNG），本 crate 不引第三方随机数依赖，改用「纳秒时间 + PID + 进程内自增
//! 经 splitmix64 打散」——与 [`crate::common::mix_all::MixAll::create_uniq_name`]
//! 同一口径。对 traceparent 而言要求的只是「唯一且不可预测性足够低的 32/16 hex」，
//! 线上语义（长度、字符集、非全 0）与 Python 完全一致。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::message::{Message, MessageExt};
use crate::common::mix_all::MixAll;

/// W3C 键名（小写，与 OTel / HTTP 头一致）。
pub const TRACE_CONTEXT_PROPERTY: &str = "traceparent";
/// W3C `tracestate` 键名（透传第三方 vendor 状态用，本模块只定义键名）。
pub const TRACE_STATE_PROPERTY: &str = "tracestate";
/// 开关环境变量（对齐本仓库其它 `ROCKETMQ_*` 开关惯例）。
pub const TRACE_CONTEXT_ENABLE_ENV: &str = "ROCKETMQ_TRACE_CONTEXT_ENABLE";

const HEX_ARRAY: &[u8; 16] = b"0123456789abcdef";
/// trace-id 长度（hex 字符数）。
const TRACE_ID_HEX_LEN: usize = 32;
/// parent-id / span-id 长度（hex 字符数）。
const PARENT_ID_HEX_LEN: usize = 16;
/// version 00 下固定的采样标记（`01` = sampled，与 Python 生成端一致）。
const SAMPLED_FLAGS: &str = "01";

/// 生成合法的根 traceparent：`00-<32hex>-<16hex>-01`（记录采样）。
pub fn generate_traceparent() -> String {
    format!(
        "00-{}-{}-{SAMPLED_FLAGS}",
        random_id_hex(TRACE_ID_HEX_LEN),
        random_id_hex(PARENT_ID_HEX_LEN)
    )
}

/// 按 W3C 语法与「不全 0」规则校验。宽松接受大写 hex（转发时不重写）。
pub fn is_valid_traceparent(value: Option<&str>) -> bool {
    let Some(raw) = value else {
        return false;
    };
    let parts: Vec<&str> = raw.trim().split('-').collect();
    if parts.len() != 4 {
        return false;
    }
    let (version, trace_id, parent_id, flags) = (parts[0], parts[1], parts[2], parts[3]);
    if version != "00" && !(version.len() == 2 && is_hex(version)) {
        return false;
    }
    // ⚠ 大小写敏感：Python 参考实现只拦字面量 "ff"，"FF" 会放过。
    // 保留该行为（三语言端口一致优先于「顺手修正」），改它会与 Python/dotnet 对不上。
    if version == "ff" {
        return false;
    }
    if trace_id.len() != TRACE_ID_HEX_LEN
        || parent_id.len() != PARENT_ID_HEX_LEN
        || flags.len() != 2
    {
        return false;
    }
    for (part, disallow_zero) in [(trace_id, true), (parent_id, true), (flags, false)] {
        if !is_hex(part) {
            return false;
        }
        if disallow_zero && part.bytes().all(|b| b == b'0') {
            return false;
        }
    }
    true
}

/// 同一 trace-id 下生成子 span（换 parent-id）；`parent` 非法返回 `None`。
pub fn child_traceparent(parent: Option<&str>) -> Option<String> {
    if !is_valid_traceparent(parent) {
        return None;
    }
    let parts: Vec<&str> = parent?.trim().split('-').collect();
    Some(format!(
        "00-{}-{}-{SAMPLED_FLAGS}",
        parts[1].to_ascii_lowercase(),
        random_id_hex(PARENT_ID_HEX_LEN)
    ))
}

/// 消息没有 `traceparent` 属性时注入根上下文，返回（注入后的）值。
///
/// 已有值**不覆盖**——上游传播进来的上下文优先（对齐链路追踪的通用约定）。
pub fn inject_trace_context(message: &mut Message) -> String {
    if let Some(existing) = message.get_property(TRACE_CONTEXT_PROPERTY) {
        if !existing.is_empty() {
            return existing.to_string();
        }
    }
    let traceparent = generate_traceparent();
    message.put_property(TRACE_CONTEXT_PROPERTY, &traceparent);
    traceparent
}

/// 从消息属性里取出 traceparent（未注入 / 为空返回 `None`）。
pub fn extract_traceparent(msg: &MessageExt) -> Option<String> {
    msg.get_property(TRACE_CONTEXT_PROPERTY)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
}

/// 环境变量 `ROCKETMQ_TRACE_CONTEXT_ENABLE` 是否打开了 traceparent 注入。
pub fn trace_context_enabled_from_env() -> bool {
    env_value_means_on(&std::env::var(TRACE_CONTEXT_ENABLE_ENV).unwrap_or_default())
}

/// 开关取值判定：去空白 + 转小写后落在 `1` / `true` / `yes` 才算开（其它一律关）。
fn env_value_means_on(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes"
    )
}

/// 十六进制字符集校验（大小写均可，对应 Python 的 `part.lower()` 后查表）。
fn is_hex(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// 生成 `len` 位小写 hex，且**保证不全为 0**（trace-id / span-id 的硬约束）。
fn random_id_hex(len: usize) -> String {
    let mut hex = random_hex(len);
    if hex.bytes().all(|b| b == b'0') {
        hex.replace_range(len - 1.., "1");
    }
    hex
}

fn random_hex(len: usize) -> String {
    let mut out = String::with_capacity(len);
    while out.len() < len {
        let word = random_u64();
        for shift in (0..64).step_by(4).rev() {
            if out.len() == len {
                break;
            }
            out.push(HEX_ARRAY[((word >> shift) & 0x0F) as usize] as char);
        }
    }
    out
}

/// 无第三方依赖的伪随机源：纳秒时间 ^ PID ^ 进程内自增，再过 splitmix64 终化，
/// 使相邻两次调用的高低位在统计上也相互独立。
fn random_u64() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut z =
        nanos ^ ((MixAll::cached_pid() as u64) << 32) ^ seq.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_traceparent_shape() {
        let tp = generate_traceparent();
        assert!(tp.starts_with("00-"), "{tp}");
        assert!(tp.ends_with("-01"), "{tp}");
        let parts: Vec<&str> = tp.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[1].len(), 32);
        assert_eq!(parts[2].len(), 16);
        assert_eq!(parts[3], "01");
        assert!(is_valid_traceparent(Some(&tp)));
    }

    #[test]
    fn two_generated_ids_differ() {
        assert_ne!(generate_traceparent(), generate_traceparent());
    }

    #[test]
    fn validate_traceparent_accepts_and_rejects() {
        let good = generate_traceparent();
        assert!(is_valid_traceparent(Some(&good)));
        // 宽松：大写也认（转发时不重写）
        assert!(is_valid_traceparent(Some(&good.to_ascii_uppercase())));
        assert!(is_valid_traceparent(Some(
            "00-1234567890abcdef1234567890abcdef-1234567890abcdef-00"
        )));
        assert!(!is_valid_traceparent(None));
        assert!(!is_valid_traceparent(Some("")));
        assert!(!is_valid_traceparent(Some("00-abc-def-01")));
        assert!(!is_valid_traceparent(Some("01")));
        assert!(!is_valid_traceparent(Some(
            "00-1234567890abcdef1234567890abcdef12-1234567890abcdef-01"
        )));
        // trace-id 全 0 / parent-id 全 0 都非法
        assert!(!is_valid_traceparent(Some(&format!(
            "00-{}-{}-01",
            "0".repeat(32),
            "1".repeat(16)
        ))));
        assert!(!is_valid_traceparent(Some(&format!(
            "00-{}-{}-01",
            "1".repeat(32),
            "0".repeat(16)
        ))));
        // version：ff 非法、非 hex 非法、其它两位 hex 放行
        assert!(!is_valid_traceparent(Some(&format!(
            "ff-{}-{}-01",
            "1".repeat(32),
            "1".repeat(16)
        ))));
        assert!(!is_valid_traceparent(Some(&format!(
            "zz-{}-{}-01",
            "1".repeat(32),
            "1".repeat(16)
        ))));
        assert!(is_valid_traceparent(Some(&format!(
            "01-{}-{}-01",
            "1".repeat(32),
            "1".repeat(16)
        ))));
        // flags 可以全 0（不采样），但必须是两位 hex
        assert!(is_valid_traceparent(Some(&format!(
            "00-{}-{}-00",
            "1".repeat(32),
            "1".repeat(16)
        ))));
        assert!(!is_valid_traceparent(Some(&format!(
            "00-{}-{}-0g",
            "1".repeat(32),
            "1".repeat(16)
        ))));
    }

    #[test]
    fn child_traceparent_keeps_trace_id() {
        let parent = generate_traceparent();
        let child = child_traceparent(Some(&parent)).expect("valid parent");
        assert!(is_valid_traceparent(Some(&child)));
        assert_eq!(child.split('-').nth(1), parent.split('-').nth(1));
        assert_ne!(child, parent);
        assert_eq!(child_traceparent(Some("garbage")), None);
        assert_eq!(child_traceparent(None), None);
    }

    #[test]
    fn child_traceparent_lowercases_uppercase_parent() {
        let upper = format!("00-{}-{}-01", "A".repeat(32), "B".repeat(16));
        let child = child_traceparent(Some(&upper)).expect("valid");
        let parts: Vec<&str> = child.split('-').collect();
        assert_eq!(parts[1], "a".repeat(32));
        // span-id 是新造的，不会照抄 parent 的
        assert_ne!(parts[2], "b".repeat(16));
        assert!(is_valid_traceparent(Some(&child)));
    }

    #[test]
    fn inject_does_not_overwrite_and_extract_round_trip() {
        let mut msg = Message::new("TopicTest", Some(b"body"));
        let tp1 = inject_trace_context(&mut msg);
        assert!(is_valid_traceparent(Some(&tp1)));
        assert_eq!(msg.get_property(TRACE_CONTEXT_PROPERTY), Some(tp1.as_str()));
        // 已有值不覆盖
        let tp2 = inject_trace_context(&mut msg);
        assert_eq!(tp1, tp2);

        let mut ext = MessageExt::new();
        ext.put_property(TRACE_CONTEXT_PROPERTY, &tp1);
        assert_eq!(extract_traceparent(&ext).as_deref(), Some(tp1.as_str()));
        assert_eq!(extract_traceparent(&MessageExt::new()), None);
    }

    #[test]
    fn extract_treats_empty_value_as_absent() {
        let mut ext = MessageExt::new();
        ext.put_property(TRACE_CONTEXT_PROPERTY, "");
        assert_eq!(extract_traceparent(&ext), None);
    }

    #[test]
    fn property_names_match_w3c() {
        assert_eq!(TRACE_CONTEXT_PROPERTY, "traceparent");
        assert_eq!(TRACE_STATE_PROPERTY, "tracestate");
        assert_eq!(TRACE_CONTEXT_ENABLE_ENV, "ROCKETMQ_TRACE_CONTEXT_ENABLE");
    }

    #[test]
    fn env_switch_only_accepts_truthy_words() {
        for truthy in ["1", "true", "TRUE", "Yes", " yes ", "\tyes\n"] {
            assert!(env_value_means_on(truthy), "{truthy:?}");
        }
        for falsy in ["", "  ", "0", "false", "off", "no", "y", "true1"] {
            assert!(!env_value_means_on(falsy), "{falsy:?}");
        }
        // 未设置该环境变量时默认关闭；设置了则以上面的判定为准
        assert_eq!(
            trace_context_enabled_from_env(),
            env_value_means_on(&std::env::var(TRACE_CONTEXT_ENABLE_ENV).unwrap_or_default())
        );
    }

    #[test]
    fn random_hex_respects_length_and_charset() {
        for len in [1usize, 2, 15, 16, 32, 33, 64] {
            let hex = random_id_hex(len);
            assert_eq!(hex.len(), len);
            assert!(hex.bytes().all(|b| b.is_ascii_hexdigit()), "{hex}");
            assert!(!hex.bytes().all(|b| b == b'0'), "{hex}");
        }
    }
}
