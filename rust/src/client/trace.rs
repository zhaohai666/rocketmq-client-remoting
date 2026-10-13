//! 线上形态：编码结果作为消息 body 发到轨迹 topic —— 默认
//! [`MixAll::TRACE_TOPIC`]（`RMQ_SYS_TRACE_TOPIC`），`AccessChannel::Cloud` 下改用
//! [`TraceConstants::TRACE_TOPIC_PREFIX`] + regionId；轨迹消息的 KEYS 属性由各记录的
//! `trans_key` 拼成，控制台按它反查轨迹。攒批与发送由 `client::trace_dispatcher` 负责，
//! 各字段的填充由 `client::trace_hook` 负责。

use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::sync::OnceLock;


use crate::client::result::LocalTransactionState;
use crate::common::message_const::KEY_SEPARATOR;
use crate::common::message_type::MessageType;
use crate::common::mix_all::MixAll;
use crate::common::util_all;
use crate::error::{Error, Result};
use crate::rmq_warn;

pub struct TraceConstants;

impl TraceConstants {
    /// 内部轨迹生产者的组名前缀（分发器拼 `<前缀>-<group>-<PRODUCE|CONSUME>-<N>`）。
    pub const GROUP_NAME_PREFIX: &'static str = "_INNER_TRACE_PRODUCER";
    /// 一条记录内字段之间的分隔符（SOH，`\x01`）。
    pub const CONTENT_SPLITOR: char = '\u{1}';
    /// 记录之间的分隔符（STX，`\x02`）。
    pub const FIELD_SPLITOR: char = '\u{2}';
    /// 内部轨迹生产者的 instanceName 前缀。
    pub const TRACE_INSTANCE_NAME: &'static str = "PID_CLIENT_INNER_TRACE_PRODUCER";
    pub const TRACE_TOPIC_PREFIX: &'static str = "rmq_sys_TRACE_DATA_";
    pub const TO_PREFIX: &'static str = "To_";
    pub const FROM_PREFIX: &'static str = "From_";
    pub const END_TRANSACTION: &'static str = "EndTransaction";

    pub const ROCKETMQ_SERVICE: &'static str = "rocketmq";
    pub const ROCKETMQ_SUCCESS: &'static str = "rocketmq.success";
    pub const ROCKETMQ_TAGS: &'static str = "rocketmq.tags";
    pub const ROCKETMQ_KEYS: &'static str = "rocketmq.keys";
    pub const ROCKETMQ_STORE_HOST: &'static str = "rocketmq.store_host";
    pub const ROCKETMQ_BODY_LENGTH: &'static str = "rocketmq.body_length";
    pub const ROCKETMQ_MSG_ID: &'static str = "rocketmq.mgs_id";
    /// ⚠ 同上，`mgs_type` 为线上字面量。
    pub const ROCKETMQ_MSG_TYPE: &'static str = "rocketmq.mgs_type";
    pub const ROCKETMQ_REGION_ID: &'static str = "rocketmq.region_id";
    pub const ROCKETMQ_TRANSACTION_ID: &'static str = "rocketmq.transaction_id";
    pub const ROCKETMQ_TRANSACTION_STATE: &'static str = "rocketmq.transaction_state";
    pub const ROCKETMQ_IS_FROM_TRANSACTION_CHECK: &'static str =
        "rocketmq.is_from_transaction_check";
    /// ⚠ 同上，这个键名线上就是复数 `retry_times`。
    pub const ROCKETMQ_RETRY_TIMERS: &'static str = "rocketmq.retry_times";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TraceType {
    Pub,
    Recall,
    SubBefore,
    SubAfter,
    EndTransaction,
}

impl TraceType {
    pub fn name(self) -> &'static str {
        match self {
            TraceType::Pub => "Pub",
            TraceType::Recall => "Recall",
            TraceType::SubBefore => "SubBefore",
            TraceType::SubAfter => "SubAfter",
            TraceType::EndTransaction => "EndTransaction",
        }
    }

    pub fn from_name(name: &str) -> Option<TraceType> {
        Some(match name {
            "Pub" => TraceType::Pub,
            "Recall" => TraceType::Recall,
            "SubBefore" => TraceType::SubBefore,
            "SubAfter" => TraceType::SubAfter,
            "EndTransaction" => TraceType::EndTransaction,
            _ => return None,
        })
    }

    pub fn all() -> [TraceType; 5] {
        [
            TraceType::Pub,
            TraceType::Recall,
            TraceType::SubBefore,
            TraceType::SubAfter,
            TraceType::EndTransaction,
        ]
    }
}

impl fmt::Display for TraceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum AccessChannel {
    /// 连自建 IDC 集群。
    #[default]
    Local = 0,
    /// 连云上集群。
    Cloud = 1,
}

impl AccessChannel {
    pub fn name(self) -> &'static str {
        match self {
            AccessChannel::Local => "LOCAL",
            AccessChannel::Cloud => "CLOUD",
        }
    }

    pub fn is_cloud(self) -> bool {
        matches!(self, AccessChannel::Cloud)
    }
}

impl fmt::Display for AccessChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Rust 的 `split` 会保留末尾空串，而编码结果末尾正好补了
/// FIELD_SPLITOR，所以直接用 `str::split` 会多出一段空记录；对内容段
/// （CONTENT_SPLITOR）同理 —— SubBefore 无 keys 时字段会整体错位。
/// 这条差异是真金白银踩过的坑，勿改成 `str::split`。
pub fn java_split(value: &str, sep: char) -> Vec<&str> {
    let mut parts: Vec<&str> = value.split(sep).collect();
    while parts.last() == Some(&"") {
        parts.pop();
    }
    parts
}

pub fn local_address() -> &'static str {
    static LOCAL: OnceLock<String> = OnceLock::new();
    LOCAL.get_or_init(compute_local_address)
}

fn compute_local_address() -> String {
    let ip = MixAll::get_ip_str();
    if util_all::is_ipv4(&ip) {
        return ip;
    }
    match ip.parse::<IpAddr>() {
        Ok(IpAddr::V6(v6)) => {
            let segments = v6.segments();
            let mut out = String::with_capacity(39);
            for (i, seg) in segments.iter().enumerate() {
                if i > 0 {
                    out.push(':');
                }
                out.push_str(&format!("{seg:04x}"));
            }
            out
        }
        // 解析不了（例如宿主返回了主机名）就原样用，不报错
        _ => ip,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TraceBean {
    pub topic: String,
    pub msg_id: String,
    pub offset_msg_id: String,
    pub tags: String,
    pub keys: String,
    pub store_host: String,
    pub client_host: String,
    pub store_time: i64,
    pub retry_times: i32,
    pub body_length: i32,
    pub msg_type: MessageType,
    pub transaction_state: Option<String>,
    pub transaction_id: Option<String>,
    pub from_transaction_check: bool,
}

impl Default for TraceBean {
    fn default() -> Self {
        TraceBean {
            topic: String::new(),
            msg_id: String::new(),
            offset_msg_id: String::new(),
            tags: String::new(),
            keys: String::new(),
            store_host: local_address().to_string(),
            client_host: local_address().to_string(),
            store_time: 0,
            retry_times: 0,
            body_length: 0,
            msg_type: MessageType::default(),
            transaction_state: None,
            transaction_id: None,
            from_transaction_check: false,
        }
    }
}

impl TraceBean {
    pub fn new() -> Self {
        Self::default()
    }

    /// 发送钩子的 `EndTransactionContext#transactionState` 就是这个枚举。
    pub fn set_transaction_state(&mut self, state: LocalTransactionState) {
        self.transaction_state = Some(state.to_string());
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TraceContext {
    pub trace_type: Option<TraceType>,
    pub time_stamp: i64,
    pub region_id: String,
    pub region_name: String,
    pub group_name: String,
    pub cost_time: i32,
    pub is_success: bool,
    pub request_id: String,
    pub context_code: i32,
    pub access_channel: Option<AccessChannel>,
    pub trace_beans: Vec<TraceBean>,
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

impl TraceContext {
    pub fn new() -> Self {
        TraceContext {
            trace_type: None,
            time_stamp: util_all::current_time_millis(),
            region_id: String::new(),
            region_name: String::new(),
            group_name: String::new(),
            cost_time: 0,
            is_success: true,
            request_id: util_all::create_uniq_id(),
            context_code: 0,
            access_channel: None,
            trace_beans: Vec::new(),
        }
    }

    pub fn access_channel_or_local(&self) -> AccessChannel {
        self.access_channel.unwrap_or_default()
    }
}

impl fmt::Display for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_name = self.trace_type.map(TraceType::name).unwrap_or_default();
        write!(
            f,
            "TraceContext{{{type_name}_{}_{}_{}_",
            self.group_name, self.region_id, self.is_success
        )?;
        for bean in &self.trace_beans {
            write!(f, "{}_{}_", bean.msg_id, bean.topic)?;
        }
        f.write_str("}")
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TraceTransferBean {
    /// 攒好的一批记录（每条以 [`TraceConstants::FIELD_SPLITOR`] 结尾）。
    pub trans_data: String,
    /// 原始消息 msgId + 按 [`KEY_SEPARATOR`] 拆开的业务 keys。
    pub trans_key: BTreeSet<String>,
}

impl TraceTransferBean {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceDataEncoder;

impl TraceDataEncoder {
    pub fn decoder_from_trace_data_string(trace_data: Option<&str>) -> Vec<TraceContext> {
        let mut res: Vec<TraceContext> = Vec::new();
        let Some(trace_data) = trace_data.filter(|v| !v.is_empty()) else {
            return res;
        };
        for context in java_split(trace_data, TraceConstants::FIELD_SPLITOR) {
            if context.is_empty() {
                continue;
            }
            match Self::decode_context(context) {
                Ok(Some(ctx)) => res.push(ctx),
                Ok(None) => {}
                Err(e) => rmq_warn!("decode trace context failed: {e}, context={context:?}"),
            }
        }
        res
    }

    /// 解一条轨迹记录（一段 FIELD_SPLITOR 之内）。类型无法识别时返回 `None`。
    fn decode_context(context: &str) -> Result<Option<TraceContext>> {
        let line = java_split(context, TraceConstants::CONTENT_SPLITOR);
        if line.is_empty() {
            return Ok(None);
        }
        let Some(kind) = TraceType::from_name(line[0]) else {
            return Ok(None);
        };
        let ctx = match kind {
            TraceType::Pub => Self::decode_pub(&line)?,
            TraceType::SubBefore => Self::decode_sub_before(&line)?,
            TraceType::SubAfter => Self::decode_sub_after(&line)?,
            TraceType::EndTransaction => Self::decode_end_transaction(&line)?,
            TraceType::Recall => Self::decode_recall(&line)?,
        };
        Ok(Some(ctx))
    }

    /// Pub：`Pub ts region group topic msgId tags keys storeHost bodyLength costTime
    /// msgType [offsetMsgId] success [clientHost]`。
    fn decode_pub(line: &[&str]) -> Result<TraceContext> {
        let mut ctx = TraceContext::new();
        ctx.trace_type = Some(TraceType::Pub);
        ctx.time_stamp = parse_long(line, 1)?;
        ctx.region_id = field(line, 2)?.to_string();
        ctx.group_name = field(line, 3)?.to_string();
        let mut bean = TraceBean::new();
        bean.topic = field(line, 4)?.to_string();
        bean.msg_id = field(line, 5)?.to_string();
        bean.tags = field(line, 6)?.to_string();
        bean.keys = field(line, 7)?.to_string();
        bean.store_host = field(line, 8)?.to_string();
        bean.body_length = parse_int(line, 9)?;
        ctx.cost_time = parse_int(line, 10)?;
        bean.msg_type = parse_msg_type(line, 11)?;
        match line.len() {
            // 老版本：没有 offsetMsgId
            13 => ctx.is_success = parse_bool(line, 12)?,
            14 => {
                bean.offset_msg_id = field(line, 12)?.to_string();
                ctx.is_success = parse_bool(line, 13)?;
            }
            _ => {}
        }
        if line.len() >= 15 {
            bean.offset_msg_id = field(line, 12)?.to_string();
            ctx.is_success = parse_bool(line, 13)?;
            bean.client_host = field(line, 14)?.to_string();
        }
        ctx.trace_beans = vec![bean];
        Ok(ctx)
    }

    /// SubBefore：`SubBefore ts region group requestId msgId retryTimes [keys]`。
    fn decode_sub_before(line: &[&str]) -> Result<TraceContext> {
        let mut ctx = TraceContext::new();
        ctx.trace_type = Some(TraceType::SubBefore);
        ctx.time_stamp = parse_long(line, 1)?;
        ctx.region_id = field(line, 2)?.to_string();
        ctx.group_name = field(line, 3)?.to_string();
        ctx.request_id = field(line, 4)?.to_string();
        let mut bean = TraceBean::new();
        bean.msg_id = field(line, 5)?.to_string();
        bean.retry_times = parse_int(line, 6)?;
        bean.keys = opt_field(line, 7).unwrap_or("").to_string();
        ctx.trace_beans = vec![bean];
        Ok(ctx)
    }

    /// SubAfter：`SubAfter requestId msgId costTime success keys [contextCode [ts group]]`。
    fn decode_sub_after(line: &[&str]) -> Result<TraceContext> {
        let mut ctx = TraceContext::new();
        ctx.trace_type = Some(TraceType::SubAfter);
        ctx.request_id = field(line, 1)?.to_string();
        let mut bean = TraceBean::new();
        bean.msg_id = field(line, 2)?.to_string();
        bean.keys = field(line, 5)?.to_string();
        ctx.trace_beans = vec![bean];
        ctx.cost_time = parse_int(line, 3)?;
        ctx.is_success = parse_bool(line, 4)?;
        if line.len() >= 7 {
            ctx.context_code = parse_int(line, 6)?;
        }
        if line.len() >= 9 {
            // 兼容老版本：只有非 CLOUD 通道编码时才有这两段
            ctx.time_stamp = parse_long(line, 7)?;
            ctx.group_name = field(line, 8)?.to_string();
        }
        Ok(ctx)
    }

    /// EndTransaction：`EndTransaction ts region group topic msgId tags keys storeHost
    /// msgType transactionId transactionState fromTransactionCheck`。
    fn decode_end_transaction(line: &[&str]) -> Result<TraceContext> {
        let mut ctx = TraceContext::new();
        ctx.trace_type = Some(TraceType::EndTransaction);
        ctx.time_stamp = parse_long(line, 1)?;
        ctx.region_id = field(line, 2)?.to_string();
        ctx.group_name = field(line, 3)?.to_string();
        let mut bean = TraceBean::new();
        bean.topic = field(line, 4)?.to_string();
        bean.msg_id = field(line, 5)?.to_string();
        bean.tags = field(line, 6)?.to_string();
        bean.keys = field(line, 7)?.to_string();
        bean.store_host = field(line, 8)?.to_string();
        bean.msg_type = parse_msg_type(line, 9)?;
        bean.transaction_id = Some(field(line, 10)?.to_string());
        bean.transaction_state = Some(field(line, 11)?.to_string());
        bean.from_transaction_check = parse_bool(line, 12)?;
        ctx.trace_beans = vec![bean];
        Ok(ctx)
    }

    /// Recall：`Recall ts region group topic msgId success`。
    fn decode_recall(line: &[&str]) -> Result<TraceContext> {
        let mut ctx = TraceContext::new();
        ctx.trace_type = Some(TraceType::Recall);
        ctx.time_stamp = parse_long(line, 1)?;
        ctx.region_id = field(line, 2)?.to_string();
        ctx.group_name = field(line, 3)?.to_string();
        let mut bean = TraceBean::new();
        bean.topic = field(line, 4)?.to_string();
        bean.msg_id = field(line, 5)?.to_string();
        ctx.is_success = parse_bool(line, 6)?;
        ctx.trace_beans = vec![bean];
        Ok(ctx)
    }

    pub fn encoder_from_context_bean(ctx: Option<&TraceContext>) -> Option<TraceTransferBean> {
        let ctx = ctx?;
        let mut tb = TraceTransferBean::new();
        let mut sb: Vec<String> = Vec::with_capacity(16);
        match ctx.trace_type {
            Some(TraceType::Pub) => {
                if let Some(bean) = ctx.trace_beans.first() {
                    sb.extend([
                        TraceType::Pub.name().to_string(),
                        ctx.time_stamp.to_string(),
                        ctx.region_id.clone(),
                        ctx.group_name.clone(),
                        bean.topic.clone(),
                        bean.msg_id.clone(),
                        bean.tags.clone(),
                        bean.keys.clone(),
                        bean.store_host.clone(),
                        bean.body_length.to_string(),
                        ctx.cost_time.to_string(),
                        bean.msg_type.value().to_string(),
                        bean.offset_msg_id.clone(),
                        bool_str(ctx.is_success).to_string(),
                    ]);
                    tb.trans_data = join_record(&sb);
                }
            }
            Some(TraceType::SubBefore) => {
                for bean in &ctx.trace_beans {
                    sb.clear();
                    sb.extend([
                        TraceType::SubBefore.name().to_string(),
                        ctx.time_stamp.to_string(),
                        ctx.region_id.clone(),
                        ctx.group_name.clone(),
                        ctx.request_id.clone(),
                        bean.msg_id.clone(),
                        bean.retry_times.to_string(),
                        bean.keys.clone(),
                    ]);
                    tb.trans_data.push_str(&join_record(&sb));
                }
            }
            Some(TraceType::SubAfter) => {
                for bean in &ctx.trace_beans {
                    sb.clear();
                    sb.extend([
                        TraceType::SubAfter.name().to_string(),
                        ctx.request_id.clone(),
                        bean.msg_id.clone(),
                        ctx.cost_time.to_string(),
                        bool_str(ctx.is_success).to_string(),
                        bean.keys.clone(),
                        ctx.context_code.to_string(),
                    ]);
                    if !ctx.access_channel_or_local().is_cloud() {
                        sb.push(ctx.time_stamp.to_string());
                        sb.push(ctx.group_name.clone());
                    }
                    tb.trans_data.push_str(&join_record(&sb));
                }
            }
            Some(TraceType::EndTransaction) => {
                if let Some(bean) = ctx.trace_beans.first() {
                    sb.extend([
                        TraceType::EndTransaction.name().to_string(),
                        ctx.time_stamp.to_string(),
                        ctx.region_id.clone(),
                        ctx.group_name.clone(),
                        bean.topic.clone(),
                        bean.msg_id.clone(),
                        bean.tags.clone(),
                        bean.keys.clone(),
                        bean.store_host.clone(),
                        bean.msg_type.value().to_string(),
                        bean.transaction_id.clone().unwrap_or_default(),
                        bean.transaction_state.clone().unwrap_or_default(),
                        bool_str(bean.from_transaction_check).to_string(),
                    ]);
                    tb.trans_data = join_record(&sb);
                }
            }
            Some(TraceType::Recall) => {
                if let Some(bean) = ctx.trace_beans.first() {
                    sb.extend([
                        TraceType::Recall.name().to_string(),
                        ctx.time_stamp.to_string(),
                        ctx.region_id.clone(),
                        ctx.group_name.clone(),
                        bean.topic.clone(),
                        bean.msg_id.clone(),
                        bool_str(ctx.is_success).to_string(),
                    ]);
                    tb.trans_data = join_record(&sb);
                }
            }
            None => {}
        }
        for bean in &ctx.trace_beans {
            tb.trans_key.insert(bean.msg_id.clone());
            if !bean.keys.is_empty() {
                for key in bean.keys.split(KEY_SEPARATOR) {
                    tb.trans_key.insert(key.to_string());
                }
            }
        }
        Some(tb)
    }
}

/// `SOH.join(fields) + STX`：一条记录内字段用 CONTENT_SPLITOR 相连，末尾补 FIELD_SPLITOR。
fn join_record(fields: &[String]) -> String {
    let mut out = String::with_capacity(256);
    for (i, value) in fields.iter().enumerate() {
        if i > 0 {
            out.push(TraceConstants::CONTENT_SPLITOR);
        }
        out.push_str(value);
    }
    out.push(TraceConstants::FIELD_SPLITOR);
    out
}

fn bool_str(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

fn field<'a>(line: &[&'a str], index: usize) -> Result<&'a str> {
    opt_field(line, index).ok_or_else(|| {
        Error::Decode(format!(
            "trace field index {index} missing (only {} parts)",
            line.len()
        ))
    })
}

/// 按下标取段，越界返回 `None`（只用于 SubBefore 的 keys 那一处兜底）。
fn opt_field<'a>(line: &[&'a str], index: usize) -> Option<&'a str> {
    line.get(index).copied()
}

fn parse_long(line: &[&str], index: usize) -> Result<i64> {
    let raw = field(line, index)?;
    raw.parse::<i64>()
        .map_err(|e| Error::Decode(format!("trace field {index}={raw:?} is not a long: {e}")))
}

fn parse_int(line: &[&str], index: usize) -> Result<i32> {
    let raw = field(line, index)?;
    raw.parse::<i32>()
        .map_err(|e| Error::Decode(format!("trace field {index}={raw:?} is not an int: {e}")))
}

fn parse_bool(line: &[&str], index: usize) -> Result<bool> {
    Ok(field(line, index)? == "true")
}

fn parse_msg_type(line: &[&str], index: usize) -> Result<MessageType> {
    let ordinal = parse_int(line, index)?;
    MessageType::from_value(ordinal)
        .ok_or_else(|| Error::Decode(format!("trace msgType {ordinal} out of MessageType range")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOH: char = TraceConstants::CONTENT_SPLITOR;
    const STX: char = TraceConstants::FIELD_SPLITOR;

    const MSG_ID_1: &str = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6";
    const MSG_ID_2: &str = "AC1400A1F0A018B4AAC2A1B2C3D4E5F7";
    const OFFSET_MSG_ID: &str = "AC1400A1000027100000000000000001";
    const TS: i64 = 1_700_000_000_000;

    fn expect(fields: &[&str]) -> String {
        let mut out = String::new();
        for (i, f) in fields.iter().enumerate() {
            if i > 0 {
                out.push(SOH);
            }
            out.push_str(f);
        }
        out.push(STX);
        out
    }

    const EXPECTED_PUB: &str = "Pub\u{1}1700000000000\u{1}DefaultRegion\u{1}GID_test\
         \u{1}TopicTest\u{1}AC1400A1F0A018B4AAC2A1B2C3D4E5F6\u{1}TagA\u{1}KeyA KeyB\
         \u{1}127.0.0.1:10911\u{1}42\u{1}7\u{1}0\
         \u{1}AC1400A1000027100000000000000001\u{1}true\u{2}";

    fn bean(msg_id: &str, keys: &str, retry_times: i32) -> TraceBean {
        TraceBean {
            topic: "TopicTest".into(),
            msg_id: msg_id.into(),
            offset_msg_id: OFFSET_MSG_ID.into(),
            tags: "TagA".into(),
            keys: keys.into(),
            store_host: "127.0.0.1:10911".into(),
            client_host: local_address().to_string(),
            store_time: 1_700_000_000_123,
            retry_times,
            body_length: 42,
            msg_type: MessageType::NormalMsg,
            transaction_id: Some("TRAN-001".into()),
            transaction_state: Some(LocalTransactionState::CommitMessage.to_string()),
            from_transaction_check: false,
        }
    }

    fn ctx_of(trace_type: TraceType, beans: Vec<TraceBean>) -> TraceContext {
        TraceContext {
            trace_type: Some(trace_type),
            time_stamp: TS,
            region_id: "DefaultRegion".into(),
            group_name: "GID_test".into(),
            request_id: "REQ-SUB-001".into(),
            trace_beans: beans,
            ..TraceContext::new()
        }
    }


    #[test]
    fn encode_pub_matches_wire_vector() {
        let mut ctx = ctx_of(TraceType::Pub, vec![bean(MSG_ID_1, "KeyA KeyB", 2)]);
        ctx.group_name = "GID_test".into();
        ctx.cost_time = 7;
        ctx.is_success = true;
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        assert_eq!(tb.trans_data, EXPECTED_PUB);
        assert_eq!(
            expect(&[
                "Pub",
                "1700000000000",
                "DefaultRegion",
                "GID_test",
                "TopicTest",
                MSG_ID_1,
                "TagA",
                "KeyA KeyB",
                "127.0.0.1:10911",
                "42",
                "7",
                "0",
                OFFSET_MSG_ID,
                "true"
            ]),
            EXPECTED_PUB
        );
        assert_eq!(
            tb.trans_key.iter().cloned().collect::<Vec<_>>(),
            vec![MSG_ID_1.to_string(), "KeyA".into(), "KeyB".into()]
        );
    }

    #[test]
    fn encode_sub_before_matches_wire_vector() {
        let ctx = ctx_of(
            TraceType::SubBefore,
            vec![bean(MSG_ID_1, "KeyA KeyB", 2), bean(MSG_ID_2, "KeyC", 0)],
        );
        let mut ctx = ctx;
        ctx.group_name = "CID_test".into();
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        assert_eq!(
            tb.trans_data,
            expect(&[
                "SubBefore",
                "1700000000000",
                "DefaultRegion",
                "CID_test",
                "REQ-SUB-001",
                MSG_ID_1,
                "2",
                "KeyA KeyB"
            ]) + &expect(&[
                "SubBefore",
                "1700000000000",
                "DefaultRegion",
                "CID_test",
                "REQ-SUB-001",
                MSG_ID_2,
                "0",
                "KeyC"
            ])
        );
        assert_eq!(tb.trans_key.len(), 5);
        assert!(tb.trans_key.contains(MSG_ID_2));
        assert!(tb.trans_key.contains("KeyC"));
    }

    #[test]
    fn encode_sub_after_matches_wire_vector() {
        let mut ctx = ctx_of(TraceType::SubAfter, vec![bean(MSG_ID_1, "KeyA KeyB", 2)]);
        ctx.group_name = "CID_test".into();
        ctx.cost_time = 11;
        ctx.is_success = false;
        ctx.context_code = 2;
        ctx.access_channel = Some(AccessChannel::Local);
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        assert_eq!(
            tb.trans_data,
            expect(&[
                "SubAfter",
                "REQ-SUB-001",
                MSG_ID_1,
                "11",
                "false",
                "KeyA KeyB",
                "2",
                "1700000000000",
                "CID_test"
            ])
        );
    }

    #[test]
    fn encode_sub_after_cloud_drops_timestamp_and_group() {
        let mut ctx = ctx_of(TraceType::SubAfter, vec![bean(MSG_ID_1, "KeyA KeyB", 2)]);
        ctx.cost_time = 11;
        ctx.is_success = false;
        ctx.context_code = 2;
        ctx.access_channel = Some(AccessChannel::Cloud);
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        assert_eq!(
            tb.trans_data,
            expect(&[
                "SubAfter",
                "REQ-SUB-001",
                MSG_ID_1,
                "11",
                "false",
                "KeyA KeyB",
                "2"
            ])
        );
    }

    #[test]
    fn encode_sub_after_null_access_channel_falls_back_to_local() {
        let mut ctx = ctx_of(TraceType::SubAfter, vec![bean(MSG_ID_1, "", 0)]);
        ctx.access_channel = None;
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        assert!(tb.trans_data.contains("1700000000000"));
        assert_eq!(ctx.access_channel_or_local(), AccessChannel::Local);
    }

    #[test]
    fn encode_end_transaction_matches_wire_vector() {
        let ctx = ctx_of(
            TraceType::EndTransaction,
            vec![bean(MSG_ID_1, "KeyA KeyB", 2)],
        );
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        assert_eq!(
            tb.trans_data,
            expect(&[
                "EndTransaction",
                "1700000000000",
                "DefaultRegion",
                "GID_test",
                "TopicTest",
                MSG_ID_1,
                "TagA",
                "KeyA KeyB",
                "127.0.0.1:10911",
                "0",
                "TRAN-001",
                "COMMIT_MESSAGE",
                "false"
            ])
        );
    }

    #[test]
    fn encode_recall_matches_wire_vector() {
        let ctx = ctx_of(TraceType::Recall, vec![bean(MSG_ID_1, "KeyA KeyB", 2)]);
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        assert_eq!(
            tb.trans_data,
            expect(&[
                "Recall",
                "1700000000000",
                "DefaultRegion",
                "GID_test",
                "TopicTest",
                MSG_ID_1,
                "true"
            ])
        );
    }

    #[test]
    fn encode_null_and_degenerate_inputs() {
        assert!(TraceDataEncoder::encoder_from_context_bean(None).is_none());
        // 无 trace_type / 无 bean：编出空串而不是 panic
        let bare = TraceContext::new();
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(&bare)).unwrap();
        assert_eq!(tb.trans_data, "");
        assert!(tb.trans_key.is_empty());
        let no_beans = ctx_of(TraceType::Pub, vec![]);
        assert_eq!(
            TraceDataEncoder::encoder_from_context_bean(Some(&no_beans))
                .unwrap()
                .trans_data,
            ""
        );
    }

    // ---------------- 解码 ----------------

    #[test]
    fn trace_data_split_drops_trailing_empty() {
        assert_eq!(java_split("a\u{2}", STX), vec!["a"]);
        assert_eq!(java_split("\u{2}", STX), Vec::<&str>::new());
        assert_eq!(java_split("a\u{2}b\u{2}", STX), vec!["a", "b"]);
        assert_eq!(java_split("a\u{1}\u{1}b", SOH), vec!["a", "", "b"]);
        // 原生 split 会保留末尾空串 —— 这正是不能直接用它的原因
        assert_eq!("a\u{2}".split(STX).collect::<Vec<_>>(), vec!["a", ""]);
    }

    #[test]
    fn decode_pub_round_trip() {
        let mut ctx = ctx_of(TraceType::Pub, vec![bean(MSG_ID_1, "KeyA KeyB", 2)]);
        ctx.cost_time = 7;
        let encoded = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        let decoded = TraceDataEncoder::decoder_from_trace_data_string(Some(&encoded.trans_data));
        assert_eq!(decoded.len(), 1);
        let got = &decoded[0];
        assert_eq!(got.trace_type, Some(TraceType::Pub));
        assert_eq!(got.time_stamp, TS);
        assert_eq!(got.region_id, "DefaultRegion");
        assert_eq!(got.group_name, "GID_test");
        assert_eq!(got.cost_time, 7);
        assert!(got.is_success);
        let bean = &got.trace_beans[0];
        assert_eq!(bean.topic, "TopicTest");
        assert_eq!(bean.msg_id, MSG_ID_1);
        assert_eq!(bean.tags, "TagA");
        assert_eq!(bean.keys, "KeyA KeyB");
        assert_eq!(bean.store_host, "127.0.0.1:10911");
        assert_eq!(bean.body_length, 42);
        assert_eq!(bean.offset_msg_id, OFFSET_MSG_ID);
        assert_eq!(bean.msg_type, MessageType::NormalMsg);
        assert_eq!(bean.client_host, local_address());
    }

    #[test]
    fn decode_pub_legacy_13_fields_has_no_offset_msg_id() {
        let legacy = expect(&[
            "Pub",
            "1700000000000",
            "DefaultRegion",
            "GID_test",
            "TopicTest",
            MSG_ID_1,
            "TagA",
            "KeyA",
            "127.0.0.1:10911",
            "42",
            "7",
            "3",
            "false",
        ]);
        let got = &TraceDataEncoder::decoder_from_trace_data_string(Some(&legacy))[0];
        assert!(!got.is_success);
        assert_eq!(got.cost_time, 7);
        let bean = &got.trace_beans[0];
        assert_eq!(bean.msg_type, MessageType::DelayMsg);
        assert_eq!(bean.offset_msg_id, "");
    }

    #[test]
    fn decode_pub_ge_15_fields_reads_client_host() {
        let data = expect(&[
            "Pub",
            "1700000000000",
            "DefaultRegion",
            "GID",
            "T",
            MSG_ID_1,
            "TagA",
            "K",
            "127.0.0.1:10911",
            "42",
            "7",
            "0",
            OFFSET_MSG_ID,
            "true",
            "10.0.0.9:5678",
        ]);
        let got = &TraceDataEncoder::decoder_from_trace_data_string(Some(&data))[0];
        assert_eq!(got.trace_beans[0].client_host, "10.0.0.9:5678");
        assert_eq!(got.trace_beans[0].offset_msg_id, OFFSET_MSG_ID);
    }

    #[test]
    fn decode_sub_before_keeps_request_id_and_retry_times() {
        let mut ctx = ctx_of(
            TraceType::SubBefore,
            vec![bean(MSG_ID_1, "KeyA KeyB", 2), bean(MSG_ID_2, "KeyC", 0)],
        );
        ctx.group_name = "CID_test".into();
        let encoded = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        let decoded = TraceDataEncoder::decoder_from_trace_data_string(Some(&encoded.trans_data));
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].trace_beans[0].msg_id, MSG_ID_1);
        assert_eq!(decoded[1].trace_beans[0].msg_id, MSG_ID_2);
        assert_eq!(decoded[0].trace_beans[0].retry_times, 2);
        assert_eq!(decoded[1].trace_beans[0].retry_times, 0);
        assert!(decoded.iter().all(|d| d.request_id == "REQ-SUB-001"));
        assert!(decoded.iter().all(|d| d.group_name == "CID_test"));
    }

    #[test]
    fn decode_sub_before_without_keys_does_not_break() {
        // keys 为空 => java_split 会连末尾 CONTENT_SPLITOR 一起丢掉，只剩 7 段
        let mut ctx = ctx_of(TraceType::SubBefore, vec![bean(MSG_ID_1, "", 0)]);
        for b in &mut ctx.trace_beans {
            b.keys = String::new();
        }
        let encoded = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        let records = java_split(&encoded.trans_data, STX);
        assert_eq!(records.len(), 1);
        assert_eq!(
            java_split(records[0], SOH).len(),
            7,
            "payload = {encoded:?}"
        );
        let decoded = TraceDataEncoder::decoder_from_trace_data_string(Some(&encoded.trans_data));
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].trace_beans[0].keys, "");
        assert_eq!(decoded[0].trace_beans[0].msg_id, MSG_ID_1);
    }

    #[test]
    fn decode_sub_after_context_code_and_legacy_branch() {
        let mut ctx = ctx_of(TraceType::SubAfter, vec![bean(MSG_ID_1, "KeyA KeyB", 2)]);
        ctx.group_name = "CID_test".into();
        ctx.cost_time = 11;
        ctx.is_success = false;
        ctx.context_code = 4;
        ctx.access_channel = Some(AccessChannel::Local);
        let encoded = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        let got = &TraceDataEncoder::decoder_from_trace_data_string(Some(&encoded.trans_data))[0];
        assert_eq!(got.context_code, 4);
        assert!(!got.is_success);
        assert_eq!(got.time_stamp, TS);
        assert_eq!(got.group_name, "CID_test");

        // 老版本只有 7 段：不该读到 timestamp / group
        let legacy = expect(&["SubAfter", "REQ", MSG_ID_1, "5", "true", "KeyA", "0"]);
        let got = &TraceDataEncoder::decoder_from_trace_data_string(Some(&legacy))[0];
        assert_eq!(got.context_code, 0);
        assert!(got.time_stamp > TS, "落到 TraceContext::new() 的当前时间");
        assert_eq!(got.group_name, "");
    }

    #[test]
    fn decode_end_transaction_and_recall_fields() {
        let encoded = TraceDataEncoder::encoder_from_context_bean(Some(&ctx_of(
            TraceType::EndTransaction,
            vec![bean(MSG_ID_1, "KeyA KeyB", 2)],
        )))
        .unwrap();
        let got = &TraceDataEncoder::decoder_from_trace_data_string(Some(&encoded.trans_data))[0];
        assert_eq!(got.trace_type, Some(TraceType::EndTransaction));
        let tx_bean = &got.trace_beans[0];
        assert_eq!(tx_bean.transaction_id.as_deref(), Some("TRAN-001"));
        assert_eq!(tx_bean.transaction_state.as_deref(), Some("COMMIT_MESSAGE"));
        assert!(!tx_bean.from_transaction_check);
        assert_eq!(tx_bean.msg_type, MessageType::NormalMsg);

        let encoded = TraceDataEncoder::encoder_from_context_bean(Some(&ctx_of(
            TraceType::Recall,
            vec![bean(MSG_ID_1, "KeyA KeyB", 2)],
        )))
        .unwrap();
        let got = &TraceDataEncoder::decoder_from_trace_data_string(Some(&encoded.trans_data))[0];
        assert_eq!(got.trace_type, Some(TraceType::Recall));
        assert!(got.is_success);
        assert_eq!(got.trace_beans[0].topic, "TopicTest");
    }

    #[test]
    fn decode_empty_unknown_and_broken_records() {
        assert!(TraceDataEncoder::decoder_from_trace_data_string(None).is_empty());
        assert!(TraceDataEncoder::decoder_from_trace_data_string(Some("")).is_empty());
        assert!(
            TraceDataEncoder::decoder_from_trace_data_string(Some(&format!("Bogus{SOH}1{STX}")))
                .is_empty()
        );
        // 未知 msgType 越界 => 整条记录跳过
        let bad_type = expect(&[
            "Pub",
            "1700000000000",
            "DefaultRegion",
            "GID",
            "T",
            MSG_ID_1,
            "",
            "",
            "h",
            "1",
            "7",
            "99",
            OFFSET_MSG_ID,
            "true",
        ]);
        assert!(TraceDataEncoder::decoder_from_trace_data_string(Some(&bad_type)).is_empty());
        // 时间戳不是数字 => 跳过
        let bad_ts = expect(&[
            "Pub",
            "not-a-number",
            "DefaultRegion",
            "GID",
            "T",
            MSG_ID_1,
            "",
            "",
            "h",
            "1",
            "7",
            "0",
            OFFSET_MSG_ID,
            "true",
        ]);
        assert!(TraceDataEncoder::decoder_from_trace_data_string(Some(&bad_ts)).is_empty());
        let mixed = format!(
            "{}{}",
            expect(&["Pub", "1700000000000"]),
            TraceDataEncoder::encoder_from_context_bean(Some(&ctx_of(
                TraceType::Recall,
                vec![bean(MSG_ID_1, "", 0)]
            )))
            .unwrap()
            .trans_data
        );
        let decoded = TraceDataEncoder::decoder_from_trace_data_string(Some(&mixed));
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].trace_type, Some(TraceType::Recall));
    }

    #[test]
    fn encode_decode_is_stable_over_multiple_records() {
        let pub_ctx = ctx_of(TraceType::Pub, vec![bean(MSG_ID_1, "KeyA KeyB", 2)]);
        let recall_ctx = ctx_of(TraceType::Recall, vec![bean(MSG_ID_2, "", 0)]);
        let batch = format!(
            "{}{}",
            TraceDataEncoder::encoder_from_context_bean(Some(&pub_ctx))
                .unwrap()
                .trans_data,
            TraceDataEncoder::encoder_from_context_bean(Some(&recall_ctx))
                .unwrap()
                .trans_data
        );
        let decoded = TraceDataEncoder::decoder_from_trace_data_string(Some(&batch));
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].trace_type, Some(TraceType::Pub));
        assert_eq!(decoded[1].trace_type, Some(TraceType::Recall));
    }

    // ---------------- 常量与辅助类型 ----------------

    #[test]
    fn trace_constants_match_wire_contract() {
        assert_eq!(TraceConstants::CONTENT_SPLITOR, '\u{1}');
        assert_eq!(TraceConstants::FIELD_SPLITOR, '\u{2}');
        assert_eq!(TraceConstants::GROUP_NAME_PREFIX, "_INNER_TRACE_PRODUCER");
        assert_eq!(
            TraceConstants::TRACE_INSTANCE_NAME,
            "PID_CLIENT_INNER_TRACE_PRODUCER"
        );
        assert_eq!(TraceConstants::TRACE_TOPIC_PREFIX, "rmq_sys_TRACE_DATA_");
        assert_eq!(TraceConstants::ROCKETMQ_MSG_ID, "rocketmq.mgs_id");
        assert_eq!(
            TraceConstants::ROCKETMQ_RETRY_TIMERS,
            "rocketmq.retry_times"
        );
        assert_eq!(MixAll::TRACE_TOPIC, "RMQ_SYS_TRACE_TOPIC");
        assert_eq!(MixAll::DEFAULT_TRACE_REGION_ID, "DefaultRegion");
        assert_eq!(KEY_SEPARATOR, " ");
    }

    #[test]
    fn trace_type_names_are_wire_enum_names() {
        assert_eq!(TraceType::Pub.name(), "Pub");
        assert_eq!(TraceType::Recall.name(), "Recall");
        assert_eq!(TraceType::SubBefore.name(), "SubBefore");
        assert_eq!(TraceType::SubAfter.name(), "SubAfter");
        assert_eq!(TraceType::EndTransaction.name(), "EndTransaction");
        for t in TraceType::all() {
            assert_eq!(TraceType::from_name(t.name()), Some(t));
        }
        assert_eq!(TraceType::from_name("pub"), None);
        assert_eq!(TraceType::from_name(""), None);
        assert_eq!(TraceType::EndTransaction.to_string(), "EndTransaction");
        assert!(TraceType::Pub < TraceType::Recall);
        assert!(TraceType::SubAfter < TraceType::EndTransaction);
    }

    #[test]
    fn access_channel_defaults_to_local() {
        assert_eq!(AccessChannel::default(), AccessChannel::Local);
        assert_eq!(AccessChannel::Local.name(), "LOCAL");
        assert_eq!(AccessChannel::Cloud.to_string(), "CLOUD");
        assert!(AccessChannel::Cloud.is_cloud());
        assert!(!AccessChannel::Local.is_cloud());
    }

    #[test]
    fn trace_context_uses_contract_defaults() {
        let ctx = TraceContext::new();
        assert_eq!(ctx.trace_type, None);
        assert!(ctx.time_stamp > 0);
        assert!(ctx.is_success, "isSuccess 默认 true");
        assert_eq!(ctx.cost_time, 0);
        assert_eq!(ctx.context_code, 0);
        assert_eq!(ctx.access_channel, None);
        assert!(ctx.trace_beans.is_empty());
        // requestId 32 位大写 hex，两次调用不重复
        assert_eq!(ctx.request_id.len(), 32);
        assert!(ctx.request_id.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(
            ctx.request_id,
            ctx.request_id.to_ascii_uppercase(),
            "requestId 是大写 hex"
        );
        assert_ne!(ctx.request_id, TraceContext::new().request_id);
    }

    #[test]
    fn trace_bean_defaults_use_local_address() {
        let bean = TraceBean::new();
        assert_eq!(bean.topic, "");
        assert_eq!(bean.store_host, local_address());
        assert_eq!(bean.client_host, local_address());
        assert_eq!(bean.msg_type, MessageType::NormalMsg);
        assert_eq!(bean.transaction_state, None);
        let mut bean = bean;
        bean.set_transaction_state(LocalTransactionState::RollbackMessage);
        assert_eq!(bean.transaction_state.as_deref(), Some("ROLLBACK_MESSAGE"));
    }

    #[test]
    fn local_address_looks_like_an_address() {
        let addr = local_address();
        assert!(!addr.is_empty());
        // IPv4 点分十进制，或摊平后的 8 组 IPv6
        let groups = addr.split(':').count();
        assert!(
            groups == 1 || groups == 8,
            "unexpected local address {addr:?}"
        );
        if groups == 8 {
            assert!(addr.chars().all(|c| c.is_ascii_hexdigit() || c == ':'));
        }
        assert_eq!(addr, local_address(), "必须进程内缓存（OnceLock）");
    }

    #[test]
    fn trace_context_display_matches_python_repr() {
        let ctx = ctx_of(TraceType::Pub, vec![bean(MSG_ID_1, "", 0)]);
        assert_eq!(
            ctx.to_string(),
            format!("TraceContext{{Pub_GID_test_DefaultRegion_true_{MSG_ID_1}_TopicTest_}}")
        );
        let bare = TraceContext::new();
        assert_eq!(bare.to_string(), "TraceContext{___true_}");
    }

    #[test]
    fn transfer_bean_starts_empty() {
        let tb = TraceTransferBean::new();
        assert_eq!(tb.trans_data, "");
        assert!(tb.trans_key.is_empty());
    }

    #[test]
    fn encoder_uses_only_ordinary_fields_in_trans_key() {
        // 连续空格切出的空串会进集合，分发器拼 KEYS 时才过滤
        let ctx = ctx_of(TraceType::Recall, vec![bean(MSG_ID_1, "A  B", 0)]);
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap();
        assert!(tb.trans_key.contains("A"));
        assert!(tb.trans_key.contains("B"));
        assert!(tb.trans_key.contains(MSG_ID_1));
    }
}
