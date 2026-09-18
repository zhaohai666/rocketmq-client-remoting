//! `MixAll` 常量与工具（对应 Java `org.apache.rocketmq.common.MixAll`，
//! 参考实现 `python/rocketmq/common/mix_all.py`）。
//!
//! `PermName` 在 Python 里放在 `sysflag.py`，Rust 沿用该位置（见
//! [`crate::common::sysflag::PermName`），此处只做 re-export，避免两处定义漂移。

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::util_all;
use crate::remoting::protocol::ext_fields::StringMap;

pub use crate::common::sysflag::PermName;

/// 对应 Java `MixAll`（纯静态类，这里只作为常量与方法的作用域）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MixAll;

impl MixAll {
    pub const NAMESRV_ADDR_PROPERTY: &'static str = "rocketmq.namesrv.addr";
    pub const NAMESRV_ADDR_ENV: &'static str = "NAMESRV_ADDR";
    pub const MESSAGE_COMPRESS_LEVEL: &'static str = "rocketmq.message.compressLevel";
    pub const DEFAULT_TOPIC: &'static str = "TBW102";
    pub const BENCHMARK_TOPIC: &'static str = "BenchmarkTest";
    pub const DEFAULT_PRODUCER_GROUP: &'static str = "DEFAULT_PRODUCER";
    pub const DEFAULT_CONSUMER_GROUP: &'static str = "DEFAULT_CONSUMER";
    pub const CLIENT_INNER_PRODUCER_GROUP: &'static str = "CLIENT_INNER_PRODUCER";
    pub const SELF_TEST_PRODUCER_GROUP: &'static str = "SELF_TEST_P_GROUP";
    pub const SELF_TEST_CONSUMER_GROUP: &'static str = "SELF_TEST_C_GROUP";
    pub const SCHEDULE_CONSUMER_GROUP: &'static str = "SCHEDULE_CONSUMER";
    pub const ONS_HTTP_PROXY_GROUP: &'static str = "CID_ONS-HTTP-PROXY";
    pub const CID_ONSAPI_PERMISSION_GROUP: &'static str = "CID_ONSAPI_PERMISSION";
    pub const CID_ONSAPI_OWNER_GROUP: &'static str = "CID_ONSAPI_OWNER";
    pub const CID_ONSAPI_PULL_GROUP: &'static str = "CID_ONSAPI_PULL";
    pub const CID_SYS_RMQ_TRANS: &'static str = "CID_SYS_RMQ_TRANS";
    pub const ONS_ADDR: &'static str = "ONS_ADDR";
    pub const CID_RMQ_SYS_PREFIX: &'static str = "CID_RMQ_SYS_";
    pub const CID_ONSAPI_PREFIX: &'static str = "CID_ONSAPI_";
    pub const CID_SDK_SYNC_PREFIX: &'static str = "CID_SDK_SYNC_";
    pub const CID_SDK_ASYNC_PREFIX: &'static str = "CID_SDK_ASYNC_";
    pub const CID_SDK_PROXY_PREFIX: &'static str = "CID_SDK_PROXY_";
    pub const PROXY_NAME: &'static str = "MQProxy";
    pub const DEFAULT_PRODUCER_GROUP_AND_STREAM: &'static str = "DEFAULT_PRODUCER_AND_STREAM";

    pub const RETRY_GROUP_TOPIC_PREFIX: &'static str = "%RETRY%";
    pub const DLQ_GROUP_TOPIC_PREFIX: &'static str = "%DLQ%";
    pub const REPLY_TOPIC_PREFIX: &'static str = "%REPLY%";
    /// Request-Reply：应答 topic 名 = `<cluster>_REPLY_TOPIC`。
    pub const REPLY_TOPIC_POSTFIX: &'static str = "REPLY_TOPIC";
    /// Request-Reply：应答消息的 `MSG_TYPE` 属性值。
    pub const REPLY_MESSAGE_FLAG: &'static str = "reply";
    pub const SYSTEM_TOPIC_PREFIX: &'static str = "rmq_sys_";
    pub const TOOLS_CONSUMER_GROUP: &'static str = "TOOLS_CONSUMER";
    pub const FILTERSRV_CONSUMER_GROUP: &'static str = "FILTERSRV_CONSUMER";
    pub const MONITOR_CONSUMER_GROUP: &'static str = "__MONITOR_CONSUMER";
    pub const CLIENT_INNER_CONSUMER_GROUP: &'static str = "CLIENT_INNER_CONSUMER";
    pub const SELF_TEST_CONSUMER_GROUP2: &'static str = "SELF_TEST_C_GROUP2";
    pub const ONS_NAMESPACE: &'static str = "namespace";
    /// ⚠ Java `MixAll.UNIQUE_MSG_QUERY_FLAG` 是 extFields 的**键名**（值为
    /// "true"/"false"），不是数字标志位。
    pub const UNIQUE_MSG_QUERY_FLAG: &'static str = "_UNIQUE_KEY_QUERY";
    pub const TRACE_TOPIC: &'static str = "RMQ_SYS_TRACE_TOPIC";
    pub const REAL_TRACE_TOPIC: &'static str = "rmq_sys_TRACE_DATA";
    /// 轨迹里的 region 占位值：SEND 响应头没带 MSG_REGION 时用它。
    pub const DEFAULT_TRACE_REGION_ID: &'static str = "DefaultRegion";
    pub const TRANS_STAT_PROGRESS_TOPIC: &'static str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    pub const RMQ_SYS_TRANS_HALF_TOPIC: &'static str = "RMQ_SYS_TRANS_HALF_TOPIC";
    pub const RMQ_SYS_TRANS_OP_HALF_TOPIC: &'static str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    pub const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: &'static str = "RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC";
    pub const RMQ_SYS_TRANS_CHECK_MAX_TIME: i32 = 15;
    pub const TRANS_CHECK_MAX_TIME: i32 = 15;
    pub const UNIT_PREFIX: &'static str = "unit_";
    pub const LMQ_PREFIX: &'static str = "%LMQ%";
    pub const LMQ_QUEUE_ID: i32 = 0;
    pub const DEFAULT_TOPIC_QUEUE_NUMS: i32 = 4;
    pub const DEFAULT_TOPIC_READ_QUEUE_NUMS: i32 = 4;
    pub const DEFAULT_TOPIC_WRITE_QUEUE_NUMS: i32 = 4;
    pub const MAX_TOPIC_LENGTH: i32 = 127;
    pub const MAX_GROUP_LENGTH: i32 = 255;
    pub const CHARACTER_MAX_LENGTH: i32 = 255;
    pub const PULL_THRESHOLD_LEVEL_HIGH: i32 = 1;
    pub const PULL_THRESHOLD_LEVEL_MEDIUM: i32 = 2;
    pub const PULL_THRESHOLD_LEVEL_LOW: i32 = 3;
    pub const PULL_TIMEOUT_MILLIS_HIGH: i64 = 30000;
    pub const PULL_TIMEOUT_MILLIS_MEDIUM: i64 = 20000;
    pub const PULL_TIMEOUT_MILLIS_LOW: i64 = 10000;
    pub const LOG_STATS_TOPIC: &'static str = "LOG_STATS_TOPIC";

    pub const W_HELPER: &'static str = "HELPER";
    pub const W_EXPIRY_DATE: &'static str = "EXPIRY_DATE";
    pub const W_AVATAR: &'static str = "AVATAR";
    pub const W_REGION_ID: &'static str = "REGION_ID";

    pub const MASTER_ID: i32 = 0;
    pub const DEFAULT_CENTER: &'static str = "DEFAULT_CENTER";

    pub const NAMESPACE_PATTERN: &'static str = "^[%s]{4}[a-zA-Z0-9_-]+$";

    /// `PermName.PERM_READ | PermName.PERM_WRITE`
    pub const READ_PERM_BY_DEFAULT: i32 = 4 | 2;

    /// admin 侧「全部 topic」哨兵值。⚠ Python 参考实现与本仓库 Java 快照都没有这个
    /// 常量，它也不参与线上编解码，只作为客户端层参数校验的哨兵。
    pub const ALL_TOPIC: &'static str = "all";

    /// 对应 Java `MixAll.getRetryTopic`。
    pub fn get_retry_topic(consumer_group: &str) -> String {
        format!("{}{}", Self::RETRY_GROUP_TOPIC_PREFIX, consumer_group)
    }

    pub fn is_retry_topic(topic: Option<&str>) -> bool {
        match topic {
            Some(t) => t.starts_with(Self::RETRY_GROUP_TOPIC_PREFIX),
            None => false,
        }
    }

    /// 对应 Java `MixAll.getDLQTopic`。
    pub fn get_dlq_topic(consumer_group: &str) -> String {
        format!("{}{}", Self::DLQ_GROUP_TOPIC_PREFIX, consumer_group)
    }

    pub fn is_dlq_topic(topic: Option<&str>) -> bool {
        match topic {
            Some(t) => t.starts_with(Self::DLQ_GROUP_TOPIC_PREFIX),
            None => false,
        }
    }

    /// 对应 Java `MixAll.getReplyTopic(clusterName)`。
    ///
    /// 这**不是**控制台里那个 `%REPLY%<topic>` 前缀（那是另一套东西），只用于 request-reply。
    pub fn get_reply_topic(cluster_name: &str) -> String {
        format!("{}_{}", cluster_name, Self::REPLY_TOPIC_POSTFIX)
    }

    pub fn get_broker_circuit_breaker_consume_group() -> &'static str {
        "BROKER_CIRCUIT_BREAKER"
    }

    pub fn get_broker_circuit_breaker_topic() -> &'static str {
        "BROKER_CIRCUIT_BREAKER_TOPIC"
    }

    pub fn is_sys_topic(topic: Option<&str>) -> bool {
        match topic {
            Some(t) => t.starts_with(Self::SYSTEM_TOPIC_PREFIX),
            None => false,
        }
    }

    /// 对应 Java `MixAll.isLmq`（LMQ topic 以 `%LMQ%` 开头）。
    pub fn is_lmq(lmq_meta_data: Option<&str>) -> bool {
        match lmq_meta_data {
            Some(t) => t.starts_with(Self::LMQ_PREFIX),
            None => false,
        }
    }

    /// 对应 Java `MixAll.isSysConsumerGroup`（`CID_RMQ_SYS_` 前缀）。
    pub fn is_sys_consumer_group(consumer_group: Option<&str>) -> bool {
        match consumer_group {
            Some(g) => g.starts_with(Self::CID_RMQ_SYS_PREFIX),
            None => false,
        }
    }

    /// 对应 Java `MixAll.isPredefinedGroup` 的 `PREDEFINE_GROUP_SET`。
    pub fn is_predefined_group(consumer_group: &str) -> bool {
        Self::predefine_group_set().contains(&consumer_group)
    }

    /// 对应 Java `MixAll.PREDEFINE_GROUP_SET`。
    pub fn predefine_group_set() -> &'static [&'static str] {
        &[
            Self::DEFAULT_CONSUMER_GROUP,
            Self::DEFAULT_PRODUCER_GROUP,
            Self::TOOLS_CONSUMER_GROUP,
            Self::SCHEDULE_CONSUMER_GROUP,
            Self::FILTERSRV_CONSUMER_GROUP,
            Self::MONITOR_CONSUMER_GROUP,
            Self::CLIENT_INNER_PRODUCER_GROUP,
            Self::SELF_TEST_PRODUCER_GROUP,
            Self::SELF_TEST_CONSUMER_GROUP,
            Self::ONS_HTTP_PROXY_GROUP,
            Self::CID_ONSAPI_PERMISSION_GROUP,
            Self::CID_ONSAPI_OWNER_GROUP,
            Self::CID_ONSAPI_PULL_GROUP,
            Self::CID_SYS_RMQ_TRANS,
        ]
    }

    /// 已知系统 topic（admin 过滤用户 topic 时用；broker 侧真值以
    /// `GET_SYSTEM_TOPIC_LIST` 返回为准，Python `admin.getUserTopicConfig` 两路都查）。
    pub fn system_topic_set() -> &'static [&'static str] {
        &[
            Self::DEFAULT_TOPIC,
            Self::TRACE_TOPIC,
            Self::REAL_TRACE_TOPIC,
            Self::TRANS_STAT_PROGRESS_TOPIC,
            Self::RMQ_SYS_TRANS_HALF_TOPIC,
            Self::RMQ_SYS_TRANS_OP_HALF_TOPIC,
            Self::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
            Self::LOG_STATS_TOPIC,
            Self::BENCHMARK_TOPIC,
        ]
    }

    /// 系统 topic：显式名单 + `rmq_sys_` 前缀 + `%RETRY%` / `%DLQ%`。
    pub fn is_system_topic(topic: &str) -> bool {
        Self::system_topic_set().contains(&topic)
            || Self::is_sys_topic(Some(topic))
            || Self::is_retry_topic(Some(topic))
            || Self::is_dlq_topic(Some(topic))
    }

    /// 对应 Java `MixAll.resetRetryAndDLQTopic`：剥掉 `%RETRY%` / `%DLQ%` 前缀。
    pub fn reset_retry_and_dlq_topic(topic: Option<&str>) -> Option<String> {
        let topic = topic?;
        if Self::is_retry_topic(Some(topic)) {
            return Some(topic[Self::RETRY_GROUP_TOPIC_PREFIX.len()..].to_string());
        }
        if Self::is_dlq_topic(Some(topic)) {
            return Some(topic[Self::DLQ_GROUP_TOPIC_PREFIX.len()..].to_string());
        }
        Some(topic.to_string())
    }

    /// 对应 Java `MixAll.compareAndIncreaseNamespace`。
    pub fn compare_and_increase_namespace(instance_name: &str, namespace: Option<&str>) -> String {
        let namespace = match namespace {
            Some(ns) if !ns.is_empty() => ns,
            _ => return instance_name.to_string(),
        };
        if instance_name.starts_with(namespace) {
            return instance_name.to_string();
        }
        let namespace_prefix = format!("%{namespace}");
        if instance_name.starts_with(&namespace_prefix) {
            return instance_name.to_string();
        }
        format!("%{namespace}%%{instance_name}")
    }

    /// 对应 Java `MixAll.createUniqName`（Python 用 `uuid4().hex`，这里用
    /// 「纳秒 + PID + 进程内自增」拼 32 位十六进制，唯一性来源相同）。
    pub fn create_uniq_name(prefix: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let seq = COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        format!(
            "{prefix}{:08x}{:08x}{:08x}{:08x}",
            util_all::get_pid(),
            (nanos >> 32) as u32,
            nanos as u32,
            (seq >> 32) as u32 ^ (seq as u32).rotate_left(16)
        )
    }

    /// 对应 Python `MixAll.get_ip_str`：探测出口 IP，失败回落 `127.0.0.1`。
    pub fn get_ip_str() -> String {
        util_all::local_ip()
    }

    /// 进程内缓存版 `get_ip_str`（对应 Java 的 `MixAll.LOCAL_INET_ADDRESS` 静态字段：
    /// 每个进程只探测一次，msgId / clientId 都复用它）。
    pub fn cached_ip_str() -> &'static str {
        static IP: OnceLock<String> = OnceLock::new();
        IP.get_or_init(Self::get_ip_str)
    }

    pub fn pid() -> u32 {
        util_all::get_pid()
    }

    /// 进程内缓存 PID（对应 Java `MixAll.CURRENT_JVM_PID`）。
    pub fn cached_pid() -> u32 {
        static PID: OnceLock<u32> = OnceLock::new();
        *PID.get_or_init(util_all::get_pid)
    }

    /// 对应 Java `ClientConfig#buildMQClientId`：`ip@instanceName[@unitName]`。
    pub fn build_mq_client_id(client_ip: &str, instance_name: &str, unit_name: Option<&str>) -> String {
        let mut sb = String::with_capacity(client_ip.len() + instance_name.len() + 2);
        sb.push_str(client_ip);
        sb.push('@');
        sb.push_str(instance_name);
        if util_all::is_not_blank_str(unit_name.unwrap_or("")) {
            sb.push('@');
            sb.push_str(unit_name.unwrap_or(""));
        }
        sb
    }

    /// Python 客户端层的 client_id 口径：`instanceName@yyyyMMddHHmmss`。
    pub fn build_default_client_id(instance_name: &str) -> String {
        let now = chrono::Local::now().format("%Y%m%d%H%M%S").to_string();
        format!("{instance_name}@{now}")
    }

    /// 对应 Java `MixAll.brokerVIPChannel`：VIP 通道 = 端口 - 2。
    ///
    /// 端口不可解析时原样返回（Java 会抛 NumberFormatException，这里不让传输层崩掉）。
    pub fn broker_vip_channel(is_change: bool, broker_addr: &str) -> String {
        if !is_change {
            return broker_addr.to_string();
        }
        match broker_addr.rsplit_once(':') {
            Some((host, port)) => match port.parse::<i64>() {
                Ok(v) => format!("{host}:{}", v - 2),
                Err(_) => broker_addr.to_string(),
            },
            None => broker_addr.to_string(),
        }
    }

    /// 对应 Java 4.x `MixAll.messageQueue2string`：`topic brokerName queueId`。
    pub fn message_queue_to_string(mq: &crate::common::message::MessageQueue) -> String {
        format!("{} {} {}", mq.topic, mq.broker_name, mq.queue_id)
    }

    /// 对应 Java 4.x `MixAll.string2messageQueue`：按 `separator` 拆 3 段。
    pub fn string_to_message_queue(
        queue: &str,
        separator: &str,
    ) -> crate::error::Result<crate::common::message::MessageQueue> {
        let parts: Vec<&str> = queue.split(separator).collect();
        if parts.len() < 3 {
            return Err(crate::error::Error::Decode(format!(
                "message queue string {queue:?} has {} parts, need 3",
                parts.len()
            )));
        }
        let queue_id = parts[2].trim().parse::<i32>().map_err(|e| {
            crate::error::Error::Decode(format!("bad queueId in {queue:?}: {e}"))
        })?;
        Ok(crate::common::message::MessageQueue::new(parts[0], parts[1], queue_id))
    }

    /// 对应 Java 4.x `MixAll.string2messageQueues`：换行分隔，**逐条容错**
    /// （Java 里解析失败的行会被跳过并打日志）。
    pub fn string_to_message_queues(queues: &str) -> Vec<crate::common::message::MessageQueue> {
        let mut out = Vec::new();
        for line in queues.split('\n') {
            if line.trim().is_empty() {
                continue;
            }
            match Self::string_to_message_queue(line, " ") {
                Ok(mq) => out.push(mq),
                Err(_) => continue,
            }
        }
        out
    }

    /// 对应 Java `MixAll.properties2String`：每条 `key=value\n`，null 值跳过。
    pub fn properties_to_string(properties: &StringMap, is_sort: bool) -> String {
        let mut items: Vec<(&str, &str)> =
            properties.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        if is_sort {
            items.sort_by(|a, b| a.0.cmp(b.0));
        }
        let mut buf = String::new();
        for (k, v) in items {
            buf.push_str(k);
            buf.push('=');
            buf.push_str(v);
            buf.push('\n');
        }
        buf
    }

    /// 对应 Java `MixAll.string2Properties`（`java.util.Properties.load` 语义）：
    /// - 跳过空行与 `#` / `!` 注释行；
    /// - 行尾**未转义**的 `\` 表示续行，下一行前导空白被丢弃；
    /// - 键与值以**第一个** `=`、`:` 或**空白**分隔（空白也是合法分隔符！）；
    /// - 分隔符前后的空白被跳过；值的**尾部**空白保留（Java 不去尾空白）。
    ///
    /// 注：Java 还会处理 `\t \n \uXXXX` 等转义，broker 配置导出里不出现，
    /// 这里不实现（避免把反斜杠语义做错反而不一致）。
    pub fn string_to_properties(text: &str) -> StringMap {
        const PROP_WS: &[char] = &[' ', '\t', '\u{c}'];
        let mut result = StringMap::new();

        // 先把续行合并成逻辑行
        let mut logical: Vec<String> = Vec::new();
        let mut pending: Option<String> = None;
        for raw in text.split('\n') {
            let raw = raw.trim_end_matches('\r');
            let line = match pending.take() {
                Some(prefix) => {
                    let mut s = prefix;
                    s.push_str(raw.trim_start_matches(PROP_WS));
                    s
                }
                None => raw.to_string(),
            };
            let trailing = line.len() - line.trim_end_matches('\\').len();
            if trailing % 2 == 1 {
                pending = Some(line[..line.len() - 1].to_string());
                continue;
            }
            logical.push(line);
        }
        if let Some(tail) = pending {
            logical.push(tail);
        }

        for line in logical {
            let stripped = line.trim();
            if stripped.is_empty() || stripped.starts_with('#') || stripped.starts_with('!') {
                continue;
            }
            let chars: Vec<char> = line.chars().collect();
            let n = chars.len();
            let mut i = 0;
            while i < n && PROP_WS.contains(&chars[i]) {
                i += 1;
            }
            let key_start = i;
            while i < n && chars[i] != '=' && chars[i] != ':' && !PROP_WS.contains(&chars[i]) {
                i += 1;
            }
            let key: String = chars[key_start..i].iter().collect();
            while i < n && PROP_WS.contains(&chars[i]) {
                i += 1;
            }
            if i < n && (chars[i] == '=' || chars[i] == ':') {
                i += 1;
                while i < n && PROP_WS.contains(&chars[i]) {
                    i += 1;
                }
            }
            let value: String = chars[i..].iter().collect();
            result.insert(key, value);
        }
        result
    }
}

/// 按 key 查消息的三种模式（对应 tools 的
/// `QueryMsgByKeySubCommand.QueryMsgType`）。
///
/// 与 `MixAll.UNIQUE_MSG_QUERY_FLAG` 不是一个东西：后者是 extFields 里的**键名**。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryMsgType;

impl QueryMsgType {
    pub const ALL_MESSAGE: i32 = 0;
    pub const UNIQUE_KEY: i32 = 1;
    pub const NORMAL: i32 = 2;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::MessageQueue;

    #[test]
    fn topic_and_group_prefixes() {
        assert_eq!(MixAll::RETRY_GROUP_TOPIC_PREFIX, "%RETRY%");
        assert_eq!(MixAll::DLQ_GROUP_TOPIC_PREFIX, "%DLQ%");
        assert_eq!(MixAll::LMQ_PREFIX, "%LMQ%");
        assert_eq!(MixAll::SYSTEM_TOPIC_PREFIX, "rmq_sys_");
        assert_eq!(MixAll::get_retry_topic("GroupA"), "%RETRY%GroupA");
        assert_eq!(MixAll::get_dlq_topic("GroupA"), "%DLQ%GroupA");
        assert_eq!(MixAll::get_reply_topic("DefaultCluster"), "DefaultCluster_REPLY_TOPIC");
        assert!(MixAll::is_retry_topic(Some("%RETRY%G")));
        assert!(!MixAll::is_retry_topic(None));
        assert!(MixAll::is_dlq_topic(Some("%DLQ%G")));
        assert!(MixAll::is_lmq(Some("%LMQ%foo")));
        assert!(!MixAll::is_lmq(Some("normal")));
        assert!(MixAll::is_sys_consumer_group(Some("CID_RMQ_SYS_x")));
        assert!(!MixAll::is_sys_consumer_group(Some("normal")));
        assert!(MixAll::is_sys_topic(Some("rmq_sys_TRACE_DATA")));
    }

    #[test]
    fn misc_constants() {
        assert_eq!(MixAll::DEFAULT_TOPIC, "TBW102");
        assert_eq!(MixAll::UNIQUE_MSG_QUERY_FLAG, "_UNIQUE_KEY_QUERY");
        assert_eq!(MixAll::DEFAULT_TRACE_REGION_ID, "DefaultRegion");
        assert_eq!(MixAll::REPLY_MESSAGE_FLAG, "reply");
        assert_eq!(MixAll::READ_PERM_BY_DEFAULT, 6);
        assert_eq!(MixAll::MASTER_ID, 0);
        assert_eq!(MixAll::LMQ_QUEUE_ID, 0);
        assert_eq!(MixAll::DEFAULT_TOPIC_QUEUE_NUMS, 4);
        assert_eq!(MixAll::MAX_TOPIC_LENGTH, 127);
        assert_eq!(MixAll::MAX_GROUP_LENGTH, 255);
        assert_eq!(MixAll::RMQ_SYS_TRANS_CHECK_MAX_TIME, 15);
        assert_eq!(MixAll::TRANS_CHECK_MAX_TIME, 15);
        assert_eq!(
            (
                MixAll::PULL_THRESHOLD_LEVEL_HIGH,
                MixAll::PULL_THRESHOLD_LEVEL_MEDIUM,
                MixAll::PULL_THRESHOLD_LEVEL_LOW
            ),
            (1, 2, 3)
        );
        assert_eq!(
            (
                MixAll::PULL_TIMEOUT_MILLIS_HIGH,
                MixAll::PULL_TIMEOUT_MILLIS_MEDIUM,
                MixAll::PULL_TIMEOUT_MILLIS_LOW
            ),
            (30000i64, 20000, 10000)
        );
        assert_eq!(MixAll::NAMESRV_ADDR_ENV, "NAMESRV_ADDR");
        assert_eq!(MixAll::NAMESPACE_PATTERN, "^[%s]{4}[a-zA-Z0-9_-]+$");
    }

    #[test]
    fn predefined_and_system_groups() {
        assert!(MixAll::is_predefined_group("TOOLS_CONSUMER"));
        assert!(!MixAll::is_predefined_group("my-group"));
        assert_eq!(MixAll::predefine_group_set().len(), 14);
        assert!(MixAll::is_system_topic("rmq_sys_TRACE_DATA"));
        assert!(MixAll::is_system_topic("TBW102"));
        assert!(MixAll::is_system_topic("%RETRY%G"));
        assert!(!MixAll::is_system_topic("TopicTest"));
    }

    #[test]
    fn retry_and_dlq_reset() {
        assert_eq!(
            MixAll::reset_retry_and_dlq_topic(Some("%RETRY%TopicA")).as_deref(),
            Some("TopicA")
        );
        assert_eq!(MixAll::reset_retry_and_dlq_topic(Some("%DLQ%G")).as_deref(), Some("G"));
        assert_eq!(
            MixAll::reset_retry_and_dlq_topic(Some("plain")).as_deref(),
            Some("plain")
        );
        assert_eq!(MixAll::reset_retry_and_dlq_topic(None), None);
    }

    #[test]
    fn namespace_helper() {
        assert_eq!(MixAll::compare_and_increase_namespace("inst", None), "inst");
        assert_eq!(MixAll::compare_and_increase_namespace("inst", Some("")), "inst");
        assert_eq!(MixAll::compare_and_increase_namespace("inst", Some("inst")), "inst");
        assert_eq!(MixAll::compare_and_increase_namespace("%ns%inst", Some("ns")), "%ns%inst");
        assert_eq!(
            MixAll::compare_and_increase_namespace("inst", Some("ns")),
            "%ns%%inst"
        );
    }

    #[test]
    fn uniq_name_is_32_hex_chars_after_prefix() {
        let a = MixAll::create_uniq_name("PID-");
        assert!(a.starts_with("PID-"));
        let tail = &a["PID-".len()..];
        assert_eq!(tail.len(), 32);
        // Python 用 uuid4().hex：32 位**小写**十六进制
        assert!(tail
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)));
        assert_ne!(a, MixAll::create_uniq_name("PID-"));
    }

    #[test]
    fn cached_accessors_are_stable() {
        assert_eq!(MixAll::cached_ip_str(), MixAll::cached_ip_str());
        assert!(util_all::is_ipv4(MixAll::cached_ip_str()) || util_all::is_ipv6(MixAll::cached_ip_str()));
        assert_eq!(MixAll::cached_pid(), MixAll::cached_pid());
        assert!(MixAll::cached_pid() > 0);
    }

    #[test]
    fn client_id_builders() {
        assert_eq!(MixAll::build_mq_client_id("10.0.0.1", "inst", None), "10.0.0.1@inst");
        assert_eq!(MixAll::build_mq_client_id("10.0.0.1", "inst", Some("")), "10.0.0.1@inst");
        assert_eq!(MixAll::build_mq_client_id("10.0.0.1", "inst", Some("  ")), "10.0.0.1@inst");
        assert_eq!(
            MixAll::build_mq_client_id("10.0.0.1", "inst", Some("unit-a")),
            "10.0.0.1@inst@unit-a"
        );
        let id = MixAll::build_default_client_id("inst");
        assert!(id.starts_with("inst@"));
        assert_eq!(id.len(), "inst@".len() + 14);
    }

    #[test]
    fn broker_vip_channel_subtracts_two_from_port() {
        assert_eq!(MixAll::broker_vip_channel(true, "127.0.0.1:10911"), "127.0.0.1:10909");
        assert_eq!(MixAll::broker_vip_channel(false, "127.0.0.1:10911"), "127.0.0.1:10911");
        assert_eq!(MixAll::broker_vip_channel(true, "127.0.0.1:notaport"), "127.0.0.1:notaport");
        assert_eq!(MixAll::broker_vip_channel(true, "no-colon"), "no-colon");
    }

    #[test]
    fn message_queue_strings() {
        let mq = MessageQueue::new("TopicTest", "broker-a", 3);
        assert_eq!(MixAll::message_queue_to_string(&mq), "TopicTest broker-a 3");
        let back = MixAll::string_to_message_queue("TopicTest broker-a 3", " ").unwrap();
        assert_eq!(back, mq);
        let list = MixAll::string_to_message_queues("T1 b1 0\nT1 b1 1\n\nbroken\n");
        assert_eq!(list.len(), 2);
        assert_eq!(list[1], MessageQueue::new("T1", "b1", 1));
        assert!(MixAll::string_to_message_queue("T b", " ").is_err());
        assert!(MixAll::string_to_message_queue("T b x", " ").is_err());
    }

    #[test]
    fn properties_text_round_trip_matches_java() {
        let text = "a = b\n  c : d  \nk=v\n#comment\n!c2\nempty=\ncont=first\\\n    second\n";
        let props = MixAll::string_to_properties(text);
        assert_eq!(props.get("a"), Some("b"));
        assert_eq!(props.get("c"), Some("d  "));
        assert_eq!(props.get("k"), Some("v"));
        assert_eq!(props.get("empty"), Some(""));
        assert_eq!(props.get("cont"), Some("firstsecond"));
        assert!(!props.contains_key("#comment"));

        let round_trip = MixAll::string_to_properties(&MixAll::properties_to_string(&props, false));
        assert_eq!(round_trip, props);
    }

    #[test]
    fn properties_whitespace_is_a_valid_separator() {
        let props = MixAll::string_to_properties("a b\nonlykey\nk=v\n");
        assert_eq!(props.get("a"), Some("b"));
        assert_eq!(props.get("onlykey"), Some(""));
        assert_eq!(props.get("k"), Some("v"));
        // 空白分隔时，值里的 '=' 原样保留
        assert_eq!(
            MixAll::string_to_properties("a b=c=d\n").get("a"),
            Some("b=c=d")
        );
        // 有 '=' 时 '=' 优先于空白成为分隔符（值里的空格保留）
        assert_eq!(
            MixAll::string_to_properties("messageDelayLevel=1s 5s 10s\n").get("messageDelayLevel"),
            Some("1s 5s 10s")
        );
    }

    #[test]
    fn properties_to_string_sorting_option() {
        let mut props = StringMap::new();
        props.insert("z", "1");
        props.insert("a", "2");
        assert_eq!(MixAll::properties_to_string(&props, false), "z=1\na=2\n");
        assert_eq!(MixAll::properties_to_string(&props, true), "a=2\nz=1\n");
    }

    #[test]
    fn query_msg_type_values() {
        assert_eq!(
            (QueryMsgType::ALL_MESSAGE, QueryMsgType::UNIQUE_KEY, QueryMsgType::NORMAL),
            (0, 1, 2)
        );
    }
}
