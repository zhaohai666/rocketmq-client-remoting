//! 消费者层（对应 `org.apache.rocketmq.client.consumer.*` 与 Python
//! `client/consumer.py`）。
//!
//! 本模块按 Python 的文件切分收三件事：投递前的过滤工具、`MessageSelector`、
//! `PopProcessQueue`，以及 [`DefaultMQPushConsumer`] —— 注册监听器 + 重平衡 +
//! 拉取/POP 消费循环 + 位点持久化 + broker 主动请求的四条接缝。
//! `DefaultMQPullConsumer` 与 `DefaultLitePullConsumer` 在
//! [`crate::client::pull_consumer`]（Python 与它们同文件，Rust 里拆成两个模块，
//! 见下「模块划分」）。
//!
//! # 与 Python 参考实现的对应关系
//!
//! Python 用 `threading.Thread` 跑重平衡、每队列拉取、POP、消费分发、位点持久化、
//! 队列锁六类循环；Rust 这边全部落在 tokio 上，用 `tokio::sync::mpsc` +
//! 定时轮询替代 `queue.Queue` + `Condition`。行为口径不变，实现手段的差别如下。
//!
//! # 与 Java 5.5.1 的有意偏离
//!
//! 1. **一个消费者一个 `ProcessQueue`**：Java 的 `ProcessQueue` 带有序消费的红黑树
//!    与 `msgTreeMap` 锁语义，`ConsumeMessageOrderlyService` 还依赖 broker 侧队列锁；
//!    Python 侧只实现了并发消费的缓冲（`msg_found_list` 直接进 `deque`），这里
//!    与 Python 同口径，不引入 Java 的树结构。
//! 2. **`Clone` 句柄 + `Arc<Inner>`**：与 [`crate::client::producer`] 同样的手法 ——
//!    消费者要同时被心跳任务、拉取任务与 broker 请求处理器持有，故句柄可复制、
//!    状态集中在 `Inner` 里用 `RwLock`/`Mutex` 保护。
//! 3. **监听器/策略/钩子用 trait 对象**：Python 靠鸭子类型，Rust 用
//!    `Arc<dyn ...>`（`MessageListenerConcurrently`、
//!    [`AllocateMessageQueueStrategy`](crate::client::allocate_strategy::AllocateMessageQueueStrategy)
//!    等）。
//! 4. **后台任务持 `Weak<Inner>`**：与生产者一致，避免 实例 → 传输 → 任务 → 消费者
//!    的强引用环；`Drop for Inner` 负责发停止信号并 abort 任务。
//! 5. **轨迹分发器由调用方注入**：见
//!    [`set_trace_dispatcher`](Self::set_trace_dispatcher)（生产者侧同一约定，
//!    差异说明见 [`crate::client::producer`] 模块头 4）。
//!
//! # 模块划分
//!
//! Python 的 `consumer.py` 有 2700 行、四个消费者类。Rust 拆成
//! `consumer.rs`（过滤工具 + `MessageSelector` + `PopProcessQueue` +
//! push 消费者）与 `pull_consumer.rs`（`DefaultMQPullConsumer` +
//! `DefaultLitePullConsumer`），因为后两者只是 RPC 的门面组合，与 push 消费者的
//! 循环/重平衡状态机没有共享代码；两模块共用的只有本文件导出的
//! [`filter_messages_for_delivery`] 等工具。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

use crate::client::allocate_strategy::{
    AllocateMessageQueueAveragely, AllocateMessageQueueStrategy,
};
use crate::client::consume_executor::{ConsumeExecutor, ConsumeTask};
use crate::client::consumer_stats::ConsumerStatsManager;
use crate::client::hook::{
    execute_consume_hook_after, execute_consume_hook_before, ConsumeMessageContext,
    ConsumeMessageHook, ConsumeMessageHookList, FilterMessageContext, FilterMessageHookList,
};
use crate::client::mq_client::{
    ConsumerFuture, MQClientInstance, MQClientInstanceConfig, RegisteredConsumer,
};
use crate::client::producer::{SinkAdapter, TraceDispatcherChannel};
use crate::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, ConsumeReturnType, MessageListenerConcurrently, MessageListenerOrderly,
    PopStatus, PullStatus,
};
use crate::client::top_addressing::DefaultTopAddressing;
use crate::client::trace::AccessChannel;
use crate::client::trace_hook::{ConsumeMessageTraceHook, TraceReportSink};
use crate::client::validators;
use crate::common::message::{MessageExt, MessageQueue};
use crate::common::message_const::{
    PROPERTY_MAX_OFFSET, PROPERTY_POP_CK, PROPERTY_RETRY_TOPIC,
};
use crate::common::mix_all::MixAll;
use crate::common::sysflag::{ConsumeInitMode, PullSysFlag};
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};
use crate::remoting::protocol::admin_body::MessageQueueKey;
use crate::remoting::protocol::body::{
    CMResult, ConsumeMessageDirectlyResult, ConsumerRunningInfo, ProcessQueueInfo,
};
use crate::remoting::protocol::codes::request_code;
use crate::remoting::protocol::ext_fields::StringMap;
use crate::remoting::protocol::extra_info;
use crate::remoting::protocol::headers::ConsumerSendMsgBackRequestHeader;
use crate::remoting::protocol::heartbeat::{
    ConsumeFromWhere, ConsumeType, FilterAPI, MessageModel, SubscriptionData,
};
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::remoting::protocol::remoting_command::RemotingCommand;
use crate::remoting::rpchook::RPCHook;
use crate::{bail, rmq_debug, rmq_error, rmq_info, rmq_warn};

/// POP 消费失败的延迟梯度（**秒**），逐项对应 Java
/// `DefaultMQPushConsumerImpl.popDelayLevel`。
pub const POP_DELAY_LEVEL: [i32; 16] = [
    10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
];

/// Java `DefaultMQPushConsumerImpl.MIN/MAX_POP_INVISIBLE_TIME`：超出范围一律回落到 60000。
pub const MIN_POP_INVISIBLE_TIME: i64 = 5000;
/// 见 [`MIN_POP_INVISIBLE_TIME`]。
pub const MAX_POP_INVISIBLE_TIME: i64 = 300000;
/// 不可见时间越界时的回落值（Java `POP_HIDDEN_TIME_MAX/DEFAULT`）。
pub const DEFAULT_POP_INVISIBLE_TIME: i64 = 60_000;

// ================================================================ 队列与过滤工具

/// 队列排序键，语义对齐 Java `MessageQueue.compareTo`：topic → brokerName → queueId。
///
/// Java 的 rebalance 会先把 `mqAll`/`cidAll` 排序再分配；顺序不一致会让不同实例算出
/// 不同的分配结果（同一队列被两个实例同时消费）。
pub fn mq_sort_key(mq: &MessageQueue) -> (String, String, i32) {
    (mq.topic.clone(), mq.broker_name.clone(), mq.queue_id)
}

/// 把队列按 [`mq_sort_key`] 排序（Java `Collections.sort(candds)`）。
pub fn sort_mqs(mqs: &mut [MessageQueue]) {
    mqs.sort_by_key(mq_sort_key);
}

/// 客户端二次 tag 过滤（对应 Java `PullAPIWrapper.processPullResult:113-122`）。
///
/// broker 侧是按 tag 的**哈希（codeSet）**过滤的，存在哈希碰撞误放；Java 因此让客户端
/// 再按字符串核一遍。守卫 `!tagsSet.isEmpty() && !isClassFilterMode` 意味着：
/// 订阅 `"*"`（SUB_ALL）时不过滤 —— 所以 `SubscriptionData::new` 对 SUB_ALL
/// 必须保持 `tags_set` 为空。
pub fn client_side_tag_filter(
    sub: Option<&SubscriptionData>,
    msgs: Vec<MessageExt>,
) -> Vec<MessageExt> {
    let Some(sub) = sub else {
        return msgs;
    };
    if msgs.is_empty() || sub.tags_set.is_empty() || sub.class_filter_mode {
        return msgs;
    }
    msgs.into_iter()
        .filter(|m| m.get_tags().is_some_and(|t| sub.tags_set.iter().any(|s| s == t)))
        .collect()
}

/// 投递前过滤 = 客户端二次 tag 过滤 + FilterMessageHook（拉取/POP/pull 三处共用）。
///
/// 钩子拿到的是**可变的** `msg_list`；被摘掉的消息由调用方决定处置方式：
/// 拉取路径 = 静默跳过（位点照常推进，不 ack，Java 亦然）；
/// POP 路径 = 必须立刻 ack，否则 `invisibleTime` 到期后会复活重投。
pub fn filter_messages_for_delivery(
    consumer_group: &str,
    hooks: &FilterMessageHookList,
    mq: &MessageQueue,
    sub: Option<&SubscriptionData>,
    msgs: Vec<MessageExt>,
) -> Vec<MessageExt> {
    let mut out = client_side_tag_filter(sub, msgs);
    if !out.is_empty() && hooks.has_hooks() {
        let mut context = FilterMessageContext::new(consumer_group, Some(out), Some(mq.clone()));
        // unit_mode 恒 false：本项目没有 unit mode（Java isUnitMode() 亦恒 false）
        crate::client::hook::execute_filter_hooks(hooks, &mut context);
        out = context.msg_list;
    }
    out
}

/// 把位点表按「队列排序键」稳定地投进 `BTreeMap`（Python 用 dict 插入序，
/// 这里显式排序，保证 `get_consumer_status`/221 响应在跨语言比对时可预测）。
pub fn offset_table_to_sorted(
    table: impl IntoIterator<Item = (MessageQueue, i64)>,
) -> BTreeMap<(String, String, i32), i64> {
    let mut out = BTreeMap::new();
    for (mq, offset) in table {
        out.insert(mq_sort_key(&mq), offset);
    }
    out
}

// ================================================================ MessageSelector

/// 表达式类型常量的来源（`TAG` / `SQL92` / `CLASS_FILTER`）。
///
/// 与 [`SubscriptionData::expression_type`] 同口径：存的是**字符串**，不是枚举
/// —— Java 的 `ExpressionType` 本身就是几个常量。
pub use crate::remoting::protocol::heartbeat::ExpressionType;

/// 消息选择器（对应 Java `MessageSelector`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSelector {
    /// Java `type`（`ExpressionType::TAG` / `SQL92`）。
    pub selector_type: String,
    /// Java `expression`。
    pub expression: String,
}

impl MessageSelector {
    /// Python `MessageSelector(selector_type, expression)`。
    pub fn new(selector_type: &str, expression: &str) -> MessageSelector {
        MessageSelector {
            selector_type: selector_type.to_string(),
            expression: expression.to_string(),
        }
    }

    /// Python `MessageSelector.by_tag`。
    pub fn by_tag(tag: &str) -> MessageSelector {
        MessageSelector::new(ExpressionType::TAG, tag)
    }

    /// Python `MessageSelector.by_sql`。
    pub fn by_sql(sql: &str) -> MessageSelector {
        MessageSelector::new(ExpressionType::SQL92, sql)
    }

    /// 订阅表达式（Python 直接把 `selector.expression` 传给 `subscribe`）。
    pub fn sub_expression(&self) -> &str {
        &self.expression
    }
}

// ================================================================ MessageQueueListener

/// 队列变更监听器（对应 Java `MessageQueueListener`）。
///
/// Python 只在 `DefaultMQPullConsumer` / `DefaultLitePullConsumer` 上持有该监听器
/// 字段，且只有 LitePull 真的会回调（`consumer.py:2705`）；push 消费者没有这个接缝，
/// 与 Python 一致。接缝实现见 [`crate::client::pull_consumer`]。
pub trait MessageQueueListener: Send + Sync {
    /// Java `messageQueueChanged`。`mq_all` 是该 topic 的全部队列，
    /// `mq_divided` 是分给本实例的队列。
    fn message_queue_changed(
        &self,
        topic: &str,
        mq_all: &[MessageQueue],
        mq_divided: &[MessageQueue],
    );
}

// ================================================================ PopProcessQueue

/// POP 模式的队列状态（对应
/// `org.apache.rocketmq.client.impl.consumer.PopProcessQueue`）。
///
/// 与 pull 模式的过程队列不同，POP **没有「已拉未消费」缓冲**：消息一弹出就交给
/// 消费线程，确认靠 ack。这里只跟踪两件事：
///
/// - `wait_ack_counter`：已弹出但还没 ack / 还没延长不可见时间的条数，用于流控；
/// - `dropped`：队列是否已被 rebalance 撤走（撤走后本批消息不再消费、也不 ack，
///   交给 `invisibleTime` 到期后 broker 自动复活重投）。
#[derive(Debug, Default)]
pub struct PopProcessQueue {
    wait_ack_counter: AtomicI32,
    dropped: AtomicBool,
    /// Python `last_pop_timestamp = time.time()`：**只写不读**（`consumer.py:276,1309`）。
    /// Java 用它做过期清理，本项目没有那条清理路径，这里保留字段仅为了
    /// 「最近一次弹出的时刻」在诊断时可见，单位换成毫秒。
    pub last_pop_timestamp: AtomicI64,
}

impl PopProcessQueue {
    /// Python `PopProcessQueue()`。
    pub fn new() -> Arc<PopProcessQueue> {
        Arc::new(PopProcessQueue {
            last_pop_timestamp: AtomicI64::new(
                crate::common::util_all::current_time_millis(),
            ),
            ..Default::default()
        })
    }

    /// Java `incFoundMsg`。
    pub fn inc_found_msg(&self, count: i32) {
        self.wait_ack_counter.fetch_add(count, Ordering::SeqCst);
    }

    /// Java `decFoundMsg(-msgs.size())`：Python 按「减多少」理解（同名方法直接
    /// `+= count`），这里保持 Python 口径 —— 传正数即减少等待 ack 的条数。
    pub fn dec_found_msg(&self, count: i32) {
        self.wait_ack_counter.fetch_add(-count, Ordering::SeqCst);
    }

    /// Java `ack()`：递减并返回递减后的值。
    pub fn ack(&self) -> i32 {
        self.wait_ack_counter.fetch_add(-1, Ordering::SeqCst) - 1
    }

    /// Java `getWaitAckCounter()`。
    pub fn wait_ack_count(&self) -> i32 {
        self.wait_ack_counter.load(Ordering::SeqCst)
    }

    /// Java `isDropped()`。
    pub fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::SeqCst)
    }

    /// Java `setDropped`。
    pub fn set_dropped(&self, dropped: bool) {
        self.dropped.store(dropped, Ordering::SeqCst);
    }

    /// 记一次弹出时间（毫秒），对齐 Python `pq.last_pop_timestamp = time.time()`。
    pub fn touch(&self, timestamp_millis: i64) {
        self.last_pop_timestamp.store(timestamp_millis, Ordering::SeqCst);
    }
}

/// 监听器（对应 Java `MessageListener` 的两个实现）。
///
/// Python 用 `isinstance(listener, MessageListenerOrderly)` 判定顺序/并发
/// （`consumer.py:2020` 的 `_is_orderly`）；Rust 没有运行时类型探测，这里用枚举标签
/// 表达同一件事 —— 只是**声明方式**不同，判定口径与 Python 完全一致。
#[derive(Clone)]
pub enum MessageListener {
    /// Java `MessageListenerConcurrently`（Python 同名类）。
    Concurrently(Arc<dyn MessageListenerConcurrently>),
    /// Java `MessageListenerOrderly`（Python 同名类）。
    Orderly(Arc<dyn MessageListenerOrderly>),
}

impl MessageListener {
    /// Python `_is_orderly`。
    pub fn is_orderly(&self) -> bool {
        matches!(self, MessageListener::Orderly(_))
    }
}

impl std::fmt::Debug for MessageListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.is_orderly() {
            "MessageListener::Orderly"
        } else {
            "MessageListener::Concurrently"
        })
    }
}

/// 消费者配置（对应 Python `DefaultMQPushConsumer.__init__` 里那批平铺属性）。
///
/// Python 的属性可以随时直接赋值，这里聚成一个 `pub` 字段的结构体，由
/// [`DefaultMQPushConsumer::config`] / [`DefaultMQPushConsumer::update_config`] 读写，
/// 语义等价（生产者侧同一手法）。默认值逐项照抄 `consumer.py:314-438`。
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Python `consumer_group`。
    pub consumer_group: String,
    /// Python `namespace`（Java `ClientConfig#namespace`）。
    pub namespace: String,
    /// Python `instance_name`，默认 `"DEFAULT"`。
    pub instance_name: String,
    /// Python `client_id`；`None` 时 `start()` 里现造。
    pub client_id: Option<String>,
    /// Python `name_server_addrs`。
    pub name_server_addrs: Vec<String>,
    /// Python `tls_enable`；`None` = 交给环境变量 `ROCKETMQ_TLS_ENABLE`。
    pub tls_enable: Option<bool>,
    /// Python `message_model`（[`MessageModel`] 里的常量）。
    pub message_model: String,
    /// Python `consume_from_where`（[`ConsumeFromWhere`] 里的常量）。
    pub consume_from_where: String,
    /// Python `consume_timestamp`：`yyyyMMddHHmmss`，默认 30 分钟前。
    pub consume_timestamp: String,
    /// Java `consumeThreadMin` = 20。
    pub consume_thread_min: i32,
    /// Java `consumeThreadMax` = 64。
    pub consume_thread_max: i32,
    /// Java `adjustThreadPoolNumsThreshold` = 100000。
    pub adjust_thread_pool_nums_threshold: i64,
    /// Java `consumeConcurrentlyMaxSpan` = 2000。
    pub consume_concurrently_max_span: i64,
    /// Java `pullThresholdForQueue` = 1000（**条数**）。
    pub pull_threshold_for_queue: i32,
    /// Java `pullThresholdSizeForQueue` = 100（**MiB**）。
    pub pull_threshold_size_for_queue: i32,
    /// Java `pullThresholdForTopic` = -1（关闭）。
    pub pull_threshold_for_topic: i32,
    /// Java `pullThresholdSizeForTopic` = -1（关闭）。
    pub pull_threshold_size_for_topic: i32,
    /// Java `pullInterval` = 0（本项目拉取循环未使用，与 Python 一致）。
    pub pull_interval: i64,
    /// Python `pull_timeout_millis` = 30000。
    pub pull_timeout_millis: i64,
    /// Python `pull_suspend_timeout_millis` = 20000（长轮询挂起时长）。
    pub pull_suspend_timeout_millis: i64,
    /// Java `consumeMessageBatchMaxSize` = 1。
    pub consume_message_batch_max_size: i32,
    /// Java `pullBatchSize` = 32。
    pub pull_batch_size: i32,
    /// Java `pullBatchSizeInBytes`? Python 用 256 KiB 作为 `maxMsgBytes`。
    pub pull_batch_size_in_bytes: i32,
    /// Java `maxReconsumeTimes` = -1（交给 broker，默认 16 次后转 `%DLQ%`）。
    pub max_reconsume_times: i32,
    /// Java `suspendCurrentQueueTimeMillis` = 1000。
    pub suspend_current_queue_time_millis: i64,
    /// Java `consumeTimeout`（**分钟**）= 15，只用于 `ConsumeReturnType::TimeOut` 判定。
    pub consume_timeout: i64,
    /// Java `clientRebalance` = true（本项目恒走客户端 rebalance，字段仅为对齐形状）。
    pub client_rebalance: bool,
    /// Java `heartbeatBrokerInterval` = 30000。
    pub heartbeat_interval_millis: i64,
    /// Python `heartbeat_enabled`。
    pub heartbeat_enabled: bool,
    /// Python `pop_mode`：true 走 POP + ack，false 走长轮询 + 位点。
    pub pop_mode: bool,
    /// Java `popInvisibleTime` = 60000。
    pub pop_invisible_time: i64,
    /// Java `popBatchNums` = 32（broker 侧 >32 会拒）。
    pub pop_batch_nums: i32,
    /// Java `popThresholdForQueue` = 96。
    pub pop_threshold_for_queue: i32,
    /// Java `popDelayLevel`（**秒**），见 [`POP_DELAY_LEVEL`]。
    pub pop_delay_level: Vec<i32>,
    /// POP 长轮询挂起时长；0 = 短轮询。⚠ 非 0 时请求超时必须大于它。
    pub pop_poll_time_millis: i64,
    /// POP 请求超时。
    pub pop_timeout_millis: i64,
    /// Java `enableTrace`（Python `enable_trace`）。
    pub enable_trace: bool,
    /// Java `customizedTraceTopic`；`None` → `RMQ_SYS_TRACE_TOPIC`。
    pub trace_topic: Option<String>,
    /// Python `trace_msg_batch_num` = 10。
    pub trace_msg_batch_num: i32,
}

impl Default for ConsumerConfig {
    fn default() -> ConsumerConfig {
        ConsumerConfig {
            consumer_group: MixAll::DEFAULT_CONSUMER_GROUP.to_string(),
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            client_id: None,
            name_server_addrs: Vec::new(),
            tls_enable: None,
            message_model: MessageModel::CLUSTERING.to_string(),
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string(),
            consume_timestamp: default_consume_timestamp(),
            consume_thread_min: 20,
            consume_thread_max: 64,
            adjust_thread_pool_nums_threshold: 100_000,
            consume_concurrently_max_span: 2000,
            pull_threshold_for_queue: 1000,
            pull_threshold_size_for_queue: 100,
            pull_threshold_for_topic: -1,
            pull_threshold_size_for_topic: -1,
            pull_interval: 0,
            pull_timeout_millis: 30_000,
            pull_suspend_timeout_millis: 20_000,
            consume_message_batch_max_size: 1,
            pull_batch_size: 32,
            pull_batch_size_in_bytes: 256 * 1024,
            max_reconsume_times: -1,
            suspend_current_queue_time_millis: 1000,
            consume_timeout: 15,
            client_rebalance: true,
            heartbeat_interval_millis: 30_000,
            heartbeat_enabled: true,
            pop_mode: false,
            pop_invisible_time: 60_000,
            pop_batch_nums: 32,
            pop_threshold_for_queue: 96,
            pop_delay_level: POP_DELAY_LEVEL.to_vec(),
            pop_poll_time_millis: 15_000,
            pop_timeout_millis: 25_000,
            enable_trace: false,
            trace_topic: None,
            trace_msg_batch_num: 10,
        }
    }
}

/// Python `instance_name` 默认值（与生产者同名常量，但两处各自持有，避免跨模块耦合）。
pub const DEFAULT_INSTANCE_NAME: &str = "DEFAULT";
/// Java `Short.MAX_VALUE`：`updateCorePoolSize` 的第二道守卫。
pub const SHORT_MAX_VALUE: i32 = 32767;

/// Python `time.strftime("%Y%m%d%H%M%S", time.localtime(time.time() - 30*60))`。
pub(crate) fn default_consume_timestamp() -> String {
    let now = chrono::Local::now();
    let past = now - chrono::Duration::minutes(30);
    past.format("%Y%m%d%H%M%S").to_string()
}

/// Java `UtilAll.parseDate(ts, UtilAll.YYYYMMDDHHMMSS)`：该字段**只**是 14 位本地墙钟日期。
///
/// 与 Python 同用**本地时区**（`time.mktime` / `chrono::Local`），否则
/// `CONSUME_FROM_TIMESTAMP` 的起点会差一个时区。解析不了必须硬失败（Java 在 start 时抛
/// `consumeTimestamp is invalid`）——静默回落到「现在 - 30 分钟」会让起点错位无人察觉。
pub(crate) fn consume_timestamp_millis(text: &str) -> Result<i64> {
    use chrono::TimeZone;
    chrono::NaiveDateTime::parse_from_str(text, "%Y%m%d%H%M%S")
        .ok()
        .and_then(|naive| chrono::Local.from_local_datetime(&naive).single())
        .map(|dt| dt.timestamp_millis())
        .ok_or_else(|| {
            Error::client(format!(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received {text}"
            ))
        })
}

// ================================================================ 内部可变状态

/// Python 里那批以 `self._lock` 保护的字典（`_offset_table` / `_pending` /
/// `_consume_offsets` / `_mq_map` / `_assigned` …）。
///
/// 键一律是 [`DefaultMQPushConsumer::mq_key`] 那串 `topic+brokerName+queueId`，
/// 与 Python 逐字一致；用 `BTreeMap` 只为让遍历顺序确定（Python 是 dict 插入序，
/// 会影响 `consumerRunningInfo` 的报文顺序，见模块头差异说明）。
#[derive(Default)]
struct State {
    /// Python `subscription_data: Dict[topic, SubscriptionData]`。
    subscription_data: Vec<(String, SubscriptionData)>,
    /// Python `_assigned`（Java `ProcessQueueTable` 的键集）。
    assigned: Vec<MessageQueue>,
    /// Python `_offset_table`：**拉取游标**（nextBeginOffset）。
    offset_table: BTreeMap<String, i64>,
    /// Python `_consume_offsets`：**已消费位点**，周期持久化到 broker 的是这张。
    consume_offsets: BTreeMap<String, i64>,
    /// Python `_pending`：已拉未消费缓冲（Java `ProcessQueue`）。
    pending: BTreeMap<String, VecDeque<MessageExt>>,
    /// Python `_mq_map`：队列 key -> MessageQueue。
    mq_map: BTreeMap<String, MessageQueue>,
    /// Python `_lock_ok`：顺序消费下 broker 已确认锁定的队列。
    lock_ok: BTreeSet<String>,
    /// Python `_msg_acc_cnt_table`：Java `ProcessQueue.msgAccCnt`。
    msg_acc_cnt: BTreeMap<String, i64>,
    /// Python `_queue_threads`：每队列拉取/POP 循环的**归属令牌**。
    ///
    /// Python 存线程对象并用 `is threading.current_thread()` 判归属；Rust 的循环是
    /// tokio 任务，一个任务可能在线程间迁移，故换成单调递增令牌，语义等价。
    queue_owners: BTreeMap<String, u64>,
    /// Python `_pop_queues`：队列 key -> [`PopProcessQueue`]。
    pop_queues: BTreeMap<String, Arc<PopProcessQueue>>,
}

impl State {
    fn subscription(&self, topic: &str) -> Option<&SubscriptionData> {
        self.subscription_data
            .iter()
            .find(|(k, _)| k == topic)
            .map(|(_, v)| v)
    }
}

struct Inner {
    cfg: RwLock<ConsumerConfig>,
    state: Mutex<State>,
    client: Mutex<Option<MQClientInstance>>,
    started: AtomicBool,
    runtime: OnceLock<tokio::runtime::Handle>,
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    listener: Mutex<Option<MessageListener>>,
    strategy: RwLock<Arc<dyn AllocateMessageQueueStrategy>>,
    consume_hooks: ConsumeMessageHookList,
    filter_hooks: FilterMessageHookList,
    stats: Mutex<Option<Arc<ConsumerStatsManager>>>,
    pop_executor: Mutex<Option<ConsumeExecutor>>,
    trace: Mutex<Option<Arc<dyn TraceDispatcherChannel>>>,
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
    /// Python `_heartbeat_count`。
    heartbeat_count: AtomicUsize,
    /// Python `_flow_control_triggered`（普通属性，无锁自增）。
    flow_control_triggered: AtomicUsize,
    /// Python `_core_pool_size`（声明值）。
    core_pool_size: AtomicI32,
    /// Python `_owns_consume_executor`：本实现不支持外部注入执行器，恒 true，
    /// 保留字段是为了让 `update_core_pool_size` 的守卫形状与 Java 一致。
    owns_consume_executor: AtomicBool,
    /// Python `_start_time`（毫秒）。
    start_time_millis: AtomicI64,
    /// Python `_rebalance_now`（`threading.Event`）。
    ///
    /// ⚠ 用「标志位 + `Notify`」而不是裸 `Notify::notify_waiters()`：后者只叫醒
    /// **当下**在等的任务，通知落在两轮之间就会丢；`Event` 是持久的，
    /// 标志位补上这个语义。
    rebalance_now: AtomicBool,
    rebalance_signal: Notify,
    /// [`State::queue_owners`] 的令牌发号器。
    next_token: AtomicU64,
}

impl Drop for Inner {
    /// 忘记 `shutdown()` 也不能留下还在跑的循环（见生产者模块头同名差异）。
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        for task in self
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
    }
}

/// 取锁（Python 的 `with self._lock`）；中毒时照用，理由同 `consume_executor::lock`。
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn read_cfg(inner: &Inner) -> ConsumerConfig {
    inner
        .cfg
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

fn require_client(inner: &Inner) -> Result<MQClientInstance> {
    if !inner.started.load(Ordering::Acquire) {
        return Err(Error::client("consumer not started, call start() first"));
    }
    lock(&inner.client)
        .clone()
        .ok_or_else(|| Error::client("consumer not started, call start() first"))
}

/// 队列 key：Python `_mq_key`（`"%s%s%d" % (topic, brokerName, queueId)`，无分隔符）。
pub fn mq_key(mq: &MessageQueue) -> String {
    format!("{}{}{}", mq.topic, mq.broker_name, mq.queue_id)
}

// ================================================================ DefaultMQPushConsumer

/// 推模式消费者（对应 Java `DefaultMQPushConsumer` + `DefaultMQPushConsumerImpl`，
/// 移植自 Python `consumer.DefaultMQPushConsumer`）。
///
/// 生命周期：`subscribe(..)` → [`start`](Self::start) → 后台重平衡 + 消费 →
/// [`shutdown`](Self::shutdown)。克隆得到的副本共享同一份状态（模块头差异 2）。
#[derive(Clone)]
pub struct DefaultMQPushConsumer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for DefaultMQPushConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cfg = read_cfg(&self.inner);
        f.debug_struct("DefaultMQPushConsumer")
            .field("consumer_group", &cfg.consumer_group)
            .field("namespace", &cfg.namespace)
            .field("instance_name", &cfg.instance_name)
            .field("client_id", &cfg.client_id)
            .field("name_server_addrs", &cfg.name_server_addrs)
            .field("message_model", &cfg.message_model)
            .field("pop_mode", &cfg.pop_mode)
            .field("started", &self.inner.started.load(Ordering::Acquire))
            .field("subscriptions", &lock(&self.inner.state).subscription_data.len())
            .finish()
    }
}

impl DefaultMQPushConsumer {
    /// Python `DefaultMQPushConsumer(consumer_group)`；组名空白则报错
    /// （`MQClientException("consumerGroup is empty")`）。
    pub fn new(consumer_group: &str) -> Result<DefaultMQPushConsumer> {
        let cfg = ConsumerConfig {
            consumer_group: consumer_group.to_string(),
            ..Default::default()
        };
        DefaultMQPushConsumer::with_config(cfg)
    }

    /// Python `DefaultMQPushConsumer(consumer_group, rpc_hook=..)`。
    pub fn with_rpc_hook(
        consumer_group: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Result<DefaultMQPushConsumer> {
        let consumer = DefaultMQPushConsumer::new(consumer_group)?;
        consumer.set_rpc_hook(rpc_hook);
        Ok(consumer)
    }

    /// 直接以一份完整配置构造（Python 靠逐个赋属性，这里等价）。
    pub fn with_config(cfg: ConsumerConfig) -> Result<DefaultMQPushConsumer> {
        if cfg.consumer_group.trim().is_empty() {
            bail!("consumerGroup is empty");
        }
        let inner = Arc::new(Inner {
            cfg: RwLock::new(cfg),
            state: Mutex::new(State::default()),
            client: Mutex::new(None),
            started: AtomicBool::new(false),
            runtime: OnceLock::new(),
            stop: watch::channel(false).0,
            tasks: Mutex::new(Vec::new()),
            listener: Mutex::new(None),
            strategy: RwLock::new(Arc::new(AllocateMessageQueueAveragely)),
            consume_hooks: ConsumeMessageHookList::new(),
            filter_hooks: FilterMessageHookList::new(),
            stats: Mutex::new(None),
            pop_executor: Mutex::new(None),
            trace: Mutex::new(None),
            rpc_hook: RwLock::new(None),
            heartbeat_count: AtomicUsize::new(0),
            flow_control_triggered: AtomicUsize::new(0),
            core_pool_size: AtomicI32::new(0),
            owns_consume_executor: AtomicBool::new(true),
            start_time_millis: AtomicI64::new(0),
            rebalance_now: AtomicBool::new(false),
            rebalance_signal: Notify::new(),
            next_token: AtomicU64::new(1),
        });
        let consumer = DefaultMQPushConsumer { inner };
        // Python `__init__` 里 `self._core_pool_size = self.consume_thread_min`
        let min = consumer.config().consume_thread_min;
        consumer.inner.core_pool_size.store(min.max(1), Ordering::SeqCst);
        Ok(consumer)
    }

    /// 当前配置快照（Python 直接读属性）。
    pub fn config(&self) -> ConsumerConfig {
        read_cfg(&self.inner)
    }

    /// 改配置（Python 的直接赋属性）。`start()` 之后除少数运行期可调项外应拒绝。
    pub fn update_config(&self, f: impl FnOnce(&mut ConsumerConfig)) {
        {
            let mut w = self
                .inner
                .cfg
                .write()
                .unwrap_or_else(|e| e.into_inner());
            f(&mut w);
        }
        // Python 的 set_consume_thread_* 会顺带同步声明的 core size
        let cfg = self.config();
        let cur = self.inner.core_pool_size.load(Ordering::SeqCst);
        if cur < cfg.consume_thread_min {
            self.inner
                .core_pool_size
                .store(cfg.consume_thread_min, Ordering::SeqCst);
        }
        self.apply_core_pool_size();
    }

    pub fn consumer_group(&self) -> String {
        read_cfg(&self.inner).consumer_group
    }

    pub fn client_id(&self) -> String {
        read_cfg(&self.inner).client_id.clone().unwrap_or_default()
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    pub fn set_namesrv_addr(&self, addr: &str) {
        let addrs = addr
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect::<Vec<String>>();
        self.update_config(|c| c.name_server_addrs = addrs);
    }

    pub fn set_name_server_addresses(&self, addrs: &[String]) {
        let addrs = addrs.to_vec();
        self.update_config(|c| c.name_server_addrs = addrs);
    }

    pub fn set_instance_name(&self, name: &str) {
        let name = name.to_string();
        self.update_config(|c| c.instance_name = name);
    }

    pub fn set_message_listener(&self, listener: MessageListener) {
        *lock(&self.inner.listener) = Some(listener);
    }

    /// Python `set_message_listener` 的并发消费便捷入口。
    pub fn set_message_listener_concurrently(&self, listener: Arc<dyn MessageListenerConcurrently>) {
        self.set_message_listener(MessageListener::Concurrently(listener));
    }

    /// Python `set_message_listener` 的顺序消费便捷入口。
    pub fn set_message_listener_orderly(&self, listener: Arc<dyn MessageListenerOrderly>) {
        self.set_message_listener(MessageListener::Orderly(listener));
    }

    pub fn set_allocate_message_queue_strategy(
        &self,
        strategy: Arc<dyn AllocateMessageQueueStrategy>,
    ) {
        *self
            .inner
            .strategy
            .write()
            .unwrap_or_else(|e| e.into_inner()) = strategy;
    }

    /// 当前队列分配策略（对应 Java `DefaultMQPushConsumer.getAllocateMessageQueueStrategy`）。
    pub fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy> {
        self.inner
            .strategy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn set_rpc_hook(&self, hook: Option<Arc<dyn RPCHook>>) {
        *self
            .inner
            .rpc_hook
            .write()
            .unwrap_or_else(|e| e.into_inner()) = hook;
    }

    /// 注入轨迹分发器（模块头差异 5）。
    pub fn set_trace_dispatcher(&self, dispatcher: Option<Arc<dyn TraceDispatcherChannel>>) {
        *lock(&self.inner.trace) = dispatcher;
    }

    pub fn trace_dispatcher(&self) -> Option<Arc<dyn TraceDispatcherChannel>> {
        lock(&self.inner.trace).clone()
    }

    pub fn register_consume_message_hook(&self, hook: Arc<dyn ConsumeMessageHook>) {
        self.inner.consume_hooks.register(hook);
    }

    pub fn has_consume_message_hook(&self) -> bool {
        self.inner.consume_hooks.has_hooks()
    }

    pub fn register_filter_message_hook(
        &self,
        hook: Arc<dyn crate::client::hook::FilterMessageHook>,
    ) {
        self.inner.filter_hooks.register(hook);
    }

    pub fn has_filter_message_hook(&self) -> bool {
        self.inner.filter_hooks.has_hooks()
    }

    // ---------------- 订阅 ----------------

    /// Python `subscribe(topic, sub_expression="*")`。
    pub fn subscribe(&self, topic: &str, sub_expression: &str) -> Result<()> {
        self.assert_not_started()?;
        let topic = self.with_namespace(topic);
        let sub = FilterAPI::build_subscription_data(&topic, Some(sub_expression))?;
        self.put_subscription(topic, sub);
        Ok(())
    }

    /// Python `subscribe(topic, "*")`（订阅全部）。
    pub fn subscribe_all(&self, topic: &str) -> Result<()> {
        self.subscribe(topic, "*")
    }

    /// Python `subscribe_with_selector`。
    ///
    /// 对齐 Java `FilterAPI.build(topic, subString, type)`：
    /// TAG（或 type 为空）走 `buildSubscriptionData` 填 `tagsSet`/`codeSet`；
    /// SQL92 / CLASS_FILTER 只设 topic/subString/expressionType，两个集合留空。
    pub fn subscribe_with_selector(&self, topic: &str, selector: &MessageSelector) -> Result<()> {
        self.assert_not_started()?;
        let topic = self.with_namespace(topic);
        let sub = if selector.selector_type == ExpressionType::TAG {
            let mut s = FilterAPI::build_subscription_data(&topic, Some(&selector.expression))?;
            s.expression_type = selector.selector_type.clone();
            s
        } else {
            if selector.expression.is_empty() {
                bail!("Expression can't be null! {}", selector.selector_type);
            }
            SubscriptionData {
                topic: topic.clone(),
                sub_string: selector.expression.clone(),
                expression_type: selector.selector_type.clone(),
                ..Default::default()
            }
        };
        self.put_subscription(topic, sub);
        Ok(())
    }

    /// Python `unsubscribe`。
    pub fn unsubscribe(&self, topic: &str) {
        let topic = self.with_namespace(topic);
        let mut state = lock(&self.inner.state);
        state.subscription_data.retain(|(k, _)| *k != topic);
    }

    fn put_subscription(&self, topic: String, sub: SubscriptionData) {
        let mut state = lock(&self.inner.state);
        match state.subscription_data.iter_mut().find(|(k, _)| *k == topic) {
            Some(slot) => slot.1 = sub,
            None => state.subscription_data.push((topic, sub)),
        }
    }

    /// 当前订阅表快照（Python 直接读 `subscription_data`）。
    pub fn subscriptions(&self) -> Vec<SubscriptionData> {
        lock(&self.inner.state)
            .subscription_data
            .iter()
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Python `_with_namespace`（Java `DefaultMQPushConsumer.subscribe(withNamespace(topic))`）。
    fn with_namespace(&self, topic: &str) -> String {
        let namespace = read_cfg(&self.inner).namespace;
        if namespace.is_empty() {
            topic.to_string()
        } else {
            NamespaceUtil::wrap_namespace(&namespace, topic)
        }
    }

    fn assert_not_started(&self) -> Result<()> {
        if self.is_started() {
            return Err(Error::client(
                "consumer already started, cannot change configuration",
            ));
        }
        Ok(())
    }

    // ---------------- 生命周期 ----------------

    /// Python `start()`（对应 Java `DefaultMQPushConsumerImpl.start`）。
    ///
    /// 顺序与 Python 逐行对齐：校验 → 拼命名空间 → 建并启动实例 → 注册消费者 →
    /// 自动订阅 `%RETRY%` → 注册 NOTIFY_CONSUMER_IDS_CHANGED(40) → 建 POP 执行器 →
    /// 拉路由 → **同步**首轮心跳 → **同步**首轮重平衡 → 起各循环 → 轨迹分发器。
    ///
    /// 两处刻意的实现差别（行为不变）：
    /// 1. Python 的心跳报文由消费者自己拼（`_build_heartbeat`）；这里用
    ///    [`MQClientInstance::send_heartbeat_to_all_broker`]，它按 Java 的口径从
    ///    注册表拼全量 `ConsumerData`。因为 `register_consumer` 在它之前，
    ///    本消费者一定在报文里。
    /// 2. Python 用 `threading.Thread`；这里是 tokio 任务，句柄缺失时（无运行时）
    ///    只告警不报错，与生产者一致。
    pub async fn start(&self) -> Result<()> {
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let cfg = self.config();
        // 消费组拼命名空间必须在算重试主题之前（Java DefaultMQPushConsumer.start:763）。
        let group = if cfg.namespace.is_empty() {
            cfg.consumer_group.clone()
        } else {
            NamespaceUtil::wrap_namespace(&cfg.namespace, &cfg.consumer_group)
        };
        // 对应 Java DefaultMQPushConsumerImpl.checkConfig(:1026)：先 Validators.check_group
        // （blank / 120 长度 / 字符表），再挡 DEFAULT_CONSUMER —— 共用默认组会让 broker 侧
        // 的订阅关系判定把两组混在一起，回投与重平衡都错乱。
        // ⚠ checkConfig 是 Java start() 的第一步，所以这里领先于地址/订阅/监听器检查：
        // 组名非法时既不碰网络，也不该被后面的错误盖掉真正原因。
        if let Err(e) = validators::check_group(&group) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        if group == MixAll::DEFAULT_CONSUMER_GROUP {
            self.inner.started.store(false, Ordering::Release);
            return Err(Error::client(
                "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.",
            ));
        }
        if cfg.name_server_addrs.is_empty() && !DefaultTopAddressing::is_configured() {
            self.inner.started.store(false, Ordering::Release);
            bail!("name server address is not set");
        }
        {
            let state = lock(&self.inner.state);
            if state.subscription_data.is_empty() {
                self.inner.started.store(false, Ordering::Release);
                bail!("subscription is not set, call subscribe() first");
            }
        }
        if !lock(&self.inner.listener).is_some() {
            self.inner.started.store(false, Ordering::Release);
            bail!("message listener is not set");
        }

        let client_id = cfg.client_id.clone().unwrap_or_else(|| {
            // Python: "%s@%s" % (instance_name, strftime("%Y%m%d%H%M%S"))
            format!(
                "{}@{}",
                cfg.instance_name,
                chrono::Local::now().format("%Y%m%d%H%M%S")
            )
        });
        self.update_config(|c| {
            c.consumer_group = group.clone();
            c.client_id = Some(client_id.clone());
        });

        let instance_cfg = MQClientInstanceConfig {
            tls_enable: cfg.tls_enable,
            ..Default::default()
        };
        let client =
            MQClientInstance::create_mq_client_instance(&client_id, cfg.name_server_addrs.clone(), instance_cfg);
        if let Some(hook) = self
            .inner
            .rpc_hook
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            client.remoting_client().register_rpc_hook(hook);
        }
        if let Err(e) = client.start().await {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        *lock(&self.inner.client) = Some(client.clone());
        // 动态 name server：实例可能已从地址服务器拿到地址，回填（Python 同）。
        if cfg.name_server_addrs.is_empty() {
            let addrs = client.name_server_addrs();
            if !addrs.is_empty() {
                self.update_config(|c| c.name_server_addrs = addrs);
            }
        }
        // 消费统计（Java MQClientFactory.getConsumerStatsManager，实例级共享）
        *lock(&self.inner.stats) = Some(client.consumer_stats_manager().clone());

        // 集群模式自动订阅重试 topic（Java copySubscription）：SUB_ALL 下
        // tagsSet/codeSet 均为**空**，见 heartbeat::FilterAPI。
        if self.config().message_model != MessageModel::BROADCASTING {
            let retry_topic = MixAll::get_retry_topic(&group);
            let exist = lock(&self.inner.state)
                .subscription_data
                .iter()
                .any(|(k, _)| *k == retry_topic);
            if !exist {
                let sub = FilterAPI::build_subscription_data(&retry_topic, Some("*"))?;
                self.put_subscription(retry_topic, sub);
            }
        }

        // broker 的 NOTIFY_CONSUMER_IDS_CHANGED(40) 由 **实例** 统一处理并扇出回来
        // （`RegisteredConsumer::rebalance_immediately`，见 mq_client.rs），本消费者
        // 不自己注册处理器 —— 下面 register_consumer 之前必须先置好信号位，
        // 否则首轮 `do_rebalance` 会被自己的唤醒重复触发一次。
        self.inner
            .start_time_millis
            .store(current_time_millis(), Ordering::SeqCst);
        self.inner.rebalance_now.store(false, Ordering::SeqCst);
        let _ = self.inner.stop.send(false);
        client.register_consumer(&group, Arc::new(self.clone()));

        // POP 的消费执行器必须在 rebalance 之前建好：重平衡会立刻起每队列 POP 循环，
        // 循环拿到消息要投到这里（Java 的 consumeExecutor）。
        if self.config().pop_mode {
            let cfg = self.config();
            let executor = ConsumeExecutor::with_params(
                self.inner.core_pool_size.load(Ordering::SeqCst).max(1),
                cfg.consume_thread_max.max(1),
                Duration::from_secs(60),
                format!("rmq-popconsume-{}", cfg.consumer_group),
            );
            if let Some(handle) = self.runtime_handle() {
                let executor = executor.with_handle(&handle);
                *lock(&self.inner.pop_executor) = Some(executor);
            } else {
                *lock(&self.inner.pop_executor) = Some(executor);
            }
        }

        // 拉一次路由 → 同步首轮心跳 → 同步首轮重平衡 → 起循环（顺序照 Python）
        self.refresh_routes().await;
        let ok = client.send_heartbeat_to_all_broker(5000).await;
        if ok > 0 {
            self.inner
                .heartbeat_count
                .fetch_add(1, Ordering::SeqCst);
        }
        if let Err(e) = self.do_rebalance().await {
            rmq_warn!("initial rebalance failed: {e}");
        }
        self.spawn_loops();
        self.start_trace_dispatcher();
        Ok(())
    }

    /// Python `shutdown()`。
    pub fn shutdown(&self) {
        if self
            .inner
            .started
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let _ = self.inner.stop.send(true);
        {
            // POP：把所有队列标成 dropped，在途批次不再 ack（交给 broker 复活重投）
            let mut state = lock(&self.inner.state);
            if self.config().pop_mode {
                for pq in state.pop_queues.values() {
                    pq.set_dropped(true);
                }
                state.pop_queues.clear();
            }
            state.queue_owners.clear();
        }
        if let Some(executor) = lock(&self.inner.pop_executor).take() {
            executor.shutdown();
        }
        for task in self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
        // 退出前把已消费位点持久化一次（Java MQClientInstance.shutdown →
        // persistAllConsumerOffset）。必须在 started=false 之后仍能取到 client，
        // 所以这里直接用 client 引用而不是 require_client()。
        let client = lock(&self.inner.client).clone();
        if let Some(client) = client {
            let group = self.consumer_group();
            let broadcast = self.config().message_model == MessageModel::BROADCASTING;
            // 顺序消费清退时解锁队列（Java ConsumeMessageOrderlyService.shutdown → unlockAll）
            let orderly = lock(&self.inner.listener)
                .as_ref()
                .is_some_and(MessageListener::is_orderly);
            let items: Vec<(MessageQueue, i64)> = {
                let state = lock(&self.inner.state);
                state
                    .consume_offsets
                    .iter()
                    .filter_map(|(k, off)| state.mq_map.get(k).map(|mq| (mq.clone(), *off)))
                    .collect()
            };
            let locked_mqs: Vec<MessageQueue> = if orderly && !broadcast {
                lock(&self.inner.state).assigned.clone()
            } else {
                Vec::new()
            };
            if broadcast {
                if let Err(e) = save_local_offsets(&self.inner) {
                    rmq_debug!("persist local offsets on shutdown failed: {e}");
                }
            }
            // 先从本地注册表摘掉自己：表里没有消费者时本实例就不会再发心跳
            // （`send_heartbeat_to_all_broker` 空表直接返回 0）。Python 只在
            // start() 里 register，少了 Java `MQClientInstance#unregisterConsumer`。
            client.unregister_consumer(&group);
            if let Some(handle) = self.runtime_handle() {
                let client_id = self.client_id();
                handle.spawn(async move {
                    // 清退三步必须**串行**（刷位点 → 解锁 → 注销）：并发送的话注销可能
                    // 插在解锁之前，broker 已按 clientId 丢掉记录，位点和锁白刷。
                    if !broadcast {
                        for (mq, off) in &items {
                            if let Err(e) =
                                client.update_consumer_offset(&group, mq, *off, 5000, None).await
                            {
                                rmq_debug!("persist offset on shutdown failed for {mq:?}: {e}");
                            }
                        }
                        if !locked_mqs.is_empty() {
                            let _ = client
                                .unlock_batch_mq(&group, &client_id, &locked_mqs, 1000)
                                .await;
                        }
                    }
                    // 优雅注销（Java MQClientInstance.unregisterClient），不必等心跳超时
                    client
                        .unregister_client_all_brokers(&client_id, "", &group, 5000)
                        .await;
                    // 被 abort 的心跳可能已经在线上（`JoinHandle::abort` 要到下一个让出点
                    // 才生效），它落回 broker 会把本 clientId 重新塞进 ConsumerManager，同组
                    // 其它实例就得等 broker 的通道扫描（默认 120s）才接管。Java 靠「先停心跳
                    // 线程 → 注销 → 关连接」消掉这个窗口；这里连接可能和同 clientId 的其它
                    // 客户端共用，不能随手关，那就等一拍再补偿性注销一次。
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if client.find_consumer(&group).is_none() {
                        client
                            .unregister_client_all_brokers(&client_id, "", &group, 5000)
                            .await;
                    }
                });
            }
        }
        lock(&self.inner.client).take();
        self.inner.started.store(false, Ordering::Release);
        // 轨迹分发器最后关（flush 剩余轨迹，Java DefaultMQPushConsumer.shutdown:794）
        if let Some(dispatcher) = self.trace_dispatcher() {
            dispatcher.shutdown();
        }
    }

    fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        if let Some(handle) = self.inner.runtime.get() {
            return Some(handle.clone());
        }
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let _ = self.inner.runtime.set(handle.clone());
        Some(handle)
    }

    /// Python `_start_trace_dispatcher`：`enable_trace` 时注册消费轨迹钩子并启动通道。
    ///
    /// 与生产者一致（模块头差异 5）：`AsyncTraceDispatcher` 由
    /// [`set_trace_dispatcher`](Self::set_trace_dispatcher) 注入，这里只挂钩子 + start。
    fn start_trace_dispatcher(&self) {
        let cfg = self.config();
        let Some(dispatcher) = self.trace_dispatcher() else {
            if cfg.enable_trace {
                rmq_warn!(
                    "system mqtrace hook init skipped: enableTrace is on but no trace dispatcher \
                     was injected (see consumer.rs module header deviation 5)"
                );
            }
            return;
        };
        if cfg.enable_trace
            && !self
                .inner
                .consume_hooks
                .hooks()
                .iter()
                .any(|h| h.hook_name() == "ConsumeMessageTraceHook")
        {
            let sink: Arc<dyn TraceReportSink> = Arc::new(SinkAdapter(dispatcher.clone()));
            self.register_consume_message_hook(Arc::new(ConsumeMessageTraceHook::new(sink)));
        }
        if let Err(e) = dispatcher.start(&self.namesrv_addr()) {
            rmq_warn!("trace dispatcher start failed: {e}");
        }
    }

    fn namesrv_addr(&self) -> String {
        self.config().name_server_addrs.join(";")
    }

    // ---------------- 心跳 ----------------

    /// Python `_refresh_routes`：把订阅 topic 登记为「在用」并各拉一次路由。
    async fn refresh_routes(&self) {
        let Ok(client) = require_client(&self.inner) else {
            return;
        };
        let topics: Vec<String> = lock(&self.inner.state)
            .subscription_data
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        for topic in topics {
            client.register_topic_in_use(&topic);
            if let Err(e) = client.get_topic_publish_info(&topic, false).await {
                rmq_debug!("refresh route for {topic} failed: {e}");
            }
        }
    }

    /// Python `_send_heartbeat_to_all_broker`：返回成功的 broker 台数。
    pub async fn send_heartbeat_to_all_broker(&self) -> usize {
        let Ok(client) = require_client(&self.inner) else {
            return 0;
        };
        let ok = client.send_heartbeat_to_all_broker(5000).await;
        if ok > 0 {
            self.inner.heartbeat_count.fetch_add(1, Ordering::SeqCst);
        }
        ok
    }

    /// Python `heartbeat_count`（真机验证用）。
    pub fn heartbeat_count(&self) -> usize {
        self.inner.heartbeat_count.load(Ordering::SeqCst)
    }

    /// Python `flow_control_triggered`（真机验证流控是否生效）。
    pub fn flow_control_triggered(&self) -> usize {
        self.inner.flow_control_triggered.load(Ordering::SeqCst)
    }

    /// Python `assigned_queue_count`。
    pub fn assigned_queue_count(&self) -> usize {
        lock(&self.inner.state).assigned.len()
    }

    /// Python `assigned_queue_keys`（多实例分配「不重不漏」验证用）。
    pub fn assigned_queue_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = lock(&self.inner.state)
            .assigned
            .iter()
            .map(mq_key)
            .collect();
        keys.sort();
        keys
    }

    /// Python `_lock_ok`：`LOCK_BATCH_MQ` 真正拿到锁的队列（顺序消费才会填）。
    ///
    /// Java 里同一个事实是 `ProcessQueue.isLocked()`；四个移植版的
    /// `consumerRunningInfo` 都不回填 `ProcessQueueInfo.locked`（Python
    /// `consumer.py:1778-1783` 只写 commitOffset/cachedMsgCount/droped），
    /// 所以这是唯一能观测锁定状态的入口。
    pub fn locked_queue_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = lock(&self.inner.state).lock_ok.iter().cloned().collect();
        keys.sort();
        keys
    }

    // ---------------- 重平衡 ----------------

    /// Python `_all_queues_of_topic`（Java `RebalanceImpl.topicSubscribeInfoTable`）。
    async fn all_queues_of_topic(&self, topic: &str) -> Vec<MessageQueue> {
        let Ok(client) = require_client(&self.inner) else {
            return Vec::new();
        };
        match client.get_topic_publish_info(topic, false).await {
            Ok(publish) => publish
                .msg_queue_list()
                .into_iter()
                .map(|mq| MessageQueue::new(topic, &mq.broker_name, mq.queue_id))
                .collect(),
            Err(e) => {
                rmq_debug!("rebalance: no route for topic {topic}: {e}");
                Vec::new()
            }
        }
    }

    /// Python `_do_rebalance`（Java `RebalanceImpl.rebalanceByTopic`）。
    ///
    /// BROADCASTING 全部队列归自己；CLUSTERING 查 broker 上的消费者列表 → 排序 →
    /// 分配策略。**查不到消费者列表时保留现有分配**（绝不回退成「独占全部队列」，
    /// 否则同组多实例互相重复消费）。
    pub async fn do_rebalance(&self) -> Result<()> {
        let client = require_client(&self.inner)?;
        let group = self.consumer_group();
        let cfg = self.config();
        let was: BTreeSet<String> = lock(&self.inner.state)
            .assigned
            .iter()
            .map(mq_key)
            .collect();
        let topics: Vec<String> = lock(&self.inner.state)
            .subscription_data
            .iter()
            .map(|(k, _)| k.clone())
            .collect();

        let mut assigned: Vec<MessageQueue> = Vec::new();
        if cfg.message_model == MessageModel::BROADCASTING {
            for topic in &topics {
                assigned.extend(self.all_queues_of_topic(topic).await);
            }
        } else {
            for topic in &topics {
                let mut mq_all = self.all_queues_of_topic(topic).await;
                sort_mqs(&mut mq_all);
                if mq_all.is_empty() {
                    continue;
                }
                let cid_all = client
                    .get_consumer_id_list_by_group(topic, &group, 5000)
                    .await;
                let Some(mut cid_all) = cid_all else {
                    rmq_debug!(
                        "rebalance: no consumer id list for {group}/{topic}, keep current"
                    );
                    let keep: Vec<MessageQueue> = lock(&self.inner.state)
                        .assigned
                        .iter()
                        .filter(|mq| mq.topic == *topic)
                        .cloned()
                        .collect();
                    assigned.extend(keep);
                    continue;
                };
                if cid_all.is_empty() {
                    rmq_debug!("rebalance: empty consumer id list for {group}/{topic}");
                    continue;
                }
                cid_all.sort();
                let strategy = self
                    .inner
                    .strategy
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                match strategy.allocate(&group, &cfg.client_id.clone().unwrap_or_default(), &mq_all, &cid_all) {
                    Ok(got) => assigned.extend(got),
                    Err(e) => {
                        // Python 在这里直接 `return`：本轮分配结果整个作废，
                        // 连已算好的 topic 都不写入（下一轮 20s 后再试）。照抄。
                        rmq_error!(
                            "allocate message queue exception. strategy name: {}, ex: {}",
                            strategy.get_name(),
                            e
                        );
                        return Ok(());
                    }
                }
            }
        }
        {
            let mut state = lock(&self.inner.state);
            state.assigned = assigned.clone();
        }
        let now: BTreeSet<String> = assigned.iter().map(mq_key).collect();
        if now != was {
            rmq_info!(
                "rebalance result changed, group={group} clientId={} assigned={}",
                cfg.client_id.clone().unwrap_or_default(),
                assigned.len()
            );
        }
        // 新分配的队列**立刻**解析初始位点（Java updateProcessQueueTableInRebalance →
        // computePullFromWhereWithException）。留到首次拉取才惰性解析会漏掉
        // 「分配之后、首拉之前」新产生的消息。
        for mq in &assigned {
            let key = mq_key(mq);
            if was.contains(&key) {
                continue;
            }
            let sub = lock(&self.inner.state).subscription(&mq.topic).cloned();
            let Some(sub) = sub else { continue };
            match resolve_initial_offset(&self.inner, &client, mq, &sub).await {
                Ok(off) => {
                    let mut state = lock(&self.inner.state);
                    state.offset_table.entry(key).or_insert(off);
                }
                Err(e) => rmq_debug!("resolve initial offset failed for {mq:?}: {e}"),
            }
        }
        self.rebalance_pull_threads().await;
        Ok(())
    }

    /// Python `_rebalance_pull_threads`：按当前分配同步循环任务集，并清理被撤销队列。
    ///
    /// 队列被撤走时必须 ①持久化已消费位点 ②丢弃缓冲（在途消息不再消费）
    /// ③顺序消费还要 UNLOCK_BATCH_MQ —— 少任何一步，被撤销队列里的在途消息会被
    /// 旧实例继续消费，与新属主重复。
    async fn rebalance_pull_threads(&self) {
        let pop = self.config().pop_mode;
        let current: BTreeMap<String, MessageQueue> = lock(&self.inner.state)
            .assigned
            .iter()
            .map(|mq| (mq_key(mq), mq.clone()))
            .collect();
        let mut revoked: Vec<(MessageQueue, Option<i64>)> = Vec::new();
        let mut to_spawn: Vec<(MessageQueue, u64)> = Vec::new();
        {
            let mut state = lock(&self.inner.state);
            for (key, mq) in &current {
                if state.queue_owners.contains_key(key) {
                    continue;
                }
                if pop && !state.pop_queues.contains_key(key) {
                    state.pop_queues.insert(key.clone(), PopProcessQueue::new());
                }
                let token = self
                    .inner
                    .next_token
                    .fetch_add(1, Ordering::SeqCst);
                state.queue_owners.insert(key.clone(), token);
                to_spawn.push((mq.clone(), token));
            }
            let stale: Vec<String> = state
                .queue_owners
                .keys()
                .filter(|k| !current.contains_key(*k))
                .cloned()
                .collect();
            for key in stale {
                state.queue_owners.remove(&key); // 循环内检测到退出
                state.pending.remove(&key);
                state.lock_ok.remove(&key);
                let off = state.consume_offsets.remove(&key);
                state.offset_table.remove(&key);
                let mq = state.mq_map.remove(&key);
                if let Some(pq) = state.pop_queues.remove(&key) {
                    pq.set_dropped(true);
                }
                if let Some(mq) = mq {
                    revoked.push((mq, off));
                }
            }
        }
        let weak = Arc::downgrade(&self.inner);
        if let Some(handle) = self.runtime_handle() {
            for (mq, token) in to_spawn {
                let w = weak.clone();
                let task = handle.spawn(async move {
                    if let Some(inner) = w.upgrade() {
                        if pop {
                            queue_pop_loop(inner, mq, token).await;
                        } else {
                            queue_pull_loop(inner, mq, token).await;
                        }
                    }
                });
                self.inner
                    .tasks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(task);
            }
        }
        if !revoked.is_empty() {
            self.on_queues_revoked(&revoked).await;
        }
    }

    /// Python `_on_queues_revoked`（Java `RebalanceImpl.removeUnnecessaryMessageQueue`）。
    async fn on_queues_revoked(&self, revoked: &[(MessageQueue, Option<i64>)]) {
        if revoked.is_empty() {
            return;
        }
        let cfg = self.config();
        if cfg.message_model == MessageModel::BROADCASTING {
            // 广播模式位点只存本地
            let inner = &self.inner;
            if let Err(e) = save_local_offsets(inner) {
                rmq_debug!("save local offsets failed: {e}");
            }
            return;
        }
        let Ok(client) = require_client(&self.inner) else {
            return;
        };
        let group = cfg.consumer_group.clone();
        let orderly = lock(&self.inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        for (mq, off) in revoked {
            if let Some(off) = off {
                if let Err(e) = client.update_consumer_offset(&group, mq, *off, 5000, None).await {
                    rmq_debug!("persist offset on revoke failed for {mq:?}: {e}");
                }
            }
            // 顺序消费：释放 broker 队列锁，新属主才能立刻接上
            if orderly {
                let _ = client
                    .unlock_batch_mq(&group, &self.client_id(), std::slice::from_ref(mq), 1000)
                    .await;
            }
        }
        rmq_info!(
            "queues revoked, group={group} count={}",
            revoked.len()
        );
    }

    /// Python `_on_consumer_ids_changed`（40 的处理）。
    fn request_rebalance(&self) {
        self.inner.rebalance_now.store(true, Ordering::SeqCst);
        self.inner.rebalance_signal.notify_waiters();
    }

}

// ---------------- 位点解析 ----------------

/// Python `_resolve_initial_offset`（Java `computePullFromWhere`）。
///
/// 自由函数：拉取循环只持有 `Arc<Inner>`（模块头差异 4），拿不到消费者句柄。
async fn resolve_initial_offset(
    inner: &Inner,
    client: &MQClientInstance,
    mq: &MessageQueue,
    sub: &SubscriptionData,
) -> Result<i64> {
    let cfg = read_cfg(inner);
    if sub.expression_type == ExpressionType::SQL92 {
        // SQL 过滤无 offset 语义，默认最新
        return client.get_max_offset(mq, 5000, None).await;
    }
    let key = mq_key(mq);
    if cfg.message_model == MessageModel::BROADCASTING {
        // 广播模式：offset 只存本地（Java LocalFileOffsetStore）
        let stored = load_local_offsets(inner);
        if let Some(off) = stored.get(&key) {
            return Ok(*off);
        }
        if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET {
            return client.get_min_offset(mq, 5000, None).await;
        }
        return client.get_max_offset(mq, 5000, None).await;
    }
    // 集群模式：先查 broker 上已提交的位点（Java RemoteBrokerOffsetStore.readOffset）
    match client
        .query_consumer_offset(&cfg.consumer_group, mq, 5000, None, false)
        .await
    {
        Ok(Some(off)) if off >= 0 => return Ok(off),
        Ok(_) => {}
        Err(e) => rmq_debug!("query consumer offset for {mq:?} not found: {e}"),
    }
    if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET {
        return client.get_min_offset(mq, 5000, None).await;
    }
    if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_TIMESTAMP {
        let ts = consume_timestamp_millis(&cfg.consume_timestamp)?;
        return client.search_offset_by_timestamp(mq, ts, 5000, None).await;
    }
    if NamespaceUtil::is_retry_topic(&mq.topic) {
        // Java RebalancePushImpl:181-182：%RETRY% 主题首次消费从 0 开始，
        // 不能按 CONSUME_FROM_LAST_OFFSET 跳过（重试消息要全量重试）
        return Ok(0);
    }
    client.get_max_offset(mq, 5000, None).await
}

// ================================================================ 后台循环
//
// 拆成自由函数而不是 `&self` 方法：任务只持 `Weak<Inner>`（模块头差异 4），
// 否则 `实例 → 传输 → 任务 → 消费者` 成环，消费者永不释放。

/// `stop` 通道是否已置位（含发送端已释放）。
fn stopped(rx: &watch::Receiver<bool>) -> bool {
    *rx.borrow() || rx.has_changed().is_err()
}

/// Python 的 `Event.wait(timeout)`：到点返回 false，收到停止信号返回 true。
async fn wait_or_stop(rx: &mut watch::Receiver<bool>, millis: u64) -> bool {
    match tokio::time::timeout(Duration::from_millis(millis), rx.changed()).await {
        Ok(Ok(())) => stopped(rx),
        // 发送端已释放（Inner 正在析构）= 立刻退出
        Ok(Err(_)) => true,
        Err(_) => false,
    }
}

/// Python `_owns_queue`：本循环是否仍持有该队列。
fn owns_queue(inner: &Inner, key: &str, token: u64) -> bool {
    lock(&inner.state)
        .queue_owners
        .get(key)
        .is_some_and(|t| *t == token)
}

/// Python `_queue_pull_loop`：单队列长轮询拉取 → 推入待消费缓冲。
///
/// 每队列一个循环（不是共享线程池），因为 broker 为每个队列各挂起一个长轮询；
/// 共用一个循环会让空队列的 ~15s 挂起阻塞其余队列的投递。
async fn queue_pull_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let Ok(client) = require_client(&inner) else {
        return;
    };
    let key = mq_key(&mq);
    let mut rx = inner.stop.subscribe();
    while !stopped(&rx) && inner.started.load(Ordering::Acquire) {
        if !owns_queue(&inner, &key, token) {
            return;
        }
        let sub = lock(&inner.state).subscription(&mq.topic).cloned();
        let Some(sub) = sub else { return };
        let cfg = read_cfg(&inner);
        let orderly = lock(&inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        // 顺序消费：broker 未确认锁定（LOCK_BATCH_MQ）的队列不拉取
        if orderly && !lock(&inner.state).lock_ok.contains(&key) {
            if wait_or_stop(&mut rx, 200).await {
                return;
            }
            continue;
        }
        if flow_control_hit(&inner, &mq, &key) {
            if wait_or_stop(&mut rx, 100).await {
                return;
            }
            continue;
        }
        let cached = lock(&inner.state).offset_table.get(&key).copied();
        let offset = match cached {
            Some(off) => off,
            None => match resolve_initial_offset(&inner, &client, &mq, &sub).await {
                Ok(off) => {
                    lock(&inner.state).offset_table.insert(key.clone(), off);
                    off
                }
                Err(e) => {
                    rmq_debug!("resolve initial offset failed for {mq:?}: {e}");
                    if wait_or_stop(&mut rx, 1000).await {
                        return;
                    }
                    continue;
                }
            },
        };
        let sys_flag = PullSysFlag::build_sys_flag_basic(false, true, true, false);
        let began = current_time_millis();
        let result = match client
            .pull_message(
                &cfg.consumer_group,
                &mq,
                offset,
                cfg.pull_batch_size,
                sys_flag,
                0,
                &sub.sub_string,
                sub.sub_version,
                &sub.expression_type,
                cfg.pull_timeout_millis,
                cfg.pull_batch_size_in_bytes,
                cfg.pull_suspend_timeout_millis,
                None,
                0,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                // 长轮询在 suspend 期间无新消息触发客户端超时属**正常行为**：broker 把
                // 挂起时长钳制到自己的 brokerSuspendMaxTimeMillis，空闲队列会周期性超时。
                let benign = matches!(
                    e,
                    Error::Timeout { .. } | Error::TooMuchRequest(_)
                );
                if benign {
                    rmq_debug!("pull long-poll timeout for {mq:?} (benign, will retry): {e}");
                    continue;
                }
                // 其余多为 topic 尚未创建等预期路径：debug + 短暂退避，避免热循环
                rmq_debug!("pull error for {mq:?}: {e}");
                if wait_or_stop(&mut rx, 500).await {
                    return;
                }
                continue;
            }
        };
        // 消费统计（Java PullCallback.onSuccess：RT 恒记，TPS 只在有消息时记）
        if let Some(stats) = lock(&inner.stats).clone() {
            stats.inc_pull_rt(&cfg.consumer_group, &mq.topic, current_time_millis() - began);
            if result.status == PullStatus::Found && !result.msg_found_list.is_empty() {
                stats.inc_pull_tps(
                    &cfg.consumer_group,
                    &mq.topic,
                    i64::try_from(result.msg_found_list.len()).unwrap_or(i64::MAX),
                );
            }
        }
        let mut msgs = result.msg_found_list;
        if result.status == PullStatus::Found && !msgs.is_empty() {
            // 投递前的客户端侧过滤（Java PullAPIWrapper.processPullResult:113-128）。
            // 必须在拿锁之前做 —— 钩子是用户代码，可能阻塞。
            // 拉取路径被摘掉的消息不 ack（Java 亦然）：位点照常推进 = 静默跳过。
            msgs = filter_messages_for_delivery(
                &cfg.consumer_group,
                &inner.filter_hooks,
                &mq,
                Some(&sub),
                msgs,
            );
        }
        // 入队与「是否仍持有该队列」必须同一把锁内完成：挂起期间被撤走的队列，
        // 这批消息按 Java ProcessQueue.isDropped() 语义**直接丢弃**——不消费、不推进
        // 位点，由新属主从最后持久化的位点重投，否则两实例重复消费。
        {
            let mut state = lock(&inner.state);
            if state
                .queue_owners
                .get(&key)
                .is_none_or(|t| *t != token)
            {
                rmq_debug!(
                    "queue {key} revoked during pull, discard {} fetched messages",
                    msgs.len()
                );
                return;
            }
            if !msgs.is_empty() {
                state.mq_map.insert(key.clone(), mq.clone());
                state
                    .pending
                    .entry(key.clone())
                    .or_default()
                    .extend(msgs.iter().cloned());
            }
            state
                .offset_table
                .insert(key.clone(), result.next_begin_offset);
        }
        // update_msg_acc_cnt 自己取同一把锁，必须在上面释放之后调用。
        update_msg_acc_cnt(&inner, &key, &msgs);
    }
}

/// Java `ProcessQueue.putMessage` 里的 `msgAccCnt` 计算（`ProcessQueue.java:148-158`）：
/// 取**本批最后一条**的 `MAX_OFFSET - queueOffset`，>0 才更新。
fn update_msg_acc_cnt(inner: &Inner, key: &str, msgs: &[MessageExt]) {
    let Some(last) = msgs.last() else {
        return;
    };
    let Some(prop) = last.get_property(PROPERTY_MAX_OFFSET) else {
        return;
    };
    let Ok(max_offset) = prop.parse::<i64>() else {
        return;
    };
    let acc_total = max_offset - last.queue_offset;
    if acc_total > 0 {
        lock(&inner.state).msg_acc_cnt.insert(key.to_string(), acc_total);
    }
}

/// Python `_flow_control_hit`（Java `ProcessQueue.putMessage` 的五个阈值）。
fn flow_control_hit(inner: &Inner, mq: &MessageQueue, key: &str) -> bool {
    let cfg = read_cfg(inner);
    let (count, size_mb, span, topic_pending) = {
        let state = lock(&inner.state);
        let dq: &VecDeque<MessageExt> = state.pending.get(key).unwrap_or(&EMPTY_DEQUE);
        let count = dq.len();
        let size_mb = dq.iter().map(|m| i64::from(m.store_size)).sum::<i64>() as f64
            / (1024.0 * 1024.0);
        let span = if dq.is_empty() {
            0
        } else {
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            for m in dq {
                min = min.min(m.queue_offset);
                max = max.max(m.queue_offset);
            }
            max - min
        };
        let topic_pending: Vec<(i32, i64)> = state
            .pending
            .iter()
            .filter(|(k, _)| {
                state.mq_map.get(*k).is_some_and(|m| m.topic == mq.topic)
            })
            .flat_map(|(_, dq)| dq.iter())
            .map(|m| (m.store_size, m.queue_offset))
            .collect();
        (count, size_mb, span, topic_pending)
    };
    let reason: Option<String> = if count >= cfg.pull_threshold_for_queue.max(1) as usize {
        Some(format!("count={count}"))
    } else if cfg.pull_threshold_size_for_queue > 0
        && size_mb >= f64::from(cfg.pull_threshold_size_for_queue)
    {
        Some(format!("size={size_mb:.1}MB"))
    } else if cfg.consume_concurrently_max_span > 0
        && span > cfg.consume_concurrently_max_span
    {
        Some(format!("span={span}"))
    } else if cfg.pull_threshold_for_topic > 0 || cfg.pull_threshold_size_for_topic > 0 {
        if cfg.pull_threshold_for_topic > 0
            && topic_pending.len() >= cfg.pull_threshold_for_topic as usize
        {
            Some(format!("topicCount={}", topic_pending.len()))
        } else if cfg.pull_threshold_size_for_queue > 0 {
            let topic_mb = topic_pending
                .iter()
                .map(|(s, _)| i64::from(*s))
                .sum::<i64>() as f64
                / (1024.0 * 1024.0);
            if topic_mb >= f64::from(cfg.pull_threshold_size_for_topic) {
                Some(format!("topicSize={topic_mb:.1}MB"))
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };
    match reason {
        None => false,
        Some(reason) => {
            inner
                .flow_control_triggered
                .fetch_add(1, Ordering::SeqCst);
            rmq_debug!("flow control: queue {key} {reason}, pause pull");
            true
        }
    }
}

static EMPTY_DEQUE: VecDeque<MessageExt> = VecDeque::new();

/// Python `_rebalance_loop`（Java `RebalanceService`，默认 20s；被 40 通知时立刻）。
///
/// 启动 60s 内且当前没有任何分配时缩短为 2s：消费者可能先于 topic 创建而启动
/// （`autoCreateTopicEnable` 下 topic 由生产者首次发送时建），死等 20s 会长时间不消费。
async fn rebalance_loop(consumer: DefaultMQPushConsumer, rx: watch::Receiver<bool>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut next_run = current_time_millis();
    loop {
        if stopped(&rx) || !consumer.is_started() {
            return;
        }
        let starting_up =
            current_time_millis() - consumer.inner.start_time_millis.load(Ordering::SeqCst) < 60_000;
        let interval = if starting_up && consumer.assigned_queue_count() == 0 {
            2_000
        } else {
            20_000
        };
        let requested = consumer
            .inner
            .rebalance_now
            .swap(false, Ordering::SeqCst);
        if !requested && current_time_millis() < next_run {
            let _ = ticker.tick().await;
            continue;
        }
        next_run = current_time_millis() + interval;
        if let Err(e) = consumer.do_rebalance().await {
            rmq_debug!("rebalance error: {e}");
        }
    }
}

/// Python `_heartbeat_loop`（消费者**必须**注册到 broker，否则 rebalance 的
/// GET_CONSUMER_LIST_BY_GROUP 拿不到列表）。
async fn heartbeat_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    loop {
        let interval = read_cfg(&inner).heartbeat_interval_millis.max(1000) as u64;
        if wait_or_stop(&mut rx, interval).await {
            return;
        }
        let cfg = read_cfg(&inner);
        if !cfg.heartbeat_enabled || !inner.started.load(Ordering::Acquire) {
            continue;
        }
        let Ok(client) = require_client(&inner) else {
            return;
        };
        let ok = client.send_heartbeat_to_all_broker(5000).await;
        if ok > 0 {
            inner.heartbeat_count.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Python `_offset_persist_loop`（Java `MQClientInstance.startScheduledTask`：每 5s）。
async fn offset_persist_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    loop {
        if wait_or_stop(&mut rx, 5_000).await {
            return;
        }
        let client = match require_client(&inner) {
            Ok(c) => c,
            Err(_) => return,
        };
        persist_offsets_once(&inner, &client).await;
    }
}

/// Python `_lock_loop`（Java `ConsumeMessageOrderlyService.lockMQ`：每 20s，启动即试一次）。
async fn lock_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    loop {
        let mqs: Vec<MessageQueue> = lock(&inner.state).assigned.clone();
        if !mqs.is_empty() {
            let cfg = read_cfg(&inner);
            let client = match require_client(&inner) {
                Ok(c) => c,
                Err(_) => return,
            };
            let client_id = cfg.client_id.clone().unwrap_or_default();
            match client
                .lock_batch_mq(&cfg.consumer_group, &client_id, &mqs, 1000)
                .await
            {
                Ok(ok) => {
                    let keys: BTreeSet<String> = ok.iter().map(mq_key).collect();
                    let n = keys.len();
                    lock(&inner.state).lock_ok = keys;
                    rmq_debug!("lock_batch_mq: {n}/{} queues locked", mqs.len());
                }
                Err(e) => rmq_debug!("lock mq error: {e}"),
            }
        }
        if wait_or_stop(&mut rx, 20_000).await {
            return;
        }
    }
}

/// Python `_dispatch_loop`：串行地把各队列缓冲里的批次交给监听器消费。
async fn dispatch_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    loop {
        if stopped(&rx) || !inner.started.load(Ordering::Acquire) {
            return;
        }
        let keys: Vec<String> = lock(&inner.state).pending.keys().cloned().collect();
        let mut progressed = false;
        for key in keys {
            if stopped(&rx) || !inner.started.load(Ordering::Acquire) {
                return;
            }
            let (mq, batch) = {
                let mut state = lock(&inner.state);
                let Some(mq) = state.mq_map.get(&key).cloned() else {
                    continue;
                };
                let Some(dq) = state.pending.get_mut(&key) else {
                    continue;
                };
                if dq.is_empty() {
                    continue;
                }
                let cfg = read_cfg(&inner);
                let n = dq.len().min(cfg.consume_message_batch_max_size.max(1) as usize);
                let batch: Vec<MessageExt> = dq.drain(..n).collect();
                (mq.clone(), batch)
            };
            if batch.is_empty() {
                continue;
            }
            match consume_batch(&inner, &key, &mq, batch.clone()).await {
                Ok(done) => progressed = progressed || done,
                Err(e) => {
                    // 分发路径意外异常：批次塞回队首，稍后重试（不能杀死分发任务）
                    rmq_error!("dispatch batch error (will retry): {e}");
                    {
                        let mut state = lock(&inner.state);
                        if let Some(dq) = state.pending.get_mut(&key) {
                            for m in batch.iter().rev() {
                                dq.push_front(m.clone());
                            }
                        }
                    }
                    if wait_or_stop(&mut rx, 100).await {
                        return;
                    }
                }
            }
        }
        if !progressed && wait_or_stop(&mut rx, 50).await {
            return;
        }
    }
}

impl DefaultMQPushConsumer {
    /// 起全部后台循环（Python `_start_*_loop` 的集合）。
    fn spawn_loops(&self) {
        let Some(handle) = self.runtime_handle() else {
            rmq_warn!("consumer: no tokio runtime, background loops disabled");
            return;
        };
        let weak = Arc::downgrade(&self.inner);
        macro_rules! spawn_loop {
            ($f:path) => {{
                let w = weak.clone();
                let rx = self.inner.stop.subscribe();
                handle.spawn(async move {
                    if let Some(inner) = w.upgrade() {
                        $f(inner, rx).await;
                    }
                })
            }};
        }
        let tasks = &mut *lock(&self.inner.tasks);
        tasks.push(spawn_loop!(heartbeat_loop));
        tasks.push(spawn_loop!(offset_persist_loop));
        tasks.push(spawn_loop!(lock_loop));
        // 分发与重平衡：重平衡需要句柄（要起每队列循环），单独用消费者克隆体
        let consumer = self.clone();
        let rx = self.inner.stop.subscribe();
        tasks.push(handle.spawn(async move { rebalance_loop(consumer, rx).await }));
        tasks.push(spawn_loop!(dispatch_loop));
    }
}

/// Python `_persist_offsets_once`（集群模式刷 broker / 广播模式刷本地）。
async fn persist_offsets_once(inner: &Arc<Inner>, client: &MQClientInstance) {
    let cfg = read_cfg(inner);
    if cfg.message_model == MessageModel::BROADCASTING {
        if let Err(e) = save_local_offsets(inner) {
            rmq_debug!("save local offsets failed: {e}");
        }
        return;
    }
    let items: Vec<(MessageQueue, i64)> = {
        let state = lock(&inner.state);
        state
            .consume_offsets
            .iter()
            .filter_map(|(k, off)| state.mq_map.get(k).map(|mq| (mq.clone(), *off)))
            .collect()
    };
    for (mq, off) in items {
        if let Err(e) = client
            .update_consumer_offset(&cfg.consumer_group, &mq, off, 5000, None)
            .await
        {
            rmq_debug!("update consumer offset failed for {mq:?}: {e}");
        }
    }
}

/// Python `_local_offset_path`（Java `LocalFileOffsetStore`：
/// `$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json`）。
fn local_offset_path(inner: &Inner) -> Option<std::path::PathBuf> {
    let cfg = read_cfg(inner);
    let home = crate::common::util_all::user_home()?;
    let base = std::path::Path::new(&home)
        .join(".rocketmq_offsets")
        .join(cfg.client_id.as_deref().unwrap_or("DEFAULT"))
        .join(&cfg.consumer_group);
    Some(base.join("offsets.json"))
}

/// Python `_save_local_offsets`：先写 `.tmp` 再 `os.replace`，避免半截文件。
fn save_local_offsets(inner: &Inner) -> Result<()> {
    let Some(path) = local_offset_path(inner) else {
        return Err(Error::client("HOME is not set, cannot store local offsets"));
    };
    let items: BTreeMap<String, i64> = lock(&inner.state).consume_offsets.clone();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_file_name("offsets.json.tmp");
    let text = serde_json::to_string(&items)?;
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Python `_load_local_offsets`：任何读失败都当「没有本地位点」。
fn load_local_offsets(inner: &Inner) -> BTreeMap<String, i64> {
    let Some(path) = local_offset_path(inner) else {
        return BTreeMap::new();
    };
    match std::fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(_) => BTreeMap::new(),
    }
}

// ================================================================ POP 消费

/// Python `_queue_pop_loop`（Java `DefaultMQPushConsumerImpl.popMessage` 回调部分）。
///
/// 与拉取路径的关键差别：**不查也不提交消费位点**（进度由 broker 侧 checkpoint
/// 跟踪，确认只靠 ack）；弹出即投递给消费线程；`POLLING_NOT_FOUND` 是正常态。
async fn queue_pop_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let Ok(client) = require_client(&inner) else {
        return;
    };
    let key = mq_key(&mq);
    let cfg = read_cfg(&inner);
    let mut invisible = cfg.pop_invisible_time;
    if !(MIN_POP_INVISIBLE_TIME..=MAX_POP_INVISIBLE_TIME).contains(&invisible) {
        // Java 的钳制：超出 [5s, 300s] 一律回落到 60s
        invisible = DEFAULT_POP_INVISIBLE_TIME;
    }
    // Java PopRequest 默认 ConsumeInitMode.MAX；这里按 consume_from_where 映射，
    // 让「从头消费」在 POP 模式下也成立。
    let init_mode = if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET {
        ConsumeInitMode::MIN
    } else {
        ConsumeInitMode::MAX
    };
    let mut rx = inner.stop.subscribe();
    while !stopped(&rx) && inner.started.load(Ordering::Acquire) {
        if !owns_queue(&inner, &key, token) {
            return;
        }
        let pq = match lock(&inner.state).pop_queues.get(&key) {
            Some(pq) => pq.clone(),
            None => return,
        };
        if pq.is_dropped() {
            return;
        }
        let sub = match lock(&inner.state).subscription(&mq.topic).cloned() {
            Some(sub) => sub,
            None => return,
        };
        let cfg = read_cfg(&inner);
        // 流控：已弹未 ack 太多就先缓一缓（Java popThresholdForQueue）
        if pq.wait_ack_count() > cfg.pop_threshold_for_queue {
            if wait_or_stop(&mut rx, 50).await {
                return;
            }
            continue;
        }
        let began = current_time_millis();
        let exp = if sub.sub_string.is_empty() { "*" } else { &sub.sub_string };
        let result = match client
            .pop_message(
                &cfg.consumer_group,
                &mq.topic,
                mq.queue_id,
                cfg.pop_batch_nums,
                invisible,
                cfg.pop_poll_time_millis,
                init_mode,
                Some(exp),
                Some(&sub.expression_type),
                false,
                Some(&mq.broker_name),
                cfg.pop_timeout_millis,
                None,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if matches!(e, Error::Timeout { .. }) {
                    rmq_debug!("pop long-poll timeout for {mq:?} (benign, will retry)");
                    continue;
                }
                rmq_debug!("pop error for {mq:?}: {e}");
                if wait_or_stop(&mut rx, 500).await {
                    return;
                }
                continue;
            }
        };
        // 弹出后队列被撤走：这一批**既不消费也不 ack**，交给 invisibleTime 到期后
        // broker 自动复活重投给新属主（Java PopProcessQueue.isDropped() 分支）。
        if !owns_queue(&inner, &key, token) || pq.is_dropped() {
            rmq_debug!(
                "queue {key} revoked during pop, discard {} messages un-acked",
                result.msg_found_list.len()
            );
            return;
        }
        pq.touch(current_time_millis());
        if result.status == PopStatus::Found && !result.msg_found_list.is_empty() {
            pq.inc_found_msg(
                i32::try_from(result.msg_found_list.len()).unwrap_or(i32::MAX),
            );
            // 投递前过滤（Java processPopResult:621-661）：POP 路径**必须 ack 被摘掉的**，
            // 否则 invisibleTime 到期后 broker 会复活重投 —— 表现为「过滤没生效」。
            let kept = filter_messages_for_delivery(
                &cfg.consumer_group,
                &inner.filter_hooks,
                &mq,
                Some(&sub),
                result.msg_found_list.clone(),
            );
            let mut dropped_cnt = 0usize;
            if kept.len() != result.msg_found_list.len() {
                let kept_ids: BTreeSet<(String, i64)> = kept
                    .iter()
                    .map(|m| (m.msg_id.clone().unwrap_or_default(), m.queue_offset))
                    .collect();
                for msg in &result.msg_found_list {
                    let id = (msg.msg_id.clone().unwrap_or_default(), msg.queue_offset);
                    if !kept_ids.contains(&id) {
                        ack_pop_msg(&inner, msg);
                        pq.ack();
                        dropped_cnt += 1;
                    }
                }
                rmq_info!(
                    "pop filter dropped {dropped_cnt} of {} messages (acked)",
                    result.msg_found_list.len()
                );
            }
            if !kept.is_empty() {
                submit_pop_consume_request(&inner, kept, pq.clone(), mq.clone()).await;
            }
        } else if current_time_millis() - began < 200 {
            // 空结果：broker 没按 poll_time 挂起（立即返回）会变成热循环，兜底退避
            if wait_or_stop(&mut rx, 200).await {
                return;
            }
        }
    }
}

/// Python `_submit_pop_consume_request`（Java
/// `ConsumeMessagePopConcurrentlyService.submitPopConsumeRequest`）：
/// 按 `consumeMessageBatchMaxSize` 切批后投给消费线程池。
async fn submit_pop_consume_request(
    inner: &Arc<Inner>,
    msgs: Vec<MessageExt>,
    pq: Arc<PopProcessQueue>,
    mq: MessageQueue,
) {
    let size = read_cfg(inner).consume_message_batch_max_size.max(1) as usize;
    let mut batches: Vec<Vec<MessageExt>> = Vec::new();
    let mut rest = msgs;
    while rest.len() > size {
        batches.push(rest.drain(..size).collect());
    }
    if !rest.is_empty() {
        batches.push(rest);
    }
    let executor = lock(&inner.pop_executor).clone();
    for batch in batches {
        let pq = Arc::clone(&pq);
        let mq = mq.clone();
        match &executor {
            Some(executor) => {
                let w = Arc::downgrade(inner);
                let task: ConsumeTask = Box::pin(async move {
                    if let Some(inner) = w.upgrade() {
                        consume_pop_batch(inner, batch, pq, mq).await;
                    }
                });
                if let Err(e) = executor.submit(task) {
                    rmq_debug!("pop executor submit failed: {e}");
                }
            }
            // 未起执行器（单测或未 start）：同步执行，与 Python 一致
            None => consume_pop_batch(inner.clone(), batch, pq, mq).await,
        }
    }
}

/// Python `_consume_pop_batch`（Java
/// `ConsumeMessagePopConcurrentlyService$ConsumeRequest.run`）。
async fn consume_pop_batch(
    inner: Arc<Inner>,
    msgs: Vec<MessageExt>,
    pq: Arc<PopProcessQueue>,
    mq: MessageQueue,
) {
    if pq.is_dropped() || msgs.is_empty() {
        return;
    }
    let (mut pop_time, mut invisible) = (0i64, 0i64);
    match msgs[0].get_property(PROPERTY_POP_CK) {
        Some(ck) => match extra_info::split(ck) {
            Ok(segments) => {
                pop_time = extra_info::get_pop_time(&segments).unwrap_or(0);
                invisible = extra_info::get_invisible_time(&segments).unwrap_or(0);
            }
            Err(e) => rmq_debug!("parse pop ck failed for {mq:?}, treat as timed out: {e}"),
        },
        None => rmq_debug!("message without POP_CK, treat as timed out"),
    }
    if is_pop_timeout(&msgs, pop_time, invisible) {
        // 已经超过 invisibleTime：ack 也不会被承认，直接放弃本批（等 broker 复活重投）
        rmq_debug!("pop timeout, abort consume for {mq:?}: popTime={pop_time} invisible={invisible}");
        pq.dec_found_msg(i32::try_from(msgs.len()).unwrap_or(i32::MAX));
        return;
    }
    let mut msgs = msgs;
    reset_retry_topic_and_namespace(&inner, &mut msgs);
    let mut context = ConsumeConcurrentlyContext::new(Some(mq.clone()));
    // ⚠ 对齐 Java `ConsumeConcurrentlyContext.ackIndex = Integer.MAX_VALUE`：默认就是
    // 「全部 ack」。本仓库 ConsumeConcurrentlyContext 的默认值是 -1（回投路径语义），
    // 不在这里改成 size-1 会让 CONSUME_SUCCESS **一条都不 ack**，消息在 invisibleTime
    // 到期后复活重投 —— 短观测窗口下会伪装成通过。
    context.ack_index = i32::try_from(msgs.len()).unwrap_or(i32::MAX) - 1;

    let mut hook_ctx = if inner.consume_hooks.has_hooks() {
        Some(build_consume_hook_context(&inner, &msgs, &mq))
    } else {
        None
    };
    if let Some(ctx) = hook_ctx.as_mut() {
        execute_consume_hook_before(&inner.consume_hooks, ctx);
    }
    let begin_ms = current_time_millis();
    let (status, has_exception) = call_concurrently_listener(&inner, &msgs, &mut context).await;
    let failed = matches!(status, Some(ConsumeConcurrentlyStatus::ReconsumeLater));
    let succeeded = matches!(status, Some(ConsumeConcurrentlyStatus::ConsumeSuccess));
    // 钩子的 returnType 判定在「status 归一化为 RECONSUME_LATER」**之前**做，与 Java 一致
    record_consume_stats(&inner, &mq.topic, msgs.len(), begin_ms, failed);
    if let Some(ctx) = hook_ctx.as_mut() {
        finish_consume_hook(&inner, ctx, status, has_exception, begin_ms, failed, succeeded);
    }
    let status = match status {
        Some(s) => s,
        None => ConsumeConcurrentlyStatus::ReconsumeLater,
    };
    // 消费期间队列被撤走或已超时：结果不再处理
    if pq.is_dropped() || is_pop_timeout(&msgs, pop_time, invisible) {
        pq.dec_found_msg(i32::try_from(msgs.len()).unwrap_or(i32::MAX));
        return;
    }
    process_pop_consume_result(&inner, status, &context, &msgs, &pq).await;
}

/// Java `ConsumeRequest.isPopTimeout`：解不出 popTime/invisibleTime 时按超时处理。
fn is_pop_timeout(msgs: &[MessageExt], pop_time: i64, invisible: i64) -> bool {
    if msgs.is_empty() || pop_time <= 0 || invisible <= 0 {
        return true;
    }
    current_time_millis() - pop_time >= invisible
}

/// Python `_process_pop_consume_result`（Java
/// `ConsumeMessagePopConcurrentlyService.processConsumeResult`）。
async fn process_pop_consume_result(
    inner: &Arc<Inner>,
    status: ConsumeConcurrentlyStatus,
    context: &ConsumeConcurrentlyContext,
    msgs: &[MessageExt],
    pq: &Arc<PopProcessQueue>,
) {
    let mut ack_index = context.ack_index;
    if status == ConsumeConcurrentlyStatus::ConsumeSuccess {
        let len = i32::try_from(msgs.len()).unwrap_or(i32::MAX);
        if ack_index >= len {
            ack_index = len - 1;
        }
    } else {
        // RECONSUME_LATER：一条都不 ack
        ack_index = -1;
    }
    // `acked` 既是「前 acked 条成功、逐条 ack」的边界，也是 Python 第二个
    // `range(ack_index + 1, len(msgs))` 的起点：ack_index == -1 时它是 0，
    // 剩下的每一条仍要 pq.ack() 并延长不可见时间。
    let acked = if ack_index >= 0 {
        usize::try_from(ack_index).unwrap_or(0) + 1
    } else {
        0
    };
    for msg in msgs.iter().take(acked) {
        ack_pop_msg(inner, msg);
        pq.ack();
    }
    let cfg = read_cfg(inner);
    for msg in msgs.iter().skip(acked) {
        pq.ack();
        // 超过最大重试次数：Java checkNeedAckOrDelay（太老就直接 ack 丢弃，
        // 否则按消息已存活时间选一个延迟档位）
        if cfg.max_reconsume_times >= 0 && msg.reconsume_times >= cfg.max_reconsume_times {
            check_need_ack_or_delay(inner, msg, &cfg);
            continue;
        }
        change_pop_invisible_time(inner, msg, context.delay_level_when_next_consume, &cfg);
    }
}

/// Python `_check_need_ack_or_delay`：重试次数用尽后的兜底。
fn check_need_ack_or_delay(inner: &Inner, msg: &MessageExt, cfg: &ConsumerConfig) {
    let table = &cfg.pop_delay_level;
    let Some((last_second,)) = table.last().map(|v| (*v,)) else {
        return;
    };
    let msg_delay_time = current_time_millis() - msg.born_timestamp;
    if msg_delay_time > i64::from(last_second) * 1000 * 2 {
        rmq_warn!(
            "pop consume too many times, ack and drop: key={}",
            msg.get_keys().unwrap_or_default()
        );
        ack_pop_msg(inner, msg);
        return;
    }
    let mut level = table.len() as i32 - 1;
    while level >= 0 {
        if msg_delay_time >= i64::from(table[level as usize]) * 1000 {
            level += 1;
            break;
        }
        level -= 1;
    }
    change_pop_invisible_time(inner, msg, level, cfg);
}

/// Python `_pop_ck_target`：从 POP_CK 解出 ack/延长不可见时间需要的五元组。
///
/// 两处都不能想当然：① topic 要按 CK 的 retryFlag 用 `getRealTopic` 还原（复活消息的
/// 真实 topic 是 `%RETRY%<group>_<topic>`，不是消息上的 topic）；② 地址要按 CK 里的
/// brokerName 反查，不能按 topic 查路由（retry topic 通常没有独立路由表项）。
fn pop_ck_target(inner: &Inner, msg: &MessageExt) -> Option<(String, String, i32, i64, String)> {
    let ck = msg.get_property(PROPERTY_POP_CK)?;
    if ck.is_empty() {
        rmq_debug!("pop message without POP_CK, cannot ack");
        return None;
    }
    let segments = match extra_info::split(ck) {
        Ok(s) => s,
        Err(e) => {
            rmq_debug!("bad POP_CK {ck:?}: {e}");
            return None;
        }
    };
    let broker_name = extra_info::get_broker_name(&segments).ok()?;
    let queue_id = extra_info::get_queue_id(&segments).ok()?;
    let offset = extra_info::get_queue_offset(&segments).ok()?;
    let retry = extra_info::get_retry(&segments).ok()?;
    let group = read_cfg(inner).consumer_group;
    let topic = extra_info::get_real_topic(&msg.topic, &group, &retry).ok()?;
    Some((topic, broker_name, queue_id, offset, ck.to_string()))
}

/// Python `_ack_pop_msg`（Java `DefaultMQPushConsumerImpl.ackAsync`）。
///
/// ⚠ ack 是 RPC，这里刻意保持**同步语义**的调用点（过滤摘除路径）由调用方在
/// async 上下文里 `let _ =` 掉：ack 失败不致命，消息会在 invisibleTime 到期后复活重投。
/// 为了让上层三种调用形状都能用，这里提供阻塞无关的「发任务」实现。
fn ack_pop_msg(inner: &Inner, msg: &MessageExt) {
    let Some((topic, broker_name, queue_id, offset, ck)) = pop_ck_target(inner, msg) else {
        return;
    };
    let Ok(client) = require_client(inner) else {
        return;
    };
    let addr = client.broker_addr_of(&broker_name);
    let group = read_cfg(inner).consumer_group;
    let Some(handle) = inner.runtime.get().cloned().or_else(|| tokio::runtime::Handle::try_current().ok())
    else {
        rmq_debug!("ack skipped: no runtime to run the RPC");
        return;
    };
    handle.spawn(async move {
        match client
            .ack_message(
                &group,
                &topic,
                queue_id,
                &ck,
                offset,
                Some(&broker_name),
                3000,
                addr.as_deref(),
            )
            .await
        {
            Ok(_) => {}
            Err(e) => rmq_debug!("ack failed: {e}"),
        }
    });
}

/// Python `_change_pop_invisible_time`：`delay_level == 0` 时用消息已重试次数当档位；
/// 档位表单位是**秒**，接口要毫秒。
fn change_pop_invisible_time(
    inner: &Inner,
    msg: &MessageExt,
    delay_level: i32,
    cfg: &ConsumerConfig,
) {
    let Some((topic, broker_name, queue_id, offset, ck)) = pop_ck_target(inner, msg) else {
        return;
    };
    let level = if delay_level == 0 {
        msg.reconsume_times
    } else {
        delay_level
    };
    let table = &cfg.pop_delay_level;
    let Some(last) = table.last() else {
        return;
    };
    let delay_second = if level as usize >= table.len() {
        *last
    } else {
        table[level.max(0) as usize]
    };
    let Ok(client) = require_client(inner) else {
        return;
    };
    let addr = client.broker_addr_of(&broker_name);
    let group = cfg.consumer_group.clone();
    let Some(handle) = inner.runtime.get().cloned().or_else(|| tokio::runtime::Handle::try_current().ok())
    else {
        rmq_debug!("change invisible time skipped: no runtime to run the RPC");
        return;
    };
    handle.spawn(async move {
        match client
            .change_invisible_time(
                &group,
                &topic,
                queue_id,
                &ck,
                offset,
                i64::from(delay_second) * 1000,
                Some(&broker_name),
                3000,
                addr.as_deref(),
            )
            .await
        {
            Ok(_) => {}
            Err(e) => rmq_debug!("change invisible time failed: {e}"),
        }
    });
}

// ================================================================ 消费与钩子

/// Python `_reset_retry_topic_and_namespace`（Java
/// `DefaultMQPushConsumerImpl.resetRetryAndNamespace`，交给 listener 之前调用）。
///
/// 重投消息实际存在 `%RETRY%<group>` 下，broker 把原始 topic 写进 `RETRY_TOPIC` 属性；
/// 不还原的话用户按 topic 分支的代码会走错。
fn reset_retry_topic_and_namespace(inner: &Inner, msgs: &mut [MessageExt]) {
    let cfg = read_cfg(inner);
    let group_topic = MixAll::get_retry_topic(&cfg.consumer_group);
    for msg in msgs.iter_mut() {
        let retry_topic = msg.get_property(PROPERTY_RETRY_TOPIC).map(str::to_string);
        if let Some(retry_topic) = retry_topic {
            if msg.topic == group_topic {
                msg.topic = retry_topic;
            }
        }
        if !cfg.namespace.is_empty() {
            msg.topic = NamespaceUtil::without_namespace(&msg.topic, &cfg.namespace);
        }
    }
}

/// Python `_build_consume_hook_context`：Java 初值 success=False、props 为空。
fn build_consume_hook_context(
    inner: &Inner,
    msgs: &[MessageExt],
    mq: &MessageQueue,
) -> ConsumeMessageContext {
    let group = read_cfg(inner).consumer_group;
    let mut context = ConsumeMessageContext::new(&group, Some(msgs.to_vec()), Some(mq.clone()));
    context.success = false;
    context.props = Some(Default::default());
    context.access_channel = Some(AccessChannel::Local.name().to_string());
    context
}

/// Python `_consume_return_type`（决定轨迹的 contextCode）。
///
/// Python 的 `succeeded` 形参在这里用不到 —— 非 failed 的两种情况都返回 SUCCESS，
/// 故本函数不收这个参数。
fn consume_return_type(
    status: Option<&str>,
    has_exception: bool,
    consume_rt_ms: i64,
    consume_timeout_minutes: i64,
    failed: bool,
) -> ConsumeReturnType {
    if status.is_none() {
        return if has_exception {
            ConsumeReturnType::Exception
        } else {
            ConsumeReturnType::ReturnNull
        };
    }
    if consume_rt_ms >= consume_timeout_minutes * 60 * 1000 {
        return ConsumeReturnType::TimeOut;
    }
    if failed {
        ConsumeReturnType::Failed
    } else {
        // Python 这里还写了 `if succeeded: SUCCESS`，两条分支同值，故合并
        ConsumeReturnType::Success
    }
}

/// Python `_record_consume_stats`（Java ConsumeRequest.run：RT 恒记，OK/FAILED 按结果）。
fn record_consume_stats(inner: &Inner, topic: &str, msg_count: usize, begin_ms: i64, failed: bool) {
    let Some(stats) = lock(&inner.stats).clone() else {
        return;
    };
    let group = read_cfg(inner).consumer_group;
    let rt = current_time_millis() - begin_ms;
    let msgs = i64::try_from(msg_count).unwrap_or(0);
    if failed {
        stats.inc_consume_failed_tps(&group, topic, msgs);
    } else {
        stats.inc_consume_ok_tps(&group, topic, msgs);
    }
    stats.inc_consume_rt(&group, topic, rt);
}

/// Python `_finish_consume_hook`：写回 returnType/status/success 并触发 after 钩子。
fn finish_consume_hook(
    inner: &Inner,
    ctx: &mut ConsumeMessageContext,
    status: Option<ConsumeConcurrentlyStatus>,
    has_exception: bool,
    begin_ms: i64,
    failed: bool,
    succeeded: bool,
) {
    let cfg = read_cfg(inner);
    let rt = current_time_millis() - begin_ms;
    let ret = consume_return_type(
        status.map(|s| match s {
            ConsumeConcurrentlyStatus::ConsumeSuccess => "CONSUME_SUCCESS",
            ConsumeConcurrentlyStatus::ReconsumeLater => "RECONSUME_LATER",
        }),
        has_exception,
        rt,
        cfg.consume_timeout,
        failed,
    );
    ctx
        .props
        .get_or_insert_with(Default::default)
        .insert("ConsumeContextType".to_string(), ret.name().to_string());
    ctx.status = Some(
        status
            .map(|s| s.name().to_string())
            .unwrap_or_else(|| "None".to_string()),
    );
    ctx.success = succeeded;
    execute_consume_hook_after(&inner.consume_hooks, ctx);
}

/// 调并发监听器。返回 `(status, has_exception)`。
///
/// 与 Python 的两处口径差别：
/// 1. Python 的 `status is None`（listener 不返回）在 Rust 不可能出现，因为 trait 的
///    返回类型是非 Optional 的枚举；**没有设置监听器**时 Python 抛 AttributeError，
///    这里同样按「异常 → RECONSUME_LATER」处理，`status=None` 就是这条路径。
/// 2. 监听器是用户代码，可能阻塞（Python 就让它占着分发线程）。这里放进
///    `spawn_blocking`：分发循环仍然是「等这一批消费完才继续」的串行语义
///    （与 Python 一致），但不会占住 tokio 的 worker 线程。监听器 panic 会被
///    `JoinError` 捕获，语义正好等于 Python 的「抛异常 → RECONSUME_LATER」。
async fn call_concurrently_listener(
    inner: &Arc<Inner>,
    msgs: &[MessageExt],
    context: &mut ConsumeConcurrentlyContext,
) -> (Option<ConsumeConcurrentlyStatus>, bool) {
    let listener = match lock(&inner.listener).as_ref().cloned() {
        Some(MessageListener::Concurrently(l)) => l,
        Some(MessageListener::Orderly(_)) => {
            // 顺序监听器 + 并发调用：Python 里 isinstance 判过，不会走到这里
            return (None, true);
        }
        None => return (None, true),
    };
    let owned = msgs.to_vec();
    let mut ctx = context.clone();
    match tokio::task::spawn_blocking(move || {
        let status = listener.consume_message(&owned, &mut ctx);
        (status, ctx)
    })
    .await
    {
        Ok((status, ctx)) => {
            *context = ctx;
            (Some(status), false)
        }
        Err(e) => {
            rmq_debug!("listener error, treat as RECONSUME_LATER: {e}");
            (None, true)
        }
    }
}

/// 调顺序监听器（同上，返回 `(status, has_exception)`）。
async fn call_orderly_listener(
    inner: &Arc<Inner>,
    msgs: &[MessageExt],
    context: &mut ConsumeOrderlyContext,
) -> (Option<ConsumeOrderlyStatus>, bool) {
    let listener = match lock(&inner.listener).as_ref().cloned() {
        Some(MessageListener::Orderly(l)) => l,
        _ => return (None, true),
    };
    let owned = msgs.to_vec();
    let mut ctx = context.clone();
    match tokio::task::spawn_blocking(move || {
        let status = listener.consume_message(&owned, &mut ctx);
        (status, ctx)
    })
    .await
    {
        Ok((status, ctx)) => {
            *context = ctx;
            (Some(status), false)
        }
        Err(e) => {
            rmq_debug!("orderly listener error (retry in place): {e}");
            (None, true)
        }
    }
}

/// Python `_consume_batch`：消费一个批次并处理回投/挂起，返回位点是否前进。
async fn consume_batch(
    inner: &Arc<Inner>,
    key: &str,
    mq: &MessageQueue,
    batch: Vec<MessageExt>,
) -> Result<bool> {
    let cfg = read_cfg(inner);
    let broadcast = cfg.message_model == MessageModel::BROADCASTING;
    let mut batch = batch;
    reset_retry_topic_and_namespace(inner, &mut batch);
    let orderly = lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly);

    if orderly {
        // ---- 顺序消费（Java ConsumeMessageOrderlyService）----
        let mut ocontext = ConsumeOrderlyContext::new(Some(mq.clone()));
        let mut hook_ctx = if inner.consume_hooks.has_hooks() {
            Some(build_consume_hook_context(inner, &batch, mq))
        } else {
            None
        };
        if let Some(ctx) = hook_ctx.as_mut() {
            execute_consume_hook_before(&inner.consume_hooks, ctx);
        }
        let begin_ms = current_time_millis();
        let (status, has_exception) = call_orderly_listener(inner, &batch, &mut ocontext).await;
        let failed = !matches!(status, Some(ConsumeOrderlyStatus::Success));
        record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, failed);
        if let Some(ctx) = hook_ctx.as_mut() {
            let succeeded = matches!(status, Some(ConsumeOrderlyStatus::Success));
            let rt = current_time_millis() - begin_ms;
            let ret = consume_return_type(
                status.map(|s| s.name()),
                has_exception,
                rt,
                cfg.consume_timeout,
                failed,
            );
            ctx
                .props
                .get_or_insert_with(Default::default)
                .insert("ConsumeContextType".to_string(), ret.name().to_string());
            ctx.status = Some(
                status
                    .map(|s| s.name().to_string())
                    .unwrap_or_else(|| "None".to_string()),
            );
            ctx.success = succeeded;
            execute_consume_hook_after(&inner.consume_hooks, ctx);
        }
        if matches!(
            status,
            Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment)
        ) {
            {
                let mut state = lock(&inner.state);
                if let Some(dq) = state.pending.get_mut(key) {
                    for m in batch.iter().rev() {
                        dq.push_front(m.clone());
                    }
                }
            }
            let suspend = cfg.suspend_current_queue_millis();
            tokio::time::sleep(Duration::from_millis(suspend)).await;
            return Ok(false);
        }
        advance_consume_offset(inner, key, &batch);
        return Ok(true);
    }

    // ---- 并发消费（Java ConsumeMessageConcurrentlyService$ConsumeRequest.run）----
    let mut context = ConsumeConcurrentlyContext::new(Some(mq.clone()));
    let mut hook_ctx = if inner.consume_hooks.has_hooks() {
        Some(build_consume_hook_context(inner, &batch, mq))
    } else {
        None
    };
    if let Some(ctx) = hook_ctx.as_mut() {
        execute_consume_hook_before(&inner.consume_hooks, ctx);
    }
    let begin_ms = current_time_millis();
    let (status, has_exception) = call_concurrently_listener(inner, &batch, &mut context).await;
    let failed = matches!(status, Some(ConsumeConcurrentlyStatus::ReconsumeLater)) || status.is_none();
    let succeeded = matches!(status, Some(ConsumeConcurrentlyStatus::ConsumeSuccess));
    record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, failed);
    if let Some(ctx) = hook_ctx.as_mut() {
        finish_consume_hook(inner, ctx, status, has_exception, begin_ms, failed, succeeded);
    }
    let status = match status {
        Some(s) => s,
        None => ConsumeConcurrentlyStatus::ReconsumeLater,
    };
    if status == ConsumeConcurrentlyStatus::ConsumeSuccess {
        advance_consume_offset(inner, key, &batch);
        return Ok(true);
    }
    // RECONSUME_LATER：广播模式不回投（只告警，位点照常前进，重启后重新消费）；
    // 集群模式逐条回投 %RETRY%topic（延迟档位 3+reconsumeTimes；超限由 broker 转 %DLQ%）
    if broadcast {
        rmq_warn!(
            "BROADCASTING: message consume failed, no redelivery: {} msgs in {mq:?}",
            batch.len()
        );
        advance_consume_offset(inner, key, &batch);
        return Ok(true);
    }
    if send_back_batch(inner, &batch, context.delay_level_when_next_consume).await {
        advance_consume_offset(inner, key, &batch);
        return Ok(true);
    }
    // 回投失败：批次塞回队首稍后重试（Java 中这些消息不从 ProcessQueue 移除）
    {
        let mut state = lock(&inner.state);
        if let Some(dq) = state.pending.get_mut(key) {
            for m in batch.iter().rev() {
                dq.push_front(m.clone());
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(false)
}

/// Python `_send_back_batch`（Java processConsumeResult → sendMessageBack）。
async fn send_back_batch(
    inner: &Arc<Inner>,
    batch: &[MessageExt],
    delay_level_from_context: i32,
) -> bool {
    let mut ok = true;
    for msg in batch {
        // 重投次数在 MessageExt 线上格式第 13 字段（Java msg.getReconsumeTimes()），
        // broker 重投时 +1；不是 properties 键（PROPERTY_RECONSUME_TIME 是另一回事）。
        let mut delay_level = delay_level_from_context;
        if delay_level == 0 {
            // Java：delayLevelWhenNextConsume == 0 → 3 + reconsumeTimes
            delay_level = 3 + msg.reconsume_times;
        }
        if let Err(e) = send_message_back(inner, msg, delay_level, None) {
            rmq_debug!("send message back failed: {e}");
            ok = false;
        }
    }
    ok
}

/// Python `_advance_consume_offset`：取本批最大 queueOffset + 1，且**不回退**。
fn advance_consume_offset(inner: &Inner, key: &str, batch: &[MessageExt]) {
    let next_off = batch.iter().map(|m| m.queue_offset).max().unwrap_or(0) + 1;
    let mut state = lock(&inner.state);
    let cur = state.consume_offsets.get(key).copied().unwrap_or(0);
    state.consume_offsets.insert(key.to_string(), cur.max(next_off));
}

/// Python `send_message_back`（CONSUMER_SEND_MSG_BACK=36）。
fn send_message_back(
    inner: &Arc<Inner>,
    msg: &MessageExt,
    delay_level: i32,
    broker_name: Option<&str>,
) -> Result<()> {
    let client = require_client(inner)?;
    let cfg = read_cfg(inner);
    let broker = broker_name
        .map(str::to_string)
        .or_else(|| msg.broker_name.clone())
        .unwrap_or_default();
    let addr = client
        .broker_addr_of(&broker)
        .ok_or_else(|| Error::client(format!("broker {broker} not found")))?;
    // Java：maxReconsumeTimes == -1 时按 16 传给 broker（超限由 broker 转 %DLQ%）
    let max_reconsume = if cfg.max_reconsume_times == -1 {
        16
    } else {
        cfg.max_reconsume_times
    };
    let header = ConsumerSendMsgBackRequestHeader {
        offset: Some(msg.commit_log_offset),
        group: Some(cfg.consumer_group.clone()),
        delay_level: Some(delay_level),
        origin_msg_id: msg.msg_id.clone(),
        origin_topic: Some(msg.topic.clone()),
        unit_mode: Some(false),
        max_reconsume_times: Some(max_reconsume),
    };
    let mut request =
        RemotingCommand::create_request_command(request_code::CONSUMER_SEND_MSG_BACK, Some(Box::new(header)));
    let group = cfg.consumer_group.clone();
    let handle = inner
        .runtime
        .get()
        .cloned()
        .or_else(|| tokio::runtime::Handle::try_current().ok())
        .ok_or_else(|| Error::client("no tokio runtime to send message back"))?;
    handle.spawn(async move {
        match client.invoke_sync(&addr, &mut request, 5000).await {
            Ok(response) => {
                if let Err(e) = MQClientInstance::check_response(&response) {
                    rmq_debug!("send message back rejected: {e}");
                }
            }
            Err(e) => rmq_debug!("send message back failed, group={group}: {e}"),
        }
    });
    Ok(())
}

// ================================================================ broker 主动请求
//
// 220/221/307/309 的处理入口注册在 MQClientInstance 上（它按 consumerGroup 找
// **对应的**消费者），这里只实现被回调的那一侧。40 是唯一由消费者自己注册的，
// 因为它的动作就是「叫醒本消费者的 rebalance 循环」。

/// Python `_on_consumer_ids_changed`：broker 通知消费组实例变化 → 立即重算。
impl RegisteredConsumer for DefaultMQPushConsumer {
    fn client_id(&self) -> String {
        self.client_id()
    }

    fn consumer_group(&self) -> String {
        self.consumer_group()
    }

    fn consume_type(&self) -> String {
        ConsumeType::CONSUME_PASSIVELY.to_string()
    }

    fn message_model(&self) -> String {
        self.config().message_model
    }

    fn consume_from_where(&self) -> String {
        self.config().consume_from_where
    }

    fn is_unit_mode(&self) -> bool {
        // Python 没有 unit mode（Java isUnitMode() 亦恒 false）
        false
    }

    fn subscription(&self) -> Vec<String> {
        lock(&self.inner.state)
            .subscription_data
            .iter()
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn subscriptions(&self) -> Vec<SubscriptionData> {
        self.subscriptions()
    }

    /// 实例收到 broker 的 `NOTIFY_CONSUMER_IDS_CHANGED(40)` 后扇出到这里
    /// （Java `MQClientInstance#rebalanceImmediately` → `RebalanceService#wakeup`）。
    fn rebalance_immediately(&self) {
        self.inner.rebalance_now.store(true, Ordering::SeqCst);
        self.inner.rebalance_signal.notify_waiters();
    }

    /// Python `adjust_thread_pool`：⚠ **Java 5.5.1 里这是 no-op**
    /// （`AbstractConsumeMessageService:70-75` 的 inc/decCorePoolSize 是空方法体）。
    /// 保留阈值比较与日志只为让 `msgAccCnt` 可观测，**不要「顺手修好」**；
    /// 真正生效的是 [`DefaultMQPushConsumer::update_core_pool_size`]。
    fn adjust_thread_pool(&self) {
        let acc_total = self.compute_accumulation_total();
        let threshold = self.config().adjust_thread_pool_nums_threshold;
        if acc_total >= threshold {
            rmq_debug!("adjustThreadPool: acc={acc_total} >= incThreshold={threshold} (inc is a no-op upstream)");
        }
        if acc_total < threshold * 4 / 5 {
            rmq_debug!("adjustThreadPool: acc={acc_total} < decThreshold={}/0.8 (dec is a no-op upstream)", threshold);
        }
    }

    fn reset_offset(
        self: Arc<Self>,
        topic: String,
        offset_table: Vec<(MessageQueue, i64)>,
    ) -> ConsumerFuture<()> {
        Box::pin(async move {
            if topic.is_empty() || offset_table.is_empty() {
                return Ok(());
            }
            let mut hit: Vec<(MessageQueue, String)> = Vec::new();
            let mut new_offsets: Vec<(String, i64)> = Vec::new();
            {
                let state = lock(&self.inner.state);
                for (key, mq) in &state.mq_map {
                    if mq.topic != topic {
                        continue;
                    }
                    let Some(off) = offset_table.iter().find(|(m, _)| m == mq).map(|(_, o)| *o)
                    else {
                        continue;
                    };
                    hit.push((mq.clone(), key.clone()));
                    new_offsets.push((key.clone(), off));
                }
            }
            {
                let mut state = lock(&self.inner.state);
                for (key, off) in new_offsets {
                    state.pending.remove(&key); // 等价 ProcessQueue.clear()
                    state.offset_table.remove(&key);
                    state.consume_offsets.insert(key, off);
                }
            }
            if hit.is_empty() {
                return Ok(());
            }
            // Java 用 5s 等并发消费跑完；这里缩短以免阻塞读线程（220 是 oneway）。
            tokio::time::sleep(Duration::from_millis(200)).await;
            let revoked: Vec<(MessageQueue, Option<i64>)> =
                hit.iter().map(|(mq, _)| (mq.clone(), None)).collect();
            self.on_queues_revoked(&revoked).await;
            if let Err(e) = self.do_rebalance().await {
                rmq_debug!("rebalance after reset offset failed: {e}");
            }
            rmq_info!(
                "reset offset applied, group={} topic={topic} queues={}",
                self.consumer_group(),
                hit.len()
            );
            Ok(())
        })
    }

    fn get_consumer_status(&self, topic: Option<&str>) -> Vec<(MessageQueue, i64)> {
        // Java 返回 offsetStore.cloneOffsetTable(topic)，即**已消费位点**（不是拉取游标）
        let state = lock(&self.inner.state);
        let mut out: Vec<(MessageQueue, i64)> = state
            .consume_offsets
            .iter()
            .filter_map(|(k, off)| {
                let mq = state.mq_map.get(k)?;
                if let Some(topic) = topic {
                    if mq.topic != topic {
                        return None;
                    }
                }
                Some((mq.clone(), *off))
            })
            .collect();
        out.sort_by_key(|(mq, _)| mq_sort_key(mq));
        out
    }

    fn consumer_running_info(&self) -> ConsumerRunningInfo {
        let cfg = self.config();
        let mut properties = StringMap::new();
        properties.insert(
            ConsumerRunningInfo::PROP_NAMESERVER_ADDR,
            format!("{};", cfg.name_server_addrs.join(";")),
        );
        properties.insert(
            ConsumerRunningInfo::PROP_CONSUME_TYPE,
            ConsumeType::CONSUME_PASSIVELY,
        );
        let orderly = lock(&self.inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        properties.insert(
            ConsumerRunningInfo::PROP_CONSUME_ORDERLY,
            orderly.to_string(),
        );
        properties.insert(
            ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE,
            self.get_core_pool_size().to_string(),
        );
        properties.insert(
            ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP,
            self.inner.start_time_millis.load(Ordering::SeqCst).to_string(),
        );
        properties.insert(ConsumerRunningInfo::PROP_CLIENT_VERSION, "V5_5_1");
        let mut info = ConsumerRunningInfo {
            properties,
            ..Default::default()
        };
        let state = lock(&self.inner.state);
        for (key, mq) in &state.mq_map {
            let pqi = ProcessQueueInfo {
                commit_offset: state.consume_offsets.get(key).copied().unwrap_or(0),
                cached_msg_count: i32::try_from(
                    state.pending.get(key).map_or(0, |dq| dq.len()),
                )
                .unwrap_or(i32::MAX),
                droped: false,
                ..Default::default()
            };
            info.mq_table.push((
                MessageQueueKey::new(&mq.topic, &mq.broker_name, mq.queue_id),
                pqi.to_json_value(),
            ));
        }
        if cfg.pop_mode {
            // Python `consumer.py:1785-1792` 用 `self._mq_map.get(key)` 反查队列，但
            // pop 路径从不写 `_mq_map`（只有拉取入队时写），所以参考版的 mqPopTable
            // 恒空。这里按 Java `DefaultMQPushConsumerImpl.consumerRunningInfo:1473`
            // 的口径——popProcessQueueTable 本来就以 MessageQueue 为键——从当前分配
            // 反查，使运维视图真的能看到弹出队列。
            let mut mq_of: BTreeMap<String, &MessageQueue> = BTreeMap::new();
            for mq in &state.assigned {
                mq_of.insert(mq_key(mq), mq);
            }
            for (key, mq) in &state.mq_map {
                mq_of.insert(key.clone(), mq);
            }
            for (key, pq) in &state.pop_queues {
                let Some(mq) = mq_of.get(key) else {
                    continue;
                };
                let pqi = ProcessQueueInfo {
                    cached_msg_count: pq.wait_ack_count(),
                    droped: pq.is_dropped(),
                    ..Default::default()
                };
                info.mq_pop_table.push((
                    MessageQueueKey::new(&mq.topic, &mq.broker_name, mq.queue_id),
                    pqi.to_json_value(),
                ));
            }
        }
        info.subscription_set = state
            .subscription_data
            .iter()
            .map(|(_, sub)| sub.to_json_value())
            .collect();
        // statusTable：Java consumeStatus(group, topic) 的 minute 快照
        let stats = lock(&self.inner.stats).clone();
        for (topic, _) in &state.subscription_data {
            let status = match &stats {
                Some(stats) => stats.consume_status(&cfg.consumer_group, topic),
                None => crate::remoting::protocol::body::ConsumeStatus::default(),
            };
            info.status_table
                .push((topic.clone(), status.to_json_value()));
        }
        info
    }

    /// Python `consume_message_directly`（Java
    /// `ConsumeMessageConcurrentlyService.consumeMessageDirectly`，309）。
    fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<String>,
    ) -> Result<ConsumeMessageDirectlyResult> {
        // Default 已带上 Java 的初值（order=false、autoCommit=true）
        let mut result = ConsumeMessageDirectlyResult::default();
        let mq = MessageQueue::new(&msg.topic, broker_name.as_deref().unwrap_or(""), msg.queue_id);
        let mut msgs = vec![msg];
        reset_retry_topic_and_namespace(&self.inner, &mut msgs);
        let mut context = ConsumeConcurrentlyContext::new(Some(mq.clone()));
        let begin = current_time_millis();
        // ⚠ 这里是**同步**接口（`RegisteredConsumer::consume_message_directly` 不是
        // async），不能像正常消费路径那样 `spawn_blocking`；Python 同样在 remoting
        // 读线程上直接调 listener。差别只在这一条路径上用户代码会占住读线程。
        let status = match lock(&self.inner.listener).as_ref().cloned() {
            Some(MessageListener::Concurrently(listener)) => {
                Some(listener.consume_message(&msgs, &mut context))
            }
            Some(MessageListener::Orderly(listener)) => {
                let mut octx = ConsumeOrderlyContext::new(Some(mq));
                match listener.consume_message(&msgs, &mut octx) {
                    ConsumeOrderlyStatus::Success => {
                        Some(ConsumeConcurrentlyStatus::ConsumeSuccess)
                    }
                    ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                        Some(ConsumeConcurrentlyStatus::ReconsumeLater)
                    }
                }
            }
            // Python 的 `else None`：没有监听器 → 状态为 None → CR_RETURN_NULL
            None => None,
        };
        result.consume_result = Some(match status {
            Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => CMResult::CR_SUCCESS,
            Some(ConsumeConcurrentlyStatus::ReconsumeLater) => CMResult::CR_LATER,
            None => CMResult::CR_RETURN_NULL,
        }
        .to_string());
        result.spent_time_mills = current_time_millis() - begin;
        let _ = msgs;
        Ok(result)
    }

    fn persist_consumer_offset(self: Arc<Self>) -> ConsumerFuture<()> {
        Box::pin(async move {
            if let Ok(client) = require_client(&self.inner) {
                persist_offsets_once(&self.inner, &client).await;
            }
            Ok(())
        })
    }
}

// ================================================================ 对外能力与弹性

impl DefaultMQPushConsumer {
    /// Python `get_consumer_group`。
    pub fn get_consumer_group(&self) -> String {
        self.consumer_group()
    }

    /// Python `fetch_subscribe_message_queues`。
    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = require_client(&self.inner)?;
        let publish = client.get_topic_publish_info(topic, false).await?;
        Ok(publish
            .msg_queue_list()
            .into_iter()
            .map(|q| MessageQueue::new(&q.topic, &q.broker_name, q.queue_id))
            .collect())
    }

    /// Python `msg_acc_cnt(key=None)`：给 key 读单队列，不给则求和。
    pub fn msg_acc_cnt(&self, key: Option<&str>) -> i64 {
        let state = lock(&self.inner.state);
        match key {
            None => state.msg_acc_cnt.values().sum(),
            Some(key) => state.msg_acc_cnt.get(key).copied().unwrap_or(0),
        }
    }

    /// Python `compute_accumulation_total`：所有 ProcessQueue 的 `msgAccCnt` 之和。
    pub fn compute_accumulation_total(&self) -> i64 {
        self.msg_acc_cnt(None)
    }

    /// Python `update_core_pool_size`（Java `AbstractConsumeMessageService:63-71`）。
    ///
    /// Java 的守卫逐条照抄：`ownsConsumeExecutor && 0 < core <= Short.MAX_VALUE
    /// && core < consumeThreadMax`，任一条不满足就**静默忽略**（Java 也不抛）。
    /// 返回值只给单测断言「是否真的生效」，Java 侧无返回值。
    pub fn update_core_pool_size(&self, core_pool_size: i32) -> bool {
        if !self.inner.owns_consume_executor.load(Ordering::SeqCst) {
            return false;
        }
        if !(0 < core_pool_size && core_pool_size <= SHORT_MAX_VALUE) {
            return false;
        }
        if core_pool_size >= self.config().consume_thread_max {
            return false;
        }
        self.inner.core_pool_size.store(core_pool_size, Ordering::SeqCst);
        self.apply_core_pool_size();
        true
    }

    /// Python `get_core_pool_size`：非自建执行器时返回 -1。
    pub fn get_core_pool_size(&self) -> i32 {
        if !self.inner.owns_consume_executor.load(Ordering::SeqCst) {
            return -1;
        }
        match lock(&self.inner.pop_executor).as_ref() {
            Some(executor) => executor.get_core_pool_size(),
            None => self.inner.core_pool_size.load(Ordering::SeqCst),
        }
    }

    /// Python `_apply_core_pool_size`：把声明值落到真实执行器（没有就只记声明值）。
    fn apply_core_pool_size(&self) {
        let core = self.inner.core_pool_size.load(Ordering::SeqCst);
        if let Some(executor) = lock(&self.inner.pop_executor).as_ref() {
            if let Err(e) = executor.set_core_pool_size(core) {
                rmq_debug!("set_core_pool_size({core}) failed: {e}");
            }
        }
    }

    /// 已拉未消费缓冲的总条数（真机验证流控/背压用）。
    pub fn buffered_message_count(&self) -> usize {
        lock(&self.inner.state)
            .pending
            .values()
            .map(VecDeque::len)
            .sum()
    }

    /// 已弹未 ack 的总条数（POP 模式，真机验证用）。
    pub fn wait_ack_count(&self) -> i32 {
        lock(&self.inner.state)
            .pop_queues
            .values()
            .map(|pq| pq.wait_ack_count())
            .sum()
    }

    /// 触发一次立即重平衡（等价 broker 的 40 通知；测试与运维手动触发用）。
    pub fn rebalance_immediately(&self) {
        self.request_rebalance();
    }
}

impl ConsumerConfig {
    /// `suspend_current_queue_time_millis` 的取整小工具（i64 -> u64 毫秒）。
    fn suspend_current_queue_millis(&self) -> u64 {
        u64::try_from(self.suspend_current_queue_time_millis.max(0)).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::Message;
    use crate::remoting::protocol::heartbeat::ExpressionType;

    fn ext(topic: &str, tags: Option<&str>) -> MessageExt {
        let mut m = MessageExt::new();
        m.topic = topic.to_string();
        if let Some(t) = tags {
            let mut src = Message::new(topic, Some(b"x"));
            src.set_tags(t);
            m.properties = src.properties.clone();
        }
        m
    }

    fn sub(topic: &str, expression: &str) -> SubscriptionData {
        let mut s = SubscriptionData::new(topic, expression);
        if expression != "*" {
            s.tags_set = expression.split("||").map(str::to_string).collect();
        }
        s
    }

    #[test]
    fn sort_key_orders_topic_then_broker_then_queue() {
        let mut mqs = vec![
            MessageQueue::new("b", "broker-a", 1),
            MessageQueue::new("a", "broker-b", 0),
            MessageQueue::new("a", "broker-a", 3),
            MessageQueue::new("a", "broker-a", 0),
        ];
        sort_mqs(&mut mqs);
        let keys: Vec<(String, String, i32)> = mqs.iter().map(mq_sort_key).collect();
        assert_eq!(
            keys,
            vec![
                ("a".to_string(), "broker-a".to_string(), 0),
                ("a".to_string(), "broker-a".to_string(), 3),
                ("a".to_string(), "broker-b".to_string(), 0),
                ("b".to_string(), "broker-a".to_string(), 1),
            ]
        );
    }

    #[test]
    fn tag_filter_keeps_only_string_matches() {
        let msgs = vec![ext("T", Some("TagA")), ext("T", Some("TagB")), ext("T", None)];
        let kept = client_side_tag_filter(Some(&sub("T", "TagA")), msgs.clone());
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].get_tags(), Some("TagA"));

        // 订阅 "*"：tags_set 为空 ⇒ 不过滤（连无 tag 的消息也留着）。
        assert_eq!(client_side_tag_filter(Some(&sub("T", "*")), msgs.clone()).len(), 3);

        // classFilterMode：Java 的守卫同样跳过二次过滤。
        let mut class_sub = sub("T", "TagA");
        class_sub.class_filter_mode = true;
        assert_eq!(client_side_tag_filter(Some(&class_sub), msgs.clone()).len(), 3);

        // 没有订阅信息时原样返回。
        assert_eq!(client_side_tag_filter(None, msgs.clone()).len(), 3);
        assert!(client_side_tag_filter(Some(&sub("T", "TagA")), Vec::new()).is_empty());
    }

    #[test]
    fn delivery_filter_combines_tag_filter_and_hooks() {
        let hooks = Arc::new(crate::client::hook::FilterMessageHookList::new());
        let msgs = vec![ext("T", Some("TagA")), ext("T", Some("TagC"))];
        let out = filter_messages_for_delivery(
            "G",
            &hooks,
            &MessageQueue::new("T", "broker-a", 0),
            Some(&sub("T", "TagA")),
            msgs,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get_tags(), Some("TagA"));
    }

    #[test]
    fn selector_factories_match_expression_type() {
        assert_eq!(
            MessageSelector::by_tag("a||b"),
            MessageSelector {
                selector_type: ExpressionType::TAG.to_string(),
                expression: "a||b".to_string()
            }
        );
        let sql = MessageSelector::by_sql("a > 1");
        assert_eq!(sql.selector_type, ExpressionType::SQL92);
        assert_eq!(sql.sub_expression(), "a > 1");
    }

    #[test]
    fn pop_process_queue_tracks_wait_ack() {
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(3);
        assert_eq!(pq.wait_ack_count(), 3);
        assert_eq!(pq.ack(), 2);
        assert_eq!(pq.ack(), 1);
        // Python 口径：dec_found_msg(正数) = 减少那么多条。
        pq.dec_found_msg(1);
        assert_eq!(pq.wait_ack_count(), 0);

        assert!(!pq.is_dropped());
        pq.set_dropped(true);
        assert!(pq.is_dropped());

        pq.touch(1234);
        assert_eq!(pq.last_pop_timestamp.load(Ordering::SeqCst), 1234);
    }

    #[test]
    fn offset_table_sorted_by_queue_key() {
        let table = offset_table_to_sorted(vec![
            (MessageQueue::new("T", "broker-b", 0), 5),
            (MessageQueue::new("T", "broker-a", 1), 7),
            (MessageQueue::new("T", "broker-a", 0), 3),
        ]);
        let pairs: Vec<((String, String, i32), i64)> = table.into_iter().collect();
        assert_eq!(pairs[0].0, ("T".to_string(), "broker-a".to_string(), 0));
        assert_eq!(pairs[0].1, 3);
        assert_eq!(pairs[2].1, 5);
    }

    // ---------------- 配置与启动校验 ----------------

    /// 默认值逐项对照 `python/rocketmq/client/consumer.py`（`DefaultMQPushConsumer.__init__`）。
    #[test]
    fn config_defaults_match_the_python_reference() {
        let cfg = ConsumerConfig::default();
        assert_eq!(cfg.consumer_group, MixAll::DEFAULT_CONSUMER_GROUP);
        assert_eq!(cfg.instance_name, "DEFAULT");
        assert_eq!(cfg.client_id, None);
        assert!(cfg.name_server_addrs.is_empty());
        assert_eq!(cfg.tls_enable, None);
        assert_eq!(cfg.message_model, MessageModel::CLUSTERING);
        assert_eq!(
            cfg.consume_from_where,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET
        );
        assert_eq!((cfg.consume_thread_min, cfg.consume_thread_max), (20, 64));
        assert_eq!(cfg.adjust_thread_pool_nums_threshold, 100_000);
        assert_eq!(cfg.consume_concurrently_max_span, 2000);
        assert_eq!(
            (cfg.pull_threshold_for_queue, cfg.pull_threshold_size_for_queue),
            (1000, 100)
        );
        // 主题级阈值默认关闭（Java -1）
        assert_eq!(
            (cfg.pull_threshold_for_topic, cfg.pull_threshold_size_for_topic),
            (-1, -1)
        );
        assert_eq!(cfg.pull_interval, 0);
        assert_eq!(
            (cfg.pull_timeout_millis, cfg.pull_suspend_timeout_millis),
            (30_000, 20_000)
        );
        assert_eq!(cfg.consume_message_batch_max_size, 1);
        assert_eq!(cfg.pull_batch_size, 32);
        assert_eq!(cfg.pull_batch_size_in_bytes, 256 * 1024);
        assert_eq!(cfg.max_reconsume_times, -1);
        assert_eq!(cfg.suspend_current_queue_time_millis, 1000);
        // consumeTimeout 的单位是**分钟**（只用于 ConsumeReturnType::TimeOut）
        assert_eq!(cfg.consume_timeout, 15);
        assert!(cfg.client_rebalance);
        assert_eq!(cfg.heartbeat_interval_millis, 30_000);
        assert!(cfg.heartbeat_enabled);
        // 默认走长轮询 + 位点，而不是 POP + ack
        assert!(!cfg.pop_mode);
        assert_eq!(cfg.pop_invisible_time, 60_000);
        assert_eq!((cfg.pop_batch_nums, cfg.pop_threshold_for_queue), (32, 96));
        assert_eq!(cfg.pop_delay_level.to_vec(), POP_DELAY_LEVEL.to_vec());
        assert_eq!(
            (cfg.pop_poll_time_millis, cfg.pop_timeout_millis),
            (15_000, 25_000)
        );
        // POP 长轮询挂起时长必须短于请求超时，否则每次拉取都必然客户端超时
        assert!(cfg.pop_poll_time_millis < cfg.pop_timeout_millis);
        assert!(!cfg.enable_trace);
        assert_eq!(cfg.trace_topic, None);
        assert_eq!(cfg.trace_msg_batch_num, 10);
        // CONSUME_FROM_TIMESTAMP 的起点：默认「30 分钟前」，格式 yyyyMMddHHmmss
        assert_eq!(cfg.consume_timestamp.len(), 14);
        let past = consume_timestamp_millis(&cfg.consume_timestamp).unwrap();
        assert!((past..current_time_millis()).contains(&(current_time_millis() - 1000)));
        // 回归守卫：14 位纯数字是**本地墙钟日期**，不能被当成 epoch（否则是公元 2611 年）
        let expected = {
            use chrono::TimeZone;
            chrono::Local
                .with_ymd_and_hms(2023, 1, 1, 0, 0, 0)
                .single()
                .expect("local 2023-01-01 00:00:00 should resolve")
        };
        assert_eq!(
            consume_timestamp_millis("20230101000000").unwrap(),
            expected.timestamp_millis()
        );
        // 解析不了的字符串必须硬失败（Java checkConfig :1058），不能静默回落
        let err = consume_timestamp_millis("not-a-time").unwrap_err();
        assert!(
            format!("{err}").contains("consumeTimestamp is invalid"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn blank_consumer_group_is_rejected() {
        // Python：`MQClientException("consumerGroup is empty")`
        for group in ["", "   ", "\t\n"] {
            assert!(
                DefaultMQPushConsumer::new(group).is_err(),
                "group {group:?} must be rejected"
            );
        }
        assert!(DefaultMQPushConsumer::new("G").is_ok());
    }

    /// Java `DefaultMQPushConsumer:89`（字段默认 `new AllocateMessageQueueAveragely()`）
    /// 与 `:196-202`（getter/setter）。
    /// ⚠ Python/C++/.NET 那条「置 null 后 checkConfig 拒绝启动」在这里由
    /// `Arc<dyn ...>` 表达成「类型上不可表示」，所以没有对应的拒绝分支。
    #[test]
    fn push_consumer_exposes_the_allocate_strategy() {
        use crate::client::allocate_strategy::{
            AllocateMessageQueueAveragelyByCircle, AllocateMessageQueueByConfig,
        };

        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert_eq!(
            consumer.allocate_message_queue_strategy().get_name(),
            "AVG",
            "默认策略必须是 AVG"
        );

        consumer.set_allocate_message_queue_strategy(Arc::new(AllocateMessageQueueAveragelyByCircle));
        assert_eq!(
            consumer.allocate_message_queue_strategy().get_name(),
            "AVG_BY_CIRCLE",
            "策略可替换，getter 读回同一个"
        );

        let by_config = Arc::new(AllocateMessageQueueByConfig::new(vec![MessageQueue::new(
            "T", "broker-a", 1,
        )]));
        consumer.set_allocate_message_queue_strategy(by_config.clone());
        // 存的是共享引用：换进消费者之后再改列表依然对所有 rebalance 生效（Java/Python 同）
        by_config.set_message_queue_list(vec![
            MessageQueue::new("T", "broker-a", 0),
            MessageQueue::new("T", "broker-a", 1),
        ]);
        let strategy = consumer.allocate_message_queue_strategy();
        assert_eq!(strategy.get_name(), "CONFIG");
        assert_eq!(
            strategy
                .allocate("G", "cid", &[], &["cid".to_string()])
                .unwrap()
                .len(),
            2
        );
    }

    /// Python `getMQQueueCacheKey`：三段**直接拼接**，无分隔符。
    #[test]
    fn mq_key_concatenates_topic_broker_and_queue_id() {
        assert_eq!(
            mq_key(&MessageQueue::new("Topic", "broker-a", 3)),
            "Topicbroker-a3"
        );
    }

    struct NoopListener;

    impl MessageListenerConcurrently for NoopListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    struct NoopOrderlyListener;

    impl MessageListenerOrderly for NoopOrderlyListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeOrderlyContext,
        ) -> ConsumeOrderlyStatus {
            ConsumeOrderlyStatus::Success
        }
    }

    /// listener 是**存**在 `Inner` 里而不是取走：投递、重平衡、运维接口都要反复读它，
    /// 而 `is_orderly` 决定拉取循环是否要求 broker 锁（`LOCK_BATCH_MQ`）。
    #[test]
    fn listener_kind_is_recorded_and_not_consumed() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert!(lock(&consumer.inner.listener).is_none());

        consumer.set_message_listener_concurrently(Arc::new(NoopListener));
        for _ in 0..2 {
            let listener = lock(&consumer.inner.listener).clone();
            assert_eq!(listener.as_ref().map(MessageListener::is_orderly), Some(false));
        }

        // 换成分区顺序 listener：覆盖而不是追加
        consumer.set_message_listener_orderly(Arc::new(NoopOrderlyListener));
        let listener = lock(&consumer.inner.listener).clone();
        assert_eq!(listener.as_ref().map(MessageListener::is_orderly), Some(true));
    }

    /// `start()` 的三道校验必须在任何网络动作之前（Python `start()` 开头同样先校验）。
    ///
    /// 名字服务地址用 `127.0.0.1:1`：万一校验顺序被改坏，测试会失败而不是连到真集群
    /// 消费别人的消息。
    #[tokio::test]
    async fn start_validates_configuration_before_any_network_io() {
        if DefaultTopAddressing::is_configured() {
            // 环境里配了域名寻址，会绕过 name_server_addrs 校验，跳过第一轮断言
            return;
        }
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert!(
            consumer.start().await.is_err(),
            "既无名字服务地址又无域名寻址"
        );
        assert!(!consumer.is_started(), "失败的 start 必须回滚 started 标志");

        consumer.set_namesrv_addr("127.0.0.1:1");
        assert!(consumer.start().await.is_err(), "未订阅");
        assert!(!consumer.is_started());

        consumer.subscribe("T", "TagA").unwrap();
        assert!(consumer.start().await.is_err(), "未设置 listener");
        assert!(!consumer.is_started());
    }

    /// 组名校验是这三道校验里的第一道：地址、订阅、listener 全都不缺，照样因为组名失败，
    /// 报的也只是组名的错。用 `127.0.0.1:1` 兜底，顺序被改坏时会连不上而不是打到真集群。
    #[tokio::test]
    async fn start_rejects_bad_consumer_group_before_the_other_gates() {
        let long_group = std::iter::repeat_n('g', 121).collect::<String>();
        for (group, needle) in [
            (
                MixAll::DEFAULT_CONSUMER_GROUP,
                "consumerGroup can not equal DEFAULT_CONSUMER",
            ),
            ("bad group", "contains illegal characters"),
            (long_group.as_str(), "is longer than group max length"),
        ] {
            let consumer = DefaultMQPushConsumer::new(group).expect("构造不该提前拒绝");
            consumer.set_namesrv_addr("127.0.0.1:1");
            consumer.subscribe("T", "TagA").unwrap();
            consumer.set_message_listener_concurrently(Arc::new(NoopListener));
            let err = consumer.start().await.expect_err("非法组名必须本地失败");
            assert!(err.to_string().contains(needle), "{group}: {err}");
            assert!(!consumer.is_started(), "{group}: 失败的 start 必须回滚 started");
        }
    }

    /// 订阅表：表达式经 `FilterAPI` 解析，重复订阅覆盖，`unsubscribe` 删除。
    #[test]
    fn subscriptions_are_namespaced_and_overwritable() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.subscribe("T", "TagA||TagB").unwrap();
        let subs = consumer.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].topic, "T");
        assert_eq!(subs[0].sub_string, "TagA||TagB");
        assert_eq!(subs[0].expression_type, ExpressionType::TAG);

        // 同 topic 再订阅是**替换**，不是追加（Python `_subscription_data[topic] = ..`）
        consumer
            .subscribe_with_selector("T", &MessageSelector::by_sql("a > 1"))
            .unwrap();
        let subs = consumer.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].sub_string, "a > 1");
        assert_eq!(subs[0].expression_type, ExpressionType::SQL92);
        assert!(lock(&consumer.inner.state).subscription("T").is_some());

        // 命名空间在订阅时就拼进 topic（Python `_with_namespace`）
        consumer.update_config(|c| c.namespace = "Ns".to_string());
        consumer.subscribe("Other", "*").unwrap();
        assert!(
            lock(&consumer.inner.state)
                .subscription("Ns%Other")
                .is_some()
        );

        consumer.unsubscribe("Other");
        assert!(lock(&consumer.inner.state).subscription("Ns%Other").is_none());
    }

    // ---------------- 流控与统计 ----------------

    fn queue(topic: &str, broker: &str, id: i32) -> MessageQueue {
        MessageQueue::new(topic, broker, id)
    }

    fn sized_msg(store_size: i32, queue_offset: i64) -> MessageExt {
        let mut m = ext("T", None);
        m.store_size = store_size;
        m.queue_offset = queue_offset;
        m
    }

    fn stage(consumer: &DefaultMQPushConsumer, mq: &MessageQueue, msgs: Vec<MessageExt>) {
        let key = mq_key(mq);
        let mut state = lock(&consumer.inner.state);
        state.mq_map.insert(key.clone(), mq.clone());
        state.pending.insert(key, msgs.into_iter().collect());
    }

    /// 关掉除待测阈值以外的所有闸门，否则先命中的那条会掩盖被测分支。
    fn only(consumer: &DefaultMQPushConsumer, f: impl FnOnce(&mut ConsumerConfig)) {
        consumer.update_config(|c| {
            c.pull_threshold_for_queue = i32::MAX;
            c.pull_threshold_size_for_queue = 0;
            c.consume_concurrently_max_span = 0;
            c.pull_threshold_for_topic = -1;
            c.pull_threshold_size_for_topic = -1;
            f(c);
        });
    }

    /// Python `_flow_control_hit`（Java `ProcessQueue.putMessage` 的五个阈值）。
    #[test]
    fn flow_control_hits_each_threshold() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let triggered = || consumer.inner.flow_control_triggered.load(Ordering::SeqCst);

        // 缓冲为空：任何阈值都不触发
        assert!(!flow_control_hit(&consumer.inner, &mq, &key));
        assert_eq!(triggered(), 0);

        // 条数：>= pullThresholdForQueue；Java 的 max(1) 守卫配 0 也按 1 条算
        only(&consumer, |c| c.pull_threshold_for_queue = 3);
        stage(
            &consumer,
            &mq,
            vec![sized_msg(100, 0), sized_msg(100, 1), sized_msg(100, 2)],
        );
        assert!(flow_control_hit(&consumer.inner, &mq, &key), "count >= 3");
        assert_eq!(triggered(), 1);

        // 大小：pullThresholdSizeForQueue 单位是 MiB
        only(&consumer, |c| c.pull_threshold_size_for_queue = 1);
        assert!(!flow_control_hit(&consumer.inner, &mq, &key), "300B < 1MiB");
        stage(
            &consumer,
            &mq,
            vec![sized_msg(1024 * 1024, 0), sized_msg(1024 * 1024, 1)],
        );
        assert!(flow_control_hit(&consumer.inner, &mq, &key), "2MiB >= 1MiB");

        // 跨度：严格大于 consumeConcurrentlyMaxSpan（Java 同）
        only(&consumer, |c| c.consume_concurrently_max_span = 10);
        stage(&consumer, &mq, vec![sized_msg(1, 0), sized_msg(1, 100)]);
        assert!(flow_control_hit(&consumer.inner, &mq, &key), "span 100 > 10");
        only(&consumer, |c| c.consume_concurrently_max_span = 100);
        assert!(
            !flow_control_hit(&consumer.inner, &mq, &key),
            "span 100 不严格大于 100"
        );

        // 主题级条数：按 topic 汇总**所有**队列的缓冲（不是单队列）
        only(&consumer, |c| c.pull_threshold_for_topic = 2);
        stage(&consumer, &mq, vec![sized_msg(1, 0)]);
        assert!(!flow_control_hit(&consumer.inner, &mq, &key), "1 < 2");
        stage(&consumer, &mq, vec![sized_msg(1, 0), sized_msg(1, 1)]);
        assert!(flow_control_hit(&consumer.inner, &mq, &key), "2 >= 2");

        // 主题级大小：闸门复用队列级 size 开关（Python 同形，非笔误）
        only(&consumer, |c| {
            c.pull_threshold_size_for_queue = 100;
            c.pull_threshold_size_for_topic = 1;
        });
        stage(&consumer, &mq, vec![sized_msg(1024 * 1024 / 2, 0)]);
        assert!(!flow_control_hit(&consumer.inner, &mq, &key), "0.5MiB < 1MiB");
        stage(&consumer, &mq, vec![sized_msg(2 * 1024 * 1024, 0)]);
        assert!(flow_control_hit(&consumer.inner, &mq, &key), "2MiB >= 1MiB");

        // 默认配置：主题级阈值关闭，只看队列级
        let plain = DefaultMQPushConsumer::new("G").unwrap();
        stage(&plain, &mq, vec![sized_msg(1, 0)]);
        assert!(!flow_control_hit(&plain.inner, &mq, &key));
    }

    /// Java `ProcessQueue.msgAccCnt`：只看**本批最后一条**，且只接受正数。
    #[test]
    fn msg_acc_cnt_uses_the_last_message_only() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let key = mq_key(&queue("T", "broker-a", 0));
        let key = key.as_str();
        let table = || lock(&consumer.inner.state).msg_acc_cnt.clone();

        // 空批次 / 缺 MAX_OFFSET 属性：不写表
        update_msg_acc_cnt(&consumer.inner, key, &[]);
        assert!(table().is_empty());
        update_msg_acc_cnt(&consumer.inner, key, &[ext("T", None)]);
        assert!(table().is_empty());

        let with_max = |offset: i64, max: &str| {
            let mut m = ext("T", None);
            m.queue_offset = offset;
            m.put_property(PROPERTY_MAX_OFFSET, max);
            m
        };
        // 取最后一条：100 - 90 = 10（而不是第一条的 100 - 10 = 90）
        update_msg_acc_cnt(
            &consumer.inner,
            key,
            &[with_max(10, "100"), with_max(90, "100")],
        );
        assert_eq!(table().get(key), Some(&10));

        // 差值 <= 0 不更新（保留上一次的值）；非数字 MAX_OFFSET 忽略
        update_msg_acc_cnt(&consumer.inner, key, &[with_max(100, "100")]);
        assert_eq!(table().get(key), Some(&10));
        update_msg_acc_cnt(&consumer.inner, key, &[with_max(100, "abc")]);
        assert_eq!(table().get(key), Some(&10));
        update_msg_acc_cnt(&consumer.inner, key, &[with_max(10, "1000")]);
        assert_eq!(table().get(key), Some(&990));
    }

    // ---------------- POP ----------------

    /// Python `_is_pop_timeout`：拿不到 popTime/invisibleTime 时**一律算超时**，
    /// 于是走「重新计算可见性」而不是「原样重投」。
    #[test]
    fn pop_timeout_compares_the_invisible_window() {
        let now = current_time_millis();
        let one = || vec![ext("T", None)];
        assert!(is_pop_timeout(&[], now, 60_000));
        assert!(is_pop_timeout(&one(), 0, 60_000));
        assert!(is_pop_timeout(&one(), now - 1000, 0));
        assert!(!is_pop_timeout(&one(), now, 60_000), "仍在不可见窗口内");
        assert!(is_pop_timeout(&one(), now - 60_000, 60_000), "窗口已过");
    }

    /// Python `_process_pop_consume_result` 的两段 `range`：无论消费结果如何，
    /// 每条消息都必须**恰好** `pq.ack()` 一次 —— 漏一条，`waitAckCounter` 就永久
    /// 泄漏，攒到 `popThresholdForQueue` 后该队列再也不弹新消息。
    #[tokio::test]
    async fn every_pop_message_is_acked_whatever_the_result() {
        for (status, ack_index, len) in [
            (ConsumeConcurrentlyStatus::ConsumeSuccess, 1, 3),
            // ack_index 越界：钳到 len-1
            (ConsumeConcurrentlyStatus::ConsumeSuccess, 99, 3),
            // 默认 -1：一条都不算「成功 ack」，但其余每条仍要延长不可见时间
            (ConsumeConcurrentlyStatus::ConsumeSuccess, -1, 3),
            // RECONSUME_LATER：Python 依然遍历每一条
            (ConsumeConcurrentlyStatus::ReconsumeLater, -1, 3),
            (ConsumeConcurrentlyStatus::ReconsumeLater, 0, 1),
        ] {
            let consumer = DefaultMQPushConsumer::new("G").unwrap();
            let inner = consumer.inner.clone();
            let pq = PopProcessQueue::new();
            pq.inc_found_msg(len);
            let msgs: Vec<MessageExt> = (0..len).map(|_| ext("T", None)).collect();
            let mut context = ConsumeConcurrentlyContext::new(Some(queue("T", "broker-a", 0)));
            context.ack_index = ack_index;
            // 未 start()：ack / 改可见时间的 RPC 都退化成空操作，只验证计数清零
            process_pop_consume_result(&inner, status, &context, &msgs, &pq).await;
            assert_eq!(
                pq.wait_ack_count(),
                0,
                "{status:?} with ack_index={ack_index} over {len} msgs"
            );
        }
    }

    /// Python `_pop_ck_target`：POP_CK 缺失/畸形都当「无法 ack」，不 panic。
    #[test]
    fn pop_ck_target_rejects_missing_or_malformed_ck() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let inner = &consumer.inner;

        assert!(pop_ck_target(inner, &ext("T", None)).is_none());

        let mut empty_ck = ext("T", None);
        empty_ck.put_property(PROPERTY_POP_CK, "");
        assert!(pop_ck_target(inner, &empty_ck).is_none());

        let mut junk = ext("T", None);
        junk.put_property(PROPERTY_POP_CK, "only-three-segments");
        assert!(pop_ck_target(inner, &junk).is_none());

        // 真实 CK 是 **8 段**：broker 重编 CK 时会把 msgQueueOffset 追加为第 8 段
        // （`PopMessageProcessor:847`），而 `getQueueOffset` 正是取这一段。
        let mut ok = ext("T", None);
        ok.put_property(
            PROPERTY_POP_CK,
            "1234 1700000000000 60000 0 0 broker-a 3 99",
        );
        assert_eq!(
            pop_ck_target(inner, &ok),
            Some((
                "T".to_string(),
                "broker-a".to_string(),
                3,
                99,
                "1234 1700000000000 60000 0 0 broker-a 3 99".to_string()
            ))
        );
        // retryFlag=1：真实主题是 %RETRY%<group>_<topic>，不是消息上的 topic
        let mut retry = ok.clone();
        retry.put_property(
            PROPERTY_POP_CK,
            "1234 1700000000000 60000 0 1 broker-a 3 99",
        );
        let (topic, ..) = pop_ck_target(inner, &retry).unwrap();
        assert_eq!(topic, "%RETRY%G_T");
        // retryFlag=2：v2 格式 %RETRY%<group>+<topic>
        let mut retry_v2 = ok.clone();
        retry_v2.put_property(
            PROPERTY_POP_CK,
            "1234 1700000000000 60000 0 2 broker-a 3 99",
        );
        let (topic, ..) = pop_ck_target(inner, &retry_v2).unwrap();
        assert_eq!(topic, "%RETRY%G+T");
        // 只有 7 段：拿不到 offset ⇒ 无法 ack（Java 的 IllegalArgumentException 同口径）
        let mut seven = ok.clone();
        seven.put_property(PROPERTY_POP_CK, "1234 1700000000000 60000 0 0 broker-a 3");
        assert!(pop_ck_target(inner, &seven).is_none());
        // 未知 retryFlag 同样拒绝（getRetry 只认 0/1/2）
        let mut bad_flag = ok.clone();
        bad_flag.put_property(
            PROPERTY_POP_CK,
            "1234 1700000000000 60000 0 T broker-a 3 99",
        );
        assert!(pop_ck_target(inner, &bad_flag).is_none());
    }

    // ---------------- 消费结果与投递前还原 ----------------

    /// Python `_consume_return_type`（决定轨迹 contextCode）。
    #[test]
    fn consume_return_type_follows_the_python_branches() {
        // status 为 None：有异常 → EXCEPTION，无异常 → RETURNNULL
        assert_eq!(
            consume_return_type(None, true, 0, 15, true),
            ConsumeReturnType::Exception
        );
        assert_eq!(
            consume_return_type(None, false, 0, 15, false),
            ConsumeReturnType::ReturnNull
        );
        // 超时判定**优先于**成功/失败（Java 同：先比 rt >= timeout）
        assert_eq!(
            consume_return_type(Some("CONSUME_SUCCESS"), false, 15 * 60 * 1000, 15, true),
            ConsumeReturnType::TimeOut
        );
        assert_eq!(
            consume_return_type(Some("RECONSUME_LATER"), false, 15 * 60 * 1000 - 1, 15, true),
            ConsumeReturnType::Failed
        );
        assert_eq!(
            consume_return_type(Some("RECONSUME_LATER"), false, 100, 15, true),
            ConsumeReturnType::Failed
        );
        assert_eq!(
            consume_return_type(Some("CONSUME_SUCCESS"), false, 100, 15, false),
            ConsumeReturnType::Success
        );
        // 枚举名进轨迹属性，必须与 Java `ConsumeReturnType` 逐字一致
        assert_eq!(ConsumeReturnType::Success.name(), "SUCCESS");
        assert_eq!(ConsumeReturnType::TimeOut.name(), "TIME_OUT");
        assert_eq!(ConsumeReturnType::Exception.name(), "EXCEPTION");
        assert_eq!(ConsumeReturnType::ReturnNull.name(), "RETURNNULL");
        assert_eq!(ConsumeReturnType::Failed.name(), "FAILED");
        assert_eq!(ConsumeReturnType::Failed.code(), 4);
    }

    /// Python `_reset_retry_topic_and_namespace`：交给 listener 之前还原 topic。
    #[test]
    fn retry_topic_and_namespace_are_reset_before_delivery() {
        let consumer = DefaultMQPushConsumer::new("MyGroup").unwrap();
        let group_topic = MixAll::get_retry_topic("MyGroup");
        assert_eq!(group_topic, "%RETRY%MyGroup");

        // 只有「topic 确实是本组的重试主题」时才用 RETRY_TOPIC 属性还原
        let mut retried = ext(&group_topic, None);
        retried.put_property(PROPERTY_RETRY_TOPIC, "OrderTopic");
        let mut unrelated = ext("T", None);
        unrelated.put_property(PROPERTY_RETRY_TOPIC, "OrderTopic");
        let mut msgs = vec![retried.clone(), unrelated.clone()];
        reset_retry_topic_and_namespace(&consumer.inner, &mut msgs);
        assert_eq!(msgs[0].topic, "OrderTopic");
        assert_eq!(msgs[1].topic, "T", "非本组重试主题不动（Java 同）");

        // 带命名空间：还原原始 topic 之后再剥前缀，两步复合
        consumer.update_config(|c| c.namespace = "Ns".to_string());
        let mut namespaced_retry = ext(&group_topic, None);
        namespaced_retry.put_property(PROPERTY_RETRY_TOPIC, "Ns%Order");
        let plain_namespaced = ext("Ns%T", None);
        let mut msgs = vec![namespaced_retry.clone(), plain_namespaced.clone()];
        reset_retry_topic_and_namespace(&consumer.inner, &mut msgs);
        assert_eq!(msgs[0].topic, "Order");
        assert_eq!(msgs[1].topic, "T");

        // 空批次不报错
        reset_retry_topic_and_namespace(&consumer.inner, &mut []);
    }

    // ---------------- 本地（BROADCASTING）位点 ----------------

    /// 位点文件是「队列 key -> offset」的扁平 JSON 对象（Python `json.dump(dict)`）。
    #[test]
    fn local_offsets_are_a_flat_json_object_of_queue_key() {
        let consumer = DefaultMQPushConsumer::new("RustTestNeverWrittenGroup").unwrap();
        let Some(path) = local_offset_path(&consumer.inner) else {
            // 没有 HOME 环境变量时无本地路径，读取退化成空表
            assert!(load_local_offsets(&consumer.inner).is_empty());
            return;
        };
        assert!(path.ends_with("RustTestNeverWrittenGroup/offsets.json"));
        assert!(!path.exists(), "本测试不写文件");
        // 文件不存在 → 空表（Python：任何读失败都当「没有位点」，不抛异常）
        assert!(load_local_offsets(&consumer.inner).is_empty());

        let items: BTreeMap<String, i64> =
            BTreeMap::from([(mq_key(&queue("T", "broker-a", 0)), 42)]);
        let text = serde_json::to_string(&items).unwrap();
        assert_eq!(text, r#"{"Tbroker-a0":42}"#);
        let back: BTreeMap<String, i64> = serde_json::from_str(&text).unwrap();
        assert_eq!(back.get("Tbroker-a0"), Some(&42));
        // 坏 JSON 会让读取失败 → Python 的 try/except 语义下退化成空表
        assert!(serde_json::from_str::<BTreeMap<String, i64>>("[not, an, object]").is_err());
    }

    // ---------------- runningInfo 的 pop 视图 ----------------

    /// POP 模式下 `mqPopTable` 要能看到每个弹出队列。Python 版查 `_mq_map` 反解队列，
    /// 而 pop 路径从不写它 → 恒空；Rust 按 Java
    /// `DefaultMQPushConsumerImpl.consumerRunningInfo:1473-1481`（popProcessQueueTable
    /// 本身以 MessageQueue 为键）从当前分配反查。
    #[test]
    fn pop_running_info_reports_every_popped_queue() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let q0 = queue("T", "broker-a", 0);
        let q1 = queue("T", "broker-a", 1);
        let (k0, k1) = (mq_key(&q0), mq_key(&q1));
        {
            let mut state = lock(&consumer.inner.state);
            // 故意逆序塞，验证输出按 key 排序（broker 回包给运维时要稳定）
            state.assigned = vec![q1.clone(), q0.clone()];
            state.pop_queues.insert(k1.clone(), PopProcessQueue::new());
            let pq0 = PopProcessQueue::new();
            pq0.inc_found_msg(3);
            state.pop_queues.insert(k0, pq0);
        }
        let info = consumer.consumer_running_info();
        let keys: Vec<String> = info
            .mq_pop_table
            .iter()
            .map(|(k, _)| format!("{}{}{}", k.topic, k.broker_name, k.queue_id))
            .collect();
        assert_eq!(keys, vec!["Tbroker-a0".to_string(), "Tbroker-a1".to_string()]);
        assert!(
            info.mq_table.is_empty(),
            "pop 模式没有拉取队列，mqTable 必须为空（Java processQueueTable 同）"
        );
        // 在途未 ack 的条数走 cachedMsgCount
        assert_eq!(
            info.mq_pop_table[0].1.get("cachedMsgCount"),
            Some(&serde_json::Value::from(3))
        );
        assert_eq!(
            info.mq_pop_table[1].1.get("cachedMsgCount"),
            Some(&serde_json::Value::from(0))
        );
    }

    /// `locked_queue_keys` 是顺序消费锁状态的唯一观测点：四个移植版的
    /// `ProcessQueueInfo.locked` 都不回填。
    #[test]
    fn locked_queue_keys_are_sorted_and_start_empty() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert!(consumer.locked_queue_keys().is_empty());
        {
            let mut state = lock(&consumer.inner.state);
            state.lock_ok.insert(mq_key(&queue("T", "broker-a", 1)));
            state.lock_ok.insert(mq_key(&queue("T", "broker-a", 0)));
        }
        assert_eq!(
            consumer.locked_queue_keys(),
            vec!["Tbroker-a0".to_string(), "Tbroker-a1".to_string()]
        );
    }
}
