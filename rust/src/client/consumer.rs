//! 推/拉消费者层：经典 remoting 协议下的消费端实现。
//!
//! 本模块收三件事：投递前的过滤工具、`MessageSelector`、`PopProcessQueue`，
//! 以及 [`DefaultMQPushConsumer`] —— 注册监听器 + 重平衡 +
//! 拉取/POP 消费循环 + 位点持久化 + broker 主动请求的四条接缝。
//! `DefaultMQPullConsumer` 与 `DefaultLitePullConsumer` 在
//! [`crate::client::pull_consumer`]（见下「模块划分」）。
//!
//! # 并发模型的实现手段
//!
//! 重平衡、每队列拉取、POP、消费分发、位点持久化、队列锁这六类循环全部落在
//! tokio 上，用 `tokio::sync::mpsc` + 定时轮询做任务间通信与唤醒。
//!
//! # 若干有意的结构取舍
//!
//! 1. **一个消费者一个 `ProcessQueue`**：有序消费的红黑树与 `msgTreeMap` 锁语义
//!    只在顺序消费场景才有价值，而顺序消费还额外依赖 broker 侧队列锁；本模块只
//!    保留并发消费的缓冲（拉到的消息直接进 `deque`），不引入树结构。
//! 2. **`Clone` 句柄 + `Arc<Inner>`**：与 [`crate::client::producer`] 同样的手法 ——
//!    消费者要同时被心跳任务、拉取任务与 broker 请求处理器持有，故句柄可复制、
//!    状态集中在 `Inner` 里用 `RwLock`/`Mutex` 保护。
//! 3. **监听器/策略/钩子用 trait 对象**：消费者要在运行时接受用户提供的实现，
//!    统一以 `Arc<dyn ...>` 持有（`MessageListenerConcurrently`、
//!    [`AllocateMessageQueueStrategy`](crate::client::allocate_strategy::AllocateMessageQueueStrategy)
//!    等）。
//! 4. **后台任务持 `Weak<Inner>`**：与生产者一致，避免 实例 → 传输 → 任务 → 消费者
//!    的强引用环；`Drop for Inner` 负责发停止信号并 abort 任务。
//! 5. **轨迹分发器：注入或自动桥接**：见
//!    [`set_trace_dispatcher`](Self::set_trace_dispatcher)（与生产者同一套约定，
//!    见 [`crate::client::producer`] 模块头 4）。`enable_trace=true` 且未注入时，
//!    `start()` 会自动建 `AsyncTraceDispatcher`（Type=CONSUME）+ 真实内部生产者，
//!    轨迹按 CONSUME 类型上报到 trace 主题。
//!
//! # 模块划分
//!
//! 消费侧的四个消费者类分成两个模块：
//! `consumer.rs`（过滤工具 + `MessageSelector` + `PopProcessQueue` +
//! push 消费者）与 `pull_consumer.rs`（`DefaultMQPullConsumer` +
//! `DefaultLitePullConsumer`），因为后两者只是 RPC 的门面组合，与 push 消费者的
//! 循环/重平衡状态机没有共享代码；两模块共用的只有本文件导出的
//! [`filter_messages_for_delivery`] 等工具。

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
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
    ConsumerFuture, MQClientInstance, MQClientInstanceConfig, PublishMessage, RegisteredConsumer,
    MQ_CLIENT_API_TIMEOUT_MILLIS,
};
use crate::client::producer::{auto_bridged_dispatcher, SinkAdapter, TraceDispatcherChannel};
use crate::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, ConsumeReturnType, MessageListenerConcurrently, MessageListenerOrderly,
    PopResult, PopStatus, PullStatus,
};
use crate::client::shutdown::{run_finalize_blocking, SHUTDOWN_FINALIZE_BUDGET};
use crate::client::top_addressing::DefaultTopAddressing;
use crate::client::trace::AccessChannel;
use crate::client::trace_dispatcher::{TraceDispatcherType, TraceHost};
use crate::client::trace_hook::{ConsumeMessageTraceHook, TraceReportSink};
use crate::client::validators;
use crate::common::message::{Message, MessageExt, MessageQueue};
use crate::common::message_const::{
    PROPERTY_CONSUME_START_TIMESTAMP, PROPERTY_MAX_OFFSET, PROPERTY_MAX_RECONSUME_TIMES,
    PROPERTY_ORIGIN_MESSAGE_ID, PROPERTY_POP_CK, PROPERTY_RECONSUME_TIME, PROPERTY_RETRY_TOPIC,
    PROPERTY_TRANSACTION_PREPARED,
};
use crate::common::mix_all::MixAll;
use crate::common::sysflag::{ConsumeInitMode, PullSysFlag};
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};
use crate::remoting::protocol::admin_body::MessageQueueKey;
use crate::remoting::protocol::body::{
    CMResult, ConsumeMessageDirectlyResult, ConsumerRunningInfo, ProcessQueueInfo,
};
use crate::remoting::protocol::ext_fields::StringMap;
use crate::remoting::protocol::extra_info;
use crate::remoting::protocol::heartbeat::{
    ConsumeFromWhere, ConsumeType, FilterAPI, MessageModel, SubscriptionData,
};
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::remoting::protocol::query_assignment_body::MESSAGE_REQUEST_MODE_POP;
use crate::remoting::protocol::response_code;
use crate::remoting::rpchook::RPCHook;
use crate::remoting::socks::SocksProxyConfig;
use crate::{bail, rmq_debug, rmq_error, rmq_info, rmq_warn};

/// POP 消费失败的延迟梯度（**秒**），逐项对应经典 remoting 协议约定的
/// `popDelayLevel` 默认序列。
pub const POP_DELAY_LEVEL: [i32; 16] = [
    10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
];

/// POP 不可见时间的允许下界：超出 `[MIN, MAX]` 一律回落到 60000。
pub const MIN_POP_INVISIBLE_TIME: i64 = 5000;
/// 见 [`MIN_POP_INVISIBLE_TIME`]。
pub const MAX_POP_INVISIBLE_TIME: i64 = 300000;
/// 不可见时间越界时的回落值（协议默认 60000ms）。
pub const DEFAULT_POP_INVISIBLE_TIME: i64 = 60_000;

/// 拉取空闲上限（系统属性 `rocketmq.client.pull.pullMaxIdleTime`，
/// 默认 120000ms）：一条拉取/POP 循环超过这个时长没**发起**过拉取，rebalance 就认定它停摆，
/// 主动撤掉并重建（停摆判定用严格 `>`）。
pub const PULL_MAX_IDLE_TIME: i64 = 120_000;

pub const QUERY_ASSIGNMENT_TIMEOUT_MILLIS: i64 = 3000;

/// 停机收尾给「在途批次收完（含回投 RPC）」的预算。
///
/// 覆盖一个批次的 listener + 逐条回投（每次 3000ms 超时）绰绰有余；顺序消费的
/// 挂起重试最多钳到 30s，超了就放弃等待 —— 位点持久化按下限钳制（按「剩余最小
/// offset」口径），不会把没落定的消息跳过去。
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

// ================================================================ 队列与过滤工具

/// 队列排序键：topic → brokerName → queueId。
///
/// rebalance 会先把候选队列与消费组成员排序再分配；顺序不一致会让不同实例算出
/// 不同的分配结果（同一队列被两个实例同时消费）。
pub fn mq_sort_key(mq: &MessageQueue) -> (String, String, i32) {
    (mq.topic.clone(), mq.broker_name.clone(), mq.queue_id)
}

/// 把队列按 [`mq_sort_key`] 排序。
pub fn sort_mqs(mqs: &mut [MessageQueue]) {
    mqs.sort_by_key(mq_sort_key);
}

/// 客户端二次 tag 过滤。
///
/// broker 侧是按 tag 的**哈希（codeSet）**过滤的，存在哈希碰撞误放；因此客户端
/// 再按字符串核一遍。约束是「订阅集非空且不是 classFilterMode」才做二次过滤：
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
        .filter(|m| {
            m.get_tags()
                .is_some_and(|t| sub.tags_set.iter().any(|s| s == t))
        })
        .collect()
}

/// 投递前过滤 = 客户端二次 tag 过滤 + FilterMessageHook（拉取/POP/pull 三处共用）。
///
/// 钩子拿到的是**可变的** `msg_list`；被摘掉的消息由调用方决定处置方式：
/// 拉取路径 = 静默跳过（位点照常推进，不 ack）；
/// POP 路径 = 必须立刻 ack，否则 `invisibleTime` 到期后会复活重投。
pub fn filter_messages_for_delivery(
    consumer_group: &str,
    hooks: &FilterMessageHookList,
    mq: &MessageQueue,
    sub: Option<&SubscriptionData>,
    msgs: Vec<MessageExt>,
    unit_mode: bool,
) -> Vec<MessageExt> {
    let mut out = client_side_tag_filter(sub, msgs);
    if !out.is_empty() && hooks.has_hooks() {
        let mut context = FilterMessageContext::new(consumer_group, Some(out), Some(mq.clone()));
        // FilterMessageContext 的 `unitMode` 取消费者的 `ClientConfig#unitMode`。
        context.unit_mode = unit_mode;
        crate::client::hook::execute_filter_hooks(hooks, &mut context);
        out = context.msg_list;
    }
    out
}

/// 把位点表按「队列排序键」稳定地投进 `BTreeMap`（这里显式排序，
/// 保证 `get_consumer_status`/221 响应在跨语言比对时可预测）。
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
/// —— 经典 remoting 协议里 `expressionType` 就是一个字符串字段。
pub use crate::remoting::protocol::heartbeat::ExpressionType;

/// 消息选择器：订阅表达式的类型 + 内容。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSelector {
    /// 表达式类型（`ExpressionType::TAG` / `SQL92`）。
    pub selector_type: String,
    /// 表达式内容。
    pub expression: String,
}

impl MessageSelector {
    /// 用「类型 + 表达式」构造选择器。
    pub fn new(selector_type: &str, expression: &str) -> MessageSelector {
        MessageSelector {
            selector_type: selector_type.to_string(),
            expression: expression.to_string(),
        }
    }

    /// 按 tag 选择。
    pub fn by_tag(tag: &str) -> MessageSelector {
        MessageSelector::new(ExpressionType::TAG, tag)
    }

    /// 按 SQL92 表达式选择。
    pub fn by_sql(sql: &str) -> MessageSelector {
        MessageSelector::new(ExpressionType::SQL92, sql)
    }

    /// 订阅表达式：直接作为 `subscribe` 的表达式参数上送。
    pub fn sub_expression(&self) -> &str {
        &self.expression
    }
}

// ================================================================ MessageQueueListener

/// 队列变更监听器。
///
/// 只有 `DefaultMQPullConsumer` / `DefaultLitePullConsumer` 持有该监听器字段，
/// 且只有 LitePull 真的会回调；push 消费者没有这个接缝。
/// 接缝实现见 [`crate::client::pull_consumer`]。
pub trait MessageQueueListener: Send + Sync {
    /// 队列分配发生变化时回调。`mq_all` 是该 topic 的全部队列，
    /// `mq_divided` 是分给本实例的队列。
    fn message_queue_changed(
        &self,
        topic: &str,
        mq_all: &[MessageQueue],
        mq_divided: &[MessageQueue],
    );
}

// ================================================================ PopProcessQueue

/// POP 模式的队列状态。
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
    /// 最近一次**发起**弹出的时刻（毫秒）。
    /// 这个字段有两处读：停摆判定（超过 `PULL_MAX_IDLE_TIME` 判停摆，
    /// 由 [`State::last_pull_at`] 承担）和 `ConsumerRunningInfo` 的运维视图。
    pub last_pop_timestamp: AtomicI64,
}

impl PopProcessQueue {
    /// 新建一个 POP 队列状态，弹出时间戳初始化为当前时刻。
    pub fn new() -> Arc<PopProcessQueue> {
        Arc::new(PopProcessQueue {
            last_pop_timestamp: AtomicI64::new(crate::common::util_all::current_time_millis()),
            ..Default::default()
        })
    }

    /// 累加「等待 ack」的条数（本批弹出的消息数）。
    pub fn inc_found_msg(&self, count: i32) {
        self.wait_ack_counter.fetch_add(count, Ordering::SeqCst);
    }

    /// 递减「等待 ack」的条数：传**正数**即减少等待 ack 的条数。
    pub fn dec_found_msg(&self, count: i32) {
        self.wait_ack_counter.fetch_add(-count, Ordering::SeqCst);
    }

    /// ack 一条：递减并返回递减后的值。
    pub fn ack(&self) -> i32 {
        self.wait_ack_counter.fetch_add(-1, Ordering::SeqCst) - 1
    }

    /// 当前等待 ack 的条数。
    pub fn wait_ack_count(&self) -> i32 {
        self.wait_ack_counter.load(Ordering::SeqCst)
    }

    /// 队列是否已被 rebalance 撤走。
    pub fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::SeqCst)
    }

    /// 标记队列被撤走 / 恢复。
    pub fn set_dropped(&self, dropped: bool) {
        self.dropped.store(dropped, Ordering::SeqCst);
    }

    /// 记一次弹出时间（毫秒）。
    pub fn touch(&self, timestamp_millis: i64) {
        self.last_pop_timestamp
            .store(timestamp_millis, Ordering::SeqCst);
    }
}

/// 消息监听器：并发与顺序两种。
///
/// 顺序/并发的判定必须在运行时可查（拉取循环要按它选消费路径），而 Rust 没有
/// 运行时类型探测，这里用枚举标签表达 —— 判定口径就是「监听器实现的是哪个 trait」。
#[derive(Clone)]
pub enum MessageListener {
    /// 并发监听器。
    Concurrently(Arc<dyn MessageListenerConcurrently>),
    /// 顺序监听器。
    Orderly(Arc<dyn MessageListenerOrderly>),
}

impl MessageListener {
    /// 是否顺序监听器。
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

/// 消费者配置：所有可调参数的平铺集合。
///
/// 聚成一个 `pub` 字段的结构体，由 [`DefaultMQPushConsumer::config`] /
/// [`DefaultMQPushConsumer::update_config`] 整体读写，可以随时替换（生产者侧同一手法）。
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// 消费组名。
    pub consumer_group: String,
    /// 客户端命名空间（v1 机制：给 topic/组拼 `%` 前缀）。
    pub namespace: String,
    /// 实例名，默认 `"DEFAULT"`。
    pub instance_name: String,
    /// 显式指定的 clientId；`None` 时 `start()` 里现造。
    pub client_id: Option<String>,
    /// 单元化名 `unitName`（默认无）：非空时进 clientId 后缀，
    /// 并作为地址服务器 URL 的 `-<unitName>` 段。
    pub unit_name: Option<String>,
    /// 服务端命名空间 `namespaceV2`（默认无）：5.x **服务端**命名空间，
    /// 非空时每个请求带 `nsd=true` / `ns=<namespaceV2>` 扩展头（`NamespaceRpcHook`），
    /// 与 v1 `namespace` 的客户端 `%` 前缀机制是两套东西。
    pub namespace_v2: Option<String>,
    /// 单元化模式 `unitMode`（默认 false）：随发送/回投/鉴权/消息过滤
    /// 一起上线，broker 据此给自动创建的 topic 打 UNIT / UNIT_SUB 位。
    pub unit_mode: bool,
    /// 流式请求类型开关：true 时每个请求带 `ReqT=0`，
    /// clientId 末尾多一段 `@STREAM`。
    ///
    /// 推模式消费者不碰这个开关，默认 false。
    pub enable_stream_request_type: bool,
    /// 路由周期刷新间隔（默认 30000ms）：在用 topic 的路由按这个周期刷新，
    /// `start()` 时透传给 `MQClientInstance`。
    pub poll_name_server_interval_millis: u64,
    /// 位点落盘周期（默认 5000ms）：后台位点落盘周期；`shutdown()` 的收尾落盘
    /// 与该值无关。同样只在 `start()` 时读一次。
    pub persist_consumer_offset_interval_millis: u64,
    /// 名字服务地址列表。
    pub name_server_addrs: Vec<String>,
    /// TLS 开关；`None` = 交给环境变量 `ROCKETMQ_TLS_ENABLE`。
    pub tls_enable: Option<bool>,
    pub socks_proxy: Option<SocksProxyConfig>,
    /// Python `message_model`（[`MessageModel`] 里的常量）。
    pub message_model: String,
    /// 起始位点策略（[`ConsumeFromWhere`] 里的常量）。
    pub consume_from_where: String,
    /// 起始位点时间戳：`yyyyMMddHHmmss`，默认 30 分钟前。
    pub consume_timestamp: String,
    /// 并发消费线程池下界，默认 20。
    pub consume_thread_min: i32,
    /// 并发消费线程池上界，默认 20 —— 与 min 同为 20 是刻意取值。
    /// 它同时是 `update_core_pool_size` 的上界（守卫 `core < max`），
    /// 于是默认配置下只能把并发度往**下**调。
    pub consume_thread_max: i32,
    /// 线程池动态扩容的堆积阈值，默认 100000。
    pub adjust_thread_pool_nums_threshold: i64,
    /// 并发消费允许的位点跨度上限（**条**），默认 2000。
    pub consume_concurrently_max_span: i64,
    /// 单队列缓存条数阈值，默认 1000（**条数**）。
    pub pull_threshold_for_queue: i32,
    /// 单队列缓存体积阈值，默认 100（**MiB**）。
    pub pull_threshold_size_for_queue: i32,
    /// topic 级缓存条数阈值，默认 -1（关闭）。
    pub pull_threshold_for_topic: i32,
    /// topic 级缓存体积阈值，默认 -1（关闭）。
    pub pull_threshold_size_for_topic: i32,
    /// 两次拉取之间的间隔（默认 0，本项目拉取循环未使用，仅保留字段）。
    pub pull_interval: i64,
    /// 拉取 RPC 超时（默认 30000ms）。
    pub pull_timeout_millis: i64,
    /// 长轮询挂起时长（默认 20000ms）。
    pub pull_suspend_timeout_millis: i64,
    /// 单次交给 listener 的最大条数，默认 1。
    pub consume_message_batch_max_size: i32,
    /// 单次拉取的最大条数，默认 32。
    pub pull_batch_size: i32,
    /// 单次拉取的最大字节数，默认 256 KiB（对应请求里的 `maxMsgBytes`）。
    pub pull_batch_size_in_bytes: i32,
    /// 最大重试次数，默认 -1（交给 broker，默认 16 次后转 `%DLQ%`）。
    pub max_reconsume_times: i32,
    /// 顺序消费挂起时长（默认 1000ms）。
    pub suspend_current_queue_time_millis: i64,
    /// 消费超时（**分钟**，默认 15），只用于 `ConsumeReturnType::TimeOut` 判定。
    pub consume_timeout: i64,
    pub client_rebalance: bool,
    /// broker 心跳周期（默认 30000ms）。
    pub heartbeat_interval_millis: i64,
    /// 是否发心跳。
    pub heartbeat_enabled: bool,
    /// Python `pop_mode`：true 走 POP + ack（且队列分配改走 broker 侧 QUERY_ASSIGNMENT），
    /// false 走长轮询 + 位点（本地 rebalance 不变）。
    pub pop_mode: bool,
    /// POP 不可见时间（默认 60000ms）。
    pub pop_invisible_time: i64,
    /// 单次弹出条数，默认 32（broker 侧 >32 会拒）。
    pub pop_batch_nums: i32,
    /// 单队列待 ack 条数阈值，默认 96。
    pub pop_threshold_for_queue: i32,
    /// POP 失败重试的延迟梯度（**秒**），见 [`POP_DELAY_LEVEL`]。
    pub pop_delay_level: Vec<i32>,
    /// POP 长轮询挂起时长；0 = 短轮询。⚠ 非 0 时请求超时必须大于它。
    pub pop_poll_time_millis: i64,
    /// POP 请求超时。
    pub pop_timeout_millis: i64,
    /// 是否上报消费轨迹。
    pub enable_trace: bool,
    /// 自定义轨迹主题；未设置 → `RMQ_SYS_TRACE_TOPIC`。
    pub trace_topic: Option<String>,
    /// 轨迹批量上报条数，默认 10。
    pub trace_msg_batch_num: i32,
    /// 是否把订阅表达式随 PULL_MESSAGE 上送（SUBSCRIPTION 位），默认 false：
    /// 仅类过滤模式的服务端场景需要。
    pub post_subscription_when_pull: bool,
}

impl Default for ConsumerConfig {
    fn default() -> ConsumerConfig {
        ConsumerConfig {
            consumer_group: MixAll::DEFAULT_CONSUMER_GROUP.to_string(),
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            unit_name: None,
            namespace_v2: None,
            unit_mode: false,
            enable_stream_request_type: false,
            // 路由刷新 1000 * 30，位点落盘 1000 * 5
            poll_name_server_interval_millis: 30_000,
            persist_consumer_offset_interval_millis: 5_000,
            client_id: None,
            name_server_addrs: Vec::new(),
            tls_enable: None,
            socks_proxy: None,
            message_model: MessageModel::CLUSTERING.to_string(),
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string(),
            consume_timestamp: default_consume_timestamp(),
            consume_thread_min: 20,
            consume_thread_max: 20,
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
            post_subscription_when_pull: false,
        }
    }
}

/// `instance_name` 的默认值（与生产者同名常量，但两处各自持有，避免跨模块耦合）。
pub const DEFAULT_INSTANCE_NAME: &str = "DEFAULT";
/// 16 位有符号整数的上限（32767）：`update_core_pool_size` 的第二道守卫。
pub const SHORT_MAX_VALUE: i32 = 32767;

/// 默认的起始位点时间：本地时区「当前时刻 - 30 分钟」，格式 `%Y%m%d%H%M%S`。
pub(crate) fn default_consume_timestamp() -> String {
    let now = chrono::Local::now();
    let past = now - chrono::Duration::minutes(30);
    past.format("%Y%m%d%H%M%S").to_string()
}

/// 把 `consumeTimestamp` 解析成毫秒时间戳：该字段**只**是 14 位本地墙钟日期。
///
/// 必须按**本地时区**解析，否则 `CONSUME_FROM_TIMESTAMP` 的起点会差一个时区。
/// 解析不了必须硬失败（服务端在启动时就会以 `consumeTimestamp is invalid` 拒绝）——
/// 静默回落到「现在 - 30 分钟」会让起点错位无人察觉。
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

// ================================================================ 配置数值闸门

/// 启动期配置校验的数值段：十三道区间/大小闸门，**顺序与区间都是判据**
/// （每条错误文案按约定不带 FAQUrl 后缀）。
///
/// 为什么要在 `start()` 拦而不是各字段自己夹：这些值直接决定缓冲水位与线程池规模，
/// 错法的方向都是**静默**的 —— `pull_threshold_*=0` 在运行期被 `max(1,n)` 兜底成
/// "1 条就停"，队列永久停拉而消费者不报任何错；`pop_batch_nums>32` broker 直接回
/// `INVALID_PARAMETER`，POP 循环退化成错误重试；巨值绕开流控判断直到 OOM。把
/// 闸门放在启动期，就是要在**没建实例、没起任务**的时候一次性拒绝。
///
/// 两处容易写错的"不对称"必须注意，否则会把合法配置拒了：
/// * `pull_threshold_for_topic` / `pull_threshold_size_for_topic` 的 `-1` 是
///   "未设置，用队列级阈值"的哨兵，`!= -1` 才进区间判断（`-2` 仍然要拒）；
/// * `pull_interval` 的下界是 **0**（不是 1），`pop_batch_nums` 的判据是 `<= 0`。
fn check_config_ranges(cfg: &ConsumerConfig) -> Result<()> {
    // consumeThreadMin
    if cfg.consume_thread_min < 1 || cfg.consume_thread_min > 1000 {
        bail!("consumeThreadMin Out of range [1, 1000]");
    }
    // consumeThreadMax
    if cfg.consume_thread_max < 1 || cfg.consume_thread_max > 1000 {
        bail!("consumeThreadMax Out of range [1, 1000]");
    }
    // consumeThreadMin can't be larger than consumeThreadMax
    if cfg.consume_thread_min > cfg.consume_thread_max {
        bail!(
            "consumeThreadMin ({}) is larger than consumeThreadMax ({})",
            cfg.consume_thread_min,
            cfg.consume_thread_max
        );
    }
    // consumeConcurrentlyMaxSpan
    if cfg.consume_concurrently_max_span < 1 || cfg.consume_concurrently_max_span > 65535 {
        bail!("consumeConcurrentlyMaxSpan Out of range [1, 65535]");
    }
    // pullThresholdForQueue
    if cfg.pull_threshold_for_queue < 1 || cfg.pull_threshold_for_queue > 65535 {
        bail!("pullThresholdForQueue Out of range [1, 65535]");
    }
    // pullThresholdForTopic
    if cfg.pull_threshold_for_topic != -1
        && (cfg.pull_threshold_for_topic < 1 || cfg.pull_threshold_for_topic > 6553500)
    {
        bail!("pullThresholdForTopic Out of range [1, 6553500]");
    }
    // pullThresholdSizeForQueue
    if cfg.pull_threshold_size_for_queue < 1 || cfg.pull_threshold_size_for_queue > 1024 {
        bail!("pullThresholdSizeForQueue Out of range [1, 1024]");
    }
    // pullThresholdSizeForTopic
    if cfg.pull_threshold_size_for_topic != -1
        && (cfg.pull_threshold_size_for_topic < 1 || cfg.pull_threshold_size_for_topic > 102400)
    {
        bail!("pullThresholdSizeForTopic Out of range [1, 102400]");
    }
    // pullInterval
    if cfg.pull_interval < 0 || cfg.pull_interval > 65535 {
        bail!("pullInterval Out of range [0, 65535]");
    }
    // consumeMessageBatchMaxSize
    if cfg.consume_message_batch_max_size < 1 || cfg.consume_message_batch_max_size > 1024 {
        bail!("consumeMessageBatchMaxSize Out of range [1, 1024]");
    }
    // pullBatchSize
    if cfg.pull_batch_size < 1 || cfg.pull_batch_size > 1024 {
        bail!("pullBatchSize Out of range [1, 1024]");
    }
    // popInvisibleTime
    if cfg.pop_invisible_time < MIN_POP_INVISIBLE_TIME
        || cfg.pop_invisible_time > MAX_POP_INVISIBLE_TIME
    {
        bail!("popInvisibleTime Out of range [{MIN_POP_INVISIBLE_TIME}, {MAX_POP_INVISIBLE_TIME}]");
    }
    if cfg.pop_batch_nums <= 0 || cfg.pop_batch_nums > 32 {
        bail!("popBatchNums Out of range [1, 32]");
    }
    Ok(())
}

// ================================================================ 内部可变状态

/// 消费者内部那批需要互斥保护的表（位点、缓冲、队列归属 …）。
///
/// 键一律是 [`DefaultMQPushConsumer::mq_key`] 那串 `topic+brokerName+queueId`；
/// 用 `BTreeMap` 只为让遍历顺序确定 —— `consumerRunningInfo` 的报文顺序依赖它。
#[derive(Default)]
struct State {
    /// 订阅表：topic -> SubscriptionData。
    subscription_data: Vec<(String, SubscriptionData)>,
    /// 本实例当前被分配到的队列集合。
    assigned: Vec<MessageQueue>,
    /// **拉取游标**（nextBeginOffset）。
    offset_table: BTreeMap<String, i64>,
    /// **已消费位点**，周期持久化到 broker 的是这张。
    consume_offsets: BTreeMap<String, i64>,
    /// 已拉未消费的缓冲（待分发消息）。
    pending: BTreeMap<String, VecDeque<MessageExt>>,
    /// 在途批次计数：`pending` 里已被取走、正在被 listener 消费的那部分。
    /// `dispatch_loop` 取走批次时 +1、消费收尾（含异常回塞）时 -1。
    /// `pending` 与它合起来才是队列的完整消息数，
    /// [`correct_tags_offset_locked`] 的闸门两个都要看。
    inflight: BTreeMap<String, usize>,
    /// 已分发给 listener、还没落定的消息本体。
    ///
    /// `pending`（已拉未分发）∪ 它 = 队列在册消息的完整视图，cleanExpiredMsg 的
    /// 「队首清扫」与 [`process_queue_contains_locked`] 都看这个并集。
    /// **只登记并发批次**：顺序消费的消息会被挪进单独的「消费中」集合，
    /// 而清扫对顺序直接早退。
    /// 身份用 `queue_offset`（同一队列内位点唯一；本端的批次是值拷贝，
    /// 拿引用身份对不上）。
    inflight_msgs: BTreeMap<String, Vec<MessageExt>>,
    /// 被 `OFFSET_ILLEGAL` 纠错冻结的位点。冻结期间在途 ack 与空应答修正都不许改它，
    /// 直到这条队列按修正位点重建（`rebalance_pull_threads` 的建分支解冻）。
    frozen_offsets: BTreeSet<String>,
    /// 每条队列「ProcessQueue 的代号」，撤销时 +1。
    ///
    /// `dispatch_loop` 取批次时连同代号一起取走，消费/ack 前比对 —— 不一致说明这批消息
    /// 属于已被丢弃的旧 ProcessQueue，必须整批作废；少了它，旧批次的 ack 会把刚被
    /// broker 纠正的位点又推回非法值，两边来回弹跳。
    queue_epoch: BTreeMap<String, u64>,
    /// 队列 key -> MessageQueue。
    mq_map: BTreeMap<String, MessageQueue>,
    /// 顺序消费下 broker 已确认锁定的队列。
    lock_ok: BTreeSet<String>,
    /// 每队列本批的堆积增量（用于 topic 级流控的水位估算）。
    msg_acc_cnt: BTreeMap<String, i64>,
    /// 每队列拉取/POP 循环的**归属令牌**。
    ///
    /// tokio 任务可能在线程间迁移，无法用线程对象判归属，故换成单调递增令牌。
    queue_owners: BTreeMap<String, u64>,
    /// 队列 key -> [`PopProcessQueue`]。
    pop_queues: BTreeMap<String, Arc<PopProcessQueue>>,
    /// POP 顺序消费的去重集：同一个 (弹出队列, 队列) 只挂一个消费请求。
    /// 相等性由「弹出队列的引用 + 队列 key」决定，
    /// 这里用 Arc 指针地址表达引用、[`mq_key`] 表达队列。
    pop_orderly_requests: BTreeSet<(usize, String)>,
    /// 每队列**最近一次发起**拉取/弹出的时刻（毫秒）。
    ///
    /// 盖章发生在循环**发起**网络请求之前，所以长轮询挂起和流控等待都不会把还在跑的
    /// 循环判成停摆。循环已退出但仍占着归属时写 `0`（Rust 任务没有 `is_alive()`，
    /// 用这个哨兵表达「死了」）。
    last_pull_at: BTreeMap<String, i64>,
    /// 队列 → 下一轮拉取该选的 brokerId。
    ///
    /// 由拉取应答头里的 `suggestWhichBrokerId` 驱动，缺省按 master(=0) 记账；
    /// 下次拉取据此选主/从节点。用 `HashMap` 而不是 `BTreeMap`：`MessageQueue`
    /// 已实现 `Hash + Eq`，且这张表不参与任何报文序列化，遍历顺序无影响。
    pull_from_which_node: HashMap<MessageQueue, i64>,
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
    /// 分发循环的句柄，**不在** [`Inner::tasks` 里]：停机时它可能正带着一批消息做
    /// 回投 RPC，abort 会把在途回投从中间掐断。停机改走「有界等它跑完当前批次」
    /// （见 [`DefaultMQPushConsumer::shutdown`] 的收尾任务）。
    dispatch_task: Mutex<Option<JoinHandle<()>>>,
    listener: Mutex<Option<MessageListener>>,
    strategy: RwLock<Arc<dyn AllocateMessageQueueStrategy>>,
    consume_hooks: ConsumeMessageHookList,
    filter_hooks: FilterMessageHookList,
    stats: Mutex<Option<Arc<ConsumerStatsManager>>>,
    pop_executor: Mutex<Option<ConsumeExecutor>>,
    trace: Mutex<Option<Arc<dyn TraceDispatcherChannel>>>,
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
    /// 已发心跳次数（观测/单测用）。
    heartbeat_count: AtomicUsize,
    /// 本地流控触发次数（无锁自增）。
    flow_control_triggered: AtomicUsize,
    /// 线程池核心数的声明值。
    core_pool_size: AtomicI32,
    /// 本实现不支持外部注入执行器，恒 true；
    /// 保留字段是为了让 `update_core_pool_size` 的守卫形状完整。
    owns_consume_executor: AtomicBool,
    /// 启动时刻（毫秒）。
    start_time_millis: AtomicI64,
    /// 「立刻重平衡」的请求标志。
    ///
    /// ⚠ 用「标志位 + `Notify`」而不是裸 `Notify::notify_waiters()`：后者只叫醒
    /// **当下**在等的任务，通知落在两轮之间就会丢；标志位是持久的，
    /// 补上这个语义。
    rebalance_now: AtomicBool,
    rebalance_signal: Notify,
    /// [`State::queue_owners`] 的令牌发号器。
    next_token: AtomicU64,
    /// 暂停标志：[`suspend`](DefaultMQPushConsumer::suspend) 置 true，
    /// [`resume`](DefaultMQPushConsumer::resume) 置 false。
    /// 拉取/弹出循环看到 true 时不发请求，按
    /// [`PULL_TIME_DELAY_MILLS_WHEN_SUSPEND`] 挂起后重试。
    pause: AtomicBool,
}

// ================================================================ 流控常量

/// 拉取/弹出 RPC 出错后的退避。
pub const PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION: u64 = 3000;
/// 本地缓存（待消费/待 ack）打满流控时的退避。
pub const PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL: u64 = 50;
/// broker 回 `FLOW_CONTROL(215)` 时的退避
/// （远短于普通异常：broker 只是让客户端慢一点）。
pub const PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL: u64 = 20;
/// `suspend()` 后的轮询间隔。
pub const PULL_TIME_DELAY_MILLS_WHEN_SUSPEND: u64 = 1000;

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

/// 取锁；中毒时照用，理由同 `consume_executor::lock`。
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn read_cfg(inner: &Inner) -> ConsumerConfig {
    inner.cfg.read().unwrap_or_else(|e| e.into_inner()).clone()
}

fn require_client(inner: &Inner) -> Result<MQClientInstance> {
    if !inner.started.load(Ordering::Acquire) {
        return Err(Error::client("consumer not started, call start() first"));
    }
    lock(&inner.client)
        .clone()
        .ok_or_else(|| Error::client("consumer not started, call start() first"))
}

/// 停机排空窗口内的**内部回投**专用取实例：只看实例在不在，不看 `started`。
///
/// `shutdown()` 的第一步就把 `started` 置 false，然后才排空在途批次（等回投 RPC 跑到
/// 应答）。回投若沿用 [`require_client`] 的 started 门，排空等到的全是
/// 「consumer not started」快速失败 —— 回投一条也发不出去，消息只能靠位点下限从原
/// topic 重投，`%RETRY%`/`%DLQ%` 永远进不去（真机立即退进程测试抓到，见
/// `examples/live_shutdown_race.rs`）。回投 RPC 本身只依赖客户端实例，与消费者自身的
/// serviceState 无关；实例本体到 shutdown 收尾的最后一步才被摘掉，
/// 这里也只要求它在。
fn require_client_for_send_back(inner: &Inner) -> Result<MQClientInstance> {
    lock(&inner.client)
        .clone()
        .ok_or_else(|| Error::client("consumer not started, call start() first"))
}

/// topic + brokerName + queueId 拼成的队列 key（无分隔符）。
pub fn mq_key(mq: &MessageQueue) -> String {
    format!("{}{}{}", mq.topic, mq.broker_name, mq.queue_id)
}

// ================================================================ DefaultMQPushConsumer

/// 推模式消费者：订阅 + 重平衡 + 拉取/POP 消费循环的门面句柄。
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
            .field(
                "subscriptions",
                &lock(&self.inner.state).subscription_data.len(),
            )
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

    /// 直接以一份完整配置构造（逐个赋属性再启动的等价写法）。
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
            dispatch_task: Mutex::new(None),
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
            pause: AtomicBool::new(false),
        });
        let consumer = DefaultMQPushConsumer { inner };
        // Python `__init__` 里 `self._core_pool_size = self.consume_thread_min`
        let min = consumer.config().consume_thread_min;
        consumer
            .inner
            .core_pool_size
            .store(min.max(1), Ordering::SeqCst);
        Ok(consumer)
    }

    /// 当前配置快照（Python 直接读属性）。
    pub fn config(&self) -> ConsumerConfig {
        read_cfg(&self.inner)
    }

    /// 改配置（Python 的直接赋属性）。`start()` 之后除少数运行期可调项外应拒绝。
    pub fn update_config(&self, f: impl FnOnce(&mut ConsumerConfig)) {
        {
            let mut w = self.inner.cfg.write().unwrap_or_else(|e| e.into_inner());
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

    /// [`MQClientInstance`]（Python `_mq_client`、C++ `client()`），未启动时 `None`。
    pub fn client(&self) -> Option<MQClientInstance> {
        lock(&self.inner.client).clone()
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

    /// 设置单元化名：`None`/空白等价于不设（拼 clientId 时按空白判）。
    pub fn set_unit_name(&self, unit_name: Option<&str>) {
        let unit_name = unit_name.map(str::to_string);
        self.update_config(|c| c.unit_name = unit_name);
    }

    /// 设置 5.x **服务端**命名空间（`ns`/`nsd`
    /// 扩展头，见 `NamespaceRpcHook`）。`None`/空串 = 不设，钩子退化为 no-op。
    /// `start()` 时透传给 `MQClientInstance`，晚于 start 修改不影响已建实例。
    pub fn set_namespace_v2(&self, namespace_v2: Option<&str>) {
        let namespace_v2 = namespace_v2.map(str::to_string);
        self.update_config(|c| c.namespace_v2 = namespace_v2);
    }

    /// 读当前服务端命名空间。
    pub fn get_namespace_v2(&self) -> Option<String> {
        self.config().namespace_v2
    }

    pub fn set_socks_proxy(&self, socks_proxy: Option<SocksProxyConfig>) {
        self.update_config(|c| c.socks_proxy = socks_proxy);
    }

    pub fn get_socks_proxy(&self) -> Option<SocksProxyConfig> {
        self.config().socks_proxy
    }

    pub fn set_unit_mode(&self, unit_mode: bool) {
        self.update_config(|c| c.unit_mode = unit_mode);
    }

    /// 设置流式请求类型开关（true 时请求带 `ReqT=0`，clientId 末尾多 `@STREAM`）。
    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.update_config(|c| c.enable_stream_request_type = enable);
    }

    /// 设置是否把订阅表达式随 PULL_MESSAGE 上送（默认 false）。
    ///
    /// 打开后每次 PULL_MESSAGE 会把订阅表达式随请求上送（SUBSCRIPTION 位置位），
    /// 让 broker 侧做过滤；默认关闭 —— 过滤由客户端 `filter_messages_for_delivery`
    /// 兜底，只有 broker 侧需要订阅表达式做类过滤时才该打开。
    pub fn set_post_subscription_when_pull(&self, enable: bool) {
        self.update_config(|c| c.post_subscription_when_pull = enable);
    }

    /// 设置起始位点策略（取 [`ConsumeFromWhere`] 里的常量）。
    pub fn set_consume_from_where(&self, where_: &str) {
        let where_ = where_.to_string();
        self.update_config(|c| c.consume_from_where = where_);
    }

    /// 设置 `CONSUME_FROM_TIMESTAMP` 的起点，格式 `yyyyMMddHHmmss`。
    pub fn set_consume_timestamp(&self, timestamp: &str) {
        let timestamp = timestamp.to_string();
        self.update_config(|c| c.consume_timestamp = timestamp);
    }

    /// 设置在用 topic 的路由刷新周期。
    pub fn set_poll_name_server_interval_millis(&self, millis: u64) {
        self.update_config(|c| c.poll_name_server_interval_millis = millis);
    }

    /// 设置后台位点落盘周期。
    pub fn set_persist_consumer_offset_interval_millis(&self, millis: u64) {
        self.update_config(|c| c.persist_consumer_offset_interval_millis = millis);
    }

    /// 只读访问共享的 [`MQClientInstance`]（node `consumer.mqClient`、python
    /// `_mq_client` 同形）：静态主题腿用它读端点重写结果；未启动报错。
    pub fn mq_client(&self) -> Result<MQClientInstance> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(Error::client("consumer not started, call start() first"));
        }
        lock(&self.inner.client)
            .clone()
            .ok_or_else(|| Error::client("consumer not started, call start() first"))
    }

    /// 注册并发消费监听器。
    pub fn set_message_listener(&self, listener: MessageListener) {
        *lock(&self.inner.listener) = Some(listener);
    }

    /// 注册并发消费监听器。
    pub fn set_message_listener_concurrently(
        &self,
        listener: Arc<dyn MessageListenerConcurrently>,
    ) {
        self.set_message_listener(MessageListener::Concurrently(listener));
    }

    /// 注册顺序消费监听器。
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

    /// 当前队列分配策略。
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

    /// 当前注册的 rpc 钩子（自动桥接轨迹分发器时透传给内部生产者）。
    pub(crate) fn rpc_hook(&self) -> Option<Arc<dyn RPCHook>> {
        self.inner
            .rpc_hook
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
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

    /// 订阅一个 topic，表达式默认 `"*"`。
    ///
    /// `start()` 之后**仍可**调用：订阅表是活的（心跳与重平衡每轮都重读它），
    /// put 进去之后立即推一轮心跳（见 [`Self::notify_subscription_changed`]）。
    pub fn subscribe(&self, topic: &str, sub_expression: &str) -> Result<()> {
        let topic = self.with_namespace(topic);
        let sub = FilterAPI::build_subscription_data(&topic, Some(sub_expression))?;
        self.put_subscription(topic.clone(), sub);
        self.notify_subscription_changed(&topic);
        Ok(())
    }

    /// 订阅一个 topic 的全部消息（等价 `subscribe(topic, "*")`）。
    pub fn subscribe_all(&self, topic: &str) -> Result<()> {
        self.subscribe(topic, "*")
    }

    /// 用 [`MessageSelector`] 订阅。
    ///
    /// 按表达式类型分流：
    /// TAG（或 type 为空）走 `buildSubscriptionData` 填 `tagsSet`/`codeSet`；
    /// SQL92 / CLASS_FILTER 只设 topic/subString/expressionType，两个集合留空。
    /// 与 `subscribe` 相同：start() 之后可调用，并立即推一轮心跳。
    pub fn subscribe_with_selector(&self, topic: &str, selector: &MessageSelector) -> Result<()> {
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
        self.put_subscription(topic.clone(), sub);
        self.notify_subscription_changed(&topic);
        Ok(())
    }

    /// 取消订阅一个 topic。
    ///
    /// 这里**只删表项、不发心跳**（后一轮心跳自然带出新订阅集），所以刻意不调
    /// `notify_subscription_changed`。
    pub fn unsubscribe(&self, topic: &str) {
        let topic = self.with_namespace(topic);
        let mut state = lock(&self.inner.state);
        state.subscription_data.retain(|(k, _)| *k != topic);
    }

    /// 订阅表新增/更新后的立即动作：同步推一轮心跳 + 把 topic 登记为「在用」。
    ///
    /// 为什么必须立即：
    /// broker 的 `ConsumerManager` 只在收到心跳时才把 topic→group 记进它自己那张
    /// topicGroupTable（ClientManageProcessor → registerConsumer），而
    /// `QUERY_TOPIC_CONSUME_BY_WHO(300)` 读的正是这张表；晚一轮就是默认 30s 的空窗。
    ///
    /// 另外把新 topic 登记为「在用」，让后台路由刷新任务覆盖到它 ——
    /// 刷新任务收集「要刷哪些 topic」时读的是消费者的**当前**订阅表，
    /// 新订阅必须当场可见才能被覆盖到。
    /// 未启动时没有 client，只落订阅表。
    ///
    /// `subscribe` 是同步签名而心跳是异步 RPC：放到 runtime 上 fire-and-forget
    /// （与 `rebalance` 起每队列拉取循环同一套 `runtime_handle()` 手法）。
    fn notify_subscription_changed(&self, topic: &str) {
        let Ok(client) = require_client(&self.inner) else {
            return;
        };
        client.register_topic_in_use(topic);
        let Some(handle) = self.runtime_handle() else {
            rmq_debug!("subscribe: no tokio runtime, immediate heartbeat skipped");
            return;
        };
        let weak = Arc::downgrade(&self.inner);
        handle.spawn(async move {
            if let Some(inner) = weak.upgrade() {
                heartbeat_once(&inner).await;
            }
        });
    }

    fn put_subscription(&self, topic: String, sub: SubscriptionData) {
        let mut state = lock(&self.inner.state);
        match state
            .subscription_data
            .iter_mut()
            .find(|(k, _)| *k == topic)
        {
            Some(slot) => slot.1 = sub,
            None => state.subscription_data.push((topic, sub)),
        }
    }

    /// 当前订阅表快照。
    pub fn subscriptions(&self) -> Vec<SubscriptionData> {
        lock(&self.inner.state)
            .subscription_data
            .iter()
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// 给 topic 拼上客户端命名空间的 `%` 前缀（订阅入口统一先过这一步）。
    fn with_namespace(&self, topic: &str) -> String {
        let namespace = read_cfg(&self.inner).namespace;
        if namespace.is_empty() {
            topic.to_string()
        } else {
            NamespaceUtil::wrap_namespace(&namespace, topic)
        }
    }

    // ---------------- 生命周期 ----------------

    /// 启动消费者：建实例、注册、拉路由、首轮心跳与重平衡，再起后台循环。
    ///
    /// 顺序是判据：校验 → 拼命名空间 → 建并启动实例 → 注册消费者 →
    /// 自动订阅 `%RETRY%` → 注册 NOTIFY_CONSUMER_IDS_CHANGED(40) → 建 POP 执行器 →
    /// 拉路由 → **同步**首轮心跳 → **同步**首轮重平衡 → 起各循环 → 轨迹分发器。
    ///
    /// 两处刻意的实现选择（行为不变）：
    /// 1. 心跳报文由 [`MQClientInstance::send_heartbeat_to_all_broker`] 从注册表拼
    ///    全量 `ConsumerData`，而不是消费者自己拼一份。因为 `register_consumer` 在它
    ///    之前，本消费者一定在报文里。
    /// 2. 后台循环是 tokio 任务，句柄缺失时（无运行时）只告警不报错，与生产者一致。
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
        let group = if cfg.namespace.is_empty() {
            cfg.consumer_group.clone()
        } else {
            NamespaceUtil::wrap_namespace(&cfg.namespace, &cfg.consumer_group)
        };
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
        if let Err(e) = consume_timestamp_millis(&cfg.consume_timestamp) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        if let Err(e) = check_config_ranges(&cfg) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }

        let instance_name = MixAll::instance_name_for_model(
            &cfg.instance_name,
            cfg.message_model == MessageModel::CLUSTERING,
        );
        let client_id = cfg.client_id.clone().unwrap_or_else(|| {
            MixAll::build_default_client_id(
                &instance_name,
                cfg.unit_name.as_deref(),
                cfg.enable_stream_request_type,
            )
        });
        self.update_config(|c| {
            c.consumer_group = group.clone();
            c.client_id = Some(client_id.clone());
            c.instance_name = instance_name;
        });

        let instance_cfg = MQClientInstanceConfig {
            tls_enable: cfg.tls_enable,
            socks_proxy: cfg.socks_proxy.clone(),
            unit_name: cfg.unit_name.clone(),
            namespace_v2: cfg.namespace_v2.clone(),
            enable_stream_request_type: cfg.enable_stream_request_type,
            route_refresh_interval_millis: cfg.poll_name_server_interval_millis,
            persist_offset_interval_millis: cfg.persist_consumer_offset_interval_millis,
            ..Default::default()
        };
        let client = MQClientInstance::create_mq_client_instance(
            &client_id,
            cfg.name_server_addrs.clone(),
            instance_cfg,
        );
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
        *lock(&self.inner.stats) = Some(client.consumer_stats_manager().clone());

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
        if let Err(e) = client.check_client_in_broker().await {
            self.shutdown();
            return Err(e);
        }
        let ok = client.send_heartbeat_to_all_broker(5000).await;
        if ok > 0 {
            self.inner.heartbeat_count.fetch_add(1, Ordering::SeqCst);
        }
        if let Err(e) = self.do_rebalance().await {
            rmq_warn!("initial rebalance failed: {e}");
        }
        self.spawn_loops();
        self.start_trace_dispatcher();
        Ok(())
    }

    /// Python `shutdown()`。
    ///
    /// ⚠ 与「spawn 后立即返回」的旧形态不同：清退（等在途批次 → 刷位点/解锁 →
    /// 35 号注销 → 拆实例）挂在运行时上跑，但本方法**阻塞等它落地**（上限
    /// [`SHUTDOWN_FINALIZE_BUDGET`]）。「shutdown() 返回后立刻退进程」不会再丢
    /// 在途回投 —— 该进 `%DLQ%` 的消息在返回前已经发出。current_thread 运行时里
    /// 无法阻塞（会 panic / 死锁），此时收尾仍是游离任务并告警（见
    /// [`run_finalize_blocking`] 的情形 2/3）。
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
            // Python `self._last_pull_table.clear()`：停机后残留的时刻会让下次
            // start() 的自愈判定读到旧实例的盖章。
            state.last_pull_at.clear();
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
        // 分发循环不在 tasks 里（见字段注释）：它可能正带着一批消息做回投 RPC，
        // abort 会把在途回投从中间掐断。由下面的收尾等待（`run_finalize_blocking`）
        // 有界等它跑完当前批次。
        let dispatch_task = lock(&self.inner.dispatch_task).take();
        // POP 执行器同样挪进收尾任务：先停接收，再有界等已提交任务跑完。
        let pop_executor = lock(&self.inner.pop_executor).take();
        let client = lock(&self.inner.client).clone();
        if let Some(client) = client {
            let group = self.consumer_group();
            let broadcast = self.config().message_model == MessageModel::BROADCASTING;
            let orderly = lock(&self.inner.listener)
                .as_ref()
                .is_some_and(MessageListener::is_orderly);
            let locked_mqs: Vec<MessageQueue> = if orderly && !broadcast && !self.config().pop_mode
            {
                lock(&self.inner.state).assigned.clone()
            } else {
                Vec::new()
            };
            if broadcast {
                if let Err(e) = save_local_offsets(&self.inner) {
                    rmq_debug!("persist local offsets on shutdown failed: {e}");
                }
            }
            client.unregister_consumer(&group);
            // 同生产者：注销 35 在 spawn 的任务里，登记表不能等任务 —— 立刻重启会
            // 复用回这份正要被拆的实例（见 `MQClientInstance` 上那段注释）。
            client.detach_from_registry_if_last_tenant();
            if let Some(handle) = self.runtime_handle() {
                let client_id = self.client_id();
                let inner = Arc::clone(&self.inner);
                let finalize = async move {
                    // ① 有界排空在途批次：等分发循环把手里的这批消费/回投完（回投 RPC
                    //    完整跑到应答），POP 执行器把已提交的消费任务跑完。超时则放弃
                    //    —— 下面 ② 的位点下限钳制保证没落定的消息仍会被 broker 重投。
                    if let Some(task) = dispatch_task {
                        if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, task)
                            .await
                            .is_err()
                        {
                            rmq_warn!(
                                "push shutdown: dispatch loop still busy after \
                                 {SHUTDOWN_DRAIN_TIMEOUT:?}; persisting floored offsets"
                            );
                        }
                    }
                    if let Some(executor) = pop_executor {
                        executor.shutdown();
                        if tokio::time::timeout(
                            SHUTDOWN_DRAIN_TIMEOUT,
                            executor.await_termination(),
                        )
                        .await
                        .is_err()
                        {
                            rmq_warn!(
                                "push shutdown: pop consume executor still busy after \
                                 {SHUTDOWN_DRAIN_TIMEOUT:?}"
                            );
                        }
                    }
                    // ② 清退三步必须**串行**（刷位点 → 解锁 → 注销）：并发送的话注销可能
                    // 插在解锁之前，broker 已按 clientId 丢掉记录，位点和锁白刷。
                    if !broadcast {
                        let items = shutdown_persist_items(&inner);
                        for (mq, off) in &items {
                            if let Err(e) = client
                                .update_consumer_offset(&group, mq, *off, 5000, None)
                                .await
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
                    client
                        .unregister_client_all_brokers(
                            &client_id,
                            "",
                            &group,
                            MQ_CLIENT_API_TIMEOUT_MILLIS,
                        )
                        .await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if client.find_consumer(&group).is_none() {
                        client
                            .unregister_client_all_brokers(
                                &client_id,
                                "",
                                &group,
                                MQ_CLIENT_API_TIMEOUT_MILLIS,
                            )
                            .await;
                    }
                    client.shutdown();
                };
                let _ = run_finalize_blocking(
                    &handle,
                    "push consumer shutdown",
                    SHUTDOWN_FINALIZE_BUDGET,
                    finalize,
                );
            } else {
                // 无运行时：末次持久化与注销都发不出去（Python 那里线程照起），
                // 但至少把该还的还掉。
                rmq_warn!(
                    "push shutdown: no tokio runtime, skip final offset persist and \
                           consumer unregister"
                );
                client.shutdown();
            }
        }
        lock(&self.inner.client).take();
        self.inner.started.store(false, Ordering::Release);
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

    /// `enable_trace` 时注册消费轨迹钩子并启动通道。
    ///
    /// 注入优先（[`set_trace_dispatcher`](Self::set_trace_dispatcher)，模块头差异 5）；
    /// `enable_trace=true` 且未注入时经 [`auto_bridged_dispatcher`] 自动建一套
    /// （Type=CONSUME + 真实内部生产者）。
    fn start_trace_dispatcher(&self) {
        let cfg = self.config();
        let dispatcher: Arc<dyn TraceDispatcherChannel> = match self.trace_dispatcher() {
            Some(d) => d,
            None if cfg.enable_trace => {
                match auto_bridged_dispatcher(
                    &cfg.consumer_group,
                    TraceDispatcherType::Consume,
                    self.rpc_hook(),
                    cfg.trace_msg_batch_num,
                    cfg.trace_topic.clone(),
                ) {
                    Ok(d) => {
                        // 建桥成功后把本消费者登记为轨迹通道的宿主
                        d.set_host_consumer(Arc::new(self.clone()));
                        let channel: Arc<dyn TraceDispatcherChannel> = d;
                        self.set_trace_dispatcher(Some(channel.clone()));
                        channel
                    }
                    Err(e) => {
                        // 该报错文案与线上基准逐字一致
                        rmq_error!(
                            "system mqtrace hook init failed ,maybe can't send msg trace data: {e}"
                        );
                        return;
                    }
                }
            }
            None => return,
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
        heartbeat_once(&self.inner).await
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

    /// 已持锁队列集合：`LOCK_BATCH_MQ` 真正拿到锁的队列（顺序消费才会填）。
    pub fn locked_queue_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = lock(&self.inner.state).lock_ok.iter().cloned().collect();
        keys.sort();
        keys
    }

    pub fn pop_orderly_request_count(&self) -> usize {
        lock(&self.inner.state).pop_orderly_requests.len()
    }

    // ---------------- 拉取停摆自愈的可观测接缝 ----------------
    //
    // 一并公开这些接缝：真机验证脚本要能把时钟倒拨、触发一次
    // rebalance，再确认「停摆的队列被撤掉重建、消息一条不重不丢」。这些判据本身
    // 是内部状态，没有接缝就只能靠 sleep 120s 猜。

    pub fn pull_stalled(&self, key: &str) -> bool {
        let state = lock(&self.inner.state);
        pull_stalled_locked(&state, key, current_time_millis())
    }

    /// 最近一次发起拉取/弹出的时刻（毫秒）；`None` 表示这把队列没盖过章。
    pub fn last_pull_at(&self, key: &str) -> Option<i64> {
        lock(&self.inner.state).last_pull_at.get(key).copied()
    }

    /// 把盖章强行拨到某个时刻（验证用：等价于「这条循环已经 X 毫秒没动静了」）。
    pub fn set_last_pull_at(&self, key: &str, millis: i64) {
        lock(&self.inner.state)
            .last_pull_at
            .insert(key.to_string(), millis);
    }

    // ---------------- OFFSET_ILLEGAL 纠错的可观测接缝 ----------------
    //
    // 一并公开这些接缝：真机验证脚本要能断言「队列被丢弃（代号 +1）、
    // 位点被冻结、缓冲里的旧消息整批作废」，这些都是内部状态，没有接缝就只能靠
    // 「消息有没有投过来」间接猜。

    pub fn queue_epoch(&self, key: &str) -> u64 {
        lock(&self.inner.state)
            .queue_epoch
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    pub fn offset_frozen(&self, key: &str) -> bool {
        lock(&self.inner.state).frozen_offsets.contains(key)
    }

    pub fn cached_message_count(&self, key: &str) -> usize {
        lock(&self.inner.state)
            .pending
            .get(key)
            .map_or(0, VecDeque::len)
    }

    // ---------------- cleanExpiredMsg 的可观测接缝 ----------------
    //
    // 一并公开这些接缝：真机验证要能断言「挂住的批次登记在册（带
    // CONSUME_START_TIME）、清扫把它从视图里收走、回投消息停在哪条队列的缓冲里」，
    // 判据全是内部状态，没有接缝就只能靠「消息有没有来第二次」间接猜。

    pub fn process_queue_entries(&self, key: &str) -> Vec<MessageExt> {
        let state = lock(&self.inner.state);
        process_queue_entries_locked(&state, key)
    }

    pub fn pending_messages(&self, key: &str) -> Vec<MessageExt> {
        lock(&self.inner.state)
            .pending
            .get(key)
            .map(|dq| dq.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 立刻执行一次循环集合同步（等价于 rebalance 的那一步，验证用）。
    ///
    /// 刻意做成 `async`：收尾要发 RPC，在已经是 async 上下文的调用方里再 `block_on`
    /// 会直接 panic。
    pub async fn sync_pull_threads(&self) {
        self.rebalance_pull_threads().await;
    }

    // ---------------- 重平衡 ----------------

    async fn all_queues_of_topic(&self, topic: &str) -> Vec<MessageQueue> {
        let Ok(client) = require_client(&self.inner) else {
            return Vec::new();
        };
        let queues = client.get_topic_subscribe_info(topic).await;
        if queues.is_empty() {
            rmq_debug!("rebalance: no subscribe info for topic {topic}");
        }
        queues
    }

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
        let orderly = lock(&self.inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        let broker_assignment = cfg.pop_mode
            && !orderly
            && cfg.message_model == MessageModel::CLUSTERING;

        let mut assigned: Vec<MessageQueue> = Vec::new();
        if cfg.message_model == MessageModel::BROADCASTING {
            for topic in &topics {
                assigned.extend(self.all_queues_of_topic(topic).await);
            }
        } else {
            for topic in &topics {
                if broker_assignment {
                    match self.assigned_from_broker(&group, topic).await {
                        Some(queues) => assigned.extend(queues),
                        None => {
                            let keep: Vec<MessageQueue> = lock(&self.inner.state)
                                .assigned
                                .iter()
                                .filter(|mq| mq.topic == *topic)
                                .cloned()
                                .collect();
                            assigned.extend(keep);
                        }
                    }
                    continue;
                }
                let mut mq_all = self.all_queues_of_topic(topic).await;
                sort_mqs(&mut mq_all);
                if mq_all.is_empty() {
                    continue;
                }
                let cid_all = client
                    .get_consumer_id_list_by_group(topic, &group, 5000)
                    .await;
                let Some(mut cid_all) = cid_all else {
                    rmq_debug!("rebalance: no consumer id list for {group}/{topic}, keep current");
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
                match strategy.allocate(
                    &group,
                    &cfg.client_id.clone().unwrap_or_default(),
                    &mq_all,
                    &cid_all,
                ) {
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

    /// 分配项自带的 mode（POP / PULL）只**观察不服从**：本消费者的消费模式是启动前
    /// 定死的（`pop_mode`），rebalance 消费的是队列**集合**；混合应答只告警不拆分
    /// —— 那几乎总是 `SET_MESSAGE_REQUEST_MODE` 没有全量生效的形状。
    async fn assigned_from_broker(
        &self,
        group: &str,
        topic: &str,
    ) -> Option<Vec<MessageQueue>> {
        let client = self.client()?;
        let cfg = self.config();
        let strategy_name = self
            .inner
            .strategy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get_name()
            .to_string();
        let assignments = match client
            .query_assignment(
                topic,
                group,
                &strategy_name,
                &cfg.message_model,
                QUERY_ASSIGNMENT_TIMEOUT_MILLIS,
            )
            .await
        {
            Ok(opt) => opt,
            Err(e) => {
                if e.response_code() == Some(response_code::TOPIC_NOT_EXIST) {
                    rmq_warn!(
                        "queryAssignment: topic {topic} not exist on broker, clear assignment \
                         for group {group}"
                    );
                    return Some(Vec::new());
                }
                rmq_error!(
                    "allocate message queue exception. strategy name: {strategy_name}, \
                     topic {topic}: {e} (keep current assignment)"
                );
                return None;
            }
        };
        let assignments = match assignments {
            Some(list) => list,
            None => {
                rmq_warn!(
                    "queryAssignment: no assignment set for topic {topic} group {group}, \
                     keep current assignment"
                );
                return None;
            }
        };
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut out: Vec<MessageQueue> = Vec::with_capacity(assignments.len());
        let mut pop_cnt = 0usize;
        let mut pull_cnt = 0usize;
        for a in assignments {
            let Some(mq) = a.message_queue else { continue };
            if mq.topic.is_empty() {
                continue;
            }
            let mq = MessageQueue::new(&mq.topic, &mq.broker_name, mq.queue_id);
            if !seen.insert(mq_key(&mq)) {
                continue;
            }
            if a.mode == MESSAGE_REQUEST_MODE_POP {
                pop_cnt += 1;
            } else {
                pull_cnt += 1;
            }
            out.push(mq);
        }
        if pop_cnt > 0 && pull_cnt > 0 {
            rmq_warn!(
                "queryAssignment for {group}/{topic} returned mixed modes \
                 (pop={pop_cnt} pull={pull_cnt}): the queue set is taken as-is, \
                 each queue is consumed per this client's own mode"
            );
        }
        Some(out)
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
        let group = self.consumer_group();
        let now = current_time_millis();
        {
            let mut state = lock(&self.inner.state);
            // ①撤：先收集，再统一摘干净（含停摆自愈）
            let doomed: Vec<(String, Option<MessageQueue>)> = state
                .queue_owners
                .keys()
                .filter_map(|key| {
                    if !current.contains_key(key) {
                        return Some((key.clone(), None));
                    }
                    if self.inner.started.load(Ordering::Acquire)
                        && pull_stalled_locked(&state, key, now)
                    {
                        rmq_error!(
                            "[BUG]doRebalance, {group}, try remove unnecessary mq, {key}, \
                             because pull is pause, so try to fixed it"
                        );
                        return Some((key.clone(), current.get(key).cloned()));
                    }
                    None
                })
                .collect();
            for (key, fallback) in doomed {
                Self::retire_queue_locked(&mut state, &key, fallback.as_ref(), &mut revoked, pop);
            }
            // ②建（只登记归属与 token，真正把任务 spawn 出去要等 ③收尾之后）：
            // 本轮不再拥有归属的队列（新分配 + 刚被自愈撤走的）都在这里重新接手
            for (key, mq) in &current {
                if state.queue_owners.contains_key(key) {
                    continue;
                }
                if pop && !state.pop_queues.contains_key(key) {
                    state.pop_queues.insert(key.clone(), PopProcessQueue::new());
                }
                state.frozen_offsets.remove(key);
                let token = self.inner.next_token.fetch_add(1, Ordering::SeqCst);
                state.queue_owners.insert(key.clone(), token);
                // Python 在分配那一刻就把 `_mq_map[key] = mq` 登记好：队列即使一条消息都没
                // 拉到，也要能在 307/220 里看到、位点也能持久化。
                state.mq_map.insert(key.clone(), mq.clone());
                // 新循环的第一次盖章由它自己在发起拉取时打；这里先给个起点，
                // 免得刚建好就被下一轮 rebalance 误判成「从没盖过章 = 停摆」。
                state.last_pull_at.insert(key.clone(), now);
                to_spawn.push((mq.clone(), token));
            }
        }
        if !revoked.is_empty() {
            self.on_queues_revoked(&revoked).await;
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
    }

    /// Python `_retire_queue_locked`：把一把队列的所有痕迹摘干净，并把它要持久化的
    /// 已消费位点交给 `revoked` 列表。
    ///
    /// `fallback_mq` 是停摆自愈用的：队列仍在分配里（`mq_map` 不能留成脏），
    /// 而真正被撤销的队列走 `mq_map` 反查即可。
    fn retire_queue_locked(
        state: &mut State,
        key: &str,
        fallback_mq: Option<&MessageQueue>,
        revoked: &mut Vec<(MessageQueue, Option<i64>)>,
        pop: bool,
    ) {
        state.queue_owners.remove(key); // 循环内下一轮 owns_queue 失效
        state.pending.remove(key);
        state.inflight_msgs.remove(key);
        state.lock_ok.remove(key);
        state.last_pull_at.remove(key); // 同名队列复用时不能继承旧时刻
        *state.queue_epoch.entry(key.to_string()).or_insert(0) += 1;
        let off = state.consume_offsets.remove(key);
        state.offset_table.remove(key);
        let mq = state.mq_map.remove(key).or_else(|| fallback_mq.cloned());
        if pop {
            if let Some(pq) = state.pop_queues.remove(key) {
                pq.set_dropped(true);
            }
        }
        if let Some(mq) = mq {
            revoked.push((mq, off));
        }
    }

    async fn on_queues_revoked(&self, revoked: &[(MessageQueue, Option<i64>)]) {
        revoke_queues(&self.inner, revoked).await;
    }

    /// Python `_on_consumer_ids_changed`（40 的处理）。
    fn request_rebalance(&self) {
        self.inner.rebalance_now.store(true, Ordering::SeqCst);
        self.inner.rebalance_signal.notify_waiters();
    }
}

// ---------------- 位点解析 ----------------

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
        let stored = load_local_offsets(inner);
        if let Some(off) = stored.get(&key) {
            return Ok(*off);
        }
        if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET {
            return Ok(0);
        }
        return client.get_max_offset(mq, 5000, None).await;
    }
    match client
        .query_consumer_offset(&cfg.consumer_group, mq, 5000, None, false)
        .await
    {
        Ok(Some(off)) if off >= 0 => return Ok(off),
        Ok(_) => {}
        Err(e) => rmq_debug!("query consumer offset for {mq:?} not found: {e}"),
    }
    if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET {
        return Ok(0);
    }
    if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_TIMESTAMP {
        let ts = consume_timestamp_millis(&cfg.consume_timestamp)?;
        return client.search_offset_by_timestamp(mq, ts, 5000, None).await;
    }
    if NamespaceUtil::is_retry_topic(&mq.topic) {
        return Ok(0);
    }
    client.get_max_offset(mq, 5000, None).await
}

// ---------------- 队列撤销与位点纠错 ----------------

/// 自由函数（与下面的 `handle_offset_illegal` 同一理由）：拉取循环只持有 `Arc<Inner>`。
async fn revoke_queues(inner: &Arc<Inner>, revoked: &[(MessageQueue, Option<i64>)]) {
    if revoked.is_empty() {
        return;
    }
    let cfg = read_cfg(inner);
    if cfg.message_model == MessageModel::BROADCASTING {
        if let Err(e) = save_local_offsets_with(inner, revoked) {
            rmq_debug!("save local offsets failed: {e}");
        }
        return;
    }
    let Ok(client) = require_client(inner) else {
        return;
    };
    let group = cfg.consumer_group.clone();
    let orderly = lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly);
    let client_id = cfg.client_id.clone().unwrap_or_default();
    for (mq, off) in revoked {
        if let Some(off) = off {
            if let Err(e) = client
                .update_consumer_offset(&group, mq, *off, 5000, None)
                .await
            {
                rmq_debug!("persist offset on revoke failed for {mq:?}: {e}");
            }
        }
        if orderly && !cfg.pop_mode {
            let _ = client
                .unlock_batch_mq(&group, &client_id, std::slice::from_ref(mq), 1000)
                .await;
        }
    }
    rmq_info!("queues revoked, group={group} count={}", revoked.len());
}

/// broker 说"你请求的位点非法"（队列被截断、commitlog 过期或被服务端重置，见
/// `PullMessageProcessor` 对 OFFSET_OVERFLOW_BADLY / NO_MESSAGE_IN_QUEUE / OFFSET_RESET
/// 一律回 `PULL_OFFSET_MOVED`），并把修正值放在 `nextBeginOffset` 里。纠错不只是把游标
/// 拨过去：这条队列上**已经取回、还没消费/没 ack 的消息全部作废**（它们落在被跳过的区间
/// 里，ack 它们会把位点推回非法值），然后按修正位点把队列重建。
///
///
/// 须在锁外调用：里面要做网络 RPC。调用点在拉取回调持锁块之后。
async fn handle_offset_illegal(inner: &Arc<Inner>, key: &str) {
    let mut revoked: Vec<(MessageQueue, Option<i64>)> = Vec::new();
    {
        let mut state = lock(&inner.state);
        DefaultMQPushConsumer::retire_queue_locked(
            &mut state,
            key,
            None,
            &mut revoked,
            read_cfg(inner).pop_mode,
        );
    }
    revoke_queues(inner, &revoked).await;
    inner.rebalance_now.store(true, Ordering::SeqCst);
    inner.rebalance_signal.notify_waiters();
    rmq_warn!("the pull request offset illegal, fix it, queue={key}");
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

/// Python 里循环入口的 `self._last_pull_table[key] = time.time()` +
/// `pq.last_pop_timestamp = now`：在**发起**本轮拉取/弹出前盖章。
///
/// POP 模式下两处时间一起写：`_last_pull_table` 供 rebalance 判停摆，
/// [`PopProcessQueue::last_pop_timestamp`] 供 `ConsumerRunningInfo` 的运维视图。
fn stamp_pull_at(inner: &Inner, key: &str, pop: bool) {
    let now = current_time_millis();
    let pq = if pop {
        lock(&inner.state).pop_queues.get(key).cloned()
    } else {
        None
    };
    lock(&inner.state).last_pull_at.insert(key.to_string(), now);
    if let Some(pq) = pq {
        pq.touch(now);
    }
}

/// 拉取停摆判据：仍归本实例、但拉取循环已经停摆的队列。
fn pull_stalled_locked(state: &State, key: &str, now: i64) -> bool {
    match state.last_pull_at.get(key) {
        None => false,
        Some(&0) => true,
        Some(&began) => now - began > PULL_MAX_IDLE_TIME,
    }
}

/// 拉取循环退出标记：循环**自己**退出时留下哨兵，
/// 让下一轮 rebalance 把它当成停摆重建。仍在归属表里才写，否则说明已被撤走、
/// 新属主已经接管，写 `0` 反而会误伤它。
fn mark_pull_loop_exited(inner: &Inner, key: &str, token: u64) {
    let mut state = lock(&inner.state);
    if state.queue_owners.get(key).is_some_and(|t| *t == token) {
        state.last_pull_at.insert(key.to_string(), 0);
    }
}

fn pull_subscription_expression(cfg: &ConsumerConfig, sub: &SubscriptionData) -> Option<String> {
    if cfg.post_subscription_when_pull && !sub.class_filter_mode {
        Some(sub.sub_string.clone())
    } else {
        None
    }
}

fn recalc_pull_from_which_node(state: &State, mq: &MessageQueue) -> i64 {
    state
        .pull_from_which_node
        .get(mq)
        .copied()
        .unwrap_or(MixAll::MASTER_ID as i64)
}

fn update_pull_from_which_node(state: &mut State, mq: &MessageQueue, suggest: Option<i64>) {
    state
        .pull_from_which_node
        .insert(mq.clone(), suggest.unwrap_or(MixAll::MASTER_ID as i64));
}

/// Python `_queue_pull_loop`：单队列长轮询拉取 → 推入待消费缓冲。
///
/// 每队列一个循环（不是共享线程池），因为 broker 为每个队列各挂起一个长轮询；
/// 共用一个循环会让空队列的 ~15s 挂起阻塞其余队列的投递。
///
/// 这层壳只负责收尾：任务体无论从哪里 `return`，只要还占着归属就留下停摆哨兵，
/// 下一轮 rebalance 会重建它（Python 靠 `thread.is_alive()`，tokio 任务没这个问法）。
async fn queue_pull_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let key = mq_key(&mq);
    run_queue_pull_loop(inner.clone(), mq, token).await;
    mark_pull_loop_exited(&inner, &key, token);
}

async fn run_queue_pull_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let Ok(client) = require_client(&inner) else {
        return;
    };
    let key = mq_key(&mq);
    let mut rx = inner.stop.subscribe();
    while !stopped(&rx) && inner.started.load(Ordering::Acquire) {
        if !owns_queue(&inner, &key, token) {
            return;
        }
        stamp_pull_at(&inner, &key, false);
        if inner.pause.load(Ordering::SeqCst) {
            if wait_or_stop(&mut rx, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND).await {
                return;
            }
            continue;
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
            if wait_or_stop(&mut rx, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL).await {
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
        let sub_expression = pull_subscription_expression(&cfg, &sub);
        let sys_flag =
            PullSysFlag::build_sys_flag_basic(false, true, sub_expression.is_some(), false);
        let broker_id = {
            let state = lock(&inner.state);
            recalc_pull_from_which_node(&state, &mq)
        };
        let began = current_time_millis();
        let result = match client
            .pull_message(
                &cfg.consumer_group,
                &mq,
                offset,
                cfg.pull_batch_size,
                sys_flag,
                0,
                sub_expression.as_deref().unwrap_or(""),
                sub.sub_version,
                &sub.expression_type,
                cfg.pull_timeout_millis,
                cfg.pull_batch_size_in_bytes,
                cfg.pull_suspend_timeout_millis,
                None,
                0,
                Some(broker_id),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                // 长轮询在 suspend 期间无新消息触发客户端超时属**正常行为**：broker 把
                // 挂起时长钳制到自己的 brokerSuspendMaxTimeMillis，空闲队列会周期性超时。
                let benign = matches!(e, Error::Timeout { .. } | Error::TooMuchRequest(_));
                if benign {
                    rmq_debug!("pull long-poll timeout for {mq:?} (benign, will retry): {e}");
                    continue;
                }
                let delay = match &e {
                    Error::Server {
                        response_code: response_code::FLOW_CONTROL,
                        ..
                    }
                    | Error::Broker {
                        response_code: response_code::FLOW_CONTROL,
                        ..
                    } => PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
                    _ => PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION,
                };
                // 其余多为 topic 尚未创建等预期路径：debug + 短暂退避，避免热循环
                rmq_debug!("pull error for {mq:?}: {e}");
                if wait_or_stop(&mut rx, delay).await {
                    return;
                }
                continue;
            }
        };
        {
            let mut state = lock(&inner.state);
            update_pull_from_which_node(&mut state, &mq, result.suggest_which_broker_id);
        }
        if let Some(stats) = lock(&inner.stats).clone() {
            stats.inc_pull_rt(
                &cfg.consumer_group,
                &mq.topic,
                current_time_millis() - began,
            );
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
            msgs = filter_messages_for_delivery(
                &cfg.consumer_group,
                &inner.filter_hooks,
                &mq,
                Some(&sub),
                msgs,
                cfg.unit_mode,
            );
        }
        let mut illegal = false;
        {
            let mut state = lock(&inner.state);
            if state.queue_owners.get(&key).is_none_or(|t| *t != token) {
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
            if result.status == PullStatus::OffsetIllegal {
                if result.next_begin_offset >= 0 {
                    state
                        .consume_offsets
                        .insert(key.clone(), result.next_begin_offset);
                }
                state.frozen_offsets.insert(key.clone());
                illegal = true;
            } else {
                correct_tags_offset_locked(
                    &mut state,
                    &key,
                    result.status,
                    result.next_begin_offset,
                );
            }
        }
        // update_msg_acc_cnt 自己取同一把锁，必须在上面释放之后调用。
        update_msg_acc_cnt(&inner, &key, &msgs);
        if illegal {
            handle_offset_illegal(&inner, &key).await;
        }
    }
}

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
        lock(&inner.state)
            .msg_acc_cnt
            .insert(key.to_string(), acc_total);
    }
}

fn flow_control_hit(inner: &Inner, mq: &MessageQueue, key: &str) -> bool {
    let cfg = read_cfg(inner);
    let (count, size_mb, span, topic_pending) = {
        let state = lock(&inner.state);
        let dq: &VecDeque<MessageExt> = state.pending.get(key).unwrap_or(&EMPTY_DEQUE);
        let count = dq.len();
        let size_mb =
            dq.iter().map(|m| i64::from(m.store_size)).sum::<i64>() as f64 / (1024.0 * 1024.0);
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
            .filter(|(k, _)| state.mq_map.get(*k).is_some_and(|m| m.topic == mq.topic))
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
    } else if cfg.consume_concurrently_max_span > 0 && span > cfg.consume_concurrently_max_span {
        Some(format!("span={span}"))
    } else if cfg.pull_threshold_for_topic > 0 || cfg.pull_threshold_size_for_topic > 0 {
        if cfg.pull_threshold_for_topic > 0
            && topic_pending.len() >= cfg.pull_threshold_for_topic as usize
        {
            Some(format!("topicCount={}", topic_pending.len()))
        } else if cfg.pull_threshold_size_for_topic > 0 {
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
            inner.flow_control_triggered.fetch_add(1, Ordering::SeqCst);
            rmq_debug!("flow control: queue {key} {reason}, pause pull");
            true
        }
    }
}

static EMPTY_DEQUE: VecDeque<MessageExt> = VecDeque::new();

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
        let starting_up = current_time_millis()
            - consumer.inner.start_time_millis.load(Ordering::SeqCst)
            < 60_000;
        let interval = if starting_up && consumer.assigned_queue_count() == 0 {
            2_000
        } else {
            20_000
        };
        let requested = consumer.inner.rebalance_now.swap(false, Ordering::SeqCst);
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

/// 发一轮心跳（`heartbeat_loop` 的周期心跳、`send_heartbeat_to_all_broker` 的显式调用、
/// 以及 `subscribe` 之后的立即心跳共用），返回成功的 broker 台数。
async fn heartbeat_once(inner: &Arc<Inner>) -> usize {
    let Ok(client) = require_client(inner) else {
        return 0;
    };
    let ok = client.send_heartbeat_to_all_broker(5000).await;
    if ok > 0 {
        inner.heartbeat_count.fetch_add(1, Ordering::SeqCst);
    }
    ok
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
        heartbeat_once(&inner).await;
    }
}

async fn lock_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    loop {
        if !read_cfg(&inner).pop_mode {
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
            let (mq, batch, epoch) = {
                let mut state = lock(&inner.state);
                let Some(mq) = state.mq_map.get(&key).cloned() else {
                    continue;
                };
                let batch = {
                    let Some(dq) = state.pending.get_mut(&key) else {
                        continue;
                    };
                    if dq.is_empty() {
                        continue;
                    }
                    let cfg = read_cfg(&inner);
                    let n = dq
                        .len()
                        .min(cfg.consume_message_batch_max_size.max(1) as usize);
                    let batch: Vec<MessageExt> = dq.drain(..n).collect();
                    batch
                };
                // 与"取走批次"同一临界区登记在途：correct_tags_offset_locked 的闸门靠它
                // 区分"消息还在缓冲里"和"消息在 listener 手里"——两个都不算空。
                *state.inflight.entry(key.clone()).or_insert(0) += 1;
                let epoch = state.queue_epoch.get(&key).copied().unwrap_or(0);
                (mq, batch, epoch)
            };
            if batch.is_empty() {
                continue;
            }
            let outcome = consume_batch(&inner, &key, &mq, batch.clone(), Some(epoch)).await;
            {
                let mut state = lock(&inner.state);
                let left = match state.inflight.get_mut(&key) {
                    Some(v) => {
                        *v = v.saturating_sub(1);
                        *v
                    }
                    None => 0,
                };
                if left == 0 {
                    state.inflight.remove(&key);
                }
            }
            match outcome {
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
        tasks.push(spawn_loop!(lock_loop));
        // 分发与重平衡：重平衡需要句柄（要起每队列循环），单独用消费者克隆体。
        // 分发循环不进 tasks（停机不能 abort 它 —— 它可能正带着在途回投 RPC），
        // 单独登记进 dispatch_task，停机时由收尾任务有界等待。
        let consumer = self.clone();
        let rx = self.inner.stop.subscribe();
        tasks.push(handle.spawn(async move { rebalance_loop(consumer, rx).await }));
        *lock(&self.inner.dispatch_task) = Some(spawn_loop!(dispatch_loop));
        let orderly = lock(&self.inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        if !orderly && !read_cfg(&self.inner).pop_mode {
            tasks.push(spawn_loop!(clean_expire_loop));
        }
    }
}

/// 停机位点持久化清单：`consume_offsets` 与「仍未落定消息的最小 queueOffset」取小。
fn shutdown_persist_items(inner: &Inner) -> Vec<(MessageQueue, i64)> {
    let state = lock(&inner.state);
    state
        .consume_offsets
        .iter()
        .filter_map(|(k, off)| {
            let mq = state.mq_map.get(k)?;
            let floor = state
                .inflight_msgs
                .get(k)
                .into_iter()
                .flatten()
                .chain(state.pending.get(k).into_iter().flatten())
                .map(|m| m.queue_offset)
                .min();
            Some((mq.clone(), floor.map_or(*off, |f| (*off).min(f))))
        })
        .collect()
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

fn local_offset_path(inner: &Inner) -> Option<std::path::PathBuf> {
    let cfg = read_cfg(inner);
    let home = crate::common::util_all::user_home()?;
    let base = std::path::Path::new(&home)
        .join(".rocketmq_offsets")
        .join(cfg.client_id.as_deref().unwrap_or("DEFAULT"))
        .join(&cfg.consumer_group);
    Some(base.join("offsets.json"))
}

fn build_local_offsets_json(
    items: &BTreeMap<String, i64>,
    mq_map: &BTreeMap<String, MessageQueue>,
) -> String {
    fn jstr(s: &str) -> String {
        serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
    }
    let mut s = String::from("{\"offsetTable\":{");
    let mut first = true;
    for (key, off) in items {
        let Some(mq) = mq_map.get(key) else { continue };
        if !first {
            s.push(',');
        }
        first = false;
        s.push_str(&format!(
            "{{\"brokerName\":{},\"queueId\":{},\"topic\":{}}}:{}",
            jstr(&mq.broker_name),
            mq.queue_id,
            jstr(&mq.topic),
            off
        ));
    }
    s.push_str("}}");
    s
}

fn parse_local_offsets_text(text: &str) -> Option<BTreeMap<String, i64>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(flat) = serde_json::from_str::<BTreeMap<String, i64>>(trimmed) {
        return Some(flat);
    }
    let entries = scan_offset_store_entries(trimmed)?;
    Some(
        entries
            .into_iter()
            .map(|(topic, broker, qid, off)| {
                (mq_key(&MessageQueue::new(&topic, &broker, qid)), off)
            })
            .collect(),
    )
}

/// 无 regex crate，手写扫描器：`{"offsetTable":{{...}:9,...}}`（容忍 fastjson2
/// pretty 版的换行缩进）。返回 `(topic, brokerName, queueId, offset)` 列表。
fn scan_offset_store_entries(text: &str) -> Option<Vec<(String, String, i32, i64)>> {
    struct Scanner<'a> {
        b: &'a [u8],
        pos: usize,
    }
    impl<'a> Scanner<'a> {
        fn skip_ws(&mut self) {
            while self.pos < self.b.len() && self.b[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            }
        }
        fn eat(&mut self, c: u8) -> bool {
            self.skip_ws();
            if self.b.get(self.pos) == Some(&c) {
                self.pos += 1;
                true
            } else {
                false
            }
        }
        fn peek(&mut self) -> Option<u8> {
            self.skip_ws();
            self.b.get(self.pos).copied()
        }
        fn scan_string(&mut self) -> Option<String> {
            if self.peek()? != b'"' {
                return None;
            }
            self.pos += 1;
            let mut out = String::new();
            let mut raw_start = self.pos;
            loop {
                let c = *self.b.get(self.pos)?;
                if c == b'"' {
                    out.push_str(std::str::from_utf8(&self.b[raw_start..self.pos]).ok()?);
                    self.pos += 1;
                    return Some(out);
                }
                if c == b'\\' {
                    out.push_str(std::str::from_utf8(&self.b[raw_start..self.pos]).ok()?);
                    self.pos += 1;
                    let e = *self.b.get(self.pos)?;
                    self.pos += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hex =
                                std::str::from_utf8(self.b.get(self.pos..self.pos + 4)?).ok()?;
                            out.push(char::from_u32(u32::from_str_radix(hex, 16).ok()?)?);
                            self.pos += 4;
                        }
                        _ => return None,
                    }
                    raw_start = self.pos;
                    continue;
                }
                self.pos += 1;
            }
        }
        fn scan_int(&mut self) -> Option<i64> {
            self.skip_ws();
            let start = self.pos;
            if self.b.get(self.pos) == Some(&b'-') {
                self.pos += 1;
            }
            let digits = self.pos;
            while matches!(self.b.get(self.pos), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
            if self.pos == digits {
                return None;
            }
            std::str::from_utf8(&self.b[start..self.pos])
                .ok()?
                .parse()
                .ok()
        }
    }

    let mut s = Scanner {
        b: text.as_bytes(),
        pos: 0,
    };
    if !s.eat(b'{') {
        return None;
    }
    if s.scan_string()?.as_str() != "offsetTable" {
        return None;
    }
    if !s.eat(b':') || !s.eat(b'{') {
        return None;
    }
    let mut out = Vec::new();
    loop {
        if s.eat(b'}') {
            break;
        }
        if !s.eat(b'{') {
            return None;
        }
        let (mut topic, mut broker, mut qid) = (None, None, None);
        loop {
            if s.eat(b'}') {
                break;
            }
            let fkey = s.scan_string()?;
            if !s.eat(b':') {
                return None;
            }
            if s.peek() == Some(b'"') {
                let v = s.scan_string()?;
                match fkey.as_str() {
                    "topic" => topic = Some(v),
                    "brokerName" => broker = Some(v),
                    _ => {}
                }
            } else {
                let v = s.scan_int()?;
                if fkey == "queueId" {
                    qid = Some(i32::try_from(v).ok()?);
                }
            }
            if s.eat(b',') {
                continue;
            }
            if s.eat(b'}') {
                break;
            }
            return None;
        }
        if !s.eat(b':') {
            return None;
        }
        let off = s.scan_int()?;
        out.push((topic?, broker?, qid?, off));
        if s.eat(b',') {
            continue;
        }
        if s.eat(b'}') {
            break;
        }
        return None;
    }
    // 表收尾后还有 wrapper 本身的 `}`（{"offsetTable":{...}} 两层括号）
    if !s.eat(b'}') {
        return None;
    }
    s.skip_ws();
    if s.pos != s.b.len() {
        return None;
    }
    Some(out)
}

fn save_local_offsets(inner: &Inner) -> Result<()> {
    save_local_offsets_with(inner, &[])
}

/// `save_local_offsets` 加"刚被撤下来、已不在 `consume_offsets`/`mq_map` 里的队列位点"。
fn save_local_offsets_with(inner: &Inner, extra: &[(MessageQueue, Option<i64>)]) -> Result<()> {
    let Some(path) = local_offset_path(inner) else {
        return Err(Error::client("HOME is not set, cannot store local offsets"));
    };
    let (mut items, mut mq_map) = {
        let st = lock(&inner.state);
        (st.consume_offsets.clone(), st.mq_map.clone())
    };
    merge_retired_offsets(&mut items, &mut mq_map, extra);
    save_local_offsets_at(&path, &items, &mq_map)
}

/// 把刚撤下来（已不在 `consume_offsets`/`mq_map` 里）的队列位点并进要落盘的快照。
///
/// 队列信息也要补进 `mq_map`：`build_local_offsets_json` 查不到队列就跳过该条目，
/// 只并位点等于没并。
fn merge_retired_offsets(
    items: &mut BTreeMap<String, i64>,
    mq_map: &mut BTreeMap<String, MessageQueue>,
    extra: &[(MessageQueue, Option<i64>)],
) {
    for (mq, off) in extra {
        let Some(off) = off else { continue };
        let key = mq_key(mq);
        items.insert(key.clone(), *off);
        mq_map.entry(key).or_insert_with(|| mq.clone());
    }
}

fn save_local_offsets_at(
    path: &std::path::Path,
    items: &BTreeMap<String, i64>,
    mq_map: &BTreeMap<String, MessageQueue>,
) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    if let Ok(prev) = std::fs::read_to_string(path) {
        if !prev.is_empty() {
            let _ = std::fs::write(path.with_file_name("offsets.json.bak"), prev);
        }
    }
    let tmp = path.with_file_name("offsets.json.tmp");
    std::fs::write(&tmp, build_local_offsets_json(items, mq_map))?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Python `_load_local_offsets`：主文件缺失/损坏时回退 `.bak`，都失败当「没有
/// 本地位点」。
fn load_local_offsets(inner: &Inner) -> BTreeMap<String, i64> {
    let Some(path) = local_offset_path(inner) else {
        return BTreeMap::new();
    };
    load_local_offsets_at(&path)
}

fn load_local_offsets_at(path: &std::path::Path) -> BTreeMap<String, i64> {
    for candidate in [path.to_path_buf(), path.with_file_name("offsets.json.bak")] {
        let Ok(text) = std::fs::read_to_string(&candidate) else {
            continue;
        };
        if let Some(parsed) = parse_local_offsets_text(&text) {
            return parsed;
        }
    }
    BTreeMap::new()
}

// ================================================================ POP 消费

/// 独立成函数是因为漏记是**静默**故障：消息照弹照 ack、消费完全正常，只有 307
/// 状态表（运维看板）上一片 0 —— 而"这个消费者没在拉取"和"压根没起来"是两种处置。
fn record_pop_pull_stats(
    stats: &ConsumerStatsManager,
    group: &str,
    topic: &str,
    result: &PopResult,
    began: i64,
) {
    if result.status != PopStatus::Found {
        return;
    }
    stats.inc_pull_rt(group, topic, current_time_millis() - began);
    if !result.msg_found_list.is_empty() {
        stats.inc_pull_tps(
            group,
            topic,
            i64::try_from(result.msg_found_list.len()).unwrap_or(i64::MAX),
        );
    }
}

/// 与拉取路径的关键差别：**不查也不提交消费位点**（进度由 broker 侧 checkpoint
/// 跟踪，确认只靠 ack）；弹出即投递给消费线程；`POLLING_NOT_FOUND` 是正常态。
async fn queue_pop_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let key = mq_key(&mq);
    run_queue_pop_loop(inner.clone(), mq, token).await;
    mark_pull_loop_exited(&inner, &key, token);
}

async fn run_queue_pop_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let Ok(client) = require_client(&inner) else {
        return;
    };
    let key = mq_key(&mq);
    let cfg = read_cfg(&inner);
    let mut invisible = cfg.pop_invisible_time;
    if !(MIN_POP_INVISIBLE_TIME..=MAX_POP_INVISIBLE_TIME).contains(&invisible) {
        invisible = DEFAULT_POP_INVISIBLE_TIME;
    }
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
        stamp_pull_at(&inner, &key, true);
        if inner.pause.load(Ordering::SeqCst) {
            if wait_or_stop(&mut rx, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND).await {
                return;
            }
            continue;
        }
        let sub = match lock(&inner.state).subscription(&mq.topic).cloned() {
            Some(sub) => sub,
            None => return,
        };
        let cfg = read_cfg(&inner);
        if pq.wait_ack_count() > cfg.pop_threshold_for_queue {
            if wait_or_stop(&mut rx, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL).await {
                return;
            }
            continue;
        }
        let began = current_time_millis();
        let exp = if sub.sub_string.is_empty() {
            "*"
        } else {
            &sub.sub_string
        };
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
                let delay = match &e {
                    Error::Server {
                        response_code: response_code::FLOW_CONTROL,
                        ..
                    }
                    | Error::Broker {
                        response_code: response_code::FLOW_CONTROL,
                        ..
                    } => PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
                    _ => PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION,
                };
                rmq_debug!("pop error for {mq:?}: {e}");
                if wait_or_stop(&mut rx, delay).await {
                    return;
                }
                continue;
            }
        };
        if !owns_queue(&inner, &key, token) || pq.is_dropped() {
            rmq_debug!(
                "queue {key} revoked during pop, discard {} messages un-acked",
                result.msg_found_list.len()
            );
            return;
        }
        pq.touch(current_time_millis());
        // 拉取统计：与 pull 路径同一份状态表，POP 模式才有；放在撤队列早退之后，
        // 已丢弃的那一批不进统计（那批消息压根没投递）。
        if let Some(stats) = lock(&inner.stats).clone() {
            let group = read_cfg(&inner).consumer_group;
            record_pop_pull_stats(&stats, &group, &mq.topic, &result, began);
        }
        if result.status == PopStatus::Found && !result.msg_found_list.is_empty() {
            pq.inc_found_msg(i32::try_from(result.msg_found_list.len()).unwrap_or(i32::MAX));
            let kept = filter_messages_for_delivery(
                &cfg.consumer_group,
                &inner.filter_hooks,
                &mq,
                Some(&sub),
                result.msg_found_list.clone(),
                cfg.unit_mode,
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

async fn submit_pop_consume_request(
    inner: &Arc<Inner>,
    msgs: Vec<MessageExt>,
    pq: Arc<PopProcessQueue>,
    mq: MessageQueue,
) {
    let orderly = lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly);
    if orderly {
        submit_pop_orderly_request(inner, pq, mq, false).await;
        return;
    }
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
            // 未起执行器（单测或未 start）：就地同步执行，不丢批次
            None => consume_pop_batch(inner.clone(), batch, pq, mq).await,
        }
    }
}

async fn submit_pop_orderly_request(
    inner: &Arc<Inner>,
    pq: Arc<PopProcessQueue>,
    mq: MessageQueue,
    force: bool,
) {
    let key = (Arc::as_ptr(&pq) as usize, mq_key(&mq));
    {
        let mut state = lock(&inner.state);
        let is_new = state.pop_orderly_requests.insert(key.clone());
        if !force && !is_new {
            return;
        }
    }
    let executor = lock(&inner.pop_executor).clone();
    match executor {
        Some(executor) => {
            let w = Arc::downgrade(inner);
            let task_mq = mq.clone();
            let task: ConsumeTask = Box::pin(async move {
                if let Some(inner) = w.upgrade() {
                    run_pop_orderly_request(inner, pq, task_mq).await;
                }
            });
            if let Err(e) = executor.submit(task) {
                rmq_error!("error submit pop orderly request: {e}, mq: {mq:?}");
            }
        }
        // 未起执行器（单测或未 start）：同步执行，与并发路径一致
        None => run_pop_orderly_request(inner.clone(), pq, mq).await,
    }
}

async fn run_pop_orderly_request(inner: Arc<Inner>, pq: Arc<PopProcessQueue>, mq: MessageQueue) {
    if pq.is_dropped() {
        rmq_warn!("run, message queue not be able to consume, because it's dropped. {mq:?}");
        let key = (Arc::as_ptr(&pq) as usize, mq_key(&mq));
        lock(&inner.state).pop_orderly_requests.remove(&key);
    }
}

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
        rmq_debug!(
            "pop timeout, abort consume for {mq:?}: popTime={pop_time} invisible={invisible}"
        );
        pq.dec_found_msg(i32::try_from(msgs.len()).unwrap_or(i32::MAX));
        return;
    }
    let mut msgs = msgs;
    reset_retry_topic_and_namespace(&inner, &mut msgs);
    let mut context = ConsumeConcurrentlyContext::new(Some(mq.clone()));
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
    let pop_stamp_now = current_time_millis();
    for m in msgs.iter_mut() {
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, &pop_stamp_now.to_string());
    }
    let (status, has_exception) = call_concurrently_listener(&inner, &msgs, &mut context).await;
    let raw_status = status;
    let status = match status {
        Some(s) => s,
        None => {
            rmq_warn!(
                "consumeMessage return null, Group: {} Msgs: {} MQ: {mq:?}",
                read_cfg(&inner).consumer_group,
                msgs.len()
            );
            ConsumeConcurrentlyStatus::ReconsumeLater
        }
    };
    let failed = matches!(status, ConsumeConcurrentlyStatus::ReconsumeLater);
    let succeeded = matches!(status, ConsumeConcurrentlyStatus::ConsumeSuccess);
    record_consume_stats(&inner, &mq.topic, msgs.len(), begin_ms, failed, None);
    if let Some(ctx) = hook_ctx.as_mut() {
        finish_consume_hook(
            &inner,
            ctx,
            raw_status.map(ConsumeConcurrentlyStatus::name),
            status.name(),
            has_exception,
            begin_ms,
            failed,
            succeeded,
        );
    }
    // 消费期间队列被撤走或已超时：结果不再处理
    if pq.is_dropped() || is_pop_timeout(&msgs, pop_time, invisible) {
        pq.dec_found_msg(i32::try_from(msgs.len()).unwrap_or(i32::MAX));
        return;
    }
    process_pop_consume_result(&inner, status, &context, &msgs, &pq).await;
}

fn is_pop_timeout(msgs: &[MessageExt], pop_time: i64, invisible: i64) -> bool {
    if msgs.is_empty() || pop_time <= 0 || invisible <= 0 {
        return true;
    }
    current_time_millis() - pop_time >= invisible
}

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
    let group = read_cfg(inner).consumer_group;
    let Some(handle) = inner
        .runtime
        .get()
        .cloned()
        .or_else(|| tokio::runtime::Handle::try_current().ok())
    else {
        rmq_debug!("ack skipped: no runtime to run the RPC");
        return;
    };
    handle.spawn(async move {
        let addr = match client.pop_ack_addr(&broker_name, &topic, queue_id).await {
            Ok(addr) => addr,
            Err(e) => {
                rmq_debug!("ack failed: {e}");
                return;
            }
        };
        match client
            .ack_message(
                &group,
                &topic,
                queue_id,
                &ck,
                offset,
                Some(&broker_name),
                3000,
                Some(&addr),
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
    let group = cfg.consumer_group.clone();
    let Some(handle) = inner
        .runtime
        .get()
        .cloned()
        .or_else(|| tokio::runtime::Handle::try_current().ok())
    else {
        rmq_debug!("change invisible time skipped: no runtime to run the RPC");
        return;
    };
    handle.spawn(async move {
        let addr = match client.pop_ack_addr(&broker_name, &topic, queue_id).await {
            Ok(addr) => addr,
            Err(e) => {
                rmq_debug!("change invisible time failed: {e}");
                return;
            }
        };
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
                Some(&addr),
            )
            .await
        {
            Ok(_) => {}
            Err(e) => rmq_debug!("change invisible time failed: {e}"),
        }
    });
}

// ================================================================ 消费与钩子

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

fn record_consume_stats(
    inner: &Inner,
    topic: &str,
    msg_count: usize,
    begin_ms: i64,
    failed: bool,
    ack_count: Option<usize>,
) {
    let Some(stats) = lock(&inner.stats).clone() else {
        return;
    };
    let group = read_cfg(inner).consumer_group;
    let rt = current_time_millis() - begin_ms;
    let msgs = i64::try_from(msg_count).unwrap_or(0);
    if failed {
        stats.inc_consume_failed_tps(&group, topic, msgs);
    } else {
        let ok = ack_count.unwrap_or(msg_count);
        let ok64 = i64::try_from(ok).unwrap_or(0);
        stats.inc_consume_ok_tps(&group, topic, ok64);
        if msgs > ok64 {
            stats.inc_consume_failed_tps(&group, topic, msgs - ok64);
        }
    }
    stats.inc_consume_rt(&group, topic, rt);
}

fn record_consume_rt(inner: &Inner, topic: &str, begin_ms: i64) {
    let Some(stats) = lock(&inner.stats).clone() else {
        return;
    };
    let rt = current_time_millis() - begin_ms;
    stats.inc_consume_rt(&read_cfg(inner).consumer_group, topic, rt);
}

/// Python `_finish_consume_hook`：把 returnType/status/success 写回上下文并触发 after 钩子。
#[allow(clippy::too_many_arguments)]
fn finish_consume_hook(
    inner: &Inner,
    ctx: &mut ConsumeMessageContext,
    raw_status_name: Option<&str>,
    hook_status_name: &str,
    has_exception: bool,
    begin_ms: i64,
    failed: bool,
    succeeded: bool,
) {
    let cfg = read_cfg(inner);
    let rt = current_time_millis() - begin_ms;
    let ret = consume_return_type(
        raw_status_name,
        has_exception,
        rt,
        cfg.consume_timeout,
        failed,
    );
    ctx.props
        .get_or_insert_with(Default::default)
        .insert("ConsumeContextType".to_string(), ret.name().to_string());
    ctx.status = Some(hook_status_name.to_string());
    ctx.success = succeeded;
    execute_consume_hook_after(&inner.consume_hooks, ctx);
}

/// 调并发监听器。返回 `(status, has_exception)`。
///
/// 并发监听器调用的两处口径说明：
/// 1. trait 的返回类型是非 Optional 的枚举，「监听器自己不返回状态」这种情况
///    不可能出现；**没有设置监听器**时按「异常 → RECONSUME_LATER」处理，
///    `status=None` 就是这条路径。
/// 2. 监听器是用户代码，可能阻塞。这里放进
///    `spawn_blocking`：分发循环仍然是「等这一批消费完才继续」的串行语义，
///    但不会占住 tokio 的 worker 线程。监听器 panic 会被
///    `JoinError` 捕获，语义正好等于「抛异常 → RECONSUME_LATER」。
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
    epoch: Option<u64>,
) -> Result<bool> {
    {
        let state = lock(&inner.state);
        let dropped = state.queue_epoch.get(key).copied().unwrap_or(0);
        if epoch.is_some_and(|e| e != dropped) {
            rmq_warn!(
                "the message queue not be able to consume, because it's dropped. \
                 group={} mq={mq:?} msgs={} epoch={epoch:?}->{dropped}",
                read_cfg(inner).consumer_group,
                batch.len()
            );
            return Ok(false);
        }
    }
    let cfg = read_cfg(inner);
    let broadcast = cfg.message_model == MessageModel::BROADCASTING;
    let mut batch = batch;
    reset_retry_topic_and_namespace(inner, &mut batch);
    let orderly = lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly);

    if orderly {
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
        // `raw_status` 为 None 只可能是 listener 抛异常（call_orderly_listener 的 join 失败）
        let (raw_status, has_exception) = call_orderly_listener(inner, &batch, &mut ocontext).await;
        if matches!(
            raw_status,
            None | Some(ConsumeOrderlyStatus::Rollback)
                | Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment)
        ) {
            rmq_warn!(
                "consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {mq:?}",
                cfg.consumer_group,
                batch.len()
            );
        }
        let status = raw_status.unwrap_or(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let failed = status == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment;
        let succeeded = matches!(
            status,
            ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit
        );
        if let Some(ctx) = hook_ctx.as_mut() {
            finish_consume_hook(
                inner,
                ctx,
                raw_status.map(|s| s.name()),
                status.name(),
                has_exception,
                begin_ms,
                failed,
                succeeded,
            );
        }
        if ocontext.auto_commit {
            let status = match status {
                ConsumeOrderlyStatus::Commit | ConsumeOrderlyStatus::Rollback => {
                    rmq_warn!(
                        "the message queue consume result is illegal, \
                         we think you want to ack these message {mq:?}"
                    );
                    ConsumeOrderlyStatus::Success
                }
                other => other,
            };
            if status == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment {
                record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, true, None);
                if check_orderly_reconsume_times(inner, &mut batch).await {
                    requeue_pending(inner, key, &batch);
                    let suspend = orderly_suspend_millis(&cfg, &ocontext);
                    tokio::time::sleep(Duration::from_millis(suspend)).await;
                    return Ok(false);
                }
            } else {
                record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, false, None);
            }
            advance_consume_offset(inner, key, &batch, None, epoch);
            return Ok(true);
        }
        match status {
            ConsumeOrderlyStatus::Commit => {
                record_consume_rt(inner, &mq.topic, begin_ms);
                advance_consume_offset(inner, key, &batch, None, epoch);
                return Ok(true);
            }
            ConsumeOrderlyStatus::Rollback => {
                record_consume_rt(inner, &mq.topic, begin_ms);
                requeue_pending(inner, key, &batch);
                let suspend = orderly_suspend_millis(&cfg, &ocontext);
                tokio::time::sleep(Duration::from_millis(suspend)).await;
                return Ok(false);
            }
            ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, true, None);
                if check_orderly_reconsume_times(inner, &mut batch).await {
                    requeue_pending(inner, key, &batch);
                    let suspend = orderly_suspend_millis(&cfg, &ocontext);
                    tokio::time::sleep(Duration::from_millis(suspend)).await;
                }
                return Ok(false);
            }
            ConsumeOrderlyStatus::Success => {
                record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, false, None);
                requeue_pending(inner, key, &batch);
                let suspend = orderly_suspend_millis(&cfg, &ocontext);
                tokio::time::sleep(Duration::from_millis(suspend)).await;
                return Ok(false);
            }
        }
    }

    let hook_ctx = if inner.consume_hooks.has_hooks() {
        let mut ctx = build_consume_hook_context(inner, &batch, mq);
        execute_consume_hook_before(&inner.consume_hooks, &mut ctx);
        Some(ctx)
    } else {
        None
    };
    let begin_ms = current_time_millis();
    let stamp_now = current_time_millis();
    for m in batch.iter_mut() {
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, &stamp_now.to_string());
    }
    register_inflight_msgs(inner, key, &batch);
    let result = consume_concurrent_batch(
        inner, key, mq, &batch, epoch, &cfg, broadcast, hook_ctx, begin_ms,
    )
    .await;
    // 正常收尾（含回投失败回塞、被清扫摘除）后这批不再「在 listener 手里」；
    // 被清扫/回塞提前摘过的条目在这里是幂等的。
    deregister_inflight_msgs(inner, key, &batch);
    result
}

/// 从 [`consume_batch`] 拆出来只为把「登记在途 → 消费 → 注销在途」围成一段：
/// 并发分支有一堆提前返回（整批认可、广播丢弃、回投失败……），登记表摘不干净的话
/// 位点会被 `correct_tags_offset_locked` 的在途闸门永久挡停。before 钩子与
/// `begin_ms` 由调用方做（要在盖章/登记之前），这里只负责 listener 与其后的收尾。
#[allow(clippy::too_many_arguments)]
async fn consume_concurrent_batch(
    inner: &Arc<Inner>,
    key: &str,
    mq: &MessageQueue,
    batch: &[MessageExt],
    epoch: Option<u64>,
    cfg: &ConsumerConfig,
    broadcast: bool,
    mut hook_ctx: Option<ConsumeMessageContext>,
    begin_ms: i64,
) -> Result<bool> {
    let mut context = ConsumeConcurrentlyContext::new(Some(mq.clone()));
    let (raw_status, has_exception) = call_concurrently_listener(inner, batch, &mut context).await;
    if raw_status.is_none() {
        rmq_warn!(
            "consumeMessage return null, Group: {} Msgs: {} MQ: {mq:?}",
            cfg.consumer_group,
            batch.len()
        );
    }
    let failed = matches!(raw_status, Some(ConsumeConcurrentlyStatus::ReconsumeLater))
        || raw_status.is_none();
    let succeeded = matches!(raw_status, Some(ConsumeConcurrentlyStatus::ConsumeSuccess));
    // 钩子/统计仍看**原始**返回值（None 要记 RETURNNULL），ackIndex 判定才归一化。
    let status = raw_status.unwrap_or(ConsumeConcurrentlyStatus::ReconsumeLater);
    let batch_len = i32::try_from(batch.len()).unwrap_or(i32::MAX);
    let mut ack_index = context.ack_index;
    if status == ConsumeConcurrentlyStatus::ConsumeSuccess {
        if ack_index >= batch_len {
            ack_index = batch_len - 1;
        }
    } else {
        ack_index = -1;
    }
    let acked = if ack_index >= 0 {
        usize::try_from(ack_index).unwrap_or(0) + 1
    } else {
        0
    };
    record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, failed, Some(acked));
    if let Some(ctx) = hook_ctx.as_mut() {
        finish_consume_hook(
            inner,
            ctx,
            raw_status.map(|s| s.name()),
            status.name(),
            has_exception,
            begin_ms,
            failed,
            succeeded,
        );
    }
    if broadcast {
        let dropped = batch.len() - acked;
        if dropped > 0 {
            rmq_warn!(
                "BROADCASTING, the message consume failed, drop it: {dropped} msgs in {mq:?}"
            );
        }
        advance_consume_offset(inner, key, batch, None, epoch);
        return Ok(true);
    }
    if acked >= batch.len() {
        // 整批认可（默认路径）：什么都不用回投，位点直接前进
        advance_consume_offset(inner, key, batch, None, epoch);
        return Ok(true);
    }
    // 集群模式：未认可的 [acked..) 逐条回投 %RETRY%topic（延迟档位 3+reconsumeTimes；
    // 超限由 broker 转 %DLQ%）
    let msg_back_failed = send_back_batch(
        inner,
        key,
        &batch[acked..],
        acked,
        context.delay_level_when_next_consume,
    )
    .await;
    if !msg_back_failed.is_empty() {
        {
            let mut state = lock(&inner.state);
            if let Some(dq) = state.pending.get_mut(key) {
                for (_, m) in msg_back_failed.iter().rev() {
                    dq.push_front(m.clone());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let floor = msg_back_failed.iter().map(|(_, m)| m.queue_offset).min();
    let handled: Vec<MessageExt> = batch
        .iter()
        .enumerate()
        .filter(|(i, _)| !msg_back_failed.iter().any(|(fi, _)| fi == i))
        .map(|(_, m)| m.clone())
        .collect();
    advance_consume_offset(inner, key, &handled, floor, epoch);
    Ok(msg_back_failed.is_empty())
}

async fn send_back_batch(
    inner: &Arc<Inner>,
    key: &str,
    batch: &[MessageExt],
    base: usize,
    delay_level_from_context: i32,
) -> Vec<(usize, MessageExt)> {
    let mut failed: Vec<(usize, MessageExt)> = Vec::new();
    for (i, msg) in batch.iter().enumerate() {
        {
            let state = lock(&inner.state);
            if !process_queue_contains_locked(&state, key, msg.queue_offset) {
                rmq_info!(
                    "Message is not found in its process queue; skip send-back-procedure, \
                     topic={}, brokerName={}, queueId={}, queueOffset={}",
                    msg.topic,
                    msg.broker_name.as_deref().unwrap_or(""),
                    msg.queue_id,
                    msg.queue_offset
                );
                continue;
            }
        }
        let mut delay_level = delay_level_from_context;
        if delay_level == 0 {
            delay_level = 3 + msg.reconsume_times;
        }
        let mut msg = msg.clone();
        if let Err(e) = send_message_back(inner, &msg, delay_level, None).await {
            rmq_debug!("send message back failed: {e}");
            msg.reconsume_times += 1;
            failed.push((base + i, msg));
        }
    }
    failed
}

/// Python `_advance_consume_offset`：取本批最大 queueOffset + 1，且**不回退**。
/// `floor` 是「不能越过的位点」（回投失败被塞回队首的那几条里最小的 offset）。
fn advance_consume_offset(
    inner: &Inner,
    key: &str,
    batch: &[MessageExt],
    floor: Option<i64>,
    epoch: Option<u64>,
) {
    let mut state = lock(&inner.state);
    if epoch.is_some_and(|e| e != state.queue_epoch.get(key).copied().unwrap_or(0)) {
        rmq_debug!("drop ack for {key}: process queue was dropped");
        return;
    }
    if state.frozen_offsets.contains(key) {
        return;
    }
    if batch.is_empty() {
        // 整批回投都失败时没有任何条目被认可，位点原地不动
        return;
    }
    let mut next_off = batch.iter().map(|m| m.queue_offset).max().unwrap_or(0) + 1;
    if let Some(floor) = floor {
        next_off = next_off.min(floor);
    }
    let cur = state.consume_offsets.get(key).copied().unwrap_or(0);
    state
        .consume_offsets
        .insert(key.to_string(), cur.max(next_off));
}

fn requeue_pending(inner: &Inner, key: &str, batch: &[MessageExt]) {
    let mut state = lock(&inner.state);
    if let Some(dq) = state.pending.get_mut(key) {
        for m in batch.iter().rev() {
            dq.push_front(m.clone());
        }
    }
}


/// 把这一批登记进「在 listener 手里」的视图（须与 [`deregister_inflight_msgs`] 成对）。
fn register_inflight_msgs(inner: &Inner, key: &str, batch: &[MessageExt]) {
    if batch.is_empty() {
        return;
    }
    let mut state = lock(&inner.state);
    state
        .inflight_msgs
        .entry(key.to_string())
        .or_default()
        .extend(batch.iter().cloned());
}

/// 把这批消息从「在 listener 手里」的视图摘除（按位点，幂等）。
fn deregister_inflight_msgs(inner: &Inner, key: &str, batch: &[MessageExt]) {
    if batch.is_empty() {
        return;
    }
    let offsets: std::collections::BTreeSet<i64> = batch.iter().map(|m| m.queue_offset).collect();
    let mut state = lock(&inner.state);
    let Some(bucket) = state.inflight_msgs.get_mut(key) else {
        return;
    };
    bucket.retain(|m| !offsets.contains(&m.queue_offset));
    if bucket.is_empty() {
        state.inflight_msgs.remove(key);
    }
}

/// 配对操作（登记/摘除、塞回缓冲）都在 `state` 锁内成对完成，一条消息在同一瞬间只会
/// 出现在一边；这里按位点再去一次重只为防御未来新调用点写出重叠。
fn process_queue_entries_locked(state: &State, key: &str) -> Vec<MessageExt> {
    let mut seen: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    let mut out: Vec<MessageExt> = Vec::new();
    for m in state
        .inflight_msgs
        .get(key)
        .into_iter()
        .flatten()
        .chain(state.pending.get(key).into_iter().flatten())
    {
        if seen.insert(m.queue_offset) {
            out.push(m.clone());
        }
    }
    out
}

fn process_queue_first_entry_locked(state: &State, key: &str) -> Option<MessageExt> {
    process_queue_entries_locked(state, key)
        .into_iter()
        .min_by_key(|m| m.queue_offset)
}

/// 被 cleanExpiredMsg 清扫掉的不算（它已经在回 broker 的路上）。
fn process_queue_contains_locked(state: &State, key: &str, queue_offset: i64) -> bool {
    state
        .inflight_msgs
        .get(key)
        .into_iter()
        .flatten()
        .chain(state.pending.get(key).into_iter().flatten())
        .any(|m| m.queue_offset == queue_offset)
}

/// 摘除刚清扫过的那条（调用方已确认它就是队首）。须持 `state` 锁。
///
/// 两边都摘：正常情况下它只在一边；并发路径「回投失败塞回缓冲」与「收尾摘除登记」
/// 之间有一瞬两边都在，从两处一起摘掉才能保证下轮清扫不会再看见它。缓冲侧按位点扫，
/// 不是只看队首 —— 回投失败的条目是 `push_front` 进去的，缓冲不一定严格按位点排序。
fn remove_process_queue_entry_locked(state: &mut State, key: &str, queue_offset: i64) {
    if let Some(bucket) = state.inflight_msgs.get_mut(key) {
        bucket.retain(|m| m.queue_offset != queue_offset);
        if bucket.is_empty() {
            state.inflight_msgs.remove(key);
        }
    }
    if let Some(dq) = state.pending.get_mut(key) {
        dq.retain(|m| m.queue_offset != queue_offset);
    }
}

/// 「仍是队首」这一问不能省：listener 恰好在此期间正常收尾（消息已被
/// `deregister_inflight_msgs` 摘掉），或前面冒出了更小的位点（回投失败塞回的条目），
/// 都该让位给正常路径 —— 抢摘会把不该动的那条从视图里抹掉。须持 `state` 锁。
fn remove_expired_entry_if_still_head(state: &mut State, key: &str, queue_offset: i64) -> bool {
    let still_head = process_queue_first_entry_locked(state, key)
        .is_some_and(|m| m.queue_offset == queue_offset);
    if still_head {
        remove_process_queue_entry_locked(state, key, queue_offset);
    }
    still_head
}

fn consume_start_timestamp(msg: &MessageExt) -> Option<i64> {
    let raw = msg.get_property(PROPERTY_CONSUME_START_TIMESTAMP)?;
    if raw.is_empty() {
        return None;
    }
    raw.parse::<i64>().ok()
}

fn is_consume_expired(stamp: Option<i64>, now_ms: i64, consume_timeout_minutes: i64) -> bool {
    match stamp {
        Some(ts) => now_ms - ts > consume_timeout_minutes * 60 * 1000,
        None => false,
    }
}

fn clean_expire_period_millis(consume_timeout_minutes: i64) -> u64 {
    let minutes = consume_timeout_minutes.max(1);
    u64::try_from(minutes).unwrap_or(u64::MAX / 60_000) * 60_000
}

/// 语义三件套：只看队首（最小位点）、单条判定严格大于 consumeTimeout、每轮最多 16 条。
async fn clean_expired_queue(inner: &Arc<Inner>, key: &str) -> usize {
    if lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly)
    {
        return 0;
    }
    let cfg = read_cfg(inner);
    let loop_n = {
        let state = lock(&inner.state);
        process_queue_entries_locked(&state, key).len().min(16)
    };
    let mut attempts = 0usize;
    for _ in 0..loop_n {
        let head = {
            let state = lock(&inner.state);
            match process_queue_first_entry_locked(&state, key) {
                Some(m) => m,
                None => break,
            }
        };
        if !is_consume_expired(
            consume_start_timestamp(&head),
            current_time_millis(),
            cfg.consume_timeout,
        ) {
            break;
        }
        attempts += 1;
        if let Err(e) = send_message_back(inner, &head, 3, None).await {
            rmq_error!("send expired msg exception: {e}");
            continue;
        }
        rmq_info!(
            "send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}",
            head.topic,
            head.msg_id.as_deref().unwrap_or(""),
            head.get_store_host().unwrap_or(""),
            head.queue_id,
            head.queue_offset
        );
        let mut state = lock(&inner.state);
        remove_expired_entry_if_still_head(&mut state, key, head.queue_offset);
    }
    attempts
}

async fn clean_expired_msg_once(inner: &Arc<Inner>) -> usize {
    let keys: Vec<String> = lock(&inner.state).mq_map.keys().cloned().collect();
    let mut attempts = 0;
    for key in keys {
        attempts += clean_expired_queue(inner, &key).await;
    }
    attempts
}

async fn clean_expire_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    // initialDelay 与 period 同值（都是 consumeTimeout 分钟），所以首轮清扫也要等满
    // 一个周期 —— 立刻扫会把刚开始消费、还没超时的消息全看一遍（虽不会误伤，但没必要）。
    let period = clean_expire_period_millis(read_cfg(&inner).consume_timeout);
    if wait_or_stop(&mut rx, period).await {
        return;
    }
    loop {
        if stopped(&rx) || !inner.started.load(Ordering::Acquire) {
            return;
        }
        clean_expired_msg_once(&inner).await;
        if wait_or_stop(&mut rx, period).await {
            return;
        }
    }
}

/// 拉取应答是 `NO_NEW_MSG`（队列里真没消息）或 `NO_MATCHED_MSG`（broker 侧按订阅表达式
/// 过完一轮、一条都没匹配上，见 `MQClientAPIImpl:1095-1097` 把 `PULL_RETRY_IMMEDIATELY`
/// 映射成它）时，这条队列的「已消费位点」必须跟着拉取游标走，否则会**永久卡死**：
/// 没人 ack 的消息不属于任何人（broker 侧被过滤掉的不在应答里，客户端二次 tag 过滤摘掉的
/// 又明确不 ack），位点不动就永远停在原地，重投/重启后再把这批没人要的消息从头扫一遍。
fn correct_tags_offset_locked(state: &mut State, key: &str, status: PullStatus, next_off: i64) {
    if !matches!(status, PullStatus::NoNewMsg | PullStatus::NoMatchedMsg) {
        return;
    }
    if state.frozen_offsets.contains(key) {
        return;
    }
    if !state.pending.get(key).is_none_or(|dq| dq.is_empty()) {
        return;
    }
    if state.inflight.get(key).copied().unwrap_or(0) != 0 {
        return;
    }
    match state.consume_offsets.get(key).copied() {
        None => {
            state.consume_offsets.insert(key.to_string(), next_off);
        }
        Some(cur) if next_off > cur => {
            state.consume_offsets.insert(key.to_string(), next_off);
        }
        Some(_) => {}
    }
}

fn orderly_suspend_millis(cfg: &ConsumerConfig, ctx: &ConsumeOrderlyContext) -> u64 {
    let mut ms = ctx.suspend_current_queue_time_millis;
    if ms == -1 {
        ms = cfg.suspend_current_queue_time_millis;
    }
    ms.clamp(10, 30000) as u64
}

/// Python `send_message_back`（CONSUMER_SEND_MSG_BACK=36）。
async fn send_message_back(
    inner: &Arc<Inner>,
    msg: &MessageExt,
    delay_level: i32,
    broker_name: Option<&str>,
) -> Result<()> {
    let client = require_client_for_send_back(inner)?;
    let cfg = read_cfg(inner);
    let broker = broker_name
        .map(str::to_string)
        .or_else(|| msg.broker_name.clone())
        .unwrap_or_default();
    let addr = client
        .find_broker_address_in_publish(&broker)
        .ok_or_else(|| Error::client(format!("Broker[{broker}] master node does not exist")))?;
    let max_reconsume = if cfg.max_reconsume_times == -1 {
        16
    } else {
        cfg.max_reconsume_times
    };
    client
        .consumer_send_msg_back(
            &cfg.consumer_group,
            msg,
            delay_level,
            Some(max_reconsume),
            3_000,
            &addr,
            cfg.unit_mode,
        )
        .await
}

/// 顺序消费的消息一直停在本地队列里原地重试，broker 侧压根没有重投计数，所以默认就该
/// 重试到成功为止 —— `-1` 在这里读成 `i32::MAX`。**不是**并发回投那条链路的 16
///（[`send_message_back`]，那边每轮都过一遍 broker，16 是 broker 默认的 retryMaxTimes）：
/// 把两套并成一个常量，等于要么给顺序消费凭空造出死信，要么让并发消息无限重投。
fn orderly_max_reconsume_times(cfg: &ConsumerConfig) -> i32 {
    if cfg.max_reconsume_times == -1 {
        i32::MAX
    } else {
        cfg.max_reconsume_times
    }
}

async fn check_orderly_reconsume_times(inner: &Arc<Inner>, batch: &mut [MessageExt]) -> bool {
    let cfg = read_cfg(inner);
    let max_times = orderly_max_reconsume_times(&cfg);
    let mut suspend = false;
    for msg in batch {
        if msg.reconsume_times >= max_times {
            if !orderly_send_message_back(inner, msg, &cfg, max_times).await {
                suspend = true;
                msg.reconsume_times += 1;
            }
        } else {
            suspend = true;
            msg.reconsume_times += 1;
        }
    }
    suspend
}

/// 失败只返回 false、绝不抛：见 [`check_orderly_reconsume_times`]。
async fn orderly_send_message_back(
    inner: &Arc<Inner>,
    msg: &MessageExt,
    cfg: &ConsumerConfig,
    max_times: i32,
) -> bool {
    let outcome: Result<()> = async {
        let client = require_client_for_send_back(inner)?;
        let mut new_msg = build_retry_message(cfg, msg, max_times);
        let publish = client.get_topic_publish_info(&new_msg.topic, true).await?;
        let mq = publish
            .select_one_message_queue(&[])?
            .ok_or_else(|| Error::client(format!("no writable queue for {}", new_msg.topic)))?;
        client
            .send_message(
                MixAll::CLIENT_INNER_PRODUCER_GROUP,
                &mut PublishMessage::Single(&mut new_msg),
                &mq,
                3000,
                0,
                cfg.unit_mode,
                MixAll::DEFAULT_TOPIC,
                MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
            )
            .await?;
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => true,
        Err(e) => {
            rmq_debug!(
                "orderly send message back failed, group={}: {e}",
                read_cfg(inner).consumer_group
            );
            false
        }
    }
}

fn build_retry_message(cfg: &ConsumerConfig, msg: &MessageExt, max_times: i32) -> Message {
    let mut new_msg = Message::new(
        MixAll::get_retry_topic(&cfg.consumer_group).as_str(),
        msg.body.as_deref(),
    );
    new_msg.properties = msg.properties.clone();
    new_msg.flag = msg.flag;
    let origin_msg_id = msg
        .get_property(PROPERTY_ORIGIN_MESSAGE_ID)
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .or_else(|| msg.msg_id.clone())
        .unwrap_or_default();
    if !origin_msg_id.is_empty() {
        new_msg.put_property(PROPERTY_ORIGIN_MESSAGE_ID, &origin_msg_id);
    }
    new_msg.put_property(PROPERTY_RETRY_TOPIC, &msg.topic);
    new_msg.put_property(
        PROPERTY_RECONSUME_TIME,
        &(msg.reconsume_times + 1).to_string(),
    );
    new_msg.put_property(PROPERTY_MAX_RECONSUME_TIMES, &max_times.to_string());
    // 半消息标记必须清掉，否则 broker 会把它再当事务回查消息处理
    new_msg.properties.remove(PROPERTY_TRANSACTION_PREPARED);
    new_msg.set_delay_time_level(3 + msg.reconsume_times);
    new_msg
}

// ================================================================ broker 主动请求
//
// 220/221/307/309 的处理入口注册在 MQClientInstance 上（它按 consumerGroup 找
// **对应的**消费者），这里只实现被回调的那一侧。40 是唯一由消费者自己注册的，
// 因为它的动作就是「叫醒本消费者的 rebalance 循环」。

/// Python `set_host_consumer(self)`（`trace_dispatcher.py:117-118`）的宿主端：
/// 分发器 `_client_id` 读的就是宿主 `_mq_client.client_id`，未启动时是空串。
impl TraceHost for DefaultMQPushConsumer {
    fn client_id(&self) -> String {
        DefaultMQPushConsumer::client_id(self)
    }
}

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
        self.config().unit_mode
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

    fn rebalance_immediately(&self) {
        self.inner.rebalance_now.store(true, Ordering::SeqCst);
        self.inner.rebalance_signal.notify_waiters();
    }

    fn adjust_thread_pool(&self) {
        let acc_total = self.compute_accumulation_total();
        let threshold = self.config().adjust_thread_pool_nums_threshold;
        if acc_total >= threshold {
            rmq_debug!("adjustThreadPool: acc={acc_total} >= incThreshold={threshold} (inc is a no-op upstream)");
        }
        if acc_total < threshold * 4 / 5 {
            rmq_debug!(
                "adjustThreadPool: acc={acc_total} < decThreshold={}/0.8 (dec is a no-op upstream)",
                threshold
            );
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
            let pop = self.config().pop_mode;
            let mut revoked: Vec<(MessageQueue, Option<i64>)> = Vec::new();
            {
                // 先收 key 再逐个撤：`retire_queue_locked` 会擦 `mq_map` 的条目
                let mut state = lock(&self.inner.state);
                let hits: Vec<(String, MessageQueue, i64)> = state
                    .mq_map
                    .iter()
                    .filter(|(_, mq)| mq.topic == topic)
                    .filter_map(|(key, mq)| {
                        offset_table
                            .iter()
                            .find(|(m, _)| m == mq)
                            .map(|(_, off)| (key.clone(), mq.clone(), *off))
                    })
                    .collect();
                for (key, mq, off) in hits {
                    state.consume_offsets.insert(key.clone(), off);
                    DefaultMQPushConsumer::retire_queue_locked(
                        &mut state,
                        &key,
                        Some(&mq),
                        &mut revoked,
                        pop,
                    );
                }
            }
            if revoked.is_empty() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            self.on_queues_revoked(&revoked).await;
            if let Err(e) = self.do_rebalance().await {
                rmq_debug!("rebalance after reset offset failed: {e}");
            }
            rmq_info!(
                "reset offset applied, group={} topic={topic} queues={}",
                self.consumer_group(),
                revoked.len()
            );
            Ok(())
        })
    }

    fn get_consumer_status(&self, topic: Option<&str>) -> Vec<(MessageQueue, i64)> {
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
            self.inner
                .start_time_millis
                .load(Ordering::SeqCst)
                .to_string(),
        );
        properties.insert(ConsumerRunningInfo::PROP_CLIENT_VERSION, "V5_5_1");
        let mut info = ConsumerRunningInfo {
            properties,
            ..Default::default()
        };
        let state = lock(&self.inner.state);
        for (key, mq) in &state.mq_map {
            if cfg.pop_mode && state.pop_queues.contains_key(key) {
                continue;
            }
            let pqi = ProcessQueueInfo {
                commit_offset: state.consume_offsets.get(key).copied().unwrap_or(0),
                cached_msg_count: i32::try_from(state.pending.get(key).map_or(0, |dq| dq.len()))
                    .unwrap_or(i32::MAX),
                droped: false,
                last_pull_timestamp: state.last_pull_at.get(key).copied().unwrap_or(0),
                ..Default::default()
            };
            info.mq_table.push((
                MessageQueueKey::new(&mq.topic, &mq.broker_name, mq.queue_id),
                pqi.to_json_value(),
            ));
        }
        if cfg.pop_mode {
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
                    last_pull_timestamp: pq.last_pop_timestamp.load(Ordering::SeqCst),
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

    fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<String>,
    ) -> Result<ConsumeMessageDirectlyResult> {
        let mut result = ConsumeMessageDirectlyResult::default();
        let mq = MessageQueue::new(
            &msg.topic,
            broker_name.as_deref().unwrap_or(""),
            msg.queue_id,
        );
        let mut msgs = vec![msg];
        reset_retry_topic_and_namespace(&self.inner, &mut msgs);
        let mut context = ConsumeConcurrentlyContext::new(Some(mq.clone()));
        let begin = current_time_millis();
        // ⚠ 这里是**同步**接口（`RegisteredConsumer::consume_message_directly` 不是
        // async），不能像正常消费路径那样 `spawn_blocking`，只能在 remoting
        // 读线程上直接调 listener。代价是这一条路径上用户代码会占住读线程。
        let status = match lock(&self.inner.listener).as_ref().cloned() {
            Some(MessageListener::Concurrently(listener)) => {
                result.order = false;
                match listener.consume_message(&msgs, &mut context) {
                    ConsumeConcurrentlyStatus::ConsumeSuccess => CMResult::CR_SUCCESS,
                    ConsumeConcurrentlyStatus::ReconsumeLater => CMResult::CR_LATER,
                }
            }
            Some(MessageListener::Orderly(listener)) => {
                let mut octx = ConsumeOrderlyContext::new(Some(mq));
                let status = listener.consume_message(&msgs, &mut octx);
                result.order = true;
                result.auto_commit = octx.auto_commit;
                match status {
                    ConsumeOrderlyStatus::Success => CMResult::CR_SUCCESS,
                    ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => CMResult::CR_LATER,
                    ConsumeOrderlyStatus::Commit => CMResult::CR_COMMIT,
                    ConsumeOrderlyStatus::Rollback => CMResult::CR_ROLLBACK,
                }
            }
            // Python 的 `else None`：没有监听器 → 状态为 None → CR_RETURN_NULL
            None => CMResult::CR_RETURN_NULL,
        };
        result.consume_result = Some(status.to_string());
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
        Ok(client.get_topic_subscribe_info(topic).await)
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


    pub fn suspend(&self) {
        self.inner.pause.store(true, Ordering::SeqCst);
        rmq_info!("suspend this consumer, {}", self.consumer_group());
    }

    pub fn resume(&self) {
        self.inner.pause.store(false, Ordering::SeqCst);
        self.rebalance_immediately();
        rmq_info!("resume this consumer, {}", self.consumer_group());
    }

    pub fn is_pause(&self) -> bool {
        self.inner.pause.load(Ordering::SeqCst)
    }


    pub async fn send_message_back(&self, msg: &MessageExt, delay_level: i32) -> Result<()> {
        send_message_back(&self.inner, msg, delay_level, None).await
    }

    pub async fn send_message_back_to_broker(
        &self,
        msg: &MessageExt,
        delay_level: i32,
        broker_name: &str,
    ) -> Result<()> {
        send_message_back(&self.inner, msg, delay_level, Some(broker_name)).await
    }

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
        self.inner
            .core_pool_size
            .store(core_pool_size, Ordering::SeqCst);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::Message;
    use crate::common::message_const::PROPERTY_KEYS;
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
    fn namespace_v2_setter_getter_round_trip() {
        let c = DefaultMQPushConsumer::new("GID_nsv2").expect("合法组名不该构造失败");
        assert_eq!(c.get_namespace_v2(), None, "默认不设");
        c.set_namespace_v2(Some("NS_V2"));
        assert_eq!(c.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(c.config().namespace_v2, Some("NS_V2".to_string()));
        c.set_namespace_v2(None);
        assert_eq!(
            c.get_namespace_v2(),
            None,
            "None = 清除"
        );
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
        let msgs = vec![
            ext("T", Some("TagA")),
            ext("T", Some("TagB")),
            ext("T", None),
        ];
        let kept = client_side_tag_filter(Some(&sub("T", "TagA")), msgs.clone());
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].get_tags(), Some("TagA"));

        // 订阅 "*"：tags_set 为空 ⇒ 不过滤（连无 tag 的消息也留着）。
        assert_eq!(
            client_side_tag_filter(Some(&sub("T", "*")), msgs.clone()).len(),
            3
        );

        let mut class_sub = sub("T", "TagA");
        class_sub.class_filter_mode = true;
        assert_eq!(
            client_side_tag_filter(Some(&class_sub), msgs.clone()).len(),
            3
        );

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
            false,
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

    /// 离线只能证明「不再拒绝 + 都落进活订阅表 + unsubscribe 不发心跳」：立即心跳是异步 RPC
    /// （`notify_subscription_changed` fire-and-forget 到 runtime），零 broker 时不发包。
    /// 真机报文与 broker 侧 topicGroupTable 的对照见 `examples/live_subscribe.rs`。
    #[tokio::test]
    async fn subscribe_is_allowed_after_start_and_lands_in_the_live_table() {
        let consumer = DefaultMQPushConsumer::new("GID_SubAfterStartUnit").unwrap();
        // 造一个「已启动 + 有 client」的状态；client 指向零路由实例，不发任何包。
        *lock(&consumer.inner.client) = Some(MQClientInstance::new("unit-sub-after-start", vec![]));
        consumer.inner.started.store(true, Ordering::Release);

        consumer.subscribe("T_A", "*").unwrap();
        consumer.subscribe_all("T_B").unwrap();
        consumer
            .subscribe_with_selector("T_C", &MessageSelector::by_sql("a > 1"))
            .unwrap();

        let topics: Vec<String> = consumer
            .subscriptions()
            .into_iter()
            .map(|s| s.topic)
            .collect();
        assert!(
            topics.contains(&"T_A".to_string()),
            "后置 subscribe 要落进活订阅表"
        );
        assert!(topics.contains(&"T_B".to_string()));
        assert!(topics.contains(&"T_C".to_string()));

        let before = consumer.heartbeat_count();
        consumer.unsubscribe("T_B");
        assert_eq!(consumer.heartbeat_count(), before);
        assert!(!consumer.subscriptions().iter().any(|s| s.topic == "T_B"));
    }

    #[test]
    fn pop_process_queue_tracks_wait_ack() {
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(3);
        assert_eq!(pq.wait_ack_count(), 3);
        assert_eq!(pq.ack(), 2);
        assert_eq!(pq.ack(), 1);
        // 口径：dec_found_msg(正数) = 减少那么多条。
        pq.dec_found_msg(1);
        assert_eq!(pq.wait_ack_count(), 0);

        assert!(!pq.is_dropped());
        pq.set_dropped(true);
        assert!(pq.is_dropped());

        pq.touch(1234);
        assert_eq!(pq.last_pop_timestamp.load(Ordering::SeqCst), 1234);
    }

    #[test]
    fn suspend_and_resume_toggle_pause_and_trigger_rebalance() {
        let c = DefaultMQPushConsumer::new("G_suspend").unwrap();
        assert!(!c.is_pause());
        c.suspend();
        assert!(c.is_pause());
        c.suspend();
        assert!(c.is_pause());
        c.resume();
        assert!(!c.is_pause());
        assert!(c.inner.rebalance_now.load(Ordering::SeqCst));
    }

    #[test]
    fn flow_control_constants_match_contract() {
        assert_eq!(PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION, 3000);
        assert_eq!(PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL, 50);
        assert_eq!(PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL, 20);
        assert_eq!(PULL_TIME_DELAY_MILLS_WHEN_SUSPEND, 1000);
    }

    /// 公开回投的参数接线：未启动时报错、broker 名来自消息 / 显式实参。
    #[tokio::test]
    async fn public_send_message_back_requires_started_client() {
        let c = DefaultMQPushConsumer::new("G_send_back_pub").unwrap();
        let msg = MessageExt::from_message(&Message::new("T", Some(b"x")));
        // 未 start：require_client_for_send_back 拿不到实例
        let err = c.send_message_back(&msg, 3).await.unwrap_err();
        assert!(err.to_string().contains("not started"), "{err}");
        let err = c
            .send_message_back_to_broker(&msg, 3, "broker-a")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not started"), "{err}");
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

    /// 默认值逐项对照 `python/client/consumer.py`（`DefaultMQPushConsumer.__init__`）。
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
        assert_eq!((cfg.consume_thread_min, cfg.consume_thread_max), (20, 20));
        assert_eq!(cfg.adjust_thread_pool_nums_threshold, 100_000);
        assert_eq!(cfg.consume_concurrently_max_span, 2000);
        assert_eq!(
            (
                cfg.pull_threshold_for_queue,
                cfg.pull_threshold_size_for_queue
            ),
            (1000, 100)
        );
        assert_eq!(
            (
                cfg.pull_threshold_for_topic,
                cfg.pull_threshold_size_for_topic
            ),
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

    #[test]
    fn update_core_pool_size_guards_match_contract() {
        let c = DefaultMQPushConsumer::new("G").unwrap();
        assert_eq!(
            c.config().consume_thread_max,
            20,
            "默认两侧同为 20"
        );
        // 无界队列 ⇒ 真实并发度 == core；默认 max=20，故只能往**下**调
        assert!(c.update_core_pool_size(15));
        assert_eq!(c.get_core_pool_size(), 15);
        assert!(!c.update_core_pool_size(0));
        assert!(!c.update_core_pool_size(-1));
        assert!(!c.update_core_pool_size(20), "== consume_thread_max 也不行");
        assert!(
            !c.update_core_pool_size(30),
            "回归：默认 max 不能是 4.x 的 64"
        );
        assert!(c.update_core_pool_size(19), "刚好低于 max");
        assert_eq!(c.get_core_pool_size(), 19);
        c.update_config(|x| x.consume_thread_max = 40_000);
        assert!(!c.update_core_pool_size(32768), "above Short.MAX_VALUE");
        assert!(c.update_core_pool_size(32767), "上界本身合法");
        assert_eq!(c.get_core_pool_size(), 32767);
    }

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

        consumer
            .set_allocate_message_queue_strategy(Arc::new(AllocateMessageQueueAveragelyByCircle));
        assert_eq!(
            consumer.allocate_message_queue_strategy().get_name(),
            "AVG_BY_CIRCLE",
            "策略可替换，getter 读回同一个"
        );

        let by_config = Arc::new(AllocateMessageQueueByConfig::new(vec![MessageQueue::new(
            "T", "broker-a", 1,
        )]));
        consumer.set_allocate_message_queue_strategy(by_config.clone());
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
            assert_eq!(
                listener.as_ref().map(MessageListener::is_orderly),
                Some(false)
            );
        }

        // 换成分区顺序 listener：覆盖而不是追加
        consumer.set_message_listener_orderly(Arc::new(NoopOrderlyListener));
        let listener = lock(&consumer.inner.listener).clone();
        assert_eq!(
            listener.as_ref().map(MessageListener::is_orderly),
            Some(true)
        );
    }

    /// `start()` 的三道校验必须在任何网络动作之前（`start()` 开头即先校验）。
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


    struct RangeGate {
        field: &'static str,
        lo: i64,
        hi: i64,
        message: &'static str,
        set: fn(&mut ConsumerConfig, i64),
        minus_one_off: bool,
    }

    const RANGE_GATES: &[RangeGate] = &[
        RangeGate {
            field: "consume_thread_min",
            lo: 1,
            hi: 1000,
            message: "consumeThreadMin Out of range [1, 1000]",
            set: |c, v| c.consume_thread_min = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "consume_thread_max",
            lo: 1,
            hi: 1000,
            message: "consumeThreadMax Out of range [1, 1000]",
            set: |c, v| c.consume_thread_max = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "consume_concurrently_max_span",
            lo: 1,
            hi: 65535,
            message: "consumeConcurrentlyMaxSpan Out of range [1, 65535]",
            set: |c, v| c.consume_concurrently_max_span = v,
            minus_one_off: false,
        },
        RangeGate {
            field: "pull_threshold_for_queue",
            lo: 1,
            hi: 65535,
            message: "pullThresholdForQueue Out of range [1, 65535]",
            set: |c, v| c.pull_threshold_for_queue = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "pull_threshold_for_topic",
            lo: 1,
            hi: 6553500,
            message: "pullThresholdForTopic Out of range [1, 6553500]",
            set: |c, v| c.pull_threshold_for_topic = v as i32,
            minus_one_off: true,
        },
        RangeGate {
            field: "pull_threshold_size_for_queue",
            lo: 1,
            hi: 1024,
            message: "pullThresholdSizeForQueue Out of range [1, 1024]",
            set: |c, v| c.pull_threshold_size_for_queue = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "pull_threshold_size_for_topic",
            lo: 1,
            hi: 102400,
            message: "pullThresholdSizeForTopic Out of range [1, 102400]",
            set: |c, v| c.pull_threshold_size_for_topic = v as i32,
            minus_one_off: true,
        },
        RangeGate {
            field: "pull_interval",
            lo: 0,
            hi: 65535,
            message: "pullInterval Out of range [0, 65535]",
            set: |c, v| c.pull_interval = v,
            minus_one_off: false,
        },
        RangeGate {
            field: "consume_message_batch_max_size",
            lo: 1,
            hi: 1024,
            message: "consumeMessageBatchMaxSize Out of range [1, 1024]",
            set: |c, v| c.consume_message_batch_max_size = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "pull_batch_size",
            lo: 1,
            hi: 1024,
            message: "pullBatchSize Out of range [1, 1024]",
            set: |c, v| c.pull_batch_size = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "pop_invisible_time",
            lo: MIN_POP_INVISIBLE_TIME,
            hi: MAX_POP_INVISIBLE_TIME,
            message: "popInvisibleTime Out of range [5000, 300000]",
            set: |c, v| c.pop_invisible_time = v,
            minus_one_off: false,
        },
        RangeGate {
            field: "pop_batch_nums",
            lo: 1,
            hi: 32,
            message: "popBatchNums Out of range [1, 32]",
            set: |c, v| c.pop_batch_nums = v as i32,
            minus_one_off: false,
        },
    ];

    /// 只改一道闸门，其余保持默认；线程数先摊开成整条合法域，否则单独抬
    /// `consume_thread_min` 会先撞上「min > max」那条相对检查，拿到的是另一句文案，
    /// 范围分支就没测到。
    fn gate_cfg(gate: &RangeGate, value: i64) -> ConsumerConfig {
        let mut cfg = ConsumerConfig {
            consume_thread_min: 1,
            consume_thread_max: 1000,
            ..Default::default()
        };
        (gate.set)(&mut cfg, value);
        cfg
    }

    fn gate_error(cfg: &ConsumerConfig) -> String {
        match check_config_ranges(cfg) {
            Ok(()) => "<accepted>".to_string(),
            Err(e) => e.to_string(),
        }
    }

    fn expect_rejected(gate: &RangeGate, value: i64) {
        let want = format!("MQClientException: {}", gate.message);
        assert_eq!(
            gate_error(&gate_cfg(gate, value)),
            want,
            "{}={}",
            gate.field,
            value
        );
    }

    fn expect_accepted(gate: &RangeGate, value: i64) {
        assert_eq!(
            gate_error(&gate_cfg(gate, value)),
            "<accepted>",
            "{}={} 必须合法",
            gate.field,
            value
        );
    }

    #[test]
    fn defaults_pass_check_config_ranges() {
        check_config_ranges(&ConsumerConfig::default()).expect("默认配置必须通过 checkConfig");
    }

    #[test]
    fn each_range_gate_accepts_both_bounds() {
        for gate in RANGE_GATES {
            expect_accepted(gate, gate.lo);
            expect_accepted(gate, gate.hi);
        }
    }

    #[test]
    fn each_range_gate_rejects_both_ends() {
        for gate in RANGE_GATES {
            expect_rejected(gate, gate.lo - 1);
            expect_rejected(gate, gate.hi + 1);
        }
    }

    #[test]
    fn zero_is_illegal_except_pull_interval() {
        for gate in RANGE_GATES {
            if gate.lo == 0 {
                expect_accepted(gate, 0);
            } else {
                expect_rejected(gate, 0);
            }
        }
    }

    #[test]
    fn minus_one_sentinel_only_closes_topic_gates() {
        for gate in RANGE_GATES {
            if gate.minus_one_off {
                expect_accepted(gate, -1);
            } else {
                expect_rejected(gate, -1);
            }
        }
    }

    #[test]
    fn thread_min_must_not_exceed_thread_max() {
        assert_eq!(
            gate_error(&ConsumerConfig {
                consume_thread_min: 8,
                consume_thread_max: 4,
                ..Default::default()
            }),
            "MQClientException: consumeThreadMin (8) is larger than consumeThreadMax (4)"
        );

        check_config_ranges(&ConsumerConfig {
            consume_thread_min: 8,
            consume_thread_max: 8,
            ..Default::default()
        })
        .expect("min == max 必须合法");

        // 相对检查排在两条范围检查**之后**：min=0 时先报范围，不报 min>max
        assert_eq!(
            gate_error(&ConsumerConfig {
                consume_thread_min: 0,
                consume_thread_max: 0,
                ..Default::default()
            }),
            "MQClientException: consumeThreadMin Out of range [1, 1000]"
        );
    }

    #[test]
    fn gates_are_checked_in_contract_order() {
        assert_eq!(
            gate_error(&ConsumerConfig {
                consume_concurrently_max_span: 0,
                pull_batch_size: 0,
                ..Default::default()
            }),
            "MQClientException: consumeConcurrentlyMaxSpan Out of range [1, 65535]",
            "跨度排在条数之前，先报跨度"
        );
        assert_eq!(
            gate_error(&ConsumerConfig {
                pull_threshold_for_queue: 0,
                pull_batch_size: 0,
                ..Default::default()
            }),
            "MQClientException: pullThresholdForQueue Out of range [1, 65535]",
            "条数排在 pullBatchSize 之前"
        );
        assert_eq!(
            gate_error(&ConsumerConfig {
                pull_batch_size: 0,
                ..Default::default()
            }),
            "MQClientException: pullBatchSize Out of range [1, 1024]",
            "前面几道修好之后才轮到 pullBatchSize"
        );
    }

    /// 坏配置必须在**建实例之前**被拒：起了后台任务再抛错就泄漏任务，而且
    /// `started` 会留在 true 上，`shutdown()` 之后再 start() 会静默返回。
    #[tokio::test]
    async fn start_rejects_out_of_range_config_before_any_network_io() {
        let consumer = DefaultMQPushConsumer::new("CID_check_config_ranges_rust").unwrap();
        consumer.set_namesrv_addr("127.0.0.1:1");
        consumer.subscribe("T", "TagA").unwrap();
        consumer.set_message_listener_concurrently(Arc::new(NoopListener));
        consumer.update_config(|c| c.pull_batch_size = 1025);

        let e = consumer
            .start()
            .await
            .expect_err("pullBatchSize=1025 必须在启动时被拒");
        assert_eq!(
            e.to_string(),
            "MQClientException: pullBatchSize Out of range [1, 1024]"
        );
        assert!(
            !consumer.is_started(),
            "被拒的 start() 必须回滚 started 标志"
        );
        assert!(
            lock(&consumer.inner.client).is_none(),
            "数值闸门必须排在建 MQClientInstance 之前，否则后台任务泄漏"
        );

        // 改对之后同一个对象还能起来（校验不留脏状态）
        consumer.update_config(|c| c.pull_batch_size = 1024);
        consumer.start().await.expect("修好数值后必须能启动");
        consumer.shutdown();
    }

    #[tokio::test]
    async fn start_accepts_every_gate_boundary() {
        for (label, at_max) in [("min", false), ("max", true)] {
            let consumer =
                DefaultMQPushConsumer::new(&format!("CID_check_config_boundary_rust_{label}"))
                    .unwrap();
            consumer.set_namesrv_addr("127.0.0.1:1");
            consumer.subscribe("T", "TagA").unwrap();
            consumer.set_message_listener_concurrently(Arc::new(NoopListener));
            consumer.update_config(|c| {
                for gate in RANGE_GATES {
                    let value = if at_max { gate.hi } else { gate.lo };
                    (gate.set)(c, value);
                }
            });
            if let Err(e) = consumer.start().await {
                consumer.shutdown();
                panic!("边界配置 {label} 被误拒：{e}");
            }
            consumer.shutdown();
        }
    }

    #[tokio::test]
    async fn scheduled_intervals_reach_the_instance() {
        let consumer = DefaultMQPushConsumer::new("CID_interval_probe_rust").unwrap();
        assert_eq!(
            consumer.config().poll_name_server_interval_millis,
            30_000,
            "ClientConfig:58 = 1000 * 30"
        );
        assert_eq!(
            consumer.config().persist_consumer_offset_interval_millis,
            5_000,
            "ClientConfig:66 = 1000 * 5"
        );
        consumer.set_namesrv_addr("127.0.0.1:1");
        consumer.subscribe("T", "TagA").unwrap();
        consumer.set_message_listener_concurrently(Arc::new(NoopListener));
        consumer.set_poll_name_server_interval_millis(2_000);
        consumer.set_persist_consumer_offset_interval_millis(700);

        consumer.start().await.expect("静态地址下 start 不该失败");
        let client = lock(&consumer.inner.client)
            .clone()
            .expect("start 之后必须已有实例");
        assert_eq!(client.config().route_refresh_interval_millis, 2_000);
        assert_eq!(client.config().persist_offset_interval_millis, 700);
        consumer.shutdown();
    }

    #[tokio::test]
    async fn start_rejects_unparsable_consume_timestamp() {
        let consumer = DefaultMQPushConsumer::new("CID_check_config_ts_rust").unwrap();
        consumer.set_namesrv_addr("127.0.0.1:1");
        consumer.subscribe("T", "TagA").unwrap();
        consumer.set_message_listener_concurrently(Arc::new(NoopListener));
        consumer.update_config(|c| {
            c.consume_from_where = ConsumeFromWhere::CONSUME_FROM_TIMESTAMP.to_string();
            c.consume_timestamp = "2026-01-01 00:00:00".to_string();
        });
        let e = consumer
            .start()
            .await
            .expect_err("非 yyyyMMddHHmmss 必须被拒");
        assert!(
            e.to_string().contains("consumeTimestamp is invalid"),
            "unexpected error: {e}"
        );
        assert!(!consumer.is_started());
    }

    #[tokio::test]
    async fn client_id_follows_buildmqclientid_contract() {
        let clustering = DefaultMQPushConsumer::new("CID-clientid-parity-clustering").unwrap();
        clustering.set_namesrv_addr("127.0.0.1:1");
        clustering.subscribe("T", "TagA").unwrap();
        clustering.set_message_listener_concurrently(Arc::new(NoopListener));
        clustering
            .start()
            .await
            .expect("CLUSTERING 消费者应该能启动");

        let id = clustering.client_id();
        let (ip, instance) = id
            .split_once('@')
            .unwrap_or_else(|| panic!("clientId 少了 IP@instanceName 的分隔符: {id}"));
        assert_eq!(ip, MixAll::cached_ip_str(), "{id}");
        assert!(
            instance.starts_with(&format!("{}#", MixAll::cached_pid())),
            "{id}"
        );
        assert_eq!(
            clustering.config().instance_name,
            instance,
            "instanceName 要就地写回配置，否则重启会换一个 clientId"
        );
        clustering.shutdown();
        clustering.start().await.expect("重启");
        assert_eq!(clustering.client_id(), id, "重启换了 clientId");
        let clustering_id = clustering.client_id();
        clustering.shutdown();

        let broadcast = |group: &str| {
            let cfg = ConsumerConfig {
                consumer_group: group.to_string(),
                name_server_addrs: vec!["127.0.0.1:1".to_string()],
                message_model: MessageModel::BROADCASTING.to_string(),
                ..Default::default()
            };
            let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
            c.subscribe("T", "TagA").unwrap();
            c.set_message_listener_concurrently(Arc::new(NoopListener));
            c
        };
        let first = broadcast("CID-clientid-parity-broadcast-a");
        first.start().await.expect("广播消费者应该能启动");
        assert_eq!(
            first.client_id(),
            format!("{}@DEFAULT", MixAll::cached_ip_str()),
            "广播模式不许改写 instanceName"
        );
        assert_eq!(first.config().instance_name, "DEFAULT");
        assert_ne!(first.client_id(), clustering_id);

        let second = broadcast("CID-clientid-parity-broadcast-b");
        second.start().await.expect("第二个广播消费者应该能启动");
        assert_eq!(second.client_id(), first.client_id());
        let shared = MQClientInstance::find_instance(&first.client_id()).expect("实例必须已登记");
        assert!(
            shared
                .find_consumer(&first.config().consumer_group)
                .is_some()
                && shared
                    .find_consumer(&second.config().consumer_group)
                    .is_some(),
            "两个广播消费者没落在同一份实例上"
        );
        second.shutdown();
        first.shutdown();
        // 推送消费者的 `client.shutdown()` 排在清退任务最后（刷位点 → 解锁 → 注销 →
        // 关实例，见 `shutdown`），所以要轮询等它跑完，不能同步断言。
        let mut torn_down = false;
        for _ in 0..100 {
            if MQClientInstance::find_instance(&first.client_id()).is_none() {
                torn_down = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(torn_down, "最后一个租户退场后实例没被拆掉");
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
            assert!(
                !consumer.is_started(),
                "{group}: 失败的 start 必须回滚 started"
            );
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
        assert!(lock(&consumer.inner.state)
            .subscription("Ns%Other")
            .is_some());

        consumer.unsubscribe("Other");
        assert!(lock(&consumer.inner.state)
            .subscription("Ns%Other")
            .is_none());
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

    #[test]
    fn flow_control_hits_each_threshold() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let triggered = || consumer.inner.flow_control_triggered.load(Ordering::SeqCst);

        // 缓冲为空：任何阈值都不触发
        assert!(!flow_control_hit(&consumer.inner, &mq, &key));
        assert_eq!(triggered(), 0);

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

        only(&consumer, |c| c.consume_concurrently_max_span = 10);
        stage(&consumer, &mq, vec![sized_msg(1, 0), sized_msg(1, 100)]);
        assert!(
            flow_control_hit(&consumer.inner, &mq, &key),
            "span 100 > 10"
        );
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

        // 主题级大小：闸门只看 pullThresholdSizeForTopic（Python 同一判据），
        // 队列级那道关掉（0）时主题级那道必须照常生效 —— 复用队列级开关会让它静默失效
        only(&consumer, |c| {
            c.pull_threshold_size_for_queue = 100;
            c.pull_threshold_size_for_topic = 1;
        });
        stage(&consumer, &mq, vec![sized_msg(1024 * 1024 / 2, 0)]);
        assert!(
            !flow_control_hit(&consumer.inner, &mq, &key),
            "0.5MiB < 1MiB"
        );
        stage(&consumer, &mq, vec![sized_msg(2 * 1024 * 1024, 0)]);
        assert!(flow_control_hit(&consumer.inner, &mq, &key), "2MiB >= 1MiB");
        only(&consumer, |c| {
            c.pull_threshold_size_for_queue = 0;
            c.pull_threshold_size_for_topic = 1;
        });
        assert!(
            flow_control_hit(&consumer.inner, &mq, &key),
            "队列级 size 闸门关掉时主题级 size 仍要生效"
        );

        // 默认配置：主题级阈值关闭，只看队列级
        let plain = DefaultMQPushConsumer::new("G").unwrap();
        stage(&plain, &mq, vec![sized_msg(1, 0)]);
        assert!(!flow_control_hit(&plain.inner, &mq, &key));
    }

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

    // ---------------- 并发消费（classic 路径）的 ackIndex ----------------

    /// 这些断言**只能**离线做：这个端口在没有 `start()` 过的实例上回投必然失败
    /// （`require_client` 直接 Err），于是「回投失败」那条分支的可见结果 ——
    /// 尾巴塞回队首、`reconsumeTimes+1`、位点不越过它 —— 恰好是最容易被写错的部分。
    /// 回投**成功**时的语义（尾巴交给 broker、位点整批前进）由真机
    /// `examples/live_ack_index.rs` 取证。
    struct AckListener {
        ack_index: Option<i32>,
        status: ConsumeConcurrentlyStatus,
    }

    impl MessageListenerConcurrently for AckListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            if let Some(i) = self.ack_index {
                context.ack_index = i;
            }
            self.status
        }
    }

    /// 同一个队列上 queueOffset = 0..n 的一批。
    fn offset_batch(n: i64) -> Vec<MessageExt> {
        (0..n)
            .map(|i| {
                let mut m = ext("T", None);
                m.broker_name = Some("broker-a".to_string());
                m.queue_offset = i;
                m
            })
            .collect()
    }

    struct AckHarness {
        c: DefaultMQPushConsumer,
        key: String,
        mq: MessageQueue,
    }

    impl AckHarness {
        fn new(
            broadcast: bool,
            ack_index: Option<i32>,
            status: ConsumeConcurrentlyStatus,
        ) -> AckHarness {
            let cfg = ConsumerConfig {
                consumer_group: "G".to_string(),
                message_model: if broadcast {
                    MessageModel::BROADCASTING.to_string()
                } else {
                    MessageModel::CLUSTERING.to_string()
                },
                ..Default::default()
            };
            let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
            c.set_message_listener_concurrently(Arc::new(AckListener { ack_index, status }));
            let mq = queue("T", "broker-a", 0);
            let key = mq_key(&mq);
            lock(&c.inner.state)
                .pending
                .insert(key.clone(), VecDeque::new());
            AckHarness { c, key, mq }
        }

        async fn run(&self, n: i64) -> bool {
            consume_batch(&self.c.inner, &self.key, &self.mq, offset_batch(n), None)
                .await
                .expect("consume_batch must not error")
        }

        fn offset(&self) -> i64 {
            lock(&self.c.inner.state)
                .consume_offsets
                .get(&self.key)
                .copied()
                .unwrap_or(0)
        }

        fn pending(&self) -> Vec<(i64, i32)> {
            lock(&self.c.inner.state)
                .pending
                .get(&self.key)
                .map(|dq| {
                    dq.iter()
                        .map(|m| (m.queue_offset, m.reconsume_times))
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    #[tokio::test]
    async fn success_with_default_ack_index_never_send_backs() {
        // 默认 Integer.MAX_VALUE 钳到 size-1：整批认可，一条都不回投
        let h = AckHarness::new(false, None, ConsumeConcurrentlyStatus::ConsumeSuccess);
        assert!(h.run(3).await);
        assert_eq!(h.offset(), 3);
        assert_eq!(h.pending(), vec![]);
    }

    #[tokio::test]
    async fn success_acks_prefix_and_holds_the_offset_at_the_first_unacked() {
        let h = AckHarness::new(false, Some(0), ConsumeConcurrentlyStatus::ConsumeSuccess);
        // 本端口未 start()，尾巴的回投必然失败 → 走「塞回队首 + 位点不越过」这条分支
        assert!(!h.run(3).await);
        assert_eq!(
            h.pending(),
            vec![(1, 1), (2, 1)],
            "未认可的尾巴要先把 reconsumeTimes 补上再重投"
        );
        assert_eq!(h.offset(), 1, "位点只能停在第一条未被认可的消息上");
    }

    #[tokio::test]
    async fn reconsume_later_overrides_a_wider_ack_index() {
        let h = AckHarness::new(false, Some(2), ConsumeConcurrentlyStatus::ReconsumeLater);
        assert!(!h.run(3).await);
        assert_eq!(h.pending(), vec![(0, 1), (1, 1), (2, 1)]);
        assert_eq!(h.offset(), 0, "整批都没被认可，位点原地不动");
    }

    #[tokio::test]
    async fn broadcasting_drops_the_tail_without_send_back() {
        let h = AckHarness::new(true, Some(0), ConsumeConcurrentlyStatus::ConsumeSuccess);
        assert!(h.run(3).await);
        assert_eq!(h.offset(), 3);
        assert_eq!(h.pending(), vec![]);
    }

    #[tokio::test]
    async fn broadcasting_reconsume_later_still_advances() {
        let h = AckHarness::new(true, None, ConsumeConcurrentlyStatus::ReconsumeLater);
        assert!(h.run(2).await);
        assert_eq!(h.offset(), 2);
        assert_eq!(h.pending(), vec![]);
    }

    // ---------------- 顺序消费的 checkReconsumeTimes ----------------

    /// 三处坏掉都是**静默**的：少 +1 永远到不了阈值（毒消息原地转到天荒地老）；`-1` 读成
    /// 16 会给顺序消费凭空造死信；回投成功后还挂起，则一条毒消息永久占住那条队列 ——
    /// 真机上看起来跟「消费者挂了」一模一样。所以判据全部离线锁死，只有「回投成功 →
    /// 位点前进」那一支（未 `start()` 时必然走不到）交给真机 `examples/live_consumer.rs`。
    struct OrderlyListener {
        status: ConsumeOrderlyStatus,
        auto_commit: Option<bool>,
    }

    impl MessageListenerOrderly for OrderlyListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            context: &mut ConsumeOrderlyContext,
        ) -> ConsumeOrderlyStatus {
            if let Some(auto) = self.auto_commit {
                context.auto_commit = auto;
            }
            self.status
        }
    }

    /// 记录 after 钩子看到的 `(status, success, ConsumeContextType)`。
    struct RecordingHook {
        after: Mutex<Vec<(String, bool, String)>>,
    }

    impl ConsumeMessageHook for RecordingHook {
        fn hook_name(&self) -> &str {
            "recording-hook"
        }

        fn consume_message_after(&self, ctx: &mut ConsumeMessageContext) -> Result<()> {
            let context_type = ctx
                .props
                .as_ref()
                .and_then(|p| p.get("ConsumeContextType").cloned())
                .unwrap_or_default();
            lock(&self.after).push((
                ctx.status.clone().unwrap_or_default(),
                ctx.success,
                context_type,
            ));
            Ok(())
        }
    }

    struct OrderlyHarness {
        c: DefaultMQPushConsumer,
        key: String,
        mq: MessageQueue,
    }

    impl OrderlyHarness {
        fn new(max_reconsume_times: i32, status: ConsumeOrderlyStatus) -> OrderlyHarness {
            OrderlyHarness::build(max_reconsume_times, status, None)
        }

        fn new_manual(max_reconsume_times: i32, status: ConsumeOrderlyStatus) -> OrderlyHarness {
            OrderlyHarness::build(max_reconsume_times, status, Some(false))
        }

        fn build(
            max_reconsume_times: i32,
            status: ConsumeOrderlyStatus,
            auto_commit: Option<bool>,
        ) -> OrderlyHarness {
            let cfg = ConsumerConfig {
                consumer_group: "G".to_string(),
                max_reconsume_times,
                suspend_current_queue_time_millis: 0,
                ..Default::default()
            };
            let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
            c.set_message_listener_orderly(Arc::new(OrderlyListener {
                status,
                auto_commit,
            }));
            let mq = queue("T", "broker-a", 0);
            let key = mq_key(&mq);
            lock(&c.inner.state)
                .pending
                .insert(key.clone(), VecDeque::new());
            OrderlyHarness { c, key, mq }
        }

        fn with_hook(self, hook: Arc<dyn ConsumeMessageHook>) -> OrderlyHarness {
            self.c.register_consume_message_hook(hook);
            self
        }

        async fn run(&self, batch: Vec<MessageExt>) -> bool {
            consume_batch(&self.c.inner, &self.key, &self.mq, batch, None)
                .await
                .expect("consume_batch must not error")
        }

        fn offset(&self) -> i64 {
            lock(&self.c.inner.state)
                .consume_offsets
                .get(&self.key)
                .copied()
                .unwrap_or(0)
        }

        fn pending(&self) -> Vec<(i64, i32)> {
            lock(&self.c.inner.state)
                .pending
                .get(&self.key)
                .map(|dq| {
                    dq.iter()
                        .map(|m| (m.queue_offset, m.reconsume_times))
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    /// queueOffset = `start`..`start+n`，每条自带 reconsumeTimes。
    fn orderly_batch(start: i64, reconsume_times: &[i32]) -> Vec<MessageExt> {
        reconsume_times
            .iter()
            .enumerate()
            .map(|(i, rt)| {
                let i = i64::try_from(i).expect("批次长度不会超过 i64");
                let mut m = ext("T", None);
                m.broker_name = Some("broker-a".to_string());
                m.queue_offset = start + i;
                m.reconsume_times = *rt;
                m.msg_id = Some(format!("mid-{}", start + i));
                m
            })
            .collect()
    }

    fn cfg_with(max_reconsume_times: i32) -> ConsumerConfig {
        ConsumerConfig {
            consumer_group: "G".to_string(),
            max_reconsume_times,
            ..Default::default()
        }
    }

    #[test]
    fn orderly_minus_one_means_unlimited_not_the_concurrent_sixteen() {
        assert_eq!(orderly_max_reconsume_times(&cfg_with(-1)), i32::MAX);
        assert_eq!(orderly_max_reconsume_times(&cfg_with(3)), 3);
    }

    #[tokio::test]
    async fn default_cap_never_judges_the_poison_message_exhausted() {
        // 默认配置（-1）下即便 reconsumeTimes 已到 i32::MAX-1，也只是本地 +1 继续挂起
        // 重试，不回投（更不会被 broker 投进 %DLQ%）。
        let h = OrderlyHarness::new(-1, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let inner = h.c.inner.clone();
        let mut batch = orderly_batch(0, &[i32::MAX - 1]);
        assert!(check_orderly_reconsume_times(&inner, &mut batch).await);
        assert_eq!(batch[0].reconsume_times, i32::MAX);
    }

    #[tokio::test]
    async fn below_cap_counts_locally_and_suspends() {
        // 没用尽时压根不回投（未 start 也回投不了，所以这条判据必须自己走判据函数）
        let h = OrderlyHarness::new(3, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let inner = h.c.inner.clone();
        let mut batch = orderly_batch(0, &[0, 2]);
        assert!(check_orderly_reconsume_times(&inner, &mut batch).await);
        assert_eq!(
            batch.iter().map(|m| m.reconsume_times).collect::<Vec<_>>(),
            vec![1, 3],
            "broker 侧没记这次失败，客户端不就地 +1 就永远到不了阈值"
        );
    }

    #[tokio::test]
    async fn at_cap_with_failed_send_back_still_suspends() {
        let h = OrderlyHarness::new(2, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let inner = h.c.inner.clone();
        let mut batch = orderly_batch(7, &[2]);
        assert!(check_orderly_reconsume_times(&inner, &mut batch).await);
        assert_eq!(batch[0].reconsume_times, 3);
    }

    #[tokio::test]
    async fn empty_batch_does_not_suspend() {
        let h = OrderlyHarness::new(0, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let inner = h.c.inner.clone();
        let mut empty: Vec<MessageExt> = Vec::new();
        assert!(!check_orderly_reconsume_times(&inner, &mut empty).await);
    }

    #[test]
    fn retry_message_carries_exactly_the_contract_fields() {
        let cfg = cfg_with(2);
        let poison = {
            let mut m = orderly_batch(7, &[2]).remove(0);
            m.put_property(PROPERTY_TRANSACTION_PREPARED, "true");
            m.put_property(PROPERTY_KEYS, "k7");
            m
        };
        let new_msg = build_retry_message(&cfg, &poison, orderly_max_reconsume_times(&cfg));
        assert_eq!(new_msg.topic, "%RETRY%G", "顺序回投走 %RETRY%<group>");
        assert_eq!(new_msg.body, poison.body);
        assert_eq!(new_msg.get_property(PROPERTY_KEYS), Some("k7"));
        assert_eq!(new_msg.get_property(PROPERTY_RETRY_TOPIC), Some("T"));
        assert_eq!(new_msg.get_property(PROPERTY_RECONSUME_TIME), Some("3"));
        assert_eq!(
            new_msg.get_property(PROPERTY_MAX_RECONSUME_TIMES),
            Some("2")
        );
        assert_eq!(
            new_msg.get_property("DELAY"),
            Some("5"),
            "delayLevel = 3 + 2"
        );
        assert_eq!(
            new_msg.get_property(PROPERTY_ORIGIN_MESSAGE_ID),
            Some("mid-7"),
            "轨迹要靠 ORIGIN_MESSAGE_ID 把重投串回原消息"
        );
        assert_eq!(
            new_msg.get_property(PROPERTY_TRANSACTION_PREPARED),
            None,
            "半消息标记必须清掉，否则 broker 会把它当回查消息"
        );
    }

    #[tokio::test]
    async fn suspend_requeues_batch_while_below_cap() {
        let h = OrderlyHarness::new(3, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        // 队尾还有下一条（offset 2），本批要按原顺序插到它前面
        let mut tail: VecDeque<MessageExt> = orderly_batch(2, &[0]).into_iter().collect();
        lock(&h.c.inner.state)
            .pending
            .get_mut(&h.key)
            .expect("pending")
            .append(&mut tail);
        assert!(!h.run(orderly_batch(0, &[0, 1])).await);
        assert_eq!(h.offset(), 0, "还在原地重试，位点不能越过它");
        assert_eq!(
            h.pending(),
            vec![(0, 1), (1, 2), (2, 0)],
            "整批按原顺序塞回队首（顺序消费不许乱序）"
        );
    }

    #[tokio::test]
    async fn success_path_skips_the_reconsume_gate() {
        // 阈值 0：一旦走到判据就会尝试回投（离线必败 → 挂起），所以「位点前进」
        // 本身就证明了 SUCCESS 没碰判据。
        let h = OrderlyHarness::new(0, ConsumeOrderlyStatus::Success);
        assert!(h.run(orderly_batch(0, &[0])).await);
        assert_eq!(h.offset(), 1);
        assert_eq!(h.pending(), vec![]);
    }

    // ---------------- 挂起时长：context 优先 + 钳到 [10, 30000] ----------------

    #[test]
    fn orderly_suspend_millis_resolves_then_clamps() {
        let cfg = |ms: i64| ConsumerConfig {
            suspend_current_queue_time_millis: ms,
            ..Default::default()
        };
        let ctx = |ms: i64| {
            let mut c = ConsumeOrderlyContext::new(None);
            c.suspend_current_queue_time_millis = ms;
            c
        };
        assert_eq!(
            orderly_suspend_millis(&cfg(1000), &ctx(-1)),
            1000,
            "-1 = 用配置"
        );
        assert_eq!(orderly_suspend_millis(&cfg(250), &ctx(-1)), 250);
        assert_eq!(
            orderly_suspend_millis(&cfg(0), &ctx(-1)),
            10,
            "配置侧也要过钳位"
        );
        assert_eq!(orderly_suspend_millis(&cfg(60_000), &ctx(-1)), 30_000);
        assert_eq!(
            orderly_suspend_millis(&cfg(900), &ctx(70)),
            70,
            "给了值就忽略配置"
        );
        for (asked, expect) in [
            (0i64, 10u64),
            (9, 10),
            (10, 10),
            (250, 250),
            (30_000, 30_000),
            (30_001, 30_000),
            (i64::MAX, 30_000),
        ] {
            assert_eq!(
                orderly_suspend_millis(&cfg(1000), &ctx(asked)),
                expect,
                "asked={asked}"
            );
        }
    }

    // ---------------- autoCommit=true：COMMIT/ROLLBACK 是非法用法 ----------------

    #[tokio::test]
    async fn illegal_commit_and_rollback_under_auto_commit_are_acked() {
        for status in [ConsumeOrderlyStatus::Commit, ConsumeOrderlyStatus::Rollback] {
            let h = OrderlyHarness::new(0, status);
            assert!(
                h.run(orderly_batch(0, &[0])).await,
                "{status:?} 应按整批认可处理"
            );
            assert_eq!(h.offset(), 1, "{status:?} 位点必须前进");
            assert_eq!(h.pending(), vec![], "{status:?} 不该回投");
        }
    }


    #[tokio::test]
    async fn manual_commit_advances_without_requeueing() {
        let h = OrderlyHarness::new_manual(0, ConsumeOrderlyStatus::Commit);
        assert!(h.run(orderly_batch(0, &[0])).await);
        assert_eq!(h.offset(), 1, "COMMIT 就是要提交位点");
        assert_eq!(h.pending(), vec![]);
    }

    #[tokio::test]
    async fn manual_rollback_requeues_and_holds_the_offset() {
        let h = OrderlyHarness::new_manual(0, ConsumeOrderlyStatus::Rollback);
        assert!(!h.run(orderly_batch(0, &[0])).await);
        assert_eq!(h.offset(), 0, "回滚不能提交位点");
        assert_eq!(h.pending(), vec![(0, 0)], "消息退回队首重试");
    }

    #[tokio::test]
    async fn manual_success_holds_the_batch_without_committing() {
        let h = OrderlyHarness::new_manual(0, ConsumeOrderlyStatus::Success);
        // 两条都是 reconsumeTimes=0：手动模式下 SUCCESS 连本地计数都不该动
        assert!(!h.run(orderly_batch(0, &[0, 0])).await);
        assert_eq!(h.offset(), 0, "手动模式下 SUCCESS 不提交");
        assert_eq!(h.pending(), vec![(0, 0), (1, 0)], "整批按原顺序退回");
    }

    #[tokio::test]
    async fn manual_suspend_never_commits_the_offset() {
        let h = OrderlyHarness::new_manual(2, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        assert!(!h.run(orderly_batch(7, &[2])).await);
        assert_eq!(h.offset(), 0, "手动模式挂了也不提交位点");
    }

    #[tokio::test]
    async fn orderly_hook_gets_raw_status_and_normalized_success() {
        let cases = [
            (ConsumeOrderlyStatus::Commit, "COMMIT", true),
            (ConsumeOrderlyStatus::Rollback, "ROLLBACK", false),
            (
                ConsumeOrderlyStatus::SuspendCurrentQueueAMoment,
                "SUSPEND_CURRENT_QUEUE_A_MOMENT",
                false,
            ),
            (ConsumeOrderlyStatus::Success, "SUCCESS", true),
        ];
        for (status, expect_status, expect_success) in cases {
            let hook = Arc::new(RecordingHook {
                after: Mutex::new(Vec::new()),
            });
            let h = OrderlyHarness::new(0, status).with_hook(hook.clone());
            let _ = h.run(orderly_batch(0, &[0])).await;
            let seen = lock(&hook.after);
            assert_eq!(seen.len(), 1, "{status:?}: after 钩子必须触发一次");
            assert_eq!(seen[0].0, expect_status, "{status:?} 的钩子 status");
            assert_eq!(seen[0].1, expect_success, "{status:?} 的钩子 success");
        }
    }

    // ---------------- 309 CONSUME_MESSAGE_DIRECTLY ----------------

    #[test]
    fn direct_consume_maps_orderly_commit_rollback_and_auto_commit() {
        let cases = [
            (ConsumeOrderlyStatus::Success, CMResult::CR_SUCCESS),
            (
                ConsumeOrderlyStatus::SuspendCurrentQueueAMoment,
                CMResult::CR_LATER,
            ),
            (ConsumeOrderlyStatus::Commit, CMResult::CR_COMMIT),
            (ConsumeOrderlyStatus::Rollback, CMResult::CR_ROLLBACK),
        ];
        for (status, expect) in cases {
            let h = OrderlyHarness::new_manual(0, status);
            let r =
                h.c.consume_message_directly(
                    orderly_batch(0, &[0]).remove(0),
                    Some("broker-a".to_string()),
                )
                .expect("direct consume must not error");
            assert_eq!(
                r.consume_result.as_deref(),
                Some(expect),
                "{status:?} 的映射"
            );
            assert!(r.order, "{status:?}: 顺序 listener 必须报 order=true");
            assert!(
                !r.auto_commit,
                "{status:?}: listener 设的 autoCommit=false 要原样回给 broker"
            );
        }
    }

    #[test]
    fn direct_consume_maps_concurrent_success_and_auto_commit_default() {
        let c = DefaultMQPushConsumer::with_config(cfg_with(0)).expect("consumer");
        c.set_message_listener_concurrently(Arc::new(AckListener {
            ack_index: None,
            status: ConsumeConcurrentlyStatus::ConsumeSuccess,
        }));
        let r = c
            .consume_message_directly(ext("T", None), Some("broker-a".to_string()))
            .expect("direct consume must not error");
        assert_eq!(r.consume_result.as_deref(), Some(CMResult::CR_SUCCESS));
        assert!(!r.order, "并发 listener 必须报 order=false");
        assert!(r.auto_commit, "并发侧沿用初值 autoCommit=true");
    }

    // ---------------- POP ----------------

    #[test]
    fn pop_loop_records_pull_rt_even_when_the_pop_is_empty() {
        let group = "GID_PopStats";
        let topic = "PopStatsTopic";
        let key = ConsumerStatsManager::key(topic, group);
        let stats = ConsumerStatsManager::new();
        // 倒退 30ms：RT 是 `now - began`，正向断言 >0 不依赖调度精度
        let began = current_time_millis() - 30;

        let rt = || {
            stats
                .topic_and_group_pull_rt()
                .find(&key)
                .unwrap_or_else(|| panic!("PULL_RT 应该被记进 {key}"))
        };
        let tps = || {
            stats
                .topic_and_group_pull_tps()
                .find(&key)
                .unwrap_or_else(|| panic!("PULL_TPS 应该被记进 {key}"))
        };

        // ① FOUND + 2 条：RT 记一次、TPS 按条数记一次
        record_pop_pull_stats(
            &stats,
            group,
            topic,
            &PopResult {
                status: PopStatus::Found,
                msg_found_list: vec![ext(topic, None), ext(topic, None)],
                ..Default::default()
            },
            began,
        );
        assert_eq!(rt().times(), 1);
        assert!(rt().value() >= 30, "RT 应是本轮弹出耗时: {}", rt().value());
        assert_eq!(tps().times(), 1);
        assert_eq!(tps().value(), 2);

        record_pop_pull_stats(
            &stats,
            group,
            topic,
            &PopResult {
                status: PopStatus::Found,
                ..Default::default()
            },
            began,
        );
        assert_eq!(rt().times(), 2, "FOUND 的空手而归也要记 RT");
        assert_eq!(tps().times(), 1, "空列表不能记 TPS");

        // ③ POLLING_NOT_FOUND：两格都不记。POP 的空轮询是常态，把挂起时长算进平均
        //    拉取耗时会让看板上的 pullRT 完全失去意义。
        record_pop_pull_stats(
            &stats,
            group,
            topic,
            &PopResult {
                status: PopStatus::PollingNotFound,
                ..Default::default()
            },
            began,
        );
        assert_eq!(rt().times(), 2);
        assert_eq!(tps().times(), 1);
    }

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


    struct CountingOrderlyListener(Arc<AtomicUsize>);

    impl MessageListenerOrderly for CountingOrderlyListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeOrderlyContext,
        ) -> ConsumeOrderlyStatus {
            self.0.fetch_add(1, Ordering::SeqCst);
            ConsumeOrderlyStatus::Success
        }
    }

    struct CountingConcurrentlyListener(Arc<AtomicUsize>);

    impl MessageListenerConcurrently for CountingConcurrentlyListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            self.0.fetch_add(1, Ordering::SeqCst);
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    fn pop_ck_msg(queue_offset: i64) -> MessageExt {
        let mut m = ext("T", None);
        m.put_property(
            PROPERTY_POP_CK,
            &extra_info::build_extra_info(
                queue_offset,
                current_time_millis(),
                60_000,
                0,
                "T",
                "broker-a",
                0,
                Some(queue_offset),
            ),
        );
        m
    }

    #[tokio::test]
    async fn pop_orderly_dispatch_never_invokes_the_listener() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let calls = Arc::new(AtomicUsize::new(0));
        consumer.set_message_listener_orderly(Arc::new(CountingOrderlyListener(calls.clone())));
        let inner = consumer.inner.clone();
        let mq = queue("T", "broker-a", 0);
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(2);
        submit_pop_consume_request(
            &inner,
            vec![pop_ck_msg(0), pop_ck_msg(1)],
            pq.clone(),
            mq.clone(),
        )
        .await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "顺序 POP 停摆"
        );
        assert_eq!(
            pq.wait_ack_count(),
            2,
            "未 ack：等 invisibleTime 到期 broker 复活重投"
        );
        assert_eq!(consumer.pop_orderly_request_count(), 1);
    }

    /// 同一入口、并发监听器：走 ConsumeMessagePopConcurrentlyService，照常消费 + ack。
    #[tokio::test]
    async fn pop_concurrent_dispatch_still_consumes() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let calls = Arc::new(AtomicUsize::new(0));
        consumer.set_message_listener_concurrently(Arc::new(CountingConcurrentlyListener(
            calls.clone(),
        )));
        let inner = consumer.inner.clone();
        let mq = queue("T", "broker-a", 0);
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(1);
        submit_pop_consume_request(&inner, vec![pop_ck_msg(0)], pq.clone(), mq.clone()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(pq.wait_ack_count(), 0);
    }

    #[tokio::test]
    async fn pop_orderly_requests_dedup_by_pq_and_queue() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let inner = consumer.inner.clone();
        let pq = PopProcessQueue::new();
        let q0 = queue("T", "broker-a", 0);
        let q1 = queue("T", "broker-a", 1);
        submit_pop_orderly_request(&inner, pq.clone(), q0.clone(), false).await;
        submit_pop_orderly_request(&inner, pq.clone(), q0.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 1, "同 (pq, mq) 去重");
        submit_pop_orderly_request(&inner, pq.clone(), q1.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 2, "mq 不同算新请求");
        let pq2 = PopProcessQueue::new();
        submit_pop_orderly_request(&inner, pq2.clone(), q0.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 3, "pq 不同算新请求");
        submit_pop_orderly_request(&inner, pq.clone(), q0.clone(), true).await;
        assert_eq!(
            consumer.pop_orderly_request_count(),
            3,
            "force 只放行投递，不加集合条目"
        );
    }

    #[tokio::test]
    async fn pop_orderly_dropped_request_is_removed_from_the_set() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let inner = consumer.inner.clone();
        let pq = PopProcessQueue::new();
        let mq = queue("T", "broker-a", 0);
        submit_pop_orderly_request(&inner, pq.clone(), mq.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 1);
        pq.set_dropped(true);
        submit_pop_orderly_request(&inner, pq.clone(), mq.clone(), true).await;
        assert_eq!(
            consumer.pop_orderly_request_count(),
            0,
            "已撤队列：出集，不滞留"
        );

        // 先撤再提交：进得了集合，run 立刻又把它摘掉
        let pq2 = PopProcessQueue::new();
        pq2.set_dropped(true);
        submit_pop_orderly_request(&inner, pq2, mq.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 0);
    }

    #[tokio::test]
    async fn pop_orderly_run_on_live_queue_is_noop() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let inner = consumer.inner.clone();
        let pq = PopProcessQueue::new();
        let mq = queue("T", "broker-a", 0);
        submit_pop_orderly_request(&inner, pq.clone(), mq.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 1);
        run_pop_orderly_request(inner, pq, mq).await;
        assert_eq!(consumer.pop_orderly_request_count(), 1);
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
        assert_eq!(msgs[1].topic, "T", "非本组重试主题不动");

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
        assert_eq!(
            keys,
            vec!["Tbroker-a0".to_string(), "Tbroker-a1".to_string()]
        );
        assert!(
            info.mq_table.is_empty(),
            "pop 模式没有拉取队列，mqTable 必须为空"
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


    /// 直接种盖章，绕开真循环（本模块没有 `MQClientInstance`，循环一上来就会退出）。
    fn stamp_at(consumer: &DefaultMQPushConsumer, key: &str, at: i64) {
        lock(&consumer.inner.state)
            .last_pull_at
            .insert(key.to_string(), at);
    }

    fn stalled(consumer: &DefaultMQPushConsumer, key: &str, now: i64) -> bool {
        let state = lock(&consumer.inner.state);
        pull_stalled_locked(&state, key, now)
    }

    /// 让 consumer 看起来"已启动"，这样自愈判定才会参与 rebalance；
    /// 但不注入 `MQClientInstance`，被撤队列的位点持久化会在这条路径上直接短路。
    fn mark_started(consumer: &DefaultMQPushConsumer) {
        consumer.inner.started.store(true, Ordering::SeqCst);
    }

    #[test]
    fn pull_idle_threshold_matches_contract_and_is_strictly_greater() {
        assert_eq!(PULL_MAX_IDLE_TIME, 120_000);
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let key = mq_key(&queue("T", "broker-a", 0));
        let began = 1_700_000_000_000;

        // 从没盖过章 = 刚分配、第一轮还没发起，不能被判停摆
        assert!(!stalled(&consumer, &key, began + 10 * 60_000));
        stamp_at(&consumer, &key, began);
        assert!(!stalled(&consumer, &key, began + PULL_MAX_IDLE_TIME - 1));
        assert!(!stalled(&consumer, &key, began + PULL_MAX_IDLE_TIME));
        assert!(stalled(&consumer, &key, began + PULL_MAX_IDLE_TIME + 1));
        // 哨兵 0 = 循环已退出（tokio 任务没有 `Thread.is_alive()` 可问）
        stamp_at(&consumer, &key, 0);
        assert!(stalled(&consumer, &key, 0));
        // 逐队列独立：一把停摆不牵连另一把
        let other = mq_key(&queue("T", "broker-a", 1));
        stamp_at(&consumer, &other, began);
        assert!(!stalled(&consumer, &other, began + 1000));
    }

    #[test]
    fn stamping_marks_both_pop_clocks() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let pq = PopProcessQueue::new();
        pq.set_dropped(true); // 撤走过一次的队列，盖章不该把它复活
        lock(&consumer.inner.state)
            .pop_queues
            .insert(key.clone(), pq.clone());
        stamp_at(&consumer, &key, 0);
        assert!(stalled(&consumer, &key, current_time_millis()));

        stamp_pull_at(&consumer.inner, &key, true);
        let popped = {
            let state = lock(&consumer.inner.state);
            assert!(!pull_stalled_locked(&state, &key, current_time_millis()));
            pq.last_pop_timestamp.load(Ordering::SeqCst)
        };
        assert!(current_time_millis() - popped < 5000);

        // 循环退出留下哨兵，下一轮 rebalance 就能看见
        lock(&consumer.inner.state)
            .queue_owners
            .insert(key.clone(), 42);
        mark_pull_loop_exited(&consumer.inner, &key, 42);
        assert_eq!(
            lock(&consumer.inner.state).last_pull_at.get(&key).copied(),
            Some(0)
        );
        // 归属已经换人时不能再写哨兵，否则会误伤新属主
        let other = mq_key(&queue("T", "broker-a", 1));
        stamp_at(&consumer, &other, 555);
        lock(&consumer.inner.state)
            .queue_owners
            .insert(other.clone(), 7);
        mark_pull_loop_exited(&consumer.inner, &other, 999); // 令牌对不上
        assert_eq!(
            lock(&consumer.inner.state)
                .last_pull_at
                .get(&other)
                .copied(),
            Some(555)
        );
    }

    /// `retire_queue_locked`：撤一把队列要把所有痕迹摘干净，并把已消费位点交给收尾路径。
    #[test]
    fn retiring_clears_every_trace_and_reports_the_offset_to_persist() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let pq = PopProcessQueue::new();
        {
            let mut state = lock(&consumer.inner.state);
            state.queue_owners.insert(key.clone(), 7);
            state.mq_map.insert(key.clone(), mq.clone());
            state.pending.insert(key.clone(), VecDeque::new());
            state.offset_table.insert(key.clone(), 99);
            state.consume_offsets.insert(key.clone(), 42);
            state.lock_ok.insert(key.clone());
            state.last_pull_at.insert(key.clone(), 123);
            state.pop_queues.insert(key.clone(), pq.clone());
            state
                .inflight_msgs
                .insert(key.clone(), vec![ext("T", None)]);
        }
        let mut revoked = Vec::new();
        let mut state = lock(&consumer.inner.state);
        DefaultMQPushConsumer::retire_queue_locked(&mut state, &key, None, &mut revoked, true);
        assert_eq!(revoked, vec![(mq.clone(), Some(42))]);
        assert!(state.queue_owners.is_empty());
        assert!(state.pending.is_empty());
        assert!(
            state.inflight_msgs.is_empty(),
            "撤销后旧批次的引用不能留在清扫视图里"
        );
        assert!(state.offset_table.is_empty());
        assert!(state.consume_offsets.is_empty());
        assert!(state.lock_ok.is_empty());
        assert!(state.pop_queues.is_empty());
        assert!(pq.is_dropped());
        // 盖章必须一起清掉：同名队列复用时继承旧时刻会立刻被误判停摆
        assert!(state.last_pull_at.is_empty());

        // 队列不在 `mq_map` 里（停摆自愈：还归本实例，只是循环死了）走 fallback
        let mut revoked = Vec::new();
        state.mq_map.insert("gone".to_string(), mq.clone());
        state.queue_owners.insert("gone".to_string(), 3);
        DefaultMQPushConsumer::retire_queue_locked(
            &mut state,
            "gone",
            Some(&mq),
            &mut revoked,
            false,
        );
        // 没有已消费位点就没什么可持久化，但队列仍要进收尾列表
        assert_eq!(revoked, vec![(mq, None)]);
        // 非 POP 模式不去碰 pop_queues（也不该凭空造一把）
        assert!(state.pop_queues.is_empty());
    }

    /// 停机位点下限钳制（[`shutdown_persist_items`]）：已推进位点若越过了仍未落定
    /// 消息（回投失败压在 pending / 还在 listener 手里）的最小 offset，持久化清单
    /// 必须钳回去 —— 否则 broker 永远不会重投这几条（立即关闭时的真丢失）。
    #[test]
    fn shutdown_persist_items_floors_offsets_below_unsettled_messages() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let mut m_low = ext("T", None);
        m_low.queue_offset = 5;
        let mut m_high = ext("T", None);
        m_high.queue_offset = 9;
        {
            let mut state = lock(&consumer.inner.state);
            state.mq_map.insert(key.clone(), mq.clone());
            // 位点已被推进到 10（例如同一批里回投成功的那几条先落定）
            state.consume_offsets.insert(key.clone(), 10);
            // 一条回投失败压在 pending，一条还在 listener 手里
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![m_low]));
            state.inflight_msgs.insert(key.clone(), vec![m_high]);
        }
        let items = shutdown_persist_items(&consumer.inner);
        assert_eq!(items, vec![(mq, 5)]);

        // 没有未落定消息的队列按已推进位点原样持久化
        let other = queue("T", "broker-a", 1);
        let okey = mq_key(&other);
        {
            let mut state = lock(&consumer.inner.state);
            state.mq_map.insert(okey.clone(), other.clone());
            state.consume_offsets.insert(okey, 7);
        }
        let items = shutdown_persist_items(&consumer.inner);
        assert!(items.contains(&(other, 7)), "items={items:?}");
    }

    #[tokio::test]
    async fn a_stalled_queue_is_dropped_and_rebuilt_in_one_pass() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        mark_started(&consumer);
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let before = current_time_millis();
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            // 旧令牌故意用一个不会被重新发出的值，才能看出"归属确实换了"
            state.queue_owners.insert(key.clone(), 999);
            state.mq_map.insert(key.clone(), mq.clone());
            state.consume_offsets.insert(key.clone(), 42);
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![ext("T", None)]));
            state.offset_table.insert(key.clone(), 7);
            state
                .last_pull_at
                .insert(key.clone(), before - PULL_MAX_IDLE_TIME - 1000);
        }
        consumer.rebalance_pull_threads().await;
        let state = lock(&consumer.inner.state);
        // 换了归属令牌 = 旧循环失效、新循环接管
        assert_ne!(state.queue_owners.get(&key).copied(), Some(999));
        // 缓冲与游标全清：在途消息不再由旧循环消费
        assert!(state.pending.is_empty());
        assert!(state.offset_table.is_empty());
        // 已消费位点交给收尾路径持久化（这里没有 client，只验它被摘了出来）
        assert!(!state.consume_offsets.contains_key(&key));
        // 队列对象仍登记着：自愈不该让订阅变得不可见
        assert_eq!(state.mq_map.get(&key), Some(&mq));
        // 新循环从"现在"重新计时，不会被下一轮立刻再判停摆
        let seeded = *state.last_pull_at.get(&key).unwrap_or(&0);
        assert!(
            seeded >= before || seeded == 0,
            "stamp should be reseeded, got {seeded} (before={before})"
        );
    }

    /// 健康的队列必须**原封不动**：换令牌等于把在途消息丢掉。
    #[tokio::test]
    async fn a_healthy_queue_keeps_its_loop_and_offset() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        mark_started(&consumer);
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            state.queue_owners.insert(key.clone(), 5);
            state.mq_map.insert(key.clone(), mq.clone());
            state.consume_offsets.insert(key.clone(), 42);
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![ext("T", None)]));
            state
                .last_pull_at
                .insert(key.clone(), current_time_millis());
        }
        consumer.rebalance_pull_threads().await;
        let state = lock(&consumer.inner.state);
        assert_eq!(state.queue_owners.get(&key).copied(), Some(5));
        assert_eq!(state.pending.get(&key).map(VecDeque::len), Some(1));
        assert_eq!(state.consume_offsets.get(&key).copied(), Some(42));
    }

    /// 未启动（含正在停机）时不做自愈：那会把清退过程变成 `[BUG]` 日志风暴，
    /// 而且停摆判据本身在 `started=false` 下没有意义。
    #[tokio::test]
    async fn stalled_sweep_only_runs_while_started() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            state.queue_owners.insert(key.clone(), 1);
            state.mq_map.insert(key.clone(), mq.clone());
        }
        stamp_at(&consumer, &key, 1); // 老得不能再老
        consumer.rebalance_pull_threads().await;
        assert_eq!(
            lock(&consumer.inner.state).queue_owners.get(&key).copied(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn pop_mode_swaps_the_process_queue_when_self_healing() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        mark_started(&consumer);
        consumer.update_config(|c| c.pop_mode = true);
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let old = PopProcessQueue::new();
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            state.queue_owners.insert(key.clone(), 999);
            state.mq_map.insert(key.clone(), mq.clone());
            state.pop_queues.insert(key.clone(), old.clone());
        }
        stamp_at(
            &consumer,
            &key,
            current_time_millis() - PULL_MAX_IDLE_TIME - 1,
        );
        consumer.rebalance_pull_threads().await;
        let state = lock(&consumer.inner.state);
        assert!(old.is_dropped());
        let fresh = state.pop_queues.get(&key).cloned().expect("rebuilt");
        assert!(!Arc::ptr_eq(&old, &fresh));
        assert!(!fresh.is_dropped());
        assert_ne!(state.queue_owners.get(&key).copied(), Some(999));
    }

    #[test]
    fn running_info_publishes_the_pull_clock() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        {
            let mut state = lock(&consumer.inner.state);
            state.mq_map.insert(key.clone(), mq.clone());
        }
        stamp_at(&consumer, &key, 1_700_000_000_456);
        let info = consumer.consumer_running_info();
        let pqi = &info.mq_table[0].1;
        assert_eq!(
            pqi.get("lastPullTimestamp"),
            Some(&serde_json::Value::from(1_700_000_000_456_i64))
        );
        // 没盖过章的队列报 0，而不是拿当前时间冒充"还在拉"
        let other = queue("T", "broker-a", 1);
        lock(&consumer.inner.state)
            .mq_map
            .insert(mq_key(&other), other.clone());
        let info = consumer.consumer_running_info();
        let pqi = &info.mq_table[1].1;
        assert_eq!(
            pqi.get("lastPullTimestamp"),
            Some(&serde_json::Value::from(0))
        );
    }

    #[test]
    fn pop_running_info_publishes_the_pop_clock() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let pq = PopProcessQueue::new();
        pq.touch(1_700_000_000_789);
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            state.pop_queues.insert(key.clone(), pq);
        }
        let info = consumer.consumer_running_info();
        assert_eq!(info.mq_pop_table.len(), 1);
        assert_eq!(
            info.mq_pop_table[0].1.get("lastPullTimestamp"),
            Some(&serde_json::Value::from(1_700_000_000_789_i64))
        );
    }

    /// 停机要把盖章表清空：下次 start() 不能继承旧实例的时刻。
    #[test]
    fn shutdown_clears_the_pull_clock() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        mark_started(&consumer);
        let key = mq_key(&queue("T", "broker-a", 0));
        stamp_at(&consumer, &key, 123);
        consumer.shutdown();
        assert!(lock(&consumer.inner.state).last_pull_at.is_empty());
        assert!(lock(&consumer.inner.state).queue_owners.is_empty());
    }


    // 真实 fastjson2 2.0.59 + rocketmq-client 5.5.0 的输出（含 pretty 版）
    const CONTRACT_OFFSETS_COMPACT: &str = concat!(
        r#"{"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9,"#,
        r#"{"brokerName":"broker-a","queueId":0,"topic":"Tt"}:7}}"#,
    );
    const CONTRACT_OFFSETS_PRETTY: &str =
        "{\n\t\"offsetTable\":{\n\t\t{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7\n\t}\n}";

    fn test_mq_map() -> BTreeMap<String, MessageQueue> {
        (0..3)
            .map(|i| {
                let mq = MessageQueue::new("Tt", "broker-a", i);
                (mq_key(&mq), mq)
            })
            .collect()
    }

    #[test]
    fn local_offsets_build_uses_contract_object_as_key_format() {
        let text = build_local_offsets_json(
            &BTreeMap::from([
                ("Ttbroker-a0".to_string(), 7),
                ("Ttbroker-a1".to_string(), 9),
            ]),
            &test_mq_map(),
        );
        assert!(text.starts_with("{\"offsetTable\":{"));
        // 字段序 brokerName/queueId/topic（fastjson2 字母序），每个 key 是内嵌对象
        assert!(text.contains(r#""brokerName":"broker-a","queueId":0,"topic":"Tt"}:7"#));
        assert!(text.contains(r#""brokerName":"broker-a","queueId":1,"topic":"Tt"}:9"#));
    }

    #[test]
    fn local_offsets_build_skips_entries_without_queue_info() {
        let text = build_local_offsets_json(
            &BTreeMap::from([("Ttbroker-a0".to_string(), 7), ("Orphan_b0".to_string(), 3)]),
            &test_mq_map(),
        );
        assert!(!text.contains("Orphan"));
    }

    #[test]
    fn local_offsets_parse_contract_format_compact_and_pretty() {
        let compact = parse_local_offsets_text(CONTRACT_OFFSETS_COMPACT).unwrap();
        assert_eq!(compact.get("Ttbroker-a1"), Some(&9));
        assert_eq!(compact.get("Ttbroker-a0"), Some(&7));
        let pretty = parse_local_offsets_text(CONTRACT_OFFSETS_PRETTY).unwrap();
        assert_eq!(pretty.get("Ttbroker-a0"), Some(&7));
    }

    #[test]
    fn local_offsets_parse_accepts_legacy_flat_map() {
        let flat = parse_local_offsets_text(r#"{"Ttbroker-a0":5}"#).unwrap();
        assert_eq!(flat.get("Ttbroker-a0"), Some(&5));
    }

    #[test]
    fn local_offsets_parse_rejects_garbage() {
        assert!(parse_local_offsets_text("{\"offsetTable\":{{{").is_none());
        assert!(parse_local_offsets_text("{\"offsetTable\":").is_none());
        assert!(parse_local_offsets_text("").is_none());
    }

    #[test]
    fn local_offsets_roundtrip_built_text() {
        let text = build_local_offsets_json(
            &BTreeMap::from([
                ("Ttbroker-a0".to_string(), 7),
                ("Ttbroker-a1".to_string(), 9),
                ("Ttbroker-a2".to_string(), 11),
            ]),
            &test_mq_map(),
        );
        let parsed = parse_local_offsets_text(&text).unwrap();
        assert_eq!(parsed.get("Ttbroker-a0"), Some(&7));
        assert_eq!(parsed.get("Ttbroker-a1"), Some(&9));
        assert_eq!(parsed.get("Ttbroker-a2"), Some(&11));
    }

    #[test]
    fn local_offsets_save_writes_contract_format_and_rolls_bak() {
        let dir = std::env::temp_dir().join(format!(
            "rmq_rust_offsets_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("offsets.json");
        let items = BTreeMap::from([
            ("Ttbroker-a0".to_string(), 7),
            ("Ttbroker-a1".to_string(), 9),
            ("Ttbroker-a2".to_string(), 11),
        ]);
        let mq_map = test_mq_map();

        save_local_offsets_at(&path, &items, &mq_map).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.starts_with("{\"offsetTable\":{"));
        assert!(!path.with_file_name("offsets.json.bak").exists());

        save_local_offsets_at(
            &path,
            &BTreeMap::from([("Ttbroker-a0".to_string(), 99)]),
            &mq_map,
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(path.with_file_name("offsets.json.bak")).unwrap(),
            raw
        );

        // 主文件在 → 读主文件；主文件缺失 → .bak（上一代）
        let loaded = load_local_offsets_at(&path);
        assert_eq!(loaded.get("Ttbroker-a0"), Some(&99));
        std::fs::remove_file(&path).unwrap();
        let loaded = load_local_offsets_at(&path);
        assert_eq!(loaded.get("Ttbroker-a0"), Some(&7));

        // .bak 也缺失 → 空（按首次启动处理）
        std::fs::remove_file(path.with_file_name("offsets.json.bak")).unwrap();
        assert!(load_local_offsets_at(&path).is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn local_offsets_load_reads_contract_written_file() {
        let dir = std::env::temp_dir().join(format!(
            "rmq_rust_offsets_java_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("offsets.json");
        std::fs::write(&path, CONTRACT_OFFSETS_COMPACT).unwrap();
        let loaded = load_local_offsets_at(&path);
        assert_eq!(loaded.get("Ttbroker-a1"), Some(&9));
        assert_eq!(loaded.get("Ttbroker-a0"), Some(&7));
        let _ = std::fs::remove_dir_all(dir);
    }

    // ---------------- P5: postSubscriptionWhenPull / pullFromWhichNode ----------------
    //
    // 覆盖 push 侧这两节的判据。这层是纯函数，
    // 真报文形状（SUBSCRIPTION 位与 `subscription` 键是否上线、slave 地址选择、从节点
    // COMMIT_OFFSET 清位、suggestWhichBrokerId 透传）在 `mq_client.rs` 的假 broker 用例里锁死。

    const P5_TOPIC: &str = "P5PostSubscriptionTopic";

    #[test]
    fn pull_subscription_expression_follows_contract_default() {
        let s = sub(P5_TOPIC, "TagA");
        // 默认关闭
        assert_eq!(
            pull_subscription_expression(&ConsumerConfig::default(), &s),
            None
        );
        // 打开
        let cfg = ConsumerConfig {
            post_subscription_when_pull: true,
            ..ConsumerConfig::default()
        };
        assert_eq!(
            pull_subscription_expression(&cfg, &s).as_deref(),
            Some("TagA")
        );
        // 类过滤模式：即使打开也不上送（表达式是过滤类名，broker 侧 TAG 过滤会误判）
        let mut class_mode = sub(P5_TOPIC, "com.example.MyFilter");
        class_mode.class_filter_mode = true;
        assert_eq!(pull_subscription_expression(&cfg, &class_mode), None);
    }

    #[test]
    fn pull_from_which_node_defaults_to_master_and_round_trips() {
        let mut state = State::default();
        let mq = MessageQueue::new(P5_TOPIC, "broker-a", 0);
        let other = MessageQueue::new(P5_TOPIC, "broker-a", 1);
        let master = i64::from(MixAll::MASTER_ID);

        assert_eq!(
            recalc_pull_from_which_node(&state, &mq),
            master,
            "首轮打主节点"
        );

        // 老 broker 不带 suggest：按 master 记账，而不是保留旧值
        update_pull_from_which_node(&mut state, &mq, Some(master + 1));
        assert_eq!(recalc_pull_from_which_node(&state, &mq), master + 1);
        update_pull_from_which_node(&mut state, &mq, None);
        assert_eq!(recalc_pull_from_which_node(&state, &mq), master);

        // 按队列隔离
        update_pull_from_which_node(&mut state, &mq, Some(2));
        assert_eq!(recalc_pull_from_which_node(&state, &mq), 2);
        assert_eq!(recalc_pull_from_which_node(&state, &other), master);
    }


    const CTO_TOPIC: &str = "P6CorrectTagsOffsetTopic";

    fn cto_mq() -> MessageQueue {
        queue(CTO_TOPIC, "broker-a", 0)
    }

    /// 直接种状态、直接调[`correct_tags_offset_locked`]：状态判据也收在函数里，
    /// 于是一次调用就能同时锁死"哪些状态要修正"和"何时允许修正"。
    struct CtoHarness {
        state: State,
        key: String,
    }

    impl CtoHarness {
        fn new() -> CtoHarness {
            let key = mq_key(&cto_mq());
            let mut state = State::default();
            state.pending.insert(key.clone(), VecDeque::new());
            CtoHarness { state, key }
        }

        fn correct(&mut self, status: PullStatus, next_off: i64) {
            correct_tags_offset_locked(&mut self.state, &self.key, status, next_off);
        }

        fn offset(&self) -> Option<i64> {
            self.state.consume_offsets.get(&self.key).copied()
        }
    }

    /// 空应答的两个状态（NO_NEW_MSG / NO_MATCHED_MSG）都推进；其余状态一律不碰位点。
    ///
    /// NO_MATCHED_MSG 在本端口来自 broker 的 `PULL_RETRY_IMMEDIATELY`
    /// （`MQClientAPIImpl:1095-1097` 的映射）：这一段的每一条都被 tags/表达式滤掉时，
    /// 客户端既收不到消息、`msgCount` 又恒为 0 —— 不修正就永远不会前进。
    #[test]
    fn correct_tags_offset_advances_only_on_empty_pull_statuses() {
        for status in [PullStatus::NoNewMsg, PullStatus::NoMatchedMsg] {
            let mut h = CtoHarness::new();
            h.correct(status, 42);
            assert_eq!(h.offset(), Some(42), "{status:?} 要从 0 抬到应答位点");
            h.correct(status, 43);
            assert_eq!(h.offset(), Some(43), "已有记录时继续按应答前进");
        }
        for status in [PullStatus::Found, PullStatus::OffsetIllegal] {
            let mut h = CtoHarness::new();
            h.correct(status, 42);
            assert_eq!(h.offset(), None, "{status:?} 不是空应答，不许碰位点");
        }
    }

    #[test]
    fn correct_tags_offset_creates_the_record_even_for_a_zero_offset() {
        let mut h = CtoHarness::new();
        h.correct(PullStatus::NoNewMsg, 0);
        assert_eq!(h.offset(), Some(0), "空队列的 0 也要建记录");
        h.correct(PullStatus::NoNewMsg, 0);
        assert_eq!(h.offset(), Some(0), "建完再修正：不前进也不回退");
    }

    /// `increaseOnly=true`：只前进不回退。
    #[test]
    fn correct_tags_offset_never_regresses() {
        let mut h = CtoHarness::new();
        h.state.consume_offsets.insert(h.key.clone(), 100);
        h.correct(PullStatus::NoNewMsg, 50);
        assert_eq!(h.offset(), Some(100), "应答位点更小：原地不动");
        h.correct(PullStatus::NoNewMsg, 100);
        assert_eq!(h.offset(), Some(100), "相等：不动");
        h.correct(PullStatus::NoNewMsg, 101);
        assert_eq!(h.offset(), Some(101));
    }

    /// 闸门：`ProcessQueue` 里还有消息（缓冲里的 + listener 手里的）就一律不动位点。
    #[test]
    fn correct_tags_offset_respects_the_process_queue_guard() {
        // 缓冲里还有一条没消费
        let mut h = CtoHarness::new();
        h.state
            .pending
            .get_mut(&h.key)
            .unwrap()
            .push_back(ext(CTO_TOPIC, None));
        h.correct(PullStatus::NoNewMsg, 42);
        assert_eq!(h.offset(), None, "pending 非空");

        // 缓冲空了，但有一条在 listener 手里
        let mut h = CtoHarness::new();
        h.state.inflight.insert(h.key.clone(), 1);
        h.correct(PullStatus::NoMatchedMsg, 42);
        assert_eq!(h.offset(), None, "在途也算 msgCount != 0");

        // 显式的 0（收尾时短暂可能出现）不挡路
        h.state.inflight.insert(h.key.clone(), 0);
        h.correct(PullStatus::NoNewMsg, 42);
        assert_eq!(h.offset(), Some(42), "inflight 归零后可以修正");

        // 连 pending 表项都没有（从未拉过该队列）按空处理，不能因为缺项就漏掉修正
        let mut fresh = CtoHarness {
            state: State::default(),
            key: mq_key(&cto_mq()),
        };
        fresh.correct(PullStatus::NoNewMsg, 7);
        assert_eq!(fresh.offset(), Some(7));
    }

    /// listener 侧的门闩：
    /// - 进来先记下"在途期间调一次修正"的结果（**不能**在这里断言：listener 跑在
    ///   `spawn_blocking` 线程上，panic 会被 `call_concurrently_listener` 吞成
    ///   `RECONSUME_LATER`，测试反而"通过"了）；
    /// - 然后一直阻塞到主线程放开 —— 正好把"批次在途"那一瞬定住。
    ///
    /// 门闩用 tokio 通道而不是 `std::sync::mpsc`：后者的 `Sender` 不是 `Sync`，
    /// 进不了 `Send + Sync` 的监听器 trait。
    struct GatedListener {
        inner: Arc<Inner>,
        key: String,
        entered: tokio::sync::mpsc::UnboundedSender<Option<i64>>,
        release: Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>,
    }

    impl MessageListenerConcurrently for GatedListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            let observed = {
                let mut state = lock(&self.inner.state);
                correct_tags_offset_locked(&mut state, &self.key, PullStatus::NoNewMsg, 110);
                state.consume_offsets.get(&self.key).copied()
            };
            let _ = self.entered.send(observed);
            let _ = self.release.lock().expect("release lock").blocking_recv();
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    /// 真机链路（broker 过滤 → NO_MATCHED_MSG → broker 侧位点前移）见
    /// `examples/live_correct_tags_offset.rs`。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_loop_keeps_the_correction_out_until_the_listener_returns() {
        let cfg = ConsumerConfig {
            consume_message_batch_max_size: 2,
            ..Default::default()
        };
        let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
        let mq = cto_mq();
        let key = mq_key(&mq);

        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = tokio::sync::mpsc::unbounded_channel();
        c.set_message_listener_concurrently(Arc::new(GatedListener {
            inner: c.inner.clone(),
            key: key.clone(),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }));

        {
            let mut state = lock(&c.inner.state);
            state.mq_map.insert(key.clone(), mq.clone());
            let mut batch = offset_batch(2);
            for m in batch.iter_mut() {
                m.topic = CTO_TOPIC.to_string();
            }
            state
                .pending
                .insert(key.clone(), batch.into_iter().collect());
        }
        c.inner.started.store(true, Ordering::Release);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(dispatch_loop(c.inner.clone(), stop_rx));

        // listener 已进入：批次在途
        let observed = tokio::time::timeout(Duration::from_secs(5), entered_rx.recv())
            .await
            .expect("listener 未被调用（5s）")
            .expect("entered 通道意外关闭");
        assert_eq!(observed, None, "在途期间位点必须原地不动");
        assert_eq!(
            lock(&c.inner.state).inflight.get(&key).copied(),
            Some(1),
            "批次在途时 inflight 必须是 1"
        );

        release_tx.send(()).expect("release 通道已关闭");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if lock(&c.inner.state)
                .inflight
                .get(&key)
                .copied()
                .unwrap_or(0)
                == 0
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "批次未在 5s 内结清");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // 成功消费已把位点推到 max(queueOffset)+1；此时空应答的修正才允许生效
        assert_eq!(
            lock(&c.inner.state).consume_offsets.get(&key).copied(),
            Some(2)
        );
        {
            let mut state = lock(&c.inner.state);
            correct_tags_offset_locked(&mut state, &key, PullStatus::NoMatchedMsg, 110);
            assert_eq!(state.consume_offsets.get(&key).copied(), Some(110));
        }
        // 计数归零后表项就该删掉，别堆无主的 key
        assert!(!lock(&c.inner.state).inflight.contains_key(&key));

        drop(stop_tx);
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }

    // ---------------- cleanExpiredMsg（挂起 listener 的逃生口） ----------------

    #[test]
    fn clean_expire_period_equals_consume_timeout_minutes() {
        assert_eq!(clean_expire_period_millis(15), 900_000, "默认 15 分钟");
        assert_eq!(clean_expire_period_millis(1), 60_000);
        assert_eq!(
            clean_expire_period_millis(0),
            60_000,
            "0 钳到 1 分钟，别忙等"
        );
        assert_eq!(clean_expire_period_millis(-3), 60_000);
    }

    #[test]
    fn consume_start_timestamp_reads_only_well_formed_stamps() {
        let mut m = ext("T", None);
        assert_eq!(consume_start_timestamp(&m), None, "没盖章");
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, "");
        assert_eq!(consume_start_timestamp(&m), None, "空串");
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, "not-a-number");
        assert_eq!(consume_start_timestamp(&m), None, "坏值");
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, "1700000000000");
        assert_eq!(consume_start_timestamp(&m), Some(1_700_000_000_000));
    }

    /// 过期判据是**严格**大于（等于阈值不动手），没盖章不当过期。
    #[test]
    fn is_consume_expired_is_strict_and_ignores_missing_stamps() {
        let now = 10_000_000i64;
        assert!(
            is_consume_expired(Some(now - 60_001), now, 1),
            "刚过阈值 1ms"
        );
        assert!(
            !is_consume_expired(Some(now - 60_000), now, 1),
            "恰好等于阈值：严格大于才动手"
        );
        assert!(!is_consume_expired(Some(now), now, 1));
        assert!(!is_consume_expired(None, now, 1), "没盖章不当过期");
        // 默认 15 分钟：14 分 59.999 秒不动手，15 分零 1 毫秒动手
        assert!(!is_consume_expired(Some(now - 899_999), now, 15));
        assert!(is_consume_expired(Some(now - 900_001), now, 15));
    }

    #[tokio::test]
    async fn clean_expired_queue_stops_at_a_fresh_head() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let key = mq_key(&queue("T", "broker-a", 0));
        let now = current_time_millis();
        {
            let mut state = lock(&consumer.inner.state);
            let mut head = ext("T", None); // 头没盖章
            head.queue_offset = 0;
            let mut tail = ext("T", None); // 尾巴过期了也不该动
            tail.queue_offset = 1;
            tail.put_property(
                PROPERTY_CONSUME_START_TIMESTAMP,
                &(now - 120_000).to_string(),
            );
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![head, tail]));
        }
        assert_eq!(
            clean_expired_queue(&consumer.inner, &key).await,
            0,
            "队首没过期：后面的更不可能过期（命中即停）"
        );
        let state = lock(&consumer.inner.state);
        assert_eq!(state.pending.get(&key).unwrap().len(), 2, "一条都不许动");
    }

    #[tokio::test]
    async fn clean_expired_queue_leaves_unsigned_messages_alone() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let key = mq_key(&queue("T", "broker-a", 0));
        {
            let mut state = lock(&consumer.inner.state);
            let mut m = ext("T", None);
            m.queue_offset = 0;
            state.pending.insert(key.clone(), VecDeque::from(vec![m]));
        }
        assert_eq!(clean_expired_queue(&consumer.inner, &key).await, 0);
        assert_eq!(
            lock(&consumer.inner.state).pending.get(&key).unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn clean_expired_queue_keeps_the_entry_when_send_back_fails() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let key = mq_key(&queue("T", "broker-a", 0));
        let now = current_time_millis();
        let expired = |off: i64| {
            let mut m = ext("T", None);
            m.queue_offset = off;
            m.put_property(
                PROPERTY_CONSUME_START_TIMESTAMP,
                &(now - 120_000).to_string(),
            );
            m
        };
        {
            let mut state = lock(&consumer.inner.state);
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![expired(0), expired(1)]));
        }
        assert_eq!(
            clean_expired_queue(&consumer.inner, &key).await,
            2,
            "loop=min(2,16)=2：回投失败后本轮仍会对同一条队首再试一次"
        );
        let state = lock(&consumer.inner.state);
        assert_eq!(
            state.pending.get(&key).unwrap().len(),
            2,
            "回投失败绝不摘除（摘了就真丢了）"
        );
        assert!(!state.consume_offsets.contains_key(&key), "位点不许动");
    }

    #[tokio::test]
    async fn clean_expired_queue_caps_one_round_at_sixteen() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let key = mq_key(&queue("T", "broker-a", 0));
        let now = current_time_millis();
        let batch: Vec<MessageExt> = (0..20)
            .map(|i| {
                let mut m = ext("T", None);
                m.queue_offset = i;
                m.put_property(
                    PROPERTY_CONSUME_START_TIMESTAMP,
                    &(now - 120_000).to_string(),
                );
                m
            })
            .collect();
        lock(&consumer.inner.state)
            .pending
            .insert(key.clone(), batch.into());
        assert_eq!(
            clean_expired_queue(&consumer.inner, &key).await,
            16,
            "loop 在循环之前算一次：上限 16"
        );
    }

    /// 顺序消费没有这条路径（`ProcessQueue:76-78` 直接早退）。
    #[tokio::test]
    async fn clean_expired_queue_skips_orderly_consumers() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        consumer.set_message_listener_orderly(Arc::new(NoopOrderlyListener));
        let key = mq_key(&queue("T", "broker-a", 0));
        let mut m = ext("T", None);
        m.queue_offset = 0;
        m.put_property(
            PROPERTY_CONSUME_START_TIMESTAMP,
            &(current_time_millis() - 120_000).to_string(),
        );
        lock(&consumer.inner.state)
            .pending
            .insert(key.clone(), VecDeque::from(vec![m]));
        assert_eq!(clean_expired_queue(&consumer.inner, &key).await, 0);
    }

    /// 「仍是队首才摘」的两种让位：正常收尾（条目已摘/已前进）与更小位点冒头。
    #[test]
    fn remove_expired_entry_only_when_still_head() {
        let key = mq_key(&queue("T", "broker-a", 0));
        let msg = |off: i64| {
            let mut m = ext("T", None);
            m.queue_offset = off;
            m
        };

        // 仍是队首：两边一起摘
        let mut state = State::default();
        state
            .inflight_msgs
            .insert(key.clone(), vec![msg(0), msg(1)]);
        state
            .pending
            .insert(key.clone(), VecDeque::from(vec![msg(0)]));
        assert!(remove_expired_entry_if_still_head(&mut state, &key, 0));
        assert_eq!(
            state.inflight_msgs.get(&key).unwrap().len(),
            1,
            "登记侧摘掉"
        );
        assert!(state.pending.get(&key).unwrap().is_empty(), "缓冲侧也摘");

        // 前面冒出了更小的位点：让位，不许抢摘
        let mut state = State::default();
        state.inflight_msgs.insert(key.clone(), vec![msg(5)]);
        state
            .pending
            .insert(key.clone(), VecDeque::from(vec![msg(3)]));
        assert!(!remove_expired_entry_if_still_head(&mut state, &key, 5));
        assert_eq!(state.inflight_msgs.get(&key).unwrap().len(), 1);

        // 已经不在视图里（listener 正常收尾先摘了）：同样为 false，且不 panic
        let mut state = State::default();
        assert!(!remove_expired_entry_if_still_head(&mut state, &key, 0));
    }

    /// 并集视图与 containsMessage：被清扫摘掉的条目对回投判据立刻不可见。
    #[test]
    fn process_queue_view_unions_inflight_and_pending_by_offset() {
        let key = mq_key(&queue("T", "broker-a", 0));
        let msg = |off: i64| {
            let mut m = ext("T", None);
            m.queue_offset = off;
            m
        };
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        {
            let mut state = lock(&consumer.inner.state);
            state.inflight_msgs.insert(key.clone(), vec![msg(1)]);
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![msg(2), msg(1)]));
            assert_eq!(
                process_queue_entries_locked(&state, &key).len(),
                2,
                "按位点去重（登记的与缓冲的重叠只算一条）"
            );
            assert_eq!(
                process_queue_first_entry_locked(&state, &key)
                    .unwrap()
                    .queue_offset,
                1
            );
            assert!(process_queue_contains_locked(&state, &key, 1));
            assert!(process_queue_contains_locked(&state, &key, 2));
            assert!(!process_queue_contains_locked(&state, &key, 3));
        }
        {
            let mut state = lock(&consumer.inner.state);
            remove_process_queue_entry_locked(&mut state, &key, 1);
            assert!(!process_queue_contains_locked(&state, &key, 1));
            assert!(process_queue_contains_locked(&state, &key, 2));
            assert!(!state.inflight_msgs.contains_key(&key), "空桶要删掉");
        }
    }

    #[tokio::test]
    async fn send_back_batch_skips_entries_swept_away() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let key = mq_key(&queue("T", "broker-a", 0));
        let batch: Vec<MessageExt> = {
            let mut a = ext("T", None);
            a.queue_offset = 0;
            let mut b = ext("T", None);
            b.queue_offset = 1;
            vec![a, b]
        };
        // 两条都在清扫视图里（正常在途态）
        register_inflight_msgs(&consumer.inner, &key, &batch);
        // 清扫把 o1 摘走（模拟挂起超时后被打回 broker）：视图里从此没有它
        {
            let mut state = lock(&consumer.inner.state);
            remove_process_queue_entry_locked(&mut state, &key, 1);
            assert!(!process_queue_contains_locked(&state, &key, 1));
        }

        let failed = send_back_batch(&consumer.inner, &key, &batch, 0, 0).await;
        assert_eq!(
            failed.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            vec![0],
            "o0 尝试回投但失败（未 start）；o1 已被清扫，直接跳过"
        );
        assert_eq!(failed[0].1.reconsume_times, 1, "失败就地 +1");
        assert_eq!(batch[1].reconsume_times, 0, "被跳过的条目不许加次数");
    }

    #[test]
    fn send_back_lookup_ignores_started_flag_until_instance_is_taken() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert!(
            require_client(&consumer.inner).is_err(),
            "公开路径仍要 started 门"
        );
        assert!(
            require_client_for_send_back(&consumer.inner).is_err(),
            "没有实例谁都不放行"
        );

        *lock(&consumer.inner.client) =
            Some(MQClientInstance::new("gid-shutdown-sendback", vec![]));
        // shutdown() 的第一步：先置 false 再排空在途批次。
        consumer.inner.started.store(false, Ordering::Release);
        assert!(require_client(&consumer.inner).is_err());
        assert!(
            require_client_for_send_back(&consumer.inner).is_ok(),
            "排空窗口内回投必须还能取到实例"
        );

        lock(&consumer.inner.client).take();
        assert!(require_client_for_send_back(&consumer.inner).is_err());
    }

    /// 接线取证：`dispatch_loop` 走并发路径时必须把**盖过章**的这批登记进清扫视图，
    /// 并在收尾后注销。登记早了（盖章之前）表里就是未盖章副本，清扫永远看不见它们；
    /// 漏注销则位点被 correctTagsOffset 的在途闸门永久挡停。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_registers_a_stamped_inflight_view() {
        let cfg = ConsumerConfig {
            consume_message_batch_max_size: 2,
            consume_timeout: 1,
            ..Default::default()
        };
        let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);

        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = tokio::sync::mpsc::unbounded_channel();
        c.set_message_listener_concurrently(Arc::new(GatedListener {
            inner: c.inner.clone(),
            key: key.clone(),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }));
        {
            let mut state = lock(&c.inner.state);
            state.mq_map.insert(key.clone(), mq.clone());
            state.pending.insert(key.clone(), offset_batch(2).into());
        }
        c.inner.started.store(true, Ordering::Release);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(dispatch_loop(c.inner.clone(), stop_rx));

        let _ = tokio::time::timeout(Duration::from_secs(5), entered_rx.recv())
            .await
            .expect("listener 未被调用（5s）")
            .expect("entered 通道意外关闭");
        {
            let state = lock(&c.inner.state);
            let entries = process_queue_entries_locked(&state, &key);
            assert_eq!(entries.len(), 2, "在途批次要出现在清扫视图里");
            assert!(
                entries.iter().all(|m| consume_start_timestamp(m).is_some()),
                "登记进视图的副本必须已盖 CONSUME_START_TIME"
            );
            assert!(
                !is_consume_expired(
                    consume_start_timestamp(&entries[0]),
                    current_time_millis(),
                    1
                ),
                "刚登记的消息不该立刻判成过期"
            );
        }

        release_tx.send(()).expect("release 通道已关闭");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if !lock(&c.inner.state).inflight_msgs.contains_key(&key) {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "登记未在 5s 内注销");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // 清扫视图随之回到「只剩缓冲消息」的状态
        assert!(process_queue_entries_locked(&lock(&c.inner.state), &key).is_empty());

        drop(stop_tx);
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }

    #[tokio::test]
    async fn clean_expired_msg_once_scans_held_queues_only() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let held = mq_key(&queue("T", "broker-a", 0));
        let other = mq_key(&queue("T", "broker-a", 1));
        let now = current_time_millis();
        let expired = {
            let mut m = ext("T", None);
            m.queue_offset = 0;
            m.put_property(
                PROPERTY_CONSUME_START_TIMESTAMP,
                &(now - 120_000).to_string(),
            );
            m
        };
        {
            let mut state = lock(&consumer.inner.state);
            state.mq_map.insert(held.clone(), queue("T", "broker-a", 0));
            state
                .pending
                .insert(held.clone(), VecDeque::from(vec![expired.clone()]));
            // 另一个队列不在 mq_map 里（已撤）：即便缓冲里还残留也不该被扫到
            state
                .pending
                .insert(other.clone(), VecDeque::from(vec![expired]));
        }
        assert_eq!(
            clean_expired_msg_once(&consumer.inner).await,
            1,
            "只有仍持有的那条队列被扫"
        );
    }

    #[tokio::test]
    async fn pop_dispatch_stamps_the_consume_start_time() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let seen = Arc::new(Mutex::new(Vec::<Option<i64>>::new()));
        consumer.set_message_listener_concurrently(Arc::new(StampRecordingListener(seen.clone())));
        let inner = consumer.inner.clone();
        let mq = queue("T", "broker-a", 0);
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(1);
        submit_pop_consume_request(&inner, vec![pop_ck_msg(0)], pq.clone(), mq.clone()).await;
        let stamps = seen.lock().unwrap().clone();
        assert_eq!(stamps.len(), 1);
        assert!(
            stamps[0].is_some(),
            "POP 批次在 listener 之前必须盖 CONSUME_START_TIME"
        );
    }

    /// 记录 listener 看到的 CONSUME_START_TIME（POP 盖章取证用）。
    struct StampRecordingListener(Arc<Mutex<Vec<Option<i64>>>>);

    impl MessageListenerConcurrently for StampRecordingListener {
        fn consume_message(
            &self,
            msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            self.0
                .lock()
                .unwrap()
                .extend(msgs.iter().map(consume_start_timestamp));
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    // 这条路径错了是**静默**的两种极端 ——
    //
    //   * 只把拉取游标拨到 `nextBeginOffset` 而不丢队列/不冻结：broker 刚把位点纠正到合法
    //     区间，这条队列上**已经取回还没 ack** 的旧批次一 ack 又把位点推回非法值，下一轮
    //     拉取再被 broker 拒一次 —— 客户端与 broker 之间来回弹跳，永不停歇；
    //   * 纠错后的位点没立刻落盘：进程在下一轮周期落盘（默认 5s）之前崩掉，broker 上留着的
    //     还是非法位点，重启后从非法位点起拉 —— 这条纠错等于没做。
    //
    // 两个方向在真机短期窗口里都看不出差别（消息照消费、只是一直在弹/一次崩溃才暴露），
    // 所以断言全部放离线；真机另有一条链路证明（见 `examples/live_offset_illegal.rs`）。

    /// 模拟 listener 返回后的位点推进（`consume_batch` 收尾调的就是它）。
    fn ack_batch(c: &DefaultMQPushConsumer, key: &str, offsets: &[i64], epoch: u64) {
        let batch: Vec<MessageExt> = offsets
            .iter()
            .map(|o| {
                let mut m = ext("T", None);
                m.broker_name = Some("broker-a".to_string());
                m.queue_offset = *o;
                m
            })
            .collect();
        advance_consume_offset(&c.inner, key, &batch, None, Some(epoch));
    }

    struct IllegalHarness {
        c: DefaultMQPushConsumer,
        key: String,
        mq: MessageQueue,
    }

    impl IllegalHarness {
        /// `assigned=false` 模拟「队列已经不在本实例名下」；`epoch` 非 0 时预置代号。
        fn new(assigned: bool, epoch: u64) -> IllegalHarness {
            let cfg = ConsumerConfig {
                consumer_group: "G".to_string(),
                message_model: MessageModel::CLUSTERING.to_string(),
                ..Default::default()
            };
            let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
            let mq = queue("T", "broker-a", 0);
            let key = mq_key(&mq);
            if assigned {
                let mut state = lock(&c.inner.state);
                state.mq_map.insert(key.clone(), mq.clone());
                state.queue_owners.insert(key.clone(), 1);
                state.offset_table.insert(key.clone(), 3);
                state.consume_offsets.insert(key.clone(), 3);
                state.last_pull_at.insert(key.clone(), 1_700_000_000_000);
                if epoch != 0 {
                    state.queue_epoch.insert(key.clone(), epoch);
                }
            }
            IllegalHarness { c, key, mq }
        }

        /// 模拟拉取回调里 `OFFSET_ILLEGAL` 的锁内动作（游标 + 冻结 + 修正值）。
        fn freeze(&self, off: i64) {
            let mut state = lock(&self.c.inner.state);
            state.offset_table.insert(self.key.clone(), off);
            state.frozen_offsets.insert(self.key.clone());
            state.consume_offsets.insert(self.key.clone(), off);
        }

        /// `handle_offset_illegal` 里的撤队列那一半（锁内），返回要落盘的对象。
        fn retire(&self) -> Vec<(MessageQueue, Option<i64>)> {
            let mut revoked = Vec::new();
            let mut state = lock(&self.c.inner.state);
            DefaultMQPushConsumer::retire_queue_locked(
                &mut state,
                &self.key,
                None,
                &mut revoked,
                false,
            );
            revoked
        }

        fn offset(&self) -> Option<i64> {
            lock(&self.c.inner.state)
                .consume_offsets
                .get(&self.key)
                .copied()
        }

        fn epoch(&self) -> u64 {
            lock(&self.c.inner.state)
                .queue_epoch
                .get(&self.key)
                .copied()
                .unwrap_or(0)
        }

        fn frozen(&self) -> bool {
            lock(&self.c.inner.state).frozen_offsets.contains(&self.key)
        }

        fn ack(&self, offsets: &[i64], epoch: u64) {
            ack_batch(&self.c, &self.key, offsets, epoch);
        }
    }

    /// 数 listener 被调了几次（被丢弃的批次连 listener 都不该进）。
    struct CountingListener {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MessageListenerConcurrently for CountingListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    #[test]
    fn retire_drops_every_local_trace_and_hands_the_offset_to_persist() {
        let h = IllegalHarness::new(true, 0);
        lock(&h.c.inner.state)
            .pending
            .insert(h.key.clone(), VecDeque::from(offset_batch(2)));
        h.freeze(0);
        let revoked = h.retire();

        // 已消费位点交给调用方去落盘（`handle_offset_illegal` 里就是 `revoke_queues`）
        assert_eq!(revoked.len(), 1);
        assert_eq!(revoked[0].0, h.mq);
        assert_eq!(revoked[0].1, Some(0), "落盘的是纠错后的冻结位点");

        let state = lock(&h.c.inner.state);
        assert!(!state.pending.contains_key(&h.key), "缓冲整批作废");
        assert!(!state.offset_table.contains_key(&h.key));
        assert!(!state.consume_offsets.contains_key(&h.key));
        assert!(!state.mq_map.contains_key(&h.key));
        assert!(!state.queue_owners.contains_key(&h.key));
        assert!(
            !state.last_pull_at.contains_key(&h.key),
            "同名队列复用时不能继承旧时刻"
        );
        assert_eq!(
            state.queue_epoch.get(&h.key).copied(),
            Some(1),
            "代号 +1（setDropped）"
        );
        assert!(state.frozen_offsets.contains(&h.key), "冻结要留到队列重建");
    }

    #[test]
    fn retire_without_a_queue_is_a_noop() {
        // 队列已经不在本实例名下（并发撤销）：没有 mq 就没有可落盘的对象，不能凭空造一条
        let h = IllegalHarness::new(false, 0);
        assert!(h.retire().is_empty());
        assert_eq!(
            h.epoch(),
            1,
            "撤队列照样清状态 + 代号前进（重建是调用方的下一步）"
        );
    }

    #[test]
    fn retire_twice_keeps_the_epoch_monotonic() {
        let h = IllegalHarness::new(true, 0);
        h.retire();
        assert_eq!(h.epoch(), 1);
        h.retire();
        assert_eq!(
            h.epoch(),
            2,
            "重建前又出一次非法：代号继续前进，旧批次依旧作废"
        );
    }

    #[tokio::test]
    async fn handle_offset_illegal_retires_and_wakes_the_rebalance_loop() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        assert!(!h.c.inner.rebalance_now.load(Ordering::SeqCst));
        handle_offset_illegal(&h.c.inner, &h.key).await;
        assert_eq!(h.epoch(), 1);
        assert!(!lock(&h.c.inner.state).mq_map.contains_key(&h.key));
        assert!(
            h.c.inner.rebalance_now.load(Ordering::SeqCst),
            "removeProcessQueue 之后要靠 rebalanceImmediately 重建这条队列"
        );
    }

    #[test]
    fn frozen_offset_ignores_ack() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        h.ack(&[0, 1, 2], 0);
        assert_eq!(h.offset(), Some(0));
    }

    #[test]
    fn frozen_offset_ignores_correct_tags_offset() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        {
            let mut state = lock(&h.c.inner.state);
            correct_tags_offset_locked(&mut state, &h.key, PullStatus::NoNewMsg, 110);
        }
        assert_eq!(h.offset(), Some(0));
    }

    #[test]
    fn freeze_survives_the_retire_until_the_queue_is_rebuilt() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        h.retire();
        assert!(h.frozen());
    }

    #[tokio::test]
    async fn rebuild_clears_the_freeze() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        h.retire();
        // 重建：把队列重新划给自己（`rebalance_pull_threads` 的建分支）
        lock(&h.c.inner.state).assigned = vec![h.mq.clone()];
        h.c.rebalance_pull_threads().await;
        assert!(
            !h.frozen(),
            "新 ProcessQueue 就位就该解冻，否则队列从此只拉不 ack"
        );
        // 重建后 ack 恢复正常：只有**新代号**的批次算数，旧代号的依旧作废
        h.ack(&[0, 1], 0);
        assert_eq!(h.offset(), None, "旧代号的 ack 还是不许动位点");
        h.ack(&[0, 1], h.epoch());
        assert_eq!(h.offset(), Some(2));
    }

    #[tokio::test]
    async fn stale_epoch_batch_is_not_consumed() {
        // 队列被撤销/重建（代号 0 → 1）后，排队期间被丢弃的批次不能再消费
        let h = IllegalHarness::new(true, 1);
        let listener = Arc::new(CountingListener {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });
        h.c.set_message_listener_concurrently(listener.clone());
        let done = consume_batch(&h.c.inner, &h.key, &h.mq, offset_batch(2), Some(0))
            .await
            .expect("consume_batch must not error");
        assert!(!done);
        assert_eq!(
            listener.calls.load(Ordering::SeqCst),
            0,
            "连 listener 都不该进"
        );
        assert_eq!(h.offset(), Some(3), "旧批次不动位点");
    }

    #[tokio::test]
    async fn current_epoch_batch_is_consumed() {
        let h = IllegalHarness::new(true, 1);
        let listener = Arc::new(CountingListener {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });
        h.c.set_message_listener_concurrently(listener.clone());
        lock(&h.c.inner.state)
            .consume_offsets
            .insert(h.key.clone(), 0);
        let done = consume_batch(&h.c.inner, &h.key, &h.mq, offset_batch(2), Some(1))
            .await
            .expect("consume_batch must not error");
        assert!(done);
        assert_eq!(listener.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.offset(), Some(2));
    }

    #[test]
    fn stale_epoch_ack_is_dropped_and_current_epoch_advances() {
        let h = IllegalHarness::new(true, 1);
        lock(&h.c.inner.state)
            .consume_offsets
            .insert(h.key.clone(), 0);
        h.ack(&[0, 1, 2], 0);
        assert_eq!(h.offset(), Some(0), "旧代号的 ack 不能动位点");
        h.ack(&[0, 1, 2], 1);
        assert_eq!(h.offset(), Some(3));
    }

    #[test]
    fn illegal_recovery_does_not_touch_sibling_queues() {
        // 同实例的另一条队列不受牵连：撤队列/冻结必须按 key 隔离
        let h = IllegalHarness::new(true, 0);
        let other = queue("T", "broker-a", 1);
        let other_key = mq_key(&other);
        {
            let mut state = lock(&h.c.inner.state);
            state.mq_map.insert(other_key.clone(), other.clone());
            state.queue_owners.insert(other_key.clone(), 2);
            state.consume_offsets.insert(other_key.clone(), 7);
            state
                .last_pull_at
                .insert(other_key.clone(), 1_700_000_000_000);
        }
        h.freeze(0);
        h.retire();
        let state = lock(&h.c.inner.state);
        assert!(state.mq_map.contains_key(&other_key));
        assert!(state.consume_offsets.contains_key(&other_key));
        assert!(state.queue_owners.contains_key(&other_key));
        assert_eq!(
            state.last_pull_at.get(&other_key).copied(),
            Some(1_700_000_000_000)
        );
        assert_eq!(state.queue_epoch.get(&other_key).copied().unwrap_or(0), 0);
        assert!(!state.frozen_offsets.contains(&other_key));
    }

    // ---------------- 220 RESET_CONSUMER_CLIENT_OFFSET 的处理 ----------------
    //
    //
    // 只把新位点写进内存表的做法是**静默**失效的：重置前取回、重置后才返回的批次一 ack
    // 就把位点推回原处，broker 上刚写下的新位点被盖掉，消费从重置前的位置继续 —— 而
    // 222 的响应里那一组 offsetTable 看着完全正常。撤销 + 代号（setDropped）才是判据。
    // 真机链路见 `examples/live_reset_offset.rs`（含 broker 端位点的回读）。

    struct ResetHarness {
        c: Arc<DefaultMQPushConsumer>,
        key: String,
        mq: MessageQueue,
    }

    impl ResetHarness {
        fn new(model: &str) -> ResetHarness {
            let cfg = ConsumerConfig {
                consumer_group: "G".to_string(),
                message_model: model.to_string(),
                ..Default::default()
            };
            let c = Arc::new(DefaultMQPushConsumer::with_config(cfg).unwrap());
            let mq = queue("T", "broker-a", 0);
            let key = mq_key(&mq);
            {
                let mut state = lock(&c.inner.state);
                state.mq_map.insert(key.clone(), mq.clone());
                state.queue_owners.insert(key.clone(), 1);
                state.offset_table.insert(key.clone(), 7);
                state.consume_offsets.insert(key.clone(), 7);
                state
                    .pending
                    .insert(key.clone(), VecDeque::from(offset_batch(2)));
                state.last_pull_at.insert(key.clone(), 1_700_000_000_000);
            }
            ResetHarness { c, key, mq }
        }

        async fn reset(&self, off: i64) {
            self.c
                .clone()
                .reset_offset("T".to_string(), vec![(self.mq.clone(), off)])
                .await
                .expect("reset_offset must not error");
        }

        fn offset(&self) -> Option<i64> {
            lock(&self.c.inner.state)
                .consume_offsets
                .get(&self.key)
                .copied()
        }

        fn epoch(&self) -> u64 {
            lock(&self.c.inner.state)
                .queue_epoch
                .get(&self.key)
                .copied()
                .unwrap_or(0)
        }
    }

    /// 未 start() 的实例上，落盘与 rebalance 都没有 client 可发（与
    /// `handle_offset_illegal` 的离线测试同一约束），所以这里锁的是**状态迁移**：
    /// 队列被撤（代号 +1）、表项清空、旧代号的 ack 不再生效。
    #[tokio::test]
    async fn reset_offset_retires_the_queue_and_drops_the_pre_reset_batch() {
        let h = ResetHarness::new(MessageModel::CLUSTERING);
        h.reset(0).await;
        assert_eq!(h.epoch(), 1, "队列被撤，代号 +1");
        assert_eq!(h.offset(), None, "位点交给落盘");
        {
            let state = lock(&h.c.inner.state);
            assert!(!state.mq_map.contains_key(&h.key));
            assert!(!state.pending.contains_key(&h.key), "缓冲整批作废");
            assert!(!state.offset_table.contains_key(&h.key));
            assert!(!state.queue_owners.contains_key(&h.key));
        }
        // 重置前取回、重置后才返回的批次（代号 0）：ack 必须被丢弃，位点不能复活
        ack_batch(&h.c, &h.key, &[0, 1], 0);
        assert_eq!(h.offset(), None, "旧批次的 ack 已被代号挡住");
        // 重建后的队列（代号 1）ack 照常
        ack_batch(&h.c, &h.key, &[4, 5], 1);
        assert_eq!(h.offset(), Some(6));
    }

    #[tokio::test]
    async fn reset_offset_only_touches_the_matching_topic_and_table_entries() {
        let h = ResetHarness::new(MessageModel::CLUSTERING);
        let other = queue("T", "broker-a", 1);
        let other_key = mq_key(&other);
        {
            let mut state = lock(&h.c.inner.state);
            state.mq_map.insert(other_key.clone(), other.clone());
            state.queue_owners.insert(other_key.clone(), 2);
            state.offset_table.insert(other_key.clone(), 9);
            state.consume_offsets.insert(other_key.clone(), 9);
            state
                .last_pull_at
                .insert(other_key.clone(), 1_700_000_000_000);
        }
        // offsetTable 只含 0 号队列：同 topic 的 1 号队列不受牵连
        h.reset(0).await;
        {
            let state = lock(&h.c.inner.state);
            assert!(state.mq_map.contains_key(&other_key));
            assert!(
                !state.pending.contains_key(&other_key),
                "1 号队列本来就没有缓冲"
            );
            assert_eq!(state.consume_offsets.get(&other_key).copied(), Some(9));
            assert_eq!(state.queue_epoch.get(&other_key).copied().unwrap_or(0), 0);
            assert_eq!(state.queue_owners.get(&other_key).copied(), Some(2));
        }
        // 表里没有本实例持有的队列（别的实例/别的 broker）：不动任何状态
        h.c.clone()
            .reset_offset("T".to_string(), vec![(queue("T", "broker-b", 0), 0)])
            .await
            .expect("reset_offset must not error");
        assert_eq!(h.epoch(), 1, "上一笔的代号不回退");
        assert_eq!(h.offset(), None);
        assert_eq!(
            lock(&h.c.inner.state)
                .consume_offsets
                .get(&other_key)
                .copied(),
            Some(9)
        );
    }

    /// 广播撤销要把撤下来的位点连同队列一起并进本地点位文件：只并位点不够，
    /// `build_local_offsets_json` 在 `mq_map` 里查不到队列会**静默跳过**该条目。
    #[test]
    fn retired_offsets_are_merged_into_the_local_snapshot() {
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);

        let mut items = BTreeMap::new();
        let mut mq_map = BTreeMap::new();
        merge_retired_offsets(&mut items, &mut mq_map, &[(mq.clone(), Some(3))]);
        assert_eq!(items.get(&key), Some(&3));
        assert_eq!(mq_map.get(&key), Some(&mq));
        // 真的能读回来（对照：只并 items 不并 mq_map，构建器会把这条跳过）
        let text = build_local_offsets_json(&items, &mq_map);
        assert_eq!(parse_local_offsets_text(&text).unwrap().get(&key), Some(&3));

        // 从未消费过（撤销时位点是 None）的队列不能凭空造条目
        let mut items = BTreeMap::new();
        let mut mq_map = BTreeMap::new();
        merge_retired_offsets(&mut items, &mut mq_map, &[(mq.clone(), None)]);
        assert!(items.is_empty() && mq_map.is_empty());

        // 快照里已有该队列（同一批撤多把/残留）时按撤销值覆盖
        let mut items = BTreeMap::from([(key.clone(), 1)]);
        let mut mq_map = BTreeMap::from([(key.clone(), mq.clone())]);
        merge_retired_offsets(&mut items, &mut mq_map, &[(mq.clone(), Some(9))]);
        assert_eq!(items.get(&key), Some(&9));
    }

    // ---------------- 轨迹自动桥接 ----------------

    #[test]
    fn trace_host_reports_empty_client_id_before_start() {
        let c = DefaultMQPushConsumer::new("GID_trace_host").expect("合法组名不该构造失败");
        // Python `_client_id`：宿主没有 _mq_client 时返回空串
        assert_eq!(TraceHost::client_id(&c), "");
    }

    // ================================================================
    // POP 模式 broker 侧分配（QUERY_ASSIGNMENT(400)）—— 进程内假集群
    // ================================================================

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    use crate::common::sysflag::PermName;
    use crate::remoting::protocol::codes::request_code;
    use crate::remoting::protocol::remoting_command::RemotingCommand;
    use crate::remoting::protocol::route::{BrokerData, QueueData, TopicRouteData};

    /// 400 应答的脚本：`(code, body)` 二值组合加一个「不答直接断连」档。
    #[derive(Clone)]
    struct AssignFixture {
        /// 响应码（`response_code::SUCCESS` / `SYSTEM_ERROR` / `TOPIC_NOT_EXIST`…）。
        code: i32,
        body: Option<Vec<u8>>,
        /// true = 收到请求后**不回话直接关闭连接**：客户端拿到传输层错误
        /// （response_code 为 None 的 Err），走「保留上一次分配」分支。
        silent_close: bool,
    }

    impl AssignFixture {
        /// SUCCESS + 指定分配数组（空 Vec 编出显式 `[]` = 「清除全部分配」）。
        fn with_assignments(items: &[(&str, i32, &str)]) -> AssignFixture {
            AssignFixture {
                code: response_code::SUCCESS,
                body: Some(assignment_body(items)),
                silent_close: false,
            }
        }
        fn null_set() -> AssignFixture {
            AssignFixture {
                code: response_code::SUCCESS,
                body: None,
                silent_close: false,
            }
        }
        fn with_code(code: i32) -> AssignFixture {
            AssignFixture {
                code,
                body: None,
                silent_close: false,
            }
        }
        fn drop_connection() -> AssignFixture {
            AssignFixture {
                code: response_code::SUCCESS,
                body: None,
                silent_close: true,
            }
        }
    }

    /// 编 400 响应 body（走真实编码器，保证测试与线上报文同一形状）。
    fn assignment_body(items: &[(&str, i32, &str)]) -> Vec<u8> {
        use crate::remoting::protocol::query_assignment_body::MessageQueueAssignment;
        let list: Vec<MessageQueueAssignment> = items
            .iter()
            .map(|(broker, id, mode)| {
                MessageQueueAssignment::new(MessageQueueKey::new("T", *broker, *id), mode)
            })
            .collect();
        crate::remoting::protocol::query_assignment_body::QueryAssignmentResponseBody::encode(
            Some(&list),
        )
    }

    struct AssignState {
        /// None = 默认剧本：SUCCESS，broker-a 的 0/1 两条 POP 队列。
        fixture: Mutex<Option<AssignFixture>>,
        /// 捕获到的 QUERY_ASSIGNMENT 请求体（断言 400 的 body 字段就看这里）。
        bodies: Mutex<Vec<Vec<u8>>>,
    }

    /// 1 namesrv（任何 topic 都回同一份路由：broker-a，4 条读队列）
    /// + 1 broker（QUERY_ASSIGNMENT 按脚本答，POP 回 POLLING 空转，其余 SUCCESS）。
    struct AssignCluster {
        namesrv_addr: String,
        state: Arc<AssignState>,
    }

    const ASSIGN_MAX_FRAME: i32 = 20 * 1024 * 1024;

    impl AssignCluster {
        async fn start() -> AssignCluster {
            let broker = TcpListener::bind("127.0.0.1:0").await.expect("bind mock broker");
            let broker_addr = broker.local_addr().expect("mock broker addr").to_string();
            let namesrv = TcpListener::bind("127.0.0.1:0").await.expect("bind mock namesrv");
            let namesrv_addr = namesrv.local_addr().expect("mock namesrv addr").to_string();
            let state = Arc::new(AssignState {
                fixture: Mutex::new(None),
                bodies: Mutex::new(Vec::new()),
            });
            let _namesrv_task = spawn_assign_namesrv(namesrv, broker_addr);
            let _broker_task = spawn_assign_broker(broker, state.clone());
            AssignCluster {
                namesrv_addr,
                state,
            }
        }

        fn set(&self, fixture: Option<AssignFixture>) {
            *lock(&self.state.fixture) = fixture;
        }

        /// 假 broker 收到过的全部 400 请求体（解码后）。
        fn requests(&self) -> Vec<crate::remoting::protocol::query_assignment_body::QueryAssignmentRequestBody> {
            let raw = lock(&self.state.bodies).clone();
            raw.iter()
                .map(|b| {
                    crate::remoting::protocol::query_assignment_body::QueryAssignmentRequestBody::decode(b)
                        .expect("请求体必须是合法 JSON")
                })
                .collect()
        }
    }

    async fn read_assign_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
        let mut len_buf = [0_u8; 4];
        stream.read_exact(&mut len_buf).await.ok()?;
        let total = i32::from_be_bytes(len_buf);
        if total <= 0 || total > ASSIGN_MAX_FRAME {
            return None;
        }
        let mut frame = vec![0_u8; 4 + total as usize];
        frame[..4].copy_from_slice(&len_buf);
        stream.read_exact(&mut frame[4..]).await.ok()?;
        Some(frame)
    }

    async fn write_assign_frame(stream: &mut TcpStream, response: &mut RemotingCommand) {
        let bytes = response.encode();
        let _ = stream.write_all(&bytes).await;
        let _ = stream.flush().await;
    }

    fn assign_response_for(request: &RemotingCommand, code: i32) -> RemotingCommand {
        let mut response = RemotingCommand::create_response(code, Some("mock".to_string()));
        response.opaque = request.opaque;
        response.serialize_type_current_rpc = request.serialize_type_current_rpc;
        response
    }

    /// 路由：broker-a（假 broker 地址）上 4 条读队列 —— 分配只给其中 2 条时，
    /// 「退化成全要」会立刻在断言里露出来。
    fn assign_route_json(broker_addr: &str) -> Vec<u8> {
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                "broker-a".to_string(),
                4,
                4,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "MockCluster",
                "broker-a".to_string(),
                vec![(i64::from(MixAll::MASTER_ID), broker_addr.to_string())],
                "",
            )],
            ..Default::default()
        };
        serde_json::to_vec(&route.to_json_value()).expect("路由可序列化")
    }

    fn spawn_assign_namesrv(listener: TcpListener, broker_addr: String) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let addr = broker_addr.clone();
                tokio::spawn(async move {
                    while let Some(frame) = read_assign_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else { return };
                        let mut response =
                            if request.code == request_code::GET_ROUTEINFO_BY_TOPIC {
                                let mut r = assign_response_for(&request, response_code::SUCCESS);
                                r.set_body(Some(assign_route_json(&addr)));
                                r
                            } else {
                                assign_response_for(&request, response_code::SUCCESS)
                            };
                        write_assign_frame(&mut stream, &mut response).await;
                    }
                });
            }
        })
    }

    fn spawn_assign_broker(listener: TcpListener, state: Arc<AssignState>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    while let Some(frame) = read_assign_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else { return };
                        if request.code == request_code::QUERY_ASSIGNMENT {
                            state.bodies.lock().unwrap_or_else(|e| e.into_inner()).push(
                                request.body().unwrap_or(&[]).to_vec(),
                            );
                            let fixture = lock(&state.fixture).clone();
                            match fixture {
                                Some(f) if f.silent_close => return, // 不回话，直接拆连接
                                Some(f) => {
                                    let mut response = assign_response_for(&request, f.code);
                                    if let Some(body) = f.body {
                                        response.set_body(Some(body));
                                    }
                                    write_assign_frame(&mut stream, &mut response).await;
                                }
                                None => {
                                    // 默认剧本：broker-a 的 0/1 两条 POP 队列
                                    let mut response = assign_response_for(
                                        &request,
                                        response_code::SUCCESS,
                                    );
                                    response.set_body(Some(assignment_body(&[
                                        ("broker-a", 0, "POP"),
                                        ("broker-a", 1, "POP"),
                                    ])));
                                    write_assign_frame(&mut stream, &mut response).await;
                                }
                            }
                            continue;
                        }
                        if request.code == request_code::POP_MESSAGE {
                            // 起环的 POP 循环在这里空转（started=false 时循环其实会立刻
                            // 退出，这层延迟只是给「误起环」的形态兜底，不让它打满 CPU）。
                            tokio::time::sleep(Duration::from_millis(150)).await;
                            let mut response =
                                assign_response_for(&request, response_code::PULL_NOT_FOUND);
                            write_assign_frame(&mut stream, &mut response).await;
                            continue;
                        }
                        let mut response = assign_response_for(&request, response_code::SUCCESS);
                        if request.code == request_code::QUERY_CONSUMER_OFFSET
                            || request.code == request_code::GET_MAX_OFFSET
                        {
                            response.add_ext_field("offset", "0");
                        }
                        write_assign_frame(&mut stream, &mut response).await;
                    }
                });
            }
        })
    }

    /// 造一个「POP 模式 + 已挂上指向假集群的实例」的消费者（不起 start，
    /// 不 spawn 后台循环，分配完全由手动 do_rebalance 驱动）。
    fn pop_consumer_on_cluster(instance: &str, cluster: &AssignCluster) -> DefaultMQPushConsumer {
        let c = DefaultMQPushConsumer::new("GID_pop_assign").expect("合法组名");
        c.subscribe("T", "*").expect("订阅");
        c.set_message_listener_concurrently(Arc::new(NoopListener));
        c.update_config(|cfg| {
            cfg.pop_mode = true;
            cfg.name_server_addrs = vec![cluster.namesrv_addr.clone()];
        });
        let client = MQClientInstance::new(
            &format!("cid-{instance}"),
            vec![cluster.namesrv_addr.clone()],
        );
        *lock(&c.inner.client) = Some(client);
        // require_client 认这个标志；后台循环任务（rebalance/heartbeat/lock/persist）
        // 只有 start() 才 spawn，所以这里不会打乱手动驱动的节拍。
        c.inner.started.store(true, Ordering::Release);
        c
    }

    fn sorted_keys(c: &DefaultMQPushConsumer) -> Vec<String> {
        let mut keys = c.assigned_queue_keys();
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn pop_assignment_comes_from_broker_query_assignment() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-ok", &cluster);

        c.do_rebalance().await.expect("rebalance 不报错");

        assert_eq!(
            sorted_keys(&c),
            vec!["Tbroker-a0".to_string(), "Tbroker-a1".to_string()],
            "分配 = broker 给的 0/1，而不是路由里的全部 0..3"
        );
        let reqs = cluster.requests();
        assert_eq!(reqs.len(), 1, "每 topic 一笔 400");
        assert_eq!(reqs[0].topic, "T");
        assert_eq!(reqs[0].consumer_group, "GID_pop_assign");
        assert_eq!(reqs[0].client_id, "cid-pop-qa-ok", "clientId 用实例的");
        assert_eq!(reqs[0].strategy_name, "AVG", "策略名随 body 上送，broker 按它选策略");
        assert_eq!(reqs[0].message_model, "CLUSTERING");
        // POP 模式不再发 GET_CONSUMER_LIST_BY_GROUP：队列集完全由 400 决定
    }

    /// 显式空数组 = broker 说「把你之前的负载全收走」：旧分配必须被清掉。
    #[tokio::test]
    async fn pop_empty_assignment_clears_previous_queues() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-empty", &cluster);

        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c).len(), 2, "先拿到默认分配");

        cluster.set(Some(AssignFixture::with_assignments(&[])));
        c.do_rebalance().await.unwrap();
        assert!(
            sorted_keys(&c).is_empty(),
            "空集是明确的清除指令，不是「保留」"
        );
    }

    /// TOPIC_NOT_EXIST(17)：topic 在 broker 侧已消失 → 回空集收敛（本端口硬规则），
    /// 不能让 POP 循环抱着注定撞墙的队列。
    #[tokio::test]
    async fn pop_topic_not_exist_clears_assignment() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-gone", &cluster);

        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c).len(), 2);

        cluster.set(Some(AssignFixture::with_code(
            response_code::TOPIC_NOT_EXIST,
        )));
        c.do_rebalance().await.unwrap();
        assert!(
            sorted_keys(&c).is_empty(),
            "17 = topic 没了，分配必须清空而不是抱着旧队列"
        );
    }

    #[tokio::test]
    async fn pop_assignment_failure_keeps_previous_never_all() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-keep", &cluster);

        c.do_rebalance().await.unwrap();
        let before = sorted_keys(&c);
        assert_eq!(before, vec!["Tbroker-a0".to_string(), "Tbroker-a1".to_string()]);

        // ① broker 错误码
        cluster.set(Some(AssignFixture::with_code(response_code::SYSTEM_ERROR)));
        c.do_rebalance().await.unwrap();
        assert_eq!(
            sorted_keys(&c),
            before,
            "非 SUCCESS（除 17）= 保留上一次分配；路由里有 0..3，退化成全要即为回归"
        );

        // ② SUCCESS + null 集
        cluster.set(Some(AssignFixture::null_set()));
        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c), before, "null means invalid result, skip the update");

        // ③ 传输层失败：400 收到请求后直接断连（3s RPC 超时预算内必然返回错误）
        cluster.set(Some(AssignFixture::drop_connection()));
        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c), before, "RPC 打不出去同样只能保留");
    }

    /// 分配变更感知：下一轮 rebalance（周期循环每 tick 调的就是它）拿到不同的集就
    /// 立即切换 —— 旧的撤、新的上。
    #[tokio::test]
    async fn pop_assignment_change_is_applied_on_next_rebalance_round() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-change", &cluster);

        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c), vec!["Tbroker-a0", "Tbroker-a1"]);

        // broker 侧把这条队列挪走/换掉（POP 重平衡的真实形态）
        cluster.set(Some(AssignFixture::with_assignments(&[
            ("broker-a", 2, "POP"),
            ("broker-a", 3, "POP"),
        ])));
        c.do_rebalance().await.unwrap();
        assert_eq!(
            sorted_keys(&c),
            vec!["Tbroker-a2", "Tbroker-a3"],
            "第二轮 rebalance 必须感知到集合并切换"
        );
        assert_eq!(cluster.requests().len(), 2, "每轮 topic 各一笔 400");
    }

    #[tokio::test]
    async fn pop_assignment_dedups_and_survives_mixed_modes() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-dedup", &cluster);
        let body = crate::remoting::protocol::query_assignment_body::QueryAssignmentResponseBody::encode(
            Some(&{
                let mut items = assignment_items(&[("broker-a", 0, "POP")]);
                items.extend(assignment_items(&[("broker-a", 0, "POP")])); // 重复项
                items.push(crate::remoting::protocol::query_assignment_body::MessageQueueAssignment {
                    message_queue: None,
                    mode: "POP".to_string(),
                });
                items.push(crate::remoting::protocol::query_assignment_body::MessageQueueAssignment {
                    message_queue: Some(MessageQueueKey::new("T", "broker-a", 1)),
                    mode: "PULL".to_string(), // 混合模式：只观察
                });
                items
            }),
        );
        cluster.set(Some(AssignFixture {
            code: response_code::SUCCESS,
            body: Some(body),
            silent_close: false,
        }));

        c.do_rebalance().await.unwrap();
        assert_eq!(
            sorted_keys(&c),
            vec!["Tbroker-a0", "Tbroker-a1"],
            "重复的 (broker-a,0) 去重、null 队列被跳过、PULL 项照常进集合"
        );
    }

    fn assignment_items(
        items: &[(&str, i32, &str)],
    ) -> Vec<crate::remoting::protocol::query_assignment_body::MessageQueueAssignment> {
        items
            .iter()
            .map(|(broker, id, mode)| {
                crate::remoting::protocol::query_assignment_body::MessageQueueAssignment::new(
                    MessageQueueKey::new("T", *broker, *id),
                    mode,
                )
            })
            .collect()
    }
}
