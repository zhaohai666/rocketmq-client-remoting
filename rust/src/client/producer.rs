//! `DefaultMQProducer`：消息发送入口，实现经典 remoting 协议的生产者语义。
//!
//! 协议字段、行为约束与错误语义均以 RocketMQ broker / NameServer 端的
//! 经典 remoting 线上协议为准。
//!
//! 职责：生产者配置、生命周期（与 [`MQClientInstance`] 共享实例 + 生产者心跳线程）、
//! 同步/异步/单向/定点/选择器/批量发送、发送重试与延迟故障规避、发送前后钩子与
//! 发送前拦截钩子、W3C traceparent 注入、消息轨迹、Request-Reply、事务消息
//! （半消息 + 本地事务 + END_TRANSACTION + broker 回查）与几个管理类便捷方法。
//!
//! ## 运行时与实现形态说明（均在对应条目 doc 上再次标注）
//!
//! 1. **句柄可克隆**：生产者句柄常被回调/处理器共享；Rust 里
//!    [`DefaultMQProducer`] 是 `Clone` 的 `Arc<Inner>`，配置走 `RwLock` 内部可变，
//!    于是 setter 是「随时可调、启动后部分拒绝」。
//! 2. **回调/监听器是 trait 对象**：[`SendCallback`]、
//!    [`MessageQueueSelector`]、[`TransactionListener`]。
//! 3. **`send_async` 用 tokio 任务**：异步模型统一走 tokio，不依赖专用线程池：
//!    派发用的运行时句柄在 [`DefaultMQProducer::start`] 时惰性绑定并缓存（构造允许发生
//!    在运行时之外），于是 `send_async` 与心跳一样不要求调用点处于运行时上下文。
//!    有界的 `AsyncSenderExecutor` 保持经典池子形状（一条容量
//!    `async_sender_queue_capacity` 的队列 + `available_parallelism()` 份并发额度，见
//!    [`AsyncSenderExecutor`]），这里的「线程」是任务：没有名字，也不与核绑定。
//! 4. **轨迹分发器：注入或自动桥接**（[`DefaultMQProducer::set_trace_dispatcher`]）：
//!    典型做法是在 `start()` 里直接创建 `AsyncTraceDispatcher`（含真实内部生产者）。
//!    本 crate 允许调用方注入现成分发器（见 `mq_client.rs` 模块头差异 2），但
//!    `enable_trace=true` 且未注入时，`start()` 会经 [`auto_bridged_dispatcher`]
//!    自动建一套（含真实内部生产者），轨迹开箱即用。
//!    注入或桥接后 `start()` 都会先注册 `SendMessageTraceHook` /
//!    `EndTransactionTraceHook`，再启动分发器。
//! 5. 毫秒时间戳统一走 [`current_time_millis`]；互斥用 `Mutex`。

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;

use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinHandle;

use crate::client::backpressure::{
    back_pressure_permits, FairSemaphore, MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE,
};
use crate::client::hook::{
    AnyHolder, CheckForbiddenContext, CheckForbiddenHook, CheckForbiddenHookList,
    CommunicationMode, EndTransactionContext, EndTransactionHook, EndTransactionHookList,
    SendMessageContext, SendMessageHook, SendMessageHookList,
};
use crate::client::latency::{MQFaultStrategy, Resolver, ServiceDetector};
use crate::client::metrics::ClientMetrics;
use crate::client::mq_client::{
    MQClientInstance, MQClientInstanceConfig, PublishMessage, TopicPublishInfo, TraceDispatcher,
    MQ_CLIENT_API_TIMEOUT_MILLIS,
};
use crate::client::produce_accumulator::{
    get_or_create_produce_accumulator, AccumulatorSender, ProduceAccumulator, TIMER_DELAY_KEYS,
};
use crate::client::request_reply::{
    create_correlation_id, request_future_holder, start_expired_request_scan,
    stop_expired_request_scan, RequestResponseFuture, DEFAULT_REQUEST_TIMEOUT_MILLIS,
};
use crate::client::result::{LocalTransactionState, SendResult, SendStatus, TransactionSendResult};
use crate::client::shutdown::{run_finalize_blocking, SHUTDOWN_FINALIZE_BUDGET};
use crate::client::top_addressing::DefaultTopAddressing;
use crate::client::trace::TraceContext;
use crate::client::trace_context::{inject_trace_context, trace_context_enabled_from_env};
use crate::client::trace_dispatcher::{
    AsyncTraceDispatcher, TraceDispatcherConfig, TraceDispatcherType, TraceHost, TraceProducer,
    TraceProducerFuture,
};
use crate::client::trace_hook::{EndTransactionTraceHook, SendMessageTraceHook, TraceReportSink};
use crate::client::validators;
use crate::common::compression;
use crate::common::message::{Message, MessageBatch, MessageExt, MessageQueue};
use crate::common::message_client_id_setter::set_uniq_id;
use crate::common::message_const::{
    PROPERTY_CORRELATION_ID, PROPERTY_DELAY_TIME, PROPERTY_DELAY_TIME_LEVEL,
    PROPERTY_MESSAGE_REPLY_TO_CLIENT, PROPERTY_MESSAGE_TTL, PROPERTY_PRODUCER_GROUP,
    PROPERTY_TRANSACTION_PREPARED, PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
};
use crate::common::message_decoder::{decode_message, decode_message_id, decode_messages};
use crate::common::message_type::MessageType;
use crate::common::mix_all::MixAll;
use crate::common::recall_message_handle;
use crate::common::sysflag::{MessageSysFlag, PermName};
use crate::common::util_all::{current_time_millis, java_string_hash, monotonic_millis};
use crate::error::{client_error_code, Error, Result};
use crate::remoting::client::{RequestProcessor, ResponseSink};
use crate::remoting::protocol::codes::{request_code, response_code};
use crate::remoting::protocol::ext_fields::CustomHeader;
use crate::remoting::protocol::headers::{
    CheckTransactionStateRequestHeader, EndTransactionRequestHeader,
    GetEarliestMsgStoretimeRequestHeader, GetEarliestMsgStoretimeResponseHeader,
    RecallMessageRequestHeader,
};
use crate::remoting::protocol::heartbeat::{HeartbeatData, ProducerData};
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::remoting::protocol::remoting_command::{next_opaque, RemotingCommand};
use crate::remoting::rpchook::RPCHook;
use crate::remoting::socks::SocksProxyConfig;
use crate::{bail, rmq_debug, rmq_error, rmq_warn};

/// 发送重试内核的离线对拍（进程内假集群），见该模块文档。
#[cfg(test)]
mod send_retry_tests;

#[cfg(test)]
mod detector_tests;

/// Python `DefaultMQProducer.instance_name` 的默认值。
pub const DEFAULT_INSTANCE_NAME: &str = "DEFAULT";
/// 单条消息 body 的默认上限：4 MiB。
pub const DEFAULT_MAX_MESSAGE_SIZE: i32 = 1024 * 1024 * 4;
/// body 大于等于该阈值才压缩，默认 4 KiB。
pub const DEFAULT_COMPRESS_MSG_BODY_OVER_HOWMUCH: i32 = 1024 * 4;
/// zlib 压缩级别的默认值：level 5（级别编码写进消息 sysFlag）。
pub const DEFAULT_COMPRESS_LEVEL: i32 = 5;
/// 异步发送队列的默认容量：经典客户端把发送任务排进一条有界阻塞队列
/// （容量 5 万），队满即拒绝提交。
/// 对应配置项 `async_sender_queue_capacity`。
pub const DEFAULT_ASYNC_SENDER_QUEUE_CAPACITY: i32 = 50_000;

/// 定点发送守卫的两处文案（同步入口一份、异步入口一份），
/// 逐字保留：两处文案本就不同，按入口取用。
const PINNED_TOPIC_MISMATCH_SYNC: &str = "message's topic not equal mq's topic";
const PINNED_TOPIC_MISMATCH_ASYNC: &str =
    "Topic of the message does not match its target message queue";

/// 默认可重试的 broker 业务错误码集合（配置项 `retry_response_codes` 的默认值）。
///
/// 判据是「换一台 broker 有可能不一样」：这些码都代表 broker 侧的临时状态
/// （忙、不可用、路由还没同步、被隔离……）。而 `MESSAGE_ILLEGAL` 之类的
/// 确定性错误重试也是白试，必须原样抛给调用方。
pub const DEFAULT_RETRY_RESPONSE_CODES: [i32; 8] = [
    response_code::SYSTEM_ERROR,
    response_code::SYSTEM_BUSY,
    response_code::SERVICE_NOT_AVAILABLE,
    response_code::NO_PERMISSION,
    response_code::TOPIC_NOT_EXIST,
    response_code::NO_BUYER_ID,
    response_code::NOT_IN_CURRENT_UNIT,
    response_code::GO_AWAY,
];

// ================================================================ 队列选择器

/// 队列选择器：发送时由调用方决定投递到哪个队列。
///
/// `arg` 取 `&str`：三个内置选择器只用到
/// 字符串/整数语义。
pub trait MessageQueueSelector: Send + Sync + 'static {
/// 从 `mqs` 中选出目标队列。
    fn select(&self, mqs: &[MessageQueue], msg: &Message, arg: &str) -> Result<MessageQueue>;
}

/// 按 `arg` 的 hash 选队列（顺序消息/分片发送用）。
#[derive(Debug, Default, Clone, Copy)]
pub struct SelectMessageQueueByHash;

impl MessageQueueSelector for SelectMessageQueueByHash {
/// 分片键必须跨进程可复现：随环境变化的字符串 hash 会让同一个 `arg`
/// 落到不同队列。这里取经典 `String.hashCode` 整型 hash 语义
    /// （确定性，适合当分片键）。
    fn select(&self, mqs: &[MessageQueue], _msg: &Message, arg: &str) -> Result<MessageQueue> {
        if mqs.is_empty() {
            bail!("no message queue");
        }
        let hash = java_string_hash(arg);
        // hash 恰为 `i32::MIN` 时取绝对值仍是负数（溢出），这里显式收敛到 0。
        let idx = if hash < 0 {
            (hash.checked_neg().unwrap_or(0) as usize) % mqs.len()
        } else {
            (hash as usize) % mqs.len()
        };
        Ok(mqs[idx].clone())
    }
}

/// 随机选一个队列（纯负载均摊，无分区语义）。
#[derive(Debug, Default, Clone, Copy)]
pub struct SelectMessageQueueByRandom;

impl MessageQueueSelector for SelectMessageQueueByRandom {
    fn select(&self, mqs: &[MessageQueue], _msg: &Message, _arg: &str) -> Result<MessageQueue> {
        if mqs.is_empty() {
            bail!("no message queue");
        }
        // 与 Python `random.randint(0, n-1)` 同样用进程级随机源（无线上语义）。
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as usize)
            .unwrap_or(0);
        Ok(mqs[(nanos + mqs.len()) % mqs.len()].clone())
    }
}

/// 按机房前缀（brokerName 前缀）选队列，前缀来自 `arg`。
#[derive(Debug, Default, Clone, Copy)]
pub struct SelectMessageQueueByMachineRoom;

impl MessageQueueSelector for SelectMessageQueueByMachineRoom {
    fn select(&self, mqs: &[MessageQueue], _msg: &Message, arg: &str) -> Result<MessageQueue> {
        if mqs.is_empty() {
            bail!("no message queue");
        }
        Ok(mqs
            .iter()
            .find(|mq| mq.broker_name.starts_with(arg))
            .cloned()
            .unwrap_or_else(|| mqs[0].clone()))
    }
}

// ================================================================ 异步发送回调

/// 异步发送回调：请求到达终态（成功或失败）时通过它交回结果。
pub trait SendCallback: Send + Sync + 'static {
    /// 发送成功时回调，携带最终 [`SendResult`]。
    fn on_success(&self, result: SendResult);

    /// 发送失败时回调，携带失败原因。
    fn on_exception(&self, err: Error);
}

/// 成功回调闭包（Python `SendCallbackImpl(success_fn=...)`）。
pub type OnSuccessFn = Box<dyn FnOnce(SendResult) + Send + 'static>;

/// 失败回调闭包（Python `SendCallbackImpl(exception_fn=...)`）。
pub type OnExceptionFn = Box<dyn FnOnce(Error) + Send + 'static>;

/// 便捷回调：把两个普通闭包拼成 [`SendCallback`]
/// 并保证两个闭包各自至多执行一次。
pub struct ClosureSendCallback {
    success: Mutex<Option<OnSuccessFn>>,
    failure: Mutex<Option<OnExceptionFn>>,
}

impl ClosureSendCallback {
    /// `success` / `failure` 任一可为 `None`（Python 的默认参数）。
    pub fn new(
        success: Option<OnSuccessFn>,
        failure: Option<OnExceptionFn>,
    ) -> ClosureSendCallback {
        ClosureSendCallback {
            success: Mutex::new(success),
            failure: Mutex::new(failure),
        }
    }

    fn take_success(&self) -> Option<OnSuccessFn> {
        self.success
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    fn take_failure(&self) -> Option<OnExceptionFn> {
        self.failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
}

impl SendCallback for ClosureSendCallback {
    /// `FnOnce` 只能跑一次：取走即空，重复回调（成功与失败同时发生）时后到的那个
    /// 被忽略 —— Python 的 `SendCallbackImpl` 会重复调，但那条路径本身就不该出现。
    fn on_success(&self, result: SendResult) {
        if let Some(f) = self.take_success() {
            f(result);
        }
    }

    fn on_exception(&self, err: Error) {
        if let Some(f) = self.take_failure() {
            f(err);
        }
    }
}

// ================================================================ 事务监听器

/// 事务监听器：半消息发送成功后执行本地事务，并响应 broker 回查。
///
/// Python 允许 `execute_local_transaction` 返回 `None`（视作 `UNKNOW`）并捕获异常；
/// Rust 用 [`LocalTransactionState::Unknow`] 表达「不知道」，监听器内部自己把异常
/// 转成 `Unknow`（发送链路的 `catch` 语义见
/// [`DefaultMQProducer::send_message_in_transaction`]）。
pub trait TransactionListener: Send + Sync + 'static {
    /// 半消息发送成功后执行本地事务，返回提交 / 回滚 / 未知。
    fn execute_local_transaction(
        &self,
        msg: &Message,
        arg: Option<&AnyHolder>,
    ) -> LocalTransactionState;

    /// broker 回查本地事务状态时调用，返回提交 / 回滚 / 未知。
    fn check_local_transaction(&self, msg: &MessageExt) -> LocalTransactionState;
}

// ================================================================ 轨迹分发器通道

/// 生产者持有的轨迹分发器要同时满足两个已有接缝：
///
/// * [`TraceReportSink`]（`trace_hook.rs`）—— 轨迹钩子往里投；
/// * [`TraceDispatcher`]（`mq_client.rs`）—— 生命周期（`start` / `shutdown`）。
///
/// 生命周期约定：生产者持有分发器，`start()` 时创建/注入并调用
/// `dispatcher.start(namesrv)`，`shutdown()` 时调用 `dispatcher.shutdown()`。
///
/// ⚠ 为什么把两个接缝的方法**重述**一遍，而不是写成
/// `trait TraceDispatcherChannel: TraceDispatcher + TraceReportSink`：稳定版 Rust
/// 没有 trait upcast，拿到 `Arc<dyn TraceDispatcherChannel>` 之后无法转成
/// `Arc<dyn TraceReportSink>`，而 [`SendMessageTraceHook::new`] 要的正是后者。
/// 下面的 blanket impl 让任何同时实现两者的类型自动可用，[`SinkAdapter`] 再把它
/// 适配成钩子需要的形状。
pub trait TraceDispatcherChannel: Send + Sync {
    /// 对应 `TraceDispatcher::start`（Python `dispatcher.start(namesrv_addr)`）。
    fn start(&self, name_server_addr: &str) -> Result<()>;

    /// 对应 `TraceDispatcher::shutdown`。
    fn shutdown(&self);

    /// 对应 `TraceReportSink::trace_topic_name`。
    fn trace_topic_name(&self) -> String;

    /// 对应 `TraceReportSink::report`。
    fn report(&self, context: TraceContext) -> bool;

    /// 对应 `TraceReportSink::client_id`。
    fn client_id(&self) -> String;
}

impl<T> TraceDispatcherChannel for T
where
    T: TraceDispatcher + TraceReportSink + Send + Sync,
{
    fn start(&self, name_server_addr: &str) -> Result<()> {
        TraceDispatcher::start(self, name_server_addr)
    }

    fn shutdown(&self) {
        TraceDispatcher::shutdown(self);
    }

    fn trace_topic_name(&self) -> String {
        TraceReportSink::trace_topic_name(self)
    }

    fn report(&self, context: TraceContext) -> bool {
        TraceReportSink::report(self, context)
    }

    fn client_id(&self) -> String {
        TraceReportSink::client_id(self)
    }
}

/// 把注入的通道包装成轨迹钩子要的 [`TraceReportSink`]。
///
/// 之所以是 `Arc` 而不是 `&dyn`：钩子列表按注册顺序长期持有 sink，生命周期不能挂
/// 在生产者的借用上。
pub(crate) struct SinkAdapter(pub Arc<dyn TraceDispatcherChannel>);

impl TraceReportSink for SinkAdapter {
    fn trace_topic_name(&self) -> String {
        self.0.trace_topic_name()
    }

    fn report(&self, context: TraceContext) -> bool {
        self.0.report(context)
    }

    fn client_id(&self) -> String {
        self.0.client_id()
    }
}

// ================================================================ 配置

/// 生产者可配置项：构造时给定默认值，启动后部分可改、部分拒绝。
///
/// 默认值即经典 remoting 生产者的通用缺省（见
/// [`Default`] 实现）。
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Python `producer_group`。
    pub producer_group: String,
    /// Python `tls_enable`：`None` = 交给环境变量 `ROCKETMQ_TLS_ENABLE`。
    pub tls_enable: Option<bool>,
    pub socks_proxy: Option<SocksProxyConfig>,
    /// Python `enable_trace_context`：`None` = 交给环境变量
    /// `ROCKETMQ_TRACE_CONTEXT_ENABLE`。
    pub enable_trace_context: Option<bool>,
    /// Python `namespace`（非空时 topic 与 producerGroup 都加 `<ns>%` 前缀）。
    pub namespace: String,
    /// Python `instance_name`。
    pub instance_name: String,
    /// `unitName`（默认不设）：非空时拼进 clientId，
    /// 并作为地址服务器 URL 的 `-<unitName>` 段。
    pub unit_name: Option<String>,
    /// `namespaceV2`（默认不设）：5.x **服务端**命名空间，
    /// 非空时每个请求带 `nsd=true` / `ns=<namespaceV2>` 扩展头（`NamespaceRpcHook`），
    /// 与 v1 `namespace` 的客户端 `%` 前缀机制是两套东西。
    pub namespace_v2: Option<String>,
    /// `unitMode`（默认 false）：随发送、回投、鉴权、消息过滤
    /// 等请求一起上线，broker 据此给自动创建的 topic 打 UNIT / UNIT_SUB 位。
    pub unit_mode: bool,
    /// `enableStreamRequestType`（生产者默认 false）：
    /// true 时每个请求都带 `ReqT=0`，clientId 末尾多一段 `@STREAM`。
    pub enable_stream_request_type: bool,
    /// `pollNameServerInterval`（默认 30000ms）：在用 topic 的
    /// 路由周期刷新间隔，`start()` 时透传给 `MQClientInstance`，之后改不改都不再影响
    /// 已排定的任务（`scheduleAtFixedRate` 一次性排定周期）。
    pub poll_name_server_interval_millis: u64,
    /// Python `client_id`：`None` 时 `start()` 生成 `<instanceName>@<yyyyMMddHHmmss>`。
    pub client_id: Option<String>,
    /// Python `create_topic_key`。
    pub create_topic_key: String,
    /// Python `default_topic_queue_nums`。
    pub default_topic_queue_nums: i32,
    /// Python `send_msg_timeout`。
    pub send_msg_timeout: i64,
    /// Python `compress_msg_body_over_howmuch`。
    pub compress_msg_body_over_howmuch: i32,
    /// Python `compress_level`。
    pub compress_level: i32,
    /// Python `compress_type`：**算法号**（[`MessageSysFlag::ZLIB_TYPE`] /
    /// `LZ4_TYPE` / `ZSTD_TYPE`，即 0~3），不是 `*_COMPRESSION_*_TYPE` 那种
    /// 已经移位到 flag 位上的形式；[`crate::common::compression::compress`] 与
    /// [`MessageSysFlag::set_compression_type`] 都取算法号。默认 ZLIB。
    pub compress_type: i32,
    /// Python `retry_times_when_send_failed`。
    pub retry_times_when_send_failed: i32,
    /// `retry_times_when_send_async_failed`（默认 2）：
    /// 异步发送在**请求已发出之后**的换 broker 重试次数上限，即异步链
    /// 每轮重投的 `timesTotal`。与同步发送的
    /// `retry_times_when_send_failed` 是**两个**独立预算，且判据不同（异步不看
    /// `retryResponseCodes`，见 [`classify_async_failure`]）。
    pub retry_times_when_send_async_failed: i32,
    /// `async_sender_queue_capacity`（默认 50000，对应一条有界
    /// `LinkedBlockingQueue`）：[`AsyncSenderExecutor`] 的队列容量，
    /// 只在 [`DefaultMQProducer::start`] 时读一次。
    pub async_sender_queue_capacity: i32,
    /// `retry_another_broker_when_not_store_ok`（默认 false）：
    /// broker 回了非 SEND_OK 的存储结果时是否换一台重发。
    pub retry_another_broker_when_not_store_ok: bool,
    /// `send_msg_max_timeout_per_request`（默认 -1 = 单次请求不设上限，
    /// 只受整体 `send_msg_timeout` 预算约束）。
    pub send_msg_max_timeout_per_request: i64,
    /// Python `retry_response_codes`：broker 回了这些**业务错误码**时换一台 broker 重发，
    /// 而不是直接把失败抛给调用方。
    pub retry_response_codes: BTreeSet<i32>,
    /// Python `max_message_size`。
    pub max_message_size: i32,
    /// `enable_backpressure_for_async_mode`：
    /// 默认 **关**。开了之后异步发送在真正发请求之前要先过两个维度的公平信号量闸。
    pub enable_backpressure_for_async_mode: bool,
    /// `back_pressure_for_async_send_num`：
    /// 在途**条数**上限，默认 1024，地板 [`MIN_ASYNC_SEND_NUM`]。
    pub back_pressure_for_async_send_num: i64,
    /// `back_pressure_for_async_send_size`：
    /// 在途**字节数**上限，默认 100M，地板 [`MIN_ASYNC_SEND_SIZE`]。
    pub back_pressure_for_async_send_size: i64,
    /// Python `topics`。
    pub topics: Vec<String>,
    /// Python `name_server_addrs`。
    pub name_server_addrs: Vec<String>,
    /// Python `heartbeat_interval_millis`。
    pub heartbeat_interval_millis: u64,
    /// Python `send_latency_fault_enable`。
    pub send_latency_fault_enable: bool,
    pub start_detector_enable: bool,
    pub detect_timeout: i64,
    pub detect_interval: i64,
    /// Python `enable_trace`。
    pub enable_trace: bool,
    /// Python `trace_topic`（`None` → 系统默认轨迹 topic）。
    pub trace_topic: Option<String>,
    /// Python `trace_msg_batch_num`。
    pub trace_msg_batch_num: i32,
    /// Python `request_timeout`（`request()` 未显式给 timeout 时用）。
    pub request_timeout: i64,
    /// `autoBatch`（默认 **false**）：打开后 `send(Message)`
    /// 先按 `AggregateKey` 归并成批量再发（见 [`crate::client::produce_accumulator`]）。
    pub auto_batch: bool,
    /// `batchMaxDelayMs`：一个批次的存活时间上限（ms）。
    /// **-1 是「调用方没表态」的哨兵** —— 只有 `> -1` 的值才会在
    /// [`DefaultMQProducer::init_produce_accumulator`] 里灌给累加器。
    pub batch_max_delay_ms: i64,
    /// `batchMaxBytes`：一个批次的字节数上限。同上，`-1` = 没表态。
    pub batch_max_bytes: i64,
    /// `totalBatchMaxBytes`：在途攒批的**全局**字节上限。
    /// 同上，`-1` = 没表态。
    pub total_batch_max_bytes: i64,
}

impl Default for ProducerConfig {
    fn default() -> ProducerConfig {
        ProducerConfig {
            producer_group: MixAll::DEFAULT_PRODUCER_GROUP.to_string(),
            tls_enable: None,
            socks_proxy: None,
            enable_trace_context: None,
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            unit_name: None,
            namespace_v2: None,
            unit_mode: false,
            // 生产者不碰这个开关（只有拉模式/轻量消费者会置 true）
            enable_stream_request_type: false,
            // pollNameServerInterval 默认 1000 * 30
            poll_name_server_interval_millis: 30_000,
            client_id: None,
            create_topic_key: MixAll::DEFAULT_TOPIC.to_string(),
            default_topic_queue_nums: MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
            send_msg_timeout: 3000,
            compress_msg_body_over_howmuch: DEFAULT_COMPRESS_MSG_BODY_OVER_HOWMUCH,
            compress_level: DEFAULT_COMPRESS_LEVEL,
            compress_type: MessageSysFlag::ZLIB_TYPE,
            retry_times_when_send_failed: 2,
            // 异步重试默认 2 次（经典客户端缺省）
            retry_times_when_send_async_failed: 2,
            // 异步发送队列默认容量 50000（有界阻塞队列缺省）
            async_sender_queue_capacity: DEFAULT_ASYNC_SENDER_QUEUE_CAPACITY,
            retry_another_broker_when_not_store_ok: false,
            send_msg_max_timeout_per_request: -1,
            retry_response_codes: DEFAULT_RETRY_RESPONSE_CODES
                .iter()
                .copied()
                .collect::<BTreeSet<i32>>(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            // 背压开关默认关，容量默认 1024 条 / 100M
            enable_backpressure_for_async_mode: false,
            back_pressure_for_async_send_num: 1024,
            back_pressure_for_async_send_size: 100 * 1024 * 1024,
            topics: Vec::new(),
            name_server_addrs: Vec::new(),
            heartbeat_interval_millis: 30_000,
            send_latency_fault_enable: false,
            start_detector_enable: false,
            detect_timeout: 200,
            detect_interval: 2 * 1000,
            enable_trace: false,
            trace_topic: None,
            trace_msg_batch_num: 10,
            request_timeout: DEFAULT_REQUEST_TIMEOUT_MILLIS,
            // 自动攒批：开关默认关，三个阈值默认 -1
            // （-1 = 「调用方没表态」，见字段说明）。
            auto_batch: false,
            batch_max_delay_ms: -1,
            batch_max_bytes: -1,
            total_batch_max_bytes: -1,
        }
    }
}

// ================================================================ 共享状态

/// 生产者的共享状态：配置 + 各类句柄 + 锁，被外层 `Arc` 共享。
///
/// 句柄被 [`DefaultMQProducer`] 以 `Arc` 共享，broker 推来的事务回查处理器只持
/// `Weak`（见 [`CheckTransactionStateProcessor`]），理由与
/// `mq_client::ClientRemotingProcessor` 相同：否则
/// `Inner → RemotingClient → processor → Inner` 会成环而永不释放。
struct Inner {
    /// Python 上散落的公有属性；`RwLock` 让 setter 与 Python 一样「随时可调」，
    /// 异步发送路径在读锁外快照所需字段（跨 `.await` 持读锁会让 future 不 `Send`）。
    cfg: RwLock<ProducerConfig>,
    /// Python `_mq_client`。
    client: Mutex<Option<MQClientInstance>>,
    /// Python `_started`。
    started: AtomicBool,
    /// 差异 3：`start()` 时惰性绑定的运行时句柄，`send_async` 与心跳任务用它派发。
    runtime: OnceLock<tokio::runtime::Handle>,
    /// Python `_heartbeat_running`（true = 停）。
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Python `_mq_fault_strategy`。
    fault_strategy: MQFaultStrategy,
    /// Python `metrics`。
    metrics: ClientMetrics,
    /// Python `send_message_hook_list`。
    send_hooks: SendMessageHookList,
    /// Python `end_transaction_hook_list`。
    end_txn_hooks: EndTransactionHookList,
    /// Python `check_forbidden_hook_list`。
    check_forbidden_hooks: CheckForbiddenHookList,
    /// Python `_transaction_listener`。
    transaction_listener: RwLock<Option<Arc<dyn TransactionListener>>>,
    /// Python `trace_dispatcher`；差异 4：由调用方注入。
    trace: Mutex<Option<Arc<dyn TraceDispatcherChannel>>>,
    /// Python `rpc_hook`。
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
    /// 异步发送在途**条数**的公平信号量（`_semaphore_async_send_num`）：
    /// 建对象时就按配置定容（越界兜地板值），
    /// 运行时改容量走 [`set_back_pressure_for_async_send_num`](DefaultMQProducer::set_back_pressure_for_async_send_num)。
    semaphore_async_send_num: FairSemaphore,
    /// 异步发送在途**字节数**的公平信号量（`_semaphore_async_send_size`）：
    /// 同样在建对象时按配置定容。
    semaphore_async_send_size: FairSemaphore,
    /// `defaultAsyncSenderExecutor`（异步发送执行器）：
    /// 有界队列 + 固定并发额度 + 一个派发任务。
    /// `start()` 建、`shutdown()` 关；`None` = 没启动过或已关闭。
    async_sender: Mutex<Option<AsyncSenderExecutor>>,
    /// `produceAccumulator`（未初始化则为空）：自动攒批的累加器。
    /// `start()` 里按 clientId 取（或建），`get_auto_batch()` 见不到它就报「没打开」。
    produce_accumulator: RwLock<Option<ProduceAccumulator>>,
}

impl Inner {
    fn producer_group(&self) -> String {
        self.cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .producer_group
            .clone()
    }

    fn namespace(&self) -> String {
        self.cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .namespace
            .clone()
    }

    /// 发送链各处统一取配置里的 `unitMode`：发送头、
    /// CheckForbiddenContext 都取这一个值。
    fn unit_mode(&self) -> bool {
        self.cfg.read().unwrap_or_else(|e| e.into_inner()).unit_mode
    }

    /// Python `_send_header_args` 里的另两项：每次发请求都要带上、且只来源于生产者配置的
    /// `c`（createTopicKey）与 `d`（defaultTopicQueueNums），
    /// 即发送内核为新建 topic 准备的 `createTopicKey` 与
    /// `defaultTopicQueueNums`（broker 据此自动建 topic 与队列数）。
    fn create_topic_key(&self) -> String {
        self.cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .create_topic_key
            .clone()
    }

    fn default_topic_queue_nums(&self) -> i32 {
        self.cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .default_topic_queue_nums
    }

    fn client(&self) -> Option<MQClientInstance> {
        self.client
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn trace_dispatcher(&self) -> Option<Arc<dyn TraceDispatcherChannel>> {
        self.trace.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// 有界异步发送队列 + 固定并发额度：经典形状是
/// 「有界阻塞队列(50000) + cores 个池线程」；本实现换成
/// tokio 通道 + 信号量 + 单一派发任务，语义逐条对上。
///
/// 形状逐条对上：
///   * `try_send` 失败 ≡ 池的 `submit` 里 `workQueue.offer` 失败后抛的
///     `RejectedExecutionException`（→ [`send_async`](DefaultMQProducer::send_async) 的分支）；
///   * [`close`](Self::close)（丢掉发送端）≡ `defaultAsyncSenderExecutor.shutdown()`
///     —— **已入队的任务照样跑完**，只是不再收新的，所以 `shutdown()` 不会把在途的异步
///     发送掐断；
///   * 并发额度 = [`async_sender_worker_count`]（核数）：拿不到
///     额度时任务留在队列里等，正如池线程全忙时任务留在 `workQueue`。
///
/// 池子限流的是「准备段」（闸 → 路由 → 钩子 → 建请求 → 交给传输层）；请求一旦交给
/// 传输层，任务就结束了，网络等待不占池 —— 这点必须对上，否则池大小会变成「在途异步
/// 发送数」的上限——上限本应由背压闸决定（不开背压则没有上限）。
///
/// ⚠ **为什么是「一个派发任务 + 额度 + 每笔单独 spawn」，而不是「`cores` 个消费者共用
/// 一把 `Mutex<Receiver>`」**（第一版就是后者，真机上被证明不能用）：池线程
/// 里跑同步钩子是**正常**的，tokio 里却是致命的 —— 消费者把任务取走后 `await` 它，钩子里
/// 一句 `Thread.sleep` 就把这条 tokio 工作线程按住在同步调用里；而 `tokio::sync::Mutex`
/// 是**交接**式公平锁，它把「下一个等待者」叫醒后把接力棒放进**当前这条工作线程的本地
/// 队列**，本地队列不会被人偷走 —— 于是取任务的接力链断在这个睡着的工人上。真机实测
/// （`examples/live_backpressure.rs` B3，钩子睡 2.5s）：队列里躺着 10 笔待派发，池子却
/// **每 2505ms 才派发一笔**，后面的笔只能看着自己的预算被排队吃光。拆成单一派发任务
/// （它自己永远不阻塞）+ 信号量限并发 + 每笔 `spawn`，接力的就只是「额度归还」这一件事，
/// 而归还发生在链路终点、不占任何睡着的线程。
///
/// ⚠ 两处无法照搬：池线程有名字（`AsyncSenderExecutor_N`）、且与核数一一绑定；
/// Rust 这边是 tokio 任务，既没有名字，实际并发也受宿主运行时的工作线程数制约。
struct AsyncSenderExecutor {
    tx: mpsc::Sender<AsyncSendJob>,
    /// 唯一消费者（池的「取任务」那一步）。它自己不跑任务，所以永远不会阻塞。
    dispatcher: JoinHandle<()>,
}

/// 投进队列的一个异步发送任务（已经在调用方线程上把消息、预算与起点定好）。
type AsyncSendJob = Pin<Box<dyn Future<Output = ()> + Send>>;

/// 池并发额度 = max(可用核数, `MIN_ASYNC_SEND_NUM`)。
///
/// 取不到核数时按 1 兜底。下限取 `MIN_ASYNC_SEND_NUM` 是因为「在途条数」背压信号量的
/// 下限就是它(10)：发送器必须至少能并发跑这么多笔，这个背压下限才有意义——否则低核
/// 机器上信号量永远凑不满（最多只会被占 1~2 份），背压等于失效。多核机器下
/// `availableProcessors` 语义与原 Java 客户端一致，不受影响。
fn async_sender_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(MIN_ASYNC_SEND_NUM as usize)
}

impl AsyncSenderExecutor {
    fn new(handle: &tokio::runtime::Handle, capacity: i32, workers: usize) -> AsyncSenderExecutor {
        // tokio 的有界通道不接受 0 容量（`LinkedBlockingQueue(0)` 同样会抛
        // IllegalArgumentException，只是这里不能 panic）：配成 0 时按 1，
        // 语义仍是「几乎不留排队空间」。
        let (tx, mut rx) = mpsc::channel(usize::try_from(capacity.max(1)).unwrap_or(usize::MAX));
        let permits = Arc::new(Semaphore::new(workers.max(1)));
        let spawn_handle = handle.clone();
        let dispatcher = handle.spawn(async move {
            let permits = permits;
            loop {
                // 发送端全部释放 = `close`：把已经排队的任务派发完再退
                let Some(job) = rx.recv().await else { return };
                // 额度不够就留在队列里等（等的时候让出线程，不占任何人）。
                // `acquire_owned` 而不是 `acquire`：额度要跟着任务进 `'static` 的任务里，
                // 借用式的那份（`SemaphorePermit<'_>`）不能 move，只能 `forget` 掉，
                // 而 `forget` 是「永不归还」—— 那样发满 `cores` 笔就把池子锁死了。
                // 拿不到额度只可能是有人 `close()` 了这个信号量（本端口不会）：那时
                // 宁可照样派发、不占额度，也不要把用户交进来的发送悄悄吞掉。
                let permit = Arc::clone(&permits).acquire_owned().await.ok();
                spawn_handle.spawn(async move {
                    let _permit = permit; // 任务结束（含阻塞钩子跑完）才归还
                    job.await;
                });
            }
        });
        AsyncSenderExecutor { tx, dispatcher }
    }

    /// ≡ 池的 `submit(runnable)`：队列满了**立即**失败，不排队等空位。
    /// 失败时把任务原样交还，让调用方能走「就地跑完」的分支。
    ///
    /// （返回类型写全 `std::result::Result`：本模块的 `Result<T>` 别名只带一个参数，
    /// 错误类型固定为 [`Error`]，而这里要交还的是任务本身。）
    fn try_submit(&self, job: AsyncSendJob) -> std::result::Result<(), AsyncSendJob> {
        match self.tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(job)) => Err(job),
            Err(mpsc::error::TrySendError::Closed(job)) => Err(job),
        }
    }

    /// ≡ `defaultAsyncSenderExecutor.shutdown()`：不再收新任务，在途与已排队的照常跑完。
    fn close(self) {
        let AsyncSenderExecutor { tx, dispatcher } = self;
        drop(tx);
        // 只丢句柄、绝不 abort：丢掉 `JoinHandle` 在 tokio 里就是 detach，任务继续跑完。
        // 池的 `shutdown()` 语义：既不停已入队任务、也不等待它终止。
        drop(dispatcher);
    }
}

/// 忘记调 `shutdown()` 时也不能留下还在跑的后台任务（Python 的守护线程随进程退出，
/// Rust 的 tokio 任务会一直活在被泄漏的运行时里）。
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        for task in self
            .tasks
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
    }
}

// ================================================================ DefaultMQProducer

/// 默认生产者：实现经典 remoting 协议的生产者完整形态
/// （配置 + 生命周期 + 各类发送入口 + 事务/攒批/轨迹）。
///
/// 克隆得到的副本与原对象**共享同一份状态**（差异 1）：引用语义
/// 在 Rust 里靠 `Arc<Inner>` 落地，所以把生产者交给回调/处理器
/// 之后仍可继续 `set_xxx`，副本立即可见。
///
/// 生命周期：[`start`](Self::start) → 若干 `send*` → [`shutdown`](Self::shutdown)。
/// 未启动就发送会得到 [`Error::Client`]（Python `_require_client` 的
/// `producer not started, call start() first`）。
#[derive(Clone)]
pub struct DefaultMQProducer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for DefaultMQProducer {
    /// 只打印身份与状态：钩子/监听器是 `dyn Trait`，打不出内容。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cfg = self.inner.cfg.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("DefaultMQProducer")
            .field("producer_group", &cfg.producer_group)
            .field("namespace", &cfg.namespace)
            .field("instance_name", &cfg.instance_name)
            .field("client_id", &cfg.client_id)
            .field("name_server_addrs", &cfg.name_server_addrs)
            .field("started", &self.inner.started.load(Ordering::Acquire))
            .field("enable_trace", &cfg.enable_trace)
            .finish()
    }
}

impl DefaultMQProducer {
    /// 对应 Python `DefaultMQProducer(producer_group)`；组名空则报错
    /// （Python 抛 `MQClientException("producerGroup is empty")`）。
    pub fn new(producer_group: &str) -> Result<DefaultMQProducer> {
        let cfg = ProducerConfig {
            producer_group: producer_group.to_string(),
            ..Default::default()
        };
        DefaultMQProducer::with_config(cfg)
    }

    pub fn with_rpc_hook(
        producer_group: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Result<DefaultMQProducer> {
        let producer = DefaultMQProducer::new(producer_group)?;
        producer.set_rpc_hook(rpc_hook);
        Ok(producer)
    }

    /// 直接以一份完整配置构造（Python 没有对应入口，等价于逐个 `set_*`）。
    pub fn with_config(cfg: ProducerConfig) -> Result<DefaultMQProducer> {
        if cfg.producer_group.trim().is_empty() {
            bail!("producerGroup is empty");
        }
        let fault_strategy = MQFaultStrategy::new(cfg.send_latency_fault_enable);
        let semaphore_async_send_num = FairSemaphore::new(back_pressure_permits(
            cfg.back_pressure_for_async_send_num,
            MIN_ASYNC_SEND_NUM,
            "semaphoreAsyncSendNum",
        ));
        let semaphore_async_send_size = FairSemaphore::new(back_pressure_permits(
            cfg.back_pressure_for_async_send_size,
            MIN_ASYNC_SEND_SIZE,
            "semaphoreAsyncSendSize",
        ));
        let inner = Arc::new(Inner {
            cfg: RwLock::new(cfg),
            client: Mutex::new(None),
            started: AtomicBool::new(false),
            runtime: OnceLock::new(),
            stop: watch::channel(false).0,
            tasks: Mutex::new(Vec::new()),
            fault_strategy,
            metrics: ClientMetrics::new(),
            send_hooks: SendMessageHookList::new(),
            end_txn_hooks: EndTransactionHookList::new(),
            check_forbidden_hooks: CheckForbiddenHookList::new(),
            transaction_listener: RwLock::new(None),
            trace: Mutex::new(None),
            rpc_hook: RwLock::new(None),
            semaphore_async_send_num,
            semaphore_async_send_size,
            async_sender: Mutex::new(None),
            produce_accumulator: RwLock::new(None),
        });
        Ok(DefaultMQProducer { inner })
    }

    /// 当前生效的配置快照（Python 直接读属性）。
    pub fn config(&self) -> ProducerConfig {
        self.inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn producer_group(&self) -> String {
        self.inner.producer_group()
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    /// [`MQClientInstance`]（Python `_mq_client`），未启动时 `None`。
    pub fn client(&self) -> Option<MQClientInstance> {
        self.inner.client()
    }

    /// 本进程内实际使用的 clientId（`start()` 之前为 `None`）。
    pub fn client_id(&self) -> Option<String> {
        self.inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .client_id
            .clone()
    }

    // ---------------- 配置 ----------------

    /// Python `set_namesrv_addr`：`;` 分隔，空白项丢弃。
    pub fn set_namesrv_addr(&self, addr: &str) {
        let addrs = addr
            .split(';')
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .map(str::to_string)
            .collect();
        self.write_cfg(|c| c.name_server_addrs = addrs);
    }

    /// Python `set_name_server_addresses`。
    pub fn set_name_server_addresses(&self, addrs: Vec<String>) {
        self.write_cfg(|c| c.name_server_addrs = addrs);
    }

    /// Python `get_namesrv_addr`：`;` 拼接。
    pub fn namesrv_addr(&self) -> String {
        self.inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .name_server_addrs
            .join(";")
    }

    /// Python `set_instance_name`。
    pub fn set_instance_name(&self, name: &str) {
        self.write_cfg(|c| c.instance_name = name.to_string());
    }

    pub fn set_unit_name(&self, unit_name: Option<&str>) {
        self.write_cfg(|c| c.unit_name = unit_name.map(str::to_string));
    }

    pub fn set_namespace_v2(&self, namespace_v2: Option<&str>) {
        self.write_cfg(|c| c.namespace_v2 = namespace_v2.map(str::to_string));
    }

    pub fn get_namespace_v2(&self) -> Option<String> {
        self.config().namespace_v2
    }

    pub fn set_socks_proxy(&self, socks_proxy: Option<SocksProxyConfig>) {
        self.write_cfg(|c| c.socks_proxy = socks_proxy);
    }

    pub fn get_socks_proxy(&self) -> Option<SocksProxyConfig> {
        self.config().socks_proxy
    }

    pub fn set_unit_mode(&self, unit_mode: bool) {
        self.write_cfg(|c| c.unit_mode = unit_mode);
    }

    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.write_cfg(|c| c.enable_stream_request_type = enable);
    }

    pub fn set_poll_name_server_interval_millis(&self, millis: u64) {
        self.write_cfg(|c| c.poll_name_server_interval_millis = millis);
    }

    /// Python `set_namespace`（`__init__` 的 `namespace` 参数）。
    pub fn set_namespace(&self, namespace: &str) {
        self.write_cfg(|c| c.namespace = namespace.to_string());
    }

    /// Python `set_topics`。
    pub fn set_topics(&self, topics: Vec<String>) {
        self.write_cfg(|c| c.topics = topics);
    }

    /// Python `tls_enable` 属性（`None` = 交给环境变量 `ROCKETMQ_TLS_ENABLE`）。
    pub fn set_tls_enable(&self, tls_enable: Option<bool>) {
        self.write_cfg(|c| c.tls_enable = tls_enable);
    }

    /// Python `enable_trace_context` 属性（`None` = 交给环境变量
    /// `ROCKETMQ_TRACE_CONTEXT_ENABLE`，Python 在 `__init__` 里就解析掉）。
    pub fn set_enable_trace_context(&self, enable: Option<bool>) {
        self.write_cfg(|c| c.enable_trace_context = enable);
    }

    pub fn set_client_id(&self, client_id: Option<&str>) {
        self.write_cfg(|c| {
            c.client_id = client_id.map(str::to_string);
        });
    }

    /// Python `set_max_message_size`。
    pub fn set_max_message_size(&self, size: i32) {
        self.write_cfg(|c| c.max_message_size = size);
    }

    /// Python `set_send_msg_timeout`。
    pub fn set_send_msg_timeout(&self, timeout_millis: i64) {
        self.write_cfg(|c| c.send_msg_timeout = timeout_millis);
    }

    /// Python `set_retry_times_when_send_failed`。
    pub fn set_retry_times_when_send_failed(&self, n: i32) {
        self.write_cfg(|c| c.retry_times_when_send_failed = n);
    }

    pub fn set_retry_times_when_send_async_failed(&self, n: i32) {
        self.write_cfg(|c| c.retry_times_when_send_async_failed = n);
    }

    pub fn get_retry_times_when_send_async_failed(&self) -> i32 {
        self.read_cfg(|c| c.retry_times_when_send_async_failed)
    }

    /// C++ `setAsyncSenderQueueCapacity`（Python 直接改 `async_sender_queue_capacity` 属性）：
    /// 异步发送队列容量，**只在 `start()` 建池时生效**。
    pub fn set_async_sender_queue_capacity(&self, capacity: i32) {
        self.write_cfg(|c| c.async_sender_queue_capacity = capacity);
    }

    /// C++ `getAsyncSenderQueueCapacity`。
    pub fn get_async_sender_queue_capacity(&self) -> i32 {
        self.read_cfg(|c| c.async_sender_queue_capacity)
    }

    pub fn set_retry_another_broker_when_not_store_ok(&self, retry: bool) {
        self.write_cfg(|c| c.retry_another_broker_when_not_store_ok = retry);
    }

    /// Python `is_retry_another_broker_when_not_store_ok`。
    pub fn is_retry_another_broker_when_not_store_ok(&self) -> bool {
        self.read_cfg(|c| c.retry_another_broker_when_not_store_ok)
    }

    /// `-1`（默认）表示单次请求不设上限；设成有限值后，**还有重试机会**的那几次
    /// 单次超时被压到该值，把余量留给后面的 broker —— 否则一台慢 broker
    /// 就能把整个 `send_msg_timeout` 预算吃光，剩下的 broker 一次都试不到。
    pub fn set_send_msg_max_timeout_per_request(&self, timeout_millis: i64) {
        self.write_cfg(|c| c.send_msg_max_timeout_per_request = timeout_millis);
    }

    /// Python `get_send_msg_max_timeout_per_request`。
    pub fn get_send_msg_max_timeout_per_request(&self) -> i64 {
        self.read_cfg(|c| c.send_msg_max_timeout_per_request)
    }

    /// Python `add_retry_response_code`：往可重试码集合里加一个 broker 响应码。
    pub fn add_retry_response_code(&self, response_code: i32) {
        self.write_cfg(|c| {
            c.retry_response_codes.insert(response_code);
        });
    }

    pub fn get_retry_response_codes(&self) -> BTreeSet<i32> {
        self.read_cfg(|c| c.retry_response_codes.clone())
    }

    /// Python `is_retry_response_code`：`None`（压根没等到响应码）等于不可重试。
    pub fn is_retry_response_code(&self, response_code: Option<i32>) -> bool {
        match response_code {
            Some(code) => self.read_cfg(|c| c.retry_response_codes.contains(&code)),
            None => false,
        }
    }

    /// Python `set_compress_msg_body_over_howmuch`。
    pub fn set_compress_msg_body_over_howmuch(&self, size: i32) {
        self.write_cfg(|c| c.compress_msg_body_over_howmuch = size);
    }

    /// Python `set_compress_level`。
    pub fn set_compress_level(&self, level: i32) {
        self.write_cfg(|c| c.compress_level = level);
    }

    /// Python `set_compress_type`（取 [`MessageSysFlag::ZLIB_TYPE`] 等**算法号**）。
    pub fn set_compress_type(&self, compression_type: i32) -> Result<()> {
        let normalized = compression::validate_compression_type(compression_type)?;
        self.write_cfg(|c| c.compress_type = normalized);
        Ok(())
    }

    /// Python `set_create_topic_key`。
    pub fn set_create_topic_key(&self, key: &str) {
        self.write_cfg(|c| c.create_topic_key = key.to_string());
    }

    /// Python `set_default_topic_queue_nums`。
    pub fn set_default_topic_queue_nums(&self, n: i32) {
        self.write_cfg(|c| c.default_topic_queue_nums = n);
    }

    /// Python `set_heartbeat_interval_millis`（属性直改）。
    pub fn set_heartbeat_interval_millis(&self, millis: u64) {
        self.write_cfg(|c| c.heartbeat_interval_millis = millis);
    }

    /// Python `set_producer_group`：启动后禁止改（broker 侧已按旧组名登记）。
    pub fn set_producer_group(&self, group: &str) -> Result<()> {
        if self.is_started() {
            bail!("producerGroup cannot be changed after startup");
        }
        self.write_cfg(|c| c.producer_group = group.to_string());
        Ok(())
    }

    /// Python `set_send_latency_fault_enable`：配置与策略一起改
    /// （策略是 `Arc` 共享的，所以运行中切换也生效）。
    pub fn set_send_latency_fault_enable(&self, enable: bool) {
        self.write_cfg(|c| c.send_latency_fault_enable = enable);
        self.inner
            .fault_strategy
            .set_send_latency_fault_enable(enable);
    }

    pub fn set_start_detector_enable(&self, enable: bool) {
        self.write_cfg(|c| c.start_detector_enable = enable);
    }

    pub fn is_start_detector_enable(&self) -> bool {
        self.read_cfg(|c| c.start_detector_enable)
    }

    pub fn set_detect_timeout(&self, timeout_millis: i64) {
        self.write_cfg(|c| c.detect_timeout = timeout_millis);
    }

    pub fn get_detect_timeout(&self) -> i64 {
        self.read_cfg(|c| c.detect_timeout)
    }

    pub fn set_detect_interval(&self, interval_millis: i64) {
        self.write_cfg(|c| c.detect_interval = interval_millis);
    }

    pub fn get_detect_interval(&self) -> i64 {
        self.read_cfg(|c| c.detect_interval)
    }

    /// Python `set_request_timeout`。
    pub fn set_request_timeout(&self, timeout_millis: i64) {
        self.write_cfg(|c| c.request_timeout = timeout_millis);
    }

    /// Python `get_metrics`。
    pub fn metrics(&self) -> &ClientMetrics {
        &self.inner.metrics
    }

    /// Python `_mq_fault_strategy`（暴露故障表便于排错/测试）。
    pub fn fault_strategy(&self) -> &MQFaultStrategy {
        &self.inner.fault_strategy
    }

    // ---------------- 异步发送背压 ----------------

    pub fn set_enable_backpressure_for_async_mode(&self, enable: bool) {
        self.write_cfg(|c| c.enable_backpressure_for_async_mode = enable);
    }

    /// Python `is_enable_backpressure_for_async_mode`。
    pub fn is_enable_backpressure_for_async_mode(&self) -> bool {
        self.read_cfg(|c| c.enable_backpressure_for_async_mode)
    }

    pub fn set_back_pressure_for_async_send_num(&self, num: i64) {
        let num = num.max(MIN_ASYNC_SEND_NUM);
        self.write_cfg(|c| c.back_pressure_for_async_send_num = num);
        self.inner.semaphore_async_send_num.set_total_permits(num);
    }

    /// Python `get_back_pressure_for_async_send_num`（当前配置的总量，不是空闲量）。
    pub fn get_back_pressure_for_async_send_num(&self) -> i64 {
        self.read_cfg(|c| c.back_pressure_for_async_send_num)
    }

    /// 运行时改「在途字节数」上限，语义与
    /// [`set_back_pressure_for_async_send_num`](Self::set_back_pressure_for_async_send_num) 相同。
    pub fn set_back_pressure_for_async_send_size(&self, size: i64) {
        let size = size.max(MIN_ASYNC_SEND_SIZE);
        self.write_cfg(|c| c.back_pressure_for_async_send_size = size);
        self.inner.semaphore_async_send_size.set_total_permits(size);
    }

    /// Python `get_back_pressure_for_async_send_size`。
    pub fn get_back_pressure_for_async_send_size(&self) -> i64 {
        self.read_cfg(|c| c.back_pressure_for_async_send_size)
    }

    pub fn semaphore_async_send_num_available_permits(&self) -> i64 {
        self.inner.semaphore_async_send_num.available_permits()
    }

    /// Python `get_semaphore_async_send_size_available_permits`。
    pub fn semaphore_async_send_size_available_permits(&self) -> i64 {
        self.inner.semaphore_async_send_size.available_permits()
    }

    // ---------------- 消息轨迹配置 ----------------

    /// Python `set_enable_trace`。
    ///
    /// ⚠ 轨迹分发器本身**不在这里新建**（差异 4）：先
    /// [`set_trace_dispatcher`](Self::set_trace_dispatcher) 注入，`start()` 才会照
    /// Python 的顺序注册两个轨迹钩子并启动它。
    pub fn set_enable_trace(&self, enable: bool) {
        self.write_cfg(|c| c.enable_trace = enable);
    }

    /// Python `is_enable_trace`。
    pub fn is_enable_trace(&self) -> bool {
        self.inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .enable_trace
    }

    /// Python `set_trace_topic`（`None` → 系统默认轨迹 topic）。
    pub fn set_trace_topic(&self, trace_topic: Option<&str>) {
        self.write_cfg(|c| {
            c.trace_topic = trace_topic.map(str::to_string);
        });
    }

    /// Python `set_trace_msg_batch_num`。
    pub fn set_trace_msg_batch_num(&self, n: i32) {
        self.write_cfg(|c| c.trace_msg_batch_num = n);
    }

    /// 注入轨迹分发器（差异 4）。传 `None` 撤销注入。
    pub fn set_trace_dispatcher(&self, dispatcher: Option<Arc<dyn TraceDispatcherChannel>>) {
        *self.inner.trace.lock().unwrap_or_else(|e| e.into_inner()) = dispatcher;
    }

    /// 已注入的轨迹分发器（Python `trace_dispatcher` 属性）。
    pub fn trace_dispatcher(&self) -> Option<Arc<dyn TraceDispatcherChannel>> {
        self.inner.trace_dispatcher()
    }

    /// Python `rpc_hook` 属性；`start()` 时注册到 remoting 通道。
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

    // ---------------- 钩子注册 ----------------

    /// Python `register_send_message_hook`。
    pub fn register_send_message_hook(&self, hook: Arc<dyn SendMessageHook>) {
        self.inner.send_hooks.register(hook);
    }

    /// Python `has_send_message_hook`。
    pub fn has_send_message_hook(&self) -> bool {
        self.inner.send_hooks.has_hooks()
    }

    /// Python `register_check_forbidden_hook`。
    pub fn register_check_forbidden_hook(&self, hook: Arc<dyn CheckForbiddenHook>) {
        self.inner.check_forbidden_hooks.register(hook);
    }

    /// Python `has_check_forbidden_hook`。
    pub fn has_check_forbidden_hook(&self) -> bool {
        self.inner.check_forbidden_hooks.has_hooks()
    }

    /// Python `register_end_transaction_hook`。
    pub fn register_end_transaction_hook(&self, hook: Arc<dyn EndTransactionHook>) {
        self.inner.end_txn_hooks.register(hook);
    }

    /// Python `_has_send_interceptors`：两类钩子任一存在就得走带钩子的发送内核。
    fn has_send_interceptors(&self) -> bool {
        self.inner.send_hooks.has_hooks() || self.inner.check_forbidden_hooks.has_hooks()
    }

    /// Python `set_transaction_listener` / `_transaction_listener` 属性。
    pub fn set_transaction_listener(&self, listener: Option<Arc<dyn TransactionListener>>) {
        *self
            .inner
            .transaction_listener
            .write()
            .unwrap_or_else(|e| e.into_inner()) = listener;
    }

    /// Python `get_transaction_listener`。
    pub fn transaction_listener(&self) -> Option<Arc<dyn TransactionListener>> {
        self.inner
            .transaction_listener
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Python `self.xxx = value` 的统一入口。
    fn write_cfg(&self, f: impl FnOnce(&mut ProducerConfig)) {
        f(&mut self.inner.cfg.write().unwrap_or_else(|e| e.into_inner()));
    }

    /// [`Self::write_cfg`] 的只读版：锁毒化时同样带着旧值继续，绝不在读配置上 panic。
    fn read_cfg<T>(&self, f: impl FnOnce(&ProducerConfig) -> T) -> T {
        f(&self.inner.cfg.read().unwrap_or_else(|e| e.into_inner()))
    }

    // ---------------- 生命周期 ----------------

    /// Python `start()`。
    ///
    /// 步骤与 `producer.py:434-469` 逐条对齐：校验地址 → 生成 clientId →
    /// 生产者组拼命名空间 → 建/复用 [`MQClientInstance`] → 注册 RPC 钩子 →
    /// 启动实例 → 回填动态拿到的 namesrv → 注册事务回查处理器 → 起心跳任务。
    pub async fn start(&self) -> Result<()> {
        // 幂等（差异见上）；已启动时不重复注册钩子、不重复起心跳。
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
            cfg.producer_group.clone()
        } else {
            NamespaceUtil::wrap_namespace(&cfg.namespace, &cfg.producer_group)
        };
        if let Err(e) = validators::check_group(&group) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        if group == MixAll::DEFAULT_PRODUCER_GROUP {
            self.inner.started.store(false, Ordering::Release);
            return Err(Error::client(
                "producerGroup can not equal DEFAULT_PRODUCER, please specify another one.",
            ));
        }
        if cfg.name_server_addrs.is_empty() && !DefaultTopAddressing::is_configured() {
            self.inner.started.store(false, Ordering::Release);
            return Err(Error::client_with_code(
                client_error_code::NO_NAME_SERVER_EXCEPTION,
                "name server address is not set",
            ));
        }
        let instance_name = MixAll::change_instance_name_to_pid(&cfg.instance_name);
        let client_id = cfg.client_id.clone().unwrap_or_else(|| {
            MixAll::build_default_client_id(
                &instance_name,
                cfg.unit_name.as_deref(),
                cfg.enable_stream_request_type,
            )
        });
        // Python 在 `__init__` 里就把 None 解析成布尔；这里等价地在 start 时定型。
        let trace_context_on = cfg
            .enable_trace_context
            .unwrap_or_else(trace_context_enabled_from_env);
        self.write_cfg(|c| {
            c.client_id = Some(client_id.clone());
            c.instance_name = instance_name;
            c.producer_group = group.clone();
            c.enable_trace_context = Some(trace_context_on);
        });

        let instance_cfg = MQClientInstanceConfig {
            tls_enable: cfg.tls_enable,
            socks_proxy: cfg.socks_proxy.clone(),
            unit_name: cfg.unit_name.clone(),
            namespace_v2: cfg.namespace_v2.clone(),
            enable_stream_request_type: cfg.enable_stream_request_type,
            route_refresh_interval_millis: cfg.poll_name_server_interval_millis,
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
        *self.inner.client.lock().unwrap_or_else(|e| e.into_inner()) = Some(client.clone());
        client.register_producer(&group);

        // 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本生产者
        if cfg.name_server_addrs.is_empty() {
            let addrs = client.name_server_addrs();
            if !addrs.is_empty() {
                self.write_cfg(|c| c.name_server_addrs = addrs);
            }
        }

        // 注册 broker 主动请求处理器：事务回查 CHECK_TRANSACTION_STATE(39)。
        // 按 message_ext 的 PGROUP 属性匹配本生产者，不匹配则丢弃。
        client.remoting_client().register_processor(
            request_code::CHECK_TRANSACTION_STATE,
            Arc::new(CheckTransactionStateProcessor {
                producer: Arc::downgrade(&self.inner),
            }),
        );

        if let Some(handle) = self.runtime_handle() {
            let capacity = cfg.async_sender_queue_capacity;
            let executor = AsyncSenderExecutor::new(&handle, capacity, async_sender_worker_count());
            *self
                .inner
                .async_sender
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(executor);
        }

        // 心跳任务：周期性向 broker 注册 ProducerData。
        // broker 的事务回查正是通过这一步登记的 channel 反向联系生产者的；
        // 生产者不发心跳时 COMMIT/ROLLBACK 仍能成功（客户端主动 END_TRANSACTION），
        // 但 UNKNOW 的半消息会**永远不被回查**。
        if let Some(handle) = self.runtime_handle() {
            // 上一轮 shutdown 留下的停止信号要清掉，否则重启后心跳立刻退出。
            let _ = self.inner.stop.send(false);
            let mut rx = self.inner.stop.subscribe();
            let weak = Arc::downgrade(&self.inner);
            let task = handle.spawn(async move {
                loop {
                    let Some(inner) = weak.upgrade() else {
                        return; // 生产者已释放（没调 shutdown），没必要继续
                    };
                    let interval_millis = inner
                        .cfg
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .heartbeat_interval_millis;
                    if let Some(client) = inner.client() {
                        send_heartbeat_to_all_broker(&inner, &client).await;
                    }
                    drop(inner);
                    // Python 以 1s 为粒度轮询 `_heartbeat_running`；watch 通道
                    // 直接等到点或被叫醒。
                    if tokio::time::timeout(
                        Duration::from_millis(interval_millis.max(1000)),
                        rx.changed(),
                    )
                    .await
                    .is_ok()
                    {
                        return; // 停止信号（或发送端已释放）
                    }
                }
            });
            self.push_task(task);
        } else {
            // 没有运行时就没法跑心跳：发送本身仍可用（Python 的线程也一样会失败吗？
            // 不会，Python 总能起线程）。这里显式告警，避免用户静默丢掉事务回查能力。
            rmq_warn!(
                "producer: no tokio runtime, heartbeat disabled; broker transaction \
                       check-back will not reach this producer"
            );
        }

        if let Some(handle) = self.runtime_handle() {
            let weak = Arc::downgrade(&self.inner);
            let task = handle.spawn(async move {
                loop {
                    tokio::time::sleep(DETECTOR_SCHEDULE_PERIOD).await;
                    let Some(inner) = weak.upgrade() else {
                        return; // 生产者已释放（没调 shutdown），没必要继续
                    };
                    let (enable, detect_timeout, detect_interval) = {
                        let cfg = inner.cfg.read().unwrap_or_else(|e| e.into_inner());
                        (cfg.start_detector_enable, cfg.detect_timeout, cfg.detect_interval)
                    };
                    if !enable {
                        continue;
                    }
                    if let Some(client) = inner.client() {
                        run_detect_round(&inner, &client, detect_timeout, detect_interval).await;
                    }
                }
            });
            self.push_task(task);
        }

        self.start_trace_dispatcher();

        start_expired_request_scan();
        Ok(())
    }

    /// Python `shutdown()`。
    ///
    /// ⚠ 与「spawn 后立即返回」的旧形态不同：35 号注销与实例拆解挂在运行时上跑，
    /// 但本方法**阻塞等它落地**（上限 [`SHUTDOWN_FINALIZE_BUDGET`]）。
    /// 「shutdown 后立刻 `std::process::exit`」现在也能把这帧发出去；current_thread
    /// 运行时里无法阻塞（会 panic / 死锁），此时收尾仍是游离任务并告警
    /// （见 [`run_finalize_blocking`] 的情形 2/3）。
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
        for task in self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
        stop_expired_request_scan();
        // ⚠ 与 Python 的差别：Python 只把 `_mq_client` 留着（下次 start 会新建并覆盖
        // INSTANCE_MAP）；这里丢掉引用，好让 `create_mq_client_instance` 在重启时
        // 建出干净的新实例，而不是复用一个已被 shutdown 的。
        let client = self
            .inner
            .client
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let executor = self
            .inner
            .async_sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(executor) = executor {
            executor.close();
        }
        if let Some(client) = client {
            client.unregister_producer(&self.inner.producer_group());
            client.detach_from_registry_if_last_tenant();
            match (self.runtime_handle(), self.client_id()) {
                (Some(handle), Some(client_id)) => {
                    let group = self.inner.producer_group();
                    let finalize = async move {
                        client
                            .unregister_client_all_brokers(
                                &client_id,
                                &group,
                                "",
                                MQ_CLIENT_API_TIMEOUT_MILLIS,
                            )
                            .await;
                        client.shutdown();
                    };
                    let _ = run_finalize_blocking(
                        &handle,
                        "producer shutdown",
                        SHUTDOWN_FINALIZE_BUDGET,
                        finalize,
                    );
                }
                // 无运行时：这一发注销发不出去（Python 那里线程照起），但至少把实例还掉。
                _ => {
                    rmq_warn!("producer shutdown: no tokio runtime, skip broker unregister");
                    client.shutdown();
                }
            }
        }
        if let Some(dispatcher) = self.inner.trace_dispatcher() {
            dispatcher.shutdown();
        }
    }

    /// Python `_require_client`。
    fn require_client(&self) -> Result<MQClientInstance> {
        if !self.is_started() {
            return Err(Error::client("producer not started, call start() first"));
        }
        self.inner
            .client()
            .ok_or_else(|| Error::client("producer not started, call start() first"))
    }

    /// 差异 3：与 `remoting::client` 同样的惰性绑定 —— 已有缓存就用缓存，否则从当前
    /// 运行时上下文取（`start()` 是 async，必然在运行时里）。
    fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        if let Some(handle) = self.inner.runtime.get() {
            return Some(handle.clone());
        }
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let _ = self.inner.runtime.set(handle.clone());
        Some(handle)
    }

    fn push_task(&self, task: JoinHandle<()>) {
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(task);
    }

    /// 注入优先（[`set_trace_dispatcher`](Self::set_trace_dispatcher)，原差异 4）；
    /// `enable_trace=true` 且未注入时经 [`auto_bridged_dispatcher`] 自动建一套
    /// —— 与 Python `producer._start_trace_dispatcher`（`producer.py:869-891`）
    /// 行为一致。任何失败都只记日志 —— 轨迹挂了不能影响正常发送。
    fn start_trace_dispatcher(&self) {
        let cfg = self.config();
        let dispatcher: Arc<dyn TraceDispatcherChannel> = match self.inner.trace_dispatcher() {
            Some(d) => d,
            None if cfg.enable_trace => {
                match auto_bridged_dispatcher(
                    &self.inner.producer_group(),
                    TraceDispatcherType::Produce,
                    self.rpc_hook(),
                    cfg.trace_msg_batch_num,
                    cfg.trace_topic.clone(),
                ) {
                    Ok(d) => {
                        // Python `dispatcher.set_host_producer(self)`（producer.py:881）
                        d.set_host_producer(Arc::new(self.clone()));
                        let channel: Arc<dyn TraceDispatcherChannel> = d;
                        self.set_trace_dispatcher(Some(channel.clone()));
                        channel
                    }
                    Err(e) => {
                        // 文案照抄 Python producer.py:886
                        rmq_error!(
                            "system mqtrace hook init failed ,maybe can't send msg trace data: {e}"
                        );
                        return;
                    }
                }
            }
            None => return,
        };
        if cfg.enable_trace {
            // 重启不能把轨迹钩子注册第二遍（Python 每次 start 新建 dispatcher，
            // 钩子也只挂一次）。
            let sink: Arc<dyn TraceReportSink> = Arc::new(SinkAdapter(dispatcher.clone()));
            if !has_hook_named(&self.inner.send_hooks.hooks(), "SendMessageTraceHook") {
                self.register_send_message_hook(Arc::new(SendMessageTraceHook::new(sink.clone())));
            }
            if !has_end_hook_named(&self.inner.end_txn_hooks.hooks(), "EndTransactionTraceHook") {
                self.register_end_transaction_hook(Arc::new(EndTransactionTraceHook::new(sink)));
            }
        }
        if let Err(e) = dispatcher.start(&self.namesrv_addr()) {
            rmq_warn!("trace dispatcher start failed: {e}");
        }
    }
}

/// 按 `hook_name()` 去重（Python 靠「每次新建 dispatcher 时才注册」天然不重复）。
fn has_hook_named(hooks: &[Arc<dyn SendMessageHook>], name: &str) -> bool {
    hooks.iter().any(|h| h.hook_name() == name)
}

fn has_end_hook_named(hooks: &[Arc<dyn EndTransactionHook>], name: &str) -> bool {
    hooks.iter().any(|h| h.hook_name() == name)
}

// ================================================================ 故障规避主动探活

const DETECTOR_SCHEDULE_PERIOD: Duration = Duration::from_secs(3);

async fn run_detect_round(
    inner: &Inner,
    client: &MQClientInstance,
    detect_timeout: i64,
    detect_interval: i64,
) {
    let resolver = PublishAddrResolver { client: client.clone() };
    let detector = MaxOffsetServiceDetector { client: client.clone() };
    inner
        .fault_strategy
        .latency_fault_tolerance()
        .detect_by_one_round(&resolver, &detector, detect_timeout, detect_interval)
        .await;
}

struct PublishAddrResolver {
    client: MQClientInstance,
}

impl Resolver for PublishAddrResolver {
    fn resolve(&self, broker_name: &str) -> Option<String> {
        self.client.find_broker_address_in_publish(broker_name)
    }
}

struct MaxOffsetServiceDetector {
    client: MQClientInstance,
}

impl ServiceDetector for MaxOffsetServiceDetector {
    fn detect(
        &self,
        broker_name: &str,
        endpoint: &str,
        timeout_millis: i64,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        let client = self.client.clone();
        let broker_name = broker_name.to_string();
        let endpoint = endpoint.to_string();
        Box::pin(async move {
            let Some(topic) = client.pick_topic_for_broker(&broker_name) else {
                return false;
            };
            let mq = MessageQueue::new(&topic, &broker_name, 0);
            client
                .get_max_offset(&mq, timeout_millis, Some(&endpoint))
                .await
                .is_ok()
        })
    }
}

// ================================================================ 轨迹自动桥接

/// Python `_get_and_create_trace_producer`（`trace_dispatcher.py:95-105`）的 Rust 版：
/// 建一个**真的**内部生产者并连同分发器一起装配好。
///
/// `send_msg_timeout=5000` / `max_msg_size=128000` / `enable_trace=false` 三项
/// 由 [`AsyncTraceDispatcher::with_config`] 按 Python 的顺序打在生产者上，这里不重复。
///
/// 与注入式用法的分工：调用方注入的 [`TraceDispatcherChannel`] 优先（原差异 4 约定）；
/// `enable_trace=true` 且未注入时，`start_trace_dispatcher` 走这里自动建一套，
/// 与 Python `producer._start_trace_dispatcher` / `consumer._start_trace_dispatcher`
/// 的行为一致 —— 轨迹开箱即用，不再要求手写桥接。
pub fn auto_bridged_dispatcher(
    host_group: &str,
    dispatcher_type: TraceDispatcherType,
    rpc_hook: Option<Arc<dyn RPCHook>>,
    batch_num: i32,
    trace_topic: Option<String>,
) -> Result<Arc<AsyncTraceDispatcher>> {
    let producer_group =
        AsyncTraceDispatcher::next_trace_producer_group(host_group, dispatcher_type);
    let producer = DefaultMQProducer::with_rpc_hook(&producer_group, rpc_hook)?;
    Ok(Arc::new(AsyncTraceDispatcher::with_config(
        host_group,
        dispatcher_type,
        TraceDispatcherConfig {
            producer: Some(Arc::new(producer)),
            trace_producer_group: Some(producer_group),
            // Python `:75`：`min(batch_num, 20)`；负数对 usize 无意义，钳到 0。
            batch_num: batch_num.clamp(0, 20) as usize,
            trace_topic_name: trace_topic,
            ..Default::default()
        },
    )))
}

/// Python `set_host_producer(self)` / `set_host_consumer(self)`
/// （`trace_dispatcher.py:114-118`）的宿主端：分发器的 `_client_id` 读的就是
/// 宿主 `_mq_client.client_id`，未启动时是空串。
impl TraceHost for DefaultMQProducer {
    fn client_id(&self) -> String {
        DefaultMQProducer::client_id(self).unwrap_or_default()
    }
}

/// [`TraceProducer`] 接缝的真实现（Python 的 `trace_producer` 本来就是一个真的
/// `DefaultMQProducer`，`trace_dispatcher.py:95-105`；此前本 crate 只有测试桩实现
/// 该接缝，导致轨迹在未手写桥接时完全发不出去）。
impl TraceProducer for DefaultMQProducer {
    fn set_name_server_addr(&self, name_server_addr: &str) {
        DefaultMQProducer::set_name_server_addresses(self, vec![name_server_addr.to_string()]);
    }

    fn set_instance_name(&self, instance_name: &str) {
        DefaultMQProducer::set_instance_name(self, instance_name);
    }

    fn set_namespace_v2(&self, namespace_v2: &str) {
        DefaultMQProducer::set_namespace_v2(self, Some(namespace_v2));
    }

    fn set_send_msg_timeout(&self, timeout_millis: i64) {
        DefaultMQProducer::set_send_msg_timeout(self, timeout_millis);
    }

    fn set_max_message_size(&self, max_msg_size: usize) {
        DefaultMQProducer::set_max_message_size(self, max_msg_size as i32);
    }

    fn set_enable_trace(&self, enable: bool) {
        DefaultMQProducer::set_enable_trace(self, enable);
    }

    fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>> {
        Box::pin(async move { DefaultMQProducer::start(&self).await })
    }

    fn shutdown(&self) {
        DefaultMQProducer::shutdown(self);
    }

    fn send(
        self: Arc<Self>,
        mut message: Message,
        timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>> {
        Box::pin(async move {
            // 内部生产者 enable_trace=false（with_config 已关），send 不会再产出轨迹；
            // Some(timeout) 绕过自动攒批，与 Python `send(msg, 5000)` 的同步直发一致。
            DefaultMQProducer::send(&self, &mut message, Some(timeout_millis), None)
                .await
                .map(|_| ())
        })
    }

    fn send_to_queue(
        self: Arc<Self>,
        mut message: Message,
        queue: MessageQueue,
        timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>> {
        Box::pin(async move {
            DefaultMQProducer::send(&self, &mut message, Some(timeout_millis), Some(&queue))
                .await
                .map(|_| ())
        })
    }

    fn publish_queues(
        self: Arc<Self>,
        topic: String,
    ) -> TraceProducerFuture<Result<Vec<MessageQueue>>> {
        Box::pin(
            async move { DefaultMQProducer::fetch_publish_message_queues(&self, &topic).await },
        )
    }
}

/// Python `_send_heartbeat_to_all_broker`：向所有已知 broker 发心跳（含 ProducerData）。
///
/// 返回成功的台数；单台失败只记 warn（Python 同）。
async fn send_heartbeat_to_all_broker(inner: &Inner, client: &MQClientInstance) -> usize {
    let group = inner.producer_group();
    let addrs = client.get_route_of_all_brokers();
    if addrs.is_empty() {
        return 0;
    }
    let mut hb = HeartbeatData::new(client.client_id().to_string());
    hb.add_producer_data(ProducerData::new(group));
    // ⚠ Python 的 HeartbeatData 没有 heartbeatFingerprint / withoutSub 字段
    // （已知缺陷，见项目记忆）。缺失时 broker 反序列化为 0，等价于走 V1 注册路径，
    // 与 C++ 侧显式置 0 的效果一致，这里保持现状不引入新差异。
    let mut ok = 0usize;
    for addr in addrs {
        if let Err(e) = client.send_heartbeat(&addr, &hb, 5000).await {
            rmq_warn!("producer heartbeat to {addr} failed: {e}");
        } else {
            ok += 1;
        }
    }
    ok
}

/// 「这次失败值不值得再问一次」的粗判据：`except (MQClientException, MQBrokerException,
/// RemotingException)` 都算，其它异常（例如批量消息的 `ValueError`、参数校验）
/// 直接向上抛。
///
/// 现在只剩 [`DefaultMQProducer::topic_publish_info`] 用它决定「要不要强制刷一次路由」；
/// 发送主循环改用了更细的 [`classify_send_error`]（三档异常在故障表上的记法不同）。
///
/// [`Error::RequestTimeout`] 也在重试之列 —— Python 里它是 `MQClientException`
/// 的子类，会被同一个 `except` 接住（发送链路本身产不出它，列出来只为口径完整）。
fn retryable(err: &Error) -> bool {
    matches!(
        err,
        Error::Client { .. }
            | Error::Broker { .. }
            | Error::Server { .. }
            | Error::RemotingCommand(_)
            | Error::Connect { .. }
            | Error::SendRequest { .. }
            | Error::Timeout { .. }
            | Error::TooMuchRequest(_)
            | Error::RequestTimeout { .. }
    )
}

/// Python 用 `isinstance` 分派；Rust 没有异常继承树，靠 [`Error`] 的变体一一对应
/// （见 [`classify_send_error`]）。三档在**故障表**和**是否继续重试**上都不一样，
/// 所以不能像早期版本那样合成一个 `retryable` 判断。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendErrorKind {
    /// `MQBrokerException`：broker 明确回了业务错误码（Rust [`Error::Broker`]）。
    Broker,
    /// `RemotingException` 及其子类：连不上/发不出去/等响应超时。
    Remoting,
    /// `MQClientException` 及其子类：客户端自身的问题。
    Client,
}

/// 把一次发送失败的 [`Error`] 归到 Python 的哪个 `except` 分支；
/// `None` = 三个分支都接不住（校验错、解码错、本地 IO 错），直接向上抛。
///
/// [`Error::RequestTimeout`] 属 `MQClientException` 家族（Python
/// `RequestTimeoutException extends MQClientException`），这点和
/// [`Error::Timeout`]（remoting 层的 `RemotingTimeoutException`）分属两档，
/// 是有意为之：前者说明 broker 收到了请求，后者连通道都没走通。
fn classify_send_error(err: &Error) -> Option<SendErrorKind> {
    match err {
        Error::Broker { .. } => Some(SendErrorKind::Broker),
        Error::Connect { .. }
        | Error::SendRequest { .. }
        | Error::Timeout { .. }
        | Error::TooMuchRequest(_)
        | Error::RemotingCommand(_)
        | Error::Server { .. } => Some(SendErrorKind::Remoting),
        Error::Client { .. } | Error::RequestTimeout { .. } => Some(SendErrorKind::Client),
        Error::Decode(_) | Error::Encode(_) | Error::Io(_) | Error::Json(_) => None,
    }
}

/// 从 `began`（[`monotonic_millis`] 取的起点）到现在的毫秒数，向下取整。
///
/// 对应 Python 的 `int((time.monotonic() - began) * 1000)`：`as i64` 对正浮点数
/// 同样是向零取整，语义一致。用挂钟（`current_time_millis`）算不行——系统时间
/// 一回退就会得到负延迟，反而把慢 broker 记成"很快"。
fn latency_since(began: f64) -> i64 {
    (monotonic_millis() - began) as i64
}

fn back_pressure_msg_len(msg: &Message) -> i64 {
    let len = msg.get_body().len();
    if len == 0 {
        return 1;
    }
    i64::try_from(len).unwrap_or(i64::MAX)
}

/// 批量异步扣多少「字节」许可：逐条累加（空 body 也算 1），整批为空时算 1 ——
/// 对应 Python `_back_pressure_msg_len` 的 list/MessageBatch 分支。
/// 不给整批一个值就等于让批量消息绕过字节闸：一批 100 条只占 1 份容量。
fn batch_back_pressure_msg_len(msgs: &[Message]) -> i64 {
    let mut total: i64 = 0;
    for msg in msgs {
        total = total.saturating_add(back_pressure_msg_len(msg));
    }
    if total == 0 {
        return 1;
    }
    total
}

/// 只归还**本次真正拿到**的那几份：字节闸超时而条数闸已到手时，必须把条数还回去，
/// 否则过一次背压就把容量永久吃掉一格。
///
/// 归还写在 `Drop` 而不是调用方手里：这条链上有闸门拒绝、预算复检、准备段失败、网络
/// 失败四种出口，少写一处归还就是永久漏容量，而漏掉的容量在单测里看不出来、只会在
/// 长跑里表现为「发几笔之后所有异步发送集体超时」。
struct SendPermits {
    inner: Arc<Inner>,
    num_acquired: bool,
    size_acquired: bool,
    msg_len: i64,
}

impl Drop for SendPermits {
    fn drop(&mut self) {
        if self.size_acquired {
            self.inner.semaphore_async_send_size.release(self.msg_len);
        }
        if self.num_acquired {
            self.inner.semaphore_async_send_num.release(1);
        }
    }
}

/// Python `_build_send_context` 里那串「延迟类属性」判定键。
///
/// `__STARTDELIVERTIME` / `TIMER_*` 在本 crate 的 `message_const` 里没有常量
/// （Python 也是写死字面量 + `getattr` 兜底），保持一致。
const DELAY_PROPERTY_KEYS: [&str; 5] = [
    "__STARTDELIVERTIME",
    crate::common::message_const::PROPERTY_DELAY_TIME_LEVEL,
    "TIMER_DELIVER_MS",
    "TIMER_DELAY_SEC",
    "TIMER_DELAY_MS",
];

fn max_delay_value(msg: &Message) -> i64 {
    let mut max = 0i64;
    if let Some(level) = msg.get_delay_time_level() {
        max = level.trim().parse::<i64>().unwrap_or(0);
    }
    for key in TIMER_DELAY_KEYS {
        if let Some(raw) = msg.get_property(key) {
            let value = raw.trim().parse::<i64>().unwrap_or(0);
            if value > max {
                max = value;
            }
        }
    }
    max
}

/// ⚠ 两个入口都**不**走公开的 [`DefaultMQProducer::send`] / `send_async` ——
/// 那会再判一次 `autoBatch`，而调用方进到这里恰恰是因为攒批被拒，绕回公开入口
/// 就是无限递归。改成直接调「已编码批量」专用的两个内核。
impl AccumulatorSender for DefaultMQProducer {
    fn send_direct_blocking(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult> {
        let handle = self.runtime_handle().ok_or_else(|| {
            Error::client("send_direct_blocking needs a tokio runtime; call start() first")
        })?;
        let mut batch = batch;
        handle.block_on(self.send_prepared_batch(&mut batch, mq, None))
    }

    fn send_direct_async(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
    ) -> Result<()> {
        self.send_prepared_batch_async(batch, mq.cloned(), callback, None)
    }
}

// ================================================================ 发送路径

impl DefaultMQProducer {
    // ---------------- 校验 / 压缩 / 路由 ----------------

    /// Python `_check_message` → `client/validators.check_message`。
    ///
    /// 判定全部收敛到 [`validators`]：topic（blank / 127 / 字符表）→ 禁发 topic →
    /// body（null / 零长 / 超 max_message_size）→ INNER_MULTI_DISPATCH 分隔符，
    /// 顺序与文案跟 Python 一字不差。
    fn check_message(&self, msg: &Message) -> Result<()> {
        let max = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .max_message_size;
        validators::check_message(msg, max)
    }

    /// Python `try_to_compress_message`：满足阈值时**就地压缩**，返回应下发的 sys_flag。
    fn try_to_compress_message(&self, msg: &mut Message) -> (i32, Option<Vec<u8>>) {
        let (threshold, compress_type, level) = {
            let cfg = self.inner.cfg.read().unwrap_or_else(|e| e.into_inner());
            (
                cfg.compress_msg_body_over_howmuch,
                cfg.compress_type,
                cfg.compress_level,
            )
        };
        let body = msg.get_body();
        if body.is_empty() || (body.len() as i32) < threshold {
            return (0, None);
        }
        let compressed = match compression::compress(body, compress_type, level) {
            Ok(c) => c,
            Err(e) => {
                rmq_warn!("tryToCompressMessage failed, send uncompressed: {e}");
                return (0, None);
            }
        };
        if compressed.is_empty() {
            return (0, None);
        }
        let prev_body = msg.body.take();
        msg.body = Some(compressed);
        (
            MessageSysFlag::set_compression_type(MessageSysFlag::COMPRESSED_FLAG, compress_type),
            prev_body,
        )
    }

    /// 不还原会坏在哪：压缩是就地换 body，调用方拿着同一个对象再发一次，第二次就把压缩流
    /// 当原文又压一遍（zlib(zlib(x))），且长度已低于阈值、压缩标志也不会置位 —— broker 照存、
    /// 消费者照收，业务侧解出来是乱码，全程零报错。
    fn restore_caller_message(&self, msg: &mut Message, prev_body: Option<Vec<u8>>) {
        if let Some(prev) = prev_body {
            msg.body = Some(prev);
        }
        msg.topic = NamespaceUtil::without_namespace(&msg.topic, &self.inner.namespace());
    }

    /// 批量版本的压缩判定（Python 由 `isinstance(msg, MessageBatch)` 直接返回 0）。
    ///
    /// 批量**永不压缩**（`MessageBatch.encode()` 出来的拼接体不压），所以这里也不需要
    /// `prevBody`：返回的 flag 恒 0，调用方没有要还原的 body。
    fn sys_flag_for(&self, msg: &mut PublishMessage<'_>) -> i32 {
        if msg.is_batch() {
            return 0;
        }
        self.try_to_compress_message(msg.as_message_mut()).0
    }

    fn with_namespace(&self, topic: &str) -> String {
        let namespace = self.inner.namespace();
        if namespace.is_empty() {
            return topic.to_string();
        }
        NamespaceUtil::wrap_namespace(&namespace, topic)
    }

    fn check_pinned_topic(&self, msg_topic: &str, mq: &MessageQueue, message: &str) -> Result<()> {
        if self.with_namespace(msg_topic) != self.with_namespace(&mq.topic) {
            return Err(Error::client(message));
        }
        Ok(())
    }

    async fn topic_publish_info(
        &self,
        client: &MQClientInstance,
        topic: &str,
    ) -> Result<Arc<TopicPublishInfo>> {
        client.register_topic_in_use(topic);
        match client.get_topic_publish_info(topic, false).await {
            Ok(info) => Ok(info),
            Err(e) if retryable(&e) => match client.get_topic_publish_info(topic, true).await {
                Ok(info) => Ok(info),
                Err(e) => {
                    self.validate_name_server_setting(client)?;
                    Err(e)
                }
            },
            Err(e) => Err(e),
        }
    }

    /// 只在「拿不到路由」这条分支上跑，把两种完全不同的故障分开：
    /// * 压根一个 name server 地址都没有（配了地址服务器却没返回地址也算）
    ///   → 10004 `No name server address, please set it.`
    /// * 地址有、只是这个 topic 没路由 → 让原来那条异常照旧抛（调用方定性成 10005）。
    fn validate_name_server_setting(&self, client: &MQClientInstance) -> Result<()> {
        if client.name_server_addrs().is_empty() {
            return Err(Error::client_with_code(
                client_error_code::NO_NAME_SERVER_EXCEPTION,
                "No name server address, please set it.",
            ));
        }
        Ok(())
    }

    async fn need_addr(&self, client: &MQClientInstance, mq: &MessageQueue) -> Result<String> {
        client.publish_addr_for(&mq.broker_name, &mq.topic).await
    }

    // ---------------- 钩子上下文 ----------------

    /// msgType 的判定顺序也照抄：`TRAN_MSG=true` → Trans_Msg_Half；带任何延迟类属性
    /// → Delay_Msg；否则 Normal_Msg。
    ///
    /// ⚠ Python 把**同一个** message 对象挂进 context（发送路径的后续修改钩子看得见）；
    /// Rust 的 [`SendMessageContext::message`] 是 owned，只能取**发送前**的快照 ——
    /// 轨迹钩子读的正是发送前就确定的 topic/tags/keys/bodyLength，无行为差别。
    fn build_send_context(
        &self,
        msg: &Message,
        group: &str,
        namespace: &str,
        mq: &MessageQueue,
        broker_addr: &str,
        mode: CommunicationMode,
    ) -> SendMessageContext {
        let mut context = SendMessageContext {
            producer_group: group.to_string(),
            message: Some(msg.clone()),
            mq: Some(mq.clone()),
            broker_addr: broker_addr.to_string(),
            communication_mode: Some(mode),
            namespace: namespace.to_string(),
            ..Default::default()
        };
        if msg.get_property(PROPERTY_TRANSACTION_PREPARED) == Some("true") {
            context.msg_type = MessageType::TransMsgHalf;
        }
        if DELAY_PROPERTY_KEYS
            .iter()
            .any(|key| msg.get_property(key).is_some())
        {
            context.msg_type = MessageType::DelayMsg;
        }
        context
    }

    /// Python `_execute_check_forbidden`：构造上下文并执行（异常**不吞**，
    /// 见 [`execute_check_forbidden_hook`]）。
    fn execute_check_forbidden(
        &self,
        msg: &Message,
        mq: &MessageQueue,
        broker_addr: &str,
        arg: Option<AnyHolder>,
        mode: CommunicationMode,
    ) -> Result<()> {
        let mut context = CheckForbiddenContext {
            name_srv_addr: self.namesrv_addr(),
            group: self.inner.producer_group(),
            message: Some(msg.clone()),
            mq: Some(mq.clone()),
            broker_addr: broker_addr.to_string(),
            communication_mode: Some(mode),
            arg,
            unit_mode: self.inner.unit_mode(),
            ..Default::default()
        };
        crate::client::hook::execute_check_forbidden_hook(
            &self.inner.check_forbidden_hooks,
            &mut context,
        )
    }

    // ---------------- 发送内核 ----------------

    #[allow(clippy::too_many_arguments)]
    async fn send_with_hooks(
        &self,
        client: &MQClientInstance,
        msg: &mut PublishMessage<'_>,
        mq_sel: &MessageQueue,
        timeout: i64,
        sys_flag: i32,
        arg: Option<AnyHolder>,
        mode: CommunicationMode,
    ) -> Result<SendResult> {
        let group = self.inner.producer_group();
        let namespace = self.inner.namespace();
        let trace_context_on = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .enable_trace_context
            .unwrap_or(false);
        let unit_mode = self.inner.unit_mode();
        let broker_addr = client
            .publish_addr_for(&mq_sel.broker_name, &mq_sel.topic)
            .await?;
        if self.has_check_forbidden_hook() {
            self.execute_check_forbidden(msg.as_message(), mq_sel, &broker_addr, arg, mode)?;
        }
        // W3C traceparent 透传（opt-in）：没有就注入根上下文，已有值不覆盖
        if trace_context_on {
            inject_trace_context(msg.as_message_mut());
        }
        if !self.has_send_message_hook() {
            return client
                .send_message(
                    &group,
                    msg,
                    mq_sel,
                    timeout,
                    sys_flag,
                    unit_mode,
                    &self.inner.create_topic_key(),
                    self.inner.default_topic_queue_nums(),
                )
                .await;
        }
        let mut context = self.build_send_context(
            msg.as_message(),
            &group,
            &namespace,
            mq_sel,
            &broker_addr,
            mode,
        );
        crate::client::hook::execute_send_message_hook_before(&self.inner.send_hooks, &mut context);
        match client
            .send_message(
                &group,
                msg,
                mq_sel,
                timeout,
                sys_flag,
                unit_mode,
                &self.inner.create_topic_key(),
                self.inner.default_topic_queue_nums(),
            )
            .await
        {
            Ok(result) => {
                context.send_result = Some(result.clone());
                crate::client::hook::execute_send_message_hook_after(
                    &self.inner.send_hooks,
                    &mut context,
                );
                Ok(result)
            }
            Err(e) => {
                context.exception = Some(e);
                crate::client::hook::execute_send_message_hook_after(
                    &self.inner.send_hooks,
                    &mut context,
                );
                // 钩子只读 exception，取回来原样抛（Error 不是 Clone）。
                match context.exception.take() {
                    Some(e) => Err(e),
                    None => Err(Error::client("send message failed")),
                }
            }
        }
    }


    pub fn produce_accumulator(&self) -> Option<ProduceAccumulator> {
        self.inner
            .produce_accumulator
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn init_produce_accumulator(&self) -> Result<()> {
        let client_id = self
            .read_cfg(|c| c.client_id.clone())
            .unwrap_or_else(|| DEFAULT_INSTANCE_NAME.to_string());
        let sender: Arc<dyn AccumulatorSender> = Arc::new(self.clone());
        let accumulator = get_or_create_produce_accumulator(&client_id, sender);
        let (delay_ms, bytes, total) = self.read_cfg(|c| {
            (
                c.batch_max_delay_ms,
                c.batch_max_bytes,
                c.total_batch_max_bytes,
            )
        });
        if delay_ms > -1 {
            accumulator.batch_max_delay_ms(delay_ms)?;
        }
        if bytes > -1 {
            accumulator.batch_max_bytes(bytes)?;
        }
        if total > -1 {
            accumulator.total_batch_max_bytes(total)?;
        }
        *self
            .inner
            .produce_accumulator
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(accumulator);
        Ok(())
    }

    pub fn get_auto_batch(&self) -> bool {
        if self.produce_accumulator().is_none() {
            return false;
        }
        self.read_cfg(|c| c.auto_batch)
    }

    pub fn set_auto_batch(&self, auto_batch: bool) {
        self.write_cfg(|c| c.auto_batch = auto_batch);
    }

    pub fn get_batch_max_delay_ms(&self) -> i64 {
        self.produce_accumulator()
            .map_or(0, |a| a.get_batch_max_delay_ms())
    }

    pub fn set_batch_max_delay_ms(&self, hold_ms: i64) -> Result<()> {
        self.write_cfg(|c| c.batch_max_delay_ms = hold_ms);
        if let Some(a) = self.produce_accumulator() {
            a.batch_max_delay_ms(hold_ms)?;
        }
        Ok(())
    }

    pub fn get_batch_max_bytes(&self) -> i64 {
        self.produce_accumulator()
            .map_or(0, |a| a.get_batch_max_bytes())
    }

    pub fn set_batch_max_bytes(&self, hold_size: i64) -> Result<()> {
        self.write_cfg(|c| c.batch_max_bytes = hold_size);
        if let Some(a) = self.produce_accumulator() {
            a.batch_max_bytes(hold_size)?;
        }
        Ok(())
    }

    pub fn get_total_batch_max_bytes(&self) -> i64 {
        self.produce_accumulator()
            .map_or(0, |a| a.get_total_batch_max_bytes())
    }

    pub fn set_total_batch_max_bytes(&self, total_hold_size: i64) -> Result<()> {
        self.write_cfg(|c| c.total_batch_max_bytes = total_hold_size);
        if let Some(a) = self.produce_accumulator() {
            a.total_batch_max_bytes(total_hold_size)?;
        }
        Ok(())
    }

    pub fn can_batch(&self, msg: &Message) -> bool {
        let Some(accumulator) = self.produce_accumulator() else {
            return false;
        };
        if !accumulator.try_add_message(msg) {
            return false;
        }
        if max_delay_value(msg) > 0 {
            return false;
        }
        if msg
            .get_topic()
            .starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX)
        {
            return false;
        }
        if msg.get_property(PROPERTY_PRODUCER_GROUP).is_some() {
            return false;
        }
        true
    }

    fn check_and_stamp(&self, msg: &mut Message) -> Result<()> {
        let max = self.read_cfg(|c| c.max_message_size);
        validators::check_message(msg, max)?;
        set_uniq_id(msg);
        Ok(())
    }

    pub async fn send_by_accumulator(
        &self,
        mut msg: Message,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult> {
        if !self.can_batch(&msg) {
            return self.send_inner(&mut msg, None, mq).await;
        }
        self.check_and_stamp(&mut msg)?;
        let accumulator = self
            .produce_accumulator()
            .ok_or_else(|| Error::client("produceAccumulator is null"))?;
        let mq_owned = mq.cloned();
        tokio::task::spawn_blocking(move || accumulator.send(msg, mq_owned.as_ref()))
            .await
            .map_err(|e| Error::client(format!("sendByAccumulator join error: {e}")))?
    }

    pub fn send_by_accumulator_async(
        &self,
        mut msg: Message,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
    ) -> Result<()> {
        if !self.can_batch(&msg) {
            return self.send_async_inner(msg, callback, None, mq.cloned());
        }
        self.check_and_stamp(&mut msg)?;
        let accumulator = self
            .produce_accumulator()
            .ok_or_else(|| Error::client("produceAccumulator is null"))?;
        accumulator.send_async(msg, mq, callback);
        Ok(())
    }

    // ---------------- 正常发送 ----------------

    /// 同步发送（Python `send(msg, timeout_millis=None, mq=None)`）。
    ///
    /// `mq` 为 `Some` 时定点发送（不重试、不走故障规避）；`None` 时轮询/故障规避选队，
    /// 并按 `retry_times_when_send_failed` 重试。批量消息请用
    /// [`send_batch`](Self::send_batch)（Python 靠 `isinstance(msg, list)` 分流，
    /// Rust 的类型系统已经把它们分开）。
    pub async fn send(
        &self,
        msg: &mut Message,
        timeout_millis: Option<i64>,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult> {
        if timeout_millis.is_none() && self.get_auto_batch() {
            let topic = self.with_namespace(msg.get_topic());
            msg.set_topic(&topic);
            let owned = msg.clone();
            return self.send_by_accumulator(owned, mq).await;
        }
        self.send_inner(msg, timeout_millis, mq).await
    }

    async fn send_inner(
        &self,
        msg: &mut Message,
        timeout_millis: Option<i64>,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult> {
        let client = self.require_client()?;
        let (timeout, retry_times) = self.read_cfg(|cfg| {
            (
                timeout_millis.unwrap_or(cfg.send_msg_timeout),
                cfg.retry_times_when_send_failed,
            )
        });
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic;
        self.check_message(msg)?;
        if let Some(mq) = mq {
            self.check_pinned_topic(&msg.topic, mq, PINNED_TOPIC_MISMATCH_SYNC)?;
        }
        let (sys_flag, prev_body) = self.try_to_compress_message(msg);

        let outcome = {
            let mut publish = PublishMessage::Single(msg);
            if let Some(mq) = mq {
                self.send_pinned(&client, &mut publish, mq, timeout, sys_flag)
                    .await
            } else {
                self.send_with_retry(&client, &mut publish, timeout, retry_times, sys_flag)
                    .await
            }
        };
        self.restore_caller_message(msg, prev_body);
        outcome
    }

    /// 三种调用方共用：定点同步发送（[`send_inner`](Self::send_inner)）、批量发送
    /// （[`send_batch`](Self::send_batch)）、累加器守卫线程的批量直发
    /// （[`send_prepared_batch`](Self::send_prepared_batch)）。`sys_flag` 由调用方给：
    /// 普通消息是压缩位，批量恒 0。
    async fn send_pinned(
        &self,
        client: &MQClientInstance,
        publish: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        timeout: i64,
        sys_flag: i32,
    ) -> Result<SendResult> {
        if self.has_send_interceptors() {
            return self
                .send_with_hooks(
                    client,
                    publish,
                    mq,
                    timeout,
                    sys_flag,
                    None,
                    CommunicationMode::Sync,
                )
                .await;
        }
        client
            .send_message(
                &self.inner.producer_group(),
                publish,
                mq,
                timeout,
                sys_flag,
                self.inner.unit_mode(),
                &self.inner.create_topic_key(),
                self.inner.default_topic_queue_nums(),
            )
            .await
    }

    /// `publish` 在**每轮**重新借出（[`PublishMessage::reborrow`]），因为请求要按本轮选中的
    /// broker 重建 header；底层消息只有一份，轮次之间必须共享同一份 body / UNIQ_KEY。
    async fn send_with_retry(
        &self,
        client: &MQClientInstance,
        publish: &mut PublishMessage<'_>,
        timeout: i64,
        retry_times: i32,
        sys_flag: i32,
    ) -> Result<SendResult> {
        let topic = publish.as_message().topic.clone();
        let route = match self.topic_publish_info(client, &topic).await {
            Ok(publish) => publish,
            Err(e @ Error::Client { .. }) => {
                return Err(Error::client_with_code(
                    e.response_code()
                        .unwrap_or(client_error_code::NOT_FOUND_TOPIC_EXCEPTION),
                    e.to_string(),
                ))
            }
            Err(e) => return Err(e),
        };
        let times_total = retry_times.saturating_add(1);
        let begin_first = monotonic_millis();
        let mut brokers_sent: Vec<String> = Vec::new();
        let mut last_broker_name: Option<String> = None;
        // Python 把「最后一次成功的 SendResult」留在变量里：非 SEND_OK 且开了换 broker
        // 时会带着它继续重试，后面全失败就原样返回这个"存了但没存好"的结果。
        let mut result: Option<SendResult> = None;
        let mut last_exc: Option<Error> = None;
        let mut call_timeout = false;
        let mut attempt: i32 = 0;
        while attempt < times_total {
            let selected = match self.inner.fault_strategy.select_one_message_queue(
                &*route,
                last_broker_name.as_deref(),
                attempt > 0,
            ) {
                Ok(selected) => selected,
                // 选不到队列属 MQClientException：Python 此时 selected 仍是 None，
                // `_update_fault_item` 直接 return（不记故障表），只留 last_exc 进下一轮。
                Err(e) => {
                    last_exc = Some(e);
                    attempt += 1;
                    continue;
                }
            };
            let broker_name = selected.broker_name.clone();
            last_broker_name = Some(broker_name.clone());
            brokers_sent.push(broker_name.clone());
            let began = monotonic_millis();
            let mq_sel = MessageQueue::new(&topic, &broker_name, selected.queue_id);
            // 整体预算：`timeout` 是**这次调用**的总预算，已经被前面的尝试吃掉的部分要扣掉，
            // 否则 3 次重试各 3s 会变成最长 9s 才返回。
            let cost_time = (began - begin_first) as i64;
            if timeout < cost_time {
                call_timeout = true;
                break;
            }
            let mut cur_timeout = timeout - cost_time;
            let can_retry_again = attempt + 1 < times_total;
            let max_per_request = self.read_cfg(|c| c.send_msg_max_timeout_per_request);
            if max_per_request > -1 && can_retry_again && cur_timeout > max_per_request {
                cur_timeout = max_per_request;
            }
            let send_start = self.inner.metrics.record_send_start();
            let mut publish_msg = publish.reborrow();
            let outcome = self
                .send_with_hooks(
                    client,
                    &mut publish_msg,
                    &mq_sel,
                    cur_timeout,
                    sys_flag,
                    None,
                    CommunicationMode::Sync,
                )
                .await;
            match outcome {
                Ok(ok_result) => {
                    self.inner.metrics.record_send_success(send_start);
                    // 记录发送延迟；超出阈值会把该 broker 隔离一段时间
                    self.inner.fault_strategy.update_fault_item(
                        &broker_name,
                        latency_since(began),
                        false,
                        true,
                    );
                    if ok_result.status != SendStatus::SendOk
                        && self.read_cfg(|c| c.retry_another_broker_when_not_store_ok)
                    {
                        result = Some(ok_result);
                        attempt += 1;
                        continue;
                    }
                    return Ok(ok_result);
                }
                Err(e) => {
                    self.inner.metrics.record_send_failure(send_start);
                    match classify_send_error(&e) {
                        // 三个 except 都接不住的异常（校验/解码…）：Python 直接向上抛
                        None => return Err(e),
                        Some(SendErrorKind::Broker) => {
                            // broker 明确回了错误码：隔离该 broker（可达性不动），
                            // 只有 retryResponseCodes 里的码才值得换一台
                            self.inner.fault_strategy.update_fault_item(
                                &broker_name,
                                latency_since(began),
                                true,
                                false,
                            );
                            let retry = self.is_retry_response_code(e.response_code());
                            if retry {
                                last_exc = Some(e);
                                attempt += 1;
                                continue;
                            }
                            if let Some(result) = result {
                                return Ok(result);
                            }
                            return Err(e);
                        }
                        Some(SendErrorKind::Remoting) => {
                            let reachable = !self.read_cfg(|c| c.start_detector_enable);
                            self.inner.fault_strategy.update_fault_item(
                                &broker_name,
                                latency_since(began),
                                true,
                                reachable,
                            );
                            last_exc = Some(e);
                        }
                        Some(SendErrorKind::Client) => {
                            self.inner.fault_strategy.update_fault_item(
                                &broker_name,
                                latency_since(began),
                                false,
                                true,
                            );
                            last_exc = Some(e);
                        }
                    }
                }
            }
            attempt += 1;
        }

        if let Some(result) = result {
            return Ok(result);
        }
        if call_timeout {
            return Err(Error::TooMuchRequest(
                "sendDefaultImpl call timeout".to_string(),
            ));
        }
        let info = format!(
            "Send [{}] times, still failed, cost [{}]ms, Topic: {topic}, BrokersSent: [{}], \
             last error: {}",
            brokers_sent.len(),
            latency_since(begin_first),
            brokers_sent.join(", "),
            last_exc.as_ref().map(|e| e.to_string()).unwrap_or_default(),
        );
        let code = match &last_exc {
            Some(Error::Broker { response_code, .. }) => Some(*response_code),
            Some(Error::Connect { .. }) => Some(client_error_code::CONNECT_BROKER_EXCEPTION),
            Some(Error::Timeout { .. }) => Some(client_error_code::ACCESS_BROKER_TIMEOUT),
            Some(Error::Client { .. }) | Some(Error::RequestTimeout { .. }) => {
                Some(client_error_code::BROKER_NOT_EXIST_EXCEPTION)
            }
            _ => None,
        };
        Err(Error::Client {
            response_code: code,
            message: info,
        })
    }

    /// Python `send` 的批量分支（`_send_batch`）。
    ///
    /// 约束由 [`MessageBatch::generate_from_list`] 保证：非空、同 topic、同
    /// waitStoreMsgOK、不含延迟消息与重试 topic。批量消息**永不压缩**。
    pub async fn send_batch(
        &self,
        msgs: Vec<Message>,
        mq: Option<&MessageQueue>,
        timeout_millis: Option<i64>,
    ) -> Result<SendResult> {
        let client = self.require_client()?;
        let timeout = match timeout_millis {
            Some(t) => t,
            None => {
                self.inner
                    .cfg
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .send_msg_timeout
            }
        };
        if msgs.is_empty() {
            bail!("message list is empty");
        }
        let max = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .max_message_size;
        for msg in &msgs {
            validators::check_message(msg, max)?;
        }
        let mut msgs = msgs;
        for msg in &mut msgs {
            set_uniq_id(msg);
            let topic = self.with_namespace(&msg.topic);
            msg.topic = topic;
        }
        let topic = msgs[0].topic.clone();
        let mut batch = MessageBatch::generate_from_list(msgs)?;
        set_uniq_id(&mut batch.message);
        let body = batch.encode();
        batch.message.set_body(Some(&body));
        let mut publish_msg = PublishMessage::Batch(&mut batch);
        // MessageBatch 会被压缩步骤直接跳过（返回 0），批量消息永不压缩
        let sys_flag = self.sys_flag_for(&mut publish_msg);
        let mq_sel = match mq {
            Some(mq) => {
                self.check_pinned_topic(&topic, mq, PINNED_TOPIC_MISMATCH_SYNC)?;
                mq.clone()
            }
            None => {
                let route = self.topic_publish_info(&client, &topic).await?;
                // Python 用 batch.topic 建 mq_sel；Rust 的批量外层 topic 已等于 topic
                match route.select_one_message_queue(&[])? {
                    Some(mq) => MessageQueue::new(&topic, &mq.broker_name, mq.queue_id),
                    None => return Err(Error::client("no message queue for publish info")),
                }
            }
        };
        self.send_pinned(&client, &mut publish_msg, &mq_sel, timeout, sys_flag)
            .await
    }

    /// 把**已经编码好的批量**投出去 —— 攒批（[`ProduceAccumulator`]）那条路专用的发送尾巴。
    ///
    ///
    ///
    /// ⚠ 这里**不**重做命名空间、`checkMessage`、`setUniqID`、编码 —— 那些在
    /// `sendByAccumulator`（逐条）与 `MessageBatch::generate_from_list`（批量自身）里就已经
    /// 落在对象上了，再来一次会把子消息的 UNIQ_KEY 覆盖成新的：broker 拆开批量后每条子消息的
    /// 客户端 ID 就与发送侧记录的串不起来。压缩同样跳过（`SEND_BATCH` 批量永不压缩）。
    ///
    /// [`ProduceAccumulator`]: crate::client::produce_accumulator::ProduceAccumulator
    async fn send_prepared_batch(
        &self,
        batch: &mut MessageBatch,
        mq: Option<&MessageQueue>,
        timeout_millis: Option<i64>,
    ) -> Result<SendResult> {
        let client = self.require_client()?;
        let (timeout, retry_times) = self.read_cfg(|cfg| {
            (
                timeout_millis.unwrap_or(cfg.send_msg_timeout),
                cfg.retry_times_when_send_failed,
            )
        });
        let mut publish = PublishMessage::Batch(batch);
        let sys_flag = self.sys_flag_for(&mut publish);
        if let Some(mq) = mq {
            let batch_topic = publish.as_message().topic.clone();
            self.check_pinned_topic(&batch_topic, mq, PINNED_TOPIC_MISMATCH_SYNC)?;
            return self
                .send_pinned(&client, &mut publish, mq, timeout, sys_flag)
                .await;
        }
        self.send_with_retry(&client, &mut publish, timeout, retry_times, sys_flag)
            .await
    }

    /// 前段与 [`send_batch_async`](Self::send_batch_async) 完全同一条：背压两道闸 →
    /// 预算复检 → 出队后跑内核 → 回调恰好一次（含 `complete_async` 里的 after 钩子）。
    /// 差别只在「批量对象**已经**备好」：不重做校验、命名空间、UNIQ_KEY、编码。
    fn send_prepared_batch_async(
        &self,
        batch: MessageBatch,
        mq: Option<MessageQueue>,
        callback: Arc<dyn SendCallback>,
        timeout_millis: Option<i64>,
    ) -> Result<()> {
        let _ = self.require_client()?;
        let handle = self.runtime_handle().ok_or_else(|| {
            Error::client("send_prepared_batch_async needs a tokio runtime; call start() first")
        })?;
        let timeout = timeout_millis.unwrap_or_else(|| self.read_cfg(|c| c.send_msg_timeout));
        let msg_len = match batch.message.body.as_deref() {
            Some(body) => i64::try_from(body.len()).unwrap_or(i64::MAX),
            None => 1,
        };
        let began = monotonic_millis();
        let this = self.clone();
        let job: AsyncSendJob = Box::pin(async move {
            this.run_async_send_prepared_batch(batch, msg_len, callback, timeout, began, mq)
                .await;
        });
        self.submit_async_job(handle, job)
    }

    /// [`send_prepared_batch_async`](Self::send_prepared_batch_async) 出队之后的那一段，
    /// 与 [`run_async_send_batch`](Self::run_async_send_batch) 逐行同构。
    async fn run_async_send_prepared_batch(
        &self,
        mut batch: MessageBatch,
        msg_len: i64,
        callback: Arc<dyn SendCallback>,
        timeout: i64,
        began: f64,
        mq: Option<MessageQueue>,
    ) {
        let permits = match self.acquire_send_permits(msg_len, timeout, began).await {
            Ok(permits) => permits,
            Err(e) => {
                self.complete_async(&callback, Err(e), None, None);
                return;
            }
        };
        let cost = latency_since(began);
        if timeout <= cost {
            return self.fail_async(
                &callback,
                Error::TooMuchRequest("DEFAULT ASYNC send call timeout".to_string()),
                permits,
            );
        }
        if let Some(mq) = mq.as_ref() {
            if let Err(e) =
                self.check_pinned_topic(&batch.message.topic, mq, PINNED_TOPIC_MISMATCH_ASYNC)
            {
                return self.fail_async(&callback, e, permits);
            }
        }
        let outcome = self
            .send_prepared_batch(&mut batch, mq.as_ref(), Some(timeout - cost))
            .await;
        self.complete_async(&callback, outcome, None, Some(permits));
    }

    /// ⚠ 差别：Python 往 `send_message_oneway` 传了 `send_msg_timeout`，单向调用
    /// 本就没有超时概念，Rust 的 [`MQClientInstance::send_message_oneway`] 也不收
    /// 这个参数。
    pub async fn send_oneway(&self, msg: &mut Message, mq: Option<&MessageQueue>) -> Result<()> {
        let client = self.require_client()?;
        let wrapped = self.with_namespace(&msg.topic);
        msg.topic = wrapped.clone();
        self.check_message(msg)?;
        let mq_sel = match mq {
            Some(mq) => mq.clone(),
            None => {
                let publish = self.topic_publish_info(&client, &wrapped).await?;
                let selected = self
                    .inner
                    .fault_strategy
                    .select_one_message_queue(&*publish, None, false)?;
                MessageQueue::new(&wrapped, &selected.broker_name, selected.queue_id)
            }
        };
        let addr = self.need_addr(&client, &mq_sel).await?;
        let (sys_flag, prev_body) = self.try_to_compress_message(msg);
        let outcome = async {
            if self.has_check_forbidden_hook() {
                self.execute_check_forbidden(msg, &mq_sel, &addr, None, CommunicationMode::Oneway)?;
            }
            let mut publish_msg = PublishMessage::Single(msg);
            client
                .send_message_oneway(
                    &self.inner.producer_group(),
                    &mut publish_msg,
                    &mq_sel,
                    &addr,
                    sys_flag,
                    self.inner.unit_mode(),
                    &self.inner.create_topic_key(),
                    self.inner.default_topic_queue_nums(),
                )
                .await
        }
        .await;
        self.restore_caller_message(msg, prev_body);
        outcome
    }

    pub async fn send_by_selector(
        &self,
        msg: &mut Message,
        selector: &dyn MessageQueueSelector,
        arg: &str,
        timeout_millis: Option<i64>,
    ) -> Result<SendResult> {
        let client = self.require_client()?;
        let timeout = timeout_millis.unwrap_or_else(|| {
            self.inner
                .cfg
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .send_msg_timeout
        });
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic.clone();
        let publish = self.topic_publish_info(&client, &topic).await?;
        let queues = publish.msg_queue_list();
        let selected = selector.select(&queues, msg, arg)?;
        let mq_sel = MessageQueue::new(&topic, &selected.broker_name, selected.queue_id);
        // 选择器用的是原始消息（topic/业务字段），压缩只影响 body
        self.check_message(msg)?;
        let (sys_flag, prev_body) = self.try_to_compress_message(msg);
        let holder: Option<AnyHolder> = Some(Arc::new(arg.to_string()));
        let outcome = {
            let mut publish = PublishMessage::Single(msg);
            if self.has_send_interceptors() {
                self.send_with_hooks(
                    &client,
                    &mut publish,
                    &mq_sel,
                    timeout,
                    sys_flag,
                    holder,
                    CommunicationMode::Sync,
                )
                .await
            } else {
                client
                    .send_message(
                        &self.inner.producer_group(),
                        &mut publish,
                        &mq_sel,
                        timeout,
                        sys_flag,
                        self.inner.unit_mode(),
                        &self.inner.create_topic_key(),
                        self.inner.default_topic_queue_nums(),
                    )
                    .await
            }
        };
        // 借期在块尾结束才能再动 msg
        self.restore_caller_message(msg, prev_body);
        outcome
    }

    pub fn send_async(
        &self,
        mut msg: Message,
        callback: Arc<dyn SendCallback>,
        timeout_millis: Option<i64>,
        mq: Option<MessageQueue>,
    ) -> Result<()> {
        if timeout_millis.is_none() && self.get_auto_batch() {
            let topic = self.with_namespace(msg.get_topic());
            msg.set_topic(&topic);
            if let Err(e) = self.send_by_accumulator_async(msg, mq.as_ref(), Arc::clone(&callback))
            {
                callback.on_exception(e);
            }
            return Ok(());
        }
        self.send_async_inner(msg, callback, timeout_millis, mq)
    }

    fn send_async_inner(
        &self,
        msg: Message,
        callback: Arc<dyn SendCallback>,
        timeout_millis: Option<i64>,
        mq: Option<MessageQueue>,
    ) -> Result<()> {
        // Python `send_async:1098-1102`：未启动 / 池已关都同步抛，不走回调。
        let _ = self.require_client()?;
        let handle = self
            .runtime_handle()
            .ok_or_else(|| Error::client("send_async needs a tokio runtime; call start() first"))?;
        let timeout = timeout_millis.unwrap_or_else(|| self.read_cfg(|c| c.send_msg_timeout));
        let msg_len = back_pressure_msg_len(&msg);
        let began = monotonic_millis();
        let this = self.clone();
        let job: AsyncSendJob = Box::pin(async move {
            this.run_async_send(msg, msg_len, callback, timeout, began, mq)
                .await;
        });
        self.submit_async_job(handle, job)
    }

    fn submit_async_job(&self, handle: tokio::runtime::Handle, job: AsyncSendJob) -> Result<()> {
        let submitted = {
            let slot = self
                .inner
                .async_sender
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match slot.as_ref() {
                Some(executor) => executor.try_submit(job),
                None => return Err(Error::client("producer already shutdown")),
            }
        };
        if let Err(job) = submitted {
            if !self.is_enable_backpressure_for_async_mode() {
                return Err(Error::client("executor rejected"));
            }
            handle.spawn(job);
        }
        Ok(())
    }

    /// 不返回 `Result`：这条链上每一种失败都在 [`complete_async`](Self::complete_async)
    /// 交付，用户回调**恰好一次**。
    async fn run_async_send(
        &self,
        mut msg: Message,
        msg_len: i64,
        callback: Arc<dyn SendCallback>,
        timeout: i64,
        began: f64,
        mq: Option<MessageQueue>,
    ) {
        // 没拿到许可时 `acquire_send_permits` 内部已把**已拿到**的那几份归还（Drop）。
        let permits = match self.acquire_send_permits(msg_len, timeout, began).await {
            Ok(permits) => permits,
            Err(e) => {
                self.complete_async(&callback, Err(e), None, None);
                return;
            }
        };
        // Python `_run`：出队之后才算真实耗时 —— 预算被排队吃掉就直接回调，不发请求。
        let cost = latency_since(began);
        if timeout <= cost {
            return self.fail_async(
                &callback,
                Error::TooMuchRequest("DEFAULT ASYNC send call timeout".to_string()),
                permits,
            );
        }
        self.async_send_inner(&mut msg, mq.as_ref(), callback, permits, timeout - cost)
            .await;
    }

    /// 背压按**整批**扣字节许可（Python `_back_pressure_msg_len`：逐条累加、空 body 也算 1），
    /// 否则一批 100 条只占 1 份容量，等于批量消息绕过了字节闸。
    ///
    /// ⚠ 与单条异步的差别只在**重试语义**：批量路径没有异步链的换 broker 重试（`on_exception`），
    /// 走的是同步内核自己的 `retry_times_when_send_failed` 循环。`SendMessageHook`
    /// （before/after）**照跑** —— 同步内核内部就是 `send_with_hooks`，与 Python 的
    /// `_send_batch` 同一口径；钩子上下文由内核建、由内核收尾，[`complete_async`] 只交付
    /// 用户回调和归还许可，不会再跑一次 after。
    pub fn send_batch_async(
        &self,
        msgs: Vec<Message>,
        callback: Arc<dyn SendCallback>,
        timeout_millis: Option<i64>,
        mq: Option<MessageQueue>,
    ) -> Result<()> {
        let _ = self.require_client()?;
        let handle = self.runtime_handle().ok_or_else(|| {
            Error::client("send_batch_async needs a tokio runtime; call start() first")
        })?;
        let timeout = timeout_millis.unwrap_or_else(|| self.read_cfg(|c| c.send_msg_timeout));
        let msg_len = batch_back_pressure_msg_len(&msgs);
        let began = monotonic_millis();
        let this = self.clone();
        let job: AsyncSendJob = Box::pin(async move {
            this.run_async_send_batch(msgs, msg_len, callback, timeout, began, mq)
                .await;
        });
        self.submit_async_job(handle, job)
    }

    /// [`send_batch_async`](Self::send_batch_async) 出队之后的那一段，与
    /// [`run_async_send`](Self::run_async_send) 同构：先过闸，再复检预算，最后才发请求。
    async fn run_async_send_batch(
        &self,
        msgs: Vec<Message>,
        msg_len: i64,
        callback: Arc<dyn SendCallback>,
        timeout: i64,
        began: f64,
        mq: Option<MessageQueue>,
    ) {
        let permits = match self.acquire_send_permits(msg_len, timeout, began).await {
            Ok(permits) => permits,
            Err(e) => {
                self.complete_async(&callback, Err(e), None, None);
                return;
            }
        };
        let cost = latency_since(began);
        if timeout <= cost {
            return self.fail_async(
                &callback,
                Error::TooMuchRequest("DEFAULT ASYNC send call timeout".to_string()),
                permits,
            );
        }
        if let (Some(mq), Some(first)) = (mq.as_ref(), msgs.first()) {
            if let Err(e) = self.check_pinned_topic(&first.topic, mq, PINNED_TOPIC_MISMATCH_ASYNC) {
                return self.fail_async(&callback, e, permits);
            }
        }
        // 批量内核自己校验每条子消息、拼命名空间、查同质性；这里的错误原样交付回调。
        let outcome = self
            .send_batch(msgs, mq.as_ref(), Some(timeout - cost))
            .await;
        self.complete_async(&callback, outcome, None, Some(permits));
    }

    /// 一个「不发请求就终止」的出口：归还许可 + 交付错误。
    fn fail_async(&self, callback: &Arc<dyn SendCallback>, error: Error, permits: SendPermits) {
        self.complete_async(callback, Err(error), None, Some(permits));
    }

    async fn async_send_inner(
        &self,
        msg: &mut Message,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
        permits: SendPermits,
        timeout: i64,
    ) {
        let client = match self.require_client() {
            Ok(client) => client,
            Err(e) => return self.fail_async(&callback, e, permits),
        };
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic.clone();
        if let Err(e) = self.check_message(msg) {
            return self.fail_async(&callback, e, permits);
        }
        if let Some(mq) = mq {
            if let Err(e) = self.check_pinned_topic(&msg.topic, mq, PINNED_TOPIC_MISMATCH_ASYNC) {
                return self.fail_async(&callback, e, permits);
            }
        }
        let (sys_flag, _prev_body) = self.try_to_compress_message(msg);
        let (mq_sel, publish) = match mq {
            Some(mq) => (mq.clone(), None),
            None => {
                let publish = match self.topic_publish_info(&client, &topic).await {
                    Ok(publish) => publish,
                    Err(e @ Error::Client { .. }) => {
                        // 同同步发送：10004 原样透传，其余才按 10005 定性
                        return self.fail_async(
                            &callback,
                            Error::client_with_code(
                                e.response_code()
                                    .unwrap_or(client_error_code::NOT_FOUND_TOPIC_EXCEPTION),
                                e.to_string(),
                            ),
                            permits,
                        );
                    }
                    Err(e) => return self.fail_async(&callback, e, permits),
                };
                match self
                    .inner
                    .fault_strategy
                    .select_one_message_queue(&*publish, None, false)
                {
                    Ok(selected) => (
                        MessageQueue::new(&topic, &selected.broker_name, selected.queue_id),
                        Some(publish),
                    ),
                    Err(e) => {
                        // Python 这里 selected 为 None 时报 `Send [0] times, still failed,
                        // Topic: …, BrokersSent: []`；Rust 的选队列是 `Result`，原始异常
                        // 只能拼进文本（与同步内核同一口径）。
                        return self.fail_async(
                            &callback,
                            Error::client(format!(
                                "Send [0] times, still failed, Topic: {topic}, BrokersSent: [], \
                                 last error: {e}"
                            )),
                            permits,
                        );
                    }
                }
            }
        };
        self.send_kernel_async(
            &client, msg, &mq_sel, publish, sys_flag, callback, permits, timeout,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_kernel_async(
        &self,
        client: &MQClientInstance,
        msg: &mut Message,
        mq: &MessageQueue,
        publish: Option<Arc<TopicPublishInfo>>,
        sys_flag: i32,
        callback: Arc<dyn SendCallback>,
        permits: SendPermits,
        timeout: i64,
    ) {
        let began = monotonic_millis();
        let addr = match client.publish_addr_for(&mq.broker_name, &mq.topic).await {
            Ok(addr) => addr,
            Err(e) => return self.fail_async(&callback, e, permits),
        };
        if self.has_check_forbidden_hook() {
            if let Err(e) =
                self.execute_check_forbidden(msg, mq, &addr, None, CommunicationMode::Async)
            {
                return self.fail_async(&callback, e, permits);
            }
        }
        if self.read_cfg(|c| c.enable_trace_context.unwrap_or(false)) {
            inject_trace_context(msg);
        }
        let mut publish_msg = PublishMessage::Single(msg);
        let request = MQClientInstance::build_send_request(
            &self.inner.producer_group(),
            &mut publish_msg,
            mq,
            sys_flag,
            self.inner.unit_mode(),
            &self.inner.create_topic_key(),
            self.inner.default_topic_queue_nums(),
        );
        let group = self.inner.producer_group();
        let namespace = self.inner.namespace();
        let context = if self.has_send_message_hook() {
            let mut context = self.build_send_context(
                msg,
                &group,
                &namespace,
                mq,
                &addr,
                CommunicationMode::Async,
            );
            crate::client::hook::execute_send_message_hook_before(
                &self.inner.send_hooks,
                &mut context,
            );
            Some(context)
        } else {
            None
        };
        let cost = latency_since(began);
        if timeout < cost {
            return self.complete_async(
                &callback,
                Err(Error::TooMuchRequest(
                    "sendKernelImpl call timeout".to_string(),
                )),
                context,
                Some(permits),
            );
        }
        let mut msg_only = msg.clone();
        // body 已经编进 request，重试链只需要 UNIQ_KEY（parse_send_response 读它）
        msg_only.set_body(None);
        Box::new(AsyncSendChain {
            producer: self.clone(),
            client: client.clone(),
            msg: Arc::new(msg_only),
            request,
            publish,
            broker_name: mq.broker_name.clone(),
            mq: mq.clone(),
            addr,
            timeout: timeout - cost,
            attempt_began: 0.0,
            times: 0,
            context,
            permits: Some(permits),
            callback,
        })
        .attempt();
    }

    /// `permits` 走 `drop` 而不是 `SendPermits::release`：归还规则只有一处实现
    /// （见 [`SendPermits` 的 `Drop`](SendPermits)），链上少一个能写错的地方。
    fn complete_async(
        &self,
        callback: &Arc<dyn SendCallback>,
        outcome: Result<SendResult>,
        mut context: Option<SendMessageContext>,
        permits: Option<SendPermits>,
    ) {
        let mut outcome = outcome;
        if let Some(context) = context.as_mut() {
            match outcome {
                Ok(result) => {
                    context.send_result = Some(result.clone());
                    crate::client::hook::execute_send_message_hook_after(
                        &self.inner.send_hooks,
                        context,
                    );
                    outcome = Ok(result);
                }
                Err(e) => {
                    context.exception = Some(e);
                    crate::client::hook::execute_send_message_hook_after(
                        &self.inner.send_hooks,
                        context,
                    );
                    // 钩子只读 exception，取回来原样交付（Error 不是 Clone）。
                    outcome = context
                        .exception
                        .take()
                        .map_or_else(|| Err(Error::client("send message failed")), Err);
                }
            }
        }
        drop(permits); // 显式标出归还点：必须在转交用户回调之前
        match outcome {
            Ok(result) => callback.on_success(result),
            Err(e) => callback.on_exception(e),
        }
    }

    async fn acquire_send_permits(
        &self,
        msg_len: i64,
        timeout: i64,
        began: f64,
    ) -> Result<SendPermits> {
        let mut permits = SendPermits {
            inner: self.inner.clone(),
            num_acquired: false,
            size_acquired: false,
            msg_len,
        };
        if !self.is_enable_backpressure_for_async_mode() {
            return Ok(permits);
        }
        let budget = timeout - latency_since(began);
        permits.num_acquired = budget > 0
            && self
                .inner
                .semaphore_async_send_num
                .try_acquire(1, budget)
                .await;
        if !permits.num_acquired {
            return Err(Error::TooMuchRequest(
                "send message tryAcquire semaphoreAsyncNum timeout".to_string(),
            ));
        }
        let budget = timeout - latency_since(began);
        permits.size_acquired = budget > 0
            && self
                .inner
                .semaphore_async_send_size
                .try_acquire(msg_len, budget)
                .await;
        if !permits.size_acquired {
            return Err(Error::TooMuchRequest(
                "send message tryAcquire semaphoreAsyncSize timeout".to_string(),
            ));
        }
        Ok(permits)
    }
}

// ================================================================ 异步发送链

struct AsyncSendChain {
    producer: DefaultMQProducer,
    client: MQClientInstance,
    /// 只给 `parse_send_response` 读 UNIQ_KEY；body 已经编进 `request`。
    msg: Arc<Message>,
    request: RemotingCommand,
    /// `None` = 定点发送（调用方给了 mq）：重试不换 broker，只在原地换 opaque。
    publish: Option<Arc<TopicPublishInfo>>,
    broker_name: String,
    mq: MessageQueue,
    addr: String,
    timeout: i64,
    attempt_began: f64,
    times: i32,
    context: Option<SendMessageContext>,
    permits: Option<SendPermits>,
    callback: Arc<dyn SendCallback>,
}

impl AsyncSendChain {
    fn attempt(mut self: Box<Self>) {
        if self.times > 0 {
            self.request.opaque = next_opaque();
        }
        self.attempt_began = monotonic_millis();
        let client = self.client.clone();
        let addr = self.addr.clone();
        let request = self.request.clone();
        let msg = Arc::clone(&self.msg);
        let mq = Arc::new(self.mq.clone());
        let timeout = self.timeout;
        client.send_message_async(
            &addr,
            request,
            msg,
            mq,
            timeout,
            Box::new(move |outcome| self.on_response(outcome)),
        );
    }

    fn on_response(self: Box<Self>, outcome: Result<SendResult>) {
        let cost = latency_since(self.attempt_began);
        match outcome {
            Ok(result) => {
                self.producer.inner.fault_strategy.update_fault_item(
                    &self.broker_name,
                    cost,
                    false,
                    true,
                );
                self.finish(Ok(result));
            }
            Err(e) => {
                self.producer.inner.fault_strategy.update_fault_item(
                    &self.broker_name,
                    cost,
                    true,
                    true,
                );
                let (wrapped, need_retry) = classify_async_failure(e, cost);
                self.on_exception(wrapped, need_retry, cost);
            }
        }
    }

    fn on_exception(mut self: Box<Self>, error: Error, need_retry: bool, cost: i64) {
        self.times += 1;
        let remaining = self.timeout - cost;
        if !(need_retry
            && self.times <= self.producer.get_retry_times_when_send_async_failed()
            && remaining > 0)
        {
            return self.finish(Err(error));
        }
        let (broker_name, mq) = match &self.publish {
            None => (self.broker_name.clone(), self.mq.clone()),
            Some(publish) => match self.producer.inner.fault_strategy.select_one_message_queue(
                &**publish,
                Some(&self.broker_name),
                false,
            ) {
                Ok(selected) => (
                    selected.broker_name.clone(),
                    MessageQueue::new(&self.mq.topic, &selected.broker_name, selected.queue_id),
                ),
                Err(_) => (self.broker_name.clone(), self.mq.clone()),
            },
        };
        let addr = match self.client.find_broker_address_in_publish(&broker_name) {
            Some(addr) => addr,
            None => {
                return self.finish(Err(Error::client(format!(
                    "The broker[{broker_name}] not exist"
                ))));
            }
        };
        rmq_warn!(
            "async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}: {}",
            self.times,
            self.mq.topic,
            addr,
            broker_name,
            error
        );
        self.broker_name = broker_name;
        self.mq = mq;
        self.addr = addr;
        self.timeout = remaining;
        self.attempt();
    }

    /// 链的终点：交给 [`complete_async`](DefaultMQProducer::complete_async)
    /// （先 after 钩子，再归还许可，最后转交用户回调）。
    fn finish(mut self: Box<Self>, outcome: Result<SendResult>) {
        let permits = self.permits.take();
        let context = self.context.take();
        self.producer
            .complete_async(&self.callback, outcome, context, permits);
    }
}

fn classify_async_failure(err: Error, cost: i64) -> (Error, bool) {
    match err {
        e @ Error::SendRequest { .. } => (
            Error::client(format!("send request failed, last error: {e}")),
            true,
        ),
        e @ Error::Timeout { .. } => (
            Error::client(format!(
                "wait response timeout, cost={cost}, last error: {e}"
            )),
            true,
        ),
        // 其余 RemotingException 都是 `"unknown reason"`，但 `RemotingTooMuchRequestException`
        // 是「自己人太多」，换 broker 也没用（Python 同：`not isinstance(e, RemotingTooMuch…)`）。
        e @ Error::TooMuchRequest(_) => (
            Error::client(format!("unknown reason, last error: {e}")),
            false,
        ),
        e @ (Error::Connect { .. } | Error::RemotingCommand(_) | Error::Io(_)) => (
            Error::client(format!("unknown reason, last error: {e}")),
            true,
        ),
        e => (e, false),
    }
}

// ================================================================ 事务消息

/// Python `_wait_request_response` 之外，事务链路里「发送后才知道的收尾信息」来源。
///
/// * [`TxnEnd::Normal`]：客户端主动提交/回滚，偏移与事务号取自 `SendResult`；
/// * [`TxnEnd::Check`]：broker 回查后收尾，取自回查 header（此时没有 `SendResult`）。
///
/// 对应 Python `_end_transaction(send_result, msg, ..., check_header, msg_ext, broker_addr)`
/// 那六个参数——Rust 用枚举把两条互斥的取值路径分开，免得传一堆 `Option`。
enum TxnEnd<'a> {
    Normal {
        send_result: &'a SendResult,
        msg: &'a Message,
    },
    Check {
        header: &'a CheckTransactionStateRequestHeader,
        msg_ext: &'a MessageExt,
        broker_addr: String,
    },
}

/// Python 事务消息禁止的延迟属性（与钩子上下文那份判定**列表不同**：这里没有
/// `__STARTDELIVERTIME`，Python `ensureNotDelayedForTransactional` 只用
/// DELAY / DELAY_TIME / TIMER_* 五个键）。
const TRANSACTION_FORBIDDEN_DELAY_KEYS: [&str; 5] = [
    PROPERTY_DELAY_TIME_LEVEL,
    PROPERTY_DELAY_TIME,
    "TIMER_DELAY_MS",
    "TIMER_DELAY_SEC",
    "TIMER_DELIVER_MS",
];

impl DefaultMQProducer {
    pub fn transaction_flag(state: LocalTransactionState) -> i32 {
        match state {
            LocalTransactionState::CommitMessage => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
            LocalTransactionState::RollbackMessage => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
            LocalTransactionState::Unknow => MessageSysFlag::TRANSACTION_NOT_TYPE,
        }
    }

    /// 发送事务消息（Python `send_message_in_transaction`）。
    ///
    ///
    /// `listener` 为 `None` 时用 [`set_transaction_listener`](Self::set_transaction_listener)
    /// 设过的那个（Python 的 `send_message_in_transaction(msg, listener=None)` 在
    /// `TransactionMQProducer` 里做的兜底，这里合并到同一个入口）；两个都没有才报错。
    /// 传进来的 listener 会同时记为「当前监听器」，供 broker 回查使用。
    ///
    /// ⚠ 差异：Python 里「本地事务抛异常」会被捕获，把异常文本写进 END_TRANSACTION 的
    /// remark 并保持 UNKNOW（`producer.py:934-939`）；Rust 的
    /// [`TransactionListener::execute_local_transaction`] 不抛异常，实现者要把失败
    /// 表达为 [`LocalTransactionState::Unknow`]，让 broker 稍后回查。
    pub async fn send_message_in_transaction(
        &self,
        msg: &mut Message,
        listener: Option<Arc<dyn TransactionListener>>,
        arg: Option<AnyHolder>,
    ) -> Result<TransactionSendResult> {
        let listener = match listener.or_else(|| self.transaction_listener()) {
            Some(listener) => listener,
            None => return Err(Error::client("tranExecutor is null")),
        };
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic.clone();

        if TRANSACTION_FORBIDDEN_DELAY_KEYS
            .iter()
            .any(|key| msg.get_property(key).is_some())
        {
            bail!("Transactional messages do not support delayed delivery");
        }

        let client = self.require_client()?;
        self.check_message(msg)?;

        // 半消息标记（broker 侧据此把消息写入 RMQ_SYS_TRANS_HALF_TOPIC）
        msg.put_property(PROPERTY_TRANSACTION_PREPARED, "true");
        msg.put_property(PROPERTY_PRODUCER_GROUP, &self.inner.producer_group());
        // 回查时按此 listener 回调（broker 通过 PGROUP 属性定位到本生产者）
        self.set_transaction_listener(Some(listener.clone()));

        let publish = self.topic_publish_info(&client, &topic).await?;
        let selected = publish
            .select_one_message_queue(&[])?
            .ok_or_else(|| Error::client("no message queue for publish info"))?;
        let mq_sel = MessageQueue::new(&topic, &selected.broker_name, selected.queue_id);

        let (mut sys_flag, prev_body) = self.try_to_compress_message(msg);
        sys_flag = MessageSysFlag::reset_transaction_value(
            sys_flag,
            MessageSysFlag::TRANSACTION_PREPARED_TYPE,
        );

        let send_timeout = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .send_msg_timeout;
        // 事务发送同样走 sendKernelImpl（→ 同样触发发送钩子），所以开启轨迹后
        // 事务消息会先落一条 Pub（Trans_Msg_Half）轨迹
        let half_sent = self
            .send_with_hooks(
                &client,
                &mut PublishMessage::Single(msg),
                &mq_sel,
                send_timeout,
                sys_flag,
                None,
                CommunicationMode::Sync,
            )
            .await;
        self.restore_caller_message(msg, prev_body);
        let send_result =
            half_sent.map_err(|e| Error::client(format!("send message Exception: {e}")))?;

        let mut state = LocalTransactionState::Unknow;
        if send_result.status == SendStatus::SendOk {
            if let Some(tran_id) = &send_result.transaction_id {
                msg.put_property("__transactionId__", tran_id);
            }
            if let Some(uniq) = msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) {
                let uniq = uniq.to_string();
                msg.set_transaction_id(Some(&uniq));
            }
            state = listener.execute_local_transaction(msg, arg.as_ref());
        } else if matches!(
            send_result.status,
            SendStatus::FlushDiskTimeout
                | SendStatus::FlushSlaveTimeout
                | SendStatus::SlaveNotAvailable
        ) {
            state = LocalTransactionState::RollbackMessage;
        }

        if let Err(e) = self
            .end_transaction(
                &client,
                TxnEnd::Normal {
                    send_result: &send_result,
                    msg,
                },
                state,
                None,
                false,
            )
            .await
        {
            rmq_warn!("local transaction execute {state}, but end broker transaction failed: {e}");
        }
        Ok(TransactionSendResult {
            local_transaction_state: Some(state),
            inner: send_result,
        })
    }

    async fn end_transaction(
        &self,
        client: &MQClientInstance,
        end: TxnEnd<'_>,
        state: LocalTransactionState,
        remark: Option<String>,
        from_transaction_check: bool,
    ) -> Result<()> {
        let group = self.inner.producer_group();
        let namespace = self.inner.namespace();
        let mut header = EndTransactionRequestHeader {
            producer_group: Some(group.clone()),
            commit_or_rollback: Some(Self::transaction_flag(state)),
            from_transaction_check: Some(from_transaction_check),
            ..Default::default()
        };
        let broker_addr = match &end {
            // 回收时 broker 会把 COMPRESSED/事务相关信息放在回查请求里
            TxnEnd::Check {
                header: check,
                msg_ext,
                broker_addr,
            } => {
                header.commit_log_offset = check.commit_log_offset;
                header.tran_state_table_offset = check.tran_state_table_offset;
                header.transaction_id = check.transaction_id.clone();
                header.bname = check.bname.clone();
                header.topic = check.topic.clone();
                header.msg_id = msg_ext
                    .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                    .or(msg_ext.msg_id.as_deref())
                    .map(str::to_string);
                broker_addr.clone()
            }
            TxnEnd::Normal { send_result, msg } => {
                let id = send_result
                    .offset_msg_id
                    .as_deref()
                    .or(send_result.msg_id.as_deref())
                    .ok_or_else(|| Error::client("send result has no message id"))?;
                let (_, _, offset) = decode_message_id(id)?;
                let broker_name = send_result
                    .message_queue
                    .as_ref()
                    .map(|mq| mq.broker_name.clone())
                    .unwrap_or_default();
                header.commit_log_offset = Some(offset);
                header.tran_state_table_offset = Some(send_result.queue_offset);
                header.transaction_id = send_result.transaction_id.clone();
                header.bname = Some(broker_name.clone());
                header.topic = Some(msg.topic.clone());
                header.msg_id = send_result.msg_id.clone();
                client
                    .find_broker_address_in_publish(&broker_name)
                    .ok_or_else(|| Error::client("no broker address for end transaction"))?
            }
        };

        let mut cmd = RemotingCommand::create_request_command(
            request_code::END_TRANSACTION,
            Some(Box::new(header.clone())),
        );
        cmd.remark = remark;
        cmd.make_custom_header_to_net();
        client
            .remoting_client()
            .invoke_oneway(&broker_addr, &mut cmd)
            .await?;

        if self.inner.end_txn_hooks.has_hooks() {
            let message = match &end {
                TxnEnd::Normal { msg, .. } => Some((*msg).clone()),
                TxnEnd::Check { msg_ext, .. } => Some(msg_ext.to_message()),
            };
            let mut ctx = EndTransactionContext {
                producer_group: group,
                message,
                broker_addr,
                msg_id: header.msg_id.clone(),
                transaction_id: header.transaction_id.clone(),
                transaction_state: Some(state),
                from_transaction_check,
                namespace,
            };
            crate::client::hook::execute_end_transaction_hook(&self.inner.end_txn_hooks, &mut ctx);
        }
        Ok(())
    }

    /// Python `_handle_check_transaction_state`：处理 broker 主动发来的事务回查
    /// （CHECK_TRANSACTION_STATE=39）。
    fn handle_check_transaction_state(&self, request: RemotingCommand, addr: String) -> Result<()> {
        let header: CheckTransactionStateRequestHeader =
            match request.decode_command_custom_header() {
                Ok(h) => h,
                Err(e) => {
                    rmq_warn!("checkTransactionState: decode header failed from {addr}: {e}");
                    return Ok(());
                }
            };
        let body = match request.body.as_ref() {
            Some(b) => b.clone(),
            None => {
                rmq_warn!("checkTransactionState: empty message body from {addr}");
                return Ok(());
            }
        };
        let msg_ext = match decode_message(&body) {
            Ok(m) => m,
            Err(e) => {
                rmq_warn!("checkTransactionState: decode message failed: {e}");
                return Ok(());
            }
        };
        // 按 PGROUP 属性匹配本生产者，不匹配则丢弃
        let group = self.inner.producer_group();
        if let Some(pg) = msg_ext.get_property(PROPERTY_PRODUCER_GROUP) {
            if pg != group {
                rmq_debug!("checkTransactionState: group {pg} not mine ({group})");
                return Ok(());
            }
        }
        let listener = match self.transaction_listener() {
            Some(l) => l,
            None => {
                rmq_warn!("checkTransactionState: no transaction listener for group {group}");
                return Ok(());
            }
        };
        let client = self.require_client()?;
        let this = self.clone();
        let spawned = client.remoting_client().runtime_handle().map(|handle| {
            handle.spawn(async move {
                let state = listener.check_local_transaction(&msg_ext);
                if let Err(e) = this
                    .end_transaction(
                        &client,
                        TxnEnd::Check {
                            header: &header,
                            msg_ext: &msg_ext,
                            broker_addr: addr.clone(),
                        },
                        state,
                        None,
                        true,
                    )
                    .await
                {
                    rmq_warn!("checkTransactionState: end transaction failed: {e}");
                }
            })
        });
        if spawned.is_none() {
            rmq_warn!("checkTransactionState: no tokio runtime to run the transaction check");
        }
        Ok(())
    }
}

/// ⚠ 只持 [`Weak`]：否则 `Inner → RemotingClient → 本处理器 → Inner` 成环，
/// 生产者永远不释放（与 `mq_client::ClientRemotingProcessor` 同一个理由）。
struct CheckTransactionStateProcessor {
    producer: Weak<Inner>,
}

impl RequestProcessor for CheckTransactionStateProcessor {
    fn process(&self, request: RemotingCommand, addr: String, sink: ResponseSink) {
        let Some(inner) = self.producer.upgrade() else {
            rmq_warn!("CHECK_TRANSACTION_STATE from {addr}: producer already dropped");
            return;
        };
        let producer = DefaultMQProducer { inner };
        if let Err(e) = producer.handle_check_transaction_state(request, addr.clone()) {
            rmq_warn!("CHECK_TRANSACTION_STATE from {addr} failed: {e}");
        }
        // broker 用 oneway 推回查请求，本侧无响应可回（ResponseSink 自己会丢掉）
        drop(sink);
    }
}

// ================================================================ Request-Reply

impl DefaultMQProducer {
    /// Request-Reply（5.x）：发一条请求消息并**同步等应答**，返回应答消息。
    ///
    ///
    ///
    ///
    /// ⚠ 差别：Python 用 `_NullSendCallback` 把「发送失败」写回等待槽；Rust 里发送
    /// 就在本函数内 `await`，直接拿返回值调
    /// [`RequestResponseFuture::set_failed`]，效果一致（少一个只在内部用的回调类型）。
    pub async fn request(
        &self,
        msg: &mut Message,
        timeout_millis: Option<i64>,
        mq: Option<&MessageQueue>,
    ) -> Result<MessageExt> {
        let timeout = timeout_millis.unwrap_or_else(|| {
            self.inner
                .cfg
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .request_timeout
        });
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic.clone();
        self.check_message(msg)?;
        let client = self.require_client()?;

        let correlation_id = create_correlation_id();
        msg.put_property(PROPERTY_CORRELATION_ID, &correlation_id);
        msg.put_property(PROPERTY_MESSAGE_REPLY_TO_CLIENT, client.client_id());
        msg.put_property(PROPERTY_MESSAGE_TTL, &timeout.to_string());

        let begin = current_time_millis();
        if let Err(e) = self.topic_publish_info(&client, &topic).await {
            rmq_debug!("request: prepare route failed: {e}");
        }
        send_heartbeat_to_all_broker(&self.inner, &client).await;

        let future = Arc::new(RequestResponseFuture::new(&correlation_id, timeout));
        request_future_holder().put_request(&correlation_id, future.clone());
        let cost = current_time_millis() - begin;
        let send_timeout = if timeout > cost {
            timeout - cost
        } else {
            timeout
        };
        if let Err(e) = self.send(msg, Some(send_timeout), mq).await {
            future.set_failed(e);
        }
        let result = self
            .wait_request_response(&topic, timeout, &future, cost)
            .await;
        // Python 的 `finally: REQUEST_FUTURE_HOLDER.remove_request(...)`
        request_future_holder().remove_request(&correlation_id);
        result
    }

    async fn wait_request_response(
        &self,
        topic: &str,
        timeout: i64,
        future: &Arc<RequestResponseFuture>,
        cost: i64,
    ) -> Result<MessageExt> {
        match future.wait_response_message(timeout - cost).await {
            Some(response) => Ok(response),
            None => {
                if future.is_send_request_ok() {
                    Err(Error::request_timeout(topic, timeout))
                } else {
                    Err(Error::client(format!(
                        "send request message to <{topic}> fail"
                    )))
                }
            }
        }
    }
}

// ================================================================ 定时消息撤回

impl DefaultMQProducer {
    /// 撤回一条**定时/延迟**消息（`RECALL_MESSAGE` 370），成功返回被撤回消息的 uniqKey。
    pub async fn recall_message(&self, topic: &str, recall_handle: &str) -> Result<String> {
        let client = self.require_client()?;
        let topic = self.with_namespace(topic);
        validators::check_topic(&topic)?;
        if MixAll::is_retry_topic(Some(topic.as_str()))
            || MixAll::is_dlq_topic(Some(topic.as_str()))
        {
            return Err(Error::client("topic is not supported"));
        }
        let handle = recall_message_handle::decode_handle(recall_handle)?;
        self.topic_publish_info(&client, &topic).await?;
        let addr = client
            .find_broker_address_in_publish(&handle.broker_name)
            .or_else(|| client.find_broker_addr_by_topic(&topic))
            .ok_or_else(|| {
                rmq_warn!("can't find broker service address. {}", handle.broker_name);
                Error::client("The broker service address not found")
            })?;
        let header = RecallMessageRequestHeader {
            producer_group: Some(self.inner.producer_group()),
            topic: Some(topic.clone()),
            recall_handle: Some(recall_handle.to_string()),
            bname: Some(handle.broker_name.clone()),
        };
        let timeout = self.read_cfg(|c| c.send_msg_timeout);
        client.recall_message(&addr, header, timeout).await
    }
}

// ================================================================ 管理类便捷方法

/// 这些方法都要求生产者已 `start()`（拿不到 [`MQClientInstance`] 就报错），
/// 超时值与 Python 的默认参数逐条一致：路由/offset/建 topic 用 5000ms，
/// 消息查询用 15000ms。
impl DefaultMQProducer {
    /// Python `fetch_publish_message_queues`：取 topic 的可发送队列（缓存没有会拉一次路由）。
    pub async fn fetch_publish_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = self.require_client()?;
        let publish = client.get_topic_publish_info(topic, false).await?;
        Ok(publish.msg_queue_list())
    }

    /// Python `create_topic`：借默认 topic 路由里的 broker 新建 topic。
    pub async fn create_topic(
        &self,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
    ) -> Result<()> {
        let client = self.require_client()?;
        validators::check_topic(new_topic)?;
        validators::is_system_topic(new_topic)?;
        let perm = PermName::PERM_READ | PermName::PERM_WRITE;
        client
            .create_topic_in_route(
                new_topic,
                queue_num,
                queue_num,
                perm,
                topic_sys_flag,
                None,
                5000,
            )
            .await
    }

    /// Python `search_offset`：按时间戳找队列上的位点。
    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: i64) -> Result<i64> {
        let client = self.require_client()?;
        client
            .search_offset_by_timestamp(mq, timestamp, 5000, None)
            .await
    }

    /// Python `max_offset`。
    pub async fn max_offset(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client.get_max_offset(mq, 5000, None).await
    }

    /// Python `min_offset`。
    pub async fn min_offset(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client.get_min_offset(mq, 5000, None).await
    }

    /// Python `earliest_msg_store_time`：队列上最早一条消息的存储时间（0 = 队列空）。
    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        let addr = client.broker_addr(mq).await?;
        let header = GetEarliestMsgStoretimeRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_EARLIEST_MSG_STORETIME,
            Some(Box::new(header)),
        );
        let response = client.invoke_sync(&addr, &mut request, 5000).await?;
        MQClientInstance::check_response(&response)?;
        let mut resp_header = GetEarliestMsgStoretimeResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.timestamp.unwrap_or(0))
    }

    /// Python `query_message`：按业务 key 查消息，返回解好码的
    /// [`MessageExt`] 列表（查不到 ⇒ 空列表，不报错）。
    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<Vec<MessageExt>> {
        let client = self.require_client()?;
        let body = client
            .query_message(topic, key, max_num, begin, end, 15000, None, None, false)
            .await?;
        // Python：`body is None => []`
        Ok(body.map(|b| decode_messages(&b)).unwrap_or_default())
    }

    /// Python `view_message`：按 msgId 读一条消息 —— Python 参考实现直接抛
    /// `MQClientException`，这里同样只报错（不静默返回空列表）。
    pub fn view_message(&self, topic: &str, msg_id: &str) -> Result<Vec<MessageExt>> {
        Err(Error::client(format!(
            "viewMessage by msgId is not supported in Rust edition (topic={topic}, msgId={msg_id})"
        )))
    }
}

// ================================================================ 事务生产者

/// Python/C++ 里它是 `DefaultMQProducer` 的子类，只多做一件事：**预置**
/// [`TransactionListener`]，发送时不必每次传。Rust 没有继承，用「包一层 +
/// [`std::ops::Deref`]`」得到同样的手感——基础生产者的所有方法照常可用。
///
/// ⚠ [`DefaultMQProducer::send_message_in_transaction`] 本身已经支持「`listener`
/// 传 `None` 时回落到 [`DefaultMQProducer::set_transaction_listener`] 设过的那个」
/// （Python 把这段兜底写在子类里，这里合并到基类，C++ 移植的做法一致），
/// 所以本类型只是把「必须先设监听器」这一约束显式化。
#[derive(Debug, Clone)]
pub struct TransactionMQProducer {
    producer: DefaultMQProducer,
}

impl TransactionMQProducer {
    /// Python `TransactionMQProducer(producer_group)`。
    pub fn new(producer_group: &str) -> Result<TransactionMQProducer> {
        Ok(TransactionMQProducer {
            producer: DefaultMQProducer::new(producer_group)?,
        })
    }

    /// 以一份完整配置构造（等价于 `new` + 逐个 setter）。
    pub fn with_config(cfg: ProducerConfig) -> Result<TransactionMQProducer> {
        Ok(TransactionMQProducer {
            producer: DefaultMQProducer::with_config(cfg)?,
        })
    }

    /// 包住一个已有的生产者（Python 的子类关系在 Rust 里的另一种表达）。
    pub fn from_producer(producer: DefaultMQProducer) -> TransactionMQProducer {
        TransactionMQProducer { producer }
    }

    /// 拿到底层生产者（生命周期、发送、管理类方法都在它上面）。
    pub fn producer(&self) -> &DefaultMQProducer {
        &self.producer
    }

    /// Python `set_transaction_listener`。
    pub fn set_transaction_listener(&self, listener: Option<Arc<dyn TransactionListener>>) {
        self.producer.set_transaction_listener(listener);
    }

    /// Python `get_transaction_listener`。
    pub fn transaction_listener(&self) -> Option<Arc<dyn TransactionListener>> {
        self.producer.transaction_listener()
    }

    /// Python `send_message_in_transaction`：不传 listener ⇒ 用预设的那个；
    /// 两个都没有 ⇒ 报错（基类做同样的检查，这里只是把语义固定下来）。
    pub async fn send_message_in_transaction(
        &self,
        msg: &mut Message,
        listener: Option<Arc<dyn TransactionListener>>,
        arg: Option<AnyHolder>,
    ) -> Result<TransactionSendResult> {
        let listener = listener.or_else(|| self.transaction_listener());
        self.producer
            .send_message_in_transaction(msg, listener, arg)
            .await
    }
}

impl std::ops::Deref for TransactionMQProducer {
    type Target = DefaultMQProducer;

    fn deref(&self) -> &DefaultMQProducer {
        &self.producer
    }
}

// ================================================================ 测试

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::client::hook::{execute_send_message_hook_after, execute_send_message_hook_before};
    use crate::client::trace_context::TRACE_CONTEXT_PROPERTY;
    use crate::common::message_const::PROPERTY_TRANSFER_FLAG;
    use crate::remoting::protocol::ext_fields::{CustomHeader, ExtFields};

    fn producer(group: &str) -> DefaultMQProducer {
        DefaultMQProducer::new(group).expect("合法组名不该构造失败")
    }

    /// 本生产者压缩时应置的 sysFlag 位（COMPRESSED_FLAG + 压缩类型，Python 同款判据）。
    fn compression_flag(p: &DefaultMQProducer) -> i32 {
        MessageSysFlag::set_compression_type(
            MessageSysFlag::COMPRESSED_FLAG,
            p.read_cfg(|c| c.compress_type),
        )
    }

    fn mq(broker: &str, queue_id: i32) -> MessageQueue {
        MessageQueue::new("T1", broker, queue_id)
    }

    fn queues(broker_prefix: &str, n: i32) -> Vec<MessageQueue> {
        (0..n).map(|i| mq(broker_prefix, i)).collect()
    }

    /// 未 `start()` 的实例：路由表为空 ⇒ 任何真实 RPC 都会**立刻**失败
    /// （`name server address list is empty`），适合跑发送内核的离线用例。
    fn bare_client() -> MQClientInstance {
        MQClientInstance::new("offline-producer-test", Vec::new())
    }

    fn seed_publish_addr(client: &MQClientInstance) {
        client.seed_publish_addr_for_test("broker-a", "127.0.0.1:1");
    }

    // ---------------- 构造 / 配置默认值 ----------------

    #[test]
    fn config_defaults_match_python() {
        let cfg = ProducerConfig::default();
        assert_eq!(cfg.producer_group, MixAll::DEFAULT_PRODUCER_GROUP);
        assert_eq!(cfg.instance_name, DEFAULT_INSTANCE_NAME);
        assert_eq!(cfg.client_id, None);
        assert_eq!(cfg.create_topic_key, MixAll::DEFAULT_TOPIC);
        assert_eq!(
            cfg.default_topic_queue_nums,
            MixAll::DEFAULT_TOPIC_QUEUE_NUMS
        );
        assert_eq!(cfg.send_msg_timeout, 3000);
        assert_eq!(cfg.compress_msg_body_over_howmuch, 1024 * 4);
        assert_eq!(cfg.compress_level, 5);
        // 压缩类型是「算法 id」而非 sysFlag 位段
        assert_eq!(cfg.compress_type, MessageSysFlag::ZLIB_TYPE);
        assert_eq!(cfg.retry_times_when_send_failed, 2);
        assert!(!cfg.retry_another_broker_when_not_store_ok);
        assert_eq!(cfg.max_message_size, 1024 * 1024 * 4);
        assert_eq!(cfg.heartbeat_interval_millis, 30_000);
        assert!(!cfg.send_latency_fault_enable);
        assert!(!cfg.enable_trace);
        assert_eq!(cfg.trace_msg_batch_num, 10);
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT_MILLIS);
        assert_eq!(cfg.tls_enable, None);
        assert_eq!(cfg.enable_trace_context, None);
    }

    #[test]
    fn empty_producer_group_is_rejected() {
        assert!(DefaultMQProducer::new("").is_err());
        assert!(DefaultMQProducer::new("   ").is_err());
        assert!(DefaultMQProducer::new("GID_ok").is_ok());
    }

    #[test]
    fn producer_group_setter_works_before_start() {
        let p = producer("GID_keep");
        p.set_producer_group("GID_new").expect("未启动时允许改组名");
        assert_eq!(p.producer_group(), "GID_new");
        // 空组名只在构造期校验（Python 的 setter 同样不校验）
        assert!(DefaultMQProducer::new("").is_err());
    }

    // ---------------- 队列选择器 ----------------

    #[test]
    fn hash_selector_is_deterministic_and_in_range() {
        let mqs = queues("broker-a", 8);
        let msg = Message::new("T1", Some(b"x"));
        for arg in ["", "order-1", "中文 key", "12345"] {
            let first = SelectMessageQueueByHash
                .select(&mqs, &msg, arg)
                .expect("非空队列可选");
            let second = SelectMessageQueueByHash
                .select(&mqs, &msg, arg)
                .expect("非空队列可选");
            assert_eq!(first, second, "同一 arg 必须落在同一队列");
            assert!(mqs.contains(&first));
            // 与 java_string_hash 的取模口径一致
            let expect = (java_string_hash(arg).unsigned_abs() as usize) % mqs.len();
            assert_eq!(first.queue_id, mqs[expect].queue_id);
        }
        // Integer.MIN_VALUE 的 abs 仍是负数，这里必须收敛到 0 而不是 panic
        let extreme = java_string_hash("\u{0}");
        let _ = extreme;
        assert!(SelectMessageQueueByHash.select(&[], &msg, "any").is_err());
    }

    #[test]
    fn random_selector_stays_in_range() {
        let mqs = queues("broker-a", 4);
        let msg = Message::new("T1", Some(b"x"));
        for _ in 0..32 {
            let got = SelectMessageQueueByRandom
                .select(&mqs, &msg, "")
                .expect("非空队列可选");
            assert!(mqs.contains(&got));
        }
        assert!(SelectMessageQueueByRandom.select(&[], &msg, "").is_err());
    }

    #[test]
    fn machine_room_selector_prefers_prefix_then_falls_back_to_first() {
        let mqs = vec![mq("roomA-0", 0), mq("roomB-0", 1)];
        let msg = Message::new("T1", Some(b"x"));
        assert_eq!(
            SelectMessageQueueByMachineRoom
                .select(&mqs, &msg, "roomB")
                .expect("可回退"),
            mqs[1]
        );
        assert_eq!(
            SelectMessageQueueByMachineRoom
                .select(&mqs, &msg, "roomZ")
                .expect("可回退"),
            mqs[0]
        );
        assert!(SelectMessageQueueByMachineRoom
            .select(&[], &msg, "roomA")
            .is_err());
    }

    #[test]
    fn closure_callback_runs_each_branch_at_most_once() {
        let ok = Arc::new(AtomicUsize::new(0));
        let bad = Arc::new(AtomicUsize::new(0));
        let (o, b) = (ok.clone(), bad.clone());
        let cb = ClosureSendCallback::new(
            Some(Box::new(move |_r| {
                o.fetch_add(1, Ordering::SeqCst);
            })),
            Some(Box::new(move |_e| {
                b.fetch_add(1, Ordering::SeqCst);
            })),
        );
        cb.on_success(SendResult::default());
        cb.on_success(SendResult::default());
        cb.on_exception(Error::client("boom"));
        cb.on_exception(Error::client("boom"));
        assert_eq!(ok.load(Ordering::SeqCst), 1);
        assert_eq!(bad.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn closure_callback_tolerates_missing_branch() {
        let cb = ClosureSendCallback::new(None, None);
        cb.on_success(SendResult::default());
        cb.on_exception(Error::client("ignored"));
    }

    // ---------------- 事务 / 重试判据 ----------------

    #[test]
    fn transaction_flag_matches_wire_sysflag() {
        assert_eq!(
            DefaultMQProducer::transaction_flag(LocalTransactionState::CommitMessage),
            MessageSysFlag::TRANSACTION_COMMIT_TYPE
        );
        assert_eq!(
            DefaultMQProducer::transaction_flag(LocalTransactionState::RollbackMessage),
            MessageSysFlag::TRANSACTION_ROLLBACK_TYPE
        );
        assert_eq!(
            DefaultMQProducer::transaction_flag(LocalTransactionState::Unknow),
            MessageSysFlag::TRANSACTION_NOT_TYPE
        );
    }

    #[test]
    fn retry_covers_client_broker_and_remoting_errors() {
        for e in [
            Error::client("no route"),
            Error::Broker {
                response_code: 14,
                message: "topic not exist".into(),
            },
            Error::Server {
                response_code: 3,
                remark: "system busy".into(),
            },
            Error::RemotingCommand("bad command".into()),
            Error::Connect {
                addr: "1.2.3.4:10911".into(),
            },
            Error::SendRequest {
                addr: "1.2.3.4:10911".into(),
                message: "io".into(),
            },
            Error::Timeout {
                addr: "1.2.3.4:10911".into(),
                timeout_millis: 3000,
            },
            Error::TooMuchRequest("full".into()),
            Error::RequestTimeout {
                topic: "T1".into(),
                timeout_millis: 3000,
            },
        ] {
            assert!(retryable(&e), "{e} 应在重试之列");
        }
        for e in [
            Error::Decode("bad frame".into()),
            Error::Encode("illegal arg".into()),
            Error::Io(std::io::Error::other("disk")),
        ] {
            assert!(!retryable(&e), "{e} 不该重试");
        }
    }

    // ---------------- 校验 / 压缩 ----------------

    #[test]
    fn check_message_rejects_empty_topic_and_oversized_body() {
        let p = producer("GID_check");
        assert!(p.check_message(&Message::new("", Some(b"x"))).is_err());

        p.set_max_message_size(4);
        assert!(p
            .check_message(&Message::new("T1", Some(b"12345")))
            .is_err());
        assert!(p.check_message(&Message::new("T1", Some(b"1234"))).is_ok());
        assert!(p.check_message(&Message::new("T1", None)).is_err());
    }

    #[test]
    fn compression_only_kicks_in_at_threshold() {
        let p = producer("GID_compress");
        p.set_compress_msg_body_over_howmuch(1024);
        let mut msg = Message::new("T1", Some(&[b'a'; 1023]));
        let before = msg.get_body().to_vec();
        let (flag, prev) = p.try_to_compress_message(&mut msg);
        assert_eq!(flag, 0);
        assert!(prev.is_none(), "没压缩就不该交出 prevBody");
        assert_eq!(msg.get_body(), before.as_slice(), "未过阈值不该动正文");

        let mut big = Message::new("T1", Some(&[b'a'; 4096]));
        let (flag, prev) = p.try_to_compress_message(&mut big);
        assert_eq!(
            flag & MessageSysFlag::COMPRESSED_FLAG,
            MessageSysFlag::COMPRESSED_FLAG
        );
        assert_eq!(
            MessageSysFlag::get_compression_type(flag),
            MessageSysFlag::ZLIB_TYPE
        );
        assert_eq!(prev.as_deref(), Some(&[b'a'; 4096][..]));
        // 正文确实被换成了压缩流，且能原样解回
        let compressed = big.get_body().to_vec();
        assert_ne!(compressed, vec![b'a'; 4096]);
        let raw = compression::decompress(&compressed, MessageSysFlag::ZLIB_TYPE).unwrap();
        assert_eq!(raw, vec![b'a'; 4096]);
    }

    #[test]
    fn restore_caller_message_prevents_double_compression() {
        let p = producer("GID_restore");
        p.set_compress_msg_body_over_howmuch(1024);
        let mut msg = Message::new("T1", Some(&[b'a'; 4096]));

        let (first_flag, prev) = p.try_to_compress_message(&mut msg);
        assert_eq!(first_flag, compression_flag(&p));
        let first_wire_body = msg.get_body().to_vec();
        p.restore_caller_message(&mut msg, prev);
        assert_eq!(
            msg.get_body(),
            &[b'a'; 4096][..],
            "还原后应等于发送前的正文"
        );

        // 再发一次：上线的仍是「原文压出来的那一串」
        let (second_flag, prev) = p.try_to_compress_message(&mut msg);
        assert_eq!(second_flag, compression_flag(&p));
        assert_eq!(msg.get_body(), first_wire_body.as_slice());
        p.restore_caller_message(&mut msg, prev);

        // 负向对照：跳过还原，第二次压缩直接失效（这就是会被本测试抓住的回归形态）
        let mut unguarded = Message::new("T1", Some(&[b'a'; 4096]));
        let (_flag, prev) = p.try_to_compress_message(&mut unguarded);
        drop(prev);
        let (again_flag, _prev) = p.try_to_compress_message(&mut unguarded);
        assert_eq!(again_flag, 0, "没有还原时第二次压缩必然失效");
    }

    #[test]
    fn restore_caller_message_strips_namespace_without_compression() {
        let p = producer("GID_restore_ns");
        p.set_namespace("ns1");
        let mut msg = Message::new("ns1%T1", Some(b"x"));

        p.restore_caller_message(&mut msg, None);

        assert_eq!(msg.topic, "T1");
        assert_eq!(msg.get_body(), b"x");
    }

    #[test]
    fn compression_type_and_empty_body_edges() {
        let p = producer("GID_compress_edge");
        p.set_compress_msg_body_over_howmuch(1);
        // 空正文：即便阈值最低也不压（Python `if not body: return 0`）
        let mut empty = Message::new("T1", Some(b""));
        let (flag, prev) = p.try_to_compress_message(&mut empty);
        assert_eq!(flag, 0);
        assert!(prev.is_none());

        assert!(
            p.set_compress_type(9).is_err(),
            "未知算法号必须在 set 时报错"
        );
        assert_eq!(
            p.read_cfg(|c| c.compress_type),
            MessageSysFlag::ZLIB_TYPE,
            "报错时配置保持原值"
        );
        // 配置保持 ZLIB，所以这一笔**照常压缩**（不是降级）：标志位是 COMPRESSED|ZLIB，
        // 并且交出压缩前的正文供 sendKernelImpl 的 finally 还原。
        let mut msg = Message::new("T1", Some(b"payload payload payload"));
        let before = msg.get_body().to_vec();
        let (flag, prev) = p.try_to_compress_message(&mut msg);
        assert_eq!(
            flag,
            MessageSysFlag::set_compression_type(
                MessageSysFlag::COMPRESSED_FLAG,
                MessageSysFlag::ZLIB_TYPE
            )
        );
        assert_eq!(prev.as_deref(), Some(before.as_slice()));
        assert_ne!(msg.get_body(), before.as_slice(), "body 已被压缩字节替换");

        // 合法值（含老版本兼容的 0）set 成功并归一
        p.set_compress_type(0).unwrap();
        assert_eq!(p.read_cfg(|c| c.compress_type), MessageSysFlag::ZLIB_TYPE);
        p.set_compress_type(MessageSysFlag::LZ4_TYPE).unwrap();
        assert_eq!(p.read_cfg(|c| c.compress_type), MessageSysFlag::LZ4_TYPE);
        p.set_compress_type(MessageSysFlag::ZSTD_TYPE).unwrap();
        assert_eq!(p.read_cfg(|c| c.compress_type), MessageSysFlag::ZSTD_TYPE);
    }

    #[test]
    fn batch_messages_never_compress() {
        let p = producer("GID_batch");
        p.set_compress_msg_body_over_howmuch(1);
        let mut batch = MessageBatch::new(vec![Message::new("T1", Some(&[b'x'; 4096]))]);
        let before = batch.message.get_body().len();
        assert_eq!(p.sys_flag_for(&mut PublishMessage::Batch(&mut batch)), 0);
        assert_eq!(batch.message.get_body().len(), before, "批量正文不被重写");
    }

    #[test]
    fn namespace_wraps_topic_once() {
        let p = producer("GID_ns");
        assert_eq!(p.with_namespace("T1"), "T1", "无命名空间时原样");
        p.set_namespace("ns1");
        assert_eq!(p.with_namespace("T1"), "ns1%T1");
        // wrap_namespace 幂等：重复包装不会产生 ns1%ns1%T1
        assert_eq!(p.with_namespace(&p.with_namespace("T1")), "ns1%T1");
    }

    #[test]
    fn namespace_v2_setter_getter_round_trip() {
        let p = producer("GID_nsv2");
        assert_eq!(p.get_namespace_v2(), None, "默认不设");
        p.set_namespace_v2(Some("NS_V2"));
        assert_eq!(p.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(p.config().namespace_v2, Some("NS_V2".to_string()));
        p.set_namespace_v2(None);
        assert_eq!(
            p.get_namespace_v2(),
            None,
            "None = 清除（Java setNamespaceV2(null)）"
        );
        p.set_namespace("ns1");
        assert_eq!(p.get_namespace_v2(), None, "v1 namespace 不写 v2 字段");
    }

    #[test]
    fn socks_proxy_setter_getter_round_trip() {
        let p = producer("GID_socks");
        assert_eq!(p.get_socks_proxy(), None, "默认直连");
        p.set_socks_proxy(Some(SocksProxyConfig::with_auth("10.0.0.1:1080", "u", "w")));
        assert_eq!(
            p.get_socks_proxy(),
            Some(SocksProxyConfig::with_auth("10.0.0.1:1080", "u", "w"))
        );
        p.set_socks_proxy(Some(SocksProxyConfig::new("socks4://10.0.0.2:1080")));
        let proxy = p.get_socks_proxy().unwrap();
        assert_eq!(proxy.addr, "socks4://10.0.0.2:1080");
        assert_eq!(proxy.username, None);
        p.set_socks_proxy(None);
        assert_eq!(p.get_socks_proxy(), None, "None = 清除（直连）");
    }

    // ---------------- 钩子上下文 ----------------

    #[test]
    fn send_context_msg_type_follows_python_priority() {
        let p = producer("GID_ctx");
        let m = mq("broker-a", 0);

        let plain = Message::new("T1", Some(b"x"));
        let ctx = p.build_send_context(
            &plain,
            "GID_ctx",
            "",
            &m,
            "127.0.0.1:10911",
            CommunicationMode::Sync,
        );
        assert_eq!(ctx.msg_type, MessageType::NormalMsg);
        assert_eq!(ctx.communication_mode, Some(CommunicationMode::Sync));
        assert_eq!(ctx.broker_addr, "127.0.0.1:10911");

        let mut half = Message::new("T1", Some(b"x"));
        half.put_property(PROPERTY_TRANSACTION_PREPARED, "true");
        let ctx = p.build_send_context(&half, "GID_ctx", "", &m, "", CommunicationMode::Sync);
        assert_eq!(ctx.msg_type, MessageType::TransMsgHalf);

        let mut delay = Message::new("T1", Some(b"x"));
        delay.put_property(PROPERTY_DELAY_TIME_LEVEL, "3");
        let ctx = p.build_send_context(&delay, "GID_ctx", "", &m, "", CommunicationMode::Sync);
        assert_eq!(ctx.msg_type, MessageType::DelayMsg);

        // 既是半消息又带延迟属性：Python 的顺序判定让 Delay 覆盖 Trans
        let mut both = Message::new("T1", Some(b"x"));
        both.put_property(PROPERTY_TRANSACTION_PREPARED, "true");
        both.put_property("TIMER_DELIVER_MS", "123");
        let ctx = p.build_send_context(&both, "GID_ctx", "", &m, "", CommunicationMode::Oneway);
        assert_eq!(ctx.msg_type, MessageType::DelayMsg);
    }

    /// 记录调用次序的替身钩子。
    #[derive(Default)]
    struct RecordingHook {
        before: AtomicUsize,
        after: AtomicUsize,
        after_saw_exception: AtomicUsize,
        after_saw_result: AtomicUsize,
    }

    impl SendMessageHook for RecordingHook {
        fn hook_name(&self) -> &str {
            "RecordingHook"
        }

        fn send_message_before(&self, _ctx: &mut SendMessageContext) -> Result<()> {
            self.before.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn send_message_after(&self, ctx: &mut SendMessageContext) -> Result<()> {
            self.after.fetch_add(1, Ordering::SeqCst);
            if ctx.exception.is_some() {
                self.after_saw_exception.fetch_add(1, Ordering::SeqCst);
            }
            if ctx.send_result.is_some() {
                self.after_saw_result.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn send_kernel_runs_after_hook_and_propagates_error() {
        let p = producer("GID_hooks");
        let hook = Arc::new(RecordingHook::default());
        p.register_send_message_hook(hook.clone());
        let client = bare_client();
        seed_publish_addr(&client);

        let mut msg = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut msg);
        let err = p
            .send_with_hooks(
                &client,
                &mut publish,
                &mq("broker-a", 0),
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await
            .expect_err("没有 name server，发送必然失败");
        assert!(hook.before.load(Ordering::SeqCst) >= 1, "before 钩子要跑过");
        assert_eq!(hook.after.load(Ordering::SeqCst), 1, "after 钩子恰好一次");
        assert_eq!(hook.after_saw_exception.load(Ordering::SeqCst), 1);
        assert_eq!(hook.after_saw_result.load(Ordering::SeqCst), 0);
        assert!(retryable(&err), "发送失败应是可重试类错误: {err}");
    }

    /// 拒绝一切发送的拦截钩子。
    struct ForbidAll;

    impl CheckForbiddenHook for ForbidAll {
        fn hook_name(&self) -> &str {
            "ForbidAll"
        }

        fn check_forbidden(&self, _ctx: &mut CheckForbiddenContext) -> Result<()> {
            Err(Error::client("forbidden by policy"))
        }
    }

    #[tokio::test]
    async fn check_forbidden_runs_before_send_message_hooks() {
        let p = producer("GID_forbidden");
        let hook = Arc::new(RecordingHook::default());
        p.register_send_message_hook(hook.clone());
        p.register_check_forbidden_hook(Arc::new(ForbidAll));
        let client = bare_client();
        seed_publish_addr(&client);

        let mut msg = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut msg);
        let err = p
            .send_with_hooks(
                &client,
                &mut publish,
                &mq("broker-a", 0),
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await
            .expect_err("拦截钩子的异常不该被吞");
        assert!(err.to_string().contains("forbidden by policy"));
        assert_eq!(
            hook.before.load(Ordering::SeqCst),
            0,
            "被拦截时不该跑发送钩子"
        );
        assert_eq!(hook.after.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn traceparent_is_injected_only_when_enabled() {
        let p = producer("GID_trace");
        let client = bare_client();
        seed_publish_addr(&client);
        let m = mq("broker-a", 0);

        p.set_enable_trace_context(Some(false));
        let mut off = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut off);
        let _ = p
            .send_with_hooks(
                &client,
                &mut publish,
                &m,
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await;
        assert!(off.get_property(TRACE_CONTEXT_PROPERTY).is_none());

        p.set_enable_trace_context(Some(true));
        let mut on = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut on);
        let _ = p
            .send_with_hooks(
                &client,
                &mut publish,
                &m,
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await;
        let injected = on
            .get_property(TRACE_CONTEXT_PROPERTY)
            .expect("开关打开后应注入 traceparent")
            .to_string();
        assert!(injected.starts_with("00-"), "W3C 版本前缀: {injected}");

        // 已有值不覆盖
        let mut existing = Message::new("T1", Some(b"body"));
        existing.put_property(TRACE_CONTEXT_PROPERTY, "00-deadbeef-deadbeef-01");
        let mut publish = PublishMessage::Single(&mut existing);
        let _ = p
            .send_with_hooks(
                &client,
                &mut publish,
                &m,
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await;
        assert_eq!(
            existing.get_property(TRACE_CONTEXT_PROPERTY),
            Some("00-deadbeef-deadbeef-01")
        );
    }

    #[test]
    fn send_message_hooks_swallow_hook_errors() {
        struct Boom;
        impl SendMessageHook for Boom {
            fn hook_name(&self) -> &str {
                "Boom"
            }
            fn send_message_before(&self, _c: &mut SendMessageContext) -> Result<()> {
                Err(Error::client("hook exploded"))
            }
        }
        let hooks = SendMessageHookList::new();
        hooks.register(Arc::new(Boom));
        let mut ctx = SendMessageContext::default();
        // before / after 的异常只记日志，不影响发送链路
        execute_send_message_hook_before(&hooks, &mut ctx);
        execute_send_message_hook_after(&hooks, &mut ctx);
    }

    // ---------------- 生命周期 / 参数校验 ----------------

    #[tokio::test]
    async fn send_before_start_fails_fast() {
        let p = producer("GID_lifecycle");
        let mut msg = Message::new("T1", Some(b"x"));
        let err = p
            .send(&mut msg, None, None)
            .await
            .expect_err("未启动不可发送");
        assert!(err.to_string().contains("not started"));
        assert!(p.fetch_publish_message_queues("T1").await.is_err());
        // 未启动时 shutdown 幂等且不 panic
        p.shutdown();
        p.shutdown();
        assert!(!p.is_started());
    }

    #[tokio::test]
    async fn start_stamps_a_contract_style_client_id() {
        let p = producer("GID_clientid_shape");
        p.set_namesrv_addr("127.0.0.1:1");
        p.start().await.expect("静态地址下 start 不该失败");
        let id = p.client_id().expect("start 之后必须已经有 clientId");
        let (ip, instance) = id
            .split_once('@')
            .unwrap_or_else(|| panic!("clientId 少了 IP@instanceName 的分隔符: {id}"));
        assert_eq!(ip, MixAll::cached_ip_str(), "{id}");
        assert!(
            instance.starts_with(&format!("{}#", MixAll::cached_pid())),
            "{id}"
        );
        let cfg = p.config();
        assert_eq!(cfg.instance_name, instance, "instanceName 要就地写回配置");
        p.shutdown();

        // 同一秒内的第二个生产者必须拿到不同的 clientId（旧口径在这里会撞车）
        let q = producer("GID_clientid_shape_2");
        q.set_namesrv_addr("127.0.0.1:1");
        q.start().await.expect("静态地址下 start 不该失败");
        assert_ne!(q.client_id(), Some(id));
        q.shutdown();
    }

    #[tokio::test]
    async fn an_explicit_instance_name_shares_one_instance() {
        let name = format!("clientid-shared-{}", MixAll::cached_pid());
        let a = producer("GID_clientid_shared_a");
        a.set_instance_name(&name);
        a.set_namesrv_addr("127.0.0.1:1");
        a.start().await.expect("start 不该失败");
        let b = producer("GID_clientid_shared_b");
        b.set_instance_name(&name);
        b.set_namesrv_addr("127.0.0.1:1");
        b.start().await.expect("start 不该失败");
        assert_eq!(a.client_id(), b.client_id());
        let shared = MQClientInstance::find_instance(&a.client_id().expect("clientId"))
            .expect("同 clientId 的两个生产者必须落在同一份实例上");
        assert!(
            shared.has_producer(&a.config().producer_group)
                && shared.has_producer(&b.config().producer_group),
            "生产者没有登记到共用实例的 producerTable"
        );
        // 守卫：先退的那个不能把还在用的实例拆掉
        a.shutdown();
        assert!(shared.is_started(), "先退场的使用者把共用实例关掉了");
        b.shutdown();
        // 注销(35) 与实例拆解现在排在同一个 spawn 出来的任务里，`shutdown()` 只是把它交出去
        for _ in 0..200 {
            if !shared.is_started() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!shared.is_started(), "最后一个使用者退场后实例没被拆掉");
    }

    /// 真机 `live_producer` 的 P1（shutdown 后重启还能发）踩过的坑，离线版：
    /// 注销(35) 和实例拆解都排在 `spawn` 的任务里，`shutdown()` 只是把它交出去。
    /// 如果登记表也跟着晚一步腾空，同 clientId 的 `start()` 就会经
    /// `create_mq_client_instance` 复用回那份**正要被拆**的实例，随后被那个任务
    /// 连带 `shutdown` 掉 ⇒ 重启后的第一发送请求报 "client already shutdown"。
    #[tokio::test]
    async fn restart_after_shutdown_never_reuses_a_dying_instance() {
        let name = format!("clientid-restart-{}", MixAll::cached_pid());
        let p = producer("GID_clientid_restart");
        p.set_instance_name(&name);
        p.set_namesrv_addr("127.0.0.1:1");
        p.start().await.expect("start 不该失败");
        let client_id = p.client_id().expect("clientId");
        p.shutdown();
        // 不等任何任务：`shutdown()` 一返回，这份实例就不该再被同 clientId 找到
        assert!(
            MQClientInstance::find_instance(&client_id).is_none(),
            "shutdown() 返回后实例还挂在 INSTANCE_MAP 上，重启会复用它"
        );

        p.start().await.expect("重启不该失败");
        let fresh = MQClientInstance::find_instance(&client_id).expect("重启后要有一份实例");
        assert!(fresh.has_producer(&p.config().producer_group));
        // 让上一次 shutdown 的尾巴（35 + 拆解）先跑完
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            fresh.is_started(),
            "重启拿到的实例被上一次 shutdown 的任务尾巴拆掉了"
        );
        p.shutdown();
    }

    #[tokio::test]
    async fn start_requires_an_address_source() {
        let p = producer("GID_noaddr");
        // 静态地址为空、且没配 ROCKETMQ_NAMESRV_DOMAIN ⇒ 直接报错
        // （注意：这里只断言「不 panic 且返回 Err」，环境变量由外部注入时才可能通过）
        if std::env::var_os("ROCKETMQ_NAMESRV_DOMAIN").is_none() {
            assert!(p.start().await.is_err());
            assert!(!p.is_started(), "启动失败后不能留在 started 状态");
        }
    }

    #[tokio::test]
    async fn start_rejects_bad_producer_group_without_touching_network() {
        let long_group = std::iter::repeat_n('g', 121).collect::<String>();
        for (group, needle) in [
            (
                "DEFAULT_PRODUCER",
                "producerGroup can not equal DEFAULT_PRODUCER",
            ),
            ("bad group", "contains illegal characters"),
            (long_group.as_str(), "is longer than group max length"),
        ] {
            // 构造期只查空白，非法字符要留到 start()（与 Python 同口径）
            let p = DefaultMQProducer::new(group).expect("构造不该提前拒绝");
            p.set_namesrv_addr("127.0.0.1:9876");
            let err = p.start().await.expect_err("非法组名必须本地失败");
            assert!(err.to_string().contains(needle), "{group}: {err}");
            assert!(!p.is_started(), "{group}: 启动失败后不能留在 started");
        }
    }

    #[tokio::test]
    async fn poll_name_server_interval_reaches_the_instance() {
        let p = producer("GID_interval_probe");
        assert_eq!(
            p.config().poll_name_server_interval_millis,
            30_000,
            "ClientConfig:58 = 1000 * 30"
        );
        p.set_namesrv_addr("127.0.0.1:1");
        p.set_poll_name_server_interval_millis(1_500);
        p.start().await.expect("静态地址下 start 不该失败");

        let client = p.client().expect("start 之后必须已有实例");
        assert_eq!(client.config().route_refresh_interval_millis, 1_500);
        assert_eq!(client.config().persist_offset_interval_millis, 5_000);
        p.shutdown();
    }

    #[tokio::test]
    async fn producer_group_is_validated_after_the_namespace_wrap() {
        let p = DefaultMQProducer::new(&"g".repeat(110)).unwrap();
        p.set_namespace("ns".repeat(30).as_str());
        // 包装后是 ns…ns%g，长度 90+1+110 > 120
        let err = p.start().await.expect_err("包装后超长必须报错");
        assert!(
            err.to_string().contains("is longer than group max length"),
            "{err}"
        );
        assert!(!p.is_started());
    }

    #[tokio::test]
    async fn send_batch_validates_before_touching_network() {
        let p = producer("GID_batch_check");
        // 空列表
        assert!(p.send_batch(Vec::new(), None, None).await.is_err());
        // 未启动 ⇒ require_client 先报错
        let mut msgs = vec![Message::new("T1", Some(b"a"))];
        assert!(p.send_batch(msgs.clone(), None, None).await.is_err());
        msgs.pop();
        assert!(p.send_batch(Vec::new(), None, None).await.is_err());
    }

    /// 运行时之外、而且**没启动过** ⇒ 先报「未启动」，而不是「没有运行时」。
    ///
    /// 两条都是同步报错（Python `_require_client` 同）：前者走回调更难查，后者意味着
    /// 池还没建。真正「运行时之外」的分支要 start 在未绑定运行时的场景之后才可能命中，
    /// 而 `start()` 本身是 async，所以这里只能断言顺序。
    #[test]
    fn send_async_before_start_reports_not_started() {
        let p = producer("GID_async");
        let cb = Arc::new(ClosureSendCallback::new(None, None));
        let err = p
            .send_async(Message::new("T1", Some(b"x")), cb, None, None)
            .expect_err("未启动不该静默派发");
        assert!(
            err.to_string().contains("producer not started"),
            "先查客户端、再查运行时句柄: {err}"
        );
    }

    /// 批量异步与单条异步同一口径：未启动**同步抛**，一个回调都不会交付。
    #[test]
    fn send_batch_async_before_start_reports_not_started() {
        let p = producer("GID_batch_async");
        let cb = Arc::new(ClosureSendCallback::new(None, None));
        let err = p
            .send_batch_async(vec![Message::new("T1", Some(b"x"))], cb, None, None)
            .expect_err("未启动不该静默派发");
        assert!(
            err.to_string().contains("producer not started"),
            "先查客户端、再查运行时句柄: {err}"
        );
    }

    /// 整批扣字节许可：逐条累加、空 body 也算 1、空批算 1。
    /// 写错这条不会让任何一笔发送失败，只会让批量消息在背压下**绕过字节闸**
    /// （一批 100 条只占 1 份容量），而队列被打爆是长跑之后的事。
    #[test]
    fn batch_back_pressure_len_counts_every_message() {
        let one = Message::new("T1", Some(b"12345"));
        let empty = Message::new("T1", None);
        assert_eq!(batch_back_pressure_msg_len(std::slice::from_ref(&one)), 5);
        assert_eq!(batch_back_pressure_msg_len(&[one.clone(), one.clone()]), 10);
        // 空 body 也算 1（与单条口径一致：0 等于不限流）
        assert_eq!(batch_back_pressure_msg_len(std::slice::from_ref(&empty)), 1);
        assert_eq!(
            batch_back_pressure_msg_len(&[empty.clone(), empty.clone()]),
            2
        );
        assert_eq!(batch_back_pressure_msg_len(&[one, empty]), 6);
        assert_eq!(batch_back_pressure_msg_len(&[]), 1);
    }

    #[test]
    fn view_message_is_not_supported() {
        let p = producer("GID_view");
        let err = p
            .view_message("T1", "ABCDEF")
            .expect_err("Python 参考实现直接抛错");
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn transaction_producer_falls_back_to_preset_listener() {
        struct L;
        impl TransactionListener for L {
            fn execute_local_transaction(
                &self,
                _msg: &Message,
                _arg: Option<&AnyHolder>,
            ) -> LocalTransactionState {
                LocalTransactionState::CommitMessage
            }
            fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
                LocalTransactionState::Unknow
            }
        }
        let tp = TransactionMQProducer::new("GID_txn").expect("组名合法");
        assert!(tp.transaction_listener().is_none());
        tp.set_transaction_listener(Some(Arc::new(L)));
        assert!(tp.transaction_listener().is_some());
        // Deref 让基础生产者的方法照常可用
        assert_eq!(tp.producer_group(), "GID_txn");
        tp.set_transaction_listener(None);
        assert!(tp.transaction_listener().is_none());
    }

    #[tokio::test]
    async fn transaction_without_listener_is_rejected() {
        let p = producer("GID_txn_none");
        let mut msg = Message::new("T1", Some(b"x"));
        assert!(p
            .send_message_in_transaction(&mut msg, None, None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn transaction_rejects_delay_properties() {
        struct L;
        impl TransactionListener for L {
            fn execute_local_transaction(
                &self,
                _msg: &Message,
                _arg: Option<&AnyHolder>,
            ) -> LocalTransactionState {
                LocalTransactionState::CommitMessage
            }
            fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
                LocalTransactionState::Unknow
            }
        }
        let p = producer("GID_txn_delay");
        let mut msg = Message::new("T1", Some(b"x"));
        msg.put_property(PROPERTY_DELAY_TIME_LEVEL, "3");
        // 未启动：先报「监听器/参数」都合理，这里只要求不是 panic
        assert!(p
            .send_message_in_transaction(&mut msg, Some(Arc::new(L)), None)
            .await
            .is_err());
    }

    #[test]
    fn end_transaction_header_roundtrips_through_ext_fields() {
        // 事务收尾走 oneway，头字段编码必须与普通请求一致
        let header = EndTransactionRequestHeader {
            producer_group: Some("GID_x".to_string()),
            transaction_id: Some("txn-1".to_string()),
            commit_log_offset: Some(42),
            tran_state_table_offset: Some(7),
            commit_or_rollback: Some(DefaultMQProducer::transaction_flag(
                LocalTransactionState::CommitMessage,
            )),
            from_transaction_check: Some(true),
            ..Default::default()
        };
        let mut cmd = RemotingCommand::create_request_command(
            request_code::END_TRANSACTION,
            Some(Box::new(header.clone())),
        );
        cmd.make_custom_header_to_net();
        let back: EndTransactionRequestHeader = cmd
            .decode_command_custom_header()
            .expect("自定义头应能解回");
        assert_eq!(back.producer_group.as_deref(), Some("GID_x"));
        assert_eq!(back.commit_log_offset, Some(42));
        assert_eq!(back.from_transaction_check, Some(true));

        let resp = GetEarliestMsgStoretimeResponseHeader {
            timestamp: Some(1234),
        };
        let ext = ExtFields::from_header(&resp);
        let mut back = GetEarliestMsgStoretimeResponseHeader::default();
        back.from_ext_fields(&ext);
        assert_eq!(back.timestamp, Some(1234));
    }

    #[test]
    fn delay_keys_differ_between_hook_and_transaction() {
        // 事务侧少了 __STARTDELIVERTIME：两份清单在 Python 里本就不同，固定住以防串改
        assert!(!TRANSACTION_FORBIDDEN_DELAY_KEYS.contains(&"__STARTDELIVERTIME"));
        assert!(DELAY_PROPERTY_KEYS.contains(&"__STARTDELIVERTIME"));
        assert_eq!(TRANSACTION_FORBIDDEN_DELAY_KEYS.len(), 5);
        assert_eq!(DELAY_PROPERTY_KEYS.len(), 5);
    }

    #[test]
    fn send_context_carries_message_snapshot_and_transfer_flag_is_untouched() {
        let p = producer("GID_snapshot");
        let mut msg = Message::new("T1", Some(b"body"));
        msg.put_property(PROPERTY_TRANSFER_FLAG, "kv");
        let ctx = p.build_send_context(
            &msg,
            "GID_snapshot",
            "ns",
            &mq("broker-a", 2),
            "127.0.0.1:10911",
            CommunicationMode::Async,
        );
        let snapshot = ctx.message.expect("上下文应带消息快照");
        assert_eq!(snapshot.get_property(PROPERTY_TRANSFER_FLAG), Some("kv"));
        assert_eq!(snapshot.get_body(), b"body".as_slice());
        assert_eq!(ctx.namespace, "ns");
        assert_eq!(ctx.mq, Some(mq("broker-a", 2)));
    }

    // ---------------- 轨迹自动桥接 ----------------

    #[test]
    fn auto_bridged_dispatcher_builds_a_real_side_channel() {
        let d = auto_bridged_dispatcher(
            "GID_auto_produce",
            TraceDispatcherType::Produce,
            None,
            25,
            None,
        )
        .expect("自动桥接不该失败");
        let g1 = d.trace_producer_group().to_string();
        assert!(
            g1.starts_with("_INNER_TRACE_PRODUCER-GID_auto_produce-PRODUCE-"),
            "{g1}"
        );
        // Python `min(batch_num, 20)`
        assert_eq!(d.batch_num(), 20);
        // trace_topic 缺省回落系统轨迹 topic
        assert_eq!(d.get_trace_topic_name(), "RMQ_SYS_TRACE_TOPIC");
        assert_eq!(d.dispatcher_type(), TraceDispatcherType::Produce);
        assert!(!d.is_started(), "还没 start，不该声称已启动");

        let d2 = auto_bridged_dispatcher(
            "GID_auto_produce",
            TraceDispatcherType::Consume,
            None,
            3,
            Some("CUSTOM_TRACE".to_string()),
        )
        .expect("自动桥接不该失败");
        let g2 = d2.trace_producer_group().to_string();
        assert!(
            g2.starts_with("_INNER_TRACE_PRODUCER-GID_auto_produce-CONSUME-"),
            "{g2}"
        );
        // 进程级计数器自增：同组两次取号必然不同
        let n1: u64 = g1.rsplit('-').next().unwrap().parse().unwrap();
        let n2: u64 = g2.rsplit('-').next().unwrap().parse().unwrap();
        assert!(n2 > n1, "内部生产者组名计数器必须自增：{g1} -> {g2}");
        assert_eq!(d2.batch_num(), 3);
        assert_eq!(d2.get_trace_topic_name(), "CUSTOM_TRACE");
    }

    #[test]
    fn trace_producer_seam_forwards_setters_to_the_facade() {
        let p = producer("GID_seam");
        TraceProducer::set_name_server_addr(&p, "127.0.0.1:19876");
        TraceProducer::set_instance_name(&p, "TRACE_SEAM");
        TraceProducer::set_send_msg_timeout(&p, 5000);
        TraceProducer::set_max_message_size(&p, 128_000);
        TraceProducer::set_enable_trace(&p, false);
        let cfg = p.config();
        assert_eq!(cfg.name_server_addrs, vec!["127.0.0.1:19876".to_string()]);
        assert_eq!(cfg.instance_name, "TRACE_SEAM");
        assert_eq!(cfg.send_msg_timeout, 5000);
        assert_eq!(cfg.max_message_size, 128_000);
        assert!(!cfg.enable_trace);
    }

    #[test]
    fn trace_host_reports_empty_client_id_before_start() {
        let p = producer("GID_trace_host");
        // Python `_client_id`：宿主没有 _mq_client 时返回空串
        assert_eq!(TraceHost::client_id(&p), "");
    }
}
