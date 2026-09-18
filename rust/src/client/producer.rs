//! `DefaultMQProducer`：消息发送入口（对应
//! `org.apache.rocketmq.client.producer.DefaultMQProducer` +
//! `impl.producer.DefaultMQProducerImpl`，逐条移植
//! `python/rocketmq/client/producer.py`）。
//!
//! 职责：生产者配置、生命周期（与 [`MQClientInstance`] 共享实例 + 生产者心跳线程）、
//! 同步/异步/单向/定点/选择器/批量发送、发送重试与延迟故障规避、发送前后钩子与
//! 发送前拦截钩子、W3C traceparent 注入、消息轨迹、Request-Reply、事务消息
//! （半消息 + 本地事务 + END_TRANSACTION + broker 回查）与几个管理类便捷方法。
//!
//! ## 与 Python 参考实现的有意差异（均在对应条目 doc 上再次标注）
//!
//! 1. **句柄可克隆**：Python 的生产者对象被回调/处理器共享；Rust 里
//!    [`DefaultMQProducer`] 是 `Clone` 的 `Arc<Inner>`，配置走 `RwLock` 内部可变，
//!    于是 setter 与 Python 一样是「随时可调、启动后部分拒绝」。
//! 2. **回调/监听器是 trait 对象**（Python 是鸭子类型）：[`SendCallback`]、
//!    [`MessageQueueSelector`]、[`TransactionListener`]。
//! 3. **`send_async` 用 `tokio::spawn`**（Python 起线程；Java 用 Netty 异步 +
//!    线程池回调）。派发用的运行时句柄在 [`DefaultMQProducer::start`] 时惰性绑定并
//!    缓存（构造允许发生在运行时之外），所以 `send_async` 与心跳一样不要求调用点
//!    处于运行时上下文。
//! 4. **轨迹分发器由调用方注入**（[`DefaultMQProducer::set_trace_dispatcher`]）：
//!    Python 在 `start()` 里 `new AsyncTraceDispatcher(...)`；本 crate 的统一约定是
//!    依赖注入（见 `mq_client.rs` 模块头差异 2），构造 `AsyncTraceDispatcher` 需要
//!    它自己的内部生产者与运行时，放进生产者锁里不合适。注入后 `start()` 仍会照
//!    Python 的顺序注册 `SendMessageTraceHook` / `EndTransactionTraceHook` 并启动它。
//! 5. `time.time()*1000` → [`current_time_millis`]；`threading.Lock` → `Mutex`。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::client::hook::{
    AnyHolder, CheckForbiddenContext, CheckForbiddenHook, CheckForbiddenHookList,
    CommunicationMode, EndTransactionContext, EndTransactionHook, EndTransactionHookList,
    SendMessageContext, SendMessageHook, SendMessageHookList,
};
use crate::client::latency::MQFaultStrategy;
use crate::client::metrics::ClientMetrics;
use crate::client::mq_client::{
    MQClientInstance, MQClientInstanceConfig, PublishMessage, TopicPublishInfo, TraceDispatcher,
};
use crate::client::request_reply::{
    create_correlation_id, request_future_holder, RequestResponseFuture,
    DEFAULT_REQUEST_TIMEOUT_MILLIS,
};
use crate::client::result::{
    LocalTransactionState, SendResult, SendStatus, TransactionSendResult,
};
use crate::client::top_addressing::DefaultTopAddressing;
use crate::client::trace::TraceContext;
use crate::client::trace_hook::{
    EndTransactionTraceHook, SendMessageTraceHook, TraceReportSink,
};
use crate::common::compression;
use crate::common::message::{Message, MessageBatch, MessageExt, MessageQueue};
use crate::common::message_const::{
    PROPERTY_CORRELATION_ID, PROPERTY_DELAY_TIME_LEVEL, PROPERTY_DELAY_TIME,
    PROPERTY_MESSAGE_REPLY_TO_CLIENT, PROPERTY_MESSAGE_TTL, PROPERTY_PRODUCER_GROUP,
    PROPERTY_TRANSACTION_PREPARED, PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
};
use crate::common::message_decoder::{decode_message, decode_message_id, decode_messages};
use crate::common::message_type::MessageType;
use crate::common::mix_all::MixAll;
use crate::common::sysflag::{MessageSysFlag, PermName};
use crate::common::util_all::{current_time_millis, java_string_hash};
use crate::error::{Error, Result};
use crate::remoting::client::{RequestProcessor, ResponseSink};
use crate::remoting::protocol::codes::request_code;
use crate::remoting::protocol::ext_fields::CustomHeader;
use crate::remoting::protocol::headers::{
    CheckTransactionStateRequestHeader, EndTransactionRequestHeader,
    GetEarliestMsgStoretimeRequestHeader, GetEarliestMsgStoretimeResponseHeader,
};
use crate::remoting::protocol::heartbeat::{HeartbeatData, ProducerData};
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::remoting::protocol::remoting_command::RemotingCommand;
use crate::remoting::rpchook::RPCHook;
use crate::client::trace_context::{inject_trace_context, trace_context_enabled_from_env};
use crate::{bail, rmq_debug, rmq_warn};

/// Python `DefaultMQProducer.instance_name` 的默认值。
pub const DEFAULT_INSTANCE_NAME: &str = "DEFAULT";
/// Java `DefaultMQProducer#maxMessageSize` 默认 4 MiB。
pub const DEFAULT_MAX_MESSAGE_SIZE: i32 = 1024 * 1024 * 4;
/// Java `DefaultMQProducer#compressMsgBodyOverHowmuch` 默认 4 KiB。
pub const DEFAULT_COMPRESS_MSG_BODY_OVER_HOWMUCH: i32 = 1024 * 4;
/// Java `MessageSysFlag.COMPRESSION_LEVEL` 默认 zlib level 5。
pub const DEFAULT_COMPRESS_LEVEL: i32 = 5;

// ================================================================ 队列选择器

/// 队列选择器（对应 Java `MessageQueueSelector`，Python `producer.MessageQueueSelector`）。
///
/// `arg` 与 C++/dotnet 移植一致取 `&str`（Python 是任意对象，但三个内置选择器只用到
/// 字符串/整数语义）。
pub trait MessageQueueSelector: Send + Sync + 'static {
    /// 对应 Java `MessageQueueSelector#select`。
    fn select(&self, mqs: &[MessageQueue], msg: &Message, arg: &str) -> Result<MessageQueue>;
}

/// 按 `arg` 的 hash 选队列（Java `SelectMessageQueueByHash`）。
#[derive(Debug, Default, Clone, Copy)]
pub struct SelectMessageQueueByHash;

impl MessageQueueSelector for SelectMessageQueueByHash {
    /// Python 用内建 `hash(arg)`，字符串的 `hash()` 随 `PYTHONHASHSEED` 变化、跨进程
    /// 不可复现；这里与 C++/dotnet 移植相同，取 Java `String.hashCode()` 语义
    /// （确定性，适合当分片键）。
    fn select(&self, mqs: &[MessageQueue], _msg: &Message, arg: &str) -> Result<MessageQueue> {
        if mqs.is_empty() {
            bail!("no message queue");
        }
        let hash = java_string_hash(arg);
        // Java `Math.abs(Integer.MIN_VALUE)` 仍是负数，这里显式收敛到 0（C++ 移植同处理）。
        let idx = if hash < 0 {
            (hash.checked_neg().unwrap_or(0) as usize) % mqs.len()
        } else {
            (hash as usize) % mqs.len()
        };
        Ok(mqs[idx].clone())
    }
}

/// 随机选一个队列（Java `SelectMessageQueueByRandom`）。
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

/// 按机房前缀（brokerName 前缀）选队列（Java `SelectMessageQueueByMachineRoom`）。
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

/// 异步发送回调（对应 Java `SendCallback`，Python `producer.SendCallback`）。
pub trait SendCallback: Send + Sync + 'static {
    /// 对应 Java `onSuccess`。
    fn on_success(&self, result: SendResult);

    /// 对应 Java `onException`。
    fn on_exception(&self, err: Error);
}

/// 成功回调闭包（Python `SendCallbackImpl(success_fn=...)`）。
pub type OnSuccessFn = Box<dyn FnOnce(SendResult) + Send + 'static>;

/// 失败回调闭包（Python `SendCallbackImpl(exception_fn=...)`）。
pub type OnExceptionFn = Box<dyn FnOnce(Error) + Send + 'static>;

/// 便捷回调：把两个普通闭包拼成 [`SendCallback`]
/// （对应 Python `SendCallbackImpl`）。
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
        self.success.lock().unwrap_or_else(|e| e.into_inner()).take()
    }

    fn take_failure(&self) -> Option<OnExceptionFn> {
        self.failure.lock().unwrap_or_else(|e| e.into_inner()).take()
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

/// 事务监听器（对应 Java `TransactionListener`，Python `producer.TransactionListener`）。
///
/// Python 允许 `execute_local_transaction` 返回 `None`（视作 `UNKNOW`）并捕获异常；
/// Rust 用 [`LocalTransactionState::Unknow`] 表达「不知道」，监听器内部自己把异常
/// 转成 `Unknow`（发送链路的 `catch` 语义见
/// [`DefaultMQProducer::send_message_in_transaction`]）。
pub trait TransactionListener: Send + Sync + 'static {
    /// 对应 Java `executeLocalTransaction`。
    fn execute_local_transaction(
        &self,
        msg: &Message,
        arg: Option<&AnyHolder>,
    ) -> LocalTransactionState;

    /// 对应 Java `checkLocalTransaction`。
    fn check_local_transaction(&self, msg: &MessageExt) -> LocalTransactionState;
}

// ================================================================ 轨迹分发器通道

/// 生产者持有的轨迹分发器要同时满足两个已有接缝：
///
/// * [`TraceReportSink`]（`trace_hook.rs`）—— 轨迹钩子往里投；
/// * [`TraceDispatcher`]（`mq_client.rs`）—— 生命周期（`start` / `shutdown`）。
///
/// 对应 Python `self.trace_dispatcher = AsyncTraceDispatcher(...)`、
/// `dispatcher.start(namesrv, AccessChannel.LOCAL)`、`dispatcher.shutdown()`。
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

/// 生产者可配置项（对应 Python `DefaultMQProducer.__init__` 里那批属性）。
///
/// 默认值逐条对齐 Java `DefaultMQProducer` 的字段初始化（见
/// [`Default`] 实现，与 Python 构造函数一致）。
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Python `producer_group`。
    pub producer_group: String,
    /// Python `tls_enable`：`None` = 交给环境变量 `ROCKETMQ_TLS_ENABLE`。
    pub tls_enable: Option<bool>,
    /// Python `enable_trace_context`：`None` = 交给环境变量
    /// `ROCKETMQ_TRACE_CONTEXT_ENABLE`。
    pub enable_trace_context: Option<bool>,
    /// Python `namespace`（非空时 topic 与 producerGroup 都加 `<ns>%` 前缀）。
    pub namespace: String,
    /// Python `instance_name`。
    pub instance_name: String,
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
    /// Python `retry_times_when_send_async_failed`（Java 有、Python 未读取，这里同样只存）。
    pub retry_times_when_send_async_failed: i32,
    /// Python `retry_another_broker_when_not_store_ok`（Java
    /// `retryAnotherBrokerWhenNotStoreOK`，默认 false）。
    pub retry_another_broker_when_not_store_ok: bool,
    /// Python `max_message_size`。
    pub max_message_size: i32,
    /// Python `topics`。
    pub topics: Vec<String>,
    /// Python `name_server_addrs`。
    pub name_server_addrs: Vec<String>,
    /// Python `heartbeat_interval_millis`。
    pub heartbeat_interval_millis: u64,
    /// Python `send_latency_fault_enable`。
    pub send_latency_fault_enable: bool,
    /// Python `enable_trace`。
    pub enable_trace: bool,
    /// Python `trace_topic`（`None` → 系统默认轨迹 topic）。
    pub trace_topic: Option<String>,
    /// Python `trace_msg_batch_num`。
    pub trace_msg_batch_num: i32,
    /// Python `request_timeout`（`request()` 未显式给 timeout 时用）。
    pub request_timeout: i64,
}

impl Default for ProducerConfig {
    fn default() -> ProducerConfig {
        ProducerConfig {
            producer_group: MixAll::DEFAULT_PRODUCER_GROUP.to_string(),
            tls_enable: None,
            enable_trace_context: None,
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            client_id: None,
            create_topic_key: MixAll::DEFAULT_TOPIC.to_string(),
            default_topic_queue_nums: MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
            send_msg_timeout: 3000,
            compress_msg_body_over_howmuch: DEFAULT_COMPRESS_MSG_BODY_OVER_HOWMUCH,
            compress_level: DEFAULT_COMPRESS_LEVEL,
            compress_type: MessageSysFlag::ZLIB_TYPE,
            retry_times_when_send_failed: 2,
            retry_times_when_send_async_failed: 2,
            retry_another_broker_when_not_store_ok: false,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            topics: Vec::new(),
            name_server_addrs: Vec::new(),
            heartbeat_interval_millis: 30_000,
            send_latency_fault_enable: false,
            enable_trace: false,
            trace_topic: None,
            trace_msg_batch_num: 10,
            request_timeout: DEFAULT_REQUEST_TIMEOUT_MILLIS,
        }
    }
}

// ================================================================ 共享状态

/// 生产者的共享状态（对应 Python `DefaultMQProducer` 的实例属性 + `_lock`）。
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

    fn client(&self) -> Option<MQClientInstance> {
        self.client
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn trace_dispatcher(&self) -> Option<Arc<dyn TraceDispatcherChannel>> {
        self.trace
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
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

/// 默认生产者（对应 Java `DefaultMQProducer` + `DefaultMQProducerImpl`，
/// 移植自 Python `producer.DefaultMQProducer`）。
///
/// 克隆得到的副本与原对象**共享同一份状态**（差异 1）：Python 的对象引用语义、
/// Java 的引用语义在 Rust 里都靠 `Arc<Inner>` 落地，所以把生产者交给回调/处理器
/// 之后仍可继续 `set_xxx`，与 `producer.py` 的行为一致。
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
        let cfg = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner());
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

    /// 对应 Python `DefaultMQProducer(producer_group, rpc_hook)`
    /// （Java `DefaultMQProducer(group, RPCHook)`）。
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

    /// Python 的 `client_id` 属性：显式指定则 `start()` 不再生成
    /// （Java `ClientConfig#setInstanceId` 之外的一条口子，Python 直接改属性）。
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

    /// Python `set_compress_msg_body_over_howmuch`。
    pub fn set_compress_msg_body_over_howmuch(&self, size: i32) {
        self.write_cfg(|c| c.compress_msg_body_over_howmuch = size);
    }

    /// Python `set_compress_level`。
    pub fn set_compress_level(&self, level: i32) {
        self.write_cfg(|c| c.compress_level = level);
    }

    /// Python `set_compress_type`（取 [`MessageSysFlag::ZLIB_TYPE`] 等**算法号**）。
    pub fn set_compress_type(&self, compression_type: i32) {
        self.write_cfg(|c| c.compress_type = compression_type);
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
        self.inner.fault_strategy.set_send_latency_fault_enable(enable);
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
        *self
            .inner
            .trace
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = dispatcher;
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
        f(&mut self
            .inner
            .cfg
            .write()
            .unwrap_or_else(|e| e.into_inner()));
    }

    // ---------------- 生命周期 ----------------

    /// Python `start()`。
    ///
    /// 步骤与 `producer.py:434-469` 逐条对齐：校验地址 → 生成 clientId →
    /// 生产者组拼命名空间 → 建/复用 [`MQClientInstance`] → 注册 RPC 钩子 →
    /// 启动实例 → 回填动态拿到的 namesrv → 注册事务回查处理器 → 起心跳任务。
    ///
    /// 与 Python 的两处有意差别：
    /// 1. Python 每次 `start()` 都 `MQClientInstance(...)` 新建实例（同 clientId 会
    ///    覆盖 `INSTANCE_MAP`）；这里走 Java 的
    ///    [`MQClientInstance::create_mq_client_instance`]，同 clientId 存活实例**复用**。
    /// 2. 幂等靠 `started` 的 CAS（Python 靠 `_lock` + `_started` 标志）；重复调用
    ///    直接 `Ok(())`，`shutdown()` 之后可再次 `start()`。
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
        if cfg.name_server_addrs.is_empty() && !DefaultTopAddressing::is_configured() {
            self.inner.started.store(false, Ordering::Release);
            // 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
            return Err(Error::client("name server address is not set"));
        }
        let client_id = cfg
            .client_id
            .clone()
            .unwrap_or_else(|| MixAll::build_default_client_id(&cfg.instance_name));
        // 生产者组也拼命名空间（对齐 Java DefaultMQProducer.start:375
        // setProducerGroup(withNamespace(producerGroup))），broker 侧按带前缀的组名登记
        let group = if cfg.namespace.is_empty() {
            cfg.producer_group.clone()
        } else {
            NamespaceUtil::wrap_namespace(&cfg.namespace, &cfg.producer_group)
        };
        // Python 在 `__init__` 里就把 None 解析成布尔；这里等价地在 start 时定型。
        let trace_context_on = cfg
            .enable_trace_context
            .unwrap_or_else(trace_context_enabled_from_env);
        self.write_cfg(|c| {
            c.client_id = Some(client_id.clone());
            c.producer_group = group.clone();
            c.enable_trace_context = Some(trace_context_on);
        });

        let instance_cfg = MQClientInstanceConfig {
            tls_enable: cfg.tls_enable,
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
        *self
            .inner
            .client
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(client.clone());

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
                    if tokio::time::timeout(Duration::from_millis(interval_millis.max(1000)), rx.changed())
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
            rmq_warn!("producer: no tokio runtime, heartbeat disabled; broker transaction \
                       check-back will not reach this producer");
        }

        // 轨迹分发器在锁外启动（Java 同样在 defaultMQProducerImpl.start() 之后做）：
        // 它要新建内部生产者并拉路由，属网络操作，不该占着生产者自己的锁。
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
        for task in self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
        // ⚠ 与 Python 的差别：Python 只把 `_mq_client` 留着（下次 start 会新建并覆盖
        // INSTANCE_MAP）；这里丢掉引用，好让 `create_mq_client_instance` 在重启时
        // 建出干净的新实例，而不是复用一个已被 shutdown 的。
        let client = self
            .inner
            .client
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(client) = client {
            client.shutdown();
        }
        // 顺序对齐 Java DefaultMQProducer.shutdown()：先关本生产者，再 flush 并关轨迹分发器
        // （分发器用的是**自己的**内部生产者，与本客户端实例无关，所以关掉了照样能发完）
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

    /// Python `_start_trace_dispatcher`（对应 Java `DefaultMQProducer.start():380-405`）。
    ///
    /// 差异 4：`AsyncTraceDispatcher` 由调用方
    /// [`set_trace_dispatcher`](Self::set_trace_dispatcher) 注入，本函数只负责
    /// 「注册两个轨迹钩子 + start」。任何失败都只记日志 —— 轨迹挂了不能影响正常发送。
    fn start_trace_dispatcher(&self) {
        let cfg = self.config();
        let Some(dispatcher) = self.inner.trace_dispatcher() else {
            if cfg.enable_trace {
                rmq_warn!(
                    "system mqtrace hook init skipped: enableTrace is on but no trace dispatcher \
                     was injected (see producer.rs module header deviation 4)"
                );
            }
            return;
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

/// Python `send` 的重试判据：`except (MQClientException, MQBrokerException,
/// RemotingException)` 才重试，其它异常（例如批量消息的 `ValueError`、参数校验）
/// 直接向上抛。
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

// ================================================================ 发送路径

impl DefaultMQProducer {
    // ---------------- 校验 / 压缩 / 路由 ----------------

    /// Python `_check_message`。
    ///
    /// ⚠ Python 对 `body is None` 会 `len(None)` 抛 `TypeError`；这里把「无正文」
    /// 当作 0 字节放过（Java 的 `Message#getBody()` 返回 null 时同样不进长度比较）。
    fn check_message(&self, msg: &Message) -> Result<()> {
        let max = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .max_message_size;
        if msg.topic.is_empty() {
            bail!("message topic is empty");
        }
        let len = msg.get_body().len();
        if len > max as usize {
            bail!("message body size {len} exceeds maxMessageSize {max}");
        }
        Ok(())
    }

    /// Python `try_to_compress_message`：满足阈值时**就地压缩**，返回应下发的 sys_flag。
    ///
    /// 与 Java 逐条对齐：批量消息永不压缩；body 长度 >= 阈值才压；压缩失败**降级为
    /// 不压缩**（不让发送失败）；压缩后不比较体积。不压缩时返回 0。
    fn try_to_compress_message(&self, msg: &mut Message) -> i32 {
        let (threshold, compress_type, level) = {
            let cfg = self
                .inner
                .cfg
                .read()
                .unwrap_or_else(|e| e.into_inner());
            (
                cfg.compress_msg_body_over_howmuch,
                cfg.compress_type,
                cfg.compress_level,
            )
        };
        let body = msg.get_body();
        if body.is_empty() || (body.len() as i32) < threshold {
            return 0;
        }
        let compressed = match compression::compress(body, compress_type, level) {
            Ok(c) => c,
            Err(e) => {
                // 对齐 Java：压缩失败降级为不压缩
                rmq_warn!("tryToCompressMessage failed, send uncompressed: {e}");
                return 0;
            }
        };
        if compressed.is_empty() {
            return 0;
        }
        msg.set_body(Some(&compressed));
        MessageSysFlag::set_compression_type(MessageSysFlag::COMPRESSED_FLAG, compress_type)
    }

    /// 批量版本的压缩判定（Python 由 `isinstance(msg, MessageBatch)` 直接返回 0）。
    fn sys_flag_for(&self, msg: &mut PublishMessage<'_>) -> i32 {
        if msg.is_batch() {
            return 0;
        }
        self.try_to_compress_message(msg.as_message_mut())
    }

    /// Python `_with_namespace`：给 topic 拼上命名空间前缀
    /// （对应 Java `ClientConfig.withNamespace`）。
    ///
    /// Java 在每个 `DefaultMQProducer.send*` 公开入口都做
    /// `msg.setTopic(withNamespace(...))`，broker 侧看到的资源名是
    /// `<namespace>%<topic>`；生产者组在 `start()` 里同样被包装。
    fn with_namespace(&self, topic: &str) -> String {
        let namespace = self.inner.namespace();
        if namespace.is_empty() {
            return topic.to_string();
        }
        NamespaceUtil::wrap_namespace(&namespace, topic)
    }

    /// Python `_topic_publish_info`（对应 Java
    /// `DefaultMQProducerImpl.tryToFindTopicPublishInfo`）。
    ///
    /// 先拉**真实**路由；只有确实拉不到（新 topic 尚未在 NameServer 注册）时才按 Java
    /// 的做法回退到默认 topic（TBW102）为该 topic 合成发布信息，否则新 topic 的
    /// **首条**消息没队列可选。消费者路径不做这个兜底。
    async fn topic_publish_info(
        &self,
        client: &MQClientInstance,
        topic: &str,
    ) -> Result<Arc<TopicPublishInfo>> {
        client.register_topic_in_use(topic);
        match client.get_topic_publish_info(topic, false).await {
            Ok(info) => Ok(info),
            Err(e) if retryable(&e) => client.get_topic_publish_info(topic, true).await,
            Err(e) => Err(e),
        }
    }

    /// Python `_need_addr`：broker 地址缺失时报错（oneway 路径没有「拿不到就不填」
    /// 的余地）。
    fn need_addr(&self, client: &MQClientInstance, mq: &MessageQueue) -> Result<String> {
        client
            .broker_addr_of(&mq.broker_name)
            .ok_or_else(|| Error::client(format!("broker address not found for {}", mq.broker_name)))
    }

    // ---------------- 钩子上下文 ----------------

    /// Python `_build_send_context`（对齐 Java `DefaultMQProducerImpl:969-989`）。
    ///
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
            ..Default::default()
        };
        crate::client::hook::execute_check_forbidden_hook(
            &self.inner.check_forbidden_hooks,
            &mut context,
        )
    }

    // ---------------- 发送内核 ----------------

    /// Python `_send_with_hooks`：真正发起请求的那一步
    /// （对应 Java `sendKernelImpl` 内的钩子点）。
    ///
    /// 执行顺序严格照抄 Java `sendKernelImpl:956-990`：
    ///   1. **CheckForbiddenHook**（每次尝试都跑；异常**不吞**，直接抛给重试链）
    ///   2. SendMessageHook.before
    ///   3. 发请求
    ///   4. SendMessageHook.after（成功带 sendResult / 失败带 exception）
    ///
    /// 重试时每轮都会重建 context，所以钩子会被调用多次 —— 与 Java 一致。
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
        // Python `try: ... except Exception: pass` —— 拿不到地址就按空串继续
        let broker_addr = client
            .broker_addr_of(&mq_sel.broker_name)
            .unwrap_or_default();
        if self.has_check_forbidden_hook() {
            self.execute_check_forbidden(msg.as_message(), mq_sel, &broker_addr, arg, mode)?;
        }
        // W3C traceparent 透传（opt-in）：没有就注入根上下文，已有值不覆盖
        if trace_context_on {
            inject_trace_context(msg.as_message_mut());
        }
        if !self.has_send_message_hook() {
            return client.send_message(&group, msg, mq_sel, timeout, sys_flag).await;
        }
        let mut context =
            self.build_send_context(msg.as_message(), &group, &namespace, mq_sel, &broker_addr, mode);
        crate::client::hook::execute_send_message_hook_before(&self.inner.send_hooks, &mut context);
        match client.send_message(&group, msg, mq_sel, timeout, sys_flag).await {
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
        let client = self.require_client()?;
        let (timeout, retry_times) = {
            let cfg = self
                .inner
                .cfg
                .read()
                .unwrap_or_else(|e| e.into_inner());
            (
                timeout_millis.unwrap_or(cfg.send_msg_timeout),
                cfg.retry_times_when_send_failed,
            )
        };
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic.clone();
        self.check_message(msg)?;
        // 在重试循环**之外**压缩一次：Java 是在循环内调用 tryToCompressMessage 的，
        // 而它会就地 setBody，重试时会把已压缩的 body 再压一遍（zlib(zlib(x))），
        // 消费端只解一层就拿到压缩流。这里避免该问题。
        let sys_flag = self.try_to_compress_message(msg);

        if let Some(mq) = mq {
            // 定点发送同样要过钩子（Java：目标是 mq 也走 sendKernelImpl）
            let mut publish = PublishMessage::Single(msg);
            if self.has_send_interceptors() {
                return self
                    .send_with_hooks(
                        &client,
                        &mut publish,
                        mq,
                        timeout,
                        sys_flag,
                        None,
                        CommunicationMode::Sync,
                    )
                    .await;
            }
            return client
                .send_message(&self.inner.producer_group(), &mut publish, mq, timeout, sys_flag)
                .await;
        }

        let mut last_exc: Option<Error> = None;
        let mut last_broker_name: Option<String> = None;
        for _ in 0..=retry_times.max(0) {
            match self
                .send_attempt(&client, &topic, msg, timeout, sys_flag, &mut last_broker_name)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) if retryable(&e) => last_exc = Some(e),
                Err(e) => return Err(e),
            }
        }
        match last_exc {
            Some(e) => Err(e),
            // retry_times 被配成负数：Python 在这里会 `raise None`（TypeError），
            // 这里给一条可读的客户端错误。
            None => Err(Error::client("no send attempt was made")),
        }
    }

    /// Python `send` 重试循环的**单次尝试**（选队 + 指标 + 故障规避 + 发送）。
    async fn send_attempt(
        &self,
        client: &MQClientInstance,
        topic: &str,
        msg: &mut Message,
        timeout: i64,
        sys_flag: i32,
        last_broker_name: &mut Option<String>,
    ) -> Result<SendResult> {
        let publish = self.topic_publish_info(client, topic).await?;
        // 故障规避：开启时按 broker 延迟/隔离状态选队列（Java MQFaultStrategy）；
        // 关闭时退化为普通轮询（策略内部判断）。
        let selected = self.inner.fault_strategy.select_one_message_queue(
            &*publish,
            last_broker_name.as_deref(),
            false,
        )?;
        *last_broker_name = Some(selected.broker_name.clone());
        let mq_sel = MessageQueue::new(topic, &selected.broker_name, selected.queue_id);
        let broker_name = selected.broker_name.clone();
        let send_start = self.inner.metrics.record_send_start();
        let mut publish_msg = PublishMessage::Single(msg);
        let result = self
            .send_with_hooks(
                client,
                &mut publish_msg,
                &mq_sel,
                timeout,
                sys_flag,
                None,
                CommunicationMode::Sync,
            )
            .await;
        match result {
            Ok(result) => {
                self.inner.metrics.record_send_success(send_start);
                // 记录发送延迟；超出阈值会把该 broker 隔离一段时间
                let latency = current_time_millis() as f64 - send_start;
                self.inner
                    .fault_strategy
                    .update_fault_item(&broker_name, latency as i64, false, true);
                Ok(result)
            }
            Err(e) => {
                self.inner.metrics.record_send_failure(send_start);
                self.inner
                    .fault_strategy
                    .update_fault_item(&broker_name, 0, true, false);
                Err(e)
            }
        }
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
        let mut msgs = msgs;
        for msg in &mut msgs {
            let topic = self.with_namespace(&msg.topic);
            msg.topic = topic;
        }
        let topic = msgs[0].topic.clone();
        let mut batch = MessageBatch::generate_from_list(msgs)?;
        let mut publish_msg = PublishMessage::Batch(&mut batch);
        // MessageBatch 会被压缩步骤直接跳过（返回 0），批量消息永不压缩
        let sys_flag = self.sys_flag_for(&mut publish_msg);
        let target = match mq {
            Some(mq) => Some(mq.clone()),
            None => {
                let publish = self.topic_publish_info(&client, &topic).await?;
                publish
                    .select_one_message_queue(&[])?
                    .map(|mq| MessageQueue::new(&topic, &mq.broker_name, mq.queue_id))
            }
        };
        // Python 用 batch.topic 建 mq_sel；Rust 的批量外层 topic 已等于 topic
        let mq_sel = target.ok_or_else(|| Error::client("no message queue for publish info"))?;
        if self.has_send_interceptors() {
            return self
                .send_with_hooks(
                    &client,
                    &mut publish_msg,
                    &mq_sel,
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
                &mut publish_msg,
                &mq_sel,
                timeout,
                sys_flag,
            )
            .await
    }

    /// Python `send_oneway`（对应 Java `sendOneway`）。
    ///
    /// ⚠ 差别：Python 往 `send_message_oneway` 传了 `send_msg_timeout`，单向调用
    /// 本就没有超时概念，Rust 的 [`MQClientInstance::send_message_oneway`] 也不收
    /// 这个参数。
    pub async fn send_oneway(&self, msg: &mut Message, mq: Option<&MessageQueue>) -> Result<()> {
        let client = self.require_client()?;
        let wrapped = self.with_namespace(&msg.topic);
        msg.topic = wrapped.clone();
        self.check_message(msg)?;
        let sys_flag = self.try_to_compress_message(msg);
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
        let addr = self.need_addr(&client, &mq_sel)?;
        if self.has_check_forbidden_hook() {
            // Java sendOneway 同样走 sendKernelImpl → 拦截钩子照跑（communicationMode=ONEWAY）
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
            )
            .await
    }

    /// Python `send_by_selector`（对应 Java `send(msg, selector, arg)`）。
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
        let sys_flag = self.try_to_compress_message(msg);
        let holder: Option<AnyHolder> = Some(Arc::new(arg.to_string()));
        if self.has_send_interceptors() {
            // arg 要透传给 CheckForbiddenContext（Java sendKernelImpl 的 context.setArg）
            return self
                .send_with_hooks(
                    &client,
                    &mut PublishMessage::Single(msg),
                    &mq_sel,
                    timeout,
                    sys_flag,
                    holder,
                    CommunicationMode::Sync,
                )
                .await;
        }
        client
            .send_message(
                &self.inner.producer_group(),
                &mut PublishMessage::Single(msg),
                &mq_sel,
                timeout,
                sys_flag,
            )
            .await
    }

    /// Python `send_async`（对应 Java `send(msg, callBack, timeout)`）。
    ///
    /// 差异 3：Python 起线程、Java 用 Netty 异步 + 线程池回调；这里 `tokio::spawn`
    /// 一个任务，用 `start()` 时绑定的运行时句柄派发。没有句柄就一定没启动过
    /// （`start()` 本身是 async），所以这里直接报错，而不是临时建一个运行时 ——
    /// 那会让传输层缓存到一个随调用结束就销毁的 Handle。
    pub fn send_async(
        &self,
        msg: Message,
        callback: Arc<dyn SendCallback>,
        timeout_millis: Option<i64>,
        mq: Option<MessageQueue>,
    ) -> Result<()> {
        let handle = self.runtime_handle().ok_or_else(|| {
            Error::client("send_async needs a tokio runtime; call start() first")
        })?;
        let this = self.clone();
        self.push_task(handle.spawn(async move {
            let mut msg = msg;
            let result = this.send(&mut msg, timeout_millis, mq.as_ref()).await;
            // Python 的 send_async 就是一个 try/except：失败也走回调，不抛
            match result {
                Ok(result) => callback.on_success(result),
                Err(e) => callback.on_exception(e),
            }
        }));
        Ok(())
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
    /// Python `_transaction_flag`：`LocalTransactionState` → Java `MessageSysFlag`
    /// 的 commitOrRollback 值。
    pub fn transaction_flag(state: LocalTransactionState) -> i32 {
        match state {
            LocalTransactionState::CommitMessage => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
            LocalTransactionState::RollbackMessage => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
            LocalTransactionState::Unknow => MessageSysFlag::TRANSACTION_NOT_TYPE,
        }
    }

    /// 发送事务消息（Python `send_message_in_transaction`）。
    ///
    /// 对齐 Java `DefaultMQProducerImpl.sendMessageInTransaction`（L1433-1509）的
    /// **两阶段**：
    ///   1. 半消息：打 `TRAN_MSG` / `PGROUP` 属性，发送时 sysFlag 置
    ///      `TRANSACTION_PREPARED_TYPE`；
    ///   2. 本地事务：仅 `SEND_OK` 时执行，结果汇总为 [`LocalTransactionState`]；
    ///   3. `endTransaction`：以 END_TRANSACTION(37, oneway) 告知 broker
    ///      提交/回滚/未知；
    ///   4. 若 UNKNOW（或本地事务没执行成功），broker 会回查
    ///      CHECK_TRANSACTION_STATE(39)，由 [`CheckTransactionStateProcessor`]
    ///      调 `check_local_transaction` 后再 END_TRANSACTION。
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

        // Java ensureNotDelayedForTransactional：事务消息不支持任何形式的延迟投递
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
        msg.put_property(
            PROPERTY_PRODUCER_GROUP,
            &self.inner.producer_group(),
        );
        // 回查时按此 listener 回调（broker 通过 PGROUP 属性定位到本生产者）
        self.set_transaction_listener(Some(listener.clone()));

        let publish = self.topic_publish_info(&client, &topic).await?;
        let selected = publish
            .select_one_message_queue(&[])?
            .ok_or_else(|| Error::client("no message queue for publish info"))?;
        let mq_sel = MessageQueue::new(&topic, &selected.broker_name, selected.queue_id);

        // 压缩与普通发送一致（Java 的事务发送同样走 sendKernelImpl），
        // 再叠加事务类型位（对应 Java L951-953 检测 TRAN_MSG 后置 TRANSACTION_PREPARED）
        let mut sys_flag = self.try_to_compress_message(msg);
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
        let send_result = self
            .send_with_hooks(
                &client,
                &mut PublishMessage::Single(msg),
                &mq_sel,
                send_timeout,
                sys_flag,
                None,
                CommunicationMode::Sync,
            )
            .await
            .map_err(|e| Error::client(format!("send message Exception: {e}")))?;

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
            SendStatus::FlushDiskTimeout | SendStatus::FlushSlaveTimeout | SendStatus::SlaveNotAvailable
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
            // Java：end broker transaction 失败只 warn，不影响返回结果
            rmq_warn!("local transaction execute {state}, but end broker transaction failed: {e}");
        }
        Ok(TransactionSendResult {
            local_transaction_state: Some(state),
            inner: send_result,
        })
    }

    /// Python `_end_transaction`：向 broker 发送 END_TRANSACTION(37, oneway)，
    /// 对齐 Java `endTransaction` + `checkTransactionState`。
    ///
    /// 收尾之后照 Java 一样跑 `executeEndTransactionHook`（无论主动提交还是 broker
    /// 回查后提交，都会落一条 EndTransaction 轨迹）。
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
            // Java：id = decodeMessageId(offsetMsgId != null ? offsetMsgId : msgId)
            TxnEnd::Normal {
                send_result,
                msg,
            } => {
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
                    .broker_addr_of(&broker_name)
                    .ok_or_else(|| Error::client("no broker address for end transaction"))?
            }
        };

        let mut cmd = RemotingCommand::create_request_command(
            request_code::END_TRANSACTION,
            Some(Box::new(header.clone())),
        );
        cmd.remark = remark;
        cmd.make_custom_header_to_net();
        client.remoting_client().invoke_oneway(&broker_addr, &mut cmd).await?;

        // 对应 Java endTransaction 末尾的 executeEndTransactionHook
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
    ///
    /// 对齐 Java `ClientRemotingProcessor.checkTransactionState` +
    /// `DefaultMQProducerImpl.checkTransactionState`：broker 是 **oneway** 发来的
    /// （body 为整条编码后的 `MessageExt`），因此**不回响应**，而是在新任务里调
    /// `listener.check_local_transaction`，再以
    /// `END_TRANSACTION(fromTransactionCheck=true)` 把最终状态告知 broker。
    fn handle_check_transaction_state(
        &self,
        request: RemotingCommand,
        addr: String,
    ) -> Result<()> {
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
                rmq_warn!(
                    "checkTransactionState: no transaction listener for group {group}"
                );
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

/// broker 主动推来的 CHECK_TRANSACTION_STATE(39) 处理器（对应 Java
/// `ClientRemotingProcessor#checkTransactionState` 的那一半）。
///
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
    /// 对应 Java `DefaultMQProducerImpl#request(msg, mq, timeout)`（:1738-1767）。
    /// 请求方做三件事：
    ///   1. 给请求消息写上 CORRELATION_ID（随机 UUID）、REPLY_TO_CLIENT（**本客户端
    ///      clientId**）、TTL（= timeout）；后两个是 broker 找回本连接、应答方原样带回的依据。
    ///   2. 把等待槽按 correlationId 登记到进程内
    ///      [`request_future_holder()`]。
    ///   3. 发送后阻塞等待；应答由 broker 经 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 推回，
    ///      由 `MQClientInstance::process_reply_message` 投递进等待槽。
    ///
    /// 超时返回 [`Error::RequestTimeout`]（消息已发出但没等到应答）；发送本身失败
    /// 返回 [`Error::Client`]（`send request message to <topic> fail`），与 Java 一致。
    ///
    /// `REPLY_TO_CLIENT` 是 clientId —— broker 要靠它反查 channel，所以本生产者必须
    /// 先发过心跳（`start()` 已起心跳任务；这里也补一次，对齐 Java `prepareSendRequest`
    /// 的 `sendHeartbeatToAllBrokerWithLock`）。
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
        // 对齐 Java prepareSendRequest：确保路由已知，然后补一次心跳 ——
        // 没在 broker 上登记为 producer，broker 就找不到 channel 把应答推回来。
        if let Err(e) = self.topic_publish_info(&client, &topic).await {
            rmq_debug!("request: prepare route failed: {e}");
        }
        send_heartbeat_to_all_broker(&self.inner, &client).await;

        let future = Arc::new(RequestResponseFuture::new(&correlation_id, timeout));
        request_future_holder().put_request(&correlation_id, future.clone());
        let cost = current_time_millis() - begin;
        // 说明：Java 用 ASYNC 发送并等 latch；本实现的 send_async 是「同步发送 +
        // 立即回调」的包装，所以这里等价于同步发。协议上无差别 —— 应答是 broker 通过
        // **另一条** 326 通道推回来的，与本次发送的 CommunicationMode 无关。
        // 发送失败时把 future 标成 !send_request_ok 并主动唤醒等待方。
        let send_timeout = if timeout > cost { timeout - cost } else { timeout };
        if let Err(e) = self.send(msg, Some(send_timeout), mq).await {
            future.set_failed(e);
        }
        let result = self.wait_request_response(&topic, timeout, &future, cost).await;
        // Python 的 `finally: REQUEST_FUTURE_HOLDER.remove_request(...)`
        request_future_holder().remove_request(&correlation_id);
        result
    }

    /// Python `_wait_request_response`（对应 Java `waitResponse`）：
    /// 超时 / 发送失败分别抛不同异常。
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

// ================================================================ 管理类便捷方法

/// Python `producer.py` 尾部那批 `_require_client()` 转发（对应 Java 里
/// `DefaultMQProducerImpl` 暴露给 `MQAdminExt` 之外的少量管理入口）。
///
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
    ///
    /// `perm` 固定 `PERM_READ | PERM_WRITE`（=6，对应 Java `PermName`）。
    /// ⚠ 两处与 Python 的有意差别：
    ///   1. Python 签名里的 `key`（`createTopicKey`）从未被用到，Rust 不保留无用参数；
    ///   2. Python 也没把 `topic_sys_flag` 透传下去（恒为 0），这里按参数语义转发。
    pub async fn create_topic(
        &self,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
    ) -> Result<()> {
        let client = self.require_client()?;
        let perm = PermName::PERM_READ | PermName::PERM_WRITE;
        client
            .create_topic_in_route(new_topic, queue_num, queue_num, perm, topic_sys_flag, None, 5000)
            .await
    }

    /// Python `search_offset`：按时间戳找队列上的位点。
    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: i64) -> Result<i64> {
        let client = self.require_client()?;
        client.search_offset_by_timestamp(mq, timestamp, 5000, None).await
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

/// 事务消息生产者（对应 Java `TransactionMQProducer`，Python/C++ 同名类）。
///
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

    // ---------------- 构造 / 配置默认值 ----------------

    #[test]
    fn config_defaults_match_python() {
        let cfg = ProducerConfig::default();
        assert_eq!(cfg.producer_group, MixAll::DEFAULT_PRODUCER_GROUP);
        assert_eq!(cfg.instance_name, DEFAULT_INSTANCE_NAME);
        assert_eq!(cfg.client_id, None);
        assert_eq!(cfg.create_topic_key, MixAll::DEFAULT_TOPIC);
        assert_eq!(cfg.default_topic_queue_nums, MixAll::DEFAULT_TOPIC_QUEUE_NUMS);
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
        p.set_producer_group("GID_new")
            .expect("未启动时允许改组名");
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
        assert!(SelectMessageQueueByHash
            .select(&[], &msg, "any")
            .is_err());
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
        assert!(SelectMessageQueueByRandom
            .select(&[], &msg, "")
            .is_err());
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
        // 没有任何前缀命中时取第一个（Java 的 `else first`）
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
    fn transaction_flag_matches_java_sysflag() {
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
            Error::Connect { addr: "1.2.3.4:10911".into() },
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
        assert!(p.check_message(&Message::new("T1", Some(b"12345"))).is_err());
        assert!(p.check_message(&Message::new("T1", Some(b"1234"))).is_ok());
        // 空正文按 0 字节放过（Python 在这里会 TypeError，Java 也不进长度比较）
        assert!(p.check_message(&Message::new("T1", None)).is_ok());
    }

    #[test]
    fn compression_only_kicks_in_at_threshold() {
        let p = producer("GID_compress");
        p.set_compress_msg_body_over_howmuch(1024);
        let mut msg = Message::new("T1", Some(&[b'a'; 1023]));
        let before = msg.get_body().to_vec();
        assert_eq!(p.try_to_compress_message(&mut msg), 0);
        assert_eq!(msg.get_body(), before.as_slice(), "未过阈值不该动正文");

        let mut big = Message::new("T1", Some(&[b'a'; 4096]));
        let flag = p.try_to_compress_message(&mut big);
        assert_eq!(flag & MessageSysFlag::COMPRESSED_FLAG, MessageSysFlag::COMPRESSED_FLAG);
        assert_eq!(
            MessageSysFlag::get_compression_type(flag),
            MessageSysFlag::ZLIB_TYPE
        );
        // 正文确实被换成了压缩流，且能原样解回
        let compressed = big.get_body().to_vec();
        assert_ne!(compressed, vec![b'a'; 4096]);
        let raw = compression::decompress(&compressed, MessageSysFlag::ZLIB_TYPE).unwrap();
        assert_eq!(raw, vec![b'a'; 4096]);
    }

    #[test]
    fn compression_type_and_empty_body_edges() {
        let p = producer("GID_compress_edge");
        p.set_compress_msg_body_over_howmuch(1);
        // 空正文：即便阈值最低也不压（Python `if not body: return 0`）
        let mut empty = Message::new("T1", Some(b""));
        assert_eq!(p.try_to_compress_message(&mut empty), 0);

        // 不支持的算法：降级为不压缩，正文保持原样
        p.set_compress_type(9);
        let mut msg = Message::new("T1", Some(b"payload payload payload"));
        let before = msg.get_body().to_vec();
        assert_eq!(p.try_to_compress_message(&mut msg), 0);
        assert_eq!(msg.get_body(), before.as_slice());
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

    // ---------------- 钩子上下文 ----------------

    #[test]
    fn send_context_msg_type_follows_python_priority() {
        let p = producer("GID_ctx");
        let m = mq("broker-a", 0);

        let plain = Message::new("T1", Some(b"x"));
        let ctx = p.build_send_context(&plain, "GID_ctx", "", &m, "127.0.0.1:10911", CommunicationMode::Sync);
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
        assert_eq!(hook.before.load(Ordering::SeqCst), 0, "被拦截时不该跑发送钩子");
        assert_eq!(hook.after.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn traceparent_is_injected_only_when_enabled() {
        let p = producer("GID_trace");
        let client = bare_client();
        let m = mq("broker-a", 0);

        p.set_enable_trace_context(Some(false));
        let mut off = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut off);
        let _ = p
            .send_with_hooks(&client, &mut publish, &m, 100, 0, None, CommunicationMode::Sync)
            .await;
        assert!(off.get_property(TRACE_CONTEXT_PROPERTY).is_none());

        p.set_enable_trace_context(Some(true));
        let mut on = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut on);
        let _ = p
            .send_with_hooks(&client, &mut publish, &m, 100, 0, None, CommunicationMode::Sync)
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
            .send_with_hooks(&client, &mut publish, &m, 100, 0, None, CommunicationMode::Sync)
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
        let err = p.send(&mut msg, None, None).await.expect_err("未启动不可发送");
        assert!(err.to_string().contains("not started"));
        assert!(p.fetch_publish_message_queues("T1").await.is_err());
        // 未启动时 shutdown 幂等且不 panic
        p.shutdown();
        p.shutdown();
        assert!(!p.is_started());
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

    /// 没有运行时上下文 ⇒ 明确报错，而不是偷偷新建一个 runtime
    /// （那样会让传输层缓存到一个已关闭的 `Handle`）。
    #[test]
    fn send_async_needs_a_bound_runtime() {
        let p = producer("GID_async");
        let cb = Arc::new(ClosureSendCallback::new(None, None));
        let err = p
            .send_async(Message::new("T1", Some(b"x")), cb, None, None)
            .expect_err("运行时之外不该静默派发");
        assert!(err.to_string().contains("start()"));
    }

    #[tokio::test]
    async fn send_async_dispatches_inside_a_runtime() {
        let p = producer("GID_async_ok");
        let done = Arc::new(AtomicUsize::new(0));
        let d = done.clone();
        let cb = Arc::new(ClosureSendCallback::new(
            None,
            Some(Box::new(move |_e| {
                d.fetch_add(1, Ordering::SeqCst);
            })),
        ));
        // 未 start() 也没关系：Handle::try_current() 能拿到测试运行时
        p.send_async(Message::new("T1", Some(b"x")), cb, None, None)
            .expect("运行时内可派发");
        // 没路由 ⇒ 走失败回调
        assert!(wait_until(|| done.load(Ordering::SeqCst) == 1).await);
    }

    /// 轮询等待条件成立（最多 ~2s），避免用 sleep 猜时长。
    async fn wait_until<F: Fn() -> bool>(cond: F) -> bool {
        for _ in 0..200 {
            if cond() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cond()
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

        let resp = GetEarliestMsgStoretimeResponseHeader { timestamp: Some(1234) };
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
}
