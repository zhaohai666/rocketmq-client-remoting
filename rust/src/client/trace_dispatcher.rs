//! 异步轨迹分发器（对应 Java
//! `org.apache.rocketmq.client.trace.AsyncTraceDispatcher`，逐条移植
//! `python/rocketmq/client/trace_dispatcher.py`）。
//!
//! 职责与 Python 模块头一致：钩子把 [`TraceContext`] 丢进内存队列（[`append`]，
//! `trace_dispatcher.py:165`），后台任务按「攒够 `batch_num` 条 或 距上次发送超过 5s」
//! 两个条件触发刷写（`trace_dispatcher.py:190`），再把编码后的文本用**独立的内部生产者**
//! 发到轨迹 topic（默认 [`MixAll::TRACE_TOPIC`]）。
//!
//! [`append`]: AsyncTraceDispatcher::append
//!
//! ## 与已有接缝的关系
//!
//! * [`crate::client::mq_client::TraceDispatcher`] —— 生命周期 `start` / `shutdown`
//!   （`trace_dispatcher.py:127` / `:144`）；
//! * [`crate::client::trace_hook::TraceReportSink`] —— 轨迹钩子要的三件事
//!   `trace_topic_name` / `report`(=`append`) / `client_id`（`trace_dispatcher.py:111` /
//!   `:165` / `:120`）；
//! * [`TraceProducer`] —— **本模块新引入**的「内部轨迹生产者」接缝，对应 Python 构造函数
//!   里 `new DefaultMQProducer(...)` 之后被调用的那几个方法（见下）。
//!
//! ## 与 Python 参考实现的有意差异（逐条在对应条目 doc 上再次标注）
//!
//! 1. **内部生产者靠注入**（[`TraceDispatcherConfig::producer`]）。Python 在
//!    `__init__` 里直接 `DefaultMQProducer(self._gen_group_name_for_trace(), rpc_hook)`
//!    （`trace_dispatcher.py:95` + `:98-105`）；本 crate 的统一约定是依赖注入
//!    （见 `mq_client.rs` 模块头差异 2），且 `producer.rs` 尚未落地，构造点只能在调用方。
//!    [`TraceProducer`] 的方法集 = Python 真正打到 `self.trace_producer` 上的方法集
//!    （`set_send_msg_timeout` / `set_max_message_size` / `set_enable_trace` /
//!    `set_namesrv_addr` / `set_instance_name` / `start` / `shutdown` / `send` /
//!    `send_by_selector` / `_topic_publish_info`），`producer.rs` 落地后由
//!    `DefaultMQProducer` 实现它即可。未注入时使用 [`DisabledTraceProducer`]
//!    （只记日志、发送必失败）并在构造时打一条 WARN。组名字面量另拆了一个**纯**函数
//!    [`format_trace_producer_group`]，`next_trace_producer_group` 只是「取号 + 调它」；
//!    拆分动机是单测要能确定性地钉住格式（进程级计数器在并行用例下互相插号）。
//! 2. **`send_by_selector` 的两步搬到分发器里**：Python
//!    `producer.send_by_selector(msg, _BrokerSetSelector(counter), broker_set, 5000)`
//!    （`trace_dispatcher.py:262`）在生产者内部做「取路由 → 调 selector → 定点发」。
//!    Rust 拆成分发器里的 [`BrokerSetSelector::select`] + 生产者接缝上的
//!    [`TraceProducer::send_to_queue`]，等价性：Python `send_by_selector` 与
//!    `send(msg, timeout, mq)` 除了「是否自己取路由 / 是否把 `arg` 透传给
//!    `CheckForbiddenContext`」外完全一致，而内部轨迹生产者**既没注册任何钩子**
//!    （`_get_and_create_trace_producer` 只设 group/timeout/maxSize/enableTrace=False）
//!    也**不需要** `arg`，故两条路径的线上字节与选中的队列都相同。
//! 3. **`_topic_publish_info` 只暴露 `msg_queue_list`**（[`TraceProducer::publish_queues`]）：
//!    Python 拿到 publish info 后只读 `publish.msg_queue_list`
//!    （`trace_dispatcher.py:275-277`），而 Rust 的
//!    [`TopicPublishInfo`](crate::client::mq_client::TopicPublishInfo) 队列只能由实例路由
//!    刷新写入、单测无法构造，故接缝直接给出队列列表。
//! 4. **`start` 有两个版本**：Python `start()` 是同步的（内部 `producer.start()` 是阻塞
//!    网络调用）。Rust 的具体类型给出 [`AsyncTraceDispatcher::start`]（`async fn`，与
//!    Python 逐行对应，producer/consumer 移植层应当用它），对象安全的
//!    [`TraceDispatcher::start`] 则把同一段逻辑丢进 `tokio::spawn`（拿不到 await 结果，
//!    失败只记 ERROR），并在**没有 tokio 运行时**时返回 `Err`。派发用的运行时句柄按
//!    `OnceLock` 惰性绑定（构造可以发生在运行时之外）。
//! 5. **`ThreadPoolExecutor(max_workers=4)` → `tokio::spawn` + 在途任务表**
//!    （Python `trace_dispatcher.py:93-94`）。线程池的无界工作队列在 Rust 里就是
//!    「spawn 出去的 future」；线程名前缀 `MQTraceSendThread_<id>_` 与 worker 线程名
//!    `MQ-AsyncArrayDispatcher-Thread<id>`（`:140`）在 Rust 没有对应物，只影响日志。
//!    `WAIT_FOR_SHUTDOWN = 5000`（`:70`）在 Python 里是**死常量**（`_executor.shutdown
//!    (wait=False)`，`:150`），Rust 按 Java `ThreadUtils.shutdownGracefully(traceExecutor,
//!    WAIT_FOR_SHUTDOWN)`（`AsyncTraceDispatcher.java:207`）把它用作 [`flush_and_wait`] /
//!    [`shutdown_gracefully`] 的等待上限。
//!
//! [`flush_and_wait`]: AsyncTraceDispatcher::flush_and_wait
//! [`shutdown_gracefully`]: AsyncTraceDispatcher::shutdown_gracefully
//! 6. **`atexit.register(self.shutdown)`（`:142` / `:159`）与 Java
//!    `Runtime.addShutdownHook`（`AsyncTraceDispatcher.java:215-247`）无 Rust 等价物**：
//!    进程退出前不会自动 flush，调用方必须显式 `shutdown()`。为免「忘了 shutdown 就
//!    永久泄漏一个后台任务」，worker 任务只持 `Weak<Inner>`，句柄全部释放后自行退出
//!    （等价于 Python 的 daemon 线程随解释器退出）。
//! 7. **防忙等的 `time.sleep(0.005)`（`:204-205`）** 在 Python 里位于
//!    `_flush_trace_context` 内部（本轮没刷出批次才睡）；Rust 该函数保持同步，
//!    将 5ms 的 `tokio::time::sleep` 上移到 worker 循环里，触发条件完全相同。
//! 8. **`threading.RLock`（`:91`）→ `tokio::sync::Mutex`**：Python 的锁临界区里只有同步
//!    调用，Rust 临界区要跨 `await`（`producer.start()`），故用异步锁（Java 用
//!    `AtomicBoolean#compareAndSet`，`AsyncTraceDispatcher.java:152`）。
//! 9. **`shutdown()` 会 `take()` 掉 worker 句柄**，因此之后可以重新 `start()`。Python
//!     shutdown 后 `self.worker` 仍指向已死的 `Thread`（`:137` 的 `is None` 判断不成立），
//!    再 `start()` 不会重起后台刷写；Java 每次 `start()` 都新建 worker —— Rust 跟 Java。
//!    Rust 在置 `stopped` 之后还 `abort()` 该任务（Python 只能等线程自己回到循环顶部）：
//!    不丢东西 —— 刷写段是同步的，跑到下一个 await 点前不会被打断，已提交的发送批次在
//!    独立任务里、由在途任务表记账（见第 5 条）。
//! 10. **`flush()` 在 `batch_num == 0` 时不会死循环**：Python `while not empty` +
//!     `for _ in range(0)` ⇒ 一次也取不出来 ⇒ 永久空转（`:177-181` + `:197`）。Rust 发现
//!     「队列非空但本轮没刷出批次」时记一条 WARN 并退出。
//! 11. **切块阈值比较的是码点**：Java `StringBuilder.length()` 是 UTF-16 code unit
//!     （`AsyncTraceDispatcher.java:348`），Python `len(buffer)` 是码点
//!     （`trace_dispatcher.py:244`）；Rust 跟 Python。纯 ASCII 轨迹文本下三者一致。
//!     另外 Java 比的是 `traceProducer.getMaxMessageSize()`，Python 比的是自己的
//!     `self.max_msg_size`（同值 128000），Rust 跟 Python 并保留 `set_max_msg_size`。
//! 12. **分组容器保持插入顺序**：Python `dict` 天然是插入序（`:228` `setdefault`），
//!     Java 用 `HashMap`（`AsyncTraceDispatcher.java:308`，顺序随机）。Rust 用
//!     「Vec + 索引 HashMap」复刻 Python 的逐组发送顺序（多线程下轨迹到达顺序本就
//!     不保证，但单发批次内的顺序是可测的线上行为）。
//! 13. **`key.split(CONTENT_SPLITOR)` 的解包**：Python `topic, trace_topic = key.split(...)`
//!     （`:230`）在 topic 真含 `\x01` 时抛 `ValueError`，异常被线程池吞掉；Rust 只按
//!     **第一个** `\x01` 切一次（`split_once`），切不出两段则记 DEBUG 后跳过该组。
//! 14. **`is_started` 在 `shutdown()` 后仍为 `true`** —— Python 原样（`:144-162` 没有复位
//!     它），因此二次 `shutdown()` 仍会再调一次 `trace_producer.shutdown()`；保留。
//! 15. **`namespace_v2`（`:86`）未移植**：Python 只在构造函数里赋了 `""`，全文从未读取
//!     （Java `start()` 会 `traceProducer.setNamespaceV2(...)`，
//!     `AsyncTraceDispatcher.java:155`），属死状态；等命名空间 2.0 的接缝真需要时再补。
//! 16. **`set_host_producer` / `set_host_consumer`（`:114-118`）走 [`TraceHost`] 接缝**：
//!     Python 靠鸭子类型 `getattr(host, "_mq_client").client_id`（`:120-124`）拿 clientId，
//!     Rust 声明一个只提供 `client_id()` 的小接缝，[`MQClientInstance`] 已实现它。
//! 17. 编码（`TraceDataEncoder.encoder_from_context_bean`）、`TraceContext` 的字段与
//!     `\x01`/`\x02` 拼串一律复用 [`crate::client::trace`]，本模块**不重复**实现也不
//!     重复其单测；本模块只对齐「攒批 / 分组 / 切块 / KEYS 拼装 / 选队 / 生命周期」。

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::client::mq_client::{MQClientInstance, TraceDispatcher};
use crate::client::trace::{
    AccessChannel, TraceConstants, TraceContext, TraceDataEncoder, TraceTransferBean,
};
use crate::client::trace_hook::TraceReportSink;
use crate::common::message::{Message, MessageQueue};
use crate::common::message_const::KEY_SEPARATOR;
use crate::common::mix_all::MixAll;
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};
use crate::{rmq_debug, rmq_error, rmq_info, rmq_warn};

// ================================================================ 枚举 / 进程级计数

/// 分发器类型（对应 Java `TraceDispatcher.Type`，`TraceDispatcher.java:27-30`；
/// Python `TraceDispatcherType`，`trace_dispatcher.py:41-45`）。
///
/// 只影响内部生产者组名里的那一段字面量（`PRODUCE` / `CONSUME`，Python 的 `.value`、
/// Java 的枚举名），线上不落轨迹正文，故无编码风险。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TraceDispatcherType {
    /// 生产侧（Python `TraceDispatcherType.PRODUCE`，`trace_dispatcher.py:44`）。
    Produce,
    /// 消费侧（Python `TraceDispatcherType.CONSUME`，`trace_dispatcher.py:45`）。
    Consume,
}

impl TraceDispatcherType {
    /// Python `.value`（`trace_dispatcher.py:109` 组名里那一段）／Java `Enum#name()`：
    /// `PRODUCE` | `CONSUME`（枚举本体 `:41-45`）。
    pub fn name(self) -> &'static str {
        match self {
            TraceDispatcherType::Produce => "PRODUCE",
            TraceDispatcherType::Consume => "CONSUME",
        }
    }
}

/// 为了让组名拼接处（`trace_dispatcher.py:109`）能像 Python 的 `%s`（= `.value`）
/// 那样直接把类型写进 `format!`。
impl std::fmt::Display for TraceDispatcherType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Python `AsyncTraceDispatcher._COUNTER = itertools.count(1)`（`trace_dispatcher.py:68`）：
/// 进程级内部生产者组名序号，**从 1 开始**（Java `COUNTER.incrementAndGet()`，
/// `AsyncTraceDispatcher.java:56` + `:181`）。
fn next_group_counter() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::AcqRel) + 1
}

/// [`AsyncTraceDispatcher::next_trace_producer_group`] 的**纯**格式化部分
/// （把自增号当参数传进来）。
///
/// Python `_gen_group_name_for_trace`（`trace_dispatcher.py:107-109`）／Java
/// `genGroupNameForTrace`（`AsyncTraceDispatcher.java:180-182`）本身是
/// 「格式化 + 顺手自增」一体的；这里拆开只为让单测能确定性地断言字面格式 ——
/// 进程级计数器在 `cargo test` 的并行用例下互相插号，「恰好 +1」这种断言不可靠。
fn format_trace_producer_group(
    group: &str,
    dispatcher_type: TraceDispatcherType,
    seq: u64,
) -> String {
    format!(
        "{}-{group}-{dispatcher_type}-{seq}",
        TraceConstants::GROUP_NAME_PREFIX
    )
}

/// Python `AsyncTraceDispatcher._INSTANCE_NUM = itertools.count(0)`（`trace_dispatcher.py:69`）：
/// 进程级实例序号，**从 0 开始**（Java `INSTANCE_NUM.getAndIncrement()`，
/// `AsyncTraceDispatcher.java:57` + `:60`）。
fn next_instance_id() -> u64 {
    static INSTANCE_NUM: AtomicU64 = AtomicU64::new(0);
    INSTANCE_NUM.fetch_add(1, Ordering::AcqRel)
}

// ================================================================ 内部生产者接缝

/// 内部轨迹生产者的异步返回类型（无 async-trait 依赖的装箱 future，风格同
/// [`crate::client::mq_client::ConsumerFuture`]）。
///
/// Python 打到内部生产者上的调用全是同步阻塞的（`trace_dispatcher.py:134` 的
/// `producer.start()`、`:260` 的 `send`、`:262` 的 `send_by_selector`、
/// `:275` 的 `_topic_publish_info`），Rust 侧换成 tokio 异步后统一用本类型装箱。
pub type TraceProducerFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// 内部轨迹生产者接缝（对应 Python 的 `self.trace_producer`，一个
/// `enable_trace=False` 的 `DefaultMQProducer`）。
///
/// 方法集严格等于 Python 真正打到内部生产者上的调用，逐条：
/// * [`Self::set_send_msg_timeout`] / [`Self::set_max_message_size`] /
///   [`Self::set_enable_trace`] ← `trace_dispatcher.py:101-104`（构造时）
///   （Java `getAndCreateTraceProducer`，`AsyncTraceDispatcher.java:167-178`；
///   Java 还多了 `setVipChannelEnabled(false)`，Python 无 VIP 通道概念）；
/// * [`Self::set_name_server_addr`] / [`Self::set_instance_name`] /
///   [`Self::set_enable_trace`] / [`Self::start`] ← `trace_dispatcher.py:130-134`；
/// * [`Self::shutdown`] ← `trace_dispatcher.py:155`；
/// * [`Self::send`] ← `trace_dispatcher.py:260`（`producer.send(msg, 5000)`）；
/// * [`Self::send_to_queue`] ← 见模块差异 2（Python 的 `send_by_selector` 落点）；
/// * [`Self::publish_queues`] ← `trace_dispatcher.py:275-277`
///   （`producer._topic_publish_info(topic).msg_queue_list`）。
///
/// ⚠ **实现方必须保证自身不再产生轨迹**（Python `set_enable_trace(False)`，
/// `trace_dispatcher.py:104` / `:133`；Java 同名），否则轨迹会自我复制到无限。
///
/// 对象安全：需要 `await` 的方法用 `self: Arc<Self>` 接收者 + 装箱 future。
pub trait TraceProducer: Send + Sync {
    /// Python `producer.set_namesrv_addr(name_srv_addr)`（`trace_dispatcher.py:130`）。
    fn set_name_server_addr(&self, name_server_addr: &str);

    /// Python `producer.set_instance_name(...)`（`trace_dispatcher.py:131-132`）。
    fn set_instance_name(&self, instance_name: &str);

    /// Python `producer.set_send_msg_timeout(5000)`（`trace_dispatcher.py:101`）。
    fn set_send_msg_timeout(&self, timeout_millis: i64);

    /// Python `producer.set_max_message_size(self.max_msg_size)`
    /// （`trace_dispatcher.py:102`）。参数用 `usize` 与本模块的
    /// [`AsyncTraceDispatcher::max_msg_size`] 同宽（实现方自行窄化成 Java 的 `int`）。
    fn set_max_message_size(&self, max_msg_size: usize);

    /// Python `producer.set_enable_trace(False)`（`trace_dispatcher.py:104` / `:133`）。
    /// 分发器只会传 `false`，签名保留布尔以便实现方复用现成 setter。
    fn set_enable_trace(&self, enable: bool);

    /// Python `producer.start()`（`trace_dispatcher.py:134`）。失败 ⇒ `Err`，
    /// 由 [`AsyncTraceDispatcher::start`] 原样上抛（Python 冒到
    /// `producer._start_trace_dispatcher` 的 `except` 里记 warning）。
    fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>>;

    /// Python `producer.shutdown()`（`trace_dispatcher.py:155`）。
    /// Python 用 `try/except` 包住并记 debug 日志；Rust 的接缝没有返回值，
    /// 实现方（`DefaultMQProducer::shutdown` 一类）自己保证不 panic。
    fn shutdown(&self);

    /// Python `producer.send(msg, 5000)`（`trace_dispatcher.py:260`）。
    /// 返回的 `SendResult` 两边都被丢弃，故只回 `Result<()>`。
    fn send(
        self: Arc<Self>,
        message: Message,
        timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>>;

    /// 定点发送（对应 Python `producer.send(msg, timeout, mq=选中的队列)`，
    /// 见模块差异 2）：`mq` 是 [`Self::publish_queues`] 里挑出来的队列。
    fn send_to_queue(
        self: Arc<Self>,
        message: Message,
        queue: MessageQueue,
        timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>>;

    /// Python `producer._topic_publish_info(topic).msg_queue_list`
    /// （`trace_dispatcher.py:275-276`，实现在 `producer.py:591-603`，
    /// Java `tryGetMessageQueueBrokerSet` 走的是同一份路由缓存）。
    /// 拿不到路由 ⇒ `Err`（Python 抛异常，由调用方 `except` 吞掉）。
    fn publish_queues(
        self: Arc<Self>,
        topic: String,
    ) -> TraceProducerFuture<Result<Vec<MessageQueue>>>;
}

/// [`TraceProducer`] 的「未接线」实现（模块差异 1）：什么都不发，只记日志。
/// Python 在 `__init__` 里总会自己 new 一个内部生产者（`trace_dispatcher.py:95`），
/// Rust 改为注入，未注入时用本类型兜底。
///
/// 注入真实生产者之前用它，行为 = 轨迹链路断开（`start` 返回 `Err`、发送返回 `Err`），
/// 调用方（Python `producer._start_trace_dispatcher` 的 `except`）记 warning，
/// 与「轨迹起不来但不影响业务发送」的 Java/Python 语义一致。
#[derive(Debug, Default, Clone, Copy)]
pub struct DisabledTraceProducer;

impl TraceProducer for DisabledTraceProducer {
    fn set_name_server_addr(&self, name_server_addr: &str) {
        rmq_debug!("trace producer not wired: ignore namesrv_addr {name_server_addr}");
    }

    fn set_instance_name(&self, instance_name: &str) {
        rmq_debug!("trace producer not wired: ignore instanceName {instance_name}");
    }

    fn set_send_msg_timeout(&self, timeout_millis: i64) {
        rmq_debug!("trace producer not wired: ignore send_msg_timeout {timeout_millis}");
    }

    fn set_max_message_size(&self, max_msg_size: usize) {
        rmq_debug!("trace producer not wired: ignore max_message_size {max_msg_size}");
    }

    fn set_enable_trace(&self, enable: bool) {
        rmq_debug!("trace producer not wired: ignore enable_trace {enable}");
    }

    fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>> {
        Box::pin(async {
            Err(Error::client(
                "trace producer is not wired (inject TraceDispatcherConfig::producer)",
            ))
        })
    }

    fn shutdown(&self) {}

    fn send(
        self: Arc<Self>,
        _message: Message,
        _timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>> {
        Box::pin(async { Err(Error::client("trace producer is not wired")) })
    }

    fn send_to_queue(
        self: Arc<Self>,
        _message: Message,
        _queue: MessageQueue,
        _timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>> {
        Box::pin(async { Err(Error::client("trace producer is not wired")) })
    }

    fn publish_queues(
        self: Arc<Self>,
        _topic: String,
    ) -> TraceProducerFuture<Result<Vec<MessageQueue>>> {
        Box::pin(async { Err(Error::client("trace producer is not wired")) })
    }
}

// ================================================================ 宿主接缝

/// 分发器宿主（Python 里是 `host_producer` / `host_consumer` 两个裸对象，
/// `trace_dispatcher.py:87-88` + `:114-118`）。
///
/// Python 从宿主上**只读一件事**：`host._mq_client.client_id`
/// （`_client_id`，`trace_dispatcher.py:120-124`；Java 是
/// `getHostProducer().getMqClientFactory().getClientId()`）。所以这里不搬
/// `DefaultMQProducerImpl` / `DefaultMQPushConsumerImpl`，只声明 clientId 接缝。
pub trait TraceHost: Send + Sync {
    /// Python `host._mq_client.client_id`。
    fn client_id(&self) -> String;
}

/// 直接用 [`MQClientInstance`] 当宿主（等价于 Python 的 `host._mq_client`）。
impl TraceHost for MQClientInstance {
    fn client_id(&self) -> String {
        MQClientInstance::client_id(self).to_string()
    }
}

// ================================================================ 配置

/// [`AsyncTraceDispatcher`] 的构造参数（对应 Python `__init__` 的关键字参数 +
/// 类内常量，`trace_dispatcher.py:68-95`）。
#[derive(Clone)]
pub struct TraceDispatcherConfig {
    /// 内部轨迹生产者（模块差异 1）。`None` ⇒ [`DisabledTraceProducer`]。
    pub producer: Option<Arc<dyn TraceProducer>>,
    /// 内部生产者组名。`None` ⇒ 由 [`AsyncTraceDispatcher::next_trace_producer_group`]
    /// 现生成（Python 构造函数里 `_gen_group_name_for_trace()`，`:95` + `:107-109`）。
    /// 注入生产者时应当把当初建生产者用的同一个名字传进来，以免进程级计数器多走一格。
    pub trace_producer_group: Option<String>,
    /// Python `self.batch_num = min(batch_num, 20)`（`:75`，Java 注释「max value 20」）。
    pub batch_num: usize,
    /// Python `self.trace_topic_name = trace_topic_name or MixAll.TRACE_TOPIC`（`:92`）。
    /// `None` **与空串都**回落到默认轨迹 topic（Python 的 `or` 语义，Java 用
    /// `UtilAll.isBlank` —— 纯空白串在 Python 里会被原样保留，这里跟 Python）。
    pub trace_topic_name: Option<String>,
    /// Python `self.max_msg_size = 128000`（`:76`）。
    pub max_msg_size: usize,
    /// Python `producer.set_send_msg_timeout(5000)`（`:101`）与
    /// `send` / `send_by_selector` 的 `5000`（`:260` / `:263`）—— 同一个常量。
    pub send_msg_timeout_millis: i64,
    /// Python `FLUSH_TRACE_INTERVAL = 5000`（`:71`，Java `flushTraceInterval`，
    /// `AsyncTraceDispatcher.java:80`）。
    pub flush_trace_interval_millis: i64,
    /// Python `WAIT_FOR_SHUTDOWN = 5000`（`:70`；见模块差异 5）。
    pub wait_for_shutdown_millis: u64,
    /// Python `queue.Queue(maxsize=2048)`（`:80`，Java `ArrayBlockingQueue(2048)`）。
    pub queue_capacity: usize,
}

impl Default for TraceDispatcherConfig {
    fn default() -> Self {
        TraceDispatcherConfig {
            producer: None,
            trace_producer_group: None,
            batch_num: 10,
            trace_topic_name: None,
            max_msg_size: 128_000,
            send_msg_timeout_millis: 5_000,
            flush_trace_interval_millis: 5_000,
            wait_for_shutdown_millis: 5_000,
            queue_capacity: 2_048,
        }
    }
}

/// 手写 `Debug`：注入的接缝（`Arc<dyn TraceProducer>`）不要求 `Debug`。
impl std::fmt::Debug for TraceDispatcherConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceDispatcherConfig")
            .field("producer", &if self.producer.is_some() { "injected" } else { "none" })
            .field("trace_producer_group", &self.trace_producer_group)
            .field("batch_num", &self.batch_num)
            .field("trace_topic_name", &self.trace_topic_name)
            .field("max_msg_size", &self.max_msg_size)
            .field("send_msg_timeout_millis", &self.send_msg_timeout_millis)
            .field("flush_trace_interval_millis", &self.flush_trace_interval_millis)
            .field("wait_for_shutdown_millis", &self.wait_for_shutdown_millis)
            .field("queue_capacity", &self.queue_capacity)
            .finish()
    }
}

// ================================================================ 选择器

/// 只在指定 broker 集合里轮询选队列（对应 Python `_BrokerSetSelector`，
/// `trace_dispatcher.py:48-62`；Java 是 `sendTraceDataByMQ` 里那个匿名
/// `MessageQueueSelector`，`AsyncTraceDispatcher.java:389-403`）。
///
/// `counter` 就是宿主分发器的 `self._send_which_queue`（`trace_dispatcher.py:89`，
/// `itertools.count(0)`）：Python 每次发送都新建一个选择器对象，但**计数器是共享的**，
/// 所以轮询位置跨批次连续。
struct BrokerSetSelector {
    counter: Arc<AtomicU64>,
}

impl BrokerSetSelector {
    /// Python `_BrokerSetSelector.select`（`:54-62`）。
    ///
    /// `None` 只在 `mqs` 整体为空时出现 —— Python 会退化到全量列表再 `% len(filtered)`
    /// 抛 `ZeroDivisionError`，被 `_send_trace_data_by_mq` 的 `except` 记成发送失败；
    /// 这里用 `None` 表达同一结果。Java 则是 `filterMqs.get(pos)` 越界抛
    /// `IndexOutOfBoundsException`（`AsyncTraceDispatcher.java:400-401`）。
    fn select(&self, mqs: &[MessageQueue], broker_set: &HashSet<String>) -> Option<MessageQueue> {
        let mut filtered: Vec<&MessageQueue> = mqs
            .iter()
            .filter(|q| broker_set.contains(&q.broker_name))
            .collect();
        if filtered.is_empty() {
            // Python `:57-60`：过滤后为空 ⇒ 退化为全量轮询（比 Java 的越界异常更安全，
            // 语义上等价于「没有跨集群过滤需求」）。
            filtered = mqs.iter().collect();
        }
        if filtered.is_empty() {
            return None;
        }
        let pos = self.counter.fetch_add(1, Ordering::AcqRel) % filtered.len() as u64;
        Some(filtered[pos as usize].clone())
    }
}

// ================================================================ AsyncTraceDispatcher

/// 分发器实体（`AsyncTraceDispatcher` 的字段集合，`trace_dispatcher.py:73-95`）。
///
/// 之所以拆成 `Arc<Inner>` + 句柄：Python 的对象被钩子、宿主生产者、后台线程共享，
/// Rust 侧 [`TraceReportSink`] / [`TraceDispatcher`] 两个接缝都要 `Arc<dyn ...>`，
/// 而后台任务又需要「不阻止回收」的弱引用（模块差异 6）。
struct Inner {
    /// Python `self.group`（`:78`）。
    group: String,
    /// Python `self.type`（`:79`）。
    dispatcher_type: TraceDispatcherType,
    /// Python `self.trace_instance_id`（`:77`，进程级自增）。
    trace_instance_id: u64,
    /// Python 内部生产者组名（`:100` + `:107-109`）。
    trace_producer_group: String,
    /// Python `self.batch_num`（`:75`，已 clamp 到 20）。
    batch_num: usize,
    /// Python `self.max_msg_size`（`:76`；可像 Python 一样直接改，见
    /// [`AsyncTraceDispatcher::set_max_msg_size`]）。
    max_msg_size: AtomicUsize,
    /// Python `self.trace_topic_name`（`:92`）。
    trace_topic_name: String,
    /// Python `self.access_channel`（`:85`，`start()` 里按入参覆盖，`:136`）。
    access_channel: AtomicU8,
    /// Python `self.trace_context_queue`（`:80`，`queue.Queue(maxsize=2048)`）。
    queue: Mutex<VecDeque<TraceContext>>,
    /// Python `self.trace_context_queue.maxsize`。
    queue_capacity: usize,
    /// Python `self.discard_count`（`:81`）。
    discard_count: AtomicU64,
    /// Python `self.stopped`（`:82`）。
    stopped: AtomicBool,
    /// Python `self.is_started`（`:83`）。
    is_started: AtomicBool,
    /// Python `self._last_flush_time`（`:90`）。
    last_flush_time: AtomicI64,
    /// Python `FLUSH_TRACE_INTERVAL`（`:71`）。
    flush_trace_interval_millis: i64,
    /// Python `self._send_which_queue`（`:89`）。
    send_which_queue: Arc<AtomicU64>,
    /// Python `self._lock`（`:91`，`RLock`；只保护 `start()` 里的 `is_started` 段，
    /// 差异见模块头第 8 条）。
    start_lock: tokio::sync::Mutex<()>,
    /// Python `self.worker`（`:84` + `:137-141`）。
    worker: Mutex<Option<JoinHandle<()>>>,
    /// Python `self._executor`（`:93-94`）：在途发送任务句柄（用于优雅等待）。
    pending: Mutex<Vec<JoinHandle<()>>>,
    /// Python `self.trace_producer`（`:95`）。
    trace_producer: Arc<dyn TraceProducer>,
    /// Python `self.host_producer` / `self.host_consumer`（`:87-88` + `:114-118`）。
    host_producer: Mutex<Option<Arc<dyn TraceHost>>>,
    host_consumer: Mutex<Option<Arc<dyn TraceHost>>>,
    /// Python `self.send_msg_timeout`（内部生产者上的 5000，`:101`）+
    /// 发送时传的 `5000`（`:260` / `:263`）。
    send_msg_timeout_millis: i64,
    /// Python `WAIT_FOR_SHUTDOWN`（`:70`，模块差异 5）。
    wait_for_shutdown_millis: u64,
    /// 惰性绑定的 tokio 运行时句柄（模块差异 4）。
    handle: OnceLock<Handle>,
    /// 指向自己的弱引用，供 `submit` 造 `'static` future 用。
    this: Weak<Inner>,
}

/// 轨迹异步分发器（对应 Python `AsyncTraceDispatcher` /
/// Java `AsyncTraceDispatcher implements TraceDispatcher`）。
///
/// 用法（Python `producer._start_trace_dispatcher`，`producer.py:471-493`）：
/// ```text
/// let group = AsyncTraceDispatcher::next_trace_producer_group("GID_test", Produce);
/// let producer = /* 用 group + rpc_hook 建内部生产者（enable_trace 必须为 false） */;
/// let d = Arc::new(AsyncTraceDispatcher::with_config("GID_test", Produce,
///     TraceDispatcherConfig { producer: Some(producer), trace_producer_group: Some(group),
///                             ..Default::default() }));
/// d.start(namesrv, Some(AccessChannel::Local)).await?;   // 或 TraceDispatcher::start(&*d, ..)
/// SendMessageTraceHook::new(d.clone()) ...                // d 即 TraceReportSink
/// d.shutdown_gracefully().await;                          // 或 TraceDispatcher::shutdown(&*d)
/// ```
#[derive(Clone)]
pub struct AsyncTraceDispatcher {
    inner: Arc<Inner>,
}

impl Inner {
    /// Python `FLUSH_TRACE_INTERVAL` 之外那条防忙等间隔（`:204-205`，5ms）。
    const IDLE_SLEEP_MILLIS: u64 = 5;

    /// Python `self.access_channel`（`:85` + `:136`）的读侧。
    fn access_channel(&self) -> AccessChannel {
        access_channel_from_u8(self.access_channel.load(Ordering::Acquire))
    }
}

impl AsyncTraceDispatcher {
    /// Python `AsyncTraceDispatcher(group, type_, batch_num=10)` +
    /// 默认 `trace_topic_name=None`（`:73-74`）；`producer` 取代 Python 在构造里
    /// 自己 new 的那个内部生产者（模块差异 1）。
    pub fn new(
        group: &str,
        dispatcher_type: TraceDispatcherType,
        producer: Arc<dyn TraceProducer>,
    ) -> AsyncTraceDispatcher {
        AsyncTraceDispatcher::with_config(
            group,
            dispatcher_type,
            TraceDispatcherConfig {
                producer: Some(producer),
                ..Default::default()
            },
        )
    }

    /// Python `__init__`（`trace_dispatcher.py:73-95`）的完整注入版本。
    pub fn with_config(
        group: &str,
        dispatcher_type: TraceDispatcherType,
        config: TraceDispatcherConfig,
    ) -> AsyncTraceDispatcher {
        if config.producer.is_none() {
            rmq_warn!(
                "trace dispatcher for group {group} has no internal trace producer: \
                 trace data will be dropped (see module doc deviation 1)"
            );
        }
        // Python `:95` + `:98-105`：组名自增一次（除非调用方已注入自己算好的名字），
        // 并且这三项设置在 Python 里就打在内部生产者上。
        let trace_producer_group = config.trace_producer_group.clone().unwrap_or_else(|| {
            AsyncTraceDispatcher::next_trace_producer_group(group, dispatcher_type)
        });
        let producer = config
            .producer
            .clone()
            .unwrap_or_else(|| Arc::new(DisabledTraceProducer));
        producer.set_send_msg_timeout(config.send_msg_timeout_millis);
        producer.set_max_message_size(config.max_msg_size);
        producer.set_enable_trace(false);
        AsyncTraceDispatcher {
            inner: Arc::new_cyclic(|weak: &Weak<Inner>| {
                let max_msg_size = AtomicUsize::new(config.max_msg_size);
                Inner {
                    group: group.to_string(),
                    dispatcher_type,
                    trace_instance_id: next_instance_id(),
                    trace_producer_group,
                    batch_num: config.batch_num.min(20),
                    max_msg_size,
                    trace_topic_name: config
                        .trace_topic_name
                        .filter(|topic| !topic.is_empty())
                        .unwrap_or_else(|| MixAll::TRACE_TOPIC.to_string()),
                    access_channel: AtomicU8::new(0),
                    queue: Mutex::new(VecDeque::new()),
                    queue_capacity: config.queue_capacity,
                    discard_count: AtomicU64::new(0),
                    stopped: AtomicBool::new(false),
                    is_started: AtomicBool::new(false),
                    last_flush_time: AtomicI64::new(current_time_millis()),
                    flush_trace_interval_millis: config.flush_trace_interval_millis,
                    send_which_queue: Arc::new(AtomicU64::new(0)),
                    start_lock: tokio::sync::Mutex::new(()),
                    worker: Mutex::new(None),
                    pending: Mutex::new(Vec::new()),
                    trace_producer: producer,
                    host_producer: Mutex::new(None),
                    host_consumer: Mutex::new(None),
                    send_msg_timeout_millis: config.send_msg_timeout_millis,
                    wait_for_shutdown_millis: config.wait_for_shutdown_millis,
                    handle: OnceLock::new(),
                    this: weak.clone(),
                }
            }),
        }
    }

    /// Python `_gen_group_name_for_trace`（`:107-109`）／Java
    /// `genGroupNameForTrace`（`AsyncTraceDispatcher.java:180-182`）：
    /// `_INNER_TRACE_PRODUCER-<group>-<PRODUCE|CONSUME>-<N>`，`N` 是进程级自增。
    ///
    /// 与实例方法版 [`Self::trace_producer_group`] 的区别：这里是**模块函数**，
    /// 让调用方能在构造内部生产者之前先把名字算出来（Python 是构造内部生产者时现算，
    /// 顺序无从外置 —— 见模块差异 1）。
    pub fn next_trace_producer_group(
        group: &str,
        dispatcher_type: TraceDispatcherType,
    ) -> String {
        format_trace_producer_group(group, dispatcher_type, next_group_counter())
    }

    // ---------------- 属性读（Python 直接读同名属性） ----------------

    /// Python `self.group`（`:78`）。
    pub fn group(&self) -> &str {
        &self.inner.group
    }

    /// Python `self.type`（`:79`）。
    pub fn dispatcher_type(&self) -> TraceDispatcherType {
        self.inner.dispatcher_type
    }

    /// Python `self.trace_instance_id`（`:77`）。
    pub fn trace_instance_id(&self) -> u64 {
        self.inner.trace_instance_id
    }

    /// 本分发器认定的内部生产者组名（Python 里只存在于生产者对象内部，`:100`）。
    pub fn trace_producer_group(&self) -> &str {
        &self.inner.trace_producer_group
    }

    /// Python `self.batch_num`（`:75`，已 clamp 到 20）。
    pub fn batch_num(&self) -> usize {
        self.inner.batch_num
    }

    /// Python `self.max_msg_size`（`:76`）：切块阈值（码点，模块差异 11）。
    pub fn max_msg_size(&self) -> usize {
        self.inner.max_msg_size.load(Ordering::Acquire)
    }

    /// Python `d.max_msg_size = ...`（直接改属性；单测 `test_trace.py:388` 就这么干）。
    pub fn set_max_msg_size(&self, max_msg_size: usize) {
        self.inner.max_msg_size.store(max_msg_size, Ordering::Release);
    }

    /// Python `self.trace_topic_name` 的读侧（`get_trace_topic_name`，`:111-112`）。
    pub fn get_trace_topic_name(&self) -> String {
        self.inner.trace_topic_name.clone()
    }

    /// Python `self.access_channel`（`:85` + `:136`）。
    pub fn access_channel(&self) -> AccessChannel {
        access_channel_from_u8(self.inner.access_channel.load(Ordering::Acquire))
    }

    /// Python `self.discard_count`（`:81`）。
    pub fn discard_count(&self) -> u64 {
        self.inner.discard_count.load(Ordering::Acquire)
    }

    /// Python `self.trace_context_queue.qsize()`（队列声明 `:80`，读取点 `:191`）。
    pub fn queue_size(&self) -> usize {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Python `self.is_started`（`:83`；`shutdown()` 不会复位它，见模块差异 14）。
    pub fn is_started(&self) -> bool {
        self.inner.is_started.load(Ordering::Acquire)
    }

    /// Python `self.stopped`（`:82`）。
    pub fn is_stopped(&self) -> bool {
        self.inner.stopped.load(Ordering::Acquire)
    }

    /// Python `self._last_flush_time`（`:90`）的读侧。
    pub fn last_flush_time(&self) -> i64 {
        self.inner.last_flush_time.load(Ordering::Acquire)
    }

    /// Python `self._last_flush_time` 的写侧（声明 `:90`；Python 只在
    /// `_async_send_trace_message` 里 `:210` 自己赋值，没有 setter ——
    /// 这里开放给单测：不起后台任务也能验证「超过 5s 触发刷写」那条分支）。
    pub fn set_last_flush_time(&self, last_flush_time: i64) {
        self.inner.last_flush_time.store(last_flush_time, Ordering::Release);
    }

    // ---------------- 宿主 ----------------

    /// Python `set_host_producer`（`:114-115`）。
    pub fn set_host_producer(&self, host: Arc<dyn TraceHost>) {
        *self.inner.host_producer.lock().unwrap_or_else(|e| e.into_inner()) = Some(host);
    }

    /// Python `set_host_consumer`（`:117-118`）。
    pub fn set_host_consumer(&self, host: Arc<dyn TraceHost>) {
        *self.inner.host_consumer.lock().unwrap_or_else(|e| e.into_inner()) = Some(host);
    }

    // ---------------- 生命周期 ----------------

    /// Python `start(name_srv_addr, access_channel=None)`（`:127-142`）／Java
    /// `start(String, AccessChannel)`（`AsyncTraceDispatcher.java:151-165`）。
    ///
    /// 顺序与 Python 逐行一致：锁内（`is_started` 为假时）设 namesrv / instanceName /
    /// `enable_trace=False` 并启动内部生产者 ⇒ 覆盖 `access_channel` ⇒
    /// worker 未起时拉起后台任务。`atexit.register(self.shutdown)`（`:142`）无等价物
    /// （模块差异 6）。
    ///
    /// 失败：内部生产者启动失败 ⇒ 原样上抛 `Err`（Python 让异常冒到
    /// `producer._start_trace_dispatcher` 的 `except` 里记 warning），此时
    /// `is_started` 仍为 `false`。必须在一个 tokio 运行时上下文里调用
    /// （模块差异 4）。
    pub async fn start(
        &self,
        name_server_addr: &str,
        access_channel: Option<AccessChannel>,
    ) -> Result<()> {
        self.inner.bind_handle();
        {
            let _lock = self.inner.start_lock.lock().await;
            if !self.inner.is_started.load(Ordering::Acquire) {
                // Python `:130-132`：instanceName = "PID_CLIENT_INNER_TRACE_PRODUCER_<namesrv>"
                self.inner
                    .trace_producer
                    .set_name_server_addr(name_server_addr);
                self.inner.trace_producer.set_instance_name(&format!(
                    "{}_{name_server_addr}",
                    TraceConstants::TRACE_INSTANCE_NAME
                ));
                // Python `:133`：⚠ 必须关闭自身的轨迹，否则轨迹消息会被再次追踪
                self.inner.trace_producer.set_enable_trace(false);
                Arc::clone(&self.inner.trace_producer).start().await?;
                self.inner.is_started.store(true, Ordering::Release);
            }
        }
        // Python `:136`：`access_channel or AccessChannel.LOCAL`（None ⇒ LOCAL）
        self.inner.set_access_channel(access_channel.unwrap_or_default());
        self.inner.start_worker();
        Ok(())
    }

    /// Python `shutdown()`（`:144-162`）的**优雅**版本：先 `flush()` 并等在途发送跑完
    /// （上限 [`TraceDispatcherConfig::wait_for_shutdown_millis`]，对齐 Java
    /// `ThreadUtils.shutdownGracefully`，模块差异 5），再执行与
    /// [`TraceDispatcher::shutdown`] 完全相同的收尾。
    ///
    /// 生产代码里 producer/consumer 移植层应优先用它（Python 的 `wait=False` 会让
    /// 最后一批轨迹与生产者关闭相互竞争）。
    pub async fn shutdown_gracefully(&self) {
        self.inner.flush_and_wait().await;
        TraceDispatcher::shutdown(self);
    }

    // ---------------- 入队 / 刷写 ----------------

    /// Python `append(ctx)`（`:165-173`）／Java `append(Object)`
    /// （`AsyncTraceDispatcher.java:184-191`）：把一个 [`TraceContext`] 入队；
    /// 队列满时计数 + INFO 并返回 `false`（与 Java 一致，**不阻塞业务**）。
    ///
    /// 与 Java/Python 一致：**不看 `stopped`**（[`TraceReportSink::report`] 的注释里
    /// 「已停止」只是实现方可选项，这里保留参考实现的语义）。
    pub fn append(&self, context: TraceContext) -> bool {
        match self.inner.push_context(context) {
            Some(rejected) => {
                let discard = self.inner.discard_count.fetch_add(1, Ordering::AcqRel) + 1;
                // 文案照抄 Python `:172`（`"buffer full%d ,context is %s"`，那个缺失的
                // 空格与逗号是线上字面量）
                rmq_info!("buffer full{discard} ,context is {rejected}");
                false
            }
            None => true,
        }
    }

    /// Python `flush()`（`:175-181`）／Java `flush()`
    /// （`AsyncTraceDispatcher.java:193-202`）：强制把队列排空，**不等待**在途发送
    /// （要等请用 [`Self::flush_and_wait`]）。
    pub fn flush(&self) {
        self.inner.flush();
    }

    /// Python `flush()`（`trace_dispatcher.py:175-181`，= Java `flush()`
    /// `AsyncTraceDispatcher.java:193-202`，两者都不等待）+ Java `shutdown()` 里那句
    /// `ThreadUtils.shutdownGracefully(traceExecutor, WAIT_FOR_SHUTDOWN)`
    /// （`AsyncTraceDispatcher.java:204-213`，Python 用的是 `wait=False`）：
    /// 排空队列、提交批次，并等在途发送任务收敛（最多
    /// [`TraceDispatcherConfig::wait_for_shutdown_millis`]，即 Python `:70` 的死常量）。
    pub async fn flush_and_wait(&self) {
        self.inner.flush_and_wait().await;
    }
}

// ---------------------------------------------------------------- 私有实现
//
// 以下都是 `Inner` 的方法，一一对应 Python `AsyncTraceDispatcher` 的下划线方法；
// 之所以写在 `Inner` 上：后台任务与发送链路只持 `Weak<Inner>`/`Arc<Inner>`，
// 不依赖对外的 [`AsyncTraceDispatcher`] 句柄（模块差异 6）。

impl Inner {
    /// Python `_async_run`（`:183-188`）／Java `AsyncRunnable.run`
    /// （`AsyncTraceDispatcher.java:249-266`）：后台循环。
    ///
    /// 与 Python 同：每轮 `_flush_trace_context(False)`，本轮没刷出批次时睡 5ms
    /// （模块差异 7）；`stopped` 置位后退出。只持 `Weak`，宿主句柄全释放后自动退出
    /// （模块差异 6）。
    async fn async_run(inner: Weak<Inner>) {
        loop {
            let Some(inner) = inner.upgrade() else {
                rmq_debug!("trace dispatcher released, worker exits");
                return;
            };
            if inner.stopped.load(Ordering::Acquire) {
                return;
            }
            if inner.flush_trace_context(false) == 0 {
                tokio::time::sleep(Duration::from_millis(Self::IDLE_SLEEP_MILLIS)).await;
            }
        }
    }

    /// Python `_flush_trace_context(force_flush)`（`:190-205`）／Java
    /// `flushTraceContext`（`AsyncTraceDispatcher.java:268-287`）。
    ///
    /// 触发条件三者取其一：`force_flush`、队列长度 `>= batch_num`、
    /// 距上次刷写超过 `FLUSH_TRACE_INTERVAL`；每次最多取 `batch_num` 条。
    ///
    /// 返回值是本轮真正取走的条数（0 ⇒ 什么都没刷出去，Python 在这种情形下睡 5ms）。
    /// Python 的 `time.sleep(5)` 与异常吞掉分别由 [`Self::async_run`] 和
    /// [`Inner::async_send_trace_message`] 内部记日志替代（模块差异 7）。
    fn flush_trace_context(&self, force_flush: bool) -> usize {
        let size = self.queue_len();
        if size != 0 {
            let now = current_time_millis();
            if force_flush
                || size >= self.batch_num
                || now - self.last_flush_time.load(Ordering::Acquire)
                    > self.flush_trace_interval_millis
            {
                let mut context_list: Vec<TraceContext> = Vec::with_capacity(self.batch_num);
                for _ in 0..self.batch_num {
                    match self.pop_context() {
                        Some(context) => context_list.push(context),
                        None => break,
                    }
                }
                let drained = context_list.len();
                self.async_send_trace_message(context_list);
                return drained;
            }
        }
        0
    }

    /// Python `_async_send_trace_message`（`:207-211`）／Java
    /// `asyncSendTraceMessage`（`AsyncTraceDispatcher.java:289-293`）：
    /// 空批次直接返回；**先**更新 `last_flush_time` 再提交（Python 就是这个顺序，
    /// 影响下一轮的超时判定）。
    fn async_send_trace_message(&self, context_list: Vec<TraceContext>) {
        if context_list.is_empty() {
            return;
        }
        self.last_flush_time
            .store(current_time_millis(), Ordering::Release);
        let drained = context_list.len();
        if let Err(e) = self.submit(context_list) {
            // Python：`_executor.submit` 抛错会冒到 `_async_run` / `flush` 的
            // `except`，记 "flushTraceContext error"；这里就地记同一条量级的日志。
            rmq_error!("flushTraceContext error: {e} ({drained} contexts dropped)");
        }
    }

    /// 把一批上下文交给「executor」（Python `_executor.submit`，`:211`）。
    fn submit(&self, context_list: Vec<TraceContext>) -> Result<()> {
        let handle = self.runtime_handle()?;
        let me = self
            .this
            .upgrade()
            .ok_or_else(|| Error::client("trace dispatcher already released"))?;
        let task = handle.spawn(async move {
            me.send_trace_data(context_list).await;
        });
        self.track(task);
        Ok(())
    }

    /// 在途任务记账（保留未跑完的句柄，供 [`Inner::flush_and_wait`] 等待）。
    fn track(&self, handle: JoinHandle<()>) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.retain(|h| !h.is_finished());
        pending.push(handle);
    }

    fn take_pending(&self) -> Vec<JoinHandle<()>> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *pending)
    }

    /// 惰性绑定运行时句柄（模块差异 4）：构造时不碰 `Handle`，只在真正要派发动作时
    /// 取一次并缓存。
    fn bind_handle(&self) -> Option<Handle> {
        if let Some(handle) = self.handle.get() {
            return Some(handle.clone());
        }
        let handle = Handle::try_current().ok()?;
        let _ = self.handle.set(handle.clone());
        Some(handle)
    }

    fn runtime_handle(&self) -> Result<Handle> {
        self.bind_handle().ok_or_else(|| {
            Error::client(
                "no tokio runtime available: trace dispatcher needs start() inside a runtime",
            )
        })
    }

    /// Python `with self._lock:` + `if self.worker is None:`（`:137-141`）。
    fn start_worker(&self) {
        let mut worker = self.worker.lock().unwrap_or_else(|e| e.into_inner());
        if worker.is_none() {
            self.stopped.store(false, Ordering::Release);
            let weak = match self.this.upgrade() {
                Some(strong) => Arc::downgrade(&strong),
                None => return,
            };
            // Python 线程名 "MQ-AsyncArrayDispatcher-Thread<id>"（`:140`）无法在
            // tokio 任务上复刻（模块差异 5）；这里只把实例号打进调试日志。
            rmq_debug!("start trace worker for instance {}", self.trace_instance_id);
            *worker = Some(tokio::spawn(Inner::async_run(weak)));
        }
    }

    fn flush(&self) {
        // Python `:177-181`
        while self.queue_len() != 0 {
            if self.flush_trace_context(true) == 0 {
                rmq_warn!(
                    "flushTraceContext made no progress with {} queued (batch_num=0?)",
                    self.queue_len()
                );
                return;
            }
        }
    }

    async fn flush_and_wait(&self) {
        let deadline = Instant::now() + Duration::from_millis(self.wait_for_shutdown_millis);
        loop {
            self.flush();
            let pending = self.take_pending();
            if pending.is_empty() {
                return;
            }
            for join in pending {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    rmq_warn!(
                        "trace dispatcher wait for shutdown timeout ({}ms), \
                         pending trace batches may be lost",
                        self.wait_for_shutdown_millis
                    );
                    return;
                }
                match tokio::time::timeout(remaining, join).await {
                    Err(_) => rmq_warn!(
                        "trace dispatcher wait for shutdown timeout ({}ms)",
                        self.wait_for_shutdown_millis
                    ),
                    Ok(Err(e)) => rmq_debug!("trace send task cancelled: {e}"),
                    Ok(Ok(())) => {}
                }
            }
        }
    }

    /// Python `shutdown()` 的同步部分（`:144-162`）。
    fn shutdown(&self) {
        self.flush();
        // Python `:149-152` `_executor.shutdown(wait=False)`：在途任务不取消
        // （Python 线程池会把已提交的任务跑完）；Rust 侧同样只丢句柄不 abort。
        // Python `:153-157`：只有启动过才关内部生产者
        if self.is_started.load(Ordering::Acquire) {
            self.trace_producer.shutdown();
        }
        // Python `:158-161` `atexit.unregister(self.shutdown)`：无对应物（模块差异 6）
        self.stored_stop();
    }

    /// Python `self.stopped = True`（`:162`）+ 终止后台任务（模块差异 9）。
    fn stored_stop(&self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(handle) = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take() {
            handle.abort();
        }
    }

    // ---------------- 发送 ----------------

    /// Python `_send_trace_data`（`:214-231`）／Java `AsyncDataSendTask.sendTraceData`
    /// （`AsyncTraceDispatcher.java:307-335`）：按「业务 topic + 轨迹 topic」分组，
    /// 逐组交给 [`Inner::flush_data`]。
    ///
    /// 跳过规则同 Python `:220`：`region_id` 为空（含 Java 的 `null`）或
    /// `trace_beans` 为空 —— ⚠ Java 只判 `null`（`AsyncTraceDispatcher.java:316`），
    /// 空串的 `region_id` 在 Python 里会被丢掉，这里跟 Python
    /// （`test_trace.py::test_dispatcher_skips_context_without_region_or_beans`）。
    async fn send_trace_data(&self, context_list: Vec<TraceContext>) {
        let default_channel = self.access_channel();
        let mut groups: Vec<(String, Vec<TraceTransferBean>)> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        for context in context_list {
            let channel = context.access_channel.unwrap_or(default_channel);
            if context.region_id.is_empty() || context.trace_beans.is_empty() {
                continue;
            }
            let trace_topic = if channel.is_cloud() {
                format!(
                    "{}{}",
                    TraceConstants::TRACE_TOPIC_PREFIX,
                    context.region_id
                )
            } else {
                self.trace_topic_name.clone()
            };
            // Python `:226`：`context.trace_beans[0].topic`（上面已判非空）
            let Some(first) = context.trace_beans.first() else {
                continue;
            };
            let key = format!(
                "{}{}{trace_topic}",
                first.topic,
                TraceConstants::CONTENT_SPLITOR
            );
            let Some(bean) = TraceDataEncoder::encoder_from_context_bean(Some(&context)) else {
                continue;
            };
            match positions.get(&key) {
                Some(&index) => groups[index].1.push(bean),
                None => {
                    positions.insert(key.clone(), groups.len());
                    groups.push((key, vec![bean]));
                }
            }
        }
        for (key, bean_list) in groups {
            // Python `:230` `topic, trace_topic = key.split(CONTENT_SPLITOR)`
            let Some((topic, trace_topic)) =
                key.split_once(TraceConstants::CONTENT_SPLITOR)
            else {
                rmq_debug!("trace group key {key:?} has no content splitor, skipped");
                continue;
            };
            self.flush_data(bean_list, topic, trace_topic).await;
        }
    }

    /// Python `_flush_data`（`:233-251`）／Java `flushData`
    /// （`AsyncTraceDispatcher.java:337-359`）：把一组记录拼成一个 payload，
    /// 累计长度达到 `max_msg_size` 就切一块发出去。
    ///
    /// `topic` 参数在 Python 与 Java 里都只是形参（发送只用 `trace_topic`），
    /// 保留形参以对齐 `_flush_data(bean_list, topic, trace_topic)` 的调用形状。
    ///
    /// Python 末尾的 `trans_bean_list.clear()`（`:251`）由 Rust 的所有权接管。
    async fn flush_data(
        &self,
        trans_bean_list: Vec<TraceTransferBean>,
        _topic: &str,
        trace_topic: &str,
    ) {
        if trans_bean_list.is_empty() {
            return;
        }
        let limit = self.max_msg_size.load(Ordering::Acquire);
        let mut buffer = String::new();
        let mut buffer_chars = 0usize;
        let mut key_set: BTreeSet<String> = BTreeSet::new();
        let mut count = 0usize;
        for bean in trans_bean_list {
            key_set.extend(bean.trans_key);
            buffer_chars += bean.trans_data.chars().count();
            buffer.push_str(&bean.trans_data);
            count += 1;
            if buffer_chars >= limit {
                self.send_trace_data_by_mq(
                    std::mem::take(&mut key_set),
                    std::mem::take(&mut buffer),
                    trace_topic,
                )
                .await;
                buffer_chars = 0;
                count = 0;
            }
        }
        if count > 0 {
            self.send_trace_data_by_mq(key_set, buffer, trace_topic).await;
        }
    }

    /// Python `_send_trace_data_by_mq`（`:253-265`）／Java `sendTraceDataByMQ`
    /// （`AsyncTraceDispatcher.java:368-409`）：一块 payload = 一条轨迹消息。
    ///
    /// * body：拼好的文本（UTF-8）；
    /// * topic：`trace_topic`（LOCAL ⇒ `RMQ_SYS_TRACE_TOPIC`，CLOUD ⇒
    ///   `rmq_sys_TRACE_DATA_<regionId>`）；
    /// * KEYS：块内所有 `trans_key` 的并集，**按 [`KEY_SEPARATOR`] 拼、且先过滤空串**
    ///   —— Python `:256` 显式 `sorted(...)`（Java 是 `message.setKeys(keySet)`，
    ///   `HashSet` 顺序不定）。Rust 跟 Python：排序后拼接（`TraceTransferBean.trans_key`
    ///   本身是 `BTreeSet`，天然有序）。放的是**原始消息的 msgId**（不是 offsetMsgId），
    ///   控制台按它反查轨迹。
    async fn send_trace_data_by_mq(
        &self,
        key_set: BTreeSet<String>,
        data: String,
        trace_topic: &str,
    ) {
        let mut msg = Message::new(trace_topic, Some(data.as_bytes()));
        let keys: Vec<&str> = key_set
            .iter()
            .filter(|key| !key.is_empty())
            .map(|key| key.as_str())
            .collect();
        msg.set_keys(&keys.join(KEY_SEPARATOR));

        let broker_set = self.try_get_message_queue_broker_set(trace_topic).await;
        let producer = Arc::clone(&self.trace_producer);
        let outcome = if broker_set.is_empty() {
            // Python `:259-260`：拿不到 broker 集合 ⇒ 普通发送（生产者内部轮询）
            producer.send(msg, self.send_msg_timeout_millis).await
        } else {
            // Python `:261-263` + 模块差异 2：先取路由再选队，最后定点发送
            let queues = producer
                .clone()
                .publish_queues(trace_topic.to_string())
                .await
                .unwrap_or_default();
            let selector = BrokerSetSelector {
                counter: Arc::clone(&self.send_which_queue),
            };
            match selector.select(&queues, &broker_set) {
                Some(queue) => producer.send_to_queue(msg, queue, self.send_msg_timeout_millis).await,
                None => Err(Error::client(format!(
                    "no message queue of trace topic {trace_topic} for broker set {:?}",
                    sorted_broker_names(&broker_set)
                ))),
            }
        };
        if let Err(e) = outcome {
            // 文案照抄 Python `:265`
            rmq_error!("send trace data failed, the traceData is {data}: {e}");
        }
    }

    /// Python `_try_get_message_queue_broker_set`（`:267-280`）／Java 同名方法
    /// （`AsyncTraceDispatcher.java:411-425`）：该 topic 涉及的 broker 名集合。
    ///
    /// 只有跨集群（CLOUD）场景才需要按 broker 过滤；拿不到路由时返回空集 ⇒ 走普通轮询。
    /// 日志级别同 Python（`logger.debug`）。
    async fn try_get_message_queue_broker_set(&self, topic: &str) -> HashSet<String> {
        let mut broker_set = HashSet::new();
        let producer = Arc::clone(&self.trace_producer);
        match producer.publish_queues(topic.to_string()).await {
            Ok(queues) => {
                for queue in queues {
                    broker_set.insert(queue.broker_name);
                }
            }
            Err(e) => rmq_debug!("tryGetMessageQueueBrokerSet({topic}) failed: {e}"),
        }
        broker_set
    }

    /// Python `_client_id`（`:120-124`）：**producer 优先于 consumer**，
    /// 没有宿主（或宿主没有 `_mq_client`）时返回空串。
    fn client_id(&self) -> String {
        let host = {
            let producer = self.host_producer.lock().unwrap_or_else(|e| e.into_inner());
            producer.clone().or_else(|| {
                self.host_consumer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
            })
        };
        match host {
            Some(host) => host.client_id(),
            None => String::new(),
        }
    }

    fn set_access_channel(&self, channel: AccessChannel) {
        self.access_channel
            .store(access_channel_to_u8(channel), Ordering::Release);
    }

    // ---------------- 队列 ----------------

    /// 入队；满了把原值退回（对应 Python `put_nowait` 抛 `queue.Full`）。
    fn push_context(&self, context: TraceContext) -> Option<TraceContext> {
        let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        if queue.len() >= self.queue_capacity {
            return Some(context);
        }
        queue.push_back(context);
        None
    }

    /// 出队（对应 Python `get_nowait`，空队列返回 `None`）。
    fn pop_context(&self) -> Option<TraceContext> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).pop_front()
    }

    fn queue_len(&self) -> usize {
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

impl TraceDispatcher for AsyncTraceDispatcher {
    /// Python `start(name_srv_addr, access_channel=None)` 的**同步**入口
    /// （对象安全要求，见模块差异 4）：整段启动逻辑交给 tokio 任务，
    /// 因此本方法返回 `Ok` 只保证「任务已排队」，内部生产者起没起来要看日志。
    ///
    /// 唯一的同步可见错误：当前线程不在 tokio 运行时上下文里且此前没绑定过句柄
    /// ⇒ `Err`（Python 无线程模型限制）。
    /// 能 await 的调用方请用 [`AsyncTraceDispatcher::start`]（Python 原版语义）。
    fn start(&self, name_server_addr: &str) -> Result<()> {
        let handle = self.inner.runtime_handle()?;
        let me = self.clone();
        let addr = name_server_addr.to_string();
        handle.spawn(async move {
            if let Err(e) = AsyncTraceDispatcher::start(&me, &addr, None).await {
                rmq_error!("trace dispatcher start failed: {e}");
            }
        });
        Ok(())
    }

    /// Python `shutdown()`（`:144-162`）：flush（不等待）+ 关内部生产者 + `stopped=True`。
    /// 要等发完请用 [`AsyncTraceDispatcher::shutdown_gracefully`]。
    fn shutdown(&self) {
        self.inner.shutdown();
    }
}

impl TraceReportSink for AsyncTraceDispatcher {
    /// Python `get_trace_topic_name()`（`:111-112`）／Java `getTraceTopicName()`。
    fn trace_topic_name(&self) -> String {
        self.inner.trace_topic_name.clone()
    }

    /// Python `append(ctx)`（`:165-173`）。
    fn report(&self, context: TraceContext) -> bool {
        self.append(context)
    }

    /// Python `_client_id()`（`:120-124`）。
    fn client_id(&self) -> String {
        self.inner.client_id()
    }
}

/// `AccessChannel` 的可表示形式（Python 是个属性值，这里用原子量存，见
/// `trace_dispatcher.py:85`）。
fn access_channel_to_u8(channel: AccessChannel) -> u8 {
    match channel {
        AccessChannel::Local => 0,
        AccessChannel::Cloud => 1,
    }
}

fn access_channel_from_u8(raw: u8) -> AccessChannel {
    match raw {
        1 => AccessChannel::Cloud,
        _ => AccessChannel::Local,
    }
}

/// 只为了让「选不到队列」这条错误日志稳定可断言（`HashSet` 迭代顺序不定）。
fn sorted_broker_names(broker_set: &HashSet<String>) -> Vec<&str> {
    let mut names: Vec<&str> = broker_set.iter().map(|name| name.as_str()).collect();
    names.sort_unstable();
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::result::LocalTransactionState;
    use crate::client::trace::{TraceBean, TraceType};
    use crate::common::message_type::MessageType;
    use std::sync::atomic::AtomicUsize;

    // 金标准来源：`python/tests/test_trace.py` + 本机跑
    // `python/rocketmq/client/trace_dispatcher.py` 的探针（/tmp/probe_td.py、
    // /tmp/probe_td2.py，PYTHONPATH=. python3 执行），非手推。
    const SOH: char = TraceConstants::CONTENT_SPLITOR;
    const STX: char = TraceConstants::FIELD_SPLITOR;
    const MSG_ID_1: &str = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6";
    const MSG_ID_2: &str = "AC1400A1F0A018B4AAC2A1B2C3D4E5F7";
    const OFFSET_MSG_ID: &str = "AC1400A1000027100000000000000001";
    const TS: i64 = 1_700_000_000_000;
    /// Python `EXPECTED_PUB`（`test_trace.py:37-39`）＝探针第 4 节抓到的 body。
    const EXPECTED_PUB: &str = "Pub\u{1}1700000000000\u{1}DefaultRegion\u{1}GID_test\
         \u{1}TopicTest\u{1}AC1400A1F0A018B4AAC2A1B2C3D4E5F6\u{1}TagA\u{1}KeyA KeyB\
         \u{1}127.0.0.1:10911\u{1}42\u{1}7\u{1}0\
         \u{1}AC1400A1000027100000000000000001\u{1}true\u{2}";
    const EXPECTED_PUB_KEYS: &str = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6 KeyA KeyB";

    // ---------------- 假生产者（Python _FakeTraceProducer 的等价物） ----------------

    #[derive(Debug, Clone)]
    struct SentRecord {
        /// `send` / `send_to_queue`：区分 Python 的两条发送路径。
        path: &'static str,
        topic: String,
        body: String,
        keys: String,
        timeout_millis: i64,
        queue: Option<MessageQueue>,
    }

    #[derive(Default)]
    struct Recorder {
        records: Mutex<Vec<SentRecord>>,
        setters: Mutex<Vec<String>>,
        started: AtomicUsize,
        shutdowns: AtomicUsize,
        publish_calls: AtomicUsize,
        queues: Mutex<Vec<MessageQueue>>,
    }

    impl Recorder {
        fn push_setter(&self, text: String) {
            self.setters.lock().unwrap_or_else(|e| e.into_inner()).push(text);
        }

        fn setters(&self) -> Vec<String> {
            self.setters.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }

        fn records(&self) -> Vec<SentRecord> {
            self.records.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }

        fn set_queues(&self, queues: Vec<MessageQueue>) {
            *self.queues.lock().unwrap_or_else(|e| e.into_inner()) = queues;
        }

        fn push(&self, record: SentRecord) {
            self.records.lock().unwrap_or_else(|e| e.into_inner()).push(record);
        }
    }

    struct FakeProducer {
        recorder: Arc<Recorder>,
        /// 打开后每次发送都返回 `Err`，用来验证 Python `:264-265` 那条
        /// 「捕获发送异常、只记日志、继续发下一块」的路径。
        fail_send: bool,
    }

    impl TraceProducer for FakeProducer {
        fn set_name_server_addr(&self, name_server_addr: &str) {
            self.recorder.push_setter(format!("namesrv={name_server_addr}"));
        }

        fn set_instance_name(&self, instance_name: &str) {
            self.recorder.push_setter(format!("instance={instance_name}"));
        }

        fn set_send_msg_timeout(&self, timeout_millis: i64) {
            self.recorder
                .push_setter(format!("send_msg_timeout={timeout_millis}"));
        }

        fn set_max_message_size(&self, max_msg_size: usize) {
            self.recorder.push_setter(format!("max_message_size={max_msg_size}"));
        }

        fn set_enable_trace(&self, enable: bool) {
            self.recorder.push_setter(format!("enable_trace={enable}"));
        }

        fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>> {
            let recorder = Arc::clone(&self.recorder);
            Box::pin(async move {
                recorder.started.fetch_add(1, Ordering::AcqRel);
                Ok(())
            })
        }

        fn shutdown(&self) {
            self.recorder.shutdowns.fetch_add(1, Ordering::AcqRel);
        }

        fn send(
            self: Arc<Self>,
            message: Message,
            timeout_millis: i64,
        ) -> TraceProducerFuture<Result<()>> {
            let fail_send = self.fail_send;
            Box::pin(async move {
                self.recorder.push(record_of("send", &message, timeout_millis, None));
                if fail_send {
                    return Err(Error::client("fake trace send failure"));
                }
                Ok(())
            })
        }

        fn send_to_queue(
            self: Arc<Self>,
            message: Message,
            queue: MessageQueue,
            timeout_millis: i64,
        ) -> TraceProducerFuture<Result<()>> {
            Box::pin(async move {
                self.recorder.push(record_of(
                    "send_to_queue",
                    &message,
                    timeout_millis,
                    Some(queue),
                ));
                Ok(())
            })
        }

        fn publish_queues(
            self: Arc<Self>,
            _topic: String,
        ) -> TraceProducerFuture<Result<Vec<MessageQueue>>> {
            Box::pin(async move {
                self.recorder.publish_calls.fetch_add(1, Ordering::AcqRel);
                let queues = self
                    .recorder
                    .queues
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                if queues.is_empty() {
                    // Python 探针里 `_FakeTraceProducer._topic_publish_info` 直接 raise
                    Err(Error::client("no route in unit test"))
                } else {
                    Ok(queues)
                }
            })
        }
    }

    fn record_of(
        path: &'static str,
        message: &Message,
        timeout_millis: i64,
        queue: Option<MessageQueue>,
    ) -> SentRecord {
        SentRecord {
            path,
            topic: message.topic.clone(),
            body: String::from_utf8_lossy(message.body.as_deref().unwrap_or_default()).into_owned(),
            keys: message.get_keys().unwrap_or_default().to_string(),
            timeout_millis,
            queue,
        }
    }

    fn dispatcher_with(
        batch_num: usize,
        trace_topic: Option<&str>,
    ) -> (AsyncTraceDispatcher, Arc<Recorder>) {
        build_dispatcher(batch_num, trace_topic, false)
    }

    /// 同 [`dispatcher_with`]，但内部生产者每次发送都失败。
    fn failing_dispatcher(batch_num: usize) -> (AsyncTraceDispatcher, Arc<Recorder>) {
        build_dispatcher(batch_num, None, true)
    }

    fn build_dispatcher(
        batch_num: usize,
        trace_topic: Option<&str>,
        fail_send: bool,
    ) -> (AsyncTraceDispatcher, Arc<Recorder>) {
        // 两个 Arc 指向同一个 Recorder： FakeProducer 记账，测试读账
        let recorder = Arc::new(Recorder::default());
        let producer = Arc::new(FakeProducer {
            recorder: Arc::clone(&recorder),
            fail_send,
        });
        let dispatcher = AsyncTraceDispatcher::with_config(
            "GID_test",
            TraceDispatcherType::Produce,
            TraceDispatcherConfig {
                producer: Some(producer),
                batch_num,
                trace_topic_name: trace_topic.map(|t| t.to_string()),
                ..Default::default()
            },
        );
        (dispatcher, recorder)
    }

    fn dispatcher() -> (AsyncTraceDispatcher, Arc<Recorder>) {
        dispatcher_with(10, None)
    }

    // ---------------- 上下文构造（与 python/tests/test_trace.py 同形） ----------------

    fn bean(topic: &str, msg_id: &str, keys: &str) -> TraceBean {
        TraceBean {
            topic: topic.to_string(),
            msg_id: msg_id.to_string(),
            offset_msg_id: OFFSET_MSG_ID.to_string(),
            tags: "TagA".into(),
            keys: keys.into(),
            store_host: "127.0.0.1:10911".into(),
            client_host: "127.0.0.1:10911".into(),
            store_time: 1_700_000_000_123,
            retry_times: 2,
            body_length: 42,
            msg_type: MessageType::NormalMsg,
            transaction_id: Some("TRAN-001".into()),
            transaction_state: Some(LocalTransactionState::CommitMessage.to_string()),
            from_transaction_check: false,
        }
    }

    fn pub_context(topic: &str, msg_id: &str, region: &str) -> TraceContext {
        TraceContext {
            trace_type: Some(TraceType::Pub),
            time_stamp: TS,
            region_id: region.to_string(),
            region_name: String::new(),
            group_name: "GID_test".into(),
            cost_time: 7,
            is_success: true,
            request_id: "REQ-PUB-001".into(),
            context_code: 0,
            access_channel: None,
            trace_beans: vec![bean(topic, msg_id, "KeyA KeyB")],
        }
    }

    fn simple_pub() -> TraceContext {
        pub_context("TopicTest", MSG_ID_1, "DefaultRegion")
    }

    fn record_count(body: &str) -> usize {
        body.chars().filter(|c| *c == STX).count()
    }

    // ---------------- 默认值 / 常量（探针第 1 节） ----------------

    #[test]
    fn defaults_match_python_reference() {
        let (d, recorder) = dispatcher();
        assert_eq!(d.batch_num(), 10);
        assert_eq!(d.max_msg_size(), 128_000);
        assert_eq!(d.get_trace_topic_name(), MixAll::TRACE_TOPIC);
        assert_eq!(d.group(), "GID_test");
        assert_eq!(d.dispatcher_type(), TraceDispatcherType::Produce);
        assert_eq!(d.discard_count(), 0);
        assert!(!d.is_started());
        assert!(!d.is_stopped());
        assert_eq!(d.access_channel(), AccessChannel::Local);
        assert_eq!(d.queue_size(), 0);
        // Python `__init__` 里打在内部生产者上的三项（trace_dispatcher.py:101-104）
        assert_eq!(
            recorder.setters(),
            vec![
                "send_msg_timeout=5000",
                "max_message_size=128000",
                "enable_trace=false"
            ]
        );
    }

    #[test]
    fn batch_num_is_clamped_to_twenty() {
        // Python `min(batch_num, 20)`（:75）
        for (input, want) in [(50usize, 20usize), (1, 1), (100, 20), (20, 20)] {
            let (d, _) = dispatcher_with(input, None);
            assert_eq!(d.batch_num(), want, "batch_num({input})");
        }
    }

    #[test]
    fn trace_topic_falls_back_on_none_and_empty_string() {
        // Python `trace_topic_name or MixAll.TRACE_TOPIC`（:92）
        let (d, _) = dispatcher_with(10, Some("MyTraceTopic"));
        assert_eq!(d.get_trace_topic_name(), "MyTraceTopic");
        assert_eq!(
            TraceReportSink::trace_topic_name(&d),
            "MyTraceTopic",
            "钩子侧读的是同一个值"
        );
        let (d, _) = dispatcher_with(10, Some(""));
        assert_eq!(d.get_trace_topic_name(), "RMQ_SYS_TRACE_TOPIC");
    }

    #[test]
    fn trace_producer_group_format() {
        // 字面格式 = Python `_gen_group_name_for_trace`（`:107-109`）的金标准：
        // 连建两个分发器后 gen 依次是
        //   _INNER_TRACE_PRODUCER-GID_test-PRODUCE-3
        //   _INNER_TRACE_PRODUCER-CID_test-CONSUME-4
        // （前两个构造占掉 1、2 号 —— 计数器从 1 开始，`itertools.count(1)`，`:68`）
        assert_eq!(
            format_trace_producer_group("GID_test", TraceDispatcherType::Produce, 3),
            "_INNER_TRACE_PRODUCER-GID_test-PRODUCE-3"
        );
        assert_eq!(
            format_trace_producer_group("CID_test", TraceDispatcherType::Consume, 4),
            "_INNER_TRACE_PRODUCER-CID_test-CONSUME-4"
        );
        // 自增版：只能断言「严格变大」—— 并行的其他用例也在占号
        let first = AsyncTraceDispatcher::next_trace_producer_group(
            "GID_test",
            TraceDispatcherType::Produce,
        );
        let second = AsyncTraceDispatcher::next_trace_producer_group(
            "GID_test",
            TraceDispatcherType::Produce,
        );
        assert!(
            first.starts_with("_INNER_TRACE_PRODUCER-GID_test-PRODUCE-"),
            "{first}"
        );
        assert!(group_seq(&second) > group_seq(&first), "{first} -> {second}");
        // 构造时也算一次（Python 在构造里给生产者起名，`:100`）
        let (d, _) = dispatcher();
        assert!(
            d.trace_producer_group()
                .starts_with("_INNER_TRACE_PRODUCER-GID_test-PRODUCE-"),
            "{}",
            d.trace_producer_group()
        );
    }

    #[test]
    fn injected_group_name_is_used_verbatim() {
        // 模块差异 1：调用方先把名字算出来（建内部生产者要用），构造分发器时注入回去
        let group = format_trace_producer_group("G", TraceDispatcherType::Produce, 999);
        let d = AsyncTraceDispatcher::with_config(
            "G",
            TraceDispatcherType::Produce,
            TraceDispatcherConfig {
                producer: Some(Arc::new(DisabledTraceProducer)),
                trace_producer_group: Some(group.clone()),
                ..Default::default()
            },
        );
        assert_eq!(d.trace_producer_group(), group, "注入的名字原样保留");
        assert_eq!(d.group(), "G", "业务组名不受影响");
        // 不注入时才走进程级计数器（名字前缀同 Python，号 > 0）
        let (built, _) = dispatcher();
        assert!(
            built
                .trace_producer_group()
                .starts_with("_INNER_TRACE_PRODUCER-GID_test-PRODUCE-"),
            "{}",
            built.trace_producer_group()
        );
        assert!(group_seq(built.trace_producer_group()) > 0);
    }

    /// 从组名尾巴上取自增号；解析不出来时返回 0（`unwrap_or` 保证测试不 panic）。
    fn group_seq(name: &str) -> u64 {
        name.rsplit('-')
            .next()
            .unwrap_or_default()
            .parse::<u64>()
            .unwrap_or(0)
    }

    #[test]
    fn instance_id_is_process_wide_and_increments() {
        // Python `_INSTANCE_NUM = itertools.count(0)`（`:69`）：进程级、从 0 开始。
        // 并行用例也会构造分发器 ⇒ 断言严格递增而非 +1。
        let (a, _) = dispatcher();
        let (b, _) = dispatcher();
        assert!(
            b.trace_instance_id() > a.trace_instance_id(),
            "{} !> {}",
            b.trace_instance_id(),
            a.trace_instance_id()
        );
    }

    // ---------------- append / 溢出 ----------------

    #[test]
    fn append_returns_true_and_queues() {
        let (d, _) = dispatcher();
        assert!(d.append(simple_pub()));
        assert_eq!(d.queue_size(), 1);
        assert_eq!(d.discard_count(), 0);
    }

    #[test]
    fn append_returns_false_when_queue_full() {
        // Python 探针第 11 节：2048 条收、第 2049 条丢 + discard_count=1
        let (d, _) = dispatcher();
        for _ in 0..2048 {
            assert!(d.append(simple_pub()));
        }
        assert_eq!(d.queue_size(), 2048);
        assert!(!d.append(simple_pub()));
        assert_eq!(d.discard_count(), 1);
        assert!(!d.append(simple_pub()));
        assert_eq!(d.discard_count(), 2);
        assert_eq!(d.queue_size(), 2048);
    }

    #[test]
    fn report_sink_delegates_to_append() {
        let (d, _) = dispatcher();
        assert!(TraceReportSink::report(&d, simple_pub()));
        assert_eq!(d.queue_size(), 1);
    }

    // ---------------- 发送：与 Python 探针逐字节对齐 ----------------

    #[tokio::test]
    async fn send_trace_data_single_pub_matches_python_golden() {
        let (d, recorder) = dispatcher();
        d.inner.send_trace_data(vec![simple_pub()]).await;
        let records = recorder.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        // 没有路由 ⇒ broker 集合为空 ⇒ Python `producer.send(msg, 5000)` 那条路径
        assert_eq!(record.path, "send");
        assert_eq!(record.topic, "RMQ_SYS_TRACE_TOPIC");
        assert_eq!(record.body, EXPECTED_PUB);
        assert_eq!(record.keys, EXPECTED_PUB_KEYS);
        assert_eq!(record.timeout_millis, 5_000);
        assert_eq!(record.queue, None);
    }

    #[tokio::test]
    async fn send_trace_data_cloud_channel_uses_region_topic() {
        // 探针第 5 节：rmq_sys_TRACE_DATA_DefaultRegion
        let (d, recorder) = dispatcher();
        let mut ctx = simple_pub();
        ctx.access_channel = Some(AccessChannel::Cloud);
        d.inner.send_trace_data(vec![ctx]).await;
        let records = recorder.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].topic, "rmq_sys_TRACE_DATA_DefaultRegion");
        assert_eq!(records[0].body, EXPECTED_PUB);
    }

    #[tokio::test]
    async fn dispatcher_access_channel_applies_to_context_without_one() {
        // Python `:218` `context.access_channel or self.access_channel`
        let (d, recorder) = dispatcher();
        d.inner.set_access_channel(AccessChannel::Cloud);
        d.inner.send_trace_data(vec![simple_pub()]).await;
        assert_eq!(
            recorder.records()[0].topic,
            "rmq_sys_TRACE_DATA_DefaultRegion"
        );
    }

    #[tokio::test]
    async fn contexts_without_region_or_beans_are_skipped() {
        // 探针第 7 节：region 空串 / beans 为空 都不发
        let (d, recorder) = dispatcher();
        d.inner
            .send_trace_data(vec![
                pub_context("TopicTest", MSG_ID_1, ""),
                {
                    let mut ctx = simple_pub();
                    ctx.trace_beans = Vec::new();
                    ctx
                },
            ])
            .await;
        assert!(recorder.records().is_empty());
    }

    #[tokio::test]
    async fn groups_by_business_topic_in_insertion_order() {
        // 探针第 6 节：2 组，TopicA 组 2 条、TopicB 组 1 条（顺序即插入序）
        let (d, recorder) = dispatcher();
        d.inner
            .send_trace_data(vec![
                pub_context("TopicA", MSG_ID_1, "DefaultRegion"),
                pub_context("TopicB", MSG_ID_1, "DefaultRegion"),
                pub_context("TopicA", MSG_ID_2, "DefaultRegion"),
            ])
            .await;
        let records = recorder.records();
        assert_eq!(records.len(), 2);
        assert_eq!(record_count(&records[0].body), 2, "TopicA 那组 2 条");
        assert_eq!(record_count(&records[1].body), 1);
        assert!(records[0].body.contains("TopicA"), "{}", records[0].body);
        assert!(records[1].body.contains("TopicB"), "{}", records[1].body);
        // 每组的 KEYS 只含本组的 msgId
        assert!(records[0].body.contains(MSG_ID_2));
        assert_eq!(records[1].keys, EXPECTED_PUB_KEYS);
    }

    #[tokio::test]
    async fn two_records_concatenate_in_order() {
        // 探针第 17 节的 body / keys
        let (d, recorder) = dispatcher();
        d.inner
            .send_trace_data(vec![
                pub_context("TopicTest", MSG_ID_1, "DefaultRegion"),
                pub_context("TopicTest", MSG_ID_2, "DefaultRegion"),
            ])
            .await;
        let records = recorder.records();
        assert_eq!(records.len(), 1);
        let expected_second = EXPECTED_PUB.replace(MSG_ID_1, MSG_ID_2);
        assert_eq!(records[0].body, format!("{EXPECTED_PUB}{expected_second}"));
        assert_eq!(
            records[0].keys,
            format!("{MSG_ID_1} {MSG_ID_2} KeyA KeyB")
        );
    }

    #[tokio::test]
    async fn flush_data_splits_by_max_msg_size() {
        // 探针第 8 节：单条 160 码点
        //   limit=160 / 3 条 ⇒ [1,1,1]
        //   limit=320 / 5 条 ⇒ [2,2,1]
        //   limit=319 / 5 条 ⇒ [2,2,1]
        //   limit=1_000_000 / 4 条 ⇒ [4]
        let bean_len = EXPECTED_PUB.chars().count();
        assert_eq!(bean_len, 160, "Python 探针：len(trans_data)=160");
        for (limit, n, expect_records) in [(160, 3, vec![1, 1, 1]), (320, 5, vec![2, 2, 1]),
                                           (319, 5, vec![2, 2, 1]), (1_000_000, 4, vec![4])]
        {
            let (d, recorder) = dispatcher();
            d.set_max_msg_size(limit);
            let beans: Vec<TraceTransferBean> = (0..n)
                .map(|i| {
                    let ctx = pub_context(
                        "TopicTest",
                        if i % 2 == 0 { MSG_ID_1 } else { MSG_ID_2 },
                        "DefaultRegion",
                    );
                    TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap_or_default()
                })
                .collect();
            d.inner
                .flush_data(beans, "TopicTest", "RMQ_SYS_TRACE_TOPIC")
                .await;
            let records = recorder.records();
            assert_eq!(
                records.iter().map(|r| record_count(&r.body)).collect::<Vec<_>>(),
                expect_records,
                "limit={limit} n={n}"
            );
            assert!(records.iter().all(|r| r.topic == "RMQ_SYS_TRACE_TOPIC"));
        }
    }

    #[tokio::test]
    async fn flush_data_keys_are_union_of_chunk() {
        // 探针第 8 节 limit=320：前两块 keys 含两个 msgId，第三块只剩后一个
        let (d, recorder) = dispatcher();
        d.set_max_msg_size(320);
        let beans: Vec<TraceTransferBean> = [MSG_ID_2, MSG_ID_1, MSG_ID_2, MSG_ID_1, MSG_ID_2]
            .iter()
            .map(|msg_id| {
                let ctx = pub_context("TopicTest", msg_id, "DefaultRegion");
                TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap_or_default()
            })
            .collect();
        d.inner
            .flush_data(beans, "TopicTest", "RMQ_SYS_TRACE_TOPIC")
            .await;
        let keys: Vec<String> = recorder.records().iter().map(|r| r.keys.clone()).collect();
        assert_eq!(
            keys,
            vec![
                format!("{MSG_ID_1} {MSG_ID_2} KeyA KeyB"),
                format!("{MSG_ID_1} {MSG_ID_2} KeyA KeyB"),
                format!("{MSG_ID_2} KeyA KeyB"),
            ]
        );
    }

    #[tokio::test]
    async fn empty_bean_list_sends_nothing() {
        let (d, recorder) = dispatcher();
        d.inner.flush_data(Vec::new(), "TopicTest", "RMQ_SYS_TRACE_TOPIC").await;
        assert!(recorder.records().is_empty());
    }

    #[tokio::test]
    async fn blank_keys_are_filtered_out() {
        // 探针第 9 节：空串 key 被过滤；全空 ⇒ KEYS 为空串
        let (d, recorder) = dispatcher();
        let mut bean =
            TraceDataEncoder::encoder_from_context_bean(Some(&simple_pub())).unwrap_or_default();
        bean.trans_key.insert(String::new());
        d.inner
            .flush_data(vec![bean], "TopicTest", "RMQ_SYS_TRACE_TOPIC")
            .await;
        assert_eq!(recorder.records()[0].keys, EXPECTED_PUB_KEYS);

        let (d, recorder) = dispatcher();
        let mut bean =
            TraceDataEncoder::encoder_from_context_bean(Some(&simple_pub())).unwrap_or_default();
        bean.trans_key = BTreeSet::new();
        d.inner
            .flush_data(vec![bean], "TopicTest", "RMQ_SYS_TRACE_TOPIC")
            .await;
        assert_eq!(recorder.records()[0].keys, "");
    }

    // ---------------- 发送失败：Python 只记日志，不中断（`:264-265`） ----------------

    #[tokio::test]
    async fn send_failure_is_logged_and_next_chunk_still_sent() {
        // Python `:264-265`：`except Exception: logger.error(...)` —— 一块失败既不
        // 冒到 `_flush_data`，也不阻止后面的块继续发（Java 同理，
        // `AsyncTraceDispatcher.java:406-407` 的 catch 兜住整段选队+发送）。
        // 探针（max_msg_size=1 + send 抛异常）实测 2 条记录 ⇒ 2 次尝试、无异常外抛。
        let (d, recorder) = failing_dispatcher(10);
        d.set_max_msg_size(1); // 每条记录自成一块 ⇒ 两次发送
        let bean_a = TraceDataEncoder::encoder_from_context_bean(Some(&simple_pub()))
            .unwrap_or_default();
        let bean_b = TraceDataEncoder::encoder_from_context_bean(Some(&pub_context(
            "TopicTest",
            MSG_ID_2,
            "DefaultRegion",
        )))
        .unwrap_or_default();
        d.inner
            .flush_data(vec![bean_a, bean_b], "TopicTest", "RMQ_SYS_TRACE_TOPIC")
            .await;
        let records = recorder.records();
        assert_eq!(records.len(), 2, "两块都试过了");
        assert!(
            records.iter().all(|r| r.path == "send"),
            "单测里没有路由 ⇒ broker_set 为空，走普通发送（Python `:259-260`）"
        );
    }

    #[tokio::test]
    async fn failing_send_does_not_block_shutdown() {
        // 队列照样排空、内部生产者照样关（Python `shutdown` 不看发送结果，`:144-162`；
        // 探针 batch_num=1 + send 抛异常：qsize 归 0，2 条都试过）
        let (d, recorder) = failing_dispatcher(1);
        d.start("127.0.0.1:9876", None).await.unwrap();
        d.set_last_flush_time(current_time_millis());
        d.append(simple_pub());
        d.append(pub_context("TopicTest", MSG_ID_2, "DefaultRegion"));
        d.shutdown_gracefully().await;
        assert_eq!(d.queue_size(), 0);
        assert_eq!(recorder.records().len(), 2);
        assert_eq!(recorder.shutdowns.load(Ordering::Acquire), 1);
        assert!(d.is_stopped());
    }

    // ---------------- broker 集合选队（探针第 10 节） ----------------

    #[tokio::test]
    async fn broker_set_round_robin_matches_python() {
        let (d, recorder) = dispatcher();
        recorder.set_queues(vec![
            MessageQueue::new("RMQ_SYS_TRACE_TOPIC", "broker-a", 0),
            MessageQueue::new("RMQ_SYS_TRACE_TOPIC", "broker-b", 3),
            MessageQueue::new("RMQ_SYS_TRACE_TOPIC", "broker-a", 1),
        ]);
        for msg_id in [MSG_ID_1, MSG_ID_2, MSG_ID_1] {
            d.inner
                .send_trace_data(vec![pub_context("TopicTest", msg_id, "DefaultRegion")])
                .await;
        }
        let records = recorder.records();
        assert_eq!(records.len(), 3);
        assert!(
            records.iter().all(|r| r.path == "send_to_queue"),
            "有路由时走 selector 路径"
        );
        let selected: Vec<(String, i32)> = records
            .iter()
            .map(|r| {
                let q = r.queue.clone().unwrap_or_default();
                (q.broker_name, q.queue_id)
            })
            .collect();
        assert_eq!(
            selected,
            vec![
                ("broker-a".to_string(), 0),
                ("broker-b".to_string(), 3),
                ("broker-a".to_string(), 1),
            ]
        );
        // 计数器在 Python 里从 0 开始（itertools.count(0)，:89）
        assert_eq!(d.inner.send_which_queue.load(Ordering::Acquire), 3);
        // 两次路由查询：一次算 broker 集合、一次给 selector 选队（Python 同样查两次）
        assert_eq!(recorder.publish_calls.load(Ordering::Acquire), 6);
    }

    #[tokio::test]
    async fn broker_set_filter_only_touches_listed_brokers() {
        let (d, _) = dispatcher();
        let queues = vec![
            MessageQueue::new("T", "broker-a", 0),
            MessageQueue::new("T", "broker-b", 7),
        ];
        let selector = BrokerSetSelector {
            counter: Arc::clone(&d.inner.send_which_queue),
        };
        let only_a: HashSet<String> = ["broker-a".to_string()].into_iter().collect();
        for _ in 0..4 {
            let q = selector.select(&queues, &only_a).unwrap_or_default();
            assert_eq!(q.broker_name, "broker-a");
            assert_eq!(q.queue_id, 0);
        }
        // Python `:57-60`：集合与路由无交集 ⇒ 退化全量轮询（Java 会越界抛异常）
        let none: HashSet<String> = ["broker-z".to_string()].into_iter().collect();
        let mut seen = Vec::new();
        for _ in 0..4 {
            seen.push(
                selector
                    .select(&queues, &none)
                    .unwrap_or_else(|| MessageQueue::new("T", "none", -1))
                    .broker_name,
            );
        }
        // 计数器已被上面用掉 4 次 ⇒ 4 % 2 == 0，从路由列表第一个开始
        // （探针实测：交集命中 4 次全是 Q(T,broker-a,0)，退化后依次
        // Q(T,broker-a,0)、Q(T,broker-b,7)、Q(T,broker-a,0)、Q(T,broker-b,7)）
        assert_eq!(seen, vec!["broker-a", "broker-b", "broker-a", "broker-b"]);
        assert!(selector.select(&[], &none).is_none());
    }

    // ---------------- 攒批 / 刷写（探针第 12、13 节） ----------------

    #[tokio::test]
    async fn below_batch_and_interval_flushes_nothing() {
        let (d, recorder) = dispatcher_with(10, None);
        for _ in 0..5 {
            assert!(d.append(simple_pub()));
        }
        assert_eq!(d.inner.flush_trace_context(false), 0);
        assert_eq!(d.queue_size(), 5);
        assert!(recorder.records().is_empty());
    }

    #[tokio::test]
    async fn interval_overrun_triggers_flush() {
        let (d, recorder) = dispatcher_with(10, None);
        for _ in 0..5 {
            d.append(simple_pub());
        }
        d.set_last_flush_time(current_time_millis() - 6_000);
        assert_eq!(d.inner.flush_trace_context(false), 5);
        assert_eq!(d.queue_size(), 0);
        // 刷写后 last_flush_time 被更新（Python `:210`）
        assert!(current_time_millis() - d.last_flush_time() < 5_000);
        d.flush_and_wait().await;
        assert_eq!(record_count(&recorder.records()[0].body), 5);
    }

    #[tokio::test]
    async fn batch_size_caps_drained_contexts() {
        // 探针第 12 节：batch_num=2 / 5 条，force 只取 2 条；连续 flush 依次 2/2/1
        let (d, _) = dispatcher_with(2, None);
        for _ in 0..5 {
            d.append(simple_pub());
        }
        assert_eq!(d.inner.flush_trace_context(true), 2);
        assert_eq!(d.queue_size(), 3);
        assert_eq!(d.inner.flush_trace_context(true), 2);
        assert_eq!(d.queue_size(), 1);
        assert_eq!(d.inner.flush_trace_context(true), 1);
        assert_eq!(d.queue_size(), 0);
        // 空队列：flush_trace_context 直接返回 0（Python 在这种情形睡 5ms）
        assert_eq!(d.inner.flush_trace_context(true), 0);
        assert_eq!(d.inner.flush_trace_context(false), 0);
    }

    #[tokio::test]
    async fn flush_drains_queue_in_batch_sizes() {
        // 探针第 12 节：flush() 排空 batch_num=2 / 7 条
        let (d, recorder) = dispatcher_with(2, None);
        for _ in 0..7 {
            d.append(simple_pub());
        }
        d.flush();
        assert_eq!(d.queue_size(), 0);
        d.flush_and_wait().await;
        let records = recorder.records();
        assert_eq!(records.len(), 4, "2+2+2+1 四块");
        assert_eq!(
            records.iter().map(|r| record_count(&r.body)).collect::<Vec<_>>(),
            vec![2, 2, 2, 1]
        );
    }

    #[test]
    fn empty_context_list_is_not_submitted() {
        // Python `:208-209`
        let (d, recorder) = dispatcher();
        d.inner.async_send_trace_message(Vec::new());
        assert_eq!(recorder.publish_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn submit_without_runtime_only_logs() {
        // 模块差异 4：不在运行时上下文里 submit ⇒ 批次丢弃并记 ERROR（不 panic）
        let (d, recorder) = dispatcher_with(1, None);
        d.append(simple_pub());
        assert!(d.inner.submit(vec![simple_pub()]).is_err());
        d.flush();
        assert_eq!(d.queue_size(), 0, "Python 也是先出队再提交");
        assert!(recorder.records().is_empty());
    }

    #[test]
    fn zero_batch_num_does_not_spin_forever() {
        // 模块差异 10
        let (d, _) = dispatcher_with(0, None);
        d.append(simple_pub());
        d.flush();
        assert_eq!(d.queue_size(), 1);
    }

    // ---------------- 生命周期（探针第 14、15 节） ----------------

    #[tokio::test]
    async fn start_configures_and_starts_inner_producer() {
        let (d, recorder) = dispatcher();
        d.start("127.0.0.1:9876;127.0.0.2:9876", Some(AccessChannel::Cloud))
            .await
            .unwrap();
        assert_eq!(
            recorder.setters(),
            vec![
                "send_msg_timeout=5000",
                "max_message_size=128000",
                "enable_trace=false",
                "namesrv=127.0.0.1:9876;127.0.0.2:9876",
                "instance=PID_CLIENT_INNER_TRACE_PRODUCER_127.0.0.1:9876;127.0.0.2:9876",
                "enable_trace=false",
            ]
        );
        assert_eq!(recorder.started.load(Ordering::Acquire), 1);
        assert!(d.is_started());
        assert!(!d.is_stopped());
        assert_eq!(d.access_channel(), AccessChannel::Cloud);
    }

    #[tokio::test]
    async fn second_start_keeps_producer_but_resets_access_channel() {
        // 探针第 14 节：`is_started` 后不再设 namesrv/instance、不再 start；
        // `access_channel or LOCAL` ⇒ 传 None 会把通道退回 LOCAL
        let (d, recorder) = dispatcher();
        d.start("127.0.0.1:9876", Some(AccessChannel::Cloud)).await.unwrap();
        d.start("127.0.0.1:9876", None).await.unwrap();
        assert_eq!(recorder.started.load(Ordering::Acquire), 1);
        assert_eq!(
            recorder
                .setters()
                .iter()
                .filter(|s| s.starts_with("namesrv="))
                .count(),
            1
        );
        assert_eq!(d.access_channel(), AccessChannel::Local);
    }

    #[tokio::test]
    async fn start_worker_flushes_batched_contexts() {
        // 后台循环：攒够 batch_num 就发（Python `_async_run` + `:194`）
        let (d, recorder) = dispatcher_with(2, None);
        d.start("127.0.0.1:9876", None).await.unwrap();
        d.append(simple_pub());
        d.append(pub_context("TopicTest", MSG_ID_2, "DefaultRegion"));
        for _ in 0..100 {
            if !recorder.records().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let records = recorder.records();
        assert_eq!(records.len(), 1, "一批两条：{records:?}");
        assert_eq!(record_count(&records[0].body), 2);
        TraceDispatcher::shutdown(&d);
    }

    #[tokio::test]
    async fn shutdown_flushes_queue_and_stops_producer() {
        // 探针第 15 节
        let (d, recorder) = dispatcher_with(20, None);
        d.append(simple_pub());
        d.append(pub_context("TopicTest", MSG_ID_2, "DefaultRegion"));
        d.start("127.0.0.1:9876", None).await.unwrap();
        // start 之后 start_worker 会把队列里的东西带走，这里重新灌满再关
        d.set_last_flush_time(current_time_millis());
        d.append(simple_pub());
        TraceDispatcher::shutdown(&d);
        assert_eq!(d.queue_size(), 0);
        assert!(d.is_stopped());
        assert_eq!(recorder.shutdowns.load(Ordering::Acquire), 1);
        // Python `:144-162` 不复位 is_started
        assert!(d.is_started());
        d.flush_and_wait().await;
        assert!(!recorder.records().is_empty());
    }

    #[tokio::test]
    async fn shutdown_without_start_does_not_stop_producer() {
        // Python `:153-157`：`if self.is_started`
        let (d, recorder) = dispatcher();
        d.append(simple_pub());
        TraceDispatcher::shutdown(&d);
        assert_eq!(recorder.shutdowns.load(Ordering::Acquire), 0);
        assert_eq!(d.queue_size(), 0);
        assert!(d.is_stopped());
    }

    #[tokio::test]
    async fn shutdown_gracefully_waits_for_inflight_sends() {
        let (d, recorder) = dispatcher_with(20, None);
        d.start("127.0.0.1:9876", None).await.unwrap();
        d.append(simple_pub());
        d.shutdown_gracefully().await;
        assert_eq!(recorder.records().len(), 1);
        assert_eq!(recorder.records()[0].keys, EXPECTED_PUB_KEYS);
        assert!(d.is_stopped());
    }

    #[tokio::test]
    async fn trait_start_requires_and_uses_runtime() {
        // 模块差异 4：同步入口把整段 start 丢进 tokio 任务
        let (d, recorder) = dispatcher();
        TraceDispatcher::start(&d, "127.0.0.1:9876").unwrap();
        for _ in 0..100 {
            if d.is_started() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(d.is_started());
        assert_eq!(recorder.started.load(Ordering::Acquire), 1);
        assert_eq!(d.access_channel(), AccessChannel::Local);
        TraceDispatcher::shutdown(&d);
    }

    #[tokio::test]
    async fn start_failure_keeps_dispatcher_unstarted() {
        struct FailingProducer;
        impl TraceProducer for FailingProducer {
            fn set_name_server_addr(&self, _: &str) {}
            fn set_instance_name(&self, _: &str) {}
            fn set_send_msg_timeout(&self, _: i64) {}
            fn set_max_message_size(&self, _: usize) {}
            fn set_enable_trace(&self, _: bool) {}
            fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>> {
                Box::pin(async { Err(Error::client("producer boom")) })
            }
            fn shutdown(&self) {}
            fn send(self: Arc<Self>, _: Message, _: i64) -> TraceProducerFuture<Result<()>> {
                Box::pin(async { Ok(()) })
            }
            fn send_to_queue(
                self: Arc<Self>,
                _: Message,
                _: MessageQueue,
                _: i64,
            ) -> TraceProducerFuture<Result<()>> {
                Box::pin(async { Ok(()) })
            }
            fn publish_queues(self: Arc<Self>, _: String) -> TraceProducerFuture<Result<Vec<MessageQueue>>> {
                Box::pin(async { Ok(Vec::new()) })
            }
        }
        let d = AsyncTraceDispatcher::new(
            "GID_test",
            TraceDispatcherType::Produce,
            Arc::new(FailingProducer),
        );
        assert!(d.start("127.0.0.1:9876", None).await.is_err());
        assert!(!d.is_started());
        assert!(!d.is_stopped());
    }

    #[tokio::test]
    async fn disabled_producer_path_only_logs() {
        // 模块差异 1：未注入生产者时整条链路只记日志，不影响业务
        let d = AsyncTraceDispatcher::with_config(
            "GID_test",
            TraceDispatcherType::Consume,
            TraceDispatcherConfig {
                batch_num: 1,
                ..Default::default()
            },
        );
        assert!(d.start("127.0.0.1:9876", None).await.is_err());
        d.append(simple_pub());
        d.flush_and_wait().await;
        assert_eq!(d.queue_size(), 0);
    }

    // ---------------- _client_id（探针第 16 节） ----------------

    #[tokio::test]
    async fn client_id_prefers_host_producer() {
        struct Host(FakeClientId);
        struct FakeClientId(&'static str);
        impl TraceHost for Host {
            fn client_id(&self) -> String {
                self.0 .0.to_string()
            }
        }
        let (d, _) = dispatcher();
        assert_eq!(TraceReportSink::client_id(&d), "");
        d.set_host_consumer(Arc::new(Host(FakeClientId("consumer-id"))));
        assert_eq!(TraceReportSink::client_id(&d), "consumer-id");
        d.set_host_producer(Arc::new(Host(FakeClientId("producer-id"))));
        assert_eq!(TraceReportSink::client_id(&d), "producer-id");
        // Python `:123-124`：宿主存在但 clientId 为空 ⇒ 返回空串，不再回落到 consumer
        d.set_host_producer(Arc::new(Host(FakeClientId(""))));
        assert_eq!(TraceReportSink::client_id(&d), "");
        // MQClientInstance 直接当宿主（模块差异 16）
        let client = MQClientInstance::new("10.0.0.1@inst", vec!["127.0.0.1:9876".to_string()]);
        d.set_host_producer(Arc::new(client));
        assert_eq!(TraceReportSink::client_id(&d), "10.0.0.1@inst");
    }

    // ---------------- 其它 ----------------

    #[test]
    fn trace_dispatcher_type_names() {
        assert_eq!(TraceDispatcherType::Produce.name(), "PRODUCE");
        assert_eq!(TraceDispatcherType::Consume.name(), "CONSUME");
        assert_eq!(TraceDispatcherType::Consume.to_string(), "CONSUME");
    }

    #[test]
    fn content_splitor_stays_between_topic_and_trace_topic() {
        // Python `:227`：key = topic + CONTENT_SPLITOR + trace_topic
        let key = format!("TopicTest{SOH}RMQ_SYS_TRACE_TOPIC");
        let (topic, trace_topic) = key.split_once(SOH).unwrap_or_default();
        assert_eq!((topic, trace_topic), ("TopicTest", "RMQ_SYS_TRACE_TOPIC"));
    }

    #[test]
    fn config_debug_does_not_leak_producer() {
        let cfg = TraceDispatcherConfig::default();
        let text = format!("{cfg:?}");
        assert!(text.contains("producer: \"none\""), "{text}");
        assert!(text.contains("max_msg_size: 128000"), "{text}");
    }
}
