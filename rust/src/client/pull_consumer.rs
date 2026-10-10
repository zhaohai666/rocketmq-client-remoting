//! 手动拉取消费者（对应 Python `client/consumer.py` 的
//! `DefaultMQPullConsumer`（:2005-2216）与 `DefaultLitePullConsumer`
//! （:2218-2716））。
//!
//! 两者都是「调用方拿位点」的消费者，区别只在于谁去拉：
//!
//! * [`DefaultMQPullConsumer`]：纯 RPC 门面。调用方自己 `pull(mq, offset)` 逐队列拉、
//!   自己 `update_consume_offset` 提交位点；没有本地缓冲、没有后台线程、不做重平衡。
//! * [`DefaultLitePullConsumer`]：`subscribe(..)`/`assign(..)` + 后台拉取线程把消息灌进
//!   本地缓冲 + `poll(..)` 取批；位点默认 auto-commit。
//!
//! # 与 Python 参考实现的对应关系
//!
//! Python 用 `threading.Thread` + `Condition` 跑 lite 的心跳与拉取循环；这里换成
//! tokio 任务 + [`tokio::sync::Notify`]，行为口径不变。两条刻意差别：
//!
//! 1. **RPC 全部 `async`**：Python 的 `pull`/`commit`/`seek` 是同步阻塞调用，这里
//!    是 `async fn`（与 [`crate::client::consumer`] 模块头差异 5 同因）。
//!    [`DefaultLitePullConsumer::shutdown`] 仍是同步的（与生产者/推送消费者一致），
//!    退出时的最后一次提交被派发到运行时上执行。
//! 2. **队列顺序确定化**：Python 的 `_assigned` 是 `set`（遍历序不稳定），这里用
//!    `BTreeMap<key, MessageQueue>`，后台拉取按「topic+brokerName+queueId」轮转；
//!    同时重平衡前对 `mqAll`/`cidAll` 排序（与 Java
//!    `RebalanceImpl#rebalanceByTopic` 和本项目推送消费者一致，Python 的 lite 路径
//!    漏了这一步 —— 单实例看不出来，多实例会因顺序不同算出不同分配）。
//!
//! # 与 Java 5.5.1 的有意偏离
//!
//! 1. **`pull()` 不做客户端二次 tag 过滤**。Java
//!    `DefaultMQPullConsumerImpl#pullSyncImpl` 会把 `FilterAPI.buildSubscriptionData`
//!    造出的 `subscriptionData` 交给 `PullAPIWrapper#processPullResult`，后者在
//!    `!tagsSet.isEmpty()` 时按字符串再筛一遍（:112-121）。Python/cpp/csharp 三版
//!    都只跑过滤钩子（`consumer.py:2095-2106` 的注释把这件事说成 Java 行为，其实
//!    不成立）。差别只在 broker 侧 tag **哈希**碰撞时才会显现（碰撞消息 Java 丢、
//!    本版留），这里保持与四门语言一致的口径，不改行为、只在此处记账。
//! 2. **不做 Java 的 `subscriptionAutomatically`**。Java 的拉取消费者会
//!    `registerConsumer` 进 `MQClientInstance`（`DefaultMQPullConsumerImpl:746`），
//!    由实例的心跳周期任务发出消费组心跳；本移植的实例心跳任务只遍历
//!    `consumer_table`（推送消费者专属，拉模式消费者接不了 broker 的 220/221/307/309
//!    反向请求），所以**改由消费者自己起心跳循环**（与 Python/C++/C# 四版同构，
//!    #98 补的缺口）：`start()` 刷一遍 registerTopics 的路由，
//!    同步发一轮 `consumeType=CONSUME_ACTIVELY` 的 ConsumerData，之后按
//!    `heartbeat_broker_interval_millis` 周期重发；`shutdown()` 发 35 注销。
//!    [`DefaultMQPullConsumer::register_topics`] 就是这份心跳的订阅集来源
//!    （Java `subscriptions():357-385`）。
//! 3. **`message_queue_lists` 字段不移植**：Python/cpp/csharp 里都是纯声明、零读写的
//!    死字段（Java 也只有配合 `AllocateMessageQueueByConfig` 才用），不搬进 Rust。
//! 4. **消息回投失败会抛**：Java `DefaultMQPullConsumerImpl:666` 在回投失败时吞掉异常、
//!    改用内部生产者把消息直接发进 `%RETRY%group`；本实现按 Python 同口径直接返回错误
//!    （见 [`DefaultMQPullConsumer::send_message_back`]）。
//! 5. **`MessageQueueListener` 按 Java 签名回调**：Python 的重平衡用两个实参调
//!    三参方法（`consumer.py:2705`），异常被外层 `except Exception: pass` 吞掉 ⇒
//!    监听器在 Python 里**从未真正触发过**；cpp 传的是「全部订阅队列 + 新分配」两参。
//!    这里回调 `(topic, mqAll, mqDivided)`（Java `RebalanceImpl#messageQueueChanged`），
//!    与本项目已有的 trait 形状一致。

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

use crate::client::allocate_strategy::{
    AllocateMessageQueueAveragely, AllocateMessageQueueStrategy,
};
use crate::client::consumer::{
    client_side_tag_filter, consume_timestamp_millis, default_consume_timestamp, mq_key,
    mq_sort_key, sort_mqs, MessageQueueListener, MessageSelector, DEFAULT_INSTANCE_NAME,
};
use crate::client::hook::{FilterMessageHook, FilterMessageHookList};
use crate::client::mq_client::{MQClientInstance, MQClientInstanceConfig};
use crate::client::result::{PullResult, PullStatus};
use crate::client::shutdown::{run_finalize_blocking, SHUTDOWN_FINALIZE_BUDGET};
use crate::client::top_addressing::DefaultTopAddressing;
use crate::common::message::{MessageExt, MessageQueue};
use crate::common::mix_all::MixAll;
use crate::common::sysflag::PullSysFlag;
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};
use crate::remoting::protocol::heartbeat::{
    ConsumeFromWhere, ConsumeType, ConsumerData, FilterAPI, HeartbeatData, MessageModel,
    SubscriptionData,
};
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::remoting::rpchook::RPCHook;
use crate::{bail, rmq_debug, rmq_error, rmq_warn};

use crate::client::consumer::ExpressionType;
use crate::client::validators;

/// Python 两个消费者共用的默认值：`brokerSuspendMaxTimeMillis` = 20000。
pub const DEFAULT_BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 20_000;
/// Python `consumerTimeoutMillisWhenSuspend` = 30000（长轮询时的请求超时）。
pub const DEFAULT_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: i64 = 30_000;
/// Python `DefaultMQPullConsumer.consumerPullTimeoutMillis` = 10000。
pub const DEFAULT_CONSUMER_PULL_TIMEOUT_MILLIS: i64 = 10_000;
/// Python `pull()` 里写死的 `suspendTimeoutMillis`（短轮询用不到，但报文照发）。
const PULL_SUSPEND_TIMEOUT_MILLIS: i64 = 15_000;
/// Python lite `_pull_one` 写死的请求超时。
const LITE_PULL_TIMEOUT_MILLIS: i64 = 30_000;
/// Python lite 心跳循环间隔（`for _ in range(50): sleep(0.1)`）。
const LITE_HEARTBEAT_INTERVAL_MILLIS: u64 = 5_000;
/// Python lite 心跳 RPC 超时。
const LITE_HEARTBEAT_TIMEOUT_MILLIS: i64 = 5_000;
/// Python lite 重平衡最小间隔（subscribe 模式）。
const LITE_REBALANCE_INTERVAL_MILLIS: i64 = 1_000;
/// Python `poll()` 单次 drain 上限（`while ... len(out) < 1024`）。
pub const MAX_POLL_BATCH_SIZE: usize = 1024;
/// Python lite 心跳的 RPC 超时同上，此处仅为可读性命名。
const LITE_PULL_RPC_TIMEOUT_MILLIS: i64 = 5_000;
/// Java `startScheduleTask` 的首查延迟（`scheduleAtFixedRate(.., 1000 * 10, period)`）。
const LITE_METADATA_FIRST_DELAY_MILLIS: u64 = 10_000;

/// Python 经典拉模式消费者的心跳循环间隔（`consumer.heartbeat_interval_millis`，
/// `consumer.py:870`，默认 30000ms；与 Java 实例级 `sendHeartbeatToAllBrokerWithLock`
/// 的 30s 同量级）。
pub const DEFAULT_HEARTBEAT_BROKER_INTERVAL_MILLIS: u64 = 30_000;
/// 经典拉模式消费者的心跳 RPC 超时（与 lite 的 5000ms、C++/C# 同口径）。
const PULL_HEARTBEAT_TIMEOUT_MILLIS: i64 = 5_000;

/// 取锁（Python 的 `with self._lock`）；中毒时照用，理由同 `consumer::lock`。
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn read_cfg<T: Clone>(cell: &RwLock<T>) -> T {
    cell.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// `sleep ∥ stop`：`true` = 收到停止信号，循环应当退出。
async fn wait_or_stop(rx: &mut watch::Receiver<bool>, millis: u64) -> bool {
    if *rx.borrow_and_update() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(millis)) => false,
        _ = rx.changed() => true,
    }
}

/// Python `_with_namespace`：lite 版用裸 `ns + "%" + topic`，这里统一走
/// [`NamespaceUtil::wrap_namespace`]（Java 口径：system topic 与已带前缀的不重复拼）。
fn with_namespace(namespace: &str, topic: &str) -> String {
    if namespace.is_empty() {
        topic.to_string()
    } else {
        NamespaceUtil::wrap_namespace(namespace, topic)
    }
}

/// 队列集合相等判定，对应 Java
/// `DefaultLitePullConsumerImpl#isSetEqual:1246-1260`：先比数量，再逐个元素查旧集。
/// 队列身份用 [`mq_key`]（`topic+brokerName+queueId`），与本文件其余表同口径。
fn same_queue_set(old: &[MessageQueue], new: &[MessageQueue]) -> bool {
    if old.len() != new.len() {
        return false;
    }
    let old_set: BTreeSet<String> = old.iter().map(mq_key).collect();
    new.iter().all(|mq| old_set.contains(&mq_key(mq)))
}

/// 拉取请求的 sysFlag（Python `pull` = `suspend=False`、`pull_block_if_not_found` =
/// `suspend=True`，两者 `commit_offset` 都是 `False`）。
///
/// ⚠ 短轮询这条**绝不能**置 suspend 位：broker 会在队尾挂到
/// `brokerSuspendMaxTimeMillis`（20s），而客户端 10s 就超时 ——
/// 真机必现 `RemotingTimeoutException`（Python `consumer.py:2168` 的踩坑记录）。
fn pull_sys_flag(block: bool) -> i32 {
    PullSysFlag::build_sys_flag_basic(false, block, true, false)
}

/// lite pull 的 sysFlag：Java `DefaultLitePullConsumerImpl#pullSyncImpl:1058` 的
/// `buildSysFlag(false, block, true, false, /*litePull=*/true)` —— 比经典拉取多一个
/// `FLAG_LITE_PULL_MESSAGE(0x10)`，客户端 API 层（`MQClientAPIImpl#pullMessage:816-820`）
/// 据此把请求码切成 `LITE_PULL_MESSAGE(361)`，broker 侧只对 361 施加
/// `litePullMessageEnable` 开关（`PullMessageProcessor:325`）。
///
/// ⚠ 不要并进 [`pull_sys_flag`]：那条是经典拉取的（:795/:842），多这一位会让普通消费者
/// 也撞上 lite 开关。
fn lite_pull_sys_flag() -> i32 {
    PullSysFlag::build_sys_flag(false, false, true, false, true)
}

// ================================================================ DefaultMQPullConsumer

/// [`DefaultMQPullConsumer`] 的配置（Python `DefaultMQPullConsumer.__init__`
/// 里那批平铺属性，`consumer.py:2059-2081`）。
#[derive(Debug, Clone)]
pub struct PullConsumerConfig {
    /// Python `consumer_group`。
    pub consumer_group: String,
    /// Python `namespace`。
    pub namespace: String,
    /// Python `instance_name`，默认 `"DEFAULT"`。
    pub instance_name: String,
    /// Python `client_id`；`None` 时 `start()` 现造。
    pub client_id: Option<String>,
    /// Java `ClientConfig#unitName`（默认 null）：非空时进 clientId 后缀，
    /// 并作为地址服务器 URL 的 `-<unitName>` 段。
    pub unit_name: Option<String>,
    /// Java `ClientConfig#namespaceV2`（默认 null）：5.x **服务端**命名空间，
    /// 非空时每个请求带 `nsd=true` / `ns=<namespaceV2>` 扩展头（`NamespaceRpcHook`），
    /// 与 v1 `namespace` 的客户端 `%` 前缀机制是两套东西。
    pub namespace_v2: Option<String>,
    /// Java `ClientConfig#unitMode`（默认 false）：随回投/鉴权/消息过滤上线。
    pub unit_mode: bool,
    /// Java `ClientConfig#enableStreamRequestType`：true 时每个请求带 `ReqT=0`，
    /// clientId 末尾多一段 `@STREAM`。
    ///
    /// ⚠ 拉模式默认 **true**：Java `DefaultMQPullConsumer` / `DefaultLitePullConsumer`
    /// 的每个构造函数都置 `enableStreamRequestType = true`（:113/:126 与 :213/:228），
    /// 只有推送消费者和生产者默认 false。
    pub enable_stream_request_type: bool,
    /// Java `ClientConfig#pollNameServerInterval`（默认 30000ms）：在用 topic 的
    /// 路由周期刷新间隔，`start()` 时透传给 `MQClientInstance`。
    pub poll_name_server_interval_millis: u64,
    /// Python `name_server_addrs`。
    pub name_server_addrs: Vec<String>,
    /// Python `tls_enable`：`None` = 交给环境变量 `ROCKETMQ_TLS_ENABLE`
    /// （Python 的拉取消费者没这个属性，这里与推送消费者统一）。
    pub tls_enable: Option<bool>,
    /// Python `message_model`；拉模式只影响 `subscriptionAutomatically`
    /// （本移植版不做注册，见模块头偏离 2），其余行为与它无关。
    pub message_model: String,
    /// Python `broker_suspend_max_time_millis` = 20000：长轮询时下发给 broker 的挂起时长。
    pub broker_suspend_max_time_millis: i64,
    /// Python `consumer_pull_timeout_millis` = 10000：短轮询默认请求超时。
    pub consumer_pull_timeout_millis: i64,
    /// Python `consumer_timeout_millis_when_suspend` = 30000：长轮询请求超时。
    pub consumer_timeout_millis_when_suspend: i64,
    /// Python `heartbeat_enabled`（`consumer.py:869`，默认 `True`）：置 false 后
    /// start 的同步那轮与后台循环都不发心跳（C++/C# 同名开关）。
    pub heartbeat_enabled: bool,
    /// Python `heartbeat_interval_millis`（`consumer.py:870`，默认 30000ms）。
    pub heartbeat_broker_interval_millis: u64,
}

impl Default for PullConsumerConfig {
    fn default() -> PullConsumerConfig {
        PullConsumerConfig {
            consumer_group: MixAll::DEFAULT_CONSUMER_GROUP.to_string(),
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            unit_name: None,
            namespace_v2: None,
            unit_mode: false,
            enable_stream_request_type: true,
            // Java `ClientConfig:58`：pollNameServerInterval = 1000 * 30
            poll_name_server_interval_millis: 30_000,
            client_id: None,
            name_server_addrs: Vec::new(),
            tls_enable: None,
            message_model: MessageModel::CLUSTERING.to_string(),
            broker_suspend_max_time_millis: DEFAULT_BROKER_SUSPEND_MAX_TIME_MILLIS,
            consumer_pull_timeout_millis: DEFAULT_CONSUMER_PULL_TIMEOUT_MILLIS,
            consumer_timeout_millis_when_suspend: DEFAULT_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
            heartbeat_enabled: true,
            heartbeat_broker_interval_millis: DEFAULT_HEARTBEAT_BROKER_INTERVAL_MILLIS,
        }
    }
}

struct PullInner {
    cfg: RwLock<PullConsumerConfig>,
    client: Mutex<Option<MQClientInstance>>,
    started: AtomicBool,
    /// 心跳循环的存活标记（与 lite 的 `running` 同义）。
    running: AtomicBool,
    runtime: OnceLock<tokio::runtime::Handle>,
    /// 后台循环的停止信号（true = 停）。
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// 心跳成功轮数（一轮至少一台 broker 收到才算），真机/离线用例的断言落点。
    heartbeat_count: AtomicUsize,
    filter_hooks: FilterMessageHookList,
    register_topics: Mutex<BTreeSet<String>>,
    listener: Mutex<Option<Arc<dyn MessageQueueListener>>>,
    /// 对应 Java `DefaultMQPullConsumer.allocateMessageQueueStrategy` 的字段初值
    /// （`new AllocateMessageQueueAveragely()`:89）。本端口拉模式不做 rebalance
    /// （见模块头偏离 2），所以它只是配置形状，与 Python/C++/C# 同口径。
    strategy: RwLock<Arc<dyn AllocateMessageQueueStrategy>>,
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
    /// Java `PullAPIWrapper.pullFromWhichNodeTable`：每次拉取回写响应头里的
    /// `suggestWhichBrokerId`，下次拉取按它选主/从。
    pull_from_which_node: Mutex<HashMap<MessageQueue, i64>>,
}

impl Default for PullInner {
    fn default() -> PullInner {
        PullInner {
            cfg: RwLock::new(PullConsumerConfig::default()),
            client: Mutex::new(None),
            started: AtomicBool::new(false),
            running: AtomicBool::new(false),
            runtime: OnceLock::new(),
            stop: watch::channel(false).0,
            tasks: Mutex::new(Vec::new()),
            heartbeat_count: AtomicUsize::new(0),
            filter_hooks: FilterMessageHookList::new(),
            register_topics: Mutex::new(BTreeSet::new()),
            listener: Mutex::new(None),
            strategy: RwLock::new(Arc::new(AllocateMessageQueueAveragely)),
            rpc_hook: RwLock::new(None),
            pull_from_which_node: Mutex::new(HashMap::new()),
        }
    }
}

impl Drop for PullInner {
    /// 忘记 `shutdown()` 也不能把心跳循环留着（同 lite / 推送消费者）。
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
        let _ = self.stop.send(true);
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
    }
}

/// 拉模式消费者（对应 Java `DefaultMQPullConsumer` + `DefaultMQPullConsumerImpl`，
/// 移植自 Python `consumer.DefaultMQPullConsumer`）。
///
/// 用法：`start()` → `fetch_subscribe_message_queues(topic)` 拿队列 → 逐队列
/// `pull(mq, expr, offset, maxNums)` → 自己 `update_consume_offset` 提交 →
/// `shutdown()`。克隆出的副本共享同一份状态。
#[derive(Clone)]
pub struct DefaultMQPullConsumer {
    inner: Arc<PullInner>,
}

impl std::fmt::Debug for DefaultMQPullConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cfg = read_cfg(&self.inner.cfg);
        f.debug_struct("DefaultMQPullConsumer")
            .field("consumer_group", &cfg.consumer_group)
            .field("namespace", &cfg.namespace)
            .field("instance_name", &cfg.instance_name)
            .field("client_id", &cfg.client_id)
            .field("name_server_addrs", &cfg.name_server_addrs)
            .field("message_model", &cfg.message_model)
            .field("started", &self.inner.started.load(Ordering::Acquire))
            .field("register_topics", &lock(&self.inner.register_topics).len())
            .finish()
    }
}

impl DefaultMQPullConsumer {
    /// Python `DefaultMQPullConsumer(consumer_group)`；组名空白报错
    /// （`MQClientException("consumerGroup is empty")`）。
    pub fn new(consumer_group: &str) -> Result<DefaultMQPullConsumer> {
        DefaultMQPullConsumer::with_config(PullConsumerConfig {
            consumer_group: consumer_group.to_string(),
            ..Default::default()
        })
    }

    /// Python `DefaultMQPullConsumer(consumer_group, rpc_hook=..)`。
    pub fn with_rpc_hook(
        consumer_group: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Result<DefaultMQPullConsumer> {
        let consumer = DefaultMQPullConsumer::new(consumer_group)?;
        consumer.set_rpc_hook(rpc_hook);
        Ok(consumer)
    }

    /// 直接以一份完整配置构造（Python 靠逐个赋属性，这里等价）。
    pub fn with_config(cfg: PullConsumerConfig) -> Result<DefaultMQPullConsumer> {
        if cfg.consumer_group.trim().is_empty() {
            bail!("consumerGroup is empty");
        }
        Ok(DefaultMQPullConsumer {
            inner: Arc::new({
                // `PullInner` 有 `Drop`，不能走 `..Default::default()` 的结构体更新语法
                // （同 lite），先整体默认构造再替换 cfg。
                let mut inner = PullInner::default();
                inner.cfg = RwLock::new(cfg);
                inner
            }),
        })
    }

    /// 当前配置快照（Python 直接读属性）。
    pub fn config(&self) -> PullConsumerConfig {
        read_cfg(&self.inner.cfg)
    }

    /// 改配置（Python 的直接赋属性）。
    pub fn update_config(&self, f: impl FnOnce(&mut PullConsumerConfig)) {
        {
            let mut w = self.inner.cfg.write().unwrap_or_else(|e| e.into_inner());
            f(&mut w);
        }
    }

    pub fn consumer_group(&self) -> String {
        read_cfg(&self.inner.cfg).consumer_group
    }

    pub fn client_id(&self) -> String {
        read_cfg(&self.inner.cfg)
            .client_id
            .clone()
            .unwrap_or_default()
    }

    /// Java `ClientConfig#isUnitMode()`（回投/过滤等请求都取这一个值）。
    pub fn unit_mode(&self) -> bool {
        read_cfg(&self.inner.cfg).unit_mode
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    // ---------------- 配置 ----------------

    /// Python `set_namesrv_addr`：分号分隔，逐项 trim、丢空。
    pub fn set_namesrv_addr(&self, addr: &str) {
        let addrs = split_addrs(addr);
        self.update_config(|c| c.name_server_addrs = addrs);
    }

    /// Python `set_name_server_addresses`。
    pub fn set_name_server_addresses(&self, addrs: &[String]) {
        let addrs = addrs.to_vec();
        self.update_config(|c| c.name_server_addrs = addrs);
    }

    /// Python `set_instance_name`。
    pub fn set_instance_name(&self, name: &str) {
        let name = name.to_string();
        self.update_config(|c| c.instance_name = name);
    }

    /// Java `ClientConfig#setUnitName`：`None`/空白等价于不设（拼 clientId 时按 isBlank 判）。
    pub fn set_unit_name(&self, unit_name: Option<&str>) {
        let unit_name = unit_name.map(str::to_string);
        self.update_config(|c| c.unit_name = unit_name);
    }

    /// Java `ClientConfig#setNamespaceV2`：5.x **服务端**命名空间（`ns`/`nsd`
    /// 扩展头，见 `NamespaceRpcHook`）。`None`/空串 = 不设，钩子退化为 no-op。
    /// `start()` 时透传给 `MQClientInstance`，晚于 start 修改不影响已建实例。
    pub fn set_namespace_v2(&self, namespace_v2: Option<&str>) {
        let namespace_v2 = namespace_v2.map(str::to_string);
        self.update_config(|c| c.namespace_v2 = namespace_v2);
    }

    /// Java `ClientConfig#getNamespaceV2`。
    pub fn get_namespace_v2(&self) -> Option<String> {
        self.config().namespace_v2
    }

    /// Java `ClientConfig#setUnitMode`。
    pub fn set_unit_mode(&self, unit_mode: bool) {
        self.update_config(|c| c.unit_mode = unit_mode);
    }

    /// Java `ClientConfig#setEnableStreamRequestType`。
    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.update_config(|c| c.enable_stream_request_type = enable);
    }

    /// Java `ClientConfig#setPollNameServerInterval`。
    pub fn set_poll_name_server_interval_millis(&self, millis: u64) {
        self.update_config(|c| c.poll_name_server_interval_millis = millis);
    }

    /// Python `set_namespace`。
    pub fn set_namespace(&self, namespace: &str) {
        let namespace = namespace.to_string();
        self.update_config(|c| c.namespace = namespace);
    }

    /// Python `set_message_model`。
    pub fn set_message_model(&self, model: &str) {
        let model = model.to_string();
        self.update_config(|c| c.message_model = model);
    }

    /// Python `set_rpc_hook`。
    pub fn set_rpc_hook(&self, hook: Option<Arc<dyn RPCHook>>) {
        *self
            .inner
            .rpc_hook
            .write()
            .unwrap_or_else(|e| e.into_inner()) = hook;
    }

    pub fn set_message_queue_listener(&self, listener: Arc<dyn MessageQueueListener>) {
        *lock(&self.inner.listener) = Some(listener);
    }

    /// 队列分配策略，对应 Java `DefaultMQPullConsumer` 的 getter/setter(:196-202)。
    ///
    /// ⚠ Java 的 checkConfig(:803) 会拒绝 null 策略；Rust 用 `Arc<dyn ...>` 表达
    /// 「一定有策略」，那个分支类型上不可表示，故无对应校验。本端口拉模式不做
    /// rebalance（见模块头偏离 2），策略只作为配置形状保留。
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

    /// 当前队列分配策略（对应 Java `getAllocateMessageQueueStrategy`）。
    pub fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy> {
        self.inner
            .strategy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Java `DefaultMQPullConsumer#registerTopic`：拼命名空间后登记
    /// （也是心跳订阅集的来源，见模块头偏离 2）。
    pub fn register_topics(&self) -> Vec<String> {
        lock(&self.inner.register_topics).iter().cloned().collect()
    }

    /// Java `DefaultMQPullConsumer#registerTopic`：拼命名空间后登记。
    pub fn register_topic(&self, topic: &str) {
        let topic = with_namespace(&self.config().namespace, topic);
        lock(&self.inner.register_topics).insert(topic);
    }

    // ---------------- 投递前过滤钩子 ----------------

    /// Python `register_filter_message_hook`。
    pub fn register_filter_message_hook(&self, hook: Arc<dyn FilterMessageHook>) {
        self.inner.filter_hooks.register(hook);
    }

    /// Python `has_filter_message_hook`。
    pub fn has_filter_message_hook(&self) -> bool {
        self.inner.filter_hooks.has_hooks()
    }

    /// Python `_filter_messages_for_delivery`：拉模式只跑钩子，不做客户端 tag
    /// 过滤（Java 会，见模块头偏离 1）。
    fn filter_messages_for_delivery(
        &self,
        mq: &MessageQueue,
        msgs: Vec<MessageExt>,
    ) -> Vec<MessageExt> {
        let group = self.consumer_group();
        crate::client::consumer::filter_messages_for_delivery(
            &group,
            &self.inner.filter_hooks,
            mq,
            None,
            msgs,
            self.config().unit_mode,
        )
    }

    // ---------------- 生命周期 ----------------

    /// Python `start()`：幂等、必须有 name server，`client_id` 缺省时现造。
    ///
    /// ⚠ `client_id` 的时间戳是**秒级** `instanceName@yyyyMMddHHmmss`
    /// （`consumer.py:2134`），与推送消费者同格式；同一秒内起两个实例会撞 clientId，
    /// 那时 [`MQClientInstance::create_mq_client_instance`] 会复用同一实例 —— 与
    /// Python/Java 同语义（Java 靠 `changeInstanceNameToPID` 规避，本项目四版都没做）。
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
        // 对应 Java DefaultMQPullConsumerImpl.checkConfig(:772)：组名合法性（blank / 120
        // 长度 / 字符表）+ 挡掉 DEFAULT_CONSUMER（共用默认组会混掉订阅关系与位点）。
        // 纯本地校验，排在地址检查之前，失败不碰网络。
        if let Err(e) = validators::check_group(&cfg.consumer_group) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        if cfg.consumer_group == MixAll::DEFAULT_CONSUMER_GROUP {
            self.inner.started.store(false, Ordering::Release);
            return Err(Error::client(
                "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.",
            ));
        }
        if cfg.name_server_addrs.is_empty() && !DefaultTopAddressing::is_configured() {
            self.inner.started.store(false, Ordering::Release);
            bail!("name server address is not set");
        }
        // Java `DefaultMQPullConsumerImpl#start`:712 / `DefaultLitePullConsumerImpl#start`:288：
        // CLUSTERING 才改写 instanceName，clientId 口径是 `ClientConfig#buildMQClientId`
        // 的 `<本机 IP>@<instanceName>`。
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
            c.client_id = Some(client_id.clone());
            c.instance_name = instance_name;
        });

        let instance_cfg = MQClientInstanceConfig {
            tls_enable: cfg.tls_enable,
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
        // 动态 name server：实例可能已从地址服务器拿到地址，回填（Python 同）。
        if cfg.name_server_addrs.is_empty() {
            let addrs = client.name_server_addrs();
            if !addrs.is_empty() {
                self.update_config(|c| c.name_server_addrs = addrs);
            }
        }
        // Java `DefaultMQPullConsumerImpl#start`:746 `registerConsumer`：拉模式消费者
        // 接不了 broker 的反向请求（220/221/307/309），所以只登记组名给实例的关闭
        // 守卫用，不进 `consumer_table`（实例心跳任务因此看不见本组，心跳由本消费者
        // 自己的循环发，见模块头偏离 2）。
        client.register_consumer_group(&cfg.consumer_group);
        *lock(&self.inner.client) = Some(client);
        // 心跳：先刷 registerTopics 的路由（心跳只发给路由表里已知的 broker，没有
        // 地址就发 0 份），再同步发一轮让 broker 立刻认识本组，最后交给后台循环 ——
        // 顺序对齐 Python/C++/C# 的拉模式消费者（也贴合 Java 的
        // registerConsumer:746 → mQClientFactory.start():755 首个心跳周期在 1s 内）。
        self.refresh_route_for_heartbeat().await;
        self.inner.running.store(true, Ordering::Release);
        let _ = self.inner.stop.send(false);
        if cfg.heartbeat_enabled {
            self.send_heartbeat_to_all_broker().await;
        }
        self.spawn_heartbeat_loop();
        Ok(())
    }

    /// Python `shutdown()`；未启动时是 no-op。
    ///
    /// 收尾三步（对齐 Java `DefaultMQPullConsumerImpl.shutdown:689-692`：
    /// `unregisterConsumer` → `mQClientFactory.shutdown()`，中间补 Python/C++/C#
    /// 都有的 35 号注销）：停心跳 → 逐台 broker 发 UNREGISTER_CLIENT(35) → 摘组 → 关实例。
    /// 35 让 broker 的 ConsumerManager 立刻摘掉本组，不必等 ~120s 通道扫描
    /// （Java `MQClientInstance#unregisterClient`，`DefaultMQPullConsumerImpl:691` 走同一入口）。
    /// 35 与关实例挂在运行时上跑，但本方法**阻塞等它落地**（上限
    /// [`SHUTDOWN_FINALIZE_BUDGET`]）；current_thread 运行时里退化为游离任务并告警
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
        // 先停心跳：被 abort 的那一轮可能已经在线上，落回 broker 会把本 clientId
        // 重新塞进 ConsumerManager（与推送消费者 shutdown 同一理由）。
        self.inner.running.store(false, Ordering::Release);
        let _ = self.inner.stop.send(true);
        for task in lock(&self.inner.tasks).drain(..) {
            task.abort();
        }
        let group = self.consumer_group();
        let client_id = self.client_id();
        // 注销要发 RPC —— 与生产者/推送消费者同款：挂到运行时上执行并**有界等待**
        // 落地（`run_finalize_blocking`，「shutdown 后立刻退进程」也发得出去）；
        // 没有运行时则退化为只摘组 + 关实例。
        match self.runtime_handle() {
            Some(handle) => {
                let this = self.clone();
                let finalize = async move {
                    // 锁不能跨 await（MutexGuard 不是 Send）：先把 client 取出来。
                    let client = lock(&this.inner.client).take();
                    if let Some(client) = client {
                        client
                            .unregister_client_all_brokers(
                                &client_id,
                                "",
                                &group,
                                crate::client::mq_client::MQ_CLIENT_API_TIMEOUT_MILLIS,
                            )
                            .await;
                        client.unregister_consumer_group(&group);
                        client.shutdown();
                    }
                };
                let _ = run_finalize_blocking(
                    &handle,
                    "pull consumer shutdown",
                    SHUTDOWN_FINALIZE_BUDGET,
                    finalize,
                );
            }
            None => {
                if let Some(client) = lock(&self.inner.client).take() {
                    client.unregister_consumer_group(&group);
                    client.shutdown();
                }
                rmq_warn!("pull consumer shutdown: no tokio runtime, skip unregister(35)");
            }
        }
    }

    /// Python `_require_client`。
    fn require_client(&self) -> Result<MQClientInstance> {
        if !self.is_started() {
            return Err(Error::client("consumer not started, call start() first"));
        }
        lock(&self.inner.client)
            .clone()
            .ok_or_else(|| Error::client("consumer not started, call start() first"))
    }

    // ---------------- 心跳 ----------------

    /// 心跳成功轮数（一轮至少一台 broker 收到才算；真机/离线用例的断言落点）。
    pub fn heartbeat_count(&self) -> usize {
        self.inner.heartbeat_count.load(Ordering::SeqCst)
    }

    /// Python `_refresh_route_for_heartbeat`（同 lite 版）：心跳只发给「路由表里已知的
    /// broker」，自建实例刚 start 时路由表是空的。先把 registerTopics 的路由拉一遍并
    /// 登记为在用，start() 里那次同步心跳才真正到达 broker。
    ///
    /// Java 侧这条链路是间接的：`DefaultMQPullConsumerImpl.subscriptions()` 返回
    /// registerTopics 构的订阅集 → 实例的 `updateTopicRouteInfoFromNameServer` 周期任务
    /// 据此刷路由 → `brokerAddrTable` 有地址 → 心跳发得出去。本移植的实例心跳任务
    /// 不看拉模式组（见模块头偏离 2），所以这里显式刷一遍。
    async fn refresh_route_for_heartbeat(&self) {
        let Ok(client) = self.require_client() else {
            return;
        };
        let topics: Vec<String> = lock(&self.inner.register_topics).iter().cloned().collect();
        for topic in topics {
            client.register_topic_in_use(&topic);
            if let Err(e) = client.get_topic_publish_info(&topic, false).await {
                rmq_debug!("pull start: refresh route for {topic} failed: {e}");
            }
        }
    }

    /// Python `_send_heartbeat_to_all_broker`（同 lite 版）：向路由里的每台 broker
    /// （**含从节点**）发一份，返回成功台数。
    pub async fn send_heartbeat_to_all_broker(&self) -> usize {
        send_pull_heartbeat(&self.inner).await
    }

    fn spawn_heartbeat_loop(&self) {
        let Some(handle) = self.runtime_handle() else {
            rmq_warn!("pull consumer: no tokio runtime, heartbeat loop disabled");
            return;
        };
        // 握 Weak 而不是 Arc（同 lite）：消费者被丢弃时 Drop 才有机会跑。
        let weak = Arc::downgrade(&self.inner);
        let rx = self.inner.stop.subscribe();
        let task = handle.spawn(async move {
            if let Some(inner) = weak.upgrade() {
                pull_heartbeat_loop(inner, rx).await;
            }
        });
        lock(&self.inner.tasks).push(task);
    }

    fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        if let Some(handle) = self.inner.runtime.get() {
            return Some(handle.clone());
        }
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let _ = self.inner.runtime.set(handle.clone());
        Some(handle)
    }

    // ---------------- 拉取 ----------------

    /// Python `fetch_subscribe_message_queues`：按**订阅信息**列队列。
    ///
    /// Java `DefaultMQPullConsumerImpl:142` 与 push 同源，读的是订阅信息
    /// （读位 + readQueueNums、不筛 master），不是发布信息。
    ///
    /// ⚠ 与 lite 版不同，这里**不拼命名空间**（`consumer.py` 直接把入参
    /// topic 传给路由查询），与 Python 保持逐字一致。
    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = self.require_client()?;
        // Python `list(publish.msg_queue_list)` 的口径：不改队列身份，直接返回。
        Ok(client.get_topic_subscribe_info(topic).await)
    }

    /// Python `fetch_message_queues_in_balance`：本实例「平衡后」应负责的队列
    /// （Java `MQPullConsumer:187`，官方 `example/simple/PullConsumer.java:62` 就靠它
    /// 决定去拉哪些队列）。
    ///
    /// Java（`DefaultMQPullConsumerImpl:120-135`）读的是后台 rebalance 填出来的
    /// `processQueueTable`；本端口拉模式没有那条后台线程（见模块头偏离说明），所以按
    /// `RebalanceImpl.rebalanceByTopic` 的**同一条公式**当场算：BROADCASTING 全量；
    /// CLUSTERING 用订阅信息（读位口径，与 [`Self::fetch_subscribe_message_queues`] 同源）
    /// 作 mqAll、GET_CONSUMER_LIST_BY_GROUP(38) 作 cidAll，再交给本实例配置的分配策略取
    /// 自己那一份。公式与 push 消费者的 rebalance 共用一份口径，两处一旦分叉，同一队列
    /// 会被两个实例同时认领。
    ///
    /// 查不到路由/消费者列表时**保留现有分配**：退回本地 `pull_from_which_node`
    /// （Java 同名表 `pullFromWhichNodeTable`）的键集。绝不回退成「独占全部队列」——
    /// 那会让同组多实例互相重复消费。注意「算出来确实是空」（队列全分给了同组别人）
    /// 与「算不动」是两回事，前者如实返回空集（Java 的表里那时就是空的）。
    pub async fn fetch_message_queues_in_balance(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = self.require_client()?; // Java isRunning()：未启动直接报错
        let cfg = self.config();
        let mut pulled: Vec<MessageQueue> = lock(&self.inner.pull_from_which_node)
            .keys()
            .filter(|mq| mq.topic == topic)
            .cloned()
            .collect();
        sort_mqs(&mut pulled);

        if cfg.message_model == MessageModel::BROADCASTING {
            // Java rebalanceByTopic 对 BROADCASTING 不查消费者列表、全量分配。
            let mut all = client.get_topic_subscribe_info(topic).await;
            if all.is_empty() {
                return Ok(pulled);
            }
            sort_mqs(&mut all);
            return Ok(all);
        }

        let mut mq_all = client.get_topic_subscribe_info(topic).await;
        // Java :128-131 逐个比对表键的 topic；别让策略的意外返回值把别的 topic
        // 混进调用方的拉取循环。
        mq_all.retain(|mq| mq.topic == topic);
        sort_mqs(&mut mq_all);
        let cid_all = client
            .get_consumer_id_list_by_group(topic, &cfg.consumer_group, 5000)
            .await;

        let mut allocated: Option<Vec<MessageQueue>> = None;
        if !mq_all.is_empty() {
            if let Some(cids) = cid_all.filter(|c| !c.is_empty()) {
                let mut sorted_cids = cids;
                sorted_cids.sort();
                // 对应 Java RebalanceImpl.rebalanceByTopic 的 catch (Throwable)：策略异常
                // 只记日志、本轮保持现有分配，绝不能把队列撤走。
                match self.allocate_message_queue_strategy().allocate(
                    &cfg.consumer_group,
                    cfg.client_id.as_deref().unwrap_or(""),
                    &mq_all,
                    &sorted_cids,
                ) {
                    Ok(got) => allocated = Some(got),
                    Err(e) => rmq_debug!("fetchMessageQueuesInBalance allocate failed: {e}"),
                }
            }
        }
        let out = match allocated {
            Some(v) => v,
            None => {
                rmq_debug!(
                    "fetchMessageQueuesInBalance: no route/consumer list for {}/{}, keep current assignment",
                    cfg.consumer_group,
                    topic
                );
                pulled
            }
        };
        let mut out: Vec<MessageQueue> = out.into_iter().filter(|mq| mq.topic == topic).collect();
        sort_mqs(&mut out);
        Ok(out)
    }

    /// Python `pull(mq, sub_expression="*", offset=0, max_nums=32, timeout=None)`：
    /// **短轮询**（`suspend=False`），位点由调用方 `update_consume_offset` 提交。
    pub async fn pull(
        &self,
        mq: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_nums: i32,
        timeout_millis: Option<i64>,
    ) -> Result<PullResult> {
        let client = self.require_client()?;
        let cfg = self.config();
        let timeout = timeout_millis.unwrap_or(cfg.consumer_pull_timeout_millis);
        let sub = FilterAPI::build_subscription_data(&mq.topic, Some(sub_expression))?;
        let broker_id = lock(&self.inner.pull_from_which_node)
            .get(mq)
            .copied()
            .unwrap_or(MixAll::MASTER_ID as i64);
        let result = client
            .pull_message(
                &cfg.consumer_group,
                mq,
                offset,
                max_nums,
                pull_sys_flag(false),
                0,
                sub_expression_of(&sub),
                // Java：TAG 类型时 subVersion 传 0（`isTagType ? 0L : subVersion`）
                0,
                ExpressionType::TAG,
                timeout,
                -1,
                PULL_SUSPEND_TIMEOUT_MILLIS,
                None,
                0,
                Some(broker_id),
            )
            .await?;
        lock(&self.inner.pull_from_which_node).insert(
            mq.clone(),
            result
                .suggest_which_broker_id
                .unwrap_or(MixAll::MASTER_ID as i64),
        );
        Ok(self.apply_delivery_filter(mq, result))
    }

    /// Python `pull_block_if_not_found`：长轮询（`suspend=True`，超时用
    /// `consumer_timeout_millis_when_suspend`，挂起时长用
    /// `broker_suspend_max_time_millis`）。
    ///
    /// ⚠ 请求超时必须明显大于挂起时长，否则消息恰好在挂起末尾到达时会稳定超时
    /// （Python 默认 30000 > 20000 正是这个比例）。
    pub async fn pull_block_if_not_found(
        &self,
        mq: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_nums: i32,
    ) -> Result<PullResult> {
        let client = self.require_client()?;
        let cfg = self.config();
        let sub = FilterAPI::build_subscription_data(&mq.topic, Some(sub_expression))?;
        let broker_id = lock(&self.inner.pull_from_which_node)
            .get(mq)
            .copied()
            .unwrap_or(MixAll::MASTER_ID as i64);
        let result = client
            .pull_message(
                &cfg.consumer_group,
                mq,
                offset,
                max_nums,
                pull_sys_flag(true),
                0,
                sub_expression_of(&sub),
                0,
                ExpressionType::TAG,
                cfg.consumer_timeout_millis_when_suspend,
                -1,
                cfg.broker_suspend_max_time_millis,
                None,
                0,
                Some(broker_id),
            )
            .await?;
        lock(&self.inner.pull_from_which_node).insert(
            mq.clone(),
            result
                .suggest_which_broker_id
                .unwrap_or(MixAll::MASTER_ID as i64),
        );
        Ok(self.apply_delivery_filter(mq, result))
    }

    /// 只在 FOUND 且非空时跑投递前过滤（Python 的 `if result.status == FOUND and ...`）。
    fn apply_delivery_filter(&self, mq: &MessageQueue, mut result: PullResult) -> PullResult {
        if result.status == PullStatus::Found && !result.msg_found_list.is_empty() {
            result.msg_found_list =
                self.filter_messages_for_delivery(mq, std::mem::take(&mut result.msg_found_list));
        }
        result
    }

    // ---------------- 位点管理 ----------------

    /// Python `fetch_consume_offset`：broker 无记录 ⇒ `None`
    /// （`set_zero_if_not_found=false`，与 Java `queryConsumerOffset` 的默认一致）。
    pub async fn fetch_consume_offset(&self, mq: &MessageQueue) -> Result<Option<i64>> {
        let client = self.require_client()?;
        let group = self.consumer_group();
        client
            .query_consumer_offset(&group, mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None, false)
            .await
    }

    /// Python `update_consume_offset`。
    pub async fn update_consume_offset(&self, mq: &MessageQueue, offset: i64) -> Result<()> {
        let client = self.require_client()?;
        let group = self.consumer_group();
        client
            .update_consumer_offset(&group, mq, offset, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    /// Python `search_offset`。
    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: i64) -> Result<i64> {
        let client = self.require_client()?;
        client
            .search_offset_by_timestamp(mq, timestamp, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    /// Python `max_offset`。
    pub async fn max_offset(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client
            .get_max_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    /// Python `min_offset`。
    pub async fn min_offset(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client
            .get_min_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    /// Python `earliest_msg_store_time`（GET_EARLIEST_MSG_STORETIME=32）。
    ///
    /// Python 把这条 RPC 内联在消费者里，这里走
    /// [`MQClientInstance::get_earliest_msg_store_time`]（Java 放在
    /// `MQClientAPIImpl`，与其它 offset RPC 同一层）。
    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client
            .get_earliest_msg_store_time(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    /// 消息回投（Python `send_message_back`，Java `sendMessageBack`）。
    ///
    /// 两个真机踩过的点，照 Python 的注释：
    /// 1. 地址靠 `find_broker_address_in_publish(msg.broker_name)` 反查**发布地址表**
    ///    （只认 master），所以调用方必须先用本 consumer 访问过该 topic
    ///    （Java DefaultMQPullConsumerImpl:654 走的也是 findBrokerAddressInPublish）。
    /// 2. 与 Java 的有意差异：Java 失败时吞异常、改由内部生产者把消息直接发进
    ///    `%RETRY%group`；这里直接返回错误，不换路径静默重发（模块头偏离 4）。
    ///
    /// `max_reconsume_times` 不下发（`None`）：让 broker 按订阅组的 `retryMaxTimes`
    /// 判定重试上限，超限才转 `%DLQ%`。Python 在这里传 `-1`，但它自己的注释说的是
    /// 「交给 broker 决定」——真按 Java 拉模式把 -1 带上，broker 会无条件采纳并让消息
    /// 直接进 `%DLQ%`（详见 [`MQClientInstance::consumer_send_msg_back`]）。
    pub async fn send_message_back(&self, msg: &MessageExt, delay_level: i32) -> Result<()> {
        let client = self.require_client()?;
        let group = self.consumer_group();
        let broker = msg.broker_name.clone().unwrap_or_default();
        // 从节点不接 CONSUMER_SEND_MSG_BACK（master 专属），所以这里只认 brokerId=0
        let addr = client
            .find_broker_address_in_publish(&broker)
            .ok_or_else(|| Error::client(format!("Broker[{broker}] master node does not exist")))?;
        client
            .consumer_send_msg_back(
                &group,
                msg,
                delay_level,
                None,
                5_000,
                &addr,
                self.unit_mode(),
            )
            .await
    }

    /// Python `create_topic`（Java `MQAdminImpl.createTopic`）。
    ///
    /// Java 的第 1 个参数 `key`（clusterName / brokerAddr）Python 收了不用；本移植版
    /// 干脆不收，语义 = 对默认路由里的**所有 master** 建 topic（与 Python 一致）。
    pub async fn create_topic(
        &self,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
    ) -> Result<()> {
        let client = self.require_client()?;
        client
            .create_topic_in_route(
                new_topic,
                queue_num,
                queue_num,
                6,
                topic_sys_flag,
                None,
                LITE_PULL_RPC_TIMEOUT_MILLIS,
            )
            .await
    }
}

/// Python `sub.sub_string or "*"`：空串回落 SUB_ALL。
fn sub_expression_of(sub: &SubscriptionData) -> &str {
    if sub.sub_string.is_empty() {
        FilterAPI::SUB_ALL
    } else {
        &sub.sub_string
    }
}

/// Python `addr.split(";")` + 逐项 trim + 丢空。
fn split_addrs(addr: &str) -> Vec<String> {
    addr.split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Python `DefaultMQPullConsumer._build_heartbeat`（`consumer.py:1649-1676`）：
/// 拉模式消费者组的一份 ConsumerData。
///
/// Java 依据（`MQClientInstance#prepareHeartbeatData:1031-1045` 为拉模式组出来的那一份）：
/// * `consumeType()` 恒为 `CONSUME_ACTIVELY`（`DefaultMQPullConsumerImpl:348`）、
///   `consumeFromWhere()` 恒为 `CONSUME_FROM_LAST_OFFSET`（:353）—— 与推送消费者
///   的 PASSIVELY 是两个口径，broker 侧两项都不是摆设：`consumerConnection`
///   （`AdminBrokerProcessor:1971`）按 consumeType 显示消费类型；开了
///   `rejectPullConsumerEnabled` 的 broker 会跳过非 ACTIVELY 的拉模式组
///   （`PullMessageProcessor:493-505` 回 `SUBSCRIPTION_NOT_EXIST`）。
/// * 订阅集取 `subscriptions():357-385`：逐条 `buildSubscriptionData(topic, "*")`，
///   并显式把 **subVersion 置 0**（Java `ms.setSubVersion(0L)`）—— 拉模式没有
///   "订阅版本"语义，带上当前时间戳会让 broker 每次心跳都认为订阅变了。
fn build_pull_heartbeat(inner: &PullInner) -> HeartbeatData {
    let cfg = read_cfg(&inner.cfg);
    let mut hb = HeartbeatData::new(cfg.client_id.clone().unwrap_or_default());
    let mut cd = ConsumerData::new(
        cfg.consumer_group,
        ConsumeType::CONSUME_ACTIVELY,
        cfg.message_model,
        ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
    );
    cd.unit_mode = cfg.unit_mode;
    for topic in lock(&inner.register_topics).iter() {
        if let Ok(mut sub) = FilterAPI::build_subscription_data(topic, Some(FilterAPI::SUB_ALL)) {
            // ⚠ SubscriptionData::new 的 sub_version 是 now_millis()（推送/lite 口径），
            // 拉模式这里必须显式清零，见上面 Java `ms.setSubVersion(0L)`。
            sub.sub_version = 0;
            cd.add_subscription_data(sub);
        }
    }
    hb.add_consumer_data(cd);
    hb
}

/// Python `_send_heartbeat_to_all_broker`：向所有已知 broker（含从节点）发一次本组
/// 心跳，返回成功台数。
///
/// 收件人取 `get_all_broker_addrs`，理由与 lite 版一致（Java
/// `sendHeartbeatToAllBroker`:732-750 只对 `consumerEmpty` 跳从节点；本心跳带
/// ConsumerData，故不跳）。
async fn send_pull_heartbeat(inner: &PullInner) -> usize {
    let Some(client) = lock(&inner.client).clone() else {
        return 0;
    };
    let hb = build_pull_heartbeat(inner);
    let mut ok = 0;
    for addr in client.get_all_broker_addrs() {
        match client
            .send_heartbeat(&addr, &hb, PULL_HEARTBEAT_TIMEOUT_MILLIS)
            .await
        {
            Ok(()) => ok += 1,
            Err(e) => rmq_debug!("pull heartbeat to {addr} failed: {e}"),
        }
    }
    if ok > 0 {
        inner.heartbeat_count.fetch_add(1, Ordering::SeqCst);
    }
    ok
}

/// Python `_heartbeat_loop`（`consumer.py:1663-1671`）：先等一个周期再发 ——
/// start() 里已同步发过一轮，循环的第一次是"第二个心跳周期"。
async fn pull_heartbeat_loop(inner: Arc<PullInner>, mut rx: watch::Receiver<bool>) {
    while inner.running.load(Ordering::Acquire) {
        if wait_or_stop(
            &mut rx,
            read_cfg(&inner.cfg).heartbeat_broker_interval_millis,
        )
        .await
        {
            return;
        }
        if !read_cfg(&inner.cfg).heartbeat_enabled {
            continue;
        }
        send_pull_heartbeat(&inner).await;
    }
}

// ================================================================ DefaultLitePullConsumer

/// [`DefaultLitePullConsumer`] 的配置（Python `consumer.py:2290-2343`）。
#[derive(Debug, Clone)]
pub struct LitePullConsumerConfig {
    /// Python `consumer_group`。
    pub consumer_group: String,
    /// Python `namespace`。
    pub namespace: String,
    /// Python `instance_name`，默认 `"DEFAULT"`。
    pub instance_name: String,
    /// Python `client_id`；`None` 时 `start()` 现造。
    pub client_id: Option<String>,
    /// Java `ClientConfig#unitName`（默认 null）：非空时进 clientId 后缀，
    /// 并作为地址服务器 URL 的 `-<unitName>` 段。
    pub unit_name: Option<String>,
    /// Java `ClientConfig#namespaceV2`（默认 null）：5.x **服务端**命名空间，
    /// 非空时每个请求带 `nsd=true` / `ns=<namespaceV2>` 扩展头（`NamespaceRpcHook`），
    /// 与 v1 `namespace` 的客户端 `%` 前缀机制是两套东西。
    pub namespace_v2: Option<String>,
    /// Java `ClientConfig#unitMode`（默认 false）：随回投/鉴权/消息过滤上线。
    pub unit_mode: bool,
    /// Java `ClientConfig#enableStreamRequestType`：true 时每个请求带 `ReqT=0`，
    /// clientId 末尾多一段 `@STREAM`。
    ///
    /// ⚠ 拉模式默认 **true**：Java `DefaultMQPullConsumer` / `DefaultLitePullConsumer`
    /// 的每个构造函数都置 `enableStreamRequestType = true`（:113/:126 与 :213/:228），
    /// 只有推送消费者和生产者默认 false。
    pub enable_stream_request_type: bool,
    /// Java `ClientConfig#pollNameServerInterval`（默认 30000ms）：在用 topic 的
    /// 路由周期刷新间隔，`start()` 时透传给 `MQClientInstance`。
    pub poll_name_server_interval_millis: u64,
    /// Python `name_server_addrs`。
    pub name_server_addrs: Vec<String>,
    /// Python `tls_enable`（同上，与推送消费者统一）。
    pub tls_enable: Option<bool>,
    /// Python `message_model`。
    pub message_model: String,
    /// Python `consume_from_where`：仅影响**首次**位点解析。
    pub consume_from_where: String,
    /// Python `consume_timestamp`：14 位**本地墙钟** `yyyyMMddHHmmss`，
    /// 默认 now-30min（Java `DefaultLitePullConsumer:168`）。只有
    /// `CONSUME_FROM_TIMESTAMP` 会读它，但 start 时无条件校验格式。
    pub consume_timestamp: String,
    /// Python `pull_batch_size` = 32（ setter 里 `max(1, n)`）。
    pub pull_batch_size: i32,
    /// Python `poll_timeout_millis` = 5000（`max(0, ms)`）。
    pub poll_timeout_millis: i64,
    /// Python `auto_commit` = true：**拉到即提交**，不是 poll 之后。
    pub auto_commit: bool,
    /// Python `auto_commit_interval_millis` = 5000（`max(0, ms)`）。
    pub auto_commit_interval_millis: i64,
    /// Python `consumer_timeout_millis_when_suspend` = 30000：lite 恒短轮询，
    /// 字段保留（与 Python 同形状）但拉取路径不读它。
    pub consumer_timeout_millis_when_suspend: i64,
    /// Python `broker_suspend_max_time_millis` = 20000：同上，拉取路径不读。
    pub broker_suspend_max_time_millis: i64,
    /// Python `pull_interval_millis` = 50：队尾空轮询时的退避（`max(0, ms)`）。
    pub pull_interval_millis: i64,
    /// Python `pull_thread_nums` = 1：**本实现恒为单拉取循环**（与四门语言一致），
    /// 字段只为配置形状保留。
    pub pull_thread_nums: i32,
    /// Java `DefaultLitePullConsumer.topicMetadataCheckIntervalMillis`（:160，默认 30s）：
    /// 后台比对 [`register_topic_message_queue_change_listener`] 注册 topic 的队列集合的周期。
    ///
    /// [`register_topic_message_queue_change_listener`]: DefaultLitePullConsumer::register_topic_message_queue_change_listener
    pub topic_metadata_check_interval_millis: i64,
}

impl Default for LitePullConsumerConfig {
    fn default() -> LitePullConsumerConfig {
        LitePullConsumerConfig {
            consumer_group: MixAll::DEFAULT_CONSUMER_GROUP.to_string(),
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            unit_name: None,
            namespace_v2: None,
            unit_mode: false,
            enable_stream_request_type: true,
            // Java `ClientConfig:58`：pollNameServerInterval = 1000 * 30
            poll_name_server_interval_millis: 30_000,
            client_id: None,
            name_server_addrs: Vec::new(),
            tls_enable: None,
            message_model: MessageModel::CLUSTERING.to_string(),
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string(),
            consume_timestamp: default_consume_timestamp(),
            pull_batch_size: 32,
            poll_timeout_millis: 5_000,
            auto_commit: true,
            auto_commit_interval_millis: 5_000,
            consumer_timeout_millis_when_suspend: DEFAULT_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
            broker_suspend_max_time_millis: DEFAULT_BROKER_SUSPEND_MAX_TIME_MILLIS,
            pull_interval_millis: 50,
            pull_thread_nums: 1,
            topic_metadata_check_interval_millis: 30_000,
        }
    }
}

/// Python 里由 `_lock` 保护的那几张表（键统一是 [`mq_key`] 那串
/// `topic+brokerName+queueId`，与推送消费者同口径）。
/// `Default` 手写：截止时刻的初值必须是 -1（Java 同值），不能是 0。
struct LiteState {
    /// Python `subscription: Dict[topic, sub_expression]`（subscribe 模式）。
    subscription: BTreeMap<String, String>,
    /// Python `subscription_data`：进心跳的订阅集。
    subscription_data: BTreeMap<String, SubscriptionData>,
    /// Python `_assign_sub_expr`：assign 模式下的 tag 表达式。
    assign_sub_expr: BTreeMap<String, String>,
    /// Python `_assign_mode`。
    assign_mode: bool,
    /// Python `_assigned`：当前分配（有序，见模块头差异 2）。
    assigned: BTreeMap<String, MessageQueue>,
    /// Python `_next_offset`：**拉取游标**（Java `MessageQueueState.pullOffset`）。
    /// 只回答"下一次从哪拉"，**绝不**是提交内容。
    next_offset: BTreeMap<String, i64>,
    /// Python `_consume_offset`：**已消费游标**（Java `MessageQueueState.consumeOffset`）。
    /// 只有 `poll()` 把消息交到调用方手上才前进，本地缓冲里压着的部分不算已消费。
    consume_offset: BTreeMap<String, i64>,
    /// Python `_offset_table`：提交落点，对位 Java
    /// `RemoteBrokerOffsetStore.offsetTable`（内存位点表；`persist=False` 的值就在这张表里）。
    offset_table: BTreeMap<String, i64>,
    /// Python `_seek_offset`：`seek()` 钉住的位点，优先于 `consume_from_where`。
    seek_offset: BTreeMap<String, i64>,
    /// Python `_next_auto_commit_deadline`：全局自动提交截止时刻，初值 -1 ⇒ 第一次检查就提交
    /// （Java `DefaultLitePullConsumerImpl.nextAutoCommitDeadline`）。
    next_auto_commit_deadline: i64,
    /// Python `_paused`。
    paused: BTreeSet<String>,
    /// Python `_last_rebalance_ts`。
    last_rebalance_ts: i64,
}

impl Default for LiteState {
    fn default() -> LiteState {
        LiteState {
            subscription: BTreeMap::new(),
            subscription_data: BTreeMap::new(),
            assign_sub_expr: BTreeMap::new(),
            assign_mode: false,
            assigned: BTreeMap::new(),
            next_offset: BTreeMap::new(),
            consume_offset: BTreeMap::new(),
            offset_table: BTreeMap::new(),
            seek_offset: BTreeMap::new(),
            // Java 的初值是 -1 而不是 0：0 是 epoch 起点，等价于"永远不到点"。
            next_auto_commit_deadline: -1,
            paused: BTreeSet::new(),
            last_rebalance_ts: 0,
        }
    }
}

struct LiteInner {
    cfg: RwLock<LitePullConsumerConfig>,
    state: Mutex<LiteState>,
    client: Mutex<Option<MQClientInstance>>,
    started: AtomicBool,
    running: AtomicBool,
    runtime: OnceLock<tokio::runtime::Handle>,
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    strategy: RwLock<Arc<dyn AllocateMessageQueueStrategy>>,
    listener: Mutex<Option<Arc<dyn MessageQueueListener>>>,
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
    /// Python `_local_buffer` + `_buffer_cond`。
    buffer: Mutex<VecDeque<MessageExt>>,
    buffer_signal: Notify,
    /// Java `DefaultLitePullConsumerImpl:144` topicMessageQueueChangeListenerMap +
    /// `:146` messageQueuesForTopic：监听器按 topic 键存放，快照表记录上一轮报出去的
    /// 队列集合（键集变化才回调）。
    topic_listeners: Mutex<BTreeMap<String, Arc<dyn TopicMessageQueueChangeListener>>>,
    queues_for_topic: Mutex<BTreeMap<String, Vec<MessageQueue>>>,
}

impl Default for LiteInner {
    fn default() -> LiteInner {
        LiteInner {
            cfg: RwLock::new(LitePullConsumerConfig::default()),
            state: Mutex::new(LiteState::default()),
            client: Mutex::new(None),
            started: AtomicBool::new(false),
            running: AtomicBool::new(false),
            runtime: OnceLock::new(),
            stop: watch::channel(false).0,
            tasks: Mutex::new(Vec::new()),
            strategy: RwLock::new(Arc::new(AllocateMessageQueueAveragely)),
            listener: Mutex::new(None),
            rpc_hook: RwLock::new(None),
            buffer: Mutex::new(VecDeque::new()),
            buffer_signal: Notify::new(),
            topic_listeners: Mutex::new(BTreeMap::new()),
            queues_for_topic: Mutex::new(BTreeMap::new()),
        }
    }
}

impl Drop for LiteInner {
    /// 忘记 `shutdown()` 也不能留下还在跑的循环（同推送消费者）。
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
        let _ = self.stop.send(true);
        self.buffer_signal.notify_waiters();
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
    }
}

/// 对应 Java `consumer.TopicMessageQueueChangeListener`：topic 的队列**集合**相对
/// 上一次快照有变化时才回调（扩/缩容场景）。Java 的
/// `DefaultLitePullConsumerImpl#fetchTopicMessageQueuesAndCompare:1230` 每
/// `topicMetadataCheckIntervalMillis` 比对一次，本端口同款。
pub trait TopicMessageQueueChangeListener: Send + Sync {
    /// Java `onChanged(String topic, Set<MessageQueue>)`。`topic` 就是注册监听器时
    /// 用的那个键（Java `DefaultLitePullConsumer:327` 在入口就 `withNamespace`）。
    fn on_changed(&self, topic: &str, message_queues: &[MessageQueue]);
}

/// 轻量拉取消费者（对应 Java `DefaultLitePullConsumer`，移植自 Python
/// `consumer.DefaultLitePullConsumer`）。
///
/// 两种模式：
/// * **subscribe**：`subscribe(topic, expr)` → 后台按 [`AllocateMessageQueueStrategy`]
///   重平衡 → 后台拉取；
/// * **assign**：`assign(&[mq])` 显式指定队列，不重平衡（tag 表达式用
///   [`set_sub_expression_for_assign`](Self::set_sub_expression_for_assign)）。
///
/// 两种模式都由单个后台循环做**短轮询**把消息灌进本地缓冲，`poll()` 只从缓冲取。
/// 位点默认 auto-commit —— 注意提交点是「拉进缓冲」而非「poll 给用户」，
/// 进程崩溃会丢掉缓冲里未 poll 的消息（与 Python 同一取舍，见其类文档字符串）。
///
/// ⚠ subscribe 模式下 `start()` **不**同步重平衡（Python 同）：刚 start 完
/// [`assignment`](Self::assignment) 很可能是空的，要等后台循环第一轮（≤1s）。
/// 依赖队列分配的调用方（`pause`、按队列 `seek`）先 `rebalance().await` 一次。
#[derive(Clone)]
pub struct DefaultLitePullConsumer {
    inner: Arc<LiteInner>,
}

impl std::fmt::Debug for DefaultLitePullConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cfg = read_cfg(&self.inner.cfg);
        let state = lock(&self.inner.state);
        f.debug_struct("DefaultLitePullConsumer")
            .field("consumer_group", &cfg.consumer_group)
            .field("namespace", &cfg.namespace)
            .field("client_id", &cfg.client_id)
            .field("name_server_addrs", &cfg.name_server_addrs)
            .field("message_model", &cfg.message_model)
            .field("consume_from_where", &cfg.consume_from_where)
            .field("assign_mode", &state.assign_mode)
            .field("started", &self.inner.started.load(Ordering::Acquire))
            .field("running", &self.inner.running.load(Ordering::Acquire))
            .field("assigned", &state.assigned.len())
            .field("buffered", &lock(&self.inner.buffer).len())
            .finish()
    }
}

impl DefaultLitePullConsumer {
    /// Python `DefaultLitePullConsumer(consumer_group)`。
    pub fn new(consumer_group: &str) -> Result<DefaultLitePullConsumer> {
        DefaultLitePullConsumer::with_config(LitePullConsumerConfig {
            consumer_group: consumer_group.to_string(),
            ..Default::default()
        })
    }

    /// Python `DefaultLitePullConsumer(consumer_group, rpc_hook=..)`。
    pub fn with_rpc_hook(
        consumer_group: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Result<DefaultLitePullConsumer> {
        let consumer = DefaultLitePullConsumer::new(consumer_group)?;
        consumer.set_rpc_hook(rpc_hook);
        Ok(consumer)
    }

    /// 直接以一份完整配置构造。
    pub fn with_config(cfg: LitePullConsumerConfig) -> Result<DefaultLitePullConsumer> {
        if cfg.consumer_group.trim().is_empty() {
            bail!("consumerGroup is empty");
        }
        // `LiteInner` 有 `Drop`，所以不能用 `..Default::default()` 的结构体更新语法，
        // 只能先整体默认构造再替换 cfg。
        let mut inner = LiteInner::default();
        inner.cfg = RwLock::new(cfg);
        let inner = Arc::new(inner);
        Ok(DefaultLitePullConsumer { inner })
    }

    /// 当前配置快照。
    pub fn config(&self) -> LitePullConsumerConfig {
        read_cfg(&self.inner.cfg)
    }

    /// 改配置（Python 的直接赋属性 + 一批 `set_*`；钳制规则照抄 setter）。
    pub fn update_config(&self, f: impl FnOnce(&mut LitePullConsumerConfig)) {
        {
            let mut w = self.inner.cfg.write().unwrap_or_else(|e| e.into_inner());
            f(&mut w);
            w.pull_batch_size = w.pull_batch_size.max(1);
            w.poll_timeout_millis = w.poll_timeout_millis.max(0);
            w.auto_commit_interval_millis = w.auto_commit_interval_millis.max(0);
            w.pull_interval_millis = w.pull_interval_millis.max(0);
        }
    }

    /// Python `set_pull_batch_size`：`max(1, n)`。
    pub fn set_pull_batch_size(&self, n: i32) {
        self.update_config(|c| c.pull_batch_size = n);
    }

    /// Python `set_poll_timeout_millis`：`max(0, ms)`。
    pub fn set_poll_timeout_millis(&self, ms: i64) {
        self.update_config(|c| c.poll_timeout_millis = ms);
    }

    /// Python `set_auto_commit`。
    pub fn set_auto_commit(&self, auto: bool) {
        self.update_config(|c| c.auto_commit = auto);
    }

    /// Python `set_auto_commit_interval_millis`：`max(0, ms)`。
    pub fn set_auto_commit_interval_millis(&self, ms: i64) {
        self.update_config(|c| c.auto_commit_interval_millis = ms);
    }

    /// Python `set_pull_interval_millis`：`max(0, ms)`。
    pub fn set_pull_interval_millis(&self, ms: i64) {
        self.update_config(|self_c| self_c.pull_interval_millis = ms);
    }

    /// Python `set_consume_from_where`。
    pub fn set_consume_from_where(&self, where_: &str) {
        let where_ = where_.to_string();
        self.update_config(|c| c.consume_from_where = where_);
    }

    /// Python `set_consume_timestamp`。
    pub fn set_consume_timestamp(&self, ts: &str) {
        let ts = ts.to_string();
        self.update_config(|c| c.consume_timestamp = ts);
    }

    /// Python `set_namesrv_addr`。
    pub fn set_namesrv_addr(&self, addr: &str) {
        let addrs = split_addrs(addr);
        self.update_config(|c| c.name_server_addrs = addrs);
    }

    /// Python `set_name_server_addresses`。
    pub fn set_name_server_addresses(&self, addrs: &[String]) {
        let addrs = addrs.to_vec();
        self.update_config(|c| c.name_server_addrs = addrs);
    }

    /// Python `set_instance_name`。
    pub fn set_instance_name(&self, name: &str) {
        let name = name.to_string();
        self.update_config(|c| c.instance_name = name);
    }

    /// Java `ClientConfig#setUnitName`：`None`/空白等价于不设（拼 clientId 时按 isBlank 判）。
    pub fn set_unit_name(&self, unit_name: Option<&str>) {
        let unit_name = unit_name.map(str::to_string);
        self.update_config(|c| c.unit_name = unit_name);
    }

    /// Java `ClientConfig#setNamespaceV2`：5.x **服务端**命名空间（`ns`/`nsd`
    /// 扩展头，见 `NamespaceRpcHook`）。`None`/空串 = 不设，钩子退化为 no-op。
    /// `start()` 时透传给 `MQClientInstance`，晚于 start 修改不影响已建实例。
    pub fn set_namespace_v2(&self, namespace_v2: Option<&str>) {
        let namespace_v2 = namespace_v2.map(str::to_string);
        self.update_config(|c| c.namespace_v2 = namespace_v2);
    }

    /// Java `ClientConfig#getNamespaceV2`。
    pub fn get_namespace_v2(&self) -> Option<String> {
        self.config().namespace_v2
    }

    /// Java `ClientConfig#setUnitMode`。
    pub fn set_unit_mode(&self, unit_mode: bool) {
        self.update_config(|c| c.unit_mode = unit_mode);
    }

    /// Java `ClientConfig#setEnableStreamRequestType`。
    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.update_config(|c| c.enable_stream_request_type = enable);
    }

    /// Java `ClientConfig#setPollNameServerInterval`。
    pub fn set_poll_name_server_interval_millis(&self, millis: u64) {
        self.update_config(|c| c.poll_name_server_interval_millis = millis);
    }

    /// Python `set_message_model`。
    pub fn set_message_model(&self, model: &str) {
        let model = model.to_string();
        self.update_config(|c| c.message_model = model);
    }

    /// Python `set_namespace`。
    pub fn set_namespace(&self, namespace: &str) {
        let namespace = namespace.to_string();
        self.update_config(|c| c.namespace = namespace);
    }

    /// Python `set_rpc_hook`。
    pub fn set_rpc_hook(&self, hook: Option<Arc<dyn RPCHook>>) {
        *self
            .inner
            .rpc_hook
            .write()
            .unwrap_or_else(|e| e.into_inner()) = hook;
    }

    /// Python `set_allocate_message_queue_strategy`。
    ///
    /// ⚠ Rust 用 `Arc<dyn ...>` 表达「一定有策略」，Java/Python 那个「置 null/None
    /// 再由 checkConfig 拒绝」的分支在这里类型不可表示，故无对应校验。
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

    /// 当前队列分配策略（对应 Java `getAllocateMessageQueueStrategy`，
    /// `DefaultLitePullConsumer:196` 同款）。
    pub fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy> {
        self.inner
            .strategy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Python `set_message_queue_listener`（Java 签名回调，见模块头偏离 5）。
    pub fn set_message_queue_listener(&self, listener: Arc<dyn MessageQueueListener>) {
        *lock(&self.inner.listener) = Some(listener);
    }

    pub fn consumer_group(&self) -> String {
        read_cfg(&self.inner.cfg).consumer_group
    }

    pub fn client_id(&self) -> String {
        read_cfg(&self.inner.cfg)
            .client_id
            .clone()
            .unwrap_or_default()
    }

    /// Java `ClientConfig#isUnitMode()`（回投/过滤等请求都取这一个值）。
    pub fn unit_mode(&self) -> bool {
        read_cfg(&self.inner.cfg).unit_mode
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    /// Python `is_running`。
    pub fn is_running(&self) -> bool {
        self.inner.running.load(Ordering::Acquire)
    }

    // ---------------- 订阅 / 分配 ----------------

    /// Python `subscribe(topic, sub_expression="*")`：切回 subscribe 模式。
    ///
    /// 表达式非法（`FilterAPI` 抛错）时只丢订阅表里的那条
    /// （Python `consumer.py:2409-2412`），`subscription` 仍记下 —— 与 Python 一致。
    pub fn subscribe(&self, topic: &str, sub_expression: &str) {
        let topic = with_namespace(&self.config().namespace, topic);
        {
            let mut state = lock(&self.inner.state);
            state.assign_mode = false;
            state
                .subscription
                .insert(topic.clone(), sub_expression.to_string());
            match FilterAPI::build_subscription_data(&topic, Some(sub_expression)) {
                Ok(sub) => {
                    state.subscription_data.insert(topic, sub);
                }
                Err(e) => {
                    rmq_debug!("lite subscribe: build subscription data failed: {e}");
                    state.subscription_data.remove(&topic);
                }
            }
        }
    }

    /// Python `subscribe_with_selector`：lite 只认 tag 表达式，`MessageSelector`
    /// 一律按 tag 处理（`consumer.py:2414-2416`）。
    pub fn subscribe_with_selector(&self, topic: &str, selector: &MessageSelector) {
        self.subscribe(topic, &selector.expression);
    }

    /// Python `unsubscribe`。
    pub fn unsubscribe(&self, topic: &str) {
        let topic = with_namespace(&self.config().namespace, topic);
        let mut state = lock(&self.inner.state);
        state.subscription.remove(&topic);
        state.subscription_data.remove(&topic);
    }

    /// Python `set_sub_expression_for_assign`：assign 模式下给某 topic 指定 tag
    /// 表达式，同时登记 `SubscriptionData`（assign 模式 broker 也要订阅信息）。
    pub fn set_sub_expression_for_assign(&self, topic: &str, sub_expression: &str) {
        let topic = with_namespace(&self.config().namespace, topic);
        let mut state = lock(&self.inner.state);
        state
            .assign_sub_expr
            .insert(topic.clone(), sub_expression.to_string());
        match FilterAPI::build_subscription_data(&topic, Some(sub_expression)) {
            Ok(sub) => {
                state.subscription_data.insert(topic, sub);
            }
            Err(e) => {
                rmq_debug!("lite assign expr: build subscription data failed: {e}");
                state.subscription_data.remove(&topic);
            }
        }
    }

    /// Python `assign(mqs)`：切到 assign 模式（不走重平衡）。
    ///
    /// 初始位点解析需要 RPC，故这里只登记队列，位点留给
    /// [`start`](Self::start)（Python 在 `assign()` 里同步解析，语义相同、时机后移，
    /// 因为 Rust 侧 `assign()` 不是 async）。已在 `_next_offset` 里的队列不覆盖，
    /// 与 Python 的 `if mq not in self._next_offset` 一致。
    pub fn assign(&self, message_queues: &[MessageQueue]) {
        let mut state = lock(&self.inner.state);
        state.assign_mode = true;
        let keep: BTreeSet<String> = message_queues.iter().map(mq_key).collect();
        // Java `assignedMessageQueue.updateAssignedMessageQueue`：撤掉的队列连着整份
        // MessageQueueState 丢掉 ⇒ 两条游标一起消失。**不**动内存位点表，也**不** persist：
        // 那份清理挂在 subscribe 模式的 rebalance 上，assign 模式不走 rebalance。
        for key in state
            .assigned
            .keys()
            .filter(|k| !keep.contains(*k))
            .cloned()
            .collect::<Vec<String>>()
        {
            state.next_offset.remove(&key);
            state.consume_offset.remove(&key);
        }
        state.assigned.retain(|k, _| keep.contains(k));
        for mq in message_queues {
            state.assigned.insert(mq_key(mq), mq.clone());
        }
    }

    /// 当前订阅表（topic -> 表达式），Python 直接读 `subscription`。
    pub fn subscription(&self) -> Vec<(String, String)> {
        lock(&self.inner.state)
            .subscription
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// 当前进入心跳的订阅集，Python 直接读 `subscription_data`。
    pub fn subscriptions(&self) -> Vec<SubscriptionData> {
        lock(&self.inner.state)
            .subscription_data
            .values()
            .cloned()
            .collect()
    }

    /// 是否 assign 模式（Python `_assign_mode`）。
    pub fn is_assign_mode(&self) -> bool {
        lock(&self.inner.state).assign_mode
    }

    // ---------------- 生命周期 ----------------

    /// Python `start()`：幂等；先查组名，再有 name server，必须已有订阅或 assign；
    /// **先把订阅 topic 的路由缓存进来，再同步发一次心跳**，最后起后台循环。
    ///
    /// 心跳只发给路由表里已知的 broker，所以刷路由必须在那次同步心跳之前
    /// （见 [`refresh_route_for_heartbeat`](Self::refresh_route_for_heartbeat)）：
    /// 否则 start() 那一轮发 0 份，订阅要等到 5s 心跳循环第一轮才注册上 broker，
    /// 同组多实例时对端要晚一个心跳周期才能看见彼此。
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
        // 对应 Java DefaultLitePullConsumerImpl.checkConfig(:413)：组名合法性 + 挡掉
        // DEFAULT_CONSUMER，都排在地址/订阅校验之前（纯本地判定，失败不碰网络）。
        if let Err(e) = validators::check_group(&cfg.consumer_group) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        if cfg.consumer_group == MixAll::DEFAULT_CONSUMER_GROUP {
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
            if state.subscription.is_empty() && !state.assign_mode {
                self.inner.started.store(false, Ordering::Release);
                bail!("subscription is not set, call subscribe() or assign() first");
            }
        }
        // 对应 Java DefaultMQPushConsumerImpl.checkConfig：启动即无条件校验 consumeTimestamp。
        // 下面解析初始位点的路径会吞异常，晚抛等于静默退化成「从 max offset 消费」。
        if let Err(e) = consume_timestamp_millis(&cfg.consume_timestamp) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        // Java `DefaultMQPullConsumerImpl#start`:712 / `DefaultLitePullConsumerImpl#start`:288：
        // CLUSTERING 才改写 instanceName，clientId 口径是 `ClientConfig#buildMQClientId`
        // 的 `<本机 IP>@<instanceName>`。
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
            c.client_id = Some(client_id.clone());
            c.instance_name = instance_name;
        });

        let instance_cfg = MQClientInstanceConfig {
            tls_enable: cfg.tls_enable,
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
        if cfg.name_server_addrs.is_empty() {
            let addrs = client.name_server_addrs();
            if !addrs.is_empty() {
                self.update_config(|c| c.name_server_addrs = addrs);
            }
        }
        // 本消费者不进 `consumer_table`（接不了 broker 的反向请求），但要在实例上
        // 登记组名：Java `DefaultLitePullConsumerImpl#start`:339 也是 registerConsumer
        // 之后才由实例的关闭守卫判断「还有谁在用这份实例」。心跳报文仍由自己拼
        // （Python 同），这里只取 client 引用备用。
        client.register_consumer_group(&cfg.consumer_group);
        *lock(&self.inner.client) = Some(client);

        // assign 模式：start 时补齐初始位点（Python `consumer.py:2463-2469`）
        let pending: Vec<MessageQueue> = {
            let state = lock(&self.inner.state);
            state
                .assigned
                .values()
                .filter(|mq| !state.next_offset.contains_key(&mq_key(mq)))
                .cloned()
                .collect()
        };
        if !pending.is_empty() {
            self.resolve_offsets_for(&pending).await;
        }

        // 先把 tag 订阅注册给 broker，再起后台拉取
        self.refresh_route_for_heartbeat().await;
        self.send_heartbeat_to_all_broker().await;
        self.inner.running.store(true, Ordering::Release);
        let _ = self.inner.stop.send(false);
        self.spawn_loops();
        Ok(())
    }

    /// Python `shutdown()`：停循环 → 末次 commit（auto_commit 时）→ 唤醒 poll
    /// → 关实例。
    ///
    /// 差别见模块头差异 1：末次提交与 `client.shutdown()` 一起派发到运行时上**串行**
    /// 执行（提交要发 RPC，必须发生在实例关闭之前），且本方法**阻塞等它落地**
    /// （上限 [`SHUTDOWN_FINALIZE_BUDGET`]，「shutdown 后立刻退进程」不会再丢末次
    /// 提交）；current_thread 运行时里退化为游离任务并告警（见
    /// [`run_finalize_blocking`] 的情形 2/3）；没有运行时时退化为直接关闭。
    pub fn shutdown(&self) {
        if self
            .inner
            .started
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.inner.running.store(false, Ordering::Release);
        let _ = self.inner.stop.send(true);
        for task in lock(&self.inner.tasks).drain(..) {
            task.abort();
        }
        // 唤醒可能卡在 poll() 里的调用方（Python `notify_all`）
        self.inner.buffer_signal.notify_waiters();

        let auto_commit = self.config().auto_commit;
        // Java 的 shutdown 走 `persistConsumerOffset()`：把内存位点表按**当下持有的队列**
        // 刷一遍，与 auto_commit 无关（手动模式 `persist=false` 攒下的值同样要落盘）。
        // 自动提交模式再多走一步 commitAll：本端口没有 Java 那份 5s 定时器，
        // "poll 交出去但还没到截止时刻"的位点得在这里补上，否则重启后从上一格重投。
        if auto_commit {
            let mut state = lock(&self.inner.state);
            let scope: Vec<String> = state.assigned.keys().cloned().collect();
            for key in &scope {
                // -1 守卫：没交付过的队列绝不写表（Java 的 consumerOffset is -1 那条 error）
                if let Some(offset) = state.consume_offset.get(key).copied() {
                    if offset != -1 {
                        state.offset_table.insert(key.clone(), offset);
                    }
                }
            }
        }
        let scope: Vec<String> = lock(&self.inner.state).assigned.keys().cloned().collect();
        let this = self.clone();
        match self.runtime_handle() {
            Some(handle) => {
                let finalize = async move {
                    // 里面含 Java persistAll 的"remove unused mq"清理
                    let _ = this.persist_offset_table(&scope).await;
                    this.close_client();
                };
                let _ = run_finalize_blocking(
                    &handle,
                    "lite pull shutdown",
                    SHUTDOWN_FINALIZE_BUDGET,
                    finalize,
                );
            }
            None => {
                // 无运行时：提交不了，至少把连接关掉（与 Python 的
                // 「commit 失败只 debug 日志」同级别降级）。
                rmq_warn!("lite shutdown: no tokio runtime, skip final offset commit");
                self.close_client();
            }
        }
    }

    /// 摘掉本组登记并关掉这条连接。末次提交还要用它，所以只能排在提交之后
    /// （Java `unregisterConsumer`:265 → `shutdown()`:269）。
    fn close_client(&self) {
        if let Some(client) = lock(&self.inner.client).take() {
            let group = self.consumer_group();
            client.unregister_consumer_group(&group);
            client.shutdown();
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

    fn require_client(inner: &LiteInner) -> Result<MQClientInstance> {
        if !inner.running.load(Ordering::Acquire) && !inner.started.load(Ordering::Acquire) {
            return Err(Error::client("consumer not started, call start() first"));
        }
        lock(&inner.client)
            .clone()
            .ok_or_else(|| Error::client("consumer not started, call start() first"))
    }

    // ---------------- 心跳 ----------------

    /// Python `_refresh_route_for_heartbeat`：心跳只发给「路由表里已知的 broker」，
    /// 而自建实例刚 start 时路由表还是空的，那一轮会发 0 份 —— 订阅要等到 5s 心跳
    /// 循环的第一轮才注册上 broker。所以这里先把本实例关注的 topic（订阅的 + assign
    /// 到的）路由拉一遍并登记为在用，让 start() 里那次同步心跳真正到达 broker。
    ///
    /// 对应 Java：`MQClientInstance#sendHeartbeatToAllBrokerWithLock` 依赖
    /// `topicRouteTable`，而 `DefaultLitePullConsumerImpl#start` 在注册心跳前先
    /// `updateTopicRouteInfoFromNameServer`。
    async fn refresh_route_for_heartbeat(&self) {
        let Ok(client) = Self::require_client(&self.inner) else {
            return;
        };
        let mut topics: Vec<String> = lock(&self.inner.state)
            .subscription
            .keys()
            .cloned()
            .collect();
        for mq in lock(&self.inner.state).assigned.values() {
            if !topics.contains(&mq.topic) {
                topics.push(mq.topic.clone());
            }
        }
        for topic in topics {
            client.register_topic_in_use(&topic);
            if let Err(e) = client.get_topic_publish_info(&topic, false).await {
                rmq_debug!("lite start: refresh route for {topic} failed: {e}");
            }
        }
    }

    /// Python `_send_heartbeat_to_all_broker`：向路由里的每个 broker 发一份，返回成功数。
    pub async fn send_heartbeat_to_all_broker(&self) -> usize {
        send_lite_heartbeat(&self.inner).await
    }

    // ---------------- 后台循环 ----------------

    fn spawn_loops(&self) {
        let Some(handle) = self.runtime_handle() else {
            rmq_warn!("lite pull consumer: no tokio runtime, background loops disabled");
            return;
        };
        // 后台任务握 Weak 而不是 Arc：消费者被丢弃时 Drop 才有机会跑（否则任务自己就把
        // 引用计数留着了，Drop 永不触发）。两个循环的 future 类型不同，只能各写一行。
        let mut tasks = lock(&self.inner.tasks);
        tasks.push(handle.spawn(Self::run_guarded(
            Arc::downgrade(&self.inner),
            self.inner.stop.subscribe(),
            heartbeat_loop,
        )));
        tasks.push(handle.spawn(Self::run_guarded(
            Arc::downgrade(&self.inner),
            self.inner.stop.subscribe(),
            pull_service_loop,
        )));
        tasks.push(handle.spawn(Self::run_guarded(
            Arc::downgrade(&self.inner),
            self.inner.stop.subscribe(),
            metadata_loop,
        )));
    }

    async fn run_guarded<F, Fut>(weak: Weak<LiteInner>, stop: watch::Receiver<bool>, make: F)
    where
        F: FnOnce(Arc<LiteInner>, watch::Receiver<bool>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if let Some(inner) = weak.upgrade() {
            make(inner, stop).await;
        }
    }

    // ---------------- poll / 位点 ----------------

    /// Python `poll(timeout=None)`：等缓冲非空（最多 `poll_timeout_millis`），
    /// 一次最多取 [`MAX_POLL_BATCH_SIZE`] 条。
    pub async fn poll(&self, timeout_millis: Option<i64>) -> Vec<MessageExt> {
        // Java poll() 进来先按全局截止时刻试一次自动提交：在拿缓冲锁之前做，
        // 提交要发 RPC，抱着缓冲锁等网络会把 enqueue 一起卡住。
        if self.config().auto_commit {
            self.maybe_auto_commit().await;
        }
        let timeout = timeout_millis.unwrap_or_else(|| self.config().poll_timeout_millis);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout.max(0) as u64);
        loop {
            // 先挂上唤醒再查缓冲（等价于 Python 在 `with self._buffer_cond` 里 check 后
            // 才 wait）：反过来写时，检查与挂起之间 enqueue 的那次 notify 会丢，
            // 用户只能白等到一个 deadline。`enable()` 才是真正登记兴趣的点。
            let mut notified = std::pin::pin!(self.inner.buffer_signal.notified());
            notified.as_mut().enable();
            if let Some(drained) = self.try_drain() {
                // 对位 Java `poll()`：消息交到调用方手上才推进"已消费游标"
                // （`updateConsumeOffset(mq, processQueue.removeMessage(msgs))`）。
                self.advance_consume_offset(&drained);
                return drained;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Vec::new();
            }
            // 与 Python 的 `Condition.wait(remaining)` 等价：被叫醒就再查一次缓冲，
            // 没到 deadline 就继续等（假唤醒无害）。
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    /// 一次性 drain 缓冲（Python `poll` 里 `while self._local_buffer and len(out) < 1024`）。
    /// `None` = 缓冲为空，调用方该继续等。
    fn try_drain(&self) -> Option<Vec<MessageExt>> {
        let mut buffer = lock(&self.inner.buffer);
        if buffer.is_empty() {
            return None;
        }
        let take = buffer.len().min(MAX_POLL_BATCH_SIZE);
        Some(buffer.drain(..take).collect())
    }

    /// 交付出去的这批消息推进**已消费游标**（拉取游标一格都不动）。
    ///
    /// 只认当下持有且已有拉取记录的队列：Java 的 `updateConsumeOffset` 在
    /// `MessageQueueState` 不存在时直接什么都不做。
    fn advance_consume_offset(&self, msgs: &[MessageExt]) {
        let mut state = lock(&self.inner.state);
        // Python 同款做法：先把"有拉取记录的队列"按 (topic, brokerName, queueId) 建索引，
        // 再用消息自己的坐标反查 mq_key —— 从消息字段拼 key 会绕过持有判断。
        let held: BTreeMap<(String, String, i32), String> = state
            .next_offset
            .keys()
            .filter_map(|k| state.assigned.get(k))
            .map(|mq| {
                (
                    (mq.topic.clone(), mq.broker_name.clone(), mq.queue_id),
                    mq_key(mq),
                )
            })
            .collect();
        for m in msgs {
            let Some(key) = held.get(&(
                m.topic.clone(),
                m.broker_name.clone().unwrap_or_default(),
                m.queue_id,
            )) else {
                continue;
            };
            let nxt = m.queue_offset + 1;
            if nxt > state.consume_offset.get(key).copied().unwrap_or(-1) {
                state.consume_offset.insert(key.clone(), nxt);
            }
        }
    }

    /// Python `seek`：钉住游标并丢掉缓冲里该队列早于 `offset` 的消息。
    pub fn seek(&self, mq: &MessageQueue, offset: i64) {
        let key = mq_key(mq);
        {
            let mut state = lock(&self.inner.state);
            state.seek_offset.insert(key.clone(), offset);
            state.next_offset.insert(key.clone(), offset);
            // Java 的 seek 只置 seekOffset，下一次拉取时 `nextPullOffset()` 才把它同时
            // 写进 consumeOffset：跳回去意味着"那里之前都还没消费"，否则重放的消息会被
            // 已提交位点直接跳过。这里一次性写全，效果相同。
            state.consume_offset.insert(key, offset);
        }
        let mut buffer = lock(&self.inner.buffer);
        let kept: VecDeque<MessageExt> = buffer
            .iter()
            .filter(|m| {
                !(m.topic == mq.topic
                    && m.broker_name.as_deref() == Some(mq.broker_name.as_str())
                    && m.queue_id == mq.queue_id
                    && m.queue_offset < offset)
            })
            .cloned()
            .collect();
        *buffer = kept;
    }

    /// Python `seek_to_begin`。
    pub async fn seek_to_begin(&self, mq: &MessageQueue) -> Result<()> {
        let client = Self::require_client(&self.inner)?;
        let offset = client
            .get_min_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await?;
        self.seek(mq, offset);
        Ok(())
    }

    /// Python `seek_to_end`。
    pub async fn seek_to_end(&self, mq: &MessageQueue) -> Result<()> {
        let client = Self::require_client(&self.inner)?;
        let offset = client
            .get_max_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await?;
        self.seek(mq, offset);
        Ok(())
    }

    /// Python `committed`：Java `readOffset(MEMORY_FIRST_THEN_STORE)` —— 先看内存位点表
    /// （`persist=false` 刚提交、还没发给 broker 的值也算数），再问 broker 并回填进表里。
    /// broker 无记录 ⇒ `None`（对位 Java 的 -1）。
    pub async fn committed(&self, mq: &MessageQueue) -> Result<Option<i64>> {
        let key = mq_key(mq);
        if let Some(cached) = lock(&self.inner.state).offset_table.get(&key).copied() {
            return Ok(Some(cached));
        }
        let client = Self::require_client(&self.inner)?;
        let group = self.consumer_group();
        let broker_offset = client
            .query_consumer_offset(&group, mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None, false)
            .await?;
        if let Some(offset) = broker_offset {
            lock(&self.inner.state).offset_table.insert(key, offset);
        }
        Ok(broker_offset)
    }

    /// 到点提交：只有一道全局截止时刻（Java `maybeAutoCommit`），到点走一次
    /// [`commit`](Self::commit) 并把截止时刻推到 `now + auto_commit_interval_millis`。
    ///
    /// 调用点和 Java 一致，只有 [`poll`](Self::poll) 开头一处（外加 `shutdown` 的兜底提交）。
    /// Java 里空闲消费者靠 MQClientInstance 每 5s 的 `persistConsumerOffset` 定时器刷的是
    /// **内存位点表**，而那张表也只有 commit 路径会写，所以停掉 poll 之后 Java 同样不会
    /// 往前推进 broker 位点；这里把它塞进拉取循环就会变成"没人 poll 也提交"，比 Java 激进。
    async fn maybe_auto_commit(&self) {
        let interval_millis = self.config().auto_commit_interval_millis;
        let now = current_time_millis();
        {
            let mut state = lock(&self.inner.state);
            if now < state.next_auto_commit_deadline {
                return;
            }
            state.next_auto_commit_deadline = now + interval_millis;
        }
        if let Err(e) = self.commit().await {
            rmq_debug!("lite auto-commit failed: {e}");
        }
    }

    /// Python `commit(None)`：对位 Java `commitAll()` —— 按**已消费游标**提交全部已分配队列。
    ///
    /// 提交源绝不能是拉取游标：本地缓冲里压着没交出去的消息不算已消费，
    /// 提前提交会让那段消息在重启后永远不再投递（静默丢消息）。
    pub async fn commit(&self) -> Result<()> {
        let scope: Vec<String> = lock(&self.inner.state).assigned.keys().cloned().collect();
        let targets: BTreeMap<String, i64> = {
            let state = lock(&self.inner.state);
            scope
                .iter()
                .map(|k| {
                    (
                        k.clone(),
                        state.consume_offset.get(k).copied().unwrap_or(-1),
                    )
                })
                .collect()
        };
        self.commit_targets(targets, &scope, true).await
    }

    /// Python `commit({MessageQueue: offset}, persist)`：调用方指定位点。
    /// **只改提交落点，两条游标都不动**。空 map 对位 Java：记一条 warn 就 return，
    /// 连表都不碰（上一轮 `persist=false` 攒下的内存值原样保留）。
    pub async fn commit_offsets(
        &self,
        offsets: &BTreeMap<String, i64>,
        persist: bool,
    ) -> Result<()> {
        if offsets.is_empty() {
            rmq_warn!("MessageQueues is empty, Ignore this commit ");
            return Ok(());
        }
        let scope: Vec<String> = offsets.keys().cloned().collect();
        self.commit_targets(offsets.clone(), &scope, persist).await
    }

    /// Python `commit([MessageQueue, ...], persist)`：只提交点名这几条队列，
    /// 取的是它们当下的**已消费游标**。空集合对位 Java：静默 return。
    pub async fn commit_queues(
        &self,
        message_queues: &[MessageQueue],
        persist: bool,
    ) -> Result<()> {
        if message_queues.is_empty() {
            return Ok(());
        }
        let scope: Vec<String> = message_queues.iter().map(mq_key).collect();
        let targets: BTreeMap<String, i64> = {
            let state = lock(&self.inner.state);
            scope
                .iter()
                .map(|k| {
                    (
                        k.clone(),
                        state.consume_offset.get(k).copied().unwrap_or(-1),
                    )
                })
                .collect()
        };
        self.commit_targets(targets, &scope, persist).await
    }

    /// 三个入口的共同部分：写内存位点表（两道守卫），`persist` 再把这一批刷给 broker。
    ///
    /// 两处已知的偏离，与 Python 逐字对应（见 `consumer.py` 的 `commit`）：
    /// ① Java 的 `commitAll()` 只写内存表，真正发给 broker 靠 MQClientInstance 每
    ///    `persistConsumerOffsetInterval`（5s）一次的定时器；本端口的 lite 消费者没挂那个
    ///    定时器，所以 `persist=true`（默认）就地发出去。
    /// ② Java 的 `persistAll` 用 oneway、异常只记日志；这里发同步带应答，坏位点当场可见。
    async fn commit_targets(
        &self,
        targets: BTreeMap<String, i64>,
        scope: &[String],
        persist: bool,
    ) -> Result<()> {
        {
            let mut state = lock(&self.inner.state);
            for (key, offset) in &targets {
                if *offset == -1 {
                    // Java 原文：这条队列还没消费过，记 error 并跳过。绝不能把 -1 写给
                    // broker —— 位点 -1 会让下次消费从队首重投全量。
                    rmq_error!("consumerOffset is -1 in messageQueue [{key}].");
                    continue;
                }
                if !state.assigned.contains_key(key) {
                    // Java 的 `processQueue != null && !isDropped()` 守卫：不是本实例持有的
                    // 队列一律不替它提交，静默跳过（Java 原文这里连日志都没有）。
                    continue;
                }
                state.offset_table.insert(key.clone(), *offset);
            }
        }
        if !persist {
            return Ok(());
        }
        self.persist_offset_table(scope).await
    }

    /// Python `_persist_offset`：Java `OffsetStore#persist(mq)`，只把这一条队列的内存位点
    /// 发给 broker，不做清理。
    async fn persist_offset(&self, mq: &MessageQueue) {
        let key = mq_key(mq);
        let Some(offset) = lock(&self.inner.state).offset_table.get(&key).copied() else {
            return;
        };
        let Ok(client) = Self::require_client(&self.inner) else {
            return;
        };
        let group = self.consumer_group();
        if let Err(e) = client
            .update_consumer_offset(&group, mq, offset, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
        {
            rmq_debug!("lite persist failed for {mq:?}: {e}");
        }
    }

    /// Python `_persist_offset_table`：Java `RemoteBrokerOffsetStore#persistAll(Set)` ——
    /// 内存位点表里落在 `mqs` 上的那部分写给 broker，**不在**其中的条目顺手从表里删掉
    /// （Java 日志里那句 `remove unused mq`）。
    ///
    /// 后半句是 Java 的真实行为：这张表只服务于当下持有的队列。代价是
    /// `commit(部分队列, persist=true)` 会把其余队列**尚未落盘**的内存值一起丢掉 ——
    /// 要提交谁就一次给全。
    async fn persist_offset_table(&self, mqs: &[String]) -> Result<()> {
        if mqs.is_empty() {
            return Ok(());
        }
        let wanted: BTreeSet<String> = mqs.iter().cloned().collect();
        let to_send: Vec<(MessageQueue, i64)> = {
            let mut state = lock(&self.inner.state);
            let mut out = Vec::new();
            for key in state.offset_table.keys().cloned().collect::<Vec<String>>() {
                if !wanted.contains(&key) {
                    state.offset_table.remove(&key);
                    continue;
                }
                if let (Some(mq), Some(offset)) = (
                    state.assigned.get(&key),
                    state.offset_table.get(&key).copied(),
                ) {
                    out.push((mq.clone(), offset));
                }
            }
            out
        };
        // 未启动时表照样写得进去、只是发不出去（Python/C++ 同）：Java 在这里会先
        // checkServiceState 抛错，本端口放宽这一步，好让表逻辑能离线单测。
        // 这里直接读连接槽而不是 require_client()：shutdown 已经翻掉 started 标记，
        // 但末次提交仍要用这条还没关掉的连接。
        let Some(client) = lock(&self.inner.client).clone() else {
            return Ok(());
        };
        let group = self.consumer_group();
        let mut first_err = None;
        for (mq, offset) in to_send {
            if let Err(e) = client
                .update_consumer_offset(&group, &mq, offset, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
                .await
            {
                rmq_debug!("lite persist failed for {mq:?}: {e}");
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// 拉取游标（观测点：broker 侧流量分不出两条游标，真机对拍要看的就是这个数）。
    pub fn pull_cursor_of(&self, mq: &MessageQueue) -> i64 {
        lock(&self.inner.state)
            .next_offset
            .get(&mq_key(mq))
            .copied()
            .unwrap_or(-1)
    }

    /// 已消费游标（`poll()` 交付出去的那一格）。
    pub fn consume_cursor_of(&self, mq: &MessageQueue) -> i64 {
        lock(&self.inner.state)
            .consume_offset
            .get(&mq_key(mq))
            .copied()
            .unwrap_or(-1)
    }

    /// 内存位点表里那一格（`persist=false` 提交但还没落盘的值就在这）。
    pub fn pending_commit_of(&self, mq: &MessageQueue) -> i64 {
        lock(&self.inner.state)
            .offset_table
            .get(&mq_key(mq))
            .copied()
            .unwrap_or(-1)
    }

    /// Python `offset_for_timestamp`。
    pub async fn offset_for_timestamp(&self, mq: &MessageQueue, timestamp: i64) -> Result<i64> {
        let client = Self::require_client(&self.inner)?;
        client
            .search_offset_by_timestamp(mq, timestamp, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    /// Python `assignment`。
    pub fn assignment(&self) -> Vec<MessageQueue> {
        lock(&self.inner.state).assigned.values().cloned().collect()
    }

    /// 该 topic 的全部可消费队列（订阅口径：读位、不筛 master），topic 按命名空间拼好后查。
    ///
    /// 两件事必须一起做对，否则队列变更监听静默失真：
    /// 1. **每轮现问 name server**，不吃周期刷新的路由缓存；少了这一步，扩容最快也要等
    ///    一次路由轮询才看得见（监听回调比预期慢一个周期）。
    /// 2. 空队列集是**报错**，不是返回空表 —— "查不到" ≠ "这个 topic 缩到 0 队列"，
    ///    后者会让监听器收到一次假缩容回调并把快照刷成空集。
    pub async fn fetch_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = Self::require_client(&self.inner)?;
        let topic = with_namespace(&self.config().namespace, topic);
        if let Err(e) = client
            .update_topic_route_info_from_name_server(&topic, LITE_PULL_RPC_TIMEOUT_MILLIS, false)
            .await
        {
            rmq_debug!("fetch queues: route refresh for {} failed: {}", topic, e);
        }
        let queues = client.get_topic_subscribe_info(&topic).await;
        if queues.is_empty() {
            return Err(Error::client(format!(
                "Can not find Message Queue for this topic, {} Namesrv return empty",
                topic
            )));
        }
        Ok(queues)
    }

    /// Python `fetch_subscribe_message_queues`（lite 版 = `fetch_message_queues`）。
    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        self.fetch_message_queues(topic).await
    }

    /// Python `pause`。
    pub fn pause(&self, message_queues: &[MessageQueue]) {
        let mut state = lock(&self.inner.state);
        for mq in message_queues {
            state.paused.insert(mq_key(mq));
        }
    }

    /// Python `resume`。
    pub fn resume(&self, message_queues: &[MessageQueue]) {
        let mut state = lock(&self.inner.state);
        for mq in message_queues {
            state.paused.remove(&mq_key(mq));
        }
    }

    /// Java `DefaultLitePullConsumer.setTopicMetadataCheckIntervalMillis:563`。
    /// Java 没有下限，但 0 会让 `scheduleAtFixedRate` 抛，本端口与 cpp/python 同款
    /// 夹到 1s。
    pub fn set_topic_metadata_check_interval_millis(&self, millis: i64) {
        self.update_config(|c| c.topic_metadata_check_interval_millis = millis.max(1000));
    }

    /// 当前比对周期（毫秒）。
    pub fn topic_metadata_check_interval_millis(&self) -> i64 {
        self.config().topic_metadata_check_interval_millis
    }

    /// 对应 Java `registerTopicMessageQueueChangeListener`（`DefaultLitePullConsumer:325` →
    /// `DefaultLitePullConsumerImpl:1267-1279`）：登记一个 topic 的队列集合变更监听器，
    /// 后台循环（启动后 10s 首查、此后每 `topic_metadata_check_interval_millis` 一趟）
    /// 比对队列**集合**，有变化才回调 [`TopicMessageQueueChangeListener::on_changed`]。
    ///
    /// 与 Java 逐条对齐：
    /// * topic 为空或监听器为空 → 报错（Java 抛 `MQClientException("Topic or listener is null")`）；
    /// * 重复注册同一 topic → 覆盖旧监听器并 warn 一条（`:1272`）；
    /// * 键取套好命名空间的 topic（Java `:327` 在入口就 `withNamespace`），回调收到的也是它；
    /// * 已启动时立刻记一版快照（`:1275-1277`），否则首轮会把"当前集合"误报成变化。
    pub async fn register_topic_message_queue_change_listener(
        &self,
        topic: &str,
        listener: Arc<dyn TopicMessageQueueChangeListener>,
    ) -> Result<()> {
        if topic.trim().is_empty() {
            return Err(Error::client("Topic or listener is null"));
        }
        let key = with_namespace(&self.config().namespace, topic);
        {
            let mut listeners = lock(&self.inner.topic_listeners);
            if listeners.insert(key.clone(), listener).is_some() {
                rmq_warn!(
                    "Topic {} had been registered, new listener will overwrite the old one",
                    key
                );
            }
        }
        if !self.inner.started.load(Ordering::Acquire) {
            return Ok(());
        }
        match self.fetch_message_queues(topic).await {
            Ok(queues) => {
                lock(&self.inner.queues_for_topic).insert(key, queues);
            }
            Err(e) => rmq_debug!(
                "register listener: fetch queues for {} failed: {}",
                key,
                e
            ),
        }
        Ok(())
    }

    /// 跑一轮比对（后台循环调它，测试与调用方也可以直接驱动），返回回调触发了几次。
    ///
    /// ⚠ 有意做法：单个 topic 失败只记日志、跳过它自己，同轮其余 topic 照常比对。
    /// 一个长期查不到路由的 topic 不该把排在它后面的监听器永久饿死；下一轮照常，
    /// 两种写法都不打断调度。
    pub async fn fetch_topic_message_queues_and_compare(&self) -> usize {
        let entries: Vec<(String, Arc<dyn TopicMessageQueueChangeListener>)> = lock(
            &self.inner.topic_listeners,
        )
        .iter()
        .map(|(t, l)| (t.clone(), l.clone()))
        .collect();
        let mut fired = 0usize;
        for (key, listener) in entries {
            let queues = match self.fetch_message_queues(&key).await {
                Ok(q) => q,
                Err(e) => {
                    rmq_error!(
                        "ScheduledTask fetchMessageQueuesAndCompare for {} failed: {}",
                        key,
                        e
                    );
                    continue;
                }
            };
            let changed = {
                let mut snapshots = lock(&self.inner.queues_for_topic);
                let changed = match snapshots.get(&key) {
                    // 没有快照 = Java 的 `oldSet == null`：`fetchMessageQueues` 永远回
                    // 一个非 null 集合，所以 isSetEqual 必判不等 ⇒ 首轮一定回调一次。
                    None => true,
                    Some(old) => !same_queue_set(old, &queues),
                };
                if changed {
                    snapshots.insert(key.clone(), queues.clone());
                }
                changed
            };
            if changed {
                listener.on_changed(&key, &queues);
                fired += 1;
            }
        }
        fired
    }

    /// 本地缓冲当前条数（Python 直接读 `len(_local_buffer)`；测试与观测用）。
    pub fn buffered_message_count(&self) -> usize {
        lock(&self.inner.buffer).len()
    }

    // ---------------- 拉取服务的内部件 ----------------

    /// Python `_subscription_for`：subscribe 表 → assign 表达式 → `"*"`。
    fn subscription_for(&self, topic: &str) -> String {
        let state = lock(&self.inner.state);
        if let Some(expr) = state.subscription.get(topic) {
            return expr.clone();
        }
        if let Some(expr) = state.assign_sub_expr.get(topic) {
            return expr.clone();
        }
        FilterAPI::SUB_ALL.to_string()
    }

    /// Python `_resolve_initial_offset` 的批量版：逐队列解析，失败只记日志
    /// （Python 在 `assign`/`rebalance`/首拉三处都 `try/except` 吞掉）。
    async fn resolve_offsets_for(&self, mqs: &[MessageQueue]) {
        let Ok(client) = Self::require_client(&self.inner) else {
            return;
        };
        for mq in mqs {
            let key = mq_key(mq);
            if lock(&self.inner.state).next_offset.contains_key(&key) {
                continue;
            }
            match resolve_initial_offset(&self.inner, &client, mq).await {
                Ok(off) => {
                    lock(&self.inner.state).next_offset.insert(key, off);
                }
                Err(e) => rmq_debug!("lite: resolve initial offset failed for {mq:?}: {e}"),
            }
        }
    }

    /// Python `_rebalance`（subscribe 模式）：逐 topic 取全部队列 + 消费组实例列表
    /// → 分配策略 → 并集。查不到实例列表时**保留当前分配**（Python 的
    /// `or []` + 自己补进列表，等价于「只有我一人时独占」；这里与推送消费者
    /// 同口径，见 [`crate::client::consumer::DefaultMQPushConsumer`] 的说明）。
    pub async fn rebalance(&self) {
        let Ok(client) = Self::require_client(&self.inner) else {
            return;
        };
        let group = self.consumer_group();
        let client_id = self.client_id();
        let topics: Vec<String> = lock(&self.inner.state)
            .subscription
            .keys()
            .cloned()
            .collect();
        let strategy = self
            .inner
            .strategy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        let mut new_assigned: BTreeMap<String, MessageQueue> = BTreeMap::new();
        // 本轮的分配起点：本 topic 当前的分配。策略抛错时以它兜底——
        // Java `RebalanceImpl#rebalanceByTopic` 在 `catch (Throwable)` 里直接 `return false`，
        // 位置在 `updateProcessQueueTableInRebalance` **之前**，所以一次分配异常不会把队列撤走。
        let baseline: Vec<MessageQueue> =
            lock(&self.inner.state).assigned.values().cloned().collect();
        // topic -> (全部队列, 分到的队列)，用于 MessageQueueListener 回调
        let mut per_topic: Vec<(String, Vec<MessageQueue>, Vec<MessageQueue>)> = Vec::new();
        for topic in topics {
            // 与 push 的 rebalance 同源：Java DefaultLitePullConsumerImpl 走的是同一个
            // RebalanceImpl，mqAll 来自订阅信息（读位、不筛 master），不是发布信息。
            let mut mq_all: Vec<MessageQueue> = client.get_topic_subscribe_info(&topic).await;
            if mq_all.is_empty() {
                rmq_debug!("lite rebalance: no subscribe info for topic {topic}");
            }
            sort_mqs(&mut mq_all);
            let mut cid_all = client
                .get_consumer_id_list_by_group(&topic, &group, LITE_PULL_RPC_TIMEOUT_MILLIS)
                .await
                .unwrap_or_default();
            if !cid_all.contains(&client_id) {
                cid_all.push(client_id.clone());
            }
            cid_all.sort();
            let allocated: Vec<MessageQueue> =
                match strategy.allocate(&group, &client_id, &mq_all, &cid_all) {
                    Ok(got) => got,
                    Err(e) => {
                        rmq_warn!("lite rebalance: allocate failed for {topic}: {e}");
                        baseline
                            .iter()
                            .filter(|mq| mq.topic == topic)
                            .cloned()
                            .collect()
                    }
                };
            for mq in &allocated {
                new_assigned.insert(mq_key(mq), mq.clone());
            }
            per_topic.push((topic, mq_all, allocated));
        }

        let (added, changed_topics, revoked) = {
            let mut state = lock(&self.inner.state);
            // 先快照旧的分配：`state.assigned` 之后要被整体替换，借用不能跨过那次赋值。
            let old: BTreeMap<String, MessageQueue> = state.assigned.clone();
            let added: Vec<MessageQueue> = new_assigned
                .keys()
                .filter(|k| !old.contains_key(*k))
                .filter_map(|k| new_assigned.get(k))
                .cloned()
                .collect();
            let removed: Vec<String> = old
                .keys()
                .filter(|k| !new_assigned.contains_key(*k))
                .cloned()
                .collect();
            let changed = !added.is_empty() || !removed.is_empty();
            // 撤销的队列要连着整份 MessageQueueState 一起丢（Java removeUnnecessaryMessageQueue
            // = persist(mq) 再 removeOffset(mq)）。persist 是 RPC，抱着锁做网络会把整条
            // poll/commit 路径卡住，所以这里只把**该补发的队列**记下来，锁外再发。
            let revoked: Vec<MessageQueue> = removed
                .iter()
                .filter(|k| state.offset_table.contains_key(*k))
                .filter_map(|k| old.get(k))
                .cloned()
                .collect();
            if changed {
                state.assigned = new_assigned;
                for key in &removed {
                    state.next_offset.remove(key);
                    state.consume_offset.remove(key);
                    state.seek_offset.remove(key);
                    // ⚠ `offset_table` 那一格**留到 persist 发出去之后再清**：
                    // Java 的顺序就是 removeUnnecessaryMessageQueue = persist(mq) → removeOffset(mq)。
                }
            }
            let changed_topics: Vec<(String, Vec<MessageQueue>, Vec<MessageQueue>)> = if changed {
                per_topic
                    .into_iter()
                    .filter(|(t, _, divided)| {
                        !divided.is_empty() || state.assigned.values().any(|mq| &mq.topic == t)
                    })
                    .collect()
            } else {
                Vec::new()
            };
            (added, changed_topics, revoked)
        };

        // 撤手之后补发最后那次提交（Java 的 persist 在 removeOffset **之前**：上面刻意把
        // offset_table 那一格留着就是为了这一步还能读到值）。
        for mq in &revoked {
            self.persist_offset(mq).await;
        }
        if !revoked.is_empty() {
            let mut state = lock(&self.inner.state);
            for mq in &revoked {
                state.offset_table.remove(&mq_key(mq));
            }
        }

        if !added.is_empty() {
            self.resolve_offsets_for(&added).await;
        }
        // 回调按 topic 逐个发（Java 签名；Python 的调用是死代码，见模块头偏离 5）
        if !changed_topics.is_empty() {
            let listener = lock(&self.inner.listener).clone();
            if let Some(listener) = listener {
                for (topic, mq_all, divided) in changed_topics {
                    listener.message_queue_changed(&topic, &mq_all, &divided);
                }
            }
        }
    }

    /// Python `_pull_one`：短轮询拉一批，命中则过滤 tag → 灌缓冲 → 推游标 →
    /// 节流 auto-commit。返回 `true` = 本轮有消息进缓冲（退避判断用）。
    async fn pull_one(&self, mq: &MessageQueue) -> bool {
        let Ok(client) = Self::require_client(&self.inner) else {
            return false;
        };
        let key = mq_key(mq);
        // 先取快照再 await：MutexGuard 不是 Send，直接把 lock(...) 写在 match 的
        // scrutinee 里会让整个 future 不 Send（临时量活到 match 结束）。
        let known_offset = { lock(&self.inner.state).next_offset.get(&key).copied() };
        let offset = match known_offset {
            Some(offset) => offset,
            None => match resolve_initial_offset(&self.inner, &client, mq).await {
                Ok(offset) => {
                    lock(&self.inner.state)
                        .next_offset
                        .insert(key.clone(), offset);
                    offset
                }
                Err(e) => {
                    rmq_debug!("lite: resolve offset failed for {mq:?}: {e}");
                    return false;
                }
            },
        };
        let cfg = self.config();
        let expr = self.subscription_for(&mq.topic);
        let result = match client
            .pull_message(
                &cfg.consumer_group,
                mq,
                offset,
                cfg.pull_batch_size,
                // 短轮询（suspend=False），位点由 auto-commit 单独提交；
                // lite 位见 lite_pull_sys_flag（#107）
                lite_pull_sys_flag(),
                0,
                &expr,
                0,
                ExpressionType::TAG,
                LITE_PULL_TIMEOUT_MILLIS,
                -1,
                PULL_SUSPEND_TIMEOUT_MILLIS,
                None,
                0,
                None,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                rmq_debug!("lite pull failed for {mq:?}@{offset}: {e}");
                return false;
            }
        };
        // Java `DefaultLitePullConsumerImpl#PullTaskImpl.run:982-998`：一轮拉取回来之后
        // 无论 FOUND / NO_NEW_MSG / NO_MATCHED_MSG / OFFSET_ILLEGAL，都把拉取游标推进到
        // broker 给的 nextBeginOffset —— NO_MATCHED_MSG 的已越过本轮扫过的整段不匹配区间
        // （不跟就会每轮把同一段重扫一遍），OFFSET_ILLEGAL 的是 broker 的纠正位点
        // （越界自愈也靠这一步）。
        // 刹车只有一只：**在途请求的结果不许盖掉这轮里刚 seek / 刚被撤走的位点**
        // （Java :808 的 seekOffset == -1 检查 + :979 的 isDropped 检查；本端 seek() 直接
        // 改写 next_offset、撤队列直接 remove 掉条目，用「游标还是不是我发请求时的那个值」
        // 做同一件事）。同一条刹车也管着 FOUND 分支的入缓冲（Java :986）。
        let intact = {
            let mut state = lock(&self.inner.state);
            if state.next_offset.get(&key).copied() == Some(offset) {
                state
                    .next_offset
                    .insert(key.clone(), result.next_begin_offset);
                true
            } else {
                false
            }
        };
        if !intact || result.status != PullStatus::Found || result.msg_found_list.is_empty() {
            return false;
        }
        // Python `_filter_tags`：按表达式现算 tagsSet 再筛（无集合 = 不筛）
        let sub = FilterAPI::build_subscription_data(&mq.topic, Some(&expr)).ok();
        let msgs = client_side_tag_filter(sub.as_ref(), result.msg_found_list);
        if msgs.is_empty() {
            // 全被 tag 过滤掉：窗口在 broker 眼里已经读过（游标上面推过了），
            // 只是没有可交付的——不能像旧实现那样原地重拉同一窗口。
            return false;
        }
        // 只推进**拉取游标**。「已消费游标」是 poll() 交付时才写的，两条线不是一条：
        // 缓冲里压着没交出去的消息不能算已消费（Java `processQueue.removeMessage` 同口径）。
        self.enqueue(msgs);
        true
    }

    fn enqueue(&self, msgs: Vec<MessageExt>) {
        let mut buffer = lock(&self.inner.buffer);
        buffer.extend(msgs);
        drop(buffer);
        self.inner.buffer_signal.notify_waiters();
    }
}

/// Python `_resolve_initial_offset`：`seek` → broker 已提交位点（保证重启续消费）
/// → `CONSUME_FROM_FIRST_OFFSET` ⇒ min → `CONSUME_FROM_TIMESTAMP` ⇒ 按时间查 → 否则 max。
///
/// 与推送消费者的 `resolve_initial_offset` 不同：lite 没有广播模式的本地位点存储、
/// 没有 SQL92 特例，也不给 `%RETRY%` 特殊起步（Python 亦无，且 lite 不自动订阅重试主题）。
async fn resolve_initial_offset(
    inner: &LiteInner,
    client: &MQClientInstance,
    mq: &MessageQueue,
) -> Result<i64> {
    let cfg = read_cfg(&inner.cfg);
    let key = mq_key(mq);
    if let Some(offset) = lock(&inner.state).seek_offset.get(&key).copied() {
        return Ok(offset);
    }
    match client
        .query_consumer_offset(
            &cfg.consumer_group,
            mq,
            LITE_PULL_RPC_TIMEOUT_MILLIS,
            None,
            false,
        )
        .await
    {
        Ok(Some(offset)) => return Ok(offset),
        Ok(None) => {}
        Err(e) => rmq_debug!("lite query offset failed for {mq:?}: {e}"),
    }
    if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET {
        // Java RebalanceLitePullImpl:FIRST_OFFSET 分支与 push 同形（`result = 0L`），
        // 不发 minOffset 查询 —— minOffset 属于 MQAdminImpl 口径（只认 master），
        // 主掉线期间会让新起的 lite-pull 一条都拉不到；越界由 broker 的
        // PULL_OFFSET_MOVED 纠正（见 `handle_offset_illegal`）。
        return Ok(0);
    }
    if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_TIMESTAMP {
        // 与推送消费者共用解析器：两种消费者的 consumeTimestamp 语义必须一致
        let ts = consume_timestamp_millis(&cfg.consume_timestamp)?;
        return client
            .search_offset_by_timestamp(mq, ts, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await;
    }
    client
        .get_max_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
        .await
}

/// Python `_heartbeat_loop` 里那份报文：只带**本消费者自己**的 ConsumerData
/// （lite 不进实例注册表，报文由自己拼）。
///
/// ⚠ `consumeType` 用 **CONSUME_ACTIVELY**：Java
/// `DefaultLitePullConsumerImpl.consumeType():1111-1112` 恒返回它（5.4.0 起；此前与
/// 推送同用 PASSIVELY 是错的口径）。broker 侧有三个读者：
/// `ClientManageProcessor:87-92` 对 ACTIVELY 的心跳跳过订阅注册（拉取靠自己带
/// subscription 标志走补偿分支）、`PullMessageProcessor:493-505` 对开了
/// `rejectPullConsumerEnabled` 的 broker 按它放行/拒绝拉取、`AdminBrokerProcessor:1971`
/// 按它显示消费类型（mqadmin consumerConnection）。Python/C++/C# 三版同值。
fn build_lite_heartbeat(inner: &LiteInner) -> HeartbeatData {
    let cfg = read_cfg(&inner.cfg);
    let mut hb = HeartbeatData::new(cfg.client_id.clone().unwrap_or_default());
    let mut cd = crate::remoting::protocol::heartbeat::ConsumerData::new(
        cfg.consumer_group,
        ConsumeType::CONSUME_ACTIVELY,
        cfg.message_model,
        cfg.consume_from_where,
    );
    cd.subscription_data_set = lock(&inner.state)
        .subscription_data
        .values()
        .cloned()
        .collect();
    // Java `MQClientInstance:1039`：心跳里的 unitMode 来自消费者自己的 ClientConfig
    cd.unit_mode = cfg.unit_mode;
    hb.consumer_data_set.push(cd);
    hb
}

/// Python `_send_heartbeat_to_all_broker`：路由里的每个 broker（**含从节点**）发一份。
/// 没有 client（未 start / 已 shutdown）就发 0 份而不是退出循环 —— Python 同。
///
/// 从节点也要发：Java `MQClientInstance#sendHeartbeatToAllBroker`:732-750 遍历
/// `brokerAddrTable` 的每个 brokerId，仅在 `consumerEmpty && id != MASTER_ID` 时跳过；
/// 本心跳带 ConsumerData，故不跳。broker 的 ConsumerManager 每台各自一份，从节点收不到
/// 心跳就会对指向自己的拉取回 `SUBSCRIPTION_NOT_EXIST`（`PullMessageProcessor`:420-427）。
async fn send_lite_heartbeat(inner: &LiteInner) -> usize {
    let Some(client) = lock(&inner.client).clone() else {
        return 0;
    };
    let hb = build_lite_heartbeat(inner);
    let mut ok = 0;
    for addr in client.get_all_broker_addrs() {
        match client
            .send_heartbeat(&addr, &hb, LITE_HEARTBEAT_TIMEOUT_MILLIS)
            .await
        {
            Ok(()) => ok += 1,
            Err(e) => rmq_debug!("lite heartbeat to {addr} failed: {e}"),
        }
    }
    ok
}

/// Python `_heartbeat_loop`：先发一轮（start 里已同步发过一轮，这里紧跟着第二次，
/// 与 Python 同序），之后每 5s 一轮。
async fn heartbeat_loop(inner: Arc<LiteInner>, mut rx: watch::Receiver<bool>) {
    while inner.running.load(Ordering::Acquire) {
        if *rx.borrow_and_update() {
            return;
        }
        send_lite_heartbeat(&inner).await;
        if wait_or_stop(&mut rx, LITE_HEARTBEAT_INTERVAL_MILLIS).await {
            return;
        }
    }
}

/// 启动后 10s 首查，此后每 `topic_metadata_check_interval_millis` 一趟比对队列集合变更。
async fn metadata_loop(inner: Arc<LiteInner>, rx: watch::Receiver<bool>) {
    metadata_loop_after(&inner, rx, LITE_METADATA_FIRST_DELAY_MILLIS).await;
}

/// `metadata_loop` 但首查延迟可注入，单测用它把 10s 缩成几十毫秒。
///
/// ⚠ 首查延迟只在**第一趟之前**生效。把它留在循环里，每趟都会先等满 10s，
/// 1s 的检查周期会被拖成 11s —— 扩容后监听器要晚一个数量级才动。
async fn metadata_loop_after(
    inner: &Arc<LiteInner>,
    mut rx: watch::Receiver<bool>,
    first_delay_millis: u64,
) {
    let consumer = DefaultLitePullConsumer {
        inner: inner.clone(),
    };
    if wait_or_stop(&mut rx, first_delay_millis).await {
        return;
    }
    while inner.running.load(Ordering::Acquire) {
        consumer.fetch_topic_message_queues_and_compare().await;
        let period = consumer
            .config()
            .topic_metadata_check_interval_millis
            .max(1000) as u64;
        if wait_or_stop(&mut rx, period).await {
            return;
        }
    }
}

/// Python `_pull_service_loop`：单循环轮转所有已分配队列。
/// subscribe 模式下每 >1s 重平衡一次；一轮里没有任何消息则退避
/// `pull_interval_millis`，有消息则 5ms 后立刻续拉。
async fn pull_service_loop(inner: Arc<LiteInner>, mut rx: watch::Receiver<bool>) {
    while inner.running.load(Ordering::Acquire) {
        if *rx.borrow_and_update() {
            return;
        }
        let consumer = DefaultLitePullConsumer {
            inner: inner.clone(),
        };
        let (assign_mode, last_ts) = {
            let state = lock(&inner.state);
            (state.assign_mode, state.last_rebalance_ts)
        };
        let now = current_time_millis();
        if !assign_mode && (last_ts == 0 || now - last_ts > LITE_REBALANCE_INTERVAL_MILLIS) {
            consumer.rebalance().await;
            lock(&inner.state).last_rebalance_ts = current_time_millis();
        }
        let targets: Vec<MessageQueue> = {
            let state = lock(&inner.state);
            state
                .assigned
                .values()
                .filter(|mq| !state.paused.contains(&mq_key(mq)))
                .cloned()
                .collect()
        };
        let mut got_any = false;
        for mq in targets {
            if !inner.running.load(Ordering::Acquire) {
                return;
            }
            if consumer.pull_one(&mq).await {
                got_any = true;
            }
        }
        let backoff = if got_any {
            5
        } else {
            read_cfg(&inner.cfg).pull_interval_millis.max(0) as u64
        };
        if wait_or_stop(&mut rx, backoff).await {
            return;
        }
    }
}

/// `mq_key` 的排序键形式，供 `BTreeMap` 键序与队列顺序保持一致性检查用。
fn _key_order_proof(mq: &MessageQueue) -> (String, String, i32) {
    mq_sort_key(mq)
}

#[cfg(test)]
mod tests {
    use super::*;
    // 心跳真机外的最小闭环（假 namesrv + 假 broker）：帧读写与 producer 的
    // `send_retry_tests` 同款，但只关心 34/35 两号报文。
    use crate::common::message_decoder::encode_message_ext;
    use crate::common::sysflag::PermName;
    use crate::remoting::protocol::body::GetConsumerListByGroupResponseBody;
    use crate::remoting::protocol::codes::{request_code, response_code};
    use crate::remoting::protocol::route::{BrokerData, QueueData, TopicRouteData};
    use crate::remoting::protocol::RemotingCommand;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    fn queue(topic: &str, broker: &str, id: i32) -> MessageQueue {
        MessageQueue::new(topic, broker, id)
    }

    fn msg(topic: &str, broker: &str, id: i32, offset: i64, body: &str) -> MessageExt {
        let mut m = MessageExt::new();
        m.topic = topic.to_string();
        m.broker_name = Some(broker.to_string());
        m.queue_id = id;
        m.queue_offset = offset;
        m.body = Some(body.as_bytes().to_vec());
        m
    }

    // ---------------------------------------------------------- 生命周期守卫

    #[test]
    fn blank_consumer_group_is_rejected_by_both_consumers() {
        for group in ["", "   "] {
            assert!(DefaultMQPullConsumer::new(group).is_err());
            assert!(DefaultLitePullConsumer::new(group).is_err());
        }
        assert!(DefaultMQPullConsumer::new("PG").is_ok());
        assert!(DefaultLitePullConsumer::new("PG").is_ok());
    }

    /// Java 的 `checkConfig` 是 start() 第一步，所以组名校验排在地址/订阅/时间戳之前：
    /// 地址配好、订阅齐了，也照样因为组名本地失败（`127.0.0.1:1` 兜底，跑偏了也不会
    /// 打到真集群）。
    #[tokio::test]
    async fn both_pull_consumers_reject_bad_group_before_the_other_gates() {
        let long_group = std::iter::repeat_n('g', 121).collect::<String>();
        for (group, needle) in [
            (
                MixAll::DEFAULT_CONSUMER_GROUP,
                "consumerGroup can not equal DEFAULT_CONSUMER",
            ),
            ("bad group", "contains illegal characters"),
            (long_group.as_str(), "is longer than group max length"),
        ] {
            let pull = DefaultMQPullConsumer::new(group).expect("构造不该提前拒绝");
            pull.set_namesrv_addr("127.0.0.1:1");
            let err = pull.start().await.expect_err("pull: 非法组名必须本地失败");
            assert!(err.to_string().contains(needle), "pull {group}: {err}");
            assert!(
                !pull.is_started(),
                "pull {group}: 失败的 start 必须回滚 started"
            );

            let lite = DefaultLitePullConsumer::new(group).expect("构造不该提前拒绝");
            lite.set_namesrv_addr("127.0.0.1:1");
            lite.subscribe("T", "TagA");
            let err = lite.start().await.expect_err("lite: 非法组名必须本地失败");
            assert!(err.to_string().contains(needle), "lite {group}: {err}");
            assert!(
                !lite.is_started(),
                "lite {group}: 失败的 start 必须回滚 started"
            );
        }
    }

    #[tokio::test]
    async fn pull_consumer_rejects_start_without_namesrv_and_use_before_start() {
        let c = DefaultMQPullConsumer::new("PG").unwrap();
        assert!(c.start().await.is_err());
        let mq = queue("T", "broker-a", 0);
        // 未 start 的每个入口都要给出同一句错误，而不是 panic / 空转
        for label in ["pull", "offset"] {
            let r = match label {
                "pull" => c
                    .pull(&mq, "*", 0, 32, None)
                    .await
                    .map(|_| ())
                    .err()
                    .map(|e| e.to_string()),
                _ => c
                    .fetch_consume_offset(&mq)
                    .await
                    .err()
                    .map(|e| e.to_string()),
            };
            assert_eq!(
                r,
                Some("MQClientException: consumer not started, call start() first".to_string()),
                "{label}"
            );
        }
        assert!(c.max_offset(&mq).await.is_err());
        assert!(c.min_offset(&mq).await.is_err());
        assert!(c.search_offset(&mq, 0).await.is_err());
        assert!(c.earliest_msg_store_time(&mq).await.is_err());
        assert!(c.create_topic("t2", 4, 0).await.is_err());
        assert!(c.update_consume_offset(&mq, 0).await.is_err());
        assert!(c.fetch_subscribe_message_queues("T").await.is_err());
        assert!(c
            .send_message_back(&msg("T", "broker-a", 0, 0, "b"), 3)
            .await
            .is_err());
        // 未 start 时 shutdown 是 no-op，不能抛
        c.shutdown();
        assert!(!c.is_started());
    }

    #[tokio::test]
    async fn lite_consumer_requires_subscription_or_assign_before_start() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.set_namesrv_addr("127.0.0.1:9876");
        let e = c
            .start()
            .await
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            e.contains("subscription is not set, call subscribe() or assign() first"),
            "{e}"
        );
        // 缺 name server 的校验在前，且顺序与 Python 一致
        let c2 = DefaultLitePullConsumer::new("LitePG").unwrap();
        c2.subscribe("T", "*");
        let e2 = c2
            .start()
            .await
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(e2.contains("name server address is not set"), "{e2}");
        c2.shutdown();
    }

    // ---------------------------------------------------------- 配置形状

    #[test]
    fn config_defaults_match_the_python_reference() {
        let p = PullConsumerConfig::default();
        assert_eq!(p.instance_name, "DEFAULT");
        assert_eq!(p.message_model, MessageModel::CLUSTERING);
        assert_eq!(p.broker_suspend_max_time_millis, 20_000);
        assert_eq!(p.consumer_pull_timeout_millis, 10_000);
        assert_eq!(p.consumer_timeout_millis_when_suspend, 30_000);
        assert!(p.client_id.is_none());
        assert!(p.name_server_addrs.is_empty());

        let l = LitePullConsumerConfig::default();
        assert!(l.auto_commit);
        assert_eq!(l.pull_batch_size, 32);
        assert_eq!(l.poll_timeout_millis, 5_000);
        assert_eq!(l.auto_commit_interval_millis, 5_000);
        assert_eq!(l.pull_interval_millis, 50);
        assert_eq!(l.pull_thread_nums, 1);
        assert_eq!(
            l.consume_from_where,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET
        );
        // Java DefaultLitePullConsumer.java:168：默认 now-30min 的 14 位 yyyyMMddHHmmss
        assert_eq!(l.consume_timestamp.len(), 14);
        assert!(l.consume_timestamp.bytes().all(|b| b.is_ascii_digit()));
        assert_eq!(l.broker_suspend_max_time_millis, 20_000);
        assert_eq!(l.consumer_timeout_millis_when_suspend, 30_000);
    }

    #[test]
    fn setters_clamp_like_the_python_setters() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.set_pull_batch_size(0);
        assert_eq!(c.config().pull_batch_size, 1);
        c.set_pull_batch_size(-5);
        assert_eq!(c.config().pull_batch_size, 1);
        c.set_poll_timeout_millis(-1);
        assert_eq!(c.config().poll_timeout_millis, 0);
        c.set_auto_commit_interval_millis(-1);
        assert_eq!(c.config().auto_commit_interval_millis, 0);
        c.set_pull_interval_millis(-1);
        assert_eq!(c.config().pull_interval_millis, 0);
        c.set_auto_commit(false);
        assert!(!c.config().auto_commit);
    }

    #[test]
    fn namesrv_addr_splits_on_semicolon() {
        let c = DefaultMQPullConsumer::new("PG").unwrap();
        c.set_namesrv_addr(" 127.0.0.1:9876 ; 127.0.0.1:9877 ;; ");
        assert_eq!(
            c.config().name_server_addrs,
            vec!["127.0.0.1:9876".to_string(), "127.0.0.1:9877".to_string()]
        );
    }

    // ---------------------------------------------------------- 拉取标志位

    /// Python 用 `inspect.getsource` 断言 `suspend=False`；这里直接测位本身。
    #[test]
    fn pull_is_short_poll_and_block_is_long_poll() {
        let short = pull_sys_flag(false);
        assert!(!PullSysFlag::has_commit_offset_flag(short));
        assert!(!PullSysFlag::has_suspend_flag(short));
        assert!(PullSysFlag::has_subscription_flag(short));
        assert!(!PullSysFlag::has_class_filter_flag(short));

        let long_ = pull_sys_flag(true);
        assert!(!PullSysFlag::has_commit_offset_flag(long_));
        assert!(PullSysFlag::has_suspend_flag(long_));
    }

    #[test]
    fn subscription_data_string_falls_back_to_sub_all() {
        let sub = FilterAPI::build_subscription_data("T", Some("*")).unwrap();
        assert_eq!(sub_expression_of(&sub), "*");
        let tagged = FilterAPI::build_subscription_data("T", Some("TagA || TagB")).unwrap();
        assert_eq!(sub_expression_of(&tagged), "TagA || TagB");
    }

    // ---------------------------------------------------------- 订阅 / 分配表

    #[test]
    fn subscriptions_are_namespaced_and_overwritable() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.set_namespace("NS1");
        c.subscribe("Topic", "TagA");
        let subs = c.subscription();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].0, "NS1%Topic");
        assert_eq!(c.subscriptions()[0].tags_set, vec!["TagA".to_string()]);
        // 同 topic 再订阅是覆盖，不追加
        c.subscribe("Topic", "TagB");
        assert_eq!(c.subscription().len(), 1);
        assert_eq!(c.subscription()[0].1, "TagB");
        c.unsubscribe("Topic");
        assert!(c.subscription().is_empty());
        assert!(c.subscriptions().is_empty());
    }

    #[test]
    fn bad_tag_expression_drops_only_subscription_data() {
        // `"||"` 会让 FilterAPI 抛 `subString split error`：Python 保留
        // `subscription` 条目、丢掉 `subscription_data`（consumer.py:2409-2412）
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.subscribe("T", "||");
        assert_eq!(c.subscription().len(), 1);
        assert!(c.subscriptions().is_empty());
    }

    #[test]
    fn assign_switches_mode_and_keeps_subscription_expr_for_heartbeat() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.set_sub_expression_for_assign("T", "TagA");
        assert!(!c.is_assign_mode());
        c.assign(&[queue("T", "broker-a", 1), queue("T", "broker-a", 0)]);
        assert!(c.is_assign_mode());
        // 分配快照按 key 有序（Python 是 set，遍历序不定）
        assert_eq!(
            c.assignment(),
            vec![queue("T", "broker-a", 0), queue("T", "broker-a", 1)]
        );
        assert_eq!(c.subscription_for("T"), "TagA");
        assert_eq!(c.subscription_for("other"), "*");
        assert_eq!(c.subscriptions().len(), 1);
    }

    // ---------------------------------------------------- 三张位点表（#68）

    #[tokio::test]
    async fn poll_advances_consume_cursor_not_pull_cursor() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        let q = queue("T", "broker-a", 0);
        // 拉取游标由后台拉取推进：单测里没有网络，手工写成 5（缓冲里只有 3 条交付过）
        {
            let mut state = lock(&c.inner.state);
            state.assigned.insert(mq_key(&q), q.clone());
            state.next_offset.insert(mq_key(&q), 5);
        }
        assert_eq!(c.pull_cursor_of(&q), 5, "拉取游标 = 后台已经拉到的那一格");
        assert_eq!(
            c.consume_cursor_of(&q),
            -1,
            "一条都没交付 ⇒ 已消费游标还是 -1"
        );
        // 缓冲里压着 3 条：交付之前不算已消费
        c.enqueue(
            (0..3)
                .map(|i| msg("T", "broker-a", 0, i, &format!("m{i}")))
                .collect(),
        );
        assert_eq!(c.consume_cursor_of(&q), -1, "没 poll 就不算已消费");
        let got = c.poll(Some(10)).await;
        assert_eq!(got.len(), 3);
        assert_eq!(c.consume_cursor_of(&q), 3, "交出去的那一格才是已消费");
        assert_eq!(c.pull_cursor_of(&q), 5, "poll 绝不改拉取游标");
    }

    #[tokio::test]
    async fn poll_ignores_queues_this_instance_does_not_hold() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        // 没有拉取记录（未分配）的队列：交付了也不写游标（Java 的 MessageQueueState==null）
        c.enqueue(vec![msg("T", "broker-a", 9, 0, "m")]);
        assert_eq!(c.poll(Some(10)).await.len(), 1);
        assert_eq!(c.consume_cursor_of(&queue("T", "broker-a", 9)), -1);
    }

    #[tokio::test]
    async fn commit_writes_only_what_poll_handed_out() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        let q0 = queue("T", "broker-a", 0);
        let q1 = queue("T", "broker-a", 1);
        c.assign(&[q0.clone(), q1.clone()]);
        // commitAll 走的是已消费游标：一条都没交付 ⇒ 一格都不写
        c.commit().await.unwrap();
        assert_eq!(
            c.pending_commit_of(&q0),
            -1,
            "commitAll 不提交没消费过的队列"
        );

        // 指定位点只写提交落点，两条游标一律不动
        let mut specified = BTreeMap::new();
        specified.insert(mq_key(&q0), 5);
        specified.insert(mq_key(&q1), 8);
        c.commit_offsets(&specified, false).await.unwrap();
        assert_eq!(c.pending_commit_of(&q0), 5, "指定位点进内存位点表");
        assert_eq!(c.pending_commit_of(&q1), 8, "指定位点进内存位点表(q1)");
        assert_eq!(c.pull_cursor_of(&q0), -1, "提交位点不改拉取游标");
        assert_eq!(c.consume_cursor_of(&q0), -1, "提交位点不改已消费游标");

        // -1 与「不是本实例持有的队列」两道守卫（Java 的 log.error + processQueue 守卫）
        let mut guarded = BTreeMap::new();
        guarded.insert(mq_key(&q0), -1);
        guarded.insert(mq_key(&queue("T", "broker-a", 7)), 3);
        c.commit_offsets(&guarded, false).await.unwrap();
        assert_eq!(
            c.pending_commit_of(&q0),
            5,
            "offset == -1 只记日志，不覆盖已有位点"
        );
        assert_eq!(
            c.pending_commit_of(&queue("T", "broker-a", 7)),
            -1,
            "没分配到的队列不替它提交"
        );

        // 空 map / 空集合：Java 都是直接 return，连表都不碰
        c.commit_offsets(&BTreeMap::new(), false).await.unwrap();
        assert_eq!(c.pending_commit_of(&q0), 5, "空 map 忽略这次提交");
        c.commit_queues(&[], false).await.unwrap();
        assert_eq!(c.pending_commit_of(&q0), 5, "空集合忽略这次提交");

        // commit(Set) 走的是已消费游标，不是任意指定值：未交付 ⇒ 守卫拦住
        c.commit_queues(std::slice::from_ref(&q0), false)
            .await
            .unwrap();
        assert_eq!(
            c.pending_commit_of(&q0),
            5,
            "commit(Set) 在没有交付记录时不写 -1"
        );

        // assign 缩范围：撤掉的队列连着两条游标一起丢（Java updateAssignedMessageQueue），
        // 但内存位点表**不**清 —— 那份清理挂在 subscribe 模式的 rebalance 上（Java 同）。
        c.assign(std::slice::from_ref(&q0));
        assert_eq!(c.pull_cursor_of(&q1), -1, "assign 撤队列后拉取游标消失");
        assert_eq!(
            c.consume_cursor_of(&q1),
            -1,
            "assign 撤队列后已消费游标消失"
        );
        assert_eq!(c.pending_commit_of(&q1), 8, "assign 模式不碰 offsetStore");
        let mut late = BTreeMap::new();
        late.insert(mq_key(&q1), 99);
        c.commit_offsets(&late, false).await.unwrap();
        assert_eq!(c.pending_commit_of(&q1), 8, "撤掉的队列不替它改位点");

        // Java RemoteBrokerOffsetStore#persistAll 的 "remove unused mq"：点名提交只发被点名的
        // 队列，内存表里**其余**条目顺手删掉 —— 上一轮 persist=false 攒下、还没落盘的值就此
        // 丢掉。（这里没 start()，网络那半段自然跳过，验的是清理这半段。）
        c.commit_queues(std::slice::from_ref(&q0), true)
            .await
            .unwrap();
        assert_eq!(
            c.pending_commit_of(&q1),
            -1,
            "persistAll 会把没点名的队列从内存表里丢掉"
        );
        assert_eq!(c.pending_commit_of(&q0), 5, "点名的队列留在表里");
    }

    #[test]
    fn seek_moves_both_cursors() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        let q = queue("T", "broker-a", 0);
        c.assign(std::slice::from_ref(&q));
        // Java 的 nextPullOffset() 吃掉 seekOffset 时连 consumeOffset 一起改：
        // "跳回去"意味着"那里之前都还没消费"，否则重放的段会被旧位点跳过。
        c.seek(&q, 2);
        assert_eq!(c.pull_cursor_of(&q), 2, "seek 改拉取游标");
        assert_eq!(c.consume_cursor_of(&q), 2, "seek 也要改已消费游标");
    }

    #[tokio::test]
    async fn auto_commit_deadline_starts_at_minus_one() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        // Java DefaultLitePullConsumerImpl:154：初值 -1 ⇒ 第一次检查就会提交一次
        assert_eq!(lock(&c.inner.state).next_auto_commit_deadline, -1);
        // 没交付过：这次"到点提交"发不出任何东西，只把截止时刻推到下一个周期
        c.maybe_auto_commit().await;
        assert!(
            lock(&c.inner.state).next_auto_commit_deadline > 0,
            "提交完要把截止时刻推到下一周期"
        );
        // 再查一次：还没到点，不该重复提交
        let before = lock(&c.inner.state).next_auto_commit_deadline;
        c.maybe_auto_commit().await;
        assert_eq!(
            lock(&c.inner.state).next_auto_commit_deadline,
            before,
            "没到点就不该再提交"
        );
    }

    #[test]
    fn register_topics_are_namespaced_and_sorted() {
        let c = DefaultMQPullConsumer::new("PG").unwrap();
        c.set_namespace("NS");
        c.register_topic("b");
        c.register_topic("a");
        c.register_topic("a");
        assert_eq!(
            c.register_topics(),
            vec!["NS%a".to_string(), "NS%b".to_string()]
        );
    }

    // ---------------------------------------------------------- 缓冲 / poll / seek

    #[tokio::test]
    async fn poll_drains_buffer_and_times_out_when_empty() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.enqueue(
            (0..3)
                .map(|i| msg("T", "broker-a", 0, i, &format!("m{i}")))
                .collect(),
        );
        let got = c.poll(Some(10)).await;
        assert_eq!(got.len(), 3);
        assert_eq!(
            got[0]
                .body
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).to_string()),
            Some("m0".to_string())
        );
        let started = std::time::Instant::now();
        assert!(c.poll(Some(30)).await.is_empty());
        assert!(started.elapsed() >= Duration::from_millis(30));
        assert_eq!(c.buffered_message_count(), 0);
    }

    #[tokio::test]
    async fn poll_drains_at_most_1024_messages() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.enqueue(
            (0..(MAX_POLL_BATCH_SIZE + 10))
                .map(|i| msg("T", "broker-a", 0, i as i64, "b"))
                .collect(),
        );
        assert_eq!(c.poll(Some(10)).await.len(), MAX_POLL_BATCH_SIZE);
        assert_eq!(c.poll(Some(10)).await.len(), 10);
    }

    #[test]
    fn seek_pins_offset_and_drops_earlier_buffered_messages() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.assign(&[queue("T", "broker-a", 0)]);
        c.enqueue((0..5).map(|i| msg("T", "broker-a", 0, i, "b")).collect());
        c.seek(&queue("T", "broker-a", 0), 3);
        assert_eq!(c.buffered_message_count(), 2);
        let state = lock(&c.inner.state);
        assert_eq!(
            state.next_offset.get(&mq_key(&queue("T", "broker-a", 0))),
            Some(&3)
        );
        assert_eq!(
            state.seek_offset.get(&mq_key(&queue("T", "broker-a", 0))),
            Some(&3)
        );
    }

    #[test]
    fn seek_does_not_touch_other_queues_buffer() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.enqueue(vec![
            msg("T", "broker-a", 0, 0, "x"),
            msg("T", "broker-a", 1, 0, "y"),
        ]);
        c.seek(&queue("T", "broker-a", 0), 5);
        assert_eq!(c.buffered_message_count(), 1);
    }

    #[test]
    fn pause_and_resume_are_set_operations() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        let mq = queue("T", "broker-a", 0);
        c.pause(std::slice::from_ref(&mq));
        assert!(lock(&c.inner.state).paused.contains(&mq_key(&mq)));
        c.resume(std::slice::from_ref(&mq));
        assert!(lock(&c.inner.state).paused.is_empty());
        // 重复 pause / resume 不报错
        c.pause(&[mq.clone(), mq.clone()]);
        c.resume(&[mq]);
        assert!(lock(&c.inner.state).paused.is_empty());
    }

    // ---------------------------------------------------------- 时间戳解析

    /// 与 Python `test_consume_timestamp_must_be_wall_clock` 同一批向量：
    /// 该字段**只**是 14 位本地墙钟，纯数字的 epoch 毫秒必须被拒 ——
    /// 旧实现走 `isdigit()` 分支把 "20230101000000" 当 epoch 解释成公元 2611 年。
    #[test]
    fn consume_timestamp_is_wall_clock_only() {
        use chrono::TimeZone;
        let expect = chrono::Local
            .with_ymd_and_hms(2023, 1, 1, 0, 0, 0)
            .single()
            .map(|dt| dt.timestamp_millis())
            .unwrap_or_default();
        assert_eq!(consume_timestamp_millis("20230101000000").unwrap(), expect);

        for bad in ["", "   ", "1700000000", "1700000000000", "not-a-timestamp"] {
            let err = consume_timestamp_millis(bad).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!(
                    "MQClientException: consumeTimestamp is invalid, \
                     the valid format is yyyyMMddHHmmss,but received {bad}"
                ),
                "{bad:?}"
            );
        }
    }

    /// Java `DefaultLitePullConsumer:168`：默认值是 now-30min 的 14 位墙钟串，
    /// 所以「没设过 consumeTimestamp」在 start 守卫下必然通过（Python 同）。
    #[test]
    fn default_consume_timestamp_is_valid_wall_clock() {
        let cfg = LitePullConsumerConfig::default();
        assert_eq!(cfg.consume_timestamp.len(), 14);
        assert!(cfg.consume_timestamp.bytes().all(|b| b.is_ascii_digit()));
        let past = consume_timestamp_millis(&cfg.consume_timestamp).unwrap();
        let now = current_time_millis();
        assert!((now - 31 * 60_000..=now - 29 * 60_000).contains(&past));
    }

    #[tokio::test]
    async fn lite_start_rejects_epoch_millis_timestamp() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.update_config(|cfg| {
            cfg.name_server_addrs = vec!["127.0.0.1:9876".to_string()];
            cfg.consume_timestamp = "1700000000000".to_string();
        });
        c.assign(&[queue("T", "broker-a", 0)]);
        let err = c.start().await.unwrap_err().to_string();
        assert!(err.contains("consumeTimestamp is invalid"), "{err}");
        // 合法墙钟不能被这条守卫误杀
        c.set_consume_timestamp("20230101000000");
        assert!(c.start().await.is_ok());
        c.shutdown();
    }

    #[test]
    fn namespace_v2_setter_getter_round_trip_on_both_pull_facades() {
        // Java `ClientConfig#setNamespaceV2/getNamespaceV2`：服务端命名空间
        //（`NamespaceRpcHook` 的 `nsd`/`ns` 头），与 v1 `namespace` 互不影响。
        let pull = DefaultMQPullConsumer::new("PullPG").expect("合法组名不该构造失败");
        assert_eq!(pull.get_namespace_v2(), None, "默认不设");
        pull.set_namespace_v2(Some("NS_V2"));
        assert_eq!(pull.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(pull.config().namespace_v2, Some("NS_V2".to_string()));
        pull.set_namespace_v2(None);
        assert_eq!(
            pull.get_namespace_v2(),
            None,
            "None = 清除（Java setNamespaceV2(null)）"
        );

        let lite = DefaultLitePullConsumer::new("LitePG").expect("合法组名不该构造失败");
        assert_eq!(lite.get_namespace_v2(), None);
        lite.set_namespace_v2(Some("NS_V2"));
        assert_eq!(lite.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(lite.config().namespace_v2, Some("NS_V2".to_string()));
        lite.set_namespace("ns1");
        assert_eq!(
            lite.get_namespace_v2(),
            Some("NS_V2".to_string()),
            "v1 namespace 不覆盖 v2"
        );
        lite.set_namespace_v2(None);
        assert_eq!(lite.get_namespace_v2(), None);
    }

    #[tokio::test]
    async fn lite_start_registers_namespace_hook_before_stream_and_user_acl() {
        // 门面透传接缝：`LitePullConsumerConfig.namespace_v2` → `MQClientInstanceConfig`
        // → 实例传输层。链序必须是 Namespace → Stream（拉模式默认开）→ 用户 ACL，
        // 且 nsd/ns/ReqT 都在 ACL 签名之前写入（`MQClientAPIImpl:329-335`）。
        use crate::remoting::protocol::RemotingCommand;
        use crate::remoting::rpchook::{AclClientRPCHook, SessionCredentials};
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let id = format!("LitePG-ns-order-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.update_config(|cfg| {
            cfg.name_server_addrs = vec!["127.0.0.1:9876".to_string()];
            cfg.client_id = Some(id.clone());
            cfg.namespace_v2 = Some("NS_V2".to_string());
        });
        c.set_rpc_hook(Some(Arc::new(AclClientRPCHook::new(
            SessionCredentials::new("AK", "SK"),
        ))));
        c.assign(&[queue("T", "broker-a", 0)]);
        // 不可达 namesrv：start 仍成功（路由刷新软失败，同 `lite_start_rejects_epoch_millis_timestamp`）
        c.start().await.expect("start 应成功");
        let client = DefaultLitePullConsumer::require_client(&c.inner).unwrap();
        let hooks = client.remoting_client().rpc_hooks();
        assert_eq!(hooks.len(), 3, "Namespace → Stream → 用户 ACL");

        let mut cmd = RemotingCommand::create_request_command(request_code::SEND_MESSAGE_V2, None);
        hooks[0].do_before_request("127.0.0.1:9876", &mut cmd);
        assert_eq!(
            cmd.get_ext_field(MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD),
            Some("true")
        );
        assert_eq!(
            cmd.get_ext_field(MixAll::RPC_REQUEST_HEADER_NAMESPACE_FIELD),
            Some("NS_V2")
        );
        assert_eq!(
            cmd.get_ext_field(MixAll::REQ_T),
            None,
            "第一位必须是 Namespace"
        );
        hooks[1].do_before_request("127.0.0.1:9876", &mut cmd);
        // 轻量消费者默认 enable_stream_request_type=true ⇒ 第二位是 Stream
        assert_eq!(cmd.get_ext_field(MixAll::REQ_T), Some("0"));
        assert_eq!(cmd.get_ext_field(SessionCredentials::ACCESS_KEY), None);
        hooks[2].do_before_request("127.0.0.1:9876", &mut cmd);
        assert_eq!(
            cmd.get_ext_field(SessionCredentials::ACCESS_KEY),
            Some("AK")
        );
        assert!(cmd.get_ext_field(SessionCredentials::SIGNATURE).is_some());
        c.shutdown();
    }

    // ---------------------------------------------------------- 心跳报文

    #[test]
    fn heartbeat_carries_only_own_consumer_data() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.update_config(|cfg| cfg.client_id = Some("cid-1".to_string()));
        c.subscribe("T", "TagA");
        let hb = build_lite_heartbeat(&c.inner);
        assert_eq!(hb.client_id, "cid-1");
        assert_eq!(hb.consumer_data_set.len(), 1);
        let cd = &hb.consumer_data_set[0];
        assert_eq!(cd.group_name, "LitePG");
        // Java DefaultLitePullConsumerImpl.consumeType():1111-1112 恒为 ACTIVELY
        assert_eq!(cd.consume_type, ConsumeType::CONSUME_ACTIVELY);
        assert_eq!(cd.message_model, MessageModel::CLUSTERING);
        assert_eq!(
            cd.consume_from_where,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET
        );
        assert!(!cd.unit_mode);
        assert_eq!(cd.subscription_data_set.len(), 1);
        assert_eq!(cd.subscription_data_set[0].topic, "T");
        // 没有订阅时仍是一条 ConsumerData（Python 也发）
        c.unsubscribe("T");
        assert!(build_lite_heartbeat(&c.inner).consumer_data_set[0]
            .subscription_data_set
            .is_empty());
    }

    // -------------------------------------------- 拉模式消费者心跳（#98）

    /// 假 namesrv：只答 `GET_ROUTEINFO_BY_TOPIC`，回一份「一台主 + 一台从」的路由
    /// （同一 brokerName，brokerId 0/1）；其余请求一律 SUCCESS。
    fn spawn_fake_namesrv(
        listener: TcpListener,
        broker_name: String,
        master_addr: String,
        slave_addr: String,
        read_queues: Arc<std::sync::atomic::AtomicI32>,
        seen: Arc<Mutex<Vec<String>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let broker_name = broker_name.clone();
                let master = master_addr.clone();
                let slave = slave_addr.clone();
                let read_queues = read_queues.clone();
                let seen = seen.clone();
                tokio::spawn(async move {
                    while let Some(frame) = read_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else {
                            return;
                        };
                        let mut response = answer_for(&request, response_code::SUCCESS);
                        if request.code == request_code::GET_ROUTEINFO_BY_TOPIC {
                            // 每笔请求现读：用例中途改队列数（扩缩容）必须能被下一轮
                            // 路由刷新看到，否则测不到"集合变了"。
                            let topic = request
                                .get_ext_field("topic")
                                .unwrap_or_default()
                                .to_string();
                            lock(&seen).push(topic);
                            response.set_body(Some(route_body(
                                &broker_name,
                                &master,
                                &slave,
                                read_queues.load(std::sync::atomic::Ordering::Acquire),
                            )));
                        }
                        write_frame(&mut stream, &mut response).await;
                    }
                });
            }
        })
    }

    /// 「一台主 + 一台从」的路由 body（`TopicRouteData` 的 JSON 形态）。
    /// `read_queues` 是读队列数：客户端按 `topicRouteData2TopicSubscribeInfo` 把它展开成
    /// queueId 0..n-1，平衡视图的分片判据就靠它（1 个队列时「自己那一份」与「全部」同形，
    /// 什么也证明不了）。
    fn route_body(broker_name: &str, master_addr: &str, slave_addr: &str, read_queues: i32) -> Vec<u8> {
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                broker_name,
                read_queues,
                read_queues,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "DefaultCluster",
                broker_name,
                vec![
                    (i64::from(MixAll::MASTER_ID), master_addr.to_string()),
                    (i64::from(MixAll::MASTER_ID) + 1, slave_addr.to_string()),
                ],
                "",
            )],
            ..Default::default()
        };
        serde_json::to_vec(&route.to_json_value()).expect("路由可序列化")
    }

    /// 读一整帧并解码（`decode` 从 totalLength 开始，所以帧要连长度前缀一起给）。
    async fn read_frame(stream: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
        let mut len_buf = [0_u8; 4];
        stream.read_exact(&mut len_buf).await.ok()?;
        let total = i32::from_be_bytes(len_buf);
        if total <= 4 || total > 20 * 1024 * 1024 {
            return None;
        }
        let mut frame = vec![0_u8; usize::try_from(total).ok()? + 4];
        frame[..4].copy_from_slice(&len_buf);
        stream.read_exact(&mut frame[4..]).await.ok()?;
        Some(frame)
    }

    async fn write_frame(stream: &mut tokio::net::TcpStream, command: &mut RemotingCommand) {
        let bytes = command.encode();
        let _ = stream.write_all(&bytes).await;
        let _ = stream.flush().await;
    }

    /// 带请求 `opaque` 的应答（客户端按 opaque 配对，串了就当噪声丢掉）。
    fn answer_for(request: &RemotingCommand, code: i32) -> RemotingCommand {
        let mut response = RemotingCommand::create_response(code, None);
        response.opaque = request.opaque;
        response.serialize_type_current_rpc = request.serialize_type_current_rpc;
        response
    }

    /// 一份 35（UNREGISTER_CLIENT）的 extFields 快照。
    type UnregisterExt = Vec<(String, String)>;

    /// 一笔收到的请求：请求码 + extFields 快照。
    type RecordedRequest = (i32, Vec<(String, String)>);

    /// 一笔脚本化的 PULL_MESSAGE 应答。
    struct ScriptedPull {
        code: i32,
        ext: Vec<(&'static str, String)>,
        /// 回包 body（FOUND 的消息流；其余形态为空）。
        body: Option<Vec<u8>>,
        /// 发车闸：`Some` 时先等闸放行再回包 —— 「应答还在路上，客户端先 seek 了」
        /// 这类在途竞态靠它定序。
        gate: Option<tokio::sync::oneshot::Receiver<()>>,
    }

    /// 假 broker：记录 34（心跳 body）与 35（注销 extFields）两号报文，其余一律 SUCCESS。
    struct FakeBroker {
        addr: String,
        /// 收到的 HEART_BEAT 原始 body，按到达顺序。
        heartbeat_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
        /// 收到的 UNREGISTER_CLIENT extFields，按到达顺序。
        unregisters: Arc<Mutex<Vec<UnregisterExt>>>,
        /// 收到的**每一笔**请求（code + extFields 快照），按到达顺序 —— 「这个 RPC
        /// 到底发没发」型断言（如 FIRST_OFFSET 不该发 minOffset）靠它。
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        /// 预排的 PULL_MESSAGE 应答脚本（FIFO）：先到先得，排空了按 [`answer_pull`]
        /// 的「空 broker」形态应答。
        pull_scripts: Arc<Mutex<VecDeque<ScriptedPull>>>,
        /// 38（GET_CONSUMER_LIST_BY_GROUP）的应答内容：本组当前有哪些 clientId。
        /// 空 Vec 就是「broker 不认识这个组」（回空列表），与「查不到」在两端口
        /// 是同一分支，够测平衡视图的兜底路径。
        cid_list: Arc<Mutex<Vec<String>>>,
    }

    impl FakeBroker {
        async fn start() -> Arc<FakeBroker> {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind 假 broker");
            let addr = listener.local_addr().expect("假 broker 地址").to_string();
            let broker = Arc::new(FakeBroker {
                addr,
                heartbeat_bodies: Arc::new(Mutex::new(Vec::new())),
                unregisters: Arc::new(Mutex::new(Vec::new())),
                requests: Arc::new(Mutex::new(Vec::new())),
                pull_scripts: Arc::new(Mutex::new(VecDeque::new())),
                cid_list: Arc::new(Mutex::new(Vec::new())),
            });
            let inner = Arc::clone(&broker);
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let inner = Arc::clone(&inner);
                    tokio::spawn(async move {
                        while let Some(frame) = read_frame(&mut stream).await {
                            let Ok(request) = RemotingCommand::decode(&frame) else {
                                return;
                            };
                            lock(&inner.requests).push((
                                request.code,
                                request
                                    .ext_fields()
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect(),
                            ));
                            match request.code {
                                request_code::HEART_BEAT => lock(&inner.heartbeat_bodies)
                                    .push(request.body().unwrap_or_default().to_vec()),
                                request_code::UNREGISTER_CLIENT => lock(&inner.unregisters).push(
                                    request
                                        .ext_fields()
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect(),
                                ),
                                _ => {}
                            }
                            if !request.is_oneway_rpc() {
                                let mut response = if request.code == request_code::PULL_MESSAGE
                                    || request.code == request_code::LITE_PULL_MESSAGE
                                {
                                    answer_pull(&request, &inner).await
                                } else if request.code == request_code::GET_CONSUMER_LIST_BY_GROUP {
                                    answer_consumer_list(&request, &inner)
                                } else {
                                    answer_for(&request, response_code::SUCCESS)
                                };
                                write_frame(&mut stream, &mut response).await;
                            }
                        }
                    });
                }
            });
            broker
        }

        /// 收到的请求码，按到达顺序。
        fn codes(&self) -> Vec<i32> {
            lock(&self.requests).iter().map(|(code, _)| *code).collect()
        }

        /// 收到的 PULL_MESSAGE 的 `queueOffset`，按到达顺序。
        fn pull_offsets(&self) -> Vec<i64> {
            lock(&self.requests)
                .iter()
                .filter(|(code, _)| {
                    *code == request_code::PULL_MESSAGE || *code == request_code::LITE_PULL_MESSAGE
                })
                .filter_map(|(_, ext)| {
                    ext.iter()
                        .find(|(k, _)| k == "queueOffset")
                        .and_then(|(_, v)| v.parse().ok())
                })
                .collect()
        }

        /// 收到的拉取请求的**请求码**（11=PULL_MESSAGE / 361=LITE_PULL_MESSAGE），按到达顺序。
        fn pull_request_codes(&self) -> Vec<i32> {
            lock(&self.requests)
                .iter()
                .filter(|(code, _)| {
                    *code == request_code::PULL_MESSAGE || *code == request_code::LITE_PULL_MESSAGE
                })
                .map(|(code, _)| *code)
                .collect()
        }

        /// 收到的拉取请求的 `sysFlag`，按到达顺序。
        fn pull_sys_flags(&self) -> Vec<i32> {
            lock(&self.requests)
                .iter()
                .filter(|(code, _)| {
                    *code == request_code::PULL_MESSAGE || *code == request_code::LITE_PULL_MESSAGE
                })
                .filter_map(|(_, ext)| {
                    ext.iter()
                        .find(|(k, _)| k == "sysFlag")
                        .and_then(|(_, v)| v.parse().ok())
                })
                .collect()
        }

        /// 收到的每份心跳解成 `HeartbeatData`（解不开就炸在断言线程上）。
        fn heartbeats(&self) -> Vec<HeartbeatData> {
            lock(&self.heartbeat_bodies)
                .iter()
                .map(|b| HeartbeatData::decode(b).expect("34 的 body 必须是合法 HeartbeatData"))
                .collect()
        }

        fn unregisters(&self) -> Vec<UnregisterExt> {
            lock(&self.unregisters).clone()
        }

        /// 设定 38（GET_CONSUMER_LIST_BY_GROUP）的应答内容：本组有哪些 clientId。
        fn set_cid_list(&self, cids: &[&str]) {
            *lock(&self.cid_list) = cids.iter().map(|s| s.to_string()).collect();
        }

        /// 排一笔脚本化 PULL_MESSAGE 应答（FIFO 命中，每笔只回一次）。
        fn script_pull(
            &self,
            code: i32,
            ext: &[(&'static str, &str)],
            body: Option<Vec<u8>>,
            gate: Option<tokio::sync::oneshot::Receiver<()>>,
        ) {
            lock(&self.pull_scripts).push_back(ScriptedPull {
                code,
                ext: ext.iter().map(|(k, v)| (*k, (*v).to_string())).collect(),
                body,
                gate,
            });
        }
    }

    /// PULL_MESSAGE 的应答：排了脚本就按脚本回（有闸先等闸），否则按**空 broker**
    /// 的忠实形态回 `PULL_NOT_FOUND` + `nextBeginOffset = 请求 queueOffset`
    /// （Java `PullMessageProcessor#composeResponseHeader` 对 NO_NEW_MSG 同样回填
    /// nextBeginOffset；min/max 为 0）。客户端从 #105 起每轮都信 nextBeginOffset，
    /// 这里不忠实（如裸 SUCCESS 缺 ext）就会把拉取游标推到 0。
    async fn answer_pull(request: &RemotingCommand, broker: &FakeBroker) -> RemotingCommand {
        let script = lock(&broker.pull_scripts).pop_front();
        if let Some(script) = script {
            if let Some(gate) = script.gate {
                let _ = gate.await;
            }
            let mut response = answer_for(request, script.code);
            for (key, value) in script.ext {
                response.add_ext_field(key, &value);
            }
            if script.body.is_some() {
                response.set_body(script.body);
            }
            return response;
        }
        let requested = request
            .ext_fields()
            .get("queueOffset")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        let mut response = answer_for(request, response_code::PULL_NOT_FOUND);
        response.add_ext_field("nextBeginOffset", &requested.to_string());
        response.add_ext_field("minOffset", "0");
        response.add_ext_field("maxOffset", "0");
        response
    }

    /// GET_CONSUMER_LIST_BY_GROUP(38) 的应答：把 [`FakeBroker::cid_list`] 原样包成
    /// Java 的 `GetConsumerListByGroupResponseBody`。空列表就是「broker 不认识这个组」——
    /// 真实 broker 对没注册过的组也是这么回（不是报错），平衡视图据此走兜底分支。
    fn answer_consumer_list(request: &RemotingCommand, broker: &FakeBroker) -> RemotingCommand {
        let cids = lock(&broker.cid_list).clone();
        let body = GetConsumerListByGroupResponseBody {
            consumer_id_list: cids,
        };
        let mut response = answer_for(request, response_code::SUCCESS);
        response.set_body(Some(body.encode()));
        response
    }

    /// 一台主 + 一台从（同一 brokerName）的假集群。
    struct FakePullCluster {
        namesrv_addr: String,
        master: Arc<FakeBroker>,
        slave: Arc<FakeBroker>,
        tasks: Vec<JoinHandle<()>>,
        /// 假 namesrv 现读这个值出路由：改它就是"topic 扩缩容"。
        read_queues: Arc<std::sync::atomic::AtomicI32>,
        /// 假 namesrv 收到过的每一笔路由查询（按到达顺序，含重复）。
        ns_route_queries: Arc<Mutex<Vec<String>>>,
    }

    impl FakePullCluster {
        async fn start() -> FakePullCluster {
            FakePullCluster::start_queues(1).await
        }

        /// 把路由里的读队列数改成 `n`（扩/缩容），下一次路由刷新即可见。
        fn scale_to(&self, n: i32) {
            self.read_queues.store(n, std::sync::atomic::Ordering::Release);
        }

        /// 丢弃已记录的路由查询，之后的断言只看着"这一趟"。
        fn clear_route_queries(&self) {
            lock(&self.ns_route_queries).clear();
        }

        fn route_queries(&self) -> Vec<String> {
            lock(&self.ns_route_queries).clone()
        }

        /// 主从两台的 38 应答内容一起设：查询打哪台由路由决定，用例不该关心这个细节。
        fn set_cid_list(&self, cids: &[&str]) {
            self.master.set_cid_list(cids);
            self.slave.set_cid_list(cids);
        }

        /// `read_queues` 个读队列的那一台 broker（其余行为同 [`FakePullCluster::start`]）。
        async fn start_queues(read_queues: i32) -> FakePullCluster {
            let master = FakeBroker::start().await;
            let slave = FakeBroker::start().await;
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind 假 namesrv");
            let namesrv_addr = listener.local_addr().expect("假 namesrv 地址").to_string();
            let broker_name = "broker-a".to_string();
            let read_queues = Arc::new(std::sync::atomic::AtomicI32::new(read_queues));
            let seen = Arc::new(Mutex::new(Vec::<String>::new()));
            let task = spawn_fake_namesrv(
                listener,
                broker_name,
                master.addr.clone(),
                slave.addr.clone(),
                read_queues.clone(),
                seen.clone(),
            );
            FakePullCluster {
                namesrv_addr,
                master,
                slave,
                tasks: vec![task],
                read_queues,
                ns_route_queries: seen,
            }
        }
    }

    impl Drop for FakePullCluster {
        fn drop(&mut self) {
            for task in self.tasks.drain(..) {
                task.abort();
            }
        }
    }

    /// 起一个指向假集群的拉模式消费者。`instance` 必须每个用例唯一：同 clientId 会共享
    /// [`MQClientInstance`]（连带路由表与 namesrv 地址）。
    async fn started_pull(
        instance: &str,
        group: &str,
        cluster: &FakePullCluster,
        topic: &str,
    ) -> DefaultMQPullConsumer {
        let c = DefaultMQPullConsumer::new(group).expect("组名合法");
        c.set_instance_name(instance);
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.register_topic(topic);
        c.start().await.expect("假集群里 start 应当成功");
        c
    }

    /// 轮询等条件成立（最多 5s）；失败时 panic，避免用固定 sleep 掩盖竞态。
    async fn wait_until(mut cond: impl FnMut() -> bool, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("等待超时：{what}");
    }

    /// Java `MQClientInstance#prepareHeartbeatData:1031-1045` 为拉模式组出的那份
    /// ConsumerData 的形状：`consumeType()` 恒 `CONSUME_ACTIVELY`（`DefaultMQPullConsumerImpl:348`）、
    /// `consumeFromWhere()` 恒 `CONSUME_FROM_LAST_OFFSET`（:353），订阅集来自
    /// `subscriptions():357-385` 的 registerTopics，每条 **subVersion 显式置 0**。
    #[test]
    fn pull_heartbeat_matches_the_java_shape() {
        let c = DefaultMQPullConsumer::new("PG_PullHb").unwrap();
        c.update_config(|cfg| cfg.client_id = Some("cid-1".to_string()));
        c.register_topic("T2");
        c.register_topic("T1");
        let hb = build_pull_heartbeat(&c.inner);
        assert_eq!(hb.client_id, "cid-1");
        assert_eq!(hb.consumer_data_set.len(), 1, "拉模式组只发自己的一份");
        let cd = &hb.consumer_data_set[0];
        assert_eq!(cd.group_name, "PG_PullHb");
        assert_eq!(cd.consume_type, ConsumeType::CONSUME_ACTIVELY);
        assert_eq!(
            cd.consume_from_where,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET
        );
        assert_eq!(cd.message_model, MessageModel::CLUSTERING);
        assert!(!cd.unit_mode);
        let topics: Vec<&str> = cd
            .subscription_data_set
            .iter()
            .map(|s| s.topic.as_str())
            .collect();
        assert_eq!(topics, vec!["T1", "T2"], "订阅集来自 registerTopics");
        for sub in &cd.subscription_data_set {
            assert_eq!(sub.sub_string, "*");
            assert_eq!(sub.sub_version, 0, "Java 显式 setSubVersion(0L)");
        }

        // 没有 registerTopics：照样一份 ConsumerData，订阅集为空
        let bare = DefaultMQPullConsumer::new("PG_PullHbBare").unwrap();
        let hb2 = build_pull_heartbeat(&bare.inner);
        assert_eq!(hb2.consumer_data_set.len(), 1);
        assert!(hb2.consumer_data_set[0].subscription_data_set.is_empty());
    }

    /// start() 的同步一轮必须**主从各一份**：broker 的 ConsumerManager 是每台各自一份
    /// 状态，从节点收不到心跳就会对指向自己的拉取回 `SUBSCRIPTION_NOT_EXIST`。
    #[tokio::test]
    async fn pull_start_heartbeats_master_and_slave_with_the_java_shape() {
        let cluster = FakePullCluster::start().await;
        let c = started_pull("pull_hb_start", "PG_PullHbStart", &cluster, "T").await;

        let master = cluster.master.heartbeats();
        let slave = cluster.slave.heartbeats();
        assert_eq!(master.len(), 1, "start 的同步一轮先到主节点");
        assert_eq!(slave.len(), 1, "从节点也要发");
        for hb in [&master[0], &slave[0]] {
            assert_eq!(hb.client_id, c.client_id());
            let cd = &hb.consumer_data_set[0];
            assert_eq!(cd.group_name, "PG_PullHbStart");
            assert_eq!(cd.consume_type, ConsumeType::CONSUME_ACTIVELY);
            assert_eq!(
                cd.consume_from_where,
                ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET
            );
            assert_eq!(cd.subscription_data_set.len(), 1);
            assert_eq!(cd.subscription_data_set[0].topic, "T");
            assert_eq!(cd.subscription_data_set[0].sub_version, 0);
        }
        assert_eq!(c.heartbeat_count(), 1, "一轮两台都成功才算一轮");
        c.shutdown();
    }

    /// 后台循环按 `heartbeat_broker_interval_millis` 重发；`heartbeat_enabled=false`
    /// 只停发不退出（Python `_heartbeat_loop`：`continue` 而不是 `break`）。
    #[tokio::test]
    async fn pull_heartbeat_loop_repeats_and_honours_the_disable_switch() {
        let cluster = FakePullCluster::start().await;
        let c = DefaultMQPullConsumer::new("PG_PullHbLoop").unwrap();
        c.set_instance_name("pull_hb_loop");
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.register_topic("T");
        c.update_config(|cfg| cfg.heartbeat_broker_interval_millis = 100);
        c.start().await.expect("假集群里 start 应当成功");

        // 首轮同步 + 至少两个周期轮
        wait_until(
            || cluster.master.heartbeats().len() >= 3 && cluster.slave.heartbeats().len() >= 3,
            "心跳循环按 100ms 周期重发（主从同步涨）",
        )
        .await;

        // 关掉开关：先静默一拍让在途的那轮落地，再看计数是否冻结
        c.update_config(|cfg| cfg.heartbeat_enabled = false);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let frozen = c.heartbeat_count();
        let master_seen = cluster.master.heartbeats().len();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            c.heartbeat_count(),
            frozen,
            "heartbeat_enabled=false 后不再发"
        );
        assert_eq!(
            cluster.master.heartbeats().len(),
            master_seen,
            "线上也不能再有新的 34"
        );
        c.shutdown();
    }

    /// shutdown 的收尾（Java `DefaultMQPullConsumerImpl:689-692` 的 unregisterConsumer）
    /// 要给**每一台**发 35：`consumerGroup` 有值、`producerGroup` 字段不上线（Java 传 null）。
    #[tokio::test]
    async fn pull_shutdown_unregisters_group_on_every_broker() {
        let cluster = FakePullCluster::start().await;
        let c = started_pull("pull_hb_unreg", "PG_PullHbUnreg", &cluster, "T").await;
        let client_id = c.client_id();
        c.shutdown();
        // 35 挂在运行时上异步发（shutdown 是同步 API）：等它落地
        wait_until(
            || !cluster.master.unregisters().is_empty() && !cluster.slave.unregisters().is_empty(),
            "主从都收到 35",
        )
        .await;

        for (name, broker) in [("master", &cluster.master), ("slave", &cluster.slave)] {
            let unregs = broker.unregisters();
            assert_eq!(unregs.len(), 1, "{name}: 一次 shutdown 只注销一次");
            let field = |k: &str| -> Option<String> {
                unregs[0]
                    .iter()
                    .find(|(f, _)| f == k)
                    .map(|(_, v)| v.clone())
            };
            assert_eq!(
                field("clientID").as_deref(),
                Some(client_id.as_str()),
                "{name}"
            );
            assert_eq!(
                field("consumerGroup").as_deref(),
                Some("PG_PullHbUnreg"),
                "{name}"
            );
            assert_eq!(field("producerGroup"), None, "{name}: 空槽位不上线");
        }
        // 注销之后心跳必须已经停了（循环先停、abort，再发 35）
        let beats = cluster.master.heartbeats().len();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            cluster.master.heartbeats().len(),
            beats,
            "shutdown 后不再发心跳"
        );
    }

    /// 「shutdown 后立刻退进程」也发得出去：多线程运行时里 `shutdown()` 会等 35
    /// 落地才回来。断言在返回后**不 await、不等一拍** —— 旧实现（spawn 后立即返回）
    /// 下收尾任务还没被调度过，35 必然缺席，本用例就是为了钉住这个回归。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_returns_only_after_unregister_landed() {
        let cluster = FakePullCluster::start().await;
        let c = started_pull("pull_hb_unreg_wait", "PG_PullUnregWait", &cluster, "T").await;
        let client_id = c.client_id();

        c.shutdown();

        // 到这里收尾必须已经完成：主从各一份 35。
        for (name, broker) in [("master", &cluster.master), ("slave", &cluster.slave)] {
            let unregs = broker.unregisters();
            assert_eq!(unregs.len(), 1, "{name}: shutdown 返回时 35 必须已经落地");
            let field = |k: &str| -> Option<String> {
                unregs[0]
                    .iter()
                    .find(|(f, _)| f == k)
                    .map(|(_, v)| v.clone())
            };
            assert_eq!(
                field("clientID").as_deref(),
                Some(client_id.as_str()),
                "{name}"
            );
            assert_eq!(
                field("consumerGroup").as_deref(),
                Some("PG_PullUnregWait"),
                "{name}"
            );
        }
    }

    /// 进程立刻退出也不丢：`shutdown()` 返回后马上拆掉整个运行时（等价于
    /// 「shutdown 之后紧跟 `process::exit`」）。旧实现下收尾任务随运行时一起被
    /// 取消，主从一台都收不到 35；现在 35 在返回前已经落地，拆运行时只是收尸。
    #[test]
    fn shutdown_survives_immediate_runtime_drop() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("多线程运行时");
        let (cluster, consumer, client_id) = runtime.block_on(async {
            let cluster = FakePullCluster::start().await;
            let c = started_pull("pull_unreg_exit", "PG_PullUnregExit", &cluster, "T").await;
            let client_id = c.client_id();
            (cluster, c, client_id)
        });

        // 运行时外（纯 std 线程）调用 shutdown —— 这正是 `main()` 收尾时的形态。
        consumer.shutdown();
        drop(runtime);

        for (name, broker) in [("master", &cluster.master), ("slave", &cluster.slave)] {
            let unregs = broker.unregisters();
            assert_eq!(unregs.len(), 1, "{name}: 进程退出前 35 必须已经落地");
            let field = |k: &str| -> Option<String> {
                unregs[0]
                    .iter()
                    .find(|(f, _)| f == k)
                    .map(|(_, v)| v.clone())
            };
            assert_eq!(
                field("clientID").as_deref(),
                Some(client_id.as_str()),
                "{name}"
            );
            assert_eq!(
                field("consumerGroup").as_deref(),
                Some("PG_PullUnregExit"),
                "{name}"
            );
        }
    }

    #[test]
    fn tag_filter_drops_non_matching_and_keeps_order() {
        let sub = FilterAPI::build_subscription_data("T", Some("TagA")).ok();
        let mut a = msg("T", "broker-a", 0, 0, "a");
        a.set_tags("TagA");
        let mut b = msg("T", "broker-a", 0, 1, "b");
        b.set_tags("TagB");
        let out = client_side_tag_filter(sub.as_ref(), vec![a, b]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].queue_offset, 0);
        // SUB_ALL 时 tags_set 为空 ⇒ 不筛
        let all = FilterAPI::build_subscription_data("T", Some("*")).ok();
        let mut c1 = msg("T", "broker-a", 0, 2, "c");
        c1.set_tags("TagC");
        assert_eq!(client_side_tag_filter(all.as_ref(), vec![c1]).len(), 1);
    }

    #[test]
    fn queue_key_order_matches_sort_key_order() {
        let low = queue("T", "broker-a", 0);
        let high = queue("T", "broker-a", 1);
        assert!(mq_key(&low) < mq_key(&high));
        assert!(_key_order_proof(&low) < _key_order_proof(&high));
    }

    /// 两个拉消费者的策略面（Java `DefaultMQPullConsumer:89` 字段默认 + `:196-202`
    /// getter/setter；`DefaultLitePullConsumer` 同款）。
    ///
    /// ⚠ Python/C++/C# 都有「置 null/None 后 `start()` 抛
    /// `allocateMessageQueueStrategy is null`」（Java checkConfig:803）；Rust 用
    /// `Arc<dyn ...>` 把它压成「类型上不可表示」，所以这里只测默认值与替换。
    #[test]
    fn both_pull_consumers_expose_the_allocate_strategy() {
        use crate::client::allocate_strategy::{
            AllocateMessageQueueAveragelyByCircle, AllocateMessageQueueByConfig,
        };

        let mq_all: Vec<MessageQueue> = (0..4).map(|i| queue("T", "broker-a", i)).collect();
        let cid_all: Vec<String> = vec!["cid-a".to_string(), "cid-b".to_string()];
        let allocate_ids = |strategy: &dyn AllocateMessageQueueStrategy, group: &str| -> Vec<i32> {
            strategy
                .allocate(group, "cid-a", &mq_all, &cid_all)
                .unwrap()
                .iter()
                .map(MessageQueue::get_queue_id)
                .collect()
        };

        let pull = DefaultMQPullConsumer::new("PG").unwrap();
        assert_eq!(
            pull.allocate_message_queue_strategy().get_name(),
            "AVG",
            "pull 默认策略 = AVG"
        );
        pull.set_allocate_message_queue_strategy(Arc::new(AllocateMessageQueueAveragelyByCircle));
        let pull_strategy = pull.allocate_message_queue_strategy();
        assert_eq!(pull_strategy.get_name(), "AVG_BY_CIRCLE", "pull 可替换");
        assert_eq!(
            allocate_ids(&*pull_strategy, "PG"),
            vec![0, 2],
            "pull 换上的策略就是 getter 读回的那个"
        );

        let lite = DefaultLitePullConsumer::new("LG").unwrap();
        assert_eq!(
            lite.allocate_message_queue_strategy().get_name(),
            "AVG",
            "lite 默认策略 = AVG"
        );
        let by_config = Arc::new(AllocateMessageQueueByConfig::new(vec![
            queue("T", "broker-a", 0),
            queue("T", "broker-a", 1),
        ]));
        lite.set_allocate_message_queue_strategy(by_config.clone());
        assert_eq!(lite.allocate_message_queue_strategy().get_name(), "CONFIG");
        // getter 交出的是共享引用而不是快照：之后改列表照样作用于 rebalance
        by_config.set_message_queue_list(vec![queue("T", "broker-a", 3)]);
        assert_eq!(
            allocate_ids(&*lite.allocate_message_queue_strategy(), "LG"),
            vec![3],
            "CONFIG 策略无视 mqAll/cidAll 返回配置队列"
        );
    }

    // --------------------------------- FIRST_OFFSET 的起点是字面量 0（#100 附带）

    /// Java `RebalanceLitePullImpl` 的 FIRST_OFFSET 分支（与 `RebalancePushImpl:197-208`
    /// 同形）：起点是字面量 0，**不**发 minOffset 查询。
    ///
    /// minOffset 属 MQAdminImpl 口径（只认 master），主掉线期间会让新起的 lite-pull
    /// 一条都拉不到；起点 0 越界时由 broker 用 `PULL_OFFSET_MOVED` 纠正
    /// （`handle_offset_illegal`）。
    #[tokio::test]
    async fn lite_first_offset_starts_at_zero_without_a_min_offset_rpc() {
        let cluster = FakePullCluster::start().await;

        let first = DefaultLitePullConsumer::new("LitePG_First").unwrap();
        first.set_instance_name("lite_first_offset");
        first.set_namesrv_addr(&cluster.namesrv_addr);
        first.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        first.assign(&[queue("T", "broker-a", 0)]);
        first.start().await.expect("假集群里 start 应当成功");
        wait_until(
            || !cluster.master.pull_offsets().is_empty(),
            "FIRST_OFFSET 消费者的第一次拉取",
        )
        .await;
        assert_eq!(
            cluster.master.pull_offsets()[0],
            0,
            "起点就是 0，直接上拉取请求"
        );

        // 负控：LAST_OFFSET 那一支该发 maxOffset —— 先证明这份请求日志不是哑的
        let last = DefaultLitePullConsumer::new("LitePG_Last").unwrap();
        last.set_instance_name("lite_last_offset");
        last.set_namesrv_addr(&cluster.namesrv_addr);
        last.assign(&[queue("T", "broker-a", 0)]);
        last.start().await.expect("假集群里 start 应当成功");
        wait_until(
            || {
                cluster
                    .master
                    .codes()
                    .contains(&request_code::GET_MAX_OFFSET)
            },
            "LAST_OFFSET 消费者的 maxOffset 查询",
        )
        .await;

        let codes = cluster.master.codes();
        assert!(
            !codes.contains(&request_code::GET_MIN_OFFSET),
            "FIRST_OFFSET 是字面量 0（Java RebalanceLitePullImpl），一次 minOffset 都不该发：{codes:?}"
        );
        first.shutdown();
        last.shutdown();
    }

    // --------------------------------- 请求码 / lite 位（#107）

    /// lite pull 必须带 `FLAG_LITE_PULL_MESSAGE(0x10)` 且线上请求码为
    /// `LITE_PULL_MESSAGE(361)`。Java 把这两件事拆在两处：消费者侧
    /// `DefaultLitePullConsumerImpl#pullSyncImpl:1058` 置位，客户端 API 侧
    /// `MQClientAPIImpl#pullMessage:816-820` 按位选码。少了任一步，报文仍是一个合法的
    /// pull（假 broker 照常回消息），但走普通 pull 线程池、且**不受
    /// `litePullMessageEnable` 开关管辖**（`PullMessageProcessor:325` 只拦 361）——
    /// 假 broker 不看请求码也能测，所以必须对线形状单独设卡；真机判别器见
    /// examples/live_lite_pull_code.rs。
    #[tokio::test]
    async fn lite_pull_uses_code_361_and_sets_the_lite_bit() {
        let cluster = FakePullCluster::start().await;
        let q = queue("T", "broker-a", 0);
        let c = started_lite("lite_code", "LitePG_LiteCode", &cluster, &q).await;
        wait_until(
            || !cluster.master.pull_request_codes().is_empty(),
            "lite 的第一笔拉取",
        )
        .await;

        let codes = cluster.master.pull_request_codes();
        assert!(
            codes.iter().all(|x| *x == request_code::LITE_PULL_MESSAGE),
            "lite 拉取请求码必须全是 LITE_PULL_MESSAGE(361)：{codes:?}"
        );

        let flags = cluster.master.pull_sys_flags();
        assert!(!flags.is_empty(), "至少一笔带 sysFlag 的拉取");
        for f in &flags {
            // 与 Java pullSyncImpl:1058 的 (false, block, true, false, true) 逐位对齐
            assert!(
                PullSysFlag::has_lite_pull_flag(*f),
                "lite 位必须置上：{f:#x}"
            );
            assert!(!PullSysFlag::has_commit_offset_flag(*f), "{f:#x}");
            assert!(!PullSysFlag::has_suspend_flag(*f), "{f:#x}");
            assert!(PullSysFlag::has_subscription_flag(*f), "{f:#x}");
            assert!(!PullSysFlag::has_class_filter_flag(*f), "{f:#x}");
        }

        // 负控：经典拉取（DefaultMQPullConsumerImpl.pullSyncImpl:248 的 4 参版本）必须是
        // 11 且**不带** lite 位 —— 多这一位会让普通消费者也撞上 lite 开关。
        let classic = started_pull("lite_code_classic", "PG_ClassicCode", &cluster, "T").await;
        classic
            .pull(&q, "*", 0, 32, Some(5000))
            .await
            .expect("假集群里经典拉取应当成功");
        let ccodes = cluster.master.pull_request_codes();
        assert!(
            ccodes.contains(&request_code::PULL_MESSAGE),
            "经典拉取必须是 PULL_MESSAGE(11)：{ccodes:?}"
        );
        let cflags = cluster.master.pull_sys_flags();
        assert!(
            cflags.iter().any(|f| !PullSysFlag::has_lite_pull_flag(*f)),
            "经典拉取不该带 lite 位：{cflags:?}"
        );
        c.shutdown();
        classic.shutdown();
    }

    // ---------------------- 拉取游标跟住 nextBeginOffset（#105）

    /// 起一个指向假集群的 lite-pull（assign + FIRST_OFFSET：起点是字面量 0，不做
    /// 起点位点查询 RPC，第一笔 PULL_MESSAGE 确定落在 `queueOffset=0`）。
    async fn started_lite(
        instance: &str,
        group: &str,
        cluster: &FakePullCluster,
        mq: &MessageQueue,
    ) -> DefaultLitePullConsumer {
        let c = DefaultLitePullConsumer::new(group).expect("组名合法");
        c.set_instance_name(instance);
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c.assign(std::slice::from_ref(mq));
        c.start().await.expect("假集群里 start 应当成功");
        c
    }

    /// Java `DefaultLitePullConsumerImpl#PullTaskImpl.run:982-998`：NO_MATCHED_MSG 的
    /// nextBeginOffset 已越过本轮扫过的整段不匹配区间，拉取游标必须跟过去 ——
    /// 旧实现只在 FOUND 时用 `last.queue_offset + 1` 推进，游标会永远卡在 0，
    /// 每轮把同一段不匹配区间重扫一遍。
    #[tokio::test]
    async fn lite_cursor_follows_next_begin_offset_on_no_matched_msg() {
        let cluster = FakePullCluster::start().await;
        let q = queue("T", "broker-a", 0);
        cluster.master.script_pull(
            response_code::PULL_RETRY_IMMEDIATELY,
            &[
                ("nextBeginOffset", "5"),
                ("minOffset", "0"),
                ("maxOffset", "9"),
            ],
            None,
            None,
        );
        let c = started_lite("lite_cursor_no_match", "LitePG_NoMatch", &cluster, &q).await;

        wait_until(
            || c.pull_cursor_of(&q) == 5,
            "NO_MATCHED_MSG 后拉取游标跟到 nextBeginOffset=5",
        )
        .await;
        // 不是内存表里改了个数：下一笔 PULL_MESSAGE 真的从 5 起
        wait_until(
            || cluster.master.pull_offsets().contains(&5),
            "下一笔 PULL_MESSAGE 从 5 开始",
        )
        .await;
        assert_eq!(
            c.buffered_message_count(),
            0,
            "NO_MATCHED_MSG 没有可交付的消息"
        );
        c.shutdown();
    }

    /// OFFSET_ILLEGAL 的 nextBeginOffset 是 broker 对越界位点的纠正值：跟过去才算
    /// 「越界自愈」，停在旧位点会每轮收到同一个纠正、原地打转。
    #[tokio::test]
    async fn lite_cursor_adopts_the_brokers_offset_correction() {
        let cluster = FakePullCluster::start().await;
        let q = queue("T", "broker-a", 0);
        cluster.master.script_pull(
            response_code::PULL_OFFSET_MOVED,
            &[
                ("nextBeginOffset", "42"),
                ("minOffset", "40"),
                ("maxOffset", "100"),
            ],
            None,
            None,
        );
        let c = started_lite("lite_cursor_illegal", "LitePG_Illegal", &cluster, &q).await;

        wait_until(
            || c.pull_cursor_of(&q) == 42,
            "OFFSET_ILLEGAL 后拉取游标采纳 broker 纠正",
        )
        .await;
        wait_until(
            || cluster.master.pull_offsets().contains(&42),
            "下一笔 PULL_MESSAGE 从 42 开始",
        )
        .await;
        c.shutdown();
    }

    /// 唯一一只刹车（Java :808 的 seekOffset == -1 + :979 的 isDropped）：在途应答
    /// 回来时，这轮里刚 seek 过的位点不许被盖掉，也不许把应答里的消息塞进缓冲
    /// （seek 的语义就是「游标钉在这里、旧位点的消息全丢」）。
    /// 发车闸把在途窗口拉成确定性的：请求已到 broker → seek → 放闸。
    /// 旧实现没有刹车：FOUND + 一条 offset=2 的消息会把游标改成 3 并把消息入缓冲。
    #[tokio::test]
    async fn lite_in_flight_seek_wins_over_the_pull_result() {
        let cluster = FakePullCluster::start().await;
        let q = queue("T", "broker-a", 0);
        let body =
            encode_message_ext(&msg("T", "broker-a", 0, 2, "late"), false).expect("消息可编码");
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
        cluster.master.script_pull(
            response_code::SUCCESS,
            &[
                ("nextBeginOffset", "3"),
                ("minOffset", "0"),
                ("maxOffset", "9"),
            ],
            Some(body),
            Some(gate_rx),
        );
        let c = started_lite("lite_cursor_seek_race", "LitePG_SeekRace", &cluster, &q).await;

        wait_until(
            || !cluster.master.pull_offsets().is_empty(),
            "第一笔拉取到达 broker",
        )
        .await;
        c.seek(&q, 99);
        gate_tx.send(()).expect("放闸");
        wait_until(
            || cluster.master.pull_offsets().contains(&99),
            "seek 之后下一笔 PULL_MESSAGE 从 99 起",
        )
        .await;
        assert_eq!(c.pull_cursor_of(&q), 99, "在途应答不得盖掉 seek 写下的位点");
        assert_eq!(c.buffered_message_count(), 0, "被刹车的一轮不许入缓冲");
        c.shutdown();
    }

    // ------------------------------------------------------------ 平衡视图
    //
    // Java `MQPullConsumer:187` → `DefaultMQPullConsumerImpl:120-135`，官方
    // `example/simple/PullConsumer.java:62` 就靠它决定这一轮去拉哪些队列。
    // 本端口拉模式没有后台 rebalance 线程，视图按 `RebalanceImpl.rebalanceByTopic`
    // 的同一条公式当场算，所以用例锁的是「算得对不对」而不是「表填没填上」。

    /// 同组只有本实例：整份订阅信息都是自己的，且按 house 口径排好序。
    #[tokio::test]
    async fn balance_view_sole_instance_takes_every_queue() {
        // 3 个队列：1 个队列时「自己那一份」与「全部」同形，什么也证明不了。
        let cluster = FakePullCluster::start_queues(3).await;
        let c = started_pull("bal_sole", "PG_BalSole", &cluster, "T").await;
        let cid = c.client_id();
        cluster.set_cid_list(&[cid.as_str()]);

        let view = c
            .fetch_message_queues_in_balance("T")
            .await
            .expect("假集群里平衡视图应当算得出来");
        assert_eq!(
            view.iter().map(|q| q.queue_id).collect::<Vec<i32>>(),
            vec![0, 1, 2],
            "单实例认领全部队列，且按 queueId 升序"
        );
        assert!(view.iter().all(|q| q.topic == "T" && q.broker_name == "broker-a"));
        c.shutdown();
    }

    /// 同组两实例：各自只拿到自己那一份；两份不重叠、合起来是全部。
    /// 重叠就是重复消费，这条判据只能靠两个真实例互相对拍。
    #[tokio::test]
    async fn balance_view_returns_only_this_instances_share() {
        let cluster = FakePullCluster::start_queues(2).await;
        let a = started_pull("bal_pair_a", "PG_BalPair", &cluster, "T").await;
        let b = started_pull("bal_pair_b", "PG_BalPair", &cluster, "T").await;

        // 分配前 cidAll 必排序（Java `Collections.sort(cidAll)`）；真实 broker 的返回顺序
        // 不定，用例先把两个 clientId 排好再登记，两边看到的才是同一份列表。
        let mut ids = vec![a.client_id(), b.client_id()];
        ids.sort();
        cluster.set_cid_list(
            &ids.iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
        );

        let va = a
            .fetch_message_queues_in_balance("T")
            .await
            .expect("a 的平衡视图");
        let vb = b
            .fetch_message_queues_in_balance("T")
            .await
            .expect("b 的平衡视图");
        assert_eq!(va.len(), 1, "两个实例分两个队列：每个只拿 1 个");
        assert_eq!(vb.len(), 1);
        assert_ne!(va[0], vb[0], "两份不得重叠（重叠即重复消费）");
        let mut both = vec![va[0].clone(), vb[0].clone()];
        sort_mqs(&mut both);
        assert_eq!(
            both.iter().map(|q| q.queue_id).collect::<Vec<i32>>(),
            vec![0, 1],
            "两份合起来必须覆盖全部队列，谁都不许漏"
        );
        a.shutdown();
        b.shutdown();
    }

    /// broker 不认识本组（38 回空列表）：**算不动** ≠ 算出来是空，此时保持现有分配
    /// （本地拉过的队列），绝不回退成「独占全部队列」。
    #[tokio::test]
    async fn balance_view_keeps_current_assignment_when_the_group_is_unknown() {
        let cluster = FakePullCluster::start_queues(3).await;
        let c = started_pull("bal_unknown", "PG_BalUnknown", &cluster, "T").await;
        cluster.set_cid_list(&[]);

        assert!(
            c.fetch_message_queues_in_balance("T")
                .await
                .expect("兜底路径不该报错")
                .is_empty(),
            "还没拉过任何队列：现有分配就是空"
        );

        let mut all = c
            .fetch_subscribe_message_queues("T")
            .await
            .expect("订阅信息可查");
        sort_mqs(&mut all);
        assert_eq!(all.len(), 3);
        c.pull(&all[2], "*", 0, 32, Some(5000))
            .await
            .expect("假集群里拉取应当成功");

        assert_eq!(
            c.fetch_message_queues_in_balance("T").await.expect("兜底视图"),
            vec![all[2].clone()],
            "兜底只认领自己拉过的那一个，不是路由里的全部 3 个"
        );
        assert!(
            c.fetch_message_queues_in_balance("OtherBalTopic")
                .await
                .expect("别的 topic 也走得通")
                .is_empty(),
            "兜底同样按 topic 收口，不许把 T 的队列漏给别的 topic"
        );
        c.shutdown();
    }

    /// BROADCASTING：Java `rebalanceByTopic` 对广播不查消费者列表、直接全量，
    /// 所以列表里没有本实例也照样拿到全部队列。
    #[tokio::test]
    async fn balance_view_broadcasting_takes_all_without_the_consumer_list() {
        let cluster = FakePullCluster::start_queues(3).await;
        let c = DefaultMQPullConsumer::new("PG_BalBcast").expect("组名合法");
        c.set_instance_name("bal_bcast");
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.set_message_model(MessageModel::BROADCASTING);
        c.register_topic("T");
        c.start().await.expect("假集群里 start 应当成功");
        cluster.set_cid_list(&["someone-else"]);

        let view = c
            .fetch_message_queues_in_balance("T")
            .await
            .expect("广播视图");
        assert_eq!(view.len(), 3, "广播不看消费者列表：全量认领");
        c.shutdown();
    }

    /// 未 start：Java `isRunning()` 守卫直接报错，而不是静默返回空表
    /// （空表会让调用方以为「没有我的队列」而停止拉取）。
    #[tokio::test]
    async fn balance_view_requires_a_started_consumer() {
        let c = DefaultMQPullConsumer::new("PG_BalNotStarted").expect("组名合法");
        let err = c
            .fetch_message_queues_in_balance("T")
            .await
            .expect_err("未启动必须报错");
        assert!(
            err.to_string().contains("not started"),
            "报的是未启动：{err}"
        );
    }

    // ---------------------- lite 的 topic 队列集合变更监听器
    //
    // Java `registerTopicMessageQueueChangeListener`（`DefaultLitePullConsumer:325` →
    // `DefaultLitePullConsumerImpl:1267`）此前整个端口都没有。用例钉的是它的契约：
    // 入参守卫、注册即快照（只在 RUNNING）、集合相等才判"没变"、重复注册覆盖、
    // 后台循环真的在比对。

    /// 收集 `on_changed` 的监听器（队列 id 排序后存下来，比对与顺序无关）。
    struct RecordingQueueListener {
        events: Arc<Mutex<Vec<(String, Vec<i32>)>>>,
    }

    impl RecordingQueueListener {
        fn new() -> (Arc<Self>, Arc<Mutex<Vec<(String, Vec<i32>)>>>) {
            let events = Arc::new(Mutex::new(Vec::new()));
            (
                Arc::new(RecordingQueueListener {
                    events: events.clone(),
                }),
                events,
            )
        }
    }

    impl TopicMessageQueueChangeListener for RecordingQueueListener {
        fn on_changed(&self, topic: &str, message_queues: &[MessageQueue]) {
            let mut ids: Vec<i32> = message_queues.iter().map(|q| q.queue_id).collect();
            ids.sort_unstable();
            lock(&self.events).push((topic.to_string(), ids));
        }
    }

    /// 起一个 subscribe 模式的 lite 消费者（与 `started_lite` 的 assign 版不同：
    /// 队列集合变更监听只在 subscribe 模式下有意义）。
    async fn started_lite_subscribe(
        instance: &str,
        group: &str,
        cluster: &FakePullCluster,
        topic: &str,
    ) -> DefaultLitePullConsumer {
        let c = DefaultLitePullConsumer::new(group).expect("组名合法");
        c.set_instance_name(instance);
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.subscribe(topic, "*");
        c.start().await.expect("假集群里 start 应当成功");
        c
    }

    /// 强制刷一次路由：`get_topic_subscribe_info` 读的是缓存，改完假 namesrv 的
    /// 队列数必须刷一次才看得到。
    async fn refresh_route(c: &DefaultLitePullConsumer, topic: &str) {
        let client = DefaultLitePullConsumer::require_client(&c.inner).expect("已启动");
        client
            .update_topic_route_info_from_name_server(topic, 3000, false)
            .await
            .expect("路由刷新应当成功");
    }

    #[test]
    fn same_queue_set_matches_java_is_set_equal() {
        let q = |id: i32| queue("T", "broker-a", id);
        assert!(!same_queue_set(&[], &[q(0)]), "数量不等即变化");
        assert!(same_queue_set(&[q(0), q(1)], &[q(1), q(0)]), "集合与顺序无关");
        assert!(!same_queue_set(&[q(0), q(1)], &[q(0), q(2)]), "同数量不同成员是变化");
        assert!(same_queue_set(&[q(0)], &[q(0)]));
    }

    #[tokio::test]
    async fn queue_change_listener_guards_and_interval() {
        let c = DefaultLitePullConsumer::new("LiteQC_Guard").expect("组名合法");
        let (listener, _) = RecordingQueueListener::new();
        let err = c
            .register_topic_message_queue_change_listener("  ", listener)
            .await
            .expect_err("空 topic 必须报错（Java 抛 MQClientException）");
        assert!(
            err.to_string().contains("Topic or listener is null"),
            "报的是 Java 那句：{err}"
        );
        // Java DefaultLitePullConsumer:160 默认 30s
        assert_eq!(c.topic_metadata_check_interval_millis(), 30_000);
        c.set_topic_metadata_check_interval_millis(0);
        assert_eq!(c.topic_metadata_check_interval_millis(), 1_000, "夹到 1s 下限");
        c.set_topic_metadata_check_interval_millis(2_500);
        assert_eq!(c.topic_metadata_check_interval_millis(), 2_500);
    }

    #[tokio::test]
    async fn queue_change_listener_reports_scale_out_and_scale_in() {
        let cluster = FakePullCluster::start_queues(2).await;
        let c = started_lite_subscribe("lite_qc_scale", "LiteQC_Scale", &cluster, "T").await;

        let (listener, events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("注册成功");
        // 运行中注册立刻记快照 ⇒ 首轮不许把"现状"报成变化
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            0,
            "注册即快照，未变化不该回调"
        );
        assert!(lock(&events).is_empty());

        cluster.scale_to(4); // 扩容：比对那一趟自己会去问 nameserver
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            1,
            "扩容必须回调一次"
        );
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            0,
            "同一集合不得重复回调"
        );

        cluster.scale_to(1); // 缩容
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            1,
            "缩容同样要回调"
        );
        assert_eq!(
            *lock(&events),
            vec![("T".to_string(), vec![0, 1, 2, 3]), ("T".to_string(), vec![0])],
            "回调收到的是变化后的完整队列集合"
        );
        c.shutdown();
    }

    #[tokio::test]
    async fn queue_change_listener_before_start_defers_snapshot() {
        let cluster = FakePullCluster::start_queues(2).await;
        let c = DefaultLitePullConsumer::new("LiteQC_Pre").expect("组名合法");
        c.set_instance_name("lite_qc_pre");
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.subscribe("T", "*");
        let (listener, events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("未启动也能注册");
        assert!(
            lock(&c.inner.queues_for_topic).is_empty(),
            "未启动不记快照（Java 只在 RUNNING 记）"
        );

        c.start().await.expect("start 成功");
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            1,
            "没有快照 ⇒ 首轮一定报一次"
        );
        assert_eq!(*lock(&events), vec![("T".to_string(), vec![0, 1])]);
        c.shutdown();
    }

    #[tokio::test]
    async fn queue_change_listener_reregistration_overwrites() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_lite_subscribe("lite_qc_dup", "LiteQC_Dup", &cluster, "T").await;
        let (first, first_events) = RecordingQueueListener::new();
        let (second, second_events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", first)
            .await
            .expect("首次注册");
        c.register_topic_message_queue_change_listener("T", second)
            .await
            .expect("重复注册：覆盖旧监听器并 warn");
        assert_eq!(lock(&c.inner.topic_listeners).len(), 1, "同一 topic 只留一个监听器");

        cluster.scale_to(3);
        assert_eq!(c.fetch_topic_message_queues_and_compare().await, 1);
        assert!(lock(&first_events).is_empty(), "被覆盖的监听器不该再收");
        assert_eq!(lock(&second_events).len(), 1, "现监听器收到回调");
        c.shutdown();
    }

    #[tokio::test]
    async fn queue_change_metadata_loop_actually_compares() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_lite_subscribe("lite_qc_loop", "LiteQC_Loop", &cluster, "T").await;
        let (listener, events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("注册");

        cluster.scale_to(2);
        // 首查延迟注入成 50ms（默认 10s），周期 100ms
        let inner = c.inner.clone();
        let stop = c.inner.stop.subscribe();
        let task = tokio::spawn(async move {
            metadata_loop_after(&inner, stop, 50).await;
        });
        wait_until(
            || !lock(&events).is_empty(),
            "后台循环应当自己跑出一轮比对",
        )
        .await;
        assert_eq!(lock(&events)[0].1, vec![0, 1]);
        c.inner.running.store(false, Ordering::Release);
        let _ = c.inner.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        c.shutdown();
    }

    /// 首查延迟只该作用在**第一趟之前**：留在循环里就等于每趟都「首查 + 周期」，
    /// 1s 的周期会被拖成 4s，扩容后的回调慢一个数量级。
    #[tokio::test]
    async fn queue_change_metadata_period_excludes_the_first_delay() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_lite_subscribe("lite_qc_period", "LiteQC_Period", &cluster, "T").await;
        let (listener, events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("注册");
        // 周期写 0 也会被夹到 1s 下限，所以这里的首查延迟取得比周期大得多
        c.set_topic_metadata_check_interval_millis(0);
        let inner = c.inner.clone();
        let stop = c.inner.stop.subscribe();
        let task = tokio::spawn(async move {
            metadata_loop_after(&inner, stop, 3000).await;
        });

        cluster.scale_to(2);
        wait_until(|| !lock(&events).is_empty(), "第一趟要抓到扩容").await;

        cluster.scale_to(3);
        let second = tokio::time::timeout(
            Duration::from_millis(1500),
            wait_until(|| lock(&events).len() >= 2, "第二趟要抓到再次扩容"),
        )
        .await;
        assert!(
            second.is_ok(),
            "检查周期里混进了首查延迟：每趟都多等 3s"
        );
        assert_eq!(lock(&events)[1].1, vec![0, 1, 2]);

        c.inner.running.store(false, Ordering::Release);
        let _ = c.inner.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        c.shutdown();
    }

    /// 比对那一趟必须**现问 name server**，而不是读周期刷新的路由缓存：
    /// 少了这一步，扩容最快也要等一次路由轮询才看得见，监听回调整整慢一个周期。
    #[tokio::test]
    async fn queue_change_round_asks_the_nameserver_every_time() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_lite_subscribe("lite_qc_fresh", "LiteQC_Fresh", &cluster, "T").await;
        let (listener, _) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("注册");

        cluster.clear_route_queries();
        c.fetch_topic_message_queues_and_compare().await;
        let first = cluster.route_queries();
        assert!(!first.is_empty(), "一趟比对至少问一次路由：{first:?}");
        assert!(
            first.iter().all(|t| t == "T"),
            "问的就是这个 topic：{first:?}"
        );

        cluster.clear_route_queries();
        c.fetch_topic_message_queues_and_compare().await;
        let second = cluster.route_queries();
        assert!(
            !second.is_empty(),
            "下一趟还得再问一次（吃缓存就测不出变化）"
        );
        c.shutdown();
    }

    /// 查不到队列 ⇒ **报错**，不是回空表。空表会被比对那一趟读成"这个 topic 缩到 0
    /// 队列"，于是回调一次假缩容、快照也被刷成空集。
    #[tokio::test]
    async fn fetch_message_queues_reports_instead_of_returning_empty() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_lite_subscribe("lite_qc_empty", "LiteQC_Empty", &cluster, "T").await;
        cluster.scale_to(0); // 路由里一个读队列都不剩
        let err = c
            .fetch_message_queues("T")
            .await
            .expect_err("没有可用队列时必须报错");
        assert!(
            err.to_string()
                .contains("Can not find Message Queue for this topic"),
            "报的是「查不到队列」：{err}"
        );
        // 监听器那边也不能因为这一次查不到就收到假缩容回调。
        let (listener, events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("注册");
        cluster.clear_route_queries();
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            0,
            "查不到队列的那一趟不发回调"
        );
        assert!(lock(&events).is_empty(), "没有回调事件");
        c.shutdown();
    }
}
