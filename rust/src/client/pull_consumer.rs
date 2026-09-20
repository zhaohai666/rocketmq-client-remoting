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
//!    `!tagsSet.isEmpty()` 时按字符串再筛一遍（:112-121）。Python/cpp/dotnet 三版
//!    都只跑过滤钩子（`consumer.py:2044-2055` 的注释把这件事说成 Java 行为，其实
//!    不成立）。差别只在 broker 侧 tag **哈希**碰撞时才会显现（碰撞消息 Java 丢、
//!    本版留），这里保持与四门语言一致的口径，不改行为、只在此处记账。
//! 2. **不做 Java 的 `subscriptionAutomatically` / 消费者注册**。Java 的拉取消费者
//!    会 `registerConsumer` 进 `MQClientInstance` 并发心跳（
//!    `DefaultMQPullConsumerImpl:301`、`:366`、`:821`），Python 版只是 RPC 门面，
//!    因此 broker 上不会出现该消费组的实例 —— 拉模式不依赖 broker 侧注册，语义等价。
//!    [`DefaultMQPullConsumer::register_topics`] 因此只是登记信息（Java 拿它算心跳订阅集）。
//! 3. **`message_queue_lists` 字段不移植**：Python/cpp/dotnet 里都是纯声明、零读写的
//!    死字段（Java 也只有配合 `AllocateMessageQueueByConfig` 才用），不搬进 Rust。
//! 4. **消息回投失败会抛**：Java `DefaultMQPullConsumerImpl:666` 在回投失败时吞掉异常、
//!    改用内部生产者把消息直接发进 `%RETRY%group`；本实现按 Python 同口径直接返回错误
//!    （见 [`DefaultMQPullConsumer::send_message_back`]）。
//! 5. **`MessageQueueListener` 按 Java 签名回调**：Python 的重平衡用两个实参调
//!    三参方法（`consumer.py:2650`），异常被外层 `except Exception: pass` 吞掉 ⇒
//!    监听器在 Python 里**从未真正触发过**；cpp 传的是「全部订阅队列 + 新分配」两参。
//!    这里回调 `(topic, mqAll, mqDivided)`（Java `RebalanceImpl#messageQueueChanged`），
//!    与本项目已有的 trait 形状一致。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
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
use crate::client::top_addressing::DefaultTopAddressing;
use crate::common::message::{MessageExt, MessageQueue};
use crate::common::mix_all::MixAll;
use crate::common::sysflag::PullSysFlag;
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};
use crate::remoting::protocol::heartbeat::{
    ConsumeFromWhere, ConsumeType, FilterAPI, HeartbeatData, MessageModel, SubscriptionData,
};
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::remoting::rpchook::RPCHook;
use crate::{bail, rmq_debug, rmq_warn};

use crate::client::consumer::ExpressionType;

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

/// 拉取请求的 sysFlag（Python `pull` = `suspend=False`、`pull_block_if_not_found` =
/// `suspend=True`，两者 `commit_offset` 都是 `False`）。
///
/// ⚠ 短轮询这条**绝不能**置 suspend 位：broker 会在队尾挂到
/// `brokerSuspendMaxTimeMillis`（20s），而客户端 10s 就超时 ——
/// 真机必现 `RemotingTimeoutException`（Python `consumer.py:2117` 的踩坑记录）。
fn pull_sys_flag(block: bool) -> i32 {
    PullSysFlag::build_sys_flag_basic(false, block, true, false)
}

// ================================================================ DefaultMQPullConsumer

/// [`DefaultMQPullConsumer`] 的配置（Python `DefaultMQPullConsumer.__init__`
/// 里那批平铺属性，`consumer.py:2008-2030`）。
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
}

impl Default for PullConsumerConfig {
    fn default() -> PullConsumerConfig {
        PullConsumerConfig {
            consumer_group: MixAll::DEFAULT_CONSUMER_GROUP.to_string(),
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            client_id: None,
            name_server_addrs: Vec::new(),
            tls_enable: None,
            message_model: MessageModel::CLUSTERING.to_string(),
            broker_suspend_max_time_millis: DEFAULT_BROKER_SUSPEND_MAX_TIME_MILLIS,
            consumer_pull_timeout_millis: DEFAULT_CONSUMER_PULL_TIMEOUT_MILLIS,
            consumer_timeout_millis_when_suspend: DEFAULT_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
        }
    }
}

#[derive(Default)]
struct PullInner {
    cfg: RwLock<PullConsumerConfig>,
    client: Mutex<Option<MQClientInstance>>,
    started: AtomicBool,
    filter_hooks: FilterMessageHookList,
    register_topics: Mutex<BTreeSet<String>>,
    listener: Mutex<Option<Arc<dyn MessageQueueListener>>>,
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
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
            inner: Arc::new(PullInner {
                cfg: RwLock::new(cfg),
                ..Default::default()
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
            let mut w = self
                .inner
                .cfg
                .write()
                .unwrap_or_else(|e| e.into_inner());
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

    /// Python `get_register_topics`（Java 用它拼心跳订阅集；本移植版只登记，
    /// 不发心跳，见模块头偏离 2）。
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
    fn filter_messages_for_delivery(&self, mq: &MessageQueue, msgs: Vec<MessageExt>) -> Vec<MessageExt> {
        let group = self.consumer_group();
        crate::client::consumer::filter_messages_for_delivery(
            &group,
            &self.inner.filter_hooks,
            mq,
            None,
            msgs,
        )
    }

    // ---------------- 生命周期 ----------------

    /// Python `start()`：幂等、必须有 name server，`client_id` 缺省时现造。
    ///
    /// ⚠ `client_id` 的时间戳是**秒级** `instanceName@yyyyMMddHHmmss`
    /// （`consumer.py:2083`），与推送消费者同格式；同一秒内起两个实例会撞 clientId，
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
        if cfg.name_server_addrs.is_empty() && !DefaultTopAddressing::is_configured() {
            self.inner.started.store(false, Ordering::Release);
            bail!("name server address is not set");
        }
        let client_id = cfg.client_id.clone().unwrap_or_else(|| {
            format!(
                "{}@{}",
                cfg.instance_name,
                chrono::Local::now().format("%Y%m%d%H%M%S")
            )
        });
        self.update_config(|c| c.client_id = Some(client_id.clone()));

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
        // 动态 name server：实例可能已从地址服务器拿到地址，回填（Python 同）。
        if cfg.name_server_addrs.is_empty() {
            let addrs = client.name_server_addrs();
            if !addrs.is_empty() {
                self.update_config(|c| c.name_server_addrs = addrs);
            }
        }
        *lock(&self.inner.client) = Some(client);
        Ok(())
    }

    /// Python `shutdown()`；未启动时是 no-op。
    pub fn shutdown(&self) {
        if self
            .inner
            .started
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if let Some(client) = lock(&self.inner.client).take() {
            client.shutdown();
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

    // ---------------- 拉取 ----------------

    /// Python `fetch_subscribe_message_queues`：按发布路由列队列。
    ///
    /// ⚠ 与 lite 版不同，这里**不拼命名空间**（`consumer.py:2103-2106` 直接把入参
    /// topic 传给路由查询），与 Python 保持逐字一致。
    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = self.require_client()?;
        let publish = client.get_topic_publish_info(topic, false).await?;
        // Python `list(publish.msg_queue_list)`：只是复制一份，不改队列身份。
        Ok(publish.msg_queue_list())
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
            )
            .await?;
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
            )
            .await?;
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
        client.get_max_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None).await
    }

    /// Python `min_offset`。
    pub async fn min_offset(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client.get_min_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None).await
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
    /// 1. 地址靠 `broker_addr_of(msg.broker_name)` 反查**路由表**，所以调用方必须
    ///    先用本 consumer 访问过该 topic（Java 同理，走 `findBrokerAddressInPublish`）。
    /// 2. 与 Java 的有意差异：Java 失败时吞异常、改由内部生产者把消息直接发进
    ///    `%RETRY%group`；这里直接返回错误，不换路径静默重发（模块头偏离 4）。
    ///
    /// `max_reconsume_times` 按 `consumer.py:2207` 原样下发 `-1`（Java 拉模式同样
    /// 直接传 `getMaxReconsumeTimes()`，由 broker 决定上限后转 `%DLQ%`）。
    pub async fn send_message_back(&self, msg: &MessageExt, delay_level: i32) -> Result<()> {
        let client = self.require_client()?;
        let group = self.consumer_group();
        let broker = msg.broker_name.clone().unwrap_or_default();
        let addr = client
            .broker_addr_of(&broker)
            .ok_or_else(|| Error::client(format!("broker {broker} not found")))?;
        client
            .consumer_send_msg_back(&group, msg, delay_level, -1, 5_000, &addr)
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

// ================================================================ DefaultLitePullConsumer

/// [`DefaultLitePullConsumer`] 的配置（Python `consumer.py:2239-2292`）。
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
}

impl Default for LitePullConsumerConfig {
    fn default() -> LitePullConsumerConfig {
        LitePullConsumerConfig {
            consumer_group: MixAll::DEFAULT_CONSUMER_GROUP.to_string(),
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
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
        }
    }
}

/// Python 里由 `_lock` 保护的那几张表（键统一是 [`mq_key`] 那串
/// `topic+brokerName+queueId`，与推送消费者同口径）。
#[derive(Default)]
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
    /// Python `_next_offset`：**拉取游标**，也是 auto-commit 提交的内容。
    next_offset: BTreeMap<String, i64>,
    /// Python `_seek_offset`：`seek()` 钉住的位点，优先于 `consume_from_where`。
    seek_offset: BTreeMap<String, i64>,
    /// Python `_last_commit`：上次提交时间（毫秒），auto-commit 节流用。
    last_commit: BTreeMap<String, i64>,
    /// Python `_paused`。
    paused: BTreeSet<String>,
    /// Python `_last_rebalance_ts`。
    last_rebalance_ts: i64,
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
            let mut w = self
                .inner
                .cfg
                .write()
                .unwrap_or_else(|e| e.into_inner());
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
    /// （Python `consumer.py:2358-2361`），`subscription` 仍记下 —— 与 Python 一致。
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
    /// 一律按 tag 处理（`consumer.py:2363-2365`）。
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
        state.assigned.clear();
        for mq in message_queues {
            let key = mq_key(mq);
            state.assigned.insert(key, mq.clone());
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

    /// Python `start()`：幂等；必须有 name server；必须已有订阅或 assign；
    /// **先同步发一次心跳再起后台循环**。
    ///
    /// ⚠ 真机实测：自建实例此刻路由表还是空的，所以这一轮心跳实际发 0 份
    /// （Python 逐字如此，这里保持一致）。副作用是**订阅要到 5s 心跳循环的第一轮**
    /// 才注册上 broker —— 想立刻验证心跳到达数，先 [`rebalance`](Self::rebalance)
    /// 或 [`fetch_message_queues`](Self::fetch_message_queues) 把路由缓存进来。
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
        let client_id = cfg.client_id.clone().unwrap_or_else(|| {
            format!(
                "{}@{}",
                cfg.instance_name,
                chrono::Local::now().format("%Y%m%d%H%M%S")
            )
        });
        self.update_config(|c| c.client_id = Some(client_id.clone()));

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
        if cfg.name_server_addrs.is_empty() {
            let addrs = client.name_server_addrs();
            if !addrs.is_empty() {
                self.update_config(|c| c.name_server_addrs = addrs);
            }
        }
        // 本消费者不进实例注册表：lite 的心跳报文由自己拼（Python 同），
        // 只取 client 引用备用。
        *lock(&self.inner.client) = Some(client);

        // assign 模式：start 时补齐初始位点（Python `consumer.py:2412-2418`）
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
    /// 执行（提交要发 RPC，必须发生在实例关闭之前）；没有运行时时退化为直接关闭。
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

        let client = lock(&self.inner.client).take();
        let auto_commit = self.config().auto_commit;
        let items: Vec<(MessageQueue, i64)> = {
            let state = lock(&self.inner.state);
            state
                .next_offset
                .iter()
                .filter_map(|(k, off)| state.assigned.get(k).map(|mq| (mq.clone(), *off)))
                .collect()
        };
        let group = self.consumer_group();
        match (client, auto_commit) {
            (Some(client), true) => {
                if let Some(handle) = self.runtime_handle() {
                    handle.spawn(async move {
                        for (mq, off) in &items {
                            if let Err(e) =
                                client.update_consumer_offset(&group, mq, *off, 5_000, None).await
                            {
                                rmq_debug!("lite shutdown commit failed for {mq:?}: {e}");
                            }
                        }
                        client.shutdown();
                    });
                } else {
                    // 无运行时：提交不了，至少把连接关掉（与 Python 的
                    // 「commit 失败只 debug 日志」同级别降级）。
                    rmq_warn!("lite shutdown: no tokio runtime, skip final offset commit");
                    client.shutdown();
                }
            }
            (Some(client), false) => client.shutdown(),
            (None, _) => {}
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
    }

    async fn run_guarded<F, Fut>(
        weak: Weak<LiteInner>,
        stop: watch::Receiver<bool>,
        make: F,
    ) where
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
        let timeout = timeout_millis.unwrap_or_else(|| self.config().poll_timeout_millis);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout.max(0) as u64);
        loop {
            // 先挂上唤醒再查缓冲（等价于 Python 在 `with self._buffer_cond` 里 check 后
            // 才 wait）：反过来写时，检查与挂起之间 enqueue 的那次 notify 会丢，
            // 用户只能白等到一个 deadline。`enable()` 才是真正登记兴趣的点。
            let mut notified = std::pin::pin!(self.inner.buffer_signal.notified());
            notified.as_mut().enable();
            if let Some(drained) = self.try_drain() {
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

    /// Python `seek`：钉住游标并丢掉缓冲里该队列早于 `offset` 的消息。
    pub fn seek(&self, mq: &MessageQueue, offset: i64) {
        let key = mq_key(mq);
        {
            let mut state = lock(&self.inner.state);
            state.seek_offset.insert(key.clone(), offset);
            state.next_offset.insert(key, offset);
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
        let offset = client.get_min_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None).await?;
        self.seek(mq, offset);
        Ok(())
    }

    /// Python `seek_to_end`。
    pub async fn seek_to_end(&self, mq: &MessageQueue) -> Result<()> {
        let client = Self::require_client(&self.inner)?;
        let offset = client.get_max_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None).await?;
        self.seek(mq, offset);
        Ok(())
    }

    /// Python `committed`：broker 无记录 ⇒ `None`。
    pub async fn committed(&self, mq: &MessageQueue) -> Result<Option<i64>> {
        let client = Self::require_client(&self.inner)?;
        let group = self.consumer_group();
        client
            .query_consumer_offset(&group, mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None, false)
            .await
    }

    /// Python `commit`：把**全部**拉取游标提交给 broker。
    pub async fn commit(&self) -> Result<()> {
        let client = Self::require_client(&self.inner)?;
        let group = self.consumer_group();
        let items: Vec<(MessageQueue, i64)> = {
            let state = lock(&self.inner.state);
            state
                .next_offset
                .iter()
                .filter_map(|(k, off)| state.assigned.get(k).map(|mq| (mq.clone(), *off)))
                .collect()
        };
        let mut first_err = None;
        for (mq, off) in items {
            if let Err(e) = client
                .update_consumer_offset(&group, &mq, off, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
                .await
            {
                rmq_debug!("lite commit failed for {mq:?}: {e}");
                first_err.get_or_insert(e);
            } else {
                lock(&self.inner.state).last_commit.insert(mq_key(&mq), current_time_millis());
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
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

    /// Python `fetch_message_queues`：拼命名空间后按发布路由列队列。
    pub async fn fetch_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = Self::require_client(&self.inner)?;
        let topic = with_namespace(&self.config().namespace, topic);
        let info = client.get_topic_publish_info(&topic, false).await?;
        Ok(info.msg_queue_list())
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
        // topic -> (全部队列, 分到的队列)，用于 MessageQueueListener 回调
        let mut per_topic: Vec<(String, Vec<MessageQueue>, Vec<MessageQueue>)> = Vec::new();
        for topic in topics {
            let mut mq_all: Vec<MessageQueue> = match client.get_topic_publish_info(&topic, false).await {
                Ok(info) => info.msg_queue_list(),
                Err(e) => {
                    rmq_debug!("lite rebalance: no route for topic {topic}: {e}");
                    Vec::new()
                }
            };
            sort_mqs(&mut mq_all);
            let mut cid_all = client
                .get_consumer_id_list_by_group(&topic, &group, LITE_PULL_RPC_TIMEOUT_MILLIS)
                .await
                .unwrap_or_default();
            if !cid_all.contains(&client_id) {
                cid_all.push(client_id.clone());
            }
            cid_all.sort();
            let allocated: Vec<MessageQueue> = match strategy
                .allocate(&group, &client_id, &mq_all, &cid_all)
            {
                Ok(got) => got,
                Err(e) => {
                    rmq_debug!("lite rebalance: allocate failed for {topic}: {e}");
                    Vec::new()
                }
            };
            for mq in &allocated {
                new_assigned.insert(mq_key(mq), mq.clone());
            }
            per_topic.push((topic, mq_all, allocated));
        }

        let (added, changed_topics) = {
            let mut state = lock(&self.inner.state);
            let old = &state.assigned;
            let added: Vec<MessageQueue> = new_assigned
                .keys()
                .filter(|k| !old.contains_key(*k))
                .filter_map(|k| new_assigned.get(k))
                .cloned()
                .collect();
            let removed: Vec<String> = old.keys().filter(|k| !new_assigned.contains_key(*k)).cloned().collect();
            let changed = !added.is_empty() || !removed.is_empty();
            if changed {
                state.assigned = new_assigned.clone();
                for key in &removed {
                    state.next_offset.remove(key);
                    state.last_commit.remove(key);
                    // ⚠ 与 Python 一致：撤销队列**只**清 next_offset / last_commit，
                    // seek_offset 与 paused 保留（consumer.py:2659-2661）。队列回到本实例时
                    // 用户先前 seek 的位置仍然生效。
                }
            }
            let changed_topics: Vec<(String, Vec<MessageQueue>, Vec<MessageQueue>)> = if changed {
                per_topic
                    .into_iter()
                    .filter(|(t, _, divided)| {
                        !divided.is_empty()
                            || state
                                .assigned
                                .values()
                                .any(|mq| &mq.topic == t)
                    })
                    .collect()
            } else {
                Vec::new()
            };
            (added, changed_topics)
        };

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
                    lock(&self.inner.state).next_offset.insert(key.clone(), offset);
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
                // 短轮询（suspend=False），位点由 auto-commit 单独提交
                pull_sys_flag(false),
                0,
                &expr,
                0,
                ExpressionType::TAG,
                LITE_PULL_TIMEOUT_MILLIS,
                -1,
                PULL_SUSPEND_TIMEOUT_MILLIS,
                None,
                0,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                rmq_debug!("lite pull failed for {mq:?}@{offset}: {e}");
                return false;
            }
        };
        if result.status != PullStatus::Found || result.msg_found_list.is_empty() {
            return false;
        }
        // Python `_filter_tags`：按表达式现算 tagsSet 再筛（无集合 = 不筛）
        let sub = FilterAPI::build_subscription_data(&mq.topic, Some(&expr)).ok();
        let msgs = client_side_tag_filter(sub.as_ref(), result.msg_found_list);
        let Some(last) = msgs.last() else {
            // 全被 tag 过滤掉：游标不推进，下一轮重拉同一窗口（Python 同行为，
            // 由 pull_interval_millis 退避兜住不空转）
            return false;
        };
        let next = last.queue_offset + 1;
        lock(&self.inner.state).next_offset.insert(key, next);
        self.enqueue(msgs);
        if cfg.auto_commit {
            self.maybe_commit(mq, &client, next, cfg.auto_commit_interval_millis)
                .await;
        }
        true
    }

    fn enqueue(&self, msgs: Vec<MessageExt>) {
        let mut buffer = lock(&self.inner.buffer);
        buffer.extend(msgs);
        drop(buffer);
        self.inner.buffer_signal.notify_waiters();
    }

    /// Python `_maybe_commit`：按 `auto_commit_interval_millis` 节流。
    async fn maybe_commit(
        &self,
        mq: &MessageQueue,
        client: &MQClientInstance,
        offset: i64,
        interval_millis: i64,
    ) {
        let key = mq_key(mq);
        let now = current_time_millis();
        let last = lock(&self.inner.state).last_commit.get(&key).copied().unwrap_or(0);
        if now - last < interval_millis {
            return;
        }
        let group = self.consumer_group();
        if let Err(e) = client
            .update_consumer_offset(&group, mq, offset, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
        {
            rmq_debug!("lite auto-commit failed for {mq:?}: {e}");
            return;
        }
        lock(&self.inner.state).last_commit.insert(key, now);
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
        .query_consumer_offset(&cfg.consumer_group, mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None, false)
        .await
    {
        Ok(Some(offset)) => return Ok(offset),
        Ok(None) => {}
        Err(e) => rmq_debug!("lite query offset failed for {mq:?}: {e}"),
    }
    if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET {
        return client.get_min_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None).await;
    }
    if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_TIMESTAMP {
        // 与推送消费者共用解析器：两种消费者的 consumeTimestamp 语义必须一致
        let ts = consume_timestamp_millis(&cfg.consume_timestamp)?;
        return client
            .search_offset_by_timestamp(mq, ts, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await;
    }
    client.get_max_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None).await
}

/// Python `_heartbeat_loop` 里那份报文：只带**本消费者自己**的 ConsumerData
/// （lite 不进实例注册表，报文由自己拼）。
fn build_lite_heartbeat(inner: &LiteInner) -> HeartbeatData {
    let cfg = read_cfg(&inner.cfg);
    let mut hb = HeartbeatData::new(cfg.client_id.clone().unwrap_or_default());
    let mut cd = crate::remoting::protocol::heartbeat::ConsumerData::new(
        cfg.consumer_group,
        ConsumeType::CONSUME_PASSIVELY,
        cfg.message_model,
        cfg.consume_from_where,
    );
    cd.subscription_data_set = lock(&inner.state).subscription_data.values().cloned().collect();
    cd.unit_mode = false;
    hb.consumer_data_set.push(cd);
    hb
}

/// Python `_send_heartbeat_to_all_broker`：路由里的每个 broker 发一份。
/// 没有 client（未 start / 已 shutdown）就发 0 份而不是退出循环 —— Python 同。
async fn send_lite_heartbeat(inner: &LiteInner) -> usize {
    let Some(client) = lock(&inner.client).clone() else {
        return 0;
    };
    let hb = build_lite_heartbeat(inner);
    let mut ok = 0;
    for addr in client.get_route_of_all_brokers() {
        match client.send_heartbeat(&addr, &hb, LITE_HEARTBEAT_TIMEOUT_MILLIS).await {
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
                _ => c.fetch_consume_offset(&mq).await.err().map(|e| e.to_string()),
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
        assert!(c.send_message_back(&msg("T", "broker-a", 0, 0, "b"), 3)
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
        let e = c.start().await.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(
            e.contains("subscription is not set, call subscribe() or assign() first"),
            "{e}"
        );
        // 缺 name server 的校验在前，且顺序与 Python 一致
        let c2 = DefaultLitePullConsumer::new("LitePG").unwrap();
        c2.subscribe("T", "*");
        let e2 = c2.start().await.err().map(|e| e.to_string()).unwrap_or_default();
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
        assert_eq!(l.consume_from_where, ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
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
        // `subscription` 条目、丢掉 `subscription_data`（consumer.py:2358-2361）
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

    #[test]
    fn register_topics_are_namespaced_and_sorted() {
        let c = DefaultMQPullConsumer::new("PG").unwrap();
        c.set_namespace("NS");
        c.register_topic("b");
        c.register_topic("a");
        c.register_topic("a");
        assert_eq!(c.register_topics(), vec!["NS%a".to_string(), "NS%b".to_string()]);
    }

    // ---------------------------------------------------------- 缓冲 / poll / seek

    #[tokio::test]
    async fn poll_drains_buffer_and_times_out_when_empty() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.enqueue((0..3).map(|i| msg("T", "broker-a", 0, i, &format!("m{i}"))).collect());
        let got = c.poll(Some(10)).await;
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].body.as_ref().map(|b| String::from_utf8_lossy(b).to_string()), Some("m0".to_string()));
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
        c.enqueue(
            (0..5)
                .map(|i| msg("T", "broker-a", 0, i, "b"))
                .collect(),
        );
        c.seek(&queue("T", "broker-a", 0), 3);
        assert_eq!(c.buffered_message_count(), 2);
        let state = lock(&c.inner.state);
        assert_eq!(state.next_offset.get(&mq_key(&queue("T", "broker-a", 0))), Some(&3));
        assert_eq!(state.seek_offset.get(&mq_key(&queue("T", "broker-a", 0))), Some(&3));
    }

    #[test]
    fn seek_does_not_touch_other_queues_buffer() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.enqueue(vec![msg("T", "broker-a", 0, 0, "x"), msg("T", "broker-a", 1, 0, "y")]);
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
        assert!(
            err.contains("consumeTimestamp is invalid"),
            "{err}"
        );
        // 合法墙钟不能被这条守卫误杀
        c.set_consume_timestamp("20230101000000");
        assert!(c.start().await.is_ok());
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
        assert_eq!(cd.consume_type, ConsumeType::CONSUME_PASSIVELY);
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
}
