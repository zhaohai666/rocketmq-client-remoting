//! Manual pull consumers: `DefaultMQPullConsumer` (RPC facade) and
//! `DefaultLitePullConsumer` (background pull + local buffer).
//!
//! Both are "the caller holds the offset" consumers; the only difference is who pulls:
//!
//! * [`DefaultMQPullConsumer`]: pure RPC facade. The caller pulls queue by queue with
//!   `pull(mq, offset)` and commits offsets itself with `update_consume_offset`; no local buffer, no background thread, no rebalance.
//! * [`DefaultLitePullConsumer`]: `subscribe(..)`/`assign(..)` + a background pull thread feeding
//!   the local buffer + `poll(..)` to take batches; offsets auto-commit by default.
//!
//! # Concurrency model
//!
//! The lite heartbeat and pull loops use tokio tasks + [`tokio::sync::Notify`],
//! matching the classic thread + condition-variable formulation; behavior unchanged.

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
use crate::remoting::socks::SocksProxyConfig;
use crate::{bail, rmq_debug, rmq_error, rmq_warn};

use crate::client::consumer::ExpressionType;
use crate::client::validators;

/// Defaults shared by both consumers: `broker_suspend_max_time_millis` = 20000.
pub const DEFAULT_BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 20_000;
/// `consumer_timeout_millis_when_suspend` = 30000 (request timeout when long polling).
pub const DEFAULT_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: i64 = 30_000;
/// `consumer_pull_timeout_millis` = 10000 for `DefaultMQPullConsumer`.
pub const DEFAULT_CONSUMER_PULL_TIMEOUT_MILLIS: i64 = 10_000;
/// The hard-coded `suspendTimeoutMillis` in `pull()` (unused by short polling, but still sent in the request header).
const PULL_SUSPEND_TIMEOUT_MILLIS: i64 = 15_000;
/// The hard-coded request timeout for lite per-queue pulls.
const LITE_PULL_TIMEOUT_MILLIS: i64 = 30_000;
/// Lite heartbeat loop interval (50 polls of 0.1s).
const LITE_HEARTBEAT_INTERVAL_MILLIS: u64 = 5_000;
/// Lite heartbeat RPC timeout.
const LITE_HEARTBEAT_TIMEOUT_MILLIS: i64 = 5_000;
/// Minimum lite rebalance interval (subscribe mode).
const LITE_REBALANCE_INTERVAL_MILLIS: i64 = 1_000;
/// Single-drain cap for `poll()` (1024 messages).
pub const MAX_POLL_BATCH_SIZE: usize = 1024;
/// The heartbeat RPC timeout is the same as above; named here only for readability.
const LITE_PULL_RPC_TIMEOUT_MILLIS: i64 = 5_000;
const LITE_METADATA_FIRST_DELAY_MILLIS: u64 = 10_000;

pub const DEFAULT_HEARTBEAT_BROKER_INTERVAL_MILLIS: u64 = 30_000;
/// Heartbeat RPC timeout for the classic pull-mode consumer (same 5000ms figure as lite).
const PULL_HEARTBEAT_TIMEOUT_MILLIS: i64 = 5_000;

/// Take the lock; still used when poisoned, same reason as `consumer::lock`.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn read_cfg<T: Clone>(cell: &RwLock<T>) -> T {
    cell.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// `sleep ∥ stop`: `true` = stop signal received, the loop should exit.
async fn wait_or_stop(rx: &mut watch::Receiver<bool>, millis: u64) -> bool {
    if *rx.borrow_and_update() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(millis)) => false,
        _ = rx.changed() => true,
    }
}

fn with_namespace(namespace: &str, topic: &str) -> String {
    if namespace.is_empty() {
        topic.to_string()
    } else {
        NamespaceUtil::wrap_namespace(namespace, topic)
    }
}

fn same_queue_set(old: &[MessageQueue], new: &[MessageQueue]) -> bool {
    if old.len() != new.len() {
        return false;
    }
    let old_set: BTreeSet<String> = old.iter().map(mq_key).collect();
    new.iter().all(|mq| old_set.contains(&mq_key(mq)))
}

/// sysFlag of pull requests: `pull` = `suspend = false`,
/// `pull_block_if_not_found` = `suspend = true`; `commit_offset` is false for both.
///
/// ⚠ The short-polling path must **never** set the suspend bit: the broker would hang at the
/// queue tail until `brokerSuspendMaxTimeMillis` (20s) while the client times out at 10s --
/// `RemotingTimeoutException` is guaranteed on real brokers (pitfall record).
fn pull_sys_flag(block: bool) -> i32 {
    PullSysFlag::build_sys_flag_basic(false, block, true, false)
}

/// ⚠ Do not merge into [`pull_sys_flag`]: that one is for classic pull (:795/:842); setting this
/// extra bit would make ordinary consumers hit the lite switch too.
fn lite_pull_sys_flag() -> i32 {
    PullSysFlag::build_sys_flag(false, false, true, false, true)
}

// ================================================================ Async pull callback

/// Async pull callback: fires **exactly once** per
/// [`DefaultMQPullConsumer::pull_async`] -- either [`on_success`](PullCallback::on_success) with
/// the [`PullResult`] or [`on_exception`](PullCallback::on_exception) with the failure.
pub trait PullCallback: Send + Sync + 'static {
    /// Called when the pull RPC reached a terminal success (any [`PullStatus`], including NO_NEW_MSG).
    fn on_success(&self, pull_result: PullResult);

    /// Called when the pull failed (not started races, transport errors, broker error codes, timeouts).
    fn on_exception(&self, err: Error);
}

/// Success callback closure.
pub type OnPullSuccessFn = Box<dyn FnOnce(PullResult) + Send + 'static>;

/// Failure callback closure.
pub type OnPullExceptionFn = Box<dyn FnOnce(Error) + Send + 'static>;

/// Convenience callback: glues two plain closures into a [`PullCallback`]
/// and guarantees each closure runs at most once (same shape as `producer::ClosureSendCallback`).
pub struct ClosurePullCallback {
    success: Mutex<Option<OnPullSuccessFn>>,
    failure: Mutex<Option<OnPullExceptionFn>>,
}

impl ClosurePullCallback {
    /// Either `success` or `failure` may be `None`.
    pub fn new(
        success: Option<OnPullSuccessFn>,
        failure: Option<OnPullExceptionFn>,
    ) -> ClosurePullCallback {
        ClosurePullCallback {
            success: Mutex::new(success),
            failure: Mutex::new(failure),
        }
    }

    fn take_success(&self) -> Option<OnPullSuccessFn> {
        self.success
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    fn take_failure(&self) -> Option<OnPullExceptionFn> {
        self.failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
}

impl PullCallback for ClosurePullCallback {
    /// An `FnOnce` can run only once: taking it empties the slot, so a repeated callback has its
    /// later arrival ignored.
    fn on_success(&self, pull_result: PullResult) {
        if let Some(f) = self.take_success() {
            f(pull_result);
        }
    }

    fn on_exception(&self, err: Error) {
        if let Some(f) = self.take_failure() {
            f(err);
        }
    }
}

// ================================================================ DefaultMQPullConsumer

/// Configuration of [`DefaultMQPullConsumer`] (a flat set of properties).
#[derive(Debug, Clone)]
pub struct PullConsumerConfig {
    /// Consumer group name.
    pub consumer_group: String,
    /// Namespace.
    pub namespace: String,
    /// Instance name, default `"DEFAULT"`.
    pub instance_name: String,
    /// `client_id`; created on the spot by `start()` when `None`.
    pub client_id: Option<String>,
    pub unit_name: Option<String>,
    pub namespace_v2: Option<String>,
    pub unit_mode: bool,
    pub enable_stream_request_type: bool,
    pub poll_name_server_interval_millis: u64,
    /// Name server address list.
    pub name_server_addrs: Vec<String>,
    /// `None` = defer to the `ROCKETMQ_TLS_ENABLE` environment variable
    /// (the pull consumer had no such field; unified here with the push consumer).
    pub tls_enable: Option<bool>,
    pub socks_proxy: Option<SocksProxyConfig>,
    /// `message_model`; in pull mode it only affects `subscriptionAutomatically`
    /// (this port does not register it); the rest of the behavior is independent of it.
    pub message_model: String,
    /// `broker_suspend_max_time_millis` = 20000: the suspend duration sent down to the broker when long polling.
    pub broker_suspend_max_time_millis: i64,
    /// `consumer_pull_timeout_millis` = 10000: default request timeout for short polling.
    pub consumer_pull_timeout_millis: i64,
    /// `consumer_timeout_millis_when_suspend` = 30000: request timeout for long polling.
    pub consumer_timeout_millis_when_suspend: i64,
    /// `heartbeat_enabled` (default true): once false, neither the synchronous round in
    /// start nor the background loop sends heartbeats.
    pub heartbeat_enabled: bool,
    /// `heartbeat_interval_millis` (default 30000ms).
    pub heartbeat_broker_interval_millis: u64,
    /// Heartbeat fingerprint path (default false). On, this facade's `HEART_BEAT(34)` rounds carry
    /// a non-zero `heartbeatFingerprint`, and once the broker answers `IS_SUPPORT_HEART_BEAT_V2`
    /// and the fingerprint is unchanged the subscription set drops out of the body.
    pub use_heartbeat_v2: bool,
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
            poll_name_server_interval_millis: 30_000,
            client_id: None,
            name_server_addrs: Vec::new(),
            tls_enable: None,
            socks_proxy: None,
            message_model: MessageModel::CLUSTERING.to_string(),
            broker_suspend_max_time_millis: DEFAULT_BROKER_SUSPEND_MAX_TIME_MILLIS,
            consumer_pull_timeout_millis: DEFAULT_CONSUMER_PULL_TIMEOUT_MILLIS,
            consumer_timeout_millis_when_suspend: DEFAULT_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
            heartbeat_enabled: true,
            heartbeat_broker_interval_millis: DEFAULT_HEARTBEAT_BROKER_INTERVAL_MILLIS,
            use_heartbeat_v2: false,
        }
    }
}

struct PullInner {
    cfg: RwLock<PullConsumerConfig>,
    client: Mutex<Option<MQClientInstance>>,
    started: AtomicBool,
    /// Liveness flag of the heartbeat loop (same meaning as lite's `running`).
    running: AtomicBool,
    runtime: OnceLock<tokio::runtime::Handle>,
    /// Stop signal for the background loops (true = stop).
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Successful heartbeat rounds (a round counts when at least one broker received it); assertion point for real-broker/offline cases.
    heartbeat_count: AtomicUsize,
    filter_hooks: FilterMessageHookList,
    register_topics: Mutex<BTreeSet<String>>,
    listener: Mutex<Option<Arc<dyn MessageQueueListener>>>,
    strategy: RwLock<Arc<dyn AllocateMessageQueueStrategy>>,
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
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
    /// Forgetting `shutdown()` must not leave the heartbeat loop running (same as lite / push consumers).
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
        let _ = self.stop.send(true);
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
    }
}

/// Usage: `start()` → `fetch_subscribe_message_queues(topic)` to get queues → per-queue
/// `pull(mq, expr, offset, maxNums)` → commit via `update_consume_offset` yourself →
/// `shutdown()`. Clones share the same state.
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
    /// Construct by consumer group name; a blank group name errors
    /// (`MQClientException("consumerGroup is empty")`).
    pub fn new(consumer_group: &str) -> Result<DefaultMQPullConsumer> {
        DefaultMQPullConsumer::with_config(PullConsumerConfig {
            consumer_group: consumer_group.to_string(),
            ..Default::default()
        })
    }

    /// Construct by consumer group name + RPC hook.
    pub fn with_rpc_hook(
        consumer_group: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Result<DefaultMQPullConsumer> {
        let consumer = DefaultMQPullConsumer::new(consumer_group)?;
        consumer.set_rpc_hook(rpc_hook);
        Ok(consumer)
    }

    /// Construct directly from a complete config (equivalent to assigning properties one by one).
    pub fn with_config(cfg: PullConsumerConfig) -> Result<DefaultMQPullConsumer> {
        if cfg.consumer_group.trim().is_empty() {
            bail!("consumerGroup is empty");
        }
        Ok(DefaultMQPullConsumer {
            inner: Arc::new({
                // `PullInner` has a `Drop`, so the `..Default::default()` struct-update syntax is not allowed
                // (same as lite): build the whole default first, then replace cfg.
                let mut inner = PullInner::default();
                inner.cfg = RwLock::new(cfg);
                inner
            }),
        })
    }

    /// Snapshot of the current config.
    pub fn config(&self) -> PullConsumerConfig {
        read_cfg(&self.inner.cfg)
    }

    /// Change the config (direct property assignment).
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

    pub fn unit_mode(&self) -> bool {
        read_cfg(&self.inner.cfg).unit_mode
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    // ---------------- Configuration ----------------

    /// Semicolon-separated; each item is trimmed, empty items dropped.
    pub fn set_namesrv_addr(&self, addr: &str) {
        let addrs = split_addrs(addr);
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

    pub fn set_unit_name(&self, unit_name: Option<&str>) {
        let unit_name = unit_name.map(str::to_string);
        self.update_config(|c| c.unit_name = unit_name);
    }

    pub fn set_namespace_v2(&self, namespace_v2: Option<&str>) {
        let namespace_v2 = namespace_v2.map(str::to_string);
        self.update_config(|c| c.namespace_v2 = namespace_v2);
    }

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

    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.update_config(|c| c.enable_stream_request_type = enable);
    }

    pub fn set_poll_name_server_interval_millis(&self, millis: u64) {
        self.update_config(|c| c.poll_name_server_interval_millis = millis);
    }

    pub fn set_namespace(&self, namespace: &str) {
        let namespace = namespace.to_string();
        self.update_config(|c| c.namespace = namespace);
    }

    pub fn set_message_model(&self, model: &str) {
        let model = model.to_string();
        self.update_config(|c| c.message_model = model);
    }

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

    pub fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy> {
        self.inner
            .strategy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn register_topics(&self) -> Vec<String> {
        lock(&self.inner.register_topics).iter().cloned().collect()
    }

    pub fn register_topic(&self, topic: &str) {
        let topic = with_namespace(&self.config().namespace, topic);
        lock(&self.inner.register_topics).insert(topic);
    }

    // ---------------- Pre-delivery filter hooks ----------------

    pub fn register_filter_message_hook(&self, hook: Arc<dyn FilterMessageHook>) {
        self.inner.filter_hooks.register(hook);
    }

    pub fn has_filter_message_hook(&self) -> bool {
        self.inner.filter_hooks.has_hooks()
    }

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

    // ---------------- Lifecycle ----------------

    /// Idempotent, requires a name server; `client_id` is created on the spot when absent.
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
            socks_proxy: cfg.socks_proxy.clone(),
            unit_name: cfg.unit_name.clone(),
            namespace_v2: cfg.namespace_v2.clone(),
            enable_stream_request_type: cfg.enable_stream_request_type,
            route_refresh_interval_millis: cfg.poll_name_server_interval_millis,
            use_heartbeat_v2: cfg.use_heartbeat_v2,
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
        // Dynamic name server: the instance may already have addresses from the address server; backfill.
        if cfg.name_server_addrs.is_empty() {
            let addrs = client.name_server_addrs();
            if !addrs.is_empty() {
                self.update_config(|c| c.name_server_addrs = addrs);
            }
        }
        client.register_consumer_group(&cfg.consumer_group);
        *lock(&self.inner.client) = Some(client);
        self.refresh_route_for_heartbeat().await;
        self.inner.running.store(true, Ordering::Release);
        let _ = self.inner.stop.send(false);
        if cfg.heartbeat_enabled {
            self.send_heartbeat_to_all_broker().await;
        }
        self.spawn_heartbeat_loop();
        Ok(())
    }

    /// `shutdown()`; a no-op when not started.
    pub fn shutdown(&self) {
        if self
            .inner
            .started
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        // Stop the heartbeat first: an aborted round may already be on the wire, and landing on the
        // broker would shove this clientId back into ConsumerManager (same reason as push consumer shutdown).
        self.inner.running.store(false, Ordering::Release);
        let _ = self.inner.stop.send(true);
        for task in lock(&self.inner.tasks).drain(..) {
            task.abort();
        }
        let group = self.consumer_group();
        let client_id = self.client_id();
        // Unregistering sends RPCs -- same treatment as producers/push consumers: run it on the runtime and wait
        // for it to land with a bound (`run_finalize_blocking`, so it still goes out when the process exits right after shutdown);
        // without a runtime, degrade to only dropping the group registration + shutting the instance.
        match self.runtime_handle() {
            Some(handle) => {
                let this = self.clone();
                let finalize = async move {
                    // The lock must not cross an await (MutexGuard is not Send): take the client out first.
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

    fn require_client(&self) -> Result<MQClientInstance> {
        if !self.is_started() {
            return Err(Error::client("consumer not started, call start() first"));
        }
        lock(&self.inner.client)
            .clone()
            .ok_or_else(|| Error::client("consumer not started, call start() first"))
    }

    // ---------------- Heartbeat ----------------

    /// Successful heartbeat rounds (a round counts when at least one broker received it; assertion point for real-broker/offline cases).
    pub fn heartbeat_count(&self) -> usize {
        self.inner.heartbeat_count.load(Ordering::SeqCst)
    }

    /// Route refresh before heartbeating (same as the lite version): heartbeats only go to
    /// brokers already known in the route table, which is empty right after a self-created instance
    /// starts. Pull the routes of registerTopics once and register them as in use first, so that the synchronous heartbeat inside start() actually reaches the broker.
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

    /// Send one heartbeat to every broker in the route table (same as the lite version, **including slaves**), returning the number of successes.
    pub async fn send_heartbeat_to_all_broker(&self) -> usize {
        send_pull_heartbeat(&self.inner).await
    }

    fn spawn_heartbeat_loop(&self) {
        let Some(handle) = self.runtime_handle() else {
            rmq_warn!("pull consumer: no tokio runtime, heartbeat loop disabled");
            return;
        };
        // Hold a Weak instead of an Arc (same as lite): only then can Drop get its chance to run when the consumer is discarded.
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

    // ---------------- Pulling ----------------

    /// List queues by **subscription info**.
    ///
    /// ⚠ Unlike the lite version, **no namespace is applied** here: the input topic is passed
    /// straight to the route query, verbatim identical to the online baseline.
    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = self.require_client()?;
        // Policy: do not alter queue identity; return the list directly.
        Ok(client.get_topic_subscribe_info(topic).await)
    }

    pub async fn fetch_message_queues_in_balance(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = self.require_client()?;
        let cfg = self.config();
        let mut pulled: Vec<MessageQueue> = lock(&self.inner.pull_from_which_node)
            .keys()
            .filter(|mq| mq.topic == topic)
            .cloned()
            .collect();
        sort_mqs(&mut pulled);

        if cfg.message_model == MessageModel::BROADCASTING {
            let mut all = client.get_topic_subscribe_info(topic).await;
            if all.is_empty() {
                return Ok(pulled);
            }
            sort_mqs(&mut all);
            return Ok(all);
        }

        let mut mq_all = client.get_topic_subscribe_info(topic).await;
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

    /// **Short polling** (`suspend=False`); offsets are committed by the caller via `update_consume_offset`.
    pub async fn pull(
        &self,
        mq: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_nums: i32,
        timeout_millis: Option<i64>,
    ) -> Result<PullResult> {
        let cfg = self.config();
        let timeout = timeout_millis.unwrap_or(cfg.consumer_pull_timeout_millis);
        let sub = FilterAPI::build_subscription_data(&mq.topic, Some(sub_expression))?;
        self.pull_with_subscription(mq, &sub, offset, max_nums, false, timeout, PULL_SUSPEND_TIMEOUT_MILLIS)
            .await
    }

    /// Short-polling flavour of [`pull`](Self::pull) driven by a [`MessageSelector`]: the
    /// selector's expression type travels with the request, so a SQL92 expression is not
    /// reinterpreted as a tag list. A `TAG` selector (or an empty type) behaves exactly like
    /// [`pull`](Self::pull): `tagsSet`/`codeSet` compiled by the filter API, `subVersion` sent
    /// as 0; a non-tag selector carries only its raw expression with its own `expressionType`.
    pub async fn pull_with_selector(
        &self,
        mq: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_nums: i32,
        timeout_millis: Option<i64>,
    ) -> Result<PullResult> {
        let cfg = self.config();
        let timeout = timeout_millis.unwrap_or(cfg.consumer_pull_timeout_millis);
        let sub = pull_selector_subscription(&mq.topic, selector)?;
        self.pull_with_subscription(mq, &sub, offset, max_nums, false, timeout, PULL_SUSPEND_TIMEOUT_MILLIS)
            .await
    }

    /// Long polling (`suspend=True`), using
    /// `consumer_timeout_millis_when_suspend` for the timeout and
    /// `broker_suspend_max_time_millis` for the suspend duration.
    ///
    /// ⚠ The request timeout must clearly exceed the suspend duration, otherwise a message
    /// arriving exactly at the end of suspension will reliably time out (the default 30000 > 20000 is exactly this ratio).
    pub async fn pull_block_if_not_found(
        &self,
        mq: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_nums: i32,
    ) -> Result<PullResult> {
        let cfg = self.config();
        let sub = FilterAPI::build_subscription_data(&mq.topic, Some(sub_expression))?;
        self.pull_with_subscription(
            mq,
            &sub,
            offset,
            max_nums,
            true,
            cfg.consumer_timeout_millis_when_suspend,
            cfg.broker_suspend_max_time_millis,
        )
        .await
    }

    /// Selector flavour of [`pull_block_if_not_found`](Self::pull_block_if_not_found): same
    /// expression-type dispatch as [`pull_with_selector`](Self::pull_with_selector), same
    /// suspend timeout shape as the string form.
    pub async fn pull_block_if_not_found_with_selector(
        &self,
        mq: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_nums: i32,
    ) -> Result<PullResult> {
        let cfg = self.config();
        let sub = pull_selector_subscription(&mq.topic, selector)?;
        self.pull_with_subscription(
            mq,
            &sub,
            offset,
            max_nums,
            true,
            cfg.consumer_timeout_millis_when_suspend,
            cfg.broker_suspend_max_time_millis,
        )
        .await
    }

    /// Shared kernel of the four pull flavours. `sub` decides the `subscription` /
    /// `subVersion` / `expressionType` fields of the request: a tag-typed subscription keeps
    /// the historic shape (expression as compiled into `tagsSet`/`codeSet`, `subVersion` 0);
    /// any other type sends its own expression type and the subscription's real version.
    async fn pull_with_subscription(
        &self,
        mq: &MessageQueue,
        sub: &SubscriptionData,
        offset: i64,
        max_nums: i32,
        suspend: bool,
        timeout_millis: i64,
        suspend_timeout_millis: i64,
    ) -> Result<PullResult> {
        let client = self.require_client()?;
        let cfg = self.config();
        let broker_id = lock(&self.inner.pull_from_which_node)
            .get(mq)
            .copied()
            .unwrap_or(MixAll::MASTER_ID as i64);
        // The class-filter bit stays clear on this path: it asks for the legacy filter-server
        // route. The subscription's own expression type is still what the header reports.
        let sys_flag = pull_sys_flag(suspend);
        let result = client
            .pull_message(
                &cfg.consumer_group,
                mq,
                offset,
                max_nums,
                sys_flag,
                0,
                sub_expression_of(sub),
                // A tag subscription keeps the version-0 shape it has always sent; a non-tag one
                // rides with the subscription's own version.
                if sub.expression_type == ExpressionType::TAG { 0 } else { sub.sub_version },
                &sub.expression_type,
                timeout_millis,
                -1,
                suspend_timeout_millis,
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

    /// Async short-polling pull -- the [`PullCallback`] flavour of
    /// [`pull`](Self::pull): same `mq, subExpression, offset, maxNums, timeout` inputs, result delivered through the callback.
    ///
    /// Returns immediately; a background tokio task runs the same RPC path as [`pull`](Self::pull)
    /// (same sysFlag, same `pull_from_which_node` follow-up, same pre-delivery filter) and fires
    /// `on_success` / `on_exception` on the callback **exactly once**.
    ///
    /// A not-started (or already shut down) consumer is rejected **synchronously** with `Err` and
    /// the callback never fires -- the not-started state check fails synchronously before the
    /// request is handed to the async machinery. The spawned task is deliberately not tracked in
    /// the shutdown task list: an in-flight `pull_async` drains (its callback still lands) instead
    /// of being aborted mid-RPC.
    pub fn pull_async(
        &self,
        mq: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_nums: i32,
        timeout_millis: Option<i64>,
        callback: Arc<dyn PullCallback>,
    ) -> Result<()> {
        let _ = self.require_client()?;
        let handle = self.runtime_handle().ok_or_else(|| {
            Error::client("pull_async needs a tokio runtime; call start() first")
        })?;
        let this = self.clone();
        let mq = mq.clone();
        let sub_expression = sub_expression.to_string();
        handle.spawn(async move {
            match this
                .pull(&mq, &sub_expression, offset, max_nums, timeout_millis)
                .await
            {
                Ok(result) => callback.on_success(result),
                Err(err) => callback.on_exception(err),
            }
        });
        Ok(())
    }

    /// Run the pre-delivery filter only when FOUND and non-empty.
    fn apply_delivery_filter(&self, mq: &MessageQueue, mut result: PullResult) -> PullResult {
        if result.status == PullStatus::Found && !result.msg_found_list.is_empty() {
            result.msg_found_list =
                self.filter_messages_for_delivery(mq, std::mem::take(&mut result.msg_found_list));
        }
        result
    }

    // ---------------- Offset management ----------------

    pub async fn fetch_consume_offset(&self, mq: &MessageQueue) -> Result<Option<i64>> {
        let client = self.require_client()?;
        let group = self.consumer_group();
        client
            .query_consumer_offset(&group, mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None, false)
            .await
    }

    pub async fn update_consume_offset(&self, mq: &MessageQueue, offset: i64) -> Result<()> {
        let client = self.require_client()?;
        let group = self.consumer_group();
        client
            .update_consumer_offset(&group, mq, offset, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: i64) -> Result<i64> {
        let client = self.require_client()?;
        client
            .search_offset_by_timestamp(mq, timestamp, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    pub async fn max_offset(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client
            .get_max_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    pub async fn min_offset(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client
            .get_min_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    /// `GET_EARLIEST_MSG_STORETIME`(32): the queue's earliest store time.
    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client
            .get_earliest_msg_store_time(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    pub async fn send_message_back(&self, msg: &MessageExt, delay_level: i32) -> Result<()> {
        let client = self.require_client()?;
        let group = self.consumer_group();
        let broker = msg.broker_name.clone().unwrap_or_default();
        // Slave nodes do not accept CONSUMER_SEND_MSG_BACK (master only), so only brokerId=0 is considered here
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

/// An empty string falls back to SUB_ALL.
fn sub_expression_of(sub: &SubscriptionData) -> &str {
    if sub.sub_string.is_empty() {
        FilterAPI::SUB_ALL
    } else {
        &sub.sub_string
    }
}

/// `SubscriptionData` for one selector-driven pull: an empty **tag** expression falls back to
/// `"*"`, an empty type to `TAG`, while a non-tag selector with an empty expression is rejected
/// outright. A `TAG` selector gets `tagsSet`/`codeSet` compiled by the filter API; any other type
/// carries only its raw expression plus `expressionType`, with `CLASS_FILTER` additionally
/// setting the class-filter mode flag.
fn pull_selector_subscription(topic: &str, selector: &MessageSelector) -> Result<SubscriptionData> {
    let expression_type = if selector.selector_type.is_empty() {
        ExpressionType::TAG
    } else {
        selector.selector_type.as_str()
    };
    let expression = if expression_type == ExpressionType::TAG && selector.expression.is_empty() {
        FilterAPI::SUB_ALL
    } else {
        selector.expression.as_str()
    };
    let mut sub = FilterAPI::build(topic, expression, Some(expression_type))?;
    sub.class_filter_mode = expression_type == ExpressionType::CLASS_FILTER;
    Ok(sub)
}

/// Split on semicolons + trim each item + drop empty ones.
fn split_addrs(addr: &str) -> Vec<String> {
    addr.split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Build one ConsumerData for the pull-mode consumer group.
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
            sub.sub_version = 0;
            cd.add_subscription_data(sub);
        }
    }
    hb.add_consumer_data(cd);
    hb
}

/// Send this group's heartbeat once to all known
/// brokers (including slaves), returning the number of successes.
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

/// Heartbeat background loop: wait one full period before sending --
/// start() already sent one round synchronously, so the loop's first round is "the second heartbeat period".
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

/// Configuration of [`DefaultLitePullConsumer`].
#[derive(Debug, Clone)]
pub struct LitePullConsumerConfig {
    /// Consumer group name.
    pub consumer_group: String,
    /// Namespace.
    pub namespace: String,
    /// Instance name, default `"DEFAULT"`.
    pub instance_name: String,
    /// `client_id`; created on the spot by `start()` when `None`.
    pub client_id: Option<String>,
    pub unit_name: Option<String>,
    pub namespace_v2: Option<String>,
    pub unit_mode: bool,
    pub enable_stream_request_type: bool,
    pub poll_name_server_interval_millis: u64,
    /// Name server address list.
    pub name_server_addrs: Vec<String>,
    /// `tls_enable` (same note as above, unified with the push consumer).
    pub tls_enable: Option<bool>,
    /// SOCKS proxy (same note as above, unified with the push consumer).
    pub socks_proxy: Option<SocksProxyConfig>,
    /// `message_model`.
    pub message_model: String,
    /// Affects only the **first** offset resolution.
    pub consume_from_where: String,
    pub consume_timestamp: String,
    /// `pull_batch_size` = 32 (`max(1, n)` in the setter).
    pub pull_batch_size: i32,
    /// `poll_timeout_millis` = 5000 (`max(0, ms)`).
    pub poll_timeout_millis: i64,
    /// `auto_commit` = true: **commit as soon as pulled**, not after poll().
    pub auto_commit: bool,
    /// `auto_commit_interval_millis` = 5000 (`max(0, ms)`).
    pub auto_commit_interval_millis: i64,
    /// `consumer_timeout_millis_when_suspend` = 30000: lite always short-polls,
    /// the field is kept only for the config shape; the pull path never reads it.
    pub consumer_timeout_millis_when_suspend: i64,
    /// `broker_suspend_max_time_millis` = 20000: same as above, the pull path never reads it.
    pub broker_suspend_max_time_millis: i64,
    /// `pull_interval_millis` = 50: backoff when the queue tail is empty (`max(0, ms)`).
    pub pull_interval_millis: i64,
    /// `pull_thread_nums` = 1: **this implementation is always a single pull loop**;
    /// the field is kept only for the config shape.
    pub pull_thread_nums: i32,
    /// [`register_topic_message_queue_change_listener`]: DefaultLitePullConsumer::register_topic_message_queue_change_listener
    pub topic_metadata_check_interval_millis: i64,
    /// Heartbeat fingerprint path (default false). On, this facade's `HEART_BEAT(34)` rounds carry
    /// a non-zero `heartbeatFingerprint`, and once the broker answers `IS_SUPPORT_HEART_BEAT_V2`
    /// and the fingerprint is unchanged the subscription set drops out of the body.
    pub use_heartbeat_v2: bool,
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
            poll_name_server_interval_millis: 30_000,
            client_id: None,
            name_server_addrs: Vec::new(),
            tls_enable: None,
            socks_proxy: None,
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
            use_heartbeat_v2: false,
        }
    }
}

struct LiteState {
    /// Subscription table `topic -> sub_expression` (subscribe mode).
    subscription: BTreeMap<String, String>,
    /// The subscription set carried into heartbeats.
    subscription_data: BTreeMap<String, SubscriptionData>,
    /// Tag expressions in assign mode.
    assign_sub_expr: BTreeMap<String, String>,
    assign_mode: bool,
    /// Current assignment (`BTreeMap` keeps it ordered by queue key).
    assigned: BTreeMap<String, MessageQueue>,
    next_offset: BTreeMap<String, i64>,
    consume_offset: BTreeMap<String, i64>,
    offset_table: BTreeMap<String, i64>,
    /// Offsets pinned by `seek()`, taking precedence over `consume_from_where`.
    seek_offset: BTreeMap<String, i64>,
    next_auto_commit_deadline: i64,
    paused: BTreeSet<String>,
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
    /// Local buffer + buffer-ready notification.
    buffer: Mutex<VecDeque<MessageExt>>,
    buffer_signal: Notify,
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
    /// Forgetting `shutdown()` must not leave running loops behind (same as the push consumer).
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
        let _ = self.stop.send(true);
        self.buffer_signal.notify_waiters();
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
    }
}

pub trait TopicMessageQueueChangeListener: Send + Sync {
    fn on_changed(&self, topic: &str, message_queues: &[MessageQueue]);
}

/// Two modes:
/// * **subscribe**: `subscribe(topic, expr)` → background rebalance per [`AllocateMessageQueueStrategy`]
///   → background pull;
/// * **assign**: `assign(&[mq])` names the queues explicitly, no rebalance (tag expression via
///   [`set_sub_expression_for_assign`](Self::set_sub_expression_for_assign)).
///
/// In both modes a single background loop does **short polling** to feed messages into the local buffer; `poll()` only takes from the buffer.
/// Offsets auto-commit by default -- note the commit point is "pulled into the buffer", not "polled to the user",
/// so a process crash loses buffered messages that were never polled (the same trade-off).
///
/// ⚠ In subscribe mode `start()` does **not** rebalance synchronously: right after start,
/// [`assignment`](Self::assignment) is very likely empty; wait for the background loop's first round (≤1s).
/// Callers that depend on the queue assignment (`pause`, per-queue `seek`) should run `rebalance().await` once first.
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
    /// Construct a lite consumer by consumer group name.
    pub fn new(consumer_group: &str) -> Result<DefaultLitePullConsumer> {
        DefaultLitePullConsumer::with_config(LitePullConsumerConfig {
            consumer_group: consumer_group.to_string(),
            ..Default::default()
        })
    }

    /// Construct a lite consumer by consumer group name + RPC hook.
    pub fn with_rpc_hook(
        consumer_group: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Result<DefaultLitePullConsumer> {
        let consumer = DefaultLitePullConsumer::new(consumer_group)?;
        consumer.set_rpc_hook(rpc_hook);
        Ok(consumer)
    }

    /// Construct directly from a complete config.
    pub fn with_config(cfg: LitePullConsumerConfig) -> Result<DefaultLitePullConsumer> {
        if cfg.consumer_group.trim().is_empty() {
            bail!("consumerGroup is empty");
        }
        // `LiteInner` has a `Drop`, so the `..Default::default()` struct-update syntax is not allowed;
        // build the whole default first, then replace cfg.
        let mut inner = LiteInner::default();
        inner.cfg = RwLock::new(cfg);
        let inner = Arc::new(inner);
        Ok(DefaultLitePullConsumer { inner })
    }

    /// Snapshot of the current config.
    pub fn config(&self) -> LitePullConsumerConfig {
        read_cfg(&self.inner.cfg)
    }

    /// Change the config (direct property assignment + a batch of `set_*`; clamping rules copy the setters).
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

    /// `max(1, n)`.
    pub fn set_pull_batch_size(&self, n: i32) {
        self.update_config(|c| c.pull_batch_size = n);
    }

    /// `max(0, ms)`.
    pub fn set_poll_timeout_millis(&self, ms: i64) {
        self.update_config(|c| c.poll_timeout_millis = ms);
    }

    pub fn set_auto_commit(&self, auto: bool) {
        self.update_config(|c| c.auto_commit = auto);
    }

    /// `max(0, ms)`.
    pub fn set_auto_commit_interval_millis(&self, ms: i64) {
        self.update_config(|c| c.auto_commit_interval_millis = ms);
    }

    /// `max(0, ms)`.
    pub fn set_pull_interval_millis(&self, ms: i64) {
        self.update_config(|self_c| self_c.pull_interval_millis = ms);
    }

    pub fn set_consume_from_where(&self, where_: &str) {
        let where_ = where_.to_string();
        self.update_config(|c| c.consume_from_where = where_);
    }

    pub fn set_consume_timestamp(&self, ts: &str) {
        let ts = ts.to_string();
        self.update_config(|c| c.consume_timestamp = ts);
    }

    pub fn set_namesrv_addr(&self, addr: &str) {
        let addrs = split_addrs(addr);
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

    pub fn set_unit_name(&self, unit_name: Option<&str>) {
        let unit_name = unit_name.map(str::to_string);
        self.update_config(|c| c.unit_name = unit_name);
    }

    pub fn set_namespace_v2(&self, namespace_v2: Option<&str>) {
        let namespace_v2 = namespace_v2.map(str::to_string);
        self.update_config(|c| c.namespace_v2 = namespace_v2);
    }

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

    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.update_config(|c| c.enable_stream_request_type = enable);
    }

    pub fn set_poll_name_server_interval_millis(&self, millis: u64) {
        self.update_config(|c| c.poll_name_server_interval_millis = millis);
    }

    pub fn set_message_model(&self, model: &str) {
        let model = model.to_string();
        self.update_config(|c| c.message_model = model);
    }

    pub fn set_namespace(&self, namespace: &str) {
        let namespace = namespace.to_string();
        self.update_config(|c| c.namespace = namespace);
    }

    pub fn set_rpc_hook(&self, hook: Option<Arc<dyn RPCHook>>) {
        *self
            .inner
            .rpc_hook
            .write()
            .unwrap_or_else(|e| e.into_inner()) = hook;
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

    pub fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy> {
        self.inner
            .strategy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

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

    pub fn unit_mode(&self) -> bool {
        read_cfg(&self.inner.cfg).unit_mode
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    pub fn is_running(&self) -> bool {
        self.inner.running.load(Ordering::Acquire)
    }

    // ---------------- Subscribe / assign ----------------

    /// Switch back to subscribe mode.
    ///
    /// When the expression is invalid (`FilterAPI` errors), only that entry is dropped from the subscription table
    /// ; `subscription` still records it -- consistent with the online baseline.
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

    /// Subscribe using a [`MessageSelector`]: the selector's expression type is kept
    /// end-to-end (subscription table → heartbeat subscription set → the background pull's
    /// `expressionType`). A `TAG` selector (or an empty type) goes through the exact same tag
    /// compilation as [`subscribe`](Self::subscribe) (`tagsSet`/`codeSet`); a non-tag selector
    /// registers only its raw expression with its own `expressionType`, leaving both
    /// collections empty -- which also switches off the client-side re-filter.
    ///
    /// An expression the filter API rejects follows the same stance as [`subscribe`](Self::subscribe):
    /// the `subscription` entry is kept and the `subscription_data` entry is dropped.
    pub fn subscribe_with_selector(&self, topic: &str, selector: &MessageSelector) {
        let topic = with_namespace(&self.config().namespace, topic);
        {
            let mut state = lock(&self.inner.state);
            state.assign_mode = false;
            state
                .subscription
                .insert(topic.clone(), selector.expression.clone());
            match pull_selector_subscription(&topic, selector) {
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

    pub fn unsubscribe(&self, topic: &str) {
        let topic = with_namespace(&self.config().namespace, topic);
        let mut state = lock(&self.inner.state);
        state.subscription.remove(&topic);
        state.subscription_data.remove(&topic);
    }

    /// In assign mode, set the tag
    /// expression for a topic and register the `SubscriptionData` too (the broker needs subscription info in assign mode as well).
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

    /// Switch to assign mode (no rebalance).
    ///
    /// Initial offset resolution needs RPCs, so this only registers the queues and leaves offsets to
    /// [`start`](Self::start) (synchronous resolution has the same semantics, just later timing,
    /// because `assign()` is not async). Queues already in `next_offset` are not overwritten.
    pub fn assign(&self, message_queues: &[MessageQueue]) {
        let mut state = lock(&self.inner.state);
        state.assign_mode = true;
        let keep: BTreeSet<String> = message_queues.iter().map(mq_key).collect();
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

    /// Current subscription table (topic -> expression).
    pub fn subscription(&self) -> Vec<(String, String)> {
        lock(&self.inner.state)
            .subscription
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// The current subscription set carried into heartbeats.
    pub fn subscriptions(&self) -> Vec<SubscriptionData> {
        lock(&self.inner.state)
            .subscription_data
            .values()
            .cloned()
            .collect()
    }

    /// Whether in assign mode.
    pub fn is_assign_mode(&self) -> bool {
        lock(&self.inner.state).assign_mode
    }

    // ---------------- Lifecycle ----------------

    /// Idempotent; first checks the group name, then requires a name server and an existing subscription or assign;
    /// **first caches the routes of the subscribed topics, then sends one heartbeat synchronously**, finally starts the background loops.
    ///
    /// Heartbeats only go to brokers already known in the route table, so the route refresh must happen before that synchronous heartbeat
    /// (see [`refresh_route_for_heartbeat`](Self::refresh_route_for_heartbeat)):
    /// otherwise the start() round sends 0 heartbeats and subscriptions would only reach the broker at the heartbeat loop's first round 5s later,
    /// so with multiple instances in one group the peers would see each other one heartbeat cycle late.
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
        if let Err(e) = consume_timestamp_millis(&cfg.consume_timestamp) {
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
            use_heartbeat_v2: cfg.use_heartbeat_v2,
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
        client.register_consumer_group(&cfg.consumer_group);
        *lock(&self.inner.client) = Some(client);

        // assign mode: complete the initial offsets at start
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

        // Register the tag subscriptions with the broker first, then start the background pull
        self.refresh_route_for_heartbeat().await;
        self.send_heartbeat_to_all_broker().await;
        self.inner.running.store(true, Ordering::Release);
        let _ = self.inner.stop.send(false);
        self.spawn_loops();
        Ok(())
    }

    /// Stop the loops → final commit (when auto_commit) → wake up poll
    /// → close the instance.
    ///
    /// The final commit and `client.shutdown()` are dispatched onto the runtime to run **serially**
    /// (the commit sends RPCs and must happen before the instance closes), and this method **blocks until it lands**
    /// (bounded by [`SHUTDOWN_FINALIZE_BUDGET`], so "the process exits right after shutdown" no longer loses the final
    /// commit); on a current_thread runtime it degrades to a detached task with a warning (see
    /// [`run_finalize_blocking`] cases 2/3); with no runtime it degrades to closing directly.
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
        // Wake up any callers possibly stuck in poll()
        self.inner.buffer_signal.notify_waiters();

        let auto_commit = self.config().auto_commit;
        if auto_commit {
            let mut state = lock(&self.inner.state);
            let scope: Vec<String> = state.assigned.keys().cloned().collect();
            for key in &scope {
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
                // No runtime: the commit is impossible, at least close the connections
                // (the same degradation level as "commit failure only logs at debug").
                rmq_warn!("lite shutdown: no tokio runtime, skip final offset commit");
                self.close_client();
            }
        }
    }

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

    // ---------------- Heartbeat ----------------

    /// Heartbeats only go to "brokers already known in the route table", and a
    /// self-created instance still has an empty route table right after start, so that round sends 0 -- the subscriptions would only get registered
    /// with the broker at the heartbeat loop's first round 5s later. So this first pulls the routes of the topics this instance cares about (subscribed + assigned)
    /// and registers them as in use, letting the one synchronous heartbeat inside start() actually reach the broker.
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

    /// Send one heartbeat to each broker in the route table, returning the number of successes.
    pub async fn send_heartbeat_to_all_broker(&self) -> usize {
        send_lite_heartbeat(&self.inner).await
    }

    // ---------------- Background loops ----------------

    fn spawn_loops(&self) {
        let Some(handle) = self.runtime_handle() else {
            rmq_warn!("lite pull consumer: no tokio runtime, background loops disabled");
            return;
        };
        // Background tasks hold a Weak instead of an Arc: only then can Drop get its chance to run when the consumer is discarded (otherwise the
        // task itself keeps the reference count alive and Drop never fires). The two loops' future types differ, so each needs its own line.
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

    // ---------------- poll / offsets ----------------

    /// Wait for the buffer to become non-empty (at most `poll_timeout_millis`),
    /// taking at most [`MAX_POLL_BATCH_SIZE`] messages per call.
    pub async fn poll(&self, timeout_millis: Option<i64>) -> Vec<MessageExt> {
        if self.config().auto_commit {
            self.maybe_auto_commit().await;
        }
        let timeout = timeout_millis.unwrap_or_else(|| self.config().poll_timeout_millis);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout.max(0) as u64);
        loop {
            // Arm the wakeup before checking the buffer (equivalent to "wait only after the check"):
            // written the other way, the notify enqueued between the check and the suspend would be lost,
            // and the user would wait pointlessly until the deadline. `enable()` is the actual point where interest is registered.
            let mut notified = std::pin::pin!(self.inner.buffer_signal.notified());
            notified.as_mut().enable();
            if let Some(drained) = self.try_drain() {
                self.advance_consume_offset(&drained);
                return drained;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Vec::new();
            }
            // Equivalent of a timed condition wait: when woken, recheck the buffer once,
            // and keep waiting while the deadline has not passed (spurious wakeups are harmless).
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    /// One-shot drain of the buffer (at most 1024 messages).
    /// `None` = the buffer is empty; the caller should keep waiting.
    fn try_drain(&self) -> Option<Vec<MessageExt>> {
        let mut buffer = lock(&self.inner.buffer);
        if buffer.is_empty() {
            return None;
        }
        let take = buffer.len().min(MAX_POLL_BATCH_SIZE);
        Some(buffer.drain(..take).collect())
    }

    /// Advance the **consumed cursor** by the batch just delivered (the pull cursor does not move at all).
    fn advance_consume_offset(&self, msgs: &[MessageExt]) {
        let mut state = lock(&self.inner.state);
        // First index "queues with pull records" by (topic, brokerName, queueId),
        // then reverse-lookup mq_key via the message's own coordinates -- building the key from message fields would bypass the holding check.
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

    /// Pin the cursor and drop from the buffer this queue's messages earlier than `offset`.
    pub fn seek(&self, mq: &MessageQueue, offset: i64) {
        let key = mq_key(mq);
        {
            let mut state = lock(&self.inner.state);
            state.seek_offset.insert(key.clone(), offset);
            state.next_offset.insert(key.clone(), offset);
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

    pub async fn seek_to_begin(&self, mq: &MessageQueue) -> Result<()> {
        let client = Self::require_client(&self.inner)?;
        let offset = client
            .get_min_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await?;
        self.seek(mq, offset);
        Ok(())
    }

    pub async fn seek_to_end(&self, mq: &MessageQueue) -> Result<()> {
        let client = Self::require_client(&self.inner)?;
        let offset = client
            .get_max_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await?;
        self.seek(mq, offset);
        Ok(())
    }

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

    /// The commit source must never be the pull cursor: messages sitting undelivered in the local buffer are not consumed,
    /// and committing them early would make that range never redelivered after a restart (silent message loss).
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

    /// The common part of the three entry points: write the in-memory offset table (with two guards); `persist` then flushes this batch to the broker.
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
                    rmq_error!("consumerOffset is -1 in messageQueue [{key}].");
                    continue;
                }
                if !state.assigned.contains_key(key) {
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

    /// Pull cursor (observation point: broker-side traffic cannot tell the two cursors apart; on a real cluster this is the value to cross-check).
    pub fn pull_cursor_of(&self, mq: &MessageQueue) -> i64 {
        lock(&self.inner.state)
            .next_offset
            .get(&mq_key(mq))
            .copied()
            .unwrap_or(-1)
    }

    /// Consumed cursor (the slot handed out by `poll()`).
    pub fn consume_cursor_of(&self, mq: &MessageQueue) -> i64 {
        lock(&self.inner.state)
            .consume_offset
            .get(&mq_key(mq))
            .copied()
            .unwrap_or(-1)
    }

    /// The slot in the in-memory offset table (values committed with `persist=false` but not yet flushed live here).
    pub fn pending_commit_of(&self, mq: &MessageQueue) -> i64 {
        lock(&self.inner.state)
            .offset_table
            .get(&mq_key(mq))
            .copied()
            .unwrap_or(-1)
    }

    pub async fn offset_for_timestamp(&self, mq: &MessageQueue, timestamp: i64) -> Result<i64> {
        let client = Self::require_client(&self.inner)?;
        client
            .search_offset_by_timestamp(mq, timestamp, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await
    }

    pub fn assignment(&self) -> Vec<MessageQueue> {
        lock(&self.inner.state).assigned.values().cloned().collect()
    }

    /// All consumable queues of the topic (subscription view: read-only, no master filtering); the topic is namespace-wrapped before lookup.
    ///
    /// Two things must be right together, otherwise queue-change listeners silently drift:
    /// 1. **Ask the name server fresh every round**, not the periodically refreshed route cache; skipping this, a scale-out is visible at the earliest after
    ///    one route poll (the listener callback would lag one period behind expectation).
    /// 2. An empty queue set is **an error**, not an empty return -- "cannot find" ≠ "this topic shrank to 0 queues";
    ///    the latter would hand the listener a fake scale-in callback and flush its snapshot to the empty set.
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

    /// `fetch_subscribe_message_queues` (lite version = `fetch_message_queues`).
    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        self.fetch_message_queues(topic).await
    }

    pub fn pause(&self, message_queues: &[MessageQueue]) {
        let mut state = lock(&self.inner.state);
        for mq in message_queues {
            state.paused.insert(mq_key(mq));
        }
    }

    pub fn resume(&self, message_queues: &[MessageQueue]) {
        let mut state = lock(&self.inner.state);
        for mq in message_queues {
            state.paused.remove(&mq_key(mq));
        }
    }

    pub fn set_topic_metadata_check_interval_millis(&self, millis: i64) {
        self.update_config(|c| c.topic_metadata_check_interval_millis = millis.max(1000));
    }

    /// The current compare period (millis).
    pub fn topic_metadata_check_interval_millis(&self) -> i64 {
        self.config().topic_metadata_check_interval_millis
    }

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

    /// Run one compare round (the background loop calls it; tests and callers may also drive it directly), returning how many callbacks fired.
    ///
    /// ⚠ Deliberate: a single topic failing is logged and skipped for that topic only; the other topics of the same round still compare.
    /// A topic whose route cannot be found for a long time should not starve the listeners ordered after it forever; the next round proceeds normally,
    /// and neither style interrupts the scheduling.
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

    /// Current local buffer count (for tests and observation).
    pub fn buffered_message_count(&self) -> usize {
        lock(&self.inner.buffer).len()
    }

    // ---------------- Internals of the pull service ----------------

    /// subscribe table → assign expression → `"*"`.
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

    /// Batch form of initial offset resolution: resolve queue by queue, failures are only logged
    /// (the `assign` / `rebalance` / first-pull sites all swallow the errors).
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

    /// Rebalance (subscribe mode): per topic fetch all queues + the consumer group's instance list
    /// → allocation strategy → union. When the instance list cannot be fetched, **keep the current assignment**
    /// (equivalent to "exclusive when I am the only one"; the same stance as the push consumer here,
    /// see the note on [`crate::client::consumer::DefaultMQPushConsumer`]).
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
        let baseline: Vec<MessageQueue> =
            lock(&self.inner.state).assigned.values().cloned().collect();
        // topic -> (all queues, allocated queues), used for the MessageQueueListener callback
        let mut per_topic: Vec<(String, Vec<MessageQueue>, Vec<MessageQueue>)> = Vec::new();
        for topic in topics {
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
            // Snapshot the old assignment first: `state.assigned` is about to be replaced wholesale, and a borrow cannot survive that assignment.
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
        if !changed_topics.is_empty() {
            let listener = lock(&self.inner.listener).clone();
            if let Some(listener) = listener {
                for (topic, mq_all, divided) in changed_topics {
                    listener.message_queue_changed(&topic, &mq_all, &divided);
                }
            }
        }
    }

    /// Short-poll one batch; on a hit, filter tags → feed the buffer → advance the cursor →
    /// throttle auto-commit. Returning `true` = this round put messages into the buffer (used for the backoff decision).
    async fn pull_one(&self, mq: &MessageQueue) -> bool {
        let Ok(client) = Self::require_client(&self.inner) else {
            return false;
        };
        let key = mq_key(mq);
        // Snapshot first, then await: a MutexGuard is not Send, and calling lock(...) directly in the match
        // scrutinee would make the whole future non-Send (the temporary lives until the match ends).
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
        // A selector subscription owns its expression type and version on the wire; the tag
        // path keeps exactly the historic shape (raw expression, TAG, version 0). The same
        // entry decides the client-side re-filter, so a SQL92 expression is never compiled
        // into a tag set here.
        let registered = lock(&self.inner.state)
            .subscription_data
            .get(&mq.topic)
            .cloned();
        let expr = self.subscription_for(&mq.topic);
        let (expr, expression_type, sub_version) = match &registered {
            Some(sub) if sub.expression_type != ExpressionType::TAG => (
                sub.sub_string.clone(),
                sub.expression_type.clone(),
                sub.sub_version,
            ),
            _ => (expr, ExpressionType::TAG.to_string(), 0),
        };
        // Same class-filter rule as the pull path above: the bit stays clear, the type travels.
        let sys_flag = lite_pull_sys_flag();
        let result = match client
            .pull_message(
                &cfg.consumer_group,
                mq,
                offset,
                cfg.pull_batch_size,
                // Short polling (suspend=False); offsets are committed separately by auto-commit;
                // for the lite bit see lite_pull_sys_flag (#107)
                sys_flag,
                0,
                &expr,
                sub_version,
                &expression_type,
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
        let msgs = client_side_tag_filter(registered.as_ref(), result.msg_found_list);
        if msgs.is_empty() {
            // All dropped by the tag filter: as far as the broker is concerned this window has already been read (the cursor was advanced above),
            // there is simply nothing deliverable -- the window must not be re-pulled in place like the old implementation did.
            return false;
        }
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

/// `seek` → the offset committed on the broker (so consumption continues after a restart)
/// → `CONSUME_FROM_FIRST_OFFSET` ⇒ min → `CONSUME_FROM_TIMESTAMP` ⇒ query by timestamp → otherwise max.
///
/// Unlike the push consumer's `resolve_initial_offset`: lite has no local offset store for broadcasting mode,
/// no SQL92 special case, and no special start for `%RETRY%` (lite does not auto-subscribe retry topics).
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
        return Ok(0);
    }
    if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_TIMESTAMP {
        // Shares the parser with the push consumer: the consumeTimestamp semantics of both consumers must stay consistent
        let ts = consume_timestamp_millis(&cfg.consume_timestamp)?;
        return client
            .search_offset_by_timestamp(mq, ts, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
            .await;
    }
    client
        .get_max_offset(mq, LITE_PULL_RPC_TIMEOUT_MILLIS, None)
        .await
}

/// Heartbeat packet: carries only **this consumer's own** ConsumerData
/// (lite does not enter the instance registry; the packet is assembled here).
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
    cd.unit_mode = cfg.unit_mode;
    hb.consumer_data_set.push(cd);
    hb
}

/// Send one copy to every broker in the route table (**including slaves**).
/// With no client (not started / already shut down) it sends 0 copies instead of exiting the loop.
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

/// Sends one round first (start() already sent one round synchronously; this immediately follows as the second,
/// in the same order as the lite version), then one round every 5s.
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

/// First check 10s after startup, then compare queue-set changes every `topic_metadata_check_interval_millis`.
async fn metadata_loop(inner: Arc<LiteInner>, rx: watch::Receiver<bool>) {
    metadata_loop_after(&inner, rx, LITE_METADATA_FIRST_DELAY_MILLIS).await;
}

/// Same as `metadata_loop` but with an injectable first-check delay; unit tests use it to shrink the 10s to a few tens of milliseconds.
///
/// ⚠ The first-check delay applies only **before the first round**. Left inside the loop, every round would wait a full 10s first,
/// dragging the 1s check period out to 11s -- after a scale-out the listener would react an order of magnitude later.
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

/// A single loop round-robins all assigned queues.
/// In subscribe mode it rebalances every >1s; when a round yields no messages it backs off
/// `pull_interval_millis`; with messages it resumes pulling after just 5ms.
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

/// Sort-key form of `mq_key`, used to keep `BTreeMap` key order consistent with queue order.
fn _key_order_proof(mq: &MessageQueue) -> (String, String, i32) {
    mq_sort_key(mq)
}

#[cfg(test)]
mod tests {
    use super::*;
    // Minimal heartbeat end-to-end loop outside real brokers (fake namesrv + fake broker): the frame read/write matches the
    // producer's `send_retry_tests`, but only the 34/35 request codes matter here.
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

    // ---------------------------------------------------------- Lifecycle guards

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
            let pull = DefaultMQPullConsumer::new(group).expect("construction must not reject early");
            pull.set_namesrv_addr("127.0.0.1:1");
            let err = pull.start().await.expect_err("pull: an illegal group name must fail locally");
            assert!(err.to_string().contains(needle), "pull {group}: {err}");
            assert!(
                !pull.is_started(),
                "pull {group}: a failed start must roll back started"
            );

            let lite = DefaultLitePullConsumer::new(group).expect("construction must not reject early");
            lite.set_namesrv_addr("127.0.0.1:1");
            lite.subscribe("T", "TagA");
            let err = lite.start().await.expect_err("lite: an illegal group name must fail locally");
            assert!(err.to_string().contains(needle), "lite {group}: {err}");
            assert!(
                !lite.is_started(),
                "lite {group}: a failed start must roll back started"
            );
        }
    }

    #[tokio::test]
    async fn pull_consumer_rejects_start_without_namesrv_and_use_before_start() {
        let c = DefaultMQPullConsumer::new("PG").unwrap();
        assert!(c.start().await.is_err());
        let mq = queue("T", "broker-a", 0);
        // Every entry point before start must report the same error, not panic / spin
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
        // shutdown before start is a no-op and must not throw
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
        // The missing name server check comes first, and the order is fixed
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

    // ---------------------------------------------------------- Config shape

    #[test]
    fn config_defaults_match_the_golden_reference() {
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
        assert_eq!(l.consume_timestamp.len(), 14);
        assert!(l.consume_timestamp.bytes().all(|b| b.is_ascii_digit()));
        assert_eq!(l.broker_suspend_max_time_millis, 20_000);
        assert_eq!(l.consumer_timeout_millis_when_suspend, 30_000);
    }

    #[test]
    fn setters_clamp_like_the_golden_setters() {
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

    // ---------------------------------------------------------- Pull sysFlags

    /// Test the suspend bit of sysFlag directly.
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

    // ---------------------------------------------------------- Subscription / assignment tables

    #[test]
    fn subscriptions_are_namespaced_and_overwritable() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.set_namespace("NS1");
        c.subscribe("Topic", "TagA");
        let subs = c.subscription();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].0, "NS1%Topic");
        assert_eq!(c.subscriptions()[0].tags_set, vec!["TagA".to_string()]);
        // Subscribing the same topic again overwrites, it does not append
        c.subscribe("Topic", "TagB");
        assert_eq!(c.subscription().len(), 1);
        assert_eq!(c.subscription()[0].1, "TagB");
        c.unsubscribe("Topic");
        assert!(c.subscription().is_empty());
        assert!(c.subscriptions().is_empty());
    }

    #[test]
    fn bad_tag_expression_drops_only_subscription_data() {
        // `"||"` makes FilterAPI throw `subString split error`: keep the
        // `subscription` entry and drop the `subscription_data`
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.subscribe("T", "||");
        assert_eq!(c.subscription().len(), 1);
        assert!(c.subscriptions().is_empty());
    }

    #[test]
    fn subscribe_with_selector_keeps_the_expression_type() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.subscribe_with_selector("SQL", &MessageSelector::by_sql("a > 1"));
        let sd = &c.subscriptions()[0];
        assert_eq!(sd.expression_type, ExpressionType::SQL92);
        assert_eq!(sd.sub_string, "a > 1");
        assert!(
            sd.tags_set.is_empty() && sd.code_set.is_empty(),
            "a SQL92 entry must not compile tag collections"
        );
        assert_eq!(
            c.subscription()[0].1,
            "a > 1",
            "the raw expression stays in the subscription table"
        );

        // A TAG selector keeps exactly the tag shape `subscribe` produces.
        c.subscribe_with_selector("TAG", &MessageSelector::by_tag("TagB || TagA"));
        let sd = c
            .subscriptions()
            .into_iter()
            .find(|s| s.topic == "TAG")
            .expect("TAG entry registered");
        let built = FilterAPI::build_subscription_data("TAG", Some("TagB || TagA")).unwrap();
        assert_eq!(sd.expression_type, ExpressionType::TAG);
        assert_eq!(sd.tags_set, built.tags_set);
        assert_eq!(sd.code_set, built.code_set);
        assert_eq!(sd.sub_string, built.sub_string);

        // An empty selector type falls back to the TAG path.
        c.subscribe_with_selector("PLAIN", &MessageSelector::new("", "TagA"));
        let sd = c
            .subscriptions()
            .into_iter()
            .find(|s| s.topic == "PLAIN")
            .expect("PLAIN entry registered");
        assert_eq!(sd.expression_type, ExpressionType::TAG);
        assert_eq!(sd.tags_set, vec!["TagA".to_string()]);

        // A non-tag selector without an expression is rejected by the filter API: same stance as
        // a bad tag expression (keep the `subscription` entry, drop the `subscription_data`).
        c.subscribe_with_selector("BAD", &MessageSelector::by_sql(""));
        assert!(c.subscription().iter().any(|(t, _)| t == "BAD"));
        assert!(!c.subscriptions().iter().any(|s| s.topic == "BAD"));
    }

    #[test]
    fn lite_heartbeat_carries_selector_expression_type() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.update_config(|cfg| cfg.client_id = Some("cid-sel".to_string()));
        c.subscribe_with_selector("T", &MessageSelector::by_sql("color = 'blue'"));
        let hb = build_lite_heartbeat(&c.inner);
        let set = &hb.consumer_data_set[0].subscription_data_set;
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].topic, "T");
        assert_eq!(set[0].expression_type, ExpressionType::SQL92);
        assert_eq!(set[0].sub_string, "color = 'blue'");
        assert!(set[0].tags_set.is_empty());
        // The wire payload keeps the field names the broker parses.
        let json = set[0].to_json_value();
        assert_eq!(json["expressionType"], serde_json::json!("SQL92"));
    }

    #[test]
    fn assign_switches_mode_and_keeps_subscription_expr_for_heartbeat() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        c.set_sub_expression_for_assign("T", "TagA");
        assert!(!c.is_assign_mode());
        c.assign(&[queue("T", "broker-a", 1), queue("T", "broker-a", 0)]);
        assert!(c.is_assign_mode());
        // The assignment snapshot is ordered by key (deterministic iteration order)
        assert_eq!(
            c.assignment(),
            vec![queue("T", "broker-a", 0), queue("T", "broker-a", 1)]
        );
        assert_eq!(c.subscription_for("T"), "TagA");
        assert_eq!(c.subscription_for("other"), "*");
        assert_eq!(c.subscriptions().len(), 1);
    }

    // ---------------------------------------------------- The three offset tables (#68)

    #[tokio::test]
    async fn poll_advances_consume_cursor_not_pull_cursor() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        let q = queue("T", "broker-a", 0);
        // The pull cursor is advanced by the background pull: unit tests have no network, so it is set to 5 by hand (only 3 delivered messages sit in the buffer)
        {
            let mut state = lock(&c.inner.state);
            state.assigned.insert(mq_key(&q), q.clone());
            state.next_offset.insert(mq_key(&q), 5);
        }
        assert_eq!(c.pull_cursor_of(&q), 5, "pull cursor = the slot the background has already pulled to");
        assert_eq!(
            c.consume_cursor_of(&q),
            -1,
            "nothing delivered ⇒ the consumed cursor is still -1"
        );
        // 3 messages sit in the buffer: not consumed until they are delivered
        c.enqueue(
            (0..3)
                .map(|i| msg("T", "broker-a", 0, i, &format!("m{i}")))
                .collect(),
        );
        assert_eq!(c.consume_cursor_of(&q), -1, "not polled yet, so not consumed");
        let got = c.poll(Some(10)).await;
        assert_eq!(got.len(), 3);
        assert_eq!(c.consume_cursor_of(&q), 3, "the slot handed out is what counts as consumed");
        assert_eq!(c.pull_cursor_of(&q), 5, "poll must never change the pull cursor");
    }

    #[tokio::test]
    async fn poll_ignores_queues_this_instance_does_not_hold() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
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
        // commitAll works off the consumed cursor: nothing delivered ⇒ not a single slot is written
        c.commit().await.unwrap();
        assert_eq!(
            c.pending_commit_of(&q0),
            -1,
            "commitAll does not commit queues that were never consumed"
        );

        // Explicit offsets write only the commit target; neither cursor moves
        let mut specified = BTreeMap::new();
        specified.insert(mq_key(&q0), 5);
        specified.insert(mq_key(&q1), 8);
        c.commit_offsets(&specified, false).await.unwrap();
        assert_eq!(c.pending_commit_of(&q0), 5, "explicit offsets go into the in-memory offset table");
        assert_eq!(c.pending_commit_of(&q1), 8, "explicit offsets go into the in-memory offset table (q1)");
        assert_eq!(c.pull_cursor_of(&q0), -1, "committing an offset does not move the pull cursor");
        assert_eq!(c.consume_cursor_of(&q0), -1, "committing an offset does not move the consumed cursor");

        let mut guarded = BTreeMap::new();
        guarded.insert(mq_key(&q0), -1);
        guarded.insert(mq_key(&queue("T", "broker-a", 7)), 3);
        c.commit_offsets(&guarded, false).await.unwrap();
        assert_eq!(
            c.pending_commit_of(&q0),
            5,
            "offset == -1 only logs, it does not overwrite an existing offset"
        );
        assert_eq!(
            c.pending_commit_of(&queue("T", "broker-a", 7)),
            -1,
            "a queue that was never assigned is not committed on its behalf"
        );

        c.commit_offsets(&BTreeMap::new(), false).await.unwrap();
        assert_eq!(c.pending_commit_of(&q0), 5, "an empty map ignores this commit");
        c.commit_queues(&[], false).await.unwrap();
        assert_eq!(c.pending_commit_of(&q0), 5, "an empty queue set ignores this commit");

        // commit(Set) works off the consumed cursor, not arbitrary values: nothing delivered ⇒ the guard blocks it
        c.commit_queues(std::slice::from_ref(&q0), false)
            .await
            .unwrap();
        assert_eq!(
            c.pending_commit_of(&q0),
            5,
            "commit(Set) writes no -1 when there is no delivery record"
        );

        c.assign(std::slice::from_ref(&q0));
        assert_eq!(c.pull_cursor_of(&q1), -1, "the pull cursor disappears after assign revokes the queue");
        assert_eq!(
            c.consume_cursor_of(&q1),
            -1,
            "the consumed cursor disappears after assign revokes the queue"
        );
        assert_eq!(c.pending_commit_of(&q1), 8, "assign mode does not touch the offsetStore");
        let mut late = BTreeMap::new();
        late.insert(mq_key(&q1), 99);
        c.commit_offsets(&late, false).await.unwrap();
        assert_eq!(c.pending_commit_of(&q1), 8, "a revoked queue's offset is not amended on its behalf");

        c.commit_queues(std::slice::from_ref(&q0), true)
            .await
            .unwrap();
        assert_eq!(
            c.pending_commit_of(&q1),
            -1,
            "persistAll drops queues that were not named from the in-memory table"
        );
        assert_eq!(c.pending_commit_of(&q0), 5, "the named queue stays in the table");
    }

    #[test]
    fn seek_moves_both_cursors() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        let q = queue("T", "broker-a", 0);
        c.assign(std::slice::from_ref(&q));
        c.seek(&q, 2);
        assert_eq!(c.pull_cursor_of(&q), 2, "seek moves the pull cursor");
        assert_eq!(c.consume_cursor_of(&q), 2, "seek must move the consumed cursor too");
    }

    #[tokio::test]
    async fn auto_commit_deadline_starts_at_minus_one() {
        let c = DefaultLitePullConsumer::new("LitePG").unwrap();
        assert_eq!(lock(&c.inner.state).next_auto_commit_deadline, -1);
        // Nothing was delivered: this "due commit" sends nothing and only pushes the deadline to the next period
        c.maybe_auto_commit().await;
        assert!(
            lock(&c.inner.state).next_auto_commit_deadline > 0,
            "the commit must push the deadline to the next period"
        );
        // Check again: not due yet, so it must not commit twice
        let before = lock(&c.inner.state).next_auto_commit_deadline;
        c.maybe_auto_commit().await;
        assert_eq!(
            lock(&c.inner.state).next_auto_commit_deadline,
            before,
            "it should not commit again before the deadline"
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

    // ---------------------------------------------------------- Buffer / poll / seek

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
        // Repeated pause / resume does not error
        c.pause(&[mq.clone(), mq.clone()]);
        c.resume(&[mq]);
        assert!(lock(&c.inner.state).paused.is_empty());
    }

    // ---------------------------------------------------------- Timestamp parsing

    /// Golden vector: this field is **only** a 14-digit local wall clock; an all-digits epoch-millis value must be rejected --
    /// the old implementation took the `isdigit()` branch and read "20230101000000" as epoch millis, landing in the year 2611 AD.
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
        // A valid wall clock must not be killed by this guard by mistake
        c.set_consume_timestamp("20230101000000");
        assert!(c.start().await.is_ok());
        c.shutdown();
    }

    #[test]
    fn namespace_v2_setter_getter_round_trip_on_both_pull_facades() {
        let pull = DefaultMQPullConsumer::new("PullPG").expect("a legal group name must not fail construction");
        assert_eq!(pull.get_namespace_v2(), None, "unset by default");
        pull.set_namespace_v2(Some("NS_V2"));
        assert_eq!(pull.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(pull.config().namespace_v2, Some("NS_V2".to_string()));
        pull.set_namespace_v2(None);
        assert_eq!(
            pull.get_namespace_v2(),
            None,
            "None = clear"
        );

        let lite = DefaultLitePullConsumer::new("LitePG").expect("a legal group name must not fail construction");
        assert_eq!(lite.get_namespace_v2(), None);
        lite.set_namespace_v2(Some("NS_V2"));
        assert_eq!(lite.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(lite.config().namespace_v2, Some("NS_V2".to_string()));
        lite.set_namespace("ns1");
        assert_eq!(
            lite.get_namespace_v2(),
            Some("NS_V2".to_string()),
            "the v1 namespace does not overwrite v2"
        );
        lite.set_namespace_v2(None);
        assert_eq!(lite.get_namespace_v2(), None);
    }

    #[tokio::test]
    async fn lite_start_registers_namespace_hook_before_stream_and_user_acl() {
        // Facade pass-through seams: `LitePullConsumerConfig.namespace_v2` -> `MQClientInstanceConfig`
        // -> the instance transport layer. The chain order must be Namespace -> Stream (on by default in pull mode) -> user ACL,
        // and nsd/ns/ReqT are all written before the ACL signature is computed.
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
        // Unreachable namesrv: start still succeeds (route refresh fails softly, same as `lite_start_rejects_epoch_millis_timestamp`)
        c.start().await.expect("start should succeed");
        let client = DefaultLitePullConsumer::require_client(&c.inner).unwrap();
        let hooks = client.remoting_client().rpc_hooks();
        assert_eq!(hooks.len(), 3, "Namespace → Stream → user ACL");

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
            "the first hook must be Namespace"
        );
        hooks[1].do_before_request("127.0.0.1:9876", &mut cmd);
        // The lite consumer defaults enable_stream_request_type=true ⇒ the second hook is Stream
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

    // ---------------------------------------------------------- Heartbeat packet

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
        assert_eq!(cd.consume_type, ConsumeType::CONSUME_ACTIVELY);
        assert_eq!(cd.message_model, MessageModel::CLUSTERING);
        assert_eq!(
            cd.consume_from_where,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET
        );
        assert!(!cd.unit_mode);
        assert_eq!(cd.subscription_data_set.len(), 1);
        assert_eq!(cd.subscription_data_set[0].topic, "T");
        // With no subscriptions there is still one ConsumerData
        c.unsubscribe("T");
        assert!(build_lite_heartbeat(&c.inner).consumer_data_set[0]
            .subscription_data_set
            .is_empty());
    }

    // -------------------------------------------- Pull-mode consumer heartbeat (#98)

    /// Fake namesrv: answers only `GET_ROUTEINFO_BY_TOPIC`, returning a "one master + one slave" route
    /// (same brokerName, brokerId 0/1); all other requests just get SUCCESS.
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
                            // Re-read on every request: a case that changes the queue count mid-run (scale in/out) must be visible to the
                            // next route refresh, otherwise "the set changed" cannot be tested.
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

    /// The route body for "one master + one slave" (the JSON form of `TopicRouteData`).
    /// `read_queues` is the read-queue count: the client expands it into queueId 0..n-1 via `topicRouteData2TopicSubscribeInfo`,
    /// and the sharding criterion of the balanced view relies on it (with 1 queue, "own share" and "all" have the same shape,
    /// proving nothing).
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
        serde_json::to_vec(&route.to_json_value()).expect("the route must serialize")
    }

    /// Read one whole frame and decode it (`decode` starts at totalLength, so the frame must come with its length prefix).
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

    /// A response carrying the request's `opaque` (the client pairs by opaque and drops mismatches as noise).
    fn answer_for(request: &RemotingCommand, code: i32) -> RemotingCommand {
        let mut response = RemotingCommand::create_response(code, None);
        response.opaque = request.opaque;
        response.serialize_type_current_rpc = request.serialize_type_current_rpc;
        response
    }

    /// An extFields snapshot of one 35 (UNREGISTER_CLIENT) request.
    type UnregisterExt = Vec<(String, String)>;

    /// One received request: request code + extFields snapshot.
    type RecordedRequest = (i32, Vec<(String, String)>);

    /// One scripted PULL_MESSAGE reply.
    struct ScriptedPull {
        code: i32,
        ext: Vec<(&'static str, String)>,
        /// The reply body (the message stream for FOUND; empty in other shapes).
        body: Option<Vec<u8>>,
        /// Departure gate: with `Some`, the reply waits for the gate to open first -- in-flight races like
        /// "the reply is still on the wire while the client already seeked" are sequenced with it.
        gate: Option<tokio::sync::oneshot::Receiver<()>>,
    }

    /// Fake broker: records the 34 (heartbeat body) and 35 (unregister extFields) packets; everything else just gets SUCCESS.
    struct FakeBroker {
        addr: String,
        /// The raw HEART_BEAT bodies received, in arrival order.
        heartbeat_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
        /// The UNREGISTER_CLIENT extFields received, in arrival order.
        unregisters: Arc<Mutex<Vec<UnregisterExt>>>,
        /// **Every** received request (code + extFields snapshot), in arrival order -- the "did this RPC
        /// actually go out" style assertions (e.g. FIRST_OFFSET must not send minOffset) rely on it.
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        /// Pre-scripted PULL_MESSAGE replies (FIFO): first come first served; once drained, answer in the
        /// "empty broker" shape of [`answer_pull`].
        pull_scripts: Arc<Mutex<VecDeque<ScriptedPull>>>,
        /// The reply content for 38 (GET_CONSUMER_LIST_BY_GROUP): which clientIds the group currently has.
        /// An empty Vec means "the broker does not know this group" (it answers an empty list), which lands in the same branch as
        /// "not found" -- enough to exercise the fallback path of the balanced view.
        cid_list: Arc<Mutex<Vec<String>>>,
    }

    impl FakeBroker {
        async fn start() -> Arc<FakeBroker> {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind the fake broker");
            let addr = listener.local_addr().expect("fake broker address").to_string();
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

        /// The received request codes, in arrival order.
        fn codes(&self) -> Vec<i32> {
            lock(&self.requests).iter().map(|(code, _)| *code).collect()
        }

        /// The `queueOffset`s of the received PULL_MESSAGEs, in arrival order.
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

        /// The **request codes** of the received pull requests (11=PULL_MESSAGE / 361=LITE_PULL_MESSAGE), in arrival order.
        fn pull_request_codes(&self) -> Vec<i32> {
            lock(&self.requests)
                .iter()
                .filter(|(code, _)| {
                    *code == request_code::PULL_MESSAGE || *code == request_code::LITE_PULL_MESSAGE
                })
                .map(|(code, _)| *code)
                .collect()
        }

        /// The `sysFlag`s of the received pull requests, in arrival order.
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

        /// The value of one ext field (`expressionType` / `subscription` / `subVersion` ...) of
        /// every received pull request that carries it, in arrival order.
        fn pull_ext_values(&self, key: &str) -> Vec<String> {
            lock(&self.requests)
                .iter()
                .filter(|(code, _)| {
                    *code == request_code::PULL_MESSAGE || *code == request_code::LITE_PULL_MESSAGE
                })
                .filter_map(|(_, ext)| {
                    ext.iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.clone())
                })
                .collect()
        }

        /// Decode every received heartbeat into `HeartbeatData` (a decode failure blows up right in the assertion).
        fn heartbeats(&self) -> Vec<HeartbeatData> {
            lock(&self.heartbeat_bodies)
                .iter()
                .map(|b| HeartbeatData::decode(b).expect("the body of 34 must be a valid HeartbeatData"))
                .collect()
        }

        fn unregisters(&self) -> Vec<UnregisterExt> {
            lock(&self.unregisters).clone()
        }

        /// Set the reply content of 38 (GET_CONSUMER_LIST_BY_GROUP): which clientIds the group has.
        fn set_cid_list(&self, cids: &[&str]) {
            *lock(&self.cid_list) = cids.iter().map(|s| s.to_string()).collect();
        }

        /// Queue one scripted PULL_MESSAGE reply (FIFO hit; each request is answered only once).
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

    fn answer_consumer_list(request: &RemotingCommand, broker: &FakeBroker) -> RemotingCommand {
        let cids = lock(&broker.cid_list).clone();
        let body = GetConsumerListByGroupResponseBody {
            consumer_id_list: cids,
        };
        let mut response = answer_for(request, response_code::SUCCESS);
        response.set_body(Some(body.encode()));
        response
    }

    /// A fake cluster of one master + one slave (same brokerName).
    struct FakePullCluster {
        namesrv_addr: String,
        master: Arc<FakeBroker>,
        slave: Arc<FakeBroker>,
        tasks: Vec<JoinHandle<()>>,
        /// The fake namesrv reads this value on the spot when answering routes: changing it is "the topic scaling in/out".
        read_queues: Arc<std::sync::atomic::AtomicI32>,
        /// Every route query the fake namesrv has received (in arrival order, duplicates included).
        ns_route_queries: Arc<Mutex<Vec<String>>>,
    }

    impl FakePullCluster {
        async fn start() -> FakePullCluster {
            FakePullCluster::start_queues(1).await
        }

        /// Change the route's read-queue count to `n` (scale in/out); the next route refresh will see it.
        fn scale_to(&self, n: i32) {
            self.read_queues.store(n, std::sync::atomic::Ordering::Release);
        }

        /// Discard the recorded route queries so later assertions only see "this round".
        fn clear_route_queries(&self) {
            lock(&self.ns_route_queries).clear();
        }

        fn route_queries(&self) -> Vec<String> {
            lock(&self.ns_route_queries).clone()
        }

        /// Set the 38 reply on both master and slave together: which one the query reaches is decided by the route; cases should not care about that detail.
        fn set_cid_list(&self, cids: &[&str]) {
            self.master.set_cid_list(cids);
            self.slave.set_cid_list(cids);
        }

        /// The broker carrying `read_queues` read queues (everything else as in [`FakePullCluster::start`]).
        async fn start_queues(read_queues: i32) -> FakePullCluster {
            let master = FakeBroker::start().await;
            let slave = FakeBroker::start().await;
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind the fake namesrv");
            let namesrv_addr = listener.local_addr().expect("fake namesrv address").to_string();
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

    /// Start a pull-mode consumer pointing at the fake cluster. `instance` must be unique per case: the same clientId would share
    /// the [`MQClientInstance`] (and with it the route table and the namesrv address).
    async fn started_pull(
        instance: &str,
        group: &str,
        cluster: &FakePullCluster,
        topic: &str,
    ) -> DefaultMQPullConsumer {
        let c = DefaultMQPullConsumer::new(group).expect("the group name is legal");
        c.set_instance_name(instance);
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.register_topic(topic);
        c.start().await.expect("start should succeed against the fake cluster");
        c
    }

    /// Poll until the condition holds (at most 5s); panic on failure, so a fixed sleep cannot mask a race.
    async fn wait_until(mut cond: impl FnMut() -> bool, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("wait timed out: {what}");
    }

    #[test]
    fn pull_heartbeat_matches_the_wire_shape() {
        let c = DefaultMQPullConsumer::new("PG_PullHb").unwrap();
        c.update_config(|cfg| cfg.client_id = Some("cid-1".to_string()));
        c.register_topic("T2");
        c.register_topic("T1");
        let hb = build_pull_heartbeat(&c.inner);
        assert_eq!(hb.client_id, "cid-1");
        assert_eq!(hb.consumer_data_set.len(), 1, "the pull group sends only its own ConsumerData");
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
        assert_eq!(topics, vec!["T1", "T2"], "the subscription set comes from registerTopics");
        for sub in &cd.subscription_data_set {
            assert_eq!(sub.sub_string, "*");
            assert_eq!(sub.sub_version, 0, "subVersion is explicitly set to 0");
        }

        // Without registerTopics: still one ConsumerData, with an empty subscription set
        let bare = DefaultMQPullConsumer::new("PG_PullHbBare").unwrap();
        let hb2 = build_pull_heartbeat(&bare.inner);
        assert_eq!(hb2.consumer_data_set.len(), 1);
        assert!(hb2.consumer_data_set[0].subscription_data_set.is_empty());
    }

    /// The synchronous round of start() must send **one copy each to the master and the slave**: each broker's ConsumerManager keeps its own
    /// state, and a slave that never received the heartbeat would answer pulls pointed at it with `SUBSCRIPTION_NOT_EXIST`.
    #[tokio::test]
    async fn pull_start_heartbeats_master_and_slave_with_the_wire_shape() {
        let cluster = FakePullCluster::start().await;
        let c = started_pull("pull_hb_start", "PG_PullHbStart", &cluster, "T").await;

        let master = cluster.master.heartbeats();
        let slave = cluster.slave.heartbeats();
        assert_eq!(master.len(), 1, "the synchronous round of start reaches the master first");
        assert_eq!(slave.len(), 1, "the slave must receive one too");
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
        assert_eq!(c.heartbeat_count(), 1, "a round counts only when both brokers succeeded");
        c.shutdown();
    }

    /// The background loop repeats every `heartbeat_broker_interval_millis`; `heartbeat_enabled=false`
    /// only suppresses sending without exiting the loop (the round is skipped, not the loop ended).
    #[tokio::test]
    async fn pull_heartbeat_loop_repeats_and_honours_the_disable_switch() {
        let cluster = FakePullCluster::start().await;
        let c = DefaultMQPullConsumer::new("PG_PullHbLoop").unwrap();
        c.set_instance_name("pull_hb_loop");
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.register_topic("T");
        c.update_config(|cfg| cfg.heartbeat_broker_interval_millis = 100);
        c.start().await.expect("start should succeed against the fake cluster");

        // The initial synchronous round + at least two periodic rounds
        wait_until(
            || cluster.master.heartbeats().len() >= 3 && cluster.slave.heartbeats().len() >= 3,
            "the heartbeat loop repeats every 100ms (master and slave counts grow together)",
        )
        .await;

        // Switch off: first let one quiet beat pass so in-flight rounds land, then check whether the count froze
        c.update_config(|cfg| cfg.heartbeat_enabled = false);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let frozen = c.heartbeat_count();
        let master_seen = cluster.master.heartbeats().len();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            c.heartbeat_count(),
            frozen,
            "nothing is sent after heartbeat_enabled=false"
        );
        assert_eq!(
            cluster.master.heartbeats().len(),
            master_seen,
            "the wire must not carry any new 34 either"
        );
        c.shutdown();
    }

    #[tokio::test]
    async fn pull_shutdown_unregisters_group_on_every_broker() {
        let cluster = FakePullCluster::start().await;
        let c = started_pull("pull_hb_unreg", "PG_PullHbUnreg", &cluster, "T").await;
        let client_id = c.client_id();
        c.shutdown();
        // The 35 goes out asynchronously on the runtime (shutdown is a sync API): wait for it to land
        wait_until(
            || !cluster.master.unregisters().is_empty() && !cluster.slave.unregisters().is_empty(),
            "both the master and the slave receive a 35",
        )
        .await;

        for (name, broker) in [("master", &cluster.master), ("slave", &cluster.slave)] {
            let unregs = broker.unregisters();
            assert_eq!(unregs.len(), 1, "{name}: one shutdown unregisters exactly once");
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
            assert_eq!(field("producerGroup"), None, "{name}: an empty slot never goes on the wire");
        }
        // Once unregistered, heartbeating must already have stopped (the loop is stopped and aborted before the 35 goes out)
        let beats = cluster.master.heartbeats().len();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            cluster.master.heartbeats().len(),
            beats,
            "no heartbeat is sent after shutdown"
        );
    }

    /// Even "the process exits right after shutdown" still sends it out: on a multi_thread runtime `shutdown()` waits until the 35 has
    /// landed before returning. The assertions below run **with no await and no extra beat** after the return -- under the old
    /// implementation (spawn then return immediately) the finalize task had never been scheduled, so the 35 was necessarily missing; this case exists to pin that regression.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_returns_only_after_unregister_landed() {
        let cluster = FakePullCluster::start().await;
        let c = started_pull("pull_hb_unreg_wait", "PG_PullUnregWait", &cluster, "T").await;
        let client_id = c.client_id();

        c.shutdown();

        // By this point the finalize must be complete: one landed 35 on each of the master and the slave.
        for (name, broker) in [("master", &cluster.master), ("slave", &cluster.slave)] {
            let unregs = broker.unregisters();
            assert_eq!(unregs.len(), 1, "{name}: the 35 must have landed by the time shutdown returned");
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

    /// An immediate process exit does not lose it: the whole runtime is dropped right after `shutdown()` returns (equivalent to
    /// "`shutdown` followed immediately by `process::exit`"). Under the old implementation the finalize task was cancelled along with the runtime,
    /// and neither the master nor the slave received a 35; now the 35 has landed before the return, and tearing down the runtime only sweeps the remains.
    #[test]
    fn shutdown_survives_immediate_runtime_drop() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let (cluster, consumer, client_id) = runtime.block_on(async {
            let cluster = FakePullCluster::start().await;
            let c = started_pull("pull_unreg_exit", "PG_PullUnregExit", &cluster, "T").await;
            let client_id = c.client_id();
            (cluster, c, client_id)
        });

        // Calling shutdown outside a runtime (a plain std thread) -- exactly the shape of `main()` cleanup.
        consumer.shutdown();
        drop(runtime);

        for (name, broker) in [("master", &cluster.master), ("slave", &cluster.slave)] {
            let unregs = broker.unregisters();
            assert_eq!(unregs.len(), 1, "{name}: the 35 must have landed before process exit");
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
        // SUB_ALL leaves tags_set empty ⇒ no filtering
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
            "the pull default strategy is AVG"
        );
        pull.set_allocate_message_queue_strategy(Arc::new(AllocateMessageQueueAveragelyByCircle));
        let pull_strategy = pull.allocate_message_queue_strategy();
        assert_eq!(pull_strategy.get_name(), "AVG_BY_CIRCLE", "pull allows replacement");
        assert_eq!(
            allocate_ids(&*pull_strategy, "PG"),
            vec![0, 2],
            "the strategy set on pull is exactly the one the getter reads back"
        );

        let lite = DefaultLitePullConsumer::new("LG").unwrap();
        assert_eq!(
            lite.allocate_message_queue_strategy().get_name(),
            "AVG",
            "the lite default strategy is AVG"
        );
        let by_config = Arc::new(AllocateMessageQueueByConfig::new(vec![
            queue("T", "broker-a", 0),
            queue("T", "broker-a", 1),
        ]));
        lite.set_allocate_message_queue_strategy(by_config.clone());
        assert_eq!(lite.allocate_message_queue_strategy().get_name(), "CONFIG");
        // The getter hands out a shared reference, not a snapshot: later changes to the list still act on rebalance
        by_config.set_message_queue_list(vec![queue("T", "broker-a", 3)]);
        assert_eq!(
            allocate_ids(&*lite.allocate_message_queue_strategy(), "LG"),
            vec![3],
            "the CONFIG strategy ignores mqAll/cidAll and returns the configured queues"
        );
    }

    // --------------------------------- FIRST_OFFSET starts at literal 0 (side note of #100)

    /// minOffset is queried on the master only (the admin-side stance here); while the master is down a newly started lite-pull
    /// would fetch nothing at all; an out-of-range start offset 0 is corrected by the broker via `PULL_OFFSET_MOVED`
    /// (`handle_offset_illegal`).
    #[tokio::test]
    async fn lite_first_offset_starts_at_zero_without_a_min_offset_rpc() {
        let cluster = FakePullCluster::start().await;

        let first = DefaultLitePullConsumer::new("LitePG_First").unwrap();
        first.set_instance_name("lite_first_offset");
        first.set_namesrv_addr(&cluster.namesrv_addr);
        first.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        first.assign(&[queue("T", "broker-a", 0)]);
        first.start().await.expect("start should succeed against the fake cluster");
        wait_until(
            || !cluster.master.pull_offsets().is_empty(),
            "the FIRST_OFFSET consumer's first pull",
        )
        .await;
        assert_eq!(
            cluster.master.pull_offsets()[0],
            0,
            "the start point is simply 0, sent directly in the pull request"
        );

        // Negative control: the LAST_OFFSET branch must send maxOffset -- first prove this request log is not mute
        let last = DefaultLitePullConsumer::new("LitePG_Last").unwrap();
        last.set_instance_name("lite_last_offset");
        last.set_namesrv_addr(&cluster.namesrv_addr);
        last.assign(&[queue("T", "broker-a", 0)]);
        last.start().await.expect("start should succeed against the fake cluster");
        wait_until(
            || {
                cluster
                    .master
                    .codes()
                    .contains(&request_code::GET_MAX_OFFSET)
            },
            "the LAST_OFFSET consumer's maxOffset query",
        )
        .await;

        let codes = cluster.master.codes();
        assert!(
            !codes.contains(&request_code::GET_MIN_OFFSET),
            "FIRST_OFFSET is the literal 0: not one minOffset request may be sent: {codes:?}"
        );
        first.shutdown();
        last.shutdown();
    }

    // --------------------------------- Request codes / lite bit (#107)

    #[tokio::test]
    async fn lite_pull_uses_code_361_and_sets_the_lite_bit() {
        let cluster = FakePullCluster::start().await;
        let q = queue("T", "broker-a", 0);
        let c = started_lite("lite_code", "LitePG_LiteCode", &cluster, &q).await;
        wait_until(
            || !cluster.master.pull_request_codes().is_empty(),
            "lite's first pull",
        )
        .await;

        let codes = cluster.master.pull_request_codes();
        assert!(
            codes.iter().all(|x| *x == request_code::LITE_PULL_MESSAGE),
            "every lite pull request code must be LITE_PULL_MESSAGE(361): {codes:?}"
        );

        let flags = cluster.master.pull_sys_flags();
        assert!(!flags.is_empty(), "at least one pull carrying a sysFlag");
        for f in &flags {
            assert!(
                PullSysFlag::has_lite_pull_flag(*f),
                "the lite bit must be set: {f:#x}"
            );
            assert!(!PullSysFlag::has_commit_offset_flag(*f), "{f:#x}");
            assert!(!PullSysFlag::has_suspend_flag(*f), "{f:#x}");
            assert!(PullSysFlag::has_subscription_flag(*f), "{f:#x}");
            assert!(!PullSysFlag::has_class_filter_flag(*f), "{f:#x}");
        }

        // Negative control: the classic pull (the 4-arg form used by DefaultMQPullConsumer.pull/pull_block_if_not_found) must be
        // 11 and must **not carry** the lite bit -- that extra bit would make ordinary consumers hit the lite switch too.
        let classic = started_pull("lite_code_classic", "PG_ClassicCode", &cluster, "T").await;
        classic
            .pull(&q, "*", 0, 32, Some(5000))
            .await
            .expect("the classic pull should succeed against the fake cluster");
        let ccodes = cluster.master.pull_request_codes();
        assert!(
            ccodes.contains(&request_code::PULL_MESSAGE),
            "the classic pull must be PULL_MESSAGE(11): {ccodes:?}"
        );
        let cflags = cluster.master.pull_sys_flags();
        assert!(
            cflags.iter().any(|f| !PullSysFlag::has_lite_pull_flag(*f)),
            "the classic pull must not carry the lite bit: {cflags:?}"
        );
        c.shutdown();
        classic.shutdown();
    }

    // ---------------- Selector-driven pulls keep the selector type ----------------

    #[tokio::test]
    async fn pull_with_selector_keeps_the_selector_type_on_the_wire() {
        let cluster = FakePullCluster::start().await;
        let c = started_pull("pull_sel_types", "PG_PullSel", &cluster, "T").await;
        let q = queue("T", "broker-a", 0);

        // Control: the plain string pull stays TAG with version 0 -- exactly the historic shape.
        c.pull(&q, "TagA", 0, 32, Some(5000))
            .await
            .expect("the string pull must succeed");
        assert_eq!(
            cluster.master.pull_ext_values("expressionType").last(),
            Some(&"TAG".to_string())
        );
        assert_eq!(
            cluster.master.pull_ext_values("subVersion").last(),
            Some(&"0".to_string())
        );

        // A SQL92 selector must not be downgraded to a tag list: its own expressionType and
        // the subscription's real version ride the request.
        c.pull_with_selector(&q, &MessageSelector::by_sql("a > 1"), 0, 32, Some(5000))
            .await
            .expect("the SQL92 selector pull must succeed");
        assert_eq!(
            cluster.master.pull_ext_values("expressionType").last(),
            Some(&"SQL92".to_string())
        );
        assert_eq!(
            cluster.master.pull_ext_values("subscription").last(),
            Some(&"a > 1".to_string())
        );
        let version = cluster
            .master
            .pull_ext_values("subVersion")
            .last()
            .expect("the SQL92 pull recorded a subVersion")
            .parse::<i64>()
            .expect("subVersion is numeric");
        assert!(version > 0, "a SQL92 pull sends the subscription version");

        // A TAG selector keeps the tag behaviour (compiled expression, version 0).
        c.pull_with_selector(&q, &MessageSelector::by_tag("TagA || TagB"), 0, 32, Some(5000))
            .await
            .expect("the TAG selector pull must succeed");
        assert_eq!(
            cluster.master.pull_ext_values("expressionType").last(),
            Some(&"TAG".to_string())
        );
        assert_eq!(
            cluster.master.pull_ext_values("subVersion").last(),
            Some(&"0".to_string())
        );
        assert_eq!(
            cluster.master.pull_ext_values("subscription").last(),
            Some(&"TagA || TagB".to_string())
        );
        c.shutdown();
    }

    #[tokio::test]
    async fn pull_block_if_not_found_with_selector_sends_sql92_with_the_suspend_bit() {
        let cluster = FakePullCluster::start().await;
        let c = started_pull("pull_sel_block", "PG_PullSelBlock", &cluster, "T").await;
        let q = queue("T", "broker-a", 0);

        c.pull_block_if_not_found(&q, "TagA", 0, 32)
            .await
            .expect("the long-poll string form must still succeed");
        c.pull_block_if_not_found_with_selector(&q, &MessageSelector::by_sql("color = 'blue'"), 0, 32)
            .await
            .expect("the long-poll selector form must succeed");

        let types = cluster.master.pull_ext_values("expressionType");
        assert_eq!(types.len(), 2);
        assert_eq!(types[0], "TAG", "control: the string form stays TAG");
        assert_eq!(types[1], "SQL92");
        let flags = cluster.master.pull_sys_flags();
        assert_eq!(flags.len(), 2);
        for f in &flags {
            assert!(PullSysFlag::has_suspend_flag(*f), "both are long polls: {f:#x}");
        }
        c.shutdown();
    }

    /// A lite subscribe-mode consumer registered with a SQL92 selector: the type reaches the
    /// broker in the heartbeat **and** in the background pull; a tag subscription must keep
    /// sending `TAG` with version 0 (the control goes red together with the SQL92 wiring).
    #[tokio::test]
    async fn lite_sql92_selector_reaches_heartbeat_and_background_pull() {
        // Two read queues: once the TAG control joins the group, each instance still owns one
        // queue, so both pull shapes are guaranteed to reach the broker.
        let cluster = FakePullCluster::start_queues(2).await;
        let c = DefaultLitePullConsumer::new("LitePG_Sql92").expect("the group name is legal");
        c.set_instance_name("lite_sql92_pull");
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c.subscribe_with_selector("T", &MessageSelector::by_sql("a > 1"));
        c.start().await.expect("start must succeed");
        cluster.set_cid_list(&[c.client_id().as_str()]);

        // The heartbeat subscription set carries the selector's type.
        wait_until(
            || {
                cluster.master.heartbeats().iter().any(|hb| {
                    hb.consumer_data_set.iter().any(|cd| {
                        cd.subscription_data_set.iter().any(|sd| {
                            sd.topic == "T"
                                && sd.expression_type == "SQL92"
                                && sd.sub_string == "a > 1"
                                && sd.tags_set.is_empty()
                        })
                    })
                })
            },
            "the heartbeat carries the SQL92 subscription data",
        )
        .await;

        // The background pull request must not downgrade it to a tag subscription.
        wait_until(
            || {
                cluster.master.pull_ext_values("expressionType").iter().any(|t| t == "SQL92")
            },
            "the lite pull request carries expressionType=SQL92",
        )
        .await;
        let subs = cluster.master.pull_ext_values("subscription");
        assert!(
            subs.iter().any(|s| s == "a > 1"),
            "the raw SQL expression rides the request: {subs:?}"
        );
        let versions = cluster.master.pull_ext_values("subVersion");
        assert!(
            versions.iter().any(|v| v.parse::<i64>().unwrap_or(0) != 0),
            "a non-tag subscription sends its real version, not 0: {versions:?}"
        );

        // TAG control on the same cluster: a tag subscription must keep sending TAG/version 0.
        let plain =
            DefaultLitePullConsumer::new("LitePG_TagCtl").expect("the group name is legal");
        plain.set_instance_name("lite_tag_control");
        plain.set_namesrv_addr(&cluster.namesrv_addr);
        plain.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        plain.subscribe("T", "TagA");
        plain.start().await.expect("start must succeed");
        cluster.set_cid_list(&[c.client_id().as_str(), plain.client_id().as_str()]);
        wait_until(
            || cluster.master.pull_ext_values("expressionType").iter().any(|t| t == "TAG"),
            "the tag subscription keeps sending TAG",
        )
        .await;
        assert!(
            cluster
                .master
                .pull_ext_values("subVersion")
                .iter()
                .any(|v| v == "0"),
            "the tag pull keeps version 0"
        );
        c.shutdown();
        plain.shutdown();
    }

    // ---------------------- The pull cursor follows nextBeginOffset (#105)

    /// Start a lite-pull consumer against the fake cluster (assign + FIRST_OFFSET: the start point is the literal 0, so no
    /// start-offset query RPC is made and the first PULL_MESSAGE deterministically lands at `queueOffset=0`).
    async fn started_lite(
        instance: &str,
        group: &str,
        cluster: &FakePullCluster,
        mq: &MessageQueue,
    ) -> DefaultLitePullConsumer {
        let c = DefaultLitePullConsumer::new(group).expect("the group name is legal");
        c.set_instance_name(instance);
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c.assign(std::slice::from_ref(mq));
        c.start().await.expect("start should succeed against the fake cluster");
        c
    }

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
            "after NO_MATCHED_MSG the pull cursor follows to nextBeginOffset=5",
        )
        .await;
        // Not just a number rewritten in the memory table: the next PULL_MESSAGE really starts from 5
        wait_until(
            || cluster.master.pull_offsets().contains(&5),
            "the next PULL_MESSAGE starts from 5",
        )
        .await;
        assert_eq!(
            c.buffered_message_count(),
            0,
            "NO_MATCHED_MSG has nothing deliverable"
        );
        c.shutdown();
    }

    /// The nextBeginOffset of OFFSET_ILLEGAL is the broker's correction for an out-of-range offset: only following it counts
    /// as "self-healing from an illegal offset"; staying at the old offset would receive the same correction every round and spin in place.
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
            "after OFFSET_ILLEGAL the pull cursor adopts the broker's correction",
        )
        .await;
        wait_until(
            || cluster.master.pull_offsets().contains(&42),
            "the next PULL_MESSAGE starts from 42",
        )
        .await;
        c.shutdown();
    }

    #[tokio::test]
    async fn lite_in_flight_seek_wins_over_the_pull_result() {
        let cluster = FakePullCluster::start().await;
        let q = queue("T", "broker-a", 0);
        let body =
            encode_message_ext(&msg("T", "broker-a", 0, 2, "late"), false).expect("the message must encode");
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
            "the first pull reaching the broker",
        )
        .await;
        c.seek(&q, 99);
        gate_tx.send(()).expect("open the gate");
        wait_until(
            || cluster.master.pull_offsets().contains(&99),
            "after seek the next PULL_MESSAGE starts from 99",
        )
        .await;
        assert_eq!(c.pull_cursor_of(&q), 99, "an in-flight reply must not overwrite the offset seek wrote down");
        assert_eq!(c.buffered_message_count(), 0, "the braked round must not enter the buffer");
        c.shutdown();
    }

    // ------------------------------------------------------------ Balanced view

    /// The group holds only this instance: the entire subscribe info is its own, sorted per the house convention.
    #[tokio::test]
    async fn balance_view_sole_instance_takes_every_queue() {
        // 3 queues: with 1 queue, "own share" and "all" are the same shape, which proves nothing.
        let cluster = FakePullCluster::start_queues(3).await;
        let c = started_pull("bal_sole", "PG_BalSole", &cluster, "T").await;
        let cid = c.client_id();
        cluster.set_cid_list(&[cid.as_str()]);

        let view = c
            .fetch_message_queues_in_balance("T")
            .await
            .expect("the balanced view must be computable");
        assert_eq!(
            view.iter().map(|q| q.queue_id).collect::<Vec<i32>>(),
            vec![0, 1, 2],
            "a sole instance claims every queue, in ascending queueId"
        );
        assert!(view.iter().all(|q| q.topic == "T" && q.broker_name == "broker-a"));
        c.shutdown();
    }

    /// Two instances in the same group: each gets only its own share; the shares do not overlap and together cover everything.
    /// Overlap would mean duplicate consumption, and this criterion can only be verified by cross-checking two live instances against each other.
    #[tokio::test]
    async fn balance_view_returns_only_this_instances_share() {
        let cluster = FakePullCluster::start_queues(2).await;
        let a = started_pull("bal_pair_a", "PG_BalPair", &cluster, "T").await;
        let b = started_pull("bal_pair_b", "PG_BalPair", &cluster, "T").await;

        let mut ids = [a.client_id(), b.client_id()];
        ids.sort();
        cluster.set_cid_list(
            &ids.iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
        );

        let va = a
            .fetch_message_queues_in_balance("T")
            .await
            .expect("the balanced view of a");
        let vb = b
            .fetch_message_queues_in_balance("T")
            .await
            .expect("the balanced view of b");
        assert_eq!(va.len(), 1, "two instances across two queues: each takes exactly 1");
        assert_eq!(vb.len(), 1);
        assert_ne!(va[0], vb[0], "the two shares must not overlap (overlap means duplicate consumption)");
        let mut both = vec![va[0].clone(), vb[0].clone()];
        sort_mqs(&mut both);
        assert_eq!(
            both.iter().map(|q| q.queue_id).collect::<Vec<i32>>(),
            vec![0, 1],
            "the two shares together must cover every queue, missing none"
        );
        a.shutdown();
        b.shutdown();
    }

    /// The broker does not know this group (38 answers an empty list): "cannot compute" ≠ "computes to empty"; in that case keep the current assignment
    /// (the queues this instance has pulled locally), and never fall back to "take all queues exclusively".
    #[tokio::test]
    async fn balance_view_keeps_current_assignment_when_the_group_is_unknown() {
        let cluster = FakePullCluster::start_queues(3).await;
        let c = started_pull("bal_unknown", "PG_BalUnknown", &cluster, "T").await;
        cluster.set_cid_list(&[]);

        assert!(
            c.fetch_message_queues_in_balance("T")
                .await
                .expect("the fallback path must not error")
                .is_empty(),
            "nothing pulled yet: the current assignment is simply empty"
        );

        let mut all = c
            .fetch_subscribe_message_queues("T")
            .await
            .expect("the subscribe info must be queryable");
        sort_mqs(&mut all);
        assert_eq!(all.len(), 3);
        c.pull(&all[2], "*", 0, 32, Some(5000))
            .await
            .expect("the pull should succeed against the fake cluster");

        assert_eq!(
            c.fetch_message_queues_in_balance("T").await.expect("the fallback view"),
            vec![all[2].clone()],
            "the fallback claims only the one queue it has pulled, not all 3 in the route"
        );
        assert!(
            c.fetch_message_queues_in_balance("OtherBalTopic")
                .await
                .expect("other topics must work through the same path")
                .is_empty(),
            "the fallback is scoped per topic too: T's queues must not leak into another topic"
        );
        c.shutdown();
    }

    #[tokio::test]
    async fn balance_view_broadcasting_takes_all_without_the_consumer_list() {
        let cluster = FakePullCluster::start_queues(3).await;
        let c = DefaultMQPullConsumer::new("PG_BalBcast").expect("the group name is legal");
        c.set_instance_name("bal_bcast");
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.set_message_model(MessageModel::BROADCASTING);
        c.register_topic("T");
        c.start().await.expect("start should succeed against the fake cluster");
        cluster.set_cid_list(&["someone-else"]);

        let view = c
            .fetch_message_queues_in_balance("T")
            .await
            .expect("the broadcasting view");
        assert_eq!(view.len(), 3, "broadcasting ignores the consumer list: it claims everything");
        c.shutdown();
    }

    #[tokio::test]
    async fn balance_view_requires_a_started_consumer() {
        let c = DefaultMQPullConsumer::new("PG_BalNotStarted").expect("the group name is legal");
        let err = c
            .fetch_message_queues_in_balance("T")
            .await
            .expect_err("an unstarted consumer must error");
        assert!(
            err.to_string().contains("not started"),
            "the error must be the not-started one: {err}"
        );
    }

    // ---------------------- Lite's topic queue-set change listener

    /// Accumulator shape for listener events: a sequence of `(topic, sorted queueId list)` pairs.
    type QueueChangeEvents = Arc<Mutex<Vec<(String, Vec<i32>)>>>;

    /// A listener that collects `on_changed` calls (queue ids are sorted before storing so comparisons ignore order).
    struct RecordingQueueListener {
        events: QueueChangeEvents,
    }

    impl RecordingQueueListener {
        fn new() -> (Arc<Self>, QueueChangeEvents) {
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

    /// Start a lite consumer in subscribe mode (unlike the assign-based `started_lite`:
    /// queue-set change listeners only make sense in subscribe mode).
    async fn started_lite_subscribe(
        instance: &str,
        group: &str,
        cluster: &FakePullCluster,
        topic: &str,
    ) -> DefaultLitePullConsumer {
        let c = DefaultLitePullConsumer::new(group).expect("the group name is legal");
        c.set_instance_name(instance);
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.subscribe(topic, "*");
        c.start().await.expect("start should succeed against the fake cluster");
        c
    }

    #[test]
    fn same_queue_set_matches_by_set_equality() {
        let q = |id: i32| queue("T", "broker-a", id);
        assert!(!same_queue_set(&[], &[q(0)]), "a count mismatch is already a change");
        assert!(same_queue_set(&[q(0), q(1)], &[q(1), q(0)]), "set equality ignores order");
        assert!(!same_queue_set(&[q(0), q(1)], &[q(0), q(2)]), "same count with different members is a change");
        assert!(same_queue_set(&[q(0)], &[q(0)]));
    }

    #[tokio::test]
    async fn queue_change_listener_guards_and_interval() {
        let c = DefaultLitePullConsumer::new("LiteQC_Guard").expect("the group name is legal");
        let (listener, _) = RecordingQueueListener::new();
        let err = c
            .register_topic_message_queue_change_listener("  ", listener)
            .await
            .expect_err("a blank topic must error");
        assert!(
            err.to_string().contains("Topic or listener is null"),
            "the error must be that fixed message: {err}"
        );
        assert_eq!(c.topic_metadata_check_interval_millis(), 30_000);
        c.set_topic_metadata_check_interval_millis(0);
        assert_eq!(c.topic_metadata_check_interval_millis(), 1_000, "clamped to the 1s floor");
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
            .expect("registration must succeed");
        // Registering while running snapshots at once ⇒ the first round must not report "the status quo" as a change
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            0,
            "registration snapshots immediately: no change, no callback"
        );
        assert!(lock(&events).is_empty());

        cluster.scale_to(4); // scale out: the compare round asks the name server itself
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            1,
            "a scale-out must fire exactly one callback"
        );
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            0,
            "the same set must not call back twice"
        );

        cluster.scale_to(1); // scale in
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            1,
            "a scale-in must call back too"
        );
        assert_eq!(
            *lock(&events),
            vec![("T".to_string(), vec![0, 1, 2, 3]), ("T".to_string(), vec![0])],
            "the callback receives the complete post-change queue set"
        );
        c.shutdown();
    }

    #[tokio::test]
    async fn queue_change_listener_before_start_defers_snapshot() {
        let cluster = FakePullCluster::start_queues(2).await;
        let c = DefaultLitePullConsumer::new("LiteQC_Pre").expect("the group name is legal");
        c.set_instance_name("lite_qc_pre");
        c.set_namesrv_addr(&cluster.namesrv_addr);
        c.subscribe("T", "*");
        let (listener, events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("registration must work before start too");
        assert!(
            lock(&c.inner.queues_for_topic).is_empty(),
            "no snapshot before start (only recorded while RUNNING)"
        );

        c.start().await.expect("start must succeed");
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            1,
            "with no snapshot the first round must report once"
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
            .expect("the first registration");
        c.register_topic_message_queue_change_listener("T", second)
            .await
            .expect("re-registration: it overwrites the old listener and warns");
        assert_eq!(lock(&c.inner.topic_listeners).len(), 1, "only one listener survives per topic");

        cluster.scale_to(3);
        assert_eq!(c.fetch_topic_message_queues_and_compare().await, 1);
        assert!(lock(&first_events).is_empty(), "the overwritten listener must receive no more events");
        assert_eq!(lock(&second_events).len(), 1, "the current listener gets the callback");
        c.shutdown();
    }

    #[tokio::test]
    async fn queue_change_metadata_loop_actually_compares() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_lite_subscribe("lite_qc_loop", "LiteQC_Loop", &cluster, "T").await;
        let (listener, events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("registration");

        cluster.scale_to(2);
        // The first-check delay is injected as 50ms (default 10s); the period is 100ms
        let inner = c.inner.clone();
        let stop = c.inner.stop.subscribe();
        let task = tokio::spawn(async move {
            metadata_loop_after(&inner, stop, 50).await;
        });
        wait_until(
            || !lock(&events).is_empty(),
            "the background loop should run a compare round on its own",
        )
        .await;
        assert_eq!(lock(&events)[0].1, vec![0, 1]);
        c.inner.running.store(false, Ordering::Release);
        let _ = c.inner.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        c.shutdown();
    }

    /// The first-check delay should only apply **before the first round**: left inside the loop, every round pays "first delay + period",
    /// dragging the 1s period out to 4s, and the post-scale-out callback is an order of magnitude slower.
    #[tokio::test]
    async fn queue_change_metadata_period_excludes_the_first_delay() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_lite_subscribe("lite_qc_period", "LiteQC_Period", &cluster, "T").await;
        let (listener, events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("registration");
        // Even a 0 period is clamped to the 1s floor, so the injected first delay here is much larger than the period
        c.set_topic_metadata_check_interval_millis(0);
        let inner = c.inner.clone();
        let stop = c.inner.stop.subscribe();
        let task = tokio::spawn(async move {
            metadata_loop_after(&inner, stop, 3000).await;
        });

        cluster.scale_to(2);
        wait_until(|| !lock(&events).is_empty(), "the first round must catch the scale-out").await;

        cluster.scale_to(3);
        let second = tokio::time::timeout(
            Duration::from_millis(1500),
            wait_until(|| lock(&events).len() >= 2, "the second round must catch the next scale-out"),
        )
        .await;
        assert!(
            second.is_ok(),
            "the first delay leaked into the check period: every round would wait 3s more"
        );
        assert_eq!(lock(&events)[1].1, vec![0, 1, 2]);

        c.inner.running.store(false, Ordering::Release);
        let _ = c.inner.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        c.shutdown();
    }

    /// The compare round must **ask the name server on the spot**, not read the periodically refreshed route cache:
    /// without this step, a scale-out would be seen at the earliest after one route poll, and the listener callback would lag a whole period.
    #[tokio::test]
    async fn queue_change_round_asks_the_nameserver_every_time() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_lite_subscribe("lite_qc_fresh", "LiteQC_Fresh", &cluster, "T").await;
        let (listener, _) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("registration");

        cluster.clear_route_queries();
        c.fetch_topic_message_queues_and_compare().await;
        let first = cluster.route_queries();
        assert!(!first.is_empty(), "one compare round must query the route at least once: {first:?}");
        assert!(
            first.iter().all(|t| t == "T"),
            "the topic asked about must be exactly this one: {first:?}"
        );

        cluster.clear_route_queries();
        c.fetch_topic_message_queues_and_compare().await;
        let second = cluster.route_queries();
        assert!(
            !second.is_empty(),
            "the next round must ask again (serving from the cache could not detect the change)"
        );
        c.shutdown();
    }

    /// When no queue can be found the call must **error**, not return an empty list: the compare round would read empty as "this topic shrank
    /// to 0 queues" and fire a fake scale-in callback, and the snapshot would be flushed to the empty set.
    #[tokio::test]
    async fn fetch_message_queues_reports_instead_of_returning_empty() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_lite_subscribe("lite_qc_empty", "LiteQC_Empty", &cluster, "T").await;
        cluster.scale_to(0); // not a single read queue left in the route
        let err = c
            .fetch_message_queues("T")
            .await
            .expect_err("it must error when no queue is available");
        assert!(
            err.to_string()
                .contains("Can not find Message Queue for this topic"),
            "the error must be the 'queue not found' one: {err}"
        );
        // The listener side must not receive a fake scale-in callback just because this lookup failed.
        let (listener, events) = RecordingQueueListener::new();
        c.register_topic_message_queue_change_listener("T", listener)
            .await
            .expect("registration");
        cluster.clear_route_queries();
        assert_eq!(
            c.fetch_topic_message_queues_and_compare().await,
            0,
            "the round that cannot find queues must not fire a callback"
        );
        assert!(lock(&events).is_empty(), "no callback events");
        c.shutdown();
    }

    // ---------------------- pull_async (the async flavour of pull(.., PullCallback, timeout))

    /// Counts the callback fires and keeps the delivered result/error for assertions.
    #[derive(Default)]
    struct AsyncPullProbe {
        successes: AtomicUsize,
        failures: AtomicUsize,
        result: Mutex<Option<PullResult>>,
        error: Mutex<Option<String>>,
    }

    impl AsyncPullProbe {
        fn callback(self: &Arc<Self>) -> Arc<dyn PullCallback> {
            let ok = Arc::clone(self);
            let er = Arc::clone(self);
            Arc::new(ClosurePullCallback::new(
                Some(Box::new(move |r| {
                    ok.successes.fetch_add(1, Ordering::SeqCst);
                    *ok.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
                })),
                Some(Box::new(move |e| {
                    er.failures.fetch_add(1, Ordering::SeqCst);
                    *er.error.lock().unwrap_or_else(|e| e.into_inner()) = Some(e.to_string());
                })),
            ))
        }

        fn fires(&self) -> usize {
            self.successes.load(Ordering::SeqCst) + self.failures.load(Ordering::SeqCst)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pull_async_delivers_the_found_result_exactly_once() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_pull("pull_async_ok", "PG_AsyncOk", &cluster, "T").await;
        let q = queue("T", "broker-a", 0);
        let body =
            encode_message_ext(&msg("T", "broker-a", 0, 7, "hello"), false).expect("encodes");
        cluster.master.script_pull(
            response_code::SUCCESS,
            &[
                ("nextBeginOffset", "8"),
                ("minOffset", "0"),
                ("maxOffset", "9"),
            ],
            Some(body),
            None,
        );
        let probe = Arc::new(AsyncPullProbe::default());
        c.pull_async(&q, "*", 7, 32, Some(3_000), probe.callback())
            .expect("a started consumer must accept pull_async");
        wait_until(|| probe.fires() >= 1, "on_success lands").await;
        // Let a hypothetical double-fire show up before asserting "exactly once".
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(probe.successes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.failures.load(Ordering::SeqCst), 0, "no exception on success");
        let r = lock(&probe.result).clone().expect("a result was delivered");
        assert_eq!(r.status, PullStatus::Found);
        assert_eq!(r.next_begin_offset, 8);
        assert_eq!(r.msg_found_list.len(), 1);
        assert_eq!(
            r.msg_found_list[0].body.as_deref(),
            Some("hello".as_bytes()),
            "the FOUND body rides through the callback"
        );
        c.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pull_async_reports_broker_errors_through_on_exception() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_pull("pull_async_err", "PG_AsyncErr", &cluster, "T").await;
        let q = queue("T", "broker-a", 0);
        cluster.master.script_pull(response_code::SYSTEM_ERROR, &[], None, None);
        let probe = Arc::new(AsyncPullProbe::default());
        c.pull_async(&q, "*", 0, 32, Some(3_000), probe.callback())
            .expect("accepted");
        wait_until(|| probe.fires() >= 1, "on_exception lands").await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(probe.failures.load(Ordering::SeqCst), 1);
        assert_eq!(probe.successes.load(Ordering::SeqCst), 0);
        assert!(
            lock(&probe.error).clone().is_some_and(|e| !e.is_empty()),
            "the failure reason rides along"
        );
        c.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pull_async_timeout_lands_in_on_exception() {
        let cluster = FakePullCluster::start_queues(1).await;
        let c = started_pull("pull_async_to", "PG_AsyncTimeout", &cluster, "T").await;
        let q = queue("T", "broker-a", 0);
        // The reply waits on a gate that is never opened: the request timeout must fire instead.
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
        cluster.master.script_pull(
            response_code::SUCCESS,
            &[("nextBeginOffset", "1")],
            None,
            Some(gate_rx),
        );
        let probe = Arc::new(AsyncPullProbe::default());
        c.pull_async(&q, "*", 0, 32, Some(300), probe.callback())
            .expect("accepted");
        wait_until(|| probe.fires() >= 1, "the timeout surfaces as on_exception").await;
        assert_eq!(probe.failures.load(Ordering::SeqCst), 1);
        assert_eq!(probe.successes.load(Ordering::SeqCst), 0);
        drop(gate_tx); // release the scripted reply; nothing may fire a second time
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(probe.fires(), 1, "the late reply must not fire the callback again");
        c.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pull_async_rejected_before_start_and_after_shutdown() {
        let cluster = FakePullCluster::start_queues(1).await;
        let q = queue("T", "broker-a", 0);
        // Before start: synchronous Err from the state check, callback never fires.
        let cold = DefaultMQPullConsumer::new("PG_AsyncCold").unwrap();
        cold.set_instance_name("pull_async_cold");
        cold.set_namesrv_addr(&cluster.namesrv_addr);
        let probe = Arc::new(AsyncPullProbe::default());
        let err = cold
            .pull_async(&q, "*", 0, 32, None, probe.callback())
            .expect_err("a cold consumer must reject pull_async synchronously");
        assert!(
            err.to_string().contains("consumer not started"),
            "the rejection reason: {err}"
        );
        // After shutdown: same synchronous rejection.
        let c = started_pull("pull_async_closed", "PG_AsyncClosed", &cluster, "T").await;
        c.shutdown();
        let err = c
            .pull_async(&q, "*", 0, 32, None, probe.callback())
            .expect_err("a shut-down consumer must reject pull_async");
        assert!(
            err.to_string().contains("consumer not started"),
            "the rejection reason: {err}"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(probe.fires(), 0, "a rejected pull_async never touches the callback");
        cold.shutdown();
    }
}
