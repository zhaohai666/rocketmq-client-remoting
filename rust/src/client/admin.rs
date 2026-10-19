//! Coverage: topic create/delete/query and config, broker cluster info / runtime info / config,
//! name server KV config, subscription group management, consumer and producer connections,
//! consume stats, offset management (including real broker-side reset), message query (key / uniqKey / msgId / ConsumeQueue),
//! and trial consumption of a single message (the initiator side of CONSUME_MESSAGE_DIRECTLY(309)).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Instant;

use serde_json::{json, Value};

use crate::client::mq_client::{MQClientInstance, MQClientInstanceConfig};
use crate::common::boundary_type::BoundaryType;
use crate::common::message::{MessageExt, MessageQueue};
use crate::common::message_const::{
    INDEX_KEY_TYPE, INDEX_UNIQUE_TYPE, PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
};
use crate::common::message_decoder::{decode_message, decode_message_id};
use crate::common::mix_all::MixAll;
use crate::common::sysflag::PermName;
use crate::common::topic_config::TopicConfig;
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};
use crate::remoting::protocol::admin_body::{
    ConsumeStats, MessageQueueKey, QueryConsumeQueueResponseBody, TopicConfigSerializeWrapper,
    TopicStatsTable,
};
use crate::remoting::protocol::body::{
    ClusterInfo, ConsumeMessageDirectlyResult, ConsumeStatsList, ConsumerConnection,
    ConsumerRunningInfo, GetConsumerListByGroupResponseBody, KVTable, ProducerConnection,
    ResetOffsetBody, TopicList,
};
use crate::remoting::protocol::codes::{language_code, request_code, response_code};
use crate::remoting::protocol::ext_fields::{ExtFields, StringMap};
use crate::remoting::protocol::remoting_command::RemotingCommand;
use crate::remoting::protocol::route::TopicRouteData;
use crate::remoting::protocol::serialize::RemotingSerializable;
use crate::remoting::protocol::subscription::{SubscriptionGroupConfig, SubscriptionGroupWrapper};
use crate::remoting::rpchook::RPCHook;
use crate::{bail, rmq_warn};

pub const DEFAULT_TIMEOUT_MILLIS: i64 = 5000 * 3;
/// The admin's default instance name is fixed to `"ADMIN"`.
pub const DEFAULT_INSTANCE_NAME: &str = "ADMIN";
const ADMIN_OP_TIMEOUT_MILLIS: i64 = 5000;
/// The hardcoded 32 of `query_message_by_key(max_num=32)` and `query_message_by_uniq_key`.
pub const DEFAULT_QUERY_MESSAGE_MAX_NUM: i32 = 32;

/// The name server KV namespace holding ordered topic config.
pub const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";
/// The paging limit `maxGroupNum` of `get_all_subscription_group`.
const MAX_GROUP_NUM: i32 = 10_000;
/// The query window upper bound of `query_message_by_uniq_key` (`now + 60 * 60 * 1000`).
const UNIQ_QUERY_WINDOW_MILLIS: i64 = 60 * 60 * 1000;

/// Admin configuration.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub namespace: String,
    pub instance_name: String,
    pub client_id: Option<String>,
    pub unit_name: Option<String>,
    pub namespace_v2: Option<String>,
    pub enable_stream_request_type: bool,
    pub vip_channel_enabled: bool,
    pub poll_name_server_interval_millis: u64,
    pub name_server_addrs: Vec<String>,
    pub timeout_millis: i64,
    pub kv_namespace_to_delete_list: Vec<String>,
}

impl Default for AdminConfig {
    fn default() -> AdminConfig {
        AdminConfig {
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            unit_name: None,
            namespace_v2: None,
            enable_stream_request_type: false,
            vip_channel_enabled: false,
            poll_name_server_interval_millis: 30_000,
            client_id: None,
            name_server_addrs: Vec::new(),
            timeout_millis: DEFAULT_TIMEOUT_MILLIS,
            kv_namespace_to_delete_list: Vec::new(),
        }
    }
}

#[derive(Default)]
struct AdminInner {
    cfg: RwLock<AdminConfig>,
    client: Mutex<Option<MQClientInstance>>,
    started: AtomicBool,
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn read_cfg(cfg: &RwLock<AdminConfig>) -> AdminConfig {
    cfg.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Semicolon-separated; each item is trimmed and empties dropped.
fn split_addrs(addr: &str) -> Vec<String> {
    addr.split(';')
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(str::to_string)
        .collect()
}

/// Builds the request via `RemotingCommand::create_request_command(code, None)`, then fills extFields/body.
fn build_request(code: i32, ext: &StringMap, body: Option<Vec<u8>>) -> RemotingCommand {
    let mut request = RemotingCommand::create_request_command(code, None);
    for (key, value) in ext.iter() {
        request.add_ext_field(key, value);
    }
    if body.is_some() {
        request.set_body(body);
    }
    request
}

fn bool_flag(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

fn ext_pairs(pairs: &[(&str, String)]) -> StringMap {
    let mut ext = StringMap::new();
    for (key, value) in pairs {
        ext.insert(*key, value.clone());
    }
    ext
}

/// The extFields of 222 `INVOKE_BROKER_TO_RESET_OFFSET`.
fn reset_offset_ext_pairs(
    topic: &str,
    group: &str,
    timestamp: i64,
    is_force: bool,
    queue_id: Option<i32>,
    offset: Option<i64>,
) -> StringMap {
    let mut pairs = vec![
        ("topic", topic.to_string()),
        ("group", group.to_string()),
        ("timestamp", timestamp.to_string()),
        ("isForce", bool_flag(is_force).to_string()),
        ("offset", offset.unwrap_or(-1).to_string()),
    ];
    if let Some(queue_id) = queue_id {
        pairs.push(("queueId", queue_id.to_string()));
    }
    ext_pairs(&pairs)
}

/// Missing fields and non-numbers both count as 0.
fn ext_int(response: &RemotingCommand, key: &str) -> i64 {
    response
        .get_ext_field(key)
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

// ---------------------------------------------------------------- pure-logic helpers
// Extracted only so that the "Vec table" spots, the easiest to get wrong, can be locked down offline; there is exactly one caller.

/// With no cluster name, returns all brokers; with one, takes the brokerNames of
/// `clusterAddrTable[cluster]` (sorted, deduplicated) and collects their addresses (each
/// broker's addresses sorted as strings), falling back to the full list if none were collected.
fn cluster_broker_addrs(cluster: &ClusterInfo, cluster_name: Option<&str>) -> Vec<String> {
    let Some(name) = cluster_name.filter(|n| !n.is_empty()) else {
        return cluster.get_broker_addrs();
    };
    let Some(broker_names) = cluster
        .cluster_addr_table
        .iter()
        .find(|(cluster, _)| cluster == name)
        .map(|(_, names)| names.clone())
    else {
        return cluster.get_broker_addrs();
    };
    let mut broker_names: Vec<String> =
        broker_names.into_iter().filter(|n| !n.is_empty()).collect();
    broker_names.sort();
    broker_names.dedup();
    let mut addrs: Vec<String> = Vec::new();
    for broker_name in broker_names {
        let Some((_, id_addr)) = cluster
            .broker_addr_table
            .iter()
            .find(|(name, _)| *name == broker_name)
        else {
            continue;
        };
        let mut one: Vec<String> = id_addr
            .iter()
            .map(|(_, addr)| addr.clone())
            .filter(|addr| !addr.is_empty())
            .collect();
        one.sort();
        addrs.extend(one);
    }
    if addrs.is_empty() {
        return cluster.get_broker_addrs();
    }
    addrs
}

/// Filter for user topic configs: system topics (broker-reported + locally determined)
/// are always removed; with `special_topic=false` `%RETRY%` / `%DLQ%` are removed too.
fn filter_user_topic_configs(
    mut wrapper: TopicConfigSerializeWrapper,
    sys_topics: &[String],
    special_topic: bool,
) -> TopicConfigSerializeWrapper {
    wrapper.topic_config_table.retain(|(name, _)| {
        let name = name.as_str();
        if sys_topics.iter().any(|t| t == name) || MixAll::is_sys_topic(Some(name)) {
            return false;
        }
        if !special_topic
            && (name.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX)
                || name.starts_with(MixAll::DLQ_GROUP_TOPIC_PREFIX))
        {
            return false;
        }
        true
    });
    wrapper
}

/// Merges the paged subscription group tables: same name overwrites, first-appearance order kept.
fn merge_group_table(
    table: &mut Vec<(String, SubscriptionGroupConfig)>,
    part: Vec<(String, SubscriptionGroupConfig)>,
) {
    for (name, config) in part {
        match table.iter_mut().find(|(n, _)| *n == name) {
            Some(entry) => entry.1 = config,
            None => table.push((name, config)),
        }
    }
}

/// When both sides are JSON objects, overrides key by key.
fn merge_json_map(dst: &mut Value, src: Value) {
    let mut merged = false;
    if let (Some(dst_map), Some(src_map)) = (dst.as_object_mut(), src.as_object()) {
        for (key, value) in src_map {
            dst_map.insert(key.clone(), value.clone());
        }
        merged = true;
    }
    if !merged {
        *dst = src;
    }
}

/// Topic stats merge: the offset table is overridden per key, `topic_put_tps` is summed.
fn merge_topic_stats(merged: &mut TopicStatsTable, part: TopicStatsTable) {
    for (key, offset) in part.offset_table {
        match merged.offset_table.iter_mut().find(|(k, _)| *k == key) {
            Some(entry) => entry.1 = offset,
            None => merged.offset_table.push((key, offset)),
        }
    }
    merged.topic_put_tps += part.topic_put_tps;
}

/// Filters `route.queue_datas` by brokerName, then
/// expands `range(read_queue_nums)`.
fn queues_of_broker(route: &TopicRouteData, topic: &str, broker_name: &str) -> Vec<MessageQueue> {
    let mut queues = Vec::new();
    for queue_data in route
        .queue_datas
        .iter()
        .filter(|qd| qd.broker_name == broker_name)
    {
        for queue_id in 0..queue_data.read_queue_nums {
            queues.push(MessageQueue::new(topic, broker_name, queue_id));
        }
    }
    queues
}

/// The effective request timeout: the explicit value when given, otherwise the configured `timeout_millis`.
fn effective_timeout(configured: i64, explicit: Option<i64>) -> i64 {
    explicit.filter(|t| *t > 0).unwrap_or(configured)
}

// ---------------------------------------------------------------- message track DTO

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackType {
    Consumed,
    ConsumedButFiltered,
    Pull,
    NotConsumeYet,
    NotOnline,
    ConsumeBroadcasting,
    Unknown,
}

impl TrackType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TrackType::Consumed => "CONSUMED",
            TrackType::ConsumedButFiltered => "CONSUMED_BUT_FILTERED",
            TrackType::Pull => "PULL",
            TrackType::NotConsumeYet => "NOT_CONSUME_YET",
            TrackType::NotOnline => "NOT_ONLINE",
            TrackType::ConsumeBroadcasting => "CONSUME_BROADCASTING",
            TrackType::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageTrack {
    pub consumer_group: String,
    pub track_type: TrackType,
    pub exception_desc: Option<String>,
}

// ---------------------------------------------------------------- facade

/// Usage: `set_namesrv_addr(..)` → `start().await` → call the various `examine*/get*/create*/delete*`
/// → `shutdown()`. Cloned copies share the same state and the same connection.
#[derive(Clone)]
pub struct DefaultMQAdminExt {
    inner: Arc<AdminInner>,
}

impl std::fmt::Debug for DefaultMQAdminExt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cfg = read_cfg(&self.inner.cfg);
        f.debug_struct("DefaultMQAdminExt")
            .field("namespace", &cfg.namespace)
            .field("instance_name", &cfg.instance_name)
            .field("client_id", &cfg.client_id)
            .field("name_server_addrs", &cfg.name_server_addrs)
            .field("timeout_millis", &cfg.timeout_millis)
            .field(
                "kv_namespace_to_delete_list",
                &cfg.kv_namespace_to_delete_list,
            )
            .field("started", &self.inner.started.load(Ordering::Acquire))
            .finish()
    }
}

impl Default for DefaultMQAdminExt {
    fn default() -> DefaultMQAdminExt {
        DefaultMQAdminExt::new()
    }
}

impl DefaultMQAdminExt {
/// Constructs the admin with the default config.
    pub fn new() -> DefaultMQAdminExt {
        DefaultMQAdminExt::with_config(AdminConfig::default())
    }

/// Constructs the admin with an RPC hook.
    pub fn with_rpc_hook(rpc_hook: Option<Arc<dyn RPCHook>>) -> DefaultMQAdminExt {
        let admin = DefaultMQAdminExt::new();
        admin.set_rpc_hook(rpc_hook);
        admin
    }

    /// Constructs directly from a complete config.
    pub fn with_config(cfg: AdminConfig) -> DefaultMQAdminExt {
        DefaultMQAdminExt {
            inner: Arc::new(AdminInner {
                cfg: RwLock::new(cfg),
                ..Default::default()
            }),
        }
    }

    // ---------------- Config and lifecycle ----------------

/// Snapshot of the current config.
    pub fn config(&self) -> AdminConfig {
        read_cfg(&self.inner.cfg)
    }

/// Mutates the config in place via a closure.
    pub fn update_config(&self, f: impl FnOnce(&mut AdminConfig)) {
        {
            let mut w = self.inner.cfg.write().unwrap_or_else(|e| e.into_inner());
            f(&mut w);
        }
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

    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.update_config(|c| c.enable_stream_request_type = enable);
    }

    pub fn set_vip_channel_enabled(&self, enable: bool) {
        self.update_config(|c| c.vip_channel_enabled = enable);
        // The private instance answers the delegated offset / consumer-list family and applies
        // the rewrite itself, so flipping the switch has to reach it too -- otherwise the admin's
        // own RPCs move while the delegated ones keep the listening port.
        if let Some(client) = lock(&self.inner.client).as_ref() {
            client.set_vip_channel_enabled(enable);
        }
    }

    pub fn set_poll_name_server_interval_millis(&self, millis: u64) {
        self.update_config(|c| c.poll_name_server_interval_millis = millis);
    }

    pub fn set_timeout_millis(&self, timeout_millis: i64) {
        self.update_config(|c| c.timeout_millis = timeout_millis);
    }

    pub fn set_rpc_hook(&self, hook: Option<Arc<dyn RPCHook>>) {
        *self
            .inner
            .rpc_hook
            .write()
            .unwrap_or_else(|e| e.into_inner()) = hook;
    }

/// Joins the address list back into one string with semicolons.
    pub fn get_name_server_addr(&self) -> String {
        read_cfg(&self.inner.cfg).name_server_addrs.join(";")
    }

    pub fn get_name_server_address_list(&self) -> Vec<String> {
        read_cfg(&self.inner.cfg).name_server_addrs
    }

    pub fn client_id(&self) -> String {
        read_cfg(&self.inner.cfg).client_id.unwrap_or_default()
    }

    pub fn timeout_millis(&self) -> i64 {
        read_cfg(&self.inner.cfg).timeout_millis
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    /// Idempotent; requires a name server; builds `client_id` on the fly when absent.
    ///
    /// The admin's clientId is likewise second-granularity `instanceName@yyyyMMddHHmmss`;
    /// two admins started within the same second collide on clientId, but the admin uses a
    /// **private instance**: the later one simply overwrites the registry in the instance table, and neither affects the other's send/receive.
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
        if cfg.name_server_addrs.is_empty() {
            self.inner.started.store(false, Ordering::Release);
            bail!("name server address is not set");
        }
        let instance_name = MixAll::change_instance_name_to_pid(&cfg.instance_name);
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

        // Constructs the private instance directly.
        let client = MQClientInstance::with_config(
            &client_id,
            cfg.name_server_addrs.clone(),
            MQClientInstanceConfig {
                unit_name: cfg.unit_name.clone(),
                namespace_v2: cfg.namespace_v2.clone(),
                enable_stream_request_type: cfg.enable_stream_request_type,
                route_refresh_interval_millis: cfg.poll_name_server_interval_millis,
                vip_channel_enabled: cfg.vip_channel_enabled,
                ..Default::default()
            },
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
        // Dynamic name server: the instance may already have fetched addresses from the address server; write them back.
        let addrs = client.name_server_addrs();
        if !addrs.is_empty() {
            self.update_config(|c| c.name_server_addrs = addrs);
        }
        *lock(&self.inner.client) = Some(client);
        Ok(())
    }

    /// Idempotent; tears down only its own private instance.
    pub fn shutdown(&self) {
        if !self.inner.started.swap(false, Ordering::AcqRel) {
            return;
        }
        if let Some(client) = lock(&self.inner.client).take() {
            client.shutdown();
        }
    }

    pub fn get_mq_client_instance(&self) -> Result<MQClientInstance> {
        self.require_client()
    }

    fn require_client(&self) -> Result<MQClientInstance> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(Error::client("admin not started, call start() first"));
        }
        lock(&self.inner.client)
            .clone()
            .ok_or_else(|| Error::client("admin not started, call start() first"))
    }

    // ---------------- Low-level invoke helpers ----------------

    /// Sends to one broker over the address the fast channel listener uses when the switch is
    /// on, and checks for SUCCESS.
    async fn invoke_broker(
        &self,
        addr: &str,
        code: i32,
        ext: &StringMap,
        body: Option<Vec<u8>>,
        timeout_millis: Option<i64>,
    ) -> Result<RemotingCommand> {
        let target =
            MixAll::broker_vip_channel(read_cfg(&self.inner.cfg).vip_channel_enabled, addr);
        self.invoke_broker_on(&target, code, ext, body, timeout_millis)
            .await
    }

    /// The same round trip on the address the name server published. Reading a broker's own
    /// configuration is the one admin RPC that never moves: it answers on the listening port
    /// only, so aiming it at the fast channel would talk to a port that is closed whenever the
    /// channel is off.
    async fn invoke_broker_plain(
        &self,
        addr: &str,
        code: i32,
        ext: &StringMap,
        body: Option<Vec<u8>>,
        timeout_millis: Option<i64>,
    ) -> Result<RemotingCommand> {
        self.invoke_broker_on(addr, code, ext, body, timeout_millis)
            .await
    }

    async fn invoke_broker_on(
        &self,
        target: &str,
        code: i32,
        ext: &StringMap,
        body: Option<Vec<u8>>,
        timeout_millis: Option<i64>,
    ) -> Result<RemotingCommand> {
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let mut request = build_request(code, ext, body);
        let response = client.invoke_sync(target, &mut request, timeout).await?;
        MQClientInstance::check_response(&response)?;
        Ok(response)
    }

    async fn invoke_namesrv_all(
        &self,
        code: i32,
        ext: &StringMap,
        timeout_millis: Option<i64>,
    ) -> Result<()> {
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let mut err_response: Option<RemotingCommand> = None;
        for ns_addr in client.name_server_addrs() {
            let mut request = build_request(code, ext, None);
            let response = client.invoke_sync(&ns_addr, &mut request, timeout).await?;
            if response.code != response_code::SUCCESS {
                err_response = Some(response);
            }
        }
        if let Some(response) = err_response {
            return Err(Error::client_with_code(
                response.code,
                response
                    .remark
                    .unwrap_or_else(|| "put/delete kv config failed".to_string()),
            ));
        }
        Ok(())
    }

    async fn invoke_namesrv_one(
        &self,
        code: i32,
        ext: &StringMap,
        timeout_millis: Option<i64>,
    ) -> Result<RemotingCommand> {
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let mut last_err: Option<Error> = None;
        for ns_addr in client.name_server_addrs() {
            let mut request = build_request(code, ext, None);
            match client.invoke_sync(&ns_addr, &mut request, timeout).await {
                Ok(response) => return Ok(response),
                Err(e) => last_err = Some(e),
            }
        }
        let reason = match last_err {
            Some(e) => e.to_string(),
            None => "no name server configured".to_string(),
        };
        bail!("all name servers unreachable: {reason}")
    }

    /// Takes the first broker address from the cluster info.
    async fn first_broker_addr(&self) -> Result<String> {
        let client = self.require_client()?;
        let timeout = self.timeout_millis();
        if let Ok(cluster) = client.get_broker_cluster_info(timeout).await {
            if let Some(addr) = cluster.get_broker_addrs().into_iter().next() {
                return Ok(addr);
            }
        }
        bail!("no broker address available")
    }

    async fn broker_addrs_of_cluster(&self, cluster_name: Option<&str>) -> Result<Vec<String>> {
        let client = self.require_client()?;
        let timeout = self.timeout_millis();
        let cluster = client.get_broker_cluster_info(timeout).await?;
        Ok(cluster_broker_addrs(&cluster, cluster_name))
    }

    // ---------------- Topic management ----------------

    /// Distributes per broker via the default topic's route (one success suffices).
    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
    ) -> Result<()> {
        let _ = key;
        let client = self.require_client()?;
        client
            .create_topic_in_route(
                new_topic,
                queue_num,
                queue_num,
                MixAll::READ_PERM_BY_DEFAULT,
                topic_sys_flag,
                None,
                ADMIN_OP_TIMEOUT_MILLIS,
            )
            .await
    }

    pub async fn create_and_update_topic_config(
        &self,
        addr: &str,
        config: &TopicConfig,
    ) -> Result<()> {
        let client = self.require_client()?;
        client
            .create_topic_in_broker(
                addr,
                MixAll::DEFAULT_TOPIC,
                &config.topic_name,
                config.read_queue_nums,
                config.write_queue_nums,
                config.perm,
                config.topic_sys_flag,
                &config.topic_filter_type,
                config.order,
                None,
                ADMIN_OP_TIMEOUT_MILLIS,
                5,
            )
            .await
    }

    pub async fn create_topic_in_broker(
        &self,
        broker_addr: &str,
        topic: &str,
        read_queue_nums: i32,
        write_queue_nums: i32,
        perm: i32,
    ) -> Result<()> {
        let client = self.require_client()?;
        client
            .create_topic_in_broker(
                broker_addr,
                MixAll::DEFAULT_TOPIC,
                topic,
                read_queue_nums,
                write_queue_nums,
                perm,
                0,
                crate::common::topic_config::TopicFilterType::SINGLE_TAG,
                false,
                None,
                ADMIN_OP_TIMEOUT_MILLIS,
                5,
            )
            .await
    }

    pub async fn delete_topic_in_broker(&self, broker_addr: &str, topic: &str) -> Result<()> {
        let client = self.require_client()?;
        client
            .delete_topic_in_broker(broker_addr, topic, self.timeout_millis())
            .await
    }

    /// Synchronous call to an explicitly given NameServer address (no VIP channel -- when
    /// `invoke_broker` enables VIP it subtracts 2 from the port; the NameServer port must not follow).
    async fn invoke_namesrv_addr(
        &self,
        addr: &str,
        code: i32,
        ext: &StringMap,
        timeout_millis: Option<i64>,
    ) -> Result<RemotingCommand> {
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let mut request = build_request(code, ext, None);
        let response = client.invoke_sync(addr, &mut request, timeout).await?;
        MQClientInstance::check_response(&response)?;
        Ok(response)
    }

    /// With no address list given, clears all NameServers configured on itself.
    pub async fn delete_topic_in_name_server(
        &self,
        addrs: Option<&[String]>,
        topic: &str,
    ) -> Result<()> {
        let targets: Vec<String> = match addrs {
            Some(addrs) if !addrs.is_empty() => addrs.to_vec(),
            _ => self.config().name_server_addrs,
        };
        let ext = ext_pairs(&[("topic", topic.to_string())]);
        for ns_addr in targets {
            self.invoke_namesrv_addr(&ns_addr, request_code::DELETE_TOPIC_IN_NAMESRV, &ext, None)
                .await?;
        }
        Ok(())
    }

    /// Legacy-name compatibility: deletes the topic route via any one successful NameServer.
    pub async fn delete_topic_in_namesrv(&self, topic: &str) -> Result<()> {
        let client = self.require_client()?;
        client
            .delete_topic_in_namesrv(topic, self.timeout_millis())
            .await
    }

    /// Clears each broker first, then the NameServer route, finally the KV namespace;
    /// all three stages are "warn on failure and keep going".
    pub async fn delete_topic(&self, topic: &str, cluster_name: Option<&str>) -> Result<()> {
        let client = self.require_client()?;
        let timeout = self.timeout_millis();
        for broker in self.broker_addrs_of_cluster(cluster_name).await? {
            if let Err(e) = client.delete_topic_in_broker(&broker, topic, timeout).await {
                rmq_warn!("delete topic {topic} in broker {broker} failed: {e}");
            }
        }
        if let Err(e) = client.delete_topic_in_namesrv(topic, timeout).await {
            rmq_warn!("delete topic {topic} in name server failed: {e}");
        }
        for ns in self.config().kv_namespace_to_delete_list {
            if let Err(e) = self.delete_kv_config(&ns, topic).await {
                rmq_warn!("delete kv config {ns}/{topic} failed: {e}");
            }
        }
        Ok(())
    }

    pub async fn fetch_all_topic_list(&self) -> Result<TopicList> {
        let client = self.require_client()?;
        client
            .get_all_topic_list_from_name_server(self.timeout_millis())
            .await
    }

    /// Hits the NameServer with `GET_TOPICS_BY_CLUSTER` and fetches the topic list by cluster name.
    pub async fn fetch_topics_by_cluster(&self, cluster_name: &str) -> Result<Vec<String>> {
        let ext = ext_pairs(&[("cluster", cluster_name.to_string())]);
        let response = self
            .invoke_namesrv_one(request_code::GET_TOPICS_BY_CLUSTER, &ext, None)
            .await?;
        let mut topics: Vec<String> = Vec::new();
        if response.code == response_code::SUCCESS {
            if let Some(body) = response.body.as_deref().filter(|b| !b.is_empty()) {
                let obj = RemotingSerializable::decode(body)?;
                topics.extend(string_list(&obj, "topicList"));
            }
        }
        Ok(topics)
    }

    /// The set of cluster names whose route contains this topic's brokers.
    pub async fn get_cluster_list(&self, topic: &str) -> Result<Vec<String>> {
        let client = self.require_client()?;
        let timeout = self.timeout_millis();
        let cluster_info = client.get_broker_cluster_info(timeout).await?;
        let route = self.examine_topic_route(topic).await?;
        let broker_names: Vec<String> = route
            .get_broker_datas()
            .iter()
            .map(|bd| bd.broker_name.clone())
            .collect();
        let mut clusters: Vec<String> = cluster_info
            .cluster_addr_table
            .iter()
            .filter(|(_, names)| {
                names
                    .iter()
                    .any(|name| broker_names.iter().any(|b| b == name))
            })
            .map(|(cluster, _)| cluster.clone())
            .collect();
        clusters.sort();
        clusters.dedup();
        Ok(clusters)
    }

    /// Alias of `get_cluster_list`.
    pub async fn get_topic_cluster_list(&self, topic: &str) -> Result<Vec<String>> {
        self.get_cluster_list(topic).await
    }

    /// Walks the NameServer's topic list and pulls each route,
    /// skipping any single failure outright.
    pub async fn fetch_all_topic_route(&self) -> Result<Vec<TopicRouteData>> {
        let client = self.require_client()?;
        let mut result: Vec<TopicRouteData> = Vec::new();
        let topics = self.fetch_all_topic_list().await?;
        for topic in topics.get_topic_list() {
            match client.get_topic_route_data(&topic).await {
                Some(route) => result.push(route),
                None => continue,
            }
        }
        Ok(result)
    }

    pub async fn examine_topic_route(&self, topic: &str) -> Result<TopicRouteData> {
        let client = self.require_client()?;
        client
            .get_topic_route_data(topic)
            .await
            .ok_or_else(|| Error::client(format!("topic {topic} not exist")))
    }

    /// Single query of the topic config via `GET_TOPIC_CONFIG`; the body is TopicConfig JSON.
    pub async fn examine_topic_config(&self, addr: &str, topic: &str) -> Result<TopicConfig> {
        let ext = ext_pairs(&[("topic", topic.to_string()), ("lo", "true".to_string())]);
        let response = self
            .invoke_broker(addr, request_code::GET_TOPIC_CONFIG, &ext, None, None)
            .await?;
        let body = response
            .body
            .as_deref()
            .filter(|b| !b.is_empty())
            .ok_or_else(|| Error::Broker {
                response_code: response_code::SYSTEM_ERROR,
                message: format!("empty topic config for {topic}"),
            })?;
        let obj = RemotingSerializable::decode(body)?;
        TopicConfig::from_json_value(&obj)
    }

    pub async fn get_all_topic_config(
        &self,
        broker_addr: &str,
        timeout_millis: Option<i64>,
    ) -> Result<TopicConfigSerializeWrapper> {
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::GET_ALL_TOPIC_CONFIG,
                &ExtFields::new(),
                None,
                timeout_millis,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => TopicConfigSerializeWrapper::decode(body),
            None => Ok(TopicConfigSerializeWrapper::new()),
        }
    }

    /// Removes system topics and `%RETRY%` / `%DLQ%`.
    pub async fn get_user_topic_config(
        &self,
        broker_addr: &str,
        special_topic: bool,
        timeout_millis: Option<i64>,
    ) -> Result<TopicConfigSerializeWrapper> {
        let wrapper = self
            .get_all_topic_config(broker_addr, timeout_millis)
            .await?;
        let sys_topics = self
            .get_system_topic_list_from_broker(broker_addr, timeout_millis)
            .await?;
        Ok(filter_user_topic_configs(
            wrapper,
            &sys_topics.topic_list,
            special_topic,
        ))
    }

    pub async fn get_system_topic_list_from_broker(
        &self,
        broker_addr: &str,
        timeout_millis: Option<i64>,
    ) -> Result<TopicList> {
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::GET_SYSTEM_TOPIC_LIST_FROM_BROKER,
                &ExtFields::new(),
                None,
                timeout_millis,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => TopicList::decode(body),
            None => Ok(TopicList::new()),
        }
    }

    /// Walks all brokers of the topic and merges the stats.
    pub async fn examine_topic_stats(&self, topic: &str) -> Result<TopicStatsTable> {
        let route = self.examine_topic_route(topic).await?;
        let mut merged = TopicStatsTable::new();
        for bd in route.get_broker_datas() {
            let Some(addr) = bd.select_broker_addr().filter(|a| !a.is_empty()) else {
                continue;
            };
            let part = match self.examine_topic_stats_by_broker(&addr, topic).await {
                Ok(part) => part,
                Err(e) => {
                    // Warn only and continue (the broker may not have stats enabled).
                    rmq_warn!("getTopicStatsInfo error. topic={topic} broker={addr}: {e}");
                    continue;
                }
            };
            merge_topic_stats(&mut merged, part);
        }
        if merged.offset_table.is_empty() {
            bail!("Not found the topic stats info");
        }
        Ok(merged)
    }

    pub async fn examine_topic_stats_by_broker(
        &self,
        broker_addr: &str,
        topic: &str,
    ) -> Result<TopicStatsTable> {
        let ext = ext_pairs(&[("topic", topic.to_string())]);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::GET_TOPIC_STATS_INFO,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => TopicStatsTable::decode(body),
            None => Ok(TopicStatsTable::new()),
        }
    }

    // ---------------- Cluster / Broker ----------------

    pub async fn fetch_broker_cluster_info(&self) -> Result<ClusterInfo> {
        let client = self.require_client()?;
        client.get_broker_cluster_info(self.timeout_millis()).await
    }

    /// Alias of `fetch_broker_cluster_info`.
    pub async fn examine_broker_cluster_info(&self) -> Result<ClusterInfo> {
        self.fetch_broker_cluster_info().await
    }

    pub async fn fetch_broker_runtime_stats(
        &self,
        broker_addr: &str,
        timeout_millis: Option<i64>,
    ) -> Result<KVTable> {
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::GET_BROKER_RUNTIME_INFO,
                &ExtFields::new(),
                None,
                timeout_millis,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => KVTable::decode(body),
            None => Ok(KVTable::default()),
        }
    }

    pub async fn get_broker_runtime_info(
        &self,
        broker_addr: &str,
        timeout_millis: Option<i64>,
    ) -> Result<KVTable> {
        self.fetch_broker_runtime_stats(broker_addr, timeout_millis)
            .await
    }

    /// The response body is **properties text**, not JSON/KVTable.
    ///
    /// This read is the one broker RPC that always uses the listening port: the fast channel
    /// serves the other admin RPCs, not this one.
    pub async fn get_broker_config(
        &self,
        broker_addr: &str,
        timeout_millis: Option<i64>,
    ) -> Result<StringMap> {
        let response = self
            .invoke_broker_plain(
                broker_addr,
                request_code::GET_BROKER_CONFIG,
                &ExtFields::new(),
                None,
                timeout_millis,
            )
            .await?;
        let text = match response.body.as_deref() {
            Some(body) if !body.is_empty() => String::from_utf8_lossy(body).into_owned(),
            _ => String::new(),
        };
        Ok(MixAll::string_to_properties(&text))
    }

    /// Updates the broker config (including brokerPermission validity checking).
    ///
    /// The check runs before any IO (sending the request is the last step of the flow),
    /// so an illegal value is reported as `NO_PERMISSION`(16) even before start.
    pub async fn update_broker_config(
        &self,
        broker_addr: &str,
        properties: &StringMap,
        timeout_millis: Option<i64>,
    ) -> Result<()> {
        if let Some(perm) = properties.get("brokerPermission") {
            if !PermName::is_valid_str(perm) {
                return Err(Error::client_with_code(
                    response_code::NO_PERMISSION,
                    format!("brokerPermission value: {perm} is invalid."),
                ));
            }
        }
        let text = MixAll::properties_to_string(properties, false);
        if text.is_empty() {
            return Ok(());
        }
        self.invoke_broker(
            broker_addr,
            request_code::UPDATE_BROKER_CONFIG,
            &ExtFields::new(),
            Some(text.into_bytes()),
            timeout_millis,
        )
        .await?;
        Ok(())
    }

    /// Reads the response header `wipeTopicCount`.
    pub async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: &str,
        broker_name: &str,
    ) -> Result<i64> {
        let ext = ext_pairs(&[("brokerName", broker_name.to_string())]);
        let response = self
            .invoke_namesrv_addr(
                namesrv_addr,
                request_code::WIPE_WRITE_PERM_OF_BROKER,
                &ext,
                None,
            )
            .await?;
        Ok(ext_int(&response, "wipeTopicCount"))
    }

    /// Reads the response header `addTopicCount`.
    pub async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: &str,
        broker_name: &str,
    ) -> Result<i64> {
        let ext = ext_pairs(&[("brokerName", broker_name.to_string())]);
        let response = self
            .invoke_namesrv_addr(
                namesrv_addr,
                request_code::ADD_WRITE_PERM_OF_BROKER,
                &ext,
                None,
            )
            .await?;
        Ok(ext_int(&response, "addTopicCount"))
    }

    /// Sends `CLEAN_UNUSED_TOPIC` to each broker;
    /// any failure (including non-SUCCESS) makes the whole call return `false` (no throw).
    pub async fn clean_unused_topic(&self, cluster_name: Option<&str>) -> Result<bool> {
        let mut ok = true;
        for addr in self.broker_addrs_of_cluster(cluster_name).await? {
            if let Err(e) = self
                .invoke_broker(
                    &addr,
                    request_code::CLEAN_UNUSED_TOPIC,
                    &ExtFields::new(),
                    None,
                    None,
                )
                .await
            {
                rmq_warn!("cleanUnusedTopic on {addr} failed: {e}");
                ok = false;
            }
        }
        Ok(ok)
    }

    pub async fn view_broker_stats_data(
        &self,
        broker_addr: &str,
        stats_name: &str,
        stats_key: &str,
    ) -> Result<Value> {
        let ext = ext_pairs(&[
            ("statsName", stats_name.to_string()),
            ("statsKey", stats_key.to_string()),
        ]);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::VIEW_BROKER_STATS_DATA,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => RemotingSerializable::decode(body),
            None => Ok(json!({})),
        }
    }

    // ---------------- NameServer KV config ----------------

    pub async fn create_and_update_kv_config(
        &self,
        namespace: &str,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let ext = ext_pairs(&[
            ("namespace", namespace.to_string()),
            ("key", key.to_string()),
            ("value", value.to_string()),
        ]);
        self.invoke_namesrv_all(request_code::PUT_KV_CONFIG, &ext, None)
            .await
    }

    pub async fn put_kv_config(&self, namespace: &str, key: &str, value: &str) -> Result<()> {
        self.create_and_update_kv_config(namespace, key, value)
            .await
    }

    /// Returns `None` on non-SUCCESS (no throw).
    pub async fn get_kv_config(&self, namespace: &str, key: &str) -> Result<Option<String>> {
        let ext = ext_pairs(&[
            ("namespace", namespace.to_string()),
            ("key", key.to_string()),
        ]);
        let response = self
            .invoke_namesrv_one(request_code::GET_KV_CONFIG, &ext, None)
            .await?;
        if response.code == response_code::SUCCESS {
            return Ok(response.get_ext_field("value").map(str::to_string));
        }
        Ok(None)
    }

    /// Broadcasts the KV config deletion to all NameServers.
    pub async fn delete_kv_config(&self, namespace: &str, key: &str) -> Result<()> {
        let ext = ext_pairs(&[
            ("namespace", namespace.to_string()),
            ("key", key.to_string()),
        ]);
        self.invoke_namesrv_all(request_code::DELETE_KV_CONFIG, &ext, None)
            .await
    }

    pub async fn get_kv_list_by_namespace(&self, namespace: &str) -> Result<KVTable> {
        let ext = ext_pairs(&[("namespace", namespace.to_string())]);
        let response = self
            .invoke_namesrv_one(request_code::GET_KVLIST_BY_NAMESPACE, &ext, None)
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => KVTable::decode(body),
            None => Ok(KVTable::default()),
        }
    }

    // ---------------- Subscription group management ----------------

    /// The body is the config JSON.
    pub async fn create_and_update_subscription_group_config(
        &self,
        addr: &str,
        config: &SubscriptionGroupConfig,
    ) -> Result<()> {
        self.invoke_broker(
            addr,
            request_code::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP,
            &ExtFields::new(),
            Some(config.encode()),
            None,
        )
        .await?;
        Ok(())
    }

    /// Pulls all subscription groups and picks the target group.
    pub async fn examine_subscription_group_config(
        &self,
        addr: &str,
        group: &str,
    ) -> Result<Option<SubscriptionGroupConfig>> {
        let wrapper = self.get_all_subscription_group(addr, None).await?;
        Ok(wrapper.get(group).cloned())
    }

    /// Single query of a subscription group config via `GET_SUBSCRIPTIONGROUP_CONFIG`.
    pub async fn get_subscription_group_config(
        &self,
        addr: &str,
        group: &str,
    ) -> Result<Option<SubscriptionGroupConfig>> {
        let ext = ext_pairs(&[("group", group.to_string())]);
        let response = self
            .invoke_broker(
                addr,
                request_code::GET_SUBSCRIPTIONGROUP_CONFIG,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => SubscriptionGroupConfig::decode(body).map(Some),
            None => Ok(None),
        }
    }

    /// Accumulates **page by page** until `groupSeq >= totalGroupNum-1`.
    pub async fn get_all_subscription_group(
        &self,
        broker_addr: &str,
        timeout_millis: Option<i64>,
    ) -> Result<SubscriptionGroupWrapper> {
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let began = Instant::now();
        let mut current_version: Option<Value> = None;
        let mut group_seq: i32 = 0;
        let mut table: Vec<(String, SubscriptionGroupConfig)> = Vec::new();
        let mut forbidden: Value = json!({});
        loop {
            let elapsed = i64::try_from(began.elapsed().as_millis()).unwrap_or(i64::MAX);
            let left = timeout - elapsed;
            if left < 0 {
                bail!("invokeSync call timeout");
            }
            let mut ext = ExtFields::new();
            ext.insert("groupSeq", group_seq.to_string());
            ext.insert("maxGroupNum", MAX_GROUP_NUM.to_string());
            if let Some(version) = current_version.as_ref() {
                ext.insert("dataVersion", RemotingSerializable::to_json_string(version));
            }
            let mut request =
                build_request(request_code::GET_ALL_SUBSCRIPTIONGROUP_CONFIG, &ext, None);
            let response = client
                .invoke_sync(
                    &MixAll::broker_vip_channel(
                        read_cfg(&self.inner.cfg).vip_channel_enabled,
                        broker_addr,
                    ),
                    &mut request,
                    left,
                )
                .await?;
            if response.code != response_code::SUCCESS {
                return Err(Error::Broker {
                    response_code: response.code,
                    message: response.remark.unwrap_or_default(),
                });
            }
            let wrapper = match response.body.as_deref().filter(|b| !b.is_empty()) {
                Some(body) => SubscriptionGroupWrapper::decode(body)?,
                None => SubscriptionGroupWrapper::new(),
            };
            let page_count =
                i32::try_from(wrapper.subscription_group_table.len()).unwrap_or(i32::MAX);
            let new_version = wrapper.data_version.clone();
            merge_group_table(&mut table, wrapper.subscription_group_table);
            merge_json_map(&mut forbidden, wrapper.forbidden_table);
            if current_version.is_none() {
                current_version = Some(new_version.clone());
            }
            group_seq += page_count;

            // Old brokers: no totalGroupNum, everything comes back in one page.
            let Some(total) = response
                .get_ext_field("totalGroupNum")
                .and_then(|v| v.trim().parse::<i32>().ok())
            else {
                break;
            };
            if current_version.as_ref() != Some(&new_version) {
                rmq_warn!("subscription group dataVersion changed, restart paging");
                current_version = Some(new_version);
                group_seq = 0;
                table.clear();
                forbidden = json!({});
                continue;
            }
            if group_seq >= total - 1 {
                break;
            }
        }
        Ok(SubscriptionGroupWrapper {
            subscription_group_table: table,
            forbidden_table: forbidden,
            data_version: current_version.unwrap_or(Value::Null),
        })
    }

    /// Removes system groups and predefined groups.
    pub async fn get_user_subscription_group(
        &self,
        broker_addr: &str,
        timeout_millis: Option<i64>,
    ) -> Result<SubscriptionGroupWrapper> {
        let mut wrapper = self
            .get_all_subscription_group(broker_addr, timeout_millis)
            .await?;
        wrapper.subscription_group_table.retain(|(name, _)| {
            !MixAll::is_sys_consumer_group(Some(name.as_str()))
                && !MixAll::is_predefined_group(name.as_str())
        });
        Ok(wrapper)
    }

    pub async fn delete_subscription_group(
        &self,
        addr: &str,
        group_name: &str,
        remove_offset: bool,
    ) -> Result<()> {
        let ext = ext_pairs(&[
            ("groupName", group_name.to_string()),
            ("cleanOffset", bool_flag(remove_offset).to_string()),
        ]);
        self.invoke_broker(
            addr,
            request_code::DELETE_SUBSCRIPTIONGROUP,
            &ext,
            None,
            None,
        )
        .await?;
        Ok(())
    }

    // ---------------- Consumer / producer connections ----------------

    pub async fn examine_consumer_connection_info(
        &self,
        consumer_group: &str,
        broker_addr: Option<&str>,
    ) -> Result<ConsumerConnection> {
        let addr = match broker_addr {
            Some(a) => a.to_string(),
            None => self.first_broker_addr().await?,
        };
        let ext = ext_pairs(&[("consumerGroup", consumer_group.to_string())]);
        let response = self
            .invoke_broker(
                &addr,
                request_code::GET_CONSUMER_CONNECTION_LIST,
                &ext,
                None,
                None,
            )
            .await?;
        let body = response
            .body
            .as_deref()
            .filter(|b| !b.is_empty())
            .ok_or_else(|| Error::client(format!("consumer group {consumer_group} not online")))?;
        ConsumerConnection::decode(body)
    }

    /// Alias of `examine_consumer_connection_info`.
    pub async fn examine_consumer_connection(
        &self,
        consumer_group: &str,
        broker_addr: Option<&str>,
    ) -> Result<ConsumerConnection> {
        self.examine_consumer_connection_info(consumer_group, broker_addr)
            .await
    }

    pub async fn examine_producer_connection_info(
        &self,
        producer_group: &str,
        broker_addr: Option<&str>,
    ) -> Result<ProducerConnection> {
        let addr = match broker_addr {
            Some(a) => a.to_string(),
            None => self.first_broker_addr().await?,
        };
        let ext = ext_pairs(&[("producerGroup", producer_group.to_string())]);
        let response = self
            .invoke_broker(
                &addr,
                request_code::GET_PRODUCER_CONNECTION_LIST,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => ProducerConnection::decode(body),
            None => Ok(ProducerConnection::default()),
        }
    }

    /// Fetches the consumer running info (request 38, forwarded by the broker to the target client;
    /// on this instance's side it is answered by `MQClientInstance`'s 307 processor, see `live_client_modules`).
    pub async fn examine_consumer_running_info(
        &self,
        consumer_group: &str,
        client_id: &str,
        jstack: bool,
        broker_addr: Option<&str>,
    ) -> Result<ConsumerRunningInfo> {
        let addr = match broker_addr {
            Some(a) => a.to_string(),
            None => self.first_broker_addr().await?,
        };
        let ext = ext_pairs(&[
            ("consumerGroup", consumer_group.to_string()),
            ("clientId", client_id.to_string()),
            ("jstackEnable", bool_flag(jstack).to_string()),
        ]);
        let response = self
            .invoke_broker(
                &addr,
                request_code::GET_CONSUMER_RUNNING_INFO,
                &ext,
                None,
                None,
            )
            .await?;
        let body = response
            .body
            .as_deref()
            .filter(|b| !b.is_empty())
            .ok_or_else(|| Error::client(format!("no running info for client {client_id}")))?;
        ConsumerRunningInfo::decode(body)
    }

    /// Alias of `examine_consumer_running_info`.
    pub async fn get_consumer_running_info(
        &self,
        consumer_group: &str,
        client_id: &str,
        jstack: bool,
        broker_addr: Option<&str>,
    ) -> Result<ConsumerRunningInfo> {
        self.examine_consumer_running_info(consumer_group, client_id, jstack, broker_addr)
            .await
    }

    pub async fn get_consumer_list_by_group(
        &self,
        consumer_group: &str,
        broker_addr: Option<&str>,
    ) -> Result<GetConsumerListByGroupResponseBody> {
        let client = self.require_client()?;
        let addr = match broker_addr {
            Some(a) => a.to_string(),
            None => self.first_broker_addr().await?,
        };
        client
            .get_consumer_list_by_group(consumer_group, self.timeout_millis(), Some(addr.as_str()))
            .await
    }

    // ---------------- Consume stats ----------------

    pub async fn examine_consume_stats(
        &self,
        broker_addr: &str,
        consumer_group: &str,
        topic: Option<&str>,
        topic_list: Option<&[String]>,
    ) -> Result<ConsumeStats> {
        let mut ext = ext_pairs(&[("consumerGroup", consumer_group.to_string())]);
        if let Some(topic) = topic.filter(|t| !t.is_empty()) {
            ext.insert("topic", topic.to_string());
        }
        if let Some(list) = topic_list.filter(|l| !l.is_empty()) {
            ext.insert("topicList", list.join(";"));
        }
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::GET_CONSUME_STATS,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => ConsumeStats::decode(body),
            None => Ok(ConsumeStats::new()),
        }
    }

    pub async fn fetch_consume_stats_in_broker(
        &self,
        broker_addr: &str,
        is_order: bool,
        timeout_millis: Option<i64>,
    ) -> Result<ConsumeStatsList> {
        let ext = ext_pairs(&[("isOrder", bool_flag(is_order).to_string())]);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::GET_BROKER_CONSUME_STATS,
                &ext,
                None,
                timeout_millis,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => ConsumeStatsList::decode(body),
            None => Ok(ConsumeStatsList::new()),
        }
    }

    /// Request 300; takes the response header `groupList`.
    pub async fn query_topic_consume_by_who(
        &self,
        broker_addr: &str,
        topic: &str,
    ) -> Result<Vec<String>> {
        let ext = ext_pairs(&[("topic", topic.to_string())]);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::QUERY_TOPIC_CONSUME_BY_WHO,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => Ok(string_list(
                &RemotingSerializable::decode(body)?,
                "groupList",
            )),
            None => Ok(Vec::new()),
        }
    }


    pub async fn examine_consume_stats_group(
        &self,
        consumer_group: &str,
        topic: Option<&str>,
    ) -> Result<ConsumeStats> {
        let route = self
            .examine_topic_route(&MixAll::get_retry_topic(consumer_group))
            .await?;
        let mut result = ConsumeStats::new();
        for bd in &route.broker_datas {
            if let Some(addr) = bd.select_broker_addr() {
                let part = self
                    .examine_consume_stats(&addr, consumer_group, topic, None)
                    .await?;
                for (key, wrapper) in part.offset_table {
                    match result.offset_table.iter_mut().find(|(k, _)| *k == key) {
                        Some(slot) => slot.1 = wrapper,
                        None => result.offset_table.push((key, wrapper)),
                    }
                }
                result.consume_tps += part.consume_tps;
            }
        }
        if result.offset_table.is_empty() {
            return Err(Error::client_with_code(
                response_code::CONSUMER_NOT_ONLINE,
                format!("no consume stats for group {consumer_group}"),
            ));
        }
        Ok(result)
    }

    pub async fn consumed(&self, msg: &MessageExt, group: &str) -> Result<bool> {
        let cstats = self.examine_consume_stats_group(group, None).await?;
        let ci = self.examine_broker_cluster_info().await?;
        let store_host = msg.get_store_host_string();
        for (key, wrapper) in &cstats.offset_table {
            if key.topic == msg.topic && key.queue_id == msg.queue_id {
                let broker_addrs = ci
                    .broker_addr_table
                    .iter()
                    .find(|(name, _)| *name == key.broker_name)
                    .map(|(_, addrs)| addrs);
                if let Some(addrs) = broker_addrs {
                    let addr = addrs
                        .iter()
                        .find(|(id, _)| *id == MixAll::MASTER_ID as i64)
                        .map(|(_, a)| a);
                    if let (Some(addr), Some(store_host)) = (addr, store_host.as_deref()) {
                        if *addr == store_host && wrapper.consumer_offset > msg.queue_offset {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    pub async fn message_track_detail(&self, msg: &MessageExt) -> Result<Vec<MessageTrack>> {
        let mut result: Vec<MessageTrack> = Vec::new();
        let route = self.examine_topic_route(&msg.topic).await?;
        let mut broker_addr: Option<String> = None;
        for bd in &route.broker_datas {
            if let Some(addr) = bd.select_broker_addr() {
                broker_addr = Some(addr);
                break;
            }
        }
        let Some(broker_addr) = broker_addr else {
            return Ok(result);
        };
        let mut groups = self
            .query_topic_consume_by_who(&broker_addr, &msg.topic)
            .await?;
        groups.sort();
        for group in groups {
            let mut mt = MessageTrack {
                consumer_group: group.clone(),
                track_type: TrackType::Unknown,
                exception_desc: None,
            };
            let cc = match self.examine_consumer_connection_info(&group, None).await {
                Ok(cc) => cc,
                Err(e) => {
                    if e.response_code() == Some(response_code::CONSUMER_NOT_ONLINE) {
                        mt.track_type = TrackType::NotOnline;
                    }
                    mt.exception_desc = Some(format!("CODE:{:?} DESC:{e}", e.response_code()));
                    result.push(mt);
                    continue;
                }
            };

            match cc.consume_type.as_deref() {
                Some("CONSUME_ACTIVELY") => mt.track_type = TrackType::Pull,
                Some("CONSUME_PASSIVELY") => {
                    let if_consumed = match self.consumed(msg, &group).await {
                        Ok(v) => v,
                        Err(e) => match e.response_code() {
                            Some(code)
                                if code == response_code::CONSUMER_NOT_ONLINE
                                    || code == response_code::BROADCAST_CONSUMPTION =>
                            {
                                if code == response_code::CONSUMER_NOT_ONLINE {
                                    mt.track_type = TrackType::NotOnline;
                                    mt.exception_desc = Some(format!("CODE:{code} DESC:{e}"));
                                } else {
                                    mt.track_type = TrackType::ConsumeBroadcasting;
                                }
                                result.push(mt);
                                continue;
                            }
                            _ => {
                                mt.exception_desc = Some(format!("{e}"));
                                result.push(mt);
                                continue;
                            }
                        },
                    };

                    if if_consumed {
                        mt.track_type = TrackType::Consumed;
                        let sub = cc
                            .subscription_table
                            .iter()
                            .find(|(topic, _)| *topic == msg.topic)
                            .map(|(_, v)| v);
                        if let Some(sub) = sub {
                            let tags_set: Vec<String> = sub
                                .get("tagsSet")
                                .and_then(Value::as_array)
                                .map(|a| {
                                    a.iter()
                                        .filter_map(Value::as_str)
                                        .map(str::to_string)
                                        .collect()
                                })
                                .unwrap_or_default();
                            let msg_tag = msg.get_tags();
                            if !tags_set.is_empty()
                                && !tags_set.iter().any(|t| t == "*")
                                && !tags_set.iter().any(|t| Some(t.as_str()) == msg_tag)
                            {
                                mt.track_type = TrackType::ConsumedButFiltered;
                            }
                        }
                    } else {
                        mt.track_type = TrackType::NotConsumeYet;
                    }
                }
                _ => {}
            }
            result.push(mt);
        }
        Ok(result)
    }

    /// The broker side answers by reverse-looking-up, from the offset table (`topic@group` keys),
    /// which topics the group has consumed. So when the group has never committed any offset,
    /// an empty table comes back -- expected, not a bug.
    pub async fn query_topics_by_consumer_to_broker(
        &self,
        broker_addr: &str,
        group: &str,
    ) -> Result<TopicList> {
        let ext = ext_pairs(&[("group", group.to_string())]);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::QUERY_TOPICS_BY_CONSUMER,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => TopicList::decode(body),
            None => Ok(TopicList::new()),
        }
    }

    pub async fn query_topics_by_consumer(&self, group: &str) -> Result<TopicList> {
        let route = self
            .examine_topic_route(&MixAll::get_retry_topic(group))
            .await?;
        let mut result = TopicList::new();
        let mut seen: Vec<String> = Vec::new();
        for bd in route.get_broker_datas() {
            let Some(addr) = bd.select_broker_addr().filter(|a| !a.is_empty()) else {
                continue;
            };
            let topics = self
                .query_topics_by_consumer_to_broker(&addr, group)
                .await?;
            for topic in topics.get_topic_list() {
                if seen.iter().any(|t| *t == *topic) {
                    continue;
                }
                seen.push(topic.clone());
                result.topic_list.push(topic.clone());
            }
        }
        Ok(result)
    }

    /// Request 345: checks whether the subscription relationship is consistent.
    pub async fn query_subscription(
        &self,
        broker_addr: &str,
        group: &str,
        topic: &str,
    ) -> Result<Option<Value>> {
        let ext = ext_pairs(&[("group", group.to_string()), ("topic", topic.to_string())]);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::QUERY_SUBSCRIPTION_BY_CONSUMER,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => Ok(Some(RemotingSerializable::decode(body)?)),
            None => Ok(None),
        }
    }

    /// Request 223; takes the response header `consumerTable`.
    pub async fn get_consume_status(
        &self,
        broker_addr: &str,
        topic: &str,
        group: &str,
        client_addr: &str,
    ) -> Result<Value> {
        let ext = ext_pairs(&[
            ("topic", topic.to_string()),
            ("group", group.to_string()),
            ("clientAddr", client_addr.to_string()),
        ]);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::INVOKE_BROKER_TO_GET_CONSUMER_STATUS,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => Ok(RemotingSerializable::decode(body)?
                .get("consumerTable")
                .cloned()
                .unwrap_or_else(|| json!({}))),
            None => Ok(json!({})),
        }
    }

    /// Request 314: clones a consumer group's offsets.
    pub async fn clone_group_offset(
        &self,
        broker_addr: &str,
        src_group: &str,
        dest_group: &str,
        topic: &str,
        is_offline: bool,
    ) -> Result<()> {
        let ext = ext_pairs(&[
            ("srcGroup", src_group.to_string()),
            ("destGroup", dest_group.to_string()),
            ("topic", topic.to_string()),
            ("offline", bool_flag(is_offline).to_string()),
        ]);
        self.invoke_broker(
            broker_addr,
            request_code::CLONE_GROUP_OFFSET,
            &ext,
            None,
            None,
        )
        .await?;
        Ok(())
    }

    // ---------------- Offset management ----------------

    pub async fn max_offset(&self, mq: &MessageQueue) -> Result<i64> {
        self.require_client()?
            .get_max_offset(mq, self.timeout_millis(), None)
            .await
    }

    pub async fn min_offset(&self, mq: &MessageQueue) -> Result<i64> {
        self.require_client()?
            .get_min_offset(mq, self.timeout_millis(), None)
            .await
    }

    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: i64) -> Result<i64> {
        self.search_lower_boundary_offset(mq, timestamp).await
    }

    pub async fn search_lower_boundary_offset(
        &self,
        mq: &MessageQueue,
        timestamp: i64,
    ) -> Result<i64> {
        self.require_client()?
            .search_offset_by_boundary(
                mq,
                timestamp,
                Some(BoundaryType::Lower),
                self.timeout_millis(),
                None,
            )
            .await
    }

    /// The difference from LOWER is only visible when several messages share a storeTime, or the timestamp
    /// falls in a gap / past the queue tail: after the tail UPPER returns the last message's own offset, LOWER returns the next offset (maxOffset).
    pub async fn search_upper_boundary_offset(
        &self,
        mq: &MessageQueue,
        timestamp: i64,
    ) -> Result<i64> {
        self.require_client()?
            .search_offset_by_boundary(
                mq,
                timestamp,
                Some(BoundaryType::Upper),
                self.timeout_millis(),
                None,
            )
            .await
    }

    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> Result<i64> {
        self.require_client()?
            .get_earliest_msg_store_time(mq, self.timeout_millis(), None)
            .await
    }

    /// Returns `None` when no offset was ever committed (`QUERY_NOT_FOUND`).
    pub async fn examine_consumer_offset(
        &self,
        consumer_group: &str,
        mq: &MessageQueue,
    ) -> Result<Option<i64>> {
        self.require_client()?
            .query_consumer_offset(consumer_group, mq, self.timeout_millis(), None, false)
            .await
    }

    /// Updates the consumer offset (addr resolved from the route).
    pub async fn update_consumer_offset(
        &self,
        consumer_group: &str,
        mq: &MessageQueue,
        offset: i64,
    ) -> Result<()> {
        self.require_client()?
            .update_consumer_offset(consumer_group, mq, offset, self.timeout_millis(), None)
            .await
    }

    /// Updates the consumer offset (addr given explicitly).
    pub async fn update_consumer_offset_to_broker(
        &self,
        broker_addr: &str,
        consumer_group: &str,
        mq: &MessageQueue,
        offset: i64,
    ) -> Result<()> {
        self.require_client()?
            .update_consumer_offset(
                consumer_group,
                mq,
                offset,
                self.timeout_millis(),
                Some(broker_addr),
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn reset_offset_by_timestamp(
        &self,
        topic: &str,
        group: &str,
        timestamp: i64,
        is_force: bool,
        cluster_name: Option<&str>,
        is_cpp: bool,
    ) -> Result<Vec<(MessageQueueKey, i64)>> {
        // Topics without their own route, such as LMQ / wheel_timer, look the route up by cluster
        // name, but the `topic` sent down to the broker stays the original value.
        let route_topic = if !topic.is_empty()
            && (MixAll::is_lmq(Some(topic))
                || topic == format!("{}wheel_timer", MixAll::SYSTEM_TOPIC_PREFIX))
        {
            match cluster_name.filter(|c| !c.is_empty()) {
                Some(cluster) => cluster.to_string(),
                None => topic.to_string(),
            }
        } else {
            topic.to_string()
        };
        let route = self.examine_topic_route(&route_topic).await?;
        let mut all_offsets: Vec<(MessageQueueKey, i64)> = Vec::new();
        for bd in route.get_broker_datas() {
            let Some(addr) = bd.select_broker_addr().filter(|a| !a.is_empty()) else {
                continue;
            };
            for (key, offset) in self
                .invoke_broker_reset_offset(
                    &addr, topic, group, timestamp, is_force, is_cpp, None, None,
                )
                .await?
            {
                match all_offsets.iter_mut().find(|(k, _)| *k == key) {
                    Some(entry) => entry.1 = offset,
                    None => all_offsets.push((key, offset)),
                }
            }
        }
        if all_offsets.is_empty() {
            bail!("reset offset failed, no broker returned offset table");
        }
        Ok(all_offsets)
    }

    /// One `INVOKE_BROKER_TO_RESET_OFFSET`(222); returns the table of queues the broker actually reset.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::fn_params_excessive_bools)]
    async fn invoke_broker_reset_offset(
        &self,
        broker_addr: &str,
        topic: &str,
        group: &str,
        timestamp: i64,
        is_force: bool,
        is_cpp: bool,
        queue_id: Option<i32>,
        offset: Option<i64>,
    ) -> Result<Vec<(MessageQueueKey, i64)>> {
        let client = self.require_client()?;
        let ext = reset_offset_ext_pairs(topic, group, timestamp, is_force, queue_id, offset);
        let mut request = build_request(request_code::INVOKE_BROKER_TO_RESET_OFFSET, &ext, None);
        if is_cpp {
            request.language = language_code::CPP;
        }
        let response = client
            .invoke_sync(
                &MixAll::broker_vip_channel(
                    read_cfg(&self.inner.cfg).vip_channel_enabled,
                    broker_addr,
                ),
                &mut request,
                self.timeout_millis(),
            )
            .await?;
        if response.code != response_code::SUCCESS {
            return Err(Error::client_with_code(
                response.code,
                response
                    .remark
                    .unwrap_or_else(|| "reset offset failed".to_string()),
            ));
        }
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => Ok(ResetOffsetBody::decode(body)?.offset_table),
            None => Ok(Vec::new()),
        }
    }

    pub async fn reset_offset_by_queue_id(
        &self,
        broker_addr: &str,
        consumer_group: &str,
        topic: &str,
        queue_id: i32,
        reset_offset: i64,
    ) -> Result<Vec<(MessageQueueKey, i64)>> {
        self.update_consumer_offset_to_broker(
            broker_addr,
            consumer_group,
            &MessageQueue::new(topic, "", queue_id),
            reset_offset,
        )
        .await?;
        self.invoke_broker_reset_offset(
            broker_addr,
            topic,
            consumer_group,
            0,
            false,
            false,
            Some(queue_id),
            Some(reset_offset),
        )
        .await
    }

    /// Tries the new version first (broker-side reset); falls back to the old version when the consumer is offline.
    pub async fn reset_offset_new(
        &self,
        consumer_group: &str,
        topic: &str,
        timestamp: i64,
    ) -> Result<()> {
        match self
            .reset_offset_by_timestamp(topic, consumer_group, timestamp, true, None, false)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.response_code() == Some(response_code::CONSUMER_NOT_ONLINE) => self
                .reset_offset_by_timestamp_old(consumer_group, topic, timestamp, true)
                .await
                .map(|_| ()),
            Err(e) => Err(e),
        }
    }

    /// Runs `searchOffset` per queue, then decides by force what to write back.
    pub async fn reset_offset_by_timestamp_old(
        &self,
        consumer_group: &str,
        topic: &str,
        timestamp: i64,
        force: bool,
    ) -> Result<Vec<(MessageQueueKey, i64)>> {
        let client = self.require_client()?;
        let route = self.examine_topic_route(topic).await?;
        let mut result: Vec<(MessageQueueKey, i64)> = Vec::new();
        for bd in route.get_broker_datas() {
            let Some(addr) = bd.select_broker_addr().filter(|a| !a.is_empty()) else {
                continue;
            };
            for mq in queues_of_broker(&route, topic, &bd.broker_name) {
                // A failed offset query counts as 0 (a queue that never committed an offset is the norm).
                let consumer_offset = match client
                    .query_consumer_offset(
                        consumer_group,
                        &mq,
                        self.timeout_millis(),
                        Some(&addr),
                        false,
                    )
                    .await
                {
                    Ok(Some(offset)) => offset,
                    Ok(None) => 0,
                    Err(_) => 0,
                };
                let reset_offset = if timestamp == -1 {
                    client
                        .get_max_offset(&mq, self.timeout_millis(), Some(&addr))
                        .await?
                } else {
                    client
                        .search_offset_by_timestamp(
                            &mq,
                            timestamp,
                            self.timeout_millis(),
                            Some(&addr),
                        )
                        .await?
                };
                if force || reset_offset <= consumer_offset {
                    client
                        .update_consumer_offset(
                            consumer_group,
                            &mq,
                            reset_offset,
                            self.timeout_millis(),
                            Some(&addr),
                        )
                        .await?;
                    result.push((
                        MessageQueueKey::new(&mq.topic, &mq.broker_name, mq.queue_id),
                        reset_offset,
                    ));
                }
            }
        }
        Ok(result)
    }

    // ---------------- Message query ----------------

    /// Queries messages by key (all brokers + a client-side key re-check).
    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<Vec<MessageExt>> {
        self.query_message_brokers(topic, key, max_num, begin, end, false)
            .await
    }

    /// `indexType="U"` + `_UNIQUE_KEY_QUERY="true"`.
    ///
    /// Note: broker-side uniqKey indexes are only supported by the RocksDB index implementation
    /// (`IndexRocksDBStore`); with the default file index this query may return empty -- a broker config difference, not a client problem.
    pub async fn query_message_by_uniq_key(
        &self,
        topic: &str,
        uniq_key: &str,
    ) -> Result<Option<MessageExt>> {
        let messages = self
            .query_message_brokers(
                topic,
                uniq_key,
                DEFAULT_QUERY_MESSAGE_MAX_NUM,
                0,
                current_time_millis() + UNIQ_QUERY_WINDOW_MILLIS,
                true,
            )
            .await?;
        Ok(messages.into_iter().next())
    }

    /// Queries messages by key (NORMAL index mode).
    pub async fn query_message_by_key(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
    ) -> Result<Vec<MessageExt>> {
        self.query_message_brokers(
            topic,
            key,
            max_num,
            0,
            current_time_millis() + UNIQ_QUERY_WINDOW_MILLIS,
            false,
        )
        .await
    }

    /// Facade shell over `MQClientInstance::query_message_all_brokers`. Throws outright
    /// when not started; the indexType is uniquely determined by `uniq_key`,
    /// and the three query entry points use only these two combinations.
    async fn query_message_brokers(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
        uniq_key: bool,
    ) -> Result<Vec<MessageExt>> {
        let client = self.require_client()?;
        let index_type = if uniq_key {
            INDEX_UNIQUE_TYPE
        } else {
            INDEX_KEY_TYPE
        };
        Ok(client
            .query_message_all_brokers(
                topic,
                key,
                max_num,
                begin,
                end,
                Some(index_type),
                uniq_key,
                self.timeout_millis(),
            )
            .await)
    }

    pub async fn view_message(&self, topic: &str, msg_id: &str) -> Result<MessageExt> {
        let by_id_error: Option<String>;
        match decode_message_id(msg_id) {
            Ok((ip, port, offset)) => {
                if port > 0 && port <= 65_535 {
                    let ext =
                        ext_pairs(&[("topic", topic.to_string()), ("offset", offset.to_string())]);
                    let addr = format!("{ip}:{port}");
                    match self
                        .invoke_broker(&addr, request_code::VIEW_MESSAGE_BY_ID, &ext, None, None)
                        .await
                    {
                        Ok(response) => match response.body.as_deref().filter(|b| !b.is_empty()) {
                            Some(body) => match decode_message(body) {
                                Ok(msg) => return Ok(msg),
                                Err(e) => by_id_error = Some(e.to_string()),
                            },
                            None => by_id_error = Some(format!("message not found: {msg_id}")),
                        },
                        Err(e) => by_id_error = Some(e.to_string()),
                    }
                } else {
                    by_id_error = Some(format!("not a valid offset msgId: {msg_id}"));
                }
            }
            Err(e) => by_id_error = Some(e.to_string()),
        }

        // The uniq query sits outside the error fallback: when that path really throws, the error
        // passes through; only "queried but empty" falls through to the final NO_MESSAGE.
        if let Some(found) = self.query_message_by_uniq_key(topic, msg_id).await? {
            return Ok(found);
        }
        Err(Error::client_with_code(
            response_code::NO_MESSAGE,
            format!(
                "viewMessage failed: neither offset msgId nor uniq key matched message {msg_id} \
                 of {topic}, cause: {}",
                by_id_error.unwrap_or_default()
            ),
        ))
    }

    pub async fn consume_message_directly(
        &self,
        consumer_group: &str,
        client_id: &str,
        topic: &str,
        msg_id: &str,
    ) -> Result<ConsumeMessageDirectlyResult> {
        let msg = self.view_message(topic, msg_id).await?;
        let addr = msg.get_store_host_string().ok_or_else(|| {
            Error::client(format!("message {msg_id} has no store host, cannot consume directly"))
        })?;
        let direct_msg_id = match msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) {
            Some(_) => msg.get_offset_msg_id().unwrap_or(msg_id),
            None => msg_id,
        };
        let ext = ext_pairs(&[
            ("consumerGroup", consumer_group.to_string()),
            ("clientId", client_id.to_string()),
            ("msgId", direct_msg_id.to_string()),
            ("topic", topic.to_string()),
        ]);
        let response = self
            .invoke_broker(&addr, request_code::CONSUME_MESSAGE_DIRECTLY, &ext, None, None)
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => ConsumeMessageDirectlyResult::decode(body),
            None => Err(Error::client(format!(
                "no consume result for client {client_id}"
            ))),
        }
    }

    /// Request 321: queries the ConsumeQueue.
    pub async fn query_consume_queue(
        &self,
        broker_addr: &str,
        topic: &str,
        queue_id: i32,
        index: i32,
        count: i32,
        consumer_group: &str,
    ) -> Result<QueryConsumeQueueResponseBody> {
        let ext = ext_pairs(&[
            ("topic", topic.to_string()),
            ("queueId", queue_id.to_string()),
            ("index", index.to_string()),
            ("count", count.to_string()),
            ("consumerGroup", consumer_group.to_string()),
        ]);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::QUERY_CONSUME_QUEUE,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => QueryConsumeQueueResponseBody::decode(body),
            None => Ok(QueryConsumeQueueResponseBody::default()),
        }
    }


    /// `UPDATE_AND_CREATE_TOPIC_LIST(18)`.
    ///
    /// Wire fact: the custom header is **empty** (the topic name in the body serves as the authorized resource),
    /// the body is CreateTopicListRequestBody JSON: {"topicConfigList": [...]}.
    pub async fn create_and_update_topic_config_list(
        &self,
        broker_addr: &str,
        configs: &[TopicConfig],
    ) -> Result<()> {
        if configs.is_empty() {
            bail!("createAndUpdateTopicConfigList: empty topicConfigList");
        }
        let rows: Vec<Value> = configs.iter().map(|c| c.to_json_value()).collect();
        let body = serde_json::to_vec(&json!({ "topicConfigList": rows }))
            .map_err(|e| Error::client(format!("encode topicConfigList: {e}")))?;
        self.invoke_broker(
            broker_addr,
            request_code::UPDATE_AND_CREATE_TOPIC_LIST,
            &ExtFields::new(),
            Some(body),
            None,
        )
        .await?;
        Ok(())
    }

    /// Request 225: updates subscription group configs in bulk.
    /// Same shape as the topic list, but the body key is `groupConfigList`.
    pub async fn create_and_update_subscription_group_config_list(
        &self,
        broker_addr: &str,
        configs: &[SubscriptionGroupConfig],
    ) -> Result<()> {
        if configs.is_empty() {
            bail!("createAndUpdateSubscriptionGroupConfigList: empty groupConfigList");
        }
        let rows: Vec<Value> = configs.iter().map(|c| c.to_json_value()).collect();
        let body = serde_json::to_vec(&json!({ "groupConfigList": rows }))
            .map_err(|e| Error::client(format!("encode groupConfigList: {e}")))?;
        self.invoke_broker(
            broker_addr,
            request_code::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST,
            &ExtFields::new(),
            Some(body),
            None,
        )
        .await?;
        Ok(())
    }

    /// `UPDATE_AND_CREATE_STATIC_TOPIC(513)`:
    /// The header reuses the CreateTopicRequestHeader fields; the body is TopicQueueMappingDetail JSON.
    pub async fn create_static_topic(
        &self,
        broker_addr: &str,
        default_topic: &str,
        config: &TopicConfig,
        mapping_detail: &Value,
        force: bool,
    ) -> Result<()> {
        if mapping_detail.is_null() {
            bail!("createStaticTopic: mappingDetail must not be null");
        }
        let ext = ext_pairs(&[
            ("topic", config.topic_name.clone()),
            ("defaultTopic", default_topic.to_string()),
            ("readQueueNums", config.read_queue_nums.to_string()),
            ("writeQueueNums", config.write_queue_nums.to_string()),
            ("perm", config.perm.to_string()),
            (
                "topicFilterType",
                if config.topic_filter_type.is_empty() {
                    "SINGLE_TAG".to_string()
                } else {
                    config.topic_filter_type.clone()
                },
            ),
            ("topicSysFlag", config.topic_sys_flag.to_string()),
            ("order", config.order.to_string()),
            ("force", force.to_string()),
        ]);
        let body = serde_json::to_vec(mapping_detail)
            .map_err(|e| Error::client(format!("encode mappingDetail: {e}")))?;
        self.invoke_broker(
            broker_addr,
            request_code::UPDATE_AND_CREATE_STATIC_TOPIC,
            &ext,
            Some(body),
            None,
        )
        .await?;
        Ok(())
    }

    /// `UPDATE_AND_GET_GROUP_FORBIDDEN(353)`.
    /// `readable=None` means query-only; the response body is GroupForbidden JSON.
    pub async fn update_and_get_group_read_forbidden(
        &self,
        broker_addr: &str,
        group: &str,
        topic: &str,
        readable: Option<bool>,
    ) -> Result<Value> {
        if group.is_empty() || topic.is_empty() {
            bail!("updateAndGetGroupReadForbidden: group/topic required");
        }
        let mut pairs = vec![("group", group.to_string()), ("topic", topic.to_string())];
        if let Some(flag) = readable {
            pairs.push(("readable", flag.to_string()));
        }
        let ext = ext_pairs(&pairs);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::UPDATE_AND_GET_GROUP_FORBIDDEN,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => serde_json::from_slice(body)
                .map_err(|e| Error::client(format!("decode GroupForbidden: {e}"))),
            None => bail!("updateAndGetGroupReadForbidden: empty response body"),
        }
    }

    /// `RESUME_CHECK_HALF_MESSAGE(323)`.
    pub async fn resume_check_half_message(
        &self,
        broker_addr: &str,
        topic: &str,
        msg_id: &str,
    ) -> Result<bool> {
        if topic.is_empty() {
            bail!("resumeCheckHalfMessage: topic required");
        }
        let mut pairs = vec![("topic", topic.to_string())];
        if !msg_id.is_empty() {
            pairs.push(("msgId", msg_id.to_string()));
        }
        let ext = ext_pairs(&pairs);
        let client = self.require_client()?;
        let addr =
            MixAll::broker_vip_channel(read_cfg(&self.inner.cfg).vip_channel_enabled, broker_addr);
        let mut request = build_request(request_code::RESUME_CHECK_HALF_MESSAGE, &ext, None);
        let response = client
            .invoke_sync(&addr, &mut request, self.timeout_millis())
            .await?;
        Ok(response.code == response_code::SUCCESS)
    }

    /// **Not a standalone request**: a read-modify-write on the NameServer KV
    /// namespace=ORDER_TOPIC_CONFIG. Non-cluster mode treats the stored value as a ";"-separated
    /// list of "topic:conf" entries, replaces the matching entry, and writes the whole thing back.
    pub async fn create_or_update_order_conf(
        &self,
        key: &str,
        value: &str,
        is_cluster: bool,
    ) -> Result<()> {
        if key.is_empty() || value.is_empty() {
            bail!("createOrUpdateOrderConf: key/value required");
        }
        if is_cluster {
            return self
                .put_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, key, value)
                .await;
        }
        let old_confs = self
            .get_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, key)
            .await
            .ok()
            .flatten()
            .unwrap_or_default();
        let mut entries: Vec<(String, String)> = Vec::new();
        for entry in old_confs.split(';') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let entry_key = entry.split(':').next().unwrap_or(entry).to_string();
            match entries.iter_mut().find(|(k, _)| *k == entry_key) {
                Some(slot) => slot.1 = entry.to_string(),
                None => entries.push((entry_key, entry.to_string())),
            }
        }
        let new_key = value.split(':').next().unwrap_or("");
        if new_key.is_empty() {
            bail!("createOrUpdateOrderConf: value must start with a key");
        }
        match entries.iter_mut().find(|(k, _)| k == new_key) {
            Some(slot) => slot.1 = value.to_string(),
            None => entries.push((new_key.to_string(), value.to_string())),
        }
        let merged = entries
            .iter()
            .map(|(_, v)| v.as_str())
            .collect::<Vec<_>>()
            .join(";");
        self.put_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, key, &merged)
            .await
    }

    // ---------------- Ops cleanup ----------------

    /// `CLEAN_EXPIRED_CONSUMEQUEUE(306)`.
    pub async fn clean_expired_consumer_queue(
        &self,
        broker_addr: &str,
        time_hours: i32,
    ) -> Result<()> {
        let ext = ext_pairs(&[("time", time_hours.to_string())]);
        self.invoke_broker(
            broker_addr,
            request_code::CLEAN_EXPIRED_CONSUMEQUEUE,
            &ext,
            None,
            None,
        )
        .await?;
        Ok(())
    }

    /// Runs per address, returning the **failed addresses**.
    pub async fn clean_expired_consumer_queue_by_addr(
        &self,
        addrs: &[String],
        time_hours: i32,
    ) -> Vec<String> {
        let mut failed = Vec::new();
        for addr in addrs {
            if self
                .clean_expired_consumer_queue(addr, time_hours)
                .await
                .is_err()
            {
                failed.push(addr.clone());
            }
        }
        failed
    }

    /// `DELETE_EXPIRED_COMMITLOG(329)`.
    pub async fn delete_expired_commit_log(
        &self,
        broker_addr: &str,
        time_hours: i32,
    ) -> Result<()> {
        let ext = ext_pairs(&[("time", time_hours.to_string())]);
        self.invoke_broker(
            broker_addr,
            request_code::DELETE_EXPIRED_COMMITLOG,
            &ext,
            None,
            None,
        )
        .await?;
        Ok(())
    }

    /// Runs per address, returning the **failed addresses**.
    pub async fn delete_expired_commit_log_by_addr(
        &self,
        addrs: &[String],
        time_hours: i32,
    ) -> Vec<String> {
        let mut failed = Vec::new();
        for addr in addrs {
            if self
                .delete_expired_commit_log(addr, time_hours)
                .await
                .is_err()
            {
                failed.push(addr.clone());
            }
        }
        failed
    }

    /// A **single request** CLEAN_UNUSED_TOPIC(316);
    /// the broker cleans up unused topics by itself.
    pub async fn clean_unused_topic_by_addr(&self, broker_addr: &str) -> Result<()> {
        self.invoke_broker(
            broker_addr,
            request_code::CLEAN_UNUSED_TOPIC,
            &ExtFields::new(),
            None,
            None,
        )
        .await?;
        Ok(())
    }

    /// `QUERY_CONSUME_TIME_SPAN(303)`:
    /// walks the masters along the route and aggregates each broker's consumeTimeSpanSet.
    pub async fn query_consume_time_span(&self, topic: &str, group: &str) -> Result<Vec<Value>> {
        let route = self.examine_topic_route(topic).await?;
        let mut spans: Vec<Value> = Vec::new();
        for bd in &route.broker_datas {
            let Some(addr) = bd.select_broker_addr() else {
                continue;
            };
            let ext = ext_pairs(&[("topic", topic.to_string()), ("group", group.to_string())]);
            let response = self
                .invoke_broker(
                    &addr,
                    request_code::QUERY_CONSUME_TIME_SPAN,
                    &ext,
                    None,
                    None,
                )
                .await?;
            if let Some(body) = response.body.as_deref().filter(|b| !b.is_empty()) {
                let parsed: Value = serde_json::from_slice(body)
                    .map_err(|e| Error::client(format!("decode QueryConsumeTimeSpanBody: {e}")))?;
                if let Some(set) = parsed.get("consumeTimeSpanSet").and_then(Value::as_array) {
                    spans.extend(set.iter().cloned());
                }
            }
        }
        Ok(spans)
    }

    // ---------------- NameServer config (318/319) ----------------

    /// `UPDATE_NAMESRV_CONFIG(318)`:
    /// the properties go into the body as k=v\n text, broadcast to every NameServer; any failure throws.
    pub async fn update_name_server_config(
        &self,
        properties: &StringMap,
        timeout_millis: Option<i64>,
    ) -> Result<()> {
        let text = MixAll::properties_to_string(properties, false);
        if text.is_empty() {
            return Ok(());
        }
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let mut err_response: Option<RemotingCommand> = None;
        for ns_addr in client.name_server_addrs() {
            let mut request = build_request(
                request_code::UPDATE_NAMESRV_CONFIG,
                &ExtFields::new(),
                Some(text.as_bytes().to_vec()),
            );
            let response = client.invoke_sync(&ns_addr, &mut request, timeout).await?;
            if response.code != response_code::SUCCESS {
                err_response = Some(response);
            }
        }
        if let Some(response) = err_response {
            return Err(Error::client_with_code(
                response.code,
                response
                    .remark
                    .unwrap_or_else(|| "update name server config failed".to_string()),
            ));
        }
        Ok(())
    }

    /// `GET_NAMESRV_CONFIG(319)`: queries each NameServer
    /// in turn; the body is properties text; returns {addr: properties}.
    pub async fn get_name_server_config(
        &self,
        timeout_millis: Option<i64>,
    ) -> Result<Vec<(String, StringMap)>> {
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let mut result: Vec<(String, StringMap)> = Vec::new();
        let mut last_err: Option<Error> = None;
        for ns_addr in client.name_server_addrs() {
            let mut request =
                build_request(request_code::GET_NAMESRV_CONFIG, &ExtFields::new(), None);
            match client.invoke_sync(&ns_addr, &mut request, timeout).await {
                Ok(response) if response.code == response_code::SUCCESS => {
                    let text = response
                        .body
                        .as_deref()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .unwrap_or_default();
                    result.push((ns_addr.clone(), MixAll::string_to_properties(&text)));
                }
                Ok(response) => {
                    last_err = Some(Error::client_with_code(
                        response.code,
                        response
                            .remark
                            .unwrap_or_else(|| "get name server config failed".to_string()),
                    ));
                }
                Err(e) => last_err = Some(e),
            }
        }
        if result.is_empty() {
            if let Some(e) = last_err {
                return Err(e);
            }
        }
        Ok(result)
    }

    /// `SET_MESSAGE_REQUEST_MODE(401)`.
    pub async fn set_message_request_mode(
        &self,
        broker_addr: &str,
        topic: &str,
        consumer_group: &str,
        mode: &str,
        pop_share_queue_num: i32,
    ) -> Result<()> {
        let mut pairs = vec![
            ("topic", topic.to_string()),
            ("consumerGroup", consumer_group.to_string()),
            ("mode", mode.to_string()),
        ];
        if pop_share_queue_num > 0 {
            pairs.push(("popShareQueueNum", pop_share_queue_num.to_string()));
        }
        let ext = ext_pairs(&pairs);
        self.invoke_broker(
            broker_addr,
            request_code::SET_MESSAGE_REQUEST_MODE,
            &ext,
            None,
            None,
        )
        .await?;
        Ok(())
    }
}

/// Takes the string array from the JSON body's `key`; a missing field counts as an empty table.
fn string_list(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message_decoder::{create_message_id, encode_message_ext, ip_and_port_to_bytes};
    use crate::remoting::protocol::admin_body::TopicOffset;
    use crate::remoting::protocol::body::CMResult;
    use crate::remoting::protocol::route::QueueData;

    fn cluster() -> ClusterInfo {
        ClusterInfo {
            broker_addr_table: vec![
                (
                    "broker-b".to_string(),
                    vec![
                        (1, "10.0.0.21:10911".to_string()),
                        (0, "10.0.0.20:10911".to_string()),
                    ],
                ),
                (
                    "broker-a".to_string(),
                    vec![(0, "10.0.0.10:10911".to_string())],
                ),
            ],
            cluster_addr_table: vec![
                (
                    "DefaultCluster".to_string(),
                    vec![
                        "broker-b".to_string(),
                        "broker-a".to_string(),
                        "broker-a".to_string(),
                    ],
                ),
                ("Other".to_string(), vec!["broker-c".to_string()]),
            ],
        }
    }

    #[test]
    fn config_defaults_match_golden() {
        let admin = DefaultMQAdminExt::new();
        let cfg = admin.config();
        assert_eq!(cfg.instance_name, "ADMIN");
        assert_eq!(cfg.timeout_millis, DEFAULT_TIMEOUT_MILLIS);
        assert_eq!(DEFAULT_TIMEOUT_MILLIS, 15_000);
        assert!(cfg.name_server_addrs.is_empty());
        assert!(cfg.kv_namespace_to_delete_list.is_empty());
        assert_eq!(cfg.namespace_v2, None, "namespaceV2 not set by default");
        assert!(!admin.is_started());
        assert_eq!(admin.client_id(), "");
        assert!(!cfg.vip_channel_enabled);
    }

    #[test]
    fn namespace_v2_setter_getter_round_trip() {
        let admin = DefaultMQAdminExt::new();
        assert_eq!(admin.get_namespace_v2(), None, "not set by default");
        admin.set_namespace_v2(Some("NS_V2"));
        assert_eq!(admin.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(admin.config().namespace_v2, Some("NS_V2".to_string()));
        admin.set_namespace_v2(None);
        assert_eq!(
            admin.get_namespace_v2(),
            None,
            "None = clears"
        );
    }

    /// VIP channel switch: takes effect after set (default false; together with mix_all's
    /// `broker_vip_channel` port-minus-2 test it covers the address resolution semantics).
    #[test]
    fn vip_channel_knob_defaults_off_and_toggles() {
        let admin = DefaultMQAdminExt::new();
        assert!(!admin.config().vip_channel_enabled);
        admin.set_vip_channel_enabled(true);
        assert!(admin.config().vip_channel_enabled);
        admin.set_vip_channel_enabled(false);
        assert!(!admin.config().vip_channel_enabled);
    }

    #[test]
    fn track_type_names_match_protocol_contract() {
        assert_eq!(TrackType::Consumed.as_str(), "CONSUMED");
        assert_eq!(
            TrackType::ConsumedButFiltered.as_str(),
            "CONSUMED_BUT_FILTERED"
        );
        assert_eq!(TrackType::Pull.as_str(), "PULL");
        assert_eq!(TrackType::NotConsumeYet.as_str(), "NOT_CONSUME_YET");
        assert_eq!(TrackType::NotOnline.as_str(), "NOT_ONLINE");
        assert_eq!(
            TrackType::ConsumeBroadcasting.as_str(),
            "CONSUME_BROADCASTING"
        );
        assert_eq!(TrackType::Unknown.as_str(), "UNKNOWN");
    }

    #[test]
    fn namesrv_addr_splits_trims_and_drops_empty() {
        // semicolon-separated; each item trimmed, empties dropped
        let admin = DefaultMQAdminExt::new();
        admin.set_namesrv_addr(" 127.0.0.1:9876 ; ;127.0.0.1:9877;");
        assert_eq!(
            admin.get_name_server_address_list(),
            vec!["127.0.0.1:9876".to_string(), "127.0.0.1:9877".to_string()]
        );
        assert_eq!(
            admin.get_name_server_addr(),
            "127.0.0.1:9876;127.0.0.1:9877"
        );
    }

    #[test]
    fn reset_offset_ext_key_is_isforce() {
        let ext = reset_offset_ext_pairs("T", "G", -1, true, None, None);
        assert_eq!(ext.get("isForce"), Some("true"));
        assert!(!ext.contains_key("force"), "force is not the server-side field name");
        assert_eq!(ext.get("timestamp"), Some("-1"));
        assert_eq!(ext.get("offset"), Some("-1"));
        assert!(
            !ext.contains_key("queueId"),
            "the whole-topic overload carries no queueId"
        );

        // Negative control: isForce=false must also be sent (no "omitting falsy values"); the single-queue overload carries queueId
        let single = reset_offset_ext_pairs("T", "G", 0, false, Some(3), Some(7));
        assert_eq!(single.get("isForce"), Some("false"));
        assert!(!single.contains_key("force"));
        assert_eq!(single.get("queueId"), Some("3"));
        assert_eq!(single.get("offset"), Some("7"));
        assert_eq!(single.get("timestamp"), Some("0"));
    }

    /// Without a name server configured, `start()` throws and does **not** stay started.
    #[tokio::test]
    async fn start_without_namesrv_rolls_back() {
        let admin = DefaultMQAdminExt::new();
        let err = admin.start().await.expect_err("no namesrv must fail");
        assert_eq!(
            err.to_string(),
            "MQClientException: name server address is not set"
        );
        assert_eq!(err.response_code(), None);
        assert!(!admin.is_started());
    }

    /// Every entry point without start must yield "admin not started", not a bare panic / connection error.
    #[tokio::test]
    async fn every_call_requires_start() {
        let admin = DefaultMQAdminExt::new();
        let mq = MessageQueue::new("T", "b", 0);
        let cases: Vec<(&str, Result<()>)> = vec![
            (
                "cluster",
                admin.fetch_broker_cluster_info().await.map(|_| ()),
            ),
            ("kv", admin.get_kv_config("ns", "k").await.map(|_| ())),
            ("maxOffset", admin.max_offset(&mq).await.map(|_| ())),
            ("route", admin.examine_topic_route("T").await.map(|_| ())),
            (
                "consumeStats",
                admin
                    .examine_consume_stats("a:1", "g", None, None)
                    .await
                    .map(|_| ()),
            ),
            (
                "createTopic",
                admin.create_topic("k", "T", 4, 0).await.map(|_| ()),
            ),
        ];
        for (name, result) in cases {
            let err = result.expect_err(&format!("{name} must fail when not started"));
            assert_eq!(
                err.to_string(),
                "MQClientException: admin not started, call start() first",
                "case {name}"
            );
            assert!(matches!(
                err,
                Error::Client {
                    response_code: None,
                    ..
                }
            ));
        }
        // Query entry points also require start (error outright when not started); they must not silently return empty sets.
        assert!(admin.query_message("T", "k", 32, 0, 1).await.is_err());
        assert!(admin.query_message_by_uniq_key("T", "k").await.is_err());
        assert!(admin.query_message_by_key("T", "k", 32).await.is_err());
    }

    /// An illegal brokerPermission must be reported **before any IO** (the check precedes the request),
    /// so an admin that has not started also gives 16.
    #[tokio::test]
    async fn update_broker_config_validates_perm_locally() {
        let admin = DefaultMQAdminExt::new();
        let mut props = StringMap::new();
        props.insert("brokerPermission", "not-a-number");
        let err = admin
            .update_broker_config("127.0.0.1:10911", &props, None)
            .await
            .expect_err("non-numeric perm must be rejected");
        assert_eq!(err.response_code(), Some(response_code::NO_PERMISSION));
        assert_eq!(
            err.to_string(),
            "MQClientException(code=16): brokerPermission value: not-a-number is invalid."
        );

        props.insert("brokerPermission", "99");
        let err = admin
            .update_broker_config("127.0.0.1:10911", &props, None)
            .await
            .expect_err("perm >= PERM_PRIORITY must be rejected");
        assert_eq!(err.response_code(), Some(response_code::NO_PERMISSION));

        // A legal value only fails at the IO ⇒ the check really runs first, and the legal value passes.
        props.insert("brokerPermission", "6");
        let err = admin
            .update_broker_config("127.0.0.1:10911", &props, None)
            .await
            .expect_err("not started yet");
        assert!(err.to_string().contains("admin not started"));

        // Empty properties: return outright -- no request, no error.
        assert!(admin
            .update_broker_config("127.0.0.1:10911", &StringMap::new(), None)
            .await
            .is_ok());
    }

    #[test]
    fn cluster_broker_addrs_covers_recorded_branches() {
        let info = cluster();
        // No cluster name: returns `cluster.get_broker_addrs()` (the full list).
        assert_eq!(
            cluster_broker_addrs(&info, None),
            info.get_broker_addrs(),
            "None must fall through to all brokers"
        );
        // Cluster name given: brokerNames sorted and deduplicated, each broker's addresses sorted as strings.
        assert_eq!(
            cluster_broker_addrs(&info, Some("DefaultCluster")),
            vec![
                "10.0.0.10:10911".to_string(),
                "10.0.0.20:10911".to_string(),
                "10.0.0.21:10911".to_string(),
            ]
        );
        // Unknown cluster names / empty strings both fall back to the full list.
        assert_eq!(
            cluster_broker_addrs(&info, Some("Nope")),
            info.get_broker_addrs()
        );
        assert_eq!(
            cluster_broker_addrs(&info, Some("")),
            info.get_broker_addrs()
        );
    }

    fn wrapper_with(names: &[&str]) -> TopicConfigSerializeWrapper {
        TopicConfigSerializeWrapper {
            topic_config_table: names
                .iter()
                .map(|n| ((*n).to_string(), json!({"topicName": *n})))
                .collect(),
            data_version: Value::Null,
        }
    }

    #[test]
    fn user_topic_config_drops_sys_retry_and_dlq() {
        // The filter is "broker-reported system topic list ∪ `MixAll::is_sys_topic`",
        // and is_sys_topic only recognizes the `rmq_sys_` prefix, so
        // TBW102 / SCHEDULE_TOPIC_XXXX can only be removed via GET_SYSTEM_TOPIC_LIST_FROM_BROKER.
        let sys = vec!["TBW102".to_string(), "SCHEDULE_TOPIC_XXXX".to_string()];
        let all = wrapper_with(&[
            "TBW102",
            "SCHEDULE_TOPIC_XXXX",
            "rmq_sys_wheel_timer",
            "%RETRY%G",
            "%DLQ%G",
            "BizTestTopic",
            "AnotherTopic",
        ]);

        let kept = filter_user_topic_configs(all.clone(), &sys, false)
            .topic_config_table
            .into_iter()
            .map(|(n, _)| n)
            .collect::<Vec<_>>();
        // Broker-reported entries, rmq_sys_-prefixed entries, and %RETRY% / %DLQ% are all removed; only business topics remain.
        assert_eq!(
            kept,
            vec!["BizTestTopic".to_string(), "AnotherTopic".to_string()]
        );

        let kept_special = filter_user_topic_configs(all, &sys, true)
            .topic_config_table
            .into_iter()
            .map(|(n, _)| n)
            .collect::<Vec<_>>();
        assert_eq!(
            kept_special,
            vec![
                "%RETRY%G".to_string(),
                "%DLQ%G".to_string(),
                "BizTestTopic".to_string(),
                "AnotherTopic".to_string()
            ]
        );

        // Bare names that are neither in the broker's system list nor carry the rmq_sys_ prefix are kept.
        let loose = filter_user_topic_configs(wrapper_with(&["SCHEDULE_TOPIC_XXXX"]), &[], false)
            .topic_config_table;
        assert!(loose.iter().any(|(n, _)| n == "SCHEDULE_TOPIC_XXXX"));
    }

    #[test]
    fn group_table_merge_overrides_in_place() {
        let mut table: Vec<(String, SubscriptionGroupConfig)> = Vec::new();
        merge_group_table(
            &mut table,
            vec![
                ("g1".to_string(), SubscriptionGroupConfig::new("g1")),
                ("g2".to_string(), SubscriptionGroupConfig::new("g2")),
            ],
        );
        let mut updated = SubscriptionGroupConfig::new("g1");
        updated.retry_max_times = 9;
        merge_group_table(&mut table, vec![("g1".to_string(), updated)]);
        // Same name overwrites; order keeps first appearance.
        assert_eq!(
            table.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            vec!["g1", "g2"]
        );
        assert_eq!(table[0].1.retry_max_times, 9);
    }

    #[test]
    fn json_map_merge_matches_dict_update() {
        let mut dst = json!({"a": 1});
        merge_json_map(&mut dst, json!({"a": 2, "b": 3}));
        assert_eq!(dst, json!({"a": 2, "b": 3}));
        // Non-object (old brokers return null) overwrites outright.
        merge_json_map(&mut dst, Value::Null);
        assert_eq!(dst, Value::Null);
    }

    #[test]
    fn topic_stats_merge_overrides_keys_and_sums_tps() {
        let key = MessageQueueKey::new("T", "broker-a", 0);
        let mut merged = TopicStatsTable::new();
        merged
            .offset_table
            .push((key.clone(), TopicOffset::new(0, 3, 0)));
        merged.topic_put_tps = 1.5;
        let mut part = TopicStatsTable::new();
        part.offset_table
            .push((key.clone(), TopicOffset::new(0, 7, 0)));
        part.offset_table.push((
            MessageQueueKey::new("T", "broker-b", 0),
            TopicOffset::new(0, 2, 0),
        ));
        part.topic_put_tps = 2.5;
        merge_topic_stats(&mut merged, part);
        assert_eq!(merged.offset_table.len(), 2);
        assert_eq!(merged.offset_table[0].1.max_offset, 7);
        assert!((merged.topic_put_tps - 4.0).abs() < f64::EPSILON);
        assert_eq!(merged.total_max_offset(), 9);
    }

    #[test]
    fn queues_of_broker_filters_by_broker_name() {
        let route = TopicRouteData {
            queue_datas: vec![
                QueueData::new("broker-a", 2, 2, 6, 0),
                QueueData::new("broker-b", 3, 3, 6, 0),
            ],
            ..Default::default()
        };
        let a: Vec<(String, i32)> = queues_of_broker(&route, "T", "broker-a")
            .into_iter()
            .map(|mq| (mq.broker_name, mq.queue_id))
            .collect();
        assert_eq!(
            a,
            vec![("broker-a".to_string(), 0), ("broker-a".to_string(), 1)]
        );
        assert!(queues_of_broker(&route, "T", "broker-c").is_empty());
    }

    #[test]
    fn small_helpers_match_golden_semantics() {
        // Explicit timeout wins; 0/missing falls back to the configured value
        assert_eq!(effective_timeout(15_000, Some(3_000)), 3_000);
        assert_eq!(effective_timeout(15_000, Some(0)), 15_000);
        assert_eq!(effective_timeout(15_000, None), 15_000);
        // Booleans become `"true"` / `"false"`
        assert_eq!((bool_flag(true), bool_flag(false)), ("true", "false"));

        // Missing fields and non-numbers are both 0
        let mut cmd = RemotingCommand::new();
        assert_eq!(ext_int(&cmd, "wipeTopicCount"), 0);
        cmd.add_ext_field("wipeTopicCount", " 12 ");
        assert_eq!(ext_int(&cmd, "wipeTopicCount"), 12);
        cmd.add_ext_field("addTopicCount", "abc");
        assert_eq!(ext_int(&cmd, "addTopicCount"), 0);

        // Only string elements are accepted; a missing field counts as an empty table
        assert_eq!(
            string_list(&json!({"groupList": ["a", 1, "b"]}), "groupList"),
            vec!["a".to_string(), "b".to_string()]
        );
        assert!(string_list(&json!({}), "topicList").is_empty());
    }

    #[tokio::test]
    async fn client_error_codes_are_documented() {
        let admin = DefaultMQAdminExt::new();
        let err = admin
            .examine_topic_route("T")
            .await
            .expect_err("not started");
        assert_eq!(err.response_code(), None);
        let view_err = admin
            .view_message("T", "not-hex")
            .await
            .expect_err("not started");
        // The uniqKey fallback sits outside the error swallowing: when not started, the client
        // error from `_require_client` passes through rather than NO_MESSAGE(208). 208 only
        // appears when a message is really not found; that assertion lives in the live_admin example (needs a real broker).
        assert_eq!(view_err.response_code(), None);
        assert!(view_err
            .to_string()
            .starts_with("MQClientException: admin not started"));
    }

    // ---------------- 309 consume_message_directly (mock-broker cross-check) ----------------

    /// Request log captured by the mock broker: `(request code, extFields snapshot)`.
    type DirectLog = Arc<Mutex<Vec<(i32, Vec<(String, String)>)>>>;

    async fn read_frame(stream: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
        use tokio::io::AsyncReadExt as _;
        let mut len_buf = [0_u8; 4];
        stream.read_exact(&mut len_buf).await.ok()?;
        let total = i32::from_be_bytes(len_buf);
        if total <= 0 || total > 20 * 1024 * 1024 {
            return None;
        }
        let mut frame = vec![0_u8; 4 + total as usize];
        frame[..4].copy_from_slice(&len_buf);
        stream.read_exact(&mut frame[4..]).await.ok()?;
        Some(frame)
    }

    /// A reply carrying `opaque` (the client pairs by opaque; mismatches are dropped as noise).
    fn reply_for(request: &RemotingCommand, code: i32, body: Option<Vec<u8>>) -> RemotingCommand {
        let mut response = RemotingCommand::create_response(code, None);
        response.opaque = request.opaque;
        response.serialize_type_current_rpc = request.serialize_type_current_rpc;
        if let Some(body) = body {
            response.set_body(Some(body));
        }
        response
    }

    /// Starts the 309 cross-check mock broker: `VIEW_MESSAGE_BY_ID(33)` replies with `view_body`,
    /// `CONSUME_MESSAGE_DIRECTLY(309)` replies per the scripted `(response code, body)` and logs the extFields,
    /// all other requests (route refresh after admin start etc.) get SUCCESS.
    async fn spawn_direct_broker(
        listener: tokio::net::TcpListener,
        view_body: Vec<u8>,
        direct: (i32, Vec<u8>),
    ) -> DirectLog {
        use tokio::io::AsyncWriteExt as _;
        let log: DirectLog = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::clone(&log);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let log = Arc::clone(&inner);
                let view_body = view_body.clone();
                let direct = direct.clone();
                tokio::spawn(async move {
                    while let Some(frame) = read_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else { return };
                        if matches!(
                            request.code,
                            request_code::VIEW_MESSAGE_BY_ID | request_code::CONSUME_MESSAGE_DIRECTLY
                        ) {
                            lock(&log).push((
                                request.code,
                                request
                                    .ext_fields()
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect(),
                            ));
                        }
                        let (code, body) = match request.code {
                            request_code::VIEW_MESSAGE_BY_ID => {
                                (response_code::SUCCESS, Some(view_body.clone()))
                            }
                            request_code::CONSUME_MESSAGE_DIRECTLY => {
                                (direct.0, Some(direct.1.clone()))
                            }
                            _ => (response_code::SUCCESS, None),
                        };
                        if request.is_oneway_rpc() {
                            continue;
                        }
                        let mut response = reply_for(&request, code, body);
                        if code != response_code::SUCCESS {
                            response.remark = Some(String::from_utf8_lossy(&direct.1).into_owned());
                            response.body = None;
                        }
                        let bytes = response.encode();
                        let _ = stream.write_all(&bytes).await;
                        let _ = stream.flush().await;
                    }
                });
            }
        });
        log
    }

    /// Builds a message with `storeHost = 127.0.0.1:store_port` and commitLog offset `offset`,
    /// returning `(the 33 response body, the offset msgId of the message itself)`.
    fn directly_view_body(store_port: u16, offset: i64, with_uniq_key: bool) -> (Vec<u8>, String) {
        let mut ext = MessageExt::new();
        ext.set_topic("T");
        ext.set_body(Some(b"directly consume me"));
        ext.sys_flag = 0;
        ext.born_timestamp = 1_700_000_000_000;
        ext.store_timestamp = 1_700_000_000_123;
        ext.set_born_host(Some("127.0.0.1"));
        ext.born_host_port = 54321;
        ext.set_store_host(Some("127.0.0.1"));
        ext.store_host_port = u32::from(store_port);
        ext.queue_id = 0;
        ext.queue_offset = 7;
        ext.commit_log_offset = offset;
        let mut props = StringMap::new();
        props.insert("TAGS", "TagA");
        if with_uniq_key {
            props.insert(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "UNIQKEY-0001");
        }
        ext.set_properties(props);
        (
            encode_message_ext(&ext, false).expect("encode fixture message"),
            create_message_id(
                &ip_and_port_to_bytes("127.0.0.1", u32::from(store_port), false).expect("fixture address"),
                offset,
            ),
        )
    }

    fn direct_result_body() -> Vec<u8> {
        ConsumeMessageDirectlyResult {
            consume_result: Some(CMResult::CR_SUCCESS.to_string()),
            remark: Some("ok".to_string()),
            spent_time_mills: 42,
            ..Default::default()
        }
        .encode()
    }

    async fn started_direct_admin(addr: &str) -> DefaultMQAdminExt {
        let admin = DefaultMQAdminExt::new();
        admin.set_namesrv_addr(addr);
        admin.start().await.expect("start against the mock broker should succeed");
        admin
    }

    fn direct_ext(log: &DirectLog, n: usize) -> Vec<(String, String)> {
        lock(log)[n]
            .1
            .clone()
    }

    /// End-to-end cross-check: msgId is a legal offset msgId → 33 fetches the message (with UNIQ_KEY) → 309 hits
    /// the message's storeHost and the header msgId is replaced by the **message's own** offset msgId → the body decodes back
    /// into [`ConsumeMessageDirectlyResult`].
    ///
    /// The fixture message's commitLog offset is deliberately (4096) different from the one encoded in the caller's msgId (1024):
    /// if 309 passed the caller's msgId through, the msgId assertion would catch it immediately.
    #[tokio::test]
    async fn consume_message_directly_sends_309_to_store_host_with_offset_msg_id() {
        let (listener, addr, port) = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock broker");
            let addr = listener.local_addr().expect("mock broker addr").to_string();
            let port = listener.local_addr().expect("mock broker port").port();
            (listener, addr, port)
        };
        let (view_body, offset_msg_id) = directly_view_body(port, 4096, true);
        let log = spawn_direct_broker(
            listener,
            view_body,
            (response_code::SUCCESS, direct_result_body()),
        )
        .await;
        let admin = started_direct_admin(&addr).await;

        let input_msg_id = create_message_id(
            &ip_and_port_to_bytes("127.0.0.1", u32::from(port), false).unwrap(),
            1024,
        );
        let result = admin
            .consume_message_directly("GID_A", "cid-1", "T", &input_msg_id)
            .await
            .expect("309 end-to-end should succeed");
        assert_eq!(result.consume_result.as_deref(), Some(CMResult::CR_SUCCESS));
        assert!(result.auto_commit, "autoCommit initial value true");
        assert_eq!(result.spent_time_mills, 42);
        assert_eq!(result.remark.as_deref(), Some("ok"));

        let snapshot = log.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(snapshot.len(), 2, "33 first, then 309, one each");
        assert_eq!(snapshot[0].0, request_code::VIEW_MESSAGE_BY_ID);
        assert_eq!(snapshot[1].0, request_code::CONSUME_MESSAGE_DIRECTLY);
        let ext = direct_ext(&log, 1);
        let get = |key: &str| {
            ext.iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(get("consumerGroup"), Some("GID_A"));
        assert_eq!(get("clientId"), Some("cid-1"));
        assert_eq!(get("topic"), Some("T"));
        assert_eq!(
            get("msgId"),
            Some(offset_msg_id.as_str()),
            "a message with UNIQ_KEY must be swapped to the message's own offset msgId"
        );
        drop(ext);
        admin.shutdown();
    }

    #[tokio::test]
    async fn consume_message_directly_without_uniq_key_passes_msg_id_through() {
        let (listener, addr, port) = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock broker");
            let addr = listener.local_addr().expect("mock broker addr").to_string();
            let port = listener.local_addr().expect("mock broker port").port();
            (listener, addr, port)
        };
        let (view_body, msg_id) = directly_view_body(port, 1024, false);
        let log = spawn_direct_broker(
            listener,
            view_body,
            (response_code::SUCCESS, direct_result_body()),
        )
        .await;
        let admin = started_direct_admin(&addr).await;

        admin
            .consume_message_directly("GID_A", "cid-2", "T", &msg_id)
            .await
            .expect("no UNIQ_KEY should succeed just the same");
        let ext = direct_ext(&log, 1);
        let get = |key: &str| ext.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
        assert_eq!(
            get("msgId"),
            Some(msg_id.as_str()),
            "without UNIQ_KEY, passed through verbatim"
        );
        drop(ext);
        admin.shutdown();
    }

    /// The 309 broker business code enters no retry chain: it is thrown as-is with its code (`Error::Broker`).
    #[tokio::test]
    async fn consume_message_directly_propagates_broker_error() {
        let (listener, addr, port) = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock broker");
            let addr = listener.local_addr().expect("mock broker addr").to_string();
            let port = listener.local_addr().expect("mock broker port").port();
            (listener, addr, port)
        };
        let (view_body, msg_id) = directly_view_body(port, 1024, true);
        let log = spawn_direct_broker(
            listener,
            view_body,
            (response_code::SYSTEM_ERROR, b"no consumer".to_vec()),
        )
        .await;
        let admin = started_direct_admin(&addr).await;

        let err = admin
            .consume_message_directly("GID_A", "cid-1", "T", &msg_id)
            .await
            .expect_err("the broker business code must be thrown as-is");
        assert_eq!(err.response_code(), Some(response_code::SYSTEM_ERROR));
        assert!(err.to_string().contains("no consumer"), "{err}");
        assert_eq!(lock(&log).len(), 2);
        admin.shutdown();
    }

    #[tokio::test]
    async fn consume_message_directly_reports_missing_result_body() {
        let (listener, addr, port) = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock broker");
            let addr = listener.local_addr().expect("mock broker addr").to_string();
            let port = listener.local_addr().expect("mock broker port").port();
            (listener, addr, port)
        };
        let (view_body, msg_id) = directly_view_body(port, 1024, true);
        let log = spawn_direct_broker(listener, view_body, (response_code::SUCCESS, Vec::new()))
            .await;
        let admin = started_direct_admin(&addr).await;

        let err = admin
            .consume_message_directly("GID_A", "cid-1", "T", &msg_id)
            .await
            .expect_err("an empty body must raise an error");
        assert_eq!(err.response_code(), None);
        assert!(
            err.to_string().contains("no consume result for client cid-1"),
            "{err}"
        );
        assert_eq!(lock(&log).len(), 2);
        admin.shutdown();
    }

    #[tokio::test]
    async fn consume_message_directly_requires_start() {
        let admin = DefaultMQAdminExt::new();
        let err = admin
            .consume_message_directly("G", "c", "T", "0A00000100002A9F0000000000000400")
            .await
            .expect_err("not started");
        assert!(err
            .to_string()
            .starts_with("MQClientException: admin not started"));
    }
}
