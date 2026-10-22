//! Coverage: topic create/delete/query and config, broker cluster info / runtime info / config,
//! name server KV config, subscription group management, consumer and producer connections,
//! consume stats, offset management (including real broker-side reset), message query (key / uniqKey / msgId / ConsumeQueue),
//! and trial consumption of a single message (the initiator side of CONSUME_MESSAGE_DIRECTLY(309)),
//! and auth user / ACL management (the 3001-3010 admin RPCs).

use std::collections::HashMap;
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
    BrokerReplicasInfo, CheckRocksdbCqWriteResult, ConsumeStats, MessageQueueKey, OffsetWrapper,
    QueryConsumeQueueResponseBody, TopicConfigSerializeWrapper, TopicStatsTable,
};
use crate::remoting::protocol::auth_body::{
    acl_from_body, acls_from_body, user_from_body, users_from_body, Acl, AuthUser,
};
use crate::remoting::protocol::body::{
    ClusterInfo, ConsumeMessageDirectlyResult, ConsumeStatsList, ConsumerConnection,
    ConsumerRunningInfo, GetConsumerListByGroupResponseBody, KVTable, ProducerConnection,
    ResetOffsetBody, TopicList,
};
use crate::remoting::protocol::codes::{language_code, request_code, response_code};
use crate::remoting::protocol::ext_fields::{ExtFields, StringMap};
use crate::remoting::protocol::remoting_command::RemotingCommand;
use crate::remoting::protocol::route::{QueueData, TopicRouteData};
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
/// Default page size for the metadata paging calls (`maxTopicNum` / `maxGroupNum`).
pub const DEFAULT_MAX_PAGE_SIZE_IN_GET_METADATA: i32 = 2000;
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
    /// Page size sent as `maxTopicNum` / `maxGroupNum` when paging metadata (default 2000).
    pub max_page_size_in_get_metadata: i32,
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
            max_page_size_in_get_metadata: DEFAULT_MAX_PAGE_SIZE_IN_GET_METADATA,
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

/// Merges the paged topic config tables: same name overwrites, first-appearance order kept.
fn merge_topic_config_table(
    table: &mut Vec<(String, Value)>,
    part: Vec<(String, Value)>,
) {
    for (name, config) in part {
        match table.iter_mut().find(|(n, _)| *n == name) {
            Some(entry) => entry.1 = config,
            None => table.push((name, config)),
        }
    }
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

// ---------------------------------------------------------------- admin long-tail constants & DTO

/// The container add/remove RPCs use a fixed 20s timeout, independent of the admin config.
pub const ADMIN_CONTAINER_TIMEOUT_MILLIS: i64 = 20_000;

/// Every controller-side admin RPC is hardcoded to 3s.
pub const CONTROLLER_RPC_TIMEOUT_MILLIS: i64 = 3_000;

/// The single ext key of `SET_COMMITLOG_READ_MODE(2004)` (the request has no header class).
pub const READ_AHEAD_MODE_EXT_KEY: &str = "READ_AHEAD_MODE";

/// `%RETRY%<group>_<topic>`: the pop retry topic of a group/topic pair.
fn build_pop_retry_topic(topic: &str, consumer_group: &str) -> String {
    format!("{}{}_{}", MixAll::RETRY_GROUP_TOPIC_PREFIX, consumer_group, topic)
}

/// The config categories of `EXPORT_ROCKSDB_CONFIG_TO_JSON(355)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocksdbConfigType {
    Topics,
    SubscriptionGroups,
    ConsumerOffsets,
}

impl RocksdbConfigType {
    pub fn type_name(&self) -> &'static str {
        match self {
            RocksdbConfigType::Topics => "topics",
            RocksdbConfigType::SubscriptionGroups => "subscriptionGroups",
            RocksdbConfigType::ConsumerOffsets => "consumerOffsets",
        }
    }
}

/// The response header of the controller metadata query (decoded from the response extFields).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ControllerMetaData {
    pub group: Option<String>,
    pub controller_leader_id: Option<String>,
    pub controller_leader_address: Option<String>,
    pub is_leader: bool,
    pub peers: Option<String>,
}

/// Verdict codes of the admin tool results; the numbers are the protocol enum values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminToolsResultCode {
    Success,
    RemotingError,
    MQBrokerError,
    MQClientError,
    InterruptError,
    TopicRouteInfoNotExist,
    ConsumerNotOnline,
    BroadcastConsumption,
}

impl AdminToolsResultCode {
    pub fn code(&self) -> i32 {
        match self {
            AdminToolsResultCode::Success => 200,
            AdminToolsResultCode::RemotingError => -1001,
            AdminToolsResultCode::MQBrokerError => -1002,
            AdminToolsResultCode::MQClientError => -1003,
            AdminToolsResultCode::InterruptError => -1004,
            AdminToolsResultCode::TopicRouteInfoNotExist => -2001,
            AdminToolsResultCode::ConsumerNotOnline => -2002,
            AdminToolsResultCode::BroadcastConsumption => -2003,
        }
    }
}

/// Wrapped outcome of the concurrent fan-out admin calls.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminToolResult<T> {
    pub success: bool,
    pub code: i32,
    pub error_msg: Option<String>,
    pub data: Option<T>,
}

impl<T> AdminToolResult<T> {
    pub fn success(data: T) -> AdminToolResult<T> {
        AdminToolResult {
            success: true,
            code: AdminToolsResultCode::Success.code(),
            error_msg: Some("success".to_string()),
            data: Some(data),
        }
    }

    pub fn failure(code: AdminToolsResultCode, error_msg: impl Into<String>) -> AdminToolResult<T> {
        AdminToolResult {
            success: false,
            code: code.code(),
            error_msg: Some(error_msg.into()),
            data: None,
        }
    }

    pub fn failure_with(
        code: AdminToolsResultCode,
        error_msg: impl Into<String>,
        data: T,
    ) -> AdminToolResult<T> {
        AdminToolResult {
            success: false,
            code: code.code(),
            error_msg: Some(error_msg.into()),
            data: Some(data),
        }
    }
}

/// Per-address outcome of a fanned-out broker operator (delete topic / reset offset).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BrokerOperatorResult {
    pub success_list: Vec<String>,
    pub failure_list: Vec<String>,
}

/// Error verdict mapping of the fan-out wrapper: transport failures are remoting errors, a
/// missing route is its own code, and everything else falls into the broker / client buckets.
fn admin_tools_code_of(error: &Error) -> AdminToolsResultCode {
    match error {
        Error::Connect { .. }
        | Error::SendRequest { .. }
        | Error::Timeout { .. }
        | Error::RemotingCommand(_)
        | Error::TooMuchRequest(_)
        | Error::Io(_)
        | Error::RequestTimeout { .. } => AdminToolsResultCode::RemotingError,
        Error::Client { response_code, .. } => match response_code {
            Some(code) if *code == response_code::TOPIC_NOT_EXIST => {
                AdminToolsResultCode::TopicRouteInfoNotExist
            }
            _ => AdminToolsResultCode::MQClientError,
        },
        _ => AdminToolsResultCode::MQBrokerError,
    }
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

    /// Page size used by the topic-config and subscription-group paging calls.
    pub fn set_max_page_size_in_get_metadata(&self, page_size: i32) {
        self.update_config(|c| c.max_page_size_in_get_metadata = page_size.max(1));
    }

    pub fn max_page_size_in_get_metadata(&self) -> i32 {
        read_cfg(&self.inner.cfg).max_page_size_in_get_metadata
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

    /// The same round trip on the address the caller passed, with no fast-channel rewrite.
    /// Reading a broker's own configuration is the archetype: it answers on the listening port
    /// only, so aiming it at the fast channel would talk to a port that is closed whenever the
    /// channel is off. Any RPC that must reach exactly the address it was handed uses this.
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
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let page_size = read_cfg(&self.inner.cfg).max_page_size_in_get_metadata.max(1);
        let began = Instant::now();
        let mut current_version: Option<Value> = None;
        let mut topic_seq: i32 = 0;
        let mut table: Vec<(String, Value)> = Vec::new();
        loop {
            let elapsed = i64::try_from(began.elapsed().as_millis()).unwrap_or(i64::MAX);
            let left = timeout - elapsed;
            if left < 0 {
                bail!("invokeSync call timeout");
            }
            let mut ext = ExtFields::new();
            ext.insert("topicSeq", topic_seq.to_string());
            ext.insert("maxTopicNum", page_size.to_string());
            ext.insert(
                "dataVersion",
                current_version
                    .as_ref()
                    .map(RemotingSerializable::to_json_string)
                    .unwrap_or_default(),
            );
            let mut request = build_request(request_code::GET_ALL_TOPIC_CONFIG, &ext, None);
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
                Some(body) => TopicConfigSerializeWrapper::decode(body)?,
                None => TopicConfigSerializeWrapper::new(),
            };
            let new_version = wrapper.data_version.clone();
            let page_len = i32::try_from(wrapper.topic_config_table.len()).unwrap_or(i32::MAX);
            merge_topic_config_table(&mut table, wrapper.topic_config_table);
            let Some(total) = response
                .get_ext_field("totalTopicNum")
                .and_then(|v| v.trim().parse::<i32>().ok())
            else {
                current_version = Some(new_version);
                break;
            };
            if current_version.is_none() {
                current_version = Some(new_version.clone());
            } else if current_version.as_ref() != Some(&new_version) {
                rmq_warn!("topic config dataVersion changed, restart paging");
                current_version = Some(new_version);
                topic_seq = 0;
                table.clear();
                continue;
            }
            // The cursor walks by what the broker actually returned, not by the requested
            // page size: a short page would otherwise skip past the next page's start.
            topic_seq += page_len;
            if topic_seq >= total - 1 {
                break;
            }
        }
        Ok(TopicConfigSerializeWrapper {
            topic_config_table: table,
            data_version: current_version.unwrap_or(Value::Null),
        })
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
        let page_size = read_cfg(&self.inner.cfg).max_page_size_in_get_metadata.max(1);
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
            ext.insert("maxGroupNum", page_size.to_string());
            ext.insert(
                "dataVersion",
                current_version
                    .as_ref()
                    .map(RemotingSerializable::to_json_string)
                    .unwrap_or_default(),
            );
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
            let new_version = wrapper.data_version.clone();
            let page_len = i32::try_from(wrapper.subscription_group_table.len()).unwrap_or(i32::MAX);
            merge_group_table(&mut table, wrapper.subscription_group_table);
            merge_json_map(&mut forbidden, wrapper.forbidden_table);
            if current_version.is_none() {
                current_version = Some(new_version.clone());
            }
            group_seq += page_len;

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
        let broker_addr = self.first_track_broker_addr(&msg.topic).await?;
        let mut groups = self
            .query_topic_consume_by_who(&broker_addr, &msg.topic)
            .await?;
        groups.sort();
        let mut result: Vec<MessageTrack> = Vec::new();
        for group in groups {
            result.push(self.track_one_group(msg, &group).await);
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
            spans.extend(
                self.query_consume_time_span_by_broker(&addr, topic, group)
                    .await?,
            );
        }
        Ok(spans)
    }

    /// One broker's `QUERY_CONSUME_TIME_SPAN(303)`: the entries of `consumeTimeSpanSet`.
    pub async fn query_consume_time_span_by_broker(
        &self,
        broker_addr: &str,
        topic: &str,
        group: &str,
    ) -> Result<Vec<Value>> {
        let ext = ext_pairs(&[("topic", topic.to_string()), ("group", group.to_string())]);
        let response = self
            .invoke_broker(
                broker_addr,
                request_code::QUERY_CONSUME_TIME_SPAN,
                &ext,
                None,
                None,
            )
            .await?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => {
                let parsed: Value = serde_json::from_slice(body)
                    .map_err(|e| Error::client(format!("decode QueryConsumeTimeSpanBody: {e}")))?;
                Ok(parsed
                    .get("consumeTimeSpanSet")
                    .and_then(Value::as_array)
                    .map(|set| set.to_vec())
                    .unwrap_or_default())
            }
            None => Ok(Vec::new()),
        }
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
    ///
    /// Wire fact: the broker reads this request's **entire** payload out of the JSON body, as an
    /// object with the fields `topic`, `consumerGroup`, `mode` and `popShareQueueNum`; it looks at
    /// no custom header at all. A request that carries those values as ext fields leaves the broker
    /// with no body to decode, so it fails before reaching the mode table, and the mode never
    /// switches. Every value therefore rides the body here and the header stays empty.
    ///
    /// `popShareQueueNum` is always present as a JSON number, zero included: the body is decoded
    /// into an object, so a value the caller never asked to omit is still the value the broker
    /// applies, and dropping the key at 0 would leave the broker's own default in charge.
    pub async fn set_message_request_mode(
        &self,
        broker_addr: &str,
        topic: &str,
        consumer_group: &str,
        mode: &str,
        pop_share_queue_num: i32,
    ) -> Result<()> {
        let body = serde_json::to_vec(&json!({
            "topic": topic,
            "consumerGroup": consumer_group,
            // The caller's mode verbatim; an unset or unrecognised one is the broker's to reject.
            "mode": mode,
            "popShareQueueNum": pop_share_queue_num,
        }))
        .map_err(|e| Error::client(format!("encode message request mode: {e}")))?;
        self.invoke_broker(
            broker_addr,
            request_code::SET_MESSAGE_REQUEST_MODE,
            &ExtFields::new(),
            Some(body),
            None,
        )
        .await?;
        Ok(())
    }

    // ---------------- Auth user / ACL management (3001-3010) ----------------

    /// `AUTH_CREATE_USER(3001)` with the three-value short form.
    ///
    /// Wire fact: no `userStatus` key is sent at all — a key carrying null would ask the broker
    /// to clear a status the caller never mentioned.
    pub async fn create_user(
        &self,
        broker_addr: &str,
        username: &str,
        password: &str,
        user_type: &str,
    ) -> Result<()> {
        let mut user = AuthUser::new(username);
        user.password = Some(password.to_string());
        user.user_type = Some(user_type.to_string());
        self.create_user_with_config(broker_addr, &user).await
    }

    /// `AUTH_CREATE_USER(3001)`: the header carries the name, the body carries the whole record.
    pub async fn create_user_with_config(
        &self,
        broker_addr: &str,
        user: &AuthUser,
    ) -> Result<()> {
        self.write_user(broker_addr, request_code::AUTH_CREATE_USER, user)
            .await
    }

    /// `AUTH_UPDATE_USER(3002)` with the three-value short form; unset keys stay unset.
    pub async fn update_user(
        &self,
        broker_addr: &str,
        username: &str,
        password: &str,
        user_type: &str,
    ) -> Result<()> {
        let mut user = AuthUser::new(username);
        user.password = Some(password.to_string());
        user.user_type = Some(user_type.to_string());
        self.update_user_with_config(broker_addr, &user).await
    }

    /// `AUTH_UPDATE_USER(3002)`: same shape as 3001, and the same omission rule.
    pub async fn update_user_with_config(
        &self,
        broker_addr: &str,
        user: &AuthUser,
    ) -> Result<()> {
        self.write_user(broker_addr, request_code::AUTH_UPDATE_USER, user)
            .await
    }

    /// `AUTH_DELETE_USER(3003)`: header-only, and the reply body is never read.
    pub async fn delete_user(&self, broker_addr: &str, username: &str) -> Result<()> {
        let ext = ext_pairs(&[("username", username.to_string())]);
        self.invoke_broker_plain(broker_addr, request_code::AUTH_DELETE_USER, &ext, None, None)
            .await?;
        Ok(())
    }

    /// `AUTH_GET_USER(3004)`: a missing or empty body reads back as `None`.
    pub async fn get_user(&self, broker_addr: &str, username: &str) -> Result<Option<AuthUser>> {
        let ext = ext_pairs(&[("username", username.to_string())]);
        let response = self
            .invoke_broker_plain(broker_addr, request_code::AUTH_GET_USER, &ext, None, None)
            .await?;
        user_from_body(response.body.as_deref())
    }

    /// `AUTH_LIST_USER(3005)`: `filter=None` sends no filter header at all rather than an empty one.
    pub async fn list_user(&self, broker_addr: &str, filter: Option<&str>) -> Result<Vec<AuthUser>> {
        let ext = match filter {
            Some(v) => ext_pairs(&[("filter", v.to_string())]),
            None => ExtFields::new(),
        };
        let response = self
            .invoke_broker_plain(broker_addr, request_code::AUTH_LIST_USER, &ext, None, None)
            .await?;
        users_from_body(response.body.as_deref())
    }

    /// `AUTH_CREATE_ACL(3006)` with the per-resource short form: one policy carrying no
    /// `policyType`, and one entry per resource repeating the shared values.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_acl(
        &self,
        broker_addr: &str,
        subject: &str,
        resources: &[&str],
        actions: &[&str],
        source_ips: &[&str],
        decision: &str,
    ) -> Result<()> {
        let acl = Acl::for_resources(subject, resources, actions, source_ips, decision);
        self.create_acl_with_config(broker_addr, &acl).await
    }

    /// `AUTH_CREATE_ACL(3006)`: the header carries the subject, the body carries the policies.
    pub async fn create_acl_with_config(&self, broker_addr: &str, acl: &Acl) -> Result<()> {
        self.write_acl(broker_addr, request_code::AUTH_CREATE_ACL, acl)
            .await
    }

    /// `AUTH_UPDATE_ACL(3007)` with the per-resource short form.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_acl(
        &self,
        broker_addr: &str,
        subject: &str,
        resources: &[&str],
        actions: &[&str],
        source_ips: &[&str],
        decision: &str,
    ) -> Result<()> {
        let acl = Acl::for_resources(subject, resources, actions, source_ips, decision);
        self.update_acl_with_config(broker_addr, &acl).await
    }

    /// `AUTH_UPDATE_ACL(3007)`: same shape as 3006.
    pub async fn update_acl_with_config(&self, broker_addr: &str, acl: &Acl) -> Result<()> {
        self.write_acl(broker_addr, request_code::AUTH_UPDATE_ACL, acl)
            .await
    }

    /// `AUTH_DELETE_ACL(3008)`: the header is exactly `subject` + `resource`, and there is no body.
    pub async fn delete_acl(&self, broker_addr: &str, subject: &str, resource: &str) -> Result<()> {
        let ext = ext_pairs(&[
            ("subject", subject.to_string()),
            ("resource", resource.to_string()),
        ]);
        self.invoke_broker_plain(broker_addr, request_code::AUTH_DELETE_ACL, &ext, None, None)
            .await?;
        Ok(())
    }

    /// `AUTH_GET_ACL(3009)`: a missing or empty body reads back as `None`.
    pub async fn get_acl(&self, broker_addr: &str, subject: &str) -> Result<Option<Acl>> {
        let ext = ext_pairs(&[("subject", subject.to_string())]);
        let response = self
            .invoke_broker_plain(broker_addr, request_code::AUTH_GET_ACL, &ext, None, None)
            .await?;
        acl_from_body(response.body.as_deref())
    }

    /// `AUTH_LIST_ACL(3010)`: either filter is sent only when the caller supplies it.
    pub async fn list_acl(
        &self,
        broker_addr: &str,
        subject_filter: Option<&str>,
        resource_filter: Option<&str>,
    ) -> Result<Vec<Acl>> {
        let mut pairs = Vec::new();
        if let Some(v) = subject_filter {
            pairs.push(("subjectFilter", v.to_string()));
        }
        if let Some(v) = resource_filter {
            pairs.push(("resourceFilter", v.to_string()));
        }
        let ext = ext_pairs(&pairs);
        let response = self
            .invoke_broker_plain(broker_addr, request_code::AUTH_LIST_ACL, &ext, None, None)
            .await?;
        acls_from_body(response.body.as_deref())
    }

    /// 3001 / 3002 are the same round trip under a different code.
    async fn write_user(&self, broker_addr: &str, code: i32, user: &AuthUser) -> Result<()> {
        let ext = ext_pairs(&[("username", user.username.clone())]);
        self.invoke_broker_plain(broker_addr, code, &ext, Some(user.encode()), None)
            .await?;
        Ok(())
    }

    /// 3006 / 3007 are the same round trip under a different code.
    async fn write_acl(&self, broker_addr: &str, code: i32, acl: &Acl) -> Result<()> {
        let ext = ext_pairs(&[("subject", acl.subject.clone())]);
        self.invoke_broker_plain(broker_addr, code, &ext, Some(acl.encode()), None)
            .await?;
        Ok(())
    }

    // ---------------- Broker container ----------------

    /// `ADD_BROKER(902)`: registers a broker config file into a running broker container.
    /// The container address is used exactly as passed (no fast-channel rewrite), the timeout is
    /// a fixed 20s, and the only payload is the header `configPath`.
    pub async fn add_broker_to_container(
        &self,
        broker_container_addr: &str,
        broker_config_path: &str,
    ) -> Result<()> {
        let ext = ext_pairs(&[("configPath", broker_config_path.to_string())]);
        let client = self.require_client()?;
        let mut request = build_request(request_code::ADD_BROKER, &ext, None);
        let response = client
            .invoke_sync(
                broker_container_addr,
                &mut request,
                ADMIN_CONTAINER_TIMEOUT_MILLIS,
            )
            .await?;
        MQClientInstance::check_response(&response)?;
        Ok(())
    }

    /// `REMOVE_BROKER(903)`: counterpart of the add; the header names
    /// brokerName / brokerClusterName / brokerId.
    pub async fn remove_broker_from_container(
        &self,
        broker_container_addr: &str,
        cluster_name: &str,
        broker_name: &str,
        broker_id: i64,
    ) -> Result<()> {
        let ext = ext_pairs(&[
            ("brokerName", broker_name.to_string()),
            ("brokerClusterName", cluster_name.to_string()),
            ("brokerId", broker_id.to_string()),
        ]);
        let client = self.require_client()?;
        let mut request = build_request(request_code::REMOVE_BROKER, &ext, None);
        let response = client
            .invoke_sync(
                broker_container_addr,
                &mut request,
                ADMIN_CONTAINER_TIMEOUT_MILLIS,
            )
            .await?;
        MQClientInstance::check_response(&response)?;
        Ok(())
    }

    // ---------------- Controller administration ----------------

    /// `CONTROLLER_GET_METADATA_INFO(1005)`: the response header names the current leader.
    pub async fn get_controller_metadata(
        &self,
        controller_addr: &str,
    ) -> Result<ControllerMetaData> {
        let client = self.require_client()?;
        let mut request = build_request(request_code::CONTROLLER_GET_METADATA_INFO, &ExtFields::new(), None);
        let response = client
            .invoke_sync(controller_addr, &mut request, CONTROLLER_RPC_TIMEOUT_MILLIS)
            .await?;
        MQClientInstance::check_response(&response)?;
        Ok(ControllerMetaData {
            group: response.get_ext_field("group").map(str::to_string),
            controller_leader_id: response.get_ext_field("controllerLeaderId").map(str::to_string),
            controller_leader_address: response
                .get_ext_field("controllerLeaderAddress")
                .map(str::to_string),
            is_leader: response
                .get_ext_field("isLeader")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            peers: response.get_ext_field("peers").map(str::to_string),
        })
    }

    /// The leader address every controller-side write has to be re-routed to.
    async fn controller_leader_address(&self, controller_addr: &str) -> Result<String> {
        let meta = self.get_controller_metadata(controller_addr).await?;
        meta.controller_leader_address
            .filter(|a| !a.is_empty())
            .ok_or_else(|| {
                Error::client(format!(
                    "controller {controller_addr} did not report a leader address"
                ))
            })
    }

    /// `CONTROLLER_GET_SYNC_STATE_DATA(1006)`: a two-phase read. The first phase asks the given
    /// controller for its metadata, the second asks the **leader** for the sync state of the
    /// named brokers; the request body is a bare JSON array of broker names.
    pub async fn get_in_sync_state_data(
        &self,
        controller_addr: &str,
        brokers: &[String],
    ) -> Result<BrokerReplicasInfo> {
        let leader_addr = self.controller_leader_address(controller_addr).await?;
        let client = self.require_client()?;
        let body = serde_json::to_vec(
            &brokers
                .iter()
                .map(|name| Value::String(name.clone()))
                .collect::<Vec<_>>(),
        )
        .map_err(|e| Error::Encode(format!("encode broker list: {e}")))?;
        let mut request =
            build_request(request_code::CONTROLLER_GET_SYNC_STATE_DATA, &ExtFields::new(), Some(body));
        let response = client
            .invoke_sync(&leader_addr, &mut request, CONTROLLER_RPC_TIMEOUT_MILLIS)
            .await?;
        MQClientInstance::check_response(&response)?;
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => BrokerReplicasInfo::decode(body),
            None => Ok(BrokerReplicasInfo::new()),
        }
    }

    /// `GET_CONTROLLER_CONFIG(1010)`: polls every controller, the response body is `k=v` text.
    /// With no explicit servers the name server list is used; when even that is empty the call
    /// returns `None` without any IO.
    pub async fn get_controller_config(
        &self,
        controller_servers: &[String],
    ) -> Result<Option<Vec<(String, StringMap)>>> {
        // The empty-server early return happens before any client work: neither explicit
        // servers nor a name server list means no call at all, started or not.
        let fallback_servers = match self.require_client() {
            Ok(client) => client.name_server_addrs(),
            Err(_) => self.config().name_server_addrs,
        };
        let servers: &[String] = if controller_servers.is_empty() {
            &fallback_servers
        } else {
            controller_servers
        };
        if servers.is_empty() {
            return Ok(None);
        }
        let client = self.require_client()?;
        let mut config_map: Vec<(String, StringMap)> = Vec::new();
        for server in servers {
            let mut request =
                build_request(request_code::GET_CONTROLLER_CONFIG, &ExtFields::new(), None);
            let response = client
                .invoke_sync(server, &mut request, CONTROLLER_RPC_TIMEOUT_MILLIS)
                .await?;
            if response.code != response_code::SUCCESS {
                return Err(Error::client_with_code(
                    response.code,
                    response.remark.unwrap_or_default(),
                ));
            }
            let text = match response.body.as_deref() {
                Some(body) if !body.is_empty() => String::from_utf8_lossy(body).into_owned(),
                _ => String::new(),
            };
            config_map.push((server.clone(), MixAll::string_to_properties(&text)));
        }
        Ok(Some(config_map))
    }

    /// `UPDATE_CONTROLLER_CONFIG(1009)`: broadcasts the properties to every controller as `k=v`
    /// text. An empty property set or an empty server list is a silent no-op; the **last**
    /// non-SUCCESS response wins when several controllers fail.
    pub async fn update_controller_config(
        &self,
        properties: &StringMap,
        controllers: &[String],
    ) -> Result<()> {
        let text = MixAll::properties_to_string(properties, false);
        if text.is_empty() || controllers.is_empty() {
            return Ok(());
        }
        let client = self.require_client()?;
        let mut last_error: Option<RemotingCommand> = None;
        for controller in controllers {
            let mut request = build_request(
                request_code::UPDATE_CONTROLLER_CONFIG,
                &ExtFields::new(),
                Some(text.as_bytes().to_vec()),
            );
            let response = client
                .invoke_sync(controller, &mut request, CONTROLLER_RPC_TIMEOUT_MILLIS)
                .await?;
            if response.code != response_code::SUCCESS {
                last_error = Some(response);
            }
        }
        match last_error {
            None => Ok(()),
            Some(response) => Err(Error::client_with_code(
                response.code,
                response.remark.unwrap_or_default(),
            )),
        }
    }

    /// `CLEAN_BROKER_DATA(1011)`: routed to the controller **leader** like every controller write;
    /// the header always carries `invokeTime` and `isCleanLivingBroker`, the nullable fields are
    /// omitted when absent.
    pub async fn clean_controller_broker_data(
        &self,
        controller_addr: &str,
        cluster_name: Option<&str>,
        broker_name: &str,
        broker_controller_ids_to_clean: Option<&str>,
        is_clean_living_broker: bool,
    ) -> Result<()> {
        let leader_addr = self.controller_leader_address(controller_addr).await?;
        let mut pairs = vec![("brokerName", broker_name.to_string())];
        if let Some(cluster) = cluster_name {
            pairs.push(("clusterName", cluster.to_string()));
        }
        if let Some(ids) = broker_controller_ids_to_clean {
            pairs.push(("brokerControllerIdsToClean", ids.to_string()));
        }
        pairs.push(("isCleanLivingBroker", bool_flag(is_clean_living_broker).to_string()));
        pairs.push(("invokeTime", current_time_millis().to_string()));
        let ext = ext_pairs(&pairs);
        let client = self.require_client()?;
        let mut request = build_request(request_code::CLEAN_BROKER_DATA, &ext, None);
        let response = client
            .invoke_sync(&leader_addr, &mut request, CONTROLLER_RPC_TIMEOUT_MILLIS)
            .await?;
        MQClientInstance::check_response(&response)?;
        Ok(())
    }

    // ---------------- Cold data flow control ----------------

    /// `UPDATE_COLD_DATA_FLOW_CTR_CONFIG(2001)`: the properties go out as `k=v` text on the fast
    /// channel; an empty property set skips the RPC entirely.
    pub async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: &str,
        properties: &StringMap,
    ) -> Result<()> {
        let text = MixAll::properties_to_string(properties, false);
        if text.is_empty() {
            return Ok(());
        }
        self.invoke_broker(
            broker_addr,
            request_code::UPDATE_COLD_DATA_FLOW_CTR_CONFIG,
            &ExtFields::new(),
            Some(text.into_bytes()),
            None,
        )
        .await?;
        Ok(())
    }

    /// `REMOVE_COLD_DATA_FLOW_CTR_CONFIG(2002)`: the body is the plain group string, not JSON;
    /// an empty group skips the RPC.
    pub async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: &str,
        consumer_group: &str,
    ) -> Result<()> {
        if consumer_group.is_empty() {
            return Ok(());
        }
        self.invoke_broker(
            broker_addr,
            request_code::REMOVE_COLD_DATA_FLOW_CTR_CONFIG,
            &ExtFields::new(),
            Some(consumer_group.as_bytes().to_vec()),
            None,
        )
        .await?;
        Ok(())
    }

    /// `GET_COLD_DATA_FLOW_CTR_INFO(2003)`: answered on the listening port only; the raw body
    /// text is returned as-is (`None` when the broker sends none).
    pub async fn get_cold_data_flow_ctr_info(
        &self,
        broker_addr: &str,
    ) -> Result<Option<String>> {
        let response = self
            .invoke_broker_plain(
                broker_addr,
                request_code::GET_COLD_DATA_FLOW_CTR_INFO,
                &ExtFields::new(),
                None,
                None,
            )
            .await?;
        Ok(response
            .body
            .as_deref()
            .filter(|b| !b.is_empty())
            .map(|b| String::from_utf8_lossy(b).into_owned()))
    }

    // ---------------- CommitLog read-ahead ----------------

    /// `SET_COMMITLOG_READ_MODE(2004)`: no header class, so the single `READ_AHEAD_MODE` ext key
    /// is built by hand; no fast-channel rewrite. The response remark (not a body) is the result.
    pub async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: &str,
        mode: &str,
    ) -> Result<Option<String>> {
        let mut ext = ExtFields::new();
        ext.insert(READ_AHEAD_MODE_EXT_KEY, mode);
        let client = self.require_client()?;
        let mut request = build_request(request_code::SET_COMMITLOG_READ_MODE, &ext, None);
        let response = client
            .invoke_sync(broker_addr, &mut request, self.timeout_millis())
            .await?;
        MQClientInstance::check_response(&response)?;
        Ok(response.remark.filter(|r| !r.is_empty()))
    }

    // ---------------- Pop rollback / RocksDB ----------------

    /// `POP_ROLLBACK(200056)`: a bare request that makes the broker export its pop records. No
    /// header, no body, no fast channel; the caller's timeout is forwarded verbatim instead of
    /// the admin default.
    pub async fn export_pop_records(&self, broker_addr: &str, timeout_millis: i64) -> Result<()> {
        let client = self.require_client()?;
        let mut request = build_request(request_code::POP_ROLLBACK, &ExtFields::new(), None);
        let response = client
            .invoke_sync(broker_addr, &mut request, timeout_millis)
            .await?;
        MQClientInstance::check_response(&response)?;
        Ok(())
    }

    /// `CHECK_ROCKSDB_CQ_WRITE_PROGRESS(354)`: `checkStoreTime` is a primitive long and always
    /// sent, even when 0. Any non-SUCCESS is the broker's error.
    pub async fn check_rocksdb_cq_write_progress(
        &self,
        broker_addr: &str,
        topic: &str,
        check_store_time: i64,
    ) -> Result<CheckRocksdbCqWriteResult> {
        let ext = ext_pairs(&[
            ("topic", topic.to_string()),
            ("checkStoreTime", check_store_time.to_string()),
        ]);
        let response = self
            .invoke_broker_plain(
                broker_addr,
                request_code::CHECK_ROCKSDB_CQ_WRITE_PROGRESS,
                &ext,
                None,
                None,
            )
            .await?;
        let body = response
            .body
            .as_deref()
            .filter(|b| !b.is_empty())
            .ok_or_else(|| {
                Error::Broker {
                    response_code: response.code,
                    message: format!("empty check result for topic {topic}"),
                }
            })?;
        CheckRocksdbCqWriteResult::decode(body)
    }

    /// `EXPORT_ROCKSDB_CONFIG_TO_JSON(355)`: the config types join into one header value; the
    /// separator trails every entry (the broker's parser splits on it), so `topics` goes out as
    /// `"topics;"`. Code-only result: SUCCESS is the whole answer.
    pub async fn export_rocksdb_config_to_json(
        &self,
        broker_addr: &str,
        config_types: &[RocksdbConfigType],
    ) -> Result<()> {
        let mut config_type = String::new();
        for config in config_types {
            config_type.push_str(config.type_name());
            config_type.push(';');
        }
        let ext = ext_pairs(&[("configType", config_type)]);
        self.invoke_broker_plain(
            broker_addr,
            request_code::EXPORT_ROCKSDB_CONFIG_TO_JSON,
            &ext,
            None,
            None,
        )
        .await?;
        Ok(())
    }

    // ---------------- Lite (LMQ) administration ----------------

    /// Shared round trip of the 200074-200079 family: the fast channel serves all of them, the
    /// body (when the broker sent one) is returned decoded, and a non-SUCCESS response surfaces
    /// as the broker error with the target address attached.
    async fn invoke_broker_method(
        &self,
        addr: &str,
        code: i32,
        ext: &StringMap,
        timeout_millis: Option<i64>,
    ) -> Result<Option<Value>> {
        let target =
            MixAll::broker_vip_channel(read_cfg(&self.inner.cfg).vip_channel_enabled, addr);
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let mut request = build_request(code, ext, None);
        let response = client.invoke_sync(&target, &mut request, timeout).await?;
        if response.code != response_code::SUCCESS {
            return Err(Error::Broker {
                response_code: response.code,
                message: response.remark.unwrap_or_default(),
            });
        }
        match response.body.as_deref().filter(|b| !b.is_empty()) {
            Some(body) => Ok(Some(RemotingSerializable::decode(body)?)),
            None => Ok(None),
        }
    }

    /// `GET_BROKER_LITE_INFO(200074)`, no header.
    pub async fn get_broker_lite_info(&self, broker_addr: &str) -> Result<Option<Value>> {
        self.invoke_broker_method(
            broker_addr,
            request_code::GET_BROKER_LITE_INFO,
            &ExtFields::new(),
            None,
        )
        .await
    }

    /// `GET_PARENT_TOPIC_INFO(200075)` with header `topic`.
    pub async fn get_parent_topic_info(
        &self,
        broker_addr: &str,
        topic: &str,
    ) -> Result<Option<Value>> {
        let ext = ext_pairs(&[("topic", topic.to_string())]);
        self.invoke_broker_method(broker_addr, request_code::GET_PARENT_TOPIC_INFO, &ext, None)
            .await
    }

    /// `GET_LITE_TOPIC_INFO(200076)` with header `parentTopic` + optional `liteTopic`.
    pub async fn get_lite_topic_info(
        &self,
        broker_addr: &str,
        parent_topic: &str,
        lite_topic: Option<&str>,
    ) -> Result<Option<Value>> {
        let mut pairs = vec![("parentTopic", parent_topic.to_string())];
        if let Some(lite) = lite_topic {
            pairs.push(("liteTopic", lite.to_string()));
        }
        let ext = ext_pairs(&pairs);
        self.invoke_broker_method(broker_addr, request_code::GET_LITE_TOPIC_INFO, &ext, None)
            .await
    }

    /// `GET_LITE_CLIENT_INFO(200077)`: `maxCount` is a primitive int with a 1000 default and is
    /// therefore always on the wire.
    pub async fn get_lite_client_info(
        &self,
        broker_addr: &str,
        parent_topic: &str,
        group: &str,
        client_id: Option<&str>,
    ) -> Result<Option<Value>> {
        let mut pairs = vec![
            ("parentTopic", parent_topic.to_string()),
            ("group", group.to_string()),
        ];
        if let Some(client) = client_id {
            pairs.push(("clientId", client.to_string()));
        }
        pairs.push(("maxCount", "1000".to_string()));
        let ext = ext_pairs(&pairs);
        self.invoke_broker_method(broker_addr, request_code::GET_LITE_CLIENT_INFO, &ext, None)
            .await
    }

    /// `GET_LITE_GROUP_INFO(200078)`: `topK` is a primitive int and always sent.
    pub async fn get_lite_group_info(
        &self,
        broker_addr: &str,
        group: &str,
        lite_topic: Option<&str>,
        top_k: i32,
    ) -> Result<Option<Value>> {
        let mut pairs = vec![("group", group.to_string())];
        if let Some(lite) = lite_topic {
            pairs.push(("liteTopic", lite.to_string()));
        }
        pairs.push(("topK", top_k.to_string()));
        let ext = ext_pairs(&pairs);
        self.invoke_broker_method(broker_addr, request_code::GET_LITE_GROUP_INFO, &ext, None)
            .await
    }

    /// `TRIGGER_LITE_DISPATCH(200079)`, void result.
    pub async fn trigger_lite_dispatch(
        &self,
        broker_addr: &str,
        group: &str,
        client_id: Option<&str>,
    ) -> Result<()> {
        let mut pairs = vec![("group", group.to_string())];
        if let Some(client) = client_id {
            pairs.push(("clientId", client.to_string()));
        }
        let ext = ext_pairs(&pairs);
        self.invoke_broker_method(broker_addr, request_code::TRIGGER_LITE_DISPATCH, &ext, None)
            .await
            .map(|_| ())
    }

    // ---------------- Concurrent fan-out administration ----------------

    /// Runs `task` once per key on a cloned admin, all concurrently. Every join failure
    /// (a panicked task) is logged and skipped, never propagated. Results come back keyed
    /// by their key, in completion order.
    async fn fan_out<T, F, Fut>(&self, keys: Vec<String>, task: F) -> Vec<(String, T)>
    where
        F: Fn(DefaultMQAdminExt, String) -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let mut set = tokio::task::JoinSet::new();
        for key in keys {
            let admin = self.clone();
            let task = task.clone();
            set.spawn(async move {
                let out = task(admin, key.clone()).await;
                (key, out)
            });
        }
        let mut results = Vec::new();
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(pair) => results.push(pair),
                Err(e) => rmq_warn!("concurrent admin task failed: {e}"),
            }
        }
        results
    }

    /// The admin client, or the fan-out failure verdict for not being started.
    fn admin_tool_client(
        &self,
    ) -> std::result::Result<MQClientInstance, (AdminToolsResultCode, String)> {
        self.require_client()
            .map_err(|e| (admin_tools_code_of(&e), e.to_string()))
    }

    /// Per-broker `GET_TOPIC_STATS_INFO` in parallel: offset tables merge per key, the put
    /// TPS is summed, and any single broker failure is logged and skipped. A topic without
    /// route data is the `-2001` verdict; a route without broker data is a success with an
    /// empty table.
    pub async fn examine_topic_stats_concurrent(
        &self,
        topic: &str,
    ) -> AdminToolResult<TopicStatsTable> {
        let client = match self.admin_tool_client() {
            Ok(client) => client,
            Err((code, message)) => return AdminToolResult::failure(code, message),
        };
        let Some(route) = client.get_topic_route_data(topic).await else {
            return AdminToolResult::failure(
                AdminToolsResultCode::TopicRouteInfoNotExist,
                "topic router info not found",
            );
        };
        if route.get_broker_datas().is_empty() {
            return AdminToolResult::success(TopicStatsTable::new());
        }
        let addrs = route_masters(&route);
        let topic_owned = topic.to_string();
        let parts = self.fan_out(addrs, move |admin, addr| {
            let topic = topic_owned.clone();
            async move { admin.examine_topic_stats_by_broker(&addr, &topic).await }
        });
        let parts = parts.await;
        let mut merged = TopicStatsTable::new();
        for (addr, part) in parts {
            match part {
                Ok(part) => merge_topic_stats(&mut merged, part),
                Err(e) => rmq_warn!("getTopicStatsInfo error. topic={topic} broker={addr}: {e}"),
            }
        }
        AdminToolResult::success(merged)
    }

    /// Route discovery walks the retry topic, then the topic itself, then the pop retry
    /// topic, taking the first hit. The per-broker `GET_CONSUME_STATS` fan merges offset
    /// tables and sums the per-address TPS. An empty merged table probes the consumer
    /// connection: unreachable means the group never consumed (offline verdict), a
    /// broadcasting group gets its own verdict, and any other online group is a success
    /// with the (empty) stats.
    pub async fn examine_consume_stats_concurrent(
        &self,
        consumer_group: &str,
        topic: Option<&str>,
    ) -> AdminToolResult<ConsumeStats> {
        let client = match self.admin_tool_client() {
            Ok(client) => client,
            Err((code, message)) => return AdminToolResult::failure(code, message),
        };
        let mut route_candidates = vec![MixAll::get_retry_topic(consumer_group)];
        if let Some(topic) = topic.filter(|t| !t.is_empty()) {
            route_candidates.push(topic.to_string());
            route_candidates.push(build_pop_retry_topic(topic, consumer_group));
        }
        let mut found: Option<TopicRouteData> = None;
        for candidate in &route_candidates {
            if let Some(route) = client.get_topic_route_data(candidate).await {
                found = Some(route);
                break;
            }
        }
        let Some(route) = found else {
            return AdminToolResult::failure(
                AdminToolsResultCode::TopicRouteInfoNotExist,
                "topic router info not found",
            );
        };
        if route.get_broker_datas().is_empty() {
            return AdminToolResult::failure(
                AdminToolsResultCode::TopicRouteInfoNotExist,
                "topic router info not found",
            );
        }
        let addrs = route_masters(&route);
        let group_owned = consumer_group.to_string();
        let topic_owned = topic.filter(|t| !t.is_empty()).map(str::to_string);
        let parts = self
            .fan_out(addrs, move |admin, addr| {
                let group = group_owned.clone();
                let topic = topic_owned.clone();
                async move {
                    admin
                        .examine_consume_stats(&addr, &group, topic.as_deref(), None)
                        .await
                }
            })
            .await;
        let mut result = ConsumeStats::new();
        // One TPS slot per address: two route entries pointing at one address count once.
        let mut tps_by_addr: HashMap<String, f64> = HashMap::new();
        for (addr, part) in parts {
            match part {
                Ok(part) => {
                    for (key, wrapper) in part.offset_table {
                        match result.offset_table.iter_mut().find(|(k, _)| *k == key) {
                            Some(slot) => slot.1 = wrapper,
                            None => result.offset_table.push((key, wrapper)),
                        }
                    }
                    tps_by_addr.insert(addr, part.consume_tps);
                }
                Err(e) => rmq_warn!(
                    "getConsumeStats error. topic={topic:?} consumerGroup={consumer_group} broker={addr}: {e}"
                ),
            }
        }
        for tps in tps_by_addr.values() {
            result.consume_tps += tps;
        }
        if result.offset_table.is_empty() {
            const NOT_ONLINE_MSG: &str = "Not found the consumer group consume stats, because return offset table is empty, maybe the consumer not consume any message";
            const BROADCAST_MSG: &str = "Not found the consumer group consume stats, because return offset table is empty, the consumer is under the broadcast mode";
            return match self.examine_consumer_connection_info(consumer_group, None).await {
                Err(_) => {
                    AdminToolResult::failure(AdminToolsResultCode::ConsumerNotOnline, NOT_ONLINE_MSG)
                }
                Ok(cc) if cc.connection_set.is_empty() => {
                    AdminToolResult::failure(AdminToolsResultCode::ConsumerNotOnline, NOT_ONLINE_MSG)
                }
                Ok(cc) if cc.message_model.as_deref() == Some("BROADCASTING") => {
                    AdminToolResult::failure(
                        AdminToolsResultCode::BroadcastConsumption,
                        BROADCAST_MSG,
                    )
                }
                Ok(_) => AdminToolResult::success(result),
            };
        }
        AdminToolResult::success(result)
    }

    /// Fans `DELETE_TOPIC_IN_BROKER(215)` over the addresses the caller handed in
    /// (de-duplicated like the set on the wire side); every address lands in exactly one of
    /// the two lists, and the wrapper is always a success.
    pub async fn delete_topic_in_broker_concurrent(
        &self,
        addrs: &[String],
        topic: &str,
    ) -> AdminToolResult<BrokerOperatorResult> {
        if let Err((code, message)) = self.admin_tool_client() {
            return AdminToolResult::failure(code, message);
        }
        let mut targets: Vec<String> = Vec::new();
        for addr in addrs {
            if !targets.iter().any(|seen| seen == addr) {
                targets.push(addr.clone());
            }
        }
        let topic_owned = topic.to_string();
        let parts = self
            .fan_out(targets, move |admin, addr| {
                let topic = topic_owned.clone();
                async move { admin.delete_topic_in_broker(&addr, &topic).await }
            })
            .await;
        let mut result = BrokerOperatorResult::default();
        for (addr, outcome) in parts {
            match outcome {
                Ok(()) => result.success_list.push(addr),
                Err(e) => {
                    rmq_warn!("deleteTopicInBroker error. topic={topic} broker={addr}: {e}");
                    result.failure_list.push(addr);
                }
            }
        }
        AdminToolResult::success(result)
    }

    /// Per-broker `INVOKE_BROKER_TO_RESET_OFFSET(222)` in parallel. A group that never came
    /// online on that broker falls back to the consume-stats-driven old reset; a
    /// `SYSTEM_ERROR(1)` (the "never online" answer) counts as done. The wrapper succeeds
    /// only when every route broker succeeded, and carries both lists otherwise.
    pub async fn reset_offset_new_concurrent(
        &self,
        group: &str,
        topic: &str,
        timestamp: i64,
    ) -> AdminToolResult<BrokerOperatorResult> {
        let client = match self.admin_tool_client() {
            Ok(client) => client,
            Err((code, message)) => return AdminToolResult::failure(code, message),
        };
        let Some(route) = client.get_topic_route_data(topic).await else {
            return AdminToolResult::failure(
                AdminToolsResultCode::TopicRouteInfoNotExist,
                "topic router info not found",
            );
        };
        if route.get_broker_datas().is_empty() {
            return AdminToolResult::failure(
                AdminToolsResultCode::TopicRouteInfoNotExist,
                "topic router info not found",
            );
        }
        let total = route.get_broker_datas().len();
        let targets: Vec<(String, Option<QueueData>)> = route
            .get_broker_datas()
            .iter()
            .filter_map(|bd| {
                let addr = bd.select_broker_addr().filter(|a| !a.is_empty())?;
                let queue_data = route
                    .queue_datas
                    .iter()
                    .find(|qd| qd.broker_name == bd.broker_name)
                    .cloned();
                Some((addr, queue_data))
            })
            .collect();
        let group_owned = group.to_string();
        let topic_owned = topic.to_string();
        let addrs: Vec<String> = targets.iter().map(|(addr, _)| addr.clone()).collect();
        let targets_for_task = targets.clone();
        let parts = self
            .fan_out(addrs, move |admin, addr| {
                let group = group_owned.clone();
                let topic = topic_owned.clone();
                let queue_data = targets_for_task
                    .iter()
                    .find(|(target, _)| *target == addr)
                    .and_then(|(_, qd)| qd.clone());
                async move {
                    admin
                        .reset_offset_new_for_broker(&addr, queue_data.as_ref(), &group, &topic, timestamp)
                        .await
                }
            })
            .await;
        let mut result = BrokerOperatorResult::default();
        for (addr, ok) in parts {
            if ok {
                result.success_list.push(addr);
            } else {
                result.failure_list.push(addr);
            }
        }
        if result.success_list.len() == total {
            AdminToolResult::success(result)
        } else {
            AdminToolResult::failure_with(
                AdminToolsResultCode::MQBrokerError,
                "operator failure",
                result,
            )
        }
    }

    /// One broker's leg of the reset fan-out; `true` lands in the success list.
    async fn reset_offset_new_for_broker(
        &self,
        broker_addr: &str,
        queue_data: Option<&QueueData>,
        group: &str,
        topic: &str,
        timestamp: i64,
    ) -> bool {
        match self
            .invoke_broker_reset_offset(broker_addr, topic, group, timestamp, true, false, None, None)
            .await
        {
            Ok(_) => true,
            Err(e) if e.response_code() == Some(response_code::CONSUMER_NOT_ONLINE) => {
                match self
                    .reset_offset_old_for_broker(broker_addr, queue_data, group, topic, timestamp, true)
                    .await
                {
                    Ok(()) => true,
                    Err(e2) => {
                        rmq_warn!(
                            "resetOffsetByTimestampOld error. addr={broker_addr} topic={topic} group={group} timestamp={timestamp}: {e2}"
                        );
                        false
                    }
                }
            }
            // CODE: 1 means "the consumer group never came online": nothing to roll back.
            Err(e) if e.response_code() == Some(response_code::SYSTEM_ERROR) => true,
            Err(e) => {
                rmq_warn!(
                    "resetOffsetNewConcurrent error. addr={broker_addr} topic={topic} group={group} timestamp={timestamp}: {e}"
                );
                false
            }
        }
    }

    /// The old reset on one broker: the broker's consume stats drive the queue list, and a
    /// group that never consumed falls back to the topic stats over `readQueueNums` queues.
    async fn reset_offset_old_for_broker(
        &self,
        broker_addr: &str,
        queue_data: Option<&QueueData>,
        group: &str,
        topic: &str,
        timestamp: i64,
        force: bool,
    ) -> Result<()> {
        let stats = self.examine_consume_stats(broker_addr, group, None, None).await?;
        let mut has_consumed = false;
        for (key, wrapper) in &stats.offset_table {
            if key.topic != topic {
                continue;
            }
            has_consumed = true;
            let mq = MessageQueue::new(&key.topic, &key.broker_name, key.queue_id);
            self.reset_offset_consume_offset(broker_addr, group, &mq, wrapper, timestamp, force)
                .await?;
        }
        if has_consumed {
            return Ok(());
        }
        let queue_data = queue_data.ok_or_else(|| {
            Error::client(format!("no queue data for broker {broker_addr} while resetting {topic}"))
        })?;
        let status = self.examine_topic_stats_by_broker(broker_addr, topic).await?;
        for queue_id in 0..queue_data.read_queue_nums {
            let Some((_, offset)) = status.offset_table.iter().find(|(k, _)| {
                k.topic == topic
                    && k.broker_name == queue_data.broker_name
                    && k.queue_id == queue_id
            }) else {
                return Err(Error::client(format!(
                    "queue {topic}#{queue_id} missing in the topic stats of {broker_addr}"
                )));
            };
            let mq = MessageQueue::new(topic, &queue_data.broker_name, queue_id);
            let wrapper = OffsetWrapper {
                broker_offset: offset.max_offset,
                consumer_offset: offset.min_offset,
                ..OffsetWrapper::default()
            };
            self.reset_offset_consume_offset(broker_addr, group, &mq, &wrapper, timestamp, force)
                .await?;
        }
        Ok(())
    }

    /// `searchOffset` (or `getMaxOffset` for `-1`), written back when forced or when the
    /// reset would not move the consumer forward.
    async fn reset_offset_consume_offset(
        &self,
        broker_addr: &str,
        group: &str,
        mq: &MessageQueue,
        wrapper: &OffsetWrapper,
        timestamp: i64,
        force: bool,
    ) -> Result<()> {
        let client = self.require_client()?;
        let reset_offset = if timestamp == -1 {
            client
                .get_max_offset(mq, self.timeout_millis(), Some(broker_addr))
                .await?
        } else {
            client
                .search_offset_by_timestamp(mq, timestamp, self.timeout_millis(), Some(broker_addr))
                .await?
        };
        if force || reset_offset <= wrapper.consumer_offset {
            client
                .update_consumer_offset(group, mq, reset_offset, self.timeout_millis(), Some(broker_addr))
                .await?;
        }
        Ok(())
    }

    /// Per-broker `QUERY_TOPICS_BY_CONSUMER(343)` in parallel over the retry topic's route;
    /// every topic list is concatenated (no de-duplication) and single failures are skipped.
    pub async fn query_topics_by_consumer_concurrent(
        &self,
        group: &str,
    ) -> AdminToolResult<TopicList> {
        let client = match self.admin_tool_client() {
            Ok(client) => client,
            Err((code, message)) => return AdminToolResult::failure(code, message),
        };
        let retry_topic = MixAll::get_retry_topic(group);
        let Some(route) = client.get_topic_route_data(&retry_topic).await else {
            return AdminToolResult::failure(
                AdminToolsResultCode::TopicRouteInfoNotExist,
                "router info not found.",
            );
        };
        if route.get_broker_datas().is_empty() {
            return AdminToolResult::failure(
                AdminToolsResultCode::TopicRouteInfoNotExist,
                "router info not found.",
            );
        }
        let addrs = route_masters(&route);
        let group_owned = group.to_string();
        let parts = self
            .fan_out(addrs, move |admin, addr| {
                let group = group_owned.clone();
                async move { admin.query_topics_by_consumer_to_broker(&addr, &group).await }
            })
            .await;
        let mut result = TopicList::new();
        for (addr, topics) in parts {
            match topics {
                Ok(topics) => {
                    for topic in topics.get_topic_list() {
                        result.topic_list.push(topic.clone());
                    }
                }
                Err(e) => rmq_warn!("queryTopicsByConsumer error. group={group} broker={addr}: {e}"),
            }
        }
        AdminToolResult::success(result)
    }

    /// Per-broker `QUERY_CONSUME_TIME_SPAN(303)` in parallel; the spans are concatenated.
    /// No route at all is the `-2001` verdict, a route without broker data is a success
    /// with an empty list.
    pub async fn query_consume_time_span_concurrent(
        &self,
        topic: &str,
        group: &str,
    ) -> AdminToolResult<Vec<Value>> {
        let client = match self.admin_tool_client() {
            Ok(client) => client,
            Err((code, message)) => return AdminToolResult::failure(code, message),
        };
        let Some(route) = client.get_topic_route_data(topic).await else {
            return AdminToolResult::failure(
                AdminToolsResultCode::TopicRouteInfoNotExist,
                "topic router info not found",
            );
        };
        if route.get_broker_datas().is_empty() {
            return AdminToolResult::success(Vec::new());
        }
        let addrs = route_masters(&route);
        let topic_owned = topic.to_string();
        let group_owned = group.to_string();
        let parts = self
            .fan_out(addrs, move |admin, addr| {
                let topic = topic_owned.clone();
                let group = group_owned.clone();
                async move {
                    admin
                        .query_consume_time_span_by_broker(&addr, &topic, &group)
                        .await
                }
            })
            .await;
        let mut spans: Vec<Value> = Vec::new();
        for (addr, part) in parts {
            match part {
                Ok(part) => spans.extend(part),
                Err(e) => {
                    rmq_warn!("queryConsumeTimeSpan error. topic={topic} group={group} broker={addr}: {e}")
                }
            }
        }
        AdminToolResult::success(spans)
    }

    /// The message track of one consumer group, shared verbatim by the sequential and the
    /// concurrent track detail so both produce the same verdicts.
    async fn track_one_group(&self, msg: &MessageExt, group: &str) -> MessageTrack {
        let mut mt = MessageTrack {
            consumer_group: group.to_string(),
            track_type: TrackType::Unknown,
            exception_desc: None,
        };
        let cc = match self.examine_consumer_connection_info(group, None).await {
            Ok(cc) => cc,
            Err(e) => {
                if e.response_code() == Some(response_code::CONSUMER_NOT_ONLINE) {
                    mt.track_type = TrackType::NotOnline;
                }
                mt.exception_desc = match e.response_code() {
                    Some(code) => Some(format!("CODE:{code} DESC:{e}")),
                    None => Some(format!("{e}")),
                };
                return mt;
            }
        };

        match cc.consume_type.as_deref() {
            Some("CONSUME_ACTIVELY") => mt.track_type = TrackType::Pull,
            Some("CONSUME_PASSIVELY") => {
                let if_consumed = match self.consumed(msg, group).await {
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
                            return mt;
                        }
                        _ => {
                            mt.exception_desc = Some(format!("{e}"));
                            return mt;
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
        mt
    }

    /// The per-group track verdicts of [`Self::message_track_detail`], fanned out over the
    /// groups; the output list keeps the (sorted) group order.
    pub async fn message_track_detail_concurrent(
        &self,
        msg: &MessageExt,
    ) -> Result<Vec<MessageTrack>> {
        let broker_addr = self.first_track_broker_addr(&msg.topic).await?;
        let mut groups = self
            .query_topic_consume_by_who(&broker_addr, &msg.topic)
            .await?;
        groups.sort();
        let message = msg.clone();
        let parts = self
            .fan_out(groups.clone(), move |admin, group| {
                let msg = message.clone();
                async move { admin.track_one_group(&msg, &group).await }
            })
            .await;
        // Completion order is scheduling noise; report in the group order.
        let mut tracks: Vec<(String, MessageTrack)> = parts;
        tracks.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(tracks.into_iter().map(|(_, track)| track).collect())
    }

    /// The group discovery leg shared by both track details: the topic's route, then the
    /// first broker that has an address, then `QUERY_TOPIC_CONSUME_BY_WHO(300)`.
    async fn first_track_broker_addr(&self, topic: &str) -> Result<String> {
        let route = self.examine_topic_route(topic).await?;
        route
            .get_broker_datas()
            .iter()
            .find_map(|bd| bd.select_broker_addr())
            .filter(|a| !a.is_empty())
            .ok_or_else(|| Error::client(format!("no broker address for topic {topic}")))
    }
}

/// The master (or fall-back) address of every broker data entry, dropping entries without one.
fn route_masters(route: &TopicRouteData) -> Vec<String> {
    route
        .get_broker_datas()
        .iter()
        .filter_map(|bd| bd.select_broker_addr().filter(|a| !a.is_empty()))
        .collect()
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
    use crate::remoting::protocol::admin_body::{CheckRocksdbCqWriteStatus, TopicOffset};
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

    // ---------------- 3001-3010 auth user / ACL management (mock-broker cross-check) ----------------

    use crate::remoting::protocol::auth_body::{
        AclEntry, AclPolicy, DECISION_ALLOW, DECISION_DENY, POLICY_TYPE_CUSTOM, POLICY_TYPE_DEFAULT,
        USER_STATUS_DISABLE, USER_STATUS_ENABLE, USER_TYPE_NORMAL, USER_TYPE_SUPER,
    };

    /// One auth request exactly as it left the wire: `(request code, extFields, body)`.
    type AuthRecord = (i32, Vec<(String, String)>, Vec<u8>);
    type AuthLog = Arc<Mutex<Vec<AuthRecord>>>;

    /// How the mock broker answers one request code.
    struct AuthReply {
        code: i32,
        remark: Option<String>,
        body: Vec<u8>,
    }

    impl AuthReply {
        fn ok(body: &str) -> AuthReply {
            AuthReply {
                code: response_code::SUCCESS,
                remark: None,
                body: body.as_bytes().to_vec(),
            }
        }

        fn err(code: i32, remark: &str) -> AuthReply {
            AuthReply {
                code,
                remark: Some(remark.to_string()),
                body: Vec::new(),
            }
        }
    }

    /// Starts the auth cross-check mock broker: every request in 3001-3010 is recorded verbatim and
    /// answered from `replies` (matched by request code); everything else the start handshake and
    /// the route refreshers produce gets SUCCESS with no body, and is not recorded.
    async fn spawn_auth_broker(listener: tokio::net::TcpListener, replies: Vec<(i32, AuthReply)>) -> AuthLog {
        use tokio::io::AsyncWriteExt as _;
        let log: AuthLog = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::clone(&log);
        let replies = Arc::new(replies);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let log = Arc::clone(&inner);
                let replies = Arc::clone(&replies);
                tokio::spawn(async move {
                    while let Some(frame) = read_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else { return };
                        if (request_code::AUTH_CREATE_USER..=request_code::AUTH_LIST_ACL)
                            .contains(&request.code)
                        {
                            lock(&log).push((
                                request.code,
                                request.ext_fields().iter().cloned().collect(),
                                request.body().unwrap_or_default().to_vec(),
                            ));
                        }
                        if request.is_oneway_rpc() {
                            continue;
                        }
                        let scripted = replies
                            .iter()
                            .find(|(code, _)| *code == request.code)
                            .map(|(_, reply)| reply);
                        let mut response = match scripted {
                            Some(reply) => {
                                let mut cmd = reply_for(
                                    &request,
                                    reply.code,
                                    if reply.body.is_empty() {
                                        None
                                    } else {
                                        Some(reply.body.clone())
                                    },
                                );
                                cmd.remark = reply.remark.clone();
                                cmd
                            }
                            None => reply_for(&request, response_code::SUCCESS, None),
                        };
                        let bytes = response.encode();
                        let _ = stream.write_all(&bytes).await;
                        let _ = stream.flush().await;
                    }
                });
            }
        });
        log
    }

    /// Binds a mock broker and returns `(its address, the recorded auth traffic)`.
    async fn auth_broker(replies: Vec<(i32, AuthReply)>) -> (String, AuthLog) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock broker");
        let addr = listener.local_addr().expect("mock broker addr").to_string();
        let log = spawn_auth_broker(listener, replies).await;
        (addr, log)
    }

    /// A fresh instance name per test: the admin's clientId is second-granularity, and a shared
    /// name would make two running tests fight over one registry slot.
    async fn started_auth_admin(addr: &str, tag: &str) -> DefaultMQAdminExt {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let admin = DefaultMQAdminExt::new();
        admin.set_instance_name(&format!(
            "ADMIN_auth_{tag}_{}",
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        admin.set_namesrv_addr(addr);
        admin
            .start()
            .await
            .expect("start against the mock broker should succeed");
        admin
    }

    fn auth_record(log: &AuthLog, code: i32) -> AuthRecord {
        let records: Vec<AuthRecord> = lock(log)
            .iter()
            .filter(|r| r.0 == code)
            .cloned()
            .collect();
        assert_eq!(records.len(), 1, "code {code} must reach the wire exactly once");
        records.into_iter().next().unwrap()
    }

    fn auth_body(log: &AuthLog, code: i32) -> String {
        String::from_utf8(auth_record(log, code).2.clone()).expect("utf8 request body")
    }

    /// How many times one auth code reached the wire.
    fn auth_count(log: &AuthLog, code: i32) -> usize {
        lock(log).iter().filter(|r| r.0 == code).count()
    }

    /// Request side of 3001: the short form puts the name in the header, the record in the body,
    /// and writes no `userStatus` key at all.
    #[tokio::test]
    async fn create_user_short_form_sends_username_header_and_no_status_key() {
        let (addr, log) = auth_broker(vec![]).await;
        let admin = started_auth_admin(&addr, "create").await;

        admin
            .create_user(&addr, "rocketmq", "12345678", USER_TYPE_SUPER)
            .await
            .expect("3001 short form");

        let record = auth_record(&log, request_code::AUTH_CREATE_USER);
        assert_eq!(
            record.1,
            vec![("username".to_string(), "rocketmq".to_string())],
            "3001 carries the username header and nothing else"
        );
        assert_eq!(
            auth_body(&log, request_code::AUTH_CREATE_USER),
            r#"{"username":"rocketmq","password":"12345678","userType":"Super"}"#
        );
        admin.shutdown();
    }

    /// Paired control: the same call through the object form does write `userStatus`, so the key's
    /// absence above is the short form's doing rather than the encoder dropping it.
    #[tokio::test]
    async fn create_user_object_form_writes_status_and_keeps_key_order() {
        let (addr, log) = auth_broker(vec![]).await;
        let admin = started_auth_admin(&addr, "create_obj").await;
        let user = AuthUser {
            username: "rocketmq".to_string(),
            password: Some("12345678".to_string()),
            user_type: Some(USER_TYPE_NORMAL.to_string()),
            user_status: Some(USER_STATUS_DISABLE.to_string()),
        };

        admin
            .create_user_with_config(&addr, &user)
            .await
            .expect("3001 object form");

        assert_eq!(
            auth_body(&log, request_code::AUTH_CREATE_USER),
            r#"{"username":"rocketmq","password":"12345678","userType":"Normal","userStatus":"disable"}"#
        );
        admin.shutdown();
    }

    /// 3002 differs from 3001 only in the code, and it sends no body key the caller left unset.
    #[tokio::test]
    async fn update_user_sends_3002_with_the_same_shape_as_create() {
        let (addr, log) = auth_broker(vec![]).await;
        let admin = started_auth_admin(&addr, "update").await;

        admin
            .update_user(&addr, "alice", "abcdefgh", USER_TYPE_NORMAL)
            .await
            .expect("3002");

        let record = auth_record(&log, request_code::AUTH_UPDATE_USER);
        assert_eq!(record.1, vec![("username".to_string(), "alice".to_string())]);
        assert_eq!(
            auth_body(&log, request_code::AUTH_UPDATE_USER),
            r#"{"username":"alice","password":"abcdefgh","userType":"Normal"}"#
        );
        assert!(
            auth_count(&log, request_code::AUTH_CREATE_USER) == 0,
            "3002 must not be sent under the 3001 code"
        );
        admin.shutdown();
    }

    /// 3003 is header-only and its reply is never read: the broker answers with bytes that are not
    /// a record at all, and the call still succeeds.
    #[tokio::test]
    async fn delete_user_sends_only_the_username_header_and_ignores_the_body() {
        let (addr, log) = auth_broker(vec![(
            request_code::AUTH_DELETE_USER,
            AuthReply::ok("this is not a user record"),
        )])
        .await;
        let admin = started_auth_admin(&addr, "delete").await;

        admin
            .delete_user(&addr, "alice")
            .await
            .expect("3003 must not touch the reply body");

        let record = auth_record(&log, request_code::AUTH_DELETE_USER);
        assert_eq!(record.1, vec![("username".to_string(), "alice".to_string())]);
        assert!(record.2.is_empty(), "3003 has no request body");
        admin.shutdown();
    }

    #[tokio::test]
    async fn get_user_decodes_the_record_and_a_missing_body_is_none() {
        let (addr, log) = auth_broker(vec![(
            request_code::AUTH_GET_USER,
            AuthReply::ok(r#"{"username":"alice","userType":"Normal","userStatus":"enable"}"#),
        )])
        .await;
        let admin = started_auth_admin(&addr, "get").await;

        let user = admin
            .get_user(&addr, "alice")
            .await
            .expect("3004")
            .expect("the broker returned a record");
        assert_eq!(user.username, "alice");
        assert_eq!(user.user_type.as_deref(), Some(USER_TYPE_NORMAL));
        assert_eq!(user.user_status.as_deref(), Some(USER_STATUS_ENABLE));
        // A response that carries no password still decodes; the field stays unset
        assert_eq!(user.password, None);
        assert_eq!(
            auth_record(&log, request_code::AUTH_GET_USER).1,
            vec![("username".to_string(), "alice".to_string())]
        );

        // The same code answered with an empty body reads as "no such user", not as an error
        let (addr2, log2) = auth_broker(vec![]).await;
        let empty_admin = started_auth_admin(&addr2, "get_empty").await;
        assert_eq!(empty_admin.get_user(&addr2, "ghost").await.unwrap(), None);
        assert_eq!(auth_record(&log2, request_code::AUTH_GET_USER).0, 3004);
        admin.shutdown();
        empty_admin.shutdown();
    }

    /// 3005 sends `filter` only when the caller has one; an empty header table is the "list all"
    /// request, not `filter=`.
    #[tokio::test]
    async fn list_user_omits_the_filter_header_when_unset() {
        let (addr, log) = auth_broker(vec![(
            request_code::AUTH_LIST_USER,
            AuthReply::ok(r#"[{"username":"a"},{"username":"b","userStatus":"disable"}]"#),
        )])
        .await;
        let admin = started_auth_admin(&addr, "list_user").await;

        let all = admin.list_user(&addr, None).await.expect("3005 unfiltered");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].username, "a");
        assert_eq!(all[1].user_status.as_deref(), Some(USER_STATUS_DISABLE));
        assert_eq!(
            auth_record(&log, request_code::AUTH_LIST_USER).1,
            Vec::<(String, String)>::new(),
            "no filter must mean no header at all"
        );

        let filtered = admin
            .list_user(&addr, Some("rock*"))
            .await
            .expect("3005 filtered");
        assert_eq!(filtered.len(), 2, "the reply is the same record set here");
        let exts: Vec<Vec<(String, String)>> = lock(&log)
            .iter()
            .filter(|r| r.0 == request_code::AUTH_LIST_USER)
            .map(|r| r.1.clone())
            .collect();
        assert_eq!(
            exts,
            vec![
                Vec::new(),
                vec![("filter".to_string(), "rock*".to_string())]
            ],
            "the second call adds exactly one header, in order"
        );
        admin.shutdown();
    }

    /// 3006 short form: the header is only the subject, the body is one policy with no
    /// `policyType` key and one entry per resource repeating the shared values.
    #[tokio::test]
    async fn create_acl_short_form_sends_one_policy_without_policy_type() {
        let (addr, log) = auth_broker(vec![]).await;
        let admin = started_auth_admin(&addr, "create_acl").await;

        admin
            .create_acl(
                &addr,
                "user:alice",
                &["topic:T1", "topic:T2"],
                &["PUB", "SUB"],
                &["192.168.0.1"],
                DECISION_ALLOW,
            )
            .await
            .expect("3006 short form");

        assert_eq!(
            auth_record(&log, request_code::AUTH_CREATE_ACL).1,
            vec![("subject".to_string(), "user:alice".to_string())],
            "3006 carries the subject header and nothing else"
        );
        assert_eq!(
            auth_body(&log, request_code::AUTH_CREATE_ACL),
            concat!(
                r#"{"subject":"user:alice","policies":[{"entries":["#,
                r#"{"resource":"topic:T1","actions":["PUB","SUB"],"sourceIps":["192.168.0.1"],"decision":"Allow"},"#,
                r#"{"resource":"topic:T2","actions":["PUB","SUB"],"sourceIps":["192.168.0.1"],"decision":"Allow"}"#,
                r#"]}]}"#
            )
        );
        admin.shutdown();
    }

    /// Paired control: `policyType` is written when the caller names it, so its absence in the
    /// short form is the construction, not an encoder that drops the key.
    #[tokio::test]
    async fn create_acl_object_form_writes_policy_type() {
        let (addr, log) = auth_broker(vec![]).await;
        let admin = started_auth_admin(&addr, "create_acl_obj").await;
        let mut acl = Acl::new("user:bob");
        acl.policies.push(AclPolicy {
            policy_type: Some(POLICY_TYPE_CUSTOM.to_string()),
            entries: vec![AclEntry {
                resource: "group:G1".to_string(),
                actions: vec!["ANY".to_string()],
                source_ips: vec!["*".to_string()],
                decision: Some(DECISION_DENY.to_string()),
            }],
        });

        admin
            .create_acl_with_config(&addr, &acl)
            .await
            .expect("3006 object form");

        assert_eq!(
            auth_body(&log, request_code::AUTH_CREATE_ACL),
            concat!(
                r#"{"subject":"user:bob","policies":[{"policyType":"Custom","entries":["#,
                r#"{"resource":"group:G1","actions":["ANY"],"sourceIps":["*"],"decision":"Deny"}]}]}"#
            )
        );
        admin.shutdown();
    }

    #[tokio::test]
    async fn update_acl_short_form_sends_3007() {
        let (addr, log) = auth_broker(vec![]).await;
        let admin = started_auth_admin(&addr, "update_acl").await;

        admin
            .update_acl(
                &addr,
                "user:alice",
                &["topic:T1"],
                &["PUB"],
                &["10.0.0.1"],
                DECISION_DENY,
            )
            .await
            .expect("3007 short form");

        let record = auth_record(&log, request_code::AUTH_UPDATE_ACL);
        assert_eq!(record.1, vec![("subject".to_string(), "user:alice".to_string())]);
        assert_eq!(
            String::from_utf8(record.2.clone()).unwrap(),
            concat!(
                r#"{"subject":"user:alice","policies":[{"entries":["#,
                r#"{"resource":"topic:T1","actions":["PUB"],"sourceIps":["10.0.0.1"],"decision":"Deny"}"#,
                r#"]}]}"#
            )
        );
        admin.shutdown();
    }

    /// 3008 deletes one grant of one subject: the header table is exactly these two keys, so any
    /// extra field the request grew would be visible here.
    #[tokio::test]
    async fn delete_acl_ext_fields_are_exactly_subject_and_resource() {
        let (addr, log) = auth_broker(vec![]).await;
        let admin = started_auth_admin(&addr, "delete_acl").await;

        admin
            .delete_acl(&addr, "user:alice", "topic:T1")
            .await
            .expect("3008");

        let record = auth_record(&log, request_code::AUTH_DELETE_ACL);
        assert_eq!(
            record.1,
            vec![
                ("subject".to_string(), "user:alice".to_string()),
                ("resource".to_string(), "topic:T1".to_string()),
            ],
            "no policyType, no decision, no body"
        );
        assert_eq!(record.1.len(), 2);
        assert!(record.2.is_empty(), "3008 has no request body");
        admin.shutdown();
    }

    #[tokio::test]
    async fn get_acl_decodes_policies_and_a_missing_body_is_none() {
        let (addr, log) = auth_broker(vec![(
            request_code::AUTH_GET_ACL,
            AuthReply::ok(concat!(
                r#"{"subject":"user:alice","policies":[{"policyType":"Default","entries":["#,
                r#"{"resource":"topic:T1","actions":["ANY"],"sourceIps":["*"],"decision":"Allow"}]}]}"#
            )),
        )])
        .await;
        let admin = started_auth_admin(&addr, "get_acl").await;

        let acl = admin
            .get_acl(&addr, "user:alice")
            .await
            .expect("3009")
            .expect("the broker returned an acl");
        assert_eq!(acl.subject, "user:alice");
        assert_eq!(acl.policies.len(), 1);
        assert_eq!(acl.policies[0].policy_type.as_deref(), Some(POLICY_TYPE_DEFAULT));
        assert_eq!(acl.policies[0].entries[0].resource, "topic:T1");
        assert_eq!(acl.policies[0].entries[0].actions, vec!["ANY".to_string()]);
        assert_eq!(
            auth_record(&log, request_code::AUTH_GET_ACL).1,
            vec![("subject".to_string(), "user:alice".to_string())],
            "3009 asks by subject alone"
        );

        let (addr2, _) = auth_broker(vec![]).await;
        let empty_admin = started_auth_admin(&addr2, "get_acl_empty").await;
        assert_eq!(empty_admin.get_acl(&addr2, "user:ghost").await.unwrap(), None);
        admin.shutdown();
        empty_admin.shutdown();
    }

    /// 3010 has two independent filters, and each is sent only when supplied.
    #[tokio::test]
    async fn list_acl_sends_only_the_filters_the_caller_supplied() {
        let (addr, log) = auth_broker(vec![(
            request_code::AUTH_LIST_ACL,
            AuthReply::ok(concat!(
                r#"[{"subject":"user:a","policies":[]},{"subject":"user:b","policies":[{"entries":["#,
                r#"{"resource":"topic:T","actions":["SUB"],"sourceIps":["*"],"decision":"Deny"}]}]}]"#
            )),
        )])
        .await;
        let admin = started_auth_admin(&addr, "list_acl").await;

        let both = admin
            .list_acl(&addr, Some("user:*"), Some("topic:T*"))
            .await
            .expect("3010 with both filters");
        assert_eq!(both.len(), 2);
        assert_eq!(both[0].subject, "user:a");
        assert!(both[0].policies.is_empty(), "an empty policy list stays empty");
        assert_eq!(both[1].policies[0].entries[0].decision.as_deref(), Some(DECISION_DENY));
        assert_eq!(
            auth_record(&log, request_code::AUTH_LIST_ACL).1,
            vec![
                ("subjectFilter".to_string(), "user:*".to_string()),
                ("resourceFilter".to_string(), "topic:T*".to_string()),
            ]
        );

        // One filter only: the other key is absent rather than empty
        let one = admin
            .list_acl(&addr, None, Some("group:G1"))
            .await
            .expect("3010 with one filter");
        assert_eq!(one.len(), 2);
        let exts: Vec<Vec<(String, String)>> = lock(&log)
            .iter()
            .filter(|r| r.0 == request_code::AUTH_LIST_ACL)
            .map(|r| r.1.clone())
            .collect();
        assert_eq!(
            exts,
            vec![
                vec![
                    ("subjectFilter".to_string(), "user:*".to_string()),
                    ("resourceFilter".to_string(), "topic:T*".to_string()),
                ],
                vec![("resourceFilter".to_string(), "group:G1".to_string())],
            ],
            "an unset filter header is omitted, never sent empty"
        );
        admin.shutdown();
    }

    /// The list replies are arrays of objects; the same decoders read an empty body as no records.
    #[tokio::test]
    async fn list_bodies_of_no_records_decode_as_empty_not_error() {
        let (addr, _) = auth_broker(vec![]).await;
        let admin = started_auth_admin(&addr, "empty_lists").await;

        assert!(admin.list_user(&addr, Some("*")).await.unwrap().is_empty());
        assert!(admin.list_acl(&addr, Some("*"), None).await.unwrap().is_empty());
        admin.shutdown();
    }

    /// Only SUCCESS (0) passes: any other response code surfaces as the port's broker error,
    /// carrying the code and the remark.
    #[tokio::test]
    async fn auth_rpc_rejects_a_non_success_response_code() {
        let (addr, log) = auth_broker(vec![(
            request_code::AUTH_CREATE_USER,
            AuthReply::err(response_code::NO_PERMISSION, "acl not ready"),
        )])
        .await;
        let admin = started_auth_admin(&addr, "bad_code").await;

        let err = admin
            .create_user(&addr, "rocketmq", "12345678", USER_TYPE_SUPER)
            .await
            .expect_err("a non-SUCCESS code must not pass");
        assert_eq!(err.response_code(), Some(response_code::NO_PERMISSION));
        assert!(err.to_string().contains("acl not ready"), "{err}");
        // and the request really went out before the verdict came back
        assert_eq!(
            auth_record(&log, request_code::AUTH_CREATE_USER).0,
            request_code::AUTH_CREATE_USER
        );
        admin.shutdown();
    }

    /// The reply of a write is never parsed: SUCCESS with a record body still returns no data.
    #[tokio::test]
    async fn create_user_ignores_a_record_in_the_reply() {
        let (addr, _) = auth_broker(vec![(
            request_code::AUTH_CREATE_USER,
            AuthReply::ok(r#"{"username":"rocketmq","userStatus":"enable"}"#),
        )])
        .await;
        let admin = started_auth_admin(&addr, "void_reply").await;

        admin
            .create_user(&addr, "rocketmq", "12345678", USER_TYPE_SUPER)
            .await
            .expect("3001 is a void operation");
        admin.shutdown();
    }

    /// Addressing contract: the RPC goes to exactly the address the caller passed. With the fast
    /// channel switched on, a rewritten target would be port-2 and nothing would be recorded here.
    #[tokio::test]
    async fn auth_rpc_targets_the_address_the_caller_passed_even_with_vip_channel_on() {
        let (addr, log) = auth_broker(vec![]).await;
        let admin = DefaultMQAdminExt::new();
        admin.set_instance_name("ADMIN_auth_vip");
        admin.set_namesrv_addr(&addr);
        admin.set_vip_channel_enabled(true);
        admin.start().await.expect("start against the mock broker");

        admin
            .delete_user(&addr, "alice")
            .await
            .expect("3003 must arrive at the port it was given");

        assert_eq!(
            auth_record(&log, request_code::AUTH_DELETE_USER).1,
            vec![("username".to_string(), "alice".to_string())]
        );
        admin.shutdown();
    }

    #[tokio::test]
    async fn auth_rpcs_require_start() {
        let admin = DefaultMQAdminExt::new();
        for (name, outcome) in [
            (
                "create_user",
                admin
                    .create_user("127.0.0.1:10911", "u", "p", USER_TYPE_NORMAL)
                    .await
                    .err(),
            ),
            (
                "get_user",
                admin.get_user("127.0.0.1:10911", "u").await.err(),
            ),
            (
                "list_acl",
                admin
                    .list_acl("127.0.0.1:10911", Some("*"), Some("*"))
                    .await
                    .err(),
            ),
        ] {
            let err = outcome.unwrap_or_else(|| panic!("{name} must refuse before start()"));
            assert!(
                err.to_string().starts_with("MQClientException: admin not started"),
                "{name}: {err}"
            );
        }
    }

    /// The header names are wire-visible; a rename here silently asks the broker for nothing.
    #[test]
    fn auth_ext_field_names_match_the_protocol_spelling() {
        let mut pairs = vec![
            ("username", "u".to_string()),
            ("filter", "f".to_string()),
            ("subject", "user:u".to_string()),
            ("resource", "topic:T".to_string()),
        ];
        pairs.push(("subjectFilter", "user:*".to_string()));
        pairs.push(("resourceFilter", "topic:*".to_string()));
        let ext = ext_pairs(&pairs);
        for key in [
            "username",
            "filter",
            "subject",
            "resource",
            "subjectFilter",
            "resourceFilter",
        ] {
            assert!(ext.contains_key(key), "{key} missing");
        }
        assert!(!ext.contains_key("userName"), "no camel-case drift on username");
        assert!(!ext.contains_key("subjects"), "the subject header is singular");
        assert!(!ext.contains_key("resourceFilters"), "the filter headers are singular");
    }

    // ---------------------------------------------------------------- metadata paging

    fn paging_client(id: &str) -> MQClientInstance {
        MQClientInstance::with_config(
            id,
            vec!["127.0.0.1:9876".to_string()],
            MQClientInstanceConfig::default(),
        )
    }

    /// Wires the admin to a client whose RPCs are served from canned replies, no broker.
    fn admin_on(client: &MQClientInstance, page_size: i32) -> DefaultMQAdminExt {
        let admin = DefaultMQAdminExt::new();
        admin.update_config(|c| {
            c.max_page_size_in_get_metadata = page_size;
            c.timeout_millis = 5_000;
        });
        *lock(&admin.inner.client) = Some(client.clone());
        admin.inner.started.store(true, Ordering::Release);
        admin
    }

    fn version_value(counter: i64) -> Value {
        json!({"counter": counter, "timestamp": 1_700_000_000_000i64 + counter})
    }

    fn topic_page(counter: i64, names: &[&str]) -> Vec<u8> {
        let mut wrapper = TopicConfigSerializeWrapper::new();
        wrapper.data_version = version_value(counter);
        for name in names {
            wrapper
                .topic_config_table
                .push(((*name).to_string(), json!({"topicName": name})));
        }
        RemotingSerializable::encode(&wrapper.to_json_value())
    }

    fn group_page(counter: i64, names: &[&str]) -> Vec<u8> {
        let mut wrapper = SubscriptionGroupWrapper::new();
        wrapper.data_version = version_value(counter);
        for name in names {
            wrapper.subscription_group_table.push((
                (*name).to_string(),
                SubscriptionGroupConfig {
                    group_name: (*name).to_string(),
                    ..Default::default()
                },
            ));
        }
        RemotingSerializable::encode(&wrapper.to_json_value())
    }

    fn page_reply(body: Vec<u8>, total_key: &str, total: Option<i32>) -> RemotingCommand {
        let mut reply = RemotingCommand::create_response(response_code::SUCCESS, None);
        reply.set_body(Some(body));
        if let Some(total) = total {
            reply.add_ext_field(total_key, &total.to_string());
        }
        reply
    }

    fn paging_requests(client: &MQClientInstance, code: i32) -> Vec<ExtFields> {
        client
            .recorded_requests()
            .into_iter()
            .filter(|(request, _)| *request == code)
            .map(|(_, ext)| ext)
            .collect()
    }

    #[tokio::test]
    async fn topic_paging_advances_by_the_returned_page_size() {
        let client = paging_client("cfg-paging-topic-size");
        client.queue_canned_reply(
            request_code::GET_ALL_TOPIC_CONFIG,
            page_reply(topic_page(1, &["A", "B"]), "totalTopicNum", Some(4)),
        );
        client.queue_canned_reply(
            request_code::GET_ALL_TOPIC_CONFIG,
            page_reply(topic_page(1, &["C", "D"]), "totalTopicNum", Some(4)),
        );
        let admin = admin_on(&client, 5);

        let merged = admin
            .get_all_topic_config("127.0.0.1:10911", None)
            .await
            .unwrap();

        let names: Vec<&str> = merged
            .topic_config_table
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(names, vec!["A", "B", "C", "D"]);
        assert_eq!(merged.data_version, version_value(1));

        let requests = paging_requests(&client, request_code::GET_ALL_TOPIC_CONFIG);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].get("topicSeq"), Some("0"));
        assert_eq!(requests[0].get("maxTopicNum"), Some("5"));
        assert_eq!(requests[0].get("dataVersion"), Some(""));
        // The cursor moves by the two topics the broker returned, not by the requested five.
        assert_eq!(requests[1].get("topicSeq"), Some("2"));
        let v1 = RemotingSerializable::to_json_string(&version_value(1));
        assert_eq!(requests[1].get("dataVersion"), Some(v1.as_str()));
    }

    #[tokio::test]
    async fn topic_paging_restarts_from_zero_on_a_version_change() {
        let client = paging_client("cfg-paging-topic-version");
        for (page, total) in [
            (topic_page(1, &["A", "B"]), 4),
            (topic_page(2, &["C"]), 4),
            (topic_page(2, &["A", "B"]), 4),
            (topic_page(2, &["C", "D"]), 4),
        ] {
            client.queue_canned_reply(
                request_code::GET_ALL_TOPIC_CONFIG,
                page_reply(page, "totalTopicNum", Some(total)),
            );
        }
        let admin = admin_on(&client, 2);

        let merged = admin
            .get_all_topic_config("127.0.0.1:10911", None)
            .await
            .unwrap();

        let names: Vec<&str> = merged
            .topic_config_table
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(names, vec!["A", "B", "C", "D"]);
        assert_eq!(merged.data_version, version_value(2));

        let requests = paging_requests(&client, request_code::GET_ALL_TOPIC_CONFIG);
        let seqs: Vec<Option<&str>> = requests.iter().map(|ext| ext.get("topicSeq")).collect();
        assert_eq!(seqs, vec![Some("0"), Some("2"), Some("0"), Some("2")]);
        let v1 = RemotingSerializable::to_json_string(&version_value(1));
        let v2 = RemotingSerializable::to_json_string(&version_value(2));
        let versions: Vec<Option<&str>> =
            requests.iter().map(|ext| ext.get("dataVersion")).collect();
        assert_eq!(
            versions,
            vec![Some(""), Some(v1.as_str()), Some(v2.as_str()), Some(v2.as_str())]
        );
    }

    #[tokio::test]
    async fn topic_paging_stops_after_one_page_without_a_total() {
        let client = paging_client("cfg-paging-topic-legacy");
        client.queue_canned_reply(
            request_code::GET_ALL_TOPIC_CONFIG,
            page_reply(topic_page(3, &["A", "B"]), "totalTopicNum", None),
        );
        let admin = admin_on(&client, 2);

        let merged = admin
            .get_all_topic_config("127.0.0.1:10911", None)
            .await
            .unwrap();

        assert_eq!(merged.topic_config_table.len(), 2);
        assert_eq!(merged.data_version, version_value(3));
        assert_eq!(
            paging_requests(&client, request_code::GET_ALL_TOPIC_CONFIG).len(),
            1
        );
    }

    #[tokio::test]
    async fn group_paging_uses_the_same_cursor_and_restart_rules() {
        let client = paging_client("cfg-paging-group");
        client.queue_canned_reply(
            request_code::GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
            page_reply(group_page(1, &["G1", "G2"]), "totalGroupNum", Some(4)),
        );
        client.queue_canned_reply(
            request_code::GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
            page_reply(group_page(1, &["G3", "G4"]), "totalGroupNum", Some(4)),
        );
        let admin = admin_on(&client, 3);

        let merged = admin
            .get_all_subscription_group("127.0.0.1:10911", None)
            .await
            .unwrap();

        let names: Vec<&str> = merged
            .subscription_group_table
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(names, vec!["G1", "G2", "G3", "G4"]);
        assert_eq!(merged.data_version, version_value(1));

        let requests = paging_requests(&client, request_code::GET_ALL_SUBSCRIPTIONGROUP_CONFIG);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].get("groupSeq"), Some("0"));
        assert_eq!(requests[0].get("maxGroupNum"), Some("3"));
        assert_eq!(requests[0].get("dataVersion"), Some(""));
        assert_eq!(requests[1].get("groupSeq"), Some("2"));
        let v1 = RemotingSerializable::to_json_string(&version_value(1));
        assert_eq!(requests[1].get("dataVersion"), Some(v1.as_str()));
    }

    #[tokio::test]
    async fn group_paging_restarts_at_zero_under_the_new_version() {
        let client = paging_client("cfg-paging-group-restart");
        client.queue_canned_reply(
            request_code::GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
            page_reply(group_page(1, &["G1"]), "totalGroupNum", Some(3)),
        );
        client.queue_canned_reply(
            request_code::GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
            page_reply(group_page(2, &["G2"]), "totalGroupNum", Some(3)),
        );
        client.queue_canned_reply(
            request_code::GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
            page_reply(group_page(2, &["F1"]), "totalGroupNum", Some(3)),
        );
        client.queue_canned_reply(
            request_code::GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
            page_reply(group_page(2, &["F2"]), "totalGroupNum", Some(3)),
        );
        let admin = admin_on(&client, 1);

        let merged = admin
            .get_all_subscription_group("127.0.0.1:10911", None)
            .await
            .unwrap();

        let names: Vec<&str> = merged
            .subscription_group_table
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(names, vec!["F1", "F2"]);
        assert_eq!(merged.data_version, version_value(2));

        let requests = paging_requests(&client, request_code::GET_ALL_SUBSCRIPTIONGROUP_CONFIG);
        assert_eq!(requests.len(), 4);
        let seqs: Vec<Option<&str>> = requests
            .iter()
            .map(|ext| ext.get("groupSeq"))
            .collect();
        assert_eq!(seqs, vec![Some("0"), Some("1"), Some("0"), Some("1")]);
        let v1 = RemotingSerializable::to_json_string(&version_value(1));
        let v2 = RemotingSerializable::to_json_string(&version_value(2));
        let versions: Vec<Option<&str>> = requests
            .iter()
            .map(|ext| ext.get("dataVersion"))
            .collect();
        assert_eq!(
            versions,
            vec![Some(""), Some(v1.as_str()), Some(v2.as_str()), Some(v2.as_str())]
        );
    }

    // ---------------------------------------------------------------- admin long tail

    fn long_tail_client(id: &str) -> MQClientInstance {
        paging_client(id)
    }

    fn success_reply() -> RemotingCommand {
        RemotingCommand::create_response(response_code::SUCCESS, None)
    }

    fn reply_with_body(body: Vec<u8>) -> RemotingCommand {
        let mut reply = success_reply();
        reply.set_body(Some(body));
        reply
    }

    fn reply_with_ext(ext: &[(&str, &str)]) -> RemotingCommand {
        let mut reply = success_reply();
        for (key, value) in ext {
            reply.add_ext_field(key, value);
        }
        reply
    }

    fn requests_of(client: &MQClientInstance, code: i32) -> Vec<ExtFields> {
        paging_requests(client, code)
    }

    fn timeouts_of(client: &MQClientInstance, code: i32) -> Vec<i64> {
        client
            .recorded_timeouts()
            .into_iter()
            .filter(|(seen, _)| *seen == code)
            .map(|(_, timeout)| timeout)
            .collect()
    }

    type AdminLog = Arc<Mutex<Vec<(i32, ExtFields, Option<Vec<u8>>)>>>;
    /// One scripted broker answer: (request code, response code, response body, response ext fields).
    type AdminScript = Vec<(i32, i32, Option<Vec<u8>>, Vec<(String, String)>)>;

    /// Unscripted codes get a bare SUCCESS, which also covers the start-up chatter.
    async fn spawn_admin_broker(
        listener: tokio::net::TcpListener,
        script: AdminScript,
    ) -> AdminLog {
        use tokio::io::AsyncWriteExt as _;
        let log: AdminLog = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::clone(&log);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let log = Arc::clone(&inner);
                let script = script.clone();
                tokio::spawn(async move {
                    while let Some(frame) = read_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else {
                            return;
                        };
                        lock(&log).push((
                            request.code,
                            request.ext_fields().clone(),
                            request.body.clone(),
                        ));
                        if request.is_oneway_rpc() {
                            continue;
                        }
                        let hit = script.iter().find(|(want, ..)| *want == request.code);
                        let (code, body, ext) = match hit {
                            Some((_, code, body, ext)) => (*code, body.clone(), ext.clone()),
                            None => (response_code::SUCCESS, None, Vec::new()),
                        };
                        let mut response = reply_for(&request, code, body);
                        for (key, value) in &ext {
                            response.add_ext_field(key, value);
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

    async fn bind_admin_mock() -> (tokio::net::TcpListener, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let addr = listener.local_addr().expect("mock addr").to_string();
        (listener, addr)
    }

    /// The client the admin internally started with (test access to the recorded traffic).
    fn admin_client(admin: &DefaultMQAdminExt) -> MQClientInstance {
        lock(&admin.inner.client)
            .clone()
            .expect("admin was started with a client")
    }

    fn log_codes(log: &AdminLog) -> Vec<i32> {
        lock(log).iter().map(|(code, _, _)| *code).collect()
    }

    fn logged_request(log: &AdminLog, code: i32) -> (ExtFields, Option<Vec<u8>>) {
        let snapshot = lock(log);
        let hit = snapshot
            .iter()
            .find(|(seen, _, _)| *seen == code)
            .unwrap_or_else(|| panic!("no request {code} in the mock log"));
        (hit.1.clone(), hit.2.clone())
    }

    #[tokio::test]
    async fn container_add_remove_wire_shape_and_20s_timeout() {
        let (listener, addr) = bind_admin_mock().await;
        let log = spawn_admin_broker(listener, Vec::new()).await;
        let admin = started_direct_admin(&addr).await;

        admin
            .add_broker_to_container(&addr, "/rm/broker-a.conf")
            .await
            .expect("add broker to container");
        admin
            .remove_broker_from_container(&addr, "DefaultCluster", "broker-a", 3)
            .await
            .expect("remove broker from container");

        let (add_ext, add_body) = logged_request(&log, request_code::ADD_BROKER);
        assert_eq!(add_ext.get("configPath"), Some("/rm/broker-a.conf"));
        assert_eq!(add_ext.len(), 1, "configPath is the only header field");
        assert!(add_body.is_none(), "the add carries no body");

        let (remove_ext, remove_body) = logged_request(&log, request_code::REMOVE_BROKER);
        assert_eq!(remove_ext.get("brokerName"), Some("broker-a"));
        assert_eq!(remove_ext.get("brokerClusterName"), Some("DefaultCluster"));
        assert_eq!(remove_ext.get("brokerId"), Some("3"));
        assert!(remove_body.is_none());

        let client = admin_client(&admin);
        assert_eq!(
            timeouts_of(&client, request_code::ADD_BROKER),
            vec![ADMIN_CONTAINER_TIMEOUT_MILLIS],
            "the container add uses the fixed 20s"
        );
        assert_eq!(
            timeouts_of(&client, request_code::REMOVE_BROKER),
            vec![ADMIN_CONTAINER_TIMEOUT_MILLIS]
        );
        admin.shutdown();
    }

    #[tokio::test]
    async fn controller_metadata_decodes_the_response_header() {
        let client = long_tail_client("long-tail-ctrl-meta");
        let mut reply = reply_with_ext(&[
            ("group", "ctr-group"),
            ("controllerLeaderId", "ctr-1"),
            ("controllerLeaderAddress", "10.9.9.9:9878"),
            ("isLeader", "true"),
            ("peers", "ctr-1:10.9.9.9:9878;ctr-2:10.9.9.10:9878"),
        ]);
        reply.add_ext_field("nonsenseField", "ignored");
        client.queue_canned_reply(request_code::CONTROLLER_GET_METADATA_INFO, reply);
        let admin = admin_on(&client, 2000);

        let meta = admin
            .get_controller_metadata("10.9.9.9:9878")
            .await
            .expect("metadata");
        assert_eq!(meta.group.as_deref(), Some("ctr-group"));
        assert_eq!(meta.controller_leader_id.as_deref(), Some("ctr-1"));
        assert_eq!(meta.controller_leader_address.as_deref(), Some("10.9.9.9:9878"));
        assert!(meta.is_leader);
        assert_eq!(
            meta.peers.as_deref(),
            Some("ctr-1:10.9.9.9:9878;ctr-2:10.9.9.10:9878")
        );

        let requests = requests_of(&client, request_code::CONTROLLER_GET_METADATA_INFO);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].len(), 0, "the metadata query carries no header");
    }

    #[tokio::test]
    async fn sync_state_two_phase_goes_to_the_leader_with_a_broker_list_body() {
        let (listener, addr) = bind_admin_mock().await;
        let replicas = BrokerReplicasInfo::new();
        let mut meta_ext: Vec<(String, String)> =
            vec![("controllerLeaderAddress".to_string(), addr.clone())];
        meta_ext.push(("isLeader".to_string(), "false".to_string()));
        let log = spawn_admin_broker(
            listener,
            vec![
                (
                    request_code::CONTROLLER_GET_METADATA_INFO,
                    response_code::SUCCESS,
                    None,
                    meta_ext,
                ),
                (
                    request_code::CONTROLLER_GET_SYNC_STATE_DATA,
                    response_code::SUCCESS,
                    Some(RemotingSerializable::encode(&replicas.to_json_value())),
                    Vec::new(),
                ),
            ],
        )
        .await;
        let admin = started_direct_admin(&addr).await;

        let state = admin
            .get_in_sync_state_data(&addr, &["broker-b".to_string()])
            .await
            .expect("sync state");

        assert!(state.replicas_info_table.is_empty());
        let codes = log_codes(&log);
        assert_eq!(
            codes.iter().filter(|c| **c == request_code::CONTROLLER_GET_METADATA_INFO).count(),
            1,
            "phase one: the metadata query"
        );
        let (sync_ext, sync_body) =
            logged_request(&log, request_code::CONTROLLER_GET_SYNC_STATE_DATA);
        assert!(sync_ext.is_empty(), "the sync state request has no header");
        assert_eq!(
            sync_body.as_deref(),
            Some(br#"["broker-b"]"#.as_ref()),
            "the body is the bare JSON array of broker names"
        );
        let client = admin_client(&admin);
        assert_eq!(
            timeouts_of(&client, request_code::CONTROLLER_GET_METADATA_INFO),
            vec![CONTROLLER_RPC_TIMEOUT_MILLIS]
        );
        assert_eq!(
            timeouts_of(&client, request_code::CONTROLLER_GET_SYNC_STATE_DATA),
            vec![CONTROLLER_RPC_TIMEOUT_MILLIS]
        );
        admin.shutdown();
    }

    #[tokio::test]
    async fn controller_config_reads_properties_and_falls_back_to_name_servers() {
        let client = long_tail_client("long-tail-ctrl-config");
        client.queue_canned_reply(
            request_code::GET_CONTROLLER_CONFIG,
            reply_with_body(b"ctrSyncTopicEnable = true\nx = 1\n".to_vec()),
        );
        client.queue_canned_reply(
            request_code::GET_CONTROLLER_CONFIG,
            reply_with_body(b"other = 2\n".to_vec()),
        );
        let admin = admin_on(&client, 2000);

        // Explicit servers first.
        let configs = admin
            .get_controller_config(&["10.9.9.9:9878".to_string()])
            .await
            .expect("controller config")
            .expect("one server answered");
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].0, "10.9.9.9:9878");
        assert_eq!(configs[0].1.get("ctrSyncTopicEnable"), Some("true"));
        assert_eq!(configs[0].1.get("x"), Some("1"));

        // Without servers: the name server list takes over.
        let fallback = admin
            .get_controller_config(&[])
            .await
            .expect("fallback config")
            .expect("the configured name server list is not empty");
        assert_eq!(fallback[0].0, "127.0.0.1:9876");
        assert_eq!(fallback[0].1.get("other"), Some("2"));

        let requests = requests_of(&client, request_code::GET_CONTROLLER_CONFIG);
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|ext| ext.is_empty()));
    }

    #[tokio::test]
    async fn controller_config_with_no_servers_returns_none_without_io() {
        let admin = DefaultMQAdminExt::new();
        admin.update_config(|c| c.name_server_addrs.clear());
        let started = admin.start().await;
        assert!(started.is_err(), "no name server: start rolls back");
        // Not started, but the empty-servers short circuit fires before any IO or start check.
        let none = admin
            .get_controller_config(&[])
            .await
            .expect("no servers means no call");
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn update_controller_config_silent_on_empty_last_error_wins() {
        let client = long_tail_client("long-tail-ctrl-update");
        let admin = admin_on(&client, 2000);

        // Empty properties: silent no-op, no RPC.
        admin
            .update_controller_config(&StringMap::new(), &["10.9.9.9:9878".to_string()])
            .await
            .expect("empty properties are a no-op");
        assert!(requests_of(&client, request_code::UPDATE_CONTROLLER_CONFIG).is_empty());

        // Both controllers reject; the LAST response's code and remark win.
        let mut first = RemotingCommand::create_response(16, None);
        first.remark = Some("first failure".to_string());
        client.queue_canned_reply(request_code::UPDATE_CONTROLLER_CONFIG, first);
        let mut second = RemotingCommand::create_response(3, None);
        second.remark = Some("last failure".to_string());
        client.queue_canned_reply(request_code::UPDATE_CONTROLLER_CONFIG, second);

        let mut props = StringMap::new();
        props.insert("ctrSyncTopicEnable".to_string(), "false".to_string());
        let err = admin
            .update_controller_config(&props, &["10.9.9.9:9878".to_string(), "10.9.9.10:9878".to_string()])
            .await
            .expect_err("both controllers rejected");
        assert_eq!(err.response_code(), Some(3), "the last non-SUCCESS wins");
        assert!(err.to_string().contains("last failure"));

        let requests = requests_of(&client, request_code::UPDATE_CONTROLLER_CONFIG);
        assert_eq!(requests.len(), 2, "one request per controller");
        assert!(requests.iter().all(|ext| ext.is_empty()));
    }

    #[tokio::test]
    async fn clean_controller_broker_data_header_shape() {
        let client = long_tail_client("long-tail-ctrl-clean");
        let mut leader = reply_with_ext(&[("controllerLeaderAddress", "10.9.9.9:9878")]);
        leader.add_ext_field("isLeader", "false");
        client.queue_canned_reply(request_code::CONTROLLER_GET_METADATA_INFO, leader);
        client.queue_canned_reply(request_code::CLEAN_BROKER_DATA, success_reply());
        let admin = admin_on(&client, 2000);

        admin
            .clean_controller_broker_data(
                "10.9.9.1:9878",
                None,
                "broker-a",
                None,
                false,
            )
            .await
            .expect("clean broker data");

        let (ext, body) = logged_via_requests(&client, request_code::CLEAN_BROKER_DATA);
        assert_eq!(ext.get("brokerName"), Some("broker-a"));
        assert_eq!(ext.get("isCleanLivingBroker"), Some("false"));
        assert!(ext.contains_key("invokeTime"), "invokeTime is always sent");
        assert!(!ext.contains_key("clusterName"));
        assert!(!ext.contains_key("brokerControllerIdsToClean"));
        assert!(body.is_none());

        // The nullable fields land when the caller supplies them, and the boolean is a wire
        // string either way.
        let mut leader2 = reply_with_ext(&[("controllerLeaderAddress", "10.9.9.9:9878")]);
        leader2.add_ext_field("isLeader", "false");
        client.queue_canned_reply(request_code::CONTROLLER_GET_METADATA_INFO, leader2);
        client.queue_canned_reply(request_code::CLEAN_BROKER_DATA, success_reply());
        admin
            .clean_controller_broker_data(
                "10.9.9.1:9878",
                Some("DefaultCluster"),
                "broker-a",
                Some("ctr-1;ctr-2"),
                true,
            )
            .await
            .expect("clean broker data with the full header");
        let (ext, _) = logged_via_requests(&client, request_code::CLEAN_BROKER_DATA);
        assert_eq!(ext.get("clusterName"), Some("DefaultCluster"));
        assert_eq!(ext.get("brokerControllerIdsToClean"), Some("ctr-1;ctr-2"));
        assert_eq!(ext.get("isCleanLivingBroker"), Some("true"));
    }

    /// recorded_requests carries no body, so the body-shaped requests assert through the mock.
    fn logged_via_requests(client: &MQClientInstance, _code: i32) -> (ExtFields, Option<Vec<u8>>) {
        let ext = requests_of(client, _code);
        (ext.last().cloned().unwrap_or_default(), None)
    }

    #[tokio::test]
    async fn cold_data_update_and_remove_body_shapes() {
        let (listener, addr) = bind_admin_mock().await;
        let log = spawn_admin_broker(listener, Vec::new()).await;
        let admin = started_direct_admin(&addr).await;

        let mut props = StringMap::new();
        props.insert("coldFlowCtr.groupName".to_string(), "GID_a".to_string());
        props.insert("coldFlowCtr.enable".to_string(), "true".to_string());
        admin
            .update_cold_data_flow_ctr_group_config(&addr, &props)
            .await
            .expect("update cold data config");

        let (_, body) = logged_request(&log, request_code::UPDATE_COLD_DATA_FLOW_CTR_CONFIG);
        assert_eq!(
            body.as_deref(),
            Some(MixAll::properties_to_string(&props, false).as_bytes()),
            "the properties ride the body as k=v text"
        );

        admin
            .remove_cold_data_flow_ctr_group_config(&addr, "GID_a")
            .await
            .expect("remove cold data config");
        let (ext, body) = logged_request(&log, request_code::REMOVE_COLD_DATA_FLOW_CTR_CONFIG);
        assert!(ext.is_empty());
        assert_eq!(body.as_deref(), Some(b"GID_a".as_ref()), "the plain group string, not JSON");

        // Empty payloads skip the RPC entirely.
        let before = lock(&log).len();
        admin
            .update_cold_data_flow_ctr_group_config(&addr, &StringMap::new())
            .await
            .expect("empty properties are skipped");
        admin
            .remove_cold_data_flow_ctr_group_config(&addr, "")
            .await
            .expect("empty group is skipped");
        assert_eq!(lock(&log).len(), before, "no request went out");
        admin.shutdown();
    }

    #[tokio::test]
    async fn cold_data_info_dials_the_listening_port_verbatim_even_with_vip_on() {
        let (listener, addr) = bind_admin_mock().await;
        let log = spawn_admin_broker(
            listener,
            vec![(
                request_code::GET_COLD_DATA_FLOW_CTR_INFO,
                response_code::SUCCESS,
                Some(b"coldFlowCtr.enable=true\n".to_vec()),
                Vec::new(),
            )],
        )
        .await;
        let admin = started_direct_admin(&addr).await;
        admin.set_vip_channel_enabled(true);

        // The switch would move a rewritten broker dial two ports down; this read still lands
        // on the address the caller passed, so the mock at exactly that address serves it.
        let info = admin
            .get_cold_data_flow_ctr_info(&addr)
            .await
            .expect("cold data info without a rewrite");
        assert_eq!(info.as_deref(), Some("coldFlowCtr.enable=true\n"));
        assert_eq!(
            log_codes(&log),
            vec![request_code::GET_COLD_DATA_FLOW_CTR_INFO],
            "exactly one request, dialed verbatim"
        );

        admin.shutdown();
    }

    #[tokio::test]
    async fn read_ahead_mode_ext_key_and_remark() {
        let client = long_tail_client("long-tail-read-ahead");
        let mut ok = success_reply();
        ok.remark = Some("NORMAL".to_string());
        client.queue_canned_reply(request_code::SET_COMMITLOG_READ_MODE, ok);
        client.queue_canned_reply(request_code::SET_COMMITLOG_READ_MODE, success_reply());
        let admin = admin_on(&client, 2000);

        let remark = admin
            .set_commit_log_read_ahead_mode("10.0.0.10:10911", "NORMAL")
            .await
            .expect("read ahead mode");
        assert_eq!(remark.as_deref(), Some("NORMAL"), "the remark is the result");

        let none = admin
            .set_commit_log_read_ahead_mode("10.0.0.10:10911", "UNKNOWN")
            .await
            .expect("no remark reads as none");
        assert_eq!(none, None);

        let requests = requests_of(&client, request_code::SET_COMMITLOG_READ_MODE);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].get(READ_AHEAD_MODE_EXT_KEY), Some("NORMAL"));
        assert_eq!(requests[0].len(), 1, "READ_AHEAD_MODE is the only field");
        assert_eq!(requests[1].get(READ_AHEAD_MODE_EXT_KEY), Some("UNKNOWN"));
    }

    #[tokio::test]
    async fn pop_rollback_forwards_the_caller_timeout() {
        let (listener, addr) = bind_admin_mock().await;
        let log = spawn_admin_broker(listener, Vec::new()).await;
        let admin = started_direct_admin(&addr).await;

        admin
            .export_pop_records(&addr, 4321)
            .await
            .expect("pop rollback");

        let (ext, body) = logged_request(&log, request_code::POP_ROLLBACK);
        assert!(ext.is_empty(), "no header, no body");
        assert!(body.is_none());
        let client = admin_client(&admin);
        assert_eq!(
            timeouts_of(&client, request_code::POP_ROLLBACK),
            vec![4321],
            "the caller's timeout goes out verbatim"
        );
        admin.shutdown();
    }

    #[tokio::test]
    async fn rocksdb_check_and_export_wire_shape() {
        let client = long_tail_client("long-tail-rocksdb");
        client.queue_canned_reply(
            request_code::CHECK_ROCKSDB_CQ_WRITE_PROGRESS,
            reply_with_body(RemotingSerializable::encode(
                &CheckRocksdbCqWriteResult {
                    check_result: Some("ok".to_string()),
                    check_status: 0,
                }
                .to_json_value(),
            )),
        );
        client.queue_canned_reply(request_code::EXPORT_ROCKSDB_CONFIG_TO_JSON, success_reply());
        let admin = admin_on(&client, 2000);

        let progress = admin
            .check_rocksdb_cq_write_progress("10.0.0.10:10911", "T", 0)
            .await
            .expect("check progress");
        assert_eq!(progress.check_result.as_deref(), Some("ok"));
        assert_eq!(progress.status(), Some(CheckRocksdbCqWriteStatus::CheckOk));

        admin
            .export_rocksdb_config_to_json(
                "10.0.0.10:10911",
                &[RocksdbConfigType::Topics, RocksdbConfigType::SubscriptionGroups],
            )
            .await
            .expect("export rocksdb config");

        let checks = requests_of(&client, request_code::CHECK_ROCKSDB_CQ_WRITE_PROGRESS);
        assert_eq!(checks[0].get("topic"), Some("T"));
        assert_eq!(
            checks[0].get("checkStoreTime"),
            Some("0"),
            "the primitive long is sent even when zero"
        );
        let exports = requests_of(&client, request_code::EXPORT_ROCKSDB_CONFIG_TO_JSON);
        assert_eq!(
            exports[0].get("configType"),
            Some("topics;subscriptionGroups;"),
            "the separator trails every entry"
        );
    }

    #[tokio::test]
    async fn lite_family_wire_shapes() {
        let client = long_tail_client("long-tail-lite");
        for code in [
            request_code::GET_BROKER_LITE_INFO,
            request_code::GET_PARENT_TOPIC_INFO,
            request_code::GET_LITE_TOPIC_INFO,
            request_code::GET_LITE_CLIENT_INFO,
            request_code::GET_LITE_GROUP_INFO,
            request_code::TRIGGER_LITE_DISPATCH,
        ] {
            client.queue_canned_reply(code, success_reply());
            client.queue_canned_reply(code, success_reply());
        }
        let admin = admin_on(&client, 2000);
        let addr = "10.0.0.10:10911";

        admin.get_broker_lite_info(addr).await.expect("broker lite info");
        admin.get_parent_topic_info(addr, "T").await.expect("parent topic info");
        admin.get_lite_topic_info(addr, "T", Some("L1")).await.expect("lite topic info");
        admin.get_lite_topic_info(addr, "T", None).await.expect("lite topic info, no lite filter");
        admin
            .get_lite_client_info(addr, "T", "GID_a", Some("cid-1"))
            .await
            .expect("lite client info");
        admin
            .get_lite_client_info(addr, "T", "GID_a", None)
            .await
            .expect("lite client info without client");
        admin.get_lite_group_info(addr, "GID_a", None, 7).await.expect("lite group info");
        admin
            .trigger_lite_dispatch(addr, "GID_a", Some("cid-1"))
            .await
            .expect("trigger dispatch");

        let broker_lite = requests_of(&client, request_code::GET_BROKER_LITE_INFO);
        assert_eq!(broker_lite[0].len(), 0, "no header at all");

        let parent = requests_of(&client, request_code::GET_PARENT_TOPIC_INFO);
        assert_eq!(parent[0].get("topic"), Some("T"));

        let lite_topic = requests_of(&client, request_code::GET_LITE_TOPIC_INFO);
        assert_eq!(lite_topic[0].get("parentTopic"), Some("T"));
        assert_eq!(lite_topic[0].get("liteTopic"), Some("L1"));
        assert_eq!(
            lite_topic[1].get("liteTopic"), None,
            "the optional lite filter is omitted when absent"
        );

        let lite_client = requests_of(&client, request_code::GET_LITE_CLIENT_INFO);
        assert_eq!(lite_client[0].get("parentTopic"), Some("T"));
        assert_eq!(lite_client[0].get("group"), Some("GID_a"));
        assert_eq!(lite_client[0].get("clientId"), Some("cid-1"));
        assert_eq!(
            lite_client[0].get("maxCount"),
            Some("1000"),
            "the primitive default is always on the wire"
        );
        assert_eq!(lite_client[1].get("clientId"), None);
        assert_eq!(lite_client[1].get("maxCount"), Some("1000"));

        let lite_group = requests_of(&client, request_code::GET_LITE_GROUP_INFO);
        assert_eq!(lite_group[0].get("group"), Some("GID_a"));
        assert_eq!(lite_group[0].get("topK"), Some("7"));
        assert!(!lite_group[0].contains_key("liteTopic"));

        let dispatch = requests_of(&client, request_code::TRIGGER_LITE_DISPATCH);
        assert_eq!(dispatch[0].get("group"), Some("GID_a"));
        assert_eq!(dispatch[0].get("clientId"), Some("cid-1"));
    }

    #[tokio::test]
    async fn order_conf_non_cluster_merges_and_writes_back() {
        let client = long_tail_client("long-tail-order-conf");
        let mut current = reply_with_ext(&[("value", "b-a:2")]);
        current.add_ext_field("namespace", "ORDER_TOPIC_CONFIG");
        client.queue_canned_reply(request_code::GET_KV_CONFIG, current);
        client.queue_canned_reply(request_code::PUT_KV_CONFIG, success_reply());
        let admin = admin_on(&client, 2000);

        admin
            .create_or_update_order_conf("T1", "b-b:5", false)
            .await
            .expect("order conf update");

        let writes = requests_of(&client, request_code::PUT_KV_CONFIG);
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].get("namespace"), Some("ORDER_TOPIC_CONFIG"));
        assert_eq!(writes[0].get("key"), Some("T1"));
        assert_eq!(
            writes[0].get("value"),
            Some("b-a:2;b-b:5"),
            "the merge appends the new entry"
        );

        // A matching first token replaces the stored entry instead of appending.
        let mut replaced = reply_with_ext(&[("value", "b-a:2;b-b:5")]);
        replaced.add_ext_field("namespace", "ORDER_TOPIC_CONFIG");
        client.queue_canned_reply(request_code::GET_KV_CONFIG, replaced);
        client.queue_canned_reply(request_code::PUT_KV_CONFIG, success_reply());
        admin
            .create_or_update_order_conf("T1", "b-a:9", false)
            .await
            .expect("order conf replace");
        let writes = requests_of(&client, request_code::PUT_KV_CONFIG);
        assert_eq!(writes.len(), 2);
        assert_eq!(
            writes[1].get("value"),
            Some("b-a:9;b-b:5"),
            "the merge replaces the entry with the same first token"
        );
    }

    #[tokio::test]
    async fn list_forms_carry_an_empty_header_and_the_list_body() {
        let (listener, addr) = bind_admin_mock().await;
        let log = spawn_admin_broker(listener, Vec::new()).await;
        let admin = started_direct_admin(&addr).await;

        let topic = TopicConfig {
            topic_name: "T".to_string(),
            read_queue_nums: 4,
            write_queue_nums: 4,
            perm: 6,
            ..TopicConfig::default()
        };
        admin
            .create_and_update_topic_config_list(&addr, std::slice::from_ref(&topic))
            .await
            .expect("topic list");
        let group = SubscriptionGroupConfig {
            group_name: "GID_a".to_string(),
            ..SubscriptionGroupConfig::default()
        };
        admin
            .create_and_update_subscription_group_config_list(&addr, &[group])
            .await
            .expect("group list");

        let (_, topic_body) = logged_request(&log, request_code::UPDATE_AND_CREATE_TOPIC_LIST);
        let topic_json: Value = serde_json::from_slice(&topic_body.expect("topic body")).unwrap();
        assert_eq!(topic_json["topicConfigList"][0]["topicName"], "T");
        assert!(topic_json.get("groupConfigList").is_none());

        let (group_ext, group_body) =
            logged_request(&log, request_code::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST);
        assert!(group_ext.is_empty(), "the header stays empty");
        let group_json: Value = serde_json::from_slice(&group_body.expect("group body")).unwrap();
        assert_eq!(group_json["groupConfigList"][0]["groupName"], "GID_a");
        assert!(group_json.get("topicConfigList").is_none());

        // Empty lists are refused before any IO.
        assert!(admin.create_and_update_topic_config_list(&addr, &[]).await.is_err());
        assert!(
            admin
                .create_and_update_subscription_group_config_list(&addr, &[])
                .await
                .is_err()
        );
        admin.shutdown();
    }

    // ---------------- 401 SET_MESSAGE_REQUEST_MODE: the whole request is a JSON body ----------------

    /// Every request of one code in wire order; `logged_request` only returns the first hit.
    fn logged_requests(log: &AdminLog, code: i32) -> Vec<(ExtFields, Option<Vec<u8>>)> {
        lock(log)
            .iter()
            .filter(|(seen, _, _)| *seen == code)
            .map(|(_, ext, body)| (ext.clone(), body.clone()))
            .collect()
    }

    /// The body a 401 must carry: UTF-8 JSON the broker decodes into one object.
    fn body_json(body: &Option<Vec<u8>>) -> Value {
        let raw = body
            .as_deref()
            .expect("the request carries a body, not a set of header fields");
        serde_json::from_slice(raw).expect("the body is UTF-8 JSON")
    }

    #[tokio::test]
    async fn message_request_mode_401_sends_the_whole_request_as_a_json_body() {
        let (listener, addr) = bind_admin_mock().await;
        let log = spawn_admin_broker(listener, Vec::new()).await;
        let admin = started_direct_admin(&addr).await;

        admin
            .set_message_request_mode(&addr, "T", "GID_a", "POP_PINNER", 8)
            .await
            .expect("set message request mode");

        let (ext, body) = logged_request(&log, request_code::SET_MESSAGE_REQUEST_MODE);
        assert!(
            ext.is_empty(),
            "the broker reads this request out of the body; a header field is ignored there"
        );
        let json = body_json(&body);
        // Exact object equality: all four keys, nothing else, the caller's values.
        assert_eq!(
            json,
            json!({
                "topic": "T",
                "consumerGroup": "GID_a",
                "mode": "POP_PINNER",
                "popShareQueueNum": 8,
            })
        );
        assert_eq!(json.as_object().expect("one object").len(), 4);
        assert!(
            json["popShareQueueNum"].is_number(),
            "the queue count is a JSON number, not a string"
        );
        admin.shutdown();
    }

    /// The paired control of the header form: the four names must never reach the custom header,
    /// and a dropped body means the request carries no payload the broker can read at all.
    #[tokio::test]
    async fn message_request_mode_401_values_never_ride_the_header() {
        let (listener, addr) = bind_admin_mock().await;
        let log = spawn_admin_broker(listener, Vec::new()).await;
        let admin = started_direct_admin(&addr).await;

        admin
            .set_message_request_mode(&addr, "T", "GID_a", "POP_PINNER", 8)
            .await
            .expect("set message request mode");

        let requests = logged_requests(&log, request_code::SET_MESSAGE_REQUEST_MODE);
        assert_eq!(requests.len(), 1);
        for name in ["topic", "consumerGroup", "mode", "popShareQueueNum"] {
            assert!(
                !requests[0].0.contains_key(name),
                "{name} belongs in the body; the broker reads no header field here"
            );
        }
        let body = requests[0]
            .1
            .clone()
            .expect("the request has a body; a body-less 401 leaves the broker nothing to decode");
        assert!(!body.is_empty(), "the body carries the whole request");
        admin.shutdown();
    }

    /// `popShareQueueNum` is part of the decoded object whatever its value: 0 is a request to use
    /// zero shared queues, not an omission, so the key stays in the body.
    #[tokio::test]
    async fn message_request_mode_401_carries_pop_share_queue_num_even_at_zero() {
        let (listener, addr) = bind_admin_mock().await;
        let log = spawn_admin_broker(listener, Vec::new()).await;
        let admin = started_direct_admin(&addr).await;

        admin
            .set_message_request_mode(&addr, "T", "GID_a", "POP_PINNER", 0)
            .await
            .expect("zero is a legal queue count");
        admin
            .set_message_request_mode(&addr, "T", "GID_a", "POP_PINNER", 4)
            .await
            .expect("non-zero queue count");

        let requests = logged_requests(&log, request_code::SET_MESSAGE_REQUEST_MODE);
        assert_eq!(requests.len(), 2);
        assert_eq!(
            body_json(&requests[0].1),
            json!({
                "topic": "T",
                "consumerGroup": "GID_a",
                "mode": "POP_PINNER",
                "popShareQueueNum": 0,
            }),
            "the zero case still sends the key"
        );
        assert_eq!(
            body_json(&requests[1].1),
            json!({
                "topic": "T",
                "consumerGroup": "GID_a",
                "mode": "POP_PINNER",
                "popShareQueueNum": 4,
            })
        );
        assert!(requests.iter().all(|(ext, _)| ext.is_empty()));
        admin.shutdown();
    }

    /// The mode goes out as the caller's string: no fallback name is substituted for an empty one.
    #[tokio::test]
    async fn message_request_mode_401_mode_is_the_callers_string() {
        let (listener, addr) = bind_admin_mock().await;
        let log = spawn_admin_broker(listener, Vec::new()).await;
        let admin = started_direct_admin(&addr).await;

        admin
            .set_message_request_mode(&addr, "T", "GID_a", "", 1)
            .await
            .expect("empty mode");
        admin
            .set_message_request_mode(&addr, "T", "GID_a", "NOT_POP", 1)
            .await
            .expect("plain mode");

        let requests = logged_requests(&log, request_code::SET_MESSAGE_REQUEST_MODE);
        assert_eq!(requests.len(), 2);
        assert_eq!(
            body_json(&requests[0].1)["mode"],
            Value::String(String::new()),
            "an empty mode stays empty instead of becoming an invented default"
        );
        assert_eq!(body_json(&requests[1].1)["mode"], "NOT_POP");
        admin.shutdown();
    }
}
