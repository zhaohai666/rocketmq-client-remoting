//! 覆盖：Topic 增删查与配置、Broker 集群信息 / 运行时信息 / 配置、NameServer KV
//! 配置、订阅组管理、消费者与生产者连接、消费统计、位点管理（含真实 broker 端
//! 重置）、消息查询（key / uniqKey / msgId / ConsumeQueue）、单条消息试消费
//! （CONSUME_MESSAGE_DIRECTLY(309) 发起端）。

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
/// 管理端默认实例名固定为 `"ADMIN"`。
pub const DEFAULT_INSTANCE_NAME: &str = "ADMIN";
const ADMIN_OP_TIMEOUT_MILLIS: i64 = 5000;
/// `query_message_by_key(max_num=32)` 与 `query_message_by_uniq_key` 写死的 32。
pub const DEFAULT_QUERY_MESSAGE_MAX_NUM: i32 = 32;

/// 顺序 topic 配置所在的 NameServer KV namespace。
pub const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";
/// `get_all_subscription_group` 的分页上限 `maxGroupNum`。
const MAX_GROUP_NUM: i32 = 10_000;
/// `query_message_by_uniq_key` 的查询窗口上界（`now + 60 * 60 * 1000`）。
const UNIQ_QUERY_WINDOW_MILLIS: i64 = 60 * 60 * 1000;

/// 管理端配置。
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

/// 分号分隔，逐项 trim、丢空。
fn split_addrs(addr: &str) -> Vec<String> {
    addr.split(';')
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(str::to_string)
        .collect()
}

/// 用 `RemotingCommand::create_request_command(code, None)` 建请求，再填 extFields/body。
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

/// 222 `INVOKE_BROKER_TO_RESET_OFFSET` 的 extFields。
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

/// 缺字段/非数字都当 0。
fn ext_int(response: &RemotingCommand, key: &str) -> i64 {
    response
        .get_ext_field(key)
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

// ---------------------------------------------------------------- 纯逻辑 helper
// 抽出来只为让「Vec 表」这几个最容易写错的地方能离线锁死；调用方只有一处。

/// 不给集群名回全部 broker；给了就按
/// `clusterAddrTable[cluster]` 的 brokerName（排序、去重）取地址（每台 broker 的
/// 地址按字符串排序），一个都没取到再退回全量。
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

/// 用户 topic 配置的过滤：系统 topic（broker 上报的 + 本地判定）
/// 一律剔除；`special_topic=false` 时再剔除 `%RETRY%` / `%DLQ%`。
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

/// 合并分页返回的订阅组表：同名覆盖、保持首现顺序。
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

/// 两边都是 JSON object 时按键覆盖。
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

/// topic 统计合并：位点表按 key 覆盖，`topic_put_tps` 累加。
fn merge_topic_stats(merged: &mut TopicStatsTable, part: TopicStatsTable) {
    for (key, offset) in part.offset_table {
        match merged.offset_table.iter_mut().find(|(k, _)| *k == key) {
            Some(entry) => entry.1 = offset,
            None => merged.offset_table.push((key, offset)),
        }
    }
    merged.topic_put_tps += part.topic_put_tps;
}

/// 按 brokerName 过滤 `route.queue_datas` 后
/// 展开 `range(read_queue_nums)`。
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

/// 真正的请求超时：显式给了用给的，否则用配置的 `timeout_millis`。
fn effective_timeout(configured: i64, explicit: Option<i64>) -> i64 {
    explicit.filter(|t| *t > 0).unwrap_or(configured)
}

// ---------------------------------------------------------------- 消息轨迹 DTO

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

/// 用法：`set_namesrv_addr(..)` → `start().await` → 调各种 `examine*/get*/create*/delete*`
/// → `shutdown()`。克隆出的副本共享同一份状态与同一条连接。
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
/// 默认配置构造管理端。
    pub fn new() -> DefaultMQAdminExt {
        DefaultMQAdminExt::with_config(AdminConfig::default())
    }

/// 带 RPC 钩子构造管理端。
    pub fn with_rpc_hook(rpc_hook: Option<Arc<dyn RPCHook>>) -> DefaultMQAdminExt {
        let admin = DefaultMQAdminExt::new();
        admin.set_rpc_hook(rpc_hook);
        admin
    }

    /// 直接以一份完整配置构造。
    pub fn with_config(cfg: AdminConfig) -> DefaultMQAdminExt {
        DefaultMQAdminExt {
            inner: Arc::new(AdminInner {
                cfg: RwLock::new(cfg),
                ..Default::default()
            }),
        }
    }

    // ---------------- 配置与生命周期 ----------------

/// 当前配置快照。
    pub fn config(&self) -> AdminConfig {
        read_cfg(&self.inner.cfg)
    }

/// 用闭包就地改配置。
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

/// 地址列表用分号拼回成单串。
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

    /// 幂等、必须有 name server，`client_id` 缺省时现造。
    ///
    /// admin 的 clientId 同样是秒级 `instanceName@yyyyMMddHHmmss`；
    /// 同一秒起两个 admin 会撞 clientId，但 admin 用的是**私有实例**，
    /// 后建的会直接覆盖实例表里的登记，互不影响收发。
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

        // 直接构造私有实例（见模块头差异 2）。
        let client = MQClientInstance::with_config(
            &client_id,
            cfg.name_server_addrs.clone(),
            MQClientInstanceConfig {
                unit_name: cfg.unit_name.clone(),
                namespace_v2: cfg.namespace_v2.clone(),
                enable_stream_request_type: cfg.enable_stream_request_type,
                route_refresh_interval_millis: cfg.poll_name_server_interval_millis,
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
        // 动态 name server：实例可能已从地址服务器拿到地址，回填。
        let addrs = client.name_server_addrs();
        if !addrs.is_empty() {
            self.update_config(|c| c.name_server_addrs = addrs);
        }
        *lock(&self.inner.client) = Some(client);
        Ok(())
    }

    /// 幂等，只拆自己那份私有实例。
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

    // ---------------- 底层调用助手 ----------------

    /// 发到指定 Broker/NameServer 并校验 SUCCESS。
    async fn invoke_broker(
        &self,
        addr: &str,
        code: i32,
        ext: &StringMap,
        body: Option<Vec<u8>>,
        timeout_millis: Option<i64>,
    ) -> Result<RemotingCommand> {
        let client = self.require_client()?;
        let timeout = effective_timeout(self.timeout_millis(), timeout_millis);
        let addr = &MixAll::broker_vip_channel(read_cfg(&self.inner.cfg).vip_channel_enabled, addr);
        let mut request = build_request(code, ext, body);
        let response = client.invoke_sync(addr, &mut request, timeout).await?;
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

    /// 从集群信息里取第一台 broker 地址。
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

    // ---------------- Topic 管理 ----------------

    /// 走默认 topic 的路由逐 broker 下发（一台成功即可）。
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

    /// 显式给定 NameServer 地址的同步调用（不走 VIP 通道——`invoke_broker` 开 VIP
    /// 时会把端口 -2，NameServer 端口不能跟着改）。
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

    /// 不给地址列表就清自己配的全部 NameServer。
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

    /// 旧名兼容：任取一台成功的 NameServer 删除 topic 路由。
    pub async fn delete_topic_in_namesrv(&self, topic: &str) -> Result<()> {
        let client = self.require_client()?;
        client
            .delete_topic_in_namesrv(topic, self.timeout_millis())
            .await
    }

    /// 先清各 broker，再清 NameServer 路由，最后清 KV namespace；
    /// 三段都是「失败只告警、继续往下走」。
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

    /// `GET_TOPICS_BY_CLUSTER` 打到 NameServer，按集群名取 topic 列表。
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

    /// 含该 topic 路由 broker 的集群名集合。
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

    /// `get_cluster_list` 的别名。
    pub async fn get_topic_cluster_list(&self, topic: &str) -> Result<Vec<String>> {
        self.get_cluster_list(topic).await
    }

    /// 遍历 NameServer 的 topic 列表逐个拉路由，
    /// 单个失败直接跳过。
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

    /// `GET_TOPIC_CONFIG` 单查 topic 配置，body 为 TopicConfig JSON。
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

    /// 剔除系统 topic 与 `%RETRY%` / `%DLQ%`。
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

    /// 遍历该 topic 全部 broker 合并统计。
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
                    // 只 warn 后继续（broker 可能没开统计）。
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

    // ---------------- 集群 / Broker ----------------

    pub async fn fetch_broker_cluster_info(&self) -> Result<ClusterInfo> {
        let client = self.require_client()?;
        client.get_broker_cluster_info(self.timeout_millis()).await
    }

    /// `fetch_broker_cluster_info` 的别名。
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

    /// 响应体是 **properties 文本**，不是 JSON/KVTable。
    pub async fn get_broker_config(
        &self,
        broker_addr: &str,
        timeout_millis: Option<i64>,
    ) -> Result<StringMap> {
        let response = self
            .invoke_broker(
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

    /// 更新 broker 配置（含 brokerPermission 合法性校验）。
    ///
    /// 校验排在任何 IO 之前（发请求是整个流程的最后一步），
    /// 所以非法值在未 start 时也会以 `NO_PERMISSION`(16) 报出来。
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

    /// 读响应头的 `wipeTopicCount`。
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

    /// 读响应头的 `addTopicCount`。
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

    /// 逐 broker 下发 `CLEAN_UNUSED_TOPIC`，
    /// 任一失败（含非 SUCCESS）则整体回 `false`（不抛）。
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

    // ---------------- NameServer KV 配置 ----------------

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

    /// 非 SUCCESS 回 `None`（不抛）。
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

    /// 广播全部 NameServer 删除 KV 配置。
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

    // ---------------- 订阅组管理 ----------------

    /// body 为 config JSON。
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

    /// 拉全部订阅组后取目标 group。
    pub async fn examine_subscription_group_config(
        &self,
        addr: &str,
        group: &str,
    ) -> Result<Option<SubscriptionGroupConfig>> {
        let wrapper = self.get_all_subscription_group(addr, None).await?;
        Ok(wrapper.get(group).cloned())
    }

    /// `GET_SUBSCRIPTIONGROUP_CONFIG` 单查订阅组配置。
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

    /// **分页**累积，直到 `groupSeq >= totalGroupNum-1`。
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

            // 老 broker：不带 totalGroupNum，一次返回全部。
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

    /// 剔掉系统组与预定义组。
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

    // ---------------- 消费者 / 生产者连接 ----------------

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

    /// `examine_consumer_connection_info` 的别名。
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

    /// 拉消费者运行信息（broker 转发给目标客户端的 38 号请求，
    /// 本实例侧由 `MQClientInstance` 的 307 处理器应答，见 `live_client_modules`）。
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

    /// `examine_consumer_running_info` 的别名。
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

    // ---------------- 消费统计 ----------------

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

    /// 300 号请求，取响应头 `groupList`。
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

    /// broker 端走 `AdminBrokerProcessor#queryTopicsByConsumer:2421` →
    /// `ConsumerOffsetManager#whichTopicByConsumer`：**从位点表**（`topic@group` 键）反查该组
    /// 消费过哪些 topic。所以组从没提交过位点时回空表，这是预期而不是 bug。
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

    /// 345 号请求：查订阅关系是否一致。
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

    /// 223 号请求，取响应头 `consumerTable`。
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

    /// 314 号请求：克隆消费组位点。
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

    // ---------------- Offset 管理 ----------------

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

    /// 与 LOWER 的差异只在多条消息共享 storeTime、或时间戳落在空档/队尾时可见：
    /// 队尾之后 UPPER 回最后一条自己的位点，LOWER 回它的下一个位点（maxOffset）。
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

    /// 没提交过位点回 `None`（`QUERY_NOT_FOUND`）。
    pub async fn examine_consumer_offset(
        &self,
        consumer_group: &str,
        mq: &MessageQueue,
    ) -> Result<Option<i64>> {
        self.require_client()?
            .query_consumer_offset(consumer_group, mq, self.timeout_millis(), None, false)
            .await
    }

    /// 更新消费者位点（addr 由路由解析）。
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

    /// 更新消费者位点（指定 addr）。
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
        // LMQ / wheel_timer 这类没有独立路由的 topic，按集群名去查路由，
        // 但下发给 broker 的 `topic` 仍是原值。
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

    /// 一笔 `INVOKE_BROKER_TO_RESET_OFFSET`(222)，回 broker 实际重置的队列表。
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

    /// 先试新版（broker 端重置），消费者不在线再退化到旧版。
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

    /// 逐队列 `searchOffset` 后按 force 决策写回。
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
                // 查位点失败当 0（该队列压根没提交过位点是常态）。
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

    // ---------------- 消息查询 ----------------

    /// 按 key 查消息（查所有 broker + 客户端侧 key 二次校验）。
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

    /// `indexType="U"` + `_UNIQUE_KEY_QUERY="true"`。
    ///
    /// 注意：broker 侧 uniqKey 索引只有 RocksDB 索引实现（`IndexRocksDBStore`）支持；
    /// 默认的文件索引下该查询可能返回空，这属于 broker 配置差异而非客户端问题。
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

    /// 按 key 查消息（NORMAL 索引模式）。
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

    /// `MQClientInstance::query_message_all_brokers` 的门面壳。未 start 时
    /// 直接抛错；indexType 由 `uniq_key` 唯一决定，
    /// 三个查询入口也只有这两种组合。
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

        // uniq 查询放在错误兜底之外：该路径真抛时直接透传，
        // 只有「查到了但为空」才落到最终的 NO_MESSAGE。
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

    /// 321 号请求：查 ConsumeQueue。
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


    /// `UPDATE_AND_CREATE_TOPIC_LIST(18)`。
    ///
    /// wire 事实：custom header 为**空**（topic 名在 body 里做授权资源），
    /// body 是 CreateTopicListRequestBody JSON：{"topicConfigList": [...]}。
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

    /// 225 号请求：批量更新订阅组配置。
    /// 与 topic-list 同形，但 body key 是 `groupConfigList`。
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

    /// `UPDATE_AND_CREATE_STATIC_TOPIC(513)`：
    /// header 复用 CreateTopicRequestHeader 字段，body 是 TopicQueueMappingDetail JSON。
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

    /// `UPDATE_AND_GET_GROUP_FORBIDDEN(353)`。
    /// `readable=None` 表示仅查询；响应体是 GroupForbidden JSON。
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

    /// `RESUME_CHECK_HALF_MESSAGE(323)`。
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

    /// **不是独立请求**，是 NameServer KV
    /// namespace=ORDER_TOPIC_CONFIG 上的读改写。非集群模式把存储值当作 ";"
    /// 分隔的 "topic:conf" 列表，替换匹配条目后整体写回。
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

    // ---------------- 运维清理类 ----------------

    /// `CLEAN_EXPIRED_CONSUMEQUEUE(306)`。
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

    /// 逐个地址执行，返回**失败的地址**。
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

    /// `DELETE_EXPIRED_COMMITLOG(329)`。
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

    /// 逐个地址执行，返回**失败的地址**。
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

    /// **单请求** CLEAN_UNUSED_TOPIC(316)，
    /// 由 broker 自行清理未使用 topic。
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

    /// `QUERY_CONSUME_TIME_SPAN(303)`：
    /// 按路由遍历 master，聚合各 broker 的 consumeTimeSpanSet。
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

    // ---------------- NameServer 配置（318/319） ----------------

    /// `UPDATE_NAMESRV_CONFIG(318)`：
    /// properties 以 k=v\n 文本进 body，广播每个 NameServer，任一失败即抛。
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

    /// `GET_NAMESRV_CONFIG(319)`：逐个 NameServer
    /// 查询，body 是 properties 文本；返回 {地址: properties}。
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

    /// `SET_MESSAGE_REQUEST_MODE(401)`。
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

/// 从 JSON body 的 `key` 里取字符串数组，缺字段当空表。
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
    fn config_defaults_match_python() {
        let admin = DefaultMQAdminExt::new();
        let cfg = admin.config();
        assert_eq!(cfg.instance_name, "ADMIN");
        assert_eq!(cfg.timeout_millis, DEFAULT_TIMEOUT_MILLIS);
        assert_eq!(DEFAULT_TIMEOUT_MILLIS, 15_000);
        assert!(cfg.name_server_addrs.is_empty());
        assert!(cfg.kv_namespace_to_delete_list.is_empty());
        assert_eq!(cfg.namespace_v2, None, "namespaceV2 默认不设");
        assert!(!admin.is_started());
        assert_eq!(admin.client_id(), "");
        assert!(!cfg.vip_channel_enabled);
    }

    #[test]
    fn namespace_v2_setter_getter_round_trip() {
        let admin = DefaultMQAdminExt::new();
        assert_eq!(admin.get_namespace_v2(), None, "默认不设");
        admin.set_namespace_v2(Some("NS_V2"));
        assert_eq!(admin.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(admin.config().namespace_v2, Some("NS_V2".to_string()));
        admin.set_namespace_v2(None);
        assert_eq!(
            admin.get_namespace_v2(),
            None,
            "None = 清除"
        );
    }

    /// VIP 通道开关：set 之后生效（默认 false，配合 mix_all 的
    /// `broker_vip_channel` 端口减 2 测试覆盖取址语义）。
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
        // 分号分隔，逐项 trim、丢空
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
        assert!(!ext.contains_key("force"), "force 不是服务端的字段名");
        assert_eq!(ext.get("timestamp"), Some("-1"));
        assert_eq!(ext.get("offset"), Some("-1"));
        assert!(
            !ext.contains_key("queueId"),
            "整个 topic 的重载不带 queueId"
        );

        // 负向对照：isForce=false 也必须下发（不能"假值省略"），单队列重载带上 queueId
        let single = reset_offset_ext_pairs("T", "G", 0, false, Some(3), Some(7));
        assert_eq!(single.get("isForce"), Some("false"));
        assert!(!single.contains_key("force"));
        assert_eq!(single.get("queueId"), Some("3"));
        assert_eq!(single.get("offset"), Some("7"));
        assert_eq!(single.get("timestamp"), Some("0"));
    }

    /// 未配 name server 时 `start()` 抛错、且**不**留在 started。
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

    /// 未 start 的每个入口都得是「admin not started」，而不是裸 panic / 连接错。
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
        // 查询类同样要 start（未 start 直接报错），不能静默回空集。
        assert!(admin.query_message("T", "k", 32, 0, 1).await.is_err());
        assert!(admin.query_message_by_uniq_key("T", "k").await.is_err());
        assert!(admin.query_message_by_key("T", "k", 32).await.is_err());
    }

    /// 非法 brokerPermission 必须**在任何 IO 之前**报出来（校验先于发请求），
    /// 所以未 start 的 admin 也给 16。
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

        // 合法值走到 IO 才失败 ⇒ 证明校验排在前、且真值放行。
        props.insert("brokerPermission", "6");
        let err = admin
            .update_broker_config("127.0.0.1:10911", &props, None)
            .await
            .expect_err("not started yet");
        assert!(err.to_string().contains("admin not started"));

        // 空 properties：直接 return，不发请求也不报错。
        assert!(admin
            .update_broker_config("127.0.0.1:10911", &StringMap::new(), None)
            .await
            .is_ok());
    }

    #[test]
    fn cluster_broker_addrs_covers_python_branches() {
        let info = cluster();
        // 不给集群名：回 `cluster.get_broker_addrs()`（全量）。
        assert_eq!(
            cluster_broker_addrs(&info, None),
            info.get_broker_addrs(),
            "None must fall through to all brokers"
        );
        // 给了集群名：brokerName 排序去重，每台地址按字符串排序。
        assert_eq!(
            cluster_broker_addrs(&info, Some("DefaultCluster")),
            vec![
                "10.0.0.10:10911".to_string(),
                "10.0.0.20:10911".to_string(),
                "10.0.0.21:10911".to_string(),
            ]
        );
        // 未知集群名 / 空串都退回全量。
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
        // 过滤条件是「broker 上报的系统 topic 列表 ∪ `MixAll::is_sys_topic`」，
        // 而 is_sys_topic 只认 `rmq_sys_` 前缀，所以
        // TBW102 / SCHEDULE_TOPIC_XXXX 只能靠 GET_SYSTEM_TOPIC_LIST_FROM_BROKER 剔除。
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
        // broker 上报项、rmq_sys_ 前缀项、%RETRY% / %DLQ% 全部剔除，只剩业务 topic。
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

        // 不在 broker 系统列表、也不带 rmq_sys_ 前缀的裸名会保留。
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
        // 同名覆盖、顺序保持首现。
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
        // 非 object（老 broker 回 null）直接覆盖。
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
    fn small_helpers_match_python_semantics() {
        // 显式超时优先，0/缺省回落配置值
        assert_eq!(effective_timeout(15_000, Some(3_000)), 3_000);
        assert_eq!(effective_timeout(15_000, Some(0)), 15_000);
        assert_eq!(effective_timeout(15_000, None), 15_000);
        // 布尔转 `"true"` / `"false"`
        assert_eq!((bool_flag(true), bool_flag(false)), ("true", "false"));

        // 缺字段与非数字都是 0
        let mut cmd = RemotingCommand::new();
        assert_eq!(ext_int(&cmd, "wipeTopicCount"), 0);
        cmd.add_ext_field("wipeTopicCount", " 12 ");
        assert_eq!(ext_int(&cmd, "wipeTopicCount"), 12);
        cmd.add_ext_field("addTopicCount", "abc");
        assert_eq!(ext_int(&cmd, "addTopicCount"), 0);

        // 只收字符串元素，缺字段当空表
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
        // uniqKey 兜底放在错误吞掉之外：未 start 时透传
        // `_require_client` 的客户端错，而不是 NO_MESSAGE(208)。208 只在真正
        // 查不到消息时出现，那条断言放在 live_admin 示例里（需要真实 broker）。
        assert_eq!(view_err.response_code(), None);
        assert!(view_err
            .to_string()
            .starts_with("MQClientException: admin not started"));
    }

    // ---------------- 309 consume_message_directly（假 broker 对拍） ----------------

    /// 假 broker 收到的请求流水：`(请求码, extFields 快照)`。
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

    /// 带 `opaque` 的应答（客户端按 opaque 配对，串了就当噪声丢掉）。
    fn reply_for(request: &RemotingCommand, code: i32, body: Option<Vec<u8>>) -> RemotingCommand {
        let mut response = RemotingCommand::create_response(code, None);
        response.opaque = request.opaque;
        response.serialize_type_current_rpc = request.serialize_type_current_rpc;
        if let Some(body) = body {
            response.set_body(Some(body));
        }
        response
    }

    /// 起 309 对拍假 broker：`VIEW_MESSAGE_BY_ID(33)` 回 `view_body`，
    /// `CONSUME_MESSAGE_DIRECTLY(309)` 按脚本 `(应答码, body)` 应答并记下 extFields，
    /// 其余请求（admin 启动后的路由刷新等）一律 SUCCESS。
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

    /// 编一条 `storeHost = 127.0.0.1:store_port`、commitLog 偏移 `offset` 的消息，
    /// 返回 `(33 的应答 body, 该消息本体的 offset msgId)`。
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
            encode_message_ext(&ext, false).expect("编码夹具消息"),
            create_message_id(
                &ip_and_port_to_bytes("127.0.0.1", u32::from(store_port), false).expect("夹具地址"),
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
        admin.start().await.expect("对着假 broker start 应当成功");
        admin
    }

    fn direct_ext(log: &DirectLog, n: usize) -> Vec<(String, String)> {
        lock(log)[n]
            .1
            .clone()
    }

    /// 全链路对拍：msgId 是合法 offset msgId → 33 拿到消息（带 UNIQ_KEY）→ 309 打到
    /// 消息的 storeHost 且头里的 msgId 换成**消息本体**的 offset msgId → body 解码回
    /// [`ConsumeMessageDirectlyResult`]。
    ///
    /// 夹具消息的 commitLog 偏移故意（4096）不同于调用方 msgId 里编的（1024）：
    /// 309 若透传了调用方的 msgId，msgId 断言立刻能看出来。
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
            .expect("309 全链路应当成功");
        assert_eq!(result.consume_result.as_deref(), Some(CMResult::CR_SUCCESS));
        assert!(result.auto_commit, "autoCommit 初值 true");
        assert_eq!(result.spent_time_mills, 42);
        assert_eq!(result.remark.as_deref(), Some("ok"));

        let snapshot = log.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(snapshot.len(), 2, "先 33 后 309，各一笔");
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
            "带 UNIQ_KEY 的消息必须换成消息本体的 offset msgId"
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
            .expect("无 UNIQ_KEY 同样应当成功");
        let ext = direct_ext(&log, 1);
        let get = |key: &str| ext.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
        assert_eq!(
            get("msgId"),
            Some(msg_id.as_str()),
            "没有 UNIQ_KEY 原样透传"
        );
        drop(ext);
        admin.shutdown();
    }

    /// 309 的 broker 业务码不进任何重试链：原样带码抛出（`Error::Broker`）。
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
            .expect_err("broker 业务码必须原样抛");
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
            .expect_err("空 body 必须报错");
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
