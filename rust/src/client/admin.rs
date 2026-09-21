//! 管理端（对应 Python `client/admin.py` 的 `DefaultMQAdminExt`，上游是 Java
//! `org.apache.rocketmq.client.admin.DefaultMQAdminExt` +
//! `org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl`）。
//!
//! 覆盖：Topic 增删查与配置、Broker 集群信息 / 运行时信息 / 配置、NameServer KV
//! 配置、订阅组管理、消费者与生产者连接、消费统计、位点管理（含真实 broker 端
//! 重置）、消息查询（key / uniqKey / msgId / ConsumeQueue）。
//!
//! 对齐要点（与 `admin.py` 模块头同源，均为 Java 5.x 探针 + 源码核对结论）：
//! - `GET_BROKER_CONFIG` 的 body 是 **properties 文本**（`k=v\n`），不是 JSON。
//! - `UPDATE_AND_CREATE_SUBSCRIPTIONGROUP` 的 body 是 SubscriptionGroupConfig JSON。
//! - `GET_TOPIC_CONFIG` 请求头带 `topic` + `lo`，响应体是 TopicConfigAndQueueMapping JSON。
//! - `GET_ALL_SUBSCRIPTIONGROUP_CONFIG` 是**分页**接口（groupSeq / maxGroupNum / dataVersion）。
//! - `ResetOffsetBody.offsetTable` 是 `Map<MessageQueue, Long>`。
//! - KV 类请求打到 **NameServer**，且 PUT/DELETE 要广播到**每一台** NameServer。
//!
//! # 与 Python 参考实现的刻意差别
//!
//! 1. **RPC 全部 `async`**：与其它 facade 同因（tokio 运行时）。
//!    [`DefaultMQAdminExt::shutdown`] 保持同步。
//! 2. **实例是私有的**：Python `admin.py:92` 直接 `MQClientInstance(client_id, addrs)`
//!    构造（不走 `create_mq_client_instance` 的复用工厂），这里照抄 —— 因此 admin 的
//!    `shutdown()` 只会拆掉自己那份实例，不会连带同 clientId 的 producer/consumer。
//!    （Java 走 `MQClientManager` 共享 + `adminExtTable` 守卫；本移植的 producer 与
//!    consumer 侧现在也有守卫了，见 `mq_client.rs` 模块头差异 7，admin 不依赖它。）
//! 3. **没有默认参数**：Python 的 `timeout_millis=None` / `topic=None` / `count=32`
//!    在这里分成 `Option<...>` 与显式实参，常量（[`DEFAULT_TIMEOUT_MILLIS`]、
//!    [`DEFAULT_QUERY_MESSAGE_MAX_NUM`]）给出同一默认值。
//! 4. **`clean_unused_topic` 不带 `topic` 形参**：Python 收了它却从不下发（Java 签名里
//!    也只有 clusterName），留着只会让人误以为能按 topic 清理。
//! 5. **不定形 JSON 直接回 [`serde_json::Value`]**：`view_broker_stats_data` /
//!    `query_subscription` / `get_consume_status` 在 Python 里是 `dict`，本仓库对
//!    这类无固定 schema 的 body 既有口径就是 `Value`（见 `ConsumeStatsList`、
//!    `SubscriptionGroupWrapper.forbidden_table`）。
//! 6. **`query_topic_consume_by_who` 回 `Vec<String>` 而非集合**：Python 的 `set`
//!    丢掉了 broker 的返回顺序，这里保持原序（同 `ClusterInfo` 的处理口径）。
//! 7. **`_perm_is_valid` 的 `NumberFormatException` 等价 `false`**：见
//!    [`PermName::is_valid_str`]，与 Python `admin._perm_is_valid` 同口径。
//! 8. **位点/消息类 RPC 一律复用实例层 helper**：Python 在 admin 里手拼
//!    `GET_EARLIEST_MSG_STORETIME` 并用 `_broker_addr_for_mq` 解析地址
//!    （`admin.py:716`），这里走 `MQClientInstance::get_earliest_msg_store_time`
//!    —— 同一份请求头、同一份响应解析，地址解析口径与其它位点 RPC 完全一致。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Instant;

use serde_json::{json, Value};

use crate::client::mq_client::{MQClientInstance, MQClientInstanceConfig};
use crate::common::message::{MessageExt, MessageQueue};
use crate::common::message_const::{INDEX_KEY_TYPE, INDEX_UNIQUE_TYPE};
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
    ClusterInfo, ConsumeStatsList, ConsumerConnection, ConsumerRunningInfo,
    GetConsumerListByGroupResponseBody, KVTable, ProducerConnection, ResetOffsetBody, TopicList,
};
use crate::remoting::protocol::codes::{language_code, request_code, response_code};
use crate::remoting::protocol::ext_fields::{ExtFields, StringMap};
use crate::remoting::protocol::remoting_command::RemotingCommand;
use crate::remoting::protocol::route::TopicRouteData;
use crate::remoting::protocol::serialize::RemotingSerializable;
use crate::remoting::protocol::subscription::{SubscriptionGroupConfig, SubscriptionGroupWrapper};
use crate::remoting::rpchook::RPCHook;
use crate::{bail, rmq_warn};

/// Python `admin.DEFAULT_TIMEOUT`（Java `DefaultMQAdminExt.DEFAULT_TIMEOUT = 5000 * 3`）。
pub const DEFAULT_TIMEOUT_MILLIS: i64 = 5000 * 3;
/// Python `DefaultMQAdminExt.instance_name = "ADMIN"`。
pub const DEFAULT_INSTANCE_NAME: &str = "ADMIN";
/// Python `create_topic` / `examine_topic_route` 走 client helper 时的固定超时
/// （`mq_client.py:create_topic_in_route(timeout_millis=5000)`，对应 Java
/// `MQAdminImpl.timeoutMillis`）—— 这条**不吃** [`AdminConfig::timeout_millis`]。
const ADMIN_OP_TIMEOUT_MILLIS: i64 = 5000;
/// Python `query_message_by_key(max_num=32)` 与 `query_message_by_uniq_key` 写死的 32。
pub const DEFAULT_QUERY_MESSAGE_MAX_NUM: i32 = 32;
/// Python `get_all_subscription_group` 的 `maxGroupNum`。
const MAX_GROUP_NUM: i32 = 10_000;
/// Python `query_message_by_uniq_key` 的查询窗口上界（`now + 60 * 60 * 1000`）。
const UNIQ_QUERY_WINDOW_MILLIS: i64 = 60 * 60 * 1000;

/// 管理端配置（对应 Python `DefaultMQAdminExt.__init__` 的那几个属性）。
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Python `self.namespace`：与参考实现一样**只做登记、不参与 topic 拼接**
    /// （`admin.py` 全文没有用到它；Java 的管理端同样没有 namespace）。
    pub namespace: String,
    pub instance_name: String,
    pub client_id: Option<String>,
    /// Java `ClientConfig#unitName`（默认 null）：非空时进 clientId 后缀，
    /// 并作为地址服务器 URL 的 `-<unitName>` 段。
    pub unit_name: Option<String>,
    /// Java `ClientConfig#enableStreamRequestType`：true 时每个请求带 `ReqT=0`，
    /// clientId 末尾多一段 `@STREAM`。
    ///
    /// ⚠ admin 没有 `unitMode` 的落点：管理端不发普通消息、也不做消息过滤，
    /// Java 的 `DefaultMQAdminExtImpl` 全程没读过 `isUnitMode()`，所以这里不设该字段。
    pub enable_stream_request_type: bool,
    pub name_server_addrs: Vec<String>,
    pub timeout_millis: i64,
    /// Java `kvNamespaceToDeleteList`：`delete_topic` 时顺带清掉的 KV namespace。
    pub kv_namespace_to_delete_list: Vec<String>,
}

impl Default for AdminConfig {
    fn default() -> AdminConfig {
        AdminConfig {
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            unit_name: None,
            enable_stream_request_type: false,
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

/// Python `set_namesrv_addr`：分号分隔，逐项 trim、丢空。
fn split_addrs(addr: &str) -> Vec<String> {
    addr.split(';')
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(str::to_string)
        .collect()
}

/// Python `RemotingCommand.create_request_command(code, None)` + 填 extFields/body。
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

/// Python `response.ext_fields.get(k, 0) or 0`：缺字段/非数字都当 0。
fn ext_int(response: &RemotingCommand, key: &str) -> i64 {
    response
        .get_ext_field(key)
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

// ---------------------------------------------------------------- 纯逻辑 helper
// 抽出来只为让「Vec 表」这几个最容易写错的地方能离线锁死；调用方只有一处。

/// Python `_broker_addrs_of_cluster`：不给集群名回全部 broker；给了就按
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

/// Python `get_userTopicConfig` 的过滤：系统 topic（broker 上报的 + 本地判定）
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

/// Python `getAllSubscriptionGroup` 里的 `table.update(part)`：同名覆盖、保持首现顺序。
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

/// Python `forbidden.update(part)`：两边都是 JSON object 时按键覆盖。
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

/// Python `examineTopicStats` 的合并：`offset_table.update(part)` + `topicPutTps += part`。
fn merge_topic_stats(merged: &mut TopicStatsTable, part: TopicStatsTable) {
    for (key, offset) in part.offset_table {
        match merged.offset_table.iter_mut().find(|(k, _)| *k == key) {
            Some(entry) => entry.1 = offset,
            None => merged.offset_table.push((key, offset)),
        }
    }
    merged.topic_put_tps += part.topic_put_tps;
}

/// Python `resetOffsetByTimestampOld` 里的 `route.queue_datas` 按 brokerName 过滤后
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

/// 真正的请求超时：显式给了用给的，否则用配置的 `timeout_millis`
/// （Python 的 `timeout_millis or self.timeout_millis`）。
fn effective_timeout(configured: i64, explicit: Option<i64>) -> i64 {
    explicit.filter(|t| *t > 0).unwrap_or(configured)
}

// ---------------------------------------------------------------- facade

/// 管理客户端（对应 Java `DefaultMQAdminExt`，移植自 Python `admin.DefaultMQAdminExt`）。
///
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
    /// Python `DefaultMQAdminExt()`。
    pub fn new() -> DefaultMQAdminExt {
        DefaultMQAdminExt::with_config(AdminConfig::default())
    }

    /// Python `DefaultMQAdminExt(rpc_hook=..)`。
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

    /// 当前配置快照（Python 直接读属性）。
    pub fn config(&self) -> AdminConfig {
        read_cfg(&self.inner.cfg)
    }

    /// 改配置（Python 的直接赋属性）。
    pub fn update_config(&self, f: impl FnOnce(&mut AdminConfig)) {
        {
            let mut w = self.inner.cfg.write().unwrap_or_else(|e| e.into_inner());
            f(&mut w);
        }
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

    /// Java `ClientConfig#setEnableStreamRequestType`。
    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.update_config(|c| c.enable_stream_request_type = enable);
    }

    /// Python `set_timeout_millis`。
    pub fn set_timeout_millis(&self, timeout_millis: i64) {
        self.update_config(|c| c.timeout_millis = timeout_millis);
    }

    /// Python `set_rpc_hook`。
    pub fn set_rpc_hook(&self, hook: Option<Arc<dyn RPCHook>>) {
        *self
            .inner
            .rpc_hook
            .write()
            .unwrap_or_else(|e| e.into_inner()) = hook;
    }

    /// Python `get_name_server_addr`（分号拼回）。
    pub fn get_name_server_addr(&self) -> String {
        read_cfg(&self.inner.cfg).name_server_addrs.join(";")
    }

    /// Python `get_name_server_address_list`。
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

    /// Python `start()`：幂等、必须有 name server，`client_id` 缺省时现造。
    ///
    /// admin 的 clientId 也是秒级 `instanceName@yyyyMMddHHmmss`（`admin.py:91`）；
    /// 同一秒起两个 admin 会撞 clientId，但 admin 用的是**私有实例**（见模块头差异 2），
    /// 后建的会直接覆盖实例表里的登记（Python 同语义），互不影响收发。
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
        // Java `DefaultMQAdminExtImpl#start`:161 无条件 `changeInstanceNameToPID`，
        // clientId 再走 `ClientConfig#buildMQClientId` 的
        // `<本机 IP>@<instanceName>[@unitName][@STREAM]`。
        // 本移植的 admin instanceName 默认是 "ADMIN"（不是 Java 的 "DEFAULT"，见模块头
        // 差异 2：admin 用私有实例，不和其他客户端共用），所以改写只在调用方显式
        // 设成 "DEFAULT" 时才起作用。
        let instance_name = MixAll::change_instance_name_to_pid(&cfg.instance_name);
        let client_id = cfg
            .client_id
            .clone()
            .unwrap_or_else(|| {
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

        // Python `admin.py:92`：直接构造私有实例（见模块头差异 2）。
        let client = MQClientInstance::with_config(
            &client_id,
            cfg.name_server_addrs.clone(),
            MQClientInstanceConfig {
                unit_name: cfg.unit_name.clone(),
                enable_stream_request_type: cfg.enable_stream_request_type,
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
        // 动态 name server：实例可能已从地址服务器拿到地址，回填（Python 同）。
        let addrs = client.name_server_addrs();
        if !addrs.is_empty() {
            self.update_config(|c| c.name_server_addrs = addrs);
        }
        *lock(&self.inner.client) = Some(client);
        Ok(())
    }

    /// Python `shutdown()`：幂等，只拆自己那份私有实例。
    pub fn shutdown(&self) {
        if !self.inner.started.swap(false, Ordering::AcqRel) {
            return;
        }
        if let Some(client) = lock(&self.inner.client).take() {
            client.shutdown();
        }
    }

    /// Python `get_mq_client_instance`。
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

    /// Python `_invoke_broker`：发到指定 Broker/NameServer 并校验 SUCCESS。
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
        let mut request = build_request(code, ext, body);
        let response = client.invoke_sync(addr, &mut request, timeout).await?;
        MQClientInstance::check_response(&response)?;
        Ok(response)
    }

    /// Python `_invoke_namesrv_all`：广播到每一台 NameServer
    /// （Java `putKVConfigValue` / `deleteKVConfigValue` 语义）。
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

    /// Python `_invoke_namesrv_one`：打到第一台可用的 NameServer
    /// （Java `invokeSync(null, ...)` 语义）。
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

    /// Python `_first_broker_addr`：从集群信息里取第一台 broker 地址。
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

    /// Python `_broker_addrs_of_cluster`。
    async fn broker_addrs_of_cluster(&self, cluster_name: Option<&str>) -> Result<Vec<String>> {
        let client = self.require_client()?;
        let timeout = self.timeout_millis();
        let cluster = client.get_broker_cluster_info(timeout).await?;
        Ok(cluster_broker_addrs(&cluster, cluster_name))
    }

    // ---------------- Topic 管理 ----------------

    /// Python `create_topic(key, new_topic, queue_num, topic_sys_flag)`：
    /// 走默认 topic 的路由逐 broker 下发（一台成功即可）。
    ///
    /// `key` 是 Java `MQAdminImpl#createTopic(topicPublishTable key, ...)` 的第一个实参，
    /// Python 同样收下不用（真正下发时用 `TBW102` 作 defaultTopic），这里保留形参。
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

    /// Python `create_and_update_topic_config`（Java 走 createTopicKey）。
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

    /// Python `create_topic_in_broker`。
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

    /// Python `delete_topic_in_broker`。
    pub async fn delete_topic_in_broker(&self, broker_addr: &str, topic: &str) -> Result<()> {
        let client = self.require_client()?;
        client
            .delete_topic_in_broker(broker_addr, topic, self.timeout_millis())
            .await
    }

    /// Python `delete_topic_in_name_server`：不给地址列表就清自己配的全部 NameServer。
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
            self.invoke_broker(
                &ns_addr,
                request_code::DELETE_TOPIC_IN_NAMESRV,
                &ext,
                None,
                None,
            )
            .await?;
        }
        Ok(())
    }

    /// Python `delete_topic_in_namesrv`（旧名兼容）：任取一台成功的 NameServer。
    pub async fn delete_topic_in_namesrv(&self, topic: &str) -> Result<()> {
        let client = self.require_client()?;
        client
            .delete_topic_in_namesrv(topic, self.timeout_millis())
            .await
    }

    /// Python `delete_topic`：先清各 broker，再清 NameServer 路由，最后清 KV namespace；
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

    /// Python `fetch_all_topic_list`。
    pub async fn fetch_all_topic_list(&self) -> Result<TopicList> {
        let client = self.require_client()?;
        client
            .get_all_topic_list_from_name_server(self.timeout_millis())
            .await
    }

    /// Python `fetch_topics_by_cluster`（`GET_TOPICS_BY_CLUSTER` 打到 NameServer）。
    ///
    /// 字段名用 Java `GetTopicsByClusterRequestHeader.cluster` 的 `cluster`：Python
    /// 这里发的是 `clusterName`，namesrv 取不到集群名 ⇒ `clusterAddrTable.get(null)`
    /// 抛 NPE 被吞掉，返回 SUCCESS + 空列表（看着像「这个集群没有 topic」）。
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

    /// Python `get_cluster_list`：含该 topic 路由 broker 的集群名集合。
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

    /// Python `get_topic_cluster_list`（别名）。
    pub async fn get_topic_cluster_list(&self, topic: &str) -> Result<Vec<String>> {
        self.get_cluster_list(topic).await
    }

    /// Python `fetch_all_topic_route`：遍历 NameServer 的 topic 列表逐个拉路由，
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

    /// Python `examine_topic_route`。
    pub async fn examine_topic_route(&self, topic: &str) -> Result<TopicRouteData> {
        let client = self.require_client()?;
        client
            .get_topic_route_data(topic)
            .await
            .ok_or_else(|| Error::client(format!("topic {topic} not exist")))
    }

    /// Python `examine_topic_config`（`GET_TOPIC_CONFIG`，body 为 TopicConfig JSON）。
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

    /// Python `get_all_topic_config`。
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

    /// Python `get_user_topic_config`：剔除系统 topic 与 `%RETRY%` / `%DLQ%`。
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

    /// Python `get_system_topic_list_from_broker`。
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

    /// Python `examine_topic_stats`：遍历该 topic 全部 broker 合并统计。
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
                    // Python 只 warn 后继续（broker 可能没开统计）。
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

    /// Python `examine_topic_stats_by_broker`。
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

    /// Python `fetch_broker_cluster_info`。
    pub async fn fetch_broker_cluster_info(&self) -> Result<ClusterInfo> {
        let client = self.require_client()?;
        client.get_broker_cluster_info(self.timeout_millis()).await
    }

    /// Python `examine_broker_cluster_info`（别名）。
    pub async fn examine_broker_cluster_info(&self) -> Result<ClusterInfo> {
        self.fetch_broker_cluster_info().await
    }

    /// Python `fetch_broker_runtime_stats`。
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

    /// Python `get_broker_runtime_info`（Java 旧接口名）。
    pub async fn get_broker_runtime_info(
        &self,
        broker_addr: &str,
        timeout_millis: Option<i64>,
    ) -> Result<KVTable> {
        self.fetch_broker_runtime_stats(broker_addr, timeout_millis)
            .await
    }

    /// Python `get_broker_config`：响应体是 **properties 文本**，不是 JSON/KVTable。
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

    /// Python `update_broker_config`（含 `Validators.checkBrokerConfig` 的
    /// brokerPermission 校验）。
    ///
    /// 校验排在任何 IO 之前（Python 同理：`_invoke_broker` 是该函数最后一句），
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

    /// Python `wipe_write_perm_of_broker`：读响应头的 `wipeTopicCount`。
    pub async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: &str,
        broker_name: &str,
    ) -> Result<i64> {
        let ext = ext_pairs(&[("brokerName", broker_name.to_string())]);
        let response = self
            .invoke_broker(
                namesrv_addr,
                request_code::WIPE_WRITE_PERM_OF_BROKER,
                &ext,
                None,
                None,
            )
            .await?;
        Ok(ext_int(&response, "wipeTopicCount"))
    }

    /// Python `add_write_perm_of_broker`：读响应头的 `addTopicCount`。
    pub async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: &str,
        broker_name: &str,
    ) -> Result<i64> {
        let ext = ext_pairs(&[("brokerName", broker_name.to_string())]);
        let response = self
            .invoke_broker(
                namesrv_addr,
                request_code::ADD_WRITE_PERM_OF_BROKER,
                &ext,
                None,
                None,
            )
            .await?;
        Ok(ext_int(&response, "addTopicCount"))
    }

    /// Python `clean_unused_topic`：逐 broker 下发 `CLEAN_UNUSED_TOPIC`，
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

    /// Python `view_broker_stats_data`。
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

    /// Python `create_and_update_kv_config` → Java `putKVConfigValue`（广播全部 NameServer）。
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

    /// Python `put_kv_config`（Java `DefaultMQAdminExtImpl.putKVConfig` 是空实现，
    /// 真正干活的就是 `createAndUpdateKvConfig`）。
    pub async fn put_kv_config(&self, namespace: &str, key: &str, value: &str) -> Result<()> {
        self.create_and_update_kv_config(namespace, key, value)
            .await
    }

    /// Python `get_kv_config`：非 SUCCESS 回 `None`（不抛）。
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

    /// Python `delete_kv_config`（广播全部 NameServer）。
    pub async fn delete_kv_config(&self, namespace: &str, key: &str) -> Result<()> {
        let ext = ext_pairs(&[
            ("namespace", namespace.to_string()),
            ("key", key.to_string()),
        ]);
        self.invoke_namesrv_all(request_code::DELETE_KV_CONFIG, &ext, None)
            .await
    }

    /// Python `get_kv_list_by_namespace`。
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

    /// Python `create_and_update_subscription_group_config`：body 为 config JSON。
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

    /// Python `examine_subscription_group_config`：拉全部订阅组后取目标 group。
    pub async fn examine_subscription_group_config(
        &self,
        addr: &str,
        group: &str,
    ) -> Result<Option<SubscriptionGroupConfig>> {
        let wrapper = self.get_all_subscription_group(addr, None).await?;
        Ok(wrapper.get(group).cloned())
    }

    /// Python `get_subscription_group_config`（`GET_SUBSCRIPTIONGROUP_CONFIG` 单查）。
    ///
    /// ⚠ 这是个**会写**的读接口：Java `AdminBrokerProcessor#getSubscriptionGroup` 走
    /// `findSubscriptionGroupConfig`，broker 开了 `autoCreateSubscriptionGroup`（默认
    /// true）时，查一个不存在的组会顺手把它按默认配置建出来。所以「删组之后还在不在」
    /// 必须用 [`Self::examine_subscription_group_config`]（只读全量表）判定。
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

    /// Python `get_all_subscription_group`：**分页**累积，直到 `groupSeq >= totalGroupNum-1`。
    ///
    /// 老版本 broker 不带 `totalGroupNum`，此时一次性返回全部（单轮即结束）。
    /// 中途 `dataVersion` 变了就清零重来（Java 同），并且整轮受一个总超时约束。
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
            let response = client.invoke_sync(broker_addr, &mut request, left).await?;
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

    /// Python `get_user_subscription_group`：剔掉系统组与预定义组。
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

    /// Python `delete_subscription_group`。
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

    /// Python `examine_consumer_connection_info`。
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

    /// Python `examine_consumer_connection`（别名）。
    pub async fn examine_consumer_connection(
        &self,
        consumer_group: &str,
        broker_addr: Option<&str>,
    ) -> Result<ConsumerConnection> {
        self.examine_consumer_connection_info(consumer_group, broker_addr)
            .await
    }

    /// Python `examine_producer_connection_info`。
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

    /// Python `examine_consumer_running_info`（broker 转发给目标客户端的 38 号请求，
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

    /// Python `get_consumer_running_info`（别名）。
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

    /// Python `get_consumer_list_by_group`。
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

    /// Python `examine_consume_stats`。
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

    /// Python `fetch_consume_stats_in_broker`。
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

    /// Python `query_topic_consume_by_who`（300 → `groupList`）。
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

    /// Python `query_topics_by_consumer`（343）。
    pub async fn query_topics_by_consumer(
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

    /// Python `query_subscription`（345）。
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

    /// Python `get_consume_status`（223 → `consumerTable`）。
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

    /// Python `clone_group_offset`（314）。
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

    /// Python `max_offset`。
    pub async fn max_offset(&self, mq: &MessageQueue) -> Result<i64> {
        self.require_client()?
            .get_max_offset(mq, self.timeout_millis(), None)
            .await
    }

    /// Python `min_offset`。
    pub async fn min_offset(&self, mq: &MessageQueue) -> Result<i64> {
        self.require_client()?
            .get_min_offset(mq, self.timeout_millis(), None)
            .await
    }

    /// Python `search_offset`。
    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: i64) -> Result<i64> {
        self.require_client()?
            .search_offset_by_timestamp(mq, timestamp, self.timeout_millis(), None)
            .await
    }

    /// Python `earliest_msg_store_time`。
    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> Result<i64> {
        self.require_client()?
            .get_earliest_msg_store_time(mq, self.timeout_millis(), None)
            .await
    }

    /// Python `examine_consumer_offset`：没提交过位点回 `None`（`QUERY_NOT_FOUND`）。
    pub async fn examine_consumer_offset(
        &self,
        consumer_group: &str,
        mq: &MessageQueue,
    ) -> Result<Option<i64>> {
        self.require_client()?
            .query_consumer_offset(consumer_group, mq, self.timeout_millis(), None, false)
            .await
    }

    /// Python `update_consumer_offset`（addr 由路由解析）。
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

    /// Python `update_consumer_offset_to_broker`（指定 addr）。
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

    /// Python `reset_offset_by_timestamp`（对应 Java 同名）：
    /// 逐 broker 下发 `INVOKE_BROKER_TO_RESET_OFFSET`(222)，由 broker 端按 timestamp
    /// 算新位点、同步在线消费者并写 offset 表，汇总 `Map<MessageQueue, Long>`。
    ///
    /// 这里**不再**走「逐队列 searchOffset + updateConsumerOffset」的旧本地实现 ——
    /// 那不会同步在线消费者，也不会做 broker 端一致性校验（旧实现保留在
    /// [`DefaultMQAdminExt::reset_offset_by_timestamp_old`]，只服务于 Java 的 old 分支）。
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
        let client = self.require_client()?;
        // Python：LMQ / wheel_timer 这类没有独立路由的 topic，按集群名去查路由，
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
            let ext = ext_pairs(&[
                ("topic", topic.to_string()),
                ("group", group.to_string()),
                ("timestamp", timestamp.to_string()),
                ("force", bool_flag(is_force).to_string()),
                // Java：offset=-1 表示 offset 为空
                ("offset", "-1".to_string()),
            ]);
            let mut request =
                build_request(request_code::INVOKE_BROKER_TO_RESET_OFFSET, &ext, None);
            if is_cpp {
                request.language = language_code::CPP;
            }
            let response = client
                .invoke_sync(&addr, &mut request, self.timeout_millis())
                .await?;
            if response.code != response_code::SUCCESS {
                return Err(Error::client_with_code(
                    response.code,
                    response
                        .remark
                        .unwrap_or_else(|| "reset offset failed".to_string()),
                ));
            }
            if let Some(body) = response.body.as_deref().filter(|b| !b.is_empty()) {
                for (key, offset) in ResetOffsetBody::decode(body)?.offset_table {
                    match all_offsets.iter_mut().find(|(k, _)| *k == key) {
                        Some(entry) => entry.1 = offset,
                        None => all_offsets.push((key, offset)),
                    }
                }
            }
        }
        if all_offsets.is_empty() {
            bail!("reset offset failed, no broker returned offset table");
        }
        Ok(all_offsets)
    }

    /// Python `reset_offset_new`：先试新版（broker 端重置），消费者不在线再退化到旧版。
    pub async fn reset_offset_new(
        &self,
        consumer_group: &str,
        topic: &str,
        timestamp: i64,
    ) -> Result<()> {
        match self
            .reset_offset_by_timestamp(topic, consumer_group, timestamp, true, None, true)
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

    /// Python `reset_offset_by_timestamp_old`：逐队列 `searchOffset` 后按 force 决策写回。
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
                // Python：查位点失败当 0（该队列压根没提交过位点是常态）。
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

    /// Python `query_message`（正常 key：查所有 broker + 客户端侧 key 二次校验）。
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

    /// Python `query_message_by_uniq_key`：`indexType="U"` + `_UNIQUE_KEY_QUERY="true"`。
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

    /// Python `query_message_by_key`（工具 `queryMsgByKey` 的 NORMAL 模式）。
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

    /// `MQClientInstance::query_message_all_brokers` 的门面壳。未 start 时按 Python
    /// `_require_client` 的口径直接抛错；indexType 由 `uniq_key` 唯一决定
    /// （Python 的三个入口也只有这两种组合）。
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

    /// Python `view_message`（Java `DefaultMQAdminExtImpl#viewMessage`）。
    ///
    /// Java 先按 offsetMsgId 解出 broker 地址 + commitLog 偏移走 `VIEW_MESSAGE_BY_ID`，
    /// **任何**失败都退回按 UNIQ_KEY 查索引。这条兜底必须留：3.x 时代的 offsetMsgId 与
    /// 5.x 客户端生成的 uniqKey 都是 32 位十六进制，硬解会拼出一个不存在的 `ip:port`；
    /// 端口越界在 Rust 里更是直接不可表示，所以显式判 `0 < port <= 65535`
    /// （`decode_message_id` 返回的端口是 `u32`，Python 不校验就会把 `OverflowError`
    /// 抛到调用方手里，异常契约直接破掉）。
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

        // Python 同样把 uniq 查询放在 try/except 之外：该路径真抛时直接透传，
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

    /// Python `query_consume_queue`（321）。
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
}

/// Python `RemotingSerializable.decode_json(body).get(key, [])` 里取字符串数组。
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
    use crate::remoting::protocol::admin_body::TopicOffset;
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
        assert!(!admin.is_started());
        assert_eq!(admin.client_id(), "");
    }

    #[test]
    fn namesrv_addr_splits_trims_and_drops_empty() {
        // Python `set_namesrv_addr`：`[a.strip() for a in addr.split(";") if a.strip()]`
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

    /// Python `start()` 在没有 name server 时抛、且**不**留在 started。
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
        // 查询类同样要 start（Python `_require_client` 直接 raise），不能静默回空集。
        assert!(admin.query_message("T", "k", 32, 0, 1).await.is_err());
        assert!(admin.query_message_by_uniq_key("T", "k").await.is_err());
        assert!(admin.query_message_by_key("T", "k", 32).await.is_err());
    }

    /// 非法 brokerPermission 必须**在任何 IO 之前**报出来（Python 同理：
    /// `_invoke_broker` 是该函数最后一句），所以未 start 的 admin 也给 16。
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

        // 空 properties：Python 直接 return，不发请求也不报错。
        assert!(admin
            .update_broker_config("127.0.0.1:10911", &StringMap::new(), None)
            .await
            .is_ok());
    }

    #[test]
    fn cluster_broker_addrs_covers_python_branches() {
        let info = cluster();
        // 不给集群名：Python 回 `cluster.get_broker_addrs()`（全量）。
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
        // 未知集群名 / 空串都退回全量（Python 的 `or []` + `result or all`）。
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
        // Python 的过滤条件是「broker 上报的系统 topic 列表 ∪ MixAll.is_sys_topic」，
        // 而 is_sys_topic 只认 `rmq_sys_` 前缀（与 Python 完全一致），所以
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

        // 不在 broker 系统列表、也不带 rmq_sys_ 前缀的裸名会保留（Python 同行为）。
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
        // 同名覆盖、顺序保持首现（Python dict.update 同语义）。
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
        // Python `timeout_millis or self.timeout_millis`
        assert_eq!(effective_timeout(15_000, Some(3_000)), 3_000);
        assert_eq!(effective_timeout(15_000, Some(0)), 15_000);
        assert_eq!(effective_timeout(15_000, None), 15_000);
        // Python `"true" if x else "false"`
        assert_eq!((bool_flag(true), bool_flag(false)), ("true", "false"));

        // Python `response.ext_fields.get(k, 0) or 0`：缺字段与非数字都是 0
        let mut cmd = RemotingCommand::new();
        assert_eq!(ext_int(&cmd, "wipeTopicCount"), 0);
        cmd.add_ext_field("wipeTopicCount", " 12 ");
        assert_eq!(ext_int(&cmd, "wipeTopicCount"), 12);
        cmd.add_ext_field("addTopicCount", "abc");
        assert_eq!(ext_int(&cmd, "addTopicCount"), 0);

        // Python `obj.get("groupList", [])`
        assert_eq!(
            string_list(&json!({"groupList": ["a", 1, "b"]}), "groupList"),
            vec!["a".to_string(), "b".to_string()]
        );
        assert!(string_list(&json!({}), "topicList").is_empty());
    }

    /// 抄送一遍 Python/Java 的码值口径：管理端的纯客户端错都是 code=1（`None` 的
    /// `Error::Client` 在 Display 里没有码），带 broker 语义的才带码。
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
        // Python 把 uniqKey 兜底放在 try/except **之外**：未 start 时透传
        // `_require_client` 的客户端错，而不是 NO_MESSAGE(208)。208 只在真正
        // 查不到消息时出现，那条断言放在 live_admin 示例里（需要真实 broker）。
        assert_eq!(view_err.response_code(), None);
        assert!(view_err
            .to_string()
            .starts_with("MQClientException: admin not started"));
    }
}
